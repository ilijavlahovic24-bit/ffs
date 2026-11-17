use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::Mutex;

use common::error::FfsError;
use common::types::{BlockRef, FileType, InodeId, InodeInfo, BLOCK_SIZE};
use meta::{InodeManager, MetaStore};
use network::RaftForwarder;
use storage::allocator::Allocator;
use storage::ChunkStore;

use crate::metrics::VfsMetrics;

pub struct VfsLayer {
    inodes: Arc<InodeManager>,
    meta: Arc<MetaStore>,
    chunks: Arc<ChunkStore>,
    #[allow(dead_code)]
    allocator: Arc<Allocator>,
    forwarder: Arc<RaftForwarder>,
    pub metrics: Arc<VfsMetrics>,

    /// Per-inode write lock.
    ///
    /// FUSE dispatches multiple `write` calls on the same file handle
    /// concurrently. Without serialization, the second write reads
    /// `info.size` before the first commits its new manifest through
    /// Raft, so it sees a stale size and fails the offset check.
    ///
    /// We lock per-inode rather than globally so writes to different
    /// files stay parallel.
    write_locks: DashMap<InodeId, Arc<Mutex<()>>>,
}

impl VfsLayer {
    pub fn new(
        inodes: Arc<InodeManager>,
        meta: Arc<MetaStore>,
        chunks: Arc<ChunkStore>,
        allocator: Arc<Allocator>,
        forwarder: Arc<RaftForwarder>,
    ) -> Self {
        Self {
            inodes,
            meta,
            chunks,
            allocator,
            forwarder,
            metrics: Arc::new(VfsMetrics::new()),
            write_locks: DashMap::new(),
        }
    }

    // ---------- Namespace reads ----------

    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<InodeInfo, FfsError> {
        let ino = self
            .inodes
            .lookup(parent, name)
            .ok_or(FfsError::NotFound(parent))?;
        self.inodes.get_inode(ino).ok_or(FfsError::NotFound(ino))
    }

    pub fn getattr(&self, ino: InodeId) -> Result<InodeInfo, FfsError> {
        self.inodes.get_inode(ino).ok_or(FfsError::NotFound(ino))
    }

    pub fn readdir(&self, ino: InodeId) -> Result<Vec<InodeInfo>, FfsError> {
        if self.inodes.get_inode(ino).is_none() {
            return Err(FfsError::NotFound(ino));
        }
        Ok(self.inodes.children(ino))
    }

    // ---------- Block I/O helpers ----------

    async fn get_block(&self, block_id: u64) -> Result<Vec<u8>, FfsError> {
        match self.chunks.read_block(block_id).await {
            Ok(d) => {
                self.metrics.inc_local_read();
                Ok(d)
            }
            Err(FfsError::NotFound(_)) => {
                self.metrics.inc_forwarded_read();
                let d = self.forwarder.fetch_block(block_id).await?;
                // Cache locally so the next read is local.
                let _ = self.chunks.write_block(block_id, &d).await;
                Ok(d)
            }
            Err(e) => Err(e),
        }
    }

    async fn put_block(&self, block_id: u64, data: &[u8]) -> Result<(), FfsError> {
        if self.forwarder.is_leader().await {
            // Local write first, then fan out to peers (majority ack).
            self.chunks.write_block(block_id, data).await?;
            self.forwarder.replicate_block(block_id, data).await?;
        } else {
            // Follower forwards to leader; leader writes + replicates.
            self.forwarder.write_block(block_id, data).await?;
        }
        Ok(())
    }

    // ---------- File read ----------

    pub async fn read(
        &self,
        ino: InodeId,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, FfsError> {
        let info = self.getattr(ino)?;
        if info.kind == FileType::Directory {
            return Err(FfsError::StorageError("read on directory".into()));
        }

        if size == 0 {
            return Ok(Vec::new());
        }

        let start = offset;
        let end = offset + size as u64;
        let manifest = self.inodes.get_manifest(ino);
        let mut out = Vec::with_capacity(size as usize);

        for block_ref in &manifest {
            if block_ref.end() <= start {
                continue;
            }
            if block_ref.offset >= end {
                break;
            }

            let block_data = self.get_block(block_ref.block_id).await?;

            let from = start.saturating_sub(block_ref.offset) as usize;
            let to = (end.min(block_ref.end()) - block_ref.offset) as usize;

            if from < block_data.len() && to <= block_data.len() && from < to {
                out.extend_from_slice(&block_data[from..to]);
            }
        }

        Ok(out)
    }

    // ---------- File write ----------

    pub async fn write(
        &self,
        ino: InodeId,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FfsError> {
        // Serialize writes on the same inode so the second write sees the
        // updated size after the first commits its manifest.
        let lock = self
            .write_locks
            .entry(ino)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let info = self.getattr(ino)?;
        if info.kind == FileType::Directory {
            return Err(FfsError::StorageError("write on directory".into()));
        }

        // M3 supports full overwrite (offset 0) and append (offset == size).
        // Random write into the middle is Phase 10.
        if offset != 0 && offset != info.size {
            return Err(FfsError::StorageError(format!(
                "only append supported (offset {} != size {}); random write is Phase 10",
                offset, info.size
            )));
        }

        // Disallow truncating overwrite: we don't yet delete orphaned blocks.
        if offset == 0 && info.size > 0 && (data.len() as u64) < info.size {
            return Err(FfsError::StorageError(
                "overwrite of existing non-empty file is Phase 10".into(),
            ));
        }

        // Start from empty manifest on full overwrite, else extend existing.
        let mut manifest = if offset == 0 {
            Vec::new()
        } else {
            self.inodes.get_manifest(ino)
        };

        let mut remaining = data;
        let mut cur_offset = offset;

        // If the last block has room, fill it first (append within block).
        if let Some(last) = manifest.last_mut() {
            if (last.len as usize) < BLOCK_SIZE && last.end() == cur_offset {
                let room = BLOCK_SIZE - last.len as usize;
                let to_write = remaining.len().min(room);

                let mut block_data = self.get_block(last.block_id).await?;
                block_data.extend_from_slice(&remaining[..to_write]);

                let new_id = self.chunks.alloc_block_id();
                self.put_block(new_id, &block_data).await?;

                last.block_id = new_id;
                last.len = block_data.len() as u32;

                remaining = &remaining[to_write..];
                cur_offset += to_write as u64;
            }
        }

        // Emit full blocks for the rest.
        while !remaining.is_empty() {
            let take = remaining.len().min(BLOCK_SIZE);
            let block_data = &remaining[..take];
            let new_id = self.chunks.alloc_block_id();
            self.put_block(new_id, block_data).await?;

            manifest.push(BlockRef {
                block_id: new_id,
                offset: cur_offset,
                len: take as u32,
            });

            remaining = &remaining[take..];
            cur_offset += take as u64;
        }

        // Commit the manifest (and therefore the size) through Raft.
        self.meta.update_manifest(ino, manifest).await?;

        Ok(data.len() as u32)
    }

    // ---------- Truncate ----------

    pub async fn truncate(&self, ino: InodeId, size: u64) -> Result<(), FfsError> {
        if size != 0 {
            return Err(FfsError::StorageError(
                "truncate to nonzero is Phase 10".into(),
            ));
        }

        let lock = self
            .write_locks
            .entry(ino)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let old_manifest = self.inodes.get_manifest(ino);
        for b in &old_manifest {
            let _ = self.chunks.delete_block(b.block_id).await;
        }

        self.meta.update_manifest(ino, Vec::new()).await?;
        Ok(())
    }

    pub async fn chmod(&self, ino: InodeId, mode: u16) -> Result<(), FfsError> {
        self.meta.chmod(ino, mode).await
    }

    // ---------- Namespace writes ----------

    pub async fn create(
        &self,
        parent: InodeId,
        name: String,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        if self.forwarder.is_leader().await {
            self.metrics.inc_local_proposal();
        } else {
            self.metrics.inc_forwarded_proposal();
        }
        self.meta.create(parent, name, FileType::File, mode).await
    }

    pub async fn mkdir(
        &self,
        parent: InodeId,
        name: String,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        if self.forwarder.is_leader().await {
            self.metrics.inc_local_proposal();
        } else {
            self.metrics.inc_forwarded_proposal();
        }
        self.meta.mkdir(parent, name, mode).await
    }

    pub async fn unlink(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        if self.forwarder.is_leader().await {
            self.metrics.inc_local_proposal();
        } else {
            self.metrics.inc_forwarded_proposal();
        }

        // Delete blocks before removing inode so no orphans on success.
        if let Some(ino) = self.inodes.lookup(parent, &name) {
            let manifest = self.inodes.get_manifest(ino);
            for b in &manifest {
                let _ = self.chunks.delete_block(b.block_id).await;
            }
        }

        self.meta.unlink(parent, name).await
    }

    pub async fn rmdir(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        if let Some(ino) = self.inodes.lookup(parent, &name) {
            if !self.inodes.children(ino).is_empty() {
                return Err(FfsError::StorageError("directory not empty".into()));
            }
        } else {
            return Err(FfsError::NotFound(parent));
        }

        if self.forwarder.is_leader().await {
            self.metrics.inc_local_proposal();
        } else {
            self.metrics.inc_forwarded_proposal();
        }

        self.meta.rmdir(parent, name).await
    }

    pub async fn rename(
        &self,
        old_parent: InodeId,
        old_name: String,
        new_parent: InodeId,
        new_name: String,
    ) -> Result<(), FfsError> {
        if self.forwarder.is_leader().await {
            self.metrics.inc_local_proposal();
        } else {
            self.metrics.inc_forwarded_proposal();
        }
        self.meta
            .rename(old_parent, old_name, new_parent, new_name)
            .await
    }
}