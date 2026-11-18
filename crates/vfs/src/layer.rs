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

    /// Per-inode write lock. Serializes writes on the same inode so the
    /// second write sees the updated size after the first commits.
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
                self.metrics.inc_block_local_hit(d.len() as u64);
                Ok(d)
            }
            Err(FfsError::NotFound(_)) => {
                self.metrics.inc_forwarded_read();
                let d = self.forwarder.fetch_block(block_id).await?;
                self.metrics.inc_block_peer_fetch(d.len() as u64);
                let _ = self.chunks.write_block(block_id, &d).await;
                Ok(d)
            }
            Err(e) => Err(e),
        }
    }


    /// Write one block.
    ///
    /// On the leader: local write is synchronous (leader's copy is durable
    /// before we return), replication to peers is fire-and-forget in a
    /// spawned task. This keeps FUSE write handler latency under the
    /// kernel's ~1s timeout; blocking on all replicas would exceed it.
    ///
    /// On a follower: forward to leader and wait. The leader acks only
    /// after its own fsync, so the data is durable before we return.
    async fn put_block(&self, block_id: u64, data: &[u8]) -> Result<(), FfsError> {
        if self.forwarder.is_leader().await {
            let t0 = std::time::Instant::now();
            self.chunks.write_block(block_id, data).await?;
            tracing::info!(
                block_id,
                bytes = data.len(),
                ms = t0.elapsed().as_millis(),
                "local block write"
            );

            // Fire-and-forget replication. Do NOT block — FUSE write has
            // a kernel-side timeout and we've already done a fsync above.
            // Peers that miss the block fetch it lazily on next read.
            let peers = self.forwarder.peers_snapshot();
            let data_owned = data.to_vec();
            tokio::spawn(async move {
                let t1 = std::time::Instant::now();
                match network::forwarder::replicate_to_all(&peers, block_id, &data_owned).await {
                    Ok(()) => {
                        tracing::info!(
                            block_id,
                            ms = t1.elapsed().as_millis(),
                            "background replicate done"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            block_id,
                            ms = t1.elapsed().as_millis(),
                            "background replicate failed: {e}"
                        );
                    }
                }
            });

            Ok(())
        } else {
            self.forwarder.write_block(block_id, data).await
        }
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
        // Serialize writes on the same inode. FUSE dispatches multiple
        // write calls concurrently; without this, the second write sees
        // stale `info.size` before the first commits its manifest.
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

        let mut manifest = if offset == 0 {
            Vec::new()
        } else {
            self.inodes.get_manifest(ino)
        };

        // Always emit new blocks per write. No read-modify-write of the
        // last block — that would be O(N²) during large sequential writes
        // (write 1 MB, then read 1 MB + write 2 MB, then read 2 MB + write
        // 3 MB, ...). With new blocks per write, each write is a single
        // block write + replicate.
        let mut remaining = data;
        let mut cur_offset = offset;

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

        // Commit manifest (and therefore size) through Raft.
        let t2 = std::time::Instant::now();
        self.meta.update_manifest(ino, manifest).await?;
        tracing::info!(
            ino,
            blocks = self.inodes.get_manifest(ino).len(),
            ms = t2.elapsed().as_millis(),
            "manifest update (raft)"
        );

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

    /// Ensure every block mentioned in a manifest is present locally.
    ///
    /// Called at startup after the state machine has caught up. Fetches
    /// missing blocks from the leader. Blocks we can't fetch remain
    /// absent; they are fetched lazily on the next read.
    ///
    /// Runs in the background — mount does not wait for it to finish.
    pub async fn startup_repair(self: Arc<Self>) {
        // Give Raft a moment to elect a leader.
        let mut wait_attempts = 0;
        while self.forwarder.current_leader().await.is_none() && wait_attempts < 600 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            wait_attempts += 1;
        }

        let self_id = self.forwarder.node_id().await;
        let leader = self.forwarder.current_leader().await;

        if leader == Some(self_id) {
            tracing::info!("startup_repair: this node is leader, nothing to fetch");
            return;
        }
        if leader.is_none() {
            tracing::warn!("startup_repair: no leader after 60s, skipping");
            return;
        }

        let inodes = self.inodes.inodes_with_manifests();
        let mut total_blocks: u64 = 0;
        let mut missing: u64 = 0;
        let mut fetched: u64 = 0;

        for ino in inodes {
            for block_ref in self.inodes.get_manifest(ino) {
                total_blocks += 1;
                if self.chunks.has_block(block_ref.block_id).await {
                    continue;
                }
                missing += 1;
                match self.forwarder.fetch_block(block_ref.block_id).await {
                    Ok(data) => {
                        if self
                            .chunks
                            .write_block(block_ref.block_id, &data)
                            .await
                            .is_ok()
                        {
                            fetched += 1;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            block_id = block_ref.block_id,
                            "startup_repair fetch failed: {e}"
                        );
                    }
                }
            }
        }

        if missing > 0 {
            tracing::info!(
                total = total_blocks,
                missing,
                fetched,
                "startup_repair: done"
            );
        } else {
            tracing::info!(
                total = total_blocks,
                "startup_repair: all blocks present"
            );
        }
    }


}