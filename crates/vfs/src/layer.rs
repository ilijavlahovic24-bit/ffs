use std::sync::Arc;

use common::error::FfsError;
use common::types::{FileType, InodeId, InodeInfo};
use meta::{InodeManager, MetaStore};
use network::RaftForwarder;
use storage::allocator::Allocator;
use storage::wal::DataPath;

use crate::metrics::VfsMetrics;

/// Files larger than this go through the blob path.
/// Files smaller go through the block path (currently the same code —
/// see the `write` implementation).
pub const LARGE_FILE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

/// Interposition layer between FUSE handlers and the storage/meta stack.
///
/// Reads are served from the local state machine; blob reads are forwarded
/// to the leader when this node is a follower (ADR-003: data is not
/// replicated, only metadata is).
///
/// Writes go through `MetaStore` which routes through Raft.
pub struct VfsLayer {
    inodes: Arc<InodeManager>,
    meta: Arc<MetaStore>,
    data: Arc<DataPath>,
    #[allow(dead_code)]
    allocator: Arc<Allocator>,
    forwarder: Arc<RaftForwarder>,
    pub metrics: Arc<VfsMetrics>,
}

impl VfsLayer {
    pub fn new(
        inodes: Arc<InodeManager>,
        meta: Arc<MetaStore>,
        data: Arc<DataPath>,
        allocator: Arc<Allocator>,
        forwarder: Arc<RaftForwarder>,
    ) -> Self {
        Self {
            inodes,
            meta,
            data,
            allocator,
            forwarder,
            metrics: Arc::new(VfsMetrics::new()),
        }
    }

    // --- Reads ----------------------------------------------------------

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

    /// Read `size` bytes from `ino` starting at `offset`.
    ///
    /// Blob data is stored only on the leader (ADR-003). If this node is
    /// not the leader, the read is forwarded over TCP.
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

        let bytes = if self.forwarder.is_leader().await {
            self.metrics.inc_local_read();
            match self.data.read_blob(ino).await {
                Ok(b) => b,
                Err(FfsError::NotFound(_)) => Vec::new(),
                Err(e) => return Err(e),
            }
        } else {
            self.metrics.inc_forwarded_read();
            match self.forwarder.read_blob(ino).await {
                Ok(b) => b,
                Err(FfsError::NotFound(_)) => Vec::new(),
                Err(e) => return Err(e),
            }
        };

        let start = offset as usize;
        if start >= bytes.len() {
            return Ok(Vec::new());
        }
        let end = (start + size as usize).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    // --- Writes ---------------------------------------------------------

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
        self.meta.unlink(parent, name).await
    }

    pub async fn rmdir(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        // Non-empty check before proposing — a directory with children
        // cannot be removed.
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

    /// Truncate `ino` to `size` bytes.
    ///
    /// If we're on the leader, we handle the blob data locally; otherwise
    /// the metadata proposal goes through the forwarder and the data
    /// rewrite happens on the leader (currently unsupported — non-zero
    /// truncate from a follower is rejected).
    pub async fn truncate(&self, ino: InodeId, size: u64) -> Result<(), FfsError> {
        let is_leader = self.forwarder.is_leader().await;

        if size == 0 {
            // Fast path: empty the blob.
            if is_leader {
                self.data.write_blob(ino, &[]).await?;
            } else {
                // Follower cannot rewrite data — reject for now.
                // (Truncating to zero only needs metadata for correctness
                // of size, but the blob would linger on the leader.)
                if self.getattr(ino).map(|i| i.size).unwrap_or(0) > 0 {
                    return Err(FfsError::StorageError(
                        "truncate to zero from follower not supported".into(),
                    ));
                }
            }
            self.meta.truncate(ino, 0).await?;
            return Ok(());
        }

        if !is_leader {
            return Err(FfsError::StorageError(
                "truncate from follower not supported".into(),
            ));
        }

        // Leader path: read, resize, write back.
        let current = match self.data.read_blob(ino).await {
            Ok(b) => b,
            Err(FfsError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(e),
        };
        let new_len = (size as usize).min(current.len());
        let mut new_data = current[..new_len].to_vec();
        if (size as usize) > new_data.len() {
            new_data.resize(size as usize, 0);
        }
        self.data.write_blob(ino, &new_data).await?;
        self.meta.truncate(ino, size).await?;
        Ok(())
    }

    pub async fn chmod(&self, ino: InodeId, mode: u16) -> Result<(), FfsError> {
        self.meta.chmod(ino, mode).await
    }

    /// Write `data` to `ino` at `offset`.
    ///
    /// Only offset 0 is supported in this phase. Blob data is written
    /// locally if we're the leader; a follower forwards the metadata
    /// proposal but the data write itself is rejected (ADR-003 — data
    /// lives on the leader only).
    pub async fn write(
        &self,
        ino: InodeId,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FfsError> {
        let info = self.getattr(ino)?;
        if info.kind == FileType::Directory {
            return Err(FfsError::StorageError("write on directory".into()));
        }

        if offset != 0 {
            return Err(FfsError::StorageError(
                "non-zero offset not supported yet".into(),
            ));
        }

        // ADR-003: data lives on the leader. Followers forward the blob
        // write over TCP; only the metadata (size) is replicated via Raft.
        if self.forwarder.is_leader().await {
            self.metrics.inc_local_proposal();
            self.data.write_blob(ino, data).await?;
        } else {
            self.metrics.inc_forwarded_proposal();
            self.forwarder.write_blob(ino, data).await?;
        }

        // Update size metadata through Raft so all nodes agree.
        self.meta.truncate(ino, data.len() as u64).await?;

        Ok(data.len() as u32)
    }
}