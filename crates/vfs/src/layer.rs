use std::sync::Arc;

use common::error::FfsError;
use common::types::{FileType, InodeId, InodeInfo};
use meta::{InodeManager, MetaStore};
use storage::allocator::Allocator;
use storage::wal::DataPath;

/// Files larger than this go through the blob path.
/// Files smaller go through the block path (currently the same code —
/// see the `write` implementation).
pub const LARGE_FILE_THRESHOLD: usize = 1024 * 1024; // 1 MiB

pub struct VfsLayer {
    inodes: Arc<InodeManager>,
    meta: Arc<MetaStore>,
    data: Arc<DataPath>,
    #[allow(dead_code)]
    allocator: Arc<Allocator>,
}

impl VfsLayer {
    pub fn new(
        inodes: Arc<InodeManager>,
        meta: Arc<MetaStore>,
        data: Arc<DataPath>,
        allocator: Arc<Allocator>,
    ) -> Self {
        Self { inodes, meta, data, allocator }
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

        let bytes = match self.data.read_blob(ino).await {
            Ok(b) => b,
            Err(FfsError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(e),
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
        self.meta.create(parent, name, FileType::File, mode).await
    }

    pub async fn mkdir(
        &self,
        parent: InodeId,
        name: String,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        self.meta.mkdir(parent, name, mode).await
    }

    pub async fn unlink(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        self.meta.unlink(parent, name).await
    }

    pub async fn rmdir(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        // Non-empty check before proposing.
        if let Ok(ino) = self
            .inodes
            .lookup(parent, &name)
            .ok_or(FfsError::NotFound(parent))
        {
            if !self.inodes.children(ino).is_empty() {
                return Err(FfsError::StorageError("directory not empty".into()));
            }
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
        self.meta
            .rename(old_parent, old_name, new_parent, new_name)
            .await
    }

    pub async fn truncate(&self, ino: InodeId, size: u64) -> Result<(), FfsError> {
        if size == 0 {
            // Fast path: just update metadata. Data file may still exist
            // on disk but won't be read (size = 0).
            self.meta.truncate(ino, 0).await?;
            self.data.write_blob(ino, &[]).await?;
            return Ok(());
        }

        // Shrinking to `size`: read, truncate, write.
        let current = match self.data.read_blob(ino).await {
            Ok(b) => b,
            Err(FfsError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(e),
        };
        let new_len = (size as usize).min(current.len());
        let mut new_data = current[..new_len].to_vec();
        // If growing, pad with zeros.
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

        // Phase 7: only offset 0 supported.
        // Phase 8+: read-modify-write for partial writes.
        if offset != 0 {
            return Err(FfsError::StorageError(
                "non-zero offset not supported yet".into(),
            ));
        }

        // ADR-003: VFS decides large vs small file.
        // Both paths currently route to `DataPath` (atomic blob write).
        // A future phase will use block-based storage for small files.
        let _is_large = data.len() >= LARGE_FILE_THRESHOLD;
        self.data.write_blob(ino, data).await?;

        // Update metadata size through Raft so the state machine
        // agrees on the new file length.
        self.meta.truncate(ino, data.len() as u64).await?;

        Ok(data.len() as u32)
    }
}