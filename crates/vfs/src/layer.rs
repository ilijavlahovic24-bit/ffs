use std::sync::Arc;

use common::error::FfsError;
use common::types::{FileType, InodeId, InodeInfo};
use meta::{InodeManager, MetaStore};
use storage::allocator::Allocator;
use storage::wal::DataPath;

/// Interposition layer between FUSE handlers and the storage/meta stack.
///
/// Reads go directly to the shared `InodeManager` state machine.
/// Writes are proposed through Raft and awaited via `MetaStore`.
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
        Self {
            inodes,
            meta,
            data,
            allocator,
        }
    }

    // --- Reads (no Raft) ---

    pub fn lookup(&self, parent: InodeId, name: &str) -> Result<InodeInfo, FfsError> {
        let ino = self
            .inodes
            .lookup(parent, name)
            .ok_or(FfsError::NotFound(parent))?;
        self.inodes
            .get_inode(ino)
            .ok_or(FfsError::NotFound(ino))
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

    // --- Writes (through Raft) ---

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
        self.meta.rmdir(parent, name).await
    }

    pub async fn write(
        &self,
        ino: InodeId,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, FfsError> {
        if self.getattr(ino).is_err() {
            return Err(FfsError::NotFound(ino));
        }
        // TODO: offset != 0 read-modify-write
        if offset != 0 {
            return Err(FfsError::StorageError("non-zero offset not supported".into()));
        }

        self.data.write_blob(ino, data).await?;
        Ok(data.len() as u32)
    }
}