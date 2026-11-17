use serde::{Deserialize, Serialize};

pub type InodeId = u64;
pub type BlockId = u64;

/// Fixed block size for chunked storage
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeInfo {
    pub ino: InodeId,
    pub parent: InodeId,
    pub name: String,
    pub kind: FileType,
    pub size: u64,
    pub mode: u16,
}

/// One block within a file's manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub struct BlockRef {
    /// Unique block identifier. Content is stored under this ID.
    pub block_id: BlockId,
    /// Byte offset of this block within the file.
    pub offset: u64,
    /// Actual bytes stored in this block (last block may be < BLOCK_SIZE).
    pub len: u32,
}

impl BlockRef {
    pub fn end(&self) -> u64 {
        self.offset + self.len as u64
    }
}