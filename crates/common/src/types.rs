// common/src/types.rs
use serde::{Deserialize, Serialize};

pub type InodeId = u64;
pub type BlockId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
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