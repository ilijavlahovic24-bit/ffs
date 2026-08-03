use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use fuse3::{FileType, Result};
pub type InodeId = u64;
#[derive(Clone)]
pub struct InodeInfo {
    pub ino: InodeId,
    pub parent: u64,
    pub name: String,
    pub kind: FileType,
    pub size: u64,
    pub mode: u16,
}
pub type BlockID = u64;
#[derive(Clone)]
pub struct BlockInfo{
    pub ino:BlockID,

}