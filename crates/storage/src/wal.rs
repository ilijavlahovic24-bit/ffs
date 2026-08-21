use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::fs::File;
pub use common::types::InodeId;
use common::error::FfsError;
pub enum WalOperation {
    Create { inode_id: InodeId, parent_id: InodeId, name: String },
    Unlink { parent_id: InodeId, name: String },
    Mkdir  { inode_id: InodeId, parent_id: InodeId, name: String },
    Rmdir  { parent_id: InodeId, name: String },
    Rename { old_parent: InodeId, old_name: String, new_parent: InodeId, new_name: String }
}

struct DataPath{
    tmp_rid:PathBuf,
    data_path:PathBuf,
    wal:Arc<MetadataWall>
}
impl DataPath{
    fn write_blob(inode_id: InodeId, data: &[u8]) -> Result<(), FfsError>{
        todo!()
    }
    fn read_blob(inode_id: InodeId) -> Result<Vec<u8>, FfsError>{
        todo!()
    }
}

struct WalEntry{
    sequence: u64,
    operation: WalOperation,
    checksum: u32,
}

#[allow(unused)]
struct MetadataWall {
    path:PathBuf,
    file:File,
    sequence:AtomicU64
}
#[allow(unused)]
impl MetadataWall {
    pub fn new(path: PathBuf) -> Result<Self, FfsError>{
        todo!()
    }
    pub fn append(&mut self, op: WalOperation) -> Result<(), FfsError>{
        todo!()
    }
    pub fn read_all(path: &Path) -> Result<Vec<WalEntry>, FfsError>{
        todo!()
    }
}