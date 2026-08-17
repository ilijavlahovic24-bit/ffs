use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::fs::File;
pub use common::types::InodeId;
use common::error::FfsError;
enum Waloperation{
    Create { inode_id: InodeId, parent_id: InodeId, name: String },
    Unlink { parent_id: InodeId, name: String },
    Mkdir  { inode_id: InodeId, parent_id: InodeId, name: String },
    Rmdir  { parent_id: InodeId, name: String },
    Rename { old_parent: InodeId, old_name: String, new_parent: InodeId, new_name: String }
}

struct DataPath{
    tmp_rid:PathBuf,
    data_path:PathBuf,
    wal:Arc<MetaDataWal>
}
#[allow(unused)]
struct DataWAL{
    waloperations: Waloperation,
    inode_id:InodeId,
    filename:String,
    
}
struct WalEntry{
    sequence: u64,
    operation: Waloperation,
    checksum: u32,
}

#[allow(unused)]
struct MetaDataWal{
    path:PathBuf,
    file:File,
    sequence:AtomicU64
}
#[allow(unused)]
impl MetaDataWal {
    pub fn new(path: PathBuf) -> Result<Self, FfsError>{
        todo!()
    }
    pub fn append(&mut self, op: Waloperation) -> Result<(), FfsError>{
        todo!()
    }
    pub fn read_all(path: &Path) -> Result<Vec<WalEntry>, FfsError>{
        todo!()
    }
}