pub use common::types::InodeId;

enum Waloperations{
    Create { inode_id: InodeId, parent_id: InodeId, name: String },
    Unlink { parent_id: InodeId, name: String },
    Mkdir  { inode_id: InodeId, parent_id: InodeId, name: String },
    Rmdir  { parent_id: InodeId, name: String },
    Rename { old_parent: InodeId, old_name: String, new_parent: InodeId, new_name: String }
}
#[allow(unused)]
struct DataWAL{
    waloperations: Waloperations,
    inode_id:InodeId,
    filename:String,
    
}
#[allow(unused)]
impl DataWAL {
    pub fn append(){}
}
#[allow(unused)]
struct MetaDataWal{

}
#[allow(unused)]
impl MetaDataWal {
    pub fn append(){}
}