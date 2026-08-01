
pub use common::types::InodeId;
enum Waloperations{
    Create,Unlink,Mkdir,Rmdir,Rename
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