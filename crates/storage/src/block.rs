#[allow(unused)]
struct Block{
    id:u64,         //Block ID
    data:Vec<u8>,  //Block Data
    checksum:u32,   //Check sum for data coruption
    size:usize
}
#[allow(unused)]
impl Block{
    pub fn new(id: u64, data: Vec<u8>) -> Self {
        let size = data.len();
        Block { id, data, checksum: 0, size }
    }
    //for getting check sum
    pub fn crc32()->i32{
        todo!("CRC32 algorithmfor checking whether datta fos corupted");
        return 0;
    }
}