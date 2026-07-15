use crc32fast::Hasher;

#[allow(unused)]
struct Block{
    pub id:u64,         //Block ID
    pub data:Vec<u8>,  //Block Data
    pub checksum:u32,   //Check sum for data coruption
    pub size:usize
}
#[allow(unused)]
impl Block{
    pub fn new(id: u64, data: Vec<u8>) -> Self {
        let size = data.len();
        Block { id, data, checksum: 0, size }
    }
    //for getting check sum
    pub fn crc32(data:&[u8])->u32{
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()

    }
    pub fn verify(&self) -> bool {
        Self::crc32(&self.data) == self.checksum
    }
}