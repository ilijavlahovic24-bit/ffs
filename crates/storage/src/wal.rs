use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Deserialize, Serialize};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
pub use common::types::InodeId;
use common::error::FfsError;
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalOperation {
    Create { inode_id: InodeId, parent_id: InodeId, name: String },
    Unlink { parent_id: InodeId, name: String },
    Mkdir  { inode_id: InodeId, parent_id: InodeId, name: String },
    Rmdir  { parent_id: InodeId, name: String },
    Rename { old_parent: InodeId, old_name: String, new_parent: InodeId, new_name: String },
    WriteBlob { inode_id: InodeId },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntryPayload {
    sequence: u64,
    operation: WalOperation,
}


pub struct WalEntry{
    sequence: u64,
    operation: WalOperation,
    checksum: u32,
}
const BINCODE: bincode::config::Configuration = bincode::config::standard();

fn encode_payload(sequence: u64, op: &WalOperation) -> Result<(Vec<u8>, u32), FfsError> {
    let payload = bincode::serde::encode_to_vec(
        &WalEntryPayload { sequence, operation: op.clone() },
        BINCODE,
    )
        .map_err(|e| FfsError::StorageError(format!("bincode encode: {e}")))?;
    let checksum = crc32fast::hash(&payload);
    Ok((payload, checksum))
}

#[allow(unused)]
struct MetadataWall {
    path:PathBuf,
    file:tokio::sync::Mutex<File>,
    sequence:AtomicU64
}
#[allow(unused)]
impl MetadataWall {
    pub async fn new(path: PathBuf) -> Result<Self, FfsError>{
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self {
            path,
            file: tokio::sync::Mutex::new(file),
            sequence: AtomicU64::new(0),
        })
    }
    pub async fn append(&self, op: WalOperation) -> Result<(), FfsError> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let (payload, checksum) = encode_payload(seq, &op)?;

        let mut file = self.file.lock().await;
        file.write_all(&(payload.len() as u32).to_be_bytes()).await?;
        file.write_all(&payload).await?;
        file.write_all(&checksum.to_be_bytes()).await?;
        file.sync_all().await?;
        Ok(())
    }
    pub async fn read_all(path: &Path) -> Result<Vec<WalEntry>, FfsError>{
        let mut file = match File::open(path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut entries = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            if file.read_exact(&mut payload).await.is_err() {
                tracing::warn!("WAL: truncated payload, stopping");
                break;
            }

            let mut crc_buf = [0u8; 4];
            if file.read_exact(&mut crc_buf).await.is_err() {
                tracing::warn!("WAL: truncated checksum, stopping");
                break;
            }
            let stored_crc = u32::from_be_bytes(crc_buf);

            if crc32fast::hash(&payload) != stored_crc {
                tracing::warn!("WAL: checksum mismatch, skipping entry");
                continue;
            }

            let (p, _): (WalEntryPayload, usize) =
                bincode::serde::decode_from_slice(&payload, BINCODE)
                    .map_err(|e| FfsError::Corruption(format!("bincode decode: {e}")))?;

            entries.push(WalEntry {
                sequence: p.sequence,
                operation: p.operation,
                checksum: stored_crc,
            });
        }

        Ok(entries)
    }
}



struct DataPath{
    tmp_dir:PathBuf,
    data_path:PathBuf,
    wal:Arc<MetadataWall>
}
impl DataPath{
    pub async fn new(tmp_dir: PathBuf, data_path: PathBuf, wal: Arc<MetadataWall>) -> Self {
        Self { tmp_dir, data_path, wal }
    }
    pub async fn write_blob(&self, inode_id: InodeId, data: &[u8]) -> Result<(), FfsError>{
        let tmp=self.tmp_dir.join(format!("{inode_id}.tmp"));
        let dst = self.data_path.join(inode_id.to_string());
        {
            let mut f = File::create(&tmp).await?;
            f.write_all(data).await?;
            f.sync_all().await?;
        }
        // 2. atomic rename
        tokio::fs::rename(&tmp, &dst).await?;
        // 3. WAL
        self.wal.append(WalOperation::WriteBlob { inode_id }).await?;
        Ok(())

    }
    pub async fn read_blob(&self, inode_id: InodeId) -> Result<Vec<u8>, FfsError>{
        let dst = self.data_path.join(inode_id.to_string());
        match tokio::fs::read(&dst).await {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(FfsError::NotFound(inode_id))
            }
            Err(e) => Err(e.into()),
        }
    }
}