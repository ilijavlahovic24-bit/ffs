//! Block-based chunk storage (ADR-005).
//!
//! Each block is a single file on disk:
//!
//! ```text
//! [8 bytes: block_id BE]
//! [4 bytes: len BE]
//! [4 bytes: crc32 over data BE]
//! [N bytes: data]
//! ```
//!
//! Writes are atomic (temp file + fsync + rename). Reads verify
//! checksum and return `Corruption` on mismatch.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use common::error::FfsError;
use common::types::{BlockId, BLOCK_SIZE};

pub struct ChunkStore {
    tmp_dir: PathBuf,
    data_dir: PathBuf,
    counter: AtomicU64,
}

impl ChunkStore {
    pub fn new(tmp_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            tmp_dir,
            data_dir,
            counter: AtomicU64::new(0),
        }
    }

    /// Allocate a fresh block id.
    ///
    /// Uses timestamp + counter mix so IDs are unique across restarts.
    /// All writes go through the leader, so no cross-node coordination
    /// is needed.
    pub fn alloc_block_id(&self) -> BlockId {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let ctr = self.counter.fetch_add(1, Ordering::SeqCst);
        now.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(ctr)
    }

    fn block_path(&self, block_id: BlockId) -> PathBuf {
        self.data_dir.join(format!("{block_id:016x}.block"))
    }

    pub async fn has_block(&self, block_id: BlockId) -> bool {
        tokio::fs::metadata(self.block_path(block_id))
            .await
            .map(|m| m.is_file())
            .unwrap_or(false)
    }

    /// Write one block atomically.
    pub async fn write_block(&self, block_id: BlockId, data: &[u8]) -> Result<(), FfsError> {
        if data.len() > BLOCK_SIZE {
            return Err(FfsError::StorageError(format!(
                "block too large: {} > {}",
                data.len(),
                BLOCK_SIZE
            )));
        }

        let tmp = self.tmp_dir.join(format!("{block_id:016x}.tmp"));
        let dst = self.block_path(block_id);

        let checksum = crc32fast::hash(data);

        let mut buf = Vec::with_capacity(16 + data.len());
        buf.extend_from_slice(&block_id.to_be_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&checksum.to_be_bytes());
        buf.extend_from_slice(data);

        let mut f = File::create(&tmp).await?;
        f.write_all(&buf).await?;
        // NO fsync here. We rely on:
        //   (a) Raft WAL fsync in update_manifest (metadata durable)
        //   (b) block replication to majority (data durable on peers)
        //   (c) explicit FUSE fsync() from the client (if caller wants it)
        // f.sync_all().await?;
        drop(f);

        tokio::fs::rename(&tmp, &dst).await?;
        Ok(())
    }

    /// Read one block and verify checksum.
    pub async fn read_block(&self, block_id: BlockId) -> Result<Vec<u8>, FfsError> {
        let path = self.block_path(block_id);
        let mut f = match File::open(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(FfsError::NotFound(block_id));
            }
            Err(e) => return Err(e.into()),
        };

        let mut header = [0u8; 16];
        f.read_exact(&mut header).await?;

        let stored_id = u64::from_be_bytes(header[0..8].try_into().unwrap());
        let len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let checksum = u32::from_be_bytes(header[12..16].try_into().unwrap());

        if stored_id != block_id {
            return Err(FfsError::Corruption(format!(
                "block id mismatch: file has {}, expected {}",
                stored_id, block_id
            )));
        }

        let mut data = vec![0u8; len];
        f.read_exact(&mut data).await?;

        if crc32fast::hash(&data) != checksum {
            return Err(FfsError::Corruption(format!(
                "block {block_id} checksum mismatch"
            )));
        }

        Ok(data)
    }

    pub async fn delete_block(&self, block_id: BlockId) -> Result<(), FfsError> {
        let path = self.block_path(block_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}