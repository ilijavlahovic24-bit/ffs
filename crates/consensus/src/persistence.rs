//! Raft WAL persistence.
//!
//! Raft requires that `current_term`, `voted_for`, and every log entry
//! are durable *before* a node responds to an RPC (Raft paper, Figure 2).
//! We use a simple append-only file with length-prefixed, CRC32-checksummed
//! bincode records — same wire format as `storage::wal::MetadataWal` so
//! the two WALs are consistent in shape.
//!
//! On restart, [`RaftNode::with_persistence`] replays all records in order
//! to reconstruct the hard state and log.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use common::error::FfsError;

use crate::log::LogEntry;

const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// One durable record in the Raft WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftRecord {
    /// Persistent state that must survive a restart.
    HardState { term: u64, voted_for: Option<u64> },

    /// An entry to be written at `entry.index`. If the log already has an
    /// entry at that index, it is overwritten (implicit truncate from
    /// `entry.index` onwards).
    AppendEntry { entry: LogEntry },
}

/// Append-only writer for the Raft WAL.
pub struct RaftPersistence {
    file: BufWriter<File>,
}

impl RaftPersistence {
    /// Open (or create) the WAL at `path` in append mode.
    pub fn open(path: &Path) -> Result<Self, FfsError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }

    /// Append a record and fsync it.
    ///
    /// Returns after the record is durable on disk. This is the durability
    /// boundary Raft relies on: only after this returns may we respond
    /// to the peer that sent us the triggering RPC.
    pub fn append(&mut self, record: &RaftRecord) -> Result<(), FfsError> {
        let payload = bincode::serde::encode_to_vec(record, BINCODE)
            .map_err(|e| FfsError::StorageError(format!("raft wal encode: {e}")))?;
        let checksum = crc32fast::hash(&payload);

        self.file.write_all(&(payload.len() as u32).to_be_bytes())?;
        self.file.write_all(&payload)?;
        self.file.write_all(&checksum.to_be_bytes())?;
        self.file.flush()?;

        // fsync the underlying file handle
        self.file.get_ref().sync_all()?;
        Ok(())
    }

    /// Read every valid record from the WAL, skipping corrupt ones.
    ///
    /// Corrupt entries at the end of the file (partial writes after a crash)
    /// are dropped silently — Raft will re-replicate anything missing.
    pub fn read_all(path: &Path) -> Result<Vec<RaftRecord>, FfsError> {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut entries = Vec::new();
        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_be_bytes(len_buf) as usize;

            let mut payload = vec![0u8; len];
            if file.read_exact(&mut payload).is_err() {
                tracing::warn!("raft wal: truncated payload, stopping");
                break;
            }

            let mut crc_buf = [0u8; 4];
            if file.read_exact(&mut crc_buf).is_err() {
                tracing::warn!("raft wal: truncated checksum, stopping");
                break;
            }
            let stored = u32::from_be_bytes(crc_buf);

            if crc32fast::hash(&payload) != stored {
                tracing::warn!("raft wal: checksum mismatch, skipping");
                continue;
            }

            match bincode::serde::decode_from_slice::<RaftRecord, _>(&payload, BINCODE) {
                Ok((record, _)) => entries.push(record),
                Err(e) => {
                    tracing::warn!("raft wal: decode error, skipping: {e}");
                }
            }
        }

        Ok(entries)
    }
}