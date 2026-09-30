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

use std::path::PathBuf;

use crate::{AppendEntries, NodeState, RaftNode};

fn tmp_wal(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "ferumfs-raft-{}-{}.wal",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

#[test]
fn hard_state_survives_restart() {
    let path = tmp_wal("hard_state");

    {
        let mut n = RaftNode::with_persistence(1, vec![2, 3], &path).unwrap();
        n.start_election();
        assert_eq!(n.current_term, 1);
        assert_eq!(n.voted_for, Some(1));
    }

    let n2 = RaftNode::with_persistence(1, vec![2, 3], &path).unwrap();
    assert_eq!(n2.current_term, 1);
    assert_eq!(n2.voted_for, Some(1));
    assert_eq!(n2.state, NodeState::Follower);

    std::fs::remove_file(&path).ok();
}

#[test]
fn log_entries_survive_restart() {
    let path = tmp_wal("log_entries");

    {
        let mut n = RaftNode::with_persistence(1, vec![], &path).unwrap();
        n.state = NodeState::Leader;
        n.current_term = 1;
        n.propose(b"first".to_vec()).unwrap();
        n.propose(b"second".to_vec()).unwrap();
    }

    let n2 = RaftNode::with_persistence(1, vec![], &path).unwrap();
    assert_eq!(n2.log.last_index(), 2);
    assert_eq!(n2.log.get(1).unwrap().command, b"first");
    assert_eq!(n2.log.get(2).unwrap().command, b"second");
    assert_eq!(n2.log.get(1).unwrap().term, 1);

    std::fs::remove_file(&path).ok();
}

#[test]
fn conflicting_append_overwrites_after_restart() {
    let path = tmp_wal("conflict");

    // First run: log = [1(term1), 2(term1), 3(term1)]
    {
        let mut n = RaftNode::with_persistence(2, vec![1, 3], &path).unwrap();

        // Simulate two AppendEntries from a leader in term 1.


        let req = AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry { term: 1, index: 1, command: vec![1] },
                LogEntry { term: 1, index: 2, command: vec![2] },
                LogEntry { term: 1, index: 3, command: vec![3] },
            ],
            leader_commit: 0,
        };
        let reply = n.handle_append_entries(req);
        assert!(reply.success);
        assert_eq!(n.log.last_index(), 3);
    }

    // Second run: conflicting append at index 2 with term 2
    {
        let mut n = RaftNode::with_persistence(2, vec![1, 3], &path).unwrap();
        assert_eq!(n.log.last_index(), 3);
        assert_eq!(n.log.term_at(2), 1);

        let req = AppendEntries {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry { term: 2, index: 2, command: vec![20] },
                LogEntry { term: 2, index: 3, command: vec![30] },
            ],
            leader_commit: 0,
        };
        let reply = n.handle_append_entries(req);
        assert!(reply.success);
        assert_eq!(n.log.last_index(), 3);
        assert_eq!(n.log.term_at(2), 2);
        assert_eq!(n.log.term_at(3), 2);
        assert_eq!(n.log.get(2).unwrap().command, vec![20]);
        assert_eq!(n.log.get(3).unwrap().command, vec![30]);
    }

    // Third run: verify the conflicting state survived
    {
        let n = RaftNode::with_persistence(2, vec![1, 3], &path).unwrap();
        assert_eq!(n.log.last_index(), 3);
        assert_eq!(n.log.term_at(2), 2);
        assert_eq!(n.log.get(2).unwrap().command, vec![20]);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn empty_wal_starts_clean() {
    let path = tmp_wal("empty");
    let n = RaftNode::with_persistence(1, vec![2, 3], &path).unwrap();
    assert_eq!(n.current_term, 0);
    assert_eq!(n.voted_for, None);
    assert_eq!(n.log.last_index(), 0);
    std::fs::remove_file(&path).ok();
}