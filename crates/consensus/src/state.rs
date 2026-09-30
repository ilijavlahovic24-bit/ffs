use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;
use common::error::FfsError;
use crate::log::RaftLog;
use crate::LogEntry;
use crate::rpc::{AppendEntries, AppendEntriesReply, RequestVote, RequestVoteReply};
use crate::persistence::{RaftPersistence, RaftRecord};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub enum OutgoingMessage {
    RequestVote { to: u64, req: RequestVote },
    AppendEntries { to: u64, req: AppendEntries },
}

pub struct RaftNode {
    pub id: u64,
    pub peers: Vec<u64>,

    pub state: NodeState,
    pub current_term: u64,
    pub voted_for: Option<u64>,

    pub log: RaftLog,
    pub commit_index: u64,
    pub last_applied: u64,

    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,

    pub votes_received: HashSet<u64>,

    pub election_deadline: Instant,
    pub last_heartbeat: Instant,

    outbox: Vec<OutgoingMessage>,
    /// WAL for durable state. `None` in tests and in single-shot nodes.
    persistence: Option<RaftPersistence>,
}

impl RaftNode {
    pub fn new(id: u64, peers: Vec<u64>) -> Self {
        let now = Instant::now();
        Self {
            id,
            peers,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            log: RaftLog::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes_received: HashSet::new(),
            election_deadline: crate::timer::reset_election_deadline(),
            last_heartbeat: now,
            outbox: Vec::new(),
            persistence: None,
        }
    }

    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    pub fn majority(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    pub fn take_outbox(&mut self) -> Vec<OutgoingMessage> {
        std::mem::take(&mut self.outbox)
    }

    pub(crate) fn push_out(&mut self, msg: OutgoingMessage) {
        self.outbox.push(msg);
    }
    // If the new term is higher, go to Follower and reset the vote.
    pub(crate) fn observe_term(&mut self, term: u64) -> bool {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
            self.state = NodeState::Follower;
            self.persist_hard_state();
            true
        } else {
            false
        }
    }
    pub fn with_persistence(
        id: u64,
        peers: Vec<u64>,
        wal_path: &Path,
    ) -> Result<Self, FfsError> {
        let mut node = Self::new(id, peers);

        let records = RaftPersistence::read_all(wal_path)?;
        if !records.is_empty() {
            tracing::info!(
                node = id,
                count = records.len(),
                "raft wal: replaying records"
            );
        }
        node.replay(records);

        node.persistence = Some(RaftPersistence::open(wal_path)?);
        Ok(node)
    }
    fn replay(&mut self, records: Vec<RaftRecord>) {
        for record in records {
            match record {
                RaftRecord::HardState { term, voted_for } => {
                    // Only adopt forward-moving terms. A stale record should
                    // never bring us back to an earlier term.
                    if term >= self.current_term {
                        self.current_term = term;
                        self.voted_for = voted_for;
                    }
                }
                RaftRecord::AppendEntry { entry } => {
                    self.apply_append_record(entry);
                }
            }
        }
    }
    /// Apply one `AppendEntry` record during replay.
    ///
    /// Matches the in-memory semantics of `handle_append_entries`:
    /// overwrite at `entry.index` if the term differs, no-op if it's an
    /// exact duplicate.
    fn apply_append_record(&mut self, entry: LogEntry) {
        let last = self.log.last_index();

        if entry.index <= last {
            if self.log.term_at(entry.index) == entry.term {
                // Exact duplicate — idempotent.
                return;
            }
            // Conflicting term — truncate from this index.
            self.log.truncate_from(entry.index);
        }

        if entry.index == self.log.last_index() + 1 {
            self.log.append(entry);
        } else {
            tracing::error!(
                node = self.id,
                index = entry.index,
                last_index = self.log.last_index(),
                "raft wal replay: gap in log — record skipped"
            );
        }
    }

    /// Persist `current_term` and `voted_for`. No-op if no WAL is attached.
    ///
    /// Errors are logged but not propagated — a broken WAL is a fatal
    /// condition that will surface via a different path.
    pub(crate) fn persist_hard_state(&mut self) {
        let Some(p) = self.persistence.as_mut() else {
            return;
        };
        let record = RaftRecord::HardState {
            term: self.current_term,
            voted_for: self.voted_for,
        };
        if let Err(e) = p.append(&record) {
            tracing::error!(node = self.id, "persist hard state failed: {e}");
        }
    }

    /// Persist one appended log entry. No-op if no WAL is attached.
    pub(crate) fn persist_append(&mut self, entry: &LogEntry) {
        let Some(p) = self.persistence.as_mut() else {
            return;
        };
        let record = RaftRecord::AppendEntry {
            entry: entry.clone(),
        };
        if let Err(e) = p.append(&record) {
            tracing::error!(node = self.id, "persist append failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn majority_for_3_nodes() {
        let n = RaftNode::new(1, vec![2, 3]);
        assert_eq!(n.cluster_size(), 3);
        assert_eq!(n.majority(), 2);
    }

    #[test]
    fn majority_for_5_nodes() {
        let n = RaftNode::new(1, vec![2, 3, 4, 5]);
        assert_eq!(n.majority(), 3);
    }

    #[test]
    fn observe_term_resets_vote() {
        let mut n = RaftNode::new(1, vec![2, 3]);
        n.current_term = 5;
        n.voted_for = Some(2);
        n.state = NodeState::Leader;

        assert!(n.observe_term(7));
        assert_eq!(n.current_term, 7);
        assert_eq!(n.voted_for, None);
        assert_eq!(n.state, NodeState::Follower);

        assert!(!n.observe_term(7));
        assert!(!n.observe_term(3));
        assert_eq!(n.current_term, 7);
    }
}