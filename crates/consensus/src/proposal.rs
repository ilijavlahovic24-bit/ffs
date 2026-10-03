use tokio::sync::oneshot;

use common::error::FfsError;

use crate::log::LogEntry;
use crate::state::{NodeState, RaftNode};

impl RaftNode {
    /// Propose a command and get a receiver that resolves on commit.
    ///
    /// Fails fast if this node is not the leader. The caller should hold
    /// the node lock only long enough to call this — the returned
    /// receiver is awaited outside the lock.
    pub fn propose_with_waiter(
        &mut self,
        command: Vec<u8>,
    ) -> Result<oneshot::Receiver<Result<(), FfsError>>, FfsError> {
        if self.state != NodeState::Leader {
            return Err(FfsError::ConsensusError("not leader".into()));
        }

        // Build the entry with the correct index up front.
        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            term: self.current_term,
            index,
            command,
        };

        self.log.append(entry.clone());
        self.persist_append(&entry);

        // Register the waiter BEFORE doing anything that might commit.
        // In a single-node cluster, the commit happens synchronously
        // below, so the waiter must already be in the map.
        let (tx, rx) = oneshot::channel();
        self.pending_proposals.insert(index, tx);

        // Replicate to peers.
        for &peer in &self.peers.clone() {
            self.replicate_to(peer);
        }

        // Single-node: commit immediately.
        if self.peers.is_empty() {
            self.commit_index = index;
            self.apply_committed();
        }

        Ok(rx)
    }

    /// Resolve a pending proposal at `index` with `result`.
    pub(crate) fn resolve_proposal(&mut self, index: u64, result: Result<(), FfsError>) {
        if let Some(tx) = self.pending_proposals.remove(&index) {
            let _ = tx.send(result);
        }
    }

    /// Fail every pending proposal — used when we lose leadership.
    pub(crate) fn fail_all_proposals(&mut self, reason: &str) {
        for (_, tx) in self.pending_proposals.drain() {
            let _ = tx.send(Err(FfsError::ConsensusError(reason.to_string())));
        }
    }
}