use tokio::sync::oneshot;

use common::error::FfsError;

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

        let (tx, rx) = oneshot::channel();
        let index = self
            .propose(command)
            .ok_or_else(|| FfsError::ConsensusError("propose failed".into()))?;

        self.pending_proposals.insert(index, tx);
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