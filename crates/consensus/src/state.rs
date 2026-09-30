use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::log::RaftLog;
use crate::rpc::{AppendEntries, AppendEntriesReply, RequestVote, RequestVoteReply};

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
            // Randomize the first deadline so simultaneous starts don't
            // all trigger elections at once.
            election_deadline: crate::timer::reset_election_deadline(),
            last_heartbeat: now,
            outbox: Vec::new(),
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
            true
        } else {
            false
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