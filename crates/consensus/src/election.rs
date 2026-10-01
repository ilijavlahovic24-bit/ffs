use std::collections::HashSet;
use std::time::Instant;

use crate::rpc::{RequestVote, RequestVoteReply};
use crate::state::{NodeState, OutgoingMessage, RaftNode};
use crate::timer::reset_election_deadline;

impl RaftNode {
    /// Transition to Candidate, increment term, vote for self, and
    /// broadcast RequestVote to all peers.
    ///
    /// Durability: the new term and self-vote must hit disk before
    /// we ask anyone to vote for us.
    pub fn start_election(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received = HashSet::new();
        self.votes_received.insert(self.id);
        self.election_deadline = reset_election_deadline();

        self.persist_hard_state();

        tracing::info!(
            node = self.id,
            term = self.current_term,
            "starting election"
        );

        let req = RequestVote {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        for &peer in &self.peers.clone() {
            self.push_out(OutgoingMessage::RequestVote {
                to: peer,
                req: req.clone(),
            });
        }
    }

    /// Handle an incoming RequestVote RPC.
    ///
    /// Grants the vote if:
    /// - The candidate's term is at least our current term.
    /// - We have not voted for anyone else in that term.
    /// - The candidate's log is at least as up-to-date as ours
    ///   (Raft §5.4.1).
    ///
    /// Durability: any change to `voted_for` must be on disk before
    /// we reply `granted = true`.
    pub fn handle_request_vote(&mut self, req: RequestVote) -> RequestVoteReply {
        let mut dirty = false;

        // Candidate is ahead — adopt their term and clear our vote.
        // step_down also fails pending proposals and persists hard state.
        if req.term > self.current_term {
            self.step_down(req.term);
            dirty = true;
        }

        let mut granted = false;

        if req.term < self.current_term {
            // Stale candidate.
            granted = false;
        } else {
            // We may vote if we haven't voted yet in this term, or if
            // we already voted for this same candidate (idempotent).
            let can_vote =
                self.voted_for.is_none() || self.voted_for == Some(req.candidate_id);

            // Log comparison: term first, then index.
            let our_last_term = self.log.last_term();
            let our_last_index = self.log.last_index();
            let log_ok = req.last_log_term > our_last_term
                || (req.last_log_term == our_last_term
                && req.last_log_index >= our_last_index);

            if can_vote && log_ok {
                self.voted_for = Some(req.candidate_id);
                self.election_deadline = reset_election_deadline();
                granted = true;
                dirty = true;
            }
        }

        if dirty {
            // Durability: before telling the candidate "yes", the vote
            // must be on disk. Otherwise a crash could let us vote again
            // in the same term for a different candidate.
            self.persist_hard_state();
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted: granted,
        }
    }

    /// Handle a reply to a RequestVote we sent as a candidate.
    ///
    /// On a majority of votes, become leader.
    pub fn handle_vote_reply(&mut self, from: u64, reply: RequestVoteReply) {
        // Reply from a newer term — we are stale, step down.
        if reply.term > self.current_term {
            self.step_down(reply.term);
            return;
        }

        // Ignore replies that don't belong to our current candidacy.
        if self.state != NodeState::Candidate || reply.term != self.current_term {
            return;
        }

        if reply.vote_granted {
            self.votes_received.insert(from);

            if self.votes_received.len() >= self.majority() {
                self.become_leader();
            }
        }
    }

    /// Transition to Leader. Initialize `next_index` and `match_index`
    /// for every peer, then immediately send heartbeats.
    ///
    /// Also proposes a no-op entry (Raft §8) — this commits any entries
    /// left over from previous terms and confirms our leadership.
    pub fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        let last = self.log.last_index();

        self.next_index.clear();
        self.match_index.clear();

        for &peer in &self.peers {
            self.next_index.insert(peer, last + 1);
            self.match_index.insert(peer, 0);
        }

        self.last_heartbeat = Instant::now();

        tracing::info!(
            node = self.id,
            term = self.current_term,
            "became leader"
        );

        // Initial round of heartbeats.
        self.send_heartbeat();

        // No-op entry: lets previously committed entries apply and
        // confirms our leadership across the cluster.
        self.propose(Vec::new());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogEntry;
    use crate::rpc::RequestVote;

    /// Helper: fresh follower.
    fn node(id: u64, peers: Vec<u64>) -> RaftNode {
        RaftNode::new(id, peers)
    }

    #[test]
    fn vote_granted_when_log_up_to_date() {
        let mut n = node(1, vec![2, 3]);
        let req = RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let reply = n.handle_request_vote(req);
        assert!(reply.vote_granted);
        assert_eq!(n.voted_for, Some(2));
        assert_eq!(n.current_term, 1);
    }

    #[test]
    fn vote_denied_for_stale_term() {
        let mut n = node(1, vec![2, 3]);
        n.current_term = 5;
        let req = RequestVote {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let reply = n.handle_request_vote(req);
        assert!(!reply.vote_granted);
        assert_eq!(n.current_term, 5);
    }

    #[test]
    fn vote_denied_when_already_voted() {
        let mut n = node(1, vec![2, 3]);
        let req1 = RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let req2 = RequestVote {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(n.handle_request_vote(req1).vote_granted);
        // Second candidate in the same term — reject.
        assert!(!n.handle_request_vote(req2).vote_granted);
    }

    #[test]
    fn vote_denied_when_log_behind() {
        let mut n = node(1, vec![2, 3]);
        // Our log has an entry with term 5.
        n.log.append(LogEntry {
            term: 5,
            index: 0,
            command: vec![],
        });
        // Candidate has an empty log in term 6.
        let req = RequestVote {
            term: 6,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        let reply = n.handle_request_vote(req);
        assert!(!reply.vote_granted);
    }

    #[test]
    fn become_leader_on_majority() {
        let mut n = node(1, vec![2, 3]);
        n.start_election();
        assert_eq!(n.state, NodeState::Candidate);

        // One more vote gives us majority (2 of 3).
        n.handle_vote_reply(2, RequestVoteReply { term: 1, vote_granted: true });
        assert_eq!(n.state, NodeState::Leader);

        // next_index for each peer is last_index + 1.
        // After become_leader, a no-op was proposed, so last_index = 1
        // and next_index = 2. But we assert on the values set at
        // initialization time by checking the maps are consistent.
        let last = n.log.last_index();
        assert_eq!(n.next_index[&2], last + 1);
        assert_eq!(n.next_index[&3], last + 1);
        assert_eq!(n.match_index[&2], 0);
    }

    #[test]
    fn candidate_steps_down_on_higher_term() {
        let mut n = node(1, vec![2, 3]);
        n.start_election();
        assert_eq!(n.current_term, 1);

        n.handle_vote_reply(2, RequestVoteReply { term: 5, vote_granted: false });
        assert_eq!(n.state, NodeState::Follower);
        assert_eq!(n.current_term, 5);
    }

    #[test]
    fn become_leader_proposes_noop() {
        let mut n = node(1, vec![2, 3]);
        n.start_election();
        n.handle_vote_reply(2, RequestVoteReply { term: 1, vote_granted: true });
        assert_eq!(n.state, NodeState::Leader);

        // No-op entry should be present at index 1.
        assert_eq!(n.log.last_index(), 1);
        let entry = n.log.get(1).unwrap();
        assert!(entry.command.is_empty());
        assert_eq!(entry.term, 1);
    }

    #[test]
    fn vote_for_same_candidate_is_idempotent() {
        let mut n = node(1, vec![2, 3]);
        let req = RequestVote {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };
        assert!(n.handle_request_vote(req.clone()).vote_granted);
        // Same candidate, same term — should still grant.
        assert!(n.handle_request_vote(req).vote_granted);
    }
}