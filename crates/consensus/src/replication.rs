use crate::log::LogEntry;
use crate::rpc::{AppendEntries, AppendEntriesReply};
use crate::state::{NodeState, OutgoingMessage, RaftNode};
use crate::timer::reset_election_deadline_for;

impl RaftNode {
    /// Handle an incoming AppendEntries RPC (as a follower or candidate).
    ///
    /// Follows the Raft paper §5.3:
    /// 1. Reply false if `term < current_term`.
    /// 2. Reply false if log doesn't contain an entry at `prev_log_index`
    ///    matching `prev_log_term`.
    /// 3. If an existing entry conflicts with a new one (same index,
    ///    different term), delete the existing entry and all that follow it.
    /// 4. Append any new entries not already in the log.
    /// 5. If `leader_commit > commit_index`, set `commit_index` to
    ///    `min(leader_commit, index of last new entry)`.
    pub fn handle_append_entries(&mut self, req: AppendEntries) -> AppendEntriesReply {
        // Higher term: adopt, step down (which persists hard state).
        if req.term > self.current_term {
            self.step_down(req.term);
        }

        // Stale leader — reject.
        if req.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: None,
            };
        }

        // Legitimate leader. Reset election timer with per-node jitter
        // so that simultaneous AppendEntries don't cause all followers
        // to start elections in the same tick when the leader dies.
        self.state = NodeState::Follower;
        self.current_leader = Some(req.leader_id);
        self.election_deadline = reset_election_deadline_for(self.id);

        // Consistency check: log must contain `prev_log_index` with
        // term `prev_log_term`.
        if req.prev_log_index > 0 {
            let Some(our_entry) = self.log.get(req.prev_log_index) else {
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    conflict_index: Some(self.log.last_index() + 1),
                };
            };

            if our_entry.term != req.prev_log_term {
                let mut conflict = req.prev_log_index;
                while conflict > 1 && self.log.term_at(conflict - 1) == our_entry.term {
                    conflict -= 1;
                }
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    conflict_index: Some(conflict),
                };
            }
        }

        // Append entries, resolving conflicts.
        for entry in &req.entries {
            let idx = entry.index;
            match self.log.get(idx) {
                Some(existing) if existing.term == entry.term => continue,
                Some(_) => self.log.truncate_from(idx),
                None => {}
            }
            self.log.append(entry.clone());
            self.persist_append(entry);
        }

        // Advance commit index.
        if req.leader_commit > self.commit_index {
            self.commit_index = req.leader_commit.min(self.log.last_index());
            self.apply_committed();
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            conflict_index: None,
        }
    }

    /// Handle a reply to an AppendEntries we (as leader) sent.
    pub fn handle_append_entries_reply(&mut self, from: u64, reply: AppendEntriesReply) {
        if reply.term > self.current_term {
            self.step_down(reply.term);
            return;
        }

        if self.state != NodeState::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            let matched = next.saturating_sub(1);
            self.match_index.insert(from, matched);
            self.advance_commit_index();
        } else {
            let prev = self.next_index.get(&from).copied().unwrap_or(1);

            let new_next = match reply.conflict_index {
                Some(conflict) if conflict < prev => conflict,
                _ => prev.saturating_sub(1).max(1),
            };
            self.next_index.insert(from, new_next);
            self.replicate_to(from);
        }
    }

    /// Send an empty AppendEntries (heartbeat) to all peers.
    pub fn send_heartbeat(&mut self) {
        if self.state != NodeState::Leader {
            return;
        }

        for &peer in &self.peers.clone() {
            self.replicate_to(peer);
        }
    }

    /// Send everything the peer is missing, starting at `next_index[peer]`.
    pub fn replicate_to(&mut self, peer: u64) {
        if self.state != NodeState::Leader {
            return;
        }

        let next = *self
            .next_index
            .get(&peer)
            .unwrap_or(&(self.log.last_index() + 1));
        let prev_index = next.saturating_sub(1);
        let prev_term = self.log.term_at(prev_index);

        let entries: Vec<LogEntry> = self.log.slice_from(next).to_vec();

        // Optimistically advance next_index. On success, match_index
        // becomes next_index - 1. On failure, the reply handler pulls
        // it back to conflict_index.
        let sent_upto = self.log.last_index();
        self.next_index.insert(peer, sent_upto + 1);

        let req = AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        };

        self.push_out(OutgoingMessage::AppendEntries { to: peer, req });
    }

    /// Client proposes a command. Only the leader may accept it.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<u64> {
        if self.state != NodeState::Leader {
            return None;
        }

        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            term: self.current_term,
            index,
            command,
        };

        self.log.append(entry.clone());
        self.persist_append(&entry);

        for &peer in &self.peers.clone() {
            self.replicate_to(peer);
        }

        if self.peers.is_empty() {
            self.commit_index = index;
            self.apply_committed();
        }

        Some(index)
    }

    /// Advance `commit_index` if a majority has replicated a newer entry.
    ///
    /// Only current-term entries may be committed directly (Raft §5.4.2).
    fn advance_commit_index(&mut self) {
        let old_commit = self.commit_index;

        for idx in (self.commit_index + 1)..=self.log.last_index() {
            if self.log.term_at(idx) != self.current_term {
                continue;
            }

            let mut replicated = 1;
            for &peer in &self.peers {
                if self.match_index.get(&peer).copied().unwrap_or(0) >= idx {
                    replicated += 1;
                }
            }

            if replicated >= self.majority() {
                self.commit_index = idx;
            }
        }

        if self.commit_index > old_commit {
            // Propagate immediately so followers learn about the new
            // commit_index without waiting for the next heartbeat.
            self.send_heartbeat();
        }

        self.apply_committed();
    }

    /// Apply all committed entries from `last_applied + 1` to `commit_index`.
    pub(crate) fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let idx = self.last_applied;

            let Some(entry) = self.log.get(idx) else {
                tracing::warn!(node = self.id, index = idx, "missing log entry to apply");
                continue;
            };
            let command = entry.command.clone();
            let term = entry.term;

            let result = match &self.state_machine {
                Some(sm) => sm.apply(&command),
                None => Ok(()),
            };

            if let Err(e) = &result {
                tracing::error!(
                    node = self.id,
                    index = idx,
                    term,
                    "state machine apply failed: {e}"
                );
            } else {
                tracing::debug!(
                    node = self.id,
                    index = idx,
                    term,
                    "applied committed entry"
                );
            }

            self.resolve_proposal(idx, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::AppendEntries;

    fn leader(id: u64, peers: Vec<u64>) -> RaftNode {
        let mut n = RaftNode::new(id, peers);
        n.state = NodeState::Leader;
        n.current_term = 1;
        for &p in &n.peers {
            n.next_index.insert(p, 1);
            n.match_index.insert(p, 0);
        }
        n
    }

    #[test]
    fn follower_accepts_append() {
        let mut f = RaftNode::new(2, vec![1, 3]);

        let req = AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                term: 1,
                index: 1,
                command: vec![42],
            }],
            leader_commit: 0,
        };

        let reply = f.handle_append_entries(req);
        assert!(reply.success);
        assert_eq!(f.log.last_index(), 1);
        assert_eq!(f.log.get(1).unwrap().command, vec![42]);
    }

    #[test]
    fn follower_rejects_stale_term() {
        let mut f = RaftNode::new(2, vec![1, 3]);
        f.current_term = 5;

        let req = AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let reply = f.handle_append_entries(req);
        assert!(!reply.success);
        assert_eq!(reply.term, 5);
    }

    #[test]
    fn follower_truncates_conflicting_entries() {
        let mut f = RaftNode::new(2, vec![1, 3]);

        f.log.append(LogEntry { term: 1, index: 0, command: vec![1] });
        f.log.append(LogEntry { term: 1, index: 0, command: vec![2] });
        assert_eq!(f.log.last_index(), 2);

        let req = AppendEntries {
            term: 3,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![LogEntry {
                term: 3,
                index: 2,
                command: vec![99],
            }],
            leader_commit: 0,
        };

        let reply = f.handle_append_entries(req);
        assert!(reply.success);
        assert_eq!(f.log.last_index(), 2);
        assert_eq!(f.log.get(2).unwrap().command, vec![99]);
        assert_eq!(f.log.get(2).unwrap().term, 3);
    }

    #[test]
    fn follower_rejects_when_log_behind() {
        let mut f = RaftNode::new(2, vec![1, 3]);

        let req = AppendEntries {
            term: 1,
            leader_id: 1,
            prev_log_index: 5,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };

        let reply = f.handle_append_entries(req);
        assert!(!reply.success);
        assert!(reply.conflict_index.is_some());
    }

    #[test]
    fn single_node_leader_commits_immediately() {
        let mut n = leader(1, vec![]);
        assert_eq!(n.majority(), 1);

        let idx = n.propose(vec![1, 2, 3]).unwrap();
        assert_eq!(idx, 1);
        assert_eq!(n.commit_index, 1);
        assert_eq!(n.last_applied, 1);
    }

    #[test]
    fn leader_advances_commit_on_majority() {
        let mut l = leader(1, vec![2, 3]);
        l.propose(vec![7]).unwrap();
        assert_eq!(l.commit_index, 0);

        l.handle_append_entries_reply(2, AppendEntriesReply {
            term: 1,
            success: true,
            conflict_index: None,
        });
        assert_eq!(l.commit_index, 1);
    }

    #[test]
    fn leader_does_not_commit_without_majority() {
        let mut l = leader(1, vec![2, 3]);
        l.propose(vec![7]).unwrap();
        assert_eq!(l.commit_index, 0);
    }

    #[test]
    fn leader_steps_down_on_higher_term_in_reply() {
        let mut l = leader(1, vec![2, 3]);
        l.propose(vec![7]).unwrap();

        l.handle_append_entries_reply(2, AppendEntriesReply {
            term: 99,
            success: false,
            conflict_index: None,
        });

        assert_eq!(l.state, NodeState::Follower);
        assert_eq!(l.current_term, 99);
    }

    #[test]
    fn next_index_advances_after_replicate() {
        let mut l = leader(1, vec![2, 3]);
        l.propose(vec![7]).unwrap();

        assert_eq!(l.next_index[&2], 2);
        assert_eq!(l.next_index[&3], 2);
        assert_eq!(l.match_index[&2], 0);
        assert_eq!(l.match_index[&3], 0);
    }
}