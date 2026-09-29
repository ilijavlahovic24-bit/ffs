use crate::log::LogEntry;
use crate::rpc::{AppendEntries, AppendEntriesReply};
use crate::state::{NodeState, OutgoingMessage, RaftNode};

impl RaftNode {
    pub fn handle_append_entries(&mut self, req: AppendEntries) -> AppendEntriesReply {
        if req.term > self.current_term {
            self.current_term = req.term;
            self.voted_for = None;
            self.state = NodeState::Follower;
        }

        if req.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: None,
            };
        }

        self.state = NodeState::Follower;
        self.election_deadline = crate::timer::reset_election_deadline();

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

        for entry in &req.entries {
            let idx = entry.index;
            match self.log.get(idx) {
                Some(existing) if existing.term == entry.term => {
                    continue;
                }
                Some(_) => {
                    self.log.truncate_from(idx);
                }
                None => {}
            }
            self.log.append(entry.clone());
        }

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

    pub fn handle_append_entries_reply(&mut self, from: u64, reply: AppendEntriesReply) {
        if reply.term > self.current_term {
            self.current_term = reply.term;
            self.voted_for = None;
            self.state = NodeState::Follower;
            return;
        }

        if self.state != NodeState::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            if let Some(&next) = self.next_index.get(&from) {
                let matched = next.saturating_sub(1);
                self.match_index.insert(from, matched);
                self.next_index.insert(from, next);
                self.advance_commit_index();
            }
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

    pub fn send_heartbeat(&mut self) {
        if self.state != NodeState::Leader {
            return;
        }

        for &peer in &self.peers.clone() {
            self.replicate_to(peer);
        }
    }

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

    pub fn propose(&mut self, command: Vec<u8>) -> Option<u64> {
        if self.state != NodeState::Leader {
            return None;
        }

        let entry = LogEntry {
            term: self.current_term,
            index: 0,
            command,
        };
        self.log.append(entry);
        let index = self.log.last_index();

        for &peer in &self.peers.clone() {
            self.replicate_to(peer);
        }

        if self.peers.is_empty() {
            self.commit_index = index;
            self.apply_committed();
        }

        Some(index)
    }

    fn advance_commit_index(&mut self) {
        for idx in (self.commit_index + 1)..=self.log.last_index() {
            // Only entries from the current term can be committed directly
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

        self.apply_committed();
    }

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self.log.get(self.last_applied) {
                tracing::debug!(
                    node = self.id,
                    index = entry.index,
                    term = entry.term,
                    "applying committed entry"
                );
                
            }
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
}