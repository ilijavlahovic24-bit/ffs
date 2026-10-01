use crate::log::LogEntry;
use crate::rpc::{AppendEntries, AppendEntriesReply};
use crate::state::{NodeState, OutgoingMessage, RaftNode};

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
        // If the leader's term is higher, adopt it and step down.
        // step_down also fails any pending proposals and persists
        // the new hard state.
        if req.term > self.current_term {
            self.step_down(req.term);
        }

        // Stale leader from an old term — reject.
        if req.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: None,
            };
        }

        // A legitimate leader is contacting us. Reset the election timer
        // so we don't start our own election while the leader is alive.
        self.state = NodeState::Follower;
        self.election_deadline = crate::timer::reset_election_deadline();

        // Consistency check: our log must contain an entry at `prev_log_index`
        // whose term matches `prev_log_term`.
        if req.prev_log_index > 0 {
            let Some(our_entry) = self.log.get(req.prev_log_index) else {
                // We don't have that index at all — tell the leader where
                // our log ends so it can back off.
                return AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    conflict_index: Some(self.log.last_index() + 1),
                };
            };

            if our_entry.term != req.prev_log_term {
                // Term mismatch. Find the first index of the conflicting
                // term block so the leader can skip the whole thing.
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

        // Append entries, resolving conflicts along the way.
        for entry in &req.entries {
            let idx = entry.index;
            match self.log.get(idx) {
                Some(existing) if existing.term == entry.term => {
                    // Already have this entry — skip.
                    continue;
                }
                Some(_) => {
                    // Conflict at this index — truncate everything from here
                    // onward, then append the leader's version.
                    self.log.truncate_from(idx);
                }
                None => {}
            }
            self.log.append(entry.clone());

            // Durability: each appended entry must be on disk before we
            // acknowledge. A crash between here and the reply will cause
            // the leader to re-send — which is fine (idempotent).
            self.persist_append(entry);
        }

        // Advance commit_index if the leader has committed further.
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
        // Reply from a newer term — we are stale, step down.
        // step_down also fails all our pending proposals.
        if reply.term > self.current_term {
            self.step_down(reply.term);
            return;
        }

        // Ignore replies that don't belong to our current leadership.
        if self.state != NodeState::Leader || reply.term != self.current_term {
            return;
        }

        if reply.success {
            // `next_index` was already advanced optimistically in
            // `replicate_to` to whatever we sent. `match_index` is one less.
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            let matched = next.saturating_sub(1);
            self.match_index.insert(from, matched);
            self.advance_commit_index();
        } else {
            // Follower rejected — back off `next_index` using the hint
            // provided by `conflict_index`, if any.
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
    ///
    /// This is both the heartbeat and the replication primitive — if there
    /// are no new entries, the AppendEntries carries an empty `entries`
    /// vector.
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

        // Advance `next_index` optimistically to what we're about to send.
        // If the follower accepts, `match_index` becomes `next_index - 1`.
        // If it rejects, `handle_append_entries_reply` will pull it back
        // to `conflict_index`.
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
    ///
    /// Returns the log index the command was appended at, or `None` if
    /// this node is not the leader.
    ///
    /// This does NOT wait for commit — the caller that wants to await
    /// commit should use `propose_with_waiter` in `proposal.rs`.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<u64> {
        if self.state != NodeState::Leader {
            return None;
        }

        // Build the entry with the correct index up front so we can
        // persist exactly what we append.
        let index = self.log.last_index() + 1;
        let entry = LogEntry {
            term: self.current_term,
            index,
            command,
        };

        self.log.append(entry.clone());

        // Durability: the leader must persist its own entry before
        // replicating. Otherwise a crash could lose the entry from the
        // leader's log while followers still hold it.
        self.persist_append(&entry);

        // Replicate to all followers.
        for &peer in &self.peers.clone() {
            self.replicate_to(peer);
        }

        // Single-node cluster: commit immediately.
        if self.peers.is_empty() {
            self.commit_index = index;
            self.apply_committed();
        }

        Some(index)
    }

    /// Advance `commit_index` if a majority has replicated a newer entry.
    ///
    /// Only entries from the current term may be committed directly
    /// (Raft §5.4.2) — committing older-term entries directly can lead
    /// to a committed entry being rolled back.
    fn advance_commit_index(&mut self) {
        let old_commit = self.commit_index;

        for idx in (self.commit_index + 1)..=self.log.last_index() {
            if self.log.term_at(idx) != self.current_term {
                continue;
            }

            let mut replicated = 1; // count ourselves
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
            // Propagate the new commit_index immediately so followers
            // don't have to wait for the next periodic heartbeat.
            self.send_heartbeat();
        }

        self.apply_committed();
    }

    /// Apply all committed entries from `last_applied + 1` to `commit_index`.
    ///
    /// For each committed entry:
    /// - Decode and apply it via the state machine (if any).
    /// - Resolve any client proposal waiter for that index.
    ///
    /// Empty commands are treated as Raft no-ops and skipped, but their
    /// waiters (if any) still get resolved.
    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let idx = self.last_applied;

            // Grab the command bytes — we need them for the state machine.
            let Some(entry) = self.log.get(idx) else {
                // Should not happen; log entries are dense.
                tracing::warn!(node = self.id, index = idx, "missing log entry to apply");
                continue;
            };
            let command = entry.command.clone();
            let term = entry.term;

            // Apply to the state machine. Errors are logged and reported
            // to the client, but do not stop application — the next entry
            // must still be applied.
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

            // Resolve the client waiter (if any) for this index.
            self.resolve_proposal(idx, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::AppendEntries;

    /// Helper: create a node that is already a leader in term 1.
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

        // Our log has two entries from term 1.
        f.log.append(LogEntry { term: 1, index: 0, command: vec![1] });
        f.log.append(LogEntry { term: 1, index: 0, command: vec![2] });
        assert_eq!(f.log.last_index(), 2);

        // Leader sends a new entry at index 2 with term 3 — conflict.
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
            prev_log_index: 5, // we don't have this index
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
        // Only self has it so far.
        assert_eq!(l.commit_index, 0);

        // Reply from peer 2 pushes us to majority (2 of 3).
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

        // After propose, replicate_to moved next_index to last_index + 1 = 2.
        assert_eq!(l.next_index[&2], 2);
        assert_eq!(l.next_index[&3], 2);

        // match_index stays at 0 until a reply arrives.
        assert_eq!(l.match_index[&2], 0);
        assert_eq!(l.match_index[&3], 0);
    }
}