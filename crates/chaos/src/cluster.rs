//! In-process N-node Raft cluster for chaos testing.
//!
//! A `Cluster` holds several `RaftNode` instances and a simulated message
//! router. Nodes can be killed and restarted at any time; messages to or
//! from dead nodes are dropped. This models the failure modes a real
//! distributed system has to survive.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use consensus::{NodeState, OutgoingMessage, RaftNode};

/// Per-node liveness state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeStatus {
    Live,
    Dead,
}

struct Node {
    raft: RaftNode,
    status: NodeStatus,
    /// Number of times this node has been killed and restarted.
    restarts: u32,
}

pub struct Cluster {
    nodes: HashMap<u64, Node>,
    /// Messages currently in flight (from → payload). Delivered on the
    /// next `deliver_all`.
    in_flight: Vec<(u64, OutgoingMessage)>,
}

impl Cluster {
    pub fn new(ids: &[u64]) -> Self {
        let mut nodes = HashMap::new();
        for &id in ids {
            let peers: Vec<u64> = ids.iter().copied().filter(|&x| x != id).collect();
            nodes.insert(
                id,
                Node {
                    raft: RaftNode::new(id, peers),
                    status: NodeStatus::Live,
                    restarts: 0,
                },
            );
        }
        Self {
            nodes,
            in_flight: Vec::new(),
        }
    }

    pub fn live_ids(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.status == NodeStatus::Live)
            .map(|(id, _)| *id)
            .collect()
    }

    pub fn node(&self, id: u64) -> Option<&RaftNode> {
        self.nodes.get(&id).map(|n| &n.raft)
    }

    pub fn node_mut(&mut self, id: u64) -> Option<&mut RaftNode> {
        self.nodes.get_mut(&id).map(|n| &mut n.raft)
    }

    pub fn leader(&self) -> Option<u64> {
        self.nodes
            .iter()
            .find(|(_, n)| n.status == NodeStatus::Live && n.raft.state == NodeState::Leader)
            .map(|(id, _)| *id)
    }

    pub fn leaders(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.status == NodeStatus::Live && n.raft.state == NodeState::Leader)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Kill a node. It stops sending and receiving; its state is retained
    /// so we can observe what it looks like after restart.
    pub fn kill(&mut self, id: u64) {
        if let Some(node) = self.nodes.get_mut(&id) {
            node.status = NodeStatus::Dead;
        }
    }

    /// Restart a node. Volatile state is lost; durable state survives.
    /// Per Raft (Figure 2), durable state is `current_term`, `voted_for`,
    /// and `log[]`. Everything else is volatile and reset on restart.
    pub fn restart(&mut self, id: u64) {
        let Some(node) = self.nodes.get_mut(&id) else {
            return;
        };
        let peers = node.raft.peers.clone();

        let mut fresh = RaftNode::new(id, peers);

        // Restore durable state.
        fresh.current_term = node.raft.current_term;
        fresh.voted_for = node.raft.voted_for;
        fresh.log = node.raft.log.clone();

        node.raft = fresh;
        node.status = NodeStatus::Live;
        node.restarts += 1;
    }

    pub fn is_live(&self, id: u64) -> bool {
        self.nodes
            .get(&id)
            .map(|n| n.status == NodeStatus::Live)
            .unwrap_or(false)
    }

    pub fn restart_count(&self, id: u64) -> u32 {
        self.nodes.get(&id).map(|n| n.restarts).unwrap_or(0)
    }

    /// Tick every live node once, then deliver all pending messages.
    pub fn tick_all(&mut self) {
        let live_ids: Vec<u64> = self.live_ids();
        for id in &live_ids {
            if let Some(node) = self.nodes.get_mut(id) {
                node.raft.tick();
                let outgoing = node.raft.take_outbox();
                for msg in outgoing {
                    self.in_flight.push((*id, msg));
                }
            }
        }
        self.deliver_all();
    }

    /// Deliver all in-flight messages until no new messages are produced.
    ///
    /// A bounded iteration count protects against pathological feedback
    /// loops; if the cap is hit, the remaining in-flight messages are
    /// dropped (safe — Raft re-sends on the next tick).
    pub fn deliver_all(&mut self) {
        const MAX_ROUNDS: usize = 10_000;

        for _ in 0..MAX_ROUNDS {
            let pending = std::mem::take(&mut self.in_flight);
            if pending.is_empty() {
                return;
            }

            let mut new_messages = Vec::new();

            for (from_id, msg) in pending {
                if !self.is_live(from_id) {
                    continue;
                }

                match msg {
                    OutgoingMessage::RequestVote { to, req } => {
                        if !self.is_live(to) {
                            continue;
                        }
                        let reply = self
                            .nodes
                            .get_mut(&to)
                            .unwrap()
                            .raft
                            .handle_request_vote(req);
                        if let Some(from_node) = self.nodes.get_mut(&from_id) {
                            from_node.raft.handle_vote_reply(to, reply);
                            for m in from_node.raft.take_outbox() {
                                new_messages.push((from_id, m));
                            }
                        }
                    }
                    OutgoingMessage::AppendEntries { to, req } => {
                        if !self.is_live(to) {
                            continue;
                        }
                        let reply = self
                            .nodes
                            .get_mut(&to)
                            .unwrap()
                            .raft
                            .handle_append_entries(req);
                        if let Some(from_node) = self.nodes.get_mut(&from_id) {
                            from_node.raft.handle_append_entries_reply(to, reply);
                            for m in from_node.raft.take_outbox() {
                                new_messages.push((from_id, m));
                            }
                        }
                    }
                }
            }

            self.in_flight = new_messages;
        }

        tracing::warn!(
            "deliver_all: iteration cap reached; dropping {} in-flight messages",
            self.in_flight.len()
        );
        self.in_flight.clear();
    }

    /// Run until exactly one leader is elected, or return `None` after
    /// `timeout_ms` of real time.
    ///
    /// Sleeps between ticks so election deadlines actually fire.
    pub async fn wait_for_leader(&mut self, timeout_ms: u64) -> Option<u64> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        while Instant::now() < deadline {
            self.tick_all();
            if self.leaders().len() == 1 {
                return self.leader();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        None
    }

    /// Fast in-memory wait for tests.
    ///
    /// Forces election timeouts to fire without real time passing.
    /// Only ONE node is forced per tick — otherwise all nodes start
    /// elections in the same term and we get an eternal split vote
    /// (the classic synchronous-simulation trap).
    pub fn wait_for_leader_fast(&mut self, max_ticks: u32) -> Option<u64> {
        for tick in 0..max_ticks {
            let live = self.live_ids();
            if !live.is_empty() {
                // Round-robin through live nodes so each gets its own
                // "election round" without colliding with the others.
                let idx = (tick as usize) % live.len();
                let victim = live[idx];
                if let Some(node) = self.nodes.get_mut(&victim) {
                    if node.raft.state != NodeState::Leader {
                        node.raft.force_election_timeout();
                    }
                }
            }
            self.tick_all();
            if self.leaders().len() == 1 {
                return self.leader();
            }
        }
        None
    }

    /// Propose a command on the current leader. Returns `(leader_id, index)`
    /// or `None` if there is no leader.
    pub fn propose(&mut self, command: Vec<u8>) -> Option<(u64, u64)> {
        let leader = self.leader()?;
        let node = self.nodes.get_mut(&leader)?;
        let idx = node.raft.propose(command)?;
        self.tick_all();
        Some((leader, idx))
    }

    pub fn snapshot_terms(&self) -> HashMap<u64, u64> {
        self.nodes
            .iter()
            .map(|(id, n)| (*id, n.raft.current_term))
            .collect()
    }

    pub fn log_len(&self, id: u64) -> Option<u64> {
        self.nodes.get(&id).map(|n| n.raft.log.last_index())
    }

    pub fn commit_index(&self, id: u64) -> Option<u64> {
        self.nodes.get(&id).map(|n| n.raft.commit_index)
    }

    pub fn last_applied(&self, id: u64) -> Option<u64> {
        self.nodes.get(&id).map(|n| n.raft.last_applied)
    }

    /// Return the set of committed entries (index, term) held by any
    /// live node.
    pub fn committed_entries(&self) -> HashSet<(u64, u64)> {
        let mut entries = HashSet::new();
        for node in self.nodes.values() {
            if node.status != NodeStatus::Live {
                continue;
            }
            for idx in 1..=node.raft.commit_index {
                if let Some(entry) = node.raft.log.get(idx) {
                    entries.insert((idx, entry.term));
                }
            }
        }
        entries
    }
}

/// Convenience: run the cluster for `duration_ms` at `step_ms` per tick.
pub async fn run_for(cluster: &mut Cluster, duration_ms: u64, step_ms: u64) {
    let steps = duration_ms / step_ms.max(1);
    for _ in 0..steps {
        cluster.tick_all();
        tokio::time::sleep(Duration::from_millis(step_ms)).await;
    }
}