//! In-process integration tests for the Raft state machine.
//!
//! A `Cluster` holds several `RaftNode`s and routes messages between them
//! in memory. No TCP, no tokio tasks per node — everything is driven
//! synchronously by `tick_all()`, which advances timers and drains all
//! outboxes.

use std::collections::HashMap;
use std::time::Duration;

use consensus::{NodeState, OutgoingMessage, RaftNode};

struct Cluster {
    nodes: HashMap<u64, RaftNode>,
}

impl Cluster {
    fn new(ids: &[u64]) -> Self {
        let mut nodes = HashMap::new();
        for &id in ids {
            let peers: Vec<u64> = ids.iter().copied().filter(|&x| x != id).collect();
            nodes.insert(id, RaftNode::new(id, peers));
        }
        Self { nodes }
    }

    /// Advance every node's timers once, then deliver all pending messages.
    fn tick_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.nodes.get_mut(&id).unwrap().tick();
        }
        self.deliver_all();
    }

    /// Deliver messages until no node has anything left in its outbox.
    /// New messages can be produced as a side effect of handling a reply,
    /// hence the outer loop.
    fn deliver_all(&mut self) {
        loop {
            let mut produced = false;
            let ids: Vec<u64> = self.nodes.keys().copied().collect();
            for from_id in ids {
                let msgs = self.nodes.get_mut(&from_id).unwrap().take_outbox();
                for msg in msgs {
                    produced = true;
                    self.deliver(from_id, msg);
                }
            }
            if !produced {
                break;
            }
        }
    }

    fn deliver(&mut self, from_id: u64, msg: OutgoingMessage) {
        match msg {
            OutgoingMessage::RequestVote { to, req } => {
                if let Some(to_node) = self.nodes.get_mut(&to) {
                    let reply = to_node.handle_request_vote(req);
                    if let Some(from_node) = self.nodes.get_mut(&from_id) {
                        from_node.handle_vote_reply(to, reply);
                    }
                }
            }
            OutgoingMessage::AppendEntries { to, req } => {
                if let Some(to_node) = self.nodes.get_mut(&to) {
                    let reply = to_node.handle_append_entries(req);
                    if let Some(from_node) = self.nodes.get_mut(&from_id) {
                        from_node.handle_append_entries_reply(to, reply);
                    }
                }
            }
        }
    }

    fn leaders(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.state == NodeState::Leader)
            .map(|(id, _)| *id)
            .collect()
    }

    fn leader(&self) -> Option<u64> {
        self.leaders().into_iter().next()
    }

    /// Run the cluster for up to `max_ms`, ticking every `step_ms`.
    /// Returns the elected leader id, or `None` if no leader emerged.
    async fn run_until_leader(&mut self, max_ms: u64, step_ms: u64) -> Option<u64> {
        let iterations = max_ms / step_ms;
        for _ in 0..iterations {
            self.tick_all();
            if let Some(leader) = self.leader() {
                return Some(leader);
            }
            tokio::time::sleep(Duration::from_millis(step_ms)).await;
        }
        None
    }
}

#[tokio::test]
async fn elects_single_leader_in_three_node_cluster() {
    let mut cluster = Cluster::new(&[1, 2, 3]);

    let leader = cluster
        .run_until_leader(2000, 10)
        .await
        .expect("no leader elected within 2s");

    // Exactly one leader.
    assert_eq!(cluster.leaders().len(), 1);
    assert_eq!(cluster.leader(), Some(leader));

    // The leader has a higher term than the initial 0.
    assert!(cluster.nodes[&leader].current_term >= 1);

    // Followers are all in Follower state.
    for (&id, node) in &cluster.nodes {
        if id != leader {
            assert_eq!(node.state, NodeState::Follower);
        }
    }
}

#[tokio::test]
async fn leader_replicates_entry_to_followers() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let leader_id = cluster
        .run_until_leader(2000, 10)
        .await
        .expect("no leader");

    // Propose a command.
    let idx = cluster
        .nodes
        .get_mut(&leader_id)
        .unwrap()
        .propose(b"hello".to_vec())
        .expect("leader should accept proposal");
    assert_eq!(idx, 1);

    // Deliver everything, tick a few times so heartbeat carries commit_index.
    for _ in 0..5 {
        cluster.deliver_all();
        cluster.tick_all();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Every node must have the entry at index 1.
    for (&id, node) in &cluster.nodes {
        let entry = node
            .log
            .get(1)
            .unwrap_or_else(|| panic!("node {id} missing log entry at index 1"));
        assert_eq!(entry.command, b"hello");
    }

    // Every node must have advanced commit_index to at least 1.
    for (&id, node) in &cluster.nodes {
        assert!(
            node.commit_index >= 1,
            "node {id} commit_index = {}",
            node.commit_index
        );
        assert!(
            node.last_applied >= 1,
            "node {id} last_applied = {}",
            node.last_applied
        );
    }
}
#[tokio::test]
async fn followers_step_down_on_higher_term() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let leader_id = cluster
        .run_until_leader(2000, 10)
        .await
        .expect("no leader");

    let leader_term = cluster.nodes[&leader_id].current_term;

    // Bump one follower's term artificially. The old leader will only
    // learn about the higher term when it next sends a heartbeat and
    // receives a rejection (reply.term > current_term).
    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .unwrap();
    cluster.nodes.get_mut(&follower_id).unwrap().current_term = leader_term + 5;

    // Run for a while: the old leader should step down, and a new
    // election should produce a leader in a higher term.
    for _ in 0..300 {
        cluster.tick_all();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let leaders = cluster.leaders();
    assert!(!leaders.is_empty(), "no leader after running");

    for &l in &leaders {
        let term = cluster.nodes[&l].current_term;
        assert!(
            term > leader_term,
            "leader {l} has term {term}, expected > {leader_term}"
        );
    }
}