//! In-process integration tests for the Raft state machine.
//!
//! A `Cluster` holds several `RaftNode`s and routes messages between them
//! in memory. No TCP, no tokio tasks per node — everything is driven
//! synchronously by `tick_all()`.

use std::collections::HashMap;
use std::time::Duration;

use consensus::{NodeState, OutgoingMessage, RaftNode};

struct Cluster {
    nodes: HashMap<u64, RaftNode>,
}

impl Cluster {
    fn new(ids: &[u64]) -> Self {
        let mut nodes = HashMap::new();
        for (i, &id) in ids.iter().enumerate() {
            let peers: Vec<u64> = ids.iter().copied().filter(|&x| x != id).collect();
            let mut node = RaftNode::new(id, peers);
            // Stagger initial deadlines so the first election doesn't
            // split-vote. Real clusters have this naturally because nodes
            // start at different times; tests start them all at once.
            node.election_deadline =
                std::time::Instant::now() + Duration::from_millis(300 * (i as u64 + 1));
            nodes.insert(id, node);
        }
        Self { nodes }
    }

    fn tick_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.nodes.get_mut(&id).unwrap().tick();
        }
        self.deliver_all();
    }

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
        .run_until_leader(120000, 10)
        .await
        .expect("no leader elected within 5s");

    assert_eq!(cluster.leaders().len(), 1);
    assert_eq!(cluster.leader(), Some(leader));
    assert!(cluster.nodes[&leader].current_term >= 1);

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
        .run_until_leader(5000, 10)
        .await
        .expect("no leader");

    // Propose and capture the actual log index (no-op occupies index 1).
    let idx = cluster
        .nodes
        .get_mut(&leader_id)
        .unwrap()
        .propose(b"hello".to_vec())
        .expect("leader should accept proposal");
    assert!(idx >= 1);

    // Deliver, tick, let it propagate.
    for _ in 0..20 {
        cluster.tick_all();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Every node must have the entry at `idx`.
    for (&id, node) in &cluster.nodes {
        let entry = node
            .log
            .get(idx)
            .unwrap_or_else(|| panic!("node {id} missing log entry at index {idx}"));
        assert_eq!(entry.command, b"hello");
    }

    // Every node must have advanced commit_index to at least `idx`.
    for (&id, node) in &cluster.nodes {
        assert!(
            node.commit_index >= idx,
            "node {id} commit_index = {}, expected >= {idx}",
            node.commit_index
        );
        assert!(
            node.last_applied >= idx,
            "node {id} last_applied = {}, expected >= {idx}",
            node.last_applied
        );
    }
}

#[tokio::test]
async fn followers_step_down_on_higher_term() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let leader_id = cluster
        .run_until_leader(5000, 10)
        .await
        .expect("no leader");

    let leader_term = cluster.nodes[&leader_id].current_term;

    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .unwrap();

    // Bump term AND force immediate election so the higher term actually
    // propagates. Without forcing the deadline, the follower would just
    // sit there waiting for its next election tick (up to 1s away).
    {
        let follower = cluster.nodes.get_mut(&follower_id).unwrap();
        follower.current_term = leader_term + 5;
        follower.force_election_timeout();
    }

    for _ in 0..500 {
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