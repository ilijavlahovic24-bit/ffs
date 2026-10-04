use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Mutex;

use consensus::{NodeState, RaftNode};
use network::RaftServer;

/// Bind a listener on an ephemeral port and return it with its address.
async fn bind() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    (l, a)
}

/// Spawn a 3-node cluster over real TCP. Returns the nodes, addresses,
/// and the server tasks (which run forever; drop the runtime to stop).
async fn spawn_cluster() -> (
    HashMap<u64, Arc<Mutex<RaftNode>>>,
    HashMap<u64, SocketAddr>,
) {
    let (l1, a1) = bind().await;
    let (l2, a2) = bind().await;
    let (l3, a3) = bind().await;

    let addrs: HashMap<u64, SocketAddr> = [(1, a1), (2, a2), (3, a3)]
        .into_iter()
        .collect();

    let mut nodes: HashMap<u64, Arc<Mutex<RaftNode>>> = HashMap::new();
    let mut servers = Vec::new();

    let setup: [(u64, TcpListener, SocketAddr); 3] =
        [(1, l1, a1), (2, l2, a2), (3, l3, a3)];

    for (id, listener, _addr) in setup {
        let peers: Vec<u64> = [1u64, 2, 3]
            .iter()
            .copied()
            .filter(|&x| x != id)
            .collect();

        // Fresh per-node WAL.
        let base = std::env::temp_dir().join(format!(
            "ferumfs-net-test-{}-{}",
            id,
            std::process::id()
        ));
        let wal = base.with_extension("raft.wal");
        let _ = std::fs::remove_file(&wal);

        let node = Arc::new(Mutex::new(
            RaftNode::with_persistence(id, peers, &wal).unwrap(),
        ));

        // Build a DataPath for this node. The RaftServer needs it to
        // serve ReadBlob requests from followers.
        let storage_wal_path = base.with_extension("storage.wal");
        let _ = std::fs::remove_file(&storage_wal_path);
        let storage_wal = Arc::new(
            storage::wal::MetadataWal::new(storage_wal_path).await.unwrap(),
        );

        let tmp_dir = base.join("tmp");
        let data_dir = base.join("data");
        std::fs::create_dir_all(&tmp_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let data_path = Arc::new(
            storage::wal::DataPath::new(tmp_dir, data_dir, storage_wal).await,
        );

        // Peer address map: everyone except self.
        let peer_addrs: HashMap<u64, SocketAddr> = addrs
            .iter()
            .filter(|(pid, _)| **pid != id)
            .map(|(pid, a)| (*pid, *a))
            .collect();

        let server = RaftServer::new(listener, node.clone(), peer_addrs, data_path);
        servers.push(tokio::spawn(server.run()));

        nodes.insert(id, node);
    }

    // Servers own the listeners; run forever.
    tokio::spawn(async move {
        for s in servers {
            let _ = s.await;
        }
    });

    (nodes, addrs)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_elect_leader_over_tcp() {
    let (nodes, _) = spawn_cluster().await;

    // Wait up to 3 seconds for a leader.
    let mut leader_id = None;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        for (id, n) in &nodes {
            if n.lock().await.state == NodeState::Leader {
                leader_id = Some(*id);
                break;
            }
        }
        if leader_id.is_some() {
            break;
        }
    }

    let leader_id = leader_id.expect("no leader elected within 3s");

    // Exactly one leader.
    let leaders: Vec<u64> = {
        let mut v = Vec::new();
        for (id, n) in &nodes {
            if n.lock().await.state == NodeState::Leader {
                v.push(*id);
            }
        }
        v
    };
    assert_eq!(leaders.len(), 1);
    assert_eq!(leaders[0], leader_id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entry_replicates_to_all_nodes_over_tcp() {
    let (nodes, _) = spawn_cluster().await;

    // Wait for a leader.
    let mut leader_id = None;
    for _ in 0..300 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        for (id, n) in &nodes {
            if n.lock().await.state == NodeState::Leader {
                leader_id = Some(*id);
                break;
            }
        }
        if leader_id.is_some() {
            break;
        }
    }
    let leader_id = leader_id.expect("no leader");

    // Propose.
    {
        let mut l = nodes[&leader_id].lock().await;
        l.propose(b"over-tcp".to_vec()).expect("leader accepts");
    }

    // Give replication time.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Every node must have the entry.
    for (id, n) in &nodes {
        let node = n.lock().await;
        let entry = node
            .log
            .get(1)
            .unwrap_or_else(|| panic!("node {id} missing entry 1"));
        assert_eq!(entry.command, b"over-tcp");
        assert!(node.commit_index >= 1, "node {id} not committed");
    }
}