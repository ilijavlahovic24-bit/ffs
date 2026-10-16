//! Deterministic chaos tests with fixed seeds.

use chaos::cluster::Cluster;
use chaos::invariants::check_invariants;

#[test]
fn elects_leader_in_three_node_cluster() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let leader = cluster
        .wait_for_leader_fast(1000)
        .expect("no leader");
    assert!(cluster.live_ids().contains(&leader));
    assert_eq!(cluster.leaders().len(), 1);
}

#[test]
fn kills_leader_new_one_elected() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let old_leader = cluster.wait_for_leader_fast(1000).expect("no leader");

    cluster.kill(old_leader);

    let new_leader = cluster.wait_for_leader_fast(2000);
    assert!(new_leader.is_some(), "no new leader after killing old one");

    let new_leader = new_leader.unwrap();
    assert_ne!(new_leader, old_leader);
    assert_eq!(cluster.leaders().len(), 1);
}

#[test]
fn minority_cannot_elect_leader() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let leader = cluster.wait_for_leader_fast(1000).expect("no leader");

    // Kill the leader and one follower — only one node remains alive.
    //
    // We must kill the leader explicitly: a healthy Raft leader stays
    // leader even if all followers disappear (Raft has no leader lease).
    // Only after the leader is gone can the remaining minority try to
    // elect — and fail, because 1 out of 3 is not a quorum.
    let followers: Vec<u64> = [1u64, 2, 3]
        .iter()
        .copied()
        .filter(|&id| id != leader)
        .collect();
    cluster.kill(leader);
    cluster.kill(followers[0]);

    // The single remaining node can never get 2 votes out of 3.
    let result = cluster.wait_for_leader_fast(500);
    assert!(
        result.is_none(),
        "minority of 1 should not elect a leader, but got {:?}",
        result
    );
}
#[test]
fn proposal_replicates_to_all_live_nodes() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    cluster.wait_for_leader_fast(1000).expect("no leader");

    let (leader, idx) = cluster
        .propose(b"test-cmd".to_vec())
        .expect("proposal failed");

    for _ in 0..100 {
        cluster.tick_all();
    }

    for id in cluster.live_ids() {
        let node = cluster.node(id).unwrap();
        let entry = node
            .log
            .get(idx)
            .unwrap_or_else(|| panic!("node {id} missing entry at {idx}"));
        assert_eq!(entry.command, b"test-cmd");
    }

    assert_eq!(cluster.leader(), Some(leader));
}

#[test]
fn invariants_hold_after_kill_restart_cycle() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    cluster.wait_for_leader_fast(1000).expect("no leader");

    for cycle in 0..50 {
        let live = cluster.live_ids();
        if live.len() > 1 {
            let victim = live[cycle % live.len()];
            cluster.kill(victim);
        }
        cluster.wait_for_leader_fast(500);

        let dead: Vec<u64> = [1, 2, 3]
            .iter()
            .copied()
            .filter(|id| !cluster.is_live(*id))
            .collect();
        if let Some(&v) = dead.first() {
            cluster.restart(v);
        }
        cluster.wait_for_leader_fast(500);

        let violations = check_invariants(&cluster);
        assert!(
            violations.is_empty(),
            "invariant violated on cycle {cycle}: {violations:?}"
        );
    }
}

#[test]
fn committed_entries_survive_leader_kill() {
    let mut cluster = Cluster::new(&[1, 2, 3]);
    let leader = cluster.wait_for_leader_fast(1000).expect("no leader");

    for i in 0..5u32 {
        cluster
            .propose(format!("cmd-{i}").into_bytes())
            .expect("proposal failed");
        cluster.tick_all();
    }

    let committed_before = cluster.committed_entries();
    assert!(!committed_before.is_empty());

    cluster.kill(leader);
    let new_leader = cluster.wait_for_leader_fast(2000).expect("no new leader");
    cluster.restart(leader);
    cluster.wait_for_leader_fast(500);

    for entry in &committed_before {
        let still_held = cluster.live_ids().iter().any(|&id| {
            cluster
                .node(id)
                .and_then(|n| n.log.get(entry.0))
                .map(|e| e.term == entry.1)
                .unwrap_or(false)
        });
        assert!(
            still_held,
            "committed entry {:?} was lost after leader kill",
            entry
        );
    }

    assert_eq!(cluster.leader(), Some(new_leader));
}