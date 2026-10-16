//! Invariant checks for a running Raft cluster.
//!
//! These are the properties that must hold at every observable moment,
//! regardless of failures.

use serde::Serialize;

use consensus::NodeState;

use crate::cluster::Cluster;

#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    pub kind: String,
    pub details: String,
}

/// Check every invariant we know about. Returns an empty vec when the
/// cluster is healthy.
pub fn check_invariants(cluster: &Cluster) -> Vec<Violation> {
    let mut violations = Vec::new();

    // 1. Election Safety: at most one leader per term.
    let mut leaders_by_term: std::collections::HashMap<u64, Vec<u64>> =
        std::collections::HashMap::new();
    for id in cluster.live_ids() {
        if let Some(node) = cluster.node(id) {
            if node.state == NodeState::Leader {
                leaders_by_term
                    .entry(node.current_term)
                    .or_default()
                    .push(id);
            }
        }
    }
    for (term, leaders) in &leaders_by_term {
        if leaders.len() > 1 {
            violations.push(Violation {
                kind: "election_safety".into(),
                details: format!(
                    "term {term} has {} leaders: {:?}",
                    leaders.len(),
                    leaders
                ),
            });
        }
    }

    // 2. Log Matching on committed entries.
    //
    // We only check indices that at least one node has committed.
    // Uncommitted entries may transiently differ during reconciliation
    // after a restart — that's normal Raft behavior and gets resolved
    // by the next AppendEntries from the leader.
    let live = cluster.live_ids();
    for i in 0..live.len() {
        for j in (i + 1)..live.len() {
            let (a_id, b_id) = (live[i], live[j]);
            let Some(a) = cluster.node(a_id) else { continue };
            let Some(b) = cluster.node(b_id) else { continue };

            let common_max = a.log.last_index().min(b.log.last_index());
            for idx in 1..=common_max {
                // Skip if neither node has committed this index.
                if idx > a.commit_index && idx > b.commit_index {
                    continue;
                }
                let Some(ea) = a.log.get(idx) else { continue };
                let Some(eb) = b.log.get(idx) else { continue };
                if ea.term == eb.term && ea.command != eb.command {
                    violations.push(Violation {
                        kind: "log_matching".into(),
                        details: format!(
                            "nodes {a_id} and {b_id} differ at index {idx} (term {})",
                            ea.term
                        ),
                    });
                }
            }
        }
    }

    // 3. Commit Consistency.
    for &a_id in &live {
        let Some(a) = cluster.node(a_id) else { continue };
        for idx in 1..=a.commit_index {
            let Some(ea) = a.log.get(idx) else { continue };
            for &b_id in &live {
                if a_id == b_id {
                    continue;
                }
                let Some(b) = cluster.node(b_id) else { continue };
                if b.log.last_index() < idx {
                    continue;
                }
                let Some(eb) = b.log.get(idx) else { continue };
                if ea.term == eb.term && ea.command != eb.command {
                    violations.push(Violation {
                        kind: "commit_consistency".into(),
                        details: format!(
                            "committed entry at index {idx} on {a_id} conflicts with {b_id}"
                        ),
                    });
                }
            }
        }
    }
    violations
}