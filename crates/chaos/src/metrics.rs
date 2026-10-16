//! Metrics collected during a chaos run.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use serde::Serialize;

/// Counter collection shared across the runner.
#[derive(Default)]
pub struct Metrics {
    pub kills: AtomicU64,
    pub restarts: AtomicU64,
    pub elections: AtomicU64,
    pub proposals: AtomicU64,
    pub commits: AtomicU64,
    pub violations: AtomicU64,

    election_durations_ms: Mutex<Vec<u64>>,
    propose_latencies_ms: Mutex<Vec<u64>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_kill(&self) {
        self.kills.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_restart(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_election(&self) {
        self.elections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_proposal(&self) {
        self.proposals.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_commit(&self) {
        self.commits.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_violation(&self) {
        self.violations.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_election(&self, d: Duration) {
        self.election_durations_ms
            .lock()
            .unwrap()
            .push(d.as_millis() as u64);
    }

    pub fn record_propose(&self, d: Duration) {
        self.propose_latencies_ms
            .lock()
            .unwrap()
            .push(d.as_millis() as u64);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        // Bind the locks first so the two reads don't deadlock each
        // other. `MutexGuard` temporaries in a struct literal live until
        // the end of the whole expression, not the field.
        let elections = self.election_durations_ms.lock().unwrap();
        let proposes = self.propose_latencies_ms.lock().unwrap();

        MetricsSnapshot {
            kills: self.kills.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            elections: self.elections.load(Ordering::Relaxed),
            proposals: self.proposals.load(Ordering::Relaxed),
            commits: self.commits.load(Ordering::Relaxed),
            violations: self.violations.load(Ordering::Relaxed),
            election_p50_ms: percentile(&elections, 50.0),
            election_p99_ms: percentile(&elections, 99.0),
            propose_p50_ms: percentile(&proposes, 50.0),
            propose_p99_ms: percentile(&proposes, 99.0),
        }
    }
}

/// Percentile of a list of samples using the "nearest rank" method.
/// `p` is 0.0..=100.0.
fn percentile(samples: &[u64], p: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();

    let n = sorted.len();
    // Nearest-rank: index = ceil(p/100 * n) - 1, clamped to [0, n-1].
    let rank = (p / 100.0) * (n as f64);
    let idx = (rank.ceil() as usize).saturating_sub(1).min(n - 1);
    sorted[idx]
}

/// Serializable snapshot for the JSON report.
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub kills: u64,
    pub restarts: u64,
    pub elections: u64,
    pub proposals: u64,
    pub commits: u64,
    pub violations: u64,
    pub election_p50_ms: u64,
    pub election_p99_ms: u64,
    pub propose_p50_ms: u64,
    pub propose_p99_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_of_known_data() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        assert_eq!(percentile(&data, 0.0), 1);
        assert_eq!(percentile(&data, 50.0), 5);
        assert_eq!(percentile(&data, 99.0), 10);
        assert_eq!(percentile(&data, 100.0), 10);
    }

    #[test]
    fn percentile_empty() {
        assert_eq!(percentile(&[], 50.0), 0);
    }
}