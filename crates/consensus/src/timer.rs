use std::time::{Duration, Instant};

use rand::Rng;


pub const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
pub const ELECTION_TIMEOUT_MAX_MS: u64 = 600;   // bilo 300 — širi opseg
pub const HEARTBEAT_INTERVAL_MS: u64 = 50;

pub fn random_election_timeout() -> Duration {
    let ms = rand::thread_rng()
        .gen_range(ELECTION_TIMEOUT_MIN_MS..ELECTION_TIMEOUT_MAX_MS);
    Duration::from_millis(ms)
}

pub fn heartbeat_interval() -> Duration {
    Duration::from_millis(HEARTBEAT_INTERVAL_MS)
}

/// Reset election deadline with a per-node deterministic jitter.
///
/// On a single machine with a 10ms tick, all nodes may see their deadline
/// expire in the same tick. We add a small per-node offset so ties are
/// broken deterministically before the random timeout kicks in.
pub fn reset_election_deadline_for(node_id: u64) -> Instant {
    let base = rand::thread_rng()
        .gen_range(ELECTION_TIMEOUT_MIN_MS..ELECTION_TIMEOUT_MAX_MS);
    let jitter = (node_id.wrapping_mul(37)) % 120;
    Instant::now() + Duration::from_millis(base + jitter)
}
pub fn reset_election_deadline() -> Instant {
    Instant::now() + random_election_timeout()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeout_in_range() {
        for _ in 0..100 {
            let t = random_election_timeout().as_millis() as u64;
            assert!(t >= ELECTION_TIMEOUT_MIN_MS);
            assert!(t < ELECTION_TIMEOUT_MAX_MS);
        }
    }
}