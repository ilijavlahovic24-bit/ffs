use std::time::{Duration, Instant};

use rand::Rng;

pub const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
pub const ELECTION_TIMEOUT_MAX_MS: u64 = 300;
pub const HEARTBEAT_INTERVAL_MS: u64 = 50;

pub fn random_election_timeout() -> Duration {
    let ms = rand::thread_rng()
        .gen_range(ELECTION_TIMEOUT_MIN_MS..ELECTION_TIMEOUT_MAX_MS);
    Duration::from_millis(ms)
}

pub fn heartbeat_interval() -> Duration {
    Duration::from_millis(HEARTBEAT_INTERVAL_MS)
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