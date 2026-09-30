use std::time::Instant;

use crate::state::{NodeState, RaftNode};
use crate::timer::heartbeat_interval;

impl RaftNode {
    /// Advance internal timers.
    ///
    /// Called periodically by the driver task (Phase 5) — e.g. every 10ms.
    ///
    /// - Followers/Candidates: start an election when the election deadline
    ///   has passed.
    /// - Leaders: send heartbeats when the heartbeat interval has passed.
    ///
    /// Does not block, does not sleep — the caller controls the cadence.
    pub fn tick(&mut self) {
        let now = Instant::now();

        match self.state {
            NodeState::Follower | NodeState::Candidate => {
                if now >= self.election_deadline {
                    self.start_election();
                }
            }
            NodeState::Leader => {
                if now.duration_since(self.last_heartbeat) >= heartbeat_interval() {
                    self.last_heartbeat = now;
                    self.send_heartbeat();
                }
            }
        }
    }
}