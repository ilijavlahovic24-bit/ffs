use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct VfsMetrics {
    pub forwarded_proposals: AtomicU64,
    pub local_proposals: AtomicU64,
    pub forwarded_reads: AtomicU64,
    pub local_reads: AtomicU64,
    pub not_leader_errors: AtomicU64,
}

impl VfsMetrics {
    pub fn new() -> Self { Self::default() }

    pub fn inc_forwarded_proposal(&self) {
        self.forwarded_proposals.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_local_proposal(&self) {
        self.local_proposals.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_forwarded_read(&self) {
        self.forwarded_reads.fetch_add(1, Ordering::Relaxed);
    }
    pub fn inc_local_read(&self) {
        self.local_reads.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            forwarded_proposals: self.forwarded_proposals.load(Ordering::Relaxed),
            local_proposals: self.local_proposals.load(Ordering::Relaxed),
            forwarded_reads: self.forwarded_reads.load(Ordering::Relaxed),
            local_reads: self.local_reads.load(Ordering::Relaxed),
            not_leader_errors: self.not_leader_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub forwarded_proposals: u64,
    pub local_proposals: u64,
    pub forwarded_reads: u64,
    pub local_reads: u64,
    pub not_leader_errors: u64,
}