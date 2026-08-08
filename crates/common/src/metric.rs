use std::sync::atomic::{AtomicU64, Ordering};

pub struct Metric {
    //storage operations
    reads_total:AtomicU64,
    writes_total:AtomicU64,
    wal_appends_total:AtomicU64,
    checksum_total:AtomicU64,
    //consensus operations
    elections_started_total:AtomicU64,
    leader_changes_total:AtomicU64,
    //performances
    bytes_read_total:AtomicU64,
    bytes_written_total:AtomicU64,
}
impl Metric {
    pub fn new() -> Metric {
        Self{
            reads_total:AtomicU64::new(0),
            writes_total:AtomicU64::new(0),
            wal_appends_total:AtomicU64::new(0),
            checksum_total:AtomicU64::new(0),

            elections_started_total: AtomicU64::new(0),
            leader_changes_total: AtomicU64::new(0),
            bytes_read_total: AtomicU64::new(0),
            bytes_written_total: AtomicU64::new(0),
        }
    }
    //example metrics.increment(&metrics.reads_total);
    pub fn increment(&self, counter: &AtomicU64){
        counter.fetch_add(1, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> MetricsSnapshot{
        MetricsSnapshot{
            reads_total: self.reads_total.load(Ordering::Relaxed),
            writes_total:self.writes_total.load(Ordering::Relaxed),
            wal_appends_total:self.wal_appends_total.load(Ordering::Relaxed),
            checksum_total:self.checksum_total.load(Ordering::Relaxed),

            elections_started_total: self.elections_started_total.load(Ordering::Relaxed),
            leader_changes_total: self.leader_changes_total.load(Ordering::Relaxed),
            bytes_read_total: self.bytes_read_total.load(Ordering::Relaxed),
            bytes_written_total: self.bytes_written_total.load(Ordering::Relaxed),
        }


    }
}
pub struct MetricsSnapshot {
    //storage operations
    reads_total:u64,
    writes_total:u64,
    wal_appends_total:u64,
    checksum_total:u64,
    //consensus operations
    elections_started_total:u64,
    leader_changes_total:u64,
    //performances
    bytes_read_total:u64,
    bytes_written_total:u64,
}
