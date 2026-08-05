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
    fn new() -> Metric {
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
    fn increment(&self, counter: &AtomicU64){
        counter.fetch_add(1, Ordering::Relaxed);
    }
    fn snapshot(&self) -> MetricsSnapshot{
        todo!()
    }
}
pub struct MetricsSnapshot {
    commits:AtomicU64,
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