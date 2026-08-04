use std::sync::atomic::AtomicU64;

struct Metric {
    reads_total:AtomicU64,
    writes_total:AtomicU64,
    wal_appends_total:AtomicU64,
    checksum_total:AtomicU64,
}
impl Metric {
    fn new() -> Metric {
        Self{
            reads_total:AtomicU64::new(0),
            writes_total:AtomicU64::new(0),
            wal_appends_total:AtomicU64::new(0),
            checksum_total:AtomicU64::new(0),
        }
    }
    fn increment(&self, counter: &AtomicU64){
        todo!()
    }
    fn snapshot(&self) -> MetricsSnapshot{
        todo!()
    }
}
struct MetricsSnapshot {
    commits:AtomicU64,
}