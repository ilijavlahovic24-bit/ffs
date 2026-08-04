use std::net::SocketAddr;
use std::path::PathBuf;


struct StorageConfig{
    wal_path:PathBuf,
    data_path:PathBuf,
    blk_sz:usize,
}
impl StorageConfig {
    pub fn new(wal_path: PathBuf, data_path: PathBuf) -> Self {
        Self {
            wal_path,
            data_path,
            blk_sz: 4096,
        }
    }
}

struct ConsensusConfig{
    node_id: u64,
    peers: Vec<SocketAddr>,
    election_timeout_ms: u64,
    heartbeat_interval_ms: u64
}
impl ConsensusConfig {
    pub fn new(node_id: u64, peers: Vec<SocketAddr>, election_timeout_ms: u64, heartbeat_interval_ms: u64) -> Self {
        Self {
            node_id,
            peers,
            election_timeout_ms,
            heartbeat_interval_ms
        }
    }
}
struct FFSConfig{
    storage_config: StorageConfig,
    consensus_config: ConsensusConfig,
    mount_point:PathBuf
}
impl FFSConfig {
    pub fn new(storage_config: StorageConfig, consensus_config: ConsensusConfig, mount_point: PathBuf) -> Self {
        Self{
            storage_config,
            consensus_config,
            mount_point
        }
    }
}