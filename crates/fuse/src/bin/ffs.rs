use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use fuse3::MountOptions;
use fuse3::raw::Session;

use common::error::FfsError;
use common::state_machine::StateMachine;
use consensus::RaftNode;
use fuse::DistributedFUSE;
use meta::{InodeManager, MetaStore};
use network::{RaftForwarder, RaftServer};
use storage::ChunkStore;
use storage::allocator::Allocator;
use vfs::VfsLayer;

#[derive(Parser, Debug)]
#[command(name = "ffs")]
struct Cli {
    /// Unique node id in the cluster.
    #[arg(long)]
    node_id: u64,

    /// Peers as `id=addr,id=addr`. Do NOT include self.
    #[arg(long)]
    peers: String,

    /// Address this node listens on for Raft RPCs.
    #[arg(long)]
    raft_addr: SocketAddr,

    /// Directory for FUSE mount.
    #[arg(long)]
    mount: PathBuf,

    /// Directory for WAL + data + tmp.
    #[arg(long)]
    wal_dir: PathBuf,
}

fn parse_peers(s: &str) -> Result<Vec<(u64, SocketAddr)>, FfsError> {
    if s.trim().is_empty() {
        return Ok(Vec::new());
    }
    s.split(',')
        .map(|pair| {
            let (id, addr) = pair
                .split_once('=')
                .ok_or_else(|| FfsError::StorageError(format!("bad peer spec: {pair}")))?;
            let id: u64 = id
                .trim()
                .parse()
                .map_err(|e| FfsError::StorageError(format!("bad id: {e}")))?;
            let addr: SocketAddr = addr
                .trim()
                .parse()
                .map_err(|e| FfsError::StorageError(format!("bad addr: {e}")))?;
            Ok((id, addr))
        })
        .collect()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
async fn main() -> Result<(), FfsError> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cli = Cli::parse();
    let peers = parse_peers(&cli.peers)?;

    // Directories.
    std::fs::create_dir_all(&cli.wal_dir)?;
    std::fs::create_dir_all(&cli.mount)?;
    let tmp_dir = cli.wal_dir.join("tmp");
    let data_dir = cli.wal_dir.join("data");
    std::fs::create_dir_all(&tmp_dir)?;
    std::fs::create_dir_all(&data_dir)?;

    let raft_wal_path = cli.wal_dir.join("raft.wal");

    // Chunk store (ADR-005).
    let chunks = Arc::new(ChunkStore::new(tmp_dir, data_dir));

    // Allocator (reserved for future block placement policy).
    let allocator = Arc::new(Allocator::new());

    // State machine.
    let inodes = Arc::new(InodeManager::new().await);

    // Raft node.
    let peer_ids: Vec<u64> = peers.iter().map(|(id, _)| *id).collect();
    let node = RaftNode::with_state_machine(
        cli.node_id,
        peer_ids,
        &raft_wal_path,
        inodes.clone() as Arc<dyn StateMachine>,
    )?;
    let node = Arc::new(Mutex::new(node));

    // Peer address map for forwarder + server.
    let peer_addrs: HashMap<u64, SocketAddr> = peers.iter().copied().collect();

    // Forwarder (routes follower ops to leader).
    let forwarder = Arc::new(RaftForwarder::new(node.clone(), peer_addrs.clone()));

    // TCP Raft server.
    let listener = TcpListener::bind(cli.raft_addr).await?;
    tracing::info!(addr = %cli.raft_addr, "raft listening");
    let server = RaftServer::new(listener, node.clone(), peer_addrs, chunks.clone());
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            tracing::error!("raft server died: {e}");
        }
    });

    // MetaStore + VFS.
    let meta = Arc::new(MetaStore::new(inodes.clone(), forwarder.clone()));
    let vfs = Arc::new(VfsLayer::new(
        inodes,
        meta,
        chunks,
        allocator,
        forwarder,
    ));
    // Startup repair runs in the background — mount does not wait.
    let vfs_for_repair = vfs.clone();
    tokio::spawn(async move {
        vfs_for_repair.startup_repair().await;
    });
    // FUSE.
    let fs = DistributedFUSE::new(vfs);
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mut opts = MountOptions::default();
    opts.uid(uid).gid(gid);

    tracing::info!(mount = %cli.mount.display(), "mounting FUSE");
    Session::new(opts)
        .mount_with_unprivileged(fs, cli.mount.clone())
        .await
        .map_err(|e| FfsError::StorageError(format!("mount: {e}")))?
        .await
        .map_err(|e| FfsError::StorageError(format!("session: {e}")))?;

    Ok(())
}