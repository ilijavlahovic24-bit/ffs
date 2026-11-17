use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

use fuse3::MountOptions;
use fuse3::raw::Session;

use common::error::FfsError;
use common::state_machine::StateMachine;
use consensus::RaftNode;
use meta::{InodeManager, MetaStore};
use network::RaftForwarder;
use storage::ChunkStore;
use storage::allocator::Allocator;
use vfs::VfsLayer;

use crate::DistributedFUSE;

/// Boot a single-node FerumFS instance and mount FUSE.
///
/// Single-node shortcut: no RaftServer, no TCP, no peers. The node is
/// forced into the Leader state so writes commit immediately.
pub async fn mount(
    mount_point: PathBuf,
    raft_wal_path: PathBuf,
    tmp_dir: PathBuf,
    data_dir: PathBuf,
) -> Result<(), FfsError> {
    // 1. Chunk store (ADR-005).
    let chunks = Arc::new(ChunkStore::new(tmp_dir, data_dir));

    // 2. Allocator (reserved).
    let allocator = Arc::new(Allocator::new());

    // 3. State machine.
    let inodes = Arc::new(InodeManager::new().await);

    // 4. Raft node with no peers.
    let node = RaftNode::with_state_machine(
        1,
        vec![],
        &raft_wal_path,
        inodes.clone() as Arc<dyn StateMachine>,
    )?;
    let node = Arc::new(Mutex::new(node));

    // 5. Force leadership immediately (single-node shortcut).
    {
        let mut n = node.lock().await;
        n.current_term = 1;
        n.become_leader();
    }

    // 6. Forwarder with no peers — all calls local.
    let forwarder = Arc::new(RaftForwarder::new(node.clone(), HashMap::new()));

    // 7. MetaStore.
    let meta = Arc::new(MetaStore::new(inodes.clone(), forwarder.clone()));

    // 8. VFS.
    let vfs = Arc::new(VfsLayer::new(
        inodes,
        meta,
        chunks,
        allocator,
        forwarder,
    ));

    // 9. Mount FUSE.
    let fs = DistributedFUSE::new(vfs);

    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };
    let mut opts = MountOptions::default();
    opts.uid(uid).gid(gid);

    Session::new(opts)
        .mount_with_unprivileged(fs, mount_point)
        .await
        .map_err(|e| FfsError::StorageError(format!("mount: {e}")))?
        .await
        .map_err(|e| FfsError::StorageError(format!("session: {e}")))?;

    Ok(())
}