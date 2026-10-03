use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::Mutex;

use fuse3::MountOptions;
use fuse3::raw::Session;

use common::error::FfsError;
use common::state_machine::StateMachine;
use consensus::{NodeState, RaftNode};
use meta::{InodeManager, MetaStore};
use storage::allocator::Allocator;
use storage::wal::{DataPath, MetadataWal};
use vfs::VfsLayer;

use crate::DistributedFUSE;

/// Boot a single-node Raft cluster and mount FUSE.
pub async fn mount(
    mount_point: PathBuf,
    storage_wal_path: PathBuf,
    raft_wal_path: PathBuf,
    tmp_dir: PathBuf,
    data_dir: PathBuf,
) -> Result<(), FfsError> {
    // 1. Local storage WAL + data path (for blob writes; not replicated).
    let storage_wal = Arc::new(MetadataWal::new(storage_wal_path).await?);
    let data_path = Arc::new(DataPath::new(tmp_dir, data_dir, storage_wal).await);
    let allocator = Arc::new(Allocator::new());

    // 2. State machine (also used for reads).
    let inodes = Arc::new(InodeManager::new().await);

    // 3. Raft node with no peers — commits immediately.
    let node = RaftNode::with_state_machine(
        1,
        vec![],
        &raft_wal_path,
        inodes.clone() as Arc<dyn StateMachine>,
    )?;
    let node = Arc::new(Mutex::new(node));

    // 4. Force leadership immediately (single-node shortcut).
    {
        let mut n = node.lock().await;
        n.state = NodeState::Leader;
        n.current_term = 1;
        n.become_leader(); // proposes no-op, which commits right away
    }

    // 5. MetaStore.
    let meta = Arc::new(MetaStore::new(node.clone(), inodes.clone()));

    // 6. VfsLayer.
    let vfs = Arc::new(VfsLayer::new(inodes, meta, data_path, allocator));

    // 7. Mount FUSE.
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