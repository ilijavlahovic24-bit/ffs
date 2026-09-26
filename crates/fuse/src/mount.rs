// fuse/src/mount.rs
use std::path::PathBuf;
use std::sync::Arc;

use fuse3::MountOptions;
use fuse3::raw::Session;

use common::error::FfsError;

use crate::DistributedFUSE;
use crate::inode::InodeManager;

pub async fn mount(
    mount_point: PathBuf,
    wal_path: PathBuf,
    tmp_dir: PathBuf,
    data_dir: PathBuf,
) -> Result<(), FfsError> {
    // 1. Recovery PRE mount-a: rekonstruiši InodeManager iz WAL-a.
    let mut inode_mgr = InodeManager::new().await;
    storage::recovery::recover(&wal_path, &mut inode_mgr).await?;
    let inode_mgr = Arc::new(inode_mgr);

    // 2. FS sa već rekonstruisanim stanjem.
    let fs = DistributedFUSE::new(inode_mgr, wal_path, tmp_dir, data_dir).await?;

    // 3. Mount.
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut opts = MountOptions::default();
    opts.uid(uid).gid(gid);

    // fuse3 0.9: mount_with_unprivileged vraća Future<Output = Result<Future>>,
    // pa su dva .await-a.
    Session::new(opts)
        .mount_with_unprivileged(fs, mount_point)
        .await
        .map_err(|e| FfsError::StorageError(format!("mount: {e}")))?
        .await
        .map_err(|e| FfsError::StorageError(format!("session: {e}")))?;

    Ok(())
}