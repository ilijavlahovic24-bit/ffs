use std::sync::Arc;
use fuse3::raw::prelude::*;
use fuse3::{Result};
use fuse3::raw::reply::{ReplyAttr, ReplyInit};
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime};
use crate::inode::InodeManager;

pub struct AttrHandler {
    inode_manager: Arc<InodeManager>,
}

impl AttrHandler {
    pub fn new(inode_manager: Arc<InodeManager>) -> Self {
        Self { inode_manager }
    }

    pub async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
        })
    }

    pub async fn destroy(&self, _req: Request) {
        // Cleanup
    }

    pub async fn getattr(&self, _req: Request, inode: u64, _fh: Option<u64>, _flags: u32) -> Result<ReplyAttr> {
        if let Some(info) = self.inode_manager.get_inode(inode) {
            Ok(ReplyAttr {
                ttl: Duration::from_secs(1),
                attr: FileAttr {
                    ino: info.ino,
                    size: info.size,
                    blocks: (info.size + 511) / 512,
                    atime: SystemTime::now().into(),
                    mtime: SystemTime::now().into(),
                    ctime: SystemTime::now().into(),
                    #[cfg(target_os = "macos")]
                    crtime: SystemTime::now().into(),
                    kind: info.kind,
                    perm: info.mode,
                    nlink: if info.kind == FileType::Directory { 2 } else { 1 },
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    #[cfg(target_os = "macos")]
                    flags: 0,
                    blksize: 4096,
                },
            })
        } else {
            Err(libc::ENOENT.into())
        }
    }
}