use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use fuse3::raw::prelude::*;
use fuse3::raw::reply::{ReplyAttr, ReplyInit, ReplyStatFs};
use fuse3::SetAttr;

use fuse3::Result;

use vfs::VfsLayer;

use crate::helper::to_file_attr;

pub struct AttrHandler {
    vfs: Arc<VfsLayer>,
}

impl AttrHandler {
    pub fn new(vfs: Arc<VfsLayer>) -> Self {
        Self { vfs }
    }

    pub async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(1024 * 1024).unwrap(),
        })
    }

    pub async fn destroy(&self, _req: Request) {}

    pub async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        match self.vfs.getattr(inode) {
            Ok(info) => Ok(ReplyAttr {
                ttl: Duration::from_secs(1),
                attr: to_file_attr(&info),
            }),
            Err(_) => Err(libc::ENOENT.into()),
        }
    }

    pub async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr> {
        if let Some(m) = set_attr.mode {
            let _ = self.vfs.chmod(inode, (m & 0o7777) as u16).await;
        }
        if let Some(sz) = set_attr.size {
            self.vfs
                .truncate(inode, sz)
                .await
                .map_err(|_| libc::EIO)?;
        }
        // atime/mtime/ctime updates ignored for Phase 7
        self.getattr(_req, inode, None, 0).await
    }

    pub async fn statfs(&self, _req: Request, _inode: u64) -> Result<ReplyStatFs> {
        Ok(ReplyStatFs {
            blocks: 1_000_000,
            bfree: 900_000,
            bavail: 900_000,
            files: 1_000_000,
            ffree: 999_000,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }
}