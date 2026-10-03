use std::sync::Arc;
use std::num::NonZeroU32;
use std::time::Duration;

use fuse3::raw::prelude::*;
use fuse3::raw::reply::{ReplyAttr, ReplyInit};
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
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
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
}