use std::sync::Arc;
use fuse3::raw::prelude::*;
use fuse3::{Result};
use fuse3::raw::reply::{ ReplyEntry, ReplyDirectory};
use futures_util::stream::{self, Stream};
use std::ffi::{OsStr, OsString};
use std::time::{Duration, SystemTime};
use crate::inode::{InodeManager, InodeInfo};

pub struct DirHandler {
    inode_manager: Arc<InodeManager>,
}

impl DirHandler {
    pub fn new(inode_manager: Arc<InodeManager>) -> Self {
        Self { inode_manager }
    }

    pub async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        let name_str = name.to_string_lossy();

        if let Some(ino) = self.inode_manager.lookup(parent, &name_str) {
            if let Some(info) = self.inode_manager.get_inode(ino) {
                return Ok(ReplyEntry {
                    ttl: Duration::from_secs(1),
                    attr: FileAttr {
                        ino: info.ino,
                        size: info.size,
                        blocks: 0,
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
                    generation: 0,
                });
            }
        }

        Err(libc::ENOENT.into())
    }

    pub async fn readdir(&self, _req: Request, inode: u64, _fh: u64, offset: i64) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + '_>> {
        let entries: Vec<Result<DirectoryEntry>> = vec![
            Ok(DirectoryEntry {
                inode,
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: 1,
            }),
            Ok(DirectoryEntry {
                inode: self.get_parent(inode).await,
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            }),
        ];

        Ok(ReplyDirectory {
            entries: stream::iter(entries.into_iter().skip(offset as usize)),
        })
    }

    async fn get_parent(&self, inode: u64) -> u64 {
        if let Some(info) = self.inode_manager.get_inode(inode) {
            info.parent
        } else {
            1
        }
    }

    pub async fn mkdir(&self, req: Request, parent: u64, name: &OsStr, mode: u32) -> Result<ReplyEntry> {
        // Forward to metadata service
        todo!("Implement distributed mkdir")
    }

    pub async fn unlink(&self, req: Request, parent: u64, name: &OsStr) -> Result<()> {
        // Forward to metadata service
        todo!("Implement distributed unlink")
    }

    pub async fn rmdir(&self, req: Request, parent: u64, name: &OsStr) -> Result<()> {
        // Forward to metadata service
        todo!("Implement distributed rmdir")
    }
}