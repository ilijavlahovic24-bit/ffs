use std::sync::Arc;
use std::ffi::OsStr;
use std::time::Duration;

use fuse3::raw::prelude::*;
use fuse3::raw::reply::{ReplyEntry, ReplyDirectory, ReplyDirectoryPlus, DirectoryEntryPlus};
use fuse3::Result;
use futures_util::stream::{self, Stream};

use common::types::InodeInfo;
use vfs::VfsLayer;

use crate::convert::to_fuse_type;
use crate::helper::to_file_attr;

pub struct DirHandler {
    vfs: Arc<VfsLayer>,
}

impl DirHandler {
    pub fn new(vfs: Arc<VfsLayer>) -> Self {
        Self { vfs }
    }

    pub async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        let name = name.to_string_lossy();
        match self.vfs.lookup(parent, &name) {
            Ok(info) => Ok(ReplyEntry {
                ttl: Duration::from_secs(1),
                attr: to_file_attr(&info),
                generation: 0,
            }),
            Err(_) => Err(libc::ENOENT.into()),
        }
    }

    pub async fn readdir(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + '_>> {
        let parent = self.vfs.getattr(inode).map(|i| i.parent).unwrap_or(1);

        let mut entries: Vec<Result<DirectoryEntry>> = vec![
            Ok(DirectoryEntry {
                inode,
                kind: fuse3::FileType::Directory,
                name: ".".into(),
                offset: 1,
            }),
            Ok(DirectoryEntry {
                inode: parent,
                kind: fuse3::FileType::Directory,
                name: "..".into(),
                offset: 2,
            }),
        ];

        let children = self.vfs.readdir(inode).unwrap_or_default();
        for (i, child) in children.iter().enumerate() {
            entries.push(Ok(DirectoryEntry {
                inode: child.ino,
                kind: to_fuse_type(child.kind),
                name: child.name.clone().into(),
                offset: 3 + i as i64,
            }));
        }

        let start = offset.max(0) as usize;
        Ok(ReplyDirectory {
            entries: stream::iter(entries.into_iter().skip(start)),
        })
    }

    pub async fn readdirplus(
        &self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<
        ReplyDirectoryPlus<impl Stream<Item = Result<DirectoryEntryPlus>> + Send + '_>,
    > {
        let ttl = Duration::from_secs(1);
        let grandparent = self.vfs.getattr(parent).map(|i| i.parent).unwrap_or(1);

        let mut entries: Vec<Result<DirectoryEntryPlus>> = Vec::new();

        if let Ok(info) = self.vfs.getattr(parent) {
            entries.push(Ok(DirectoryEntryPlus {
                inode: parent,
                generation: 0,
                kind: to_fuse_type(info.kind),
                name: ".".into(),
                offset: 1,
                attr: to_file_attr(&info),
                entry_ttl: ttl,
                attr_ttl: ttl,
            }));
        }
        if let Ok(info) = self.vfs.getattr(grandparent) {
            entries.push(Ok(DirectoryEntryPlus {
                inode: grandparent,
                generation: 0,
                kind: to_fuse_type(info.kind),
                name: "..".into(),
                offset: 2,
                attr: to_file_attr(&info),
                entry_ttl: ttl,
                attr_ttl: ttl,
            }));
        }

        let children: Vec<InodeInfo> = self.vfs.readdir(parent).unwrap_or_default();
        for (i, child) in children.iter().enumerate() {
            entries.push(Ok(DirectoryEntryPlus {
                inode: child.ino,
                generation: 0,
                kind: to_fuse_type(child.kind),
                name: child.name.clone().into(),
                offset: 3 + i as i64,
                attr: to_file_attr(child),
                entry_ttl: ttl,
                attr_ttl: ttl,
            }));
        }

        let start = offset as usize;
        Ok(ReplyDirectoryPlus {
            entries: stream::iter(entries.into_iter().skip(start)),
        })
    }

    pub async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
    ) -> Result<ReplyEntry> {
        let name = name.to_string_lossy().to_string();
        let mode = (mode & 0o7777) as u16;

        let ino = self
            .vfs
            .mkdir(parent, name.clone(), mode)
            .await
            .map_err(|e| {
                tracing::warn!("mkdir failed: {e}");
                libc::EIO
            })?;

        let info = self.vfs.getattr(ino).map_err(|_| libc::EIO)?;
        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: to_file_attr(&info),
            generation: 0,
        })
    }

    pub async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        let name = name.to_string_lossy().to_string();
        self.vfs
            .unlink(parent, name)
            .await
            .map_err(|_| libc::EIO.into())
    }

    pub async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        let name = name.to_string_lossy().to_string();
        self.vfs
            .rmdir(parent, name)
            .await
            .map_err(|_| libc::EIO.into())
    }
}