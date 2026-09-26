use std::sync::Arc;
use fuse3::raw::prelude::*;
use fuse3::Result;
use fuse3::raw::reply::{ReplyEntry, ReplyDirectory};
use futures_util::stream::{self, Stream};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};
use common::types::{FileType, InodeInfo};
use storage::wal::{MetadataWal, WalOperation};
use crate::convert::to_fuse_type;
use crate::helper::to_file_attr;
use crate::inode::InodeManager;

pub struct DirHandler {
    inode_manager: Arc<InodeManager>,
    wal: Arc<MetadataWal>,
}

impl DirHandler {
    pub fn new(inode_manager: Arc<InodeManager>, wal: Arc<MetadataWal>) -> Self {
        Self { inode_manager, wal }
    }

    pub async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
        let name_str = name.to_string_lossy();

        if let Some(ino) = self.inode_manager.lookup(parent, &name_str) {
            if let Some(info) = self.inode_manager.get_inode(ino) {
                return Ok(ReplyEntry {
                    ttl: Duration::from_secs(1),
                    attr: to_file_attr(&info),
                    generation: 0,
                });
            }
        }

        Err(libc::ENOENT.into())
    }

    pub async fn readdir(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<impl Stream<Item = Result<DirectoryEntry>> + Send + '_>> {
        let parent = self.get_parent(inode).await;

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

        for (i, child) in self.inode_manager.children(inode).iter().enumerate() {
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

    async fn get_parent(&self, inode: u64) -> u64 {
        self.inode_manager
            .get_inode(inode)
            .map(|i| i.parent)
            .unwrap_or(1)
    }

    pub async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
    ) -> Result<ReplyEntry> {
        let name = name.to_string_lossy().to_string();

        if self.inode_manager.lookup(parent, &name).is_some() {
            return Err(libc::EEXIST.into());
        }

        let ino = self.inode_manager.alloc_inode();
        let mode = (mode & 0o7777) as u16;

        self.wal
            .append(WalOperation::Mkdir {
                inode_id: ino,
                parent_id: parent,
                name: name.clone(),
                mode,
            })
            .await
            .map_err(|_| libc::EIO)?;

        let info = InodeInfo {
            ino,
            parent,
            name: name.clone(),
            kind: FileType::Directory,
            size: 0,
            mode,
        };
        self.inode_manager
            .add_inode(parent, name, info.clone())
            .map_err(|_| libc::EIO)?;

        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: to_file_attr(&info),
            generation: 0,
        })
    }

    pub async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        let name_str = name.to_string_lossy().to_string();

        let info = self
            .inode_manager
            .lookup(parent, &name_str)
            .and_then(|ino| self.inode_manager.get_inode(ino))
            .ok_or(libc::ENOENT)?;

        if info.kind == FileType::Directory {
            return Err(libc::EISDIR.into());
        }

        self.wal
            .append(WalOperation::Unlink {
                parent_id: parent,
                name: name_str.clone(),
            })
            .await
            .map_err(|_| libc::EIO)?;

        self.inode_manager
            .remove_inode(parent, &name_str)
            .map_err(|_| libc::EIO)?;

        Ok(())
    }

    pub async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> Result<()> {
        let name_str = name.to_string_lossy().to_string();

        let info = self
            .inode_manager
            .lookup(parent, &name_str)
            .and_then(|ino| self.inode_manager.get_inode(ino))
            .ok_or(libc::ENOENT)?;

        if info.kind != FileType::Directory {
            return Err(libc::ENOTDIR.into());
        }

        if !self.inode_manager.children(info.ino).is_empty() {
            return Err(libc::ENOTEMPTY.into());
        }

        self.wal
            .append(WalOperation::Rmdir {
                parent_id: parent,
                name: name_str.clone(),
            })
            .await
            .map_err(|_| libc::EIO)?;

        self.inode_manager
            .remove_inode(parent, &name_str)
            .map_err(|_| libc::EIO)?;

        Ok(())
    }
}