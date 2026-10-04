use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use fuse3::raw::prelude::*;
use fuse3::raw::reply::{ReplyCreated, ReplyData, ReplyOpen, ReplyWrite};
use fuse3::Result;

use common::error::FfsError;
use vfs::VfsLayer;

use crate::handles::HandleManager;
use crate::helper::to_file_attr;

pub struct FileHandler {
    vfs: Arc<VfsLayer>,
    handle_manager: Arc<HandleManager>,
}

impl FileHandler {
    pub fn new(vfs: Arc<VfsLayer>, handle_manager: Arc<HandleManager>) -> Self {
        Self {
            vfs,
            handle_manager,
        }
    }

    fn map_write_err(e: FfsError) -> i32 {
        match e {
            FfsError::NotLeader => libc::EROFS,
            FfsError::NotFound(_) => libc::ENOENT,
            FfsError::AlreadyExists(_) => libc::EEXIST,
            FfsError::StorageError(msg) if msg.contains("is a directory") => libc::EISDIR,
            FfsError::StorageError(msg) if msg.contains("not a directory") => libc::ENOTDIR,
            _ => libc::EIO,
        }
    }

    pub async fn open(
        &self,
        _req: Request,
        inode: u64,
        flags: u32,
    ) -> Result<ReplyOpen> {
        if self.vfs.getattr(inode).is_err() {
            return Err(libc::ENOENT.into());
        }
        let fh = self.handle_manager.alloc_handle(inode);
        Ok(ReplyOpen { fh, flags })
    }

    pub async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let bytes = self
            .vfs
            .read(inode, offset, size)
            .await
            .map_err(|e| {
                tracing::warn!("read failed: {e}");
                match e {
                    FfsError::NotFound(_) => libc::ENOENT,
                    _ => libc::EIO,
                }
            })?;
        Ok(ReplyData {
            data: Bytes::from(bytes),
        })
    }

    pub async fn write(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _writeflags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        let written = self
            .vfs
            .write(inode, offset, data)
            .await
            .map_err(|e| {
                tracing::warn!("write failed: {e}");
                Self::map_write_err(e)
            })?;
        Ok(ReplyWrite { written })
    }

    pub async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated> {
        let name = name.to_string_lossy().to_string();
        let mode = (mode & 0o7777) as u16;

        let ino = self
            .vfs
            .create(parent, name, mode)
            .await
            .map_err(|e| {
                tracing::warn!("create failed: {e}");
                Self::map_write_err(e)
            })?;

        let info = self.vfs.getattr(ino).map_err(|_| libc::EIO)?;
        let fh = self.handle_manager.alloc_handle(ino);

        Ok(ReplyCreated {
            ttl: Duration::from_secs(1),
            attr: to_file_attr(&info),
            generation: 0,
            fh,
            flags,
        })
    }

    pub async fn fsync(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _datasync: bool,
    ) -> Result<()> {
        // Data is fsynced at write time by DataPath::write_blob.
        Ok(())
    }

    pub async fn flush(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
    ) -> Result<()> {
        Ok(())
    }
}