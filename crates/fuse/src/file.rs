use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;
use fuse3::raw::prelude::*;
use fuse3::{Result};
use fuse3::raw::reply::{ReplyAttr, ReplyInit, ReplyOpen, ReplyData, ReplyWrite, ReplyCreated};
use bytes::Bytes;
use common::types::InodeInfo;
use crate::handles::HandleManager;
use crate::inode::InodeManager;
use storage::wal::{MetadataWal, WalOperation};
use storage::wal::DataPath;
use crate::helper::to_file_attr;
use common::types::FileType;

pub struct FileHandler {
    inode_manager: Arc<InodeManager>,
    handle_manager: Arc<HandleManager>,

    wal: Arc<MetadataWal>,
    data_path: Arc<DataPath>,
}

impl FileHandler {
    pub fn new(inode_manager: Arc<InodeManager>, handle_manager: Arc<HandleManager>, wal:Arc<MetadataWal>,data_path: Arc<DataPath>) -> Self {
        Self { inode_manager, handle_manager,wal,data_path }
    }

    pub async fn open(&self, _req: Request, inode: u64, flags: u32) -> Result<ReplyOpen> {
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
        if self.inode_manager.get_inode(inode).is_none() {
            return Err(libc::ENOENT.into());
        }

        let bytes = match self.data_path.read_blob(inode).await {
            Ok(b) => b,
            Err(common::error::FfsError::NotFound(_)) => Vec::new(),
            Err(_) => return Err(libc::EIO.into()),
        };

        let start = offset as usize;
        if start >= bytes.len() {
            return Ok(ReplyData { data: Bytes::new() });
        }
        let end = (start + size as usize).min(bytes.len());
        Ok(ReplyData { data: Bytes::copy_from_slice(&bytes[start..end]) })
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
        if self.inode_manager.get_inode(inode).is_none() {
            return Err(libc::ENOENT.into());
        }

        // TODO
        if offset != 0 {
            return Err(libc::ENOSYS.into());
        }

        self.data_path.write_blob(inode, data).await
            .map_err(|_| libc::EIO)?;

        // update size (TODO:  update through InodeManager)
        Ok(ReplyWrite { written: data.len() as u32 })
    }
    pub async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated>  {
        let name = name.to_string_lossy().to_string();

        if self.inode_manager.lookup(parent, &name).is_some() {
            return Err(libc::EEXIST.into());
        }

        let ino = self.inode_manager.alloc_inode();
        let mode = (mode & 0o7777) as u16;

        self.wal
            .append(WalOperation::Create {
                inode_id: ino,
                parent_id: parent,
                name: name.clone(),
                kind: FileType::File,
                mode,
            })
            .await
            .map_err(|_| libc::EIO)?;

        let info = InodeInfo {
            ino,
            parent,
            name: name.clone(),
            kind: FileType::File,
            size: 0,
            mode,
        };
        self.inode_manager
            .add_inode(parent, name, info.clone())
            .map_err(|_| libc::EIO)?;

        let fh = self.handle_manager.alloc_handle(ino);

        Ok(ReplyCreated {
            ttl: Duration::from_secs(1),
            attr: to_file_attr(&info),
            generation: 0,
            fh,
            flags: flags as u32,
        })
    }

}