use std::ffi::OsStr;
use std::sync::Arc;

use fuse3::raw::prelude::*;
use fuse3::raw::Filesystem;
use fuse3::raw::reply::*;
use futures_util::Stream;

use vfs::VfsLayer;

use crate::attr::AttrHandler;
use crate::dir::DirHandler;
use crate::file::FileHandler;
use crate::handles::HandleManager;

pub mod attr;
pub mod convert;
pub mod dir;
pub mod file;
pub mod handles;
pub mod helper;
pub mod mount;

pub struct DistributedFUSE {
    handle_manager: Arc<HandleManager>,
    attr_handler: Arc<AttrHandler>,
    dir_handler: Arc<DirHandler>,
    file_handler: Arc<FileHandler>,
}

impl DistributedFUSE {
    pub fn new(vfs: Arc<VfsLayer>) -> Self {
        let handle_manager = Arc::new(HandleManager::new());
        Self {
            handle_manager: handle_manager.clone(),
            attr_handler: Arc::new(AttrHandler::new(vfs.clone())),
            dir_handler: Arc::new(DirHandler::new(vfs.clone())),
            file_handler: Arc::new(FileHandler::new(vfs, handle_manager)),
        }
    }
}

impl Filesystem for DistributedFUSE {
    async fn init(&self, req: Request) -> fuse3::Result<ReplyInit> {
        self.attr_handler.init(req).await
    }

    async fn destroy(&self, req: Request) {
        self.attr_handler.destroy(req).await
    }

    async fn lookup(&self, req: Request, parent: u64, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        self.dir_handler.lookup(req, parent, name).await
    }

    async fn getattr(
        &self,
        req: Request,
        inode: u64,
        fh: Option<u64>,
        flags: u32,
    ) -> fuse3::Result<ReplyAttr> {
        self.attr_handler.getattr(req, inode, fh, flags).await
    }

    async fn open(&self, req: Request, inode: u64, flags: u32) -> fuse3::Result<ReplyOpen> {
        self.file_handler.open(req, inode, flags).await
    }

    async fn read(
        &self,
        req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> fuse3::Result<ReplyData> {
        self.file_handler.read(req, inode, fh, offset, size).await
    }

    async fn write(
        &self,
        req: Request,
        inode: u64,
        fh: u64,
        offset: u64,
        data: &[u8],
        writeflags: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyWrite> {
        self.file_handler
            .write(req, inode, fh, offset, data, writeflags, flags)
            .await
    }

    async fn readdir(
        &self,
        req: Request,
        inode: u64,
        fh: u64,
        offset: i64,
    ) -> fuse3::Result<
        ReplyDirectory<impl Stream<Item = fuse3::Result<DirectoryEntry>> + Send + '_>,
    > {
        self.dir_handler.readdir(req, inode, fh, offset).await
    }

    async fn readdirplus(
        &self,
        req: Request,
        parent: u64,
        fh: u64,
        offset: u64,
        lock_owner: u64,
    ) -> fuse3::Result<
        ReplyDirectoryPlus<impl Stream<Item = fuse3::Result<DirectoryEntryPlus>> + Send + '_>,
    > {
        self.dir_handler
            .readdirplus(req, parent, fh, offset, lock_owner)
            .await
    }

    async fn mkdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        self.dir_handler.mkdir(req, parent, name, mode & !umask).await
    }

    async fn create(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        self.file_handler.create(req, parent, name, mode, flags).await
    }

    async fn unlink(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<()> {
        self.dir_handler.unlink(req, parent, name).await
    }

    async fn rmdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<()> {
        self.dir_handler.rmdir(req, parent, name).await
    }
    async fn setattr(
        &self,
        req: Request,
        inode: u64,
        fh: Option<u64>,
        set_attr: SetAttr,
    ) -> fuse3::Result<ReplyAttr> {
        self.attr_handler.setattr(req, inode, fh, set_attr).await
    }

    async fn statfs(
        &self,
        req: Request,
        inode: u64,
    ) -> fuse3::Result<ReplyStatFs> {
        self.attr_handler.statfs(req, inode).await
    }

    async fn rename(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> fuse3::Result<()> {
        self.dir_handler
            .rename(req, parent, name, new_parent, new_name)
            .await
    }

    async fn fsync(
        &self,
        req: Request,
        inode: u64,
        fh: u64,
        datasync: bool,
    ) -> fuse3::Result<()> {
        self.file_handler.fsync(req, inode, fh, datasync).await
    }

    async fn access(
        &self,
        _req: Request,
        _inode: u64,
        _mask: u32,
    ) -> fuse3::Result<()> {
        Ok(())
    }

    async fn flush(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _lock_owner: u64,
    ) -> fuse3::Result<()> {
        Ok(())
    }
    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> fuse3::Result<()> {
        self.handle_manager.release_handle(fh);
        Ok(())
    }

}