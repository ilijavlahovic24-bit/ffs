use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;

use fuse3::raw::prelude::*;
use fuse3::raw::Filesystem;
use fuse3::raw::reply::*;
use futures_util::Stream;

use common::error::FfsError;
use storage::wal::{DataPath, MetadataWal};

use crate::attr::AttrHandler;
use crate::dir::DirHandler;
use crate::file::FileHandler;
use crate::handles::HandleManager;
use crate::inode::InodeManager;

pub mod attr;
pub mod convert;
pub mod dir;
pub mod file;
pub mod handles;
pub mod helper;
pub mod inode;
pub mod mount;

pub struct DistributedFUSE {
    inode_manager: Arc<InodeManager>,
    handle_manager: Arc<HandleManager>,
    attr_handler: Arc<AttrHandler>,
    dir_handler: Arc<DirHandler>,
    file_handler: Arc<FileHandler>,

    // drži se ovde da bi `DistributedFUSE` imao vlasništvo i da bi
    // handleri mogli da dele `Arc`-ove
    #[allow(dead_code)]
    wal: Arc<MetadataWal>,
    #[allow(dead_code)]
    data_path: Arc<DataPath>,
}

impl DistributedFUSE {
    pub async fn new(
        inode_manager: Arc<InodeManager>,
        wal_path: PathBuf,
        tmp_dir: PathBuf,
        data_dir: PathBuf,
    ) -> Result<Self, FfsError> {
        let handle_manager = Arc::new(HandleManager::new());
        let wal = Arc::new(MetadataWal::new(wal_path).await?);
        let data_path = Arc::new(DataPath::new(tmp_dir, data_dir, wal.clone()).await);

        let attr_handler = Arc::new(AttrHandler::new(inode_manager.clone()));
        let dir_handler = Arc::new(DirHandler::new(inode_manager.clone(), wal.clone()));
        let file_handler = Arc::new(FileHandler::new(
            inode_manager.clone(),
            handle_manager.clone(),
            wal.clone(),
            data_path.clone(),
        ));

        Ok(Self {
            inode_manager,
            handle_manager,
            attr_handler,
            dir_handler,
            file_handler,
            wal,
            data_path,
        })
    }
}

impl Filesystem for DistributedFUSE {
    async fn init(&self, req: Request) -> fuse3::Result<ReplyInit> {
        self.attr_handler.init(req).await
    }

    async fn destroy(&self, req: Request) {
        self.attr_handler.destroy(req).await
    }

    async fn lookup(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
    ) -> fuse3::Result<ReplyEntry> {
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

    async fn open(
        &self,
        req: Request,
        inode: u64,
        flags: u32,
    ) -> fuse3::Result<ReplyOpen> {
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

    async fn mkdir(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
    ) -> fuse3::Result<ReplyEntry> {
        self.dir_handler
            .mkdir(req, parent, name, mode & !umask)
            .await
    }

    async fn create(
        &self,
        req: Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> fuse3::Result<ReplyCreated> {
        self.file_handler
            .create(req, parent, name, mode, flags)   // bez `as i32`
            .await
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