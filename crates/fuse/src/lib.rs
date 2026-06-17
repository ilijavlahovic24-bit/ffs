use std::ffi::OsStr;
use std::sync::Arc;
use fuse3::raw::Request;
use fuse3::raw::Filesystem;
use crate::attr::AttrHandler;
use crate::dir::DirHandler;
use crate::file::FileHandler;
use crate::handles::HandleManager;
use crate::inode::InodeManager;
use fuse3::raw::reply::*;
use futures_util::Stream;

mod attr;
mod dir;
mod file;
mod handles;
mod inode;
mod mount;

pub struct DistributedFUSE {
    inode_manager: Arc<InodeManager>,
    handle_manager: Arc<HandleManager>,
    attr_handler: Arc<AttrHandler>,
    dir_handler: Arc<DirHandler>,
    file_handler: Arc<FileHandler>,
}


impl DistributedFUSE {
    pub async fn new() -> Self {
        let inode_manager = Arc::new(InodeManager::new().await);
        let handle_manager = Arc::new(HandleManager::new());

        Self {
            inode_manager: inode_manager.clone(),
            handle_manager: handle_manager.clone(),
            attr_handler: Arc::new(AttrHandler::new(inode_manager.clone())),
            dir_handler: Arc::new(DirHandler::new(inode_manager.clone())),
            file_handler: Arc::new(FileHandler::new(inode_manager.clone(), handle_manager)),
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

    async fn getattr(&self, req: Request, inode: u64, fh: Option<u64>, flags: u32) -> fuse3::Result<ReplyAttr> {
        self.attr_handler.getattr(req, inode, fh, flags).await
    }

    async fn open(&self, req: Request, inode: u64, flags: u32) -> fuse3::Result<ReplyOpen> {
        self.file_handler.open(req, inode, flags).await
    }

    async fn read(&self, req: Request, inode: u64, fh: u64, offset: u64, size: u32) -> fuse3::Result<ReplyData> {
        self.file_handler.read(req, inode, fh, offset, size).await
    }

    async fn write(&self, req: Request, inode: u64, fh: u64, offset: u64, data: &[u8],writeflags:u32, flags: u32) -> fuse3::Result<ReplyWrite> {
        self.file_handler.write(req, inode, fh, offset, data, writeflags,flags).await
    }

    async fn readdir(&self, req: Request, inode: u64, fh: u64, offset: i64) -> fuse3::Result<ReplyDirectory<impl Stream<Item = fuse3::Result<DirectoryEntry>> + Send + '_>> {
        self.dir_handler.readdir(req, inode, fh, offset).await
    }

    async fn mkdir(&self, req: Request, parent: u64, name: &OsStr, mode: u32, umask:u32) -> fuse3::Result<ReplyEntry> {
        self.dir_handler.mkdir(req, parent, name, mode).await
    }

    async fn create(&self, req: Request, parent: u64, name: &OsStr, mode: u32, flags: u32) -> fuse3::Result<ReplyCreated> {
        self.file_handler.create(req, parent, name, mode, flags).await
    }

    async fn unlink(&self, req: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        self.dir_handler.unlink(req, parent, name).await
    }

    async fn rmdir(&self, req: Request, parent: u64, name: &OsStr) -> fuse3::Result<()> {
        self.dir_handler.rmdir(req, parent, name).await
    }
}