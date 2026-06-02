use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use fuse3::{FileType, Result};

#[derive(Clone)]
pub struct InodeInfo {
    pub ino: u64,
    pub parent: u64,
    pub name: String,
    pub kind: FileType,
    pub size: u64,
    pub mode: u16,
}

pub struct InodeManager {
    next_inode: AtomicU64,
    inodes: DashMap<u64, InodeInfo>,
    name_to_inode: DashMap<(u64, String), u64>,
}

impl InodeManager {
    pub async fn new() -> Self {
        let manager = Self {
            next_inode: AtomicU64::new(1),
            inodes: DashMap::new(),
            name_to_inode: DashMap::new(),
        };

        // Create root inode
        let root = InodeInfo {
            ino: 1,
            parent: 1,
            name: "".to_string(),
            kind: FileType::Directory,
            size: 0,
            mode: 0o755,
        };
        manager.inodes.insert(1, root);

        manager
    }

    pub fn alloc_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_inode(&self, ino: u64) -> Option<InodeInfo> {
        self.inodes.get(&ino).map(|info| info.clone())
    }

    pub fn lookup(&self, parent: u64, name: &str) -> Option<u64> {
        self.name_to_inode.get(&(parent, name.to_string())).map(|ino| *ino)
    }

    pub fn add_inode(&self, parent: u64, name: String, info: InodeInfo) -> Result<()> {
        let ino = info.ino;
        self.inodes.insert(ino, info);
        self.name_to_inode.insert((parent, name), ino);
        Ok(())
    }

    pub fn remove_inode(&self, parent: u64, name: &str) -> Result<()> {
        if let Some((_, ino)) = self.name_to_inode.remove(&(parent, name.to_string())) {
            self.inodes.remove(&ino);
        }
        Ok(())
    }
}