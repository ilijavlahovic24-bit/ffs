use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use common::error::FfsError;
use common::types::FileType;
use common::types::InodeInfo;
use storage::recovery::ApplyWalEntry;
use storage::wal::{WalEntry, WalOperation};

pub struct InodeManager {
    next_inode: AtomicU64,
    inodes: DashMap<u64, InodeInfo>,
    name_to_inode: DashMap<(u64, String), u64>,
}

impl InodeManager {
    pub async fn new() -> Self {
        let manager = Self {
            next_inode: AtomicU64::new(2),//if it is 1 it would be forever same node
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

    pub fn add_inode(&self, parent: u64, name: String, info: InodeInfo) -> Result<(), FfsError> {
        let ino = info.ino;
        if self.name_to_inode.contains_key(&(parent, name.clone())) {
            return Err(FfsError::AlreadyExists(name));
        }
        self.inodes.insert(ino, info);
        self.name_to_inode.insert((parent, name), ino);
        Ok(())
    }

    pub fn remove_inode(&self, parent: u64, name: &str) -> Result<(), FfsError>{
        if let Some((_, ino)) = self.name_to_inode.remove(&(parent, name.to_string())) {
            self.name_to_inode.retain(|(p, _), _| *p != ino);
            self.inodes.remove(&ino);

        }
        Ok(())
    }
    pub fn children(&self, parent: u64) -> Vec<InodeInfo> {
        self.name_to_inode
            .iter()
            .filter(|e| e.key().0 == parent)
            .filter_map(|e| self.inodes.get(e.value()).map(|i| i.clone()))
            .collect()
    }
}

impl ApplyWalEntry for InodeManager {
    fn apply(&mut self, entry: WalEntry) -> Result<(), FfsError> {
        match entry.operation {
            WalOperation::Create { inode_id, parent_id, name, kind, mode } => {
                self.add_inode(parent_id, name.clone(), InodeInfo {
                    ino: inode_id, parent: parent_id, name, kind, size: 0, mode,
                })?;
            }
            WalOperation::Mkdir { inode_id, parent_id, name, mode } => {
                self.add_inode(parent_id, name.clone(), InodeInfo {
                    ino: inode_id, parent: parent_id, name,
                    kind: FileType::Directory, size: 0, mode,
                })?;
            }
            WalOperation::Unlink { parent_id, name } |
            WalOperation::Rmdir  { parent_id, name } => {
                self.remove_inode(parent_id, &name)?;
            }
            WalOperation::Rename { old_parent, old_name, new_parent, new_name } => {
                // TODO: implementirati kad budeš imao rename u handlerima
            }
            WalOperation::WriteBlob { .. } => { /* ne menja namespace */ }
        }
        Ok(())
    }
}