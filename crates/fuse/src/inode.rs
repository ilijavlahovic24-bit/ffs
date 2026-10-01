use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use common::error::FfsError;
use common::types::FileType;
use common::types::InodeInfo;
use storage::recovery::ApplyWalEntry;
use storage::wal::{WalEntry, WalOperation};
use common::state_machine::StateMachine;


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
    pub fn next_inode_id(&self) -> u64 {
        self.next_inode.load(std::sync::atomic::Ordering::SeqCst)
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
                // TODO
            }
            WalOperation::WriteBlob { .. } => { /* ne menja namespace */ }
        }
        Ok(())
    }
}

const BINCODE: bincode::config::Configuration = bincode::config::standard();

impl StateMachine for InodeManager {
    fn apply(&self, command: &[u8]) -> Result<(), FfsError> {
        if command.is_empty() {
            // Raft no-op entry — nothing to apply.
            return Ok(());
        }

        let op: WalOperation = bincode::serde::decode_from_slice(command, BINCODE)
            .map_err(|e| FfsError::Corruption(format!("decode wal op: {e}")))?
            .0;

        match op {
            WalOperation::Create { inode_id, parent_id, name, kind, mode } => {
                let info = InodeInfo {
                    ino: inode_id,
                    parent: parent_id,
                    name: name.clone(),
                    kind,
                    size: 0,
                    mode,
                };
                match self.add_inode(parent_id, name, info) {
                    Ok(()) => Ok(()),
                    // Idempotent replay — entry was already applied.
                    Err(FfsError::AlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Mkdir { inode_id, parent_id, name, mode } => {
                let info = InodeInfo {
                    ino: inode_id,
                    parent: parent_id,
                    name: name.clone(),
                    kind: FileType::Directory,
                    size: 0,
                    mode,
                };
                match self.add_inode(parent_id, name, info) {
                    Ok(()) => Ok(()),
                    Err(FfsError::AlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Unlink { parent_id, name } |
            WalOperation::Rmdir { parent_id, name } => {
                match self.remove_inode(parent_id, &name) {
                    Ok(()) => Ok(()),
                    // Entry already applied or never existed.
                    Err(FfsError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Rename { old_parent, old_name, new_parent, new_name } => {
                // TODO: implement rename once fuse exposes it
                let _ = (old_parent, old_name, new_parent, new_name);
                Ok(())
            }
            WalOperation::WriteBlob { .. } => Ok(()),
        }
    }

    fn alloc_inode_id(&self) -> u64 {
        // InodeManager has no allocation method that returns without
        // inserting; use next_inode via a small accessor.
        self.next_inode_id()
    }
}