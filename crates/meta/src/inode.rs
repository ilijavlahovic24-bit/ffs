use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use common::error::FfsError;
use common::state_machine::StateMachine;
use common::types::{FileType, InodeId, InodeInfo};
use storage::recovery::ApplyWalEntry;
use storage::wal::{WalEntry, WalOperation};

const BINCODE: bincode::config::Configuration = bincode::config::standard();

pub struct InodeManager {
    next_inode: AtomicU64,
    inodes: DashMap<InodeId, InodeInfo>,
    name_to_inode: DashMap<(InodeId, String), InodeId>,
}

impl InodeManager {
    pub async fn new() -> Self {
        let manager = Self {
            next_inode: AtomicU64::new(2),
            inodes: DashMap::new(),
            name_to_inode: DashMap::new(),
        };
        manager.inodes.insert(
            1,
            InodeInfo {
                ino: 1,
                parent: 1,
                name: String::new(),
                kind: FileType::Directory,
                size: 0,
                mode: 0o755,
            },
        );
        manager
    }

    pub fn alloc_inode(&self) -> u64 {
        self.next_inode.fetch_add(1, Ordering::SeqCst)
    }

    pub fn get_inode(&self, ino: InodeId) -> Option<InodeInfo> {
        self.inodes.get(&ino).map(|i| i.clone())
    }

    pub fn lookup(&self, parent: InodeId, name: &str) -> Option<InodeId> {
        self.name_to_inode
            .get(&(parent, name.to_string()))
            .map(|i| *i)
    }

    pub fn add_inode(
        &self,
        parent: InodeId,
        name: String,
        info: InodeInfo,
    ) -> Result<(), FfsError> {
        if self.name_to_inode.contains_key(&(parent, name.clone())) {
            return Err(FfsError::AlreadyExists(name));
        }
        let ino = info.ino;
        self.inodes.insert(ino, info);
        self.name_to_inode.insert((parent, name), ino);
        Ok(())
    }

    pub fn remove_inode(&self, parent: InodeId, name: &str) -> Result<(), FfsError> {
        let Some((_, ino)) = self.name_to_inode.remove(&(parent, name.to_string())) else {
            return Err(FfsError::NotFound(parent));
        };
        self.name_to_inode.retain(|(p, _), _| *p != ino);
        self.inodes.remove(&ino);
        Ok(())
    }

    pub fn children(&self, parent: InodeId) -> Vec<InodeInfo> {
        self.name_to_inode
            .iter()
            .filter(|e| e.key().0 == parent)
            .filter_map(|e| self.inodes.get(e.value()).map(|i| i.clone()))
            .collect()
    }
}

impl StateMachine for InodeManager {
    fn apply(&self, command: &[u8]) -> Result<(), FfsError> {
        if command.is_empty() {
            return Ok(()); // Raft no-op
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
                    Ok(()) | Err(FfsError::AlreadyExists(_)) => Ok(()),
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
                    Ok(()) | Err(FfsError::AlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Unlink { parent_id, name }
            | WalOperation::Rmdir { parent_id, name } => {
                match self.remove_inode(parent_id, &name) {
                    Ok(()) | Err(FfsError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Rename { .. } | WalOperation::WriteBlob { .. } => Ok(()),
        }
    }

    fn alloc_inode_id(&self) -> u64 {
        self.alloc_inode()
    }
}

impl ApplyWalEntry for InodeManager {
    fn apply(&mut self, entry: WalEntry) -> Result<(), FfsError> {
        // Reuse StateMachine::apply by re-encoding would be wasteful;
        // implement the same match inline.
        match entry.operation {
            WalOperation::Create { inode_id, parent_id, name, kind, mode } => {
                let info = InodeInfo {
                    ino: inode_id, parent: parent_id, name: name.clone(), kind, size: 0, mode,
                };
                match self.add_inode(parent_id, name, info) {
                    Ok(()) | Err(FfsError::AlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Mkdir { inode_id, parent_id, name, mode } => {
                let info = InodeInfo {
                    ino: inode_id, parent: parent_id, name: name.clone(),
                    kind: FileType::Directory, size: 0, mode,
                };
                match self.add_inode(parent_id, name, info) {
                    Ok(()) | Err(FfsError::AlreadyExists(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Unlink { parent_id, name }
            | WalOperation::Rmdir { parent_id, name } => {
                match self.remove_inode(parent_id, &name) {
                    Ok(()) | Err(FfsError::NotFound(_)) => Ok(()),
                    Err(e) => Err(e),
                }
            }
            WalOperation::Rename { .. } | WalOperation::WriteBlob { .. } => Ok(()),
        }
    }
}