use dashmap::DashMap;

use common::error::FfsError;
use common::types::InodeId;

/// In-memory extended attribute store.
///
/// Not replicated through Raft yet — kept local per node. When xattr
/// writes need to survive failover, route them through `MetaStore`.
#[derive(Default)]
pub struct XattrStore {
    attrs: DashMap<InodeId, DashMap<String, Vec<u8>>>,
}

impl XattrStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, inode: InodeId, key: String, value: Vec<u8>) {
        self.attrs
            .entry(inode)
            .or_default()
            .insert(key, value);
    }

    pub fn get(&self, inode: InodeId, key: &str) -> Option<Vec<u8>> {
        self.attrs.get(&inode)?.get(key).map(|v| v.clone())
    }

    pub fn remove(&self, inode: InodeId, key: &str) -> Result<(), FfsError> {
        let Some(map) = self.attrs.get(&inode) else {
            return Err(FfsError::NotFound(inode));
        };
        map.remove(key);
        Ok(())
    }

    pub fn list(&self, inode: InodeId) -> Vec<String> {
        self.attrs
            .get(&inode)
            .map(|m| m.iter().map(|e| e.key().clone()).collect())
            .unwrap_or_default()
    }
}