use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct HandleManager {
    next_handle: AtomicU64,
    handles: DashMap<u64, u64>, // handle -> inode
}

impl HandleManager {
    pub fn new() -> Self {
        Self {
            next_handle: AtomicU64::new(1),
            handles: DashMap::new(),
        }
    }

    pub fn alloc_handle(&self, inode: u64) -> u64 {
        let handle = self.next_handle.fetch_add(1, Ordering::SeqCst);
        self.handles.insert(handle, inode);
        handle
    }

    pub fn get_inode(&self, handle: u64) -> Option<u64> {
        self.handles.get(&handle).map(|ino| *ino)
    }

    pub fn release_handle(&self, handle: u64) {
        self.handles.remove(&handle);
    }
}