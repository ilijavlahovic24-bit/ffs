use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use common::error::FfsError;
use common::types::BlockId;

const BINCODE: bincode::config::Configuration = bincode::config::standard();

#[derive(Debug, Serialize, Deserialize)]
struct AllocatorState {
    free_blocks: BTreeSet<BlockId>,
    next_block: u64,
}
pub struct Allocator {
    free_blocks: Mutex<BTreeSet<BlockId>>,
    next_block: AtomicU64,
}

impl Allocator {
    pub fn new() -> Self {
        Self {
            free_blocks: Mutex::new(BTreeSet::new()),
            next_block: AtomicU64::new(0),
        }
    }

    /// Uzmi blok: prvo iz `free_blocks`, inače inkrementiraj `next_block`.
    pub fn alloc(&self) -> BlockId {
        let mut free = self.free_blocks.lock().unwrap();
        if let Some(&id) = free.iter().next() {
            free.remove(&id);
            id
        } else {
            drop(free);
            self.next_block.fetch_add(1, Ordering::SeqCst)
        }
    }

    /// Vrati blok u `free_blocks`. Ako je `id` iznad trenutnog `next_block`,
    /// ignoriši ga — ne želimo da alokator „vidi" blok koji nikad nije bio
    /// dodeljen.
    pub fn free(&self, id: BlockId) {
        let next = self.next_block.load(Ordering::SeqCst);
        if id >= next {
            tracing::warn!(id, next, "allocator: free of unallocated block, ignored");
            return;
        }
        self.free_blocks.lock().unwrap().insert(id);
    }

    /// Serijalizuj stanje. Piše u temp fajl, fsync, pa atomični rename.
    pub async fn persist(&self, path: &Path) -> Result<(), FfsError> {
        let state = AllocatorState {
            free_blocks: self.free_blocks.lock().unwrap().clone(),
            next_block: self.next_block.load(Ordering::SeqCst),
        };

        let bytes = bincode::serde::encode_to_vec(&state, BINCODE)
            .map_err(|e| FfsError::StorageError(format!("allocator encode: {e}")))?;

        let tmp = path.with_extension("tmp");
        tokio::fs::write(&tmp, &bytes).await?;

        // fsync tmp fajla pre rename-a (bez ovoga rename može da pretekne
        // stvarne podatke na disku i posle crash-a dobiješ prazan fajl)
        {
            let f = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&tmp)
                .await?;
            f.sync_all().await?;
        }

        tokio::fs::rename(&tmp, path).await?;
        Ok(())
    }

    /// Učitaj stanje. Ako fajl ne postoji, vraća prazan alokator.
    pub async fn load(path: &Path) -> Result<Self, FfsError> {
        let bytes = match tokio::fs::read(path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::new()),
            Err(e) => return Err(e.into()),
        };

        let (state, _): (AllocatorState, usize) =
            bincode::serde::decode_from_slice(&bytes, BINCODE)
                .map_err(|e| FfsError::Corruption(format!("allocator decode: {e}")))?;

        Ok(Self {
            free_blocks: Mutex::new(state.free_blocks),
            next_block: AtomicU64::new(state.next_block),
        })
    }
}

impl Default for Allocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_increments_when_no_free() {
        let a = Allocator::new();
        assert_eq!(a.alloc(), 0);
        assert_eq!(a.alloc(), 1);
        assert_eq!(a.alloc(), 2);
    }

    #[test]
    fn free_is_reused_before_allocating_new() {
        let a = Allocator::new();
        let x = a.alloc();
        let y = a.alloc();
        a.free(x);
        assert_eq!(a.alloc(), x);
        // sledeći alloc ide na novi, ne na y
        assert_eq!(a.alloc(), 2);
        let _ = y;
    }

    #[test]
    fn free_below_high_water_mark_only() {
        let a = Allocator::new();
        a.free(999);
        // 999 nije alociran, treba biti ignorisan
        assert_eq!(a.alloc(), 0);
    }

    #[tokio::test]
    async fn persist_and_load_roundtrip() {
        let dir = tempdir();
        let path = dir.join("alloc.bin");
        let a = Allocator::new();
        a.alloc();
        a.alloc();
        a.free(0);
        a.persist(&path).await.unwrap();

        let b = Allocator::load(&path).await.unwrap();
        // 0 je u free setu, treba ga vratiti
        assert_eq!(b.alloc(), 0);
        // next_block je 2
        assert_eq!(b.alloc(), 2);

        std::fs::remove_dir_all(&dir).ok();
    }

    fn tempdir() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "ferumfs-alloc-test-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }
}