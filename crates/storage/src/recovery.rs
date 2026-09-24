use std::path::Path;

use common::error::FfsError;

use crate::wal::{MetadataWal, WalEntry};

/// State machine that can apply WAL entry.
///
/// Implemented by `InodeManager` from `fuse` crate (Phase 3), but also any
/// test state. It is deliberately a trait and not a specific type so that `storage' does not depend
/// from `fuse` (dependency map: `fuse` → `storage`, never vice versa).
pub trait ApplyWalEntry {
    fn apply(&mut self, entry: WalEntry) -> Result<(), FfsError>;
}

/// Read all WAL entries, sort by `sequence', apply to `state'.
///
/// Sorting is defensive: in normal operation the file is append-only so it is
/// reading order already according to `sequence'. But if there were partial enrollments
/// and recoveries in the past, it is possible that the file is not strictly sorted.
pub async fn recover(
    wal_path: &Path,
    state: &mut dyn ApplyWalEntry,
) -> Result<(), FfsError> {
    let mut entries = MetadataWal::read_all(wal_path).await?;
    entries.sort_by_key(|e| e.sequence);

    tracing::info!(count = entries.len(), "WAL recovery: applying entries");

    for entry in entries {
        state.apply(entry)?;
    }
    Ok(())
}