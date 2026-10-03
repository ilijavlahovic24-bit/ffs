use crate::error::FfsError;

/// Trait implemented by anything that can act as a Raft state machine.
///
/// Called by `consensus::RaftNode` when an entry is committed. Must be
/// idempotent — Raft will reapply entries after restart.
///
/// Lives in `common` so that `meta::InodeManager` can implement it
/// without `meta` depending on `consensus`.
pub trait StateMachine: Send + Sync {
    /// Apply a committed command. The bytes are opaque to Raft.
    fn apply(&self, command: &[u8]) -> Result<(), FfsError>;

    /// Allocate a fresh inode id. Called by the leader before proposing.
    /// Must be safe to call from multiple threads.
    fn alloc_inode_id(&self) -> u64;
}