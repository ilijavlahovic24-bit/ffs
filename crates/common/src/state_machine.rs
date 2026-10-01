use crate::error::FfsError;

// Trait implemented by anything that can act as a Raft state machine.
// Called by `consensus::RaftNode` when an entry is committed. Must be
// idempotent — Raft will reapply entries after restart.
// Lives in `common` so that `fuse::InodeManager` can implement it
// without `fuse` depending on `consensus`.
pub trait StateMachine: Send + Sync {
    // Apply a committed command. The bytes are opaque to Raft; the
    // implementation decodes them however it likes.
    fn apply(&self, command: &[u8]) -> Result<(), FfsError>;

    // Allocate a fresh inode id. Only called by the leader before
    // proposing a command.
    fn alloc_inode_id(&self) -> u64;
}