use std::sync::Arc;

use tokio::sync::Mutex;

use common::error::FfsError;
use common::state_machine::StateMachine;
use common::types::{FileType, InodeId};
use consensus::RaftNode;
use storage::wal::WalOperation;

const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// Coordinates namespace operations through Raft.
///
/// Writes are proposed to the Raft log and awaited. Reads are served
/// directly from the state machine (which is shared with the Raft node).
pub struct MetaStore {
    node: Arc<Mutex<RaftNode>>,
    state_machine: Arc<dyn StateMachine>,
}

impl MetaStore {
    pub fn new(node: Arc<Mutex<RaftNode>>, state_machine: Arc<dyn StateMachine>) -> Self {
        Self { node, state_machine }
    }

    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.is_leader()
    }

    /// Create a regular file or symlink.
    pub async fn create(
        &self,
        parent: InodeId,
        name: String,
        kind: FileType,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        let inode_id = self.state_machine.alloc_inode_id();
        let op = WalOperation::Create {
            inode_id,
            parent_id: parent,
            name,
            kind,
            mode,
        };
        self.propose_and_wait(op).await?;
        Ok(inode_id)
    }

    /// Create a directory.
    pub async fn mkdir(
        &self,
        parent: InodeId,
        name: String,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        let inode_id = self.state_machine.alloc_inode_id();
        let op = WalOperation::Mkdir {
            inode_id,
            parent_id: parent,
            name,
            mode,
        };
        self.propose_and_wait(op).await?;
        Ok(inode_id)
    }

    /// Remove a file or symlink.
    pub async fn unlink(
        &self,
        parent: InodeId,
        name: String,
    ) -> Result<(), FfsError> {
        self.propose_and_wait(WalOperation::Unlink {
            parent_id: parent,
            name,
        })
            .await
    }

    /// Remove an empty directory.
    pub async fn rmdir(
        &self,
        parent: InodeId,
        name: String,
    ) -> Result<(), FfsError> {
        self.propose_and_wait(WalOperation::Rmdir {
            parent_id: parent,
            name,
        })
            .await
    }

    /// Common path: encode, propose, await commit.
    async fn propose_and_wait(&self, op: WalOperation) -> Result<(), FfsError> {
        let bytes = bincode::serde::encode_to_vec(&op, BINCODE)
            .map_err(|e| FfsError::StorageError(format!("encode wal op: {e}")))?;

        // Acquire the node lock only to register the proposal.
        let rx = {
            let mut node = self.node.lock().await;
            node.propose_with_waiter(bytes)?
        };

        // Wait for commit outside the lock.
        rx.await
            .map_err(|_| FfsError::ConsensusError("proposal channel closed".into()))?
    }
}