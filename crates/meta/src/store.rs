use std::sync::Arc;

use tokio::sync::Mutex;

use common::error::FfsError;
use common::types::{FileType, InodeId};
use consensus::RaftNode;
use storage::wal::WalOperation;

use crate::inode::InodeManager;

const BINCODE: bincode::config::Configuration = bincode::config::standard();

pub struct MetaStore {
    node: Arc<Mutex<RaftNode>>,
    inodes: Arc<InodeManager>,
}

impl MetaStore {
    pub fn new(node: Arc<Mutex<RaftNode>>, inodes: Arc<InodeManager>) -> Self {
        Self { node, inodes }
    }

    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.is_leader()
    }

    pub async fn create(
        &self,
        parent: InodeId,
        name: String,
        kind: FileType,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        let inode_id = self.inodes.alloc_inode();
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

    pub async fn mkdir(
        &self,
        parent: InodeId,
        name: String,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        let inode_id = self.inodes.alloc_inode();
        let op = WalOperation::Mkdir {
            inode_id,
            parent_id: parent,
            name,
            mode,
        };
        self.propose_and_wait(op).await?;
        Ok(inode_id)
    }

    pub async fn unlink(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        self.propose_and_wait(WalOperation::Unlink {
            parent_id: parent,
            name,
        })
            .await
    }

    pub async fn rmdir(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        self.propose_and_wait(WalOperation::Rmdir {
            parent_id: parent,
            name,
        })
            .await
    }

    async fn propose_and_wait(&self, op: WalOperation) -> Result<(), FfsError> {
        let bytes = bincode::serde::encode_to_vec(&op, BINCODE)
            .map_err(|e| FfsError::StorageError(format!("encode wal op: {e}")))?;

        let rx = {
            let mut node = self.node.lock().await;
            node.propose_with_waiter(bytes)?
        };

        rx.await
            .map_err(|_| FfsError::ConsensusError("proposal channel closed".into()))?
    }
}