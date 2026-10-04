use std::sync::Arc;

use common::error::FfsError;
use common::types::{FileType, InodeId};
use network::RaftForwarder;
use storage::wal::WalOperation;

use crate::inode::InodeManager;

const BINCODE: bincode::config::Configuration = bincode::config::standard();

pub struct MetaStore {
    inodes: Arc<InodeManager>,
    forwarder: Arc<RaftForwarder>,
}

impl MetaStore {
    pub fn new(inodes: Arc<InodeManager>, forwarder: Arc<RaftForwarder>) -> Self {
        Self { inodes, forwarder }
    }

    pub async fn is_leader(&self) -> bool {
        self.forwarder.is_leader().await
    }

    pub async fn create(
        &self,
        parent: InodeId,
        name: String,
        kind: FileType,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        if self.forwarder.is_leader().await {
            // Leader alocira inode lokalno i šalje fiksnu komandu.
            let ino = self.inodes.alloc_inode();
            let op = WalOperation::Create {
                inode_id: ino,
                parent_id: parent,
                name,
                kind,
                mode,
            };
            self.propose_op(op).await?;
            Ok(ino)
        } else {
            // Follower: placeholder id = 0, lider će ga patchevati.
            let op = WalOperation::Create {
                inode_id: 0,
                parent_id: parent,
                name,
                kind,
                mode,
            };
            let bytes = bincode::serde::encode_to_vec(&op, BINCODE)
                .map_err(|e| FfsError::StorageError(format!("encode: {e}")))?;
            let ino = self
                .forwarder
                .propose(bytes, true)
                .await?
                .ok_or_else(|| FfsError::ConsensusError("missing inode id".into()))?;
            Ok(ino)
        }
    }

    pub async fn mkdir(
        &self,
        parent: InodeId,
        name: String,
        mode: u16,
    ) -> Result<InodeId, FfsError> {
        if self.forwarder.is_leader().await {
            let ino = self.inodes.alloc_inode();
            let op = WalOperation::Mkdir {
                inode_id: ino,
                parent_id: parent,
                name,
                mode,
            };
            self.propose_op(op).await?;
            Ok(ino)
        } else {
            let op = WalOperation::Mkdir {
                inode_id: 0,
                parent_id: parent,
                name,
                mode,
            };
            let bytes = bincode::serde::encode_to_vec(&op, BINCODE)
                .map_err(|e| FfsError::StorageError(format!("encode: {e}")))?;
            let ino = self
                .forwarder
                .propose(bytes, true)
                .await?
                .ok_or_else(|| FfsError::ConsensusError("missing inode id".into()))?;
            Ok(ino)
        }
    }

    pub async fn unlink(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        self.propose_op(WalOperation::Unlink { parent_id: parent, name }).await
    }

    pub async fn rmdir(&self, parent: InodeId, name: String) -> Result<(), FfsError> {
        self.propose_op(WalOperation::Rmdir { parent_id: parent, name }).await
    }

    pub async fn rename(
        &self,
        old_parent: InodeId,
        old_name: String,
        new_parent: InodeId,
        new_name: String,
    ) -> Result<(), FfsError> {
        self.propose_op(WalOperation::Rename {
            old_parent, old_name, new_parent, new_name,
        })
            .await
    }

    pub async fn truncate(&self, ino: InodeId, size: u64) -> Result<(), FfsError> {
        self.propose_op(WalOperation::Truncate { inode_id: ino, size }).await
    }

    pub async fn chmod(&self, ino: InodeId, mode: u16) -> Result<(), FfsError> {
        self.propose_op(WalOperation::Chmod { inode_id: ino, mode }).await
    }

    async fn propose_op(&self, op: WalOperation) -> Result<(), FfsError> {
        let bytes = bincode::serde::encode_to_vec(&op, BINCODE)
            .map_err(|e| FfsError::StorageError(format!("encode: {e}")))?;
        self.forwarder.propose(bytes, false).await?;
        Ok(())
    }
}