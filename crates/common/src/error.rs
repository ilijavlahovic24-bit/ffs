use thiserror::Error;
use crate::types::InodeId;

#[derive(Debug, Error)]
pub enum FfsError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Corruption: {0}")]
    Corruption(String),
    #[error("Inode not found: {0}")]
    NotFound(InodeId),
    #[error("Already exists: {0}")]
    AlreadyExists(String),
    #[error("Consensus error: {0}")]
    ConsensusError(String),
    #[error("Storage error: {0}")]
    StorageError(String),
}