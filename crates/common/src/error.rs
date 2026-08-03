use std::fmt;
use std::io;
use std::error::Error;
use std::fmt::Formatter;
use crate::types::InodeId;

#[allow(unused)]
#[derive(Debug)]
enum FFSError{
    IO(io::Error),
    Corruption(String),
    NotFound(InodeId),
    AlreadyExist(String),
    ConsensusError(String),
    StorageError(String),
}
impl fmt::Display for FFSError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FFSError::IO(err) => write!(f, "IO error: {}", err),
            FFSError::Corruption(msg) => write!(f, "Corruption error: {}", msg),
            FFSError::NotFound(id) => write!(f, "Inode not found: {}", id),
            FFSError::AlreadyExist(name) => write!(f, "File already exists: {}", name),
            FFSError::ConsensusError(msg) => write!(f, "Consensus error: {}", msg),
            FFSError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}
#[derive(Debug)]
pub struct CorruptionError(pub String);

#[derive(Debug)]
pub struct ConsensusError(pub String);

#[derive(Debug)]
pub struct StorageError(pub String);

#[derive(Debug)]
pub struct AlreadyExistError(pub String);

impl From<io::Error> for FFSError {
    fn from(err: io::Error) -> Self {
        FFSError::IO(err)
    }
}
impl From<InodeId> for FFSError {
    fn from(err: InodeId) -> Self {
        FFSError::NotFound(err)
    }
}

impl From<String> for FFSError {
    fn from(err: String) -> Self {
        // Ovde možete dodati logiku ili jednostavno mapirati u Corruption
        FFSError::Corruption(err)
    }
}

impl From<&str> for FFSError {
    fn from(err: &str) -> Self {
        FFSError::Corruption(err.to_string())
    }
}

impl From<CorruptionError> for FFSError {
    fn from(err: CorruptionError) -> Self {
        FFSError::Corruption(err.0)
    }
}

impl From<ConsensusError> for FFSError {
    fn from(err: ConsensusError) -> Self {
        FFSError::ConsensusError(err.0)
    }
}

impl From<StorageError> for FFSError {
    fn from(err: StorageError) -> Self {
        FFSError::StorageError(err.0)
    }
}

impl From<AlreadyExistError> for FFSError {
    fn from(err: AlreadyExistError) -> Self {
        FFSError::AlreadyExist(err.0)
    }
}