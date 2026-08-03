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
        todo!()
    }
}
impl Error for FFSError {}
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
        todo!()
    }
}
impl From<&str> for FFSError {
    fn from(err: &str) -> Self {
        todo!()
    }
}
