use serde::{Deserialize, Serialize};

use common::error::FfsError;
use consensus::{AppendEntries, AppendEntriesReply, OutgoingMessage, RequestVote, RequestVoteReply};

const BINCODE: bincode::config::Configuration = bincode::config::standard();

/// Replies carry `from` so the receiver knows which peer the reply
/// belongs to without tracking connection state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVote),
    RequestVoteReply { from: u64, reply: RequestVoteReply },
    AppendEntries(AppendEntries),
    AppendEntriesReply { from: u64, reply: AppendEntriesReply },
}

pub fn encode(msg: &RaftMessage) -> Result<Vec<u8>, FfsError> {
    bincode::serde::encode_to_vec(msg, BINCODE)
        .map_err(|e| FfsError::StorageError(format!("network encode: {e}")))
}

pub fn decode(bytes: &[u8]) -> Result<RaftMessage, FfsError> {
    let (msg, _) = bincode::serde::decode_from_slice(bytes, BINCODE)
        .map_err(|e| FfsError::Corruption(format!("network decode: {e}")))?;
    Ok(msg)
}

/// Convert a `RaftNode` outbox entry into a `(peer_id, wire_message)` pair.
pub fn outgoing_to_wire(msg: OutgoingMessage) -> (u64, RaftMessage) {
    match msg {
        OutgoingMessage::RequestVote { to, req } => (to, RaftMessage::RequestVote(req)),
        OutgoingMessage::AppendEntries { to, req } => (to, RaftMessage::AppendEntries(req)),
    }
}