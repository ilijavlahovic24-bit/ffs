use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::task::JoinSet;

use common::error::FfsError;
use consensus::RaftNode;

use crate::connection::TcpConnection;
use crate::message::{decode, encode, outgoing_to_wire, RaftMessage};

/// Send one request to `addr` and return the reply.
pub async fn request(
    addr: SocketAddr,
    msg: RaftMessage,
) -> Result<RaftMessage, FfsError> {
    let mut conn = TcpConnection::connect(addr).await?;
    conn.send(&encode(&msg)?).await?;
    let bytes = conn.recv().await?;
    decode(&bytes)
}

/// Send a single outgoing Raft message to its peer, wait for the reply,
/// apply the reply to the node, and return any newly-generated outbox.
pub async fn send_one(
    addr: SocketAddr,
    msg: RaftMessage,
    node: Arc<Mutex<RaftNode>>,
) -> Result<Vec<consensus::OutgoingMessage>, FfsError> {
    let reply = request(addr, msg).await?;

    let mut n = node.lock().await;
    match reply {
        RaftMessage::RequestVoteReply { from, reply } => {
            n.handle_vote_reply(from, reply);
        }
        RaftMessage::AppendEntriesReply { from, reply } => {
            n.handle_append_entries_reply(from, reply);
        }
        // A server only ever replies to requests; receiving a request
        // here means we sent to the wrong endpoint.
        _ => {}
    }
    Ok(n.take_outbox())
}

/// Drive one round of outgoing messages in parallel.
pub async fn drive_round(
    outgoing: Vec<consensus::OutgoingMessage>,
    peers: Arc<HashMap<u64, SocketAddr>>,
    node: Arc<Mutex<RaftNode>>,
) -> Vec<consensus::OutgoingMessage> {
    let mut set = JoinSet::new();

    for msg in outgoing {
        let (to, wire) = outgoing_to_wire(msg);
        let peers = peers.clone();
        let node = node.clone();
        set.spawn(async move {
            let Some(addr) = peers.get(&to).copied() else {
                tracing::warn!("no address for peer {to}");
                return Vec::new();
            };

            // Bound each RPC so a dead-but-accepting peer cannot stall
            // the driver.
            let result = tokio::time::timeout(
                Duration::from_secs(2),
                send_one(addr, wire, node),
            )
                .await;

            match result {
                Ok(Ok(more)) => more,
                Ok(Err(e)) => {
                    tracing::warn!("send to {to} failed: {e}");
                    Vec::new()
                }
                Err(_) => {
                    tracing::warn!("send to {to} timed out");
                    Vec::new()
                }
            }
        });
    }

    let mut next = Vec::new();
    while let Some(res) = set.join_next().await {
        if let Ok(v) = res {
            next.extend(v);
        }
    }
    next
}