use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Mutex;

use common::error::FfsError;
use consensus::RaftNode;
use storage::wal::DataPath;

use crate::client::drive_round;
use crate::connection::TcpConnection;
use crate::message::{decode, encode, RaftMessage};

/// Drives one Raft node: accepts incoming RPCs and periodically ticks
/// the node, dispatching its outbox to peers over TCP.
///
/// Also handles client-side forwarding messages (`Propose`, `ReadBlob`)
/// sent by followers that need to route operations to the leader.
pub struct RaftServer {
    listener: TcpListener,
    node: Arc<Mutex<RaftNode>>,
    peers: Arc<HashMap<u64, SocketAddr>>,
    data_path: Arc<DataPath>,
}

impl RaftServer {
    pub fn new(
        listener: TcpListener,
        node: Arc<Mutex<RaftNode>>,
        peers: HashMap<u64, SocketAddr>,
        data_path: Arc<DataPath>,
    ) -> Self {
        Self {
            listener,
            node,
            peers: Arc::new(peers),
            data_path,
        }
    }

    /// Run the accept loop and the driver loop concurrently.
    ///
    /// This future runs forever unless the listener fails.
    pub async fn run(self) -> Result<(), FfsError> {
        let RaftServer {
            listener,
            node,
            peers,
            data_path,
        } = self;

        // Driver task: ticks the node and flushes its outbox.
        let driver_node = node.clone();
        let driver_peers = peers.clone();
        tokio::spawn(async move {
            run_driver(driver_node, driver_peers).await;
        });

        // Accept loop.
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            tracing::debug!("incoming connection from {peer_addr}");

            let node = node.clone();
            let data = data_path.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_incoming(stream, node, data).await {
                    tracing::debug!("connection from {peer_addr} closed: {e}");
                }
            });
        }
    }
}

/// Driver loop: every 10 ms, tick the node, drain its outbox, and deliver
/// messages to peers. Repeats for a few rounds if handling replies
/// produces new outbox entries (e.g. becoming leader triggers heartbeats).
async fn run_driver(
    node: Arc<Mutex<RaftNode>>,
    peers: Arc<HashMap<u64, SocketAddr>>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(10));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        // 1. Tick and drain outbox. Release the lock before any I/O.
        let outgoing = {
            let mut n = node.lock().await;
            n.tick();
            n.take_outbox()
        };

        // 2. Cascade through rounds until nothing new is produced.
        //    Capped to prevent runaway loops.
        let mut queue = outgoing;
        let mut rounds = 0;
        while !queue.is_empty() && rounds < 5 {
            queue = drive_round(queue, peers.clone(), node.clone()).await;
            rounds += 1;
        }
    }
}

/// Handle a single incoming connection: read one request, apply it to
/// the node (or forward it), send the reply.
async fn handle_incoming(
    stream: tokio::net::TcpStream,
    node: Arc<Mutex<RaftNode>>,
    data_path: Arc<DataPath>,
) -> Result<(), FfsError> {
    let mut conn = TcpConnection::new(stream);
    let bytes = conn.recv().await?;
    let msg = decode(&bytes)?;

    let reply = match msg {
        RaftMessage::RequestVote(req) => {
            let mut n = node.lock().await;
            let from = n.id;
            let reply = n.handle_request_vote(req);
            RaftMessage::RequestVoteReply { from, reply }
        }
        RaftMessage::AppendEntries(req) => {
            let mut n = node.lock().await;
            let from = n.id;
            let reply = n.handle_append_entries(req);
            RaftMessage::AppendEntriesReply { from, reply }
        }
        RaftMessage::Propose {
            command,
            allocate_inode,
        } => handle_propose(node.clone(), command, allocate_inode).await,
        RaftMessage::ReadBlob { inode } => {
            // Blob data lives only on the leader (ADR-003). Followers
            // forward reads here; missing blobs are treated as empty.
            let data = data_path.read_blob(inode).await.unwrap_or_default();
            RaftMessage::ReadBlobReply { data }
        }
        RaftMessage::WriteBlob { inode, data } => {
            match data_path.write_blob(inode, &data).await {
                Ok(()) => RaftMessage::WriteBlobReply {
                    success: true,
                    error: None,
                },
                Err(e) => RaftMessage::WriteBlobReply {
                    success: false,
                    error: Some(e.to_string()),
                },
            }
        }
        // We never receive replies on the server side.
        _ => {
            tracing::warn!("server received unexpected message; ignoring");
            return Ok(());
        }
    };

    conn.send(&encode(&reply)?).await?;
    Ok(())
}

/// Handle a forwarded `Propose` from a follower.
///
/// If `allocate_inode` is true, the follower sent a `WalOperation` with
/// `inode_id = 0` as a placeholder. We allocate a fresh id on the leader,
/// patch the command, then propose the patched version. The inode id is
/// returned to the caller in the reply.
async fn handle_propose(
    node: Arc<Mutex<RaftNode>>,
    mut command: Vec<u8>,
    allocate_inode: bool,
) -> RaftMessage {
    let inode_id = if allocate_inode {
        let ino = node.lock().await.alloc_inode_id();
        if let Err(e) = patch_inode_id(&mut command, ino) {
            return RaftMessage::ProposeReply {
                success: false,
                index: 0,
                inode_id: None,
                error: Some(format!("patch inode: {e}")),
            };
        }
        Some(ino)
    } else {
        None
    };

    // Register waiter and capture the index in a single lock scope.
    let (rx, index) = {
        let mut n = node.lock().await;
        if !n.is_leader() {
            return RaftMessage::ProposeReply {
                success: false,
                index: 0,
                inode_id: None,
                error: Some("not leader".into()),
            };
        }
        match n.propose_with_waiter(command) {
            Ok(rx) => {
                // propose_with_waiter just appended the entry, so
                // last_index is the index for this proposal.
                let idx = n.log.last_index();
                (rx, idx)
            }
            Err(e) => {
                return RaftMessage::ProposeReply {
                    success: false,
                    index: 0,
                    inode_id: None,
                    error: Some(e.to_string()),
                };
            }
        }
    };

    // Wait for commit outside the lock.
    match rx.await {
        Ok(Ok(())) => RaftMessage::ProposeReply {
            success: true,
            index,
            inode_id,
            error: None,
        },
        Ok(Err(e)) => RaftMessage::ProposeReply {
            success: false,
            index,
            inode_id: None,
            error: Some(e.to_string()),
        },
        Err(_) => RaftMessage::ProposeReply {
            success: false,
            index: 0,
            inode_id: None,
            error: Some("channel closed".into()),
        },
    }
}

/// Decode a `WalOperation`, replace the placeholder `inode_id` with the
/// leader-allocated id, and re-encode.
///
/// Only `Create` and `Mkdir` carry allocatable ids; other variants are
/// left untouched.
fn patch_inode_id(command: &mut Vec<u8>, new_id: u64) -> Result<(), FfsError> {
    use storage::wal::WalOperation;

    const BINCODE: bincode::config::Configuration = bincode::config::standard();

    let (mut op, _): (WalOperation, usize) =
        bincode::serde::decode_from_slice(command, BINCODE)
            .map_err(|e| FfsError::Corruption(format!("decode wal op: {e}")))?;

    match &mut op {
        WalOperation::Create { inode_id, .. } | WalOperation::Mkdir { inode_id, .. } => {
            *inode_id = new_id;
        }
        _ => {
            // No allocation needed for this variant.
        }
    }

    *command = bincode::serde::encode_to_vec(&op, BINCODE)
        .map_err(|e| FfsError::StorageError(format!("encode wal op: {e}")))?;
    Ok(())
}