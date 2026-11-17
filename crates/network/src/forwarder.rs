use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use common::error::FfsError;
use consensus::RaftNode;

use crate::connection::TcpConnection;
use crate::message::{decode, encode, RaftMessage};

/// Forward client operations from a follower to the current leader.
pub struct RaftForwarder {
    node: Arc<Mutex<RaftNode>>,
    peers: HashMap<u64, SocketAddr>,
}

impl RaftForwarder {
    pub fn new(node: Arc<Mutex<RaftNode>>, peers: HashMap<u64, SocketAddr>) -> Self {
        Self { node, peers }
    }

    pub async fn is_leader(&self) -> bool {
        self.node.lock().await.is_leader()
    }

    /// Wait for a leader to be known (up to 2s), returning its id.
    async fn wait_for_leader(&self) -> Result<u64, FfsError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(l) = self.node.lock().await.current_leader() {
                return Ok(l);
            }
            if Instant::now() >= deadline {
                return Err(FfsError::NotLeader);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Propose `command` to the cluster. If this node is leader, apply
    /// locally; otherwise forward to leader. Waits for local apply.
    ///
    /// If `allocate_inode`, the caller receives the leader-allocated inode.
    pub async fn propose(
        &self,
        command: Vec<u8>,
        allocate_inode: bool,
    ) -> Result<Option<u64>, FfsError> {
        let leader_id = self.wait_for_leader().await?;
        let self_id = self.node.lock().await.id;

        if leader_id == self_id {
            // Local propose.
            let (rx, _) = {
                let mut n = self.node.lock().await;
                // Local path doesn't support allocate_inode — this should
                // only be called by followers. Return error to be safe.
                if allocate_inode {
                    return Err(FfsError::StorageError(
                        "allocate_inode on leader path not supported".into(),
                    ));
                }
                let rx = n.propose_with_waiter(command)?;
                (rx, ())
            };
            rx.await
                .map_err(|_| FfsError::ConsensusError("channel closed".into()))??;
            return Ok(None);
        }

        // Remote forward.
        let addr = *self
            .peers
            .get(&leader_id)
            .ok_or_else(|| FfsError::ConsensusError(format!("unknown leader {leader_id}")))?;

        let wire = RaftMessage::Propose { command, allocate_inode };
        let reply = request_wire(addr, wire).await?;

        match reply {
            RaftMessage::ProposeReply { success, index, inode_id, error } => {
                if !success {
                    return Err(FfsError::ConsensusError(
                        error.unwrap_or_else(|| "propose failed".into()),
                    ));
                }
                // Wait for local state machine to catch up to `index`.
                self.wait_local_apply(index).await?;
                Ok(inode_id)
            }
            _ => Err(FfsError::StorageError("unexpected reply type".into())),
        }
    }

    /// Poll until `last_applied >= index` or timeout.
    async fn wait_local_apply(&self, index: u64) -> Result<(), FfsError> {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if self.node.lock().await.last_applied >= index {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(FfsError::ConsensusError(
                    "timeout waiting for local apply".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Read blob from leader if we're a follower.
    pub async fn read_blob(&self, inode: u64) -> Result<Vec<u8>, FfsError> {
        let leader_id = self.wait_for_leader().await?;
        let self_id = self.node.lock().await.id;

        if leader_id == self_id {
            return Err(FfsError::StorageError(
                "read_blob called on leader (use local DataPath)".into(),
            ));
        }

        let addr = *self
            .peers
            .get(&leader_id)
            .ok_or_else(|| FfsError::ConsensusError(format!("unknown leader {leader_id}")))?;

        let reply = request_wire(addr, RaftMessage::ReadBlob { inode }).await?;
        match reply {
            RaftMessage::ReadBlobReply { data } => Ok(data),
            _ => Err(FfsError::StorageError("unexpected reply type".into())),
        }
    }
    /// Forward a blob write to the leader.
    ///
    /// Called by followers when FUSE asks them to write data. The leader
    /// writes the blob to its local `DataPath` and returns success.
    /// Metadata (size, etc.) is updated separately through Raft.
    pub async fn write_blob(&self, inode: u64, data: &[u8]) -> Result<(), FfsError> {
        let leader_id = self.wait_for_leader().await?;
        let self_id = self.node.lock().await.id;

        if leader_id == self_id {
            return Err(FfsError::StorageError(
                "write_blob called on leader (use local DataPath)".into(),
            ));
        }

        let addr = *self
            .peers
            .get(&leader_id)
            .ok_or_else(|| FfsError::ConsensusError(format!("unknown leader {leader_id}")))?;

        let wire = RaftMessage::WriteBlob {
            inode,
            data: data.to_vec(),
        };
        let reply = request_wire(addr, wire).await?;
        match reply {
            RaftMessage::WriteBlobReply { success, error } => {
                if success {
                    Ok(())
                } else {
                    Err(FfsError::StorageError(
                        error.unwrap_or_else(|| "remote write failed".into()),
                    ))
                }
            }
            _ => Err(FfsError::StorageError("unexpected reply type".into())),
        }
    }

    /// Replicate a blob to all peers. Blocks until every peer acks.
    ///
    /// Only valid on the leader — followers should use `write_blob`,
    /// which forwards to the leader who will do the replication.
    pub async fn replicate_blob(&self, inode: u64, data: &[u8]) -> Result<(), FfsError> {
        replicate_to_all(&self.peers, inode, data).await
    }
}

// Re-export helpers used elsewhere.
pub async fn request_wire(
    addr: SocketAddr,
    msg: RaftMessage,
) -> Result<RaftMessage, FfsError> {
    let mut conn = crate::connection::TcpConnection::connect(addr).await?;
    conn.send(&encode(&msg)?).await?;
    let bytes = conn.recv().await?;
    decode(&bytes)
}

/// Fan-out a blob to all peers. Returns `Ok` only if every peer acked
/// within the timeout. Any failure (peer down, slow, or returned error)
/// propagates to the caller.
///
/// The leader's own write is not done here — the caller must have
/// already written locally before calling this. Total durability is
/// therefore `1 (self) + N (peers)`.
pub async fn replicate_to_all(
    peers: &HashMap<u64, SocketAddr>,
    inode: u64,
    data: &[u8],
) -> Result<(), FfsError> {
    if peers.is_empty() {
        return Ok(());
    }

    let mut set = tokio::task::JoinSet::new();
    for (_peer_id, addr) in peers {
        let addr = *addr;
        let data = data.to_vec();
        set.spawn(async move {
            let msg = RaftMessage::ReplicateBlob { inode, data };
            let result = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                request_wire(addr, msg),
            )
                .await;

            match result {
                Ok(Ok(RaftMessage::ReplicateBlobReply { success: true, .. })) => Ok(()),
                Ok(Ok(RaftMessage::ReplicateBlobReply { success: false, error })) => {
                    Err(FfsError::StorageError(format!(
                        "peer rejected replicate: {}",
                        error.unwrap_or_else(|| "unknown".into())
                    )))
                }
                Ok(Ok(_)) => Err(FfsError::StorageError("unexpected reply".into())),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(FfsError::StorageError(format!(
                    "replicate to {addr} timed out"
                ))),
            }
        });
    }

    let mut first_err: Option<FfsError> = None;
    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(FfsError::StorageError(format!("join error: {e}")));
                }
            }
        }
    }

    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}