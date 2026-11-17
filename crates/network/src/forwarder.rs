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

    pub async fn current_leader(&self) -> Option<u64> {
        self.node.lock().await.current_leader()
    }

    pub async fn node_id(&self) -> u64 {
        self.node.lock().await.id
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

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

    pub async fn propose(
        &self,
        command: Vec<u8>,
        allocate_inode: bool,
    ) -> Result<Option<u64>, FfsError> {
        let leader_id = self.wait_for_leader().await?;
        let self_id = self.node.lock().await.id;

        if leader_id == self_id {
            let rx = {
                let mut n = self.node.lock().await;
                if allocate_inode {
                    return Err(FfsError::StorageError(
                        "allocate_inode on leader path not supported".into(),
                    ));
                }
                n.propose_with_waiter(command)?
            };
            rx.await
                .map_err(|_| FfsError::ConsensusError("channel closed".into()))??;
            return Ok(None);
        }

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
                self.wait_local_apply(index).await?;
                Ok(inode_id)
            }
            _ => Err(FfsError::StorageError("unexpected reply type".into())),
        }
    }

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

    // ----------  block-level API ----------

    /// Forward a block write to the leader. Blocks until the leader has
    /// written and replicated to a majority.
    pub async fn write_block(&self, block_id: u64, data: &[u8]) -> Result<(), FfsError> {
        let leader_id = self.wait_for_leader().await?;
        let self_id = self.node.lock().await.id;
        if leader_id == self_id {
            return Err(FfsError::StorageError(
                "write_block on leader (use local ChunkStore)".into(),
            ));
        }
        let addr = *self
            .peers
            .get(&leader_id)
            .ok_or_else(|| FfsError::ConsensusError(format!("unknown leader {leader_id}")))?;
        let reply = request_wire(
            addr,
            RaftMessage::WriteBlock {
                block_id,
                data: data.to_vec(),
            },
        )
            .await?;
        match reply {
            RaftMessage::WriteBlockReply { success, error } => {
                if success {
                    Ok(())
                } else {
                    Err(FfsError::StorageError(
                        error.unwrap_or_else(|| "write failed".into()),
                    ))
                }
            }
            _ => Err(FfsError::StorageError("unexpected reply".into())),
        }
    }

    /// Fetch a block from the leader (used when local copy is missing).
    pub async fn fetch_block(&self, block_id: u64) -> Result<Vec<u8>, FfsError> {
        let leader_id = self.wait_for_leader().await?;
        let self_id = self.node.lock().await.id;
        if leader_id == self_id {
            return Err(FfsError::StorageError(
                "fetch_block on leader (read local)".into(),
            ));
        }
        let addr = *self
            .peers
            .get(&leader_id)
            .ok_or_else(|| FfsError::ConsensusError(format!("unknown leader {leader_id}")))?;
        let reply = request_wire(addr, RaftMessage::FetchBlock { block_id }).await?;
        match reply {
            RaftMessage::FetchBlockReply { data: Some(d) } => Ok(d),
            RaftMessage::FetchBlockReply { data: None } => Err(FfsError::NotFound(block_id)),
            _ => Err(FfsError::StorageError("unexpected reply".into())),
        }
    }

    /// Fan out a block to all peers, requiring a majority of acks.
    pub async fn replicate_block(&self, block_id: u64, data: &[u8]) -> Result<(), FfsError> {
        replicate_to_all(&self.peers, block_id, data).await
    }

    /// Snapshot of the peer map for background replication. Cheap clone.
    pub fn peers_snapshot(&self) -> HashMap<u64, SocketAddr> {
        self.peers.clone()
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

/// Fan-out a block to all peers, requiring a majority to ack.
///
/// `total = 1 (self) + peers.len()`, `majority = total / 2 + 1`. A write
/// succeeds as long as a majority of nodes have the block. Missing nodes
/// pick it up lazily on next read or at startup repair.
pub async fn replicate_to_all(
    peers: &HashMap<u64, SocketAddr>,
    block_id: u64,
    data: &[u8],
) -> Result<(), FfsError> {
    let total = peers.len() + 1;
    let majority = total / 2 + 1;

    if peers.is_empty() {
        return Ok(());
    }

    let mut set = tokio::task::JoinSet::new();
    for (_peer_id, addr) in peers {
        let addr = *addr;
        let data = data.to_vec();
        set.spawn(async move {
            let msg = RaftMessage::ReplicateBlock { block_id, data };
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                request_wire(addr, msg),
            )
                .await;
            match result {
                Ok(Ok(RaftMessage::ReplicateBlockReply { success: true, .. })) => Ok(()),
                Ok(Ok(RaftMessage::ReplicateBlockReply { success: false, error })) => {
                    Err(FfsError::StorageError(format!(
                        "peer rejected: {}",
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

    let mut acked = 1u32;
    let mut first_err: Option<FfsError> = None;

    while let Some(res) = set.join_next().await {
        match res {
            Ok(Ok(())) => {
                acked += 1;
                // *** EARLY RETURN as soon as we have majority ***
                if acked >= majority as u32 {
                    set.abort_all();
                    if acked < total as u32 {
                        tracing::warn!(
                            block_id, acked, total,
                            "partial replication (majority achieved, lazy repair)"
                        );
                    }
                    return Ok(());
                }
            }
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

    Err(first_err.unwrap_or_else(|| {
        FfsError::StorageError(format!("only {acked}/{total} acked"))
    }))
}