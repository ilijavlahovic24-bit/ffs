use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use common::error::FfsError;

/// Maximum frame size we're willing to read (16 MB).
const MAX_FRAME: u32 = 16 * 1024 * 1024;

/// A single TCP connection with length-prefixed framing.
///
/// Wire format: `[u32 BE length][payload bytes]`.
pub struct TcpConnection {
    stream: TcpStream,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        // Disable Nagle — Raft RPCs are small and latency matters.
        let _ = stream.set_nodelay(true);
        Self { stream }
    }

    pub async fn connect(addr: SocketAddr) -> Result<Self, FfsError> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::new(stream))
    }

    pub async fn send(&mut self, payload: &[u8]) -> Result<(), FfsError> {
        let len = payload.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(payload).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Vec<u8>, FfsError> {
        let mut len_buf = [0u8; 4];
        self.stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf);
        if len > MAX_FRAME {
            return Err(FfsError::Corruption(format!(
                "network: frame too large: {len} > {MAX_FRAME}"
            )));
        }
        let mut buf = vec![0u8; len as usize];
        self.stream.read_exact(&mut buf).await?;
        Ok(buf)
    }
}