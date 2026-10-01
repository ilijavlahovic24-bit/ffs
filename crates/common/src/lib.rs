pub mod types;
pub mod error;
mod config;
mod metric;
pub mod state_machine;

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use fuse3::{FileType, Result};
use types::BlockId;
use tokio::sync::oneshot;
pub use state_machine::StateMachine;