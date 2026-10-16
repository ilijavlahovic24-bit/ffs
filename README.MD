# FerumFS:
A Crash-Consistent, High-Performance Distributed File System
## Status: Completed

## Overview
FerumFS is a distributed file system written in Rust, focused on performance and crash consistency.
It uses Tokio for asynchronous I/O and FUSE3 for userspace filesystem mounting.

### Prerequisites
- Rust 1.9 or Newer
- Linux or WSL

## Getting Started
cargo run -- <mount_point>

## Project Structure
Project is structured in Rust Crates.
```
FerumFS
├── crates/
    ├──common/      # Shared types and utilities
    ├──consensus/   # implementation of raft
    ├──fuse/        # fuse3 ant is use in my own FUSE
    ├──meta/        #  metadata
    ├──network/     # server
    ├──storage/     # journaling and wal system
    ├──vfs/         # virtual file system
    └──chaos/       # chaos testing of raft
    
├──ffs.rs
└──cargo.toml
```

### Completed
- Project structure
- FUSE layer — userspace filesystem mounting via `fuse3`
- WAL and journaling (`storage` crate)
- Raft consensus implementation
- Metadata layer
- Network layer (inter-node communication)
- Virtual filesystem abstraction (`vfs` crate)
- Consistency guarantees
- Chaos Testing

## References and Documentation
1. [Designing Data-Intensive Applications — Martin Kleppmann] — primary guide
2. [Osnovi Operativnih Sistema- Dragan Milicev ]- secondary guide from University
3. [https://arorashu.github.io/posts/raft.html ] - Implementation of Raft in Go
4. [https://vishalcjha.medium.com/rust-in-distributed-system-environment-8b164b6a5bed ]- Rust in Distributed enviorment
5. [https://github.com/Sherlock-Holo/fuse3 ] - fuse 3 
6. [https://medium.com/@abhi18632/understanding-write-ahead-logs-in-distributed-systems-3b36892fa3ba ] - Understranding WAL
7. [https://www.youtube.com/playlist?list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB ] -MIT Opencourseware Distributed Systems
