# ADR-003: Dual-Mode WAL — Separate Metadata Log and Large-Blob Data Path

## Status
Accepted

## Context

ForumFS's storage layer must provide crash consistency, but the target
workload shapes what "crash consistency" needs to mean in practice. The
project's primary intended use case is distributed dataset serving for
ML training — many large data files (dataset shards, e.g.
`.tfrecord`/`.parquet`-style files) read sequentially across multiple
machines. 

A secondary, lower-priority use case is **ML checkpoint
storage** (infrequent, large model checkpoint writes), which should be
supported if it does not compromise the dataset-serving design, but is
not the primary design driver.

This workload differs substantially from a typical KV-store workload
(e.g. the existing Go-based DKVS project), which the storage design was
initially expected to mirror. KV-style WALs typically log small,
frequent point writes. ML dataset/checkpoint workloads instead involve:

- Large sequential reads (dataset shards)
- Large, infrequent sequential writes (checkpoints, when present)
- A strict requirement that a large write/checkpoint is either fully
  present or treated as absent — a partially-written large file is
  unusable and must never be read as if valid, unlike a KV store where
  recovery typically reconstructs a single last-known-good value.

Logging every byte range of a large sequential write as WAL entries
(the naive approach, and the closest analog to the DKVS get/set model)
would impose write-amplification and fsync overhead disproportionate to
the workload, and is not how production ML systems handle large
checkpoint writes.

## Decision
Split the storage layer's write-ahead logging into two distinct paths:

1. **Metadata WAL** — a classic write-ahead log for structural/metadata
   operations: create, unlink, rename, mkdir, rmdir, and file size /
   permission changes. Entries are small, fsync'd frequently, and
   replayed on recovery in the conventional WAL sense.

2. **Data path (large writes)** — large sequential writes (dataset
   shard ingestion, checkpoint writes when supported) do **not** go
   through the metadata WAL as byte-level entries. Instead, data is
   written to a temporary location, fsync'd, and then made visible via
   an atomic rename into its final path. The metadata WAL only records
   the fact that the rename occurred (a small, cheap entry), not the
   data contents.

The threshold or heuristic for classifying a given write as "large"
(data path) versus "small" (metadata WAL) is **not** owned by the
storage crate — this decision is delegated to the VFS layer, which has
the context to decide based on the operation being performed.

## Rationale
- Sequential dataset reads and infrequent large writes are the dominant
  access pattern for the primary use case; optimizing the WAL for
  small, frequent point writes (the DKVS/KV-store model) would optimize
  for the wrong workload.
- The temp-write-then-atomic-rename pattern is the same technique used
  internally by mainstream ML frameworks (e.g. PyTorch/TensorFlow
  checkpoint writers) specifically because it gives an all-or-nothing
  visibility guarantee for large files without requiring byte-level
  journaling of their contents.
- Keeping the metadata WAL small and decoupled from large data payloads
  means metadata operations stay fast and fsync-cheap regardless of how
  large the data files in the system get.
- Delegating the large-vs-small classification to the VFS layer keeps
  the storage crate's WAL implementation policy-free with respect to
  file size — the storage layer only needs to support "write small
  entry to WAL" and "atomically rename a fully-written file," while the
  VFS layer (which already mediates all FUSE operations) is the natural
  place to decide which path a given write should take.
- A single unified WAL (logging both metadata and full data contents)
  was rejected because it would force every large sequential write
  through the same fsync-heavy, append-only structure as metadata
  changes, which does not match the read-heavy, large-sequential-file
  access pattern the project is designed around.

## Consequences
- The storage crate must expose at least two distinct write paths: a
  metadata WAL append/fsync/replay API, and a temp-write + atomic
  rename API for large data.
- The VFS layer takes on responsibility for the large/small
  classification decision; this logic does not yet exist and must be
  designed when the VFS crate is implemented (currently a `todo!()`
  area, see ADR-002 for sequencing).
- Checkpoint storage support is explicitly secondary: if supporting it
  cleanly within this dual-mode design requires compromises to the
  dataset-serving path, dataset serving takes priority and checkpoint
  support may be deferred or simplified.
- Recovery logic differs by path: metadata WAL replay follows
  conventional WAL recovery semantics, while data-path recovery only
  needs to check whether the atomic rename completed (no partial-file
  replay logic is needed, by construction of the atomic rename
  guarantee).
