# ADR-004: Use `fuse3` Instead of `fuser` for the FUSE Layer

## Status
Accepted

## Context
FerumFS needs a Rust FUSE binding to implement the userspace filesystem
layer. The two main candidates in the Rust ecosystem are:

- **`fuser`** — a long-standing, widely used FUSE binding for Rust,
  originally forked from an older `fuse` crate. Its `Filesystem` trait
  is synchronous: each callback is a normal (blocking) function, and
  any async work inside a callback must be bridged manually (e.g. by
  blocking on a runtime handle or spawning a task and synchronizing
  back).
- **`fuse3`** — a FUSE binding designed from the ground up around
  async/await, where `Filesystem` trait methods are `async fn`s that
  integrate directly with an async runtime (Tokio or `async-io`,
  selected via feature flag).

The rest of FerumFS's stack is async-first: the FUSE layer needs to
forward operations to a storage layer doing async disk I/O, and
eventually to a network layer doing async RPCs to other cluster nodes
for replication and consensus. The FUSE binding sits at the entry point
of every one of these calls.

## Decision
Use `fuse3` (with the `tokio-runtime` and `unprivileged` features) as
the FUSE binding for FerumFS, rather than `fuser`.

## Rationale
- `fuse3`'s `Filesystem` trait methods are natively `async fn`,
  matching the async-first design of the rest of the stack (storage
  I/O, and future network/consensus RPCs). Every FUSE callback in
  FerumFS (`lookup`, `read`, `write`, etc.) will eventually need to
  `await` work from the storage layer and, later, distributed
  coordination — `fuse3` lets this be expressed directly as `async fn`
  without a sync/async boundary at the FUSE callback layer.
- `fuser`'s synchronous trait would require bridging every callback
  into the async world manually (e.g. `Handle::block_on`, or spawning
  a task and synchronizing the result back into the synchronous
  callback). This adds boilerplate and a recurring source of subtle
  bugs (blocking the FUSE callback thread, deadlocks between the sync
  callback and the async runtime) at every single FUSE entry point,
  for a project where essentially all of those entry points need async
  behavior — not just an occasional one.
- The `unprivileged` feature in `fuse3` allows mounting without root
  via `fusermount3`, which keeps local development and testing simpler
  than requiring elevated privileges for every mount/unmount cycle
  during iteration.
- `fuser` remains a reasonable choice for filesystems where most
  operations are CPU-bound or where only a few callbacks need async
  behavior (e.g. simple passthrough filesystems) — but that is not
  FerumFS's case, where nearly every operation is I/O-bound and will
  eventually involve cluster communication.

## Consequences
- All `Filesystem` trait methods in FerumFS are implemented as
  `async fn`, and the project takes a direct dependency on Tokio as
  its async runtime throughout (FUSE layer, and eventually storage and
  network layers), rather than mixing sync and async code at the FUSE
  boundary.
- `fuse3` is a smaller, less widely adopted crate than `fuser`; less
  community documentation and fewer existing examples are available
  when debugging FUSE-specific issues, which has already been a factor
  in earlier build/feature-flag issues (e.g. `mount_with_unprivileged`
  requiring the `unprivileged` feature to be explicitly enabled).
- Any future contributor or reviewer needs to be comfortable with
  async Rust and Tokio to work on the FUSE layer, since there is no
  synchronous fallback path in this design.
