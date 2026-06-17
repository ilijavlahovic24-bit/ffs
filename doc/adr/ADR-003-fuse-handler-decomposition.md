# ADR-002: FUSE Handler Decomposition with Shared Inode/Handle State

## Status
Accepted

## Context
The `fuse3` crate's `Filesystem` trait exposes a single, large interface
(init, destroy, lookup, getattr, open, read, write, readdir, mkdir,
create, unlink, rmdir, etc.). The reference/template implementation
(adapted from the `fuse3` hello-world example) implements all of these
methods directly on one struct, with no internal subdivision.

Two approaches were considered for structuring `DistributedFUSE`:

1. **Monolithic implementation** — implement every `Filesystem` trait
   method directly on a single struct, matching the structure of the
   `fuse3` example code.
2. **Decomposed handlers** — split responsibilities into separate
   handler structs grouped by concern (attributes, directory operations,
   file operations), each holding references to shared state, with the
   top-level `Filesystem` impl delegating to the appropriate handler.

## Decision
Decompose the FUSE implementation into three handler structs, each
owning a narrow slice of the `Filesystem` trait's responsibilities:

- **`AttrHandler`** — `init`, `destroy`, `getattr`
- **`DirHandler`** — `lookup`, `readdir`, `mkdir`, `unlink`, `rmdir`
- **`FileHandler`** — `open`, `read`, `write`, `create`

Shared state is extracted into two dedicated managers, held behind
`Arc` and injected into each handler that needs them:

- **`InodeManager`** — owns inode allocation, inode metadata
  (`InodeInfo`), and the `(parent, name) -> inode` lookup table.
- **`HandleManager`** — owns file handle allocation and the
  `handle -> inode` mapping for open file descriptors.

`DistributedFUSE` itself becomes a thin coordinator: it constructs the
managers and handlers, and its `Filesystem` trait implementation is
reduced to one-line delegations to the appropriate handler method.

## Rationale
- The `fuse3::Filesystem` trait mixes concerns (metadata queries,
  directory mutation, file I/O) that map naturally onto separate
  responsibilities once the implementation needs to do real work
  (as opposed to the static hello-world example, where one struct is
  sufficient because there is no real state to manage).
- Splitting by concern keeps each handler's dependencies minimal and
  explicit — e.g. `AttrHandler` only needs `InodeManager`, while
  `FileHandler` needs both `InodeManager` and `HandleManager` — making
  it clear at the type level which state each operation touches.
- Centralizing inode and handle bookkeeping in dedicated managers
  (rather than scattering `DashMap`s across handlers) avoids duplicated
  or inconsistent state, and gives a single place to later add
  concurrency-safety guarantees when these managers are extended to
  coordinate with the distributed metadata layer.
- This structure is intended to read clearly in a CV/interview context:
  it demonstrates separation of concerns and ownership design in Rust
  (`Arc`-shared state across async handlers) rather than a flat
  trait implementation copied from a reference example.
- The monolithic approach was rejected because it does not scale
  past the hello-world stage: as soon as real inode/handle state and
  distributed forwarding logic are introduced, a single struct
  implementing every trait method becomes a poor separation of
  concerns and harder to extend independently (e.g. swapping how
  directory operations forward to the metadata service without
  touching file I/O code).

## Consequences
- Every new `Filesystem` trait method must be assigned to one of the
  three handlers (or a new handler introduced) rather than implemented
  ad hoc on `DistributedFUSE` directly.
- `InodeManager` and `HandleManager` are shared via `Arc` across async
  handler calls; as concurrent distributed operations are added (e.g.
  via the future `consensus`/`network` crates), these managers are the
  designated integration points and must be revisited for correctness
  under concurrent mutation (see known race condition in
  `InodeManager::alloc_inode` / `add_inode` being a separate two-step
  call, currently accepted as a known limitation pending the
  distributed metadata layer).
- `DirHandler` and `FileHandler` currently contain `todo!()` stubs for
  operations that require forwarding to the (not yet implemented)
  storage/metadata layer — this is expected and tracked under the
  development order in ADR-001, not a defect of this decomposition.
