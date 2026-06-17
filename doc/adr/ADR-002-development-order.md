# ADR-002: Development Order — FUSE Skeleton First, Then Storage Before Raft

## Status
Accepted

## Context
FerumFS has three major subsystems that are largely independent at the
code level but have real dependencies at the integration level:

- **FUSE layer** — userspace filesystem interface (mount, lookup, read,
  write, readdir, etc.)
- **Storage layer** — WAL/journaling, crash consistency
- **Consensus layer (Raft)** — leader election, log replication, the
  subsystem that gives the project its distributed-systems identity

There is a fixed external deadline (September), which makes
development order a real decision rather than an arbitrary choice.

## Decision History

**Initial approach: FUSE first.** Development started with the FUSE
layer, using the `fuse3` hello-world example as a reference (see
ADR-001 for why `fuse3` specifically was chosen) and decomposing it
into the current `AttrHandler` / `DirHandler` / `FileHandler` structure
with shared `InodeManager` / `HandleManager` state (see ADR-003). This
produced a working FUSE skeleton: mountable, with `lookup`/`getattr`/
`readdir` functional against in-memory inode state, and `read`/`write`/
`create`/`unlink`/`rmdir`/`mkdir` left as explicit `todo!()` stubs
pending a real backend.

**Revised approach: Storage next, before continuing FUSE.** With the
FUSE skeleton in place, the next subsystem to build was reconsidered.
Three options were on the table at this point: continue deepening FUSE
(implement the `todo!()` stubs against an ad hoc local backend), build
the Raft skeleton next (leader election in isolation, independently
testable via a multi-node cluster simulation without depending on
storage or FUSE being finished), or build the Storage crate next.

The decision was to build **Storage** next, before either finishing
FUSE's stubbed operations or starting Raft. Implementing FUSE's
remaining stubs immediately would have meant writing throwaway local
read/write logic that would later be replaced once a real storage
backend existed. Building Raft next was set aside because Raft's log
persistence requirements are themselves a Storage-layer concern (see
ADR-004 for the WAL design), so Storage was judged to be the more
fundamental dependency to resolve first regardless of which subsystem
(FUSE or Raft) is finished next.

## Current Planned Order
**FUSE skeleton (done) → Storage → FUSE (real read/write/create/unlink/
rmdir/mkdir against Storage) → Raft (skeleton, then finalized against
the real WAL) → Network → Chaos testing / agent pipeline.**

## Rationale
- Storage is a hard dependency for both finishing FUSE (needs real
  read/write backed by something durable) and for Raft (needs durable
  log persistence per ADR-004). Building it next, rather than
  finishing FUSE's stubs first, avoids writing local read/write logic
  that would be discarded once Storage exists.
- Doing the FUSE skeleton first (rather than Storage or Raft first)
  was still the right starting point in hindsight: it produced an
  early, mountable filesystem and surfaced the handler decomposition
  design (ADR-003) and the inode/handle model before any storage or
  consensus complexity was introduced, making it easier to validate
  that design in isolation.
- Raft was not moved ahead of Storage despite being the subsystem most
  central to the project's distributed-systems goals, because an
  in-memory-log Raft skeleton built now would still need a later
  integration pass to swap in real WAL-backed persistence once Storage
  exists — better to resolve Storage first and avoid that rework.


## Consequences
- FUSE's `read`/`write`/`create`/`unlink`/`rmdir`/`mkdir` handlers
  (currently `todo!()` in `dir.rs` and `file.rs`) remain stubbed until
  the Storage crate's WAL is functional — this is now expected to
  persist for longer than originally anticipated, since Storage is
  being built as its own complete step rather than alongside FUSE.
- Until Storage and FUSE-with-real-backend are complete, there is no
  visible Raft progress in the repository — accepted given the
  rationale above.
- The Raft skeleton, when built, will initially need to decide whether
  to use a temporary in-memory log for leader-election testing or wait
  until the WAL is integrable — this is not yet decided and should be
  revisited when Raft work begins.