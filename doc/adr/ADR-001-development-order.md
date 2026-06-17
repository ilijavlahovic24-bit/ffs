# ADR-001: Development Order — Storage Before FUSE Before Raft

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

The central question was which subsystem to build first, given a fixed
external deadline (September) and the fact that the project is actively
being shown to internship recruiters before that deadline.

Three orderings were considered:

1. **Raft first** — build leader election and log replication in
   isolation (in-memory log), since Raft is independently testable via a
   multi-node cluster simulation and is the most CV-relevant subsystem.
2. **Storage first** — build the WAL/journaling layer first, since both
   FUSE (real read/write) and Raft (durable log persistence) ultimately
   depend on it, avoiding rework later.
3. **FUSE first** — finish the userspace filesystem against a stub/local
   backend to get an early end-to-end demoable mount.

## Decision
Build in this order: **Storage → FUSE → Raft (skeleton first, then
finalized) → Network → Chaos testing / agent pipeline**.

Concretely:
- Storage crate (dual-mode WAL, see ADR-003) is implemented first.
- FUSE handlers are connected to real storage (replacing the current
  `todo!()` stubs for read/write/create/unlink/rmdir/mkdir).
- Raft is then implemented in two passes: an initial skeleton (state
  machine, term tracking, RequestVote, AppendEntries-as-heartbeat) is
  deferred until after Storage and FUSE are functional, then finalized
  by wiring its log persistence into the real WAL from the Storage
  layer.

## Rationale
- Storage is a hard dependency for both FUSE (needs real read/write) and
  Raft (needs durable log persistence). Building it first avoids a
  reimplementation pass that would otherwise be required if Raft used
  an in-memory log temporarily.
- A working FUSE mount on top of real storage produces a demoable,
  single-node distributed-adjacent filesystem before any distributed
  complexity is introduced — useful given the project is visible to
  recruiters mid-development, where a partially complete but clearly
  structured repo (with explicit `todo!()` markers and ADRs) is
  considered acceptable and still better than an empty one.
- Raft is deliberately split into skeleton and finalized passes rather
  than fully deferred: this preserves the option to demonstrate leader
  election independently if the timeline slips, while still avoiding
  a full throwaway in-memory log implementation up front.
- The alternative of building Raft fully first was rejected because, in
  isolation, an in-memory-log Raft implementation does not represent a
  working part of the actual system end-to-end, and would require a
  later integration pass to swap in real WAL-backed persistence anyway.

## Consequences
- Until Storage and FUSE are complete, there is no visible Raft progress
  in the repository — acceptable given the chosen rationale above.
- The Raft skeleton (built after Storage/FUSE) will initially use
  in-memory log entries for leader election testing, then be wired to
  the real WAL in the finalization pass — this integration step must be
  explicitly planned for and is not optional cleanup.
- FUSE read/write/create/unlink/rmdir/mkdir handlers (currently
  `todo!()` in `dir.rs` and `file.rs`) are blocked on the Storage crate
  being available, and are the next implementation target after
  Storage's WAL is functional.
