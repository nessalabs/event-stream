# 0005. Restore application state from versioned snapshots and an exact cursor

- **Date:** 2026-09-04
- **Status:** proposed; implementation in progress
- **Prerequisite:** [0004](../004-stream-lifecycle-and-safe-restore/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

Large histories should not force every consumer to rebuild from the beginning. A snapshot is useful only when its state matches a specific cursor and the remaining history is available.

Implementation is underway. The typed API, runtime admission and owned
cancellation, and MemoryStore adapter are present. SQLite persistence,
application recovery examples, crash tests, and resource qualification remain
open. The current requirement-by-requirement evidence is tracked in
[verification-status.md](verification-status.md).

## Decision

Let applications supply opaque snapshot bytes, their application schema/version, stream incarnation, and covered cursor. The application creates and restores the state; the library stores and validates its identity and availability.

Publish a snapshot only after its bytes and metadata are complete. A reader restores that snapshot and replays strictly after its cursor. Reject incompatible state or unavailable remaining history explicitly. Creating a snapshot never authorizes deletion by itself.

Keep snapshot upload/download bounded. Stage large objects separately from publication. Define cleanup for interrupted staging. Protect the chosen snapshot and required history during recovery with a finite lifetime; do not permit abandoned readers to pin storage forever.

## Ordered work

The [snapshot implementation contract](contracts.md) defines the shared typed
API for chunk upload, publication, recovery protection and resource limits.
The runtime and Memory adapter implement this boundary. SQLite is being added
against the same contract. The ordered items below remain the full phase scope;
passing a subset of adapter tests does not complete the phase.

1. Define snapshot descriptor, completion marker, checksums, size limits, and application compatibility callbacks.
2. Specify atomic publication and retry behavior without requiring one giant in-memory byte array.
3. Define recovery selection against current floor/tail and the race with retention. Expired recovery protection must return an explicit retry/recovery error.
4. Build examples for transcript state and workflow progress that restore and continue replay.
5. Measure restore CPU, bytes read, peak memory, staging space, and impact on append latency.

## Runnable Memory example

Run `cargo run --features snapshots --example snapshot_recovery` from the
repository root. The [example](../../../examples/snapshot_recovery.rs) records
`opened` and `approved`, saves application state at cursor 2, then appends
`completed` at cursor 3. Recovery reads the snapshot and applies only record 3.
It compares the resulting transcript and workflow step with complete replay.
An application schema check rejects unsupported versions before decoding.

This path uses MemoryStore. It demonstrates the public API and application
responsibility for snapshot meaning. It does not prove database restart or
crash durability. The `snapshot-replay` verification scenario uses the same
production runtime APIs and labels that scope explicitly.

## Completion evidence

- Snapshot plus remaining records yields the same projection as complete replay for each example.
- Crashes cannot publish partial snapshots. Duplicate publication resolves consistently.
- Wrong incarnation/schema, corrupted bytes, expired protection, and missing history fail explicitly.
- Snapshot create/restore memory stays within configured limits regardless of total snapshot size.
- Snapshot I/O yields to foreground work and meets the measured budgets.

## Consequences

Applications retain responsibility for meaningful state. The library cannot determine that arbitrary snapshot bytes represent the claimed cursor. Fixtures and application-level checks must establish that relationship.
