# 0004. Change stream lifetimes explicitly and recover them safely

- **Date:** 2026-09-04
- **Status:** accepted; implementation in progress
- **Prerequisite:** [0003](../003-sqlite-durability-and-storage-layout/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

Applications eventually need to retire, reset, or restore history. Reusing offsets under the same identity would make old cursors unsafe. Deletion also races with accepted writes, subscribers, and storage cleanup.

## Decision

Add explicit delete and reset operations. Require the expected current incarnation so a stale caller cannot delete a newly recreated stream. A reset creates an empty new incarnation; existing cursors never switch to it.

Separate logical deletion from physical cleanup. Persist an unavailable state and end old subscriptions before reclaiming records in bounded chunks. Accepted old writes either commit against the old incarnation or fail explicitly. Never redirect them.

Provide a controlled restore workflow with exclusive ownership. Recovered history that diverges from previously published history needs fresh identity and an explicit consumer rebuild. Normal crash recovery preserves identity. Persist lifecycle operation IDs so retrying a lost reset response does not reset the new incarnation again.

## Ordered work

1. Specify operation inputs, stable operation IDs, stale-incarnation errors, and the transition table for active, unavailable, and replacement streams.
2. Make the logical transition transactional and crash-safe. Define exactly when new writes stop and readers are notified.
3. Add bounded, restartable cleanup with progress metadata. Keep enough lifecycle receipts to resolve uncertain retries.
4. Design backup/restore publication so a partially restored store cannot accept traffic. Check format and integrity before activating it.
5. Add numbered examples and crash schedules for every transition.

## Completion evidence

Delete/reset and bounded retired-history cleanup now have a typed domain and
application contract, MemoryStore implementation, runtime coordination, shared
adapter tests, cancellation and unknown-outcome tests, and runnable verification
scenarios. SQLite lifecycle persistence and format-1 migration are verified in
[sqlite-lifecycle-evidence.md](sqlite-lifecycle-evidence.md). Controlled restore
is being implemented against the [restore contract](restore-contract.md).
Its application manager is tested. SQLite publication recovery and resource
gates remain unfinished, so this phase is not complete.

- Retry after lost delete/reset acknowledgement returns the original outcome without a second mutation.
- A stale handle cannot write to or delete a replacement incarnation.
- Crashes during transition, restore, and cleanup leave a recoverable, explicit state.
- Old subscribers end; no reader silently follows a new incarnation.
- Cleanup has a measured I/O and time budget and does not block new streams for unbounded periods.

## Consequences

Logical deletion adds small persisted lifecycle state but avoids long transactions over entire streams. Physical secure erasure is not implied. Restore does not recreate unobserved external effects.
