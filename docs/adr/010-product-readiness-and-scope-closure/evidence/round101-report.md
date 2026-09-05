# Snapshot interruption and cleanup/read boundaries

Two previously planned console checks now call fixtures shared with integration
tests. Both use actual SQLite operations. No production code changed.

## Interrupted publication

The fixture publishes a baseline snapshot, stages two partial uploads, closes
and reopens SQLite. Only the baseline is visible. A recovery request for the
partial upload is rejected. Retrying its first chunk keeps accepted bytes at
four. Resuming publishes the exact intended bytes. Aborting the second upload
removes two chunks and its descriptor in three one-row cleanup calls. The
baseline remains unchanged. This is orderly close/reopen, not a process-kill or
power-loss schedule.

## Cleanup during replay

An injected EventStore wrapper pauses calls around the real SQLite read. It
does not construct pages or change storage results. If cleanup runs before the
adapter reads, the result is HistoryUnavailable with floor and tail at three.
If SQLite has already constructed the page, cleanup can remove all rows while
the page still returns all three exact offsets and payloads. The two streams
lose six physical records and leave no cleanup pending.

These are controlled port-boundary schedules. They do not represent concurrent
SQLite transactions. The integration test has a 15-second deadlock timeout.
The same fixture checks its own invariants when invoked by the console.

## Evidence

Both new integration tests and all 19 verification tests pass. Fresh individual
headless callbacks pass four observations each. Strict all-feature/all-target
Clippy, Rust 1.85 compatibility and formatting checks pass. Logs, callback JSON
and source hashes are beside this report with the round101 prefix. Home records
no new resource experiments. Current native visual rendering remains unverified.

The roadmap index no longer describes SQLite bootstrap and durable parser EOF
as unimplemented. Their qualification limits remain explicit. Sustained scale,
settled memory, release budgets and group-commit evaluation remain open.
