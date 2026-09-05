# SQLite adapter evidence

- **Measured:** 2026-09-04
- **Scope:** SQLite adapter correctness, process-restart recovery, ownership, and provisional physical layout
- **Related decisions:** [event-stream LLD](lld.md), [performance design](performance.md), [ADR 0003](../003-sqlite-durability-and-storage-layout/adr.md)

This evidence supports a process-restart persistence profile. It does not support a power-loss claim.

## Implementation and effective settings

The adapter uses `rusqlite 0.32.1`, `libsqlite3-sys 0.30.1`, and bundled SQLite 3.46.0. The dependency versions are pinned. Opening a store reads the engine version and the effective settings back from SQLite.

The measured host was an Apple silicon Mac running macOS 26.6 build 25G72 and Darwin 25.6.0. The database was on APFS on an Apple Fabric solid-state device. Rust was 1.98.1 for the measurements. The bundled SQLite adapter and its dependencies compile with the project's Rust 1.85 minimum version; this is not a claim that every unrelated target currently passes on that compiler.

The adapter selected these settings:

```text
journal_mode       = DELETE
synchronous        = FULL
foreign_keys       = ON
busy_timeout       = 5000 ms
cache_size         = -4096 KiB
temp_store         = MEMORY
max_page_count     = 262144 by default
statement cache    = 8 statements
worker connections = 1
database encoding  = UTF-8
```

The worker queue, input record size, page record count, page bytes, SQLite cache, statement cache, and database page count all have finite limits. Before returning stored values to Rust, SQL projections check SQLite types, UTF-8 byte lengths, the aggregate record allocation bound, and fixed-width BLOB lengths. Invalid event IDs, schema IDs, payloads, offsets, incarnations, cursors, schema versions, and format versions become an explicit corruption error without materializing the invalid value as an owned Rust result. UTF-8 encoding is required so SQLite's `octet_length` matches the domain identifier limit, including multibyte text and text after an embedded NUL.

## Physical layout comparison

Run the bounded probe with:

```sh
cargo run --release --features sqlite --example sqlite_schema_probe
```

The probe emits one JSON object per line. It records the environment followed by every raw sample. It runs each layout three times for 128-byte, 1 KiB, 16 KiB, and maximum configured records. Every case measures one insertion transaction, an eight-record replay query, an exact retry lookup, total database bytes, table bytes, index bytes, and both query plans. The captured raw JSONL, including the probe source SHA-256, is in [evidence/sqlite-schema.jsonl](evidence/sqlite-schema.jsonl).

The table reports the median of the three observed timing samples. Sizes are deterministic across the three repetitions on this host. Timings are local observations, not release budgets.

| Payload | Layout | Records | Database bytes | Insert transaction | Replay query | Retry query |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 128 B | rowid | 8,192 | 1,798,144 | 7.913 ms | 4.495 µs | 4.231 µs |
| 128 B | without rowid | 8,192 | 1,826,816 | 7.649 ms | 4.150 µs | 4.312 µs |
| 1 KiB | rowid | 4,096 | 5,787,648 | 9.971 ms | 4.774 µs | 4.192 µs |
| 1 KiB | without rowid | 4,096 | 19,296,256 | 27.958 ms | 14.058 µs | 5.062 µs |
| 16 KiB | rowid | 512 | 8,691,712 | 10.037 ms | 16.437 µs | 5.987 µs |
| 16 KiB | without rowid | 512 | 8,708,096 | 11.806 ms | 40.369 µs | 7.128 µs |
| 1,048,430 B | rowid | 8 | 8,413,184 | 9.852 ms | 897.920 µs | 114.529 µs |
| 1,048,430 B | without rowid | 8 | 8,409,088 | 11.769 ms | 1,930.229 µs | 496.858 µs |

The rowid layout uses the ordered unique index for replay and the retry unique index for event-ID lookup. The `WITHOUT ROWID` layout uses its composite primary key for replay and its retry unique index for lookup. Every observed plan was an indexed search. No observed plan scanned or sorted the table.

Neither layout wins every measurement. `WITHOUT ROWID` has a 4 KiB size advantage at the maximum payload and slightly lower median insertion and replay time at 128 bytes. The ordinary rowid layout is much smaller at 1 KiB and has lower median insertion, replay, and retry time at 1 KiB, 16 KiB, and the maximum payload. No production payload mixture has been selected, so weighting these cases would invent a requirement. Format version 1 provisionally uses the ordinary rowid table because it is SQLite's simpler default and the measured tradeoff gives no reason to add the specialized layout. Release format freeze still needs representative workload budgets.

The insertion measurement commits a whole fixture in one transaction. It does not measure the adapter's one-event durable receipt latency. The read and retry measurements materialize payload bytes, but they do not measure async queue delay or runtime scheduling.

## Correctness and recovery evidence

The SQLite integration suite uses real isolated database files. It covers:

- The shared `EventStore` contract, including concurrent create, 32 concurrent gapless appends, 32 identical first attempts with exactly one insertion, concurrent schema conflicts, stream-scoped identical IDs, page boundaries, identity rejection, and finite limits.
- Restart recovery of bytes, retry IDs, incarnation, bounds, and full-precision cursors.
- `u64` offsets across the signed boundary, a record at `u64::MAX`, and checked overflow rejection.
- Indexed bounded replay and exact retry lookup.
- In-process ownership, separate-process ownership, dropped-store cleanup, and cancellation of close while the worker queue is full.
- Parent-path normalization and rejection of database symlinks and hard links on Unix.
- Process termination before commit after the rollback journal contains data. Recovery returns tail zero and no event.
- Process termination after the event is visible in SQLite but before the append caller receives a result. Retrying the original event ID deduplicates at offset one.
- Injected failures before record insertion, after record insertion, and after tail update. Each position uses a real SQLite rollback and leaves neither a record nor a changed tail.
- An injected lost acknowledgement after a real commit. The worker resolves the original event ID before returning a receipt.
- SQLite `max_page_count` exhaustion. SQLite returns `SQLITE_FULL`; the transaction is absent afterward, and the event row, retry lookup, and tail remain unchanged.
- Newer format rejection without mutation, rejection of a foreign database without partial initialization, and rejection when format metadata exists but a required table is missing.
- Oversized or mistyped corrupt stream metadata and record fields rejected before owned Rust result materialization. Fixtures include multibyte identifiers, an embedded-NUL identifier with a long suffix, oversized payload and offset BLOBs, and non-integer schema and format versions.
- A test-only forwarding SQLite VFS that injects one write failure and one sync failure while delegating every other version-1 file and VFS callback to the bundled default VFS. Separate fired counters prove that the selected callback failed.
- Persistent main-database write failure through the forwarding VFS. At least two main-file callbacks fail during SQLite's statement and failure handling. The harness does not label the second callback as a specific restoration stage. Reopen verifies the exact baseline record, retry identity, contiguous cursors, and tail.
- A private 64 MiB APFS image on macOS filled until the operating system reports `ENOSPC`. After freeing the bounded filler, close and reopen verify the exact baseline and an idempotent retry at the next cursor.

Run the complete current suite with:

```sh
cargo test --all-features --all-targets
```

These checks establish adapter behavior on the measured host. A process-kill test is not a power-cut test. `synchronous=FULL` was read back, but device-cache flushing, directory-entry persistence, and sudden power removal were not measured.

## Failure evidence boundaries

[`tests/sqlite_faults.rs`](../../../tests/sqlite_faults.rs) contains six bounded
fault tests. The logical `max_page_count` test and the private-filesystem test
answer different questions. The page quota deterministically exercises SQLite's
`SQLITE_FULL` transaction path. The macOS-only image test verifies a real
`ENOSPC` from a separate mounted filesystem, with a 128 MiB reported-size ceiling
and a 128 MiB filler-write ceiling.

The forwarding VFS is test infrastructure, not production code. It installs one process-lifetime wrapper around the bundled default VFS, preserves the real VFS context for every forwarded callback, and adds an aligned wrapper header before each delegated `sqlite3_file`. A failed `xOpen` closes any initialized real file before returning. A static method table, process-lifetime VFS registration, binary-wide test lock, and RAII fault reset keep callback lifetimes and global fault state controlled. These tests prove SQLite behavior for the selected `xWrite` and `xSync` schedules; they do not prove every filesystem, kernel, controller, or device failure schedule.

A `StoreWriteFailed` or `CapacityExceeded` result is treated as definitive and must not coexist with the attempted event. Only `CommitUnknown` permits either recovered state before the same-ID retry. The one-shot VFS and quota schedules compare each outcome with lookup and tail. The persistent main-write and full-filesystem schedules additionally close, reopen, verify prior exact bytes, retry the same ID, and read gapless history after resolution.

Format version 2 separates a stream name from its active and retired lifetimes.
It also stores lifecycle receipts and exact quota counters. Format-1 databases
migrate while the adapter holds exclusive ownership. The migration rebuilds only
the small lifetime-parent table. It leaves `event_records` and its payload pages
in place, checks foreign keys before commit, and updates the format marker in the
same transaction. A fixture verifies that the record table root page and exact
payload bytes do not change. A pre-commit fault leaves the valid format-1 schema
and history reopenable, and the next ordinary open completes the migration.
Newer formats remain refused before persistent SQLite settings change.

The OS lock is advisory. External applications that write the database, replace the database or lock file, or mutate directory aliases while the adapter is open are unsupported. The managed directory must be controlled by the application. Automatic failover and network filesystems are not covered.

Linux execution of the isolated-filesystem exhaustion case, broader VFS schedules, device power interruption, syscall tracing, directory durability, WAL behavior, and cold-cache isolated-host measurements remain open evidence. Runtime/SQLite throughput, CPU, memory, replay interference, and receipt latency are recorded separately in [performance evidence](performance-evidence.md), with their own stated scope. This remains process-restart evidence rather than a power-loss qualification.
