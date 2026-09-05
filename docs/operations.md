# Running an event stream locally

Choose who owns the history before choosing an adapter. A Memory store owns
temporary history for one process lifetime. SQLite keeps history across a
successful close and restart. Replication maintains a separate copy of one
origin's history; it does not elect a replacement writer.

All examples below use public production APIs. They assert their expected
results and exit with an error if an operation fails. They are correctness
walkthroughs, not performance benchmarks.

## Temporary history

```sh
cargo run --locked --example projection_reconnect
```

[The example](../examples/projection_reconnect.rs) uses Memory, subscribes to live
events, disconnects, and resumes after the application's last applied cursor.
It compares replayed state with live state. A rejected application transition
does not advance that checkpoint.

Memory history disappears when the store is dropped. A cursor saved outside
the process cannot recover that lost history. Use SQLite when restart recovery
is required. Persist an application's projection and its applied cursor
together; the library cannot make external effects exactly once.

## Durable history, full capacity, and backup

Pass a directory that does not exist. The example keeps its files for inspection
and refuses to reuse an existing directory.

```sh
cargo run --locked --features sqlite --example local_recovery -- /tmp/event-stream-local-demo
```

[The walkthrough](../examples/local_recovery.rs) performs these operations:

1. Initialize SQLite and configure a small database page quota.
2. Append until SQLite returns `CapacityExceeded`. At most 128 attempts run.
3. Read every committed event with two-record pages and check its exact bytes.
4. Retry an already committed event and require the original receipt.
5. Drain the runtime and require a closed store with no unresolved work.
6. Copy the closed database, then flush the backup file and its directory.
7. Reopen the original database and check every committed record again.
8. Restore the backup into a new destination through `SqliteRestoreManager`.
9. Retry the same restore operation and require the same receipt.
10. Reject the old cursor, read using the mapped stream identity, and append
    the next record using an explicitly larger database quota.
11. Close and reopen the restored database and verify that final append too.

The SQLite page quota limits main-database pages. It does not limit temporary
journal files or reserve free filesystem space. A full disk and a full queue
are separate failure cases. This example exercises the page quota, not a
physical disk-full fault.

Stop acquiring new source data when you cannot store it, or keep it in an
application-owned bounded durable spool. Do not silently drop an event because
an append failed. A definitive capacity rejection can be retried after capacity
is available. A `CommitUnknown` result requires reconciliation using the same
event identity and bytes; it must not be treated as proof that nothing committed.
The example reports unexpected failures rather than classifying them as quota
success.

The backup copy is safe here because successful shutdown has closed SQLite and
the example exclusively owns its new directory. No other writer may reopen
the source during the copy. Do not copy a live SQLite database file this way.
A production online-backup workflow needs coordinated SQLite backup operations.
The library currently provides controlled restore, not an online backup service.

Restoring creates new stream incarnations. An incarnation identifies one
lifetime of a stream. Read the restore mapping before rebuilding application
state. An old cursor must not silently enter the restored history. The example
replays from zero using the new identity; it does not automatically translate an
external application's projection checkpoint.

## A separate durable replica

```sh
cargo run --locked --features sqlite,replication --example replicated_recovery -- /tmp/event-stream-replica-demo
```

[This example](../examples/replicated_recovery.rs) creates two SQLite stores. It
attaches one required replica from the beginning of an empty origin stream.
The replica has an 8 KiB backlog budget. Appending eventually reaches that
budget and returns `ReplicaBacklogExceeded`.

The application pauses input and retains the rejected event. Both stores close
and reopen. `Runtime::replicate_once` transfers one record per batch, retries each
completed operation with the same identities, and checks the acknowledgements.
Once the backlog drains, the application retries its rejected event. The
stores close and reopen again. The origin's acknowledgement must still show
zero backlog. Every replicated cursor, identity, schema and payload must match
exactly.

```text
origin append -> protected backlog -> destination commit -> origin acknowledgement
      |                                                           |
      +---- refuse more input at the configured limit <------------+
                                       acknowledgement releases backlog capacity
```

This is a local two-store example. The application owns networking,
authentication, retry scheduling and source acquisition. The origin runtime
owns admitted transfers through caller cancellation. Require a successful,
closed runtime shutdown before closing the destination adapter. The runtime
does not close that caller-owned destination. Its scoped transfer methods use
the production `ReplicationDriver` internally. See the
[runtime integration contract](adr/008-local-first-replication/runtime-integration.md)
for admission and shutdown rules. A destination is a copy of its origin, not
an independent writer for that same stream.

Starting from the beginning requires complete retained history. A destination
that lacks a retained prefix needs snapshot bootstrap instead. The shared
[full recovery fixture](../verification/fixtures/journal_replication.rs) covers
snapshot-plus-suffix bootstrap, history cleanup, restart, and later catch-up.
Restoring an origin rotates its identity and detaches replication state; do not
resume old replication requests against it as if it were the same origin.

## Guarantees and limits

The currently tested durable profile is process restart on local macOS SQLite.
The examples do not establish power-loss safety, network filesystem support,
automatic failover, or release performance budgets. See the
[SQLite evidence](adr/001-event-stream/sqlite-evidence.md) and
[completion audit](adr/001-event-stream/completion-audit.md).

Run the operational regression cases with:

```sh
cargo test --locked --all-features --example local_recovery --example replicated_recovery
```
