# Bounded append batch contract

A storage call can carry several already accepted events. Each event still has
its own result. One conflict must not erase a successful peer. A batch does not
promise atomic writes across streams.

The typed boundary now exists in `application::append_batch`. `EventStore` has
an ordered sequential fallback. SQLite now overrides this port with a grouped transaction. The runtime still
submits individual appends. Controlled throughput and memory comparisons remain
required before enabling runtime collection.

## Input and ownership

`AppendBatch::new(Vec<AppendRequest>, AppendBatchLimits)` validates nonempty
input, at most 64 records, and a positive logical byte budget. Each request
contains its original `StreamKey` and `NewEvent`. Input order cannot be changed
after construction. The caller must allocate within its admission budget before
construction; rejecting a vector does not undo the cost of allocating it.

The charge includes all allocated request-vector slots, including spare
capacity, plus each event's logical envelope and stream name. Checked addition
rejects overflow. This charge is not an exact RSS or transaction-memory cap.
Receipt vectors, adapter caches and transaction state need separate accounting
when the runtime selects its group size. The contract does not copy payloads.

```text
admitted queue (still charged)
      |
      v
bounded batch [A, B, retry A, conflict B, C]
      |
      v
store outcomes [inserted A, inserted B, dedup A, conflict, inserted C]
      |
      v
validate count and identities -> resolve each original caller
```

The fallback calls `append_atomic` in order and preserves every result in its
original position. It does not use one outer `Result` that could hide earlier
successful receipts when a later append fails. A later exact retry can still
succeed after another item hits a capacity limit.

The eventual runtime must own the batch until all operations finish, even if an
individual caller disconnects. Dropping the direct fallback future can stop it
between individual calls; this low-level port does not invent whole-batch
acceptance. No new queue or detached worker is added by the contract.

## Output validation

Call `batch.validate_results(&results)` before publishing any result. It rejects
missing or extra outcomes, wrong stream lifetimes, wrong cursor versions, zero
committed offsets, changed event IDs/schema/payload, and mismatch between an
ID-bearing error and its request. A failure is `StoreCorrupt`. The runtime must
retain original unresolved identities instead of silently truncating a zip.
Validation proves shape and identity only. It cannot prove durable storage or
that a claimed insertion is a real insertion; recovery tests must prove those.

A future SQLite implementation must keep track of newly staged records apart
from rows that existed before the transaction. A retry of an earlier item in
the same transaction depends on that transaction committing too. On rollback,
it must not return a successful dedup receipt for a row that never committed.

## SQLite integration order

Round 108 extracts one-event transaction work while retaining single-append behavior.
Do not nest the current `append` function in a batch: its dedup branch rolls
back its transaction, and its insertion branch commits it.

Replication preflight checks backlog and time, but applying replication changes
also updates the durable clock and counters. Some checks happen after mutation.
Either calculate the complete per-item write plan first or explicitly roll back
a per-item savepoint. A SQL failure can automatically roll back the outer
transaction, so continuing after an arbitrary database error is unsafe.

A failed commit is a group question. Reconcile newly staged identities using
original bytes and expected cursors. All present means committed; all absent
means rolled back; mixed presence means corruption. An unreadable result stays
unknown. Do not blindly retry the group. Deterministic conflicts and records
that existed before the group need separate treatment.

Only then collect already-ready runtime work across streams with bounded count
and bytes. Preserve queue order and stream gates, lifecycle exclusions,
shutdown tracking and subscriber notifications. Start with no intentional wait.
The [evaluation matrix](group-commit-evaluation.md) remains required before
selecting an optimized path.

## Evidence

Round 107 tests the default port on Memory and real SQLite. It verifies ordered
retry/conflict results across two streams, preservation of earlier success
across a capacity failure, later deduplication, exact SQLite data after reopen,
input limits and rejected malformed outcomes. Group rollback, cancellation and
unknown-commit behavior are still integration gates for the future optimized
adapter; the fallback tests do not establish those properties for batching.

## Transaction staging now implemented

`stage_append` in the SQLite adapter reads and writes through its caller's
transaction reference. It cannot be called on a bare autocommit connection and
does not own COMMIT or ROLLBACK. A returned receipt describes
staged data until the owner commits. Its retry branch no longer rolls back a
transaction that could contain earlier staged events.

Single append now rolls back explicitly for every staging error. This includes
errors from replication metadata updates. If SQLite already rolled back the
transaction, the adapter keeps the original error. If explicit rollback fails,
it faults the store rather than relying on a transaction destructor that cannot
report failure to the caller.

Uncertain single-append commits are reconciled only after the connection leaves
the transaction. Otherwise, reading the connection's own uncommitted row could
look like proof of durability. Reconciliation also compares the full staged
record, including its cursor, instead of checking only whether the ID exists.

The new tests stage insert/retry/conflict sequences in one real transaction,
check that another connection sees no premature writes, and check committed and
rolled-back history. A second test injects a failure after writing the replication
clock and verifies rollback of the clock, event and tail before a successful
retry and reopen. Existing SQLite fault tests cover the unchanged public path.

This is still preparation. A staging error can follow mutation. Group processing
must therefore explicitly undo that item's changes or abort the group; calling
the helper repeatedly and ignoring an error would violate the contract. Group
outcome reconciliation and bounded runtime collection remain to implement.

Review also aligned the successful-retry rollback exit: a rollback failure now
faults the adapter, matching staging-error rollback behavior.

## SQLite grouped transaction implementation

Round 109 adds one worker command for a bounded batch. Before cloning input,
the adapter checks its aggregate logical charge against `max_record_bytes`.
This deliberately keeps one group within the existing per-command byte limit.
A near-limit event may fit individual append but not the batch envelope. Future
runtime collection must send such an event through individual append, rather
than rejecting already admitted work or raising the adapter queue budget.

The worker processes inputs in order inside one IMMEDIATE transaction. Each
item uses a savepoint, a point to which SQLite can undo that item's changes.
Known per-event rejections roll back to that point, leaving valid peers intact.
Later items see earlier staged tails and replica backlog. Unexpected failures
abort the whole group. SQLite auto-rollback and failed rollback are checked;
corruption faults the worker. The savepoint cost is bounded but has not yet
been compared against a complete preflight approach.

On a definite group rollback, this implementation returns a failure for every
item. It does not return an insertion or same-group deduplication receipt for
uncommitted data. Existing records remain unchanged and can be retried. A
conflict alone is isolated and does not abort valid peers.

Newly inserted records are tracked separately from deduplication outcomes.
After an uncertain commit, every new record must match its original bytes and
cursor. All present resolves success. All absent resolves failure. Mixed
presence faults the store. An unreadable result or open transaction returns
an unknown outcome for each original event ID and faults the worker. No blind
retry happens inside reconciliation.

Once submitted, the worker owns the batch even if the caller drops its future.
Close drains queued commands. This preserves the adapter boundary; the runtime
still needs its own multi-item admission, stream gates, notification and shutdown
integration before it can take advantage of the port.

The round 109 tests exercise rollback, lost reply, preexisting versus same-group
retries, replica backlog isolation, caller cancellation, close/reopen, aggregate
size rejection and VFS write/sync/rollback failures. A dedicated recorder test
observes 24 SQLite sync callbacks for eight individual inserts and three for
one group, with exact data after reopen. This verifies shared commit work, not
an end-to-end throughput or memory improvement. Group process-kill schedules are now covered in round 110; full runtime resource
qualification remains open.

If the worker's reply channel closes after acceptance, every item returns
`CommitUnknown` with its original ID. A lost reply cannot prove rollback. This
is distinct from a rejected submission, where the command was never accepted.

[Round 110](evidence/round110-report.md) terminates real child processes before
and after grouped commit and verifies recovery plus whole-group retries. The
observer proves transaction state; this is not a power-loss qualification.
