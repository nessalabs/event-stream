# Evaluate bounded SQLite group commit

Status: bounded request/result contract and sequential adapter fallback implemented.
Grouped SQLite transactions are implemented behind the batch port. Runtime
collection and controlled performance measurements remain pending.
No batch optimization is selected or claimed faster by this document.

## Why the current path cannot form a batch

Several independent appends could share one durable commit. This could reduce
disk flushes. Each caller must still receive its own result after that commit,
and a conflict in one item must not silently discard another valid item.

The current [runtime worker](../../../src/application/runtime.rs) removes one
append from a stream queue, awaits `EventStore::append_atomic`, and then takes
another. SQLite advertises one concurrent writer, and runtime configuration
rejects more storage workers than the adapter supports. Increasing the
per-stream work quantum changes fairness, not the number of outstanding writes.

The [SQLite adapter](../../../src/infrastructure/sqlite.rs) submits one append
command and immediately processes it in an `IMMEDIATE` transaction. That
transaction commits before the worker processes another append. Consequently,
draining the adapter queue cannot form useful batches from this runtime: the
next append has not been submitted yet. Direct concurrent adapter callers can
populate the queue, but that is a different workload.

## Candidate boundaries

Compare a typed batch port with adapter-side draining. Keep the existing path
as the control. A candidate batch port returns one outcome for each input in
input order. It must not imply public atomic multi-stream writes. A default
adapter could perform ordered individual appends while SQLite groups physical
commits, provided each adapter preserves the same per-item guarantees.

Runtime collection must use already admitted work. It must not create a second
unaccounted queue. Bound the batch's record count, charged bytes and maximum
collection wait. Collect across ready streams so one event from each of many
agents can benefit too. Preserve queue order within each stream and the
existing fairness quantum. If multiple stream gates are needed, acquire them
in one stable order compatible with lifecycle and maintenance operations.

Before implementation, define the typed request/result shape and how malformed
adapter responses are rejected. An interrupted batch must retain enough
identity to report each event's outcome. Do not convert a partly successful
sequential fallback into one group error that loses earlier receipts.

## Correctness conditions

- Validate limits and arithmetic before allocating the batch. Process items in
  their supplied order. Identical retries deduplicate. Changed payloads under
  an existing ID conflict without consuming a cursor.
- Isolate deterministic item errors with ordered preflight or savepoints if
  necessary. Checks for retention, replica backlog and lifecycle must observe
  earlier accepted items in the same transaction.
- Release newly committed receipts only after successful durable commit. Keep
  accepted work owned when a caller cancels, and keep shutdown waiting until
  all outcomes or unresolved event IDs are registered.
- A definite rollback may return retryable failures or use a documented
  individual fallback. An uncertain commit must never trigger blind fallback.
  Reconcile each newly staged event by its original stream, ID and exact bytes.
  A mixture of present and absent newly staged rows from one atomic transaction
  is corruption; an unreadable result remains unknown. Previously committed
  deduplicated rows must not be confused with new inserts in that check.
- Notify subscribers after commit. Updating several stream tails must not
  introduce a cross-stream ordering or atomic-write promise.

## Controlled experiment

Use identical source fixtures and SQLite settings: DELETE journal, FULL
synchronization, MEMORY temporary storage and a 4 MiB cache. Preserve database
page limits. Run the single-append control beside candidate batch sizes 4, 16
and 64. Evaluate waits of zero, 50 microseconds and 250 microseconds. A 1 ms
wait is an explicit latency sensitivity case, not a default.

Measure actual collection delay as well as the requested wait. Executor timers
and OS scheduling may wake later than a microsecond-scale deadline. Start with
collecting work that is already ready. Do not introduce busy waiting or one
timer per event to make a small requested delay look precise.

Use 128-byte, 4 KiB and 64 KiB events with corresponding finite byte caps. Test
saturated single-stream input, many streams, sparse arrivals, and scheduled
arrivals that continue independently of prior receipt latency. Include 1,000
and 100,000 stream populations, and distinguish stored streams from concurrent
tasks and sustained producers.

Record offered, accepted, inserted, deduplicated, rejected and failed events;
p50/p95/p99 receipt latency; scheduling delay; queue delay; store time;
commit-to-delivery bounds; CPU; context switches; allocation traffic; retained
bytes; RSS; queue peaks; database/journal size; and actual sync calls/duration.
Use fresh processes, archived source and binaries, repeated interleaved runs,
and an explicit comparison rule chosen before qualification. Do not reward
throughput that improves by rejecting more input or weakening durability.

Required failure cases include rollback after writes, lost commit reply,
process kill before commit and after commit/before replies, full capacity,
conflicting IDs mixed with valid input, cancellation, and shutdown. Reopen and
verify every expected ID, payload and per-stream cursor before accepting a
performance sample as valid.

Keep the simple path if gains are inconclusive. A useful result must reduce
flush cost and improve a declared workload while keeping sparse/mixed latency
and memory within predeclared budgets. This evaluation currently has no fresh
samples and does not satisfy that decision gate.

## Higher-rate population evidence

[Round 106](evidence/round106-report.md) repeats a 10,000-stream workload at
4,000 offered events/s. All three SQLite runs fill the bounded 256-task harness
and reject excess offers before runtime submission. Every submitted call
succeeds, but the population is only partially covered. Memory accepts all
offers. This makes the offered-rate gap concrete and motivates this experiment.
It does not isolate flush cost or prove that batching will fix the gap.
Use this exact profile as one control/candidate comparison. Keep rejections
and uncovered streams visible alongside receipt latency.

The [typed contract](append-batch-contract.md) defines bounded requests, per-item
outcomes and malformed-response validation. Round 107 implements it without
changing the production runtime append path.

Round 109 implements the SQLite override and validates shared sync work with
real database recovery and fault tests. The runtime still calls individual
append; the higher-rate population gap has not yet been remeasured or closed.
