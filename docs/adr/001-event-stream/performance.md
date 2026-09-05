# First-principles performance and resource design

- **Status:** proposed engineering baseline; no benchmark results are claimed.
- **Decision:** [ADR 0002](../002-resource-budgets-and-performance-evidence/adr.md).
- **Storage work:** [ADR 0003](../003-sqlite-durability-and-storage-layout/adr.md).
- **Implementation contract:** [LLD](lld.md).

## 1. Start with work that must happen

An event must be read, checked, owned long enough to commit, saved, and eventually read by consumers. Every additional copy, allocation, lookup, queue, or notification needs a reason.

Make the common path easy to explain. Avoid doing work at all before trying to do it with a more complicated algorithm. Do not reduce durability or hide overloaded requests to improve a graph.

```text
source read
    |
    v
optional framing / decoding     bytes examined; retained partial input
    |
    v
owned event                     allocation or ownership transfer
    |
    v
capacity check -> stream queue  reserved bytes; queue node; notification
    |
    v
SQLite worker                   retry lookup; insert; index; tail update
    |
    v
SQLite storage layer            page changes; journal; database calls
    |
    v
kernel / filesystem / device    writes; flushes; waiting
    |
    v
commit result -> receipt        caller wake; subscriber notification
    |
    v
bounded page read -> consumer   database read; owned output; apply state
```

This is a path to measure, not a promise that every event causes a distinct system call. Batching, caching, journaling, and the operating system can combine work. Observe the selected build rather than infer counts from this diagram.

### Use a cost model before changing code

For one accepted append:

```text
receipt latency = wait for capacity
                + wait for its stream / storage worker
                + validation and retry lookup
                + transaction work and required disk-flush wait
                + result delivery
```

CPU time and waiting time are different. A slow flush can consume little CPU while dominating latency. Making a hash lookup faster will not fix that delay.

For a stable workload, average outstanding work is approximately arrival rate multiplied by average time in the system. For example, 10,000 events/s with 20 ms average completion time means about 200 outstanding events. This is a sizing estimate, not a worst-case memory bound. Bursts and slow disks still require explicit count and byte limits.

For illustration, copying each 1 KiB payload three additional times at 50,000 events/s moves about 146 MiB/s of extra payload data. That excludes envelope and allocator costs. Measure actual copies; do not label a shared buffer design “zero-copy” when framing, binding, storage, or delivery still copies bytes.

## 2. Account for every retained allocation

A memory budget must include capacity, not just the number of useful bytes. A small slice can keep a much larger allocation alive. A vector can reserve unused space. A queued caller can retain its event before it enters the main queue.

| Owner | What it may retain | Limit and release point |
| --- | --- | --- |
| Source/decoder driver | Current input, incomplete frame, pending decoded output | Per-session bytes/work; release after consumed output commits or the driver stops |
| Waiting append caller | Submitted event and wait state | Bound registered waiters as well as accepted queue entries; reject excess wait registration |
| Accepted append | Event allocation, queue node, result state | Global/per-stream bytes and count; release only when no worker still owns the input |
| Memory store | Saved record plus event-ID index | Separate history quota; release only through supported lifecycle policy |
| SQLite worker | Bound input, prepared statements, connection cache | Statement/cache limits; reset inputs after execution and bound cached statement count |
| Subscription | Page buffer, cursor, notification state | Per-reader and global limits; release on consume, cancellation, or termination |
| Runtime registry | Active coordinator and handle metadata | Active-handle/coordinator limits; remove only after the last safe reference |
| Telemetry | Samples and counters | Fixed-size counters and bounded samples; never an event-ID label per event |

External application memory is not fully controlled by the library. A caller can still create unlimited tasks or retain returned records. State this limit. Do not claim a whole-process cap based only on the internal queue budget.

```text
controlled memory budget = input and decoder capacity
                         + registered waiting-call capacity
                         + queued AND executing append capacity
                         + read pages and output conversion capacity
                         + registries and notification metadata
                         + runtime and adapter caches
                         + allocator overhead / measured headroom
```

Track memory-store history separately. Track kernel file cache and filesystem dirty data separately from application allocations. Avoid adding them to process RSS blindly; accounting overlaps differ by platform.

### Ownership and sharing rules

Move an owned buffer through the append path when its representation already fits. For borrowed input, make one bounded ownership copy at the public boundary or require the caller to supply owned data. Specify which path the API takes.

Sharing immutable records can reduce copies across consumers. It also adds reference-count operations and can extend allocation lifetimes. Compare shared record handles with owned small values under both one-reader and many-reader workloads.

Count a shared allocation once in global physical-memory estimates, but charge each subscriber for its permitted logical page size. Include outstanding references that prevent buffer reclamation. Do not put two independent counters in charge of releasing the same permit.

A tiny output slice should not retain a giant input allocation indefinitely. Copy that slice into a right-sized allocation if retained capacity would exceed the budget. This is a case where a copy is cheaper and simpler than preserving “zero-copy.”

## 3. Choose simple data structures around actual access

| Need | Initial choice | Why it fits | What to measure before changing it |
| --- | --- | --- | --- |
| Per-stream pending writes | Standard deque with count/byte limits | Push at one end, pop at the other; easy cancellation ownership | Growth reallocations, retained capacity, time under the queue lock |
| Find active stream state | Standard hash map with safe default hashing | Direct lookup; no unbounded global string interning | ID length, collisions, resize cost, idle removal, cache misses |
| In-memory ordered history | Append-only vector plus event-ID lookup | Cursor maps directly to a position while history is complete | Growth copies, large-stream pauses, map overhead; consider fixed chunks only if these matter |
| Decoder partial input | Growable byte buffer with a hard frame limit | Simple incremental state | Reallocation and rescanning; reserve conservatively, do not allocate the maximum per idle decoder |
| Read output | One limited page at a time | Simple ownership and predictable release | Bytes copied, page occupancy, fetch count, retained capacity |
| Subscriber wakeup | Change counter plus one wait registration | Many commits can share a notification | Wakeups per useful read, empty reads, lost-wake race tests |
| SQLite statements | Small fixed set of prepared statements | Avoid reparsing fixed queries repeatedly | Prepare count, reset errors, statement cache bytes |

Read input in one forward pass where possible. Retain a scan position so every new byte does not trigger rescanning the entire partial frame. Test one-byte chunks and many tiny frames, not just a large contiguous buffer.

Do not use an unsafe allocator, custom ring, intrusive list, or lock-free queue just because it has a better isolated benchmark. Compare the complete path, error handling, and retained memory. A standard queue with a short lock is the baseline.

Avoid spawning a thread or task for every event. Give each storage worker a bounded amount of work per turn. Do not busy-spin for lower latency. Idle streams should sleep, with only a bounded maintenance timer when there is maintenance to do.

## 4. Schema and index costs

The database schema determines how much metadata repeats and how many structures change per append. Start from the required queries:

```text
1. Find a stream's incarnation and bounds.
2. Find an existing record by incarnation + event ID.
3. Read records by incarnation + offset, in order, within a limit.
4. Commit a new record, retry identity, and tail together.
```

These queries justify two event access paths: ordered replay and unique retry lookup. Do not add indexes for arbitrary payload fields, timestamps, or speculative future queries. The core does not query payload meaning.

Use a small adapter-local stream key to avoid repeating long public stream names in every event and index. Persist its mapping to the public stream identity and incarnation. It is not a public cursor and may be rebuilt only through a defined migration/restore process. Compare this mapping with directly storing the bounded incarnation bytes; keep the extra mapping only if its space/query benefit is clear.

### Compare two physical layouts

The following are schematic candidates. They do not replace the LLD's logical contract or constitute final DDL.

```text
A. Ordinary rowid table
   event row:
       rowid, stream_key, offset_be8, event_id, schema, payload
   unique index: (stream_key, offset_be8)
   unique index: (stream_key, event_id)

B. Composite primary-key table, WITHOUT ROWID
   primary key: (stream_key, offset_be8)
   row value:   event_id, schema, payload
   unique index: (stream_key, event_id)
```

SQLite's `WITHOUT ROWID` layout can help some composite-key workloads, but large rows can change the tradeoff. It is not an automatic win. Evaluate both layouts with small and large payloads. [SQLite layout guidance](https://www.sqlite.org/withoutrowid.html)

Record table bytes, each index's bytes, average key/row sizes, query plans, page reads/writes, and durable latency. Test repeated short schema names versus an optional schema dictionary. A dictionary saves repeated bytes but adds lookups and migration state; do not introduce it without evidence.

Keep offsets as fixed eight-byte big-endian values in the baseline proposal so all `u64` values sort correctly. Validate encoding and comparisons around the signed boundary. A smaller signed representation would change the public range and needs an explicit API decision; it is not a silent storage optimization.

A unique event index can refer to the original row. It does not require a second full copy of the payload in a receipt table. When later retention removes payloads, ADR 0006 must revisit what information a valid retry still needs.

### Know the work caused by one append

```text
new event:
    lookup retry ID
    insert event row
    maintain ordered/retry indexes required by chosen layout
    update stream tail
    commit

identical retry:
    lookup retry ID
    compare schema and original bytes
    return original record; no new history entry
```

The actual database page changes and flush count depend on the layout and journal. Trace them. Test retries with maximum-size payloads too: exact equality has a real read/compare cost. Do not weaken it to hash equality to make retries look cheap.

Read by key range, not growing SQL `OFFSET`. Use query plans to verify that larger histories do not turn page reads into full scans or sorts. Bound row materialization and temporary query memory. Budget storage caches per connection; adding readers can multiply cache capacity.

## 5. Follow commits through the operating system

Durability is about where data has reached, not whether one function returned. A write handed to the operating system is different from the configured durability boundary.

```text
application input buffer
    -> SQLite pages and journal decisions
        -> SQLite platform I/O layer
            -> system calls
                -> filesystem and cached/dirty pages
                    -> device requests and flush completion
```

Do not bypass SQLite to flush or write its internal files independently. Let its transaction protocol control them. Observe that protocol and validate the adapter's configuration. SQLite documents the assumptions behind atomic commits, including storage and operating-system behavior. [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html)

On Linux, syncing a file does not necessarily persist its directory entry; directory durability can require a separate sync. On Apple platforms, the documented distinction between `fsync` and `F_FULLFSYNC` matters when examining device-cache behavior. These are reasons to inspect the actual platform path, not apply Linux assumptions everywhere. [Linux fsync](https://man7.org/linux/man-pages/man2/fsync.2.html), [Apple fsync documentation](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fsync.2.html)

For each supported configuration, answer:

- Which calls write journal data, database data, and metadata?
- Which call establishes the acknowledged persistence boundary?
- What work happens before the receipt, and what is deferred to a database checkpoint?
- What happens if a write, flush, close, or checkpoint fails?
- Are the filesystem, device, and SQLite build supported and recorded?

WAL separates log appends from copying data into the main database. That later work still costs I/O, and long reads can interfere with checkpoint progress. Test the whole cycle, including WAL growth and cleanup. [SQLite WAL](https://www.sqlite.org/wal.html)

SQLite synchronization settings affect persistence behavior. Read back the selected settings and record them in every run. A comparison that changes those settings must be labeled as a different durability configuration. [SQLite synchronization settings](https://www.sqlite.org/pragma.html#pragma_synchronous)

### Measure both active work and waiting

| Observation | Question it answers |
| --- | --- |
| User CPU samples | Are we parsing, hashing, copying, allocating, or executing SQL? |
| Kernel CPU / system-call samples | Are we spending work on I/O submission, locks, or memory management? |
| Worker wait time | Are we waiting for disk, another operation, or our own scheduling? |
| Context switches and wakeups | Are too many small tasks/notifications moving threads on and off CPU? |
| Page faults and cache misses | Are large buffers or working sets causing expensive memory access? |
| Bytes written and flush duration | Is index/journal work larger than the payload suggests? |
| Idle CPU and timer frequency | Does an unused runtime actually stay quiet? |

On Linux, use available performance counters and process-scoped profiling. Kernel counters include context switches and page faults; access depends on the host's profiling permissions. Record unavailable counters rather than weakening host settings automatically. [Linux perf documentation](https://docs.kernel.org/admin-guide/perf-security.html)

On macOS, collect equivalent process CPU, allocations, scheduling, and filesystem evidence with available platform tools. Record the tool/version and sampling scope. Tool availability and counter meaning differ across hardware and OS versions. Do not compare unavailable counters as zeros.

Profile short representative runs separately from uninstrumented throughput runs. Tracing can change timing and add memory. Never globally drop caches, change kernel tunables, or alter filesystem settings on a shared workstation just to improve a benchmark. Use an isolated test environment for those experiments.

“Kernel-level understanding” means explaining where time and bytes go down to the relevant calls and waits. It does not mean implementing a kernel module, bypassing the filesystem, or writing our own storage engine.

## 6. Ingestion, replay, and background work share a machine

A fast writer that prevents reads from progressing is not a usable event stream. A fast replay that stalls every commit is not usable either.

Test one hot stream and many quiet streams. A hot stream is simply the stream receiving most of the traffic. Measure each quiet stream's wait, not just aggregate throughput. Mix tiny events with maximum-size events to expose byte-budget and scheduling mistakes.

Store-backed subscriptions avoid a second history buffer, but may repeat reads across subscribers. Measure store query count and bytes read as fan-out grows. If repeated reads dominate, evaluate one bounded shared page cache before adding a separate live-delivery architecture. Eviction must affect speed only, never correctness or history availability.

Snapshots, retention, replication, parser recovery, and database checkpoints also consume CPU and I/O. Give each a byte/count/time budget per turn and measure foreground latency while it runs. If a background feature requires unlimited temporary space or monopolizes the writer, it has not completed its ADR.

Internal group commit can reduce fixed commit overhead when enough work is queued. It also adds batch wait, couples failure outcomes, and can hurt quiet-stream latency. Compare count-, byte-, and time-limited batches under the same durability settings. Keep single-append transactions if the measured benefit does not justify the extra state. Never delay a lone write indefinitely while waiting for a batch.

## 7. Reproducible evidence and release gates

Run the LLD workload matrix in a headless executable. The verification UI can display results after the timed region. Keep payload fixtures deterministic and avoid measuring unrelated rendering or development logging.

Start with: empty/idle, one stream, many streams, duplicate-heavy input, split-byte decoding, replay larger than RAM, stalled subscribers, and slow storage. Then combine the features added by each ADR. Use offered load that can exceed capacity so waiting and rejection remain visible. A client that waits for each response before sending the next request can hide queue buildup; include independent-rate load generation too.

Report:

- p50/p95/p99 receipt and delivery latency, separating queue and commit time;
- offered, accepted, rejected, failed, and committed events/bytes;
- CPU time per committed event and per decoded MiB;
- allocations, copied bytes, peak retained capacity, and process memory;
- query count, data/index/journal size, bytes written, and flush duration;
- idle CPU, wakeups, context switches, and relevant page-fault/cache counters;
- recovery time, cleanup backlog, and temporary disk space.

Denominators matter. A rejected request consumes work but does not become a committed event. Report reject/conflict cost separately so dropping more work cannot look like a performance improvement.

Use repeated baseline runs to estimate variation. Publish raw samples and the comparison rule. Set workload-specific numeric limits before a phase is marked verified; a universal throughput number is not the goal. Require the same correctness results, persistence settings, and configured capacities on both sides of a comparison.

An illustrative result shape follows. `null` means not measured. This example is deliberately not passing evidence:

```json
{
  "adr": "0003",
  "revision": "record-the-tested-revision",
  "scenario": "append-with-large-replay",
  "status": "not-run",
  "environment": {
    "os": null,
    "cpu": null,
    "filesystem": null,
    "sqlite_version": null,
    "journal_mode": null,
    "synchronous": null
  },
  "limits": { "accepted_bytes": null, "registered_waiters": null },
  "measurements": {
    "receipt_p99_ms": null,
    "cpu_ns_per_commit": null,
    "peak_retained_bytes": null,
    "flushes_per_commit": null
  },
  "budgets": { "receipt_p99_ms_max": null, "peak_retained_bytes_max": null },
  "correctness_passed": false,
  "raw_evidence": []
}
```

Store evidence using the existing [verification structure](../../../verification/STRUCTURE.md). A phase is not complete while required budget or measurement fields remain missing. Kernel traces explain behavior; automated checks and repeatable end-to-end results establish the acceptance evidence.

## 8. Stop optimizing when the extra state costs more than it saves

For every proposed optimization, write five short answers:

1. Which measured cost dominates, on which workload?
2. What simpler change was tried first?
3. What state, synchronization, dependency, or unsafe code does this add?
4. What improves, what gets worse, and how repeatable is the difference?
5. Can the original implementation be restored without a data-format or API change?

Keep the optimization only when the tradeoff is worthwhile and its new failure paths are tested. Reuse the same cost report after every roadmap phase. Removing unnecessary work and preserving clear ownership are the default optimization strategy.
