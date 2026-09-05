# Resource accounting for the event-stream foundation

- **Reviewed:** 2026-09-04
- **Scope:** ADR 0001 runtime, memory store, SQLite store, and optional ingestion
- **Source of truth:** the production types under [`src`](../../../src)

This report explains what the implementation retains and when it releases it. The limits below are configuration limits and cost formulas. They are not measurements of allocator traffic, resident memory, filesystem cache, or disk writes. Measured results live in [performance evidence](performance-evidence.md).

## Accounting units

The runtime and both stores use one logical event size:

```text
E = payload bytes
  + UTF-8 event-ID bytes
  + UTF-8 schema-ID bytes
  + 128 bytes of envelope allowance
```

[`NewEvent::accounted_bytes`](../../../src/domain/model.rs) computes `E`. Identifiers are at most 256 UTF-8 bytes. `Payload` uses an exact-size `Arc<[u8]>`, so slicing a larger input buffer cannot hide unused payload capacity.

The 128-byte allowance is a stable logical charge. It is not the measured size of a `Record`, map entry, task, channel node, allocator block, or SQLite value. Count limits bound those objects separately.

The default limits are finite:

| Area | Default limit |
| --- | ---: |
| One runtime event | 1 MiB logical bytes |
| Runtime queued appends | 1,024 records and 16 MiB |
| One stream's queued appends | 128 records and 4 MiB |
| Waiting producers | 1,024 callers and 16 MiB |
| Stream coordinators | 1,024 |
| Concurrent reads | 1 |
| Waiting reads | 1,024 |
| Subscriptions | 1,024 globally, 128 per stream |
| One page | 256 records and 1 MiB |
| Internally buffered pages | 16 MiB |
| Diagnostic samples | 256 |
| Maintenance operations | 8 operations and 64 KiB of lifecycle request charge |
| Concurrent cleanup turns | 1 |
| One cleanup turn | 256 records and 2 MiB logical bytes |
| Memory-store history | 100,000 records and 64 MiB logical bytes |
| Memory-store streams | 10,000 streams and 4 MiB logical metadata |
| Memory-store lifecycle receipts | 10,000 receipts and 4 MiB logical charge |
| Memory-store retired lifetimes | 10,000 lifetimes and 4 MiB logical metadata |
| SQLite command queue | 256 commands |
| SQLite page request | 1,024 records and 4 MiB |
| SQLite connection cache target | 4 MiB |
| Ingestion sessions | 64 active, 64 waiting |
| One ingestion input buffer | 1 MiB retained input |
| Declared decoder state | 1 MiB per decoder, 16 MiB globally |
| One decoder output step | 128 items, 1 MiB, 65,536 work units |
| One decoder drive | 1,024 steps, 65,536 items, 16 MiB output, 1,048,576 work units |

Applications may select smaller or larger finite values. Configuration validation rejects zero limits and inconsistent combinations. Store capability checks reject runtime record or concurrency settings that the selected store cannot support.

## Ownership at a glance

```text
producer future
    │ admission succeeds
    ▼
runtime coordinator queue ──► runtime worker ──► EventStore operation
    │                              │                    │
    │ caller may cancel            │ owns completion    │ owns durable action
    └──────────────────────────────┴────────────────────┘

store page ──► runtime unread subscription buffer ──► application Arc<Record>
                 page permit held                       application-owned
```

Admission control means deciding whether the runtime has capacity to accept work. Before admission, the producer owns its event and may receive `Overloaded` or `AdmissionTimeout`. After admission, the runtime owns the append through a store outcome even if the caller drops its future.

## Runtime append path

The global queued-event bounds are:

```text
queued record count <= appends.max_queued
sum(E for queued and in-flight appends) <= appends.max_queued_bytes
```

The same checks apply per stream. Waiting producers have separate count and logical-byte bounds. Therefore the runtime can retain at most the configured queued logical bytes plus the configured waiting logical bytes for these two populations. This is a logical bound, not an allocator-byte sum.

Each accepted append is owned by one `PendingAppend` in a per-stream `VecDeque`. The global state also keeps a bounded unresolved identity entry keyed by `(StreamKey, EventId)`. The ready queue contains at most one scheduled entry per coordinator. `scheduling.max_coordinators` bounds coordinator maps, stream keys, queues, write locks, notifications, and ready entries.

Cloning has these costs:

- `Payload::clone` increments an `Arc`; it does not copy payload bytes.
- Cloning `StreamId`, `EventId`, or `SchemaId` creates a new exact-size boxed string.
- Admission clones the event into `PendingAppend`. The producer future retains its original event until it returns or is cancelled.
- A worker clones the pending event for the owned store task. That clone shares payload bytes and copies the two identifier strings.
- The unresolved map and ready queue copy bounded stream and event identifiers.

The runtime byte counters charge `E` once per queued or waiting request. They do not charge the copied identifier boxes, `Arc` control blocks, `VecDeque` slots, hash-table capacity, oneshot channel, task allocation, or lock state. Count limits keep those objects finite.

A cancelled waiting producer drops an `AdmissionWaiterGuard`. Cleanup runs on the runtime's captured Tokio executor. Until that cleanup runs, its count and bytes remain charged, so cancellation cannot admit replacement work early. A cancelled accepted producer only drops its result receiver. The queue item and worker continue to the store outcome, then release queue count, queue bytes, unresolved identity, and the channel sender.

The fixed diagnostic ring retains at most `diagnostics.capacity` samples. Samples contain an enum and static detail string. Cumulative counters saturate at `u64::MAX`; they do not allocate per operation. Calling `diagnostics` creates an application-owned copy of the bounded sample ring.

## Read pages and subscriptions

Every metadata call and page read first acquires one of `reads.max_waiters` waiter permits. A page read then reserves its requested `max_bytes` from the global `reads.max_buffered_page_bytes` semaphore before waiting for a read slot. Metadata reads only wait for a read slot. Every caller keeps the same queued semaphore future across lifecycle notifications, so a busy runtime cannot repeatedly move it to the back of the fair queue. All page reads acquire permits in the same page-then-read order, which avoids a resource-order cycle.

After admission, a detached task owns the store future, read slot, active-I/O count, and page permit. Cancelling the caller does not release those resources until the store operation stops. This keeps shutdown from closing the store while cancelled I/O still uses it.

For `read_after`, the page permit is released when the page is returned. The returned `Page` belongs to the application. For a subscription, the permit stays with the unread `VecDeque<Arc<Record>>`. Delivering the last buffered record replaces that deque with a new empty deque and releases the permit. This frees pointer capacity that the next page would replace rather than reuse. Subscription termination and drop use the same page-release helper. An application can retain delivered `Arc<Record>` values without a library limit; that memory is outside the runtime's internal page budget.

The runtime reserves the requested page-byte limit rather than the page's final length. This makes concurrent internal page ownership conservative for conforming stores. It still does not measure vector slots, record envelopes, identifiers, or allocator rounding.

One SQLite detail weakens that statement into a logical bound. [`read_page`](../../../src/infrastructure/sqlite.rs) decodes the next row before deciding that adding it would exceed the page limit. A byte-limited page may transiently hold its accepted records plus one candidate record of up to `max_record_bytes`. Decoding currently obtains a `Vec<u8>` from `rusqlite` and copies it into the exact-size `Payload`, so that candidate can temporarily have two Rust payload buffers. The configured page permit does not charge this look-ahead record or temporary copy. This is bounded, but it is a known accounting gap if `reads.max_buffered_page_bytes` is interpreted as a strict allocator ceiling.

Each subscription owns its cursor state, lag timestamps, and one unread page. Its stream coordinator owns one strong `Arc` in a stable optional slot. Global and per-stream subscription counts bound live state. A dropped subscription clears its page and permit immediately; executor cleanup decrements its registration count and clears its exact slot. A runtime-detected terminal error does the same registration transition before returning the error. The executor owns the decrement task, so cancelling `next` cannot strand the slot between those steps. Until cleanup runs, the slot stays charged. A retained terminal handle no longer consumes subscription admission after cleanup completes.

Registration takes a free slot index or appends one slot. It does not scan existing subscribers. Removal clears that exact index and adds it to the bounded free-index vector. The slot and free-index vectors are released when the coordinator has no subscriptions.

The sweeper runs every `subscriptions.sweep_interval`. Each coordinator keeps stable optional subscription slots and a rotating index. One tick examines at most `subscriptions.checks_per_sweep` physical slots, including empty reusable slots, so subscription inspection and temporary subscriber `Arc` clones are bounded by that setting. Registration uses a bounded free-index vector and does not scan existing subscribers. Removal clears one exact slot. Empty coordinators release both slot vectors.

The whole tick is not yet bounded by the subscriber setting. It first clones every coordinator `Arc` into a temporary vector, which costs `O(C)` time and pointers for `C` live coordinators. Expiry clears the unread page and releases its permit even when the consumer never polls again.

## Lifecycle maintenance

A lifecycle request is charged as:

```text
operation ID UTF-8 bytes + expected stream-name UTF-8 bytes + 256
```

The runtime admits at most `maintenance.max_operations` requests and at most
`maintenance.max_operation_bytes` of lifecycle request charge. Cleanup shares
the operation count and has its own `max_cleanup_operations` concurrency limit.
Admission is immediate. There is no maintenance waiter queue.

An accepted lifecycle or cleanup call runs in an executor-owned task. Dropping
the caller does not release its reservation or let shutdown close the store
before the call finishes. A lifecycle result with an unknown commit retains its
exact request and byte charge. An identical retry reuses that reservation even
when the maintenance limit is full. The unresolved stream name stays blocked
during reconciliation. Shutdown reports bounded in-flight and unresolved
lifecycle requests and the number of unfinished cleanup turns.

Each coordinator has one maintenance-in-flight count. This keeps its stream gate
and subscription registry reachable while a transition is waiting or executing.
An unresolved-name entry keeps the same coordinator anchored after an unknown
result. Confirmed reset, delete, and unknown outcomes terminate old subscribers,
release their page buffers, and clear their indexed memberships exactly once.

## MemoryStore

The memory store owns active and retired history until bounded cleanup, `close`,
and store destruction. Each stream name is either one active `StreamHistory` or
one unavailable last `StreamKey`; it does not retain both representations.
Retired lifetimes live in one store-wide `VecDeque`, so selecting the next cleanup
prefix and checking whether more work remains are both constant-time operations.

For a new stream it charges:

```text
stream logical metadata = UTF-8 stream-ID bytes + 256
```

`max_streams` and `max_stream_metadata_bytes` both apply. An existing-stream lookup does not add another entry.

For a newly inserted event it charges:

```text
stored logical history = E + 256
```

`max_history_records` and `max_history_bytes` both apply before insertion. A successful insertion owns one `Arc<Record>` in a `BTreeMap` and a separately cloned `EventId` in the idempotency `HashMap`. The record holds exact-size identifier boxes and an `Arc` payload. An identical retry returns an `Arc` clone of the existing record and does not increase stored history.

Lifecycle receipts remain for the store lifetime and are bounded by count and
logical bytes. A retired lifetime charges the stream-name bytes plus 256 until
its metadata is finalized. Each cleanup turn removes a contiguous prefix from
one retired lifetime, bounded by record count and the `E + 256` charge for each
record. Removing records releases their tree nodes, record `Arc`s, and retry-index
entries; an application may still retain its own previously returned `Arc`s.

The two 256-byte constants are conservative logical allowances, not exact allocator charges. Tree nodes, hash buckets, spare capacity, `Arc` headers, mutexes, and allocator metadata can make actual resident memory larger than the logical quotas. The record and stream count limits keep the number of such allocations finite. Empty streams consume the stream count and metadata budget even though they consume no history budget.

A memory-store page allocates a vector of `Arc<Record>` pointers and shares the stored records and payloads. It does not copy payload bytes. The vector capacity is bounded by the requested record count and the finite stored history. The range read uses one ordered `BTreeMap::range` walk, so a page costs `O(log H + K)` tree work for stream history `H` and examined records `K`. It checks each expected offset, including a missing suffix. The earlier implementation performed `K` separate tree lookups and cost `O(K log H)`.

## SQLiteStore

One worker thread owns the `rusqlite::Connection`, prepared-statement cache, SQLite page cache, filesystem ownership lock, and all accepted commands. `worker_queue_capacity` bounds the Tokio MPSC queue by command count. A queued append owns one `NewEvent`; it shares its `Arc` payload with any external clone. Direct adapter use can therefore retain up to roughly:

```text
(worker_queue_capacity + one active worker command) × max_record_bytes
```

in logical append envelopes, plus command, channel, identifier, and allocator overhead. Read and metadata commands are smaller and use the same command slots. The runtime adds its own queue in front of this adapter and restricts SQLite to one write and one read at a time.

Submitting a command uses `try_send`, so a full adapter queue rejects immediately. Once queued, the worker completes the operation even if the oneshot receiver is dropped. The first `close` changes the adapter to `CLOSING`; if the command queue is full, at most one detached standard thread waits to enqueue that close behind already accepted commands. The connection, OS lock, and in-process ownership entry remain held until the worker closes.

Append binds the payload by borrowed slice. A successful new append moves the same `NewEvent` into its receipt record after commit, without copying payload bytes in Rust. Retry lookup and replay allocate row values in `rusqlite`; `decode_row` then copies the payload `Vec<u8>` into `Payload`. The temporary vector is released after the exact-size payload allocation is created.

SQL projections use `octet_length`, SQLite type checks, aggregate record-size checks, and fixed-size `CASE` guards before returning event IDs, schema IDs, payloads, offsets, or scalar versions to Rust. The database format requires UTF-8, so identifier byte limits match the domain model. Corrupt oversized values become `NULL` in SQLite rather than being materialized as Rust result values.

The prepared-statement cache holds at most eight statements. `cache_size=-4096` asks SQLite for an approximately 4 MiB page cache. It is not a hard process-memory limit. SQLite can allocate connection state, decoded values, pager structures, rollback-journal buffers, and temporary internal objects outside that target. These allocations use SQLite's C allocator and are not counted by the Rust global allocator harness.

`max_page_count=262144` limits the main database to a finite number of SQLite pages by default. Its byte size depends on the database page size. The rollback journal, ownership file, filesystem metadata, and kernel filesystem cache are outside that page count. `temp_store=MEMORY` keeps any SQLite temporary structures in process memory; the production range and lookup statements use declared indexes and do not require a result sort.

## Decoder and ingestion path

An `IngestionService` has three admission semaphores:

```text
active sessions <= max_sessions
waiting session starts <= max_session_waiters
sum(declared decoder retained bytes) <= max_total_decoder_bytes
```

The decoder-state permit is acquired before an asynchronous start waits for a session slot. The waiting future owns the decoder, mapper, stream key, and declared decoder bytes. Cancellation drops all of them and releases both permits.

Each active session owns one input `Vec<u8>`, a decoder, a mapper, at most one decoder output step in `pending_items`, an optional decode failure, and at most one event awaiting append resolution. Input length is checked against `max_retained_input_bytes`; capacity is exposed by `retained_input_bytes`. `Vec::reserve_exact` and allocator size classes may retain more capacity than the requested length, so this is not an exact allocator ceiling.

For 64 default sessions, the configured products permit up to 64 MiB of retained input, 16 MiB of declared decoder state globally, and up to 64 concurrent 1 MiB output steps. Item count is also limited to 128 per step. These products exclude task, vector, item, and mapper overhead.

The generic decoder contract reports `max_retained_bytes`. Every decode call also receives item, output-byte, and work-unit budgets. The driver rejects over-consumption, excess work, excess declared output, and an `OutputReady` step that consumes and emits nothing. A custom decoder's state declaration and each item's `accounted_bytes` are trusted because Rust cannot inspect arbitrary user-owned heap graphs. The mapper has no retained-byte declaration. A mapper type can therefore retain an arbitrary heap allocation per bounded session. This is a real gap if the ingestion limits are treated as a complete byte budget.

The driver also applies an explicit `DecodeDriveBudget` to one `push_chunk` or `finish` call. Steps, emitted items, declared output bytes, and reported work units each have an independent finite ceiling. A step must fit both the per-step and cumulative budgets. The total step limit stops a decoder from producing forever without consuming input. Independent output and work limits allow a legitimate decoder to expand a small compressed or encoded input when the application configures that expansion. Deriving these ceilings from retained input would have been simpler, but it would incorrectly make input buffering policy decide valid decoder output and computation.

The newline framer has an additional concrete copy path:

1. `push_chunk` copies borrowed source bytes into the session input vector.
2. The framer copies an unfinished line into its `partial` vector, bounded by `max_frame_bytes`.
3. Emitting a frame copies those bytes into an exact-size `Arc<[u8]>`.
4. The example byte-frame mapper copies the frame into `Payload`.
5. Event clones made during commit share the payload allocation but copy identifier strings.

The driver commits decoded items sequentially. It retains the exact pending event ID across append error or cancellation and rejects later input until the application retries that event. `stop` releases input capacity, the pending-item vector allocation, decoder, mapper, active-session permit, and declared decoder-byte permit. A retained terminal handle therefore does not block a new session. If a caller cancels while an append is unresolved, `stop` has not run: the pending event and the guarded resources remain owned until explicit retry or object drop.

`compact` drains consumed bytes and calls `shrink_to_fit` after every push. Fully consumed chunks therefore free the input allocation, and the next chunk allocates again. This favors retained memory over allocator traffic. Reusing a buffer would reduce allocation and copying, but retaining the configured 1 MiB maximum in every idle session would be expensive at large session counts. A reuse policy needs a global input-capacity budget or a small trim threshold before changing this tradeoff.

## Scale and operation-cost audit

An application agent is not a runtime object. One agent may own a stored stream, an active subscription, a decoder session, or only an application key. These populations have different costs. The finite defaults deliberately do not claim 100,000 simultaneous active objects: the runtime defaults to 1,024 coordinators and subscriptions, the memory store to 10,000 streams, and ingestion to 64 active sessions. A durable SQLite store can hold more inactive streams on disk because the runtime removes empty coordinators.

The following costs come directly from the current data structures. Average hash-table lookup is written as `O(1)`; collision behavior and resizing remain allocator and hash-function dependent.

| Path | Idle or active work | Retained or transient ownership |
| --- | --- | --- |
| Append admission | Global async mutex, coordinator hash lookup, coordinator mutex, and queue insertion | One queue item, oneshot, copied identifiers, shared payload; bounded by count and logical bytes |
| Insert completion | `O(S)` subscription locks for a stream with `S` registered subscribers, then broadcast wake | No payload copy; all waiting subscriber tasks may become runnable |
| Worker cleanup | Constant-time coordinator counters and emptiness checks | No subscriber scan |
| Same-stream subscription registration | Average `O(1)` free-slot reuse or one vector push | One strong subscriber pointer and, for holes, one reusable index |
| Subscription page | One bounds call and one range call; each uses an owned task plus a nested panic-supervision task | One requested page-byte permit, unread record pointers, and task allocations |
| Subscriber sweep | Every tick is `O(C + visited S)` even though expiry checks are capped | Temporary vector of all `C` coordinator `Arc` pointers |
| Memory append | `O(log H)` tree insertion and average `O(1)` identity insertion | Tree node, record, repeated stream identity in its cursor, and separate event-ID index key |
| Memory replay page | `O(log H + K)` after the ordered-range fix | `K` shared record pointers in a page vector |
| Ingestion push | Input copy, possible drain/shift and shrink allocation, decode, map, sequential appends | One input vector, one decoder step, one pending event, decoder and mapper |
| Runtime shutdown | `O(C + S + Q)` scan and drain | One owned finalizer until accepted work and close finish |

There is no library task or timer per subscription. The runtime owns a fixed number of storage-worker tasks and one sweeper task. An application that polls 100,000 subscriptions concurrently normally creates 100,000 application tasks, and those task futures are application-owned. Each admitted read creates two short-lived Tokio tasks in the library: the completion owner and the nested store operation used to catch panics without losing cancellation ownership. An append creates one nested store-operation task. SQLite adds one standard OS worker thread per open store, so one SQLite store per agent would multiply thread stacks, connections, caches, file handles, and locks. The intended scalable shape is a shared store with many stream identities when isolation policy permits it.

One shared `inner.changed` notification currently serves worker scheduling, admission-capacity changes, and lifecycle changes. `notify_waiters` can wake every producer, worker, or shutdown waiter listening at that instant. Each committed insert also scans the stream's registered subscribers to update lag state and calls the coordinator's `notify_waiters`, which can make every caught-up subscriber runnable. This one `O(S)` metadata scan and the broadcast wake are the dominant fanout costs. Observable delivery already requires `S` results, but repeated metadata locks, store bounds calls, and wakeups per individual commit are additional work.

The runtime duplicates identifier allocations because identifier values use exact-size `Box<str>`. A coordinator key appears in the global map and coordinator, and scheduling copies it into the ready queue. Unresolved append identities copy the stream and event IDs. Each memory-store record repeats its stream ID in `Record.cursor`, even though the enclosing `StreamHistory` already identifies the stream. Payload clones are cheap `Arc` increments and share bytes; identifier clones allocate and copy. At large counts, this distinction matters more than the inline integer fields.

Temporary `size_of` diagnostics on the ARM64 macOS host with Rust 1.98.1 reported 208 inline bytes for `Coordinator`, 112 for `CoordinatorState`, 280 for `SharedSubscriptionState`, and 72 for `PendingAppend`. The public layout diagnostic reported 104 bytes for `Record`. These figures exclude every string, vector buffer, map bucket, lock allocation, `Arc` allocation, task, and allocator header, and Rust does not promise them as an ABI. A `Subscription::next` call exposed a 16-byte boxed-future handle; that number excludes the heap future. `IngestionSession::push_chunk` produced a 336-byte inline future for the tested concrete types. Future sizes vary with generic types and compiler version.

### Ranked changes and candidates

1. **Completed: release consumed and terminal buffers immediately.** Delivering the final record in a subscription page replaces the empty `VecDeque` and releases its page permit. Subscription termination performs the same release and schedules owned registration cleanup, including store errors and invalid empty pages. Ingestion does the same for pending items and releases session permits after destroying the guarded decoder and mapper. Retaining either terminal handle no longer retains those capacities or admission slots after cleanup runs.
2. **Stop copying stream keys into the ready queue.** Queue `Arc<Coordinator>` values instead. This removes a stream-ID allocation and a coordinator-map lookup from each scheduling turn while preserving one scheduled entry per coordinator.
3. **Compact subscription state.** The live object keeps both the original `SubscriptionOptions` and copied lag fields, and it stores `last_delivered` both in the public subscription and shared sweep state. Retain only page limits after registration and store the shared offset rather than a second full cursor. This removes duplicated strings and state per active subscriber without changing semantics.
4. **Bound real sweeper work.** The per-coordinator stable slot walk is complete, but each tick still clones every coordinator. Replace the outer clone with a stable round-robin coordinator registry. With 100,000 subscriptions and the default 64 checks every 250 ms, a nominal full pass already takes about 390.625 seconds before execution overhead. The expiration responsiveness policy must match the supported scale profile.
5. **Completed: remove registration and cleanup scans.** Stable indexed memberships make registration, drop, terminal cleanup, and coordinator cleanup constant-time. Exact counters preserve admission and shutdown ownership.
6. **Evaluate contiguous storage for MemoryStore history.** ADR 0001 history is append-only with consecutive offsets. A `Vec<Arc<Record>>` or fixed-size chunks can remove tree nodes and change insert/read work to `O(1)`/`O(K)`. Benchmark vector spare capacity and growth copies, and preserve explicit corruption tests and any future prefix-retention design before selecting it.
7. **Avoid repeat identifier copies internally.** Queue coordinator references rather than keys first. Then measure internal `Arc<StreamKey>` or shared string storage. Reference counting enlarges ownership graphs and control blocks, so changing every public identifier to `Arc<str>` needs evidence rather than an assumption.
8. **Remove avoidable read and payload copies.** Under exclusive runtime ownership, coordinator bounds may replace a store `bounds` call before every live page if nonzero floors and adapter consistency remain correct. `ByteFrame` already owns an exact `Arc<[u8]>`, so an ownership-taking `Payload` constructor could reuse that allocation. A SQLite `Vec<u8>` cannot generally become `Arc<[u8]>` without another allocation and copy. Removing that copy needs a compatible decode allocation or a different backing type such as `Arc<Vec<u8>>` or `Bytes`, with explicit checks that unused capacity cannot be retained.
9. **Coalesce hot-stream notifications.** Separate work-available notification from capacity and lifecycle changes, and wake one storage worker for one ready coordinator. For subscribers, coalesce commit bursts by generation or a small shared delivery cadence. This trades a small amount of delivery latency for fewer runnable tasks, matching the stated memory and scheduling priority. It needs race proofs before implementation.

The first five candidates reduce memory or work without changing durable storage semantics. A shared fanout page or per-stream replay dispatcher could reduce repeated reads further, but it would introduce another retained ring or bounded mailbox per subscriber. That is a larger scheduling design and should follow measurements of the simpler changes.

## Shutdown and final release

Shutdown stops new admission, ends subscriptions, and starts one owned finalizer. The finalizer waits for queued appends, active store I/O, and active lifecycle or cleanup calls, then calls `EventStore::close`. Unresolved lifecycle requests retain their bounded retry identity in the report but do not prevent close. A caller deadline limits how long `shutdown` waits; it does not cancel the finalizer or abandon accepted work.

Dropping the last public `Runtime` handle starts the same drain on the Tokio executor captured at open. Workers hold the runtime state while it is open, so an explicit shutdown or last-handle drop is required to release the store promptly. If the owning Tokio runtime itself is destroyed, Tokio drops its tasks; adapter-specific drop and channel closure then determine final cleanup.

A store operation or `close` that never returns can retain its bounded owned state indefinitely. The library does not impose a second timeout inside the adapter call because cancelling an in-progress durability operation would make ownership and commit outcome unclear.

## What the measurements mean

The final benchmark harness reports exact runtime counters for accepted, rejected, failed, inserted, and deduplicated appends, plus peak queued append count and logical bytes. Its bounded stage samples separate submission-to-store-entry queue wait, store service time, store-return-to-delivery time, and complete caller outcomes.

The harness's Rust `GlobalAlloc` counters measure cumulative allocation and deallocation traffic for the whole Rust process, including Tokio and harness code. They are not live or peak bytes and exclude SQLite's C allocator. `getrusage` reports process CPU, context switches, faults, and process-lifetime peak RSS. A startup RSS point is also reported. RSS does not include all kernel filesystem cache use. `copied_bytes` and `flush_count` remain unavailable rather than being inferred.

These scopes explain why the formulas in this document and the observations in [performance evidence](performance-evidence.md) answer different questions. The formulas show which production owner prevents unbounded growth. The measurements show whole-process cost for named workloads on a named host.

## Open accounting findings

| Finding | Bound today | Consequence |
| --- | --- | --- |
| SQLite reads materialize one candidate before enforcing page bytes and copy its payload once inside Rust. | One extra record up to `max_record_bytes` per concurrent SQLite read. | `reads.max_buffered_page_bytes` is not a strict peak-allocation ceiling. |
| Custom mapper retained memory has no byte declaration. | Mapper count is bounded by `max_sessions`; bytes per mapper are not. | Ingestion's configured byte totals do not cover arbitrary mapper state. |
| Custom decoder and item accounting is declarative. | Session, declared-state, item-count, step-byte, and work limits. | A malicious implementation can under-report heap use. |
| Subscriber sweep work exceeds its configured check count. | Coordinators and subscriptions remain count-bounded. | Each tick still clones all coordinator pointers and may scan a full visited subscription vector. |
| Hot-stream append completion scans subscriber metadata once and broadcasts a wake. | Subscribers per stream are count-bounded. | CPU, locks, and runnable-task pressure grow linearly with fanout for every inserted event. |
| Runtime and MemoryStore byte counters use logical allowances. | Independent record, stream, waiter, coordinator, subscription, and diagnostic count limits. | Logical quotas must not be presented as allocator or RSS measurements. |

The core queues, maps, diagnostic ring, page buffers, subscription registry, store histories, and decoder drive loop have finite count, logical-byte, disk-page, work, or application-ownership boundaries. The findings above concern exact byte coverage, trusted extension accounting, and capacity lifetime. They remain visible until a production change and focused evidence close them.
