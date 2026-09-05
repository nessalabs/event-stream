# Scale and efficiency review

Status: source review and stress-test expansion in progress. Supporting 100,000 agents is a target, not a verified capacity claim.

We want low memory use and predictable work as the number of agents grows. A large memory saving can be worth a few extra microseconds. Every comparison must preserve ordering, retry behavior, durability settings, payloads, and delivered results.

## Define what one agent costs

An agent is an application concept. The library sees streams, append calls, subscriptions, and optional decoder sessions. These populations need separate tests.

```text
100,000 stored streams
    does not imply 100,000 active subscriptions
    does not imply 100,000 outstanding appends
    does not imply 100,000 OS threads

memory ≈ stored history + stream metadata + active subscription state
       + bounded queued events + bounded read pages + decoder state
       + executor, adapter, allocator, and application overhead
```

Measure 1,000, 10,000, and 100,000 entries. Compare the additional bytes per entry between adjacent sizes. Keep payload bytes, retained history, page quotas, and worker counts fixed when measuring metadata growth. Run separate tests when varying those costs.

Agent count does not specify event rate. For illustration, 100,000 agents each emitting once per minute produce about 1,667 events per second on average. Emitting once per second produces 100,000 events per second. A synchronized burst has different queue requirements from either average. Record event rate, payload size, fanout, and durability with every capacity claim.

The history itself cannot occupy constant memory while retaining an unlimited number of distinct records in memory. Durable storage moves that growth to disk. Exact retry detection also requires retained identity information; deleting it changes the retry guarantee. We should minimize overhead around these necessary costs.

At 100,000 agents, 1 KiB each is about 98 MiB. A reserved 1 MiB working buffer each would approach 98 GiB. Small persistent metadata may reasonably grow with agents. Large working buffers should belong to a bounded population of active operations, using shared byte limits. Reject or wait when that capacity is exhausted.

Some work is unavoidable. Validating or storing a payload of B bytes requires processing those bytes somewhere on the path. Delivering an event to F consumers requires F observable deliveries, even if they share one immutable payload. Retaining H distinct event identities requires information proportional to those identities if retry detection remains exact. Optimize extra copies, repeated searches, idle scans, and unnecessary scheduling around these costs. Do not promise constant total work as payload size, fanout, or retained history grows.

## Current findings

| Area | Current source behavior | Next decision or evidence |
| --- | --- | --- |
| Memory replay | Repeated `BTreeMap::get` calls have been replaced with an ordered range walk and explicit gap checks. | Shared contracts and missing-middle/missing-suffix tests pass. Compare before/after on long histories before claiming a measured speedup. |
| Memory history | Records are indexed by a tree even though public history is append-only and positions are consecutive. | Compare tree storage with a vector or fixed-size chunks. Include growth-copy pauses, unused capacity, and future prefix deletion. Do not add a custom container before evidence. |
| Same-stream registration | Registration now fills a reusable stable slot instead of filtering every existing member. Removal clears that slot and records its index for reuse. | Registration/removal membership work is amortized O(1); vector growth still allocates. Matched repeated measurements reduced 100k registration from 53.44s to 0.815s for Memory and 74.18s to 6.418s for SQLite. These measure registration, not sustained delivery. The archived prior implementation performed O(S²) total membership checks. |
| Subscriber cleanup | Each tick still copies all coordinator references. Within a coordinator, the sweep visits at most its remaining slot budget, including empty slots, and preserves its position across removals. | A deterministic churn regression checks that surviving members are visited. The outer coordinator copy still costs O(C) time and temporary references for C coordinators; the entire tick is not bounded by the subscription-check budget. |
| Cleanup responsiveness | Defaults check at most 64 subscriptions every 250 ms. | With 100,000 subscriptions a nominal pass takes about 391 seconds, before execution cost. Measure the maximum cleanup delay and define a supported scale profile. |
| Identity copies | `Box<str>` identifier clones allocate and copy. The payload is shared with `Arc<[u8]>`. | Count identifier copies along append and fanout paths. Compare sharing identifiers against reference-count traffic and larger live ownership graphs. |
| Operation tasks | A tracked read spawns an owner task and a nested store-operation task. Appends spawn a store-operation task. | Measure task allocation and scheduling cost. Preserve cancellation ownership and panic handling before consolidating tasks. |
| Async port calls | The core uses generic ports, but `async_trait` still boxes each method's returned future. | Measure the actual future allocation. Removing boxing can enlarge the caller's suspended task, so fewer allocations alone do not establish lower peak memory. |
| Read allocation | SQLite may materialize one candidate beyond the accepted page bytes. The decoder now borrows row bytes while allocating final domain values, eliminating four temporary allocations per record. | The [paired decoder experiment](../002-resource-budgets-and-performance-evidence/sqlite-decode-borrowed-ab.md) measured 44.1% fewer store allocation calls and 2.8% lower median store time. Candidate work beyond the page budget remains a separate cost. |
| Finished decoding | Terminal sessions now release admission and decoder-byte permits when the decoder and mapper are released. | A regression test retains the terminal handle and admits a replacement. Pending-event retry tests also pass. |
| Drained and ended subscriptions | The final buffered pop, terminal and drop paths release page allocation and permit. Terminal handles release registration quota without requiring handle drop. | Independent stable/MSRV tests pass. Measure warmed-idle memory after a real page has been drained; allocator-retained RSS may remain above live requested bytes. |

These are source findings, not measured speedups. The [resource accounting report](resource-accounting.md) describes current ownership and remaining accounting limits.

## Types and layout

Keep cursor offsets as integers. They represent exact positions. Floating-point values introduce rounding and offer no benefit here. Shrinking an offset to 32 bits would cap one stream at roughly 4.3 billion records and change its lifetime contract.

Measure complete object layouts rather than adding field sizes. Alignment can leave padding. A smaller field may save no space, and packed layouts can make access more expensive. `size_of` describes inline storage; it excludes strings, vector capacity, reference-counted allocations, and allocator metadata.

Our first layout measurements should cover `Record`, `NewEvent`, `Cursor`, coordinator state, subscription state, queue items, and decoder-session futures. Then measure their live heap allocations at scale. Large rare error variants should not enlarge every common value unnecessarily.

The initial local `size_of` check, before the stable-slot membership change, reported the following inline sizes on this 64-bit macOS build. All listed types have 8-byte alignment. These are diagnostic measurements of the current Rust layout, not a stable ABI or a complete memory charge.

| Type | Inline bytes |
| --- | ---: |
| `StreamId` | 16 |
| `StreamKey` | 32 |
| `Cursor` | 48 |
| `SchemaRef` | 24 |
| `Payload` | 16 |
| `NewEvent` | 56 |
| `Record` | 104 |
| `AppendReceipt` | 16 |
| `Page` | 128 |
| Internal `Coordinator` | 208 |
| Internal `CoordinatorState` | 112 |
| Internal `SharedSubscriptionState` | 280 |
| Internal `PendingAppend` | 72 |

The coordinator contains its state, so do not add both sizes as separate allocations. The concrete ingestion `push_chunk` future measured 336 inline bytes. A boxed subscription future's 16-byte handle does not describe the heap allocation it points to.

For example, a 104-byte `Record` still points to separately allocated payload and identifier bytes. A stored record also carries its stream identity even though its enclosing history already identifies the stream. This duplication deserves measurement before changing the public record representation. A compact internal record could reduce duplication but would add work when constructing public records during replay.

## Tasks, threads, and the operating system

A waiting async task retains its future and scheduling state. It does not require a dedicated OS thread. Objects held across an `.await` remain part of that task's retained state. Count this state in addition to queued events. Tokio's introductory task-overhead number is not the total memory of an application future. See [Tokio task ownership](https://tokio.rs/tokio/tutorial/spawning).

The runtime uses a fixed worker population, with additional bounded in-flight operation tasks. The SQLite adapter creates one dedicated OS worker thread per open store. Its full-queue close path can also create a temporary sender thread. Applications should share a store across agent streams when that matches their isolation requirements. Opening one SQLite store per agent also multiplies threads, connections, caches, and file handles.

Measure OS thread count, virtual memory, resident memory, context switches, and idle CPU independently. Reserved stack address space is not the same as resident stack pages. Do not reduce thread stacks without establishing the worst supported call depth. Do not assume a Linux stack default describes the macOS process.

Rust exposes stack configuration through its thread builder and `RUST_MIN_STACK`; the main thread's stack is outside that Rust setting. Check the actual host configuration before assigning a per-thread memory cost. See [Rust thread configuration](https://doc.rust-lang.org/std/thread/).

For SQLite, separate time waiting for the writer from transaction CPU and disk synchronization. A faster in-memory lookup cannot remove a required durable flush. Batching may amortize that cost, but needs an explicit transaction and receipt contract before implementation.

The bundled SQLite source also explains why synchronization settings need precise names. `synchronous=FULL` controls when SQLite calls its synchronization operation. It is different from macOS `F_FULLFSYNC`, which is a particular OS operation. In bundled SQLite 3.46.0, `sqlite3PagerSetFlags`, `unixSync` and `full_fsync` select and perform these steps. The adapter does not enable `PRAGMA fullfsync`. Its tested profile remains process restart.

One SQLite `xSync` callback can perform more than one OS call, including directory synchronization. Counting `xSync` callbacks therefore does not count kernel syscalls or device flushes. Timing a forwarded callback measures the elapsed time spent below that boundary, including waiting and scheduling. It does not isolate device service time. Keep source-path inspection, observed callback counts and sampled native stacks as separate evidence.

Keep journal and synchronization settings in every comparison. SQLite documents different sync behavior and failure guarantees for its journal modes and `synchronous` values. A settings change needs its own recovery tests and an explicit persistence contract. See [SQLite synchronization settings](https://www.sqlite.org/pragma.html#pragma_synchronous).

## Stress profiles

1. Create and retain streams without subscriptions. Measure metadata setup and idle costs.
2. Retain one idle subscription per stream. Measure settled memory, idle CPU, and cleanup responsiveness.
3. Visit all streams using a fixed producer population. Measure many-stream lookup and scheduling costs. Label the actual producer concurrency.
4. Submit a burst of concurrent calls. Keep internal queue and waiter limits fixed. Count accepted, rejected, committed, and failed calls. Verify drain and memory recovery.
5. Run sustained offered traffic below, near, and above measured capacity. Record queue growth and tail latency. A rejection is not a delivered event.
6. Stall some consumers while others continue. Verify bounded pages, expiry delay, and progress for healthy consumers.

Keep stress-harness allocations visible. Holding 100,000 futures or result objects in the harness affects whole-process memory even when the library promptly rejects excess work. Report allocator live bytes where available, settled RSS, peak RSS, and retained ownership after teardown separately.

The first objective is a simple design with bounded resources and demonstrated scale. A claim to be the fastest requires comparisons against named alternatives under equivalent semantics and persistence settings. The current evidence does not establish that claim.

## Next experiments, in order

First remove duplicated subscription data. The local handle needs its full cursor because the public API returns a borrowed cursor. The shared state may only need the offset, since its coordinator already identifies the stream. Lag settings also appear in both the local options and shared state. Measure the resulting complete layouts and live allocations; field arithmetic alone can miss padding.

Next remove the extra subscription scan on append cleanup. An exact membership count or indexed removal could make that cleanup constant work. Registration, drop, expiry, cancellation, and shutdown must all update membership exactly once. The commit path still needs separate analysis because it currently establishes when each subscriber first falls behind.

Then make sweep work proportional to its configured check budget. A reusable array of subscription slots with a rotating index could avoid copying the entire coordinator map. Reuse freed slots so churn cannot accumulate unlimited dead entries. Test a continuously registered subscriber's maximum visit delay, including churn and one heavily subscribed stream. The configuration must account for a full sweep cycle in addition to the lag threshold.

Measure async-call allocations separately before changing the port API. For a boxed future, `size_of_val(future.as_ref().get_ref())` measures its dynamic inline body; the box handle alone is only a pointer pair. Compare constructing, polling, and dropping calls. An unboxed child future becomes part of its caller's task allocation, including states that a rejected call never reaches. At 100,000 parked tasks, that can matter more than the allocation saved during execution. Preserve mockability and explicit `Send` requirements if the contract changes.

These experiments are proposed, not implemented speedups. Keep the existing design when measured savings do not justify the additional state or public API change.

## First scale diagnostic

The [raw memory-store diagnostic](evidence/performance-memory-scale-diagnostic.jsonl) contains one run per cell on the local Apple M5, with a ten-second idle window. The source manifest matched the checked production files. Each subscription cell retained exactly one real subscription per stream. Runtime shutdown reported closed, with no unresolved appends or remaining queued appends, waiters, or registered subscriptions.

| Stored streams | Stored-only RSS, MiB | With idle subscriptions RSS, MiB | Subscription-cell CPU over ten seconds, ms |
| --- | ---: | ---: | ---: |
| 1,000 | 3.80 | 5.20 | 6.118 |
| 10,000 | 8.12 | 19.28 | 22.527 |
| 100,000 | 37.23 | 142.11 | 144.959 |

At 100,000 subscriptions, registration took about 0.829 seconds. Registration's net requested Rust allocation was 111,307,632 bytes, or roughly 1,113 bytes per subscription. This is whole-process allocator observation across registration, not a pure subscription-type size. It excludes allocator metadata and includes associated coordinator and harness activity. Thread count remained 11 at the recorded process points.

The observed memory growth is roughly proportional to the population across these three points. This does not establish worst-case complexity or a sustained traffic capacity. These streams have no replayed pages or pending events. Repeated runs, warmed idle buffers, traffic, and slow-consumer expiry remain separate tests.

Memory reclamation is not closed by this result. For the 100,000-subscription cell, RSS was still 148,619,264 bytes after handle/profiler release and a ten-millisecond observation wait. That may include allocator retention and outstanding cleanup; this RSS point alone does not distinguish them. The diagnostic's reclamation rows lack population keys, so associate them with their preceding invocation rather than joining on scenario alone. A subsequent harness revision adds explicit population identity and absolute requested-live bytes.

## Stable membership measurement

The archived `d85c4020` build replaces repeated subscription scans with stable,
reusable slots. The orchestrator independently checked all 16 archived inputs,
the executable hash, and the 18 repeated warmed samples. Each population has
three samples per adapter. Every subscriber drained 256 exact records before a
ten-second idle interval. These results describe one shared stream, sequential
subscriber drain, and the recorded local macOS host.

| Metric at 100,000 subscribers | Prior build `21b53c22` | Stable slots `d85c4020` |
| --- | ---: | ---: |
| Memory registration median | 53.44 s | 0.815 s |
| SQLite registration median | 74.18 s | 6.418 s |
| Memory idle CPU per ten seconds, median | 286.9 ms | 1.91 ms |
| SQLite idle CPU per ten seconds, median | 213.2 ms | 2.07 ms |
| SQLite sequential drain median | 27.37 s | 42.06 s |

The registration state retains approximately 800 KiB more requested Rust bytes
at this population, roughly eight bytes per subscriber. That small persistent
cost removes repeated membership scans and preserves stable cleanup positions.
It does not reduce the necessary work of delivering 25.6 million records.

SQLite drain regressed in the observed comparison. Its three new drain samples
were 37.5, 84.2 and 42.1 seconds. Settled RSS also varied widely. Preserve this
variation; do not infer an overall throughput improvement or a stable RSS saving
from the registration win. Further diagnosis must separate storage/read
scheduling, host pressure, and measurement variation before attributing the
replay result to one cause.

Raw samples: [1k and 10k](evidence/performance-warmed-subscription-scale-d85c4020281e8f02a1c52950189e3dafc5beca998ec1395c554b18d0c2ef047b.jsonl)
and [100k](evidence/performance-warmed-subscription-scale-100k-d85c4020281e8f02a1c52950189e3dafc5beca998ec1395c554b18d0c2ef047b.jsonl).
Later lifecycle source edits are not part of this measured executable.
