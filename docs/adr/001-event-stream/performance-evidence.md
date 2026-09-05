# Performance evidence

This report records measured behavior of the production runtime and both
official stores. It keeps measurement epochs separate. A source archive makes
each completed epoch reproducible even when later optimization changes the
runtime.

The newest completed epoch is `21b53c...`. It measures full-page drain and
warmed idle behavior at 1,000, 10,000, and 100,000 retained subscriptions. It
also measures the overhead of Rust allocator counters. The broader 72-sample
scale matrix and the append, delivery, decoder, and large-history artifacts are
from the reproducible `bd105...` prior-source epoch. They remain evidence for
that source. They are not silently relabeled as measurements of later code.

## Current build identity

The newest warmed artifacts were produced by a release build with all features:

```text
macOS 26.6 (25G72), Darwin 25.6.0, arm64
Apple M5, 10 logical CPUs, 24 GiB RAM
APFS on Apple Fabric-attached SSD
rustc 1.98.1
rusqlite 0.32.1, bundled SQLite 3.46.0
SQLite journal_mode=DELETE, synchronous=FULL, 4 MiB connection cache
```

The identity was captured before measurement:

| Item | SHA-256 |
| --- | --- |
| Input manifest | `21b53c226641721ed8a1d1a95e056e57ea3eb90e9bd9ff3993ddddda1bb422d6` |
| Harness source | `4aa32fc1bed59003652d881ac1ff3c0b7adb914b87162f5a929d38ab28cfcd0a` |
| Instrumented binary | `7ee39fe9c097125c09bdfac975de503b6fd01bcc920a12845f8e2a47aa1b6b78` |
| Allocator-control binary | `f7a345256c31bdc5c77d29878a791b7ababe23787c99394cb8e379c74406520b` |
| Warmed 1k/10k JSONL | `0e0ad889ad74030e1b0d59fc643f14547b29a95d23d84bb80c2c967f0411eeab` |
| Warmed 100k JSONL | `002e1a6a2979b3bbf1fe3d5b6ec0627616a38fb3000c8a982930ff8a81a3504a` |
| Instrumentation A/B JSONL | `1ffb8524eba0a94d3474493966b726d4dc4a1506260f5c08c5fca057d1a353ed` |

[`evidence/performance-build-inputs.sha256`](evidence/performance-build-inputs.sha256)
contains the digest of every production Rust file, the harness, `Cargo.toml`,
and `Cargo.lock`. The harness embeds the combined input digest and its own
source digest in every environment row. The collection script checks the input
manifest and binary before and after a run, validates every JSON object and
expected scenario key, then renames `.partial` to the final filename.

The exact inputs, scripts, provenance records, and both measured binaries are
archived in
[`evidence/performance-source-21b53c226641721ed8a1d1a95e056e57ea3eb90e9bd9ff3993ddddda1bb422d6.tar`](evidence/performance-source-21b53c226641721ed8a1d1a95e056e57ea3eb90e9bd9ff3993ddddda1bb422d6.tar).
The archive SHA-256 is
`cbebc32c07a972fbf4a693ab99d79b81a0fce78ad0a8b50df7e87eecf62570b2`.
This matters because the repository does not yet have an immutable commit that
can identify an older measurement binary.

The prior `bd105...` epoch has its own archive at
[`evidence/performance-source-bd1051715ca9c7e527e1343c1d5afea83750b5af9c84c2956869d53b3cc70f25.tar`](evidence/performance-source-bd1051715ca9c7e527e1343c1d5afea83750b5af9c84c2956869d53b3cc70f25.tar),
with SHA-256
`eed61b1789de40dc306686bc4d0f68240e39a4d88cef64c26388deabe469cbb5`.

## What the prior-source scale profiles mean

An agent is an application concept. The library sees streams, subscriptions,
and append calls. The matrix measures them separately:

- `idle_scale` creates and retains 1,000, 10,000, or 100,000 stored streams.
  Its timed region is a 10-second idle interval, equal to 40 default 250 ms
  subscriber-sweep intervals.
- `subscription_scale` retains one empty subscription and therefore one active
  coordinator per stream. Registration happens before the separate 10-second
  idle region.
- `active_streams` uses 64 long-lived Tokio tasks to append once to every
  stream. It measures 100,000 active logical agents over bounded execution
  concurrency. It does not hold 100,000 coordinators simultaneously.
- `agent_burst` parks one Tokio caller task per stream behind a two-phase
  barrier. It records the exact ready-task count before releasing the burst.
  This measures overload stability and caller-owned future memory. It does not
  create an OS thread per agent.

The runtime queue, byte, waiter, and worker limits stay unchanged. The harness
only raises finite stream, coordinator, subscription, and MemoryStore metadata
limits so the named fixture can exist. Every one of the 72 `bd105...` samples
satisfied:

```text
offered = runtime_accepted + runtime_rejected
runtime_accepted = inserted + deduplicated + runtime_failed
caller_failed = 0
shutdown.closed = true
shutdown.unresolved = 0
queued appends, admission waiters, active subscriptions after shutdown = 0
```

## Resource metric scope

RSS and virtual-size values are process observations from `ps`. Thread counts
come from the macOS per-thread `ps -M` view. CPU, context switches, faults, and
process-lifetime peak RSS come from `getrusage`.

The global allocator wrapper counts requested bytes and allocation calls made
through Rust's `System` allocator. It includes the harness and Tokio. It does
not see SQLite's C allocator, allocator metadata, or kernel filesystem cache.
Allocated and deallocated bytes are cumulative traffic. Their difference at a
quiescent point is requested live Rust memory, not RSS and not a heap high-water
mark.

Subscription registration has its own time, allocation, RSS, virtual-memory,
and thread snapshots. The timed idle interval starts after registration.
Teardown starts before subscription handles are dropped and includes
`Runtime::shutdown`. A second observation follows runtime-handle and profiler
buffer release plus a bounded 10 ms quiescence wait. RSS may remain high because
the system allocator can retain freed pages.

The normal timing samples include atomic allocator-counter overhead consistently
for both stores. A paired control below measures this overhead. Results from
different instrumentation modes must not be compared as though the measurement
work were identical.

## Prior-source scale results

The following `bd105...` values are medians of three release repetitions. MiB
values use 1,048,576 bytes. These repetitions run in one process per invocation,
so later RSS samples can include allocator carryover from earlier repetitions.

| Store | Population | Profile | Accepted / rejected | Timed wall | Timed CPU | Settled RSS |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Memory | 1,000 | stored idle | 0 / 0 | 10.005 s | 1.804 ms | 4.0 MiB |
| Memory | 10,000 | stored idle | 0 / 0 | 10.010 s | 1.567 ms | 8.4 MiB |
| Memory | 100,000 | stored idle | 0 / 0 | 10.003 s | 1.830 ms | 43.6 MiB |
| Memory | 1,000 | retained subscriptions | 0 / 0 | 10.008 s | 5.513 ms | 5.5 MiB |
| Memory | 10,000 | retained subscriptions | 0 / 0 | 10.004 s | 19.967 ms | 20.9 MiB |
| Memory | 100,000 | retained subscriptions | 0 / 0 | 10.003 s | 166.729 ms | 159.2 MiB |
| SQLite | 1,000 | stored idle | 0 / 0 | 10.004 s | 1.635 ms | 5.2 MiB |
| SQLite | 10,000 | stored idle | 0 / 0 | 10.007 s | 1.853 ms | 6.5 MiB |
| SQLite | 100,000 | stored idle | 0 / 0 | 10.005 s | 2.084 ms | 14.4 MiB |
| SQLite | 1,000 | retained subscriptions | 0 / 0 | 10.011 s | 5.501 ms | 6.6 MiB |
| SQLite | 10,000 | retained subscriptions | 0 / 0 | 10.003 s | 20.040 ms | 19.4 MiB |
| SQLite | 100,000 | retained subscriptions | 0 / 0 | 10.003 s | 127.062 ms | 115.5 MiB |

At 100,000 empty subscriptions, registration retained a median 111.31 MB of requested
Rust allocations for Memory and 111.32 MB for SQLite. The median registration
times were 0.815 s and 2.585 s. Median teardown times were 131 ms and 127 ms.
Empty subscriptions do not contain a replay page. The next section measures a
full 256-record page followed by caught-up idle behavior.

## Full-page drain and warmed idle

The `21b53c...` fixture preloads 256 small records on one shared stream. Every
retained subscriber reads and validates offsets 1 through 256. Subscribers are
drained sequentially. This isolates retained buffer capacity after a real page
has emptied; it does not claim that 100,000 page buffers were live at once.
Registration, drain, and the following 10-second idle interval are separate
phases.

| Store | Subscribers | Registration | 256-record drain | Rust live change during drain | Idle CPU / 10 s | Settled RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Memory | 1,000 | 14.0 ms | 46.4 ms | +192 B | 3.95 ms | 5.3 MiB |
| Memory | 10,000 | 444 ms | 414 ms | +192 B | 24.7 ms | 18.0 MiB |
| Memory | 100,000 | 53.44 s | 4.01 s | +192 B | 286.9 ms | 64.7 MiB |
| SQLite | 1,000 | 41.9 ms | 359 ms | +1,668 B | 2.70 ms | 6.3 MiB |
| SQLite | 10,000 | 1.546 s | 4.429 s | +1,668 B | 14.4 ms | 15.4 MiB |
| SQLite | 100,000 | 74.18 s | 27.37 s | +1,668 B | 213.2 ms | 64.5 MiB |

Values are medians of three release repetitions. Requested live Rust bytes are
the allocator counters' cumulative allocations minus deallocations at the two
phase boundaries. The nearly flat result shows that drained page capacity is
released. It includes the harness and Tokio and excludes SQLite's C allocator.
RSS can remain higher because the system allocator may retain freed pages.

The one-stream setup exposes a real scaling cost. Subscription registration
checks the coordinator's retained membership, so setup grows roughly with the
square of fanout. The 100,000-subscriber registrations completed within the
900-second watchdog, but 53 to 74 seconds is not a low-latency operation. A
brief unrelated Cargo process was observed during the 100,000 SQLite batch.
That batch remains valid for exact counts, memory, and cleanup. Its timings are
shared-host observations rather than isolated-host qualification.

Every warmed sample retained the requested subscriber count through the idle
point. Every shutdown reported closed with no queued append, admission waiter,
active subscription, or unresolved operation.

## Allocator instrumentation overhead

The allocator control uses the same production code, release profile, stage
timestamps, workload, and system allocator. A compile-time harness setting
removes only the atomic allocation counters. Its allocation fields are JSON
`null`, rather than zero.

| Store | Mode | Median wall, 8,000 appends | Median CPU | Relative wall | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | counters enabled | 36.96 ms | 74.46 ms | 1.115x | 1.141x |
| Memory | control | 33.16 ms | 65.24 ms | 1.000x | 1.000x |
| SQLite | counters enabled | 2.538 s | 1.833 s | 1.009x | 1.029x |
| SQLite | control | 2.515 s | 1.782 s | 1.000x | 1.000x |

Each mode has five repetitions. Modes ran sequentially rather than interleaved,
so the ratios also include ordinary run-to-run system variation. The result
shows that allocator counters materially affect the CPU-heavy MemoryStore path
and have a smaller effect when SQLite flush work dominates. Regression timing
must keep the instrumentation mode in its comparison key.

| Store | 100,000-agent profile | Accepted / rejected | Wall | Peak queue | Settled RSS |
| --- | --- | ---: | ---: | ---: | ---: |
| Memory | 64 bounded walkers | 100,000 / 0 | 0.470 s | 64 | 121.2 MiB |
| SQLite | 64 bounded walkers | 100,000 / 0 | 28.599 s | 64 | 22.7 MiB |
| Memory | synchronized caller burst | 1,027 / 98,973 | 0.480 s | 1,024 | 316.9 MiB |
| SQLite | synchronized caller burst | 1,025 / 98,975 | 0.769 s | 1,024 | 288.9 MiB |

Every burst repetition recorded exactly 100,000 ready caller tasks. Accepted
counts can exceed the queue peak because workers move calls out of the queue
while the released callers race for admission. Every accepted call committed.
The median ready-task RSS was 304.1 MiB for Memory and 180.1 MiB for SQLite.
These values include the 100,000 caller futures and harness state. They are not
steady runtime metadata costs.

SQLite stored-stream setup performs durable metadata transactions. Creating
100,000 streams took a median 34.3 s and produced a 7,905,280-byte database.
The active-stream profile then committed 100,000 events with `FULL`
synchronization and one writer. Its 28.6 s result must not be compared with the
ephemeral MemoryStore as though they provide the same persistence profile.

## Scoped regression budgets

These thresholds apply only to the archived release binary, this machine, this
instrumentation, and the exact fixtures above. They are conservative regression
alarms, not universal product promises.

| Covered workload | Budget |
| --- | --- |
| Every scale sample | zero unexpected failures; exact acceptance conservation; clean closed shutdown |
| 100,000 stored idle streams | settled RSS <= 64 MiB; combined CPU <= 10 ms per 10-second window |
| 100,000 empty retained subscriptions | settled RSS <= 192 MiB; combined CPU <= 300 ms per 10-second window |
| 100,000 subscription registration | <= 4 s; requested live Rust growth <= 128 MB |
| 100,000 subscription teardown | <= 500 ms; zero retained active subscriptions after shutdown |
| Memory, 64 walkers over 100,000 streams | wall <= 2 s; settled RSS <= 192 MiB; zero rejection |
| SQLite, 64 walkers over 100,000 streams | wall <= 60 s; settled RSS <= 64 MiB; zero rejection |
| Synchronized 100,000-call burst | exact 100,000 ready tasks; peak queue <= 1,024; RSS <= 384 MiB; every call accepted or rejected |

A threshold breach triggers investigation and repeated measurement. It does not
identify the cause by itself. The scale profile raises `scheduling.max_coordinators` and
`subscriptions.max_total`; the library defaults remain 1,024 because applications
must opt into the memory and cleanup work of larger active populations.

The following `21b53c...` warmed alarms were calibrated after observing the
measurements. They are regression alarms, not pre-registered acceptance limits.
Every individual 100,000-subscriber sample, rather than only its median,
satisfied them.

| Warmed one-stream workload | Per-sample alarm |
| --- | --- |
| Memory registration | <= 75 s |
| SQLite registration | <= 120 s |
| Memory sequential 25.6-million-record drain | <= 10 s |
| SQLite sequential 25.6-million-record drain | <= 60 s |
| Post-drain 10-second idle | combined CPU <= 500 ms; settled RSS <= 128 MiB |
| Requested live Rust bytes after drain | <= 80 MB; no growth proportional to 256 records per subscriber |
| Ownership and shutdown | exact 100,000 active before teardown; closed with zero residual work |

## Append, delivery, and history status

The `bd105...` archive makes the broad prior-source epoch reproducible. Its
files retain `exploratory-pre-drain-release` in their historical names, but
their provenance is stronger than the earlier unarchived `493d...` diagnostic.
The archived epoch includes the repeated payload and producer matrix,
duplicates, overload, replay while appending, stalled and live subscribers,
idle, decoder modes, one-million-record history, greater-than-RAM history, and
a separate SQLite profiling run. Those measurements establish behavior of that
source snapshot. They do not establish unchanged timing after later runtime
edits.

The stored profile shows a real SQLite path through `sqlite3_step`, `pwrite`,
and `fsync`. `/usr/bin/sample` adds profiler overhead, so it is attribution
evidence rather than a latency baseline. The greater-than-RAM database was
27,950,964,736 bytes on a 24 GiB host; startup RSS remained bounded. The
one-million-record artifact contains exactly 1,000,000 records, rather than a
nominal target inferred from requested input.

The current focused epoch closes the caught-up buffer ownership and allocator
instrumentation questions. An indexed subscription-membership optimization is
being measured as a new source epoch. Its 100,000-subscriber registration and
warmed profile must be compared against `21b53c...`; unaffected prior-source
workloads remain labeled with their own epoch instead of being rerun without a
technical reason.

The harness now records requested offer deadlines and actual start lateness for
sustained traffic. The interval is a target. The report will use measured offers
over elapsed time and will not assume Tokio can schedule at 10 microseconds.

Exact SQLite C allocation counts, payload-copy counts, physical commit
timestamps, flush counts, and flush duration are unavailable. Delivery records a
safe interval instead: store entry is before commit and store return is after
commit. For a delivery time `D`, true commit-to-delivery latency is bounded by:

```text
lower = max(0, D - store_return)
upper = D - store_entry
```

Retention, snapshots, and replication belong to later ADR phases. Their
interference tests are future coverage rather than an ADR 001 release gate.

## Evidence files

- [`evidence/performance-warmed-subscription-scale.jsonl`](evidence/performance-warmed-subscription-scale.jsonl): `21b53c...` repeated 1,000/10,000 full-page drain and warmed idle matrix.
- [`evidence/performance-warmed-subscription-scale-100k.jsonl`](evidence/performance-warmed-subscription-scale-100k.jsonl): `21b53c...` repeated 100,000-subscriber full-page drain and warmed idle matrix.
- [`evidence/performance-instrumentation-overhead.jsonl`](evidence/performance-instrumentation-overhead.jsonl): paired allocator-counter and control samples.
- [`evidence/performance-scale-exploratory-pre-drain-release.jsonl`](evidence/performance-scale-exploratory-pre-drain-release.jsonl): reproducible `bd105...` prior-source 1,000/10,000/100,000 scale matrix.
- [`evidence/performance-build-provenance.json`](evidence/performance-build-provenance.json): captured input and binary identity.
- [`evidence/performance-instrumentation-provenance.json`](evidence/performance-instrumentation-provenance.json): paired binary identities.
- [`evidence/performance-build-inputs.sha256`](evidence/performance-build-inputs.sha256): exact build-input checksums.
- [`verification/run-performance-scale.sh`](../../../verification/run-performance-scale.sh): bounded watchdog, validation, and atomic publication script.
- [`verification/run-performance-warmed-scale.sh`](../../../verification/run-performance-warmed-scale.sh): full-page fanout validation and atomic publication script.
- [`verification/run-performance-instrumentation-overhead.sh`](../../../verification/run-performance-instrumentation-overhead.sh): paired instrumentation control.
- Files containing `.partial`: interrupted or failed runs. Their failure metadata and any flushed rows are retained for diagnosis, not counted as completed matrices.
