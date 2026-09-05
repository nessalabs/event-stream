# Round 106: repeated population overload diagnostic

Memory accepts every offer at this workload. SQLite cannot keep up with the
requested rate within the fixed outstanding-task budget on this host.
This identifies a concrete throughput gap; it does not qualify the release.

Three fresh processes per store visit 10,000 streams twice, with four offers
per 1 ms deadline: 4,000 aggregate offers/s, about five seconds of arrivals.
Each stream is offered work about once every 2.5 seconds. Payloads are 128 bytes.
SQLite uses DELETE/FULL persistence. No subscriptions or maintenance run here.
The generator permits at most 256 outstanding tasks. Once full, it counts an
offer as generator-rejected before calling the runtime. These are **not SQLite
errors or runtime admission rejections**. Every runtime-submitted call succeeds.
Rejected offers have no receipt-latency sample. Do not interpret receipt p99
as latency for all offered work.

| Run | Store | Accepted / 20,000 | Generator rejected | Streams covered / 10,000 | Receipt p99 ms | Lifetime RSS MiB | Peak tasks |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | memory | 20000 | 0 | 10000 | 0.119 | 27.44 | 20 |
| 1 | sqlite | 14233 | 5767 | 8625 | 113.208 | 14.47 | 256 |
| 2 | memory | 20000 | 0 | 10000 | 0.120 | 27.41 | 8 |
| 2 | sqlite | 16070 | 3930 | 9466 | 93.491 | 14.61 | 256 |
| 3 | memory | 20000 | 0 | 10000 | 0.123 | 27.44 | 48 |
| 3 | sqlite | 16193 | 3807 | 9422 | 94.260 | 14.59 | 256 |

All six processes verify exact cursors, event IDs and payloads for accepted
receipts using bounded replay. Per-cycle outcome counts balance. There are no
failed calls, queues remain bounded, and shutdown closes with no unresolved
work. Partial SQLite population coverage is a failed all-stream-service profile,
not a passing result disguised by successful replay of its accepted subset.

Source and method: the benchmark is built from the archived round 105 bounded
source. The archive and binary hashes, host and commands are retained in
[provenance](round106-provenance.json) and [raw runs](round106-population.jsonl).
[Collector](round106-run.py) and [Home exporter](round106-export.py) reproduce
collection and aggregation. Home reports medians across three samples; counts
are medians, not totals. RSS includes setup, allocator and benchmark state and
is a process-lifetime peak. No concurrent builds/tests were started while
sampling; other host activity was not controlled. These are short diagnostics,
not long-running stability or settled-memory measurements.

The next implementation question is whether bounded group commit improves
SQLite accepted throughput without weakening FULL durability or increasing
retained memory excessively. Queue wait dominates these receipt tails, but
these measurements alone do not isolate disk flushes as the cause. Keep batch
size and delay bounded and preserve per-event outcomes; compare the same offered
load and count all rejections. Larger queues alone would mostly permit more
waiting and consume more memory.

All 19 verification tests pass after exporting the two Home cells. Native
rendering was not inspected in this round.
