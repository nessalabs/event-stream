# Scheduled-arrival diagnostic

Twelve fresh processes use the archived round 95 release binary. Four producers offer 128-byte events to four streams. Each request has its own deadline. Slow writes do not postpone later deadlines.

These are short diagnostic runs. The 800/s profile schedules 4,000 requests across about five seconds. The 16,000/s profile schedules 16,384 requests across about one second. All processes account for every outcome, preserve queue limits and finish shutdown without unresolved work.

| Store | Requested rate/s | Accepted / offered (median) | Receipt p99 ms | Scheduling p99 ms | Peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| memory | 16000 | 16384 / 16384 | 0.404 | 2.140 | 41.83 |
| memory | 800 | 4000 / 4000 | 0.091 | 2.833 | 13.09 |
| sqlite | 16000 | 4063 / 16384 | 154.170 | 2.130 | 38.72 |
| sqlite | 800 | 4000 / 4000 | 12.173 | 2.753 | 14.09 |

Each latency value is the median of three per-run p99 values. Receipt latency starts when the task submits its request. Scheduling lateness measures how late the task woke relative to its intended deadline. Do not add these percentile values: the slowest requests may differ.

SQLite uses DELETE journal mode and FULL synchronization. Its overload result describes this adapter and configuration; it does not isolate engine execution from transaction, flush, queue and runtime costs. Four streams each allow 128 queued appends, so together they can reach 512 before the global 1,024 limit.

The harness allocates one task per future offer, including tasks waiting for their deadline. Peak RSS therefore includes load-generator memory. This short test does not establish long-running steady state, idle-agent memory, delivery latency, or 100,000 active producers. No numerical release latency budget was selected for these diagnostics.

The raw output also records store-service and queue-delay samples. Further investigation should isolate these costs before changing storage or scheduling.

[Raw runs](round96-sustained.jsonl) · [Provenance](round96-provenance.json) · [Summary](round96-summary.json) · [Source archive](round95-source.tar)
