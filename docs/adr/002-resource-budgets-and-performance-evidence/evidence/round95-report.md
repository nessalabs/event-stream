# Round 95 compact caller-task comparison

The compact burst harness saves **115,200,000 bytes** of Rust allocations while
preparing 100,000 waiting tasks. This is a caller/harness improvement. The two
archives contain identical library source; only `examples/performance_baseline.rs`
differs.

Each task used to retain the general 1,152-byte `Outcome` across its append and
return it to the collector. That structure contains counters and vectors for
many scenarios. A one-request task now returns a 16-byte typed result. The
collector updates the shared outcome and latency vectors after joining it.
Tasks also avoid cloning unused `Args` fields and a redundant barrier handle.

## Matched results

Twelve fresh processes: three paired repetitions per store, alternating order.
Each releases 100,000 Tokio tasks together, submitting one 128-byte event each.
Queue capacity remains 1,024. All 100,000 outcome latency samples remain present.
Successful and rejected sample counts match their respective outcomes.

| Store | Harness | Median timed burst | Median process peak RSS | Ready-task Rust allocation delta |
| --- | --- | ---: | ---: | ---: |
| Memory | Baseline | 489.27 ms | 303.30 MiB | 155,839,050 bytes |
| Memory | Compact | 375.29 ms | 193.75 MiB | 40,639,050 bytes |
| SQLite | Baseline | 775.47 ms | 283.86 MiB | 155,839,050 bytes |
| SQLite | Compact | 680.00 ms | 173.72 MiB | 40,639,050 bytes |

Waiting-task requested allocation falls about 74%. Process RSS includes stream
setup, allocator retention, profiling and Tokio; it is not a core-library heap
measurement. Timed burst scope excludes stream setup and shutdown. Per-column
medians are independent and do not identify one representative run.

Memory baseline commits 1,025–1,027 requests; every other cell commits 1,025.
The remaining requests are rejected, with no failed calls. Both versions keep
the queue within 1,024 and drain shutdown without unresolved work. Faster total
burst time is not a claim of increased successful-event throughput or service
for 100,000 sustained producers. The accepted-count variation is retained.

No durability setting, offered request, barrier, queue capacity, latency sampling
rule, or probe registration/cancellation behavior was removed. The workload has
no subscribers. Reduced task bookkeeping explains the memory result without
requiring a new allocator, unsafe code or a lock-free structure.

## Reproduction and remaining scope

- [Raw paired process outputs](round95-paired.jsonl)
- [Cell summaries](round95-summary.json)
- [Binary/archive identity and host provenance](round95-provenance.json)
- [Compact source archive](round95-source.tar)
- [Paired runner](round95-run.py) and [Home export](round95-export.py)
- [Build log](round95-build.log)

The baseline is the archived round 93 source used by round 94. Only the harness
file differs in the compact archive. Each binary is hashed before and after
measurement. No project builds or tests ran during the paired measurement.

The core's sustained service capacity, independent-arrival overload behavior,
full mixed workload and release budgets remain open. This result demonstrates
that applications should avoid retaining general-purpose aggregate result
structures inside every pending task. It does not remove the need to measure
stream metadata and actual in-flight event state separately.
