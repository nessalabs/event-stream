# Round 94 simultaneous-request scale baseline

Eighteen fresh processes completed: three repetitions for each combination of
Memory/SQLite and 1,000/10,000/100,000 Tokio tasks. Every task submits one
`try_append` after a shared release barrier. Every outcome is accounted for,
there are no failed calls, the queue stays at or below 1,024 entries, and
shutdown leaves no unresolved work.

This is overload evidence. It is not 100,000 successful concurrent producers.
At 10,000 and 100,000 tasks, most requests are rejected by the fixed queue limit.

| Store | Tasks | Successful requests, range | Median timed burst | Median process peak RSS |
| --- | ---: | ---: | ---: | ---: |
| Memory | 1,000 | 1,000 | 8.48 ms | 8.91 MiB |
| Memory | 10,000 | 1,025–1,027 | 48.61 ms | 37.20 MiB |
| Memory | 100,000 | 1,025–1,026 | 492.95 ms | 303.20 MiB |
| SQLite | 1,000 | 1,000 | 296.92 ms | 10.56 MiB |
| SQLite | 10,000 | 1,025 | 329.43 ms | 37.77 MiB |
| SQLite | 100,000 | 1,025 | 817.36 ms | 283.78 MiB |

The timed interval includes task setup, barriers, request outcomes and profiling.
It excludes stream setup and shutdown, which are recorded separately. RSS is a
process-lifetime peak and includes setup. The two columns do not cover identical
intervals. SQLite uses the existing DELETE/FULL durability profile. Rejections
must not be included as successful streaming throughput.

The process had 11 threads with Memory and 12 with SQLite at each tested size.
Tasks are not OS threads. Runtime queue bounds stayed fixed as task count rose.

At 100,000 tasks, the Rust allocation delta while preparing tasks was
155,839,050 bytes on both adapters. Source inspection found a large `Outcome`
value retained by every pending task. This includes many vectors and counters
that are unnecessary for a task performing one append. That is a harness/caller
cost, not proof that the library itself needs 156 MB for pending requests.
A compact result implementation is being prepared for a controlled follow-up.
It must retain the same offers, scheduling barriers, sampling and outcome checks.
No reduction is claimed before measuring it.

The baseline remains useful: it exposes real costs of an application pattern
with one task per request. But resource claims for the core need to separate
caller tasks, stream metadata, retained history and profiling state. The raw
resource records include ready-task, startup, settled and post-drop observations.
Rust requested-byte counters exclude allocator metadata and SQLite's C heap.

- [Raw process outputs](round94-burst.jsonl)
- [Cell summaries](round94-summary.json)
- [Run provenance and archived source identity](round94-provenance.json)
- [Runner](round94-run.py) and [Home export](round94-export.py)
- [Build log](round94-build.log)

No project builds or tests ran during measurement. The standalone child headers
say source/input digest not provided; the outer provenance records the archived
source and binary hashes, and each raw run records its exact command. The
fixture deterministically derives stream/event IDs and payloads from that command.

Remaining work: sustained independent arrivals, active producers over time,
recovery after repeated bursts, mixed replay/journal/snapshot/replication work,
and explicit release memory/latency budgets. This result does not close those gates.
