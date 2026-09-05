# Round 93 snapshot resource checkpoint

All 48 child processes passed their correctness checks. All 12 matched
foreground p99, maximum-latency and overlap gates passed. The thresholds are
unchanged: p99 at most twice its matched control plus 5 ms; maximum at most
three times its matched control plus 20 ms; at least one overlapping append.

This qualifies those checks for this run and feature profile. It does not prove
what caused the round 25 failure or qualify the full product at agent scale.
The historical failed run remains part of the evidence.

| Store | Snapshot | Median total runtime | Median sampled peak process RSS |
| --- | --- | --- | --- |
| Memory | 1 MiB | 8.275 ms | 5.02 MiB |
| Memory | 16 MiB | 91.453 ms | 23.94 MiB |
| Memory | 64 MiB | 357.268 ms | 84.38 MiB |
| SQLite | 1 MiB | 87.198 ms | 7.30 MiB |
| SQLite | 16 MiB | 289.572 ms | 10.89 MiB |
| SQLite | 64 MiB | 955.506 ms | 10.92 MiB |

Each cell uses three instrumented runs. Total runtime includes setup, fixture
hashing, snapshot operations, recovery, shutdown and SQLite reopen/cleanup.
RSS includes the process; it is not just the library heap. Rust allocator
counters exclude SQLite's C allocations. Per-metric medians are independent.

For the 1 MiB foreground cases, SQLite verification append p99 ranged from
3.120 to 3.222 ms; recovery ranged from 1.297 to 1.305 ms. Memory verification
ranged from 1.796 to 1.798 ms; recovery from 0.049 to 0.059 ms. These use four
closed-loop producers, 64 requests each. They do not model independent arrivals
or 100,000 active agents. Sampler/VFS-disabled controls are retained in the raw
matrix but not mixed into the instrumented Home medians.

The features were `sqlite,snapshots,test-support`. Forty-six archived
source/config files were checked against their manifest before execution.
No project builds or tests ran concurrently. Ambient desktop applications
remained open; host, power and load details are in the provenance file.
The reduced feature build emitted unused/dead-code warnings; those remain in
the build log. They do not change these executed results.

- [Raw samples, per-run gates and completion](round93-snapshot.jsonl)
- [Source archive](round93-source.tar)
- [Host and run provenance](round93-provenance.json)
- [Summary data](round93-summary.json)
- [Build log](round93-build.log)
- [Home export script](round93-export.py)

Binary SHA-256: `0c5e65ec2cbc24426ecbf31156c3be690dc39ca54ad6ce05a59adc12d40ed7c2`.
It was unchanged across the run. Archive SHA-256:
`bb19cee0ee40a271df15f12f3a7772c48a8f3e12832f453436d61a825e5378aa`.

Remaining work includes explicit memory release budgets, independent-arrival
load, the full mixed journal/replication workload, and logical-agent scaling.
The newer harness includes timeline instrumentation; Home uses its fingerprint
to avoid drawing an improvement line to incompatible historical samples.
