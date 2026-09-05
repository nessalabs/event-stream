# Snapshot resource evidence

Status: measured diagnostic checkpoint; **not qualified**. All 48 fresh child
processes passed their correctness checks. One of the 12 foreground latency
comparisons failed its published p99 limit. The collector exited 1 and retained
[the raw matrix](evidence/snapshot-resource-26a45d44b85ede3d.jsonl.partial).
No failed cell was dropped or replaced with another run.

## What ran

The full workload covered Memory and SQLite at 1, 16 and 64 MiB. Each cell has
three instrumented repetitions and one sampler/VFS-disabled control. Allocation
counters remain enabled in those controls. Foreground work used four closed-loop
producers with 64 appends each, alongside snapshot verification or recovery.
Each mode has three repetitions and its own matched fixture control. This is
not an independent-arrival overload or concurrent-agent capacity test.

The full elapsed scope includes open, fixture events, deterministic snapshot
hashing, upload, verification, publication, recovery and shutdown. SQLite also
includes reopen and cleanup. It is not an isolated commit latency. Foreground
elapsed time starts at barrier release and ends after snapshot work and every
append receipt. UI rendering is outside both measurements.

## Size sweep

Medians of three instrumented fresh-process runs:

| Store | Snapshot | Full elapsed | Sampled peak process RSS |
| --- | ---: | ---: | ---: |
| Memory | 1 MiB | 7.94 ms | 4.97 MiB |
| Memory | 16 MiB | 90.09 ms | 23.91 MiB |
| Memory | 64 MiB | 352.55 ms | 84.14 MiB |
| SQLite | 1 MiB | 102.99 ms | 7.25 MiB |
| SQLite | 16 MiB | 291.09 ms | 10.77 MiB |
| SQLite | 64 MiB | 963.57 ms | 10.80 MiB |

MemoryStore retains snapshot bytes in memory. SQLite stores them on disk and
uses bounded reads. These measurements are consistent with that difference;
they do not by themselves prove a universal bound. Sampled RSS can miss brief
peaks. Rust allocation counters exclude SQLite's C allocations. VFS bytes are
requested callback bytes, not physical device writes or APFS allocated space.
The raw rows include phase-local VFS deltas and run-wide temporary-file peaks.

## Failed foreground gate

Schedule 44 was SQLite verification with foreground appends. Its matched
control was schedule 42:

| Measurement | Control | Verification |
| --- | ---: | ---: |
| Append p99 | 1.155292 ms | 7.462375 ms |
| Append maximum | 1.203375 ms | 8.505500 ms |
| All work elapsed | 64.896708 ms | 143.698208 ms |

The p99 limit was `2 × control + 5 ms = 7.310584 ms`; the observed value exceeded
it by 0.151791 ms. The maximum-latency gate passed. Eight append intervals
overlapped snapshot work. All 256 appends committed with distinct contiguous
cursors and no rejection or failure.

Snapshot work itself took 3.853791 ms. The current samples cannot attribute each
slow append to a particular storage operation or scheduler delay. Investigate
that timing before changing implementation or limits. Re-running unchanged work
until it passes would not resolve the failed qualification.

## Provenance and limits

- Source epoch: `26a45d44b85ede3d204987730d20b193a511da8daedcdb27ef0be7805b25b89f`.
- All 37 manifest-listed source files matched both archive and extracted build
  directory; the archive also contains its matching source manifest.
- Archive SHA-256: `6b188036c840796eb35e80481f019c9eedba112c2665d84d97dcb2f9d0c6ea73`.
- Executed binary SHA-256 before and after:
  `097566025fff4a67300961e66b7d669ea967cd4b1ec1bdf53e1c8d9cc5e70ce8`.
- Raw matrix SHA-256: `35d94fb03e146388541b6a446e497d35305d966f3eebd128ed5eba813fa9eac0`.
- Execution: 2026-09-05 08:28:14–08:28:23 UTC, Rust 1.98.1, Apple M5,
  24 GiB RAM, macOS 26.6. The [host record](evidence/snapshot-resource-host-round25.json)
  records battery power and load before execution. No project builds or tests
  ran concurrently. The release build log was not persisted.

Home round 25 contains all 20 aggregated cells, including controls, and states
that the matrix is unqualified. Its [loader check](evidence/round25-home-test.log)
passed. No snapshot memory release threshold, overall phase completion,
100,000-agent capacity or fastest-library claim follows from these results.

## Later current-source checkpoint

[Round 93](evidence/round93-report.md) reruns the full fixed 48-process matrix
from an archived release build. All correctness and 12 unchanged foreground
gates pass. This is fresh evidence for that source/profile; the earlier failed
run above remains valid historical evidence. The report does not claim a causal
fix, comparable old/new timing where instrumentation differs, or complete
product resource qualification. Memory thresholds and mixed/agent-scale work
remain open.
