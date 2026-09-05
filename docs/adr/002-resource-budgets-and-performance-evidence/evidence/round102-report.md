# Bound scheduled arrivals before longer scale runs

The sustained benchmark no longer creates a task for every future offer. One
generator calculates deadlines and retains at most 256 append tasks. Full
capacity increments an explicit generator rejection count. Runtime rejections,
failures, successful receipts and scheduling delay remain separate. Completed
outcomes are collected during generation rather than retained as finished tasks.

This changes the harness, not the event-stream implementation. Every production
source hash matches the archived control. Both binaries use all features and the
same settings, including SQLite DELETE journal mode and FULL synchronization.

## Experiment

Three interleaved fresh processes per store and generator offer 4,000 events:
four producers, four streams, 128-byte payloads, 5 ms between each producer's
offers. Each schedule spans about five seconds. The bounded generator has 256
slots; the control allocates all 4,000 scheduled tasks. A fixed sample cap still
bounds retained measurements. The obsolete 16,384 sustained-offer restriction
is removed; the separate overload scenario retains its existing restriction.

[Collector](round102-run.py), [raw output](round102-sustained.jsonl),
[provenance](round102-provenance.json), and [summary](round102-summary.json)
retain commands, source/binary identities and all outcome counts. Each binary's
hash is checked before and after the matrix. Builds and tests were paused during
measurement. Ambient host activity was not controlled.

Receipt latency starts at runtime submission. Scheduling lateness is sampled at
task start before event construction. A generator-rejected offer has no runtime
call or receipt sample. The collector checks conservation, sample counts, queue
bounds and resolved shutdown for every run.

The hard-cap test holds two operations until a single-threaded offer loop has
processed all six offers. It verifies two spawned tasks and four generator
rejections without relying on timing. A separate test rejects a zero task limit.

These are diagnostics. A memory reduction here removes waiting-task and outcome
state from the harness. It does not demonstrate reduced production allocations
or improved runtime capacity. Longer rotating-population workloads, settled
memory, mixed-feature release budgets and sustained 100k-agent service remain
unqualified. Config fingerprints distinguish the two generators in Home.

## Observed results

Each entry is the median of three fresh processes. All runs accepted all 4,000
offers with no generator/runtime rejections or unexpected failures.

| Store / generator | Peak RSS MiB | CPU ms | Receipt p99 ms | Scheduling p99 ms |
| --- | ---: | ---: | ---: | ---: |
| memory / control | 13.44 | 158.09 | 0.082 | 2.840 |
| memory / bounded | 6.28 | 160.19 | 0.090 | 2.538 |
| sqlite / control | 15.34 | 1587.03 | 2.932 | 2.912 |
| sqlite / bounded | 7.94 | 1537.83 | 5.081 | 2.991 |

Peak RSS falls about 53% for Memory and 48% for SQLite in these runs. The
bounded generator reaches four tasks on Memory and 8–15 on SQLite. Total
allocated bytes fall only slightly: most event work still happens, but finished
or future tasks no longer remain live together. This is a reduction in retained
harness state, not eliminated storage work.

Receipt p99 increases in the measured medians: about 0.082 to 0.090 ms for
Memory and 2.93 to 5.08 ms for SQLite. Do not label this a speedup. Three short
runs do not isolate host variation from scheduling changes. Keep both values
visible while investigating longer workloads. The generator is retained because
its task count is bounded independently of experiment duration, not because it
is claimed faster.

The three example tests, strict all-feature/all-target Clippy and formatting
checks pass. Home contains four new measured cells with distinct generator
configurations. Native rendering was not inspected.

All 19 verification tests pass after the Home update. Rust 1.85 accepts the
changed benchmark target. These checks do not qualify sustained-scale capacity.
