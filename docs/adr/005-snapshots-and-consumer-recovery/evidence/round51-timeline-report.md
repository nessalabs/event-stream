# Snapshot verification timeline diagnostic — round 51

The earlier p99 failure did not recur. This does not close the failed round 25 gate. This run uses newer source and additional timeline instrumentation. It is a diagnostic, not an improvement comparison or release qualification.

Six fresh release-mode processes ran three paired SQLite controls and snapshot verifications. Each process committed 256 exact records using four producers that each wait for their previous append. The snapshot is 1 MiB. All six correctness checks passed. No sync timeline samples were dropped.

| Pair | Control append p99 | Verification append p99 | Verification maximum | Longest append overlapping sync time |
|---|---:|---:|---:|---:|
| 1 | 1.153 ms | 3.191 ms | 3.757 ms | 0.603 ms |
| 2 | 1.192 ms | 3.445 ms | 3.975 ms | 0.632 ms |
| 3 | 1.107 ms | 3.200 ms | 3.680 ms | 0.595 ms |

Verification took 3.72–4.02 ms. Eight append intervals overlapped verification in each run. Appends outside that phase stayed below 1.48 ms. The longest individual sync call was below 0.081 ms across all runs. The longest verification-run append overlapped about 0.59–0.63 ms of sync intervals, counting their union so overlapping intervals are not counted twice.

These are associations on a shared clock. They do not assign the remaining delay to CPU, scheduling, the worker queue, or SQLite. A sync call interval is also not a measurement of physical disk service time. We still need evidence for the earlier 7.46 ms outlier before choosing a fix.

Controls execute only an empty phase timestamp probe. A computed interval overlap with that nanosecond probe has no snapshot-work meaning; use the raw harness's zero verification overlap for controls.

## Reproduction and limits

The source archive includes its manifest and collector. The runner validates archived file hashes, records binary identity before and after, alternates pair order, and uses a 60-second child watchdog. It retains the `.partial` raw file deliberately. Three pairs are insufficient to characterize rare latency tails. This workload uses closed-loop producers, not independent offered load or 100,000 active agents.

The host was an Apple M5 with 24 GiB RAM on macOS 26.6. Both implementation agents paused builds and tests for this run. Other host activity was not controlled. CPU measures the timed process user-plus-system delta. Home memory measures sampled peak process RSS. Rust allocation counters exclude SQLite's C allocations. UI rendering and evidence saving are outside the child timing region.

Build: `cargo build --locked --release --no-default-features --features sqlite,snapshots,test-support --example snapshot_resource`, using the archived source and an isolated target directory. The build succeeded in 20.40 seconds with two unused-import warnings. The runner completed with exit 0.

Source archive SHA-256: `0882155840c4a5d325a7a6c9c418cfc25bbd360d0678b7868d92c0556a18f57d`.

Binary SHA-256: `ee46f588135bfc4b711def9fd4e764f823ae0154d385166390e3c282eff098e9`.

Raw JSONL SHA-256: `c660c714ca14f83311c7d6c7e5080e3095881d389e35f340376f9060b0436f01`.

Evidence: [raw samples](round51-timeline.jsonl.partial), [provenance](round51-timeline-provenance.json), [analysis](round51-timeline-analysis.json), [runner](round51-run-timeline.py), [archived source](round51-timeline-source.tar), [build log](round51-timeline-build.log), [run log](round51-timeline-run.log).
