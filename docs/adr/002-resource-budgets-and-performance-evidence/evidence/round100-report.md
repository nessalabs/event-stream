# Mixed workload diagnostic

All twelve fresh processes accepted all 512 foreground offers. Every run checked
exact history after SQLite close/reopen. The six mixed runs also parsed captured
input, transferred a snapshot, reclaimed 32 source records, lost one reply after
a real replica commit, retried it, and verified replica cursor 33. Foreground
receipts completed during maintenance in every mixed run. Each run made 64
bounded replay calls. This does not prove replay overlapped every maintenance
stage.

Each cell has three samples. Entries below are medians, except the p99 range.
RSS includes SQLite, the workload harness and the profiler. Rust live allocation
excludes SQLite's C allocator. Elapsed time includes setup and final verification.

| Workload | Elapsed ms | CPU ms | Sampled peak RSS MiB | Receipt p99 range ms |
| --- | ---: | ---: | ---: | ---: |
| Control, 64 KiB setting | 662.24 | 232.21 | 8.03 | 1.45–51.86 |
| Mixed, 64 KiB snapshot | 662.05 | 264.39 | 9.05 | 1.41–11.55 |
| Control, 1 MiB setting | 657.82 | 277.98 | 8.06 | 1.82–5.26 |
| Mixed, 1 MiB snapshot | 665.03 | 334.10 | 12.39 | 1.34–26.21 |

The control skips maintenance, including snapshot creation. Its snapshot size is
only a pairing label. The large variation in controls prevents attributing the
latency differences to maintenance. These samples do not show a speedup. The
arrival schedule lasts about 640 ms and dominates total elapsed time, so elapsed
time is not a throughput comparison. Scheduling lateness p99 is recorded
separately; its per-cell medians are about 2.25–2.35 ms.

The generator permits at most 64 outstanding tasks. Observed queues stayed
within configured bounds. This is one foreground stream with 800 offered events
per second, not sustained 1,000/10,000/100,000-agent qualification. Peak memory is
sampled every millisecond and may miss shorter peaks. No numeric release budget
was inferred from the results.

## Reproduction and review

[Raw samples](round100-mixed.jsonl), [summary](round100-summary.json),
[provenance](round100-provenance.json), [collector](round100-run.py), and
[Home exporter](round100-export.py) retain the commands and measurement scopes.
The source archive and SHA-256 manifest are stored beside this report. The
collector checks the binary hash before and after all runs. Builds/tests were
paused during measurement. Host power and starting load are recorded; unrelated
host activity was not controlled. Three repetitions cannot characterize rare
latency tails.

The native console now registers the same shared fixture as **Mixed workload**
under Product readiness. Its smaller run uses 128 offers and a 64 KiB snapshot.
The callback's execution time is a correctness diagnostic. Its resource metrics
remain unavailable; the Home measurements come from the isolated executable.
Current native visual rendering remains unverified.

The next qualification work needs longer repeated runs, explicit workload
budgets, memory after activity settles, and sustained logical-agent populations.
Group commit remains a separate unimplemented experiment. The current evidence
does not close those gates.

## Validation

All 19 verification library tests pass, including the new registered callback
and Home validation. A fresh standalone headless callback passes its five
observations. Strict all-feature/all-target core Clippy passes. Source manifest
for the console and test/callback logs are stored beside this report. Production
code did not change in this round.
