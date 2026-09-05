# Round 112: bounded SQLite stability screen

The frozen individual-append runtime passes one longer sparse-load check. It
accepts all 100,000 offers across 10,000 streams and ten complete cycles at 800
aggregate offers/s. Each stream is revisited about every 12.5 seconds. No runtime
batching or other production change was enabled.

Active duration: 124.999 seconds. Receipt p99:
3.037 ms. Process-lifetime peak RSS:
19.94 MiB. Peak generator tasks: 61.
All accepted IDs, payloads and cursors replay exactly. Shutdown resolves all work.

[Gates](round112-gates.json) were saved before execution: zero rejected/failed
offers, full coverage, exact replay, bounded task/queue counts, resolved shutdown,
RSS below 128 MiB and receipt p99 below 1 second. The two resource ceilings are
coarse diagnostic screens, not customer SLOs. [All checks](round112-checks.json)
pass. This single sample does not prove repeatability or long-running stability.

[Raw output](round112-run.json), [provenance](round112-provenance.json), source
archive/hashes, collector and Home exporter are retained. Release build uses all
features and SQLite DELETE/FULL. No build/tests ran while sampling. Other host
activity was not controlled. RSS includes setup and harness allocations; history
remains retained, so this does not measure settled memory after cleanup. There
are no subscribers, maintenance or injected faults in this workload; prior fault
tests remain separate evidence. No wider optimization matrix was run.

All 19 verification tests pass after the Home evidence update.
