# One-minute sustained workload

Six fresh processes use the archived round 102 bounded-generator binary. Each
run offers 48,000 events across four streams over one minute: four producers,
128-byte payloads, and a 5 ms per-producer interval. Memory and SQLite each have
three repetitions. SQLite retains DELETE journal mode and FULL synchronization.
No production or harness source changed during this round.

The collector checks every outcome and sampled call count, generator occupancy,
runtime queue bounds and shutdown completion. Missing offers are not hidden by
waiting for prior receipts. Generator and runtime rejections are counted
separately. These runs exceed the previous 16,384 scheduled-offer restriction.

[Raw output](round103-sustained.jsonl), [provenance](round103-provenance.json),
[collector](round103-run.py) and [summary](round103-summary.json) retain commands,
source archive identity and binary hashes. The binary was checked before and
after the matrix. Builds and tests were paused. The desktop remained open;
ambient host activity was not controlled.

These results describe four active streams, not a large active population.
There are no subscribers, cleanup operations or mixed recovery work in this
profile. MemoryStore retains the growing event history. Peak RSS includes the
harness, stored history and measurement samples. Immediate post-shutdown RSS
is not a settled-memory or leak measurement. The next
[population workload](../population-workload.md) defines actual rotation across
1k/10k/100k streams; it is not implemented by this four-stream scenario.

## Results

All six runs accepted all 48,000 offers, with zero generator/runtime rejections
and unexpected failures. Shutdown closed successfully with no unresolved work.

| Store | Receipt p99 median ms | Per-run p99 range ms | Peak RSS median MiB | CPU median seconds |
| --- | ---: | ---: | ---: | ---: |
| memory | 0.087 | 0.076–0.096 | 29.16 | 2.10 |
| sqlite | 6.207 | 3.117–87.174 | 15.08 | 21.49 |

Memory generator occupancy peaked at eight tasks in every run. SQLite peaked
at 119, 54 and 31, below the configured cap of 256. The first SQLite run's
receipt p99 was 87.17 ms, with queue-wait p99 at 86.18 ms and store-service p99
at 2.43 ms. Those percentiles describe different samples and must not be added.
They show a substantial waiting tail; they do not isolate its underlying cause.
Later SQLite receipt p99 values were 6.21 and 3.12 ms. Do not drop the first run
or claim a universal 6 ms bound from the median.

The user's preference is to accept a few milliseconds of extra latency when
substantial memory is saved, especially at large populations. AGENTS.md now
records that tradeoff. It still requires measuring the actual population,
preserving successful work and keeping latency/queues bounded. This four-stream
run does not establish the tradeoff at 100,000 agents.

All 19 verification tests pass after adding the two Home measurement cells.
No current native rendering or complete release-budget qualification is claimed.
