# Rotate writes through 1k, 10k and 100k streams

The new `population_sustained` workload visits every stream instead of mapping
all work to a few producers' streams. Offer number determines the stream and
cycle. The generator still retains at most 256 append tasks. A per-offer ledger
records actual committed cursor offsets, and per-cycle counters preserve every
accepted, rejected and failed outcome.

After active timing, bounded replay verifies every accepted ID, schema, payload
and cursor on every stream. It does not assume task scheduling preserves cycle
order. Verification reports uncovered streams explicitly. A mismatch fails the
process and invalidates its resource sample. The ledger moves into verification
without duplicating its buffers. Reported ledger/counter capacity excludes other
harness and runtime allocations; process RSS includes them.

## Scope

This first matrix runs one fresh process per store/population at 1,000, 10,000
and 100,000 streams. Each stream is offered two events, with four synchronized
producer offers every 5 ms. Aggregate offered rate is 800/s. The nominal revisit
intervals are 1.25, 12.5 and 125 seconds. This is sparse activity across a large
population, not 800/s per agent or 100,000 simultaneous append requests.

SQLite uses DELETE journaling and FULL synchronization. Setup includes creating
stream identities and is reported separately. Active CPU/latency/allocations
exclude the later replay verifier. Peak RSS is a process-lifetime value observed
at the active boundary and includes setup. MemoryStore retains the full history;
SQLite stores it on disk. Comparing their RSS does not compare equivalent cache
retention policies.

These are single diagnostic samples, not repeated release qualification. No
production source changed in this round. Library behavior is exercised through
its public APIs with explicit finite limits. Builds/tests were paused during
measurement; ambient desktop activity was not controlled.

## Reproduction and validation

[Raw matrix](round104-population.jsonl), [provenance](round104-provenance.json),
[collector](round104-run.py), and [summary](round104-summary.json) retain commands
and source/binary identities. The archive and source manifest are beside this
report. The collector verifies the binary hash before and after the matrix.

Six example tests pass, including uneven producer/population mapping, cycle
accounting, actual receipt order and corrupt cursor/ID/payload rejection. A
separate release smoke uses five streams and three producers across six cycles
on Memory and SQLite. Both produce and verify 30 exact receipts. The larger
matrix uses two complete cycles and checks per-cycle outcome conservation.

The remaining gates include repeated runs, a range of per-agent/aggregate rates,
subscribers, settled memory, mixed recovery workloads and explicit release
budgets. These results cannot establish that the library is the fastest or that
all 100,000-agent traffic profiles are supported.

## Results

All six samples accept every offer and cover every stream, with zero rejected
or failed calls. Exact replay and resolved shutdown pass. Values below are
single samples, not medians across repeated runs.

| Streams | Store | Accepted | Receipt p99 ms | Peak RSS MiB | Setup s | Verify s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | memory | 2,000 | 0.126 | 6.53 | 0.01 | 0.05 |
| 1,000 | sqlite | 2,000 | 5.480 | 7.84 | 0.40 | 0.10 |
| 10,000 | memory | 20,000 | 0.126 | 27.28 | 0.10 | 0.27 |
| 10,000 | sqlite | 20,000 | 3.206 | 13.83 | 2.90 | 1.03 |
| 100,000 | memory | 200,000 | 0.111 | 187.27 | 0.93 | 3.44 |
| 100,000 | sqlite | 200,000 | 3.495 | 24.14 | 39.66 | 10.24 |

At 100k streams, Memory holds all 200k records and reaches about 187 MiB
process RSS. SQLite reaches about 24 MiB RSS with the history on disk. Both
verify all 200k records; latency sampling remains capped and systematic. The
receipt ledger and cycle counters use about 1.6 MB of vector element capacity
in each 100k sample, separate from runtime/history/sampling memory.

The SQLite setup cost is real: creating 100k stream identities takes about
39.7 seconds in this run. It is excluded from the subsequent 250-second active
phase and must not disappear from operational planning.

A source review also found a full timestamp-index scan inside bounded Memory
retention cleanup. The [review](../../006-retention-compaction-and-retry-horizons/timestamp-cleanup-review.md)
records a concrete follow-up. This workload does not run cleanup and cannot
measure or qualify that change.

All 19 verification tests pass after the Home update. The final benchmark
source passes Rust 1.85 checking. Native rendering remains unverified.
