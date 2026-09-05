# Remove a full-index scan from bounded cleanup

MemoryStore retention cleanup used BTreeMap::retain on all commit timestamps
after every bounded record cleanup call. A tiny deleted prefix and large
surviving suffix caused repeated scans of history that did not need work.

Cleanup now tracks the last record actually removed. It pops timestamp entries
in order, stopping at that offset or the number of records removed, whichever
comes first. No temporary key vector is allocated. Timestamps for physically
unreclaimed records remain until their own cleanup step. Visible suffix records
keep their original commit ages for replication.

The source work changes from scanning the timestamp map each call to at most
one ordered removal per reclaimed record. Each tree operation may traverse the
tree height; this is not a constant-time claim for arbitrary tree size.

## Correctness

A regression with 4,096 records and a four-record floor first failed against the
old code: a one-record cleanup removed four timestamps. It now verifies that
one timestamp disappears per reclaimed record and every remaining timestamp
still matches its original value. The history and timestamp key sets match
after each step. Existing replication-age and retention checks pass.

The full library/integration suite and two operational examples pass **325
tests**. Strict all-feature/all-target Clippy, Rust 1.85 and a retention-only
feature check pass. The latter verifies the replication-disabled branch.

## Paired resource evidence

Five interleaved fresh processes per version start with 100,000 records. Both
perform 256 one-row cleanup calls, then verify the exact IDs/payloads/cursors of
all 99,744 surviving records. Setup and suffix verification are outside cleanup
wall timing. SQLite is not involved; this measures the Memory adapter with
replication enabled.

| Metric | Full scan | Bounded removal |
| --- | ---: | ---: |
| Median wall time for 256 calls |119.449ms|0.099ms|
| Observed wall-time range |87.695–125.205ms|0.094–0.170ms|
| Median CPU time |113.642ms|0.145ms|
| Allocated bytes in observed scope |35,112|35,112|

The same logical work completes with much less scanning. This supports a
cleanup-specific improvement, not a library-wide speedup. CPU/allocation
counters include sampler construction; wall timing starts afterward. The 1 ms
sampler may miss the short bounded region. Missing samples stay unavailable.
Lifetime process RSS includes 100k-record setup and is not a cleanup heap bound.

[Raw samples](round105-cleanup.jsonl), [provenance](round105-provenance.json),
[collector](round105-run.py), [summary](round105-summary.json) and separate source
archives retain the exact comparison. Both variants use the same harness and
differ only in memory_retention.rs (including a release-disabled unit test).
Binary hashes are checked before and after the matrix. Builds/tests were paused;
ambient host activity was not controlled. Native rendering remains unverified.

Repeated population qualification, mixed workload budgets and group-commit
evaluation remain open. This fix closes the identified timestamp cleanup scan.

All 19 verification tests pass after adding the paired Home rows.
