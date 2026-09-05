# Bound commit-time cleanup work

Implemented and checked in round 105. MemoryStore retention cleanup no longer
scans the full timestamp map after a bounded record cleanup call. It removes
ordered entries only through the last physically removed record and never
performs more removals than the record cleanup count. No key buffer is added.

The regression preserves exact timestamp/history correspondence during partial
cleanup and keeps original commit ages for the surviving suffix. Existing
replication-age checks pass. [The paired report](../002-resource-budgets-and-performance-evidence/evidence/round105-report.md)
records the failing-before/passing-after evidence and repeated measurements.

The improvement is specific to MemoryStore retention with replication enabled.
It does not establish a new global throughput or memory guarantee.
