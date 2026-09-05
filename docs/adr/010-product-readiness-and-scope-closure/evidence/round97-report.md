# Operational recovery and aggregate staging limits

Two executable walkthroughs now cover operational use. The
[operations guide](../../../operations.md) explains how to run them against new
directories and inspect the retained databases.

- `local_recovery` reaches a SQLite page quota, verifies exact prior history,
  retries an existing event, drains shutdown, makes a closed backup, reopens,
  restores through the controlled manager, rejects old cursors, and verifies a
  new append after another restart.
- `replicated_recovery` reaches a required replica's backlog limit, closes both
  stores, catches up after reopening, retries the rejected input, and reopens
  again to verify both the origin acknowledgement and exact destination history.

The examples use production public APIs. They do not implement an online backup
service, network transport, automatic failover, or a benchmark. A refused page
quota is distinct from a physical disk-full fault.

## Fixed quota behavior

SQLite previously compared each bootstrap's staged record count and bytes with
the entire destination staging budget. Several uploads could each fit while
their combined retained rows exceeded the budget.

The adapter now keeps aggregate staged-record counters in its accounting row.
A batch checks and updates those totals in the same transaction as its records.
An exact retry does not charge twice. Abort keeps the charge until bounded
cleanup deletes the physical records. Publication copies the suffix into
canonical history and atomically deletes its redundant staged rows and batch
receipts. The publication receipt remains available for exact retries.

Migration adds the counters for older databases. Opening audits the totals;
controlled restore rebuilds them. New publications no longer retain duplicate
suffix records. Restore accepts that layout and older backups with matching
staging copies. It rejects contradictory retained content. If retention has
already deleted part of the canonical suffix, restore checks the retained
range and its accounting bound; it cannot reconstruct the deleted payloads.

The before-commit fault hook now runs after the relevant writes. The regression
therefore tests rollback of records and counters, rather than an empty
transaction. Lost-acknowledgement tests retry the same operation and verify
accounting after reopening.

## Verification

- [312 tests pass](round97-all-tests.log): 310 library/integration tests and two
  operational examples. The eight new SQLite tests cover aggregate row and byte
  limits independently, exact retry, publication, abort cleanup, legacy migration,
  controlled restore, corruption, rollback and acknowledgement loss.
- [All 19 verification tests](round97-verification.log) pass, including every
  registered headless callback and Home data validation. No native window was
  inspected.
- [Strict all-feature/all-target Clippy](round97-clippy.log) and
  [formatting](round97-format.log) pass.
- [Rust 1.85 all-feature/all-target check](round97-msrv.log) passes.
- Strict reduced-feature checks pass for [SQLite](round97-sqlite-only-clippy.log),
  [SQLite plus snapshots](round97-snapshot-clippy.log), and
  [SQLite plus source journal](round97-journal-clippy.log). Unused replication
  helpers and imports are now gated by their actual feature requirements.
- Separate executable runs preserve [16 local records](round97-demo.log) and
  [four replica records](round97-replica-demo.log) through their walkthroughs.
  These are correctness observations, not timed performance samples.

[Source hashes](round97-source-hashes.json) identify the checked source.
Home round 97 adds no experiments and makes no speed or memory claim.

## Remaining work

The full mixed resource workload and numerical release budgets remain open.
This change has no measured staging latency or memory comparison. Bootstrap
admission still counts active uploads. Publication still measures replaced
history and published snapshot totals. Those remaining scans need separate
analysis before changing their accounting. Current native rendering also
remains unverified.
