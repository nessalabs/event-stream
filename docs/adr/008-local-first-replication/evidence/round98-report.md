# Runtime-owned replication

The origin runtime can now append, ingest, replay and replicate the same store.
Previously, the replication driver required direct adapter access while the
runtime kept its adapter private. Separate examples did not prove that those
features could be composed through the public runtime.

The [integration contract](../runtime-integration.md) defines scoped identity,
attachment, status, detach, cleanup, batch transfer and snapshot bootstrap
operations. Each reserves an aggregate operation slot and byte charge before
starting owned work. Defaults are four operations and 8 MiB. Standalone driver
defaults are unchanged.

An accepted transfer survives caller cancellation. Its runtime task waits for
the nested production driver to finish before releasing capacity or allowing
shutdown to close the origin. The destination remains caller-owned. No raw
store accessor or second replication algorithm was added.

## Evidence

- [319 tests pass](round98-all-tests.log): 317 library/integration tests plus
  both operational examples. The seven new runtime tests cover real
  Memory/SQLite transfer and bootstrap, exact retries, reopen, held-transport
  cancellation, bounded admission, invalid configuration, shutdown rejection,
  and panic recovery.
- [Focused integration checks](round98-focused.log) also pass after the shared
  journal/replication fixture switched to runtime-owned replication. Its
  process-kill schedules still recover exact downstream history.
- The [fresh full-recovery console report](round98-full-recovery.json) passes
  six observations. Its callback now uses the same runtime boundary through
  snapshot bootstrap, retention cleanup, restart and later catch-up.
- [All 19 verification tests](round98-verification.log) pass, including Home
  data validation and every registered headless callback.
- [Strict all-feature/all-target Clippy](round98-clippy.log),
  [formatting](round98-format.log), and
  [Rust 1.85 all-feature/all-target check](round98-msrv.log) pass.

[Source hashes](round98-source-hashes.json) identify this checkpoint. Home round
98 adds no performance experiments. Callback execution time is not a workload
benchmark. No current native rendering was inspected.

## Remaining qualification

The mixed resource workload still needs implementation and measurements through
this boundary. The new scoped driver adds bounded task and request state; its
cost has not been measured. Standalone bootstrap metadata charging also needs
explicit identifier/retained-copy accounting before resource qualification.

The [group-commit evaluation](../../002-resource-budgets-and-performance-evidence/group-commit-evaluation.md)
records a separate source finding: one outstanding SQLite runtime append leaves
no ready adapter queue to group. A typed batch port is a candidate for controlled
comparison, not a selected optimization or measured improvement.
