# Stability checkpoint and next priorities

Updated in round 111, following the request to conserve usage and prioritize
stability and fault tolerance. Freeze feature expansion and optimization. Keep
the tested individual-append runtime path while closing release evidence gaps.
This is a development checkpoint, not a claim that every roadmap gate is done.

## What is good enough to keep

| Area | Evidence available | Decision now |
| --- | --- | --- |
| Ordered history, deduplication, bounded replay and runtime ownership | Shared adapter/runtime contracts, cancellation/shutdown tests and exact history verification | Keep the current implementation and limits. |
| SQLite durability | Real reopen, process-kill, write/sync/full-disk/rollback faults and retry tests | Keep bundled SQLite and FULL synchronization. Evidence covers process recovery; it does not qualify device power loss. |
| Lifecycle, snapshots, retention, journal/checkpoints and replication | Recovery fixtures, corruption checks, mixed-feature scenarios and operational examples | Keep implemented features. Fix reproducible correctness issues before adding more features. |
| SQLite grouped append port | Per-item conflicts, group rollback, lost replies, VFS faults and real process-kill/retry checks | Retain the tested storage port. Do not integrate it into runtime scheduling yet. |
| Verification workspace | Shared production callbacks, persisted Home evidence and passing headless tests | Keep it as a review tool. Current native window responsiveness/rendering is not verified. |

Recent evidence: [group commit](adr/002-resource-budgets-and-performance-evidence/evidence/round109-report.md),
[group process kills](adr/002-resource-budgets-and-performance-evidence/evidence/round110-report.md),
and [mixed workloads](adr/002-resource-budgets-and-performance-evidence/evidence/round100-report.md).
These reports state their tested scope. A passing count is not a universal
reliability guarantee. No known failing correctness test is being deliberately
accepted; the final checkpoint run is recorded separately below.

## What actually blocks a release claim

1. **A reproducible source checkpoint and passing CI.** Round 115 establishes a local source/evidence checkpoint. Resolve its identity
   with `git log`; no remote is configured yet. A workflow exists
   for Linux/macOS and Rust 1.85, but this review has not observed a passing remote
   run. Commit a reviewed checkpoint and run that matrix before calling this a
   reproducible release. Historical source archives are useful evidence, but do
   not replace release versioning.
2. **A declared supported workload and sustained stability evidence.** We have
   short correctness/resource runs and sparse population measurements. We have
   not qualified long-running mixed traffic, post-burst drain, or settled memory
   at the requested scale. Run one bounded representative soak next, with clear
   pass/fail limits chosen before execution. Count every offer and error. Verify
   exact accepted history, recovery and resolved shutdown. Separate necessary
   history growth from memory that remains after its owner is released.
3. **Operational readiness for the selected deployment.** Exercise the documented
   backup/restore procedure on that environment and state the durability profile,
   quotas, overload handling and retry responsibilities. Existing examples prove
   their local scenarios; they do not establish every filesystem/device profile.

The native verification UI also needs a current visual/responsiveness check to
finish the review-console deliverable. That is separate from the storage engine's
correctness, and should not hold up a targeted headless library evaluation.

## Performance limits we must not hide

The 100k-stream diagnostic used 800 aggregate offers/s, not 100k simultaneously
busy agents. At 10k streams and 4,000 aggregate offers/s, the SQLite runs filled
the 256-task generator and rejected 19–29% of offers before library submission.
Every submitted operation succeeded, but some streams received no committed
record. That is a capacity limitation for that workload, not observed data loss.
See [round 106](adr/002-resource-budgets-and-performance-evidence/evidence/round106-report.md).

Use sparse workloads as development targets only. Do not advertise arbitrary
100k-agent capacity, a release SLO, or the fastest streaming library. The observed
reduction from 24 to 3 SQLite sync callbacks for eight grouped inserts does not
close the runtime throughput gap.

## Explicitly deferred

- Runtime batch collection and the broad optimization/benchmark matrix.
- Alternative SQLite engines, custom allocators and lower-level scheduling work.
- Terminal/provider adapters and other optional integrations.
- Further UI polish beyond checking that the current console works.

The uncompiled runtime batch prototype was removed from active source in round
111. Runtime/config were restored from their hash-verified pre-experiment source;
the batch integration tests were parked with the prototype. A temporary copy is
at `/tmp/event-stream-round111-paused-runtime-batching`; it is unreviewed and may
be cleaned by the OS. Do not restore it blindly. The implemented SQLite group
port, fault fixes and completed recovery tests remain in the project.

## How to use the remaining work budget

Run the checkpoint regression once. Fix any actual failure it finds. Then make
a reproducible source/CI checkpoint and run one declared stability profile.
Spend further implementation effort only on failures or evidence gaps from those
steps. Avoid another cycle of throughput tuning or expanding the feature scope.

Checkpoint result: the restored source passes **352 all-feature/all-target
tests** in [round 111](adr/002-resource-budgets-and-performance-evidence/evidence/round111-stability.log).
This count includes test helper entry points and does not imply that many
independent fault schedules. No performance benchmarks were run.

All 19 verification tests and formatting checks pass after the Home update.

A first [bounded stability screen](adr/002-resource-budgets-and-performance-evidence/evidence/round112-report.md)
now passes: 10,000 SQLite streams, 100,000 offers over ten cycles at 800/s, exact
replay and resolved shutdown. Peak RSS is 19.94 MiB and receipt p99 is 3.04 ms.
This is one roughly 125-second sparse-load run, not the remaining long-running
mixed-load or settled-memory qualification. Reproducible Git/CI and deployment
recovery checks remain release priorities.

Round 113 closes two CI configuration gaps: shared verification fixtures now
trigger the core matrix, and a separate macOS workflow checks console compilation
and headless scenario/Home tests. Source changes, shared fixtures, Home data and
ADR decision files trigger the relevant checks; historical benchmark archives
and logs do not. This is configuration plus local command evidence. A reviewed
Git checkpoint and an observed passing remote CI run still remain required.

[Round 114 operational checks](adr/010-product-readiness-and-scope-closure/evidence/round114-report.md)
pass using the documented CLI commands and feature selections. Local SQLite
backup/restore and replica backlog/restart/retry work on this machine. Both
durable walkthroughs are now part of the CI configuration. Remote CI and
qualification on the intended deployment filesystem/device remain outstanding.

Round 115 creates a local stability checkpoint, including retained evidence
archives. Build outputs remain excluded by .gitignore. No code changed during
checkpoint preparation. The user will push to GitHub; publishing is outside this checkpoint task.
No push or remote CI execution is claimed. A local
checkpoint records this state, not completion of every release gate.
