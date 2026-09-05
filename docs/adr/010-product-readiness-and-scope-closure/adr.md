# 0010. Close the roadmap with evidence and explicit decisions

- **Date:** 2026-09-04
- **Status:** in progress; operational examples and recovery checks exist, release qualification is incomplete
- **Prerequisite:** [0009](../009-optional-domain-adapters/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

A growing list of deferred features can hide an unfinished product. Completion needs a clear testable outcome, while possible distributed features must not become mandatory merely because they were mentioned.

## Decision

Close each prior ADR with evidence before claiming the planned product is complete. Validate a complete path from bounded source input through durable append, replay/reconnect, lifecycle, snapshot recovery, retention, parser recovery, and single-origin replication.

Give every open exploration an explicit decision: selected for a new scoped ADR, or not required with a reason and a revisit trigger. Do not implement cross-stream transactions, public batches, consumer groups, distributed writers, consensus, or automatic failover without a demonstrated use case.

Keep the first release milestone separate from the full roadmap milestone. Durable SQLite is required for the first milestone. Later history and recovery features are required for the planned follow-on milestone. Domain integrations are evaluated under ADR 0009; unselected engines/providers are not silently required.

## Ordered work

1. Audit the ADR index against evidence, public API documentation, compatibility fixtures, and supported platforms.
2. Run mixed workloads with foreground appends, replay, snapshots, cleanup, and replica backlog. Inject supported failures while resource limits remain enabled.
3. Re-measure idle CPU, active memory, allocations, copied bytes, database growth, flushes, and p99 latency. Explain changes from the foundation baseline.
4. Publish operational examples for local-only, ephemeral, and replicated use. Exercise backup/restore and full-capacity behavior.
5. Record every remaining open question with owner/work item, decision status, and completion or exclusion criteria.

## Completion evidence

- No required prior phase is marked complete without linked tests and measurements.
- Every supported configuration has explicit guarantees, finite defaults, recovery instructions, and compatibility policy.
- End-to-end examples pass after restart and under overload without silent data loss or state drift.
- Performance budgets hold for the complete supported workload, not only isolated append loops.
- Remaining explorations are explicitly excluded or become new ordered ADRs. The roadmap has no unnamed deferred task.

## Consequences

This does not claim universal exactly-once effects or distributed availability. Completion means the selected product vision is implemented, measured, documented, and reviewable. Writing these ADRs alone does not complete any implementation phase.

## Executable recovery checkpoint

The console's **End-to-end recovery** action now uses the same
[shared fixture](../../../verification/fixtures/journal_replication.rs) as the
[SQLite process-kill integration test](../../../tests/sqlite_journal_replication.rs).
It calls production APIs throughout. It does not reimplement storage or parsing.

The console closes and reopens SQLite after the first output commits, before a
parser checkpoint exists. Recovery reaches byte 4, item 2 and output cursor 2.
Snapshot-plus-suffix transfer then survives source history cleanup and reopening
both databases. A late record catches up to replica cursor 3 with zero backlog.
The [saved round 87 callback](evidence/round87-full-recovery.json) contains its
inputs, five observations, outputs and implementation fingerprint.

The separate integration test runs the shared chain after three real process
kills: capture commit, output commit and checkpoint commit.
[Those schedules pass](evidence/round87-shared.log), as do
[all 19 verification tests](evidence/round87-verification.log).
The console action itself is close/reopen evidence, not process-kill evidence.
Neither run qualifies power loss, the full mixed resource workload, or the
remaining release budgets. Native rendering was not inspected this round.

Round 92 extends that shared fixture through durable EOF. It seals captured input
at byte 4 and calls `finish_captured`. After journal cleanup, source retention,
replication and reopening SQLite, it checks that the seal and finished parser
state remain intact. The [fresh console report](evidence/round92-full-recovery.json)
contains six observations, including this state. [Three process-kill schedules](evidence/round92-mixed.log)
and [all 19 verification tests](evidence/round92-verification.log) pass. This
closes the callback's missing EOF operation; native rendering and resource
budgets still require independent evidence.

Round 97 adds [operational walkthroughs](../../operations.md) for temporary
history, durable local backup/restore, and two-store replication. Both durable
examples exercise full-capacity behavior and exact history after restart.
The same round fixes aggregate SQLite staging quotas and validates migration,
cleanup, rollback and retry behavior. [The report](evidence/round97-report.md)
links 312 passing tests and current compatibility checks. These results do not
close the mixed resource workload, release budgets, or native rendering gates.

Round 98 closes a composition gap before the mixed workload: the origin runtime
now owns scoped replication operations alongside append, ingestion and replay.
The shared full-recovery fixture uses this boundary instead of opening an
independent origin adapter for replication. [The runtime integration report](../008-local-first-replication/evidence/round98-report.md)
links cancellation, shutdown, admission and restart checks. Its added task and
buffer costs remain unmeasured.

Round 100 adds a shared mixed-workload callback and twelve fresh-process
control/mixed diagnostics. Every run accepts all 512 offers and verifies exact
SQLite history; mixed runs include parsing, snapshot transfer, cleanup and a
lost committed reply. [The report](../002-resource-budgets-and-performance-evidence/evidence/round100-report.md)
records CPU, memory and latency with source identity. Control latency varies
substantially. These short runs do not close sustained-scale or release budgets.
