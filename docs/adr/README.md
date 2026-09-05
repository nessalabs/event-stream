# Event stream ADR roadmap

Build one completed, measured slice at a time. Each ADR states a decision, the work it requires, and the evidence needed to finish it. These documents define decisions and completion gates. ADR 0001 is being implemented through the bounded-baseline and SQLite phases; implementation progress does not establish release qualification.

The original architecture is [ADR 0001](001-event-stream/adr.md). Its supporting documents are the [context guide](001-event-stream/context.md), [HLD](001-event-stream/hld.md), [LLD](001-event-stream/lld.md), and [first-principles performance design](001-event-stream/performance.md).

Each decision lives in its own folder, starting with `001-event-stream/adr.md`, then `002-resource-budgets-and-performance-evidence/adr.md`, and so on. Document titles use four-digit ADR numbers (0001, 0002, …). Folder prefixes follow the existing three-digit convention (001, 002, …).

## Implementation sequence

| Order | ADR | Outcome required before moving on |
| --- | --- | --- |
| 1 | [0002 — Resource budgets and performance evidence](002-resource-budgets-and-performance-evidence/adr.md) | Working bounded core/memory/decoder baseline with repeatable contracts and CPU/memory evidence |
| 2 | [0003 — SQLite durability and storage layout](003-sqlite-durability-and-storage-layout/adr.md) | Durable first release: tested SQLite transactions, recovery, ownership, schema, and kernel I/O costs |
| 3 | [0004 — Lifecycle and safe restore](004-stream-lifecycle-and-safe-restore/adr.md) | Explicit delete/reset/restore with crash-safe identities and bounded cleanup |
| 4 | [0005 — Snapshots and consumer recovery](005-snapshots-and-consumer-recovery/adr.md) | Versioned application snapshots and correct snapshot-plus-replay recovery |
| 5 | [0006 — Retention, compaction, and retry horizons](006-retention-compaction-and-retry-horizons/adr.md) | Controlled storage growth with explicit replay and retry guarantees |
| 6 | [0007 — Source journal and parser checkpoints](007-source-journal-and-parser-checkpoints/adr.md) | Optional recovery of captured input through committed output |
| 7 | [0008 — Local-first replication](008-local-first-replication/adr.md) | Bounded, restartable single-origin replication that does not make local commits wait for the network |
| 8 | [0009 — Optional domain adapters](009-optional-domain-adapters/adr.md) | Reviewed terminal/agent integration boundaries and explicit candidate decisions; optional packages only when selected |
| 9 | [0010 — Product readiness and scope closure](010-product-readiness-and-scope-closure/adr.md) | End-to-end evidence and a decision for every remaining exploration |

This is the chosen execution order, not a claim that every phase technically requires every earlier feature. It limits work in progress. Performance checks begin in 0002 and run through every later phase; they are not deferred to a final optimization sprint.

## What counts as complete?

A document being written or accepted does not mean its implementation is complete. Track decision status (`proposed`, `accepted`, or `superseded`) separately from execution status (`not-started`, `in-progress`, `verified`, or `blocked`). Current execution remains `in-progress` for ADRs 0001–0008 and 0010.
ADR 0009 has documented proposals and extension examples; no companion has been
selected. Its implementation is conditional on that separate decision.

The implementation now includes SQLite lifecycle/restore, snapshots, retention,
durable parser EOF, and SQLite replication/bootstrap through the owning runtime.
The [round 99 checkpoint](002-resource-budgets-and-performance-evidence/evidence/round99-report.md)
records 322 passing library/integration/operational tests. The
[round 100 mixed workload](002-resource-budgets-and-performance-evidence/evidence/round100-report.md)
combines these APIs and reports twelve short resource diagnostics. Those checks
supersede the earlier claim that SQLite bootstrap or parser EOF had not been
implemented. They do not establish complete fault coverage or release capacity.

The remaining qualification includes sustained 1k/10k/100k logical-agent profiles,
explicit release budgets, memory after activity settles, and current native
rendering. Short bursts that reject most requests do not qualify that many
successful producers. Optional terminal and provider candidates remain separate
from these required core gates. The [completion audit](001-event-stream/completion-audit.md)
retains the historical evidence and its original limitations.

For each phase, publish:

- The chosen behavior and alternatives rejected, with reasons.
- Code and fixtures exercising normal, overload, cancellation, and crash paths.
- Correctness results and resource measurements for the exact revision/configuration.
- Numeric budgets, comparison results, and explanations for any approved regression.
- Remaining limitations, compatibility behavior, and the next eligible ADR.

If a phase fails its gates, repair it or explicitly revise the decision with evidence. Do not mark it complete and move its essential guarantee into an unnamed future task.

The existing [verification structure](../../verification/STRUCTURE.md) supplies a place for scenarios and evidence. Timed benchmarks need a headless path so rendering and UI event handling do not distort library measurements. The harness is implemented. A registered scenario invokes production APIs; a planned scenario still needs an executable check. Its status is not evidence that a whole ADR has passed.

## Full vision versus optional exploration

The selected vision includes a small reusable core, official SQLite durability, incremental decoding, explicit lifecycle, recoverable bounded history, optional captured-input recovery, and optional single-origin replication. These capabilities have ordered ADRs above.

Cross-stream ordering, multi-stream transactions, public atomic batches, consumer groups, distributed writers, consensus, and automatic failover remain explicit exploration decisions in ADR 0010. A finding that they are unnecessary is a valid outcome. Their absence must not leave the selected core behavior incomplete.

Application source I/O, authorization, domain event meanings, external effects, and product UI remain application responsibilities. Companion integrations can make some of that work reusable. No network server or terminal engine becomes a core dependency by implication.
