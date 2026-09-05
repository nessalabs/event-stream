# 0008. Replicate committed history without making local writes wait for the network

- **Date:** 2026-09-04
- **Status:** accepted; implementation in progress
- **Prerequisite:** [0007](../007-source-journal-and-parser-checkpoints/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

Local applications should continue committing during network loss. Replication needs its own progress and failure rules; a remote storage adapter alone does not provide synchronization.

## Decision

Add an optional asynchronous replica consumer for committed records and required snapshot metadata. Preserve origin stream/incarnation/cursor identity. Keep replica acknowledgements separate from local commit receipts.

Start with one authoritative origin per stream. The destination deduplicates by origin identity and verifies matching content. Persist replication progress only after destination commit acknowledgement. Reconnect resumes from that saved progress. Replicated data never automatically executes its recorded external actions.

Bound network batches, in-flight bytes, retry work, and retained backlog. Retention must honor required replica progress or explicitly detach an overdue replica and require a new snapshot/bootstrap. Local writes can continue offline only while local capacity remains; report exhaustion rather than silently deleting required data.

The [implementation contract](contracts.md) defines the identity, receipt,
bootstrap, retention and bounded transport boundaries. The optional typed API,
Memory origin and destination stores, bounded snapshot-plus-suffix bootstrap,
and owned application driver now exist. SQLite bootstrap transfer, completed
receipt retry after both stores reopen, and lost publication replies have focused
integration coverage. Bounded abort cleanup and published snapshot replacement
also have real SQLite tests. Controlled replication import, broader crash tests,
and resource qualification remain required before this phase is complete.
See the [completion audit](../001-event-stream/completion-audit.md) for the exact
checks and their limits.

## Ordered work

1. Define batch records, durable remote receipts, origin identity, ordering, authentication hooks, and transport responsibilities.
2. Implement duplicate/conflict handling and restartable bootstrap from a compatible snapshot plus suffix.
3. Connect replica progress to retention without letting an unavailable destination pin unlimited storage.
4. Test long disconnection, lost acknowledgements, partial batches, destination rollback, incarnation reset, and mismatched content.
5. Measure local append impact, backlog drain rate, network bytes, CPU, and peak memory under bounded concurrency.

## Completion evidence

- Local commit success does not depend on remote availability within configured local capacity.
- Every acknowledged replica prefix matches committed origin bytes and order.
- Reconnect and restart do not duplicate destination records or skip an unacknowledged gap.
- Retention cannot silently invalidate a required replica; rebootstrap is explicit.
- Replica throughput and resource costs meet published budgets without harming foreground append limits.

## Consequences

Single-origin replication keeps conflict handling simple. Multiple independent writers, consensus, and automatic failover need a separate decision. Applications still own credentials, endpoints, and access policy.
