# 0003. Commit through official SQLite and validate the full storage path

- **Date:** 2026-09-04
- **Decision status:** accepted for implementation
- **Execution status:** in progress; see [completion audit](../001-event-stream/completion-audit.md)
- **Prerequisite:** [0002](../002-resource-budgets-and-performance-evidence/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

Durability belongs in the first usable release. We need to know which work is required to save an event and which schema choices add avoidable cost. SQLite provides the database engine; the adapter still owns correct transactions, configuration, and error handling.

## Decision

Build `SqliteStore` through a maintained Rust binding to the official SQLite engine. Keep it optional. Use one connection on a dedicated worker initially. Use prepared statements and bound values. Save record, event-ID lookup, and tail in one transaction.

Benchmark the schema candidates in the [performance design](../001-event-stream/performance.md#4-schema-and-index-costs) before freezing the file format. Preserve the full unsigned cursor range. Start with only the indexes needed for identity lookup and ordered replay.

Compare explicit journal/synchronization configurations under the same intended persistence promise. Choose one supported default and read back effective settings. No acknowledgement precedes commit success. An uncertain commit pauses affected work until its original event ID is resolved.

Hold runtime ownership across open, recovery, migrations, I/O, and close. Database write locking does not replace this lifetime rule. Do not replace SQLite with a new engine to chase an unproven advantage.

## Ordered work

1. Select and pin the binding, engine build, platform support, and packaging. Implement the section 22 contract in the [LLD](../001-event-stream/lld.md#22-sqlite-adapter-implementation-plan).
2. Compare ordinary rowid storage and composite primary-key storage using realistic payloads. Measure index bytes, query plans, pages touched, CPU, and full durable latency.
3. Test append, duplicate, conflict, paged replay, disk full, rollback failure, ownership rejection, dropped callers, and shutdown.
4. Capture the actual write and flush path on macOS and Linux where supported. Explain user CPU, kernel CPU, waiting time, and journal/checkpoint writes separately.
5. Publish restart and persistence evidence. State filesystem/device assumptions and the exact supported profile.

## Completion evidence

- Shared contracts pass against memory and SQLite with identical logical results.
- Restart recovers committed bytes, cursors, and retry identities. Unknown outcomes resolve without duplicate records.
- Two runtimes/processes cannot acquire the same store. Ownership is not released while an old worker can still write.
- Durable append and replay meet the budgets established in ADR 0002. Growing history does not cause full-journal startup allocation or unindexed replay.
- Required flush behavior is documented and verified at the adapter boundary. Process-kill evidence is not presented as a power-cut test.
- The first release includes working custom-store, decoder, and reconnect examples with documented finite defaults.

## Consequences

One writer limits peak write concurrency but keeps state clear. Additional readers, WAL tuning, and bounded internal group commit are measurements to evaluate, not prerequisites. Finish this step before calling the foundation durably usable.
