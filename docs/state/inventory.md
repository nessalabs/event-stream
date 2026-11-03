---
id: "inventory"
title: "Find every feature owner"
kind: "guide"
status: "reference"
summary: "Implemented event-history families and application-owned boundaries are mapped explicitly."
parent: "events"
sources: ["src/lib.rs", "src/application/mod.rs", "Cargo.toml", "verification/src/catalog.rs"]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
---

# Find the owning contract

| Family | Owner | Atlas and scope |
| --- | --- | --- |
| Stream/event/schema/cursor values and token codec | src/domain/model.rs, cursor_codec.rs | [Replay](replay.md); encoding is not authentication |
| Atomic append, deduplication, admission and scheduling | application/runtime.rs, ports.rs; memory.rs/sqlite.rs | [Append](append.md); narrow identity model |
| Bounded range pages and replay-to-live subscriptions | application/runtime.rs | [Replay](replay.md); range model, no wakeup proof |
| Noncreating lookup, reset/delete and retired cleanup | domain/lifecycle.rs, adapters | [Lifecycle](lifecycle.md); identity model |
| Snapshot staging, verification, publication and finite recovery | application/snapshot.rs, adapter snapshot methods | [Snapshots](snapshots.md); publication model |
| Replay floor, retry generations and cleanup | application/retention.rs, adapter retention files | [Retention](retention.md); active-protection model |
| Incremental decoder/mapper and finite ingestion drives | ingestion/mod.rs | [Journal](journal.md); transient versus captured input |
| Source capture, parser checkpoints, sealing and finish | ingestion/journal.rs, source_journal modules | [Journal](journal.md); narrow checkpoint bounds model |
| Origin/destination batches, backlog, acknowledgement, bootstrap | application/replication.rs, adapter replication files | [Replication](replication.md); acknowledgement model |
| Controlled backup restore and mapping | application/restore.rs, sqlite_restore.rs | [Restore](restore.md); actual SQLite tests |
| Shutdown, outstanding ownership and quotas | application/runtime.rs, config.rs | [Shutdown](shutdown.md); no liveness proof |
| Memory and SQLite storage, grouped store append port | infrastructure, application/append_batch.rs | [Storage](storage.md); runtime batching remains deferred |
| Native scenario console, evidence storage and Home | verification/src and verification/evidence | [Console](console.md); no new performance sample |
| Executable examples, crash/resource harnesses, CI | examples, tests, scripts, .github | [Evidence](verification.md); execution versus configured checks |

Domain depends on standard library types. Application owns use cases and ports;
infrastructure owns SQL, memory layout, filesystem ownership and IDs. Optional
ingestion maps bytes through EventSink and cannot allocate committed cursors.
Applications own source I/O, schema meaning, authentication, transport and external
effects. New models do not widen that boundary or revive deferred optimizations.

The existing ADR roadmap and stability handoff remain authoritative requirements
and historical evidence. Source-reviewed behavior is not a claim every phase,
resource profile or production deployment has been qualified.
