# 0001. Reusable event streams live in a standalone Rust crate

- **Date:** 2026-09-04
- **Decision status:** accepted for implementation
- **Execution status:** in progress; [completion audit](completion-audit.md)
- **Background:** [Event streams and cursors explained](context.md)
- **High-level design:** [Architecture, scope, and guarantees](hld.md)
- **Low-level design:** [Implementation contracts and algorithms](lld.md)
- **Implementation sequence:** [Ordered ADR roadmap](../README.md)
- **Resource design:** [CPU, memory, schema, and operating-system costs](performance.md)

## Context

Terminals, agent output, workflows, and other event sources need the same
incremental ingestion, ordered delivery, persistence, and reconnect behavior.
That infrastructure should be independently usable without application-specific concepts, conversations,
provider-specific payloads, or a particular network transport.

## Decision

Build a **standalone Rust event stream crate** for independent consumers.
The published package name is not selected by this record.

The package owns append, per-stream committed ordering, idempotent event append,
replay after an exclusive cursor, ordered replay-to-live subscription, bounds,
and explicit slow-consumer/history errors. It exposes an `EventStore` trait for
user-supplied storage implementations injected at construction. Consumers read
through the stream API, not directly from a database.

The core stores versioned opaque payload bytes. Optional framing/codec modules
and a custom incremental decoder interface let consumers process chunked input
without putting terminal, agent, or workflow semantics in the core. A caller may
also append already-parsed events. Parsing does not assign committed cursors.
Provider-specific normalizers and terminal emulators are separate consumers or
adapters, not mandatory dependencies.

The runtime owns ordering policy; the store must enforce atomic append,
idempotency lookup, and cursor allocation in one commit. Multiple producers
share one runtime; v1 requires exclusive runtime ownership of a store. The store
adapter must acquire that ownership or reject opening it. No multi-process
writers, distributed consensus, consumer groups, or network server in v1.

Provide an in-memory adapter for tests and ephemeral consumers, and a separately
selectable local durable adapter. Plan `SqliteStore`, backed by
the official SQLite engine through a Rust binding, as the first durable implementation. Release requires
validation of ownership, transactions, recovery, and performance; choosing the
implementation does not establish a durability guarantee. Every
adapter runs the same contract suite and declares its persistence guarantees;
in-memory commits make no process-restart promise.

Application storage locations, transport adaptation, credentials, and
multi-device conflict policy belong to consumers. V1 has no synchronization
service. The follow-on [replication ADR](../008-local-first-replication/adr.md) defines
an optional single-origin replication path; it does not add distributed writers
or move application authorization into the core.

## Consequences

External users can inject their own store and decoder without changing the
crate. Consumers adapt subscriptions to their own transports and access records
through the stream API.

Correctness requires transactional adapter contracts, bounded queues, replay/live
race tests, and restart tests. The crate guarantees delivery of committed
records, not recovery of unobserved provider output or exactly-once tool effects.
Snapshots, compaction, replication, and crash-resumable parser checkpoints are
follow-on work in the [ordered roadmap](../README.md), not prerequisites for the
first durable implementation. Every phase must meet explicit resource budgets
and preserve the same correctness rules.
