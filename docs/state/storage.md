---
id: "storage"
title: "Choose memory or SQLite"
kind: "guide"
status: "reference"
summary: "Store capabilities and ownership determine recovery; configured flushes alone are not power-loss evidence."
parent: "events"
sources: ["src/infrastructure/memory.rs", "src/infrastructure/sqlite.rs", "src/application/config.rs", "docs/operations.md"]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
---

# Choose how long history must survive

MemoryStore owns bounded temporary history. Dropping it loses that history; a
saved external cursor cannot recover it. SqliteStore owns a database exclusively
and advertises ProcessRestart persistence. The local test evidence does not
qualify device power loss or every filesystem. Runtime open validates required
capabilities and finite resource limits against the injected store.

SQLite serializes database work behind its adapter and commits event identity,
bytes and offset together. Exclusive ownership lasts until accepted I/O has
finished, including abandoned caller futures. StoreInUse is a refusal, not an
invitation to remove a lock. Preserve the original request across uncertain
commit outcomes and reconcile after safe reopen.

The SQLite page quota limits main-database pages; it does not reserve free disk
space or cap temporary rollback journals. A full queue, full configured database
and physical full disk are different failures. Keep ingestion bounded when storage
cannot accept more. The [local recovery walkthrough](../operations.md) exercises
capacity, exact retry, closed backup and controlled restore using real files.

Default memory quotas and sparse historical population experiments do not promise
100,000 simultaneously busy producers. The stability handoff freezes new batching
and broad optimization work. Current all-feature tests exercise both adapters;
no new throughput comparison, supported workload or deployment qualification is
claimed here. Optional grouped append remains a tested storage port, with per-item
results validated after commit, not active runtime batch scheduling.
