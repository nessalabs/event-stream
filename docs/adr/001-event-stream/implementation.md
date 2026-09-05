# ADR 0001 implementation contract

Execution status: in progress. This file tracks implementation boundaries; it does not replace the LLD acceptance criteria.

## Code boundaries

```text
external application / verification UI
                |
       application use cases
                |
       application storage port <--- infrastructure adapter
                |                       memory / SQLite / OS lock
          domain model

optional ingestion ---- EventSink ----> application use cases
```

The bounded context is generic event history. A stream is the unit of atomic history change. Append commits the event, retry identity, and next offset together. No transaction spans multiple streams. A committed event is immutable. The application runtime coordinates accepted work; infrastructure enforces atomic persistence.

| Location | Owns | Must not depend on |
| --- | --- | --- |
| `src/domain/` | Identity value objects, event bytes, record and cursor model | Application workflows, storage, executor, GPUI, decoding |
| `src/application/ports.rs` | Store, event sink, reader, runtime, subscription contracts | Concrete memory/SQLite adapters |
| `src/application/runtime.rs` | Admission, ordering, replay, subscriptions, shutdown | SQL or concrete storage implementations |
| `src/infrastructure/memory.rs` | Bounded ephemeral storage and ownership | UI or decoding |
| `src/infrastructure/sqlite.rs` | SQLite transactions, worker, locks, recovery | UI or decoding |
| `src/ingestion/` | Optional decoder and input-to-event driver | Concrete stores, committed cursor allocation |
| `verification/` | Compose scenarios and present evidence | Alternative implementations of production algorithms |

Ports use the same domain types across adapters. Infrastructure generates incarnation identities. Identifier constructors validate bounded canonical bytes; no Unicode normalization or case folding changes application identifiers. Payload allocations hold exactly their visible bytes. Runtime and adapter admission still validate configured limits.

Root exports are a public convenience facade. Internal modules import the layer they depend on. Domain code may use the Rust standard library only. Concrete dependencies are chosen at application entry points.

## Parallel work contract

The orchestrator owns domain types, application ports, dependencies, and requirement review. Runtime/memory and SQLite implementation proceed independently against the compiled `EventStore` port. Ingestion uses `EventSink` and can be implemented without knowing a concrete store. Changes to a shared contract are coordinated before dependents are updated.

Tests call these ports and compare observed results with the domain rules. A shared adapter suite runs against memory and SQLite. Runtime tests add controlled failure schedules. Database tests use real isolated files and process boundaries. Verification scenarios reuse the resulting application APIs.

## Completion audit still required

- Every LLD section 3 invariant, section 14 acceptance row, and section 21 release check has direct evidence.
- Full memory-backed application path and optional decoder are implemented, including cancellation and resource release.
- SQLite passes the shared contracts and real recovery, ownership, format, and storage-failure tests.
- Numeric budgets, repeated baseline measurements, CPU/memory accounting, schema comparisons, and available kernel I/O evidence satisfy ADRs 0002 and 0003.
- Custom-store, custom-decoder, and projection/reconnect examples compile and run externally.
- Package features, tested toolchain/platforms, persistence profile, recovery procedures, and CI checks are documented.
- Relevant verification UI cases invoke the production functions and show actual evidence.

ADRs 0004 onward retain their explicit later scope. They are not silently included in, or substituted for, the ADR 0001 durable foundation.
