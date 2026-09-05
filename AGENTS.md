# Repository guidance

## Design and technical writing

Write for an engineer who understands distributed systems and software architecture but does not want dense academic or systems jargon.

For every design decision:

- Say the simple idea first.
- Use short, direct sentences.
- Prefer concrete language over formal terminology.
- Explain why the decision matters before giving implementation details.
- Define necessary jargon immediately in plain English. For example, admission control means deciding whether there is capacity to accept more work. A high-water mark is the latest committed cursor captured at a particular moment.
- Do not compress several different ideas into one sentence. Give each idea enough space to be understood on the first read.
- Keep the technical precision. Simplify the language, not the design. Preserve guarantees, limits, error cases, ownership rules, and algorithm details.
- Keep useful diagrams, tables, code, and pseudocode. Explain the decision around them in plain language.
- When discussing data or operations, add a small JSON example, schema-like shape, pseudocode, or ASCII diagram where it makes the behavior easier to understand. Good places include record structure, before/after state, retries, and replay boundaries. Do not add one to every paragraph.
- Label illustrative schemas and pseudocode as examples when they are not finalized APIs. Keep examples consistent with the design, including byte encoding, cursor precision, transaction boundaries, and failure behavior.
- Explain position and boundary concepts with small numbered examples. Show what is included, what is excluded, and what happens at an empty range or invalid boundary. Distinguish a cursor position from the record at that position, and label future-only behavior explicitly.
- Describe intended product behavior before stating release scope. Separate long-term rules, planned later capabilities, and open exploration from what v1 implements. Do not frame the product only as a list of v1 exclusions or turn an undecided extension into a delivery promise.
- Distinguish library responsibilities, application responsibilities, and deferred features. Do not blur a planned implementation with a tested guarantee.

Prefer:

> A large replay should not block new writes.

Over:

> Reads and writes need fair bounded scheduling so heavy replay cannot starve commits.

Prefer:

> SQLite only allows one writer at a time, so writes may be serialized even when callers submit them concurrently.

Over:

> A single-writer adapter may serialize physical commits across all streams.

A reader should understand each paragraph without translating systems terminology mentally. Review design-document edits against this standard before finishing.

## Resource use and implementation decisions

- Treat CPU, memory, disk I/O, and wakeups as scarce resources. Explain the work a design adds before choosing its implementation.
- Start with simple data structures, clear ownership, explicit state transitions, and finite limits. Prefer removing unnecessary work over adding clever machinery.
- Account for allocated capacity and retained buffers, waiting callers, in-flight work, caches, and background tasks. Queue length alone is not a memory budget.
- Measure the complete path from ingestion through storage and delivery. Investigate relevant allocator, scheduling, system-call, filesystem, and disk-flush costs when they explain a bottleneck.
- Preserve durability, ordering, replay, and retry guarantees while optimizing. Never claim improvement by weakening flush settings, skipping work, or hiding rejected requests.
- Require repeatable evidence before adding custom allocators, lock-free structures, unsafe code, direct I/O, or additional scheduling layers. Keep the simpler solution if the gain is inconclusive.
- Prefer a substantial reduction in retained memory over a tiny latency improvement. Report both costs under the same workload; do not optimize a single throughput number. A small latency increase, including a few milliseconds in a large population stress test, is acceptable when it buys a substantial memory reduction. Verify the tradeoff on that actual population; preserve bounded queues, successful outcomes and recovery. Do not use this preference to excuse unbounded latency or classify an untested 100,000-agent profile as supported.
- Test scale at 1,000, 10,000, and 100,000 logical agents. Distinguish stored streams, idle subscriptions, active producers, concurrent outstanding requests, and OS threads. Report unsupported profiles honestly. Do not equate creating stream names with serving that many concurrent producers.
- Measure memory growth, idle CPU, cleanup delay, overload behavior, and recovery after a burst. Keep queues and workers bounded as agent count rises. A linear memory curve alone does not prove acceptable latency or capacity.
- Follow the ordered ADR roadmap. Finish each phase's correctness and resource gates before declaring it complete. Document the disposition of deferred work rather than leaving unnamed future tasks.

## Implementation structure and verification

- Keep UI, scenario execution, production logic, and evidence storage separate. Inject small interfaces at real ownership or I/O boundaries; do not introduce a trait for every helper.
- Scenarios must call the actual implementation under review. Reuse the same scenario through the native verification console and headless tests. Never copy production algorithms into a demo that can pass independently.
- Label harness checks, reference models, mocks, fault injection, and production checks distinctly. Unimplemented targets are blocked, not passing examples.
- Use deterministic/property tests for invariants, scenario tests for operation sequences and failure schedules, shared adapter contracts for store behavior, and real isolated SQLite databases for persistence end-to-end tests.
- Inject clocks, source input, stores, or failure hooks only where needed to control tests. Use mocks for focused failures; do not use them as evidence of database durability.
- Record fixtures, seeds/reproduction schedules, expected and observed results, implementation identity, and actual measurements. Missing CPU/memory measurements remain unavailable, never inferred.
- Keep tests reproducible and resource-bounded. UI rendering must remain outside timed benchmark regions. Persist only bounded evidence and expose save/load failures.
- Keep the verification Home page backed by `verification/evidence/home.json`. Update its local experiment-round history after each work turn with changes, source identity, and available evidence. A round is a local work iteration, not an invented conversation turn identifier.
- Mark rounds without fresh measurements as not measured. Never reuse an old sample as a new experiment or infer an improvement from a code change. Charts compare identical workload and measurement settings across rounds, show regressions as well as gains, and leave incompatible or missing measurements unconnected.

## Domain-driven design

- Keep one explicit bounded context for generic event history. Terminal semantics, provider events, network delivery, and application projections belong outside it.
- `src/domain/` owns stream identity, cursor and event value objects, and pure domain invariants. It must not import application, infrastructure, Tokio, SQLite, UI, or codec implementations.
- `src/application/` owns append/replay/subscription use cases, lifecycle coordination, resource policy, and ports. Define contracts before parallel implementation. Depend on domain types and injected ports, never concrete storage adapters.
- `src/infrastructure/` implements storage ports, database transactions, OS ownership, and identity generation. Keep memory and SQLite adapters separate. SQLite SQL and driver errors must not leak into domain objects.
- `src/ingestion/` is an optional input boundary. Decode bytes and submit application events through `EventSink`; do not give a decoder database access or cursor allocation authority.
- Keep the GPUI verification project as an external consumer. Compose concrete dependencies at entry points and exercise the same application use cases as other consumers.
- Model stream creation and append as atomic changes to one stream's history. Preserve invariants in constructors and store contracts, with shared contract tests. Use the repository's stream/cursor/event vocabulary consistently.
- Do not add generic repositories, aggregate base classes, or a trait for every value object. Every abstraction must protect a real domain or I/O boundary.

## Current delivery priority

- The user has asked to conserve remaining usage and prioritize stability and fault tolerance. Freeze new feature and performance expansion unless needed to fix a demonstrated correctness issue.
- Follow `docs/stability-handoff.md`: keep the tested individual-append runtime, obtain a reproducible checkpoint and CI evidence, then run a bounded declared stability profile. Do not resume the parked runtime batching prototype or broad benchmark matrix without renewed direction.
- Preserve honest scope: existing sparse population results do not qualify 100k simultaneously busy agents, and process recovery tests do not qualify device power loss.
