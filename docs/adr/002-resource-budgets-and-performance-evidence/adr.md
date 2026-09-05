# 0002. Give every resource a budget and optimize measured costs

- **Date:** 2026-09-04
- **Decision status:** accepted for implementation
- **Execution status:** in progress; see [completion audit](../001-event-stream/completion-audit.md)
- **Prerequisite:** [0001](../001-event-stream/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

The library must stay cheap when idle, busy, replaying, and failing. A fast append benchmark is insufficient if it hides copied bytes, growing caches, long queues, or weak disk flushing. Performance decisions affect API ownership, schema layout, and worker scheduling before any optimization is written.

## Decision

Treat CPU, memory, disk I/O, and wakeups as explicit costs. Start with ordinary owned buffers, bounded queues, standard maps, short transactions, and one clear owner for each operation. Use the [first-principles design](../001-event-stream/performance.md) for every change to these paths.

Record what each allocation stores, who releases it, and its maximum lifetime. Measure queue wait separately from actual work. Trace storage calls through SQLite, the filesystem, and flush completion. Keep one understandable baseline before trying alternatives.

An optimization must name a measured bottleneck, preserve correctness and persistence settings, and improve a stated workload beyond measurement noise. Keep the simpler version when the difference is inconclusive. Do not add custom allocators, lock-free queues, direct I/O, kernel bypass, or unsafe memory tricks without a separate evidence-backed decision.

## Ordered work

1. Add headless contract/benchmark scenarios using deterministic fixtures. The existing verification UI may display results, but timed runs exclude UI rendering.
2. Build a cost report for memory append, retry, replay, parsing, and idle subscriptions. Reuse it for every subsequent ADR.
3. Record environment, input distribution, active limits, repeated baseline runs, and profiler availability. Set numeric regression budgets from that baseline before marking this step complete.
4. Add instrumentation for allocation count/bytes, retained capacity, queue depth, worker CPU, wakeups, and per-stage latency. Measure instrumentation overhead separately.
5. Add the SQLite and kernel measurements during ADR 0003. Until then, mark those dimensions `not-run`, never zero.

## Completion evidence

The [MiniSQLite read-path diagnostic](minisqlite-comparison.md) compares an
alternative engine with simple bundled-SQLite queries and the production path.
Its single-run results identify candidates for investigation. They do not close
the repeated-measurement or durability gates below.

- All resource-owning structures have finite limits and release paths covering cancellation and failure.
- The memory adapter and generic decoder pass the shared contract suite.
- A headless run produces machine-readable measurements and a readable explanation of the dominant cost.
- Repeated measurements define noise, workload-specific budgets, and an explicit pass/fail comparison rule. Missing required measurements do not count as success.
- Slow readers, producers, and idle handles cannot cause unbounded runtime memory or polling.

## Consequences

Instrumentation and budgeting add work early. They prevent speculative optimizations and make later changes comparable. This ADR is an ongoing rule: it does not delay durability until every possible optimization is explored.

## Current burst scale checkpoint

[Round 94](evidence/round94-report.md) measures one simultaneous request per
Tokio task at 1,000, 10,000 and 100,000 tasks on both stores, with three fresh
processes per cell. It records successful and rejected calls alongside memory
and runtime. It qualifies the exercised bounded-overload behavior, not sustained
successful service at those populations. A large per-task harness allocation is
identified for a controlled follow-up; no core improvement is inferred from it.

[Round 95](evidence/round95-report.md) follows with a compact per-request harness
result. Identical library source and request settings save115.2MB of ready-task
Rust allocations at100k tasks. The report distinguishes this measured caller
cost reduction from core capacity. All outcome and latency-sample counts remain
checked; sustained service and mixed workloads remain open.

The [bounded sustained-generator contract](sustained-generator.md) replaces
allocation of every future request with a fixed task cap and explicit generator
rejections. This is required before longer measurements: experiment duration
must not determine the number of waiting tasks. Harness and library resource
costs remain separate, and a generator-limited run cannot qualify runtime
capacity by hiding requests that never reached it.
