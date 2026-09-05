# Mixed workload correctness and bootstrap accounting

The new shared fixture runs scheduled foreground appends and bounded replay
alongside source parsing, snapshot publication, replica bootstrap, retention
cleanup, and restart verification. It injects one lost reply after the real
SQLite destination commits, then verifies the exact retry and final history.
The control uses the same foreground schedule without the maintenance work.

The generator holds at most 64 outstanding tasks. Every offer is counted as
accepted, rejected by the runtime, or rejected by the generator. Receipt latency
and scheduling delay are separate. The diagnostic example offers 512 events at
800 per second. It does not represent 100,000 active producers or a steady-state
release gate.

The shared fixture is used by the integration test and the new `mixed_resource`
example. It is not yet registered as a native console callback. The example
emits JSON with elapsed time, CPU, sampled process memory, Rust allocations,
outcomes, latency percentiles and the amount of observed overlap. Whole-scenario
measurements include setup, verification and reopening. Process memory includes
the harness, SQLite and profiler. Process-lifetime maximum RSS is labeled
separately from sampled RSS. No fresh resource samples were collected this round.

Bootstrap admission now charges separately allocated identifiers and bounded
request/result copies in addition to page buffers. Two tests check rejection
before transport I/O with maximum identifiers and successful exact retry with
sufficient capacity. This is conservative accounting, not measured heap usage.

## Remaining work

Collect repeated isolated control/mixed samples with source and binary identity.
Agree on release budgets before using those results as qualification. Register
the shared scenario in the native console. Complete sustained scale and memory
recovery measurements. Evaluate group commit separately; it is not implemented
or demonstrated to improve performance.

## Validation

- 322 library, integration and operational example tests pass.
- All 19 verification tests pass.
- Strict all-feature/all-target Clippy, formatting and Rust 1.85 checks pass.
- Logs and source hashes are stored beside this report with the `round99-` prefix.
- Current native rendering remains unverified.
