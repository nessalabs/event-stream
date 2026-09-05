# Event stream

A standalone Rust library for ordered immutable events, exact append retries, bounded replay, and replay-to-live subscriptions. Applications define their event schemas and payload bytes. Storage is injected through `EventStore`.

**Status: ADR 0001 implementation is in progress.** The memory runtime, optional official SQLite adapter, cursor tokens, and decoder path exist. Release gates and remaining findings are tracked in the [completion audit](docs/adr/001-event-stream/completion-audit.md). The package is not published, and performance qualification is not complete.

## Try the implementation

```sh
cargo test --locked --all-features --all-targets
cargo run --locked --example custom_store
cargo run --locked --example projection_reconnect
cargo run --locked --features codec --example custom_decoder
cargo run --locked --features snapshots --example snapshot_recovery
```

The [operations guide](docs/operations.md) includes runnable SQLite walkthroughs
for full capacity, closed backup, controlled restore, and replica catch-up after
restart. Each walkthrough checks exact history and retains its databases for
inspection.

The crate declares Rust 1.85. No optional features are enabled by default. Enable `sqlite` for the bundled official SQLite engine, `codec` for incremental decoding, and `snapshots` for snapshot publication and protected recovery. Memory-only consumers do not build SQLite or GPUI.

`test-support` enables restore stage observers and controlled pauses used by
the crash-test harness. Ordinary builds omit those hooks. Run the restore crash
checks with `cargo test --locked --features sqlite,test-support --test sqlite_restore`.

The runtime is generic over its store. Import adapters from `event_stream::infrastructure`; use the application ports exported by `event_stream` for append, reads, subscriptions, and shutdown. The examples show concrete construction with finite limits.

SQLite currently declares process-restart persistence on the locally tested macOS configuration. This is not a power-loss guarantee. See [SQLite evidence](docs/adr/001-event-stream/sqlite-evidence.md) for transactions, ownership, engine settings, filesystem assumptions, and failure-test limitations. Linux CI is configured but has not yet been verified remotely.

## Architecture

- `src/domain/`: stream identities, immutable events, records, and cursor value objects; standard library only.
- `src/application/`: use cases, resource policy, runtime lifecycle, and injected ports.
- `src/infrastructure/`: memory and SQLite adapters, transactions, and OS ownership.
- `src/ingestion/`: optional framing and decoding through `EventSink`.
- `verification/`: a separate GPUI consumer that displays executable scenario evidence.

The [implementation contract](docs/adr/001-event-stream/implementation.md) explains these DDD boundaries. [AGENTS.md](AGENTS.md) records the coding, testing, resource, and writing rules.

## Verification workspace

Run `just` to list all available commands. `just check-build` checks every library target with all features and every verification-app target. It does not execute tests or open a window.

Run `just verify` to open the native workstream sidebar and evidence panel. `just verify-list` lists registered scenarios; `just verify-scenario roadmap-audit` runs a harness check without opening the window. Planned production cases remain marked unimplemented until their callbacks are wired to the actual library. See [verification documentation](verification/README.md).

## Design and roadmap

New to event streams? Start with [the context guide](docs/adr/001-event-stream/context.md) for sources, cursors, reconnects, and failure scenarios.

[ADR 0001](docs/adr/001-event-stream/adr.md), the [HLD](docs/adr/001-event-stream/hld.md), and the [LLD](docs/adr/001-event-stream/lld.md) define the architecture and acceptance requirements. The [performance design](docs/adr/001-event-stream/performance.md) covers allocations, scheduling, schemas, and operating-system I/O. [Release checks](docs/adr/001-event-stream/release-checks.md) distinguish configured checks from verified evidence.

The [ordered roadmap](docs/adr/README.md) continues beyond the first durable foundation into explicit lifecycle, snapshots, retention, captured-input recovery, and replication. Application source I/O, authorization, event meaning, and external effects remain application responsibilities.

The implementation goal includes ADRs 0005–0010. Their contracts, code, examples
and resource/failure evidence remain required work; writing the roadmap alone
does not complete those phases.

The proposal originated in `nessa-agent`; the important context is documented here. This repository owns its implementation and sequential ADR numbers. No external conversation link is required to understand the design.
