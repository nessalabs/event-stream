# Event stream verification workspace

A native GPUI workspace for reviewing executable evidence. Workstream folders follow ADRs 0001–0010. Each nested item is a verification with its own fixture, expected outcome, implementation target, and latest result. There is no chat interface.

## Run

```sh
just verify
just verify-check
just verify-test
just verify-list
just verify-scenario roadmap-audit
```

The same binary can run without opening a window:

```sh
cargo run --manifest-path verification/Cargo.toml -- --list
cargo run --manifest-path verification/Cargo.toml -- --scenario catalog-integrity
```

Headless execution exits 0 for passed, 1 for failed execution/evidence save, and 2 for blocked or invalid requests. Runs print actual JSON evidence. The GUI executes on a background worker and allows one run at a time; navigation and theme switching remain available.

## What works now

There are 24 scenarios in 10 workstreams. Sixteen are runnable:

- **Roadmap & contract files:** reads the actual ADR files and checks numbering and required sections.
- **Scenario wiring audit:** validates the registry used by the GUI and CLI.
- **ADR 001 core:** runs concurrent ordering, retry identity, bounded admission, and replay beside writes through the public runtime and store APIs.
- **SQLite:** uses isolated real database files for reopen, exclusive ownership, and acknowledgement-loss recovery.
- **Lifecycle:** resets a real memory runtime, rejects stale cursors, and reclaims retired records through bounded cleanup turns.
- **Controlled restore:** imports a real SQLite backup, verifies fresh identities and exact history, retries publication, then appends and reopens. This scenario proves the successful path; crash and resource qualification have separate evidence.
- **Snapshot recovery:** publishes application-owned transcript and workflow state through the production Memory runtime, restores it under a finite lease, applies the protected suffix, and compares it with complete replay. SQLite restart and crash evidence remain separate gates.
- **Retention horizon:** reuses one event ID across the legacy and generated policies, moves the replay floor, expires the old retry generation, runs bounded cleanup, and reads the exact remaining suffix through the production Memory runtime. SQLite persistence and fault qualification remain separate gates.
- **Decoder boundaries:** runs the production newline decoder at every single split and with one-byte chunks.
- **Parser recovery:** captures raw bytes, constructs an output-before-checkpoint interruption, rotates the retry generation, and resumes through the bounded production journal ingestion service without a missing or duplicate output. SQLite crash durability remains a separate gate.

The other eight have fixtures, intended targets, and expected outcomes. Their Run buttons are disabled because the production code does not exist yet. They are not simulations of passing stream behavior.

The main panel displays per-assertion results, actual output, and wall time. CPU and memory are explicitly not sampled. Latest results load on startup and remain separate for each verification. A change to the runner, scenario adapters, exercised production sources, manifests, lockfiles, or fixture invalidates old evidence. Files are limited to 256 KiB each; only the latest report per known scenario is retained under `evidence/runs/` and ignored by Git. Copy JSON to share a complete report, or reveal the responsible source and ADR from the panel.

Evidence is a local review artifact, not a durable event store. Atomic replacement prevents partially written JSON from being presented as a completed report; no power-loss guarantee is claimed for evidence files.

See [STRUCTURE.md](STRUCTURE.md) for the registration and testing contract.
