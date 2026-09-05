# Verification architecture

Keep the window thin. It chooses a scenario and presents the result; it does not decide whether production behavior is correct.

```text
GPUI sidebar / main panel                 headless CLI
             |                                 |
             +-------- ScenarioExecutor -------+
                              |
                       RegisteredExecutor
                              |
                      named Rust callback
                              |
               actual implementation / adapter
                              |
                   observations + actual output
                              |
                        EvidenceStore
                              |
                 FileEvidenceStore: latest JSON
```

## Modules and dependency injection

| File | Responsibility |
| --- | --- |
| `src/main.rs` | Compose dependencies and launch GUI or CLI |
| `src/ui.rs` | Selection, folder expansion, run state, and evidence presentation |
| `src/catalog.rs` | Ordered workstreams, stable scenario IDs, fixtures, expected results, and callable registrations |
| `src/runner.rs` | Execution, outcome calculation, evidence serialization, and real harness checks |
| `src/scenarios.rs` | Thin bounded adapters that call the production runtime, stores, and decoder |
| `src/lib.rs` | Shared headless API |

`ScenarioExecutor` and `EvidenceStore` are injected into the UI. The default implementations execute registered functions and store bounded evidence. Tests can inject a failing evidence store without breaking the real filesystem. A scenario callback accepts `RunContext`, including its explicit input and workspace root, and returns observations plus actual output.

A function callback is sufficient for scenario registration. The production component being exercised should expose its own meaningful boundary, such as `EventStore` or an incremental decoder. Do not invent a parallel implementation inside the scenario.

## Register a real verification

1. Implement the production behavior and focused tests in its owning crate/module.
2. Add a thin scenario function that constructs the real component with explicit dependencies. Pass the fixture to that component and capture its actual returned records/errors.
3. Return expected-versus-observed assertions and bounded output. Include the seed/schedule for generated tests and identify any injected mock/failure.
4. Set the scenario's `run` callback, source path, target name, and evidence scope. Remove its blocked state. Use stable IDs so saved evidence stays associated with the same verification.
5. Run the same scenario headlessly and through the window. A passing scenario with no assertions is rejected.

Keep `run: None` while the target does not exist. Neither the UI nor CLI converts missing implementations into successful evidence. Current callable checks verify this harness and its documents, not the stream core.

## Test layers

| Layer | Exercise | Evidence |
| --- | --- | --- |
| Focused tests | Validation, state transitions, error classification | Fast deterministic checks |
| Property tests | Generated event/retry/cursor histories and decoder partitions | Invariant checks plus reproducible seed/minimized counterexample |
| Scenario tests | Ordered operations and controlled failure schedules | Input, expected output, actual records/errors, timing |
| Shared store contracts | Identical behavior through each `EventStore` implementation | Memory and SQLite results checked against one contract |
| Real database E2E | Isolated SQLite file, commit, close, reopen, retry, ownership, failures | Bytes/cursors/receipts actually recovered from storage |
| UI verification | Select, run, navigate during execution, inspect saved/failed/blocked state | Window behavior; never counted as storage proof |

Property-test frameworks can be selected when production generators are introduced. Today the catalog validator has an exhaustive generated test for every duplicate-ID pair. This is harness coverage, not property coverage of a stream implementation.

Inject clocks and failure points when time or scheduling must be controlled. Mock an I/O failure for a focused test, but run real SQLite for database persistence evidence. Crash and power-loss claims need the appropriate process/device harness; an in-memory mock cannot establish them.

## Evidence and resource rules

Runs include scenario/implementation identity, compiled-source fingerprint, input, observations, output, and measured wall time. Runnable production callbacks validate that their fixed displayed input matches the fixture they execute. The fingerprint includes the scenario code, exercised production sources, public contracts, manifests, and lockfiles. CPU/memory stay `null` until instrumentation supplies them. Wall time excludes JSON save and UI rendering and is not a production benchmark.

One GUI run executes at a time. Latest evidence is retained per scenario, with a 256 KiB file limit. Preview text is capped; copying JSON returns the complete bounded report. Save/load failures are visible and do not overwrite the execution outcome. Existing saved results are historical evidence and must be rerun when documents or external fixtures change.

Long-running future scenarios need explicit deadlines and cooperative cancellation before registration. Do not block the UI, accept arbitrary shell commands from fixtures, or launch an unbounded task per event. Real DB fixtures must use isolated temporary directories and release workers/handles before cleanup.
