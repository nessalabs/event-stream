---
id: "console"
title: "Read scenario evidence honestly"
kind: "guide"
status: "reference"
summary: "The native console runs actual callbacks and distinguishes blocked scenarios from passes."
parent: "events"
sources: ["verification/src/catalog.rs", "verification/src/runner.rs", "verification/src/ui.rs", "verification/src/performance.rs", "verification/evidence/home.json"]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
---

# Find what a scenario actually exercised

The separate GPUI verification app is a consumer of the same library APIs. Choose
a workstream/scenario, inspect its fixture and target, then run its registered
callback. Planned targets without a callback are blocked, not simulated passes.
The headless CLI uses the same registry/executor and can list or run scenarios
without opening a window.

The UI admits one run at a time on a background worker. Reports show expected and
actual observations and elapsed time; unavailable CPU/memory are not inferred.
Latest reports are bounded and saved atomically under evidence/runs. Source,
runner, dependency or fixture changes invalidate older evidence. Save/load failure
is reported; a missing report is not a green test. These artifacts are local review
evidence, not a durable event store or power-loss guarantee.

Home uses the persisted experiment-round history in verification/evidence/home.json.
A round is a local work iteration. Only compatible workload/configuration samples
may be joined into comparisons; not_measured rows have no fresh performance data.
This atlas iteration adds a not-measured round with source and validation scope.
It does not reinterpret sparse 100k-stream results as 100k concurrent producers.

Native console rendering and responsiveness require their own current interaction
check. Nessa Docs browser smoke validates this atlas only. The console's headless
build/tests and live native UI coverage are reported separately in
[verification](verification.md). No atlas model simulates a passing UI scenario.
