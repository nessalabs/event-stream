---
id: "verification"
title: "Check source tests and model limits"
kind: "guide"
status: "reference"
summary: "Current tests, negative controls, independent review and historical measurements remain separate."
parent: "events"
sources: ["AGENTS.md", "docs/stability-handoff.md", "Cargo.toml", "verification/evidence/home.json"]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
---

# Check the scope before relying on a pass

The reviewed implementation checkpoint is e16580f3d3602a1ff9966e89d4915be4e752aea0.
All-feature/all-target library tests passed on the local macOS arm64 host using
isolated Rust 1.99.1: 358 passed, zero failed or ignored. The separate GPUI
verification workspace built and its 19 tests passed, with no failures or ignores.
All-feature/all-target Clippy, formatting and warnings-denied Rustdoc also passed.
The atlas browser smoke passed all 14 pages and 10 diagrams with zero errors;
see [browser receipt](../../verification/state-models/browser-results.json).
Independent reader/source/model review closed with no unresolved material findings
within its sampled scope; see the [review and correction](../../verification/state-models/review.md).

Seven finite models target selected safety rules. TLC executes those mathematical
models, not Rust or SQLite. Each needs a passing current baseline, source-grounded
adversarial review and semantic negative controls. Seven current baselines passed all 14 configured invariants, and all 15 controls
broke their expected semantic rule. The checker explored 159 aggregate states,
largest model 64. Review strengthened the lifecycle model to independently retain
and assert both retry action and expected identity, with a control for each. Models omit OS durability, network
trust, application meaning, arbitrary scale and eventual progress.

See [model results](../../verification/state-models/results.json),
[negative controls](../../verification/state-models/controls.json) and
[execution details](../../verification/state-models/execution.md). Reproduce models
with a built Nessa Docs checkout and its canonical pinned checker cache installed:

```sh
NESSA_DOCS_ROOT=/absolute/path/to/nessa-docs node scripts/state-docs/verify.mjs --write
```

The runner validates the catalog, runs each unmodified baseline before controls
and writes only canonical baseline receipts into pages. Parser failures and
uncompleted runs do not count as successful negative controls. It does not install
checker dependencies automatically.

No performance benchmark expansion or feature change is part of this work. The
Home round is not_measured. Existing sparse population measurements, process-kill
recovery and configured CI do not prove simultaneously busy 100k-agent operation,
device power loss or deployment readiness. No native console interaction pass is
implied by a headless build or by rendering the atlas in Nessa Docs.

The [stability handoff](../stability-handoff.md), [operations guide](../operations.md)
and historical ADR evidence preserve remaining qualification work. No checks,
thresholds or production code are weakened to produce a documentation pass.
