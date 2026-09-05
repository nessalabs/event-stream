# Release checks

Execution status: in progress. This checklist specifies required checks, not a release claim.

The library currently declares Rust 1.85 and keeps SQLite and decoding optional. `Cargo.lock` pins the tested dependency resolution. The package remains unpublished while acceptance and performance evidence are incomplete.

Operational behavior is described in [running and recovering a local store](operations.md).

## Automated contracts

The repository workflow `.github/workflows/core.yml` runs the shared adapter, runtime, decoder, and process-crash tests on Linux and macOS with stable Rust, plus Linux with Rust 1.85. It also compiles the domain without external dependencies and runs the custom-store, reconnect, and custom-decoder examples. Feature-off tests verify that optional adapters are not required by the core.

The workflow is configured locally. No remote workflow execution is claimed until its actual result is inspected. Linux remains an unverified target until those tests run there. Local macOS evidence does not establish Linux locking or filesystem behavior.

Formatting and warning-free Clippy are separate gates. A failing style job must be fixed before release; it must not be disabled merely to make the workflow green.

## Evidence required before publishing

- All findings in [the completion audit](completion-audit.md) resolved with direct tests and inspected results.
- Stable and minimum Rust tests passing for the release sources, with dependency and feature compatibility recorded.
- Supported operating systems and local filesystems named from actual results.
- The chosen SQLite format, engine version, effective settings, ownership restrictions, backup/reopen procedure, and persistence profile documented.
- Bounded workload measurements, regression budgets, source/configuration identity, and raw samples retained for memory, decoding, and durable paths.
- Crash and fault schedules tested at the required boundaries. Process-crash claims remain distinct from untested power-loss claims.
- GPUI verification scenarios run the same production APIs and report unavailable measurements honestly.

Publishing a package, creating a remote release, and asserting support for an untested platform are separate actions from local implementation. None has occurred as part of this checklist.

Workflow setup references: [GitHub workflow syntax](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax) and [official checkout action](https://github.com/actions/checkout). The workflow uses the documented v7 checkout action.
