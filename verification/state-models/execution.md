# Current execution scope

Source: `e16580f3d3602a1ff9966e89d4915be4e752aea0`. Date: 2026-11-03.
Host: macOS arm64. Rust 1.99.1 isolated toolchain. No production code changed.

| Command | Observed result |
| --- | --- |
| `cargo test --locked --all-features --all-targets` | 358 passed, zero failures or ignores across 51 test groups |
| `cargo test --locked --manifest-path verification/Cargo.toml` | 19 passed, zero failures or ignores; separate GPUI consumer compiled |
| `cargo clippy --locked --all-features --all-targets -- -D warnings` | Passed |
| `cargo fmt --all --check` | Passed |
| `RUSTDOCFLAGS='-D warnings' cargo doc --locked --all-features --no-deps` | Passed |
| Canonical model runner | Seven baseline models, fourteen invariants and fifteen expected semantic control failures |

The library test command includes shared adapter, SQLite fault/restart/restore,
retention, snapshot, journal, replication and resource-regression tests. Counts
include helper/test entry points and do not mean that many independent crash
schedules. Existing example targets compiled under all-targets; this command does
not run every executable example's main procedure. The separate verification
workspace tests validate headless runner/Home behavior, not an opened native window.

Local raw logs are retained outside the checkout under `work/org-docs-tools/` as
`event-tests.log`, `event-verification-tests.log`, `event-clippy.log` and
`event-rustdoc.log`. Re-run the named commands for fresh implementation evidence.
Model receipts and mutations are checked in beside this summary.

No new performance benchmark, sustained soak, physical device power-loss run,
Linux execution or native console responsiveness check is claimed. The new Home
round 120 is not_measured and creates no invented performance samples. Historical
ADR measurements retain their original source/workload/platform scopes.

The targeted `checked_in_home_catalog_loads_bounded_historical_evidence` test passed
after adding the not-measured Home round. Atlas browser smoke rendered 14 pages
and 10 diagrams with zero errors; the native console was not opened.
