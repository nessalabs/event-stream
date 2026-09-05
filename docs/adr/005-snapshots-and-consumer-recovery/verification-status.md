# Snapshot verification status

ADR 0005 is in progress. Passing the Memory adapter contract does not establish
SQLite durability, restart recovery, or the resource budgets below.

## Latest independently checked checkpoint

Round 20 independently passed the [publication process-kill test](evidence/round20-crash.log),
using 96 KiB across three chunks and exact recovery reads after reopen. The child
is killed after publication commit and before its acknowledgement. The
[historical snapshot restore test](evidence/round20-historical.log) also passed:
a snapshot whose retired stream history was fully cleaned remains restorable,
while recovery reports missing history. These cover those specific boundaries;
staging and verification crash points and resource qualification remain open.


[Round 18](evidence/snapshot-retention-round18-tests.log) passed 81 focused tests:
3 Memory retention, 8 snapshot runtime, 42 SQLite and 28 restore tests. SQLite
checks include exact retry after a deliberately lost commit acknowledgement.
Restore checks cover aborted uploads with a remaining chunk suffix, no remaining
chunks before final metadata cleanup, and a fully cleaned receipt.

The earlier [round 15 checks](evidence/snapshot-round15-tests.log) passed
13 Memory, 8 runtime and 2 application-example tests. The compiled
[Memory callback](evidence/snapshot-round15-callback.json) passed three checks.
Its display initially mixed snapshot state with final recovered state. The corrected source label passed a fresh [release build](evidence/round20-ui-build.log)
and [callback run](evidence/round20-callback.json) in round 20. Snapshot state
contains two transcript items; the recovered state contains three. Native
window capture failed after restart, so this is headless callback evidence.

These are checkpoint results. Ongoing retention edits need their own verification.
A simulated lost acknowledgement does not prove survival of abrupt process death.
The latest native Home inspection displayed 13 rounds and 93 experiments; the
JSON now contains 21 rounds. Later rounds have no fresh performance measurements.

| Required behavior | Current evidence | Remaining proof |
| --- | --- | --- |
| Exact chunk retry and conflict, partial-chunk verification, digest check, empty snapshots, equal-cursor pagination | Shared contract exercised by MemoryStore in `tests/common/mod.rs` and `tests/core_memory.rs` | SQLite shared contract and reopen now pass; extend crash coverage |
| Accepted work remains owned after cancellation; panic returns operation identity; bounded admission | Eight runtime tests in `tests/core_snapshots.rs`, including shutdown waking a capacity waiter while the accepted operation remains owned | Repeat through SQLite and integrated shutdown |
| Recovery reads use a finite lease and preserve the required retired suffix | Memory test with injected clock | SQLite retained-suffix test passes; retention races under ADR 0006 remain |
| Store close invalidates recovery access; expiry is checked under the store lock | Direct Memory close and controlled-clock expiry regressions pass | Repeat through SQLite, including restart |
| Cleanup does not interrupt a live upload | Memory cleanup selects only explicitly aborted uploads; shared and generated interleaved sequences pass; bounded staging listing is exercised | SQLite restart and aborted-cleanup restore checks pass; abrupt process-death tests remain |
| All snapshot metadata has aggregate limits | Memory descriptor and chunk aggregate quota tests passed; descriptor charges remain for retry receipts after cleanup | SQLite quota tests and measured allocation/page overhead; receipt expiration belongs to ADR 0006 |
| Snapshot plus suffix equals complete replay | Transcript and workflow example plus native/headless Memory callback use public runtime APIs and check application schema compatibility | SQLite restart and crash evidence |
| Publication survives crash and lost acknowledgement | Runtime unknown-result identity tested with a fault adapter | Real SQLite process termination during staging, verification and publication; exact retry after restart |
| Corrupt stored data is rejected before large allocation | Memory checksum mismatch tested | SQLite wrong types, lengths, offsets, digest and malformed chunk fixtures |
| Controlled restore remaps snapshot identity references | Required by the contract | Identity remap and digest validation pass; historical snapshots below the floor and fully retired lifetime references need explicit regression coverage |
| Bounded working memory and foreground latency | No snapshot performance claim | Repeated size sweeps, CPU/RSS/allocations, staging and database I/O, append latency alongside snapshot work |

## Completion rule

Keep each required gate open until its stated evidence exists. A native scenario
must call the production API it names. Memory-only checks must say so. A crash
claim needs an actual database/process failure test, not a cancelled Rust future.

The [contract](contracts.md) defines behavior. The [ADR](adr.md) defines phase
completion. Neither this checklist nor the Home page replaces those requirements.
