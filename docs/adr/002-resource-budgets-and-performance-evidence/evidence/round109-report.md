# Round 109: bounded SQLite group commit

SQLite now implements the bounded batch port with one worker command and one
IMMEDIATE transaction. It uses savepoints to isolate known per-event rejections.
Unexpected errors roll back the group. Commit uncertainty is reconciled against
every newly inserted record, including its exact cursor and event bytes.

The aggregate logical request charge is checked before cloning and cannot exceed
`max_record_bytes`. One group therefore does not multiply the queue's existing
per-command logical payload bound. Requests, outcomes and reconciliation state
remain bounded by 64 inputs. These are logical accounting rules, not an
allocator-exact RSS guarantee. The worker retains accepted work after caller
cancellation and close drains it.

## Validation

The full all-feature library/integration run passes **338 tests** before the
final closed-reply hardening. After that change, all three staging/reply tests
and all seven grouped integration/sync tests pass, with Clippy and Rust 1.85
rechecked. New checks include:

- whole-group rollback with no false inserted or same-group dedup receipt;
- preexisting retries distinguished from newly staged identities;
- lost commit acknowledgement reconciled exactly, then reopen and retry;
- replica backlog seeing earlier items while an independent stream commits;
- oversized group rejected without storage changes;
- cancelled caller followed by draining close and exact reopen;
- real VFS write, sync, main-write/rollback and rollback-truncate faults, with
  explicit evidence that each fault fired and all-or-none recovery.

The final strengthened VFS test also passes independently. Five grouped tests
pass with SQLite alone, without replication. Strict all-target Clippy, Rust
1.85 checks, formatting and all 19 verification tests pass.

A separate process installs the forwarding SQLite VFS recorder before opening
connections. Eight individual durable inserts produce **24 sync callbacks**;
eight grouped inserts produce **3**. Both histories match exact IDs, payloads
and cursors after reopen. See `round109-focused.log`. This proves shared SQLite
sync work for that fixture. It is not a throughput, latency or memory benchmark,
and callback counts do not independently enumerate every kernel system call.

## Remaining work

The runtime still calls individual append. It does not yet collect ready work
across streams or account for the batch outcome envelope. Group process-kill
schedules and controlled population, sparse-load and mixed-load comparisons
remain required. The higher-rate SQLite population gap is not closed by these
tests. Home round 109 therefore records no new latency/memory experiment rows.

See the [contract](../append-batch-contract.md) and
[evaluation](../group-commit-evaluation.md). Changed-source hashes and test logs
are retained beside this report. No schema or durability setting changed.

Final independent review identified a lost worker-reply case after command
acceptance. The adapter now returns `CommitUnknown` with each original ID;
submission failure still returns its definite error. A test closes the actual
reply channel and verifies every uncertainty identity. This is channel-failure
evidence, not a process-kill test. Final review found no other reachable issue.
