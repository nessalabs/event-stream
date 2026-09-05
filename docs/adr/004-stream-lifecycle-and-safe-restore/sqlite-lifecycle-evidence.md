# SQLite lifecycle evidence

Status: lifecycle storage implemented and verified. Controlled restore remains a
separate required slice.

## Stored state

Format 2 keeps a stream name after its last history is cleaned. The name row
stores the latest incarnation and an optional active lifetime key. Lifetime rows
hold active or retired history. Lifecycle receipts are independent rows, so
cleanup cannot erase retry truth.

```text
event_stream_names: public_id -> latest_incarnation, active_stream_key?
event_streams:      stream_key -> public_id, incarnation, floor, tail, retired
lifecycle_receipts: operation_id -> request, replacement?, logical charge
```

The metadata row stores receipt count/bytes and retired-lifetime count/bytes.
Every lifecycle transaction updates these counters with the name, lifetime, and
receipt changes. Open compares the counters with bounded receipt and retired-row
aggregates. A positive but understated counter is corruption; it cannot silently
bypass a configured quota.

The defaults permit 10,000 receipts using at most 4 MiB of logical receipt charge,
and 10,000 retired lifetimes using at most 4 MiB of retired metadata charge. The
limits must fit SQLite's signed integer range. Identical retries read their
receipt before capacity checks.

## Transaction boundaries

The dedicated adapter worker serializes append, reset, delete, and cleanup. A
lifecycle transaction validates the active name-to-lifetime link before it
retires anything. It then retires the old lifetime, optionally creates the empty
replacement, updates the name, stores the receipt, and advances counters in one
commit.

A cleanup transaction selects one retired lifetime. It deletes a contiguous
prefix within both requested limits and advances that retired floor. Empty
history below the stored tail is corruption. When the floor reaches the tail,
the transaction removes the lifetime metadata and decrements its exact counters.
The unavailable name and lifecycle receipts remain.

The cleanup byte value is the portable logical charge:

```text
event ID bytes + schema ID bytes + payload bytes + 128 + 256
```

It is not a claim about physical SQLite pages reclaimed.

## Format-1 migration

Migration runs under the same process and file ownership held by normal open.
Foreign-key enforcement is disabled only around SQLite's documented generalized
parent-table alteration procedure, then restored before open completes. One
transaction creates the format-2 lifetime table, copies lifetime metadata, drops
and renames the parent table, creates name and receipt tables, initializes zero
counters, checks foreign keys, and changes the format marker.

`event_records` is not copied or rebuilt. The real fixture checks its root page
and exact payload bytes before and after migration. A deterministic failure just
before migration commit rolls the whole transaction back. The same valid
format-1 database then migrates on retry. A separate corrupt-input fixture proves
that constraint failure does not leave a partial format-2 schema.

## Verification scope

The SQLite integration suite covers:

- the shared lifecycle adapter contract;
- receipt retries, conflicts, reset, delete, stale keys, and unavailable names;
- close/reopen after lifecycle commits and cleanup;
- receipt and retired-history capacity with exact persisted counters;
- malformed receipts, understated counters, and a name pointing at another
  lifetime;
- definite failure before lifecycle commit and resolution after a lost commit
  acknowledgement;
- cleanup rollback and internal-hole detection;
- valid format-1 migration, injected migration rollback, corrupt input, and
  newer-format refusal.

The same run also executes the existing real SQLite process-kill, database-full,
private-filesystem exhaustion, and forwarding-VFS write/sync/rollback tests.
Those tests establish the documented process-restart profile. They do not add a
device power-loss guarantee.
