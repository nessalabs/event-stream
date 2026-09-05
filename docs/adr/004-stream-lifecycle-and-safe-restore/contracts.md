# Lifecycle implementation contract

Status: implementation contract; the operations below are not yet implemented.

A reset changes which lifetime a stream name refers to. It does not rewrite old
records. A delete makes that name unavailable. Both operations must be safe to
retry after losing the response.

## Domain values

Add these values in `domain/lifecycle.rs`. Identifiers use the same nonempty,
256-byte validation as stream identifiers. They remain distinct Rust types.

```rust
struct LifecycleOperationId(/* validated string */);
enum LifecycleAction { Delete, Reset }
struct LifecycleRequest {
    operation_id: LifecycleOperationId,
    expected: StreamKey,
    action: LifecycleAction,
}
struct LifecycleReceipt {
    request: LifecycleRequest,
    replacement: Option<StreamKey>, // Some only for Reset
}
```

The operation ID is unique within one store. Compare the complete request when
an ID is reused. An identical request returns the original receipt. A different
request returns an operation conflict. Check receipts before checking the current
incarnation: a successful reset necessarily made its own expected key stale.

```text
name: agent-7              current lifetime: A
reset(op-42, expected=A)   -> lifetime B, empty history
reset(op-42, expected=A)   -> same receipt containing B
reset(op-43, expected=A)   -> stale lifetime error
append(key=A, event)       -> stale lifetime error
```

## Storage boundary

Add an optional `LifecycleStore: EventStore` application port. Existing custom
stores can continue implementing only `EventStore`. Keep these operations off the
ordinary append path.

```rust
async fn change_lifecycle(&self, request: LifecycleRequest)
    -> Result<LifecycleReceipt>;
async fn cleanup_retired(&self, limits: CleanupLimits)
    -> Result<CleanupProgress>;
```

Add explicit errors for a stale incarnation, an unavailable stream, and an
operation-ID conflict. An uncertain lifecycle commit carries its operation ID.
Do not reuse `CommitUnknown { event_id }` for a different kind of operation.

`CleanupLimits` has nonzero maximum records and accounted bytes. One maximum-size
record must fit the byte limit. `CleanupProgress` reports removed records,
accounted bytes, and whether retired history remains. Accounted bytes are logical
record/index charges, not a claim about filesystem space reclaimed.

Store options separately bound lifecycle receipt count and bytes, and retired
lifetime metadata. Refuse a transition before mutation when its receipt or
metadata cannot fit. Keep receipts for the store lifetime in this phase. Do not
evict retry truth to make space. Receipt expiry needs a later explicit contract.

## Atomic state change

Keep current-name lookup separate from histories keyed by stream and incarnation.
A transition changes only metadata and its receipt in one transaction. It must
not walk or delete the old record set inside that transaction.

| Existing state | Request | Result |
| --- | --- | --- |
| Active A | Delete, expected A | A becomes unavailable; retire its history |
| Active A | Reset, expected A | Publish empty B; retire A |
| Unavailable A | Reset, expected A | Publish empty B |
| Unavailable A | Delete, expected A, new operation ID | Receipt confirming unavailable A |
| Any state | Previously committed identical request | Original receipt |
| Current B | New request expected A | Stale incarnation; no mutation |

`create_if_absent` returns the active key, creates a genuinely new name, or returns
unavailable for a deleted name. Recreating a deleted name requires explicit reset
with its expected key. This prevents an innocent lookup from undoing deletion.

Retired history stays inaccessible through ordinary reads and appends even while
its records await cleanup. Cleanup deletes a bounded prefix of retired records
and their retry index entries. Progress and deletion commit together. It can
resume after restart. Remove retired metadata only when its history is empty;
lifecycle receipts remain independently available.

## Runtime coordination

Expose lifecycle methods only on `Runtime<S>` where `S: LifecycleStore`. They use
owned, tracked work so cancellation does not abandon an accepted database change.
Bound outstanding maintenance requests separately. Include them in shutdown.

Serialize the transition with append and subscription registration using the
existing per-stream write gate. An old append that reaches storage first may
commit. One that reaches storage after the transition fails explicitly. Neither
can append to the replacement. Queued requests retain their original `StreamKey`.

After a confirmed transition, terminate old subscriptions and release their page
and membership quotas exactly once. Wake waiting readers. A replacement gets its
own coordinator. Do not change a subscriber's cursor identity. If the outcome is
uncertain, stop the affected coordinator until retry resolves the operation; do
not keep serving a cached assumption about the active lifetime.

A read already in progress may finish before the transition. The runtime must
check terminal state before returning another buffered record after it has
observed the transition. Document this boundary; do not claim it can revoke a
record already returned to an application.

## Parallel ownership and verification

The runtime/domain implementer owns domain values, the optional port, runtime
coordination, memory storage, and shared contract tests. The SQLite implementer
owns transactional schema changes, migration, persistent cleanup, and database
fault tests. The verification consumer calls the public runtime methods after
both implementations satisfy the same contract.

Before changing SQLite's format, add a real format-1 fixture and migration tests.
Migration must run under exclusive ownership in one transaction. A failed
migration must leave a reopenable old format; a newer format must remain refused.

Test identical and conflicting operation retries, stale reset/delete/append,
delete followed by explicit reset, queued writes, blocked reads, abandoned
subscribers, cancellation, shutdown, receipt capacity, and bounded cleanup.
Property tests compare generated transition sequences with a small state model.
Database tests close and reopen at transition and cleanup boundaries. Include a
lost response after commit and prove it does not allocate a second replacement.

Measure transition latency separately from cleanup. Report foreground append
latency during cleanup, rows and bytes per turn, temporary journal space, and
actual database size. A row budget bounds requested work; it is not a hard wall
clock deadline for an OS or SQLite call.

## Controlled restore remains a separate required part

Reset/delete completion alone does not close ADR 0004. Restore requires an
exclusive staging destination, bounded copying, format and integrity checks,
and atomic publication before traffic is admitted. A divergent restore assigns
fresh stream identities and returns an explicit old-to-new mapping. Partial
staging never opens as the active store. Its detailed publication and crash
contract must be specified before implementing that path.

## Resolved implementation details

A deleted name retains its last incarnation independently of retired history.
Charge this unavailable name to the existing stream-name count/metadata limits.
Cleanup may remove A's records and lifetime row; it must not erase the name's
last incarnation or lifecycle receipts. SQLite should store the last incarnation
on the name row and use a nullable active-lifetime reference.

Look up an existing operation receipt before state **and capacity** checks.
An identical retry must still succeed when the receipt budget is full. Deleting
an already unavailable name creates only a new receipt, not another retired
lifetime. Reset from unavailable replaces the name's state; it does not retire
the same history twice.

Use portable logical charges for the shared quota contract:

```text
receipt charge = operation_id UTF-8 bytes + expected stream-name UTF-8 bytes + 256
retired lifetime charge = stream-name UTF-8 bytes + 256
cleanup record charge = event.accounted_bytes() + 256
```

The fixed overhead is a policy charge, not a measured allocation size. Actual
Rust and SQLite allocations remain separately measured. Use checked arithmetic.
Default adapter lifecycle quotas are 10,000 receipts / 4 MiB receipt charge and
10,000 retired lifetimes / 4 MiB retired metadata charge. An operation that
exceeds either quota fails before any state change. Active-name quotas still
apply independently.

Add these exact error shapes to the application layer:

```rust
enum StreamAvailability {
    Active(StreamKey),
    Unavailable(StreamKey), // key of its last lifetime
}
// Error variants:
StaleIncarnation { current: Box<StreamAvailability> }
StreamUnavailable { last: Box<StreamKey> }
LifecycleConflict { operation_id: LifecycleOperationId }
LifecycleCommitUnknown { operation_id: LifecycleOperationId }
```

A missing name remains `StreamNotFound`. The stream name is already present in
the request when checking a stale key. Keep large optional context boxed so it
does not enlarge every common `Result` unnecessarily.

Add `MaintenanceConfig` to `RuntimeConfig` when lifecycle ships:

```rust
MaintenanceConfig {
    max_operations: 8,
    max_operation_bytes: 64 * 1024,
    max_cleanup_operations: 1,
    cleanup: CleanupLimits {
        max_records: 256,
        max_bytes: 2 * 1024 * 1024,
    },
}
```

These are finite defaults. Use the receipt charge to reserve lifecycle request
bytes. Maintenance admission is immediate: return `Overloaded` when full.
There is no separate waiting queue or background polling task in this phase.
A caller explicitly requests each cleanup turn. Cleanup shares the bounded
maintenance operation count and also has its own concurrency limit. An accepted
operation is tracked until its store I/O ends, even if its caller disappears.
The existing shutdown deadline bounds how long shutdown waits; it cannot safely
interrupt an arbitrary SQLite OS call. Report unfinished work explicitly.

Extend `ShutdownReport` with `unresolved_lifecycle: Vec<LifecycleRequest>` and
`unfinished_cleanup: usize`. Keep `unresolved` for existing append identities.
The vectors/count are bounded by admitted work. Confirmed receipts clear pending
lifecycle state. A definite failure also clears it because it promises that the
mutation did not commit. An unknown result retains the exact request and blocks
operations on that name until an identical retry resolves it. A different
lifecycle request fails with `LifecycleCommitUnknown` carrying the unresolved
operation ID. Other stream names continue. Do not fault the entire runtime for
one uncertain transition. Keep this state at the runtime/name boundary, so
creating or obtaining the replacement key cannot bypass reconciliation.

A cleanup turn chooses one retired lifetime and removes a contiguous prefix of
its remaining records. Selection order across lifetimes is unspecified. Return
`CleanupProgress { stream: Option<StreamKey>, removed_records, removed_bytes,
remaining: bool }`. `stream` is None only when there was no work. `remaining`
means some retired history or retired metadata remains anywhere in the store.
Zero-record retired entries may be removed in a turn, but at most one lifetime's
metadata is finalized per call. This keeps metadata-only work bounded too.

The adapters must serialize lifecycle changes with direct `append_atomic` calls
inside the same lock or transaction. The runtime gate does not substitute for
that storage guarantee. Runtime caching assumes mutations go through its owned
store boundary; do not expose a separately mutable adapter clone from Runtime.
Tests using the adapter directly verify storage atomicity, not runtime subscriber
notification after an out-of-band mutation.

The SQLite migration rebuilds only the lifetime metadata table. It follows
SQLite's [general schema-change procedure](https://www.sqlite.org/lang_altertable.html#making_other_kinds_of_table_schema_changes): create the replacement table,
copy its rows, replace the old table inside a transaction, and check foreign keys
before committing. The event-record table stays in place. Tests check its root
page and exact stored bytes, including rollback of an interrupted valid migration.
