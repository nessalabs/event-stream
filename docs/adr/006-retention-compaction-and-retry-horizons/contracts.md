# Retention and retry-generation implementation contract

Status: implementation in progress. The typed boundary and the first bounded
MemoryStore operation sequences exist. Runtime admission and SQLite retention
have initial passing checks, including restart and recovery-lease protection.
Controlled restore, complete fault coverage, examples and resource qualification
remain open.

This contract separates two decisions. A replay floor says which event records
remain readable. A retry generation says which request identities remain valid.
Moving either boundary is explicit, durable and monotonic.

## Start bounded retries without changing history

The existing append API promises exact retries for the whole stream lifetime.
That promise remains unchanged. A store cannot convert existing lifetime IDs to
an expiring policy without changing what old callers observe.

An application may enable bounded retries in place. Existing unqualified event
identities become generation 0 without changing their records, cursors, schemas
or bytes. The store represents that default generation in existing identity
rows; it does not copy the history into a background migration. The transition
atomically records oldest generation 0 and current generation 1. After that
transition, ordinary `append_atomic(NewEvent)` returns
`RetryPolicyRequired`; it never guesses a generation.

```rust
pub struct RetryGeneration(u64); // 0 is legacy; new work starts at 1

pub struct GeneratedEvent {
    pub generation: RetryGeneration,
    pub event: NewEvent,
}

pub enum RetryPolicyState {
    Lifetime,
    Generational {
        oldest_accepted: RetryGeneration,
        current: RetryGeneration,
    },
}
```

Within the generational policy, `(stream incarnation, generation, event ID)` is
the retry identity. The same event ID may be used in a later generation because
the generation is part of the identity. An exact retry returns the original
full record and cursor. Different schema or payload under the same identity is
a conflict.

Generation 0 is reconciliation-only after the transition. An exact existing
generation-0 identity returns its original receipt. An absent generation-0 ID
returns `LegacyRetryNotFound` and never creates new work. An append in any
generation from 1 or `oldest_accepted`, whichever is greater, through `current`,
inclusive, may commit. This lets a delayed request finish while its generation
remains valid. A generation below `oldest_accepted` returns `RetryGenerationExpired`
before event lookup. A generation above `current` returns `RetryGenerationAhead`.
Neither error appends a record.

## Typed boundary

Identifiers use the same validated 1..=256 UTF-8 byte rule as lifecycle
operation IDs. Requests and their receipts are exact retry values.

```rust
pub struct RetentionOperationId(Box<str>);

pub struct EnableRetryPolicy {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
}

pub struct AdvanceRetryGeneration {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
    pub expected_current: RetryGeneration,
}

pub struct ExpireRetryGenerations {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
    pub expected_oldest: RetryGeneration,
    pub retain_from: RetryGeneration,
}

pub struct AdvanceRetentionFloor {
    pub operation_id: RetentionOperationId,
    pub stream: StreamKey,
    pub expected_floor: Cursor,
    pub new_floor: Cursor,
}

pub struct RetentionStatus {
    pub bounds: Bounds,
    pub retry_policy: RetryPolicyState,
}

pub struct EnableRetryPolicyReceipt {
    pub request: EnableRetryPolicy,
    pub status: RetentionStatus,
}

pub struct AdvanceRetryGenerationReceipt {
    pub request: AdvanceRetryGeneration,
    pub status: RetentionStatus,
}

pub struct ExpireRetryGenerationsReceipt {
    pub request: ExpireRetryGenerations,
    pub status: RetentionStatus,
}

pub struct AdvanceRetentionFloorReceipt {
    pub request: AdvanceRetentionFloor,
    pub status: RetentionStatus,
}

pub struct RetentionCleanupLimits {
    pub max_event_rows: usize,
    pub max_retry_rows: usize,
    pub max_bytes: usize,
}

pub struct RetentionCleanupProgress {
    pub removed_event_rows: usize,
    pub removed_retry_rows: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}

#[async_trait]
pub trait RetentionStore: SnapshotStore {
    async fn retention_status(&self, stream: &StreamKey)
        -> RetentionResult<RetentionStatus>;
    async fn enable_retry_policy(&self, request: EnableRetryPolicy)
        -> RetentionResult<EnableRetryPolicyReceipt>;
    async fn advance_retry_generation(&self, request: AdvanceRetryGeneration)
        -> RetentionResult<AdvanceRetryGenerationReceipt>;
    async fn expire_retry_generations(&self, request: ExpireRetryGenerations)
        -> RetentionResult<ExpireRetryGenerationsReceipt>;
    async fn append_generated(&self, stream: &StreamKey, event: GeneratedEvent)
        -> RetentionResult<AppendReceipt>;
    async fn lookup_generated(&self, stream: &StreamKey,
        generation: RetryGeneration, event_id: &EventId)
        -> RetentionResult<Option<Arc<Record>>>;
    async fn advance_retention_floor(&self, request: AdvanceRetentionFloor)
        -> RetentionResult<AdvanceRetentionFloorReceipt>;
    async fn cleanup_retention(&self, limits: RetentionCleanupLimits)
        -> RetentionResult<RetentionCleanupProgress>;
}
```

After the policy is enabled, the unqualified `EventStore::lookup_event` returns
`RetryPolicyRequired`. `lookup_generated` applies the same generation bounds as
`append_generated`. It never guesses whether an event ID means legacy work or
one of the later generations.

Lifecycle and retention operation IDs share one store-wide namespace. Every
retention mutation stores its operation ID, exact request and immutable receipt.
Repeating the same operation ID and request returns that receipt. Reusing the
operation ID with any different field returns `OperationConflict`. This is also
the reconciliation path after an unknown result; no separate receipt lookup is
needed.

The error set distinguishes `InvalidConfig`, `InvalidInput`,
`StaleIncarnation`, `StaleFloor`, `StaleGeneration`,
`RecoveryProtectionActive`, `RetryGenerationExpired`,
`RetryGenerationAhead`, `LegacyRetryNotFound`, `RetryPolicyRequired`, `OperationConflict`,
`IdempotencyConflict`, `CapacityExceeded`, `Overloaded`, `AdmissionTimeout`,
`Closed`, `CorruptStorage` and `StorageFailure`. Mutation uncertainty uses four
typed variants: `EnableUnknown(Box<EnableRetryPolicy>)`,
`AdvanceGenerationUnknown(Box<AdvanceRetryGeneration>)`,
`ExpireGenerationsUnknown(Box<ExpireRetryGenerations>)` and
`AdvanceFloorUnknown(Box<AdvanceRetentionFloor>)`. A generated append whose
commit cannot be determined returns `GeneratedAppendUnknown` carrying the exact
stream, generation and event ID; the ordinary append uncertainty lacks the
generation and is not a substitute. Callers never recover a mutation by
matching error text.

## Moving the retry boundary

`advance_retry_generation` creates exactly `current + 1`. A stale expected value
fails. `u64::MAX` fails explicitly; generations never wrap. The operation does
not expire an older generation.

`expire_retry_generations` moves `oldest_accepted` forward but never beyond
`current`. The new boundary becomes authoritative in the same transaction as
its receipt. Requests below it fail immediately even if their physical receipt
rows have not yet been removed. Cleanup later removes those rows in bounded
turns.

Output markers required by an unpublished ADR 0007 parser checkpoint block
generation expiry. Future replication acknowledgements add the same kind of
constraint. A blocked request returns the exact lowest protected generation;
it does not partially move the boundary.

## Moving the replay floor

A cursor is a position between records. Advancing the floor to 3 removes replay
access to records 1, 2 and 3. A read with `after=2` fails with
`HistoryUnavailable`; a read with `after=3` may return record 4.

The new floor must identify the exact stream incarnation. `expected_floor` must
equal the stored floor. `new_floor` must be greater than or equal to that
expected floor and less than or equal to the tail. An equal move is an exact
no-op. The store publishes the logical floor
atomically with the operation receipt before physical row cleanup.

An active recovery lease protects records strictly after its snapshot cursor
through its captured tail. Therefore a floor may advance to that covered cursor
but not beyond it while the lease is valid. Lease expiry is checked under the
same store coordination used by the floor mutation. Snapshot existence alone
does not authorize retention and does not pin event history forever.

Every `read_range` observes one consistent floor. A concurrent read either
returns the complete requested page from that view or an explicit history
error. It cannot return a shortened page that silently skips a removed prefix.
Offsets of retained records never change.

## Physical cleanup and retry receipts

`cleanup_retention` removes only event rows at or below an already published
floor and retry rows below an already published generation boundary. It checks
both row limits and the combined logical byte limit. If the first eligible row
alone exceeds `max_bytes`, it returns capacity error rather than making no
progress. `remaining` describes eligible cleanup work, not protected or future
rows.

Removing an event row does not necessarily release its payload. A still-valid
retry must return the original full record, so its receipt retains the event ID,
schema, payload and cursor until its generation expires. Event-history bytes,
retry-receipt bytes and database free pages are reported separately. Deleting
rows is never presented as immediate filesystem shrinkage.

## SQLite stores each payload once

Enabling retry generations must not copy every historical payload. Existing
records remain the canonical generation-0 storage. A separate table holds
records from later generations once, with an index on stream and cursor for
replay and a key on stream, generation and event ID for retry lookup. This
allows an event ID to repeat across generations without rebuilding existing
history. This is the selected implementation direction, still under validation.

Replay merges the two cursor-ordered sources in bounded pages. Each source uses
its cursor index. The merge must not read the complete history before applying
the caller's row and byte limits. Ordinary append and unqualified event lookup
fail once the stream uses generations.

A stored record has two reasons to remain: replay may need it, and an accepted
retry may need it. Delete its payload only after both reasons end:

```text
record offset = 4, retry generation = 1

floor=3, oldest retry=2 : keep bytes for replay; reject generation-1 retries
floor=4, oldest retry=1 : hide from replay; keep bytes for exact retries
floor=4, oldest retry=2 : eligible for bounded physical deletion
```

Logical history and retry budgets are separate from physical stored bytes.
Expiration changes retry eligibility. It does not release hard storage or
metadata reservations while those resources remain allocated. Bounded cleanup
releases each retry-specific charge once and preserves charges for bytes still
needed by replay. Later cleanup must not release the same charge again. Report processed bytes,
released logical charges and physical database size distinctly.

Reset, retirement cleanup and controlled restore must include both sources.
A build without retention support must reject a store or backup containing
retention state explicitly; it must not open a partial view that omits generated
records. Tests must prove these boundaries before the adapter is qualified.

## Restore accepts committed intermediate cleanup states

A cleanup turn may use its byte budget before deleting every eligible row.
That is a valid durable state. A backup taken between turns must restore it.
For example:

```text
record #4 belongs to generation 1
floor=4; oldest accepted generation=2

turn A: delete expired retry identity; byte budget exhausted
        payload #4 remains, pending physical deletion
backup: preserve this state; record #4 stays invisible to replay
turn B: delete payload #4; release its remaining storage charge
```

Validate visible history separately: records strictly after the floor through
the tail must form a complete contiguous range. Rows at or below the floor may
remain for an accepted retry or pending cleanup after expiry. A retry identity
must match the record's generation, event ID and cursor. An unrelated identity
at the same position is not proof. A missing visible record remains corruption;
accepting pending cleanup must not weaken that check.

## Finite configuration and admission

The adapter receives grouped retention limits at construction. Direct port
callers cannot bypass them.

```rust
pub struct RetentionStoreConfig {
    pub operations: RetentionOperationLimits,
    pub receipts: RetryReceiptLimits,
    pub cleanup: RetentionCleanupLimits,
}

pub struct RetentionOperationLimits {
    pub max_receipts: usize,
    pub max_receipt_bytes: usize,
    pub max_pending_cleanup_ranges: usize,
}

pub struct RetryReceiptLimits {
    pub max_rows: usize,
    pub max_bytes: u64,
}
```

All values are nonzero and finite. The runtime has a separate grouped
`RetentionAdmissionConfig` for concurrent operations, waiting callers, waiter
bytes, in-flight bytes and an admission timeout. Accepted mutations remain
owned after caller cancellation. Unknown outcomes retain their exact request
and capacity charge until an identical reconciliation resolves them.

The optional `retention` feature depends on `snapshots`. MemoryStore implements
the shared contract first. SQLite adds transactional state, restart and fault
evidence against the same types. Enabling neither feature adds no retention
metadata or background task to the ADR 0001 path.

## Required operation sequences

Shared generated tests cover these fixed transitions:

1. Enable on a populated lifetime, treat existing identities as generation 0,
   preserve every cursor and byte, reject ordinary append, and append generation 1.
2. Advance to generation 2 while a delayed generation-1 retry still resolves.
3. Expire generation 1, reject that delayed retry, then remove its receipt in
   bounded cleanup turns.
4. Publish a snapshot at cursor 3, capture a recovery tail at 6, advance the
   floor to 3, and reject an advance to 4 until lease release or expiry.
5. Pause a read across floor publication and observe a full old page or an
   explicit history error.
6. Cancel every accepted mutation at its storage boundary, restart SQLite, and
   reconcile the exact operation ID without duplicate state changes.
7. Fill event, retry-receipt and operation-receipt quotas independently with
   one-byte payloads before any unbounded metadata growth.
8. Inject cleanup failure, disk full and process termination after logical floor
   publication. Reopen with the floor authoritative and cleanup resumable.

Resource evidence measures foreground append/read latency during logical floor
changes, row cleanup, checkpoint/reclamation and any explicit file rewrite. It
records database, WAL, journal and temporary space. A full rewrite requires a
separate space budget and cannot run as hidden cleanup.
