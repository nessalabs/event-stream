# Snapshot implementation contract

Status: implementation in progress. The typed domain/application boundary,
runtime admission, and MemoryStore path exist. SQLite, examples, crash tests,
and resource evidence remain incomplete, so ADR 0005 is not verified.

## Save state without collecting it into one large buffer

An application supplies state bytes for a particular committed cursor. The
library stores those bytes in bounded chunks. It cannot prove that the bytes
describe the application's state correctly. Examples and application tests must
prove that relationship.

A published snapshot is immutable. A reader can see it only after every byte
has been stored and its checksum verified. Publication and recovery selection
must use the same store ownership boundary as event history. A second SQLite
connection with an independent lifecycle is not a substitute for that boundary.

```text
application state after record 50
          |
          v
begin descriptor -> chunks [0,64KiB), [64KiB,...)
          -> verify length and SHA-256 -> publish

recover: compatible snapshot at 50 + records 51 ... captured tail
```

The checksum detects changed bytes. It does not prove the state is semantically
correct. Applications choose their snapshot schema and version.

## Domain values and application boundary

The following is the implementation boundary to review before parallel coding.
Use the existing `StreamKey`, `Cursor`, `SchemaRef` and exact-size `Payload`
values. Keep domain values independent of SQLite, Tokio and hash implementations.

```rust
SnapshotId([u8; 16])
SnapshotDigest([u8; 32]) // SHA-256 of the concatenated content bytes

SnapshotDescriptor {
    id: SnapshotId,
    covered: Cursor,
    schema: SchemaRef,
    content_bytes: u64,
    digest: SnapshotDigest,
}

SnapshotChunk {
    offset: u64,          // byte position, not an event cursor
    bytes: Payload,
}

SnapshotUploadProgress {
    descriptor: SnapshotDescriptor,
    accepted_bytes: u64,
    state: Uploading | Verifying | Verified | Published | Aborted,
}

RecoveryLeaseId([u8; 16])
RecoveryPlan {
    lease: RecoveryLeaseId,
    snapshot: SnapshotDescriptor,
    through: Cursor,     // tail captured when protection was acquired
}
```

Use a separate `SnapshotError`. Include invalid input, unsupported capability,
stale incarnation, cursor ahead, missing history, operation conflict, capacity,
incomplete upload, checksum mismatch, expired protection, definite storage
failure, and unknown publication. Unknown outcomes carry the snapshot identity
and descriptor needed for an exact retry. Do not enlarge ordinary append errors
with snapshot data.

An optional `SnapshotStore: EventStore` port owns atomic storage transitions.
Application use cases perform bounded admission and retain ownership after
caller cancellation. Compose them with the runtime's existing owned store;
never open the same database again to construct a snapshot manager.

The port needs these operations, with bounded typed inputs and outputs:

```text
begin_snapshot(descriptor) -> upload progress
put_snapshot_chunk(snapshot identity, chunk) -> upload progress
snapshot_status(snapshot identity) -> upload progress
verify_and_publish_snapshot(snapshot identity) -> published descriptor
abort_snapshot(snapshot identity) -> stable aborted outcome
cleanup_snapshot_staging(record and byte limits) -> cleanup progress
list_snapshots(stream, exclusive position, page limits) -> descriptor page
acquire_recovery(snapshot identity, finite lifetime) -> recovery plan
read_snapshot_chunk(lease, byte offset, byte limit) -> chunk
read_recovery_page(lease, exclusive cursor, page limits) -> event page
release_recovery(lease) -> released outcome
```

Implementation must finalize the exact Rust signatures and constructors before
agents change the domain, runtime and adapters in parallel. The operations above
are required behavior, not permission to implement a different storage model.

### Proposed exact typed boundary

`SnapshotId` is globally scoped within one store. A caller may retry it without
also supplying a stream key. The ID constructor accepts exactly 16 bytes; random
ID generation remains an infrastructure concern. `SnapshotDigest` likewise
accepts exactly 32 bytes and has no hashing dependency in the domain layer.

The concrete domain and application values are:

```rust
pub struct SnapshotId(pub [u8; 16]);
pub struct SnapshotDigest(pub [u8; 32]);

pub struct SnapshotDescriptor {
    pub id: SnapshotId,
    pub covered: Cursor,
    pub schema: SchemaRef,
    pub content_bytes: u64,
    pub digest: SnapshotDigest,
}

pub enum SnapshotUploadState { Uploading, Verifying, Verified, Published, Aborted }
pub struct SnapshotUploadProgress {
    pub descriptor: SnapshotDescriptor,
    pub accepted_bytes: u64,
    pub verified_bytes: u64,
    pub state: SnapshotUploadState,
}
pub struct SnapshotChunk { pub offset: u64, pub bytes: Payload }
pub struct VerificationLimits { pub max_chunks: usize, pub max_bytes: usize }

pub struct SnapshotContinuation {
    pub covered: Cursor,
    pub id: SnapshotId,
}
pub struct SnapshotPage {
    pub entries: Vec<SnapshotDescriptor>,
    pub next_after: Option<SnapshotContinuation>,
    pub complete: bool,
}
pub struct SnapshotUploadPage {
    pub entries: Vec<SnapshotUploadProgress>,
    pub next_after: Option<SnapshotId>,
    pub complete: bool,
}

pub struct SnapshotBytePage {
    pub snapshot: SnapshotId,
    pub offset: u64,
    pub bytes: Payload,
    pub next_offset: u64,
    pub complete: bool,
}

pub struct SnapshotAbortReceipt { pub id: SnapshotId, pub already_aborted: bool }
pub struct SnapshotCleanupProgress {
    pub removed_snapshots: usize,
    pub removed_chunks: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}
pub struct SnapshotCleanupLimits { pub max_rows: usize, pub max_bytes: usize }

pub struct RecoveryLeaseId(pub [u8; 16]);
pub struct RecoveryPlan {
    pub lease: RecoveryLeaseId,
    pub snapshot: SnapshotDescriptor,
    pub through: Cursor,
}
pub enum RecoveryRelease { Released, AlreadyReleased }
```

The continuation contains both the covered cursor and snapshot ID. This matters
when two snapshots cover the same event cursor. Ordering is
`(covered.offset, snapshot_id)` within one exact stream incarnation, and the
continuation is exclusive. A cursor alone would skip or repeat equal-position
snapshots.

The storage port extends the exact event-store owner:

```rust
#[async_trait]
pub trait SnapshotStore: EventStore {
    async fn begin_snapshot(&self, descriptor: SnapshotDescriptor)
        -> SnapshotResult<SnapshotUploadProgress>;
    async fn put_snapshot_chunk(&self, id: SnapshotId, chunk: SnapshotChunk)
        -> SnapshotResult<SnapshotUploadProgress>;
    async fn snapshot_status(&self, id: SnapshotId)
        -> SnapshotResult<SnapshotUploadProgress>;
    async fn verify_snapshot_step(&self, id: SnapshotId, limits: VerificationLimits)
        -> SnapshotResult<SnapshotUploadProgress>;
    async fn publish_snapshot(&self, id: SnapshotId)
        -> SnapshotResult<SnapshotDescriptor>;
    async fn abort_snapshot(&self, id: SnapshotId)
        -> SnapshotResult<SnapshotAbortReceipt>;
    async fn cleanup_snapshot_staging(&self, limits: SnapshotCleanupLimits)
        -> SnapshotResult<SnapshotCleanupProgress>;
    async fn list_snapshot_uploads(
        &self,
        after: Option<SnapshotId>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage>;
    async fn list_snapshots(
        &self,
        stream: &StreamKey,
        after: Option<SnapshotContinuation>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotPage>;
    async fn acquire_recovery(
        &self,
        id: SnapshotId,
        lifetime: Duration,
    ) -> SnapshotResult<RecoveryPlan>;
    async fn read_snapshot_chunk(
        &self,
        lease: RecoveryLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> SnapshotResult<SnapshotBytePage>;
    async fn read_recovery_page(
        &self,
        lease: RecoveryLeaseId,
        after: u64,
        limits: PageLimits,
    ) -> SnapshotResult<Page>;
    async fn release_recovery(&self, lease: RecoveryLeaseId)
        -> SnapshotResult<RecoveryRelease>;
}
```

`verify_snapshot_step` does at most the supplied chunk and byte work. The store
keeps one finite in-memory SHA-256 state for each admitted verification. A
process restart discards that state and resets the next verification attempt to
byte zero. A step may stop within a stored chunk; its verification cursor tracks
both the chunk offset and the byte position inside that chunk. This guarantees
progress when `max_bytes` is smaller than a stored chunk. `Verified` is a
persisted state between bounded verification and the short publication
transaction. `publish_snapshot` rechecks the descriptor,
stream incarnation and floor before changing `Verified` to `Published`.

`Runtime<S>` gains snapshot methods only in an `impl<S: SnapshotStore>` block.
They use the runtime's existing `Arc<S>`, lifecycle coordination and shutdown
ownership. There is no second manager that opens or independently owns the same
database. The public `verify_and_publish_snapshot` use case owns its accepted
operation after caller cancellation, repeatedly calls `verify_snapshot_step`,
yields between steps, and finally calls `publish_snapshot`. Short begin, chunk,
status, list and recovery-page calls use the same bounded store scheduling as
event metadata and reads.

The store owns an injected monotonic clock for its ownership lifetime.
`acquire_recovery` accepts a finite `Duration`; no wall-clock timestamp or
`Instant` enters a domain value or persisted row. Memory tests inject a manual
clock. SQLite production uses the process monotonic clock. Lease IDs from an old
owner are absent after reopen and fail as expired.

Use these finite configuration groups, with every field nonzero and checked for
overflow. `SnapshotStoreConfig` is installed when MemoryStore or SqliteStore is
constructed. The adapter enforces it even when a caller uses `SnapshotStore`
directly. `SnapshotAdmissionConfig` belongs to `Runtime` and bounds accepted
application calls, waiting callers and their retained bytes.

```rust
pub struct SnapshotAdmissionConfig {
    pub max_concurrent: usize,
    pub max_waiters: usize,
    pub max_waiter_bytes: usize,
    pub max_in_flight_chunk_bytes: usize,
    pub admission_timeout: Duration,
}
pub struct SnapshotStoreConfig {
    pub storage: SnapshotStorageConfig,
    pub verification: SnapshotVerificationConfig,
    pub recovery: SnapshotRecoveryConfig,
    pub cleanup: SnapshotCleanupLimits,
}
pub struct SnapshotStorageConfig {
    pub max_snapshots: usize,
    pub max_published_bytes: u64,
    pub max_staging_snapshots: usize,
    pub max_staging_bytes: u64,
    pub max_chunk_bytes: usize,
    pub max_chunks: usize,
    pub max_chunk_metadata_bytes: usize,
    pub max_descriptor_metadata_bytes: usize,
    pub max_receipts: usize,
    pub max_receipt_bytes: usize,
    pub max_list_page: PageLimits,
}
pub struct SnapshotVerificationConfig {
    pub max_active: usize,
    pub max_chunks_per_step: usize,
    pub max_bytes_per_step: usize,
}
pub struct SnapshotRecoveryConfig {
    pub max_leases: usize,
    pub max_lifetime: Duration,
    pub max_chunk_bytes: usize,
    pub max_page: PageLimits,
}
```

`RuntimeConfig` contains `snapshots: SnapshotAdmissionConfig`. Each adapter's
options contain `snapshots: SnapshotStoreConfig`.

Store construction requires `cleanup.max_bytes >= storage.max_chunk_bytes` so one
stored chunk row can always be removed without exceeding a cleanup turn. The row
limit bounds descriptor and chunk deletions separately; one snapshot with many
chunks cannot turn a one-snapshot cleanup request into unbounded work.

`read_snapshot_chunk` accepts any offset through `content_bytes`. For an offset
inside content, it seeks to the indexed stored chunk containing that byte and
returns at most `max_bytes`, continuing across chunk boundaries when space
remains. `next_offset` is the first byte not returned. An offset equal to
`content_bytes` returns an empty complete page. An offset beyond it is invalid.
No other successful read returns an empty incomplete page.

Unknown outcomes are separate variants: `BeginUnknown { descriptor }`,
`ChunkUnknown { id, chunk }`, `AbortUnknown { id }`, and
`PublicationUnknown { descriptor }`. The descriptor and chunk payloads are boxed
on these rare error paths. A chunk error retains the exact bounded bytes so retry
cannot change the accepted content. Other variants are
`InvalidConfig`, `InvalidInput`, `Unsupported`, `NotFound`, `Closed`,
`Overloaded`, `AdmissionTimeout`, `StaleIncarnation`, `CursorAhead`,
`MissingHistory`, `OperationConflict`, `CapacityExceeded`, `IncompleteUpload`,
`ChecksumMismatch`, `ExpiredProtection`, `StorageFailure`, and `CorruptStorage`.
Reset and delete
atomically make staged snapshots for the old incarnation unpublishable. They do
not silently delete the upload receipt or turn a retry into a new snapshot.

## Upload and publication rules

Beginning an upload validates the cursor version and incarnation, and requires
`floor <= covered <= tail`. It reserves the declared content size and descriptor
metadata before accepting chunks. Large declared sizes cannot reserve unlimited
disk or staging capacity. An empty snapshot is valid and has the SHA-256 digest
of empty bytes.

Chunks are contiguous byte ranges. A new chunk starts at `accepted_bytes`.
Reject empty chunks, overflow, gaps and content beyond the declared total.
An exact retry of an accepted chunk compares the stored bytes and returns the
existing progress. A different value at that position is a conflict. Do not
use a digest alone as an exact equality test for chunk retries.

The descriptor cannot change after begin. Repeating begin with the same
descriptor returns its existing state. Reusing the ID with different metadata
is a conflict. Aborted IDs remain identifiable within a finite receipt quota;
exhausting that quota fails explicitly. Do not silently make an old retry into
a new upload after staging cleanup. ADR 0006 must extend this rule when it adds
a retry horizon.

Verification processes chunks incrementally. It checks offsets, exact length
and digest without allocating `content_bytes` in Rust or monopolizing the
SQLite worker for the entire object. No new chunks are accepted while verifying.
Cancellation cannot expose a partly verified object. After process restart,
verification can restart from byte zero; a resumable hash state is not required.

The final short transaction publishes only the same immutable descriptor and
content that were verified. It rechecks stream identity and remaining-history
availability. A reset during upload cannot publish against the replacement
incarnation. An acknowledgement lost after publication resolves through status
or an identical publication retry. Abort racing with publication has one
committed winner. Cleanup never removes a published object.

## Recovery protection and the retention boundary

The application examines a bounded page of descriptors and decides whether it
can restore their schemas. Compatibility code runs outside the storage worker
and database transaction. Acquiring the chosen descriptor then atomically checks
its publication state, incarnation, current floor and tail. A concurrent deletion
can make acquisition fail; the caller selects again.

A recovery plan protects its snapshot and the history strictly after `covered`
through the captured tail. For example, a plan covering 50 through 80 prevents
retention from advancing the floor above 50 while that plan is valid. New writes
may advance the stream tail beyond 80. They do not extend this plan silently.

Protection has a finite deadline and cannot be renewed indefinitely. After it
expires, both snapshot reads and recovery history reads fail explicitly. Check
validity as part of accepting each bounded read, alongside floor validation.
An already accepted page may finish; a later page must recheck. Applications
apply the snapshot and suffix to temporary state and expose that state only
after the whole protected recovery succeeds.

Protection belongs to the current exclusive store owner. A restarted owner
invalidates old lease tokens. Use an injected monotonic clock within that
ownership lifetime; wall-clock jumps must not extend a lease. ADR 0006 must use
the same protected state in its retention transaction. An application-level
check followed by unrelated deletion would leave a race.

## Resource and storage rules

Start with finite configuration groups for upload admission, stored snapshot
bytes/count, staging bytes/count, chunk bytes, metadata receipts, recovery
leases and lease lifetime, and cleanup rows/bytes. Reserve queued and in-flight
chunk bytes too. A snapshot size limit alone does not bound concurrent uploads.

SQLite stores descriptor state and ordered chunks separately. Use a key that
supports seeking directly to a chunk position. Avoid repeated scans from byte
zero for each download page. Store integer byte positions exactly. Keep
published-snapshot lookup indexed by stream incarnation and covered cursor.

Each distinct chunk charges its payload bytes plus a fixed 64-byte logical
row/index envelope. `storage.max_chunks` and
`storage.max_chunk_metadata_bytes` bound tiny-chunk overhead across staged and
published snapshots. An exact retry adds no charge. Abort or cleanup releases
the charge only when the stored chunk row is actually removed. This logical
budget is enforced by both adapters; allocator and SQLite page overhead remain
separate measured costs.

Beginning an upload also reserves its descriptor's logical charge against
`storage.max_descriptor_metadata_bytes`. The descriptor stays charged when an
aborted upload becomes a retry receipt, because the store still retains its
identity and exact metadata.

Cleanup selects only uploads already marked `Aborted`. It never decides that an
active upload or verification is abandoned. On owner startup, an application
uses the bounded `list_snapshot_uploads` page, ordered by `SnapshotId`, then
chooses to resume or explicitly abort each upload. Cleanup reports only bytes
actually released. Moving a descriptor into its retry receipt does not count as
reclaimed memory.

MemoryStore implements the same contract with finite stored-byte quotas.
Its total retained content necessarily grows with accepted snapshot bytes.
The claim is bounded working memory per operation, not constant total storage.
Its current content-page path builds a bounded `Vec<u8>` and then copies into
the returned `Payload`. Runtime read admission therefore charges twice the
requested content-page bytes plus a fixed envelope. This is a conservative
working-memory charge, not an assertion about allocator size or resident memory.
Direct adapter calls remain bounded by `recovery.max_chunk_bytes`; process RSS
and allocator overhead require measurement.

Short chunk operations share bounded scheduling with event work. Snapshot
verification must yield between bounded chunks. Cleanup has explicit progress
and budgets. Neither operation starts one permanent task or thread per snapshot.

Controlled restore must extend its identity-reference audit to snapshot
descriptors and chunks before this feature can be declared supported. Restored
descriptors retain the covered offsets and content bytes but use mapped stream
incarnations. Recovery leases are never copied into a restored database.

## Required verification before completion

- Shared Memory/SQLite contracts cover chunk retry, conflicts, invalid positions,
  empty snapshots, digest mismatch and publication after exact completion.
- An abrupt process termination during staging, verification and publication
  cannot expose partial content. Lost acknowledgement resolves consistently.
- Snapshot plus suffix equals full replay for transcript and workflow examples.
  Both examples run through public APIs and verify state after restart.
- Concurrent reset, retention and recovery acquisition produce a valid protected
  plan or an explicit failure. Clock injection proves exact lease expiration.
- Oversized or malformed stored chunks are rejected before unbounded allocation.
  Cancelled uploads, abandoned leases, disk full and cleanup failures retain
  explicit bounded ownership and retry behavior.
- Repeated size sweeps measure live/peak allocations, RSS, bytes read/written,
  staging and final space, verification time and foreground append latency.
  A large snapshot must not produce a proportional working buffer.

ADRs 0006–0008 depend on these publication and protection rules. Their existence
does not make these rules implemented; the roadmap retains separate evidence
for every required phase through ADR 0010.

## File ownership for implementation

Add an optional `snapshots` Cargo feature. It enables SHA-256 plus the snapshot
domain/application API and MemoryStore implementation. SQLite implements
`SnapshotStore` only when both `sqlite` and `snapshots` are enabled; the existing
`sqlite` feature continues to carry SHA-256 for controlled restore. Default core
builds create no snapshot metadata, hash state, tasks or provider dependencies.
CI covers default, `snapshots`, `sqlite`, and `sqlite,snapshots` on stable and
Rust 1.85.

Land the typed boundary first. One owner changes `domain/snapshot.rs`,
`application/snapshot.rs`, the domain/application module exports, and the
conditional `Runtime<S: SnapshotStore>` methods. That change fixes constructors,
errors, state transitions, grouped defaults, owned cancellation and the runtime
admission rules before adapter work begins. Domain code remains standard-library
only. Application code depends on the port, never a concrete store.

The store clock is part of adapter construction. Memory options and SQLite
options both accept the same small `MonotonicClock` application port, whose
production implementation reads process-monotonic time and whose test
implementation advances manually. The clock returns an owner-relative checked
tick. It has no wall-clock conversion and no persisted absolute value. Direct
`SnapshotStore` calls therefore use the same lease-expiry rule as Runtime calls.

After those signatures compile, one owner implements MemoryStore plus the shared
adapter contract. Those tests define quota charges, exact retry equality,
verification steps, equal-cursor pagination, cleanup progress and manual-clock
lease expiry. The same owner adds reference/property schedules for upload,
abort, reset and recovery races.

A separate owner implements SQLite schema migration, indexed queries,
transactions, restart recovery and real-database contract cases. That owner also
extends controlled restore's reference audit and logical import for descriptors
and chunks. Migration and restore tests must prove old format-2 databases remain
readable and that malformed or oversized rows are rejected before large values
cross the adapter boundary.

The final owner builds application-owned snapshot/recovery examples and wires
the same callbacks into the verification console. The GPUI rows stay blocked
until they invoke real Memory and SQLite paths. Resource and fault evidence uses
separate harness files and never runs on the UI thread.
