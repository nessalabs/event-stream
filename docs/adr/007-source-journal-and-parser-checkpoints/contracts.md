# Source journal and parser-checkpoint contract

Status: typed boundary, Memory/runtime parser path and initial SQLite journal
and restore checks implemented. Crash, corruption and resource qualification
remain in progress. [ADR 0007](adr.md)
remains proposed and depends on the retention boundary from ADR 0006.

## Capture bytes before acknowledging the source

This optional path stores raw source bytes before the application advances its
external source position. It protects only bytes that reached the journal's
durable capture boundary. It cannot recover bytes that were read but never
submitted, and it cannot make an unavailable external source replayable.

A source name is reusable, so it is not an identity by itself. Each opened
source lifetime has a fresh incarnation:

```rust
pub struct SourceId(Box<str>);             // 1..=256 UTF-8 bytes
pub struct SourceIncarnation(pub [u8; 16]);
pub struct SourceKey {
    pub id: SourceId,
    pub incarnation: SourceIncarnation,
}
pub struct SourcePosition {
    pub source: SourceKey,
    pub offset: u64,                       // boundary between bytes
}
pub struct RawSegment {
    pub start: SourcePosition,
    pub bytes: Payload,
}
```

`RawSegment` covers `[start.offset, start.offset + bytes.len())`. The end uses
checked `u64` addition. Empty segments are rejected. The first segment starts at
zero. Later segments start at the previous end. While captured bytes remain, an
exact retry compares every byte. After cleanup, the bounded receipt compares the
submitted SHA-256 digest and length. This is a cryptographic content-identity
check, not a retained byte-for-byte comparison. Receipts have explicit count
and byte quotas and are never silently evicted. The application explicitly
advances a receipt floor after its source acknowledgement is stable. A retry
below that floor returns `CaptureReceiptExpired`. A different segment at the
same position, a gap, overlap, overflow or different source incarnation is a
conflict.

The caller may acknowledge the external source only after `capture_segment`
returns its durable receipt. If that acknowledgement is lost, resubmitting the
same bytes is an exact retry. A rotated file, socket session or export uses a new
`SourceIncarnation`; it cannot continue an earlier byte position accidentally.

## Bind a source lifetime once

The selected begin request contains a store-wide operation ID, the exact source
key, parser ID/version and output stream key. Its receipt preserves that request
and the progress returned when it first committed. An exact operation retry
returns that receipt. Rebinding the same source lifetime to another parser or
output lifetime fails explicitly. A fresh incarnation starts a different source
lifetime; a reused name alone does not identify its history.

The existing decoder position's `source_byte` identifies a frame start. It is
not the consumed end of that frame. Several items from one frame may have the
same source byte and distinct contiguous item indexes. Checkpoint publication
uses its explicit consumed-byte boundary and item prefix; it must not infer
frame completion from a marker's start position. The deterministic decoder
remains responsible for the declared complete output count.

## Parser identity and checkpoint

Parser state is meaningful only to one named and versioned implementation. The
first successful `begin_source` also binds the source lifetime to this parser and
one exact output stream lifetime. Replacing either requires a new source
incarnation:

```rust
pub struct ParserId(Box<str>);             // 1..=256 UTF-8 bytes
pub struct ParserRef { pub id: ParserId, pub version: u32 }

pub struct ParserCheckpoint {
    pub source: SourcePosition,             // all bytes strictly before this boundary
    pub parser: ParserRef,
    pub state: Payload,                     // bounded opaque decoder state
    pub next_item_index: u64,
    pub output_stream: StreamKey,
    pub committed_output: Option<Cursor>,
}
```

`committed_output` is the latest output cursor covered by the checkpoint. `None`
means the checkpoint covers no output record. The output stream is exact,
including its incarnation. Resetting that stream makes an old checkpoint stale.

Checkpoint-capable decoders extend the current incremental decoder contract:

```rust
pub trait CheckpointDecoder: IncrementalDecoder {
    fn parser(&self) -> ParserRef;
    fn checkpoint_state(&self, max_bytes: usize) -> Result<Payload, CheckpointError>;
    fn restore_state(
        &mut self,
        parser: &ParserRef,
        state: &[u8],
        max_work_units: usize,
    ) -> Result<(), CheckpointError>;
}
```

The method must reject a different parser ID or version. State creation and
restore have explicit byte/work limits. A decoder reports its retained-state
capacity through the existing capability boundary. A checkpoint is not a Rust
object dump and contains no pointer, task, timer or process-local handle.

## Tie output to captured input

One decoded item may be appended at a time. The optional journal store combines
that append with a durable output marker in one adapter transaction:

```rust
pub struct JournaledOutput {
    pub source: SourceKey,
    pub position: DecodedPosition,
    pub event: NewEvent,
}

#[async_trait]
pub trait SourceJournalStore: RetentionStore {
    async fn begin_source(&self, request: BeginSource)
        -> JournalResult<BeginSourceReceipt>;
    async fn capture_segment(&self, segment: RawSegment)
        -> JournalResult<CaptureReceipt>;
    async fn advance_capture_receipt_floor(
        &self, request: AdvanceCaptureReceiptFloor,
    ) -> JournalResult<AdvanceCaptureReceiptFloorReceipt>;
    async fn append_captured(
        &self,
        output_stream: &StreamKey,
        output: JournaledOutput,
    ) -> JournalResult<AppendReceipt>;
    async fn publish_parser_checkpoint(&self, checkpoint: ParserCheckpoint)
        -> JournalResult<CheckpointReceipt>;
    async fn source_status(&self, source: &SourceKey)
        -> JournalResult<SourceProgress>;
    async fn read_captured(
        &self,
        source: &SourceKey,
        offset: u64,
        limits: RawPageLimits,
    ) -> JournalResult<RawPage>;
    async fn latest_checkpoint(&self, source: &SourceKey)
        -> JournalResult<Option<ParserCheckpoint>>;
    async fn cleanup_captured(&self, limits: JournalCleanupLimits)
        -> JournalResult<JournalCleanupProgress>;
}
```

The caller does not select a retry generation. For an existing output marker,
the adapter retries the generation stored in that marker. For a new item, the
same transaction reads the output stream's current retry generation and stores
it in the new marker. Generation rotation between output commit and checkpoint
publication therefore cannot change the identity used during recovery.

`append_captured` uses the generation-bearing exact retry rule finalized by ADR
0006. Its transaction
also records `(source incarnation, source byte, item index, output stream,
retry generation, event ID, committed cursor)`. A crash after this transaction may lose the caller
acknowledgement, but replay submits the same stable event ID and obtains the same
cursor. The marker cannot claim an event that was not committed.

`next_item_index` is the first item index after the checkpoint. Marker item
indexes start at zero and must be contiguous. `append_captured` accepts only the
next index or an exact retry of an existing index. `publish_parser_checkpoint`
checks that every index from the preceding checkpoint's `next_item_index`
through the new value, exclusive, has one committed marker. It also checks that
`committed_output` matches the last marker, or is unchanged when the range is
empty. It then publishes the checkpoint in one short transaction. It does not
append events. No atomic multi-stream batch is assumed; each output transaction
is independently recoverable and the checkpoint moves only after the complete
declared marker prefix exists.

`DecodedPosition.source_byte` is the decoded frame start. Several items from one
frame may share it. It is not the consumed end of that frame. A marker must be
within captured data. The checkpoint carries the decoder-reported consumed byte
boundary. Tests include multiple outputs at one frame start and a partial final
frame.

The store cannot prove that an opaque decoder should have emitted another item.
It trusts the deterministic decoder checkpoint to state the consumed source
boundary and the next item index. Verification proves that the declared
contiguous output prefix was committed. Tests must replay the same captured
bytes with the same parser version and compare the emitted positions; they must
not describe this application-level check as a fact inferred by the store.

The event mapper must derive stable IDs from the source key plus
`DecodedPosition`. Existing `DecodedPosition` counters remain checked for
overflow. A mapper used on this path is deterministic and has no terminal,
network, tool or other external side effect. External actions consume committed
events through their own idempotency boundary; journal replay never calls them
directly.

## Recover the original retry generation

An output's retry generation is part of its identity. The generation currently
accepting new work may change while a source still has uncheckpointed output.
Recovery must not substitute the new generation for an already committed item.

```text
item0 commits in generation1; checkpoint has not moved
application advances the output stream to generation2
process restarts and replays item0
required: resolve item0's durable marker and retry generation1
wrong:    submit item0 in generation2 and conflict or create another record
```

The store chooses the generation inside the event-plus-marker transaction.
For an existing marker, it uses that marker's stored generation and verifies
its exact event and committed cursor. For a new marker, it uses the output
stream's current retry generation. `JournaledOutput` supplies the source,
decoded position and event; the driver does not supply a generation.

This lets a long-lived source continue producing new output after the stream
advances generations. An old uncheckpointed item still resolves its original
record. Selection and append occur under the same store owner, so a concurrent
generation advance cannot split that decision from its commit.

Checkpoint publication and bounded marker cleanup release old generation
protection according to the existing rules. Do not expire a generation merely
because the application has selected a newer one for future output. A retained
marker with missing committed output is corruption; retry must not recreate
the record under a new cursor. Rotation, missing-output corruption and restart
remain explicit adapter test requirements.

## State table and crash recovery

```text
captured end C, checkpoint P, committed marker/output M

capture bytes       durable C advances; source may now be acknowledged
decode item         no durable progress
append_captured     M advances atomically with the event
publish checkpoint P advances only through the complete marker prefix
cleanup raw bytes   removes bytes strictly before P under row/byte limits
```

Recovery loads the latest compatible checkpoint. If none exists, it starts at
byte zero with an initial decoder. It reads captured bytes from `P.source`
forward in bounded pages. It decodes again and calls `append_captured` with the
same IDs. Already committed output deduplicates. Uncommitted output inserts.
Only then can a new checkpoint advance.

Crash cases have one result:

| Crash point | Recovery action |
| --- | --- |
| Before capture commit | Source must resend; journal makes no claim |
| After capture, before source acknowledgement | Exact capture retry deduplicates |
| After decode, before output commit | Decode captured bytes again |
| After output commit, before its acknowledgement | Stable event retry returns the same cursor |
| Between items from one frame | Earlier markers deduplicate; remaining items insert |
| After all outputs, before checkpoint | Replay deduplicates every output, then publishes checkpoint |
| After checkpoint, before cleanup | Resume at checkpoint; old bytes remain safe to remove |
| During cleanup | Committed checkpoint and remaining raw suffix stay authoritative |

A malformed suffix is retained with its captured position. Replaying it returns
the same bounded parser failure. The service does not repeatedly execute a
terminal callback or silently skip the bytes.

## Bounded pages, admission and storage

`RawPageLimits { max_segments, max_bytes }` bounds rows and returned payload
capacity. `RawPage` returns `start`, contiguous bytes, `next_offset` and
`complete`. It may slice a stored segment so a byte limit smaller than that
segment still makes progress. Only a request at captured end returns an empty
complete page.

Cleanup has separate limits for segment rows, marker rows, receipt rows and
bytes. One cleanup call stops when its available budget is exhausted. Its result
reports the removed rows and bytes, plus whether eligible work remains. A
constructor must reject a cleanup budget that cannot remove a permitted row;
otherwise a valid stored row could prevent cleanup from making progress.

Configuration separates stored data, retry receipts and cleanup work. This is
the current public shape; the nested types are defined in
[`source_journal.rs`](../../../src/application/source_journal.rs):

```rust
pub struct SourceJournalStoreConfig {
    pub storage: JournalStorageConfig,
    pub receipts: JournalReceiptConfig,
    pub cleanup: JournalCleanupLimits,
}

pub struct JournalCleanupLimits {
    pub max_segment_rows: usize,
    pub max_marker_rows: usize,
    pub max_receipt_rows: usize,
    pub max_bytes: usize,
}
```

Storage limits bound source count, segment count and bytes, individual segments,
output marker count and bytes, checkpoint count, individual checkpoint state,
and aggregate checkpoint storage. `max_staging_bytes` is the aggregate checkpoint
budget in the Memory adapter. It includes charged metadata as well as state.
A replacement checks the resulting total before changing the old checkpoint:

```text
new total = current total - old checkpoint charge + new checkpoint charge
reject if new total exceeds the budget; keep the old checkpoint unchanged
```

Begin, capture and receipt-floor operations share a finite receipt count and
byte budget. These receipts preserve retry truth. An acknowledgement does not
silently erase one. Capture receipt expiry requires an explicit floor advance;
that operation has its own exact request and receipt.

The runtime separately limits concurrent journal operations, waiting operations,
waiting bytes, in-flight bytes and admission time. The current
`JournalAdmissionConfig` uses one byte budget for in-flight journal work.
Returned pages, fallback retry identities and temporary copies also need
accounting. An operation count alone does not bound memory.

The ingestion composition must additionally bound decoder state and each replay
drive's steps, emitted items and work. That integration remains under construction.
A full raw backlog must pause capture or return an explicit capacity error before
accepting more source bytes. A stalled output store must not allow an unbounded
journal, decoder queue or population of waiting callers.

Unknown capture, output-marker and checkpoint results retain the exact bounded
source key, position and content needed for retry. They remain charged until
resolved. Reusing an operation or segment identity with different bytes fails
explicitly. Cleanup never deletes raw bytes at or after the latest published
checkpoint and never removes unresolved receipts. Cleanup also keeps the retry
generation required by any output marker that is newer than the published
checkpoint. The implementation uses ADR 0006’s typed generation and expiration rules.

The first implementation releases a generation pin during the bounded marker
cleanup turn after checkpoint publication. It does not release the pin in the
checkpoint call. This may delay generation expiry, but it cannot remove retry
truth while a marker is still retained.

## A bounded drive must still make progress

A decoder can consume one frame and retain several outputs internally. A later
step can emit those outputs without consuming another input byte. That is valid
progress. Reject only a step that violates the decoder contract; do not reject
all zero-consumption steps.

Reaching the captured byte tail does not prove that buffered decoder output has
been drained. Continue bounded decoder steps while it reports output ready.
Reaching that tail also does not mean the external source has ended. A partial
frame can remain in serialized parser state until another capture arrives.
Do not call decoder `finish` merely because the current journal page is empty.
Actual source EOF requires an explicit application signal and a recoverable
finalization contract before the library claims restart-safe EOF handling.

A drive limit protects other callers from a large frame. It must not prevent
that frame from ever completing. Consider a frame that emits 100 items and a
per-drive limit of 10 output commits:

```text
incorrect: drive1 commits items0..9; restart from old checkpoint
           drive2 retries items0..9; limit reached again forever
required:  each drive advances resumable parser/output progress
           after finite drives, all100 items commit in order
```

Keep unfinished state in a bounded session, or publish a valid checkpoint that
preserves the remaining decoder state and committed output prefix. A checkpoint
must never skip uncommitted outputs. After cancellation or process restart,
recover from durable captured input and markers using the same deterministic
positions and IDs. Tests must include a single frame larger than one drive's
output allowance, not just many small frames that each fit the allowance.

Bound concurrent recovery sessions too. Each can retain a raw page, decoder
state, decoded items and mapped event while waiting on an append. Per-operation
runtime admission does not account for those buffers between calls. The session
must own its byte reservation for their entire lifetime. Count separately owned
identifier buffers and temporary copies. Sharing payload bytes through `Arc`
does not share every identifier allocation. Release reservations only when work
and retained state have actually stopped or transferred to another bounded owner.

## Restore the journal together with its output history

A backup can contain captured input that has not produced an event yet. Copying
only committed events loses that input. Controlled restore must therefore import
journal sources, raw segments, capture receipts, output markers, parser
checkpoints and operation receipts along with event history. Until that import
is implemented, a journal-bearing backup must fail explicitly. An apparently
successful restore that drops the journal is unacceptable.

Preserve the source key and parser identity during the copy. Deterministic event
IDs may depend on those values. Preserve opaque parser state exactly; the store
cannot rewrite application-owned bytes. Remap each bound output stream and each
committed output cursor through the restore operation's stream-lifetime mapping.
A checkpoint-capable parser must keep runtime output handles and output stream
identity outside its serialized parser state. Otherwise the restored parser
would carry an obsolete handle that the library cannot repair.

For example, an output committed just before a crash must remain an exact retry:

```text
backup: source S, frame 0, item 0 -> output lifetime L, cursor 1
        checkpoint still at byte 0
restore: source S, frame 0, item 0 -> mapped lifetime L2, cursor 1
replay: same source S and item 0 -> same event ID -> original mapped record
```

Rebuild and validate retry-generation protection from retained markers before
the restored store accepts writes or expiry operations. Capture receipts and raw
segments have independent lifetimes. A receipt may already have expired while
its raw bytes still await parsing. The import must preserve those raw bytes and
must not require a matching receipt to find or clean them.

Import uses bounded keyset pages and byte limits, including identifier and
checkpoint metadata. Reject unsupported parser metadata shapes, invalid cursor
references, missing required raw suffixes and inconsistent counters before
publishing the restored store. The application still owns reconnecting the
external source and deciding whether to continue this source lifetime or begin
a fresh one. Restoring stored input does not reopen a process or socket.

## Optional feature and verification ownership

Use a separate optional `source-journal` feature. It depends on `codec` and `retention`, with
hash support for retained capture receipts, but adds no work to
ordinary append or non-journaled ingestion when disabled. Memory and SQLite use
the same shared contract. SQLite stores capture, markers and checkpoints under
the existing exclusive store owner; it does not open a second connection with
an independent lifecycle.

Required tests inject a crash between every state-table row, split one frame
into every input partition, emit several events from one frame, stall output
until backlog admission fails, rotate a source name, change parser versions,
lose source acknowledgements and corrupt bounded stored state. Evidence records
the extra capture writes, synchronization callbacks, CPU, allocation traffic,
RSS, backlog peaks and cleanup work separately from ordinary ingestion. These
are future completion gates, not claims made by this draft.
