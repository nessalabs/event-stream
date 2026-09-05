# Replication implementation contract

Status: implementation in progress behind the optional `replication` feature.
Memory covers origin and destination batches, snapshot-plus-suffix bootstrap,
finite published-reader leases, bounded cleanup, and an owned application
driver. SQLite covers the base batch and restart path. SQLite bootstrap,
broader fault schedules, graceful retry qualification, and resource evidence
remain open. [ADR 0008](adr.md) remains incomplete.

## Local writes finish locally

A local commit receipt says nothing about remote storage. The replication
worker reads committed history after that commit. A disconnected replica may
hold a bounded amount of history locally. When that capacity is exhausted, the
application receives an explicit capacity error or explicitly detaches the
replica. The library never silently deletes history required by an attached
replica.

Use one authoritative origin for each stream lifetime. A replica cannot append
its own events into that origin's history. Network endpoints, authentication,
credentials and authorization remain application responsibilities. The library
accepts an injected transport after the application establishes that boundary.

## Preserve identity at the destination

A replica position contains the origin store identity and the complete stream
key. A stream name alone is insufficient. Resetting a stream creates another
incarnation. Restoring an origin into a new independent store creates another
origin identity.

The destination stores origin cursors as origin cursors. It does not submit
replicated records through ordinary local append, which would allocate new
cursors and obscure gaps. Applications read replicated history through a
separate replica API. Reading that history never executes recorded commands.

Illustrative typed boundary; implementation must retain these distinctions:

```rust
struct OriginId([u8; 16]);
struct ReplicaId(Box<str>);        // 1..=256 UTF-8 bytes
struct DestinationEpoch([u8; 16]); // one destination storage lifetime
struct OriginStream { origin: OriginId, stream: StreamKey }
struct ReplicaPosition { stream: OriginStream, offset: u64 }
struct BatchId([u8; 16]);
struct ReplicaBatch {
    id: BatchId,
    destination_epoch: DestinationEpoch,
    after: ReplicaPosition,
    records: Vec<Arc<Record>>,    // bounded by record count and retained bytes
}
struct ReplicaReceipt {
    batch: BatchId,
    destination_epoch: DestinationEpoch,
    committed_through: ReplicaPosition,
}
```

For `after = 12`, a batch of three records must contain cursors 13, 14 and 15.
Every record must have the same origin stream lifetime and supported cursor
version. Checked integer arithmetic rejects overflow. Empty batches do not
advance progress. A gap, wrong lifetime or conflicting record fails explicitly.
The same event ID in different retry generations may represent separate
records. Replication compares committed cursor and complete record content;
it must not deduplicate by event ID alone.

## Account for retained capacity, not only record count

A received batch can have a small record count but a large allocated vector.
For example, a caller can reserve space for one million record pointers and
then submit one record. Keeping that vector would retain the whole allocation.
Normalize accepted batches into bounded storage or reject excess capacity before
retaining them. Sharing record payloads with `Arc` avoids copying payload bytes;
it does not remove the pointer vector or separately owned identifiers.

Receipt limits include the request identity needed for exact comparison. If an
adapter retains a batch with its receipt, account for that vector and all owned
metadata. A fixed receipt header charge alone is insufficient. Receipt-floor
operation receipts and per-stream floor entries also need count and byte limits.
A no-op floor request for an unknown stream must not create unlimited empty
metadata entries.

The destination enforces its own read-page ceiling. A caller cannot bypass it
by requesting a page larger than the configured maximum. Return bounded pages
that make progress or an explicit capacity error when even one record cannot
fit. Validate every cursor version and exact stream lifetime on input.

A record at `u64::MAX` is a valid last position if its preceding cursor is
`u64::MAX - 1`. Validate the position of each actual record with checked
arithmetic. Do not require an additional nonexistent next record to fit. Any
attempt to append beyond that last position fails before mutation.

## Commit one bounded batch atomically

The destination validates and commits a complete batch in one transaction.
It returns a receipt only after the configured durable commit boundary. A
connection ending partway through a batch does not commit a partial prefix.
This keeps reconciliation simple and gives the batch a finite rollback cost.

The destination stores the exact batch identity, request identity and result
with the records. Retrying a batch with identical content returns its original
receipt. Reusing its ID with different content fails. The request identity
covers origin, lifetime, start, every record and destination epoch. A digest
can reject a mismatch quickly, but equality must not silently omit fields.

The origin persists its acknowledged progress only after validating the remote
receipt. The receipt must match the outstanding batch ID, destination epoch,
origin lifetime and expected last cursor. A higher number alone is not proof.
There is at most one unacknowledged batch per replica/origin-stream pair in the
initial implementation. Bounded concurrency across pairs provides parallelism.

```text
origin tail=15, saved replica progress=12

read 13..15 -> send batch B -> destination commits 13..15 + receipt B
                                      |
                             acknowledgement lost
                                      |
retry exact B -> original receipt B -> save origin progress=15
```

The origin owns the pending batch until it is reconciled or the replica is
explicitly detached. Caller cancellation cannot release the byte budget while
transport work still owns those bytes. Retries use a bounded schedule with a
capped delay, rather than a busy loop. Exhausting a per-drive work limit yields
control; it does not forget the pending identity.

## Detect destination replacement

A destination epoch identifies one durable storage lifetime. Reopening the same
healthy database preserves its epoch. Replacing or restoring a destination must
assign a new epoch before it accepts replication. The origin rejects a receipt
from a different epoch and enters `NeedsBootstrap`.

An unexpected missing remote prefix within the same epoch is an integrity
failure. Do not silently lower acknowledged progress and continue. The operator
must explicitly rebootstrap or replace the destination. An application that
copies database files behind the library's ownership boundary cannot expect the
library to detect every undetectable rollback; supported restore must rotate
the destination epoch.

## Bootstrap with a snapshot and suffix

When the origin no longer retains the required prefix, replay from zero is
impossible. Bootstrap acquires a finite recovery lease for a published snapshot
and captures a suffix tail. Snapshot schema, digest, origin identity and covered
cursor travel together. The destination stores opaque snapshot bytes; it does
not interpret the application schema.

Transfer content in bounded chunks. Stage it under a fresh bootstrap identity.
Verify its exact length and digest. Then copy the contiguous suffix through the
captured tail. Publish the new replica state only after all required content
and records are durable. Existing readable replica state remains intact until
that publication. Cancellation or an expired lease leaves bounded staging that
can be explicitly aborted and cleaned in bounded turns.

A bootstrap receipt names the destination epoch, bootstrap identity, snapshot
identity and final origin cursor. The origin validates it before replacing
its saved replica progress. If the recovery lease expires before required
reads finish, restart bootstrap explicitly. Never combine unrelated snapshot
and suffix lifetimes.

## Retention is coordinated under the same owner

Attaching a required replica creates a durable replay protection at its saved
cursor. Advancing replica progress moves that protection atomically with the
saved receipt. ADR 0006 floor advancement checks these protections while it
holds the store's existing coordination boundary. A separate background check
would allow deletion to race acknowledgement.

Each required replica has a maximum unacknowledged byte backlog and a maximum
age. Reaching a limit restricts new local writes that would increase its
backlog. It does not stop sending already committed records: draining that
backlog is how the system recovers. Reads, acknowledgements and explicit detach
must still have bounded opportunities to run under overload.

Check the proposed record's charged bytes against every affected required
replica before committing it. Commit the record and backlog accounting together.
Checking a counter before entering the commit transaction leaves a race between
concurrent writers. Deduplicated retries add no backlog. A successful remote
acknowledgement subtracts only the bytes in the newly acknowledged prefix; an
exact acknowledgement retry subtracts nothing twice. Counts and byte totals use
checked integers. Never estimate backlog as offset distance times average size.

The application can keep local admission restricted or issue a named,
idempotent detach operation. Detach removes the protection atomically and records
`NeedsBootstrap`; reconnect cannot treat the old cursor as still replayable.
The origin persists the timestamp of the oldest record in each nonempty
backlog. Store construction receives an owner clock that produces durable
millisecond timestamps. A new observation lower than the last persisted value
returns `ClockRollback`; it cannot extend the age limit. The Memory adapter uses
the process wall clock because it has no restart guarantee. SQLite persists the
last accepted observation and reconstructs the same age after restart.

`FromBeginning` atomically counts every retained record after offset zero. It
fails with `NeedsBootstrap` if the local retention floor is already above zero,
and it fails with `BacklogExceeded` if the existing prefix does not fit the
declared replica backlog. `NeedsBootstrap` creates a detached attachment and no
replay claim. It can become `Required` only through the explicit origin
bootstrap transition below.

Bound receipt retention too. Before deleting receipts for acknowledged batches,
the destination publishes a receipt floor. A retry below that floor returns
`ReceiptExpired`; it must not be accepted as a new batch. The origin needs only
one outstanding batch per pair, so it can explicitly confirm completed batches
before that floor advances. Receipt cleanup is a separate bounded operation.

## Bootstrap is staged and verified in bounded steps

The destination accepts an exact `ReplicaBootstrap` request, snapshot chunks,
and contiguous `ReplicaBootstrapBatch` suffixes under one `BootstrapId`.
`verify_replica_bootstrap_step` limits chunks, records, and bytes per call. It
returns progress and never scans the whole staging area in one call. A separate
`publish_replica_bootstrap` operation atomically replaces the readable replica
state after verification completes. Exact retries return the original receipt;
an operation ID or bootstrap ID reused with different content fails.

The destination exposes the published bootstrap descriptor and bounded reads
of its opaque snapshot bytes. A reader holds a lease/version identity while it
pages through bytes, so concurrent replacement and cleanup cannot mix two
published snapshots. This lease type and its bounded lifetime are part of the
remaining bootstrap implementation slice.

Before transfer, `begin_origin_bootstrap` validates that the named snapshot is
actually published in the origin store. It binds its exact `BootstrapId`,
descriptor, destination epoch, and captured suffix tail while installing the
required retention and recovery protection. The caller cannot supply a made-up
descriptor. `acknowledge_origin_bootstrap` accepts only the exact matching
destination receipt, then advances the attachment to `Required`. Explicit
abort or detach releases the bounded protection. A lost begin or acknowledge
response is reconciled with the same operation identity.

## Ports and ownership

Use separate application ports for local replica progress, remote transport and
destination persistence. The transport moves bounded typed requests and returns
bounded typed responses. It does not own stream ordering or storage transactions.
The destination store port owns atomic records-plus-receipt publication. Both
Memory and SQLite implement the same destination contract. SQLite work runs
under its existing owner and bounded worker queue.

The optional replication feature depends on the snapshot and retention
contracts. Feature-disabled builds allocate no replica state, worker or timer.
The worker has finite limits for pairs, active transfers, queued/waiting bytes,
batch records/bytes, snapshot chunks, retained receipts and staging. Configuration
must fit one maximum record or fail at construction. Every overflow is checked.

Staging limits apply to the entire destination store, across all bootstrap
uploads. Giving each upload the full store budget would allow simultaneous
uploads to multiply that budget. Count both staged rows and their charged
bytes before admitting a batch. An exact batch retry adds no charge.

SQLite keeps these totals in the same database as the staged records. Insertion
and the counter update commit together. Aborting an upload does not free its
physical records, so those records keep consuming capacity until bounded
cleanup deletes them. Publication moves the suffix into canonical replica
history and removes its redundant staging records in the same transaction.
The canonical history then consumes the separate history budget. Publication
receipts remain available for exact retries. Startup and controlled restore
must verify or rebuild totals from actual retained rows; they must not trust a
counter merely because it is present.

## Required evidence

- Shared Memory and SQLite tests prove exact retries, content conflicts, gaps,
  cursor overflow, wrong origin/lifetime/epoch and atomic batch rejection.
- SQLite process termination before and after records-plus-receipt commit proves
  restart reconciliation. Transport fault tests separately cover truncated
  requests, lost replies, duplicate replies and invalid remote receipts.
- Integration tests disconnect the destination while local appends continue,
  fill the configured backlog, detach explicitly, advance retention, reconnect,
  bootstrap and compare snapshot plus suffix with the authoritative origin.
- Destination replacement changes epoch and requires bootstrap. A deliberately
  missing prefix under an unchanged epoch fails rather than silently rewinding.
- Repeated resource runs report foreground append latency, drain throughput,
  bytes transferred, CPU, peak memory and cleanup work under fixed limits.
  Compare against an otherwise identical replication-disabled control.
- Native and headless scenarios invoke the same production coordinator and real
  isolated SQLite adapters. Fault transports are labeled as injected failures.

Numeric resource gates must be published before qualification. Passing only the
transport mock or an in-memory example does not complete this phase.

## Overload sequence to implement and verify

This illustrative fixture uses charged record bytes, including the selected
metadata accounting. It is not a throughput target.

```text
required replica R: backlog limit = 600 bytes
A commits, charge 200  -> backlog 200
B commits, charge 300  -> backlog 500
C offered, charge 200  -> rejected; tail and backlog remain unchanged
R acknowledges A      -> backlog 300
C retried, charge 200  -> commits; backlog 500
R repeats ack for A   -> backlog remains 500
```

Run the sequence with concurrent callers too. At no point may accepted work
exceed the byte limit. Then lose the acknowledgement reply, restart both stores,
and reconcile the same pending batch. The exact receipt must advance progress
once. Verify that a full local write queue cannot prevent that acknowledgement
from releasing backlog capacity. A cancelled sender must retain its charged
buffers until the actual transport operation has stopped or transferred
ownership to another bounded task.

## Work must follow the affected stream and batch

A write to one stream must not scan every replica in the store. Keep an index
from stream lifetime to its attached replicas. Append work may grow with the
number of replicas attached to that stream. Adding unrelated streams must not
add work to its commit path. Maintain pending-batch totals directly instead of
recounting every attachment on each prepare.

Acknowledging a batch must not scan the entire remaining backlog. Subtract the
exact record count and charged bytes of the acknowledged batch. Compute and
validate the new counters before changing any state. Repeatedly draining a
history in fixed-size batches should visit each acknowledged record a bounded
number of times, not revisit the remaining history after every batch.

The oldest backlog timestamp belongs to the oldest record that still needs an
acknowledgement. For example, A committed at time 100 and B at time 200. After A
is acknowledged, B's age starts at 200. Keeping 100 would reject new writes too
early; replacing it with the acknowledgement time would let stale data appear
fresh. Preserve enough bounded metadata to recover the correct value across
partial acknowledgements and SQLite restarts.

A receipt may keep a record alive after history cleanup removes it. Charge that
retained payload until the last store-owned reference is released. Sharing an
`Arc` avoids a copy, but freeing a pending-batch counter does not free bytes still
owned by a prepare receipt. Count separately retained identifier and vector
allocations as well. Check a receipt's proposed count, including the insertion,
against the configured limit before changing its floor or storing its result.

Closing an owner rejects new mutations and exact receipt retries through its
closed handle. The persisted receipt can be reconciled after a supported reopen.
Receipt lookup must not bypass this lifecycle check.

## Backlog age and retention example

Use an injected clock for this scenario. No sleep is needed. The replica's
maximum backlog age is 150 milliseconds.

```text
time 100: A commits at cursor 1
time 200: B commits at cursor 2
          floor cannot advance past 0: the replica still needs A and B

time 300: acknowledge A
          oldest backlog timestamp becomes 200, because B remains
          floor may advance to 1, but not to 2
          C can commit: B is only 100 milliseconds old

time 351: a new write is rejected: B is now 151 milliseconds old
          tail stays at 3; failed admission creates no record
          explicit detach releases the replica's retention protection
```

An exact acknowledgement retry must leave the same counters and timestamp.
Attaching to existing history also preserves original record timestamps. A
record committed at time 100 does not become newly committed when a replica
attaches at time 200. This distinction makes age limits consistent before and
after attachment and restart.

## Pending batches stay unchanged while new records arrive

Preparing a batch captures a fixed result. Later local writes must not change
that result, even when the configured batch limit had unused space.

```text
local history: A at 1
prepare B, limit 2 records -> B contains only A; saved through = 1
local commit: C at 2
retry prepare B           -> still only A; same original receipt
ack B with through = 2    -> reject; B never carried C
ack B with through = 1    -> accept; C remains unacknowledged
```

Persist the prepared request, its fixed extent and the original result before
returning. A retry must compare the complete request and return that result.
It must not re-read up to the current tail and rebuild a larger batch under the
same identity. The same rule applies after close and reopen. Current replica
status is available through the status API; it must not replace the historical
status contained in an earlier operation receipt.

A receipt's destination epoch and complete origin stream must match too. An
unchanged batch ID and a plausible offset cannot compensate for a different
epoch or stream. Validate these fields on both first acknowledgement and exact
operation retries.

## Missing durable identity is a failure

A populated replication store with missing identity metadata is corrupt.
Opening it must not silently generate a replacement origin or destination
epoch. That would leave old records and pending operations attached to an
identity the owner no longer recognizes. Initialize a new identity only when
creating a demonstrably new replication schema. Validate partial schemas and
missing singleton rows before accepting work. A supported controlled restore
is a separate explicit operation that rotates identities and transforms state
according to its contract.

## Verification limits apply to each call

Verification remembers total progress, but its work limit starts again on each
call. With three suffix records and `max_records = 1`, three calls must verify
one record each. Comparing the total number already verified with the per-call
limit would stop permanently after the first record. Track total progress and
this call's work separately. Apply the same distinction to chunks and bytes.

A bounded test uses three snapshot chunks and three suffix records. It checks
that every successful incomplete call advances progress, no call exceeds its
limits, and the published descriptor remains absent until explicit publication.
Read back snapshot pages at sizes that cross the original chunk boundaries.
This checks the stored bytes rather than merely checking the reported digest.

## A snapshot-only replica still has a tail

A snapshot covering cursor 20 with an empty suffix represents complete history
through cursor 20. It does not represent an empty stream at cursor zero.

```text
published snapshot: covers 20
suffix records:     none
read after 20:      empty, complete, next = 20
next replica batch: record 21, after = 20
```

Derive the destination's readable tail from its published recovery boundary and
subsequent records. The largest stored record key alone cannot describe a
snapshot-only state. Both reads and subsequent batch validation use the same
boundary. Replacement and cleanup must preserve that boundary even when they
remove every older record row.

## Writes arriving during bootstrap remain pending

Bootstrap transfers a fixed recovery point. New local commits can arrive while
that transfer is in progress. They belong to the replica's backlog too.

```text
snapshot covers A at 1; captured transfer tail is B at 2
begin bootstrap -> protect everything needed after 1
local C commits at 3 while transfer runs
transfer snapshot + B; destination publishes through 2
acknowledge bootstrap -> saved progress 2, backlog still contains C
normal batch transfers C -> saved progress 3, backlog empty
```

Treat `Bootstrapping` as a protected attachment for local write admission,
backlog accounting, and retention. Counting only `Required` attachments would
lose track of C during the transfer. Acknowledging bootstrap subtracts only its
captured suffix. It must not clear the whole current backlog.

Integration evidence must transfer actual stored bytes and records through the
ports, obtain a real destination publication receipt, and reconcile it at the
origin. A hand-constructed receipt is useful for rejection tests but cannot
prove that the destination stored the recovery state. The source bootstrap's
own finite protection and the destination's published read lease are distinct
lifetimes. A temporary reader lease in a test does not prove source-bootstrap
lease expiry or cancellation behavior.

## Readers keep one published version

A published read lease names one bootstrap version. Publishing a replacement
changes what new readers acquire. It must not change the bytes an existing
reader receives halfway through its page sequence.

```text
time 100: publish old-AAAA; reader R acquires a lease expiring at 200
          R reads old-
time 110: publish new-BBBB; new reader S sees the replacement
          R still reads AAAA, never BBBB
time 200: R's next read fails with ReadLeaseExpired
```

Expiry uses an exclusive deadline: at the expiry timestamp the lease is no
longer valid. Releasing the same lease twice returns an explicit already-released
result. A rollback in the injected clock must not make an expired lease appear
valid again.

Expired leases consume memory until the store removes their metadata. A caller
may disappear without releasing or reading its lease again. Bounded cleanup
must reclaim those abandoned slots. Do not rely on a later read of the expired
handle to release capacity. Keep expiration work bounded and report remaining
cleanup honestly. Avoid scanning every live lease on each new read acquisition;
an ordered expiration index can identify the next eligible entry directly.

## Replacement and abort release content in bounded pieces

Publishing a replacement must not retain every old snapshot forever. Keep an
old version while a valid reader lease needs it. After its last reader releases
or expires, queue its chunks and suffix records for bounded cleanup. Preserve
the operation receipt according to the explicit receipt-retention contract;
content cleanup and identity expiry are separate decisions.

A small-capacity regression publishes 8 bytes, acquires an old reader, then
publishes another 8 bytes under a 16-byte staging budget. The old reader still
sees its original bytes. After release and cleanup, a third 8-byte publication
must fit. Unlimited default limits would hide this leak.

An aborted upload may be larger than one cleanup call's byte budget. Do not
require the whole upload to fit before deleting anything. For a 12 KiB upload
stored as three 4 KiB chunks, a call allowing one row and 8 KiB can delete one
chunk. Later calls finish the remaining chunks and eligible metadata. Each
successful call either makes bounded progress or explains why the next
indivisible item cannot fit. A forever-pending upload is not bounded cleanup.

Count each removed chunk or record against the row budget. Report its actual
charged bytes. A single bootstrap entry is not one unit of work if deleting it
also drops thousands of chunks and records. Use checked counter updates so
accounting corruption fails explicitly instead of being hidden by saturating
subtraction. Removing payload must not allow a previously aborted or published
operation identity to silently become a new staging attempt.

## Origin protection is checked before bootstrap acknowledgement

The origin's begin receipt reports the exclusive protection deadline. A new
acknowledgement at or after that deadline must fail explicitly, even if its
batch identity, snapshot and captured tail all match. Check the live protection
inside the same ownership boundary that changes the replica's progress. An
expired attempt must not become `Required` through a late response.

An already committed acknowledgement remains an exact operation retry after
expiry. It returns the original result because the transition happened while
valid. This differs from a first acknowledgement arriving too late. Keep these
two cases distinct in storage and tests. Losing a lease is not permission to
silently renew the old attempt; starting again uses the explicit bootstrap
transition and a fresh attempt identity.

## The application driver owns admitted work

`ReplicationDriver::replicate_once` prepares one bounded batch, sends it through
an injected `ReplicaTransport`, validates the exact remote receipt, and saves
the origin acknowledgement. A driver clone shares its concurrency and byte
limits. Requests that cannot fit are rejected immediately. The initial driver
does not create an unbounded queue of waiting callers.

After admission, a store-owned task keeps the permits until the transport and
origin acknowledgement finish. Cancelling the caller only stops waiting for
the result. It does not release capacity while the task still holds the batch.
A different request must still observe overload until that work completes.
Closing the driver rejects new admissions. Complete shutdown/drain and timeout
behavior remain separate requirements for the driver implementation.

The byte reservation includes the selected payload budget and retained pointer
vectors, identifiers and task metadata. Multiple `Arc` references share record
payloads, but cloned vectors and owned request fields still require capacity.
Check that the configured maximum request can fit the total charge before
launching transport work.

An injected transport test commits to an actual Memory destination and then
returns an intentionally lost reply. Retrying the same operation must find one
record and advance origin progress once. Another transport returns a wrong
cursor after a real commit; the driver must reject that receipt without
advancing the origin. These tests exercise production coordination with fault
injection. They do not qualify network servers, authentication or SQLite crash
recovery. The bootstrap driver and bounded retry schedule remain open.

## Closing and retry deadlines preserve ownership

Register an admitted drive in the active-work count before the final closed
check. Otherwise shutdown can observe zero work and return while another thread
is between its last open check and task registration. The closed flag and active
count use a shared sequential ordering for this handshake. If shutdown wins,
registration is undone and no transport work starts. If admission wins,
`wait_closed` waits for that task to finish.

`replicate_with_retry` retries only the selected uncertain or storage failures.
It keeps the same prepare and acknowledgement identities, caps attempts and
backoff, and applies an overall deadline. An invalid remote receipt is a failure,
not a reason to keep retrying blindly.

A deadline stops the caller's wait. It does not declare a transport commit to
have failed or discard an admitted task's permits. `wait_closed` remains pending
until that task completes. A test blocks the transport, reaches the deadline,
verifies shutdown is still waiting, then releases transport and observes the
exact origin acknowledgement before shutdown returns.
