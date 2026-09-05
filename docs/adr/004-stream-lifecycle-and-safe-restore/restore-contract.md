# Controlled restore contract

Status: implementation in progress. The application manager and SQLite restore
path pass round-trip, active process-kill, source-lock, ownership and disk-full
checks. Repeated resource measurements and remaining corruption fixtures are
still required; this is not a completed qualification.

A restore must build a complete replacement before an application can use it.
An interrupted copy must never become the active database. Restore is explicit
maintenance. It does not run inside an ordinary append or reopen operation.

## Keep the first workflow simple

The first SQLite workflow uses a closed source database and a new destination.
The caller shuts down its runtime and confirms that the store is closed before
backup starts. Backup acquires the same ownership lock as normal open and retains
it until the backup ends. Another process cannot reopen the source halfway
through copying it. Refuse a source that requires journal recovery; normal open
and close must resolve that state first.

Do not copy a live SQLite file using an ordinary file copy. A consistent online
backup would need a separate SQLite backup protocol and foreground-latency
contract. That extension is not required for this first controlled workflow.

A backup preserves the original bytes and identities as an archival artifact.
Opening a backup as a divergent active store is a restore operation. It creates
fresh identities. Normal crash recovery of the original store preserves its
identities and does not call this API.

## Bounded staging and publication

The operation accepts an existing backup path, a new destination path, a maximum
input size, a maximum staging size, and a fixed copy-buffer limit. It creates a
unique staging file in the destination directory. It never replaces an existing
destination. Keep the same filesystem for staging and publication.

Create a fresh staging database and commit its `restore_incomplete` marker before
importing any source history. Import logical rows in bounded transactions; do not
copy the source SQLite header into staging. This avoids a crash window in which
staging could look like a complete database with the old identities. Ordinary
`SqliteStore::open` rejects the incomplete marker. Temporary naming alone is not
a sufficient guard against accidental activation.

There is also a bootstrap step before SQLite can commit that marker. Reserve an
owner directory keyed by the canonical destination and operation ID. It contains
at most one child directory whose name is the full request hash, including the
backup identity. Creating and synchronizing that child records the accepted
request before the staging database exists. A directory entry is created as a
whole; there is no partly written ownership file to interpret after a crash.

```text
owner directory absent -> empty owner directory -> one request-hash child
                         no request accepted       request identity bound
                                                   then create staging DB
```

An empty owner directory can accept a request. A different existing request
child is a conflict. More than one child, an unexpected entry or a symlink is
rejected and preserved. Inspect at most two entries to detect an invalid shape.
The source path is not part of the request hash: moving the same backup artifact
does not change its identity. A completed retry needs no source file.

Ordinary store open rejects the library's reserved staging namespace, including
an empty staging file. This protects the bootstrap window. The database marker
then protects imported history independently of its name. The owner directory's
exact request binding must be validated before retry or cleanup. Never create a binding to
adopt an unrelated preexisting staging file.

Cleanup derives the exact owner and request directories, verifies their shape,
and removes only those owned entries. It does not scan a filename pattern and
delete all matches. Include directory entries and their observed filesystem
allocation in storage accounting and interruption tests; their hash lengths
are not a measurement of disk space.

```text
lock backup and destination namespace
    -> inspect source format and size
    -> create and commit incomplete staging metadata
    -> import bounded row pages and rewrite identities
    -> validate SQLite structure and event invariants
    -> persist complete marker
    -> close and synchronize staging
    -> publish at an absent destination
    -> return destination and bounded mapping reader
```

The database stores three publication states. The column is named
`restore_incomplete`; treat it as an enum, not a boolean:

```text
1 importing  ->  2 complete, awaiting publication  ->  0 published
ordinary open: reject          reject                   accept
```

The destination ownership lock stays held while the final state changes. A
retry verifies the saved operation ID and backup hash before finishing a state
transition. Normal databases use state 0. Legacy format-2 databases without
restore metadata need an explicit compatibility path; adding restore must not
make their valid history unreadable.

Use a publication primitive that fails if the destination already exists. Do not
implement this as an existence check followed by a replacing rename: another
creator could win between those operations. Keep the destination ownership lock
through publication. A published file that loses its response must be identifiable
by a caller-supplied restore operation ID, so retry returns the same result.

Validate application format/version before mutation. Validate foreign keys,
cursor encoding, record contiguity through each active tail, retry index equality,
and recorded byte/count charges. Traverse records in bounded pages. SQLite's
integrity check may perform substantial engine work; report that cost separately
and do not describe a copy-buffer limit as a limit on all engine memory or time.

## Fresh identity and mapping

Store an old-to-new incarnation mapping in the destination as part of restore.
Allocate exactly one new incarnation for every distinct old stream key in the
union of active/unavailable names, retained lifetimes, and expected/replacement
keys in lifecycle receipts. Some receipts outlive the histories they reference.
Their identities must still have a mapping. Rewrite each identity's
associated name state, records, lifecycle receipts, and any other references
consistently. Later snapshot and replication phases must extend this explicit
reference audit before their formats can be restored.

Return mapping pages with record and byte limits. Do not build one large map in
Rust memory. The application rebuilds its saved cursors explicitly from this
mapping. An old cursor submitted unchanged must fail. Restoring history does not
rerun commands, emit terminal replies, or execute recorded tool calls.

Scope a restore operation ID to its canonical destination path. Reusing it at
that destination with a different backup identity is a conflict. The same ID at
another destination is an independent operation. This avoids a global registry
solely for detecting unrelated destinations. A completed destination keeps enough
operation metadata to resolve a lost publication response. A retry must not
overwrite it or allocate another set of incarnations.

A completed retry reads the destination's saved receipt first. It does not need
the source backup to still exist. The saved backup hash must match the request.
An unfinished import still needs its source for a new attempt.

There are two separate steps at the end: finish the database, then publish its
name. A crash between them leaves a complete but unpublished staging database.
A matching retry can publish that file without assigning new identities. A
crash after publication can leave two names for the same file. Verify the saved
request and both file identities before removing the staging name. An unrelated
file at either name must never be removed or replaced.

Operation IDs are data, not filenames. Derive a bounded staging filename from a
hash of the destination and operation ID. Slashes and long IDs must not change
where staging is written.

## Failure and resource rules

Account for backup bytes, staging bytes, journal bytes and the final database
separately. Reserve no claim that one maximum-file-size value caps their sum.
Set finite quotas for input size, database pages, copy buffer and mapping pages.
Check available staging budget before copying; enforce it while writing too.
Concurrent filesystem users can still exhaust disk space, so every write can fail.

On a definite pre-publication failure, leave the active destination absent. Clean
up only files owned by this operation. If cleanup fails, return their exact paths
and leave them marked incomplete. A later explicit cleanup operation can remove
them under the same ownership check. Do not scan and delete unrelated temporary
files by filename pattern.

An ambiguous publication or synchronization outcome requires inspection by the
same restore ID. It must not be reported as a definite absence. The initial
supported durability remains the tested process-restart profile. Power-loss
claims require separate filesystem and synchronization evidence.

## Required verification

Test crashes after each arrow, lost publication acknowledgement, existing
destination, symlink/alias ownership, a concurrent source reopen, old cursors,
large mappings, corrupt format/history, quota exhaustion, disk full, and cleanup
failure. Reopen the published store and replay all fixture bytes under the new
identities. The original backup must remain unchanged.

Measure copy/validation/rewrite/publication time separately. Report peak Rust
allocations, RSS, source/staging/journal/final bytes and mapping-page memory. This
is offline maintenance; no foreground append-latency claim applies to a closed
source. Do not mark ADR 0004 complete until both lifecycle and this restore path
have their required evidence.

### Restore resource fixture

Use three fresh measured processes per size, with exactly 1,000, 10,000 and
100,000 active stream identities. Each stream has one record at offset 1 with a deterministic 128-byte
payload. Names, event IDs and the schema stay within the normal identifier
limits. Fixture creation is outside the restore timer. Close the source, record
its SHA-256 and size, and refuse the run if it exceeds the configured limit.

Use these explicit limits for every size:

```text
max operations       1
max source bytes     512 MiB
max staging bytes    512 MiB of database pages
copy buffer          64 KiB
mapping page         256 entries / 2 MiB
path bytes           4,096
process watchdog     15 minutes
```

An optional test-support observer records a fixed set of restore stages. It does
not retain per-stream labels or samples, and the default backend does no observer
work. The callbacks cover completed source validation, incomplete-staging
creation, each logical table import boundary, every successfully committed
record page, relational validation, staging completion, staging file sync, and
no-clobber publication. Run three instrumented fresh processes and one
stage-observer-disabled control for each fixture size. Use a fixed interleaved schedule
across sizes and modes rather than running every repetition of one cell together.
Preserve every sample and the execution order. Report the three instrumented
samples, their median and variation. The single control estimates observer
overhead but cannot precisely characterize its noise.

Report whole-operation elapsed time, process user-plus-system CPU, lifetime peak
RSS, context switches and page faults. Report Rust allocation calls, allocated
and deallocated traffic, and sampled live/peak Rust bytes. These counters exclude
SQLite's C allocator. A bounded 1 ms sampler reports current RSS and sampled peak
staging and rollback-journal file sizes. It also reports the owner and request
directory counts and their filesystem-allocated bytes. Label these as sampled
peaks because a short-lived journal can exist between samples. Report exact
source, final staging, final journal, owner/request directory counts and
allocated bytes, and published-database sizes at stage boundaries. A hash-name
length is logical metadata and must not be reported as disk usage. Source
validation and durable owner reservation are separate observer stages. The
owner-reserved stage includes synchronizing the directories that make an empty
staging file recognizable after a crash.

The committed harness is `examples/restore_resource.rs`; the fixed execution
schedule is `run-restore-resource.sh`. The collector enforces the 900-second
limit around the whole child process and records a timeout or nonzero exit as an
evidence row. The Rust timer and CPU/allocation deltas cover the restore call and
the always-on 1 ms sampler. Fixture creation and post-restore correctness checks
remain outside that timer but inside the child-process watchdog. The
stage-observer-disabled control still retains allocator counters and the sampler,
so it estimates callback overhead only. It is not a total instrumentation-off
comparison. Process disk-I/O counters, where the operating system provides them,
cover the whole process during the restore window. They do not isolate source
reads, staging writes, SQLite calls, filesystem cache work, or physical device
I/O. The original stream-count matrix does not measure SQLite temporary files.
The later [VFS and deeper-history run](restore-vfs-resource-evidence.md) records
SQLite callback counts and temporary logical-file bytes directly. Those byte
counts are not filesystem allocation or physical-device traffic. Database and
journal sizes are not a substitute for this separate temporary-file metric.
The harness records current RSS immediately before restore and again after the
operation and sampler stop. The second point is a post-operation observation,
not proof that the allocator returned memory to the operating system. It records
Rust live bytes both before sampler shutdown and after the sampler thread joins,
because the former includes sampler-owned state while the latter does not.

Read the mapping through the public bounded page API after publication. Record
the maximum logical mapping bytes and vector capacity for one page. Verify all
mapping entries are sorted, complete, and use fresh incarnations. Reopen the
published store and read the one record from every mapped stream through
`SqliteStore`, checking cursor, schema, event ID and every payload byte. Retry the
same restore request and require the exact receipt. Rehash the source and require
the original identity and no recovery sidecars. Correctness checks run outside
the restore timer but remain under the process watchdog.

Record import batches must span stream boundaries. Read source lifetimes and
records in `(stream_key, offset)` order with one current-stream validation state.
Charge each record to the configured page record and byte limits. Commit when
either limit would be exceeded. This keeps memory bounded while changing a
one-record-per-stream fixture from one commit per stream to
`ceil(record_count / page.max_records)` record-page commits. At every stream
transition, verify the observed contiguous range exactly matches its stored floor
and tail. Empty lifetimes must also appear in the ordered scan and pass the same
boundary check.

## Implementation boundary for the next slice

Use a `SqliteRestoreManager` instance to bound accepted maintenance calls, with
one active operation by default and immediate overload on excess calls. It owns
no persistent global receipt registry and starts no idle polling task. The
manager's filesystem root and operation limits are explicit construction inputs.
Destination paths are resolved within that root. Each operation holds the same
exclusive destination ownership boundary used by ordinary store open.

A typed request contains `RestoreOperationId`, source backup path, expected
`BackupIdentity` (SHA-256), destination path, maximum source/staging bytes, and
record/byte page limits. A receipt identifies the completed destination and its
restore ID/backup identity. Mapping reads use bounded pages; they do not return
one vector containing every stream. Read-only mapping access must still retain
store ownership for the duration of accepted I/O.

Hash the closed backup through a bounded buffer and verify its expected identity
before publication. Logical import adds index construction and transaction I/O
compared with a raw file copy. Measure that explicit cost rather than adding
custom SQLite-header editing to save it.

Define explicit errors for incomplete restore, request conflict, corrupt backup,
capacity exhaustion, existing destination, and uncertain publication. A caller
retrying an uncertain publication uses the same destination and operation ID.
The actual no-clobber filesystem primitive and its crash tests must be verified
on the supported host before implementation is declared complete.

## Typed boundary for implementation

Keep generic admission/cancellation ownership in `application/restore.rs`, pure
identities in `domain/restore.rs`, and SQLite/filesystem work in
`infrastructure/sqlite_restore.rs`. Do not put restore SQL in the application
manager. Do not add restore methods to the ordinary event-store port.

```rust
// Pure domain values.
RestoreOperationId(/* same bounded identifier rules as lifecycle IDs */)
BackupIdentity([u8; 32]) // SHA-256 of the exclusively held closed source file
IncarnationMapping { old: StreamKey, new: StreamKey }

// Application inputs and output; paths describe the filesystem boundary.
RestoreRequest {
    operation_id: RestoreOperationId,
    backup_identity: BackupIdentity,
    source: PathBuf,
    destination: PathBuf, // relative to the backend's canonical root
}
RestoreReceipt {
    operation_id: RestoreOperationId,
    backup_identity: BackupIdentity,
    destination: PathBuf, // canonical published path
    mapping_count: u64,
}
MappingPage {
    entries: Vec<IncarnationMapping>,
    next_after: Option<StreamKey>,
    complete: bool,
}
RestoreConfig {
    max_operations: 1,
    max_source_bytes: 1024 * 1024 * 1024,
    max_staging_bytes: 1024 * 1024 * 1024,
    copy_buffer_bytes: 64 * 1024,
    page: PageLimits { max_records: 256, max_bytes: 2 * 1024 * 1024 },
    max_path_bytes: 4096,
}
```

`max_staging_bytes` limits database pages, not database plus rollback journal.
State the journal's additional temporary-space requirement in receipts/evidence.
The initial profile supports format 2 backups. An old format must first undergo
ordinary exclusive migration and close before backup inspection; do not silently
mutate a source during restore. Reject WAL or nonempty recovery journal sidecars
before opening the source read-only. Keep the source and destination locks
through all accepted work.

Use a separate `RestoreError`/`RestoreResult<T>` so optional filesystem diagnostics
do not enlarge ordinary append errors. Include invalid configuration, overload,
ownership conflict, corrupt/unsupported backup, identity mismatch, request
conflict, incomplete staging, existing destination, capacity exhaustion, storage
failure, publication unknown, and failed staging cleanup. Uncertain publication
must carry the exact operation ID and destination. Keep paths bounded before
owning an accepted request.

The injected application port is `RestoreBackend` with four operations:

```rust
async fn inspect_backup(&self, source: PathBuf, config: RestoreConfig)
    -> RestoreResult<BackupIdentity>;
async fn restore(&self, request: RestoreRequest, config: RestoreConfig)
    -> RestoreResult<RestoreReceipt>;
async fn read_mapping(&self, receipt: RestoreReceipt,
    after: Option<StreamKey>, limits: PageLimits, config: RestoreConfig)
    -> RestoreResult<MappingPage>;
async fn cleanup_staging(&self, request: RestoreRequest, config: RestoreConfig)
    -> RestoreResult<()>;
```

`RestoreManager<B>` owns the backend and config, admits calls immediately under
one shared finite semaphore, and spawns owned work only after admission. Dropping
a caller must not release its slot or filesystem locks before backend I/O ends.
No unbounded waiter queue is added. Backend panics return a conservative error;
a restore panic is an unknown outcome carrying the request identity. Mapping
and cleanup use the same capacity boundary. Tests inject a paused backend to
prove cancellation and admission behavior independently of SQLite.

`SqliteRestoreBackend::new(root)` validates its canonical filesystem root. Source
paths can identify a closed backup outside that root; destination paths cannot
escape it, including through symlinked parents. `SqliteRestoreManager` may be a
public alias for `RestoreManager<SqliteRestoreBackend>`. Reuse the existing store
ownership mechanism through small crate-private helpers. Keep SQLite worker
implementation details private.

Mapping pages sort by stream-name bytes and then old incarnation bytes. Their
logical byte charge is both identifier lengths plus 128 bytes per mapping. The
cursor is exclusive: return entries strictly after `after`. An incomplete
mapping is never served. Validate the receipt against persisted restore metadata
before any mapping read or retry response.

A source is already a closed backup artifact for this API. `inspect_backup`
verifies and hashes that artifact; it does not claim to create an online backup.
A separately named backup-creation API is not implied by inspection.
