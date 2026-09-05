# SQLite restore evidence

This file records the current SQLite restore checkpoint. It does not mark the
restore phase complete. Large-restore resource measurements and several
corruption/concurrency cases remain open below.

## Implemented behavior

`SqliteRestoreBackend` imports a closed format-2 SQLite artifact into a new
database. It never publishes the staging name as an ordinary store. The
database carries one explicit state:

| State | Meaning | Ordinary `SqliteStore::open` |
| --- | --- | --- |
| `0` | Published store | Allowed when the file has one name |
| `1` | Import is incomplete | Refused |
| `2` | Import is complete and awaits publication | Refused |

Publication uses this sequence:

```text
commit state 2 in staging
fsync staging
hard-link staging to the unused destination name
fsync destination and its directory
change state 2 -> 0 through the destination name
fsync destination
unlink staging name
fsync directory
```

An exact retry recovers all three intermediate states. It can complete after
the source artifact has been removed. A mapping read never performs this
recovery. It requires a state-0 destination with one filesystem name.

Before creating SQLite, the backend creates an atomic ownership reservation.
The sibling owner directory is named with the full SHA-256 hash of the
canonical destination and operation identifier. It contains zero entries
before a request is accepted, then exactly one child directory named with the
full hash of the destination, operation, and backup identity. The child mkdir
and directory sync bind the request. A different backup for the same operation
is a conflict. Symlinks and unexpected entries are preserved and rejected.
Only two directory entries are ever read.

The flat staging filename contains both hashes. Raw operation text cannot add
path components or exceed the filesystem filename limit. An ordinary store
open recognizes this reserved shape and refuses even an empty staging file.
Destination parents are canonicalized and checked against the configured
restore root. The logical owner charge is included when deriving the SQLite
page quota; actual directory allocation is recorded separately by the resource
harness.

The importer holds the library ownership lock and a SQLite read transaction
while hashing and reading the source. A nonempty rollback journal or WAL is
rejected. Identity mapping uses one ordered SQLite cursor and bounded Rust
batches, rather than rerunning the full identity union for every page. SQLite
page caches are finite. Sort temporary storage uses files instead of an
unbounded in-memory temporary database.

Previously created format-2 databases had five application tables and no
restore marker. Read-only inspection accepts that complete legacy shape
without changing it. A normal writable open adds the two restore tables and
marker in one transaction. Partial legacy/extended shapes are corrupt. A
restore can also import the legacy shape directly.

## Current checks

On 2026-09-05, the opt-in restore command passed 25 tests:

```text
cargo test --locked --features sqlite,test-support --test sqlite_restore
```

The tests cover:

- full logical import, identity rewriting, mapping pagination, lifecycle
  receipt retry, payload equality, and destination restart;
- exact restore retry after the source file is deleted;
- cleanup of an owned state-2 staging file without reopening the source;
- legacy five-table format-2 read-only inspection, direct restore, atomic
  writable extension, and injected pre-commit rollback of that extension;
- rejection of a record outside its declared `(floor, tail]` extent;
- guarded rejection of a malformed 2 MiB mapping incarnation before returning
  a mapping value;
- guarded rejection of a 2 MiB lifecycle replacement incarnation before Rust
  materializes that value;
- typed restore state and mapping-count validation, agreement between the
  stored count and mapping rows before publication, and lifecycle-counter
  agreement on receipt retry;
- rejection of a mapping that reuses the source incarnation;
- refusal to serve mappings from an incompletely published destination without
  changing its state;
- staging-name containment for operation identifiers containing path
  separators;
- same-operation/different-backup conflict, relocated-source retry for the
  same backup, and preservation of unknown or symlinked reservation entries;
- refusal to adopt an unbound empty staging file, including when an empty
  operation owner directory already exists;
- rejection of dangling owner and request-child symlinks, plus zero-byte
  staging symlinks and hard links, before SQLite opens the staging path;
- exact paths for injected staging unlink and directory-sync failures;
- cross-stream record batching and record-page byte limits.

One opt-in test launches a separate test process for each publication
boundary. The `test-support` hook writes and synchronizes a marker, then parks
inside the blocking restore worker before it returns. The worker still owns
the restore operation and filesystem ownership locks. The parent observes the
marker, sends a real process kill, waits for process exit, removes the source,
and retries from a new manager. An RAII guard kills and waits for the child if
the marker times out or an assertion fails. The five boundaries are:

1. the request child directory was synchronized before the staging file exists;
2. the empty staging file was reserved before SQLite wrote its owner marker;
3. state 2 was committed before the destination link;
4. the destination link was created while the database remained in state 2;
5. state 0 was committed while the staging hard link still existed.

The first two retries retain the source because no complete staging database
exists. The last three remove the source before retry, so recovery depends on
the durable staging or destination state. Each case returns the exact receipt,
returns a fresh mapped incarnation, replays the exact event identity, schema,
version, offset, and payload, and repeats that replay after another store
reopen. This proves cross-process retry after abrupt process termination inside
those five boundaries. It does not prove power-loss recovery or device-cache
behavior. The permanent pause code is compiled only with the dependency-free
`test-support` feature.

The real APFS full-filesystem check fills a private 64 MiB image until the
operating system returns `ENOSPC`. The failed restore does not publish a
destination. After space is released, cleanup resolves the owned hot-journal
staging state and the exact request succeeds. The strengthened check validates
the fresh mapped incarnation and every event ID, schema, version, offset, and
full 512 KiB payload across all 32 records. It also verifies that the source
artifact identity is unchanged. This is an actual filesystem-capacity result.
It is not a power-loss test.

A separate real-source test pauses restore after source validation while the
backend still holds its ownership lock and SQLite read transaction. A raw
SQLite writer can begin and update, but its commit returns `BUSY` or `LOCKED`.
A second `SqliteStore` owner returns `StoreInUse`. After the pause is released,
restore returns the exact bytes and the source hash is unchanged. This covers
cooperating library clients and the tested rollback-journal writer. Concurrent
external path replacement and filesystem mutation remain unsupported.

The latest integrated run before the four focused malformed-data additions
passed 145 tests, including the 21 restore cases,
the source-lock case, and the real APFS case.

The repeated 1,000, 10,000, and 100,000-stream restore matrix is documented in
[restore-resource-evidence.md](restore-resource-evidence.md). All 12 fresh
processes completed with exact mapping, replay, retry, source-identity, bounded
page, and final-path checks. The largest instrumented median was 3.328 seconds
for 100,000 streams. The report keeps the allocator, RSS, whole-process I/O,
single-control, and unavailable SQLite temporary-file scopes explicit.

The preceding restore checkpoint passed strict linting:

```text
cargo clippy --locked --all-features --all-targets -- -D warnings
```

## Open evidence gates

The following work is still required before restore can be called complete:

- add malformed mapping ordering and cursor fixtures beyond the current typed
  header, count, counter, freshness, and oversized-column cases;
- attribute SQLite temporary-file bytes and add a larger bounded artifact with
  deeper history or larger payloads than the one-record-per-stream matrix;
- run the settled full suite and Rust 1.85 build after the final fault and
  measurement changes.
