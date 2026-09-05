# Running and recovering a local stream store

This describes the implemented process-restart path. Release qualification is still in progress; see the [completion audit](completion-audit.md) and [SQLite evidence](sqlite-evidence.md).

## Open one owned store

Give `SqliteOptions::new(path)` a database path inside an application-managed directory. The application chooses the directory and its permissions. Keep the database and its ownership file in place while open. The adapter rejects database symlinks and hard links, and normalizes parent-path aliases on the tested Unix path.

Open `Runtime<SqliteStore>` once and clone that runtime's handles for producers and readers. A second owner receives `StoreInUse`. Do not remove the ownership file to bypass this error. The current owner may still have accepted work running after a caller times out or drops its handle.

The SQLite adapter uses a dedicated worker and verifies its configured SQLite settings. Its declared profile is `ProcessRestart`. This means the tested guarantee concerns normal restarts and terminated processes. It does not promise survival across sudden device power loss.

## Stop without losing track of accepted work

Stop offering new work, then call `shutdown(timeout)`. Inspect the returned report:

```text
closed = true   -> the owned close finished
closed = false  -> work or close is still running; the deadline was not a forced abort
unresolved     -> stream + event ID pairs whose result needs reconciliation
```

A deadline does not authorize another owner to write. The existing runtime keeps ownership until its storage work and close finish. An application may call shutdown again to observe completion. Preserve the original event ID, schema and payload for any append whose result it did not receive.

## Reopen after a process stops

After the previous process has exited, open the same path with compatible configuration. SQLite performs its own journal recovery. The adapter validates its format, UTF-8 database encoding and required settings before accepting normal operations. Opening is not a full scan proving that every historical record is healthy; bounded reads also validate stored values and report corruption or missing records.

Retry an uncertain append with the same stream lifetime, event ID, schema and payload. If it committed, the receipt points to the original record. If it did not commit, the retry can create it. Changing the event ID would create a different event and defeat retry deduplication. Changing bytes under the same ID returns a conflict.

Resume a consumer strictly after its last successfully applied cursor. Save application state and that cursor together. If decoding or applying a record fails, keep the checkpoint unchanged. The [projection example](../../../examples/projection_reconnect.rs) demonstrates this rule with application-owned in-memory state; it is not a durable consumer database.

## Handle errors without replacing history

A newer or foreign database format is rejected. There is no automatic migration or repair command in this release. Preserve the original files when investigating a failure. Do not delete the database and recreate it under the same application name as an error-recovery shortcut: existing cursors and retry identities refer to its current history.

A database page quota limits the main database. Journals can require additional temporary disk space. A capacity failure is explicit; there is no automatic retention that deletes old history to make room. Size the application storage location for both data and transaction overhead, and use the returned error to decide whether to stop ingestion or retry after restoring capacity.

## Backup and restore scope

The current runtime supports explicit reset/delete and bounded retired-history cleanup when its adapter implements `LifecycleStore`. Memory and SQLite implement this boundary. Keep the operation ID stable when retrying, and pass the expected stream incarnation. Reset starts an empty new lifetime; it does not restore a backup. There is still no completed public backup, live-copy, or controlled restore API. Copying a database while its worker can still write is not a supported backup procedure. A closed-store archival copy does not by itself define a safe rollback or restore protocol: replacing active history with an older copy could reuse cursor positions for different events. Lifecycle behavior and the remaining controlled-restore work are tracked in [ADR 0004](../004-stream-lifecycle-and-safe-restore/adr.md).

## Verified environment

The local evidence currently covers macOS on APFS with the bundled SQLite version and settings recorded in the evidence document. Linux jobs are configured but have not been observed running remotely. Network filesystems and device power-loss behavior have no support claim from these local tests.
