---
id: "restore"
title: "Restore a closed backup safely"
kind: "flow"
status: "implemented"
summary: "Controlled restore publishes new identities; uncertain publication needs exact-request reconciliation."
parent: "events"
sources: ["src/application/restore.rs", "src/infrastructure/sqlite_restore.rs", "src/domain/restore.rs", "examples/local_recovery.rs"]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
diagramDetails: [{"diagram": 0, "node": "Unknown", "section": "Preserve the restore request"}]
---

# Restore history without reviving old cursor identities

The local recovery example closes SQLite successfully, exclusively owns its source
directory, copies and flushes the backup, then restores into a new destination.
That copying procedure is not safe for an active database. Online backup needs
coordinated SQLite facilities outside this example.

```mermaid
flowchart TD
    Closed[Closed exclusively owned backup] --> Inspect[Inspect backup identity]
    Inspect --> Stage[Bounded restore staging]
    Stage --> Publish[Publish destination with fresh incarnations]
    Publish --> Map[Read old-to-new stream mapping]
    Map --> Replay[Rebuild application state with new cursors]
    Publish --> Unknown[Publication outcome unknown]
    Unknown --> Reconcile[Retry same restore request]
```

## Preserve the restore request

RestoreManager bounds admitted operations and owns accepted backend work through
caller cancellation. PublicationUnknown names operation and destination because
publication may already exist. Retry the exact request and inspect its receipt;
do not overwrite a destination or reuse an operation ID for different input.
Incomplete staging and cleanup failure are separate results, not success.

Read the incarnation mapping before rebuilding projections. The library rejects
old cursors against restored streams; it cannot automatically translate an external
application's last-applied checkpoint. Restore also resets origin identity and
replication attachments. Validate application state and re-establish intended
replication explicitly. Backup content identity is an integrity/checkpoint input,
not a grant of authority to read a file.

Current SQLite tests include isolated database and controlled process/fault
schedules. No atlas model proves atomic filesystem publication, real device power
loss, backup trust or arbitrary path replacement. See [evidence](verification.md).
