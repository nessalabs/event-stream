//! Run against a NEW directory: cargo run --features sqlite --example local_recovery -- /tmp/my-demo
//! The example retains the database, closed backup and restored database for inspection.
use event_stream::infrastructure::{
    SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteStore,
};
use event_stream::*;
use std::{path::Path, time::Duration};

type ExampleResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const MAX_ATTEMPTS: u64 = 128;
const PAYLOAD_BYTES: usize = 2048;

fn event(index: u64) -> Result<NewEvent> {
    Ok(NewEvent {
        id: EventId::new(format!("input-{index}"))?,
        schema: SchemaRef {
            id: SchemaId::new("example.bytes")?,
            version: 1,
        },
        payload: Payload::copy_from_slice(&[7; PAYLOAD_BYTES]),
    })
}

fn page_limits() -> PageLimits {
    PageLimits {
        max_records: 2,
        max_bytes: 16 * 1024,
    }
}

async fn close(runtime: Runtime<SqliteStore>) -> Result<()> {
    let report = runtime.shutdown(Duration::from_secs(5)).await?;
    assert!(report.closed);
    assert!(report.unresolved.is_empty());
    assert!(report.unresolved_lifecycle.is_empty());
    assert_eq!(report.unfinished_cleanup, 0);
    Ok(())
}

/// Exercise public reader operations with bounded pages and exact payload checks.
async fn verify_history(
    runtime: &Runtime<SqliteStore>,
    stream: &StreamKey,
    count: u64,
) -> Result<()> {
    let bounds = runtime.bounds(stream).await?;
    assert_eq!(bounds.tail.offset, count);
    let mut after = Cursor::new(stream.clone(), 0);
    while after.offset < count {
        let page = runtime
            .read_after(&after, page_limits(), Some(&bounds.tail))
            .await?;
        assert!(!page.records.is_empty());
        for record in page.records {
            assert_eq!(record.cursor.offset, after.offset + 1);
            assert_eq!(record.event, event(after.offset)?);
            after = record.cursor.clone();
        }
    }
    assert_eq!(after, bounds.tail);
    Ok(())
}

async fn run(directory: &Path) -> ExampleResult<u64> {
    // Refuse an existing directory. This walkthrough never reuses or deletes user data.
    std::fs::create_dir(directory)?;
    let directory = directory.canonicalize()?;
    let database = directory.join("live.sqlite3");

    // Initialize first because enabled features change the number of schema pages.
    let initialized = SqliteStore::open(SqliteOptions::new(&database)).await?;
    initialized.close().await?;
    drop(initialized);
    let initialized_pages: u32 = {
        let connection = rusqlite::Connection::open(&database)?;
        connection.pragma_query_value(None, "page_count", |row| row.get(0))?
    };
    let mut options = SqliteOptions::new(&database);
    options.max_database_pages = initialized_pages.checked_add(16).ok_or("page overflow")?;
    let config = RuntimeConfig {
        events: EventConfig {
            max_bytes: 4096,
            minimum_persistence: PersistenceProfile::ProcessRestart,
        },
        ..RuntimeConfig::default()
    };
    let runtime = Runtime::<SqliteStore>::open(options.clone(), config.clone()).await?;
    let stream = runtime
        .create_stream(&StreamId::new("local-events")?)
        .await?;

    let mut committed = 0;
    let mut full = false;
    for index in 0..MAX_ATTEMPTS {
        match runtime.append(&stream, event(index)?).await {
            Ok(receipt) => {
                committed += 1;
                assert_eq!(receipt.kind, AppendKind::Inserted);
                assert_eq!(receipt.record.cursor.offset, committed);
            }
            Err(Error::CapacityExceeded) => {
                full = true;
                break;
            }
            // An unknown commit must be reconciled by identity. It is not a safe
            // signal to discard the event or pretend that the quota test passed.
            Err(error) => return Err(error.into()),
        }
    }
    assert!(full && committed > 0 && committed < MAX_ATTEMPTS);
    verify_history(&runtime, &stream, committed).await?;
    let retry = runtime.append(&stream, event(0)?).await?;
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(retry.record.cursor.offset, 1);
    close(runtime).await?;

    // Successful shutdown has closed SQLite and drained accepted work. This demo
    // owns the directory, so no other process can reopen it during the copy.
    // A live database must use a coordinated SQLite backup procedure instead.
    let backup = directory.join("closed-backup.sqlite3");
    std::fs::copy(&database, &backup)?;
    std::fs::File::open(&backup)?.sync_all()?;
    std::fs::File::open(&directory)?.sync_all()?;

    let reopened = Runtime::<SqliteStore>::open(options, config.clone()).await?;
    assert_eq!(reopened.create_stream(&stream.id).await?, stream);
    verify_history(&reopened, &stream, committed).await?;
    close(reopened).await?;

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&directory)?,
        RestoreConfig {
            max_source_bytes: 16 * 1024 * 1024,
            max_staging_bytes: 16 * 1024 * 1024,
            page: page_limits(),
            ..RestoreConfig::default()
        },
    )?;
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-local-backup")?,
        backup_identity: manager.inspect_backup(backup.clone()).await?,
        source: backup,
        destination: "restored.sqlite3".into(),
    };
    let receipt = manager.restore(request.clone()).await?;
    assert_eq!(manager.restore(request).await?, receipt);
    let mappings = manager.read_mapping(receipt, None, page_limits()).await?;
    assert!(mappings.complete);
    assert_eq!(mappings.entries.len(), 1);
    let mapping = &mappings.entries[0];
    assert_eq!(mapping.old, stream);
    assert_ne!(mapping.new.incarnation, stream.incarnation);

    let restored = Runtime::<SqliteStore>::open(
        SqliteOptions::new(directory.join("restored.sqlite3")),
        config.clone(),
    )
    .await?;
    assert!(matches!(
        restored
            .read_after(&Cursor::new(stream, 0), page_limits(), None)
            .await,
        Err(Error::StaleIncarnation { .. })
    ));
    verify_history(&restored, &mapping.new, committed).await?;
    // More capacity was explicitly configured for the restored database.
    let next = restored.append(&mapping.new, event(committed)?).await?;
    assert_eq!(next.record.cursor.offset, committed + 1);
    close(restored).await?;
    let restored = Runtime::<SqliteStore>::open(
        SqliteOptions::new(directory.join("restored.sqlite3")),
        config,
    )
    .await?;
    verify_history(&restored, &mapping.new, committed + 1).await?;
    close(restored).await?;
    Ok(committed)
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let mut args = std::env::args_os().skip(1);
    let directory = args.next().ok_or("usage: local_recovery NEW_DIRECTORY")?;
    if args.next().is_some() {
        return Err("usage: local_recovery NEW_DIRECTORY".into());
    }
    let count = run(Path::new(&directory)).await?;
    println!(
        "Verified {count} exact records through capacity rejection, reopen and controlled restore."
    );
    println!(
        "Exact retry deduplicated; old cursor rejected; restored append resumed at {}.",
        count + 1
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn full_store_backup_reopen_and_restore_preserve_exact_history() {
        let directory =
            std::env::temp_dir().join(format!("local-recovery-{}", uuid::Uuid::new_v4()));
        let count = run(&directory).await.unwrap();
        assert!(count > 0);
        // A rerun must not alter a previously populated example directory.
        assert!(run(&directory).await.is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
