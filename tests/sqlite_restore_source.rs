#![cfg(all(feature = "sqlite", feature = "test-support"))]

use event_stream::{
    infrastructure::{
        SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteRestoreObserver,
        SqliteRestoreStage, SqliteStore,
    },
    Error, EventId, EventStore, NewEvent, PageLimits, Payload, RestoreConfig, RestoreOperationId,
    RestoreRequest, SchemaId, SchemaRef, StreamId,
};
use rusqlite::{Connection, ErrorCode};
use std::{
    path::PathBuf,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

#[derive(Debug, Default)]
struct SourceGate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl SourceGate {
    fn wait_until_entered(&self) {
        let state = self.state.lock().expect("source gate lock");
        let (state, timeout) = self
            .changed
            .wait_timeout_while(state, Duration::from_secs(10), |(entered, _)| !*entered)
            .expect("source gate wait");
        assert!(
            !timeout.timed_out() && state.0,
            "restore reached source validation"
        );
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("source gate lock");
        state.1 = true;
        self.changed.notify_all();
    }
}

impl SqliteRestoreObserver for SourceGate {
    fn observe(&self, stage: SqliteRestoreStage) {
        if stage != SqliteRestoreStage::SourceValidated {
            return;
        }
        let mut state = self.state.lock().expect("source gate lock");
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            state = self.changed.wait(state).expect("source gate wait");
        }
    }
}

struct ReleaseGuard(Arc<SourceGate>);

impl Drop for ReleaseGuard {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn temp_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "event-stream-restore-source-lock-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    root.canonicalize().unwrap()
}

fn event() -> NewEvent {
    NewEvent {
        id: EventId::new("original-event").unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("restore.source-lock").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(b"original source bytes"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn validated_source_snapshot_blocks_library_and_raw_sqlite_writers() {
    let root = temp_root();
    let source = root.join("source.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("source-stream").unwrap())
        .await
        .unwrap();
    store.append_atomic(&stream, event()).await.unwrap();
    store.close().await.unwrap();
    drop(store);

    let plain = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let identity = plain.inspect_backup(source.clone()).await.unwrap();
    drop(plain);

    let gate = Arc::new(SourceGate::default());
    let release = ReleaseGuard(gate.clone());
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root)
            .unwrap()
            .with_observer(gate.clone()),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("source-lock").unwrap(),
        backup_identity: identity,
        source: source.clone(),
        destination: PathBuf::from("restored.sqlite3"),
    };
    let restore_manager = manager.clone();
    let restore_request = request.clone();
    let restore = tokio::spawn(async move { restore_manager.restore(restore_request).await });

    let wait_gate = gate.clone();
    tokio::task::spawn_blocking(move || wait_gate.wait_until_entered())
        .await
        .unwrap();

    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&source)).await,
        Err(Error::StoreInUse)
    ));

    let raw_source = source.clone();
    let raw_error = tokio::task::spawn_blocking(move || {
        let connection = Connection::open(raw_source).unwrap();
        connection.busy_timeout(Duration::from_millis(100)).unwrap();
        connection
            .execute_batch(
                "BEGIN IMMEDIATE;
             UPDATE event_records SET payload=X'6D757461746564';",
            )
            .unwrap();
        let error = connection
            .execute_batch("COMMIT;")
            .expect_err("source read transaction must prevent writer commit");
        let _ = connection.execute_batch("ROLLBACK;");
        error
    })
    .await
    .unwrap();
    assert!(matches!(
        raw_error,
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(code.code, ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
    ));

    release.0.release();
    let receipt = tokio::time::timeout(Duration::from_secs(10), restore)
        .await
        .expect("restore finishes after source release")
        .unwrap()
        .unwrap();
    let repeated_identity = manager.inspect_backup(source.clone()).await.unwrap();
    assert_eq!(repeated_identity, identity);

    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let restored_stream = restored
        .create_if_absent(&StreamId::new("source-stream").unwrap())
        .await
        .unwrap();
    let page = restored
        .read_range(
            &restored_stream,
            0,
            1,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].cursor.offset, 1);
    assert_eq!(page.records[0].event.id.as_str(), "original-event");
    assert_eq!(
        page.records[0].event.schema.id.as_str(),
        "restore.source-lock"
    );
    assert_eq!(page.records[0].event.schema.version, 1);
    assert_eq!(
        page.records[0].event.payload.as_bytes(),
        b"original source bytes"
    );
    restored.close().await.unwrap();
    drop(restored);
    drop(release);
    std::fs::remove_dir_all(root).unwrap();
}
