#![cfg(all(feature = "sqlite", feature = "replication"))]
use event_stream::infrastructure::{
    SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteStore,
};
use event_stream::*;
use std::path::PathBuf;
fn temp_root(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("{label}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    path
}
fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("restore-test").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(payload),
    }
}
#[tokio::test]
async fn restore_pending_origin_preserves_history_and_requires_new_bootstrap() {
    restore_pending_origin(None).await;
}
async fn restore_pending_origin(_kill_phase: Option<&str>) {
    use event_stream::{
        AttachReplica, DestinationEpoch, OriginStream, ReplicaId, ReplicaStart,
        ReplicationOperationId, ReplicationOriginStore,
    };
    use std::time::Duration;

    let root = temp_root("replication-restore-boundary");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("replication-restore").unwrap())
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: stream.clone(),
    };
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("replication-restore-attach").unwrap(),
            replica: ReplicaId::new("replication-restore-replica").unwrap(),
            stream: origin.clone(),
            destination_epoch: DestinationEpoch([81; 16]),
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("a", b"first"))
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("b", b"second"))
        .await
        .unwrap();
    let pending = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("pending-before-restore").unwrap(),
        batch_id: BatchId([82; 16]),
        replica: ReplicaId::new("replication-restore-replica").unwrap(),
        stream: origin.clone(),
        expected_after: ReplicaPosition {
            stream: origin.clone(),
            offset: 0,
        },
        limits: ReplicaBatchLimits {
            max_records: 1,
            max_bytes: 4096,
        },
    };
    assert_eq!(
        store
            .prepare_replica_batch(pending.clone())
            .await
            .unwrap()
            .batch
            .unwrap()
            .records
            .len(),
        1
    );
    store.close().await.unwrap();
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-boundary").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    #[cfg(feature = "test-support")]
    if let Some(phase) = _kill_phase {
        let marker = root.join("restore-pause");
        let mut child = RestoreChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "replication_restore_kill_child", "--nocapture"])
                .env("EVENT_STREAM_REPLICA_RESTORE_CHILD", &root)
                .env("EVENT_STREAM_REPLICA_RESTORE_PHASE", phase)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !marker.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {phase}"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "restore did not reach {phase}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        child.0.kill().unwrap();
        let status = child.0.wait().unwrap();
        assert!(!status.success());
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
    }
    let receipt = manager.restore(request).await.unwrap();
    let mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == stream)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let restored_origin = restored.origin_identity().await.unwrap();
    assert_ne!(restored_origin, origin.origin);
    let transformed = OriginStream {
        origin: restored_origin,
        stream: mapping.new,
    };
    let status = restored
        .replica_status(
            &ReplicaId::new("replication-restore-replica").unwrap(),
            &transformed,
        )
        .await
        .unwrap();
    assert_eq!(
        status.mode,
        event_stream::ReplicaMode::DetachedNeedsBootstrap
    );
    assert_eq!(status.backlog_records, 0);
    assert!(status.pending_batch.is_none());
    let records = restored
        .read_range(
            &transformed.stream,
            0,
            2,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(records.records.len(), 2);
    assert_eq!(records.records[0].event.payload.as_bytes(), b"first");
    assert_eq!(records.records[1].event.payload.as_bytes(), b"second");
    assert!(
        restored.prepare_replica_batch(pending).await.is_err(),
        "old origin identity was accepted"
    );
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restore_rejects_foreign_origin_attachment() {
    reject_origin_corruption("UPDATE replication_origin_replicas SET origin_id=zeroblob(16)").await;
}
#[tokio::test]
async fn restore_rejects_foreign_origin_operation_receipt() {
    reject_origin_corruption("UPDATE replication_origin_operations SET origin_id=zeroblob(16)")
        .await;
}
#[tokio::test]
async fn restore_rejects_understated_origin_backlog() {
    reject_origin_corruption("UPDATE replication_origin_replicas SET backlog_records=0").await;
}
async fn reject_origin_corruption(sql: &str) {
    use event_stream::{
        AttachReplica, DestinationEpoch, OriginStream, ReplicaId, ReplicaStart,
        ReplicationOperationId, ReplicationOriginStore,
    };
    use std::time::Duration;

    let root = temp_root("replication-restore-boundary");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("replication-restore").unwrap())
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: stream.clone(),
    };
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("replication-restore-attach").unwrap(),
            replica: ReplicaId::new("replication-restore-replica").unwrap(),
            stream: origin.clone(),
            destination_epoch: DestinationEpoch([81; 16]),
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("a", b"first"))
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("b", b"second"))
        .await
        .unwrap();
    let pending = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("pending-before-restore").unwrap(),
        batch_id: BatchId([82; 16]),
        replica: ReplicaId::new("replication-restore-replica").unwrap(),
        stream: origin.clone(),
        expected_after: ReplicaPosition {
            stream: origin.clone(),
            offset: 0,
        },
        limits: ReplicaBatchLimits {
            max_records: 1,
            max_bytes: 4096,
        },
    };
    assert_eq!(
        store
            .prepare_replica_batch(pending.clone())
            .await
            .unwrap()
            .batch
            .unwrap()
            .records
            .len(),
        1
    );
    store.close().await.unwrap();
    let connection = rusqlite::Connection::open(&source).unwrap();
    connection.execute(sql, []).unwrap();
    drop(connection);

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-boundary").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let result = manager.restore(request).await;
    assert!(
        matches!(&result, Err(RestoreError::CorruptBackup(_))),
        "corrupt origin metadata accepted after {sql}: {result:?}"
    );
    assert!(!root.join("restored.sqlite3").exists());
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restore_rejects_pending_batch_beyond_source_tail() {
    reject_origin_corruption(
        "UPDATE replication_origin_operations SET result_through=x'0000000000000003' WHERE kind=1",
    )
    .await;
}

#[tokio::test]
async fn restore_rejects_inconsistent_pending_batch_record_count() {
    reject_origin_corruption(
        "UPDATE replication_origin_operations SET result_batch_records=2 WHERE kind=1",
    )
    .await;
}

#[tokio::test]
async fn restore_completed_receipts_after_payload_retention_cleanup() {
    use event_stream::{
        AttachReplica, OriginStream, ReplicaId, ReplicaStart, ReplicationOperationId,
        ReplicationOriginStore,
    };
    use std::time::Duration;

    let root = temp_root("replication-restore-boundary");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("replication-restore").unwrap())
        .await
        .unwrap();
    let destination = event_stream::infrastructure::MemoryStore::open(
        event_stream::infrastructure::MemoryStoreOptions::default(),
    )
    .await
    .unwrap();
    let epoch = destination.destination_epoch().await.unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: stream.clone(),
    };
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("replication-restore-attach").unwrap(),
            replica: ReplicaId::new("replication-restore-replica").unwrap(),
            stream: origin.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("a", b"first"))
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("b", b"second"))
        .await
        .unwrap();
    let pending = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("pending-before-restore").unwrap(),
        batch_id: BatchId([82; 16]),
        replica: ReplicaId::new("replication-restore-replica").unwrap(),
        stream: origin.clone(),
        expected_after: ReplicaPosition {
            stream: origin.clone(),
            offset: 0,
        },
        limits: ReplicaBatchLimits {
            max_records: 2,
            max_bytes: 4096,
        },
    };
    assert_eq!(
        store
            .prepare_replica_batch(pending.clone())
            .await
            .unwrap()
            .batch
            .unwrap()
            .records
            .len(),
        2
    );
    let batch = store
        .prepare_replica_batch(pending.clone())
        .await
        .unwrap()
        .batch
        .unwrap();
    let after = batch.after.clone();
    let receipt = destination.commit_replica_batch(batch).await.unwrap();
    store
        .acknowledge_replica_batch(AcknowledgeReplicaBatch {
            operation_id: ReplicationOperationId::new("completed-before-cleanup").unwrap(),
            replica: pending.replica.clone(),
            expected_after: after,
            receipt,
        })
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-cleanup").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("expire-legacy").unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("prune-completed").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 2),
        })
        .await
        .unwrap();
    let cleaned = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_event_rows, 2);
    store.close().await.unwrap();
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-boundary").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let receipt = manager.restore(request).await.unwrap();
    let mapping = manager
        .read_mapping(
            receipt.clone(),
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| entry.old == stream)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let restored_origin = restored.origin_identity().await.unwrap();
    assert_ne!(restored_origin, origin.origin);
    let transformed = OriginStream {
        origin: restored_origin,
        stream: mapping.new,
    };
    let status = restored
        .replica_status(
            &ReplicaId::new("replication-restore-replica").unwrap(),
            &transformed,
        )
        .await
        .unwrap();
    assert_eq!(
        status.mode,
        event_stream::ReplicaMode::DetachedNeedsBootstrap
    );
    assert_eq!(status.backlog_records, 0);
    assert!(status.pending_batch.is_none());
    let records = restored
        .read_range(
            &transformed.stream,
            2,
            2,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert!(records.records.is_empty());
    assert!(
        restored.prepare_replica_batch(pending).await.is_err(),
        "old origin identity was accepted"
    );
    restored.close().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-support")]
struct RestoreChild(std::process::Child);
#[cfg(feature = "test-support")]
impl Drop for RestoreChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[cfg(feature = "test-support")]
#[tokio::test]
async fn pending_replication_restore_survives_publication_process_kills() {
    for phase in ["completed", "linked", "published"] {
        restore_pending_origin(Some(phase)).await;
    }
}
#[cfg(feature = "test-support")]
#[tokio::test]
async fn replication_restore_kill_child() {
    use event_stream::infrastructure::SqliteRestoreFailureInjection;
    let Some(root) = std::env::var_os("EVENT_STREAM_REPLICA_RESTORE_CHILD") else {
        return;
    };
    let root = PathBuf::from(root);
    let failure = match std::env::var("EVENT_STREAM_REPLICA_RESTORE_PHASE")
        .unwrap()
        .as_str()
    {
        "completed" => SqliteRestoreFailureInjection::AfterStagingCompletion,
        "linked" => SqliteRestoreFailureInjection::AfterDestinationLink,
        "published" => SqliteRestoreFailureInjection::AfterPublishedMarker,
        _ => panic!("invalid phase"),
    };
    let backend = SqliteRestoreBackend::new(&root)
        .unwrap()
        .with_pause_injection(failure, root.join("restore-pause"));
    let manager = SqliteRestoreManager::new(backend, RestoreConfig::default()).unwrap();
    let source = root.join("backup.sqlite3");
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-boundary").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: "restored.sqlite3".into(),
    };
    let _ = manager.restore(request).await;
    panic!("pause returned without parent kill");
}
