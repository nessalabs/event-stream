#![cfg(all(feature = "sqlite", feature = "replication"))]

use event_stream::infrastructure::{
    SqliteFailureInjection, SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteStore,
};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::{path::Path, path::PathBuf, sync::Arc};

fn options(path: &Path) -> SqliteOptions {
    let mut options = SqliteOptions::new(path);
    options.replica_destination.staging.max_staging_records = 1;
    options.replica_destination.staging.max_staging_record_bytes = 512;
    options.replica_destination.staging.max_chunk_bytes = 1;
    options.replica_destination.staging.max_staging_bytes = 8;
    options
}

fn stream(id: u8) -> OriginStream {
    OriginStream {
        origin: OriginId([id; 16]),
        stream: StreamKey {
            id: StreamId::new(format!("quota-{id}")).unwrap(),
            incarnation: IncarnationId([id.wrapping_add(1); 16]),
        },
    }
}

async fn begin(store: &SqliteStore, id: u8) -> ReplicaBootstrap {
    let stream = stream(id);
    let content = [id];
    let request = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new(format!("begin-{id}")).unwrap(),
        id: BootstrapId([id; 16]),
        replica: ReplicaId::new("staging-quota").unwrap(),
        destination_epoch: store.destination_epoch().await.unwrap(),
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([id.wrapping_add(40); 16]),
            covered: Cursor::new(stream.stream.clone(), 0),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: 1,
            digest: SnapshotDigest(Sha256::digest(content).into()),
        },
        through: ReplicaPosition { stream, offset: 1 },
    };
    store
        .begin_replica_bootstrap(request.clone())
        .await
        .unwrap();
    store
        .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
            id: request.id,
            chunk: SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(&content),
            },
        })
        .await
        .unwrap();
    request
}

fn batch(request: &ReplicaBootstrap, id: u8) -> ReplicaBootstrapBatch {
    ReplicaBootstrapBatch {
        id: request.id,
        batch: ReplicaBatch {
            id: BatchId([id; 16]),
            destination_epoch: request.destination_epoch,
            after: ReplicaPosition {
                stream: request.stream.clone(),
                offset: 0,
            },
            records: vec![Arc::new(Record {
                cursor: Cursor::new(request.stream.stream.clone(), 1),
                event: NewEvent {
                    id: EventId::new(format!("event-{id}")).unwrap(),
                    schema: SchemaRef {
                        id: SchemaId::new("bytes").unwrap(),
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(b"payload"),
                },
            })],
        },
    }
}

fn counters(path: &Path) -> (i64, u64) {
    let (rows, bytes): (i64, Vec<u8>) = rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT staging_record_count,staging_record_bytes
             FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    (rows, u64::from_be_bytes(bytes.try_into().unwrap()))
}

async fn publish(store: &SqliteStore, request: &ReplicaBootstrap) -> ReplicaBootstrapReceipt {
    assert!(
        store
            .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                id: request.id,
                limits: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 512,
                },
            })
            .await
            .unwrap()
            .complete
    );
    store
        .publish_replica_bootstrap(PublishReplicaBootstrap {
            operation_id: ReplicationOperationId::new(format!(
                "publish-{}",
                request.stream.stream.id.as_str()
            ))
            .unwrap(),
            id: request.id,
            destination_epoch: request.destination_epoch,
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn aggregate_staging_record_quota_retries_and_publication_release_exactly() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-staging-record-quota-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("destination.sqlite3");
    let store = SqliteStore::open(options(&path)).await.unwrap();
    let first = begin(&store, 1).await;
    let second = begin(&store, 2).await;
    let first_batch = batch(&first, 11);
    let first_receipt = store
        .put_replica_bootstrap_batch(first_batch.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .put_replica_bootstrap_batch(first_batch)
            .await
            .unwrap(),
        first_receipt,
        "an exact retry must not reserve staging capacity twice"
    );
    assert_eq!(counters(&path).0, 1);
    let second_batch = batch(&second, 12);
    assert_eq!(
        store
            .put_replica_bootstrap_batch(second_batch.clone())
            .await
            .unwrap_err(),
        ReplicationError::CapacityExceeded,
        "the staging row limit is aggregate across bootstraps"
    );

    let published = publish(&store, &first).await;
    assert_eq!(counters(&path), (0, 0));
    assert_eq!(
        store
            .publish_replica_bootstrap(PublishReplicaBootstrap {
                operation_id: ReplicationOperationId::new("publish-quota-1").unwrap(),
                id: first.id,
                destination_epoch: first.destination_epoch,
            })
            .await
            .unwrap(),
        published,
        "publication retry is served after staging rows are released"
    );
    store
        .put_replica_bootstrap_batch(second_batch)
        .await
        .unwrap();
    assert_eq!(counters(&path).0, 1);
    EventStore::close(&store).await.unwrap();

    let reopened = SqliteStore::open(options(&path)).await.unwrap();
    assert_eq!(counters(&path).0, 1);
    EventStore::close(&reopened).await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn aggregate_staging_record_byte_quota_is_independent_of_row_limit() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-staging-record-byte-quota-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("destination.sqlite3");
    let mut configured = options(&path);
    configured.replica_destination.staging.max_staging_records = 10;
    configured
        .replica_destination
        .staging
        .max_staging_record_bytes = 512;
    let store = SqliteStore::open(configured).await.unwrap();
    let first = begin(&store, 21).await;
    let second = begin(&store, 22).await;
    store
        .put_replica_bootstrap_batch(batch(&first, 31))
        .await
        .unwrap();
    assert_eq!(counters(&path).0, 1);
    assert_eq!(
        store
            .put_replica_bootstrap_batch(batch(&second, 32))
            .await
            .unwrap_err(),
        ReplicationError::CapacityExceeded
    );
    EventStore::close(&store).await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn aborted_staging_record_releases_capacity_only_during_bounded_cleanup() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-staging-record-cleanup-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("destination.sqlite3");
    let store = SqliteStore::open(options(&path)).await.unwrap();
    let first = begin(&store, 3).await;
    store
        .put_replica_bootstrap_batch(batch(&first, 13))
        .await
        .unwrap();
    let abort = AbortReplicaBootstrap {
        operation_id: ReplicationOperationId::new("abort-staging-record").unwrap(),
        id: first.id,
        destination_epoch: first.destination_epoch,
    };
    let abort_receipt = store.abort_replica_bootstrap(abort.clone()).await.unwrap();
    let second = begin(&store, 4).await;
    assert_eq!(
        store
            .put_replica_bootstrap_batch(batch(&second, 14))
            .await
            .unwrap_err(),
        ReplicationError::CapacityExceeded,
        "abort must not release bytes that remain on disk"
    );
    let cleaned = store
        .cleanup_replica_destination(ReplicaCleanupLimits {
            max_receipt_rows: 1,
            max_staging_rows: 2,
            max_bytes: 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_staging_rows, 2);
    assert_eq!(counters(&path), (0, 0));
    assert_eq!(
        store.abort_replica_bootstrap(abort).await.unwrap(),
        abort_receipt
    );
    store
        .put_replica_bootstrap_batch(batch(&second, 14))
        .await
        .unwrap();
    EventStore::close(&store).await.unwrap();

    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute(
        "UPDATE replication_destination_accounting
         SET staging_record_count=0 WHERE singleton=1",
        [],
    )
    .unwrap();
    drop(raw);
    assert!(matches!(
        SqliteStore::open(options(&path)).await,
        Err(Error::StoreCorrupt(message)) if message.contains("record counters")
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn controlled_restore_rebuilds_staging_record_accounting() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-staging-record-restore-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let source_path = root.join("source.sqlite3");
    let source = SqliteStore::open(options(&source_path)).await.unwrap();
    let request = begin(&source, 5).await;
    source
        .put_replica_bootstrap_batch(batch(&request, 15))
        .await
        .unwrap();
    EventStore::close(&source).await.unwrap();
    assert_eq!(counters(&source_path).0, 1);

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("restore-staging-record-accounting").unwrap(),
            backup_identity: manager.inspect_backup(source_path.clone()).await.unwrap(),
            source: source_path,
            destination: PathBuf::from("restored.sqlite3"),
        })
        .await
        .unwrap();
    let restored = SqliteStore::open(options(&receipt.destination))
        .await
        .unwrap();
    assert_eq!(counters(&receipt.destination).0, 1);
    let cleanup = restored
        .cleanup_replica_destination(ReplicaCleanupLimits {
            max_receipt_rows: 1,
            max_staging_rows: 2,
            max_bytes: 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.removed_staging_rows, 2);
    assert_eq!(counters(&receipt.destination), (0, 0));
    EventStore::close(&restored).await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn legacy_destination_accounting_adds_exact_staging_record_counters() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-staging-record-migration-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("destination.sqlite3");
    let store = SqliteStore::open(options(&path)).await.unwrap();
    let request = begin(&store, 6).await;
    store
        .put_replica_bootstrap_batch(batch(&request, 16))
        .await
        .unwrap();
    EventStore::close(&store).await.unwrap();

    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch(
        "ALTER TABLE replication_destination_accounting DROP COLUMN staging_record_bytes;
         ALTER TABLE replication_destination_accounting DROP COLUMN staging_record_count;",
    )
    .unwrap();
    drop(raw);
    let reopened = SqliteStore::open(options(&path)).await.unwrap();
    EventStore::close(&reopened).await.unwrap();
    assert_eq!(counters(&path).0, 1);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn staging_record_charge_reconciles_before_and_after_commit_failures() {
    for (label, injection, committed) in [
        ("before", SqliteFailureInjection::BeforeReplicaCommit, false),
        (
            "after",
            SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost,
            true,
        ),
    ] {
        let root = std::env::temp_dir().join(format!(
            "sqlite-staging-record-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("destination.sqlite3");
        let mut configured = options(&path);
        configured.failure_injection = Some(injection);
        let store = SqliteStore::open(configured).await.unwrap();
        let request = begin(&store, if committed { 8 } else { 7 }).await;
        let batch = batch(&request, if committed { 18 } else { 17 });
        let error = store
            .put_replica_bootstrap_batch(batch.clone())
            .await
            .unwrap_err();
        assert!(matches!(
            (&error, committed),
            (ReplicationError::StorageFailure(_), false)
                | (ReplicationError::BootstrapBatchUnknown(_), true)
        ));
        assert_eq!(counters(&path).0, i64::from(committed));
        store.put_replica_bootstrap_batch(batch).await.unwrap();
        assert_eq!(counters(&path).0, 1);
        EventStore::close(&store).await.unwrap();
        let reopened = SqliteStore::open(options(&path)).await.unwrap();
        assert_eq!(counters(&path).0, 1);
        EventStore::close(&reopened).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn publication_release_reconciles_before_and_after_commit_failures() {
    for (label, injection, committed) in [
        ("before", SqliteFailureInjection::BeforeReplicaCommit, false),
        (
            "after",
            SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost,
            true,
        ),
    ] {
        let root = std::env::temp_dir().join(format!(
            "sqlite-publication-record-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("destination.sqlite3");
        let store = SqliteStore::open(options(&path)).await.unwrap();
        let request = begin(&store, if committed { 10 } else { 9 }).await;
        store
            .put_replica_bootstrap_batch(batch(&request, if committed { 20 } else { 19 }))
            .await
            .unwrap();
        assert!(
            store
                .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                    id: request.id,
                    limits: ReplicaBootstrapVerificationLimits {
                        max_chunks: 1,
                        max_records: 1,
                        max_bytes: 512,
                    },
                })
                .await
                .unwrap()
                .complete
        );
        EventStore::close(&store).await.unwrap();

        let mut configured = options(&path);
        configured.failure_injection = Some(injection);
        let store = SqliteStore::open(configured).await.unwrap();
        let publish = PublishReplicaBootstrap {
            operation_id: ReplicationOperationId::new(format!("publish-fault-{label}")).unwrap(),
            id: request.id,
            destination_epoch: request.destination_epoch,
        };
        let error = store
            .publish_replica_bootstrap(publish.clone())
            .await
            .unwrap_err();
        assert!(matches!(
            (&error, committed),
            (ReplicationError::StorageFailure(_), false)
                | (ReplicationError::BootstrapPublishUnknown(_), true)
        ));
        assert_eq!(counters(&path).0, i64::from(!committed));
        store.publish_replica_bootstrap(publish).await.unwrap();
        assert_eq!(counters(&path), (0, 0));
        EventStore::close(&store).await.unwrap();
        let reopened = SqliteStore::open(options(&path)).await.unwrap();
        assert_eq!(counters(&path), (0, 0));
        EventStore::close(&reopened).await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn restore_accepts_matching_legacy_staging_copy_and_rejects_contradiction() {
    for (label, corrupt) in [("matching", false), ("contradictory", true)] {
        let root = std::env::temp_dir().join(format!(
            "sqlite-legacy-staging-copy-{label}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&root).unwrap();
        let source_path = root.join("source.sqlite3");
        let source = SqliteStore::open(options(&source_path)).await.unwrap();
        let request = begin(&source, if corrupt { 24 } else { 23 }).await;
        source
            .put_replica_bootstrap_batch(batch(&request, if corrupt { 34 } else { 33 }))
            .await
            .unwrap();
        publish(&source, &request).await;
        EventStore::close(&source).await.unwrap();

        let raw = rusqlite::Connection::open(&source_path).unwrap();
        raw.execute(
            "INSERT INTO replication_destination_bootstrap_records
             SELECT ?1,offset,event_id,schema_id,schema_version,payload
             FROM replication_destination_records
             WHERE origin_id=?2 AND public_id=?3 AND incarnation=?4",
            rusqlite::params![
                request.id.0.as_slice(),
                request.stream.origin.0.as_slice(),
                request.stream.stream.id.as_str(),
                request.stream.stream.incarnation.0.as_slice()
            ],
        )
        .unwrap();
        if corrupt {
            raw.execute(
                "UPDATE replication_destination_bootstrap_records
                 SET payload=zeroblob(length(payload)) WHERE bootstrap_id=?1",
                [request.id.0.as_slice()],
            )
            .unwrap();
        }
        let charge: i64 = raw
            .query_row(
                "SELECT octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384
                 FROM replication_destination_bootstrap_records WHERE bootstrap_id=?1",
                [request.id.0.as_slice()],
                |row| row.get(0),
            )
            .unwrap();
        raw.execute(
            "UPDATE replication_destination_accounting
             SET staging_record_count=1,staging_record_bytes=?1 WHERE singleton=1",
            [(charge as u64).to_be_bytes().as_slice()],
        )
        .unwrap();
        drop(raw);

        let manager = SqliteRestoreManager::new(
            SqliteRestoreBackend::new(&root).unwrap(),
            RestoreConfig::default(),
        )
        .unwrap();
        let result = manager
            .restore(RestoreRequest {
                operation_id: RestoreOperationId::new(format!("restore-{label}")).unwrap(),
                backup_identity: manager.inspect_backup(source_path.clone()).await.unwrap(),
                source: source_path,
                destination: PathBuf::from("restored.sqlite3"),
            })
            .await;
        if corrupt {
            assert!(matches!(result, Err(RestoreError::CorruptBackup(_))));
            assert!(!root.join("restored.sqlite3").exists());
        } else {
            let receipt = result.unwrap();
            let restored = SqliteStore::open(options(&receipt.destination))
                .await
                .unwrap();
            EventStore::close(&restored).await.unwrap();
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
