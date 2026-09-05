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
#[cfg(feature = "replication")]
#[tokio::test]
async fn restore_rejects_corrupt_published_replica_snapshot() {
    reject_corruption(
        "UPDATE replication_destination_bootstrap_chunks SET bytes=zeroblob(length(bytes))",
    )
    .await;
}
#[tokio::test]
async fn restore_rejects_missing_published_snapshot_chunk() {
    reject_corruption("DELETE FROM replication_destination_bootstrap_chunks").await;
}
#[tokio::test]
async fn restore_rejects_shifted_published_snapshot_chunk() {
    reject_corruption(
        "UPDATE replication_destination_bootstrap_chunks SET offset=x'0000000000000001'",
    )
    .await;
}
#[tokio::test]
async fn restore_rejects_missing_published_suffix_record() {
    reject_corruption("DELETE FROM replication_destination_records").await;
}
async fn reject_corruption(sql: &str) {
    restore_fixture(Some(sql), UploadStage::Published).await;
}
#[tokio::test]
async fn restore_accepts_partial_snapshot_upload() {
    restore_fixture(None, UploadStage::Partial).await;
}
#[tokio::test]
async fn restore_rejects_partial_upload_counter_mismatch() {
    restore_fixture(
        Some("UPDATE replication_destination_bootstraps SET accepted_bytes=zeroblob(8)"),
        UploadStage::Partial,
    )
    .await;
}
#[tokio::test]
async fn restore_rejects_partial_upload_chunk_gap() {
    restore_fixture(
        Some("UPDATE replication_destination_bootstrap_chunks SET offset=x'0000000000000001'"),
        UploadStage::Partial,
    )
    .await;
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum UploadStage {
    Partial,
    Verified,
    Published,
}

#[tokio::test]
async fn restore_accepts_verified_unpublished_snapshot() {
    restore_fixture(None, UploadStage::Verified).await;
}

#[tokio::test]
async fn restore_rejects_changed_verified_unpublished_snapshot() {
    restore_fixture(
        Some("UPDATE replication_destination_bootstrap_chunks SET bytes=zeroblob(length(bytes))"),
        UploadStage::Verified,
    )
    .await;
}

async fn restore_fixture(sql: Option<&str>, stage: UploadStage) {
    use event_stream::{
        BatchId, BootstrapId, Cursor, IncarnationId, OriginId, OriginStream,
        PublishReplicaBootstrap, Record, ReplicaBatch, ReplicaBatchDestinationStore,
        ReplicaBootstrap, ReplicaBootstrapBatch, ReplicaBootstrapChunk,
        ReplicaBootstrapVerificationLimits, ReplicaDestinationStore, ReplicaId, ReplicaPosition,
        ReplicationOperationId, SnapshotChunk, SnapshotDescriptor, SnapshotDigest, SnapshotId,
        StreamKey, VerifyReplicaBootstrap,
    };
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    let root = temp_root("replication-destination-roundtrip");
    let source = root.join("backup.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    let old_epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
        .await
        .unwrap();
    let stream = OriginStream {
        origin: OriginId([41; 16]),
        stream: StreamKey {
            id: StreamId::new("remote-stream").unwrap(),
            incarnation: IncarnationId([42; 16]),
        },
    };
    let content = b"restored opaque replica snapshot";
    let bootstrap = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new("restore-destination-begin").unwrap(),
        id: BootstrapId([43; 16]),
        replica: ReplicaId::new("restored-replica").unwrap(),
        destination_epoch: old_epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([44; 16]),
            covered: Cursor::new(stream.stream.clone(), 1),
            schema: SchemaRef {
                id: SchemaId::new("remote-state").unwrap(),
                version: 1,
            },
            content_bytes: content.len() as u64,
            digest: SnapshotDigest(Sha256::digest(content).into()),
        },
        through: ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    store
        .begin_replica_bootstrap(bootstrap.clone())
        .await
        .unwrap();
    store
        .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
            id: bootstrap.id,
            chunk: SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(if stage == UploadStage::Partial {
                    &content[..4]
                } else {
                    content
                }),
            },
        })
        .await
        .unwrap();
    if stage != UploadStage::Partial {
        store
            .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
                id: bootstrap.id,
                batch: ReplicaBatch {
                    id: BatchId([45; 16]),
                    destination_epoch: old_epoch,
                    after: ReplicaPosition {
                        stream: stream.clone(),
                        offset: 1,
                    },
                    records: vec![Arc::new(Record {
                        cursor: Cursor::new(stream.stream.clone(), 2),
                        event: event("restored-suffix", b"suffix"),
                    })],
                },
            })
            .await
            .unwrap();
        assert!(
            store
                .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                    id: bootstrap.id,
                    limits: ReplicaBootstrapVerificationLimits {
                        max_chunks: 1,
                        max_records: 1,
                        max_bytes: 4096,
                    },
                })
                .await
                .unwrap()
                .complete
        );
        if stage == UploadStage::Published {
            store
                .publish_replica_bootstrap(PublishReplicaBootstrap {
                    operation_id: ReplicationOperationId::new("restore-destination-publish")
                        .unwrap(),
                    id: bootstrap.id,
                    destination_epoch: old_epoch,
                })
                .await
                .unwrap();
        }
    }
    store.close().await.unwrap();
    let connection = rusqlite::Connection::open(&source).unwrap();
    if let Some(sql) = sql {
        connection.execute(sql, []).unwrap();
    }
    drop(connection);

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("restore-replication-destination").unwrap(),
        backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
        source,
        destination: PathBuf::from("restored.sqlite3"),
    };
    let result = manager.restore(request).await;
    if let Some(sql) = sql {
        assert!(
            matches!(&result, Err(RestoreError::CorruptBackup(_))),
            "corrupt backup was restored after {sql}: {result:?}"
        );
        assert!(
            !root.join("restored.sqlite3").exists(),
            "invalid restore destination was published"
        );
    } else {
        let receipt = result.expect("valid partial upload must restore");
        let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
            .await
            .unwrap();
        assert!(matches!(
            restored.begin_replica_bootstrap(bootstrap).await,
            Err(ReplicationError::DestinationReplaced { .. })
        ));
        let connection = rusqlite::Connection::open(&receipt.destination).unwrap();
        let (state, accepted): (i64, Vec<u8>) = connection
            .query_row(
                "SELECT state, accepted_bytes FROM replication_destination_bootstraps",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            state, 3,
            "restore must abort uploads from the old destination epoch"
        );
        assert_eq!(
            accepted,
            (if stage == UploadStage::Partial {
                4
            } else {
                content.len() as u64
            })
            .to_be_bytes()
        );
        drop(connection);
        restored.close().await.unwrap();
    }
    std::fs::remove_dir_all(root).unwrap();
}
