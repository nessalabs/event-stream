#![cfg(all(feature = "sqlite", feature = "replication"))]
use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
fn stream() -> OriginStream {
    OriginStream {
        origin: OriginId([1; 16]),
        stream: StreamKey {
            id: StreamId::new("leases").unwrap(),
            incarnation: IncarnationId([2; 16]),
        },
    }
}
#[tokio::test]
async fn sqlite_aborted_bootstrap_reopens_and_reclaims_bounded_chunks() {
    let directory = std::env::temp_dir().join(format!("sqlite-cleanup-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let database = directory.join("destination.db");
    let mut options = SqliteOptions::new(&database);
    options.replica_destination.staging.max_staging_bytes = 16 * 1024;
    options.replica_destination.staging.max_chunk_bytes = 4096;
    let store = SqliteStore::open(options.clone()).await.unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
        .await
        .unwrap();
    let stream = stream();
    let bytes = vec![42; 3 * 4096];
    let request = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new("abort-begin").unwrap(),
        id: BootstrapId([9; 16]),
        replica: ReplicaId::new("reader").unwrap(),
        destination_epoch: epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([9; 16]),
            covered: Cursor::new(stream.stream.clone(), 9),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: bytes.len() as u64,
            digest: SnapshotDigest(Sha256::digest(&bytes).into()),
        },
        through: ReplicaPosition { stream, offset: 9 },
    };
    store
        .begin_replica_bootstrap(request.clone())
        .await
        .unwrap();
    for (index, chunk) in bytes.chunks(4096).enumerate() {
        store
            .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
                id: request.id,
                chunk: SnapshotChunk {
                    offset: (index * 4096) as u64,
                    bytes: Payload::copy_from_slice(chunk),
                },
            })
            .await
            .unwrap();
    }
    store
        .abort_replica_bootstrap(AbortReplicaBootstrap {
            operation_id: ReplicationOperationId::new("abort").unwrap(),
            id: request.id,
            destination_epoch: epoch,
        })
        .await
        .unwrap();
    store.close().await.unwrap();
    drop(store);
    let store = SqliteStore::open(options).await.unwrap();
    let mut complete = false;
    let mut total_removed = 0;
    for _ in 0..16 {
        let progress = store
            .cleanup_replica_destination(ReplicaCleanupLimits {
                max_receipt_rows: 1,
                max_staging_rows: 1,
                max_bytes: 8192,
            })
            .await
            .unwrap();
        assert!(progress.removed_staging_rows <= 1);
        assert!(progress.removed_bytes <= 8192);
        assert!(
            progress.removed_staging_rows > 0 || !progress.remaining,
            "an eligible chunk fits but cleanup made no progress: {progress:?}"
        );
        total_removed += progress.removed_bytes;
        if !progress.remaining {
            complete = true;
            break;
        }
    }
    assert!(complete);
    assert!(total_removed >= bytes.len());
    // Keep the compact operation receipt while reclaiming the large upload.
    store
        .abort_replica_bootstrap(AbortReplicaBootstrap {
            operation_id: ReplicationOperationId::new("abort").unwrap(),
            id: request.id,
            destination_epoch: epoch,
        })
        .await
        .unwrap();
    let mut replacement = request;
    replacement.id = BootstrapId([10; 16]);
    replacement.operation_id = ReplicationOperationId::new("replacement").unwrap();
    replacement.snapshot.id = SnapshotId([10; 16]);
    store
        .begin_replica_bootstrap(replacement.clone())
        .await
        .unwrap();
    for (index, chunk) in bytes.chunks(4096).enumerate() {
        store
            .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
                id: replacement.id,
                chunk: SnapshotChunk {
                    offset: (index * 4096) as u64,
                    bytes: Payload::copy_from_slice(chunk),
                },
            })
            .await
            .unwrap();
    }
    store.close().await.unwrap();
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

async fn publish(store: &SqliteStore, id: u8, bytes: &[u8]) {
    let stream = stream();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(store)
        .await
        .unwrap();
    let bootstrap = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new(format!("begin-{id}")).unwrap(),
        id: BootstrapId([id; 16]),
        replica: ReplicaId::new("reader").unwrap(),
        destination_epoch: epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([id; 16]),
            covered: Cursor::new(stream.stream.clone(), id as u64),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: bytes.len() as u64,
            digest: SnapshotDigest(Sha256::digest(bytes).into()),
        },
        through: ReplicaPosition {
            stream,
            offset: id as u64,
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
                bytes: Payload::copy_from_slice(bytes),
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
                    max_bytes: 4096
                },
            })
            .await
            .unwrap()
            .complete
    );
    store
        .publish_replica_bootstrap(PublishReplicaBootstrap {
            operation_id: ReplicationOperationId::new(format!("publish-{id}")).unwrap(),
            id: bootstrap.id,
            destination_epoch: epoch,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn sqlite_replaced_snapshot_keeps_reader_then_reuses_capacity() {
    let directory =
        std::env::temp_dir().join(format!("sqlite-replacement-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let mut options = SqliteOptions::new(directory.join("destination.db"));
    options.replica_destination.staging.max_chunk_bytes = 8;
    options.replica_destination.staging.max_staging_bytes = 16;
    let store = SqliteStore::open(options).await.unwrap();
    publish(&store, 6, b"old-AAAA").await;
    let reader = store
        .acquire_replica_bootstrap_read(&stream(), Duration::from_secs(30))
        .await
        .unwrap();
    publish(&store, 7, b"new-BBBB").await;
    assert_eq!(
        store
            .read_replica_bootstrap_bytes(reader.lease, 0, 8)
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        b"old-AAAA"
    );
    store
        .release_replica_bootstrap_read(reader.lease)
        .await
        .unwrap();
    for _ in 0..16 {
        let progress = store
            .cleanup_replica_destination(ReplicaCleanupLimits {
                max_receipt_rows: 1,
                max_staging_rows: 1,
                max_bytes: 4096,
            })
            .await
            .unwrap();
        assert!(progress.removed_staging_rows <= 1);
        assert!(progress.removed_bytes <= 4096);
        if !progress.remaining {
            break;
        }
    }
    // Three replacements total 24 bytes, but only 16 may be retained at once.
    // Cleanup must release the retired version so this publication can proceed.
    publish(&store, 8, b"last-CCC").await;
    let latest = store
        .acquire_replica_bootstrap_read(&stream(), Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(
        store
            .read_replica_bootstrap_bytes(latest.lease, 0, 8)
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        b"last-CCC"
    );
    store.close().await.unwrap();
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[derive(Debug)]
struct Clock(AtomicU64);
impl DurableReplicationClock for Clock {
    fn now(&self) -> DurableTimestampMillis {
        DurableTimestampMillis(self.0.load(Ordering::Relaxed))
    }
}
#[tokio::test]
async fn sqlite_expired_reader_slot_is_reclaimed_by_bounded_cleanup() {
    let clock = Arc::new(Clock(AtomicU64::new(100)));
    let directory =
        std::env::temp_dir().join(format!("sqlite-lease-expiry-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let mut options = SqliteOptions::new(directory.join("destination.db"));
    options.replication_clock = clock.clone();
    options.replica_destination.reads.max_leases = 1;
    let store = SqliteStore::open(options).await.unwrap();
    publish(&store, 5, b"state").await;
    let _abandoned = store
        .acquire_replica_bootstrap_read(&stream(), Duration::from_millis(100))
        .await
        .unwrap();
    assert!(matches!(
        store
            .acquire_replica_bootstrap_read(&stream(), Duration::from_millis(100))
            .await,
        Err(ReplicationError::CapacityExceeded)
    ));
    clock.0.store(200, Ordering::Relaxed);
    let mut removed_leases = 0;
    for _ in 0..4 {
        let progress = store
            .cleanup_replica_destination(ReplicaCleanupLimits {
                max_receipt_rows: 1,
                max_staging_rows: 1,
                max_bytes: 4096,
            })
            .await
            .unwrap();
        assert!(progress.removed_receipt_rows <= 1);
        assert!(progress.removed_staging_rows <= 1);
        removed_leases += progress.removed_staging_rows;
        assert!(progress.removed_bytes <= 4096);
        if !progress.remaining {
            break;
        }
    }
    assert_eq!(
        removed_leases, 1,
        "cleanup itself must reclaim the expired slot"
    );
    let renewed = store
        .acquire_replica_bootstrap_read(&stream(), Duration::from_millis(100))
        .await;
    assert!(
        renewed.is_ok(),
        "expired abandoned lease still consumes the only slot: {renewed:?}"
    );
    store.close().await.unwrap();
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn restore_accepts_replaced_snapshot_after_retired_bytes_are_cleaned() {
    use event_stream::infrastructure::{SqliteRestoreBackend, SqliteRestoreManager};
    let directory =
        std::env::temp_dir().join(format!("sqlite-retired-restore-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let source = directory.join("backup.db");
    let store = SqliteStore::open(SqliteOptions::new(&source))
        .await
        .unwrap();
    publish(&store, 11, b"old-state").await;
    publish(&store, 12, b"new-state").await;
    let mut removed = 0;
    for _ in 0..16 {
        let progress = store
            .cleanup_replica_destination(ReplicaCleanupLimits {
                max_receipt_rows: 1,
                max_staging_rows: 1,
                max_bytes: 4096,
            })
            .await
            .unwrap();
        removed += progress.removed_bytes;
        if !progress.remaining {
            break;
        }
    }
    assert!(removed >= b"old-state".len());
    store.close().await.unwrap();
    drop(store);
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&directory).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("restore-retired").unwrap(),
            backup_identity: manager.inspect_backup(source.clone()).await.unwrap(),
            source,
            destination: "restored.db".into(),
        })
        .await
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(receipt.destination))
        .await
        .unwrap();
    let reader = restored
        .acquire_replica_bootstrap_read(&stream(), Duration::from_secs(30))
        .await
        .unwrap();
    let page = restored
        .read_replica_bootstrap_bytes(reader.lease, 0, 4096)
        .await
        .unwrap();
    assert_eq!(page.bytes.as_bytes(), b"new-state");
    restored
        .release_replica_bootstrap_read(reader.lease)
        .await
        .unwrap();
    restored.close().await.unwrap();
    drop(restored);
    std::fs::remove_dir_all(directory).unwrap();
}
