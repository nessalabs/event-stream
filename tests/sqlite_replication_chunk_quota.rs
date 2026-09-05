#![cfg(all(feature = "sqlite", feature = "replication"))]

use event_stream::infrastructure::{
    SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteStore,
};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

fn options(path: &Path) -> SqliteOptions {
    let mut options = SqliteOptions::new(path);
    options.replica_destination.staging.max_staging_chunks = 2;
    options.replica_destination.staging.max_staging_bytes = 6;
    options.replica_destination.staging.max_chunk_bytes = 3;
    options
}

async fn begin(store: &SqliteStore, id: u8, operation: &str) -> ReplicaBootstrap {
    let key = StreamKey {
        id: StreamId::new("chunk-quota").unwrap(),
        incarnation: IncarnationId([7; 16]),
    };
    let stream = OriginStream {
        origin: OriginId([8; 16]),
        stream: key.clone(),
    };
    let bytes = b"abcdefghi";
    let request = ReplicaBootstrap {
        id: BootstrapId([id; 16]),
        operation_id: ReplicationOperationId::new(operation).unwrap(),
        replica: ReplicaId::new("quota-replica").unwrap(),
        destination_epoch: store.destination_epoch().await.unwrap(),
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([id.wrapping_add(40); 16]),
            covered: Cursor::new(key, 0),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: bytes.len() as u64,
            digest: SnapshotDigest(Sha256::digest(bytes).into()),
        },
        through: ReplicaPosition { stream, offset: 0 },
    };
    store
        .begin_replica_bootstrap(request.clone())
        .await
        .unwrap();
    request
}

async fn put(
    store: &SqliteStore,
    id: BootstrapId,
    offset: u64,
    bytes: &[u8],
) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
    store
        .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
            id,
            chunk: SnapshotChunk {
                offset,
                bytes: Payload::copy_from_slice(bytes),
            },
        })
        .await
}

fn counters(path: &Path) -> (i64, Vec<u8>) {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row(
            "SELECT staging_chunk_count,staging_chunk_bytes
             FROM replication_destination_accounting WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap()
}

#[tokio::test]
async fn chunk_quota_survives_abort_bounded_cleanup_and_reopen() {
    let root = std::env::temp_dir().join(format!("replica-chunk-quota-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("store.sqlite3");
    let store = SqliteStore::open(options(&path)).await.unwrap();
    let first = begin(&store, 1, "begin-one").await;
    put(&store, first.id, 0, b"abc").await.unwrap();
    put(&store, first.id, 3, b"def").await.unwrap();
    assert_eq!(put(&store, first.id, 0, b"abc").await.unwrap().end, 3);
    assert_eq!(
        put(&store, first.id, 0, b"abd").await.unwrap_err(),
        ReplicationError::InvalidInput("bootstrap chunk retry differs".into())
    );
    assert_eq!(
        put(&store, first.id, 6, b"ghi").await.unwrap_err(),
        ReplicationError::CapacityExceeded
    );
    EventStore::close(&store).await.unwrap();
    assert_eq!(counters(&path), (2, 6u64.to_be_bytes().to_vec()));

    let store = SqliteStore::open(options(&path)).await.unwrap();
    assert_eq!(
        put(&store, first.id, 6, b"ghi").await.unwrap_err(),
        ReplicationError::CapacityExceeded
    );
    store
        .abort_replica_bootstrap(AbortReplicaBootstrap {
            operation_id: ReplicationOperationId::new("abort-one").unwrap(),
            id: first.id,
            destination_epoch: first.destination_epoch,
        })
        .await
        .unwrap();
    let second = begin(&store, 2, "begin-two").await;
    assert_eq!(
        put(&store, second.id, 0, b"abc").await.unwrap_err(),
        ReplicationError::CapacityExceeded,
        "abort released physical chunk capacity before cleanup"
    );
    let cleaned = store
        .cleanup_replica_destination(ReplicaCleanupLimits {
            max_receipt_rows: 1,
            max_staging_rows: 1,
            max_bytes: 3,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_staging_rows, 1);
    EventStore::close(&store).await.unwrap();
    assert_eq!(counters(&path), (1, 3u64.to_be_bytes().to_vec()));

    let store = SqliteStore::open(options(&path)).await.unwrap();
    put(&store, second.id, 0, b"abc").await.unwrap();
    assert_eq!(
        put(&store, second.id, 3, b"def").await.unwrap_err(),
        ReplicationError::CapacityExceeded
    );
    EventStore::close(&store).await.unwrap();
    assert_eq!(counters(&path), (2, 6u64.to_be_bytes().to_vec()));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restore_rebuilds_chunk_quota_and_corrupt_counters_fail_open() {
    let root = std::env::temp_dir().join(format!("replica-chunk-restore-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let source_path = root.join("source.sqlite3");
    let source = SqliteStore::open(options(&source_path)).await.unwrap();
    let upload = begin(&source, 3, "begin-source").await;
    put(&source, upload.id, 0, b"abc").await.unwrap();
    put(&source, upload.id, 3, b"def").await.unwrap();
    EventStore::close(&source).await.unwrap();

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("restore-chunk-quota").unwrap(),
            backup_identity: manager.inspect_backup(source_path.clone()).await.unwrap(),
            source: source_path,
            destination: PathBuf::from("restored.sqlite3"),
        })
        .await
        .unwrap();
    let restored = SqliteStore::open(options(&receipt.destination))
        .await
        .unwrap();
    EventStore::close(&restored).await.unwrap();
    assert_eq!(
        counters(&receipt.destination),
        (2, 6u64.to_be_bytes().to_vec())
    );

    let raw = rusqlite::Connection::open(&receipt.destination).unwrap();
    raw.execute(
        "UPDATE replication_destination_accounting SET staging_chunk_count=1 WHERE singleton=1",
        [],
    )
    .unwrap();
    drop(raw);
    assert!(matches!(
        SqliteStore::open(options(&receipt.destination)).await,
        Err(Error::StoreCorrupt(message)) if message.contains("chunk counters")
    ));
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn legacy_destination_accounting_adds_exact_chunk_counters() {
    let root =
        std::env::temp_dir().join(format!("replica-chunk-migration-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("store.sqlite3");
    let store = SqliteStore::open(options(&path)).await.unwrap();
    let upload = begin(&store, 4, "begin-migration").await;
    put(&store, upload.id, 0, b"abc").await.unwrap();
    EventStore::close(&store).await.unwrap();

    let raw = rusqlite::Connection::open(&path).unwrap();
    raw.execute_batch(
        "ALTER TABLE replication_destination_accounting DROP COLUMN staging_chunk_bytes;
         ALTER TABLE replication_destination_accounting DROP COLUMN staging_chunk_count;",
    )
    .unwrap();
    drop(raw);

    let reopened = SqliteStore::open(options(&path)).await.unwrap();
    EventStore::close(&reopened).await.unwrap();
    assert_eq!(counters(&path), (1, 3u64.to_be_bytes().to_vec()));
    std::fs::remove_dir_all(root).unwrap();
}
