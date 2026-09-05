#![cfg(all(feature = "sqlite", feature = "replication"))]

use event_stream::infrastructure::{
    MemoryStore, MemoryStoreOptions, SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager,
    SqliteStore,
};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};

fn event(id: &str) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(id.as_bytes()),
    }
}

#[tokio::test]
async fn restore_replays_mapped_completed_bootstrap_receipts_without_reactivating_source() {
    let root = std::env::temp_dir().join(format!(
        "sqlite-bootstrap-receipt-restore-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root).unwrap();
    let source_path = root.join("source.sqlite3");
    let source = Arc::new(
        SqliteStore::open(SqliteOptions::new(&source_path))
            .await
            .unwrap(),
    );
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let key = source
        .create_if_absent(&StreamId::new("mapped-bootstrap").unwrap())
        .await
        .unwrap();
    source.append_atomic(&key, event("one")).await.unwrap();
    source.append_atomic(&key, event("two")).await.unwrap();
    let snapshot_bytes = b"state-through-one";
    let snapshot = SnapshotDescriptor {
        id: SnapshotId([41; 16]),
        covered: Cursor::new(key.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 2,
        },
        content_bytes: snapshot_bytes.len() as u64,
        digest: SnapshotDigest(Sha256::digest(snapshot_bytes).into()),
    };
    source.begin_snapshot(snapshot.clone()).await.unwrap();
    source
        .put_snapshot_chunk(
            snapshot.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(snapshot_bytes),
            },
        )
        .await
        .unwrap();
    source
        .verify_snapshot_step(
            snapshot.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    source.publish_snapshot(snapshot.id).await.unwrap();

    let stream = OriginStream {
        origin: source.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    let replica = ReplicaId::new("mapped-destination").unwrap();
    let epoch = destination.destination_epoch().await.unwrap();
    source
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("mapped-attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 64 * 1024,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::NeedsBootstrap,
        })
        .await
        .unwrap();
    let begin = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new("mapped-origin-begin").unwrap(),
        bootstrap_id: BootstrapId([42; 16]),
        destination_operation_id: ReplicationOperationId::new("mapped-destination-begin").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: epoch,
        snapshot: snapshot.clone(),
        captured_tail: ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    let publish = ReplicationOperationId::new("mapped-publish").unwrap();
    let acknowledge = ReplicationOperationId::new("mapped-acknowledge").unwrap();
    let driver = ReplicationDriver::open(
        source.clone(),
        destination,
        ReplicationDriverConfig {
            max_concurrent: 1,
            max_in_flight_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let completed = driver
        .bootstrap_once(
            begin.clone(),
            publish,
            acknowledge.clone(),
            ReplicationBootstrapDriveLimits {
                recovery_lifetime: Duration::from_secs(30),
                snapshot_page_bytes: 4,
                suffix_page: PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
                verification: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 4096,
                },
            },
        )
        .await
        .unwrap();
    driver.close();
    driver.wait_closed().await;
    drop(driver);
    EventStore::close(source.as_ref()).await.unwrap();
    drop(source);

    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&root).unwrap(),
        RestoreConfig::default(),
    )
    .unwrap();
    let receipt = manager
        .restore(RestoreRequest {
            operation_id: RestoreOperationId::new("mapped-bootstrap-restore").unwrap(),
            backup_identity: manager.inspect_backup(source_path.clone()).await.unwrap(),
            source: source_path,
            destination: PathBuf::from("restored.sqlite3"),
        })
        .await
        .unwrap();
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
        .find(|entry| entry.old == key)
        .unwrap();
    let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
        .await
        .unwrap();
    let mapped_stream = OriginStream {
        origin: restored.origin_identity().await.unwrap(),
        stream: mapping.new.clone(),
    };

    let mut mapped_begin = begin;
    mapped_begin.stream = mapped_stream.clone();
    mapped_begin.snapshot.covered.stream = mapping.new.clone();
    mapped_begin.captured_tail.stream = mapped_stream.clone();
    let begin_replay = restored
        .begin_origin_bootstrap(mapped_begin.clone())
        .await
        .unwrap();
    assert_eq!(begin_replay.request, mapped_begin);
    assert_eq!(begin_replay.status.mode, ReplicaMode::Bootstrapping);

    let mut mapped_destination_receipt = completed.destination;
    mapped_destination_receipt.request.stream = mapped_stream.clone();
    mapped_destination_receipt.request.snapshot.covered.stream = mapping.new;
    mapped_destination_receipt.request.through.stream = mapped_stream.clone();
    mapped_destination_receipt.committed_through.stream = mapped_stream.clone();
    let mapped_acknowledgement = AcknowledgeOriginBootstrap {
        operation_id: acknowledge,
        replica: replica.clone(),
        stream: mapped_stream.clone(),
        receipt: mapped_destination_receipt,
    };
    let ack_replay = restored
        .acknowledge_origin_bootstrap(mapped_acknowledgement.clone())
        .await
        .unwrap();
    assert_eq!(ack_replay.request, mapped_acknowledgement);
    assert_eq!(ack_replay.status.mode, ReplicaMode::Required);
    assert_eq!(ack_replay.status.acknowledged.offset, 2);
    assert_eq!(
        restored
            .replica_status(&replica, &mapped_stream)
            .await
            .unwrap()
            .mode,
        ReplicaMode::DetachedNeedsBootstrap,
        "historical receipt replay reactivated the restored replica"
    );
    EventStore::close(&restored).await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
