#![cfg(feature = "replication")]

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Debug)]
struct Clock(AtomicU64);
impl DurableReplicationClock for Clock {
    fn now(&self) -> DurableTimestampMillis {
        DurableTimestampMillis(self.0.load(Ordering::Relaxed))
    }
}
fn stream() -> OriginStream {
    OriginStream {
        origin: OriginId([1; 16]),
        stream: StreamKey {
            id: StreamId::new("leases").unwrap(),
            incarnation: IncarnationId([2; 16]),
        },
    }
}
async fn publish(store: &MemoryStore, id: u8, bytes: &[u8]) {
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
async fn reader_keeps_original_snapshot_across_replacement_until_exact_expiry() {
    let clock = Arc::new(Clock(AtomicU64::new(100)));
    let options = MemoryStoreOptions {
        replication_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    };
    let store = MemoryStore::open(options).await.unwrap();
    publish(&store, 3, b"old-AAAA").await;
    let old = store
        .acquire_replica_bootstrap_read(&stream(), Duration::from_millis(100))
        .await
        .unwrap();
    let prefix = store
        .read_replica_bootstrap_bytes(old.lease, 0, 4)
        .await
        .unwrap();
    assert_eq!(prefix.bytes.as_bytes(), b"old-");
    clock.0.store(110, Ordering::Relaxed);
    publish(&store, 4, b"new-BBBB").await;
    let new = store
        .acquire_replica_bootstrap_read(&stream(), Duration::from_millis(200))
        .await
        .unwrap();
    assert_eq!(new.published.request.id, BootstrapId([4; 16]));
    let old_suffix = store
        .read_replica_bootstrap_bytes(old.lease, 4, 8)
        .await
        .unwrap();
    assert_eq!(old_suffix.id, BootstrapId([3; 16]));
    assert_eq!(old_suffix.bytes.as_bytes(), b"AAAA");
    clock.0.store(200, Ordering::Relaxed);
    assert!(matches!(
        store.read_replica_bootstrap_bytes(old.lease, 0, 4).await,
        Err(ReplicationError::ReadLeaseExpired { .. })
    ));
    assert_eq!(
        store
            .read_replica_bootstrap_bytes(new.lease, 0, 16)
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        b"new-BBBB"
    );
    assert_eq!(
        store
            .release_replica_bootstrap_read(new.lease)
            .await
            .unwrap(),
        ReplicaReadRelease::Released
    );
    assert_eq!(
        store
            .release_replica_bootstrap_read(new.lease)
            .await
            .unwrap(),
        ReplicaReadRelease::AlreadyReleased
    );
}

#[tokio::test]
async fn abandoned_expired_reader_slot_is_reclaimed_by_bounded_cleanup() {
    let clock = Arc::new(Clock(AtomicU64::new(100)));
    let mut options = MemoryStoreOptions {
        replication_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    };
    options.replica_destination.reads.max_leases = 1;
    let store = MemoryStore::open(options).await.unwrap();
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
}

#[tokio::test]
async fn released_replaced_snapshot_capacity_is_reusable() {
    let mut options = MemoryStoreOptions::default();
    options.replica_destination.staging.max_chunk_bytes = 8;
    options.replica_destination.staging.max_staging_bytes = 16;
    let store = MemoryStore::open(options).await.unwrap();
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
}

#[tokio::test]
async fn aborted_bootstrap_larger_than_cleanup_byte_budget_is_removed_in_chunks() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
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
}
