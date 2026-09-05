#![cfg(feature = "replication")]

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::time::Duration;

fn event(id: &str) -> GeneratedEvent {
    GeneratedEvent {
        generation: RetryGeneration::FIRST,
        event: NewEvent {
            id: EventId::new(id).unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(id.as_bytes()),
        },
    }
}

#[tokio::test]
async fn bootstrap_transfers_real_snapshot_and_suffix_then_drains_new_local_writes() {
    let origin = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let destination = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let key = origin
        .create_if_absent(&StreamId::new("origin").unwrap())
        .await
        .unwrap();
    origin
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: key.clone(),
        })
        .await
        .unwrap();
    origin.append_generated(&key, event("a")).await.unwrap();
    origin.append_generated(&key, event("b")).await.unwrap();
    let bytes = b"state-through-a";
    let snapshot = SnapshotDescriptor {
        id: SnapshotId([1; 16]),
        covered: Cursor::new(key.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: bytes.len() as u64,
        digest: SnapshotDigest(Sha256::digest(bytes).into()),
    };
    origin.begin_snapshot(snapshot.clone()).await.unwrap();
    origin
        .put_snapshot_chunk(
            snapshot.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(bytes),
            },
        )
        .await
        .unwrap();
    origin
        .verify_snapshot_step(
            snapshot.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    origin.publish_snapshot(snapshot.id).await.unwrap();
    let stream = OriginStream {
        origin: origin.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    let replica = ReplicaId::new("destination").unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(&destination)
        .await
        .unwrap();
    origin
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 100_000,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::NeedsBootstrap,
        })
        .await
        .unwrap();
    let begin = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new("origin-begin").unwrap(),
        bootstrap_id: BootstrapId([2; 16]),
        destination_operation_id: ReplicationOperationId::new("destination-begin").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: epoch,
        snapshot: snapshot.clone(),
        captured_tail: ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    let begun = origin.begin_origin_bootstrap(begin.clone()).await.unwrap();
    assert_eq!(
        origin.begin_origin_bootstrap(begin.clone()).await.unwrap(),
        begun
    );
    origin.append_generated(&key, event("c")).await.unwrap();
    assert_eq!(
        origin
            .replica_status(&replica, &stream)
            .await
            .unwrap()
            .backlog_records,
        2
    );
    assert!(matches!(
        origin
            .advance_retention_floor(AdvanceRetentionFloor {
                operation_id: RetentionOperationId::new("protected").unwrap(),
                stream: key.clone(),
                expected_floor: Cursor::new(key.clone(), 0),
                new_floor: Cursor::new(key.clone(), 2),
            })
            .await,
        Err(RetentionError::ReplicaProtectionActive { .. })
    ));
    let request = ReplicaBootstrap {
        operation_id: begin.destination_operation_id.clone(),
        id: begin.bootstrap_id,
        replica: replica.clone(),
        destination_epoch: epoch,
        stream: stream.clone(),
        snapshot: snapshot.clone(),
        through: begin.captured_tail.clone(),
    };
    destination.begin_replica_bootstrap(request).await.unwrap();
    // The transfer reads actual stored bytes and records from the origin.
    let recovery = origin
        .acquire_recovery(snapshot.id, Duration::from_secs(30))
        .await
        .unwrap();
    let mut offset = 0;
    while offset < snapshot.content_bytes {
        let page = origin
            .read_snapshot_chunk(recovery.lease, offset, 4)
            .await
            .unwrap();
        destination
            .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
                id: begin.bootstrap_id,
                chunk: SnapshotChunk {
                    offset,
                    bytes: page.bytes,
                },
            })
            .await
            .unwrap();
        offset = page.next_offset;
    }
    let page = origin
        .read_recovery_page(
            recovery.lease,
            1,
            PageLimits {
                max_records: 1,
                max_bytes: 2 * 1024 * 1024,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].cursor.offset, 2);
    destination
        .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
            id: begin.bootstrap_id,
            batch: ReplicaBatch {
                id: BatchId([3; 16]),
                destination_epoch: epoch,
                after: ReplicaPosition {
                    stream: stream.clone(),
                    offset: 1,
                },
                records: page.records,
            },
        })
        .await
        .unwrap();
    origin.release_recovery(recovery.lease).await.unwrap();
    let mut complete = false;
    for _ in 0..8 {
        complete = destination
            .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                id: begin.bootstrap_id,
                limits: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 4096,
                },
            })
            .await
            .unwrap()
            .complete;
        if complete {
            break;
        }
    }
    assert!(complete);
    let committed = destination
        .publish_replica_bootstrap(PublishReplicaBootstrap {
            operation_id: ReplicationOperationId::new("publish").unwrap(),
            id: begin.bootstrap_id,
            destination_epoch: epoch,
        })
        .await
        .unwrap();
    let ack = AcknowledgeOriginBootstrap {
        operation_id: ReplicationOperationId::new("origin-ack").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        receipt: committed,
    };
    let mut forged = ack.clone();
    forged.receipt.request.id = BootstrapId([99; 16]);
    assert!(matches!(
        origin.acknowledge_origin_bootstrap(forged).await,
        Err(ReplicationError::InvalidReceipt(_))
    ));
    let mut wrong_stream = ack.clone();
    wrong_stream.receipt.request.stream.origin = OriginId([98; 16]);
    assert!(matches!(
        origin.acknowledge_origin_bootstrap(wrong_stream).await,
        Err(ReplicationError::InvalidReceipt(_))
    ));
    let acknowledged = origin
        .acknowledge_origin_bootstrap(ack.clone())
        .await
        .unwrap();
    assert_eq!(acknowledged.status.acknowledged.offset, 2);
    assert_eq!(acknowledged.status.backlog_records, 1);
    assert_eq!(
        origin.acknowledge_origin_bootstrap(ack).await.unwrap(),
        acknowledged
    );
    let batch = origin
        .prepare_replica_batch(PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new("prepare-live").unwrap(),
            batch_id: BatchId([4; 16]),
            replica: replica.clone(),
            stream: stream.clone(),
            expected_after: begin.captured_tail,
            limits: ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        })
        .await
        .unwrap()
        .batch
        .unwrap();
    assert_eq!(batch.records[0].event.payload.as_bytes(), b"c");
    let after = batch.after.clone();
    let receipt = destination.commit_replica_batch(batch).await.unwrap();
    let drained = origin
        .acknowledge_replica_batch(AcknowledgeReplicaBatch {
            operation_id: ReplicationOperationId::new("ack-live").unwrap(),
            replica,
            expected_after: after,
            receipt,
        })
        .await
        .unwrap();
    assert_eq!(drained.status.backlog_records, 0);
    assert_eq!(drained.status.acknowledged.offset, 3);
}

#[derive(Debug)]
struct SnapshotClock(std::sync::atomic::AtomicU64);
impl MonotonicClock for SnapshotClock {
    fn now(&self) -> MonotonicTick {
        MonotonicTick(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }
}

#[tokio::test]
async fn origin_rejects_bootstrap_ack_at_its_protection_deadline() {
    let clock = std::sync::Arc::new(SnapshotClock(std::sync::atomic::AtomicU64::new(100)));
    let mut options = MemoryStoreOptions {
        snapshot_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    };
    options.snapshots.recovery.max_lifetime = Duration::from_nanos(100);
    let store = MemoryStore::open(options).await.unwrap();
    let key = store
        .create_if_absent(&StreamId::new("expiry").unwrap())
        .await
        .unwrap();
    store.append_atomic(&key, event("a").event).await.unwrap();
    let snapshot = SnapshotDescriptor {
        id: SnapshotId([21; 16]),
        covered: Cursor::new(key.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: 1,
        digest: SnapshotDigest(Sha256::digest(b"s").into()),
    };
    store.begin_snapshot(snapshot.clone()).await.unwrap();
    store
        .put_snapshot_chunk(
            snapshot.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(b"s"),
            },
        )
        .await
        .unwrap();
    store
        .verify_snapshot_step(
            snapshot.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1024,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(snapshot.id).await.unwrap();
    let stream = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: key,
    };
    let replica = ReplicaId::new("remote").unwrap();
    let epoch = DestinationEpoch([22; 16]);
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach-expiry").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::NeedsBootstrap,
        })
        .await
        .unwrap();
    let begin = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new("begin-expiry").unwrap(),
        bootstrap_id: BootstrapId([23; 16]),
        destination_operation_id: ReplicationOperationId::new("destination-expiry").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: epoch,
        snapshot: snapshot.clone(),
        captured_tail: ReplicaPosition {
            stream: stream.clone(),
            offset: 1,
        },
    };
    let receipt = store.begin_origin_bootstrap(begin.clone()).await.unwrap();
    assert_eq!(receipt.protection_expires_at, MonotonicTick(200));
    clock.0.store(200, std::sync::atomic::Ordering::Relaxed);
    // A fabricated matching destination receipt isolates expiry validation.
    // The real transport/storage success path is covered by the other integration test.
    let result = store
        .acknowledge_origin_bootstrap(AcknowledgeOriginBootstrap {
            operation_id: ReplicationOperationId::new("ack-expiry").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            receipt: ReplicaBootstrapReceipt {
                request: ReplicaBootstrap {
                    operation_id: begin.destination_operation_id.clone(),
                    id: begin.bootstrap_id,
                    replica: replica.clone(),
                    destination_epoch: epoch,
                    stream: stream.clone(),
                    snapshot,
                    through: begin.captured_tail.clone(),
                },
                committed_through: begin.captured_tail.clone(),
            },
        })
        .await;
    assert!(
        matches!(
            result,
            Err(ReplicationError::BootstrapProtectionExpired { .. })
        ),
        "expired protection accepted a bootstrap acknowledgement: {result:?}"
    );
    assert_ne!(
        store.replica_status(&replica, &stream).await.unwrap().mode,
        ReplicaMode::Required
    );
    let mut replacement = begin.clone();
    replacement.operation_id = ReplicationOperationId::new("replacement-begin").unwrap();
    replacement.bootstrap_id = BootstrapId([24; 16]);
    replacement.destination_operation_id =
        ReplicationOperationId::new("replacement-destination").unwrap();
    let replacement_receipt = store
        .begin_origin_bootstrap(replacement.clone())
        .await
        .unwrap();
    assert_eq!(
        replacement_receipt.protection_expires_at,
        MonotonicTick(300)
    );
    let before = store.replica_status(&replica, &stream).await.unwrap();
    assert!(matches!(
        store.begin_origin_bootstrap(begin).await,
        Err(ReplicationError::BootstrapProtectionExpired { .. })
    ));
    assert_eq!(
        store.replica_status(&replica, &stream).await.unwrap(),
        before
    );
    assert_eq!(
        store.begin_origin_bootstrap(replacement).await.unwrap(),
        replacement_receipt
    );
}

#[tokio::test]
async fn driver_bootstrap_transfers_real_pages_and_retries_exact_completion() {
    let origin = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let destination = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let key = origin
        .create_if_absent(&StreamId::new("origin").unwrap())
        .await
        .unwrap();
    origin
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: key.clone(),
        })
        .await
        .unwrap();
    origin.append_generated(&key, event("a")).await.unwrap();
    origin.append_generated(&key, event("b")).await.unwrap();
    let bytes = b"state-through-a";
    let snapshot = SnapshotDescriptor {
        id: SnapshotId([1; 16]),
        covered: Cursor::new(key.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: bytes.len() as u64,
        digest: SnapshotDigest(Sha256::digest(bytes).into()),
    };
    origin.begin_snapshot(snapshot.clone()).await.unwrap();
    origin
        .put_snapshot_chunk(
            snapshot.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(bytes),
            },
        )
        .await
        .unwrap();
    origin
        .verify_snapshot_step(
            snapshot.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    origin.publish_snapshot(snapshot.id).await.unwrap();
    let stream = OriginStream {
        origin: origin.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    let replica = ReplicaId::new("destination").unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(&destination)
        .await
        .unwrap();
    origin
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 100_000,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::NeedsBootstrap,
        })
        .await
        .unwrap();
    let begin = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new("origin-begin").unwrap(),
        bootstrap_id: BootstrapId([2; 16]),
        destination_operation_id: ReplicationOperationId::new("destination-begin").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: epoch,
        snapshot: snapshot.clone(),
        captured_tail: ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    let origin = std::sync::Arc::new(origin);
    let destination = std::sync::Arc::new(destination);
    let driver = ReplicationDriver::open(
        origin.clone(),
        destination.clone(),
        ReplicationDriverConfig {
            max_concurrent: 1,
            max_in_flight_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let limits = ReplicationBootstrapDriveLimits {
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
    };
    let publish = ReplicationOperationId::new("driver-publish").unwrap();
    let ack = ReplicationOperationId::new("driver-ack").unwrap();
    let first = driver
        .bootstrap_once(begin.clone(), publish.clone(), ack.clone(), limits.clone())
        .await
        .unwrap();
    origin
        .append_generated(&key, event("c-after-completion"))
        .await
        .unwrap();
    origin
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("reclaim-bootstrap-history").unwrap(),
            stream: key.clone(),
            expected_floor: Cursor::new(key.clone(), 0),
            new_floor: Cursor::new(key.clone(), 2),
        })
        .await
        .unwrap();
    let cleaned = origin
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_event_rows, 2);
    // Receipt replay must not reacquire a recovery plan for this now-unavailable suffix.
    let status_before_retry = origin.replica_status(&replica, &stream).await.unwrap();
    let retry = driver
        .bootstrap_once(begin, publish, ack, limits)
        .await
        .unwrap();
    assert_eq!(first, retry);
    let status_after_retry = origin.replica_status(&replica, &stream).await.unwrap();
    assert_eq!(status_after_retry, status_before_retry);
    assert_eq!(status_after_retry.backlog_records, 1);
    assert_eq!(status_after_retry.mode, ReplicaMode::Required);
    driver.close();
    driver.wait_closed().await;
}
