#![cfg(feature = "replication")]

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn destination_floor_cannot_exceed_combined_receipt_count_limit() {
    let mut options = MemoryStoreOptions::default();
    options.replica_destination.receipts.max_batch_receipts = 1;
    let store = MemoryStore::open(options).await.unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
        .await
        .unwrap();
    let stream = OriginStream {
        origin: OriginId([1; 16]),
        stream: StreamKey {
            id: StreamId::new("bounded").unwrap(),
            incarnation: IncarnationId([2; 16]),
        },
    };
    let after = ReplicaPosition {
        stream: stream.clone(),
        offset: 0,
    };
    let batch = ReplicaBatch {
        id: BatchId([3; 16]),
        destination_epoch: epoch,
        after: after.clone(),
        records: vec![Arc::new(Record {
            cursor: Cursor::new(stream.stream.clone(), 1),
            event: NewEvent {
                id: EventId::new("one").unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("bytes").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(b"one"),
            },
        })],
    };
    let receipt = store.commit_replica_batch(batch.clone()).await.unwrap();
    let result = store
        .advance_replica_receipt_floor(AdvanceReplicaReceiptFloor {
            operation_id: ReplicationOperationId::new("floor").unwrap(),
            stream: stream.clone(),
            destination_epoch: epoch,
            expected_floor: after,
            new_floor: ReplicaPosition { stream, offset: 1 },
        })
        .await;
    assert!(
        matches!(result, Err(ReplicationError::CapacityExceeded)),
        "{result:?}"
    );
    // Rejection must leave the old retry contract intact.
    assert_eq!(store.commit_replica_batch(batch).await.unwrap(), receipt);
}

#[tokio::test]
async fn closed_origin_rejects_exact_attach_retry_and_detach() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: store
            .create_if_absent(&StreamId::new("closed").unwrap())
            .await
            .unwrap(),
    };
    let replica = ReplicaId::new("destination").unwrap();
    let request = AttachReplica {
        operation_id: ReplicationOperationId::new("attach").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: DestinationEpoch([4; 16]),
        max_backlog_bytes: 4096,
        max_backlog_age: Duration::from_secs(60),
        start: ReplicaStart::FromBeginning,
    };
    store.attach_replica(request.clone()).await.unwrap();
    EventStore::close(&store).await.unwrap();
    let retry = store.attach_replica(request).await;
    assert!(matches!(retry, Err(ReplicationError::Closed)), "{retry:?}");
    let detach = store
        .detach_replica(DetachReplica {
            operation_id: ReplicationOperationId::new("detach").unwrap(),
            replica,
            stream,
        })
        .await;
    assert!(
        matches!(detach, Err(ReplicationError::Closed)),
        "{detach:?}"
    );
}

#[derive(Debug)]
struct TestClock(std::sync::atomic::AtomicU64);
impl DurableReplicationClock for TestClock {
    fn now(&self) -> DurableTimestampMillis {
        DurableTimestampMillis(self.0.load(std::sync::atomic::Ordering::Relaxed))
    }
}
impl TestClock {
    fn set(&self, value: u64) {
        self.0.store(value, std::sync::atomic::Ordering::Relaxed);
    }
}
fn input(id: &str) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(b"payload"),
    }
}

#[tokio::test]
async fn partial_ack_uses_remaining_record_age_and_moves_retention_protection() {
    let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(100)));
    let options = MemoryStoreOptions {
        replication_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    };
    let store = MemoryStore::open(options).await.unwrap();
    let key = store
        .create_if_absent(&StreamId::new("age").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: key.clone(),
        })
        .await
        .unwrap();
    let stream = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    let replica = ReplicaId::new("remote").unwrap();
    let epoch = DestinationEpoch([6; 16]);
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach-age").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 100_000,
            max_backlog_age: Duration::from_millis(150),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    let append = |id: &str| GeneratedEvent {
        generation: RetryGeneration::FIRST,
        event: input(id),
    };
    store.append_generated(&key, append("a")).await.unwrap();
    clock.set(200);
    store.append_generated(&key, append("b")).await.unwrap();
    let floor = |after, to, id: &str| AdvanceRetentionFloor {
        operation_id: RetentionOperationId::new(id).unwrap(),
        stream: key.clone(),
        expected_floor: Cursor::new(key.clone(), after),
        new_floor: Cursor::new(key.clone(), to),
    };
    assert!(matches!(
        store.advance_retention_floor(floor(0, 1, "blocked")).await,
        Err(RetentionError::ReplicaProtectionActive { .. })
    ));
    let prepared = store
        .prepare_replica_batch(PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new("prepare-age").unwrap(),
            batch_id: BatchId([7; 16]),
            replica: replica.clone(),
            stream: stream.clone(),
            expected_after: ReplicaPosition {
                stream: stream.clone(),
                offset: 0,
            },
            limits: ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        })
        .await
        .unwrap();
    let batch = prepared.batch.unwrap();
    let ack = AcknowledgeReplicaBatch {
        operation_id: ReplicationOperationId::new("ack-age").unwrap(),
        replica: replica.clone(),
        expected_after: batch.after.clone(),
        receipt: ReplicaReceipt {
            batch: batch.id,
            destination_epoch: epoch,
            committed_through: ReplicaPosition {
                stream: stream.clone(),
                offset: 1,
            },
        },
    };
    clock.set(300);
    let receipt = store.acknowledge_replica_batch(ack.clone()).await.unwrap();
    assert_eq!(
        receipt.status.oldest_backlog_at,
        Some(DurableTimestampMillis(200))
    );
    assert_eq!(receipt.status.backlog_records, 1);
    assert_eq!(store.acknowledge_replica_batch(ack).await.unwrap(), receipt);
    store
        .advance_retention_floor(floor(0, 1, "after-ack"))
        .await
        .unwrap();
    assert!(matches!(
        store
            .advance_retention_floor(floor(1, 2, "still-pinned"))
            .await,
        Err(RetentionError::ReplicaProtectionActive { .. })
    ));
    // A is 200 ms old now, but B is only 100 ms old. A must no longer block C.
    store.append_generated(&key, append("c")).await.unwrap();
    clock.set(351);
    assert!(matches!(
        store.append_generated(&key, append("d")).await,
        Err(RetentionError::ReplicaBacklogExpired { .. })
    ));
    assert_eq!(store.bounds(&key).await.unwrap().tail.offset, 3);
    store
        .detach_replica(DetachReplica {
            operation_id: ReplicationOperationId::new("detach-age").unwrap(),
            replica,
            stream,
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(floor(1, 3, "after-detach"))
        .await
        .unwrap();
}

#[tokio::test]
async fn attaching_existing_history_preserves_original_commit_age() {
    let clock = Arc::new(TestClock(std::sync::atomic::AtomicU64::new(100)));
    let options = MemoryStoreOptions {
        replication_clock: clock.clone(),
        ..MemoryStoreOptions::default()
    };
    let store = MemoryStore::open(options).await.unwrap();
    let key = store
        .create_if_absent(&StreamId::new("existing-age").unwrap())
        .await
        .unwrap();
    store.append_atomic(&key, input("old")).await.unwrap();
    clock.set(200);
    let receipt = store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach-existing").unwrap(),
            replica: ReplicaId::new("remote").unwrap(),
            stream: OriginStream {
                origin: store.origin_identity().await.unwrap(),
                stream: key,
            },
            destination_epoch: DestinationEpoch([8; 16]),
            max_backlog_bytes: 100_000,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    assert_eq!(
        receipt.status.oldest_backlog_at,
        Some(DurableTimestampMillis(100))
    );
}
