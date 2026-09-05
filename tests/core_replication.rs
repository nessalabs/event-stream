#![cfg(feature = "replication")]

mod common;

use async_trait::async_trait;
use common::{replica_batch_destination_contract, replication_origin_batch_contract};
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

#[tokio::test]
async fn memory_replica_destination_obeys_atomic_batch_contract() {
    replica_batch_destination_contract!(MemoryStore::open(MemoryStoreOptions::default()));
}

#[tokio::test]
async fn memory_replication_origin_obeys_batch_contract() {
    replication_origin_batch_contract!(MemoryStore::open(MemoryStoreOptions::default()));
}

#[derive(Debug)]
struct PausedTransport {
    destination: Arc<MemoryStore>,
    entered: Notify,
    release: Notify,
}

#[async_trait]
impl ReplicaTransport for PausedTransport {
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        self.entered.notify_one();
        self.release.notified().await;
        self.destination.commit_replica_batch(batch).await
    }
}

#[tokio::test]
async fn accepted_replication_drive_finishes_after_its_caller_is_cancelled() {
    let origin = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let key = origin
        .create_if_absent(&StreamId::new("driver").unwrap())
        .await
        .unwrap();
    origin
        .append_atomic(
            &key,
            NewEvent {
                id: EventId::new("one").unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("bytes").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(b"one"),
            },
        )
        .await
        .unwrap();
    let stream = OriginStream {
        origin: origin.origin_identity().await.unwrap(),
        stream: key,
    };
    let replica = ReplicaId::new("destination").unwrap();
    let epoch = ReplicaBatchDestinationStore::destination_epoch(destination.as_ref())
        .await
        .unwrap();
    origin
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach-driver").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    let transport = Arc::new(PausedTransport {
        destination,
        entered: Notify::new(),
        release: Notify::new(),
    });
    let driver = ReplicationDriver::open(
        origin.clone(),
        transport.clone(),
        ReplicationDriverConfig {
            max_concurrent: 1,
            max_in_flight_bytes: 8192,
        },
    )
    .unwrap();
    let prepare = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("prepare-driver").unwrap(),
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
    };
    let caller = tokio::spawn({
        let driver = driver.clone();
        async move {
            driver
                .replicate_once(prepare, ReplicationOperationId::new("ack-driver").unwrap())
                .await
        }
    });
    transport.entered.notified().await;
    caller.abort();
    let shutdown = tokio::spawn({
        let driver = driver.clone();
        async move { driver.wait_closed().await }
    });
    tokio::task::yield_now().await;
    assert!(!shutdown.is_finished());
    transport.release.notify_one();
    for _ in 0..100 {
        if origin
            .replica_status(&replica, &stream)
            .await
            .unwrap()
            .acknowledged
            .offset
            == 1
        {
            shutdown.await.unwrap();
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("owned replication drive did not finish after caller cancellation");
}
