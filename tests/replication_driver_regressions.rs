#![cfg(feature = "replication")]

use async_trait::async_trait;
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

struct Transport {
    destination: Arc<MemoryStore>,
    lose_reply: AtomicBool,
    wrong_receipt: bool,
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl ReplicaTransport for Transport {
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        let retry = batch.clone();
        let mut receipt = self.destination.commit_replica_batch(batch).await?;
        if self.lose_reply.swap(false, Ordering::SeqCst) {
            return Err(ReplicationError::CommitUnknown(Box::new(retry)));
        }
        if self.wrong_receipt {
            receipt.committed_through.offset += 1;
        }
        Ok(receipt)
    }
}
async fn fixture(
    lose_reply: bool,
    wrong_receipt: bool,
    permits: usize,
) -> (Arc<MemoryStore>, Arc<Transport>, PrepareReplicaBatch) {
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
                id: EventId::new("a").unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("bytes").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(b"actual payload"),
            },
        )
        .await
        .unwrap();
    let stream = OriginStream {
        origin: origin.origin_identity().await.unwrap(),
        stream: key,
    };
    let replica = ReplicaId::new("remote").unwrap();
    origin
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: ReplicaBatchDestinationStore::destination_epoch(
                destination.as_ref(),
            )
            .await
            .unwrap(),
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    let prepare = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("prepare").unwrap(),
        batch_id: BatchId([1; 16]),
        replica,
        stream: stream.clone(),
        expected_after: ReplicaPosition { stream, offset: 0 },
        limits: ReplicaBatchLimits {
            max_records: 1,
            max_bytes: 4096,
        },
    };
    let transport = Arc::new(Transport {
        destination,
        lose_reply: AtomicBool::new(lose_reply),
        wrong_receipt,
        entered: Semaphore::new(0),
        release: Semaphore::new(permits),
    });
    (origin, transport, prepare)
}
fn config() -> ReplicationDriverConfig {
    ReplicationDriverConfig {
        max_concurrent: 1,
        max_in_flight_bytes: 16384,
    }
}
fn ack() -> ReplicationOperationId {
    ReplicationOperationId::new("ack").unwrap()
}

#[tokio::test]
async fn lost_transport_reply_retries_exact_destination_commit() {
    let (origin, transport, prepare) = fixture(true, false, 2).await;
    let driver = ReplicationDriver::open(origin.clone(), transport.clone(), config()).unwrap();
    assert!(matches!(
        driver.replicate_once(prepare.clone(), ack()).await,
        Err(ReplicationError::CommitUnknown(_))
    ));
    assert_eq!(
        origin
            .replica_status(&prepare.replica, &prepare.stream)
            .await
            .unwrap()
            .acknowledged
            .offset,
        0
    );
    let receipt = driver.replicate_once(prepare.clone(), ack()).await.unwrap();
    assert_eq!(receipt.acknowledged.unwrap().status.acknowledged.offset, 1);
    let page = transport
        .destination
        .read_replica_after(
            &prepare.expected_after,
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].event.payload.as_bytes(), b"actual payload");
    assert!(page.complete);
}

#[tokio::test]
async fn invalid_transport_receipt_does_not_advance_origin() {
    let (origin, transport, prepare) = fixture(false, true, 1).await;
    let driver = ReplicationDriver::open(origin.clone(), transport, config()).unwrap();
    assert!(matches!(
        driver.replicate_once(prepare.clone(), ack()).await,
        Err(ReplicationError::InvalidReceipt(_))
    ));
    let status = origin
        .replica_status(&prepare.replica, &prepare.stream)
        .await
        .unwrap();
    assert_eq!(status.acknowledged.offset, 0);
    assert_eq!(status.backlog_records, 1);
    assert_eq!(status.pending_batch, Some(prepare.batch_id));
}

#[tokio::test]
async fn cancelled_caller_does_not_release_capacity_while_transport_owns_batch() {
    let (origin, transport, prepare) = fixture(false, false, 0).await;
    let driver = ReplicationDriver::open(origin.clone(), transport.clone(), config()).unwrap();
    let spawned_driver = driver.clone();
    let request = prepare.clone();
    let caller = tokio::spawn(async move { spawned_driver.replicate_once(request, ack()).await });
    tokio::time::timeout(Duration::from_secs(2), transport.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(matches!(
        driver.replicate_once(prepare.clone(), ack()).await,
        Err(ReplicationError::Overloaded)
    ));
    transport.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if origin
                .replica_status(&prepare.replica, &prepare.stream)
                .await
                .unwrap()
                .acknowledged
                .offset
                == 1
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        origin
            .replica_status(&prepare.replica, &prepare.stream)
            .await
            .unwrap()
            .backlog_records,
        0
    );
    driver.close();
    assert!(matches!(
        driver.replicate_once(prepare, ack()).await,
        Err(ReplicationError::Closed)
    ));
}

#[tokio::test]
async fn retry_policy_resends_unknown_commit_and_shutdown_drains_owned_work() {
    let (origin, transport, prepare) = fixture(true, false, 2).await;
    let driver = ReplicationDriver::open(origin, transport.clone(), config()).unwrap();
    let result = driver
        .replicate_with_retry(
            prepare,
            ack(),
            ReplicationRetryPolicy {
                max_attempts: 2,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
                deadline: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
    assert_eq!(result.acknowledged.unwrap().status.acknowledged.offset, 1);
    assert_eq!(transport.entered.available_permits(), 2);
    tokio::time::timeout(Duration::from_secs(2), driver.wait_closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn deadline_preserves_owned_transport_until_graceful_shutdown_finishes() {
    let (origin, transport, prepare) = fixture(false, false, 0).await;
    let driver = ReplicationDriver::open(origin.clone(), transport.clone(), config()).unwrap();
    let result = driver
        .replicate_with_retry(
            prepare.clone(),
            ack(),
            ReplicationRetryPolicy {
                max_attempts: 2,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(2),
                deadline: Duration::from_millis(20),
            },
        )
        .await;
    assert!(matches!(result, Err(ReplicationError::DriveUnknown { .. })));
    assert_eq!(transport.entered.available_permits(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), driver.wait_closed())
            .await
            .is_err()
    );
    transport.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), driver.wait_closed())
        .await
        .unwrap();
    assert_eq!(
        origin
            .replica_status(&prepare.replica, &prepare.stream)
            .await
            .unwrap()
            .acknowledged
            .offset,
        1
    );
}
