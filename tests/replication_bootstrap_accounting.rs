#![cfg(feature = "replication")]

use async_trait::async_trait;
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

fn identifier(byte: u8) -> String {
    std::iter::repeat_n(char::from(byte), MAX_IDENTIFIER_BYTES).collect()
}

fn limits() -> ReplicationBootstrapDriveLimits {
    ReplicationBootstrapDriveLimits {
        recovery_lifetime: Duration::from_secs(30),
        snapshot_page_bytes: 1,
        suffix_page: PageLimits {
            max_records: 1,
            max_bytes: 1,
        },
        verification: ReplicaBootstrapVerificationLimits {
            max_chunks: 1,
            max_records: 1,
            max_bytes: 1,
        },
    }
}

fn maximal_begin() -> BeginOriginBootstrap {
    let stream = OriginStream {
        origin: OriginId([1; 16]),
        stream: StreamKey {
            id: StreamId::new(identifier(b's')).unwrap(),
            incarnation: IncarnationId([2; 16]),
        },
    };
    BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new(identifier(b'o')).unwrap(),
        bootstrap_id: BootstrapId([3; 16]),
        destination_operation_id: ReplicationOperationId::new(identifier(b'd')).unwrap(),
        replica: ReplicaId::new(identifier(b'r')).unwrap(),
        stream: stream.clone(),
        destination_epoch: DestinationEpoch([4; 16]),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([5; 16]),
            covered: Cursor::new(stream.stream.clone(), 0),
            schema: SchemaRef {
                id: SchemaId::new(identifier(b'c')).unwrap(),
                version: 1,
            },
            content_bytes: 0,
            digest: SnapshotDigest(Sha256::digest([]).into()),
        },
        captured_tail: ReplicaPosition { stream, offset: 0 },
    }
}

#[derive(Default)]
struct CountingTransport {
    calls: AtomicUsize,
}

impl CountingTransport {
    fn called<T>(&self) -> ReplicationResult<T> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(ReplicationError::StorageFailure(
            "transport should not be called".into(),
        ))
    }
}

#[async_trait]
impl ReplicaTransport for CountingTransport {
    async fn send_batch(&self, _: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        self.called()
    }
}

#[async_trait]
impl ReplicaBootstrapTransport for CountingTransport {
    async fn begin_bootstrap(
        &self,
        _: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt> {
        self.called()
    }

    async fn put_bootstrap_chunk(
        &self,
        _: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
        self.called()
    }

    async fn put_bootstrap_batch(
        &self,
        _: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt> {
        self.called()
    }

    async fn verify_bootstrap(
        &self,
        _: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress> {
        self.called()
    }

    async fn publish_bootstrap(
        &self,
        _: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt> {
        self.called()
    }
}

#[tokio::test]
async fn maximum_identifiers_are_charged_before_bootstrap_io() {
    let origin = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let transport = Arc::new(CountingTransport::default());
    let driver = ReplicationDriver::open(
        origin,
        transport.clone(),
        ReplicationDriverConfig {
            max_concurrent: 1,
            // The old buffer-only calculation fit this budget. The complete
            // maximum-identifier request must not.
            max_in_flight_bytes: 4096,
        },
    )
    .unwrap();
    let error = driver
        .bootstrap_once(
            maximal_begin(),
            ReplicationOperationId::new(identifier(b'p')).unwrap(),
            ReplicationOperationId::new(identifier(b'a')).unwrap(),
            limits(),
        )
        .await
        .unwrap_err();
    assert_eq!(error, ReplicationError::Overloaded);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    driver.wait_closed().await;
}

fn generated(id: &str) -> GeneratedEvent {
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
async fn bounded_bootstrap_still_retries_the_exact_completed_receipt() {
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
    origin.append_generated(&key, generated("a")).await.unwrap();
    origin.append_generated(&key, generated("b")).await.unwrap();
    let content = b"state-a";
    let snapshot = SnapshotDescriptor {
        id: SnapshotId([10; 16]),
        covered: Cursor::new(key.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: content.len() as u64,
        digest: SnapshotDigest(Sha256::digest(content).into()),
    };
    origin.begin_snapshot(snapshot.clone()).await.unwrap();
    origin
        .put_snapshot_chunk(
            snapshot.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(content),
            },
        )
        .await
        .unwrap();
    origin
        .verify_snapshot_step(
            snapshot.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 64,
            },
        )
        .await
        .unwrap();
    origin.publish_snapshot(snapshot.id).await.unwrap();
    let stream = OriginStream {
        origin: origin.origin_identity().await.unwrap(),
        stream: key,
    };
    let replica = ReplicaId::new("destination").unwrap();
    let destination_epoch = destination.destination_epoch().await.unwrap();
    origin
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch,
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::NeedsBootstrap,
        })
        .await
        .unwrap();
    let begin = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new("origin-begin").unwrap(),
        bootstrap_id: BootstrapId([11; 16]),
        destination_operation_id: ReplicationOperationId::new("destination-begin").unwrap(),
        replica,
        stream: stream.clone(),
        destination_epoch,
        snapshot,
        captured_tail: ReplicaPosition { stream, offset: 2 },
    };
    let publish = ReplicationOperationId::new("publish").unwrap();
    let acknowledge = ReplicationOperationId::new("acknowledge").unwrap();
    let drive_limits = ReplicationBootstrapDriveLimits {
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
    let driver = ReplicationDriver::open(
        origin,
        destination,
        ReplicationDriverConfig {
            max_concurrent: 1,
            max_in_flight_bytes: 64 * 1024,
        },
    )
    .unwrap();
    let first = driver
        .bootstrap_once(
            begin.clone(),
            publish.clone(),
            acknowledge.clone(),
            drive_limits.clone(),
        )
        .await
        .unwrap();
    let retry = driver
        .bootstrap_once(begin, publish, acknowledge, drive_limits)
        .await
        .unwrap();
    assert_eq!(retry, first);
    driver.wait_closed().await;
}
