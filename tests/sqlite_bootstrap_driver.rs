#![cfg(all(feature = "sqlite", feature = "replication"))]
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions, SqliteOptions, SqliteStore};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::time::Duration;

struct LosePublicationReply {
    destination: std::sync::Arc<SqliteStore>,
    lose_reply: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl ReplicaTransport for LosePublicationReply {
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        self.destination.commit_replica_batch(batch).await
    }
}
#[async_trait::async_trait]
impl ReplicaBootstrapTransport for LosePublicationReply {
    async fn begin_bootstrap(
        &self,
        request: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt> {
        self.destination.begin_replica_bootstrap(request).await
    }
    async fn put_bootstrap_chunk(
        &self,
        request: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
        self.destination.put_replica_bootstrap_chunk(request).await
    }
    async fn put_bootstrap_batch(
        &self,
        request: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt> {
        self.destination.put_replica_bootstrap_batch(request).await
    }
    async fn verify_bootstrap(
        &self,
        request: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress> {
        self.destination
            .verify_replica_bootstrap_step(request)
            .await
    }
    async fn publish_bootstrap(
        &self,
        request: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt> {
        let receipt = self
            .destination
            .publish_replica_bootstrap(request.clone())
            .await?;
        // Lose only the reply, after the actual SQLite transaction has committed.
        if self
            .lose_reply
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(ReplicationError::BootstrapPublishUnknown(Box::new(request)));
        }
        Ok(receipt)
    }
}

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
async fn sqlite_destination_publishes_real_bootstrap_and_reopens_snapshot_and_suffix() {
    let origin = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let directory = std::env::temp_dir().join(format!("sqlite-bootstrap-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let database = directory.join("destination.db");
    let destination = SqliteStore::open(SqliteOptions::new(&database))
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
        std::sync::Arc::new(LosePublicationReply {
            destination: destination.clone(),
            lose_reply: std::sync::atomic::AtomicBool::new(true),
        }),
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
    let uncertain = driver
        .bootstrap_once(begin.clone(), publish.clone(), ack.clone(), limits.clone())
        .await;
    assert!(matches!(
        uncertain,
        Err(ReplicationError::BootstrapPublishUnknown(_))
    ));
    assert_eq!(
        origin.replica_status(&replica, &stream).await.unwrap().mode,
        ReplicaMode::Bootstrapping
    );
    assert_eq!(
        destination
            .published_replica_bootstrap(&stream)
            .await
            .unwrap()
            .unwrap()
            .committed_through
            .offset,
        2
    );
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
    drop(driver);
    EventStore::close(destination.as_ref()).await.unwrap();
    drop(destination);
    let reopened = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let published = reopened
        .published_replica_bootstrap(&stream)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.request.snapshot, snapshot);
    assert_eq!(published.committed_through.offset, 2);
    let suffix = reopened
        .read_replica_after(
            &ReplicaPosition {
                stream: stream.clone(),
                offset: 1,
            },
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(suffix.records.len(), 1);
    assert_eq!(suffix.records[0].cursor.offset, 2);
    assert_eq!(suffix.records[0].event.payload.as_bytes(), b"b");
    assert!(suffix.complete);

    let lease = reopened
        .acquire_replica_bootstrap_read(&stream, Duration::from_secs(30))
        .await
        .unwrap();
    let page = reopened
        .read_replica_bootstrap_bytes(lease.lease, 0, 4096)
        .await
        .unwrap();
    assert_eq!(page.bytes.as_bytes(), bytes);
    reopened
        .release_replica_bootstrap_read(lease.lease)
        .await
        .unwrap();
    EventStore::close(&reopened).await.unwrap();
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn sqlite_both_stores_reopen_and_retry_completed_bootstrap() {
    let directory = std::env::temp_dir().join(format!("sqlite-bootstrap-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let origin_database = directory.join("origin.db");
    let origin = SqliteStore::open(SqliteOptions::new(&origin_database))
        .await
        .unwrap();
    let database = directory.join("destination.db");
    let destination = SqliteStore::open(SqliteOptions::new(&database))
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
    origin
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("rotate-before-cleanup").unwrap(),
            stream: key.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    origin
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("expire-before-cleanup").unwrap(),
            stream: key.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::new(2),
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
        .bootstrap_once(begin.clone(), publish.clone(), ack.clone(), limits.clone())
        .await
        .unwrap();
    assert_eq!(first, retry);
    let status_after_retry = origin.replica_status(&replica, &stream).await.unwrap();
    assert_eq!(status_after_retry, status_before_retry);
    assert_eq!(status_after_retry.backlog_records, 1);
    assert_eq!(status_after_retry.mode, ReplicaMode::Required);
    driver.close();
    driver.wait_closed().await;
    drop(driver);
    EventStore::close(origin.as_ref()).await.unwrap();
    drop(origin);
    EventStore::close(destination.as_ref()).await.unwrap();
    drop(destination);
    let reopened = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let published = reopened
        .published_replica_bootstrap(&stream)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.request.snapshot, snapshot);
    assert_eq!(published.committed_through.offset, 2);
    let suffix = reopened
        .read_replica_after(
            &ReplicaPosition {
                stream: stream.clone(),
                offset: 1,
            },
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(suffix.records.len(), 1);
    assert_eq!(suffix.records[0].cursor.offset, 2);
    assert_eq!(suffix.records[0].event.payload.as_bytes(), b"b");
    assert!(suffix.complete);

    let lease = reopened
        .acquire_replica_bootstrap_read(&stream, Duration::from_secs(30))
        .await
        .unwrap();
    let page = reopened
        .read_replica_bootstrap_bytes(lease.lease, 0, 4096)
        .await
        .unwrap();
    assert_eq!(page.bytes.as_bytes(), bytes);
    reopened
        .release_replica_bootstrap_read(lease.lease)
        .await
        .unwrap();
    let origin = std::sync::Arc::new(
        SqliteStore::open(SqliteOptions::new(&origin_database))
            .await
            .unwrap(),
    );
    let reopened = std::sync::Arc::new(reopened);
    let driver = ReplicationDriver::open(
        origin.clone(),
        reopened.clone(),
        ReplicationDriverConfig {
            max_concurrent: 1,
            max_in_flight_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let replayed = driver
        .bootstrap_once(begin, publish, ack, limits)
        .await
        .unwrap();
    assert_eq!(replayed, first);
    assert_eq!(
        origin.replica_status(&replica, &stream).await.unwrap(),
        status_before_retry
    );
    driver.close();
    driver.wait_closed().await;
    drop(driver);
    EventStore::close(origin.as_ref()).await.unwrap();
    drop(origin);
    EventStore::close(reopened.as_ref()).await.unwrap();
    drop(reopened);
    std::fs::remove_dir_all(directory).unwrap();
}
