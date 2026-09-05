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
use tokio::sync::Semaphore;

fn event(index: u64) -> NewEvent {
    NewEvent {
        id: EventId::new(format!("event-{index}")).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("runtime-replication").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(&index.to_be_bytes()),
    }
}

async fn attach<S: ReplicationStore, D: ReplicaDestinationStore>(
    runtime: &Runtime<S>,
    destination: &D,
    bootstrap: bool,
) -> (OriginStream, ReplicaId) {
    let key = runtime
        .create_stream(&StreamId::new("origin").unwrap())
        .await
        .unwrap();
    let stream = OriginStream {
        origin: runtime.origin_identity().await.unwrap(),
        stream: key,
    };
    let replica = ReplicaId::new("destination").unwrap();
    runtime
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: destination.destination_epoch().await.unwrap(),
            max_backlog_bytes: 1024 * 1024,
            max_backlog_age: Duration::from_secs(60),
            start: if bootstrap {
                ReplicaStart::NeedsBootstrap
            } else {
                ReplicaStart::FromBeginning
            },
        })
        .await
        .unwrap();
    (stream, replica)
}

fn prepare(stream: &OriginStream, replica: &ReplicaId) -> PrepareReplicaBatch {
    PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("prepare").unwrap(),
        batch_id: BatchId([18; 16]),
        replica: replica.clone(),
        stream: stream.clone(),
        expected_after: ReplicaPosition {
            stream: stream.clone(),
            offset: 0,
        },
        limits: ReplicaBatchLimits {
            max_records: 2,
            max_bytes: 4096,
        },
    }
}

async fn roundtrip<S: ReplicationStore, D: ReplicaDestinationStore>(
    runtime: &Runtime<S>,
    destination: Arc<D>,
) {
    let (stream, replica) = attach(runtime, destination.as_ref(), false).await;
    for i in 0..2 {
        runtime.append(&stream.stream, event(i)).await.unwrap();
    }
    let request = prepare(&stream, &replica);
    let ack = ReplicationOperationId::new("ack").unwrap();
    let first = runtime
        .replicate_once(destination.clone(), request.clone(), ack.clone())
        .await
        .unwrap();
    assert_eq!(
        runtime
            .replicate_once(destination.clone(), request, ack)
            .await
            .unwrap(),
        first
    );
    let status = runtime.replica_status(&replica, &stream).await.unwrap();
    assert_eq!(status.acknowledged.offset, 2);
    assert_eq!(status.backlog_records, 0);
    let page = destination
        .read_replica_after(
            &ReplicaPosition { stream, offset: 0 },
            ReplicaBatchLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    for (i, record) in page.records.iter().enumerate() {
        assert_eq!(record.cursor.offset, i as u64 + 1);
        assert_eq!(record.event, event(i as u64));
    }
}

#[tokio::test]
async fn memory_runtime_replicates_its_own_appends_and_retries_exactly() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    roundtrip(&runtime, destination.clone()).await;
    let stream = OriginStream {
        origin: runtime.origin_identity().await.unwrap(),
        stream: runtime
            .create_stream(&StreamId::new("origin").unwrap())
            .await
            .unwrap(),
    };
    let replica = ReplicaId::new("destination").unwrap();
    assert!(
        runtime
            .shutdown(Duration::from_secs(2))
            .await
            .unwrap()
            .closed
    );
    assert_eq!(
        runtime.origin_identity().await.unwrap_err(),
        ReplicationError::Closed
    );
    assert_eq!(
        runtime.replica_status(&replica, &stream).await.unwrap_err(),
        ReplicationError::Closed
    );
    assert_eq!(
        runtime
            .detach_replica(DetachReplica {
                operation_id: ReplicationOperationId::new("closed-detach").unwrap(),
                replica: replica.clone(),
                stream: stream.clone(),
            })
            .await
            .unwrap_err(),
        ReplicationError::Closed
    );
    assert_eq!(
        runtime
            .replicate_once(
                destination,
                prepare(&stream, &replica),
                ReplicationOperationId::new("closed-ack").unwrap()
            )
            .await
            .unwrap_err(),
        ReplicationError::Closed
    );
}

struct GatedTransport<D> {
    destination: Arc<D>,
    entered: Semaphore,
    release: Semaphore,
    calls: AtomicUsize,
}
impl<D> GatedTransport<D> {
    fn new(destination: Arc<D>) -> Self {
        Self {
            destination,
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            calls: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl<D: ReplicaBatchDestinationStore> ReplicaTransport for GatedTransport<D> {
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.destination.commit_replica_batch(batch).await
    }
}

#[tokio::test]
async fn cancelled_transfer_keeps_its_slot_and_prevents_premature_shutdown() {
    let runtime = Runtime::<MemoryStore>::open(
        MemoryStoreOptions::default(),
        RuntimeConfig {
            replication: RuntimeReplicationConfig {
                max_concurrent: 1,
                max_in_flight_bytes: 64 * 1024,
            },
            ..RuntimeConfig::default()
        },
    )
    .await
    .unwrap();
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let (stream, replica) = attach(&runtime, destination.as_ref(), false).await;
    runtime.append(&stream.stream, event(0)).await.unwrap();
    let transport = Arc::new(GatedTransport::new(destination.clone()));
    let request = prepare(&stream, &replica);
    let ack = ReplicationOperationId::new("ack-gated").unwrap();
    let caller = tokio::spawn({
        let runtime = runtime.clone();
        let transport = transport.clone();
        let request = request.clone();
        let ack = ack.clone();
        async move { runtime.replicate_once(transport, request, ack).await }
    });
    tokio::time::timeout(Duration::from_secs(3), transport.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert_eq!(
        runtime
            .replicate_once(transport.clone(), request, ack)
            .await
            .unwrap_err(),
        ReplicationError::Overloaded
    );
    assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
    // Unrelated foreground writes still use the real runtime while transfer is held.
    runtime.append(&stream.stream, event(1)).await.unwrap();
    let report = runtime.shutdown(Duration::from_millis(10)).await.unwrap();
    assert!(!report.closed);
    assert_eq!(
        runtime.origin_identity().await.unwrap_err(),
        ReplicationError::Closed
    );
    transport.release.add_permits(1);
    assert!(
        runtime
            .shutdown(Duration::from_secs(3))
            .await
            .unwrap()
            .closed
    );
    let page = destination
        .read_replica_after(
            &ReplicaPosition { stream, offset: 0 },
            ReplicaBatchLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.records.len(),
        1,
        "the held transfer captures its original tail"
    );
    assert_eq!(page.records[0].event, event(0));
}

#[tokio::test]
async fn transfer_byte_budget_rejects_before_transport_io() {
    let runtime = Runtime::<MemoryStore>::open(
        MemoryStoreOptions::default(),
        RuntimeConfig {
            replication: RuntimeReplicationConfig {
                max_concurrent: 2,
                max_in_flight_bytes: 64 * 1024,
            },
            ..RuntimeConfig::default()
        },
    )
    .await
    .unwrap();
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let (stream, replica) = attach(&runtime, destination.as_ref(), false).await;
    runtime.append(&stream.stream, event(0)).await.unwrap();
    let transport = Arc::new(GatedTransport::new(destination));
    let mut request = prepare(&stream, &replica);
    request.limits.max_bytes = 1024 * 1024;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        runtime.replicate_once(
            transport.clone(),
            request,
            ReplicationOperationId::new("ack-budget").unwrap(),
        ),
    )
    .await
    .expect("over-budget work must reject without waiting for transport");
    assert_eq!(result.unwrap_err(), ReplicationError::CapacityExceeded);
    assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
    assert!(
        runtime
            .shutdown(Duration::from_secs(2))
            .await
            .unwrap()
            .closed
    );
}

struct PanickingTransport;

#[async_trait]
impl ReplicaTransport for PanickingTransport {
    async fn send_batch(&self, _: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        panic!("injected transport panic after origin prepare")
    }
}

#[tokio::test]
async fn panicking_transport_reports_unknown_and_exact_retry_reuses_capacity() {
    let runtime = Runtime::<MemoryStore>::open(
        MemoryStoreOptions::default(),
        RuntimeConfig {
            replication: RuntimeReplicationConfig {
                max_concurrent: 1,
                max_in_flight_bytes: 64 * 1024,
            },
            ..RuntimeConfig::default()
        },
    )
    .await
    .unwrap();
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    let (stream, replica) = attach(&runtime, destination.as_ref(), false).await;
    runtime.append(&stream.stream, event(0)).await.unwrap();
    let request = prepare(&stream, &replica);
    let ack = ReplicationOperationId::new("ack-after-panic").unwrap();
    let error = runtime
        .replicate_once(Arc::new(PanickingTransport), request.clone(), ack.clone())
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ReplicationError::DriveUnknown {
            prepare: Box::new(request.clone()),
            acknowledge_operation_id: ack.clone(),
        }
    );
    let receipt = runtime
        .replicate_once(destination, request, ack)
        .await
        .unwrap();
    assert_eq!(receipt.remote.unwrap().committed_through.offset, 1);
    assert!(
        runtime
            .shutdown(Duration::from_secs(2))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn invalid_runtime_replication_budgets_fail_before_open() {
    for limits in [
        RuntimeReplicationConfig {
            max_concurrent: 0,
            max_in_flight_bytes: 4096,
        },
        RuntimeReplicationConfig {
            max_concurrent: 1,
            max_in_flight_bytes: 0,
        },
        RuntimeReplicationConfig {
            max_concurrent: usize::MAX,
            max_in_flight_bytes: 4096,
        },
        RuntimeReplicationConfig {
            max_concurrent: 1,
            max_in_flight_bytes: usize::MAX,
        },
    ] {
        assert!(matches!(
            Runtime::<MemoryStore>::open(
                MemoryStoreOptions::default(),
                RuntimeConfig {
                    replication: limits,
                    ..RuntimeConfig::default()
                }
            )
            .await,
            Err(Error::InvalidConfig(_))
        ));
    }
}

async fn bootstrap<S: ReplicationStore, D: ReplicaDestinationStore>(
    runtime: &Runtime<S>,
    destination: Arc<D>,
) {
    let (stream, replica) = attach(runtime, destination.as_ref(), true).await;
    for i in 0..2 {
        runtime.append(&stream.stream, event(i)).await.unwrap();
    }
    let bytes = b"application-state-through-one";
    let descriptor = SnapshotDescriptor {
        id: SnapshotId([29; 16]),
        covered: Cursor::new(stream.stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: bytes.len() as u64,
        digest: SnapshotDigest(Sha256::digest(bytes).into()),
    };
    runtime.begin_snapshot(descriptor.clone()).await.unwrap();
    runtime
        .put_snapshot_chunk(
            descriptor.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(bytes),
            },
        )
        .await
        .unwrap();
    runtime
        .verify_and_publish_snapshot(
            descriptor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    let request = BeginOriginBootstrap {
        operation_id: ReplicationOperationId::new("begin-origin").unwrap(),
        bootstrap_id: BootstrapId([30; 16]),
        destination_operation_id: ReplicationOperationId::new("begin-destination").unwrap(),
        replica: replica.clone(),
        stream: stream.clone(),
        destination_epoch: destination.destination_epoch().await.unwrap(),
        snapshot: descriptor,
        captured_tail: ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    let publish = ReplicationOperationId::new("publish").unwrap();
    let ack = ReplicationOperationId::new("ack-bootstrap").unwrap();
    let limits = ReplicationBootstrapDriveLimits {
        recovery_lifetime: Duration::from_secs(30),
        snapshot_page_bytes: 8,
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
    let first = runtime
        .bootstrap_replica_once(
            destination.clone(),
            request.clone(),
            publish.clone(),
            ack.clone(),
            limits.clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        runtime
            .bootstrap_replica_once(destination.clone(), request, publish, ack, limits)
            .await
            .unwrap(),
        first
    );
    let status = runtime.replica_status(&replica, &stream).await.unwrap();
    assert_eq!(status.acknowledged.offset, 2);
    assert_eq!(status.backlog_records, 0);
    let page = destination
        .read_replica_after(
            &ReplicaPosition { stream, offset: 1 },
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].event, event(1));
}

#[tokio::test]
async fn memory_runtime_bootstraps_snapshot_and_suffix() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let destination = Arc::new(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    );
    bootstrap(&runtime, destination).await;
    assert!(
        runtime
            .shutdown(Duration::from_secs(2))
            .await
            .unwrap()
            .closed
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_runtime_replication_and_bootstrap_survive_origin_reopen() {
    use event_stream::infrastructure::{SqliteOptions, SqliteStore};
    for use_bootstrap in [false, true] {
        let directory =
            std::env::temp_dir().join(format!("runtime-replication-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let origin_path = directory.join("origin.db");
        let destination_path = directory.join("destination.db");
        let runtime = Runtime::<SqliteStore>::open(
            SqliteOptions::new(&origin_path),
            RuntimeConfig::default(),
        )
        .await
        .unwrap();
        let destination = Arc::new(
            SqliteStore::open(SqliteOptions::new(&destination_path))
                .await
                .unwrap(),
        );
        if use_bootstrap {
            bootstrap(&runtime, destination.clone()).await;
        } else {
            roundtrip(&runtime, destination.clone()).await;
        }
        let origin = runtime.origin_identity().await.unwrap();
        let stream = runtime
            .create_stream(&StreamId::new("origin").unwrap())
            .await
            .unwrap();
        assert!(
            runtime
                .shutdown(Duration::from_secs(3))
                .await
                .unwrap()
                .closed
        );
        EventStore::close(destination.as_ref()).await.unwrap();
        drop(destination);
        drop(runtime);
        let runtime = Runtime::<SqliteStore>::open(
            SqliteOptions::new(&origin_path),
            RuntimeConfig::default(),
        )
        .await
        .unwrap();
        let status = runtime
            .replica_status(
                &ReplicaId::new("destination").unwrap(),
                &OriginStream {
                    origin,
                    stream: stream.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(status.acknowledged.offset, 2);
        assert_eq!(status.backlog_records, 0);
        let destination = SqliteStore::open(SqliteOptions::new(&destination_path))
            .await
            .unwrap();
        let page = destination
            .read_replica_after(
                &ReplicaPosition {
                    stream: OriginStream { origin, stream },
                    offset: u64::from(use_bootstrap),
                },
                ReplicaBatchLimits {
                    max_records: 2,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), if use_bootstrap { 1 } else { 2 });
        for record in page.records {
            assert_eq!(record.event, event(record.cursor.offset - 1));
        }
        assert!(
            runtime
                .shutdown(Duration::from_secs(3))
                .await
                .unwrap()
                .closed
        );
        EventStore::close(&destination).await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }
}
