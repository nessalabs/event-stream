#![cfg(feature = "sqlite")]

mod common;

use event_stream::{
    infrastructure::{SqliteFailureInjection, SqliteOptions, SqliteStore, SQLITE_FORMAT_VERSION},
    AppendKind, CleanupLimits, Error, EventId, EventStore, LifecycleAction, LifecycleOperationId,
    LifecycleRequest, LifecycleStore, NewEvent, PageLimits, Payload, SchemaId, SchemaRef, StreamId,
};
use rusqlite::{params, Connection};
use std::{
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};
#[cfg(any(feature = "snapshots", feature = "source-journal"))]
use std::{process::Child, sync::mpsc};

#[cfg(feature = "snapshots")]
use event_stream::{
    Cursor, SnapshotDescriptor, SnapshotDigest, SnapshotId, SnapshotStore, VerificationLimits,
};

fn temp_db(label: &str) -> (PathBuf, PathBuf) {
    let dir = std::env::temp_dir().join(format!("event-stream-{label}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    (dir.join("events.sqlite3"), dir)
}

#[tokio::test]
async fn find_stream_preserves_absence_and_validates_active_name() {
    let (path, directory) = temp_db("find-stream");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let id = StreamId::new("find-stream").unwrap();
    assert_eq!(store.find_stream(&id).await.unwrap(), None);
    let key = store.create_if_absent(&id).await.unwrap();
    assert_eq!(store.find_stream(&id).await.unwrap(), Some(key.clone()));
    store.close().await.unwrap();

    let db = Connection::open(&path).unwrap();
    db.execute(
        "UPDATE event_stream_names SET active_stream_key=999999 WHERE public_id=?1",
        params![id.as_str()],
    )
    .unwrap();
    drop(db);
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        store.find_stream(&id).await,
        Err(Error::StoreCorrupt(_))
    ));
    store.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("test.bytes").unwrap(),
            version: 7,
        },
        payload: Payload::copy_from_slice(payload),
    }
}

#[cfg(any(feature = "snapshots", feature = "source-journal"))]
struct ChildGuard(Option<Child>);

#[cfg(any(feature = "snapshots", feature = "source-journal"))]
impl ChildGuard {
    fn child_mut(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }

    fn kill_and_wait(mut self) -> std::process::ExitStatus {
        let mut child = self.0.take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap()
    }
}

#[cfg(any(feature = "snapshots", feature = "source-journal"))]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[cfg(any(feature = "snapshots", feature = "source-journal"))]
fn wait_for_child_marker(child: &mut ChildGuard, marker: &'static str) {
    let stdout = child.child_mut().stdout.take().unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(Err("child stdout closed before marker"));
                    return;
                }
                Ok(_) if line.contains(marker) => {
                    let _ = sender.send(Ok(()));
                    return;
                }
                Ok(_) => {}
                Err(_) => {
                    let _ = sender.send(Err("failed to read child stdout"));
                    return;
                }
            }
        }
    });
    receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("child marker handshake timed out")
        .expect("child marker handshake failed");
}

#[tokio::test]
async fn shared_event_store_contract() {
    let (path, dir) = temp_db("contract");
    common::event_store_contract!(SqliteStore::open(SqliteOptions::new(&path)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn shared_lifecycle_store_contract() {
    let (path, dir) = temp_db("lifecycle-contract");
    common::lifecycle_store_contract!(SqliteStore::open(SqliteOptions::new(&path)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(not(feature = "retention"))]
#[tokio::test]
async fn retention_schema_is_rejected_when_support_is_disabled() {
    let (path, dir) = temp_db("retention-disabled");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE retention_streams(stream_key INTEGER PRIMARY KEY);")
        .unwrap();
    let result = SqliteStore::open(SqliteOptions::new(&path)).await;
    assert!(matches!(result, Err(Error::StoreCorrupt(_))));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(not(feature = "source-journal"))]
#[tokio::test]
async fn source_journal_schema_is_rejected_when_support_is_disabled() {
    let (path, dir) = temp_db("source-journal-disabled");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE journal_sources(source_id TEXT PRIMARY KEY);")
        .unwrap();
    let result = SqliteStore::open(SqliteOptions::new(&path)).await;
    assert!(matches!(result, Err(Error::StoreCorrupt(_))));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn shared_snapshot_store_contract() {
    let (path, dir) = temp_db("snapshot-contract");
    common::snapshot_store_contract!(SqliteStore::open(SqliteOptions::new(&path)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn shared_source_journal_store_contract() {
    let (path, dir) = temp_db("source-journal-contract");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    common::run_source_journal_contract(store).await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn shared_sqlite_replication_origin_batch_contract() {
    let (path, dir) = temp_db("replication-origin-contract");
    common::replication_origin_batch_contract!(SqliteStore::open(SqliteOptions::new(&path)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn shared_sqlite_replica_batch_destination_contract() {
    let (path, dir) = temp_db("replication-destination-contract");
    common::replica_batch_destination_contract!(SqliteStore::open(SqliteOptions::new(&path)));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn sqlite_replica_commit_lost_ack_resolves_exactly_after_restart() {
    use event_stream::{
        BatchId, Cursor, IncarnationId, OriginId, OriginStream, Record, ReplicaBatch,
        ReplicaBatchDestinationStore, ReplicaBatchLimits, ReplicaPosition, ReplicationError,
        StreamKey,
    };
    use std::sync::Arc;

    let (path, dir) = temp_db("replica-commit-lost-ack");
    let mut options = SqliteOptions::new(&path);
    options.failure_injection = Some(SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    let epoch = store.destination_epoch().await.unwrap();
    let stream = OriginStream {
        origin: OriginId([61; 16]),
        stream: StreamKey {
            id: StreamId::new("lost-ack-replica").unwrap(),
            incarnation: IncarnationId([62; 16]),
        },
    };
    let batch = ReplicaBatch {
        id: BatchId([63; 16]),
        destination_epoch: epoch,
        after: ReplicaPosition {
            stream: stream.clone(),
            offset: 0,
        },
        records: vec![Arc::new(Record {
            cursor: Cursor::new(stream.stream.clone(), 1),
            event: event("replicated-lost-ack", &[7, 8, 9]),
        })],
    };
    assert!(matches!(
        store.commit_replica_batch(batch.clone()).await,
        Err(ReplicationError::CommitUnknown(_))
    ));
    ReplicaBatchDestinationStore::close_replica_destination(&store)
        .await
        .unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let receipt = reopened.commit_replica_batch(batch.clone()).await.unwrap();
    assert_eq!(receipt.destination_epoch, epoch);
    assert_eq!(receipt.committed_through.offset, 1);
    let page = reopened
        .read_replica_after(
            &batch.after,
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].event, batch.records[0].event);
    ReplicaBatchDestinationStore::close_replica_destination(&reopened)
        .await
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn sqlite_replica_destination_quotas_are_atomic_and_counters_are_audited() {
    use event_stream::{
        BatchId, Cursor, DestinationEpoch, IncarnationId, OriginId, OriginStream, Record,
        ReplicaBatch, ReplicaBatchDestinationStore, ReplicaBatchLimits, ReplicaPosition,
        ReplicationError, StreamKey,
    };
    use std::sync::Arc;

    fn batch(epoch: DestinationEpoch, stream: &OriginStream, id: u8, after: u64) -> ReplicaBatch {
        ReplicaBatch {
            id: BatchId([id; 16]),
            destination_epoch: epoch,
            after: ReplicaPosition {
                stream: stream.clone(),
                offset: after,
            },
            records: vec![Arc::new(Record {
                cursor: Cursor::new(stream.stream.clone(), after + 1),
                event: event(&format!("quota-{id}"), &[id]),
            })],
        }
    }

    let (path, dir) = temp_db("replica-destination-quota");
    let mut options = SqliteOptions::new(&path);
    options.replica_destination.storage.max_history_records = 1;
    let store = SqliteStore::open(options).await.unwrap();
    let epoch = store.destination_epoch().await.unwrap();
    let stream = OriginStream {
        origin: OriginId([76; 16]),
        stream: StreamKey {
            id: StreamId::new("quota-destination").unwrap(),
            incarnation: IncarnationId([77; 16]),
        },
    };
    store
        .commit_replica_batch(batch(epoch, &stream, 78, 0))
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit_replica_batch(batch(epoch, &stream, 79, 1))
            .await,
        Err(ReplicationError::CapacityExceeded)
    ));
    ReplicaBatchDestinationStore::close_replica_destination(&store)
        .await
        .unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let page = reopened
        .read_replica_after(
            &ReplicaPosition {
                stream: stream.clone(),
                offset: 0,
            },
            ReplicaBatchLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].event.payload.as_bytes(), &[78]);
    ReplicaBatchDestinationStore::close_replica_destination(&reopened)
        .await
        .unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE replication_destination_accounting SET history_records=0",
            [],
        )
        .unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    std::fs::remove_dir_all(dir).unwrap();

    let (path, dir) = temp_db("replica-destination-receipt-quota");
    let mut options = SqliteOptions::new(&path);
    options.replica_destination.receipts.max_batch_receipts = 1;
    let store = SqliteStore::open(options).await.unwrap();
    let epoch = store.destination_epoch().await.unwrap();
    store
        .commit_replica_batch(batch(epoch, &stream, 80, 0))
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit_replica_batch(batch(epoch, &stream, 81, 1))
            .await,
        Err(ReplicationError::CapacityExceeded)
    ));
    ReplicaBatchDestinationStore::close_replica_destination(&store)
        .await
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn sqlite_replica_bootstrap_publishes_exact_snapshot_and_suffix_after_restart() {
    use event_stream::{
        BatchId, BootstrapId, Cursor, IncarnationId, OriginId, OriginStream,
        PublishReplicaBootstrap, Record, ReplicaBatch, ReplicaBatchDestinationStore,
        ReplicaBootstrap, ReplicaBootstrapBatch, ReplicaBootstrapChunk,
        ReplicaBootstrapVerificationLimits, ReplicaDestinationStore, ReplicationOperationId,
        SchemaRef, SnapshotChunk, SnapshotDescriptor, SnapshotDigest, SnapshotId, StreamKey,
        VerifyReplicaBootstrap,
    };
    use sha2::{Digest, Sha256};
    use std::sync::Arc;

    let (path, dir) = temp_db("replica-bootstrap-persistence");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let epoch = store.destination_epoch().await.unwrap();
    let stream = OriginStream {
        origin: OriginId([82; 16]),
        stream: StreamKey {
            id: StreamId::new("bootstrap-persistence").unwrap(),
            incarnation: IncarnationId([83; 16]),
        },
    };
    let content = b"durable-bootstrap";
    let request = ReplicaBootstrap {
        operation_id: ReplicationOperationId::new("bootstrap-begin").unwrap(),
        id: BootstrapId([84; 16]),
        replica: event_stream::ReplicaId::new("bootstrap-replica").unwrap(),
        destination_epoch: epoch,
        stream: stream.clone(),
        snapshot: SnapshotDescriptor {
            id: SnapshotId([85; 16]),
            covered: Cursor::new(stream.stream.clone(), 1),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: content.len() as u64,
            digest: SnapshotDigest(Sha256::digest(content).into()),
        },
        through: event_stream::ReplicaPosition {
            stream: stream.clone(),
            offset: 2,
        },
    };
    store
        .begin_replica_bootstrap(request.clone())
        .await
        .unwrap();
    for (offset, bytes) in [(0, &content[..7]), (7, &content[7..])] {
        store
            .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
                id: request.id,
                chunk: SnapshotChunk {
                    offset,
                    bytes: event_stream::Payload::copy_from_slice(bytes),
                },
            })
            .await
            .unwrap();
    }
    store
        .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
            id: request.id,
            batch: ReplicaBatch {
                id: BatchId([86; 16]),
                destination_epoch: epoch,
                after: event_stream::ReplicaPosition {
                    stream: stream.clone(),
                    offset: 1,
                },
                records: vec![Arc::new(Record {
                    cursor: Cursor::new(stream.stream.clone(), 2),
                    event: event("bootstrap-suffix", b"suffix"),
                })],
            },
        })
        .await
        .unwrap();
    let progress = store
        .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
            id: request.id,
            limits: ReplicaBootstrapVerificationLimits {
                max_chunks: 2,
                max_records: 1,
                max_bytes: 4096,
            },
        })
        .await
        .unwrap();
    assert!(progress.complete);
    let publish = PublishReplicaBootstrap {
        operation_id: ReplicationOperationId::new("bootstrap-publish").unwrap(),
        id: request.id,
        destination_epoch: epoch,
    };
    let receipt = store
        .publish_replica_bootstrap(publish.clone())
        .await
        .unwrap();
    assert_eq!(
        store.publish_replica_bootstrap(publish).await.unwrap(),
        receipt
    );
    event_stream::ReplicaBatchDestinationStore::close_replica_destination(&store)
        .await
        .unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let published = reopened
        .published_replica_bootstrap(&stream)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(published.request, request);
    let read = reopened
        .acquire_replica_bootstrap_read(&stream, Duration::from_secs(30))
        .await
        .unwrap();
    let bytes = reopened
        .read_replica_bootstrap_bytes(read.lease, 0, 64)
        .await
        .unwrap();
    assert_eq!(bytes.bytes.as_bytes(), content);
    reopened
        .release_replica_bootstrap_read(read.lease)
        .await
        .unwrap();
    let mut aborted = request.clone();
    aborted.operation_id = ReplicationOperationId::new("bootstrap-aborted-begin").unwrap();
    aborted.id = BootstrapId([87; 16]);
    aborted.snapshot.id = SnapshotId([88; 16]);
    reopened
        .begin_replica_bootstrap(aborted.clone())
        .await
        .unwrap();
    reopened
        .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
            id: aborted.id,
            chunk: SnapshotChunk {
                offset: 0,
                bytes: event_stream::Payload::copy_from_slice(content),
            },
        })
        .await
        .unwrap();
    reopened
        .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
            id: aborted.id,
            batch: ReplicaBatch {
                id: BatchId([89; 16]),
                destination_epoch: epoch,
                after: event_stream::ReplicaPosition {
                    stream: stream.clone(),
                    offset: 1,
                },
                records: vec![Arc::new(Record {
                    cursor: Cursor::new(stream.stream.clone(), 2),
                    event: event("aborted-suffix", b"aborted"),
                })],
            },
        })
        .await
        .unwrap();
    let abort = event_stream::AbortReplicaBootstrap {
        operation_id: ReplicationOperationId::new("bootstrap-abort").unwrap(),
        id: aborted.id,
        destination_epoch: epoch,
    };
    let aborted_receipt = reopened
        .abort_replica_bootstrap(abort.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened.abort_replica_bootstrap(abort).await.unwrap(),
        aborted_receipt
    );
    let first_cleanup = reopened
        .cleanup_replica_destination(event_stream::ReplicaCleanupLimits {
            max_receipt_rows: 1,
            max_staging_rows: 1,
            max_bytes: 4096,
        })
        .await
        .unwrap();
    assert_eq!(first_cleanup.removed_staging_rows, 1);
    let second_cleanup = reopened
        .cleanup_replica_destination(event_stream::ReplicaCleanupLimits {
            max_receipt_rows: 1,
            max_staging_rows: 2,
            max_bytes: 4096,
        })
        .await
        .unwrap();
    assert_eq!(second_cleanup.removed_staging_rows, 1);
    assert!(!second_cleanup.remaining);
    assert_eq!(
        reopened
            .begin_replica_bootstrap(aborted)
            .await
            .unwrap()
            .state,
        event_stream::ReplicaBootstrapState::Aborted
    );
    event_stream::ReplicaBatchDestinationStore::close_replica_destination(&reopened)
        .await
        .unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn sqlite_replica_backlog_counters_and_oldest_age_advance_by_exact_batch() {
    use event_stream::{
        AcknowledgeReplicaBatch, AttachReplica, BatchId, DestinationEpoch, DurableReplicationClock,
        DurableTimestampMillis, OriginStream, PrepareReplicaBatch, ReplicaBatchLimits,
        ReplicaPosition, ReplicaStart, ReplicationOperationId, ReplicationOriginStore,
    };
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    #[derive(Debug)]
    struct Clock(AtomicU64);
    impl DurableReplicationClock for Clock {
        fn now(&self) -> DurableTimestampMillis {
            DurableTimestampMillis(self.0.fetch_add(1, Ordering::Relaxed))
        }
    }

    let (path, dir) = temp_db("replica-backlog-counters");
    let mut options = SqliteOptions::new(&path);
    options.replication_clock = Arc::new(Clock(AtomicU64::new(100)));
    let store = SqliteStore::open(options).await.unwrap();
    let key = store
        .create_if_absent(&StreamId::new("replica-backlog").unwrap())
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    let replica = event_stream::ReplicaId::new("counter-replica").unwrap();
    let epoch = DestinationEpoch([71; 16]);
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("counter-attach").unwrap(),
            replica: replica.clone(),
            stream: origin.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 16 * 1024,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store
        .append_atomic(&key, event("counter-a", &[1]))
        .await
        .unwrap();
    let first = store.replica_status(&replica, &origin).await.unwrap();
    store
        .append_atomic(&key, event("counter-b", &[2]))
        .await
        .unwrap();
    let before = store.replica_status(&replica, &origin).await.unwrap();
    assert_eq!(before.backlog_records, 2);
    let prepare = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("counter-prepare").unwrap(),
        batch_id: BatchId([72; 16]),
        replica: replica.clone(),
        stream: origin.clone(),
        expected_after: ReplicaPosition {
            stream: origin.clone(),
            offset: 0,
        },
        limits: ReplicaBatchLimits {
            max_records: 1,
            max_bytes: 4096,
        },
    };
    let batch = store
        .prepare_replica_batch(prepare)
        .await
        .unwrap()
        .batch
        .unwrap();
    store
        .acknowledge_replica_batch(AcknowledgeReplicaBatch {
            operation_id: ReplicationOperationId::new("counter-ack").unwrap(),
            replica: replica.clone(),
            expected_after: batch.after.clone(),
            receipt: event_stream::ReplicaReceipt {
                batch: batch.id,
                destination_epoch: epoch,
                committed_through: ReplicaPosition {
                    stream: origin.clone(),
                    offset: 1,
                },
            },
        })
        .await
        .unwrap();
    let after = store.replica_status(&replica, &origin).await.unwrap();
    assert_eq!(after.backlog_records, 1);
    assert_eq!(
        after.backlog_bytes,
        before.backlog_bytes - first.backlog_bytes
    );
    assert!(after.oldest_backlog_at > first.oldest_backlog_at);
    EventStore::close(&store).await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(all(feature = "replication", feature = "retention"))]
#[tokio::test]
async fn sqlite_replication_includes_generated_history_and_survives_reopen() {
    use event_stream::{
        AcknowledgeReplicaBatch, AdvanceRetentionFloor, AdvanceRetryGeneration, AttachReplica,
        BatchId, Cursor, DestinationEpoch, EnableRetryPolicy, GeneratedEvent, OriginStream,
        PrepareReplicaBatch, ReplicaBatchLimits, ReplicaPosition, ReplicaStart,
        ReplicationOperationId, ReplicationOriginStore, RetentionError, RetentionOperationId,
        RetentionStore, RetryGeneration,
    };

    let (path, dir) = temp_db("replication-generated-history");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("replicated-generated").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("replicated-generated-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: stream.clone(),
    };
    let replica = event_stream::ReplicaId::new("generated-replica").unwrap();
    let destination_epoch = DestinationEpoch([74; 16]);
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("generated-attach").unwrap(),
            replica: replica.clone(),
            stream: origin.clone(),
            destination_epoch,
            max_backlog_bytes: 32 * 1024,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("generated-one", b"one"),
            },
        )
        .await
        .unwrap();
    store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("generated-advance").unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::new(2),
                event: event("generated-two", b"two"),
            },
        )
        .await
        .unwrap();

    let status = store.replica_status(&replica, &origin).await.unwrap();
    assert_eq!(status.backlog_records, 2);
    assert!(matches!(
        store
            .advance_retention_floor(AdvanceRetentionFloor {
                operation_id: RetentionOperationId::new("generated-floor-blocked").unwrap(),
                stream: stream.clone(),
                expected_floor: Cursor::new(stream.clone(), 0),
                new_floor: Cursor::new(stream.clone(), 2),
            })
            .await,
        Err(RetentionError::ReplicaProtectionActive { maximum_floor })
            if maximum_floor.offset == 0
    ));
    let request = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("generated-prepare").unwrap(),
        batch_id: BatchId([75; 16]),
        replica: replica.clone(),
        stream: origin.clone(),
        expected_after: ReplicaPosition {
            stream: origin.clone(),
            offset: 0,
        },
        limits: ReplicaBatchLimits {
            max_records: 2,
            max_bytes: 8192,
        },
    };
    let batch = store
        .prepare_replica_batch(request)
        .await
        .unwrap()
        .batch
        .unwrap();
    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.records[0].event.payload.as_bytes(), b"one");
    assert_eq!(batch.records[1].event.payload.as_bytes(), b"two");
    store
        .acknowledge_replica_batch(AcknowledgeReplicaBatch {
            operation_id: ReplicationOperationId::new("generated-ack").unwrap(),
            replica: replica.clone(),
            expected_after: batch.after.clone(),
            receipt: event_stream::ReplicaReceipt {
                batch: batch.id,
                destination_epoch,
                committed_through: ReplicaPosition {
                    stream: origin.clone(),
                    offset: 2,
                },
            },
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("generated-floor-after-ack").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 2),
        })
        .await
        .unwrap();
    EventStore::close(&store).await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let status = reopened.replica_status(&replica, &origin).await.unwrap();
    assert_eq!(status.acknowledged.offset, 2);
    assert_eq!(status.backlog_records, 0);
    assert_eq!(status.backlog_bytes, 0);
    EventStore::close(&reopened).await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn sqlite_rejects_understated_replica_backlog_counter_on_open() {
    use event_stream::{
        AttachReplica, DestinationEpoch, OriginStream, ReplicaStart, ReplicationOperationId,
        ReplicationOriginStore,
    };
    let (path, dir) = temp_db("replica-corrupt-counter");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let key = store
        .create_if_absent(&StreamId::new("replica-counter-corrupt").unwrap())
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("counter-corrupt-attach").unwrap(),
            replica: event_stream::ReplicaId::new("counter-corrupt-replica").unwrap(),
            stream: origin,
            destination_epoch: DestinationEpoch([73; 16]),
            max_backlog_bytes: 4096,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store
        .append_atomic(&key, event("counter-corrupt-event", &[1]))
        .await
        .unwrap();
    EventStore::close(&store).await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE replication_origin_replicas SET backlog_bytes=zeroblob(8)",
            [],
        )
        .unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await;
    assert!(matches!(reopened, Err(Error::StoreCorrupt(_))));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn sqlite_journal_receipt_cleanup_does_not_strand_raw_bytes_before_checkpoint() {
    use event_stream::{
        AdvanceCaptureReceiptFloor, BeginSource, JournalCleanupLimits, JournalOperationId,
        ParserCheckpoint, ParserId, ParserRef, RawPageLimits, RawSegment, SourceBinding, SourceId,
        SourceIncarnation, SourceJournalStore, SourceJournalStoreConfig, SourceKey, SourcePosition,
    };

    let (path, dir) = temp_db("journal-cleanup-order");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let output = store
        .create_if_absent(&StreamId::new("journal-cleanup-output").unwrap())
        .await
        .unwrap();
    let source = SourceKey {
        id: SourceId::new("journal-cleanup-input").unwrap(),
        incarnation: SourceIncarnation([44; 16]),
    };
    let parser = ParserRef {
        id: ParserId::new("bytes").unwrap(),
        version: 1,
    };
    let begin = BeginSource {
        operation_id: JournalOperationId::new("journal-cleanup-begin").unwrap(),
        binding: SourceBinding {
            source: source.clone(),
            parser: parser.clone(),
            output_stream: output.clone(),
        },
    };
    let begun = store.begin_source(begin.clone()).await.unwrap();
    store
        .capture_segment(RawSegment {
            start: SourcePosition {
                source: source.clone(),
                offset: 0,
            },
            bytes: Payload::copy_from_slice(b"abc"),
        })
        .await
        .unwrap();
    let floor = AdvanceCaptureReceiptFloor {
        operation_id: JournalOperationId::new("journal-cleanup-floor").unwrap(),
        source: source.clone(),
        expected_floor: 0,
        new_floor: 3,
    };
    let advanced = store
        .advance_capture_receipt_floor(floor.clone())
        .await
        .unwrap();
    let cleanup = store
        .cleanup_captured(JournalCleanupLimits {
            max_segment_rows: 1,
            max_marker_rows: 1,
            max_receipt_rows: 1,
            max_bytes: SourceJournalStoreConfig::default().cleanup.max_bytes,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.removed_receipt_rows, 1);
    assert_eq!(cleanup.removed_segment_rows, 0);
    assert_eq!(
        store
            .read_captured(
                &source,
                0,
                RawPageLimits {
                    max_segments: 1,
                    max_bytes: 3,
                },
            )
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        b"abc"
    );
    store
        .publish_parser_checkpoint(ParserCheckpoint {
            source: SourcePosition {
                source: source.clone(),
                offset: 3,
            },
            parser,
            state: Payload::copy_from_slice(b"done"),
            next_item_index: 0,
            output_stream: output,
            committed_output: None,
        })
        .await
        .unwrap();
    let cleanup = store
        .cleanup_captured(SourceJournalStoreConfig::default().cleanup)
        .await
        .unwrap();
    assert_eq!(cleanup.removed_segment_rows, 1);
    assert!(!cleanup.remaining);
    assert_eq!(store.begin_source(begin).await.unwrap(), begun);
    assert_eq!(
        store.advance_capture_receipt_floor(floor).await.unwrap(),
        advanced
    );
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn sqlite_journal_marker_retry_does_not_recreate_a_missing_output() {
    use event_stream::{
        BeginSource, DecodedPosition, EnableRetryPolicy, JournalOperationId, JournaledOutput,
        ParserId, ParserRef, RetentionOperationId, RetentionStore, SourceBinding, SourceId,
        SourceIncarnation, SourceJournalStore, SourceKey,
    };

    let (path, dir) = temp_db("journal-missing-output");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let output = store
        .create_if_absent(&StreamId::new("journal-output").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-journal-output").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let source = SourceKey {
        id: SourceId::new("journal-input").unwrap(),
        incarnation: SourceIncarnation([91; 16]),
    };
    store
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin-journal-input").unwrap(),
            binding: SourceBinding {
                source: source.clone(),
                parser: ParserRef {
                    id: ParserId::new("bytes").unwrap(),
                    version: 1,
                },
                output_stream: output.clone(),
            },
        })
        .await
        .unwrap();
    let request = JournaledOutput {
        source,
        position: DecodedPosition {
            source_byte: 0,
            item_index: 0,
        },
        event: event("journal-event", b"durable output"),
    };
    store
        .append_captured(&output, request.clone())
        .await
        .unwrap();

    let raw = Connection::open(&path).unwrap();
    assert_eq!(
        raw.execute("DELETE FROM retention_generated_records", [])
            .unwrap(),
        1
    );
    drop(raw);

    assert!(matches!(
        store.append_captured(&output, request).await,
        Err(event_stream::JournalError::CorruptStorage(_))
    ));
    store.close().await.unwrap();
    let raw = Connection::open(&path).unwrap();
    assert_eq!(
        raw.query_row(
            "SELECT count(*) FROM retention_generated_records",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
    drop(raw);
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn sqlite_journal_parser_recovers_output_before_checkpoint_after_restart() {
    use event_stream::{
        ingestion::{
            ByteFrame, CheckpointDecoder, CrLfPolicy, FinalLinePolicy, JournalDriveConfig,
            JournalIngestionService, NewlineFramer, NewlineFramerConfig,
        },
        BeginSource, Cursor, DecodedPosition, EnableRetryPolicy, EventReader, EventRuntime,
        JournalOperationId, JournaledOutput, RawSegment, RetentionOperationId, Runtime,
        RuntimeConfig, SourceBinding, SourceId, SourceIncarnation, SourceKey, SourcePosition,
    };

    let newline = || {
        NewlineFramer::new(NewlineFramerConfig {
            max_frame_bytes: 1024,
            emit_empty_frames: false,
            crlf: CrLfPolicy::StripCarriageReturn,
            final_line: FinalLinePolicy::RejectUnterminated,
        })
        .unwrap()
    };
    let (path, dir) = temp_db("journal-parser-restart");
    let runtime = Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
        .await
        .unwrap();
    let output = runtime
        .create_stream(&StreamId::new("parser-output").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-parser-output").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let source = SourceKey {
        id: SourceId::new("parser-input").unwrap(),
        incarnation: SourceIncarnation([92; 16]),
    };
    let parser = newline().parser();
    let binding = SourceBinding {
        source: source.clone(),
        parser,
        output_stream: output.clone(),
    };
    runtime
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin-parser-input").unwrap(),
            binding: binding.clone(),
        })
        .await
        .unwrap();
    runtime
        .capture_segment(RawSegment {
            start: SourcePosition {
                source: source.clone(),
                offset: 0,
            },
            bytes: Payload::copy_from_slice(b"one\ntwo\n"),
        })
        .await
        .unwrap();
    runtime
        .append_captured(
            &output,
            JournaledOutput {
                source: source.clone(),
                position: DecodedPosition {
                    source_byte: 0,
                    item_index: 0,
                },
                event: event("parser-0", b"one"),
            },
        )
        .await
        .unwrap();
    runtime.shutdown(Duration::from_secs(2)).await.unwrap();

    let runtime = Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
        .await
        .unwrap();
    let service =
        JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
    let progress = service
        .recover_captured(
            &binding,
            newline(),
            |frame: ByteFrame, position: DecodedPosition| {
                Ok(event(
                    &format!("parser-{}", position.item_index),
                    frame.as_bytes(),
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(progress.captured_offset, 8);
    assert_eq!(progress.next_item_index, 2);
    let page = runtime
        .read_after(
            &Cursor::new(output.clone(), 0),
            PageLimits {
                max_records: 4,
                max_bytes: 1024 * 1024,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].event.payload.as_bytes(), b"one");
    assert_eq!(page.records[1].event.payload.as_bytes(), b"two");
    runtime.shutdown(Duration::from_secs(2)).await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn sqlite_journal_output_transaction_rolls_back_or_resolves_exact_retry() {
    use event_stream::{
        BeginSource, DecodedPosition, EnableRetryPolicy, JournalError, JournalOperationId,
        JournaledOutput, ParserId, ParserRef, RetentionOperationId, RetentionStore, SourceBinding,
        SourceId, SourceIncarnation, SourceJournalStore, SourceKey,
    };

    for (label, injection, committed) in [
        (
            "before",
            SqliteFailureInjection::BeforeJournalOutputCommit,
            false,
        ),
        (
            "after",
            SqliteFailureInjection::AfterJournalOutputCommitAcknowledgementLost,
            true,
        ),
    ] {
        let (path, dir) = temp_db(&format!("journal-output-{label}"));
        let mut options = SqliteOptions::new(&path);
        options.failure_injection = Some(injection);
        let store = SqliteStore::open(options).await.unwrap();
        let output = store
            .create_if_absent(&StreamId::new("journal-output").unwrap())
            .await
            .unwrap();
        store
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new("enable-journal-output").unwrap(),
                stream: output.clone(),
            })
            .await
            .unwrap();
        let source = SourceKey {
            id: SourceId::new("journal-input").unwrap(),
            incarnation: SourceIncarnation([93; 16]),
        };
        store
            .begin_source(BeginSource {
                operation_id: JournalOperationId::new("begin-journal-input").unwrap(),
                binding: SourceBinding {
                    source: source.clone(),
                    parser: ParserRef {
                        id: ParserId::new("bytes").unwrap(),
                        version: 1,
                    },
                    output_stream: output.clone(),
                },
            })
            .await
            .unwrap();
        let request = JournaledOutput {
            source: source.clone(),
            position: DecodedPosition {
                source_byte: 0,
                item_index: 0,
            },
            event: event("journal-event", b"once"),
        };
        let first = store.append_captured(&output, request.clone()).await;
        assert_eq!(
            matches!(first, Err(JournalError::OutputUnknown { .. })),
            committed
        );
        assert_eq!(
            matches!(first, Err(JournalError::StorageFailure(_))),
            !committed
        );
        let raw = Connection::open(&path).unwrap();
        assert_eq!(
            raw.query_row("SELECT count(*) FROM journal_markers", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            i64::from(committed)
        );
        drop(raw);
        let retry = store.append_captured(&output, request).await.unwrap();
        assert_eq!(
            retry.kind,
            if committed {
                AppendKind::Deduplicated
            } else {
                AppendKind::Inserted
            }
        );
        let page = store
            .read_range(
                &output,
                0,
                1,
                PageLimits {
                    max_records: 2,
                    max_bytes: 1024 * 1024,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        store.close().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn sqlite_journal_capture_and_checkpoint_lost_ack_resolve_after_restart() {
    use event_stream::{
        BeginSource, JournalError, JournalOperationId, ParserCheckpoint, ParserId, ParserRef,
        RawSegment, SourceBinding, SourceId, SourceIncarnation, SourceJournalStore, SourceKey,
        SourcePosition,
    };

    let (path, dir) = temp_db("journal-capture-checkpoint-unknown");
    let mut options = SqliteOptions::new(&path);
    options.failure_injection =
        Some(SqliteFailureInjection::AfterJournalCaptureCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    let output = store
        .create_if_absent(&StreamId::new("journal-output").unwrap())
        .await
        .unwrap();
    let source = SourceKey {
        id: SourceId::new("journal-input").unwrap(),
        incarnation: SourceIncarnation([94; 16]),
    };
    let parser = ParserRef {
        id: ParserId::new("bytes").unwrap(),
        version: 1,
    };
    store
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin-journal-input").unwrap(),
            binding: SourceBinding {
                source: source.clone(),
                parser: parser.clone(),
                output_stream: output.clone(),
            },
        })
        .await
        .unwrap();
    let segment = RawSegment {
        start: SourcePosition {
            source: source.clone(),
            offset: 0,
        },
        bytes: Payload::copy_from_slice(b"abc"),
    };
    assert!(matches!(
        store.capture_segment(segment.clone()).await,
        Err(JournalError::CaptureUnknown(_))
    ));
    store.close().await.unwrap();

    let mut options = SqliteOptions::new(&path);
    options.failure_injection =
        Some(SqliteFailureInjection::AfterJournalCheckpointCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    let receipt = store.capture_segment(segment).await.unwrap();
    assert_eq!(receipt.end, 3);
    let checkpoint = ParserCheckpoint {
        source: SourcePosition {
            source: source.clone(),
            offset: 3,
        },
        parser,
        state: Payload::copy_from_slice(b"checkpoint"),
        next_item_index: 0,
        output_stream: output,
        committed_output: None,
    };
    assert!(matches!(
        store.publish_parser_checkpoint(checkpoint.clone()).await,
        Err(JournalError::CheckpointUnknown(_))
    ));
    store.close().await.unwrap();

    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        store.latest_checkpoint(&source).await.unwrap(),
        Some(checkpoint.clone())
    );
    assert_eq!(
        store
            .publish_parser_checkpoint(checkpoint)
            .await
            .unwrap()
            .checkpoint
            .source
            .offset,
        3
    );
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn sqlite_retention_reuses_ids_across_generations_and_cleans_only_after_both_bounds() {
    use event_stream::{
        AdvanceRetentionFloor, AdvanceRetryGeneration, EnableRetryPolicy, ExpireRetryGenerations,
        GeneratedEvent, RetentionCleanupLimits, RetentionOperationId, RetentionStore,
        RetryGeneration,
    };

    let (path, dir) = temp_db("retention-generations");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("retained").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("same", b"legacy"))
        .await
        .unwrap();
    let enabled = store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .enable_retry_policy(enabled.request.clone())
            .await
            .unwrap(),
        enabled
    );
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("same", b"generation-one"),
            },
        )
        .await
        .unwrap();
    let advanced = store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("advance").unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .advance_retry_generation(advanced.request.clone())
            .await
            .unwrap(),
        advanced
    );
    let second = RetryGeneration::new(2);
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: second,
                event: event("same", b"generation-two"),
            },
        )
        .await
        .unwrap();
    let merged = store
        .read_range(
            &stream,
            0,
            3,
            PageLimits {
                max_records: 3,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        merged
            .records
            .iter()
            .map(|record| record.event.payload.as_bytes())
            .collect::<Vec<_>>(),
        vec![
            b"legacy".as_slice(),
            b"generation-one".as_slice(),
            b"generation-two".as_slice()
        ]
    );
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("floor").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 2),
        })
        .await
        .unwrap();
    let before_expiry = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(before_expiry.removed_event_rows, 0);
    assert_eq!(
        store
            .lookup_generated(
                &stream,
                RetryGeneration::FIRST,
                &EventId::new("same").unwrap()
            )
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"generation-one"
    );
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("expire").unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: second,
        })
        .await
        .unwrap();
    let cleaned = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_retry_rows, 2);
    assert_eq!(cleaned.removed_event_rows, 2);
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        reopened
            .lookup_generated(&stream, second, &EventId::new("same").unwrap())
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"generation-two"
    );
    let page = reopened
        .read_range(
            &stream,
            2,
            3,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].event.payload.as_bytes(), b"generation-two");
    reopened
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("reset-retained").unwrap(),
            expected: stream.clone(),
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap();
    let retired = reopened
        .cleanup_retired(CleanupLimits {
            max_records: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(retired.removed_records, 1);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn sqlite_retention_floor_respects_active_snapshot_recovery_lease() {
    use event_stream::{
        AdvanceRetentionFloor, EnableRetryPolicy, GeneratedEvent, RetentionError,
        RetentionOperationId, RetentionStore, RetryGeneration, SnapshotDescriptor, SnapshotDigest,
        SnapshotId, SnapshotStore, VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let (path, dir) = temp_db("retention-recovery-pin");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("pinned").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("legacy", b"zero"))
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("pin-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("generated", b"one"),
            },
        )
        .await
        .unwrap();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([77; 16]),
        covered: Cursor::new(stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    store
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(descriptor.id).await.unwrap();
    let plan = store
        .acquire_recovery(descriptor.id, Duration::from_secs(10))
        .await
        .unwrap();
    let request = AdvanceRetentionFloor {
        operation_id: RetentionOperationId::new("pin-floor").unwrap(),
        stream: stream.clone(),
        expected_floor: Cursor::new(stream.clone(), 0),
        new_floor: Cursor::new(stream.clone(), 2),
    };
    assert!(matches!(
        store.advance_retention_floor(request.clone()).await,
        Err(RetentionError::RecoveryProtectionActive { .. })
    ));
    store.release_recovery(plan.lease).await.unwrap();
    store.advance_retention_floor(request).await.unwrap();
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "retention")]
#[tokio::test]
async fn sqlite_retention_commit_failures_roll_back_and_lost_acks_resolve_after_restart() {
    use event_stream::{
        AdvanceRetryGeneration, EnableRetryPolicy, GeneratedEvent, RetentionError,
        RetentionOperationId, RetentionStore, RetryGeneration,
    };

    let (path, dir) = temp_db("retention-commit-boundaries");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("retention-commit-boundaries").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-boundary").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store.close().await.unwrap();

    let advance = AdvanceRetryGeneration {
        operation_id: RetentionOperationId::new("advance-boundary").unwrap(),
        stream: stream.clone(),
        expected_current: RetryGeneration::FIRST,
    };
    let mut before = SqliteOptions::new(&path);
    before.failure_injection = Some(SqliteFailureInjection::BeforeRetentionCommit);
    let store = SqliteStore::open(before).await.unwrap();
    assert!(matches!(
        store.advance_retry_generation(advance.clone()).await,
        Err(RetentionError::StorageFailure(_))
    ));
    store.close().await.unwrap();

    let mut lost = SqliteOptions::new(&path);
    lost.failure_injection = Some(SqliteFailureInjection::AfterRetentionCommitAcknowledgementLost);
    let store = SqliteStore::open(lost).await.unwrap();
    assert!(matches!(
        store.advance_retry_generation(advance.clone()).await,
        Err(RetentionError::AdvanceGenerationUnknown(request)) if *request == advance
    ));
    store.close().await.unwrap();

    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let receipt = store
        .advance_retry_generation(advance.clone())
        .await
        .unwrap();
    assert_eq!(receipt.request, advance);
    store.close().await.unwrap();

    let generated = GeneratedEvent {
        generation: RetryGeneration::new(2),
        event: event("generated-boundary", b"committed-before-lost-ack"),
    };
    let mut lost = SqliteOptions::new(&path);
    lost.failure_injection = Some(SqliteFailureInjection::AfterRetentionCommitAcknowledgementLost);
    let store = SqliteStore::open(lost).await.unwrap();
    assert!(matches!(
        store.append_generated(&stream, generated.clone()).await,
        Err(RetentionError::GeneratedAppendUnknown(identity))
            if identity.stream == stream
                && identity.generation == generated.generation
                && identity.event_id == generated.event.id
    ));
    store.close().await.unwrap();

    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let retry = store.append_generated(&stream, generated).await.unwrap();
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(
        retry.record.event.payload.as_bytes(),
        b"committed-before-lost-ack"
    );
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn published_snapshot_bytes_and_upload_state_survive_restart() {
    use event_stream::{
        Cursor, SnapshotChunk, SnapshotDescriptor, SnapshotDigest, SnapshotError, SnapshotId,
        SnapshotStore, SnapshotUploadState, VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let (path, dir) = temp_db("snapshot-restart");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-restart").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("one", b"event"))
        .await
        .unwrap();
    let bytes = b"persistent snapshot";
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([11; 16]),
        covered: Cursor::new(stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state.v1").unwrap(),
            version: 1,
        },
        content_bytes: bytes.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(bytes).into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    store
        .put_snapshot_chunk(
            descriptor.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(bytes),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .verify_snapshot_step(
                descriptor.id,
                VerificationLimits {
                    max_chunks: 8,
                    max_bytes: 4096
                }
            )
            .await
            .unwrap()
            .state,
        SnapshotUploadState::Verified
    );
    store.publish_snapshot(descriptor.id).await.unwrap();

    let aborted = SnapshotDescriptor {
        id: SnapshotId::from_bytes([12; 16]),
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
        ..descriptor.clone()
    };
    store.begin_snapshot(aborted.clone()).await.unwrap();
    store.abort_snapshot(aborted.id).await.unwrap();
    assert!(matches!(
        store
            .verify_snapshot_step(
                aborted.id,
                VerificationLimits {
                    max_chunks: 1,
                    max_bytes: 1
                }
            )
            .await,
        Err(SnapshotError::IncompleteUpload { .. })
    ));
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        reopened.snapshot_status(descriptor.id).await.unwrap().state,
        SnapshotUploadState::Published
    );
    let plan = reopened
        .acquire_recovery(descriptor.id, Duration::from_secs(10))
        .await
        .unwrap();
    assert_eq!(
        reopened
            .read_snapshot_chunk(plan.lease, 0, 4096)
            .await
            .unwrap()
            .bytes
            .as_bytes(),
        bytes
    );
    assert_eq!(
        reopened.snapshot_status(aborted.id).await.unwrap().state,
        SnapshotUploadState::Aborted
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn recovery_lease_keeps_retired_suffix_readable_until_release() {
    use event_stream::{
        Cursor, SnapshotDescriptor, SnapshotDigest, SnapshotId, SnapshotStore, VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let (path, dir) = temp_db("snapshot-retired-recovery");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-retired-recovery").unwrap())
        .await
        .unwrap();
    for n in 1..=3 {
        store
            .append_atomic(&stream, event(&format!("event-{n}"), &[n]))
            .await
            .unwrap();
    }
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([21; 16]),
        covered: Cursor::new(stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("state.v1").unwrap(),
            version: 1,
        },
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    store.begin_snapshot(descriptor.clone()).await.unwrap();
    store
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(descriptor.id).await.unwrap();
    let plan = store
        .acquire_recovery(descriptor.id, Duration::from_secs(30))
        .await
        .unwrap();
    store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("reset-protected").unwrap(),
            expected: stream.clone(),
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap();
    let cleanup = store
        .cleanup_retired(CleanupLimits {
            max_records: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.removed_records, 1);
    assert!(cleanup.remaining);
    let page = store
        .read_recovery_page(
            plan.lease,
            1,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.records
            .iter()
            .map(|record| record.cursor.offset)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    store.release_recovery(plan.lease).await.unwrap();
    let cleanup = store
        .cleanup_retired(CleanupLimits {
            max_records: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleanup.removed_records, 2);
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn malformed_snapshot_identifier_and_partial_schema_are_rejected_on_open() {
    use event_stream::{Cursor, SnapshotDescriptor, SnapshotDigest, SnapshotId, SnapshotStore};
    use sha2::{Digest, Sha256};

    for partial_schema in [false, true] {
        let (path, dir) = temp_db(if partial_schema {
            "snapshot-partial-schema"
        } else {
            "snapshot-bad-id"
        });
        let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        let stream = store
            .create_if_absent(&StreamId::new("snapshot-corrupt").unwrap())
            .await
            .unwrap();
        let descriptor = SnapshotDescriptor {
            id: SnapshotId::from_bytes([31; 16]),
            covered: Cursor::new(stream, 0),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: 0,
            digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
        };
        store.begin_snapshot(descriptor).await.unwrap();
        store.close().await.unwrap();
        let conn = Connection::open(&path).unwrap();
        if partial_schema {
            conn.execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE snapshot_chunks;")
                .unwrap();
        } else {
            conn.execute_batch("PRAGMA ignore_check_constraints=ON; UPDATE snapshots SET snapshot_id=zeroblob(17);").unwrap();
        }
        drop(conn);
        assert!(matches!(
            SqliteStore::open(SqliteOptions::new(&path)).await,
            Err(Error::StoreCorrupt(_))
        ));
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn snapshot_page_limit_above_sqlite_integer_range_is_rejected() {
    use event_stream::{SnapshotError, SnapshotStore};

    let (path, dir) = temp_db("snapshot-page-overflow");
    let mut options = SqliteOptions::new(&path);
    options.snapshots.storage.max_list_page.max_records = usize::MAX;
    let store = SqliteStore::open(options).await.unwrap();
    assert!(matches!(
        store
            .list_snapshot_uploads(
                None,
                PageLimits {
                    max_records: usize::MAX,
                    max_bytes: 1
                }
            )
            .await,
        Err(SnapshotError::InvalidInput(_))
    ));
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn positive_but_forged_snapshot_accounting_is_rejected_on_open() {
    use event_stream::{Cursor, SnapshotDescriptor, SnapshotDigest, SnapshotId, SnapshotStore};
    use sha2::{Digest, Sha256};

    let (path, dir) = temp_db("snapshot-forged-accounting");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-accounting").unwrap())
        .await
        .unwrap();
    store
        .begin_snapshot(SnapshotDescriptor {
            id: SnapshotId::from_bytes([41; 16]),
            covered: Cursor::new(stream, 0),
            schema: SchemaRef {
                id: SchemaId::new("state").unwrap(),
                version: 1,
            },
            content_bytes: 0,
            digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
        })
        .await
        .unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute_batch(
            "UPDATE snapshots SET descriptor_charge=descriptor_charge+1;
         UPDATE snapshot_metadata SET descriptor_metadata_bytes=descriptor_metadata_bytes+1;",
        )
        .unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn snapshot_commit_faults_preserve_identity_and_retry_outcomes() {
    use event_stream::{
        Cursor, SnapshotChunk, SnapshotDescriptor, SnapshotDigest, SnapshotError, SnapshotId,
        SnapshotStore, SnapshotUploadState, VerificationLimits,
    };
    use sha2::{Digest, Sha256};

    let (path, dir) = temp_db("snapshot-commit-faults");
    let setup = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = setup
        .create_if_absent(&StreamId::new("snapshot-faults").unwrap())
        .await
        .unwrap();
    setup.close().await.unwrap();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([51; 16]),
        covered: Cursor::new(stream.clone(), 0),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: 1,
        digest: SnapshotDigest::from_bytes(Sha256::digest(b"x").into()),
    };

    let mut options = SqliteOptions::new(&path);
    options.failure_injection =
        Some(SqliteFailureInjection::AfterSnapshotCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    assert_eq!(
        store.begin_snapshot(descriptor.clone()).await.unwrap_err(),
        SnapshotError::BeginUnknown {
            descriptor: Box::new(descriptor.clone())
        }
    );
    store.close().await.unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        store
            .begin_snapshot(descriptor.clone())
            .await
            .unwrap()
            .state,
        SnapshotUploadState::Uploading
    );
    store.close().await.unwrap();

    let chunk = SnapshotChunk {
        offset: 0,
        bytes: Payload::copy_from_slice(b"x"),
    };
    let mut options = SqliteOptions::new(&path);
    options.failure_injection =
        Some(SqliteFailureInjection::AfterSnapshotCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    assert_eq!(
        store
            .put_snapshot_chunk(descriptor.id, chunk.clone())
            .await
            .unwrap_err(),
        SnapshotError::ChunkUnknown {
            id: descriptor.id,
            chunk: Box::new(chunk.clone())
        }
    );
    store.close().await.unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        store
            .put_snapshot_chunk(descriptor.id, chunk)
            .await
            .unwrap()
            .accepted_bytes,
        1
    );
    store
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.close().await.unwrap();

    let mut options = SqliteOptions::new(&path);
    options.failure_injection =
        Some(SqliteFailureInjection::AfterSnapshotCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    assert_eq!(
        store.publish_snapshot(descriptor.id).await.unwrap_err(),
        SnapshotError::PublicationUnknown {
            descriptor: Box::new(descriptor.clone())
        }
    );
    store.close().await.unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        store.publish_snapshot(descriptor.id).await.unwrap(),
        descriptor
    );
    store.close().await.unwrap();

    let aborted = SnapshotDescriptor {
        id: SnapshotId::from_bytes([52; 16]),
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
        ..descriptor.clone()
    };
    let setup = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    setup.begin_snapshot(aborted.clone()).await.unwrap();
    setup.close().await.unwrap();
    let mut options = SqliteOptions::new(&path);
    options.failure_injection =
        Some(SqliteFailureInjection::AfterSnapshotCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    assert_eq!(
        store.abort_snapshot(aborted.id).await.unwrap_err(),
        SnapshotError::AbortUnknown { id: aborted.id }
    );
    store.close().await.unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(
        store
            .abort_snapshot(aborted.id)
            .await
            .unwrap()
            .already_aborted
    );
    store.close().await.unwrap();

    let rejected = SnapshotDescriptor {
        id: SnapshotId::from_bytes([53; 16]),
        ..aborted
    };
    let mut options = SqliteOptions::new(&path);
    options.failure_injection = Some(SqliteFailureInjection::BeforeSnapshotCommit);
    let store = SqliteStore::open(options).await.unwrap();
    assert!(matches!(
        store.begin_snapshot(rejected.clone()).await,
        Err(SnapshotError::StorageFailure(_))
    ));
    store.close().await.unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        store.snapshot_status(rejected.id).await.unwrap_err(),
        SnapshotError::NotFound { id: rejected.id }
    );
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn lifecycle_receipts_and_unavailable_name_survive_restart_and_cleanup() {
    let (path, dir) = temp_db("lifecycle-restart");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let first = store
        .create_if_absent(&StreamId::new("retained-name").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&first, event("retired", b"history"))
        .await
        .unwrap();
    let request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("delete-restart").unwrap(),
        expected: first.clone(),
        action: LifecycleAction::Delete,
    };
    let receipt = store.change_lifecycle(request.clone()).await.unwrap();
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(reopened.change_lifecycle(request).await.unwrap(), receipt);
    let progress = reopened
        .cleanup_retired(CleanupLimits {
            max_records: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(progress.stream, Some(first.clone()));
    assert_eq!(progress.removed_records, 1);
    assert!(!progress.remaining);
    assert!(matches!(
        reopened
            .create_if_absent(&StreamId::new("retained-name").unwrap())
            .await,
        Err(Error::StreamUnavailable { last }) if *last == first
    ));
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn lifecycle_quotas_are_atomic_and_identical_retries_bypass_capacity() {
    let (path, dir) = temp_db("lifecycle-quotas");
    let mut options = SqliteOptions::new(&path);
    options.max_lifecycle_receipts = 1;
    options.max_lifecycle_receipt_bytes = 4096;
    options.max_retired_lifetimes = 1;
    options.max_retired_metadata_bytes = 4096;
    let store = SqliteStore::open(options).await.unwrap();
    let first = store
        .create_if_absent(&StreamId::new("quota-a").unwrap())
        .await
        .unwrap();
    let request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("quota-op").unwrap(),
        expected: first.clone(),
        action: LifecycleAction::Delete,
    };
    let receipt = store.change_lifecycle(request.clone()).await.unwrap();
    assert_eq!(store.change_lifecycle(request).await.unwrap(), receipt);
    let second = store
        .create_if_absent(&StreamId::new("quota-b").unwrap())
        .await
        .unwrap();
    assert!(matches!(
        store
            .change_lifecycle(LifecycleRequest {
                operation_id: LifecycleOperationId::new("quota-op-2").unwrap(),
                expected: second,
                action: LifecycleAction::Delete,
            })
            .await,
        Err(Error::CapacityExceeded)
    ));
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let accounting: (u32, u32, u32, u32) = conn
        .query_row(
            "SELECT lifecycle_receipt_count,lifecycle_receipt_bytes,retired_lifetime_count,retired_metadata_bytes FROM event_stream_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap();
    assert_eq!(accounting.0, 1);
    assert_eq!(accounting.2, 1);
    assert!(accounting.1 > 0 && accounting.3 > 0);
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn understated_lifecycle_counters_are_rejected_on_open() {
    let (path, dir) = temp_db("lifecycle-counter-corrupt");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("counter-corrupt").unwrap())
        .await
        .unwrap();
    store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("counter-op").unwrap(),
            expected: stream,
            action: LifecycleAction::Delete,
        })
        .await
        .unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE event_stream_metadata SET lifecycle_receipt_count=0",
            [],
        )
        .unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn internally_inconsistent_lifecycle_receipt_is_rejected_on_open() {
    let (path, dir) = temp_db("lifecycle-receipt-corrupt");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("receipt-corrupt").unwrap())
        .await
        .unwrap();
    let request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("receipt-corrupt-op").unwrap(),
        expected: stream,
        action: LifecycleAction::Delete,
    };
    store.change_lifecycle(request.clone()).await.unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.execute(
        "UPDATE lifecycle_receipts SET charge=charge+1 WHERE operation_id='receipt-corrupt-op'",
        [],
    )
    .unwrap();
    conn.execute(
        "UPDATE event_stream_metadata SET lifecycle_receipt_bytes=lifecycle_receipt_bytes+1",
        [],
    )
    .unwrap();
    drop(conn);
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn retired_cleanup_rejects_history_holes_and_rolls_back_prior_deletes() {
    let (path, dir) = temp_db("cleanup-hole");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("cleanup-hole").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("first", b"one"))
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("second", b"two"))
        .await
        .unwrap();
    store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("cleanup-hole-reset").unwrap(),
            expected: stream.clone(),
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let retired_key: i64 = conn
        .query_row(
            "SELECT stream_key FROM event_streams WHERE public_id='cleanup-hole' AND retired=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "DELETE FROM event_records WHERE stream_key=?1 AND offset=?2",
        params![retired_key, 2_u64.to_be_bytes().as_slice()],
    )
    .unwrap();
    drop(conn);
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        reopened
            .cleanup_retired(CleanupLimits {
                max_records: 2,
                max_bytes: 2 * 1024 * 1024,
            })
            .await,
        Err(Error::StoreCorrupt(_))
    ));
    reopened.close().await.unwrap();
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT count(*) FROM event_records WHERE stream_key=?1",
                params![retired_key],
                |r| r.get::<_, u32>(0),
            )
            .unwrap(),
        1
    );
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_active_name_pointer_cannot_retire_another_lifetime() {
    let (path, dir) = temp_db("lifecycle-pointer-corrupt");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let first = store
        .create_if_absent(&StreamId::new("pointer-a").unwrap())
        .await
        .unwrap();
    let second = store
        .create_if_absent(&StreamId::new("pointer-b").unwrap())
        .await
        .unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let second_key: i64 = conn
        .query_row(
            "SELECT active_stream_key FROM event_stream_names WHERE public_id='pointer-b'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE event_stream_names SET active_stream_key=?1 WHERE public_id='pointer-a'",
        params![second_key],
    )
    .unwrap();
    drop(conn);
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        reopened
            .create_if_absent(&StreamId::new("pointer-a").unwrap())
            .await,
        Err(Error::StoreCorrupt(_))
    ));
    assert!(matches!(
        reopened
            .change_lifecycle(LifecycleRequest {
                operation_id: LifecycleOperationId::new("pointer-op").unwrap(),
                expected: first,
                action: LifecycleAction::Delete,
            })
            .await,
        Err(Error::StoreCorrupt(_))
    ));
    assert_eq!(reopened.bounds(&second).await.unwrap().tail.offset, 0);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn lifecycle_and_cleanup_failure_boundaries_are_atomic_and_retryable() {
    let (path, dir) = temp_db("lifecycle-faults");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let first = store
        .create_if_absent(&StreamId::new("faulted-lifecycle").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&first, event("history", b"preserved"))
        .await
        .unwrap();
    store.close().await.unwrap();

    let request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("faulted-reset").unwrap(),
        expected: first.clone(),
        action: LifecycleAction::Reset,
    };
    let mut before = SqliteOptions::new(&path);
    before.failure_injection = Some(SqliteFailureInjection::BeforeLifecycleCommit);
    let store = SqliteStore::open(before).await.unwrap();
    assert!(matches!(
        store.change_lifecycle(request.clone()).await,
        Err(Error::StoreWriteFailed(_))
    ));
    assert_eq!(store.bounds(&first).await.unwrap().tail.offset, 1);
    store.close().await.unwrap();

    let mut lost = SqliteOptions::new(&path);
    lost.failure_injection = Some(SqliteFailureInjection::AfterLifecycleCommitAcknowledgementLost);
    let store = SqliteStore::open(lost).await.unwrap();
    let receipt = store.change_lifecycle(request.clone()).await.unwrap();
    let replacement = receipt.replacement.clone().unwrap();
    assert_eq!(store.change_lifecycle(request).await.unwrap(), receipt);
    store.close().await.unwrap();

    let mut cleanup_fault = SqliteOptions::new(&path);
    cleanup_fault.failure_injection = Some(SqliteFailureInjection::BeforeCleanupCommit);
    let store = SqliteStore::open(cleanup_fault).await.unwrap();
    assert!(matches!(
        store
            .cleanup_retired(CleanupLimits {
                max_records: 1,
                max_bytes: 2 * 1024 * 1024,
            })
            .await,
        Err(Error::StoreWriteFailed(_))
    ));
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let progress = reopened
        .cleanup_retired(CleanupLimits {
            max_records: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(progress.stream, Some(first));
    assert_eq!(progress.removed_records, 1);
    assert!(!progress.remaining);
    assert_eq!(reopened.bounds(&replacement).await.unwrap().tail.offset, 0);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn restart_preserves_records_deduplication_and_incarnation() {
    let (path, dir) = temp_db("restart");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(store.capabilities().format_version, SQLITE_FORMAT_VERSION);
    let stream = store
        .create_if_absent(&StreamId::new("orders").unwrap())
        .await
        .unwrap();
    let first = store
        .append_atomic(&stream, event("e-1", b"one"))
        .await
        .unwrap();
    assert_eq!(first.kind, AppendKind::Inserted);
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let same = reopened
        .create_if_absent(&StreamId::new("orders").unwrap())
        .await
        .unwrap();
    assert_eq!(same, stream);
    let retry = reopened
        .append_atomic(&same, event("e-1", b"one"))
        .await
        .unwrap();
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(retry.record.cursor.offset, 1);
    assert!(matches!(
        reopened
            .append_atomic(&same, event("e-1", b"different"))
            .await,
        Err(Error::IdempotencyConflict { .. })
    ));
    let page = reopened
        .read_range(
            &same,
            0,
            1,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].event.payload.as_bytes(), b"one");
    assert!(page.complete);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn in_process_ownership_lasts_until_worker_stops() {
    let (path, dir) = temp_db("owner-local");
    let first = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreInUse)
    ));
    first.close().await.unwrap();
    let replacement = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    replacement.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn dropping_store_releases_ownership_after_accepted_work_stops() {
    let (path, dir) = temp_db("drop-owner");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("drop").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("one", b"saved"))
        .await
        .unwrap();
    drop(store);
    let reopened = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match SqliteStore::open(SqliteOptions::new(&path)).await {
                Ok(store) => break store,
                Err(Error::StoreInUse) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected reopen error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reopened
            .lookup_event(&stream, &EventId::new("one").unwrap())
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"saved"
    );
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn dropped_close_future_still_closes_after_a_full_queue_drains() {
    let (path, dir) = temp_db("cancel-close");
    let mut options = SqliteOptions::new(&path);
    options.worker_queue_capacity = 1;
    options.failure_injection = Some(SqliteFailureInjection::PauseBeforeCommit(
        Duration::from_millis(100),
    ));
    let store = std::sync::Arc::new(SqliteStore::open(options).await.unwrap());
    let stream = store
        .create_if_absent(&StreamId::new("closing").unwrap())
        .await
        .unwrap();
    let first = {
        let store = store.clone();
        let stream = stream.clone();
        tokio::spawn(async move { store.append_atomic(&stream, event("first", b"one")).await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    let second = {
        let store = store.clone();
        let stream = stream.clone();
        tokio::spawn(async move { store.append_atomic(&stream, event("second", b"two")).await })
    };
    tokio::task::yield_now().await;
    let closing = {
        let store = store.clone();
        tokio::spawn(async move { store.close().await })
    };
    loop {
        match store.bounds(&stream).await {
            Err(Error::Closed) => break,
            Err(Error::Overloaded) => tokio::task::yield_now().await,
            result => panic!("unexpected state while starting close: {result:?}"),
        }
    }
    closing.abort();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    drop(store);
    let reopened = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match SqliteStore::open(SqliteOptions::new(&path)).await {
                Ok(store) => break store,
                Err(Error::StoreInUse) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected reopen error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(reopened.bounds(&stream).await.unwrap().tail.offset, 2);
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn sqlite_child() {
    let Some(path) = std::env::var_os("EVENT_STREAM_CHILD_DB") else {
        return;
    };
    let mode = std::env::var("EVENT_STREAM_CHILD_MODE").unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let mut options = SqliteOptions::new(PathBuf::from(path));
        if mode == "before_commit" {
            options.failure_injection = Some(SqliteFailureInjection::PauseBeforeCommit(
                Duration::from_secs(60),
            ));
        } else if mode == "lost_receipt" {
            options.failure_injection =
                Some(SqliteFailureInjection::PauseAfterCommitAcknowledgementLost(
                    Duration::from_secs(60),
                ));
        } else if mode == "snapshot_publish_lost" {
            options.failure_injection = Some(
                SqliteFailureInjection::PauseAfterSnapshotCommitAcknowledgementLost(
                    Duration::from_secs(60),
                ),
            );
        } else if mode == "journal_output_lost" {
            options.failure_injection = Some(
                SqliteFailureInjection::PauseAfterJournalOutputCommitAcknowledgementLost(
                    Duration::from_secs(60),
                ),
            );
        }
        let store = SqliteStore::open(options).await.unwrap();
        if matches!(mode.as_str(), "append" | "before_commit" | "lost_receipt") {
            let stream = store
                .create_if_absent(&StreamId::new("crash").unwrap())
                .await
                .unwrap();
            if mode != "append" {
                println!("STARTING");
                std::io::stdout().flush().unwrap();
            }
            store
                .append_atomic(&stream, event("committed", b"survives-process-kill"))
                .await
                .unwrap();
        }
        #[cfg(feature = "snapshots")]
        if mode == "snapshot_publish_lost" {
            println!("STARTING");
            std::io::stdout().flush().unwrap();
            store
                .publish_snapshot(SnapshotId::from_bytes([99; 16]))
                .await
                .unwrap();
        }
        #[cfg(feature = "source-journal")]
        if mode == "journal_output_lost" {
            use event_stream::{
                BeginSource, DecodedPosition, EnableRetryPolicy, JournalOperationId,
                JournaledOutput, ParserId, ParserRef, RetentionOperationId, RetentionStore,
                SourceBinding, SourceId, SourceIncarnation, SourceJournalStore, SourceKey,
            };
            let output = store
                .create_if_absent(&StreamId::new("journal-crash-output").unwrap())
                .await
                .unwrap();
            store
                .enable_retry_policy(EnableRetryPolicy {
                    operation_id: RetentionOperationId::new("journal-crash-enable").unwrap(),
                    stream: output.clone(),
                })
                .await
                .unwrap();
            let source = SourceKey {
                id: SourceId::new("journal-crash-input").unwrap(),
                incarnation: SourceIncarnation([95; 16]),
            };
            store
                .begin_source(BeginSource {
                    operation_id: JournalOperationId::new("journal-crash-begin").unwrap(),
                    binding: SourceBinding {
                        source: source.clone(),
                        parser: ParserRef {
                            id: ParserId::new("bytes").unwrap(),
                            version: 1,
                        },
                        output_stream: output.clone(),
                    },
                })
                .await
                .unwrap();
            println!("STARTING");
            std::io::stdout().flush().unwrap();
            store
                .append_captured(
                    &output,
                    JournaledOutput {
                        source,
                        position: DecodedPosition {
                            source_byte: 0,
                            item_index: 0,
                        },
                        event: event("journal-crash-event", &vec![0x5a; 96 * 1024]),
                    },
                )
                .await
                .unwrap();
        }
        println!("READY");
        std::io::stdout().flush().unwrap();
        if mode == "hold" {
            let mut line = String::new();
            std::io::stdin().read_line(&mut line).unwrap();
            store.close().await.unwrap();
        } else {
            loop {
                std::thread::park_timeout(Duration::from_secs(60));
            }
        }
    });
}

#[cfg(feature = "source-journal")]
#[tokio::test]
async fn process_kill_after_journal_output_commit_resolves_exact_content_retry() {
    use event_stream::{
        DecodedPosition, JournaledOutput, SourceId, SourceIncarnation, SourceJournalStore,
        SourceKey,
    };

    let (path, dir) = temp_db("journal-kill-after-output");
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_child", "--nocapture", "--test-threads=1"])
        .env("EVENT_STREAM_CHILD_DB", &path)
        .env("EVENT_STREAM_CHILD_MODE", "journal_output_lost")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));
    wait_for_child_marker(&mut child, "STARTING");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let committed = Connection::open(&path)
            .and_then(|conn| {
                conn.query_row("SELECT count(*) FROM journal_markers", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .unwrap_or(0);
        if committed == 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "journal commit was not observed"
        );
        tokio::task::yield_now().await;
    }
    assert!(!child.kill_and_wait().success());

    let store = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match SqliteStore::open(SqliteOptions::new(&path)).await {
                Ok(store) => break store,
                Err(Error::StoreInUse) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected journal reopen error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    let output = store
        .create_if_absent(&StreamId::new("journal-crash-output").unwrap())
        .await
        .unwrap();
    let retry = store
        .append_captured(
            &output,
            JournaledOutput {
                source: SourceKey {
                    id: SourceId::new("journal-crash-input").unwrap(),
                    incarnation: SourceIncarnation([95; 16]),
                },
                position: DecodedPosition {
                    source_byte: 0,
                    item_index: 0,
                },
                event: event("journal-crash-event", &vec![0x5a; 96 * 1024]),
            },
        )
        .await
        .unwrap();
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(retry.record.event.payload.as_bytes(), vec![0x5a; 96 * 1024]);
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(feature = "snapshots")]
#[tokio::test]
async fn process_kill_after_snapshot_publication_commit_resolves_exact_retry() {
    use sha2::{Digest, Sha256};

    let (path, dir) = temp_db("snapshot-kill-after-commit");
    let setup = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = setup
        .create_if_absent(&StreamId::new("snapshot-kill").unwrap())
        .await
        .unwrap();
    let content = (0..96 * 1024)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([99; 16]),
        covered: Cursor::new(stream, 0),
        schema: SchemaRef {
            id: SchemaId::new("state").unwrap(),
            version: 1,
        },
        content_bytes: content.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(&content).into()),
    };
    setup.begin_snapshot(descriptor.clone()).await.unwrap();
    for (index, bytes) in content.chunks(32 * 1024).enumerate() {
        setup
            .put_snapshot_chunk(
                descriptor.id,
                event_stream::SnapshotChunk {
                    offset: (index * 32 * 1024) as u64,
                    bytes: Payload::copy_from_slice(bytes),
                },
            )
            .await
            .unwrap();
    }
    let verified = setup
        .verify_snapshot_step(
            descriptor.id,
            VerificationLimits {
                max_chunks: 8,
                max_bytes: content.len(),
            },
        )
        .await
        .unwrap();
    assert_eq!(verified.verified_bytes, content.len() as u64);
    setup.close().await.unwrap();

    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_child", "--nocapture", "--test-threads=1"])
        .env("EVENT_STREAM_CHILD_DB", &path)
        .env("EVENT_STREAM_CHILD_MODE", "snapshot_publish_lost")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(Some(child));
    wait_for_child_marker(&mut child, "STARTING");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let published = Connection::open(&path).ok().and_then(|conn| {
                conn.query_row(
                    "SELECT state FROM snapshots WHERE snapshot_id=?1",
                    [descriptor.id.as_bytes().as_slice()],
                    |row| row.get::<_, i64>(0),
                )
                .ok()
            }) == Some(3);
            if published {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert!(!child.kill_and_wait().success());
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(
        reopened.publish_snapshot(descriptor.id).await.unwrap(),
        descriptor
    );
    let plan = reopened
        .acquire_recovery(descriptor.id, Duration::from_secs(10))
        .await
        .unwrap();
    let mut restored = Vec::new();
    let mut offset = 0;
    while offset < descriptor.content_bytes {
        let page = reopened
            .read_snapshot_chunk(plan.lease, offset, 32 * 1024)
            .await
            .unwrap();
        assert!(!page.bytes.as_bytes().is_empty());
        restored.extend_from_slice(page.bytes.as_bytes());
        offset = page.next_offset;
    }
    assert_eq!(restored, content);
    reopened.release_recovery(plan.lease).await.unwrap();
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn separate_process_cannot_take_ownership() {
    let (path, dir) = temp_db("owner-process");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_child", "--nocapture", "--test-threads=1"])
        .env("EVENT_STREAM_CHILD_DB", &path)
        .env("EVENT_STREAM_CHILD_MODE", "hold")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line.contains("READY") {
            break;
        }
    }
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreInUse)
    ));
    writeln!(child.stdin.take().unwrap(), "close").unwrap();
    assert!(child.wait().unwrap().success());
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn committed_data_recovers_after_process_kill() {
    let (path, dir) = temp_db("kill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_child", "--nocapture", "--test-threads=1"])
        .env("EVENT_STREAM_CHILD_DB", &path)
        .env("EVENT_STREAM_CHILD_MODE", "append")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line.contains("READY") {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("crash").unwrap())
        .await
        .unwrap();
    let found = store
        .lookup_event(&stream, &EventId::new("committed").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.event.payload.as_bytes(), b"survives-process-kill");
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn process_kill_before_commit_rolls_back_record_and_tail() {
    let (path, dir) = temp_db("kill-before-commit");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_child", "--nocapture", "--test-threads=1"])
        .env("EVENT_STREAM_CHILD_DB", &path)
        .env("EVENT_STREAM_CHILD_MODE", "before_commit")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line.contains("STARTING") {
            break;
        }
    }
    let journal = PathBuf::from(format!("{}-journal", path.display()));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if std::fs::metadata(&journal).is_ok_and(|metadata| metadata.len() > 0) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("crash").unwrap())
        .await
        .unwrap();
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 0);
    assert!(store
        .lookup_event(&stream, &EventId::new("committed").unwrap())
        .await
        .unwrap()
        .is_none());
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn process_kill_after_commit_before_receipt_deduplicates_retry() {
    let (path, dir) = temp_db("kill-lost-receipt");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sqlite_child", "--nocapture", "--test-threads=1"])
        .env("EVENT_STREAM_CHILD_DB", &path)
        .env("EVENT_STREAM_CHILD_MODE", "lost_receipt")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        reader.read_line(&mut line).unwrap();
        if line.contains("STARTING") {
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let committed = Connection::open(&path).ok().and_then(|conn| {
                conn.query_row(
                    "SELECT count(*) FROM event_records WHERE event_id='committed'",
                    [],
                    |row| row.get::<_, u32>(0),
                )
                .ok()
            }) == Some(1);
            if committed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    child.kill().unwrap();
    child.wait().unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("crash").unwrap())
        .await
        .unwrap();
    let retry = store
        .append_atomic(&stream, event("committed", b"survives-process-kill"))
        .await
        .unwrap();
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(retry.record.cursor.offset, 1);
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn database_quota_failure_rolls_back_record_and_tail() {
    let (path, dir) = temp_db("quota");
    let mut options = SqliteOptions::new(&path);
    options.max_record_bytes = 256 * 1024;
    let initialized = SqliteStore::open(options.clone()).await.unwrap();
    initialized.close().await.unwrap();
    let initialized_pages: u32 = Connection::open(&path)
        .unwrap()
        .pragma_query_value(None, "page_count", |row| row.get(0))
        .unwrap();
    options.max_database_pages = initialized_pages.checked_add(8).unwrap();
    let store = SqliteStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("quota").unwrap())
        .await
        .unwrap();
    let append = store
        .append_atomic(&stream, event("large", &vec![9; 128 * 1024]))
        .await;
    assert!(matches!(append, Err(Error::CapacityExceeded)), "{append:?}");
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 0);
    assert!(store
        .lookup_event(&stream, &EventId::new("large").unwrap())
        .await
        .unwrap()
        .is_none());
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn injected_transaction_boundaries_preserve_atomic_state_and_resolve_unknown_commit() {
    let (path, dir) = temp_db("fault-boundaries");
    let mut stream = None;
    for (index, fault) in [
        SqliteFailureInjection::BeforeRecordInsert,
        SqliteFailureInjection::AfterRecordInsert,
        SqliteFailureInjection::BeforeCommit,
    ]
    .into_iter()
    .enumerate()
    {
        let mut options = SqliteOptions::new(&path);
        options.failure_injection = Some(fault);
        let store = SqliteStore::open(options).await.unwrap();
        let key = store
            .create_if_absent(&StreamId::new("faults").unwrap())
            .await
            .unwrap();
        stream.get_or_insert_with(|| key.clone());
        let id = format!("before-{index}");
        assert!(matches!(
            store.append_atomic(&key, event(&id, b"rollback")).await,
            Err(Error::StoreWriteFailed(_))
        ));
        assert_eq!(store.bounds(&key).await.unwrap().tail.offset, 0);
        assert!(store
            .lookup_event(&key, &EventId::new(id).unwrap())
            .await
            .unwrap()
            .is_none());
        store.close().await.unwrap();
    }
    let stream = stream.unwrap();

    let mut after = SqliteOptions::new(&path);
    after.failure_injection = Some(SqliteFailureInjection::AfterCommitAcknowledgementLost);
    let store = SqliteStore::open(after).await.unwrap();
    let receipt = store
        .append_atomic(&stream, event("after", b"committed"))
        .await
        .unwrap();
    assert_eq!(receipt.kind, AppendKind::Inserted);
    assert_eq!(receipt.record.cursor.offset, 1);
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 1);
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn unsigned_offset_representation_crosses_signed_boundary_on_a_forged_tail() {
    let (path, dir) = temp_db("u64");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("high").unwrap())
        .await
        .unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let key: i64 = conn
        .query_row(
            "SELECT stream_key FROM event_streams WHERE public_id='high'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    conn.execute(
        "UPDATE event_streams SET tail=?1 WHERE stream_key=?2",
        params![(u64::MAX - 1).to_be_bytes().as_slice(), key],
    )
    .unwrap();
    // Direct metadata mutation reaches the representation boundary without
    // writing 2^64 records. This does not represent healthy no-gap history.
    drop(conn);
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let inserted = store
        .append_atomic(&stream, event("max", b"last"))
        .await
        .unwrap();
    assert_eq!(inserted.record.cursor.offset, u64::MAX);
    assert!(matches!(
        store.append_atomic(&stream, event("overflow", b"no")).await,
        Err(Error::OffsetOverflow)
    ));
    let page = store
        .read_range(
            &stream,
            u64::MAX - 1,
            u64::MAX,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].cursor.offset, u64::MAX);
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn newer_format_is_rejected_without_migration() {
    let (path, dir) = temp_db("format");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("format").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("preserved", b"bytes"))
        .await
        .unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let mode: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode.to_ascii_lowercase(), "wal");
    conn.execute("UPDATE event_stream_metadata SET format_version=99", [])
        .unwrap();
    drop(conn);
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::UnsupportedFormat(99))
    ));
    let conn = Connection::open(&path).unwrap();
    let version: u32 = conn
        .query_row(
            "SELECT format_version FROM event_stream_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, 99);
    let mode: String = conn
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode.to_ascii_lowercase(), "wal");
    let records: u32 = conn
        .query_row("SELECT count(*) FROM event_records", [], |row| row.get(0))
        .unwrap();
    assert_eq!(records, 1);
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
}

fn create_format_one_fixture(path: &PathBuf, valid: bool) -> i64 {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "PRAGMA foreign_keys=ON;
         CREATE TABLE event_stream_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),format_version INTEGER NOT NULL);
         CREATE TABLE event_streams(stream_key INTEGER PRIMARY KEY,public_id TEXT NOT NULL UNIQUE,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),floor BLOB NOT NULL CHECK(length(floor)=8),tail BLOB NOT NULL CHECK(length(tail)=8));
         CREATE TABLE event_records(stream_key INTEGER NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),event_id TEXT NOT NULL,schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(stream_key,offset),UNIQUE(stream_key,event_id),FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
         INSERT INTO event_stream_metadata(singleton,format_version) VALUES(1,1);",
    )
    .unwrap();
    let incarnation: Vec<u8> = if valid { vec![7; 16] } else { vec![7; 15] };
    if !valid {
        conn.execute_batch("PRAGMA ignore_check_constraints=ON;")
            .unwrap();
    }
    conn.execute(
        "INSERT INTO event_streams(stream_key,public_id,incarnation,floor,tail) VALUES(1,'migrated',?1,?2,?3)",
        params![incarnation, 0_u64.to_be_bytes().as_slice(), 1_u64.to_be_bytes().as_slice()],
    )
    .unwrap();
    if !valid {
        conn.execute_batch("PRAGMA ignore_check_constraints=OFF;")
            .unwrap();
    }
    conn.execute(
        "INSERT INTO event_records(stream_key,offset,event_id,schema_id,schema_version,payload) VALUES(1,?1,'legacy-event','legacy.schema',9,?2)",
        params![1_u64.to_be_bytes().as_slice(), b"exact legacy bytes"],
    )
    .unwrap();
    conn.query_row(
        "SELECT rootpage FROM sqlite_schema WHERE type='table' AND name='event_records'",
        [],
        |r| r.get(0),
    )
    .unwrap()
}

#[tokio::test]
async fn format_one_migration_preserves_record_table_and_exact_bytes() {
    let (path, dir) = temp_db("format-one-migration");
    let record_rootpage = create_format_one_fixture(&path, true);
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("migrated").unwrap())
        .await
        .unwrap();
    let page = store
        .read_range(
            &stream,
            0,
            1,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        page.records[0].event.payload.as_bytes(),
        b"exact legacy bytes"
    );
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT rootpage FROM sqlite_schema WHERE type='table' AND name='event_records'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap(),
        record_rootpage
    );
    assert_eq!(
        conn.query_row(
            "SELECT format_version FROM event_stream_metadata WHERE singleton=1",
            [],
            |r| r.get::<_, u32>(0),
        )
        .unwrap(),
        SQLITE_FORMAT_VERSION
    );
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_format_one_input_fails_migration_without_partial_schema_change() {
    let (path, dir) = temp_db("format-one-rollback");
    let record_rootpage = create_format_one_fixture(&path, false);
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreWriteFailed(_))
    ));
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT format_version FROM event_stream_metadata WHERE singleton=1",
            [],
            |r| r.get::<_, u32>(0),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row(
            "SELECT rootpage FROM sqlite_schema WHERE type='table' AND name='event_records'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap(),
        record_rootpage
    );
    assert_eq!(
        conn.query_row("SELECT count(*) FROM event_records", [], |r| r
            .get::<_, u32>(0))
            .unwrap(),
        1
    );
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn injected_migration_failure_preserves_valid_format_one_then_retries() {
    let (path, dir) = temp_db("format-one-injected-rollback");
    let record_rootpage = create_format_one_fixture(&path, true);
    let mut injected = SqliteOptions::new(&path);
    injected.failure_injection = Some(SqliteFailureInjection::BeforeMigrationCommit);
    assert!(matches!(
        SqliteStore::open(injected).await,
        Err(Error::StoreWriteFailed(_))
    ));
    let conn = Connection::open(&path).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT format_version FROM event_stream_metadata WHERE singleton=1",
            [],
            |r| r.get::<_, u32>(0),
        )
        .unwrap(),
        1
    );
    assert_eq!(
        conn.query_row(
            "SELECT rootpage FROM sqlite_schema WHERE type='table' AND name='event_records'",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap(),
        record_rootpage
    );
    drop(conn);
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("migrated").unwrap())
        .await
        .unwrap();
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 1);
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn malformed_or_foreign_formats_fail_without_partial_initialization() {
    let (path, dir) = temp_db("foreign-format");
    Connection::open(&path)
        .unwrap()
        .execute("CREATE TABLE application_data(value TEXT)", [])
        .unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    let tables: u32 = Connection::open(&path)
        .unwrap()
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'event_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(tables, 0);
    std::fs::remove_dir_all(dir).unwrap();

    let (path, dir) = temp_db("missing-format-table");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute("DROP TABLE event_records", [])
        .unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_stream_metadata_lengths_fail_before_blob_allocation() {
    let (path, dir) = temp_db("metadata-bounds");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("bounded").unwrap())
        .await
        .unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    conn.execute(
        "UPDATE event_streams SET tail=zeroblob(2000000) WHERE public_id='bounded'",
        [],
    )
    .unwrap();
    drop(conn);
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        store.bounds(&stream).await,
        Err(Error::StoreCorrupt(_))
    ));
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_record_identifiers_are_bounded_by_utf8_bytes_before_allocation() {
    let cases = [
        ("unicode-event", "event_id", "😀".repeat(256)),
        (
            "nul-event",
            "event_id",
            format!("x\0{}", "suffix".repeat(128)),
        ),
        ("unicode-schema", "schema_id", "😀".repeat(256)),
    ];
    for (label, column, malformed) in cases {
        let (path, dir) = temp_db(label);
        let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        let stream = store
            .create_if_absent(&StreamId::new("bounded-record").unwrap())
            .await
            .unwrap();
        store
            .append_atomic(&stream, event("valid", b"payload"))
            .await
            .unwrap();
        store.close().await.unwrap();

        let sql = format!("UPDATE event_records SET {column}=?1");
        Connection::open(&path)
            .unwrap()
            .execute(&sql, params![malformed])
            .unwrap();
        let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        assert!(matches!(
            reopened
                .read_range(
                    &stream,
                    0,
                    1,
                    PageLimits {
                        max_records: 1,
                        max_bytes: 4096,
                    },
                )
                .await,
            Err(Error::StoreCorrupt(_))
        ));
        reopened.close().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn corrupt_record_offset_is_rejected_before_blob_allocation() {
    let (path, dir) = temp_db("record-offset-bound");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("bounded-offset").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("valid", b"payload"))
        .await
        .unwrap();
    store.close().await.unwrap();

    let conn = Connection::open(&path).unwrap();
    conn.pragma_update(None, "foreign_keys", false).unwrap();
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    conn.execute("UPDATE event_records SET offset=zeroblob(2000000)", [])
        .unwrap();
    drop(conn);
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        reopened
            .lookup_event(&stream, &EventId::new("valid").unwrap())
            .await,
        Err(Error::StoreCorrupt(_))
    ));
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_record_payload_is_bounded_before_result_materialization() {
    let (path, dir) = temp_db("record-payload-bound");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("bounded-payload").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("valid", b"payload"))
        .await
        .unwrap();
    store.close().await.unwrap();

    Connection::open(&path)
        .unwrap()
        .execute("UPDATE event_records SET payload=zeroblob(2000000)", [])
        .unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        reopened
            .read_range(
                &stream,
                0,
                1,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await,
        Err(Error::StoreCorrupt(_))
    ));
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn corrupt_scalar_types_are_rejected_before_result_materialization() {
    let (path, dir) = temp_db("record-schema-version-type");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("bounded-version").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("valid", b"payload"))
        .await
        .unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE event_records SET schema_version=zeroblob(2000000)",
            [],
        )
        .unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        reopened
            .lookup_event(&stream, &EventId::new("valid").unwrap())
            .await,
        Err(Error::StoreCorrupt(_))
    ));
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();

    let (path, dir) = temp_db("format-version-type");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE event_stream_metadata SET format_version=zeroblob(2000000)",
            [],
        )
        .unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::StoreCorrupt(_))
    ));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn replay_rejects_internal_holes_and_missing_tail_rows() {
    for (label, removed_offset) in [("internal-hole", 2_u64), ("missing-tail", 3_u64)] {
        let (path, dir) = temp_db(label);
        let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        let stream = store
            .create_if_absent(&StreamId::new("corrupt-history").unwrap())
            .await
            .unwrap();
        for index in 1..=3 {
            store
                .append_atomic(&stream, event(&format!("e-{index}"), b"value"))
                .await
                .unwrap();
        }
        store.close().await.unwrap();
        Connection::open(&path)
            .unwrap()
            .execute(
                "DELETE FROM event_records WHERE offset=?1",
                params![removed_offset.to_be_bytes().as_slice()],
            )
            .unwrap();
        let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
        let result = reopened
            .read_range(
                &stream,
                0,
                3,
                PageLimits {
                    max_records: 8,
                    max_bytes: 4096,
                },
            )
            .await;
        assert!(matches!(result, Err(Error::StoreCorrupt(_))), "{result:?}");
        reopened.close().await.unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn metadata_rejects_floor_above_tail() {
    let (path, dir) = temp_db("floor-above-tail");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("bad-bounds").unwrap())
        .await
        .unwrap();
    store.close().await.unwrap();
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE event_streams SET floor=?1,tail=?2 WHERE public_id='bad-bounds'",
            params![
                2_u64.to_be_bytes().as_slice(),
                1_u64.to_be_bytes().as_slice()
            ],
        )
        .unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert!(matches!(
        reopened.bounds(&stream).await,
        Err(Error::StoreCorrupt(_))
    ));
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn ownership_rejects_hardlinks_symlinks_and_normalizes_parent_aliases() {
    use std::os::unix::fs::symlink;
    let (path, dir) = temp_db("aliases");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let dotted = dir.join(".").join("events.sqlite3");
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(dotted)).await,
        Err(Error::StoreInUse)
    ));
    store.close().await.unwrap();

    let hardlink = dir.join("hardlink.sqlite3");
    std::fs::hard_link(&path, &hardlink).unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&hardlink)).await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&path)).await,
        Err(Error::InvalidConfig(_))
    ));
    std::fs::remove_file(&hardlink).unwrap();

    let symlink_path = dir.join("symlink.sqlite3");
    symlink(&path, &symlink_path).unwrap();
    assert!(matches!(
        SqliteStore::open(SqliteOptions::new(&symlink_path)).await,
        Err(Error::InvalidConfig(_))
    ));
    std::fs::remove_file(symlink_path).unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn ordered_replay_and_retry_lookup_use_declared_indexes() {
    let (path, dir) = temp_db("plans");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    store.close().await.unwrap();
    let conn = Connection::open(&path).unwrap();
    let replay: String = conn.query_row(
        "EXPLAIN QUERY PLAN SELECT octet_length(event_id),octet_length(schema_id),octet_length(payload),CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob' AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256 AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN event_id END,CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob' AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256 AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN schema_id END,CASE WHEN typeof(schema_version)='integer' THEN schema_version END,CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob' AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256 AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN payload END FROM event_records WHERE stream_key=?1 AND offset>?2 AND offset<=?3 ORDER BY offset LIMIT ?4",
        params![1_i64, 0_u64.to_be_bytes().as_slice(), 10_u64.to_be_bytes().as_slice(), 10_i64, 4096_i64],
        |row| row.get(3),
    ).unwrap();
    assert!(
        replay.contains("INDEX") && !replay.contains("SCAN"),
        "{replay}"
    );
    let retry: String = conn.query_row(
        "EXPLAIN QUERY PLAN SELECT offset FROM event_records WHERE stream_key=?1 AND event_id=?2",
        params![1_i64, "event"], |row| row.get(3),
    ).unwrap();
    assert!(
        retry.contains("INDEX") && !retry.contains("SCAN"),
        "{retry}"
    );
    drop(conn);
    std::fs::remove_dir_all(dir).unwrap();
}
