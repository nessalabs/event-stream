// Shared deterministic fixture for native/headless verification and SQLite process-kill tests.
use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::ingestion::*;
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

pub async fn prepare_input(
    database: &std::path::Path,
    stage: &str,
) -> (Runtime<SqliteStore>, SourceBinding) {
    let runtime =
        Runtime::<SqliteStore>::open(SqliteOptions::new(database), RuntimeConfig::default())
            .await
            .unwrap();
    let output = runtime
        .create_stream(&StreamId::new("origin").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("raw-input").unwrap(),
            incarnation: SourceIncarnation([72; 16]),
        },
        parser: decoder().parser(),
        output_stream: output.clone(),
    };
    runtime
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("capture-start").unwrap(),
            binding: binding.clone(),
        })
        .await
        .unwrap();
    runtime
        .capture_segment(RawSegment {
            start: SourcePosition {
                source: binding.source.clone(),
                offset: 0,
            },
            bytes: Payload::copy_from_slice(b"a\nb\n"),
        })
        .await
        .unwrap();
    if stage == "capture" {
        return (runtime, binding);
    }
    // Stop at the real commit gap: first output exists, but no parser checkpoint does.
    runtime
        .append_captured(
            &output,
            JournaledOutput {
                source: binding.source.clone(),
                position: DecodedPosition {
                    source_byte: 0,
                    item_index: 0,
                },
                event: NewEvent {
                    payload: Payload::copy_from_slice(b"a"),
                    ..event("decoded-0").event
                },
            },
        )
        .await
        .unwrap();
    assert!(runtime
        .latest_checkpoint(&binding.source)
        .await
        .unwrap()
        .is_none());
    if stage == "checkpoint" {
        finish_parsing(&runtime, &binding).await;
    }
    (runtime, binding)
}
pub fn decoder() -> NewlineFramer {
    NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 1024,
        emit_empty_frames: false,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .unwrap()
}
pub async fn finish_parsing(runtime: &Runtime<SqliteStore>, binding: &SourceBinding) {
    runtime
        .seal_source(SealSource {
            end: SourcePosition {
                source: binding.source.clone(),
                offset: 4,
            },
        })
        .await
        .unwrap();
    let service =
        JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
    let progress = service
        .finish_captured(
            binding,
            decoder(),
            |frame: ByteFrame, position: DecodedPosition| {
                let mut output = event(&format!("decoded-{}", position.item_index)).event;
                output.payload = Payload::copy_from_slice(frame.as_bytes());
                Ok(output)
            },
        )
        .await
        .unwrap();
    assert!(progress.recovery.complete_capture);
    assert!(progress.parser_finished);
    let checkpoint = runtime
        .latest_checkpoint(&binding.source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.source.offset, 4);
    assert_eq!(checkpoint.next_item_index, 2);
    assert_eq!(checkpoint.committed_output.unwrap().offset, 2);
    drop(service);
}

#[derive(Debug)]
pub struct RecoveryEvidence {
    pub snapshot_bytes: Vec<u8>,
    pub suffix_offset: u64,
    pub removed_records: usize,
    pub caught_up_offset: u64,
    pub backlog_records: usize,
    pub finalization: SourceFinalizationStatus,
}

pub async fn verify_recovered_history(directory: &std::path::Path) -> RecoveryEvidence {
    let origin_database = directory.join("origin.db");
    let origin = open_origin(&origin_database).await;
    let database = directory.join("destination.db");
    let destination = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let key = origin
        .create_stream(&StreamId::new("origin").unwrap())
        .await
        .unwrap();
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
        .verify_and_publish_snapshot(
            snapshot.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
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
    let destination = std::sync::Arc::new(destination);
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
    let first = origin
        .bootstrap_replica_once(
            destination.clone(),
            begin.clone(),
            publish.clone(),
            ack.clone(),
            limits.clone(),
        )
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
        .advance_capture_receipt_floor(AdvanceCaptureReceiptFloor {
            operation_id: JournalOperationId::new("source-consumed").unwrap(),
            source: SourceKey {
                id: SourceId::new("raw-input").unwrap(),
                incarnation: SourceIncarnation([72; 16]),
            },
            expected_floor: 0,
            new_floor: 4,
        })
        .await
        .unwrap();
    for _ in 0..8 {
        let progress = origin
            .cleanup_captured(SourceJournalStoreConfig::default().cleanup)
            .await
            .unwrap();
        if !progress.remaining {
            break;
        }
    }
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
    let retry = origin
        .bootstrap_replica_once(
            destination.clone(),
            begin.clone(),
            publish.clone(),
            ack.clone(),
            limits.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first, retry);
    let status_after_retry = origin.replica_status(&replica, &stream).await.unwrap();
    assert_eq!(status_after_retry, status_before_retry);
    assert_eq!(status_after_retry.backlog_records, 1);
    assert_eq!(status_after_retry.mode, ReplicaMode::Required);
    close_origin(&origin).await;
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
    let origin = open_origin(&origin_database).await;
    let finalization = origin
        .source_finalization(&SourceKey {
            id: SourceId::new("raw-input").unwrap(),
            incarnation: SourceIncarnation([72; 16]),
        })
        .await
        .unwrap();
    assert_eq!(
        finalization,
        SourceFinalizationStatus {
            sealed_end: Some(4),
            parser_finished: true
        }
    );
    let reopened = std::sync::Arc::new(reopened);
    let replayed = origin
        .bootstrap_replica_once(reopened.clone(), begin, publish, ack, limits)
        .await
        .unwrap();
    assert_eq!(replayed, first);
    assert_eq!(
        origin.replica_status(&replica, &stream).await.unwrap(),
        status_before_retry
    );
    origin
        .replicate_once(
            reopened.clone(),
            PrepareReplicaBatch {
                operation_id: ReplicationOperationId::new("catch-up-after-restart").unwrap(),
                batch_id: BatchId([3; 16]),
                replica: replica.clone(),
                stream: stream.clone(),
                expected_after: ReplicaPosition {
                    stream: stream.clone(),
                    offset: 2,
                },
                limits: ReplicaBatchLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            },
            ReplicationOperationId::new("ack-after-restart").unwrap(),
        )
        .await
        .unwrap();
    let caught_up = origin.replica_status(&replica, &stream).await.unwrap();
    assert_eq!(caught_up.acknowledged.offset, 3);
    assert_eq!(caught_up.backlog_records, 0);
    let final_page = reopened
        .read_replica_after(
            &ReplicaPosition {
                stream: stream.clone(),
                offset: 2,
            },
            ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(final_page.records.len(), 1);
    assert_eq!(
        final_page.records[0].event.payload.as_bytes(),
        b"c-after-completion"
    );
    close_origin(&origin).await;
    drop(origin);
    EventStore::close(reopened.as_ref()).await.unwrap();
    drop(reopened);
    RecoveryEvidence {
        finalization,
        snapshot_bytes: page.bytes.as_bytes().to_vec(),
        suffix_offset: suffix.records[0].cursor.offset,
        removed_records: cleaned.removed_event_rows,
        caught_up_offset: caught_up.acknowledged.offset,
        backlog_records: caught_up.backlog_records,
    }
}

async fn open_origin(path: &std::path::Path) -> Runtime<SqliteStore> {
    Runtime::<SqliteStore>::open(
        SqliteOptions::new(path),
        RuntimeConfig {
            replication: RuntimeReplicationConfig {
                max_concurrent: 1,
                max_in_flight_bytes: 1024 * 1024,
            },
            ..RuntimeConfig::default()
        },
    )
    .await
    .unwrap()
}

async fn close_origin(origin: &Runtime<SqliteStore>) {
    let report = origin.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(report.closed);
    assert!(report.unresolved.is_empty());
}
