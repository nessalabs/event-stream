// Shared bounded scenario. Only public production APIs implement stream operations.
use event_stream::{
    infrastructure::{SqliteOptions, SqliteStore},
    ingestion::*,
    *,
};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::task::JoinSet;

#[derive(Clone, Copy)]
pub struct MixedConfig {
    pub mixed: bool,
    pub offers: usize,
    pub interval_us: u64,
    pub snapshot_bytes: usize,
}

#[derive(Debug)]
pub struct MixedEvidence {
    pub offered: usize,
    pub accepted: usize,
    pub runtime_rejected: usize,
    pub generator_rejected: usize,
    pub peak_tasks: usize,
    pub receipt_ns: Vec<u64>,
    pub lateness_ns: Vec<u64>,
    pub overlap_receipts: usize,
    pub maintenance_ns: u64,
    pub replay_pages: usize,
    pub removed_records: usize,
    pub replica_tail: u64,
    pub peak_runtime_queue: usize,
}

struct Call {
    index: usize,
    cursor: Option<Cursor>,
    receipt_ns: u64,
    lateness_ns: u64,
    completed_ns: u64,
}

const SOURCE_RECORDS: u64 = 32;
const MAX_TASKS: usize = 64;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        events: EventConfig {
            max_bytes: 4096,
            minimum_persistence: PersistenceProfile::ProcessRestart,
        },
        replication: RuntimeReplicationConfig {
            max_concurrent: 2,
            max_in_flight_bytes: 1024 * 1024,
        },
        ..RuntimeConfig::default()
    }
}

fn foreground_event(index: usize) -> NewEvent {
    let mut bytes = [3; 128];
    bytes[..8].copy_from_slice(&(index as u64).to_be_bytes());
    NewEvent {
        id: EventId::new(format!("offer-{index}")).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("foreground").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(&bytes),
    }
}

fn page() -> PageLimits {
    PageLimits {
        max_records: 8,
        max_bytes: 16 * 1024,
    }
}
fn ns(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap()
}

async fn close(runtime: &Runtime<SqliteStore>) {
    let report = runtime.shutdown(Duration::from_secs(10)).await.unwrap();
    assert!(report.closed && report.unresolved.is_empty());
}

/// Lose exactly one reply after the real destination transaction has committed.
struct LostReply {
    destination: Arc<SqliteStore>,
    lose: AtomicBool,
}

#[async_trait::async_trait]
impl ReplicaTransport for LostReply {
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        let receipt = self.destination.commit_replica_batch(batch).await?;
        if self.lose.swap(false, Ordering::SeqCst) {
            return Err(ReplicationError::StorageFailure(
                "injected committed reply loss".into(),
            ));
        }
        Ok(receipt)
    }
}

pub async fn run(directory: &Path, workload: MixedConfig) -> MixedEvidence {
    assert!((64..=4096).contains(&workload.offers));
    assert!((250..=10_000).contains(&workload.interval_us));
    assert!([64 * 1024, 1024 * 1024].contains(&workload.snapshot_bytes));
    std::fs::create_dir(directory).unwrap();
    let origin_path = directory.join("origin.db");
    let destination_path = directory.join("destination.db");
    let runtime = Runtime::<SqliteStore>::open(SqliteOptions::new(&origin_path), config())
        .await
        .unwrap();
    let destination = Arc::new(
        SqliteStore::open(SqliteOptions::new(&destination_path))
            .await
            .unwrap(),
    );
    let foreground = runtime
        .create_stream(&StreamId::new("foreground").unwrap())
        .await
        .unwrap();
    let source_stream = runtime
        .create_stream(&StreamId::new("source").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: source_stream.clone(),
        })
        .await
        .unwrap();
    let decoder = NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 64,
        emit_empty_frames: false,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .unwrap();
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("captured").unwrap(),
            incarnation: SourceIncarnation([31; 16]),
        },
        parser: decoder.parser(),
        output_stream: source_stream.clone(),
    };
    runtime
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("source-begin").unwrap(),
            binding: binding.clone(),
        })
        .await
        .unwrap();
    let raw = (0..SOURCE_RECORDS)
        .map(|i| format!("{i}\n"))
        .collect::<String>();
    let segment = RawSegment {
        start: SourcePosition {
            source: binding.source.clone(),
            offset: 0,
        },
        bytes: Payload::copy_from_slice(raw.as_bytes()),
    };
    let first_capture = runtime.capture_segment(segment.clone()).await.unwrap();
    assert_eq!(
        runtime.capture_segment(segment).await.unwrap(),
        first_capture
    );

    let started = Instant::now();
    let producer = tokio::spawn({
        let runtime = runtime.clone();
        let stream = foreground.clone();
        async move {
            let mut tasks = JoinSet::new();
            let mut calls = Vec::with_capacity(workload.offers);
            let mut rejected = 0;
            let mut peak = 0;
            for index in 0..workload.offers {
                let scheduled =
                    started + Duration::from_micros(workload.interval_us * index as u64);
                tokio::time::sleep_until(scheduled.into()).await;
                while let Some(call) = tasks.try_join_next() {
                    calls.push(call.unwrap());
                }
                if tasks.len() == MAX_TASKS {
                    rejected += 1;
                    continue;
                }
                let runtime = runtime.clone();
                let stream = stream.clone();
                tasks.spawn(async move {
                    let lateness_ns = ns(Instant::now().saturating_duration_since(scheduled));
                    let call = Instant::now();
                    let cursor = match runtime.try_append(&stream, foreground_event(index)).await {
                        Ok(receipt) => {
                            assert_eq!(receipt.kind, AppendKind::Inserted);
                            Some(receipt.record.cursor.clone())
                        }
                        Err(Error::Overloaded | Error::AdmissionTimeout) => None,
                        Err(error) => panic!("unexpected foreground failure: {error}"),
                    };
                    Call {
                        index,
                        cursor,
                        receipt_ns: ns(call.elapsed()),
                        lateness_ns,
                        completed_ns: ns(started.elapsed()),
                    }
                });
                peak = peak.max(tasks.len());
            }
            while let Some(call) = tasks.join_next().await {
                calls.push(call.unwrap());
            }
            (calls, rejected, peak)
        }
    });
    let replay = tokio::spawn({
        let runtime = runtime.clone();
        let stream = foreground.clone();
        async move {
            let mut pages = 0;
            for _ in 0..64 {
                let bounds = runtime.bounds(&stream).await.unwrap();
                let result = runtime
                    .read_after(&Cursor::new(stream.clone(), 0), page(), Some(&bounds.tail))
                    .await
                    .unwrap();
                for (index, record) in result.records.iter().enumerate() {
                    assert_eq!(record.cursor.offset, index as u64 + 1);
                    let id = u64::from_be_bytes(
                        record.event.payload.as_bytes()[..8].try_into().unwrap(),
                    ) as usize;
                    assert_eq!(record.event, foreground_event(id));
                }
                pages += 1;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            pages
        }
    });

    let maintenance_start = ns(started.elapsed());
    let mut removed_records = 0;
    let mut replica_tail = 0;
    let mut replicated_stream = None;
    if workload.mixed {
        runtime
            .seal_source(SealSource {
                end: SourcePosition {
                    source: binding.source.clone(),
                    offset: raw.len() as u64,
                },
            })
            .await
            .unwrap();
        let service =
            JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
        let progress = service
            .finish_captured(
                &binding,
                decoder,
                |frame: ByteFrame, position: DecodedPosition| {
                    Ok(NewEvent {
                        id: EventId::new(format!("decoded-{}", position.item_index)).unwrap(),
                        schema: SchemaRef {
                            id: SchemaId::new("frame").unwrap(),
                            version: 1,
                        },
                        payload: Payload::copy_from_slice(frame.as_bytes()),
                    })
                },
            )
            .await
            .unwrap();
        assert!(progress.parser_finished && progress.recovery.complete_capture);
        assert_eq!(
            runtime.bounds(&source_stream).await.unwrap().tail.offset,
            SOURCE_RECORDS
        );
        drop(service);

        let mut state = vec![7; workload.snapshot_bytes];
        state[..8].copy_from_slice(&16u64.to_be_bytes());
        let snapshot = SnapshotDescriptor {
            id: SnapshotId([42; 16]),
            covered: Cursor::new(source_stream.clone(), 16),
            schema: SchemaRef {
                id: SchemaId::new("state-count").unwrap(),
                version: 1,
            },
            content_bytes: state.len() as u64,
            digest: SnapshotDigest(Sha256::digest(&state).into()),
        };
        runtime.begin_snapshot(snapshot.clone()).await.unwrap();
        for (index, bytes) in state.chunks(64 * 1024).enumerate() {
            runtime
                .put_snapshot_chunk(
                    snapshot.id,
                    SnapshotChunk {
                        offset: (index * 64 * 1024) as u64,
                        bytes: Payload::copy_from_slice(bytes),
                    },
                )
                .await
                .unwrap();
        }
        runtime
            .verify_and_publish_snapshot(
                snapshot.id,
                VerificationLimits {
                    max_chunks: 1,
                    max_bytes: 64 * 1024,
                },
            )
            .await
            .unwrap();
        let stream = OriginStream {
            origin: runtime.origin_identity().await.unwrap(),
            stream: source_stream.clone(),
        };
        let replica = ReplicaId::new("replica").unwrap();
        let epoch = destination.destination_epoch().await.unwrap();
        runtime
            .attach_replica(AttachReplica {
                operation_id: ReplicationOperationId::new("attach").unwrap(),
                replica: replica.clone(),
                stream: stream.clone(),
                destination_epoch: epoch,
                max_backlog_bytes: 1024 * 1024,
                max_backlog_age: Duration::from_secs(60),
                start: ReplicaStart::NeedsBootstrap,
            })
            .await
            .unwrap();
        let begin = BeginOriginBootstrap {
            operation_id: ReplicationOperationId::new("begin").unwrap(),
            bootstrap_id: BootstrapId([43; 16]),
            destination_operation_id: ReplicationOperationId::new("destination-begin").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            snapshot,
            captured_tail: ReplicaPosition {
                stream: stream.clone(),
                offset: SOURCE_RECORDS,
            },
        };
        let publish = ReplicationOperationId::new("publish").unwrap();
        let ack = ReplicationOperationId::new("ack").unwrap();
        let limits = ReplicationBootstrapDriveLimits {
            recovery_lifetime: Duration::from_secs(30),
            snapshot_page_bytes: 4096,
            suffix_page: page(),
            verification: ReplicaBootstrapVerificationLimits {
                max_chunks: 1,
                max_records: 8,
                max_bytes: 64 * 1024,
            },
        };
        let first = runtime
            .bootstrap_replica_once(
                destination.clone(),
                begin.clone(),
                publish.clone(),
                ack.clone(),
                limits.clone(),
            )
            .await
            .unwrap();
        runtime
            .append_generated(
                &source_stream,
                GeneratedEvent {
                    generation: RetryGeneration::FIRST,
                    event: NewEvent {
                        id: EventId::new("late").unwrap(),
                        schema: SchemaRef {
                            id: SchemaId::new("frame").unwrap(),
                            version: 1,
                        },
                        payload: Payload::copy_from_slice(b"late"),
                    },
                },
            )
            .await
            .unwrap();
        runtime
            .advance_retention_floor(AdvanceRetentionFloor {
                operation_id: RetentionOperationId::new("floor").unwrap(),
                stream: source_stream.clone(),
                expected_floor: Cursor::new(source_stream.clone(), 0),
                new_floor: Cursor::new(source_stream.clone(), SOURCE_RECORDS),
            })
            .await
            .unwrap();
        runtime
            .advance_retry_generation(AdvanceRetryGeneration {
                operation_id: RetentionOperationId::new("generation").unwrap(),
                stream: source_stream.clone(),
                expected_current: RetryGeneration::FIRST,
            })
            .await
            .unwrap();
        runtime
            .advance_capture_receipt_floor(AdvanceCaptureReceiptFloor {
                operation_id: JournalOperationId::new("captured-floor").unwrap(),
                source: binding.source.clone(),
                expected_floor: 0,
                new_floor: raw.len() as u64,
            })
            .await
            .unwrap();
        let mut cleaned = false;
        for _ in 0..64 {
            if !runtime
                .cleanup_captured(SourceJournalStoreConfig::default().cleanup)
                .await
                .unwrap()
                .remaining
            {
                cleaned = true;
                break;
            }
        }
        assert!(cleaned);
        runtime
            .expire_retry_generations(ExpireRetryGenerations {
                operation_id: RetentionOperationId::new("expire").unwrap(),
                stream: source_stream.clone(),
                expected_oldest: RetryGeneration::LEGACY,
                retain_from: RetryGeneration::new(2),
            })
            .await
            .unwrap();
        let mut cleaned = false;
        for _ in 0..64 {
            let progress = runtime
                .cleanup_retention(RetentionCleanupLimits {
                    max_event_rows: 8,
                    max_retry_rows: 8,
                    // The adapter requires room for one maximum-size stored record.
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap();
            removed_records += progress.removed_event_rows;
            if !progress.remaining {
                cleaned = true;
                break;
            }
        }
        assert!(cleaned);
        assert_eq!(removed_records, SOURCE_RECORDS as usize);
        assert_eq!(
            runtime
                .bootstrap_replica_once(destination.clone(), begin, publish, ack, limits)
                .await
                .unwrap(),
            first
        );
        let transport = Arc::new(LostReply {
            destination: destination.clone(),
            lose: AtomicBool::new(true),
        });
        let request = PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new("catch-up").unwrap(),
            batch_id: BatchId([44; 16]),
            replica: replica.clone(),
            stream: stream.clone(),
            expected_after: ReplicaPosition {
                stream: stream.clone(),
                offset: SOURCE_RECORDS,
            },
            limits: ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        };
        let ack = ReplicationOperationId::new("catch-up-ack").unwrap();
        assert!(
            matches!(runtime.replicate_once(transport.clone(), request.clone(), ack.clone()).await, Err(ReplicationError::StorageFailure(message)) if message == "injected committed reply loss")
        );
        runtime
            .replicate_once(transport, request, ack)
            .await
            .unwrap();
        let status = runtime.replica_status(&replica, &stream).await.unwrap();
        assert_eq!(status.backlog_records, 0);
        replica_tail = status.acknowledged.offset;
        assert_eq!(replica_tail, SOURCE_RECORDS + 1);
        replicated_stream = Some(stream);
    }
    let maintenance_end = ns(started.elapsed());
    let (mut calls, generator_rejected, peak_tasks) = producer.await.unwrap();
    let replay_pages = replay.await.unwrap();
    let accepted = calls.iter().filter(|call| call.cursor.is_some()).count();
    let runtime_rejected = calls.len() - accepted;
    assert_eq!(calls.len() + generator_rejected, workload.offers);
    let overlap_receipts = if workload.mixed {
        calls
            .iter()
            .filter(|c| {
                c.cursor.is_some()
                    && c.completed_ns >= maintenance_start
                    && c.completed_ns <= maintenance_end
            })
            .count()
    } else {
        0
    };
    let peak_runtime_queue = runtime.diagnostics().await.peak_queued_appends;
    assert!(peak_runtime_queue <= config().appends.max_queued);
    close(&runtime).await;
    drop(runtime);
    EventStore::close(destination.as_ref()).await.unwrap();
    drop(destination);

    let runtime = Runtime::<SqliteStore>::open(SqliteOptions::new(&origin_path), config())
        .await
        .unwrap();
    calls.sort_by_key(|call| {
        call.cursor
            .as_ref()
            .map(|cursor| cursor.offset)
            .unwrap_or(u64::MAX)
    });
    let bounds = runtime.bounds(&foreground).await.unwrap();
    assert_eq!(bounds.tail.offset as usize, accepted);
    let mut after = Cursor::new(foreground, 0);
    while after.offset < bounds.tail.offset {
        let result = runtime
            .read_after(&after, page(), Some(&bounds.tail))
            .await
            .unwrap();
        assert!(!result.records.is_empty());
        for record in result.records {
            let call = &calls[after.offset as usize];
            assert_eq!(call.cursor.as_ref().unwrap(), &record.cursor);
            assert_eq!(record.event, foreground_event(call.index));
            after = record.cursor.clone();
        }
    }
    if let Some(stream) = replicated_stream {
        assert!(
            runtime
                .source_finalization(&binding.source)
                .await
                .unwrap()
                .parser_finished
        );
        let destination = SqliteStore::open(SqliteOptions::new(&destination_path))
            .await
            .unwrap();
        let lease = destination
            .acquire_replica_bootstrap_read(&stream, Duration::from_secs(30))
            .await
            .unwrap();
        let mut offset = 0;
        let mut digest = Sha256::new();
        while offset < workload.snapshot_bytes as u64 {
            let chunk = destination
                .read_replica_bootstrap_bytes(lease.lease, offset, 4096)
                .await
                .unwrap();
            assert!(!chunk.bytes.is_empty());
            digest.update(chunk.bytes.as_bytes());
            offset += chunk.bytes.len() as u64;
        }
        assert_eq!(offset, workload.snapshot_bytes as u64);
        let published = destination
            .published_replica_bootstrap(&stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            <[u8; 32]>::from(digest.finalize()),
            published.request.snapshot.digest.0
        );
        destination
            .release_replica_bootstrap_read(lease.lease)
            .await
            .unwrap();
        let mut after = 16;
        while after < replica_tail {
            let result = destination
                .read_replica_after(
                    &ReplicaPosition {
                        stream: stream.clone(),
                        offset: after,
                    },
                    ReplicaBatchLimits {
                        max_records: 8,
                        max_bytes: 16 * 1024,
                    },
                )
                .await
                .unwrap();
            assert!(!result.records.is_empty());
            for record in result.records {
                assert_eq!(record.cursor.offset, after + 1);
                if record.cursor.offset == SOURCE_RECORDS + 1 {
                    assert_eq!(record.event.payload.as_bytes(), b"late");
                } else {
                    assert_eq!(
                        record.event.payload.as_bytes(),
                        (record.cursor.offset - 1).to_string().as_bytes()
                    );
                }
                after += 1;
            }
        }
        EventStore::close(&destination).await.unwrap();
    }
    close(&runtime).await;
    MixedEvidence {
        offered: workload.offers,
        accepted,
        runtime_rejected,
        generator_rejected,
        peak_tasks,
        receipt_ns: calls
            .iter()
            .filter(|c| c.cursor.is_some())
            .map(|c| c.receipt_ns)
            .collect(),
        lateness_ns: calls.iter().map(|c| c.lateness_ns).collect(),
        overlap_receipts,
        maintenance_ns: if workload.mixed {
            maintenance_end - maintenance_start
        } else {
            0
        },
        replay_pages,
        removed_records,
        replica_tail,
        peak_runtime_queue,
    }
}
