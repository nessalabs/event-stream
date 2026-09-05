use crate::runner::{CheckResult, Observation, RunContext};
use async_trait::async_trait;
use event_stream::{
    infrastructure::{
        MemoryStore, MemoryStoreOptions, SqliteFailureInjection, SqliteOptions,
        SqliteRestoreBackend, SqliteRestoreManager, SqliteStore,
    },
    ingestion::{
        CheckpointDecoder, CrLfPolicy, DecodeBudget, DecodeState, FinalLinePolicy,
        IncrementalDecoder, JournalDriveConfig, JournalIngestionService, NewlineFramer,
        NewlineFramerConfig,
    },
    *,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Notify;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .expect("verification runtime")
}

fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).expect("bounded fixture ID"),
        schema: SchemaRef {
            id: SchemaId::new("verification.bytes.v1").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(payload),
    }
}

fn observation(
    label: &str,
    expected: &str,
    actual: impl Into<String>,
    passed: bool,
) -> Observation {
    Observation {
        label: label.into(),
        expected: expected.into(),
        actual: actual.into(),
        passed,
    }
}

fn failed(error: impl ToString) -> CheckResult {
    let error = error.to_string();
    CheckResult {
        summary: "The production scenario returned an unexpected error.".into(),
        observations: vec![observation("Execution", "success", &error, false)],
        output: json!({"error": error}),
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct SnapshotScenarioState {
    transcript: Vec<String>,
    workflow_step: u8,
}

struct SnapshotScenarioResult {
    decoded_snapshot: SnapshotScenarioState,
    recovered: SnapshotScenarioState,
    replayed: SnapshotScenarioState,
    snapshot_bytes: usize,
    suffix_offsets: Vec<u64>,
}

impl SnapshotScenarioState {
    fn apply(&mut self, record: &Record) -> std::result::Result<(), String> {
        let transition = std::str::from_utf8(record.event.payload.as_bytes())
            .map_err(|error| error.to_string())?;
        let expected = ["opened", "approved", "completed"]
            .get(usize::from(self.workflow_step))
            .ok_or_else(|| "workflow already complete".to_owned())?;
        if transition != *expected {
            return Err(format!("expected {expected}, received {transition}"));
        }
        self.transcript.push(transition.to_owned());
        self.workflow_step += 1;
        Ok(())
    }

    fn decode(schema: &SchemaRef, bytes: &[u8]) -> std::result::Result<Self, String> {
        if schema.id.as_str() != "example.application-state.v1" || schema.version != 1 {
            return Err("unsupported application snapshot schema".into());
        }
        let text = std::str::from_utf8(bytes).map_err(|error| error.to_string())?;
        let mut state = Self::default();
        for (index, transition) in text.lines().enumerate() {
            state.apply(&Record {
                cursor: Cursor::new(
                    StreamKey {
                        id: StreamId::new("snapshot-verification").unwrap(),
                        incarnation: IncarnationId([0; 16]),
                    },
                    index as u64 + 1,
                ),
                event: event(&format!("decoded-{index}"), transition.as_bytes()),
            })?;
        }
        Ok(state)
    }
}

fn require_input(cx: &RunContext, expected: serde_json::Value) -> Option<CheckResult> {
    (cx.input != expected).then(|| failed("scenario input does not match its fixed fixture"))
}

pub fn record_order(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"stream": "task-42", "producers": 8, "events_per_producer": 16}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let runtime = match Runtime::<MemoryStore>::open(
            MemoryStoreOptions::default(),
            RuntimeConfig::default(),
        )
        .await
        {
            Ok(value) => value,
            Err(error) => return failed(error),
        };
        let key = runtime
            .create_stream(&StreamId::new("task-42").unwrap())
            .await
            .unwrap();
        let mut tasks = Vec::new();
        for producer in 0..8 {
            for index in 0..16 {
                let runtime = runtime.clone();
                let key = key.clone();
                tasks.push(tokio::spawn(async move {
                    runtime
                        .append(
                            &key,
                            event(&format!("p{producer}-{index}"), &[producer, index]),
                        )
                        .await
                }));
            }
        }
        let mut receipts = Vec::new();
        for task in tasks {
            match task.await.unwrap() {
                Ok(receipt) => receipts.push(receipt),
                Err(error) => return failed(error),
            }
        }
        receipts.sort_by_key(|receipt| receipt.record.cursor.offset);
        let offsets: Vec<_> = receipts
            .iter()
            .map(|receipt| receipt.record.cursor.offset)
            .collect();
        let contiguous = offsets.iter().copied().eq(1..=128);
        let distinct = receipts
            .iter()
            .map(|receipt| receipt.record.event.id.as_str())
            .collect::<std::collections::HashSet<_>>()
            .len()
            == 128;
        let diagnostics = runtime.diagnostics().await;
        let record_sample: Vec<_> = receipts
            .iter()
            .take(4)
            .chain(receipts.iter().rev().take(4).rev())
            .map(|receipt| {
                json!({
                    "cursor": receipt.record.cursor.offset,
                    "event_id": receipt.record.event.id.as_str(),
                })
            })
            .collect();
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        CheckResult {
            summary: "Concurrent producers committed through the production runtime.".into(),
            observations: vec![
                observation("Committed offsets", "1 through 128 without gaps", format!("{} records; tail {}", offsets.len(), offsets.last().unwrap_or(&0)), contiguous),
                observation("Event identities", "128 distinct IDs", format!("{} distinct IDs", if distinct { 128 } else { 0 }), distinct),
                observation("Owned admission", "128 accepted and inserted", format!("accepted={}, inserted={}, failed={}", diagnostics.append_accepted, diagnostics.append_inserted, diagnostics.append_failed), diagnostics.append_accepted == 128 && diagnostics.append_inserted == 128 && diagnostics.append_failed == 0),
            ],
            output: json!({"offset_range": {"first": offsets.first(), "last": offsets.last(), "count": offsets.len()}, "record_sample_first_and_last": record_sample, "peak_queued_appends": diagnostics.peak_queued_appends, "peak_queued_bytes": diagnostics.peak_queued_append_bytes}),
        }
    })
}

pub fn retry_identity(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"event_id": "output-7", "payloads": ["hello", "hello", "goodbye"]}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let store = MemoryStore::open(MemoryStoreOptions::default()).await.unwrap();
        let key = store
            .create_if_absent(&StreamId::new("retry").unwrap())
            .await
            .unwrap();
        let first = store.append_atomic(&key, event("output-7", b"hello")).await.unwrap();
        let retry = store.append_atomic(&key, event("output-7", b"hello")).await.unwrap();
        let conflict = store.append_atomic(&key, event("output-7", b"goodbye")).await;
        let tail = store.bounds(&key).await.unwrap().tail.offset;
        let _ = store.close().await;
        let conflict_ok = matches!(conflict, Err(Error::IdempotencyConflict { .. }));
        CheckResult {
            summary: "The store deduplicated the exact retry and rejected changed bytes.".into(),
            observations: vec![
                observation("Receipt kinds", "inserted, deduplicated", format!("{:?}, {:?}", first.kind, retry.kind), first.kind == AppendKind::Inserted && retry.kind == AppendKind::Deduplicated),
                observation("Stable cursor", "both receipts at offset 1", format!("{}, {}", first.record.cursor.offset, retry.record.cursor.offset), first.record.cursor == retry.record.cursor),
                observation("Changed retry", "idempotency conflict and tail 1", format!("conflict={conflict_ok}, tail={tail}"), conflict_ok && tail == 1),
            ],
            output: json!({"first_offset": first.record.cursor.offset, "retry_offset": retry.record.cursor.offset, "tail": tail, "conflict": format!("{:?}", conflict.err())}),
        }
    })
}

pub fn reset_incarnation(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"stream": "task-42", "old_records": 3, "operation_id": "reset-task-42"}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let runtime = Runtime::<MemoryStore>::open(
            MemoryStoreOptions::default(),
            RuntimeConfig::default(),
        )
        .await
        .unwrap();
        let old = runtime
            .create_stream(&StreamId::new("task-42").unwrap())
            .await
            .unwrap();
        for index in 1..=3 {
            runtime
                .append(&old, event(&format!("old-{index}"), &[index]))
                .await
                .unwrap();
        }
        let old_cursor = runtime.bounds(&old).await.unwrap().tail;
        let receipt = runtime
            .change_lifecycle(LifecycleRequest {
                operation_id: LifecycleOperationId::new("reset-task-42").unwrap(),
                expected: old.clone(),
                action: LifecycleAction::Reset,
            })
            .await
            .unwrap();
        let new = receipt.replacement.unwrap();
        let new_bounds = runtime.bounds(&new).await.unwrap();
        let stale_bounds = runtime.bounds(&old).await;
        let stale_cursor = runtime
            .read_after(
                &old_cursor,
                PageLimits {
                    max_records: 4,
                    max_bytes: 1024 * 1024,
                },
                None,
            )
            .await;
        let old_append = runtime.append(&old, event("late-old", b"old")).await;
        let new_append = runtime.append(&new, event("new-1", b"new")).await.unwrap();
        let identity_changed = old.id == new.id && old.incarnation != new.incarnation;
        let stale_rejected = matches!(stale_bounds, Err(Error::StaleIncarnation { .. }))
            && matches!(stale_cursor, Err(Error::StaleIncarnation { .. }))
            && matches!(old_append, Err(Error::StaleIncarnation { .. }));
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        CheckResult {
            summary: "A production runtime reset published an empty lifetime and rejected every old identity.".into(),
            observations: vec![
                observation("Replacement identity", "same name and a different incarnation", format!("same_name={}, incarnation_changed={}", old.id == new.id, old.incarnation != new.incarnation), identity_changed),
                observation("Replacement history", "tail 0 before its first append, then offset 1", format!("initial_tail={}, first_offset={}", new_bounds.tail.offset, new_append.record.cursor.offset), new_bounds.tail.offset == 0 && new_append.record.cursor.offset == 1),
                observation("Old handle and cursor", "all rejected as stale incarnation", format!("bounds={:?}, read={:?}, append={:?}", stale_bounds.err(), stale_cursor.err(), old_append.err()), stale_rejected),
            ],
            output: json!({"old": {"stream": old.id.as_str(), "tail": old_cursor.offset, "incarnation": old.incarnation.0}, "replacement": {"tail_before_append": new_bounds.tail.offset, "first_offset": new_append.record.cursor.offset, "incarnation": new.incarnation.0}, "operation_id": "reset-task-42"}),
        }
    })
}

pub fn bounded_lifecycle_cleanup(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"stream": "cleanup-task", "retired_records": 5, "records_per_turn": 2}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let mut config = RuntimeConfig::default();
        config.maintenance.cleanup.max_records = 2;
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
            .await
            .unwrap();
        let old = runtime
            .create_stream(&StreamId::new("cleanup-task").unwrap())
            .await
            .unwrap();
        for index in 1..=5 {
            runtime
                .append(&old, event(&format!("retired-{index}"), &[index]))
                .await
                .unwrap();
        }
        let new = runtime
            .change_lifecycle(LifecycleRequest {
                operation_id: LifecycleOperationId::new("cleanup-reset").unwrap(),
                expected: old,
                action: LifecycleAction::Reset,
            })
            .await
            .unwrap()
            .replacement
            .unwrap();
        let first = runtime.cleanup_retired().await.unwrap();
        let active = runtime
            .append(&new, event("active-during-cleanup", b"active"))
            .await
            .unwrap();
        let second = runtime.cleanup_retired().await.unwrap();
        let third = runtime.cleanup_retired().await.unwrap();
        let done = runtime.cleanup_retired().await.unwrap();
        let counts = vec![
            first.removed_records,
            second.removed_records,
            third.removed_records,
        ];
        let bounded = counts == vec![2, 2, 1]
            && first.remaining
            && second.remaining
            && !third.remaining
            && done.stream.is_none();
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        CheckResult {
            summary: "Explicit maintenance calls removed one bounded retired prefix at a time while the replacement remained active.".into(),
            observations: vec![
                observation("Cleanup turns", "record counts 2, 2, 1 and then no work", format!("counts={counts:?}, final_stream={:?}", done.stream), bounded),
                observation("Active replacement", "append remains available at offset 1", format!("offset={}", active.record.cursor.offset), active.record.cursor.offset == 1),
                observation("Cleanup byte accounting", "every nonempty turn reports positive bounded logical bytes", format!("bytes=[{}, {}, {}]", first.removed_bytes, second.removed_bytes, third.removed_bytes), first.removed_bytes > 0 && second.removed_bytes > 0 && third.removed_bytes > 0 && [first.removed_bytes, second.removed_bytes, third.removed_bytes].into_iter().all(|bytes| bytes <= 2 * 1024 * 1024)),
            ],
            output: json!({"removed_records_per_turn": counts, "removed_bytes_per_turn": [first.removed_bytes, second.removed_bytes, third.removed_bytes], "remaining_after_each": [first.remaining, second.remaining, third.remaining], "replacement_append_offset": active.record.cursor.offset}),
        }
    })
}

pub fn retention_floor(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"legacy_offset": 1, "generation_one_offset": 2, "floor": 2, "suffix_offset": 3}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let mut config = RuntimeConfig::default();
        config.events.max_bytes = 1024;
        let runtime = match Runtime::<MemoryStore>::open(
            MemoryStoreOptions {
                max_record_bytes: 1024,
                ..MemoryStoreOptions::default()
            },
            config,
        )
        .await
        {
            Ok(runtime) => runtime,
            Err(error) => return failed(error),
        };
        let stream = runtime
            .create_stream(&StreamId::new("retention-verification").unwrap())
            .await
            .unwrap();
        let shared_id = EventId::new("reused-identity").unwrap();
        let legacy = runtime
            .append(&stream, event(shared_id.as_str(), b"legacy"))
            .await
            .unwrap();
        runtime
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new("enable-retention-verification").unwrap(),
                stream: stream.clone(),
            })
            .await
            .unwrap();
        let first = runtime
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::FIRST,
                    event: event(shared_id.as_str(), b"generation-one"),
                },
            )
            .await
            .unwrap();
        runtime
            .advance_retention_floor(AdvanceRetentionFloor {
                operation_id: RetentionOperationId::new("floor-retention-verification").unwrap(),
                stream: stream.clone(),
                expected_floor: Cursor::new(stream.clone(), 0),
                new_floor: Cursor::new(stream.clone(), 2),
            })
            .await
            .unwrap();
        let retained_retry = runtime
            .lookup_generated(&stream, RetryGeneration::FIRST, &shared_id)
            .await
            .unwrap();
        runtime
            .advance_retry_generation(AdvanceRetryGeneration {
                operation_id: RetentionOperationId::new("advance-retention-verification").unwrap(),
                stream: stream.clone(),
                expected_current: RetryGeneration::FIRST,
            })
            .await
            .unwrap();
        runtime
            .expire_retry_generations(ExpireRetryGenerations {
                operation_id: RetentionOperationId::new("expire-retention-verification").unwrap(),
                stream: stream.clone(),
                expected_oldest: RetryGeneration::LEGACY,
                retain_from: RetryGeneration::new(2),
            })
            .await
            .unwrap();
        let expired_retry = runtime
            .lookup_generated(&stream, RetryGeneration::FIRST, &shared_id)
            .await;
        let suffix = runtime
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::new(2),
                    event: event("suffix", b"generation-two"),
                },
            )
            .await
            .unwrap();
        let mut cleanup_turns = 0usize;
        let mut removed_rows = 0usize;
        let mut cleanup_rows = Vec::new();
        let mut cleanup_bytes = Vec::new();
        let cleanup_remaining = loop {
            let progress = runtime
                .cleanup_retention(RetentionCleanupLimits {
                    max_event_rows: 1,
                    max_retry_rows: 1,
                    max_bytes: 1024 * 1024,
                })
                .await
                .unwrap();
            cleanup_turns += 1;
            let turn_rows = progress.removed_event_rows + progress.removed_retry_rows;
            removed_rows += turn_rows;
            cleanup_rows.push(turn_rows);
            cleanup_bytes.push(progress.removed_bytes);
            if !progress.remaining || cleanup_turns == 8 {
                break progress.remaining;
            }
        };
        let page = runtime
            .read_after(
                &Cursor::new(stream.clone(), 2),
                PageLimits {
                    max_records: 4,
                    max_bytes: 1024 * 1024,
                },
                None,
            )
            .await
            .unwrap();
        let retry_survived_floor = retained_retry
            .as_ref()
            .is_some_and(|record| record.cursor.offset == 2);
        let retry_expired = matches!(
            expired_retry,
            Err(RetentionError::RetryGenerationExpired { .. })
        );
        let exact_suffix = page.records.len() == 1
            && page.records[0].cursor == suffix.record.cursor
            && page.records[0].event.payload.as_bytes() == b"generation-two"
            && page.next_after.offset == 3
            && page.complete;
        let bounded_cleanup = !cleanup_remaining
            && cleanup_turns <= 8
            && removed_rows == 4
            && cleanup_rows.iter().all(|rows| *rows <= 2)
            && cleanup_bytes.iter().all(|bytes| *bytes <= 1024 * 1024);
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        CheckResult {
            summary: "The production Memory runtime kept retry identity and replay history as separate, explicit horizons.".into(),
            observations: vec![
                observation("Generated identity reuse", "legacy offset 1 and generation-one offset 2", format!("legacy={}, generation_one={}", legacy.record.cursor.offset, first.record.cursor.offset), legacy.record.cursor.offset == 1 && first.record.cursor.offset == 2),
                observation("Retry horizon", "generation-one retry survives floor movement, then expires explicitly", format!("survived_floor={retry_survived_floor}, expired={retry_expired}"), retry_survived_floor && retry_expired),
                observation("Bounded cleanup", "finite turns complete with at most one event row, one retry row, and 1 MiB per turn", format!("turns={cleanup_turns}, rows={cleanup_rows:?}, bytes={cleanup_bytes:?}, remaining={cleanup_remaining}"), bounded_cleanup),
                observation("Replay suffix", "exactly offset 3 after floor 2", format!("offsets={:?}, complete={}", page.records.iter().map(|record| record.cursor.offset).collect::<Vec<_>>(), page.complete), exact_suffix),
            ],
            output: json!({"legacy_offset": legacy.record.cursor.offset, "generation_one_offset": first.record.cursor.offset, "retry_survived_floor": retry_survived_floor, "retry_expired": retry_expired, "cleanup_turns": cleanup_turns, "removed_rows_per_turn": cleanup_rows, "removed_bytes_per_turn": cleanup_bytes, "cleanup_remaining": cleanup_remaining, "suffix_offsets": page.records.iter().map(|record| record.cursor.offset).collect::<Vec<_>>() }),
        }
    })
}

pub fn offline_replica(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(cx, json!({"offline_events": 3, "batch_records": 2})) {
        return result;
    }
    runtime().block_on(async {
        let origin = Arc::new(MemoryStore::open(MemoryStoreOptions::default()).await.unwrap());
        let destination = Arc::new(MemoryStore::open(MemoryStoreOptions::default()).await.unwrap());
        let key = origin
            .create_if_absent(&StreamId::new("offline-replica-verification").unwrap())
            .await
            .unwrap();
        let stream = OriginStream {
            origin: origin.origin_identity().await.unwrap(),
            stream: key.clone(),
        };
        let replica = ReplicaId::new("verification-destination").unwrap();
        let epoch = destination.destination_epoch().await.unwrap();
        origin
            .attach_replica(AttachReplica {
                operation_id: ReplicationOperationId::new("verification-attach").unwrap(),
                replica: replica.clone(),
                stream: stream.clone(),
                destination_epoch: epoch,
                max_backlog_bytes: 64 * 1024,
                max_backlog_age: Duration::from_secs(60),
                start: ReplicaStart::FromBeginning,
            })
            .await
            .unwrap();
        let mut local = Vec::new();
        for (id, bytes) in [("a", b"one".as_slice()), ("b", b"two"), ("c", b"three")] {
            local.push(origin.append_atomic(&key, event(id, bytes)).await.unwrap());
        }
        let offline = origin.replica_status(&replica, &stream).await.unwrap();
        let driver = ReplicationDriver::open(
            origin.clone(),
            destination.clone(),
            ReplicationDriverConfig {
                max_concurrent: 1,
                max_in_flight_bytes: 64 * 1024,
            },
        )
        .unwrap();
        let mut after = 0u64;
        for turn in 0..2u8 {
            let result = driver
                .replicate_once(
                    PrepareReplicaBatch {
                        operation_id: ReplicationOperationId::new(format!("prepare-{turn}"))
                            .unwrap(),
                        batch_id: BatchId([turn + 1; 16]),
                        replica: replica.clone(),
                        stream: stream.clone(),
                        expected_after: ReplicaPosition {
                            stream: stream.clone(),
                            offset: after,
                        },
                        limits: ReplicaBatchLimits {
                            max_records: 2,
                            max_bytes: 16 * 1024,
                        },
                    },
                    ReplicationOperationId::new(format!("ack-{turn}")).unwrap(),
                )
                .await
                .unwrap();
            after = result
                .acknowledged
                .as_ref()
                .map_or(after, |receipt| receipt.status.acknowledged.offset);
        }
        let caught_up = origin.replica_status(&replica, &stream).await.unwrap();
        let page = destination
            .read_replica_after(
                &ReplicaPosition {
                    stream: stream.clone(),
                    offset: 0,
                },
                ReplicaBatchLimits {
                    max_records: 3,
                    max_bytes: 32 * 1024,
                },
            )
            .await
            .unwrap();
        driver.wait_closed().await;
        let offsets: Vec<_> = page.records.iter().map(|record| record.cursor.offset).collect();
        let payloads: Vec<_> = page
            .records
            .iter()
            .map(|record| String::from_utf8_lossy(record.event.payload.as_bytes()).into_owned())
            .collect();
        let ordered = offsets == vec![1, 2, 3]
            && payloads == ["one", "two", "three"]
            && page.complete;
        CheckResult {
            summary: "Three local writes accumulated while transport work was absent, then two bounded production-driver turns caught up the destination in exact order.".into(),
            observations: vec![
                observation("Offline local commits", "three local receipts before remote work", format!("local_offsets={:?}, backlog_records={}", local.iter().map(|receipt| receipt.record.cursor.offset).collect::<Vec<_>>(), offline.backlog_records), local.len() == 3 && offline.backlog_records == 3 && offline.acknowledged.offset == 0),
                observation("Bounded catch-up", "two turns with at most two records each", format!("acknowledged={}, backlog_records={}", caught_up.acknowledged.offset, caught_up.backlog_records), caught_up.acknowledged.offset == 3 && caught_up.backlog_records == 0),
                observation("Destination order", "offsets 1,2,3 with exact payloads", format!("offsets={offsets:?}, payloads={payloads:?}, complete={}", page.complete), ordered),
            ],
            output: json!({"offline_backlog_records": offline.backlog_records, "batch_records": 2, "destination_offsets": offsets, "destination_payloads": payloads, "final_backlog_records": caught_up.backlog_records}),
        }
    })
}

pub fn replica_retry(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(cx, json!({"fault": "after_remote_commit_before_receipt"}))
    {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("replica-retry");
        let origin = Arc::new(MemoryStore::open(MemoryStoreOptions::default()).await.unwrap());
        let mut options = SqliteOptions::new(&db.path);
        options.failure_injection = Some(SqliteFailureInjection::AfterReplicaCommitAcknowledgementLost);
        let destination = Arc::new(SqliteStore::open(options).await.unwrap());
        let key = origin.create_if_absent(&StreamId::new("receipt-loss").unwrap()).await.unwrap();
        origin.append_atomic(&key, event("stable-output", b"one durable record")).await.unwrap();
        let stream = OriginStream { origin: origin.origin_identity().await.unwrap(), stream: key };
        let replica = ReplicaId::new("sqlite-destination").unwrap();
        origin.attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(), stream: stream.clone(),
            destination_epoch: destination.destination_epoch().await.unwrap(),
            max_backlog_bytes: 64 * 1024, max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        }).await.unwrap();
        let request = PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new("prepare").unwrap(), batch_id: BatchId([41; 16]),
            replica: replica.clone(), stream: stream.clone(),
            expected_after: ReplicaPosition { stream: stream.clone(), offset: 0 },
            limits: ReplicaBatchLimits { max_records: 1, max_bytes: 4096 },
        };
        let config = ReplicationDriverConfig { max_concurrent: 1, max_in_flight_bytes: 64 * 1024 };
        let driver = ReplicationDriver::open(origin.clone(), destination.clone(), config.clone()).unwrap();
        let ack = ReplicationOperationId::new("ack").unwrap();
        let first = driver.replicate_once(request.clone(), ack.clone()).await;
        let unknown = matches!(first, Err(ReplicationError::CommitUnknown(_)));
        let before = origin.replica_status(&replica, &stream).await.unwrap();
        driver.close();
        driver.wait_closed().await;
        drop(driver);
        destination.close().await.unwrap();
        drop(destination);
        let destination = Arc::new(SqliteStore::open(SqliteOptions::new(&db.path)).await.unwrap());
        let driver = ReplicationDriver::open(origin.clone(), destination.clone(), config).unwrap();
        let retry = driver.replicate_once(request, ack).await.unwrap();
        let after = origin.replica_status(&replica, &stream).await.unwrap();
        let page = destination.read_replica_after(
            &ReplicaPosition { stream: stream.clone(), offset: 0 },
            ReplicaBatchLimits { max_records: 2, max_bytes: 4096 },
        ).await.unwrap();
        let exact = page.records.len() == 1 && page.records[0].cursor.offset == 1
            && page.records[0].event.payload.as_bytes() == b"one durable record" && page.complete;
        driver.close();
        driver.wait_closed().await;
        drop(driver);
        destination.close().await.unwrap();
        CheckResult {
            summary: "SQLite committed the record but lost its reply. After reopening the database, the same batch returned its receipt and advanced the origin once.".into(),
            observations: vec![
                observation("Reply loss", "commit unknown; origin remains at 0 with one pending record", format!("unknown={unknown}, acknowledged={}, backlog={}", before.acknowledged.offset, before.backlog_records), unknown && before.acknowledged.offset == 0 && before.backlog_records == 1),
                observation("Retry after reopen", "receipt advances origin to 1 and clears backlog", format!("acknowledged={}, backlog={}", after.acknowledged.offset, after.backlog_records), retry.acknowledged.is_some() && after.acknowledged.offset == 1 && after.backlog_records == 0),
                observation("Stored output", "one exact record at cursor 1", format!("records={}, complete={}", page.records.len(), page.complete), exact),
            ],
            output: json!({"fault": "after_remote_commit_before_receipt", "destination": "SQLite", "reopened": true,
                "origin_before_retry": {"after": before.acknowledged.offset, "backlog": before.backlog_records},
                "origin_after_retry": {"after": after.acknowledged.offset, "backlog": after.backlog_records},
                "destination_records": page.records.iter().map(|r| json!({"cursor": r.cursor.offset, "payload": String::from_utf8_lossy(r.event.payload.as_bytes())})).collect::<Vec<_>>() }),
        }
    })
}

pub fn restore_boundary(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"backup_records": 3, "mapping_page_records": 1, "retry_same_operation": true}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("restore");
        let source = db.dir.join("backup.sqlite3");
        let source_store = match SqliteStore::open(SqliteOptions::new(&source)).await {
            Ok(store) => store,
            Err(error) => return failed(error),
        };
        let old = source_store
            .create_if_absent(&StreamId::new("restored-task").unwrap())
            .await
            .unwrap();
        for index in 1..=3 {
            source_store
                .append_atomic(
                    &old,
                    event(&format!("backup-{index}"), &[index, index + 10]),
                )
                .await
                .unwrap();
        }
        source_store.close().await.unwrap();

        let manager = SqliteRestoreManager::new(
            SqliteRestoreBackend::new(&db.dir).unwrap(),
            RestoreConfig::default(),
        )
        .unwrap();
        let backup_identity = manager.inspect_backup(source.clone()).await.unwrap();
        let request = RestoreRequest {
            operation_id: RestoreOperationId::new("verification-restore").unwrap(),
            backup_identity,
            source,
            destination: PathBuf::from("published.sqlite3"),
        };
        let receipt = manager.restore(request.clone()).await.unwrap();
        let retry = manager.restore(request).await.unwrap();
        let mapping = manager
            .read_mapping(
                receipt.clone(),
                None,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        let replacement = mapping.entries[0].new.clone();
        let mapping_valid = mapping.entries.len() == 1
            && mapping.complete
            && mapping.next_after.as_ref() == Some(&old)
            && mapping.entries[0].old == old
            && replacement.id == old.id
            && replacement.incarnation != old.incarnation;

        let restored = SqliteStore::open(SqliteOptions::new(&receipt.destination))
            .await
            .unwrap();
        let stale = restored.bounds(&old).await;
        let page = restored
            .read_range(
                &replacement,
                0,
                3,
                PageLimits {
                    max_records: 3,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        let exact_history = page.records.len() == 3
            && page.complete
            && page.next_after.offset == 3
            && page.through.offset == 3
            && page.records.iter().enumerate().all(|(index, record)| {
                let value = index as u8 + 1;
                record.cursor.offset == index as u64 + 1
                    && record.event.schema.id.as_str() == "verification.bytes.v1"
                    && record.event.schema.version == 1
                    && record.event.id.as_str() == format!("backup-{}", index + 1)
                    && record.event.payload.as_bytes() == [value, value + 10]
            });
        let next = restored
            .append_atomic(&replacement, event("after-restore", b"new"))
            .await
            .unwrap();
        restored.close().await.unwrap();
        let reopened = SqliteStore::open(SqliteOptions::new(&receipt.destination))
            .await
            .unwrap();
        let reopened_tail = reopened.bounds(&replacement).await.unwrap().tail.offset;
        reopened.close().await.unwrap();

        let retry_stable = retry == receipt;
        let stale_rejected = matches!(stale, Err(Error::StaleIncarnation { .. }));
        CheckResult {
            summary: "The production SQLite restore published a new stream identity, preserved exact history, and deduplicated the operation retry.".into(),
            observations: vec![
                observation("Restore retry", "the same durable receipt", format!("stable={retry_stable}, mappings={}", receipt.mapping_count), retry_stable && receipt.mapping_count == 1),
                observation("Identity mapping", "same name, fresh incarnation, one complete mapping page", format!("complete={}, entries={}", mapping.complete, mapping.entries.len()), mapping_valid),
                observation("Published history", "three records at offsets 1-3 with exact schema and payload bytes", format!("records={}, next={}, through={}, exact={exact_history}", page.records.len(), page.next_after.offset, page.through.offset), exact_history),
                observation("Old identity", "stale incarnation", format!("{:?}", stale.err()), stale_rejected),
                observation("Post-restore durability", "next append at 4 and reopened tail 4", format!("append={}, reopened_tail={reopened_tail}", next.record.cursor.offset), next.record.cursor.offset == 4 && reopened_tail == 4),
            ],
            output: json!({
                "backup_identity": receipt.backup_identity.to_hex(),
                "published_path": receipt.destination,
                "mapping_count": receipt.mapping_count,
                "old_incarnation": format!("{:02x?}", old.incarnation.0),
                "new_incarnation": format!("{:02x?}", replacement.incarnation.0),
                "restored_offsets": page.records.iter().map(|record| record.cursor.offset).collect::<Vec<_>>(),
                "post_restore_offset": next.record.cursor.offset,
                "reopened_tail": reopened_tail,
            }),
        }
    })
}

pub fn snapshot_replay(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"covered_cursor": 2, "tail": 3, "schema": "example.application-state.v1"}),
    ) {
        return result;
    }
    runtime().block_on(async {
        match execute_snapshot_replay().await {
            Ok(result) => {
                let equal = result.recovered == result.replayed;
                CheckResult {
                    summary: "The production Memory runtime restored application-owned state and applied only the protected suffix.".into(),
                    observations: vec![
                        observation("Snapshot content", "application state through cursor 2", format!("{} bytes; transcript={:?}", result.snapshot_bytes, result.decoded_snapshot.transcript), result.snapshot_bytes > 0 && result.decoded_snapshot.transcript == ["opened", "approved"] && result.decoded_snapshot.workflow_step == 2),
                        observation("Protected suffix", "exactly offset 3", format!("offsets={:?}", result.suffix_offsets), result.suffix_offsets == [3]),
                        observation("Recovery result", "snapshot plus suffix equals full replay", format!("equal={equal}, transcript={:?}, workflow_step={}", result.recovered.transcript, result.recovered.workflow_step), equal && result.recovered.workflow_step == 3),
                    ],
                    output: json!({
                        "covered_cursor": 2,
                        "captured_tail": 3,
                        "snapshot_bytes": result.snapshot_bytes,
                        "decoded_snapshot": {"transcript": result.decoded_snapshot.transcript, "workflow_step": result.decoded_snapshot.workflow_step},
                        "suffix_offsets": result.suffix_offsets,
                        "recovered": {"transcript": result.recovered.transcript, "workflow_step": result.recovered.workflow_step},
                        "matches_complete_replay": equal,
                    }),
                }
            }
            Err(error) => failed(error),
        }
    })
}

async fn execute_snapshot_replay() -> std::result::Result<SnapshotScenarioResult, String> {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .map_err(|error| error.to_string())?;
    let stream = runtime
        .create_stream(&StreamId::new("snapshot-verification").unwrap())
        .await
        .map_err(|error| error.to_string())?;
    for (index, transition) in ["opened", "approved"].into_iter().enumerate() {
        runtime
            .append(
                &stream,
                event(
                    &format!("snapshot-event-{}", index + 1),
                    transition.as_bytes(),
                ),
            )
            .await
            .map_err(|error| error.to_string())?;
    }
    let content = b"opened\napproved\n";
    let id = SnapshotId::from_bytes([17; 16]);
    let descriptor = SnapshotDescriptor {
        id,
        covered: Cursor::new(stream.clone(), 2),
        schema: SchemaRef {
            id: SchemaId::new("example.application-state.v1").unwrap(),
            version: 1,
        },
        content_bytes: content.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(content).into()),
    };
    runtime
        .begin_snapshot(descriptor.clone())
        .await
        .map_err(|error| error.to_string())?;
    let mut offset = 0u64;
    for bytes in content.chunks(5) {
        runtime
            .put_snapshot_chunk(
                id,
                SnapshotChunk {
                    offset,
                    bytes: Payload::copy_from_slice(bytes),
                },
            )
            .await
            .map_err(|error| error.to_string())?;
        offset += bytes.len() as u64;
    }
    let published = runtime
        .verify_and_publish_snapshot(
            id,
            VerificationLimits {
                max_chunks: 2,
                max_bytes: 4,
            },
        )
        .await
        .map_err(|error| error.to_string())?;
    if published != descriptor {
        return Err("published descriptor changed".into());
    }
    runtime
        .append(&stream, event("snapshot-event-3", b"completed"))
        .await
        .map_err(|error| error.to_string())?;

    let plan = runtime
        .acquire_recovery(id, Duration::from_secs(30))
        .await
        .map_err(|error| error.to_string())?;
    if plan.snapshot != descriptor || plan.through.offset != 3 {
        return Err("recovery plan did not preserve snapshot identity and tail".into());
    }
    let mut restored_bytes = Vec::new();
    let mut byte_offset = 0u64;
    loop {
        let page = runtime
            .read_snapshot_chunk(plan.lease, byte_offset, 4)
            .await
            .map_err(|error| error.to_string())?;
        restored_bytes.extend_from_slice(page.bytes.as_bytes());
        byte_offset = page.next_offset;
        if page.complete {
            break;
        }
    }
    let decoded_snapshot = SnapshotScenarioState::decode(&plan.snapshot.schema, &restored_bytes)?;
    let mut recovered = decoded_snapshot.clone();
    let page_limits = PageLimits {
        max_records: 2,
        max_bytes: 1024 * 1024,
    };
    let mut after = descriptor.covered.offset;
    let mut suffix_offsets = Vec::new();
    loop {
        let page = runtime
            .read_recovery_page(plan.lease, after, page_limits)
            .await
            .map_err(|error| error.to_string())?;
        for record in &page.records {
            recovered.apply(record)?;
            suffix_offsets.push(record.cursor.offset);
        }
        after = page.next_after.offset;
        if page.complete {
            break;
        }
    }
    runtime
        .release_recovery(plan.lease)
        .await
        .map_err(|error| error.to_string())?;

    let mut replayed = SnapshotScenarioState::default();
    let mut cursor = Cursor::new(stream, 0);
    loop {
        let page = runtime
            .read_after(&cursor, page_limits, None)
            .await
            .map_err(|error| error.to_string())?;
        for record in &page.records {
            replayed.apply(record)?;
        }
        cursor = page.next_after;
        if page.complete {
            break;
        }
    }
    runtime
        .shutdown(Duration::from_secs(1))
        .await
        .map_err(|error| error.to_string())?;
    Ok(SnapshotScenarioResult {
        decoded_snapshot,
        recovered,
        replayed,
        snapshot_bytes: restored_bytes.len(),
        suffix_offsets,
    })
}

#[derive(Clone)]
struct GateOptions {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    block: Arc<AtomicBool>,
}
struct GateStore {
    memory: MemoryStore,
    options: GateOptions,
}
#[async_trait]
impl EventStore for GateStore {
    type Options = GateOptions;
    async fn open(options: Self::Options) -> Result<Self> {
        Ok(Self {
            memory: MemoryStore::open(MemoryStoreOptions::default()).await?,
            options,
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.memory.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.memory.create_if_absent(id).await
    }
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        if self.options.block.swap(false, Ordering::SeqCst) {
            self.options.entered.notify_one();
            self.options.release.notified().await;
        }
        self.memory.append_atomic(stream, event).await
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.memory.lookup_event(stream, id).await
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.memory.bounds(stream).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.memory.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> Result<()> {
        self.memory.close().await
    }
}

pub fn bounded_ingestion(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"queue_records": 1, "queue_bytes": 32768, "payload_bytes": 1024, "offered_records": 128}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let options = GateOptions { entered: Arc::new(Notify::new()), release: Arc::new(Notify::new()), block: Arc::new(AtomicBool::new(true)) };
        let config = { let mut config = RuntimeConfig::default(); config.events.max_bytes = 2048; config.appends.max_queued = 1; config.appends.max_queued_per_stream = 1; config.appends.max_queued_bytes = 32768; config.appends.max_queued_bytes_per_stream = 32768; config.appends.max_waiter_bytes = 32768; config };
        let runtime = Runtime::<GateStore>::open(options.clone(), config).await.unwrap();
        let key = runtime.create_stream(&StreamId::new("bounded").unwrap()).await.unwrap();
        let first = { let runtime = runtime.clone(); let key = key.clone(); tokio::spawn(async move { runtime.append(&key, event("event-0", &vec![1; 1024])).await }) };
        options.entered.notified().await;
        let mut rejected = 0;
        for index in 1..128 {
            if matches!(runtime.try_append(&key, event(&format!("event-{index}"), &vec![1; 1024])).await, Err(Error::Overloaded)) { rejected += 1; }
        }
        options.release.notify_one();
        let inserted = first.await.unwrap().unwrap();
        let diagnostics = runtime.diagnostics().await;
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        let exact = diagnostics.append_accepted == 1 && diagnostics.append_rejected == 127 && rejected == 127;
        CheckResult {
            summary: "Finite admission kept one owned append and rejected excess offers explicitly.".into(),
            observations: vec![
                observation("Admission", "1 accepted, 127 overloaded", format!("accepted={}, rejected={}", diagnostics.append_accepted, diagnostics.append_rejected), exact),
                observation("Released reservations", "zero queued records, bytes, and waiters after completion", format!("queued={}, bytes={}, waiters={}", diagnostics.queued_appends, diagnostics.queued_append_bytes, diagnostics.admission_waiters), diagnostics.queued_appends == 0 && diagnostics.queued_append_bytes == 0 && diagnostics.admission_waiters == 0),
                observation("Queue peak", "one record within 32768 bytes", format!("records={}, bytes={}", diagnostics.peak_queued_appends, diagnostics.peak_queued_append_bytes), diagnostics.peak_queued_appends == 1 && diagnostics.peak_queued_append_bytes <= 32768),
            ],
            output: json!({"inserted_offset": inserted.record.cursor.offset, "offered": 128, "runtime": {"accepted": diagnostics.append_accepted, "rejected": diagnostics.append_rejected, "peak_records": diagnostics.peak_queued_appends, "peak_bytes": diagnostics.peak_queued_append_bytes}}),
        }
    })
}

pub fn replay_fairness(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"history_records": 64, "page_records": 8, "foreground_writes": 4}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let runtime = Runtime::<MemoryStore>::open(
            MemoryStoreOptions::default(),
            { let mut config = RuntimeConfig::default(); config.events.max_bytes = 1024; config },
        )
        .await
        .unwrap();
        let key = runtime.create_stream(&StreamId::new("replay").unwrap()).await.unwrap();
        for index in 0..64 { runtime.append(&key, event(&format!("history-{index}"), b"history")).await.unwrap(); }
        let high = runtime.bounds(&key).await.unwrap().tail;
        let replay_runtime = runtime.clone();
        let replay_key = key.clone();
        let replay_started = Instant::now();
        let replay = tokio::spawn(async move {
            let mut after = Cursor::new(replay_key, 0);
            let mut count = 0;
            while after.offset < high.offset {
                let page = replay_runtime.read_after(&after, PageLimits { max_records: 8, max_bytes: 4096 }, Some(&high)).await.unwrap();
                count += page.records.len();
                after = page.next_after;
            }
            (count, replay_started.elapsed().as_micros())
        });
        let writes_started = Instant::now();
        let mut write_offsets = Vec::new();
        for index in 0..4 { write_offsets.push(runtime.append(&key, event(&format!("live-{index}"), b"live")).await.unwrap().record.cursor.offset); }
        let write_us = writes_started.elapsed().as_micros();
        let (replayed, replay_us) = replay.await.unwrap();
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        CheckResult {
            summary: "Bounded replay pages and foreground writes both completed through the runtime.".into(),
            observations: vec![
                observation("Replay progress", "64 records through fixed high-water mark", format!("{replayed} records in {replay_us} us"), replayed == 64),
                observation("Write progress", "four commits at offsets 65-68", format!("{:?} in {write_us} us", write_offsets), write_offsets == vec![65,66,67,68]),
            ],
            output: json!({"replay_records": replayed, "page_records": 8, "replay_elapsed_us": replay_us, "write_offsets": write_offsets, "write_elapsed_us": write_us, "cpu_ns": null, "peak_memory_bytes": null}),
        }
    })
}

struct TempDb {
    dir: PathBuf,
    path: PathBuf,
}
static NEXT_TEMP_DB: AtomicU64 = AtomicU64::new(0);

impl TempDb {
    fn new(name: &str) -> Self {
        for _ in 0..64 {
            let sequence = NEXT_TEMP_DB.fetch_add(1, Ordering::Relaxed);
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            let dir = std::env::temp_dir().join(format!(
                "event-stream-verification-{name}-{}-{timestamp}-{sequence}",
                std::process::id()
            ));
            match fs::create_dir(&dir) {
                Ok(()) => {
                    let path = dir.join("events.sqlite3");
                    return Self { dir, path };
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("create isolated scenario directory: {error}"),
            }
        }
        panic!("could not allocate an isolated scenario directory after 64 attempts")
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub fn sqlite_reopen(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"database": "isolated scenario directory", "events": 100, "reopen": true}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("reopen");
        let store = SqliteStore::open(SqliteOptions::new(&db.path)).await.unwrap();
        let key = store.create_if_absent(&StreamId::new("durable").unwrap()).await.unwrap();
        for index in 0..100 { store.append_atomic(&key, event(&format!("event-{index}"), format!("payload-{index}").as_bytes())).await.unwrap(); }
        store.close().await.unwrap();
        let reopened = SqliteStore::open(SqliteOptions::new(&db.path)).await.unwrap();
        let page = reopened.read_range(&key, 0, 100, PageLimits { max_records: 100, max_bytes: 1024 * 1024 }).await.unwrap();
        let retry = reopened.append_atomic(&key, event("event-0", b"payload-0")).await.unwrap();
        let tail = reopened.bounds(&key).await.unwrap().tail.offset;
        reopened.close().await.unwrap();
        let bytes_match = page.records.iter().enumerate().all(|(index, record)| record.event.payload.as_bytes() == format!("payload-{index}").as_bytes());
        CheckResult {
            summary: "A real isolated SQLite database preserved acknowledged records across reopen.".into(),
            observations: vec![
                observation("Recovered history", "100 records with original bytes and cursors", format!("records={}, tail={tail}", page.records.len()), page.records.len() == 100 && bytes_match && page.records.last().unwrap().cursor.offset == 100),
                observation("Retry after reopen", "deduplicated at original offset 1", format!("{:?} at {}", retry.kind, retry.record.cursor.offset), retry.kind == AppendKind::Deduplicated && retry.record.cursor.offset == 1 && tail == 100),
            ],
            output: json!({"database": "isolated temporary SQLite file", "records": page.records.len(), "first_cursor": page.records.first().map(|r| r.cursor.offset), "last_cursor": page.records.last().map(|r| r.cursor.offset), "retry_kind": format!("{:?}", retry.kind)}),
        }
    })
}

pub fn sqlite_ownership(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(cx, json!({"owners": 2, "same_store": true})) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("ownership");
        let first = SqliteStore::open(SqliteOptions::new(&db.path)).await.unwrap();
        let second = SqliteStore::open(SqliteOptions::new(&db.path)).await;
        let rejected = matches!(second, Err(Error::StoreInUse));
        first.close().await.unwrap();
        let reopened = SqliteStore::open(SqliteOptions::new(&db.path)).await;
        let released = reopened.is_ok();
        if let Ok(store) = reopened { let _ = store.close().await; }
        CheckResult {
            summary: "SQLite exclusive ownership rejected a second live owner and released on close.".into(),
            observations: vec![observation("Concurrent owner", "store_in_use, then reopen succeeds", format!("second_rejected={rejected}, reopened={released}"), rejected && released)],
            output: json!({"first_owner": "opened", "second_owner_error": format!("{:?}", second.err()), "reopened_after_close": released}),
        }
    })
}

pub fn sqlite_unknown(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"fault": "after_commit_before_receipt", "retry_same_id": true}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("unknown");
        let clean = SqliteStore::open(SqliteOptions::new(&db.path)).await.unwrap();
        let key = clean.create_if_absent(&StreamId::new("unknown").unwrap()).await.unwrap();
        clean.close().await.unwrap();
        let mut options = SqliteOptions::new(&db.path);
        options.failure_injection = Some(SqliteFailureInjection::AfterCommitAcknowledgementLost);
        let store = SqliteStore::open(options).await.unwrap();
        let recovered = store.append_atomic(&key, event("stable-id", b"committed")).await.unwrap();
        let retry = store.append_atomic(&key, event("stable-id", b"committed")).await.unwrap();
        let page = store.read_range(&key, 0, 1, PageLimits { max_records: 2, max_bytes: 4096 }).await.unwrap();
        store.close().await.unwrap();
        let exact = recovered.kind == AppendKind::Inserted && retry.kind == AppendKind::Deduplicated && page.records.len() == 1;
        CheckResult {
            summary: "The SQLite adapter resolved a lost acknowledgement by identity and kept one record.".into(),
            observations: vec![observation("Unknown commit recovery", "inserted then deduplicated, one stored record", format!("{:?}, {:?}, records={}", recovered.kind, retry.kind, page.records.len()), exact)],
            output: json!({"fault": "after_commit_before_receipt", "recovered_offset": recovered.record.cursor.offset, "retry_offset": retry.record.cursor.offset, "stored_records": page.records.len()}),
        }
    })
}

fn decode_partition(
    input: &[u8],
    boundaries: &[usize],
) -> std::result::Result<Vec<Vec<u8>>, String> {
    let mut decoder = NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 64,
        emit_empty_frames: true,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .map_err(|e| e.to_string())?;
    let budget = DecodeBudget {
        max_items: 8,
        max_bytes: 1024,
        max_work_units: 1024,
    };
    let mut output = Vec::new();
    let mut start = 0;
    for end in boundaries
        .iter()
        .copied()
        .chain(std::iter::once(input.len()))
    {
        let mut consumed = 0;
        while consumed < end - start {
            let step = decoder.decode(&input[start + consumed..end], budget);
            if step.consumed_bytes == 0 {
                return Err("decoder made no progress".into());
            }
            consumed += step.consumed_bytes;
            output.extend(
                step.items
                    .into_iter()
                    .map(|item| item.item.as_bytes().to_vec()),
            );
            if matches!(step.state, DecodeState::Failed(_)) {
                return Err("decoder failed".into());
            }
        }
        start = end;
    }
    let final_step = decoder.finish(budget);
    output.extend(
        final_step
            .items
            .into_iter()
            .map(|item| item.item.as_bytes().to_vec()),
    );
    if final_step.state != DecodeState::Finished {
        return Err(format!("unexpected EOF state: {:?}", final_step.state));
    }
    Ok(output)
}

pub fn decoder_splits(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"input": "{\"text\":\"hello\"}\n", "partitions": "unsplit, every single split, and one-byte chunks"}),
    ) {
        return result;
    }
    let input = b"{\"text\":\"hello\"}\n";
    let expected = vec![b"{\"text\":\"hello\"}".to_vec()];
    let mut cases = Vec::new();
    cases.push(Vec::new());
    for boundary in 1..input.len() {
        cases.push(vec![boundary]);
    }
    cases.push((1..input.len()).collect());
    let mut mismatch = None;
    for boundaries in &cases {
        match decode_partition(input, boundaries) {
            Ok(actual) if actual == expected => {}
            other => {
                mismatch = Some(format!("boundaries={boundaries:?}, result={other:?}"));
                break;
            }
        }
    }
    CheckResult {
        summary:
            "The production newline decoder returned identical bytes across every tested boundary."
                .into(),
        observations: vec![observation(
            "Chunk partitions",
            "unsplit, every single split, and one-byte chunks match",
            mismatch
                .clone()
                .unwrap_or_else(|| format!("{} partitions matched", cases.len())),
            mismatch.is_none(),
        )],
        output: json!({"input_hex": input.iter().map(|b| format!("{b:02x}")).collect::<String>(), "partition_cases": cases.len(), "decoded_utf8": String::from_utf8_lossy(&expected[0]), "mismatch": mismatch}),
    }
}

pub fn parser_checkpoint(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"fault": "after_output_commit_before_checkpoint", "rotate_generation_before_resume": true, "stable_output_ids": true}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let runtime = Runtime::<MemoryStore>::open(
            MemoryStoreOptions::default(),
            RuntimeConfig::default(),
        )
        .await
        .unwrap();
        let output = runtime
            .create_stream(&StreamId::new("parser-recovery-output").unwrap())
            .await
            .unwrap();
        runtime
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new("parser-recovery-enable").unwrap(),
                stream: output.clone(),
            })
            .await
            .unwrap();
        let decoder = NewlineFramer::new(NewlineFramerConfig {
            max_frame_bytes: 1024,
            emit_empty_frames: false,
            crlf: CrLfPolicy::StripCarriageReturn,
            final_line: FinalLinePolicy::RejectUnterminated,
        })
        .unwrap();
        let binding = SourceBinding {
            source: SourceKey {
                id: SourceId::new("parser-recovery-input").unwrap(),
                incarnation: SourceIncarnation([71; 16]),
            },
            parser: decoder.parser(),
            output_stream: output.clone(),
        };
        let service = JournalIngestionService::new(
            runtime.clone(),
            JournalDriveConfig {
                max_output_commits: 2,
                ..JournalDriveConfig::default()
            },
        )
        .unwrap();
        service
            .begin_source(BeginSource {
                operation_id: JournalOperationId::new("parser-recovery-begin").unwrap(),
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
                bytes: Payload::copy_from_slice(b"one\ntwo\n"),
            })
            .await
            .unwrap();
        runtime
            .append_captured(
                &output,
                JournaledOutput {
                    source: binding.source.clone(),
                    position: DecodedPosition {
                        source_byte: 0,
                        item_index: 0,
                    },
                    event: event("recovered-0", b"one"),
                },
            )
            .await
            .unwrap();
        let checkpoint_before = runtime.latest_checkpoint(&binding.source).await.unwrap();
        runtime
            .advance_retry_generation(AdvanceRetryGeneration {
                operation_id: RetentionOperationId::new("parser-recovery-generation-two").unwrap(),
                stream: output.clone(),
                expected_current: RetryGeneration::FIRST,
            })
            .await
            .unwrap();
        let progress = service
            .recover_captured(
                &binding,
                NewlineFramer::new(NewlineFramerConfig {
                    max_frame_bytes: 1024,
                    emit_empty_frames: false,
                    crlf: CrLfPolicy::StripCarriageReturn,
                    final_line: FinalLinePolicy::RejectUnterminated,
                })
                .unwrap(),
                |frame: event_stream::ingestion::ByteFrame,
                 position: DecodedPosition| {
                    Ok(event(
                        &format!("recovered-{}", position.item_index),
                        frame.as_bytes(),
                    ))
                },
            )
            .await
            .unwrap();
        let checkpoint = runtime
            .latest_checkpoint(&binding.source)
            .await
            .unwrap()
            .unwrap();
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
        let generation_one = runtime
            .lookup_generated(
                &output,
                RetryGeneration::FIRST,
                &EventId::new("recovered-0").unwrap(),
            )
            .await
            .unwrap()
            .is_some();
        let generation_two = runtime
            .lookup_generated(
                &output,
                RetryGeneration::new(2),
                &EventId::new("recovered-1").unwrap(),
            )
            .await
            .unwrap()
            .is_some();
        let exact_records = page.records.len() == 2
            && page.records[0].event.payload.as_bytes() == b"one"
            && page.records[1].event.payload.as_bytes() == b"two"
            && page.records[0].cursor.offset == 1
            && page.records[1].cursor.offset == 2;
        let exact_checkpoint = checkpoint.source.offset == 8
            && checkpoint.next_item_index == 2
            && checkpoint.committed_output.as_ref().map(|cursor| cursor.offset) == Some(2)
            && progress.complete_capture;
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
        CheckResult {
            summary: "Captured bytes recovered across an output-before-checkpoint interruption without a missing or duplicate record.".into(),
            observations: vec![
                observation("Interruption boundary", "first output committed with no checkpoint", format!("checkpoint={checkpoint_before:?}"), checkpoint_before.is_none()),
                observation("Stable recovery output", "exact payloads at offsets 1 and 2", format!("offsets={:?}", page.records.iter().map(|record| record.cursor.offset).collect::<Vec<_>>()), exact_records),
                observation("Generation rotation", "retry item 0 in generation 1; new item 1 in generation 2", format!("generation_one={generation_one}, generation_two={generation_two}"), generation_one && generation_two),
                observation("Published checkpoint", "captured byte 8, next item 2, committed output 2", format!("byte={}, next_item={}, committed={:?}", checkpoint.source.offset, checkpoint.next_item_index, checkpoint.committed_output.as_ref().map(|cursor| cursor.offset)), exact_checkpoint),
            ],
            output: json!({"checkpoint_before_resume": checkpoint_before.is_some(), "record_offsets": page.records.iter().map(|record| record.cursor.offset).collect::<Vec<_>>(), "generation_one_retry": generation_one, "generation_two_new_output": generation_two, "checkpoint": {"source_offset": checkpoint.source.offset, "next_item_index": checkpoint.next_item_index, "committed_output": checkpoint.committed_output.map(|cursor| cursor.offset)}}),
        }
    })
}

#[path = "../fixtures/journal_replication.rs"]
mod full_recovery_fixture;

#[path = "../fixtures/mixed_workload.rs"]
mod mixed_workload_fixture;

pub fn full_recovery(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"raw_input": "a\nb\n", "restart_after": "first_output_before_checkpoint", "stop": "close_reopen", "late_event": "c-after-completion"}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("full-recovery");
        let path = db.dir.join("origin.db");
        let (first, binding) = full_recovery_fixture::prepare_input(&path, "output").await;
        first.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(first);
        let resumed = Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default()).await.unwrap();
        full_recovery_fixture::finish_parsing(&resumed, &binding).await;
        let checkpoint = resumed.latest_checkpoint(&binding.source).await.unwrap().unwrap();
        resumed.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(resumed);
        let evidence = full_recovery_fixture::verify_recovered_history(&db.dir).await;
        CheckResult {
            summary: "Captured input recovered without duplicate output; snapshot and suffix survived retention and restart; the replica caught up to cursor 3.".into(),
            observations: vec![
                observation("Parser checkpoint", "byte 4 / item 2 / output 2", format!("byte {} / item {} / output {}", checkpoint.source.offset, checkpoint.next_item_index, checkpoint.committed_output.as_ref().unwrap().offset), checkpoint.source.offset == 4 && checkpoint.next_item_index == 2 && checkpoint.committed_output.as_ref().unwrap().offset == 2),
                observation("Durable EOF after cleanup and restart", "sealed at 4 / parser finished", format!("sealed at {:?} / finished {}", evidence.finalization.sealed_end, evidence.finalization.parser_finished), evidence.finalization.sealed_end == Some(4) && evidence.finalization.parser_finished),
                observation("Recovered snapshot", "state-through-a", String::from_utf8_lossy(&evidence.snapshot_bytes).into_owned(), evidence.snapshot_bytes == b"state-through-a"),
                observation("Retained suffix", "cursor 2", format!("cursor {}", evidence.suffix_offset), evidence.suffix_offset == 2),
                observation("Physical history cleanup", "2 records", format!("{} records", evidence.removed_records), evidence.removed_records == 2),
                observation("Replica after restart", "cursor 3 / backlog 0", format!("cursor {} / backlog {}", evidence.caught_up_offset, evidence.backlog_records), evidence.caught_up_offset == 3 && evidence.backlog_records == 0),
            ],
            output: json!({"captured_bytes": checkpoint.source.offset, "decoded_items": checkpoint.next_item_index, "snapshot": String::from_utf8_lossy(&evidence.snapshot_bytes), "snapshot_suffix_cursor": evidence.suffix_offset, "removed_source_records": evidence.removed_records, "replica_cursor": evidence.caught_up_offset, "backlog_records": evidence.backlog_records, "stop_mode": "close_reopen", "sealed_end": evidence.finalization.sealed_end, "parser_finished": evidence.finalization.parser_finished, "resource_measurements": null}),
        }
    })
}

pub fn mixed_workload(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({
            "mixed": true,
            "offers": 128,
            "interval_us": 1250,
            "snapshot_bytes": 65536
        }),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("mixed-workload");
        let directory = db.dir.join("fixture");
        let evidence = mixed_workload_fixture::run(
            &directory,
            mixed_workload_fixture::MixedConfig {
                mixed: true,
                offers: 128,
                interval_us: 1250,
                snapshot_bytes: 64 * 1024,
            },
        )
        .await;

        let outcomes = evidence.accepted
            + evidence.runtime_rejected
            + evidence.generator_rejected;
        let latency_samples_complete = evidence.receipt_ns.len() == evidence.accepted
            && evidence.lateness_ns.len()
                == evidence.accepted + evidence.runtime_rejected;
        let receipt_max_ns = evidence.receipt_ns.iter().copied().max().unwrap_or(0);
        let lateness_max_ns = evidence.lateness_ns.iter().copied().max().unwrap_or(0);

        CheckResult {
            summary: "The bounded mixed workload conserved every offer, completed 64 replay calls, and committed foreground receipts during maintenance through production SQLite APIs.".into(),
            observations: vec![
                observation("Offer outcomes", "128 offered equals accepted + runtime rejected + generator rejected", format!("offered={}, accepted={}, runtime_rejected={}, generator_rejected={}", evidence.offered, evidence.accepted, evidence.runtime_rejected, evidence.generator_rejected), evidence.offered == 128 && outcomes == evidence.offered),
                observation("Bounded work", "at most 64 foreground tasks and 1,024 queued runtime appends", format!("peak_tasks={}, peak_runtime_queue={}", evidence.peak_tasks, evidence.peak_runtime_queue), evidence.peak_tasks <= 64 && evidence.peak_runtime_queue <= 1024),
                observation("Concurrent progress", "64 replay pages and at least one foreground receipt during maintenance", format!("replay_pages={}, overlap_receipts={}, maintenance_ns={}", evidence.replay_pages, evidence.overlap_receipts, evidence.maintenance_ns), evidence.replay_pages == 64 && evidence.overlap_receipts > 0 && evidence.maintenance_ns > 0),
                observation("Cleanup and replica recovery", "32 source records removed and replica caught up through cursor 33", format!("removed_records={}, replica_tail={}", evidence.removed_records, evidence.replica_tail), evidence.removed_records == 32 && evidence.replica_tail == 33),
                observation("Latency samples (correctness diagnostic; not resource qualification)", "one receipt sample per accepted offer and one lateness sample per submitted call", format!("receipt_samples={}, lateness_samples={}, receipt_max_ns={}, lateness_max_ns={}", evidence.receipt_ns.len(), evidence.lateness_ns.len(), receipt_max_ns, lateness_max_ns), latency_samples_complete),
            ],
            output: json!({
                "outcomes": {
                    "offered": evidence.offered,
                    "accepted": evidence.accepted,
                    "runtime_rejected": evidence.runtime_rejected,
                    "generator_rejected": evidence.generator_rejected
                },
                "bounds": {
                    "peak_tasks": evidence.peak_tasks,
                    "max_tasks": 64,
                    "peak_runtime_queue": evidence.peak_runtime_queue,
                    "max_runtime_queue": 1024
                },
                "overlap": {
                    "replay_pages": evidence.replay_pages,
                    "foreground_receipts_during_maintenance": evidence.overlap_receipts,
                    "maintenance_ns": evidence.maintenance_ns
                },
                "cleanup_removed_records": evidence.removed_records,
                "replica_tail": evidence.replica_tail,
                "latency_correctness_diagnostic_not_resource_qualification": {
                    "receipt_samples": evidence.receipt_ns.len(),
                    "lateness_samples": evidence.lateness_ns.len(),
                    "receipt_max_ns": receipt_max_ns,
                    "lateness_max_ns": lateness_max_ns
                },
                "resource_measurements": null
            }),
        }
    })
}

#[path = "../fixtures/snapshot_interruption.rs"]
mod snapshot_interruption_fixture;

pub fn snapshot_partial(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"stop": "close_reopen_during_upload", "chunk_bytes": 4, "cleanup_rows_per_call": 1}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("snapshot-interruption");
        let evidence = snapshot_interruption_fixture::run(&db.dir.join("scenario")).await;
        CheckResult {
            summary: "Partial uploads survived reopening without becoming visible; one resumed exactly and the other was reclaimed in bounded steps.".into(),
            observations: vec![
                observation("Unpublished uploads", "12 staged bytes; only the baseline published", format!("staged_bytes={}, published={}", evidence.staged_bytes_after_reopen, evidence.published_count_while_interrupted), evidence.staged_bytes_after_reopen == 12 && evidence.published_count_while_interrupted == 1),
                observation("Exact chunk retry", "4 accepted bytes after retry", evidence.exact_retry_accepted_bytes.to_string(), evidence.exact_retry_accepted_bytes == 4),
                observation("Recovered contents", "baseline unchanged and resumed bytes exact", format!("baseline={}, resumed={}", String::from_utf8_lossy(&evidence.baseline_snapshot_bytes), String::from_utf8_lossy(&evidence.resumed_snapshot_bytes)), evidence.baseline_snapshot_bytes == b"published-baseline" && evidence.resumed_snapshot_bytes == b"resume-after-reopen"),
                observation("Bounded cleanup", "2 chunks and 1 upload removed in at most 8 calls; no uploads remain", format!("calls={}, chunks={}, uploads_removed={}, uploads_remaining={}", evidence.cleanup_steps, evidence.cleanup_removed_chunks, evidence.cleanup_removed_snapshots, evidence.uploads_remaining), evidence.cleanup_steps <= 8 && evidence.cleanup_removed_chunks == 2 && evidence.cleanup_removed_snapshots == 1 && evidence.uploads_remaining == 0),
            ],
            output: json!({"stop_mode":"close_reopen", "staged_bytes":evidence.staged_bytes_after_reopen,"published_while_interrupted":evidence.published_count_while_interrupted,"retry_accepted_bytes":evidence.exact_retry_accepted_bytes,"baseline":String::from_utf8_lossy(&evidence.baseline_snapshot_bytes),"resumed":String::from_utf8_lossy(&evidence.resumed_snapshot_bytes),"cleanup":{"calls":evidence.cleanup_steps,"chunks":evidence.cleanup_removed_chunks,"uploads":evidence.cleanup_removed_snapshots,"remaining":evidence.uploads_remaining},"resource_measurements":null}),
        }
    })
}

#[path = "../fixtures/retention_read.rs"]
mod retention_read_fixture;

pub fn retention_race(cx: &RunContext) -> CheckResult {
    if let Some(result) = require_input(
        cx,
        json!({"gate_positions": ["before_sqlite_read", "after_sqlite_page"], "records_per_stream": 3, "new_floor": 3}),
    ) {
        return result;
    }
    runtime().block_on(async {
        let db = TempDb::new("retention-read");
        let e = retention_read_fixture::run(&db.dir.join("scenario")).await;
        let expected_payloads = (1..=3).map(|n| format!("read-after-sqlite:{n}").into_bytes()).collect::<Vec<_>>();
        CheckResult {
            summary: "A read delayed before SQLite reported missing history; a complete SQLite page remained intact after physical cleanup.".into(),
            observations: vec![
                observation("Cleanup before read", "HistoryUnavailable with floor 3 and tail 3", format!("history_error={}, floor={}, tail={}", e.pending_read_was_history_unavailable, e.pending_read_floor, e.pending_read_tail), e.pending_read_was_history_unavailable && e.pending_read_floor == 3 && e.pending_read_tail == 3),
                observation("Cleanup after page creation", "complete offsets 1, 2, 3 through cursor 3", format!("offsets={:?}, complete={}, through={}", e.completed_offsets, e.completed_page_complete, e.completed_page_through), e.completed_offsets == [1,2,3] && e.completed_page_complete && e.completed_page_through == 3),
                observation("Owned payloads survive cleanup", "all three payloads unchanged", format!("{} retained payloads", e.retained_payloads_after_cleanup.len()), e.retained_payloads_after_cleanup == expected_payloads),
                observation("Physical cleanup", "6 records removed, no pending cleanup", format!("removed={}, remaining={}", e.removed_event_rows, e.cleanup_remaining), e.removed_event_rows == 6 && !e.cleanup_remaining),
            ],
            output: json!({"schedule_scope":"injected EventStore port gates around real SQLite read; not concurrent SQLite transactions", "before_read":{"history_unavailable":e.pending_read_was_history_unavailable,"floor":e.pending_read_floor,"tail":e.pending_read_tail},"owned_page":{"offsets":e.completed_offsets,"complete":e.completed_page_complete,"through":e.completed_page_through,"payloads":e.retained_payloads_after_cleanup},"removed_records":e.removed_event_rows,"cleanup_remaining":e.cleanup_remaining,"resource_measurements":null}),
        }
    })
}
