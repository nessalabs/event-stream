#![allow(dead_code, unused_imports, unused_macros)]

use event_stream::*;
use std::sync::Arc;

pub fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("test.bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(payload),
    }
}

#[cfg(feature = "source-journal")]
pub async fn run_source_journal_contract<S: SourceJournalStore>(store: S) {
    let output = store
        .create_if_absent(&StreamId::new("journal-contract-output").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("journal-contract-enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let source = SourceKey {
        id: SourceId::new("journal-contract-input").unwrap(),
        incarnation: SourceIncarnation([91; 16]),
    };
    let parser = ParserRef {
        id: ParserId::new("journal-contract-parser").unwrap(),
        version: 3,
    };
    let binding = SourceBinding {
        source: source.clone(),
        parser: parser.clone(),
        output_stream: output.clone(),
    };
    let begin = BeginSource {
        operation_id: JournalOperationId::new("journal-contract-begin").unwrap(),
        binding: binding.clone(),
    };
    let begun = store.begin_source(begin.clone()).await.unwrap();
    assert_eq!(store.begin_source(begin).await.unwrap(), begun);
    let segment = RawSegment {
        start: SourcePosition {
            source: source.clone(),
            offset: 0,
        },
        bytes: Payload::copy_from_slice(b"one frame"),
    };
    let captured = store.capture_segment(segment.clone()).await.unwrap();
    assert_eq!(store.capture_segment(segment).await.unwrap(), captured);
    let mut committed = None;
    for index in 0..2 {
        let output_request = JournaledOutput {
            source: source.clone(),
            position: DecodedPosition {
                source_byte: 0,
                item_index: index,
            },
            event: event(&format!("journal-contract-{index}"), &[index as u8]),
        };
        let receipt = store
            .append_captured(&output, output_request.clone())
            .await
            .unwrap();
        assert_eq!(
            store
                .append_captured(&output, output_request)
                .await
                .unwrap()
                .record,
            receipt.record
        );
        committed = Some(receipt.record.cursor.clone());
    }
    let checkpoint = ParserCheckpoint {
        source: SourcePosition {
            source: source.clone(),
            offset: 9,
        },
        parser,
        state: Payload::copy_from_slice(b"checkpoint"),
        next_item_index: 2,
        output_stream: output.clone(),
        committed_output: committed,
    };
    store
        .publish_parser_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    assert_eq!(
        store.latest_checkpoint(&source).await.unwrap(),
        Some(checkpoint)
    );
    store
        .advance_capture_receipt_floor(AdvanceCaptureReceiptFloor {
            operation_id: JournalOperationId::new("journal-contract-receipt-floor").unwrap(),
            source,
            expected_floor: 0,
            new_floor: 9,
        })
        .await
        .unwrap();
    let cleanup = store
        .cleanup_captured(SourceJournalStoreConfig::default().cleanup)
        .await
        .unwrap();
    assert_eq!(cleanup.removed_segment_rows, 1);
    assert_eq!(cleanup.removed_marker_rows, 2);
    assert_eq!(cleanup.removed_receipt_rows, 1);
}

pub async fn run_store_contract<S: EventStore>(store: S) {
    let store = Arc::new(store);
    let missing = StreamKey {
        id: StreamId::new("missing").unwrap(),
        incarnation: IncarnationId([42; 16]),
    };
    assert!(matches!(
        store.bounds(&missing).await,
        Err(Error::StreamNotFound)
    ));

    let id = StreamId::new("contract-stream").unwrap();
    let key = store.create_if_absent(&id).await.unwrap();
    assert_eq!(key, store.create_if_absent(&id).await.unwrap());
    let empty = store.bounds(&key).await.unwrap();
    assert_eq!((empty.floor.offset, empty.tail.offset), (0, 0));

    let first_input = event("event-1", b"exact bytes");
    let first = store
        .append_atomic(&key, first_input.clone())
        .await
        .unwrap();
    assert_eq!(first.kind, AppendKind::Inserted);
    assert_eq!(first.record.cursor.offset, 1);
    let retry = store.append_atomic(&key, first_input).await.unwrap();
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(retry.record, first.record);
    assert!(matches!(
        store
            .append_atomic(&key, event("event-1", b"different"))
            .await,
        Err(Error::IdempotencyConflict { .. })
    ));
    assert_eq!(store.bounds(&key).await.unwrap().tail.offset, 1);
    assert_eq!(
        store
            .lookup_event(&key, &EventId::new("event-1").unwrap())
            .await
            .unwrap(),
        Some(first.record.clone())
    );

    for offset in 2..=6 {
        let receipt = store
            .append_atomic(&key, event(&format!("event-{offset}"), &[offset as u8]))
            .await
            .unwrap();
        assert_eq!(receipt.record.cursor.offset, offset);
    }
    let limits = PageLimits {
        max_records: 2,
        max_bytes: 4096,
    };
    let page1 = store.read_range(&key, 0, 5, limits).await.unwrap();
    assert_eq!(
        page1
            .records
            .iter()
            .map(|r| r.cursor.offset)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(page1.next_after.offset, 2);
    assert!(!page1.complete);
    let page2 = store.read_range(&key, 2, 5, limits).await.unwrap();
    assert_eq!(
        page2
            .records
            .iter()
            .map(|r| r.cursor.offset)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    let page3 = store.read_range(&key, 4, 5, limits).await.unwrap();
    assert_eq!(
        page3
            .records
            .iter()
            .map(|r| r.cursor.offset)
            .collect::<Vec<_>>(),
        vec![5]
    );
    assert!(page3.complete);
    let empty = store.read_range(&key, 5, 5, limits).await.unwrap();
    assert!(empty.records.is_empty() && empty.complete);

    assert!(matches!(
        store.read_range(&key, 7, 7, limits).await,
        Err(Error::CursorAhead { .. })
    ));
    assert!(matches!(
        store.read_range(&key, 4, 3, limits).await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        store
            .read_range(
                &key,
                0,
                1,
                PageLimits {
                    max_records: 0,
                    max_bytes: 1
                }
            )
            .await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(matches!(
        store
            .read_range(
                &key,
                0,
                1,
                PageLimits {
                    max_records: 1,
                    max_bytes: 1
                }
            )
            .await,
        Err(Error::CapacityExceeded)
    ));

    let wrong = StreamKey {
        id: key.id.clone(),
        incarnation: IncarnationId([99; 16]),
    };
    assert!(matches!(
        store.bounds(&wrong).await,
        Err(Error::StaleIncarnation { .. })
    ));
    assert!(matches!(
        store.append_atomic(&wrong, event("wrong", b"x")).await,
        Err(Error::StaleIncarnation { .. })
    ));

    let concurrent_id = StreamId::new("concurrent").unwrap();
    let concurrent = store.create_if_absent(&concurrent_id).await.unwrap();
    let mut tasks = Vec::new();
    for number in 0..32 {
        let store = store.clone();
        let concurrent = concurrent.clone();
        tasks.push(tokio::spawn(async move {
            store
                .append_atomic(&concurrent, event(&format!("c-{number}"), &[number]))
                .await
                .unwrap()
        }));
    }
    let mut offsets = Vec::new();
    for task in tasks {
        offsets.push(task.await.unwrap().record.cursor.offset);
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (1..=32).collect::<Vec<_>>());
    assert_eq!(store.bounds(&concurrent).await.unwrap().tail.offset, 32);

    let create_id = StreamId::new("concurrent-create").unwrap();
    let mut creates = Vec::new();
    for _ in 0..16 {
        let store = store.clone();
        let create_id = create_id.clone();
        creates.push(tokio::spawn(async move {
            store.create_if_absent(&create_id).await.unwrap()
        }));
    }
    let mut created = Vec::new();
    for task in creates {
        created.push(task.await.unwrap());
    }
    assert!(created.iter().all(|key| key == &created[0]));

    let first_attempt_stream = store
        .create_if_absent(&StreamId::new("concurrent-first-attempt").unwrap())
        .await
        .unwrap();
    let first_attempt = event("same-first-id", b"same-first-payload");
    let mut first_attempts = Vec::new();
    for _ in 0..32 {
        let store = store.clone();
        let stream = first_attempt_stream.clone();
        let candidate = first_attempt.clone();
        first_attempts.push(tokio::spawn(async move {
            store.append_atomic(&stream, candidate).await.unwrap()
        }));
    }
    let mut first_attempt_receipts = Vec::new();
    for task in first_attempts {
        first_attempt_receipts.push(task.await.unwrap());
    }
    assert_eq!(
        first_attempt_receipts
            .iter()
            .filter(|receipt| receipt.kind == AppendKind::Inserted)
            .count(),
        1
    );
    assert_eq!(
        first_attempt_receipts
            .iter()
            .filter(|receipt| receipt.kind == AppendKind::Deduplicated)
            .count(),
        31
    );
    assert!(first_attempt_receipts.iter().all(|receipt| {
        receipt.record.cursor.offset == 1 && receipt.record.event == first_attempt
    }));
    assert_eq!(
        store
            .bounds(&first_attempt_stream)
            .await
            .unwrap()
            .tail
            .offset,
        1
    );

    let identity_stream = store
        .create_if_absent(&StreamId::new("concurrent-identity").unwrap())
        .await
        .unwrap();
    let established = event("shared-id", b"same");
    store
        .append_atomic(&identity_stream, established.clone())
        .await
        .unwrap();
    let mut identities = Vec::new();
    for number in 0..32 {
        let store = store.clone();
        let identity_stream = identity_stream.clone();
        let mut candidate = established.clone();
        if number % 2 == 1 {
            candidate.schema.version = 2;
        }
        identities.push(tokio::spawn(async move {
            store.append_atomic(&identity_stream, candidate).await
        }));
    }
    for (number, task) in identities.into_iter().enumerate() {
        let result = task.await.unwrap();
        if number % 2 == 0 {
            assert_eq!(result.unwrap().kind, AppendKind::Deduplicated);
        } else {
            assert!(matches!(result, Err(Error::IdempotencyConflict { .. })));
        }
    }
    assert_eq!(store.bounds(&identity_stream).await.unwrap().tail.offset, 1);

    let isolated_a = store
        .create_if_absent(&StreamId::new("isolated-a").unwrap())
        .await
        .unwrap();
    let isolated_b = store
        .create_if_absent(&StreamId::new("isolated-b").unwrap())
        .await
        .unwrap();
    let shared_id_a = event("same-id-across-streams", b"payload-a");
    let shared_id_b = event("same-id-across-streams", b"payload-b");
    let receipt_a = store
        .append_atomic(&isolated_a, shared_id_a.clone())
        .await
        .unwrap();
    let receipt_b = store
        .append_atomic(&isolated_b, shared_id_b.clone())
        .await
        .unwrap();
    assert_eq!(receipt_a.kind, AppendKind::Inserted);
    assert_eq!(receipt_b.kind, AppendKind::Inserted);
    assert_eq!(receipt_a.record.cursor.offset, 1);
    assert_eq!(receipt_b.record.cursor.offset, 1);
    assert_eq!(
        store
            .lookup_event(&isolated_a, &shared_id_a.id)
            .await
            .unwrap()
            .unwrap()
            .event,
        shared_id_a
    );
    assert_eq!(
        store
            .lookup_event(&isolated_b, &shared_id_b.id)
            .await
            .unwrap()
            .unwrap()
            .event,
        shared_id_b
    );

    // The generic stream stores application envelopes as opaque bytes. A schema
    // version unknown to this library must round-trip unchanged.
    let opaque = store
        .create_if_absent(&StreamId::new("opaque-schema").unwrap())
        .await
        .unwrap();
    let unknown = NewEvent {
        id: EventId::new("opaque-1").unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("future.vendor.envelope").unwrap(),
            version: u32::MAX,
        },
        payload: Payload::copy_from_slice(&[0, 255, 17, 99]),
    };
    store.append_atomic(&opaque, unknown.clone()).await.unwrap();
    let replay = store
        .read_range(
            &opaque,
            0,
            1,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(replay.records[0].event, unknown);

    store.close().await.unwrap();
    assert!(matches!(store.bounds(&key).await, Err(Error::Closed)));
}

pub async fn run_lifecycle_store_contract<S: LifecycleStore>(store: S) {
    let store = Arc::new(store);
    let id = StreamId::new("lifecycle-contract").unwrap();
    let first = store.create_if_absent(&id).await.unwrap();
    for index in 1..=3 {
        store
            .append_atomic(&first, event(&format!("retired-{index}"), &[index]))
            .await
            .unwrap();
    }
    let reset = LifecycleRequest {
        operation_id: LifecycleOperationId::new("reset-1").unwrap(),
        expected: first.clone(),
        action: LifecycleAction::Reset,
    };
    let reset_receipt = store.change_lifecycle(reset.clone()).await.unwrap();
    let second = reset_receipt.replacement.clone().unwrap();
    assert_ne!(first, second);
    assert_eq!(
        store.change_lifecycle(reset.clone()).await.unwrap(),
        reset_receipt
    );
    let conflict = LifecycleRequest {
        action: LifecycleAction::Delete,
        ..reset
    };
    assert!(matches!(
        store.change_lifecycle(conflict).await,
        Err(Error::LifecycleConflict { .. })
    ));
    assert!(matches!(
        store.bounds(&first).await,
        Err(Error::StaleIncarnation {
            current
        }) if *current == StreamAvailability::Active(second.clone())
    ));
    assert!(matches!(
        store.append_atomic(&first, event("stale", b"x")).await,
        Err(Error::StaleIncarnation { .. })
    ));
    assert_eq!(store.bounds(&second).await.unwrap().tail.offset, 0);

    let delete = LifecycleRequest {
        operation_id: LifecycleOperationId::new("delete-2").unwrap(),
        expected: second.clone(),
        action: LifecycleAction::Delete,
    };
    assert!(store
        .change_lifecycle(delete.clone())
        .await
        .unwrap()
        .replacement
        .is_none());
    assert!(matches!(
        store.create_if_absent(&id).await,
        Err(Error::StreamUnavailable { last }) if *last == second
    ));
    assert_eq!(
        store.change_lifecycle(delete).await.unwrap().replacement,
        None
    );

    let revive = LifecycleRequest {
        operation_id: LifecycleOperationId::new("reset-3").unwrap(),
        expected: second.clone(),
        action: LifecycleAction::Reset,
    };
    let third = store
        .change_lifecycle(revive)
        .await
        .unwrap()
        .replacement
        .unwrap();
    assert_ne!(second, third);
    assert_eq!(store.create_if_absent(&id).await.unwrap(), third);

    let limits = CleanupLimits {
        max_records: 2,
        max_bytes: 2 * 1024 * 1024,
    };
    let first_cleanup = store.cleanup_retired(limits).await.unwrap();
    assert_eq!(first_cleanup.stream, Some(first.clone()));
    assert_eq!(first_cleanup.removed_records, 2);
    assert!(first_cleanup.remaining);
    let second_cleanup = store.cleanup_retired(limits).await.unwrap();
    assert_eq!(second_cleanup.stream, Some(first));
    assert_eq!(second_cleanup.removed_records, 1);
    assert!(second_cleanup.remaining);
    let empty_lifetime = store.cleanup_retired(limits).await.unwrap();
    assert_eq!(empty_lifetime.stream, Some(second));
    assert_eq!(empty_lifetime.removed_records, 0);
    assert!(!empty_lifetime.remaining);
    assert_eq!(store.cleanup_retired(limits).await.unwrap().stream, None);

    let concurrent_key = store
        .create_if_absent(&StreamId::new("concurrent-lifecycle").unwrap())
        .await
        .unwrap();
    let concurrent_request = LifecycleRequest {
        operation_id: LifecycleOperationId::new("concurrent-reset").unwrap(),
        expected: concurrent_key,
        action: LifecycleAction::Reset,
    };
    let mut calls = Vec::new();
    for _ in 0..16 {
        let store = store.clone();
        let request = concurrent_request.clone();
        calls.push(tokio::spawn(async move {
            store.change_lifecycle(request).await.unwrap()
        }));
    }
    let mut receipts = Vec::new();
    for call in calls {
        receipts.push(call.await.unwrap());
    }
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    assert_eq!(
        store
            .create_if_absent(&concurrent_request.expected.id)
            .await
            .unwrap(),
        receipts[0].replacement.clone().unwrap()
    );
}

#[cfg(feature = "snapshots")]
pub async fn run_snapshot_store_contract<S: SnapshotStore>(store: S) {
    use sha2::{Digest, Sha256};

    let store = Arc::new(store);
    let stream = store
        .create_if_absent(&StreamId::new("snapshot-contract").unwrap())
        .await
        .unwrap();
    for offset in 1..=3 {
        store
            .append_atomic(
                &stream,
                event(&format!("snapshot-event-{offset}"), &[offset]),
            )
            .await
            .unwrap();
    }
    let content = b"abcdef";
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([1; 16]),
        covered: Cursor::new(stream.clone(), 1),
        schema: SchemaRef {
            id: SchemaId::new("snapshot.state").unwrap(),
            version: 1,
        },
        content_bytes: content.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(content).into()),
    };
    let begun = store.begin_snapshot(descriptor.clone()).await.unwrap();
    assert_eq!(begun.state, SnapshotUploadState::Uploading);
    assert_eq!(
        store.begin_snapshot(descriptor.clone()).await.unwrap(),
        begun
    );
    let mut conflicting = descriptor.clone();
    conflicting.content_bytes += 1;
    assert!(matches!(
        store.begin_snapshot(conflicting).await,
        Err(SnapshotError::OperationConflict { .. })
    ));

    let first = SnapshotChunk {
        offset: 0,
        bytes: Payload::copy_from_slice(b"abc"),
    };
    let second = SnapshotChunk {
        offset: 3,
        bytes: Payload::copy_from_slice(b"def"),
    };
    store
        .put_snapshot_chunk(descriptor.id, first.clone())
        .await
        .unwrap();
    let retry = store
        .put_snapshot_chunk(descriptor.id, first)
        .await
        .unwrap();
    assert_eq!(retry.accepted_bytes, 3);
    assert!(matches!(
        store
            .put_snapshot_chunk(
                descriptor.id,
                SnapshotChunk {
                    offset: 0,
                    bytes: Payload::copy_from_slice(b"abd"),
                },
            )
            .await,
        Err(SnapshotError::OperationConflict { .. })
    ));
    store
        .put_snapshot_chunk(descriptor.id, second)
        .await
        .unwrap();
    let limits = VerificationLimits {
        max_chunks: 1,
        max_bytes: 2,
    };
    let mut progress = store
        .verify_snapshot_step(descriptor.id, limits)
        .await
        .unwrap();
    while progress.state != SnapshotUploadState::Verified {
        let next = store
            .verify_snapshot_step(descriptor.id, limits)
            .await
            .unwrap();
        assert!(next.verified_bytes > progress.verified_bytes);
        progress = next;
    }
    assert_eq!(progress.verified_bytes, content.len() as u64);
    assert_eq!(
        store.publish_snapshot(descriptor.id).await.unwrap(),
        descriptor
    );
    assert_eq!(
        store.publish_snapshot(descriptor.id).await.unwrap(),
        descriptor
    );

    let equal_cursor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([3; 16]),
        covered: descriptor.covered.clone(),
        schema: descriptor.schema.clone(),
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    store.begin_snapshot(equal_cursor.clone()).await.unwrap();
    store
        .verify_snapshot_step(
            equal_cursor.id,
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    store.publish_snapshot(equal_cursor.id).await.unwrap();

    let page = store
        .list_snapshots(
            &stream,
            None,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.entries, vec![descriptor.clone()]);
    assert!(!page.complete);
    let second_page = store
        .list_snapshots(
            &stream,
            page.next_after,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(second_page.entries, vec![equal_cursor]);
    assert!(second_page.complete);

    let plan = store
        .acquire_recovery(descriptor.id, std::time::Duration::from_secs(30))
        .await
        .unwrap();
    assert_eq!(plan.through.offset, 3);
    let content_page = store.read_snapshot_chunk(plan.lease, 2, 3).await.unwrap();
    assert_eq!(content_page.bytes.as_bytes(), b"cde");
    assert_eq!(content_page.next_offset, 5);
    assert!(!content_page.complete);
    let history_page = store
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
        history_page
            .records
            .iter()
            .map(|record| record.cursor.offset)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        store.release_recovery(plan.lease).await.unwrap(),
        RecoveryRelease::Released
    );
    assert_eq!(
        store.release_recovery(plan.lease).await.unwrap(),
        RecoveryRelease::AlreadyReleased
    );

    let aborted = SnapshotDescriptor {
        id: SnapshotId::from_bytes([2; 16]),
        covered: Cursor::new(stream, 3),
        schema: descriptor.schema,
        content_bytes: 1,
        digest: SnapshotDigest::from_bytes(Sha256::digest(b"x").into()),
    };
    store.begin_snapshot(aborted.clone()).await.unwrap();
    let receipt = store.abort_snapshot(aborted.id).await.unwrap();
    assert!(!receipt.already_aborted);
    assert!(
        store
            .abort_snapshot(aborted.id)
            .await
            .unwrap()
            .already_aborted
    );
    assert_eq!(
        store.snapshot_status(aborted.id).await.unwrap().state,
        SnapshotUploadState::Aborted
    );

    let invalid_digest = SnapshotDescriptor {
        id: SnapshotId::from_bytes([4; 16]),
        covered: Cursor::new(aborted.covered.stream.clone(), 3),
        schema: aborted.schema,
        content_bytes: 1,
        digest: SnapshotDigest::from_bytes([0; 32]),
    };
    store.begin_snapshot(invalid_digest.clone()).await.unwrap();
    assert!(matches!(
        store
            .put_snapshot_chunk(
                invalid_digest.id,
                SnapshotChunk {
                    offset: 1,
                    bytes: Payload::copy_from_slice(b"x"),
                },
            )
            .await,
        Err(SnapshotError::InvalidInput(_))
    ));
    store
        .put_snapshot_chunk(
            invalid_digest.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(b"x"),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .verify_snapshot_step(
                invalid_digest.id,
                VerificationLimits {
                    max_chunks: 1,
                    max_bytes: 1,
                },
            )
            .await
            .unwrap_err(),
        SnapshotError::ChecksumMismatch {
            id: invalid_digest.id
        }
    );
    assert!(matches!(
        store.publish_snapshot(invalid_digest.id).await,
        Err(SnapshotError::IncompleteUpload { .. })
    ));

    let active = SnapshotDescriptor {
        id: SnapshotId::from_bytes([5; 16]),
        covered: Cursor::new(aborted.covered.stream, 3),
        schema: SchemaRef {
            id: SchemaId::new("snapshot.active").unwrap(),
            version: 1,
        },
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes(Sha256::digest([]).into()),
    };
    store.begin_snapshot(active.clone()).await.unwrap();
    let uploads = store
        .list_snapshot_uploads(
            None,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        uploads
            .entries
            .iter()
            .map(|entry| (entry.descriptor.id, entry.state))
            .collect::<Vec<_>>(),
        vec![
            (aborted.id, SnapshotUploadState::Aborted),
            (invalid_digest.id, SnapshotUploadState::Verifying),
        ]
    );
    assert!(!uploads.complete);
    assert_eq!(uploads.next_after, Some(invalid_digest.id));
    let final_upload_page = store
        .list_snapshot_uploads(
            uploads.next_after,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(final_upload_page.entries.len(), 1);
    assert_eq!(final_upload_page.entries[0].descriptor, active);
    assert_eq!(
        final_upload_page.entries[0].state,
        SnapshotUploadState::Uploading
    );
    assert!(final_upload_page.complete);

    let cleaned = store
        .cleanup_snapshot_staging(SnapshotCleanupLimits {
            max_rows: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_snapshots, 1);
    assert_eq!(cleaned.removed_chunks, 0);
    assert!(!cleaned.remaining);
    assert_eq!(
        store.snapshot_status(aborted.id).await.unwrap().state,
        SnapshotUploadState::Aborted
    );
    let remaining_uploads = store
        .list_snapshot_uploads(
            None,
            PageLimits {
                max_records: 8,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert!(remaining_uploads
        .entries
        .iter()
        .all(|entry| entry.descriptor.id != aborted.id));
    assert!(remaining_uploads
        .entries
        .iter()
        .any(|entry| entry.descriptor.id == active.id));
}

macro_rules! event_store_contract {
    ($open:expr) => {{
        common::run_store_contract($open.await.expect("store opens")).await
    }};
}

macro_rules! lifecycle_store_contract {
    ($open:expr) => {{
        let store = $open.await.expect("store opens");
        $crate::common::run_lifecycle_store_contract(store).await;
    }};
}
#[cfg(feature = "snapshots")]
macro_rules! snapshot_store_contract {
    ($open:expr) => {{
        let store = $open.await.expect("store opens");
        $crate::common::run_snapshot_store_contract(store).await;
    }};
}
pub(crate) use event_store_contract;
pub(crate) use lifecycle_store_contract;
#[cfg(feature = "snapshots")]
pub(crate) use snapshot_store_contract;

#[cfg(feature = "replication")]
pub async fn run_replica_batch_destination_contract<S: ReplicaBatchDestinationStore>(store: S) {
    let epoch = store.destination_epoch().await.unwrap();
    let stream = OriginStream {
        origin: OriginId([31; 16]),
        stream: StreamKey {
            id: StreamId::new("replica-contract").unwrap(),
            incarnation: IncarnationId([32; 16]),
        },
    };
    let make_record = |offset, byte| {
        Arc::new(Record {
            cursor: Cursor::new(stream.stream.clone(), offset),
            event: event(&format!("replicated-{offset}"), &[byte]),
        })
    };
    let batch = ReplicaBatch {
        id: BatchId([33; 16]),
        destination_epoch: epoch,
        after: ReplicaPosition {
            stream: stream.clone(),
            offset: 0,
        },
        records: vec![make_record(1, 1), make_record(2, 2)],
    };
    let receipt = store.commit_replica_batch(batch.clone()).await.unwrap();
    assert_eq!(receipt.committed_through.offset, 2);
    assert_eq!(
        store.commit_replica_batch(batch.clone()).await.unwrap(),
        receipt
    );

    let mut conflict = batch.clone();
    conflict.records[1] = make_record(2, 9);
    assert!(matches!(
        store.commit_replica_batch(conflict).await,
        Err(ReplicationError::BatchConflict { .. })
    ));
    let page = store
        .read_replica_after(
            &batch.after,
            ReplicaBatchLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].event.payload.as_bytes(), &[1]);
    assert_eq!(page.records[1].event.payload.as_bytes(), &[2]);
    assert!(page.complete);

    let floor = AdvanceReplicaReceiptFloor {
        operation_id: ReplicationOperationId::new("replica-floor-contract").unwrap(),
        stream: stream.clone(),
        destination_epoch: epoch,
        expected_floor: ReplicaPosition {
            stream: stream.clone(),
            offset: 0,
        },
        new_floor: ReplicaPosition { stream, offset: 2 },
    };
    let floor_receipt = store
        .advance_replica_receipt_floor(floor.clone())
        .await
        .unwrap();
    assert_eq!(
        store.advance_replica_receipt_floor(floor).await.unwrap(),
        floor_receipt
    );
    let conflicting_floor = AdvanceReplicaReceiptFloor {
        operation_id: floor_receipt.request.operation_id.clone(),
        stream: floor_receipt.request.stream.clone(),
        destination_epoch: epoch,
        expected_floor: ReplicaPosition {
            stream: floor_receipt.request.stream.clone(),
            offset: 2,
        },
        new_floor: ReplicaPosition {
            stream: floor_receipt.request.stream.clone(),
            offset: 2,
        },
    };
    assert!(matches!(
        store.advance_replica_receipt_floor(conflicting_floor).await,
        Err(ReplicationError::InvalidInput(_))
    ));
    assert!(matches!(
        store.commit_replica_batch(batch).await,
        Err(ReplicationError::ReceiptExpired)
    ));
}

#[cfg(feature = "replication")]
pub async fn run_replication_origin_batch_contract<S: ReplicationOriginStore>(store: S) {
    let key = store
        .create_if_absent(&StreamId::new("replication-origin-contract").unwrap())
        .await
        .unwrap();
    store.append_atomic(&key, event("one", &[1])).await.unwrap();
    let stream = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: key.clone(),
    };
    let replica = ReplicaId::new("origin-contract-replica").unwrap();
    let epoch = DestinationEpoch([41; 16]);
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("origin-contract-attach").unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            max_backlog_bytes: 4096,
            max_backlog_age: std::time::Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    store.append_atomic(&key, event("two", &[2])).await.unwrap();
    let request = PrepareReplicaBatch {
        operation_id: ReplicationOperationId::new("origin-contract-prepare").unwrap(),
        batch_id: BatchId([42; 16]),
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
    };
    let prepared = store.prepare_replica_batch(request.clone()).await.unwrap();
    assert_eq!(prepared.batch.as_ref().unwrap().records.len(), 2);
    store
        .append_atomic(&key, event("three", &[3]))
        .await
        .unwrap();
    assert_eq!(
        store.prepare_replica_batch(request.clone()).await.unwrap(),
        prepared
    );
    let mut conflicting_prepare = request;
    conflicting_prepare.batch_id = BatchId([43; 16]);
    assert!(matches!(
        store.prepare_replica_batch(conflicting_prepare).await,
        Err(ReplicationError::InvalidInput(_))
    ));
    let batch = prepared.batch.unwrap();
    let mut ack = AcknowledgeReplicaBatch {
        operation_id: ReplicationOperationId::new("origin-contract-ack").unwrap(),
        replica,
        expected_after: batch.after.clone(),
        receipt: ReplicaReceipt {
            batch: batch.id,
            destination_epoch: epoch,
            committed_through: ReplicaPosition { stream, offset: 2 },
        },
    };
    ack.receipt.committed_through.offset = 3;
    assert!(matches!(
        store.acknowledge_replica_batch(ack.clone()).await,
        Err(ReplicationError::InvalidReceipt(_))
    ));
    ack.receipt.committed_through.offset = 2;
    let receipt = store.acknowledge_replica_batch(ack.clone()).await.unwrap();
    assert_eq!(receipt.status.acknowledged.offset, 2);
    assert_eq!(receipt.status.backlog_records, 1);
    assert_eq!(store.acknowledge_replica_batch(ack).await.unwrap(), receipt);
}

#[cfg(feature = "replication")]
macro_rules! replica_batch_destination_contract {
    ($open:expr) => {{
        let store = $open.await.expect("store opens");
        $crate::common::run_replica_batch_destination_contract(store).await;
    }};
}

#[cfg(feature = "replication")]
macro_rules! replication_origin_batch_contract {
    ($open:expr) => {{
        let store = $open.await.expect("store opens");
        $crate::common::run_replication_origin_batch_contract(store).await;
    }};
}

#[cfg(feature = "replication")]
pub(crate) use replica_batch_destination_contract;
#[cfg(feature = "replication")]
pub(crate) use replication_origin_batch_contract;
