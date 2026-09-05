#![cfg(feature = "retention")]

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;

fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("retention.event").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(payload),
    }
}

fn subscription(start: StartPosition) -> SubscriptionOptions {
    SubscriptionOptions {
        start,
        page: PageLimits {
            max_records: 8,
            max_bytes: 1024 * 1024,
        },
        max_lag_records: 100,
        max_lag_duration: std::time::Duration::from_secs(2),
        catch_up_grace: std::time::Duration::ZERO,
    }
}

#[tokio::test]
async fn enabling_generations_preserves_legacy_and_qualifies_reused_ids() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("generations").unwrap())
        .await
        .unwrap();
    let legacy = store
        .append_atomic(&stream, event("same", b"legacy"))
        .await
        .unwrap();

    let enabled = store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-generations").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    assert_eq!(enabled.status.bounds.tail.offset, 1);
    assert_eq!(legacy.record.cursor.offset, 1);
    assert!(matches!(
        store.append_atomic(&stream, event("ordinary", b"x")).await,
        Err(Error::RetryPolicyRequired)
    ));
    assert!(matches!(
        store
            .lookup_event(&stream, &EventId::new("same").unwrap())
            .await,
        Err(Error::RetryPolicyRequired)
    ));

    let legacy_retry = store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::LEGACY,
                event: event("same", b"legacy"),
            },
        )
        .await
        .unwrap();
    assert_eq!(legacy_retry.kind, AppendKind::Deduplicated);
    assert_eq!(legacy_retry.record.cursor.offset, 1);
    assert!(matches!(
        store
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::LEGACY,
                    event: event("missing", b"never commit"),
                },
            )
            .await,
        Err(RetentionError::LegacyRetryNotFound { .. })
    ));

    let first = store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("same", b"generation-one"),
            },
        )
        .await
        .unwrap();
    assert_eq!(first.record.cursor.offset, 2);
    store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("advance-two").unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    let second_generation = RetryGeneration::new(2);
    let second = store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: second_generation,
                event: event("same", b"generation-two"),
            },
        )
        .await
        .unwrap();
    assert_eq!(second.record.cursor.offset, 3);
    assert_eq!(
        store
            .lookup_generated(
                &stream,
                RetryGeneration::FIRST,
                &EventId::new("same").unwrap(),
            )
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"generation-one"
    );
    assert_eq!(
        store
            .lookup_generated(&stream, second_generation, &EventId::new("same").unwrap(),)
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"generation-two"
    );
}

#[tokio::test]
async fn retry_expiry_and_floor_cleanup_are_logical_before_physical() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("cleanup").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("legacy", b"0"))
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-cleanup").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("one", b"1"),
            },
        )
        .await
        .unwrap();
    store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("advance-cleanup").unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("expire-cleanup").unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::new(2),
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .lookup_generated(
                &stream,
                RetryGeneration::FIRST,
                &EventId::new("one").unwrap(),
            )
            .await,
        Err(RetentionError::RetryGenerationExpired { .. })
    ));
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("floor-cleanup").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 2),
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .read_range(
                &stream,
                0,
                2,
                PageLimits {
                    max_records: 8,
                    max_bytes: 4096,
                },
            )
            .await,
        Err(Error::HistoryUnavailable { .. })
    ));
    let cleaned = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(cleaned.removed_event_rows, 2);
    assert_eq!(cleaned.removed_retry_rows, 2);
    assert!(!cleaned.remaining);
    assert_eq!(
        store
            .retention_status(&stream)
            .await
            .unwrap()
            .bounds
            .tail
            .offset,
        2
    );
}

#[tokio::test]
async fn operation_ids_return_exact_receipts_and_reject_reuse() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("operations").unwrap())
        .await
        .unwrap();
    let request = EnableRetryPolicy {
        operation_id: RetentionOperationId::new("same-operation").unwrap(),
        stream: stream.clone(),
    };
    let first = store.enable_retry_policy(request.clone()).await.unwrap();
    assert_eq!(store.enable_retry_policy(request).await.unwrap(), first);
    assert!(matches!(
        store
            .advance_retry_generation(AdvanceRetryGeneration {
                operation_id: RetentionOperationId::new("same-operation").unwrap(),
                stream,
                expected_current: RetryGeneration::FIRST,
            })
            .await,
        Err(RetentionError::OperationConflict { .. })
    ));
}

#[tokio::test]
async fn runtime_generated_append_wakes_live_subscription_and_honors_event_limit() {
    let mut config = RuntimeConfig::default();
    config.events.max_bytes = 512;
    let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
        .await
        .unwrap();
    let stream = runtime
        .create_stream(&StreamId::new("runtime-generated").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("runtime-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    let mut live = runtime
        .subscribe(&stream, subscription(StartPosition::Future))
        .await
        .unwrap();
    runtime
        .append_generated(
            &stream,
            GeneratedEvent {
                generation: RetryGeneration::FIRST,
                event: event("live", b"delivered"),
            },
        )
        .await
        .unwrap();
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(1), live.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(delivered.cursor.offset, 1);
    assert_eq!(delivered.event.payload.as_bytes(), b"delivered");

    let oversized = event("too-large", &[0; 512]);
    assert!(matches!(
        runtime
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::FIRST,
                    event: oversized,
                },
            )
            .await,
        Err(RetentionError::InvalidInput(_))
    ));
    runtime
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn floor_publication_ends_a_subscriber_with_buffered_removed_history() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let stream = runtime
        .create_stream(&StreamId::new("buffered-floor").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("buffered-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    for index in 1..=2 {
        runtime
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::FIRST,
                    event: event(&format!("buffered-{index}"), &[index]),
                },
            )
            .await
            .unwrap();
    }
    let mut subscriber = runtime
        .subscribe(&stream, subscription(StartPosition::Beginning))
        .await
        .unwrap();
    assert_eq!(subscriber.next().await.unwrap().unwrap().cursor.offset, 1);
    runtime
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("buffered-floor-two").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 2),
        })
        .await
        .unwrap();
    assert!(matches!(
        subscriber.next().await.unwrap(),
        Err(Error::SubscriberLagged { .. })
    ));
    assert!(subscriber.next().await.is_none());
    runtime
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test]
async fn floor_cleanup_retains_retry_until_a_later_expiry() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("floor-then-expire").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("legacy", b"kept"))
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("fte-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("fte-floor").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 1),
        })
        .await
        .unwrap();
    let first = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 1,
            max_retry_rows: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(first.removed_event_rows, 1);
    assert_eq!(
        store
            .lookup_generated(
                &stream,
                RetryGeneration::LEGACY,
                &EventId::new("legacy").unwrap(),
            )
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"kept"
    );
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("fte-expire").unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    let second = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 1,
            max_retry_rows: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(second.removed_retry_rows, 1);
    assert!(matches!(
        store
            .lookup_generated(
                &stream,
                RetryGeneration::LEGACY,
                &EventId::new("legacy").unwrap(),
            )
            .await,
        Err(RetentionError::RetryGenerationExpired { .. })
    ));
}

#[tokio::test]
async fn generated_retained_retry_cleanup_checks_bytes_before_each_mutation() {
    let options = MemoryStoreOptions {
        max_record_bytes: 256,
        retention: RetentionStoreConfig {
            receipts: RetryReceiptLimits {
                max_rows: 2,
                ..RetentionStoreConfig::default().receipts
            },
            ..RetentionStoreConfig::default()
        },
        ..MemoryStoreOptions::default()
    };
    let store = MemoryStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("cleanup-byte-stop").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("cbs-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    for id in ["a", "b"] {
        store
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::FIRST,
                    event: event(id, b"x"),
                },
            )
            .await
            .unwrap();
    }
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("cbs-floor").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), 2),
        })
        .await
        .unwrap();
    let first = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 1024,
        })
        .await
        .unwrap();
    assert_eq!(first.removed_event_rows, 2);
    store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("cbs-advance").unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("cbs-expire").unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::new(2),
        })
        .await
        .unwrap();
    let second = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 512,
        })
        .await
        .unwrap();
    assert_eq!(second.removed_retry_rows, 1);
    assert!(second.remaining);
    let third = store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 8,
            max_retry_rows: 8,
            max_bytes: 512,
        })
        .await
        .unwrap();
    assert_eq!(third.removed_retry_rows, 1);
    assert!(!third.remaining);
    for id in ["c", "d"] {
        store
            .append_generated(
                &stream,
                GeneratedEvent {
                    generation: RetryGeneration::new(2),
                    event: event(id, b"x"),
                },
            )
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn close_rejects_queued_retention_cleanup() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("closed-cleanup").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&stream, event("one", b"x"))
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("closed-enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("closed-floor").unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream, 1),
        })
        .await
        .unwrap();
    store.close().await.unwrap();
    assert_eq!(
        store
            .cleanup_retention(RetentionCleanupLimits {
                max_event_rows: 1,
                max_retry_rows: 1,
                max_bytes: 2 * 1024 * 1024,
            })
            .await
            .unwrap_err(),
        RetentionError::Closed
    );
}

#[tokio::test]
async fn lifecycle_cleanup_releases_retained_retry_capacity_in_bounded_turns() {
    let mut options = MemoryStoreOptions::default();
    options.retention.receipts.max_rows = 2;
    let store = MemoryStore::open(options).await.unwrap();
    let original = store
        .create_if_absent(&StreamId::new("retired-retries").unwrap())
        .await
        .unwrap();
    for id in ["a", "b"] {
        store
            .append_atomic(&original, event(id, b"x"))
            .await
            .unwrap();
    }
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("retired-enable").unwrap(),
            stream: original.clone(),
        })
        .await
        .unwrap();
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("retired-floor").unwrap(),
            stream: original.clone(),
            expected_floor: Cursor::new(original.clone(), 0),
            new_floor: Cursor::new(original.clone(), 2),
        })
        .await
        .unwrap();
    store
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 2,
            max_retry_rows: 2,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    let replacement = store
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("retired-reset").unwrap(),
            expected: original,
            action: LifecycleAction::Reset,
        })
        .await
        .unwrap()
        .replacement
        .unwrap();
    let first = store
        .cleanup_retired(CleanupLimits {
            max_records: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(first.removed_records, 1);
    assert!(first.remaining);
    let second = store
        .cleanup_retired(CleanupLimits {
            max_records: 1,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap();
    assert_eq!(second.removed_records, 1);
    assert!(!second.remaining);
    for id in ["c", "d"] {
        store
            .append_atomic(&replacement, event(id, b"x"))
            .await
            .unwrap();
    }
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("replacement-enable").unwrap(),
            stream: replacement,
        })
        .await
        .unwrap();
}
