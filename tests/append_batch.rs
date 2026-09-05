use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::sync::Arc;

fn request(stream: &StreamKey, id: &str, payload: &[u8]) -> AppendRequest {
    AppendRequest {
        stream: stream.clone(),
        event: NewEvent {
            id: EventId::new(id).unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("batch.bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(payload),
        },
    }
}
fn limits() -> AppendBatchLimits {
    AppendBatchLimits {
        max_records: 64,
        max_bytes: 1024 * 1024,
    }
}
fn key() -> StreamKey {
    StreamKey {
        id: StreamId::new("batch").unwrap(),
        incarnation: IncarnationId([1; 16]),
    }
}

#[test]
fn capacity_and_bytes_are_checked_before_store_use() {
    for max_records in [0, 65, usize::MAX] {
        assert!(matches!(
            AppendBatch::new(
                vec![request(&key(), "a", b"x")],
                AppendBatchLimits {
                    max_records,
                    ..limits()
                }
            ),
            Err(Error::InvalidConfig(_))
        ));
    }
    assert!(AppendBatch::new(vec![], limits()).is_err());
    assert!(AppendBatch::new(
        (0..65).map(|_| request(&key(), "a", b"x")).collect(),
        limits()
    )
    .is_err());
    let mut items = Vec::with_capacity(64);
    items.push(request(&key(), "a", b"x"));
    let charge = items.capacity() * std::mem::size_of::<AppendRequest>()
        + items[0].event.accounted_bytes()
        + key().id.as_str().len();
    let batch = AppendBatch::new(
        items,
        AppendBatchLimits {
            max_records: 1,
            max_bytes: charge,
        },
    )
    .unwrap();
    assert_eq!(batch.accounted_bytes(), charge);
    let mut items = Vec::with_capacity(64);
    items.push(request(&key(), "a", b"x"));
    assert!(matches!(
        AppendBatch::new(
            items,
            AppendBatchLimits {
                max_records: 1,
                max_bytes: charge - 1
            }
        ),
        Err(Error::CapacityExceeded)
    ));
}

#[test]
fn malformed_outcomes_cannot_pass_identity_validation() {
    let batch = AppendBatch::new(vec![request(&key(), "a", b"original")], limits()).unwrap();
    let record = Record {
        cursor: Cursor::new(key(), 1),
        event: batch.items()[0].event.clone(),
    };
    let valid = Ok(AppendReceipt {
        record: Arc::new(record.clone()),
        kind: AppendKind::Inserted,
    });
    batch
        .validate_results(std::slice::from_ref(&valid))
        .unwrap();
    assert!(batch.validate_results(&[]).is_err());
    assert!(batch.validate_results(&[valid.clone(), valid]).is_err());
    for change in 0..5 {
        let mut wrong = record.clone();
        match change {
            0 => wrong.cursor.offset = 0,
            1 => wrong.cursor.version = 0,
            2 => wrong.cursor.stream.incarnation = IncarnationId([9; 16]),
            3 => wrong.event.payload = Payload::copy_from_slice(b"wrong"),
            _ => wrong.event.id = EventId::new("other").unwrap(),
        }
        assert!(batch
            .validate_results(&[Ok(AppendReceipt {
                record: Arc::new(wrong),
                kind: AppendKind::Inserted
            })])
            .is_err());
    }
    for error in [
        Error::CommitUnknown {
            event_id: EventId::new("other").unwrap(),
        },
        Error::IdempotencyConflict {
            event_id: EventId::new("other").unwrap(),
        },
    ] {
        assert!(batch.validate_results(&[Err(error)]).is_err());
    }
    batch
        .validate_results(&[Err(Error::CommitUnknown {
            event_id: EventId::new("a").unwrap(),
        })])
        .unwrap();
}

async fn ordered_contract<S: EventStore>(store: &S) -> StreamKey {
    let stream = store
        .create_if_absent(&StreamId::new("batch-contract").unwrap())
        .await
        .unwrap();
    let other = store
        .create_if_absent(&StreamId::new("batch-other").unwrap())
        .await
        .unwrap();
    let batch = AppendBatch::new(
        vec![
            request(&stream, "a", b"first"),
            request(&stream, "a", b"first"),
            request(&stream, "a", b"conflict"),
            request(&other, "b", b"other"),
            request(&stream, "c", b"last"),
        ],
        limits(),
    )
    .unwrap();
    let results = store.append_batch(&batch).await;
    batch.validate_results(&results).unwrap();
    assert_eq!(results[0].as_ref().unwrap().kind, AppendKind::Inserted);
    assert_eq!(results[1].as_ref().unwrap().kind, AppendKind::Deduplicated);
    assert_eq!(
        results[0].as_ref().unwrap().record,
        results[1].as_ref().unwrap().record
    );
    assert!(matches!(results[2], Err(Error::IdempotencyConflict { .. })));
    assert_eq!(results[3].as_ref().unwrap().record.cursor.offset, 1);
    assert_eq!(results[4].as_ref().unwrap().record.cursor.offset, 2);
    let page = store
        .read_range(
            &stream,
            0,
            2,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records[0].event, batch.items()[0].event);
    assert_eq!(page.records[1].event, batch.items()[4].event);
    stream
}

#[tokio::test]
async fn memory_fallback_preserves_order_and_individual_errors() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    ordered_contract(&store).await;
    store.close().await.unwrap();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_batch_survives_reopen() {
    use event_stream::infrastructure::{SqliteOptions, SqliteStore};
    let dir = std::env::temp_dir().join(format!("event-stream-batch-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("store.db");
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    let stream = ordered_contract(&store).await;
    store.close().await.unwrap();
    let store = SqliteStore::open(SqliteOptions::new(&path)).await.unwrap();
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 2);
    for (id, payload, offset) in [("a", b"first".as_slice(), 1), ("c", b"last".as_slice(), 2)] {
        let record = store
            .lookup_event(&stream, &EventId::new(id).unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.event, request(&stream, id, payload).event);
        assert_eq!(record.cursor.offset, offset);
    }
    store.close().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn capacity_failure_does_not_erase_earlier_receipt_or_later_retry() {
    let store = MemoryStore::open(MemoryStoreOptions {
        max_history_records: 1,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("capacity-batch").unwrap())
        .await
        .unwrap();
    let batch = AppendBatch::new(
        vec![
            request(&stream, "a", b"x"),
            request(&stream, "b", b"y"),
            request(&stream, "a", b"x"),
        ],
        limits(),
    )
    .unwrap();
    let results = store.append_batch(&batch).await;
    batch.validate_results(&results).unwrap();
    assert_eq!(results[0].as_ref().unwrap().kind, AppendKind::Inserted);
    assert!(matches!(results[1], Err(Error::CapacityExceeded)));
    assert_eq!(results[2].as_ref().unwrap().kind, AppendKind::Deduplicated);
    assert_eq!(
        results[0].as_ref().unwrap().record,
        results[2].as_ref().unwrap().record
    );
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 1);
    store.close().await.unwrap();
}
