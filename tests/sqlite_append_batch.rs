#![cfg(feature = "sqlite")]
use event_stream::infrastructure::{SqliteFailureInjection, SqliteOptions, SqliteStore};
use event_stream::*;
use std::{path::PathBuf, sync::Arc, time::Duration};

struct Database(PathBuf);
impl Database {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("event-stream-group-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
    }
    fn path(&self) -> PathBuf {
        self.0.join("events.db")
    }
}
impl Drop for Database {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn event(id: &str, bytes: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("group.bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(bytes),
    }
}
fn batch(stream: &StreamKey) -> AppendBatch {
    AppendBatch::new(
        vec![
            AppendRequest {
                stream: stream.clone(),
                event: event("new-a", b"first"),
            },
            AppendRequest {
                stream: stream.clone(),
                event: event("new-a", b"first"),
            },
            AppendRequest {
                stream: stream.clone(),
                event: event("new-b", b"second"),
            },
        ],
        AppendBatchLimits {
            max_records: 3,
            max_bytes: 1024 * 1024,
        },
    )
    .unwrap()
}
async fn verify(store: &SqliteStore, stream: &StreamKey) {
    let page = store
        .read_range(
            stream,
            0,
            2,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].event, event("new-a", b"first"));
    assert_eq!(page.records[1].event, event("new-b", b"second"));
    assert_eq!(page.records[0].cursor.offset, 1);
    assert_eq!(page.records[1].cursor.offset, 2);
}

#[tokio::test]
async fn grouped_rollback_never_releases_insert_or_same_group_dedup() {
    for fault in [
        SqliteFailureInjection::BeforeRecordInsert,
        SqliteFailureInjection::AfterRecordInsert,
        SqliteFailureInjection::BeforeCommit,
    ] {
        let db = Database::new();
        let mut options = SqliteOptions::new(db.path());
        options.failure_injection = Some(fault);
        let store = SqliteStore::open(options).await.unwrap();
        let stream = store
            .create_if_absent(&StreamId::new("group").unwrap())
            .await
            .unwrap();
        let batch = batch(&stream);
        let outcomes = store.append_batch(&batch).await;
        batch.validate_results(&outcomes).unwrap();
        assert!(
            outcomes.iter().all(Result::is_err),
            "{fault:?}: {outcomes:?}"
        );
        assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 0);
        assert!(store
            .lookup_event(&stream, &EventId::new("new-a").unwrap())
            .await
            .unwrap()
            .is_none());
        let outcomes = store.append_batch(&batch).await;
        batch.validate_results(&outcomes).unwrap();
        assert!(outcomes.iter().all(Result::is_ok));
        assert_eq!(outcomes[1].as_ref().unwrap().kind, AppendKind::Deduplicated);
        verify(&store, &stream).await;
        store.close().await.unwrap();
        let reopened = SqliteStore::open(SqliteOptions::new(db.path()))
            .await
            .unwrap();
        verify(&reopened, &stream).await;
        reopened.close().await.unwrap();
    }
}

#[tokio::test]
async fn lost_group_reply_reconciles_all_inserted_identities() {
    let db = Database::new();
    let mut options = SqliteOptions::new(db.path());
    options.failure_injection = Some(SqliteFailureInjection::AfterCommitAcknowledgementLost);
    let store = SqliteStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("group").unwrap())
        .await
        .unwrap();
    let batch = batch(&stream);
    let outcomes = store.append_batch(&batch).await;
    batch.validate_results(&outcomes).unwrap();
    assert_eq!(outcomes[0].as_ref().unwrap().kind, AppendKind::Inserted);
    assert_eq!(outcomes[1].as_ref().unwrap().kind, AppendKind::Deduplicated);
    assert_eq!(outcomes[2].as_ref().unwrap().kind, AppendKind::Inserted);
    verify(&store, &stream).await;
    store.close().await.unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(db.path()))
        .await
        .unwrap();
    let retries = reopened.append_batch(&batch).await;
    assert!(retries
        .iter()
        .all(|r| r.as_ref().unwrap().kind == AppendKind::Deduplicated));
    verify(&reopened, &stream).await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn adapter_rejects_oversized_group_without_writing() {
    let db = Database::new();
    let mut options = SqliteOptions::new(db.path());
    options.max_record_bytes = 1024;
    let store = SqliteStore::open(options).await.unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("group").unwrap())
        .await
        .unwrap();
    let batch = AppendBatch::new(
        (0..8)
            .map(|i| AppendRequest {
                stream: stream.clone(),
                event: event(&format!("{i}"), &[0; 128]),
            })
            .collect(),
        AppendBatchLimits {
            max_records: 8,
            max_bytes: 8192,
        },
    )
    .unwrap();
    assert!(batch.accounted_bytes() > 1024);
    let outcomes = store.append_batch(&batch).await;
    batch.validate_results(&outcomes).unwrap();
    assert!(outcomes.iter().all(Result::is_err));
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 0);
    store.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_caller_does_not_cancel_accepted_group_or_early_close() {
    let db = Database::new();
    let mut options = SqliteOptions::new(db.path());
    options.failure_injection = Some(SqliteFailureInjection::PauseBeforeCommit(
        Duration::from_millis(500),
    ));
    let store = Arc::new(SqliteStore::open(options).await.unwrap());
    let stream = store
        .create_if_absent(&StreamId::new("group").unwrap())
        .await
        .unwrap();
    let worker_store = store.clone();
    let batch = batch(&stream);
    let caller = tokio::spawn(async move { worker_store.append_batch(&batch).await });
    // The journal proves the worker entered a write transaction before cancellation.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !db.path().with_extension("db-journal").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    caller.abort();
    let _ = caller.await;
    tokio::time::timeout(Duration::from_secs(5), store.close())
        .await
        .unwrap()
        .unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(db.path()))
        .await
        .unwrap();
    verify(&reopened, &stream).await;
    reopened.close().await.unwrap();
}

#[cfg(feature = "replication")]
#[tokio::test]
async fn group_observes_earlier_replica_backlog_without_discarding_other_stream() {
    let db = Database::new();
    let store = SqliteStore::open(SqliteOptions::new(db.path()))
        .await
        .unwrap();
    let stream = store
        .create_if_absent(&StreamId::new("protected").unwrap())
        .await
        .unwrap();
    let other = store
        .create_if_absent(&StreamId::new("other").unwrap())
        .await
        .unwrap();
    let origin = OriginStream {
        origin: store.origin_identity().await.unwrap(),
        stream: stream.clone(),
    };
    let replica = ReplicaId::new("replica").unwrap();
    store
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: replica.clone(),
            stream: origin.clone(),
            destination_epoch: DestinationEpoch([7; 16]),
            max_backlog_bytes: 512,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        })
        .await
        .unwrap();
    let batch = AppendBatch::new(
        vec![
            AppendRequest {
                stream: stream.clone(),
                event: event("a", b"x"),
            },
            AppendRequest {
                stream: stream.clone(),
                event: event("b", b"y"),
            },
            AppendRequest {
                stream: other.clone(),
                event: event("c", b"z"),
            },
        ],
        AppendBatchLimits {
            max_records: 3,
            max_bytes: 8192,
        },
    )
    .unwrap();
    let outcomes = store.append_batch(&batch).await;
    batch.validate_results(&outcomes).unwrap();
    assert!(outcomes[0].is_ok());
    assert!(matches!(
        outcomes[1],
        Err(Error::ReplicaBacklogExceeded { .. })
    ));
    assert!(outcomes[2].is_ok());
    assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 1);
    assert_eq!(store.bounds(&other).await.unwrap().tail.offset, 1);
    let status = store.replica_status(&replica, &origin).await.unwrap();
    assert_eq!(status.backlog_records, 1);
    assert!(status.backlog_bytes <= 512);
    store.close().await.unwrap();
    let reopened = SqliteStore::open(SqliteOptions::new(db.path()))
        .await
        .unwrap();
    assert_eq!(
        reopened.replica_status(&replica, &origin).await.unwrap(),
        status
    );
    assert_eq!(
        reopened
            .lookup_event(&other, &EventId::new("c").unwrap())
            .await
            .unwrap()
            .unwrap()
            .event,
        event("c", b"z")
    );
    assert!(reopened
        .lookup_event(&stream, &EventId::new("b").unwrap())
        .await
        .unwrap()
        .is_none());
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn preexisting_retry_is_distinct_from_new_records_during_group_reconciliation() {
    for fault in [
        SqliteFailureInjection::BeforeCommit,
        SqliteFailureInjection::AfterCommitAcknowledgementLost,
    ] {
        let db = Database::new();
        let initial = SqliteStore::open(SqliteOptions::new(db.path()))
            .await
            .unwrap();
        let stream = initial
            .create_if_absent(&StreamId::new("group").unwrap())
            .await
            .unwrap();
        let baseline = initial
            .append_atomic(&stream, event("old", b"baseline"))
            .await
            .unwrap();
        initial.close().await.unwrap();
        let mut options = SqliteOptions::new(db.path());
        options.failure_injection = Some(fault);
        let store = SqliteStore::open(options).await.unwrap();
        let batch = AppendBatch::new(
            vec![
                AppendRequest {
                    stream: stream.clone(),
                    event: event("old", b"baseline"),
                },
                AppendRequest {
                    stream: stream.clone(),
                    event: event("new", b"new"),
                },
                AppendRequest {
                    stream: stream.clone(),
                    event: event("new", b"new"),
                },
            ],
            AppendBatchLimits {
                max_records: 3,
                max_bytes: 8192,
            },
        )
        .unwrap();
        let outcomes = store.append_batch(&batch).await;
        batch.validate_results(&outcomes).unwrap();
        if matches!(fault, SqliteFailureInjection::BeforeCommit) {
            assert!(outcomes.iter().all(Result::is_err));
            assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 1);
            assert!(store
                .lookup_event(&stream, &EventId::new("new").unwrap())
                .await
                .unwrap()
                .is_none());
        } else {
            assert!(outcomes.iter().all(Result::is_ok));
            assert_eq!(outcomes[0].as_ref().unwrap().kind, AppendKind::Deduplicated);
            assert_eq!(outcomes[1].as_ref().unwrap().kind, AppendKind::Inserted);
            assert_eq!(outcomes[2].as_ref().unwrap().kind, AppendKind::Deduplicated);
            assert_eq!(store.bounds(&stream).await.unwrap().tail.offset, 2);
        }
        assert_eq!(
            store
                .lookup_event(&stream, &EventId::new("old").unwrap())
                .await
                .unwrap()
                .unwrap(),
            baseline.record
        );
        store.close().await.unwrap();
    }
}
