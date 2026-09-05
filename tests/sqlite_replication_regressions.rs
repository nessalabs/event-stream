#![cfg(all(feature = "sqlite", feature = "replication"))]

use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::*;
use std::{path::PathBuf, time::Duration};

struct Fixture {
    directory: PathBuf,
    store: SqliteStore,
    attach: AttachReplica,
}
impl Fixture {
    async fn open() -> Self {
        let directory =
            std::env::temp_dir().join(format!("replication-regression-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let store = SqliteStore::open(SqliteOptions::new(directory.join("events.db")))
            .await
            .unwrap();
        let key = store
            .create_if_absent(&StreamId::new("origin").unwrap())
            .await
            .unwrap();
        let attach = AttachReplica {
            operation_id: ReplicationOperationId::new("attach").unwrap(),
            replica: ReplicaId::new("remote").unwrap(),
            stream: OriginStream {
                origin: store.origin_identity().await.unwrap(),
                stream: key,
            },
            destination_epoch: DestinationEpoch([1; 16]),
            max_backlog_bytes: 100_000,
            max_backlog_age: Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        };
        store.attach_replica(attach.clone()).await.unwrap();
        Self {
            directory,
            store,
            attach,
        }
    }
    async fn append(&self, id: &str) {
        self.store
            .append_atomic(
                &self.attach.stream.stream,
                NewEvent {
                    id: EventId::new(id).unwrap(),
                    schema: SchemaRef {
                        id: SchemaId::new("bytes").unwrap(),
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(id.as_bytes()),
                },
            )
            .await
            .unwrap();
    }
    fn prepare(&self, max_records: usize) -> PrepareReplicaBatch {
        PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new("prepare").unwrap(),
            batch_id: BatchId([2; 16]),
            replica: self.attach.replica.clone(),
            stream: self.attach.stream.clone(),
            expected_after: ReplicaPosition {
                stream: self.attach.stream.clone(),
                offset: 0,
            },
            limits: ReplicaBatchLimits {
                max_records,
                max_bytes: 4096,
            },
        }
    }
    async fn finish(self) {
        EventStore::close(&self.store).await.unwrap();
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}

#[tokio::test]
async fn sqlite_rejects_acknowledgement_past_exact_prepared_batch() {
    let f = Fixture::open().await;
    f.append("a").await;
    f.append("b").await;
    let prepared = f.store.prepare_replica_batch(f.prepare(1)).await.unwrap();
    let batch = prepared.batch.unwrap();
    assert_eq!(batch.records.last().unwrap().cursor.offset, 1);
    let result = f
        .store
        .acknowledge_replica_batch(AcknowledgeReplicaBatch {
            operation_id: ReplicationOperationId::new("forged-ack").unwrap(),
            replica: f.attach.replica.clone(),
            expected_after: batch.after,
            receipt: ReplicaReceipt {
                batch: batch.id,
                destination_epoch: batch.destination_epoch,
                committed_through: ReplicaPosition {
                    stream: f.attach.stream.clone(),
                    offset: 2,
                },
            },
        })
        .await;
    let current = f
        .store
        .replica_status(&f.attach.replica, &f.attach.stream)
        .await
        .unwrap();
    f.finish().await;
    assert!(
        matches!(result, Err(ReplicationError::InvalidReceipt(_))),
        "{result:?}"
    );
    assert_eq!(current.acknowledged.offset, 0);
    assert_eq!(current.pending_batch, Some(BatchId([2; 16])));
}

#[tokio::test]
async fn sqlite_pending_prepare_retry_does_not_include_later_appends() {
    let f = Fixture::open().await;
    f.append("a").await;
    let request = f.prepare(2);
    let original = f
        .store
        .prepare_replica_batch(request.clone())
        .await
        .unwrap();
    f.append("b").await;
    let retry = f.store.prepare_replica_batch(request).await.unwrap();
    f.finish().await;
    assert_eq!(
        retry, original,
        "a pending batch and its receipt must remain immutable"
    );
}

#[tokio::test]
async fn sqlite_attach_operation_rejects_changed_limits() {
    let f = Fixture::open().await;
    let mut changed = f.attach.clone();
    changed.max_backlog_bytes += 1;
    let result = f.store.attach_replica(changed).await;
    f.finish().await;
    assert!(
        matches!(result, Err(ReplicationError::InvalidInput(_))),
        "{result:?}"
    );
}

#[tokio::test]
async fn sqlite_pending_batch_identity_and_receipt_survive_reopen() {
    let mut f = Fixture::open().await;
    f.append("a").await;
    let request = f.prepare(2);
    let original = f
        .store
        .prepare_replica_batch(request.clone())
        .await
        .unwrap();
    f.append("b").await;
    let origin = f.store.origin_identity().await.unwrap();
    EventStore::close(&f.store).await.unwrap();
    f.store = SqliteStore::open(SqliteOptions::new(f.directory.join("events.db")))
        .await
        .unwrap();
    let reopened_origin = f.store.origin_identity().await.unwrap();
    let retry = f.store.prepare_replica_batch(request).await.unwrap();
    f.finish().await;
    assert_eq!(reopened_origin, origin);
    assert_eq!(
        retry, original,
        "reopen must retain the original pending batch and result"
    );
}

#[tokio::test]
async fn sqlite_missing_replication_metadata_is_corruption_not_a_new_origin() {
    let f = Fixture::open().await;
    f.append("a").await;
    EventStore::close(&f.store).await.unwrap();
    let path = f.directory.join("events.db");
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute("DELETE FROM replication_metadata", [])
            .unwrap();
    }
    let result = SqliteStore::open(SqliteOptions::new(&path)).await;
    let rejected = matches!(&result, Err(Error::StoreCorrupt(_)));
    let observed = match &result {
        Ok(_) => "unexpected successful open".to_owned(),
        Err(error) => format!("{error:?}"),
    };
    if let Ok(store) = result {
        EventStore::close(&store).await.unwrap();
    }
    std::fs::remove_dir_all(f.directory).unwrap();
    assert!(
        rejected,
        "missing metadata must fail without creating a replacement identity: {observed}"
    );
}
