//! Two local SQLite stores model an origin and a replica. No network service is implied.
use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::*;
use std::{path::Path, sync::Arc, time::Duration};

type ExampleResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn event(index: u64) -> Result<NewEvent> {
    Ok(NewEvent {
        id: EventId::new(format!("event-{index}"))?,
        schema: SchemaRef {
            id: SchemaId::new("example.bytes")?,
            version: 1,
        },
        payload: Payload::copy_from_slice(&[9; 2048]),
    })
}

async fn open_origin(path: &Path) -> Result<Runtime<SqliteStore>> {
    Runtime::<SqliteStore>::open(
        SqliteOptions::new(path),
        RuntimeConfig {
            replication: RuntimeReplicationConfig {
                max_concurrent: 1,
                max_in_flight_bytes: 64 * 1024,
            },
            ..RuntimeConfig::default()
        },
    )
    .await
}

async fn close_origin(origin: &Runtime<SqliteStore>) -> Result<()> {
    let report = origin.shutdown(Duration::from_secs(5)).await?;
    assert!(report.closed);
    assert!(report.unresolved.is_empty());
    Ok(())
}

async fn run(directory: &Path) -> ExampleResult<u64> {
    std::fs::create_dir(directory)?;
    let origin_path = directory.join("origin.sqlite3");
    let replica_path = directory.join("replica.sqlite3");
    let origin = open_origin(&origin_path).await?;
    let destination = SqliteStore::open(SqliteOptions::new(&replica_path)).await?;
    let stream = OriginStream {
        origin: origin.origin_identity().await?,
        stream: origin.create_stream(&StreamId::new("events")?).await?,
    };
    let replica = ReplicaId::new("local-copy")?;
    origin
        .attach_replica(AttachReplica {
            operation_id: ReplicationOperationId::new("attach-local-copy")?,
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: ReplicaBatchDestinationStore::destination_epoch(&destination)
                .await?,
            max_backlog_bytes: 8192,
            max_backlog_age: Duration::from_secs(300),
            start: ReplicaStart::FromBeginning,
        })
        .await?;
    let mut accepted = 0;
    let mut full = false;
    for index in 0..16 {
        match origin.append(&stream.stream, event(index)?).await {
            Ok(receipt) => {
                accepted += 1;
                assert_eq!(receipt.record.cursor.offset, accepted);
            }
            Err(Error::ReplicaBacklogExceeded {
                replica: blocked, ..
            }) => {
                assert_eq!(blocked, replica);
                full = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    assert!(full && accepted > 0);
    // The application pauses source acquisition here. It keeps the rejected
    // event identity and bytes, then retries after the destination catches up.
    close_origin(&origin).await?;
    EventStore::close(&destination).await?;
    drop(origin);
    drop(destination);

    let origin = open_origin(&origin_path).await?;
    let destination = Arc::new(SqliteStore::open(SqliteOptions::new(&replica_path)).await?);
    assert_eq!(
        origin
            .replica_status(&replica, &stream)
            .await?
            .backlog_records as u64,
        accepted
    );
    for after in 0..=accepted {
        if after == accepted {
            let retry = origin.append(&stream.stream, event(accepted)?).await?;
            assert_eq!(retry.kind, AppendKind::Inserted);
            assert_eq!(retry.record.cursor.offset, accepted + 1);
        }
        let request = PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new(format!("prepare-{after}"))?,
            batch_id: BatchId(uuid::Uuid::new_v4().into_bytes()),
            replica: replica.clone(),
            stream: stream.clone(),
            expected_after: ReplicaPosition {
                stream: stream.clone(),
                offset: after,
            },
            limits: ReplicaBatchLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        };
        let ack = ReplicationOperationId::new(format!("ack-{after}"))?;
        let first = origin
            .replicate_once(destination.clone(), request.clone(), ack.clone())
            .await?;
        assert_eq!(
            origin
                .replicate_once(destination.clone(), request, ack)
                .await?,
            first
        );
    }
    let status = origin.replica_status(&replica, &stream).await?;
    assert_eq!(status.acknowledged.offset, accepted + 1);
    assert_eq!(status.backlog_records, 0);
    close_origin(&origin).await?;
    EventStore::close(destination.as_ref()).await?;
    drop(origin);
    drop(destination);

    let origin = open_origin(&origin_path).await?;
    let status = origin.replica_status(&replica, &stream).await?;
    assert_eq!(status.acknowledged.offset, accepted + 1);
    assert_eq!(status.backlog_records, 0);
    close_origin(&origin).await?;
    let destination = SqliteStore::open(SqliteOptions::new(&replica_path)).await?;
    for after in 0..=accepted {
        let page = destination
            .read_replica_after(
                &ReplicaPosition {
                    stream: stream.clone(),
                    offset: after,
                },
                ReplicaBatchLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await?;
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].cursor.offset, after + 1);
        assert_eq!(page.records[0].event, event(after)?);
    }
    EventStore::close(&destination).await?;
    Ok(accepted + 1)
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let mut args = std::env::args_os().skip(1);
    let directory = args
        .next()
        .ok_or("usage: replicated_recovery NEW_DIRECTORY")?;
    if args.next().is_some() {
        return Err("usage: replicated_recovery NEW_DIRECTORY".into());
    }
    let records = run(Path::new(&directory)).await?;
    println!("Verified {records} exact replica records after backlog rejection, restart, catch-up and retry.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bounded_backlog_restart_catchup_and_retry_keep_exact_history() {
        let directory =
            std::env::temp_dir().join(format!("replicated-recovery-{}", uuid::Uuid::new_v4()));
        assert!(run(&directory).await.unwrap() > 1);
        assert!(run(&directory).await.is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
