use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
struct OrderProjection {
    created: bool,
    paid: bool,
    checkpoint: Cursor,
}

impl OrderProjection {
    fn new(stream: StreamKey) -> Self {
        Self {
            created: false,
            paid: false,
            checkpoint: Cursor::new(stream, 0),
        }
    }

    /// Example application transaction: publish the effect and checkpoint together.
    fn apply(&mut self, record: &Record) -> std::result::Result<(), &'static str> {
        let mut next = self.clone();
        match record.event.payload.as_bytes() {
            b"created" if !next.created => next.created = true,
            b"paid" if next.created && !next.paid => next.paid = true,
            _ => return Err("unsupported or out-of-order order event"),
        }
        next.checkpoint = record.cursor.clone();
        *self = next;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (live, replayed, failed_cursor) = run_example().await?;
    println!(
        "live and replay agree at offset {}; failed apply kept offset {}",
        live.checkpoint.offset, failed_cursor.offset
    );
    assert_eq!(live, replayed);
    Ok(())
}

async fn run_example() -> Result<(OrderProjection, OrderProjection, Cursor)> {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await?;
    let stream = runtime.create_stream(&StreamId::new("orders")?).await?;
    let schema = SchemaRef {
        id: SchemaId::new("example.order")?,
        version: 1,
    };
    let mut live = OrderProjection::new(stream.clone());
    let mut first_connection = runtime
        .subscribe(&stream, options(StartPosition::Future))
        .await?;
    runtime
        .append(
            &stream,
            NewEvent {
                id: EventId::new("one")?,
                schema: schema.clone(),
                payload: Payload::copy_from_slice(b"created"),
            },
        )
        .await?;
    let first = first_connection
        .next()
        .await
        .transpose()?
        .expect("first committed event");
    live.apply(&first).expect("valid created event");
    drop(first_connection);

    let mut reconnected = runtime
        .subscribe(
            &stream,
            options(StartPosition::After(live.checkpoint.clone())),
        )
        .await?;
    runtime
        .append(
            &stream,
            NewEvent {
                id: EventId::new("two")?,
                schema: schema.clone(),
                payload: Payload::copy_from_slice(b"paid"),
            },
        )
        .await?;
    let second = reconnected
        .next()
        .await
        .transpose()?
        .expect("second committed event");
    live.apply(&second).expect("valid paid event");
    assert_eq!((first.cursor.offset, second.cursor.offset), (1, 2));

    runtime
        .append(
            &stream,
            NewEvent {
                id: EventId::new("bad")?,
                schema,
                payload: Payload::copy_from_slice(b"not-an-order-transition"),
            },
        )
        .await?;
    let bad = reconnected
        .next()
        .await
        .transpose()?
        .expect("malformed application event remains visible");
    let before_failure = live.clone();
    assert!(live.apply(&bad).is_err());
    assert_eq!(live, before_failure);
    let failed_cursor = live.checkpoint.clone();

    let mut replayed = OrderProjection::new(stream.clone());
    let mut replay = runtime
        .subscribe(&stream, options(StartPosition::Beginning))
        .await?;
    for _ in 0..3 {
        let record = replay
            .next()
            .await
            .transpose()?
            .expect("captured replay record");
        if replayed.apply(&record).is_err() {
            break;
        }
    }
    assert_eq!(replayed, live);
    assert_eq!(replayed.checkpoint.offset, 2);
    runtime.shutdown(Duration::from_secs(1)).await?;
    Ok((live, replayed, failed_cursor))
}

fn options(start: StartPosition) -> SubscriptionOptions {
    SubscriptionOptions {
        start,
        page: PageLimits {
            max_records: 16,
            max_bytes: 1024 * 1024,
        },
        max_lag_records: 1024,
        max_lag_duration: Duration::from_secs(30),
        catch_up_grace: Duration::from_secs(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_reconnect_matches_replay_and_failed_apply_keeps_checkpoint() {
        let (live, replayed, failed_cursor) = run_example().await.unwrap();
        assert_eq!(live, replayed);
        assert_eq!(failed_cursor.offset, 2);
    }
}
