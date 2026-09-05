#![cfg(feature = "snapshots")]

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use sha2::{Digest, Sha256};
use std::time::Duration;

type ExampleResultValue<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ApplicationState {
    transcript: Vec<String>,
    workflow_step: u8,
}

impl ApplicationState {
    fn apply(&mut self, record: &Record) -> std::result::Result<(), &'static str> {
        let transition = std::str::from_utf8(record.event.payload.as_bytes())
            .map_err(|_| "event payload is not UTF-8")?;
        let expected = match self.workflow_step {
            0 => "opened",
            1 => "approved",
            2 => "completed",
            _ => return Err("workflow is already complete"),
        };
        if transition != expected {
            return Err("workflow transition is out of order");
        }
        self.transcript.push(transition.to_owned());
        self.workflow_step += 1;
        Ok(())
    }

    // Application-owned example format. The library treats these bytes as opaque.
    fn encode(&self) -> Vec<u8> {
        let mut bytes = self.transcript.join("\n").into_bytes();
        if !bytes.is_empty() {
            bytes.push(b'\n');
        }
        bytes
    }

    fn decode(schema: &SchemaRef, bytes: &[u8]) -> std::result::Result<Self, &'static str> {
        if schema.id.as_str() != "example.application-state" || schema.version != 1 {
            return Err("unsupported application snapshot schema");
        }
        let text = std::str::from_utf8(bytes).map_err(|_| "snapshot is not UTF-8")?;
        let mut state = Self::default();
        for transition in text.lines() {
            let record = example_record(transition, state.workflow_step as u64 + 1);
            state.apply(&record)?;
        }
        Ok(state)
    }
}

#[derive(Debug)]
struct ExampleResult {
    recovered: ApplicationState,
    replayed: ApplicationState,
    snapshot_bytes: usize,
    suffix_offsets: Vec<u64>,
}

#[tokio::main]
async fn main() -> ExampleResultValue<()> {
    let result = run_example().await?;
    assert_eq!(result.recovered, result.replayed);
    println!(
        "restored {} snapshot bytes, replayed suffix {:?}, final transcript {:?}, workflow step {}",
        result.snapshot_bytes,
        result.suffix_offsets,
        result.recovered.transcript,
        result.recovered.workflow_step
    );
    Ok(())
}

async fn run_example() -> ExampleResultValue<ExampleResult> {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await?;
    let stream = runtime
        .create_stream(&StreamId::new("snapshot-order")?)
        .await?;
    let schema = SchemaRef {
        id: SchemaId::new("example.workflow-event")?,
        version: 1,
    };

    let mut state_at_snapshot = ApplicationState::default();
    for (index, transition) in ["opened", "approved"].into_iter().enumerate() {
        let receipt = runtime
            .append(
                &stream,
                NewEvent {
                    id: EventId::new(format!("transition-{}", index + 1))?,
                    schema: schema.clone(),
                    payload: Payload::copy_from_slice(transition.as_bytes()),
                },
            )
            .await?;
        state_at_snapshot
            .apply(&receipt.record)
            .expect("fixture transitions are valid");
    }

    let snapshot_bytes = state_at_snapshot.encode();
    let snapshot_id = SnapshotId::from_bytes([5; 16]);
    let descriptor = SnapshotDescriptor {
        id: snapshot_id,
        covered: Cursor::new(stream.clone(), 2),
        schema: SchemaRef {
            id: SchemaId::new("example.application-state")?,
            version: 1,
        },
        content_bytes: snapshot_bytes.len() as u64,
        digest: SnapshotDigest::from_bytes(Sha256::digest(&snapshot_bytes).into()),
    };
    runtime.begin_snapshot(descriptor.clone()).await?;
    let mut offset = 0u64;
    for chunk in snapshot_bytes.chunks(3) {
        runtime
            .put_snapshot_chunk(
                snapshot_id,
                SnapshotChunk {
                    offset,
                    bytes: Payload::copy_from_slice(chunk),
                },
            )
            .await?;
        offset += chunk.len() as u64;
    }
    let published = runtime
        .verify_and_publish_snapshot(
            snapshot_id,
            VerificationLimits {
                max_chunks: 2,
                max_bytes: 4,
            },
        )
        .await?;
    assert_eq!(published, descriptor);

    runtime
        .append(
            &stream,
            NewEvent {
                id: EventId::new("transition-3")?,
                schema,
                payload: Payload::copy_from_slice(b"completed"),
            },
        )
        .await?;

    let recovery = runtime
        .acquire_recovery(snapshot_id, Duration::from_secs(30))
        .await?;
    assert_eq!(recovery.snapshot, descriptor);
    assert_eq!(recovery.through.offset, 3);

    let mut restored_bytes = Vec::new();
    let mut byte_offset = 0u64;
    loop {
        let page = runtime
            .read_snapshot_chunk(recovery.lease, byte_offset, 4)
            .await?;
        assert_eq!(page.offset, byte_offset);
        restored_bytes.extend_from_slice(page.bytes.as_bytes());
        byte_offset = page.next_offset;
        if page.complete {
            break;
        }
    }
    let mut recovered = ApplicationState::decode(&recovery.snapshot.schema, &restored_bytes)
        .expect("the example recognizes its snapshot schema and bytes");
    let mut suffix_offsets = Vec::new();
    let mut after = recovery.snapshot.covered.offset;
    loop {
        let page = runtime
            .read_recovery_page(recovery.lease, after, page_limits())
            .await?;
        for record in &page.records {
            recovered
                .apply(record)
                .expect("the protected suffix is valid");
            suffix_offsets.push(record.cursor.offset);
        }
        after = page.next_after.offset;
        if page.complete {
            break;
        }
    }
    runtime.release_recovery(recovery.lease).await?;

    let mut replayed = ApplicationState::default();
    let mut after = Cursor::new(stream, 0);
    loop {
        let page = runtime.read_after(&after, page_limits(), None).await?;
        for record in &page.records {
            replayed
                .apply(record)
                .expect("the complete event history is valid");
        }
        after = page.next_after;
        if page.complete {
            break;
        }
    }
    runtime.shutdown(Duration::from_secs(1)).await?;

    Ok(ExampleResult {
        recovered,
        replayed,
        snapshot_bytes: restored_bytes.len(),
        suffix_offsets,
    })
}

fn page_limits() -> PageLimits {
    PageLimits {
        max_records: 2,
        max_bytes: 1024 * 1024,
    }
}

fn example_record(transition: &str, offset: u64) -> Record {
    Record {
        cursor: Cursor::new(
            StreamKey {
                id: StreamId::new("snapshot-order").unwrap(),
                incarnation: IncarnationId([0; 16]),
            },
            offset,
        ),
        event: NewEvent {
            id: EventId::new(format!("decoded-{offset}")).unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("example.workflow-event").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(transition.as_bytes()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_plus_protected_suffix_matches_complete_replay() {
        let result = run_example().await.unwrap();
        assert_eq!(result.recovered, result.replayed);
        assert_eq!(
            result.recovered.transcript,
            ["opened", "approved", "completed"]
        );
        assert_eq!(result.recovered.workflow_step, 3);
        assert_eq!(result.suffix_offsets, [3]);
        assert!(result.snapshot_bytes > 0);
    }

    #[test]
    fn application_rejects_an_unknown_snapshot_schema_before_decoding() {
        let schema = SchemaRef {
            id: SchemaId::new("example.application-state").unwrap(),
            version: 2,
        };
        assert_eq!(
            ApplicationState::decode(&schema, b"opened\n").unwrap_err(),
            "unsupported application snapshot schema"
        );
    }
}
