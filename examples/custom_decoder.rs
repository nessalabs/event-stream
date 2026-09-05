#[cfg(feature = "codec")]
use event_stream::{
    infrastructure::{MemoryStore, MemoryStoreOptions},
    ingestion::*,
    *,
};
#[cfg(feature = "codec")]
use std::{sync::Arc, time::Duration};

#[cfg(feature = "codec")]
struct FixedFourBytes {
    partial: Vec<u8>,
    frame_start: u64,
    position: u64,
}

#[cfg(feature = "codec")]
impl IncrementalDecoder for FixedFourBytes {
    type Item = Vec<u8>;
    fn name(&self) -> &'static str {
        "example-fixed-four-bytes"
    }
    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 4,
            max_retained_bytes: 4,
        }
    }
    fn decode(&mut self, input: &[u8], budget: DecodeBudget) -> DecodeStep<Vec<u8>> {
        let take = (4 - self.partial.len())
            .min(input.len())
            .min(budget.max_work_units);
        self.partial.extend_from_slice(&input[..take]);
        self.position += take as u64;
        if self.partial.len() == 4 && budget.max_items > 0 && budget.max_bytes >= 4 {
            let item = std::mem::take(&mut self.partial);
            let source_byte = self.frame_start;
            self.frame_start = self.position;
            DecodeStep {
                consumed_bytes: take,
                work_units: take,
                items: vec![DecodedItem {
                    item,
                    accounted_bytes: 4,
                    source_byte,
                }],
                state: if take < input.len() {
                    DecodeState::OutputReady
                } else {
                    DecodeState::NeedInput
                },
            }
        } else {
            DecodeStep {
                consumed_bytes: take,
                work_units: take,
                items: vec![],
                state: DecodeState::NeedInput,
            }
        }
    }
    fn finish(&mut self, _: DecodeBudget) -> DecodeStep<Vec<u8>> {
        if self.partial.is_empty() {
            DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: vec![],
                state: DecodeState::Finished,
            }
        } else {
            DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: vec![],
                state: DecodeState::Failed(DecodeFailure {
                    class: "truncated_fixed_record".into(),
                    source_byte: Some(self.position),
                }),
            }
        }
    }
}

#[cfg(feature = "codec")]
#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut config = RuntimeConfig::default();
    config.events.max_bytes = 4096;
    let runtime = Arc::new(
        Runtime::<MemoryStore>::open(
            MemoryStoreOptions {
                max_record_bytes: 4096,
                ..MemoryStoreOptions::default()
            },
            config,
        )
        .await?,
    );
    let stream = runtime
        .create_stream(&StreamId::new("decoded-binary-frames")?)
        .await?;
    let service = IngestionService::new(runtime.clone(), IngestionConfig::default())?;
    let decoder = FixedFourBytes {
        partial: Vec::new(),
        frame_start: 0,
        position: 0,
    };
    let mut session = service.try_start(
        stream.clone(),
        decoder,
        |frame: Vec<u8>, position: DecodedPosition| {
            Ok(NewEvent {
                id: EventId::new(format!(
                    "input-{}-{}",
                    position.source_byte, position.item_index
                ))
                .map_err(|e| e.to_string())?,
                schema: SchemaRef {
                    id: SchemaId::new("example.binary-frame").map_err(|e| e.to_string())?,
                    version: 1,
                },
                payload: Payload::copy_from_slice(&frame),
            })
        },
    )?;
    // The core transports opaque bytes, including NUL and invalid UTF-8.
    // A chunk boundary need not match the application's four-byte frame.
    session.push_chunk(b"\x00\xffa").await?;
    session.push_chunk(b"bcdef").await?;
    session.finish().await?;
    drop(session);
    let page = runtime
        .read_after(
            &Cursor::new(stream, 0),
            PageLimits {
                max_records: 3,
                max_bytes: 4096,
            },
            None,
        )
        .await?;
    assert!(page.complete);
    assert_eq!(page.records.len(), 2);
    for (record, (offset, id, payload)) in page.records.iter().zip([
        (1, "input-0-0", &b"\x00\xffab"[..]),
        (2, "input-4-1", &b"cdef"[..]),
    ]) {
        assert_eq!(record.cursor.offset, offset);
        assert_eq!(record.event.id.as_str(), id);
        assert_eq!(record.event.schema.id.as_str(), "example.binary-frame");
        assert_eq!(record.event.schema.version, 1);
        assert_eq!(record.event.payload.as_bytes(), payload);
    }
    assert_eq!(page.next_after.offset, 2);
    runtime.shutdown(Duration::from_secs(1)).await?;
    println!("Verified two exact binary frames, stable IDs, schema, and committed order.");
    Ok(())
}

#[cfg(not(feature = "codec"))]
fn main() {}
