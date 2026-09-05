#![cfg(feature = "source-journal")]

mod common;

use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::ingestion::*;
use event_stream::*;
use tokio::sync::Semaphore;

fn source() -> SourceKey {
    SourceKey {
        id: SourceId::new("input").unwrap(),
        incarnation: SourceIncarnation([7; 16]),
    }
}

fn parser() -> ParserRef {
    ParserRef {
        id: ParserId::new("newline").unwrap(),
        version: 1,
    }
}

fn event(id: &str, payload: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("journal.output").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(payload),
    }
}

fn newline() -> NewlineFramer {
    NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 1024,
        emit_empty_frames: false,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .unwrap()
}

struct BufferedOutputDecoder(u8);

impl IncrementalDecoder for BufferedOutputDecoder {
    type Item = Vec<u8>;

    fn name(&self) -> &'static str {
        "buffered-output-test"
    }

    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 16,
            max_retained_bytes: 16,
        }
    }

    fn decode(&mut self, input: &[u8], _budget: DecodeBudget) -> DecodeStep<Self::Item> {
        match self.0 {
            0 => {
                self.0 = 1;
                DecodeStep {
                    consumed_bytes: input.len(),
                    work_units: input.len(),
                    items: vec![DecodedItem {
                        item: b"first".to_vec(),
                        accounted_bytes: 5,
                        source_byte: 0,
                    }],
                    state: DecodeState::OutputReady,
                }
            }
            1 => {
                self.0 = 2;
                DecodeStep {
                    consumed_bytes: 0,
                    work_units: 1,
                    items: vec![DecodedItem {
                        item: b"second".to_vec(),
                        accounted_bytes: 6,
                        source_byte: 0,
                    }],
                    state: DecodeState::NeedInput,
                }
            }
            _ => DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: Vec::new(),
                state: DecodeState::NeedInput,
            },
        }
    }

    fn finish(&mut self, budget: DecodeBudget) -> DecodeStep<Self::Item> {
        self.decode(&[], budget)
    }
}

impl CheckpointDecoder for BufferedOutputDecoder {
    fn parser(&self) -> ParserRef {
        ParserRef {
            id: ParserId::new("buffered-output-test").unwrap(),
            version: 1,
        }
    }

    fn checkpoint_state(&self, max_bytes: usize) -> std::result::Result<Payload, CheckpointError> {
        if max_bytes == 0 {
            return Err(CheckpointError::CapacityExceeded);
        }
        Ok(Payload::copy_from_slice(&[self.0]))
    }

    fn restore_state(
        &mut self,
        parser: &ParserRef,
        state: &[u8],
        _max_work_units: usize,
    ) -> std::result::Result<(), CheckpointError> {
        if parser != &self.parser() {
            return Err(CheckpointError::InvalidParser);
        }
        if state.len() != 1 || state[0] > 2 {
            return Err(CheckpointError::InvalidState("invalid test state".into()));
        }
        self.0 = state[0];
        Ok(())
    }
}

async fn fixture() -> (MemoryStore, StreamKey, SourceBinding) {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let output = store
        .create_if_absent(&StreamId::new("output").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable-output").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let binding = SourceBinding {
        source: source(),
        parser: parser(),
        output_stream: output.clone(),
    };
    (store, output, binding)
}

#[tokio::test]
async fn begin_capture_read_checkpoint_and_explicit_receipt_expiry() {
    let (store, _, binding) = fixture().await;
    let begin = BeginSource {
        operation_id: JournalOperationId::new("begin-input").unwrap(),
        binding: binding.clone(),
    };
    let first = store.begin_source(begin.clone()).await.unwrap();
    assert_eq!(store.begin_source(begin).await.unwrap(), first);
    let segment = RawSegment {
        start: SourcePosition {
            source: binding.source.clone(),
            offset: 0,
        },
        bytes: Payload::copy_from_slice(b"abc"),
    };
    let captured = store.capture_segment(segment.clone()).await.unwrap();
    assert_eq!(store.capture_segment(segment).await.unwrap(), captured);
    let page = store
        .read_captured(
            &binding.source,
            1,
            RawPageLimits {
                max_segments: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.bytes.as_bytes(), b"b");
    assert_eq!(page.next_offset, 2);
    assert!(!page.complete);
    let checkpoint = ParserCheckpoint {
        source: SourcePosition {
            source: binding.source.clone(),
            offset: 3,
        },
        parser: binding.parser.clone(),
        state: Payload::copy_from_slice(b"state"),
        next_item_index: 0,
        output_stream: binding.output_stream.clone(),
        committed_output: None,
    };
    assert_eq!(
        store
            .publish_parser_checkpoint(checkpoint.clone())
            .await
            .unwrap()
            .checkpoint,
        checkpoint
    );
    let expiry = AdvanceCaptureReceiptFloor {
        operation_id: JournalOperationId::new("ack-input-through-3").unwrap(),
        source: binding.source.clone(),
        expected_floor: 0,
        new_floor: 3,
    };
    let receipt = store
        .advance_capture_receipt_floor(expiry.clone())
        .await
        .unwrap();
    assert_eq!(
        store.advance_capture_receipt_floor(expiry).await.unwrap(),
        receipt
    );
    let cleanup = store
        .cleanup_captured(SourceJournalStoreConfig::default().cleanup)
        .await
        .unwrap();
    assert_eq!(cleanup.removed_segment_rows, 1);
    assert_eq!(cleanup.removed_receipt_rows, 1);
    assert!(matches!(
        store
            .capture_segment(RawSegment {
                start: SourcePosition {
                    source: binding.source,
                    offset: 0,
                },
                bytes: Payload::copy_from_slice(b"abc"),
            })
            .await,
        Err(JournalError::CaptureReceiptExpired { floor: 3 })
    ));
}

#[tokio::test]
async fn multi_output_frame_is_atomic_with_markers_and_pins_its_generation() {
    let (store, output, binding) = fixture().await;
    store
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin-multi").unwrap(),
            binding: binding.clone(),
        })
        .await
        .unwrap();
    store
        .capture_segment(RawSegment {
            start: SourcePosition {
                source: binding.source.clone(),
                offset: 0,
            },
            bytes: Payload::copy_from_slice(b"one frame"),
        })
        .await
        .unwrap();
    let mut last = None;
    for index in 0..2 {
        let receipt = store
            .append_captured(
                &output,
                JournaledOutput {
                    source: binding.source.clone(),
                    position: DecodedPosition {
                        source_byte: 0,
                        item_index: index,
                    },
                    event: event(&format!("item-{index}"), &[index as u8]),
                },
            )
            .await
            .unwrap();
        last = Some(receipt.record.cursor.clone());
    }
    store
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("generation-2").unwrap(),
            stream: output.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    assert!(matches!(
        store
            .expire_retry_generations(ExpireRetryGenerations {
                operation_id: RetentionOperationId::new("premature-expiry").unwrap(),
                stream: output.clone(),
                expected_oldest: RetryGeneration::LEGACY,
                retain_from: RetryGeneration::new(2),
            })
            .await,
        Err(RetentionError::JournalProtectionActive {
            oldest_required: RetryGeneration::FIRST
        })
    ));
    store
        .publish_parser_checkpoint(ParserCheckpoint {
            source: SourcePosition {
                source: binding.source.clone(),
                offset: 9,
            },
            parser: binding.parser,
            state: Payload::copy_from_slice(b"done"),
            next_item_index: 2,
            output_stream: output.clone(),
            committed_output: last,
        })
        .await
        .unwrap();
    let cleanup = store
        .cleanup_captured(SourceJournalStoreConfig::default().cleanup)
        .await
        .unwrap();
    assert_eq!(cleanup.removed_marker_rows, 2);
    store
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new("expiry-after-checkpoint").unwrap(),
            stream: output,
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::new(2),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn shared_source_journal_contract() {
    common::run_source_journal_contract(
        MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap(),
    )
    .await;
}

#[tokio::test]
async fn aggregate_checkpoint_bytes_reject_growth_without_replacing_old_state() {
    let mut journal = SourceJournalStoreConfig::default();
    journal.storage.max_checkpoint_state_bytes = 512;
    journal.storage.max_staging_bytes = 900;
    let store = MemoryStore::open(MemoryStoreOptions {
        source_journal: journal,
        ..MemoryStoreOptions::default()
    })
    .await
    .unwrap();
    let output = store
        .create_if_absent(&StreamId::new("checkpoint-output").unwrap())
        .await
        .unwrap();
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("checkpoint-enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let mut checkpoints = Vec::new();
    for number in 0..2u8 {
        let binding = SourceBinding {
            source: SourceKey {
                id: SourceId::new(format!("checkpoint-input-{number}")).unwrap(),
                incarnation: SourceIncarnation([number; 16]),
            },
            parser: parser(),
            output_stream: output.clone(),
        };
        store
            .begin_source(BeginSource {
                operation_id: JournalOperationId::new(format!("checkpoint-begin-{number}"))
                    .unwrap(),
                binding: binding.clone(),
            })
            .await
            .unwrap();
        let checkpoint = ParserCheckpoint {
            source: SourcePosition {
                source: binding.source,
                offset: 0,
            },
            parser: binding.parser,
            state: Payload::copy_from_slice(&[number; 100]),
            next_item_index: 0,
            output_stream: output.clone(),
            committed_output: None,
        };
        store
            .publish_parser_checkpoint(checkpoint.clone())
            .await
            .unwrap();
        checkpoints.push(checkpoint);
    }
    let old = checkpoints[0].clone();
    let mut too_large = old.clone();
    too_large.state = Payload::copy_from_slice(&[9; 512]);
    assert!(matches!(
        store.publish_parser_checkpoint(too_large).await,
        Err(JournalError::CapacityExceeded)
    ));
    assert_eq!(
        store.latest_checkpoint(&old.source.source).await.unwrap(),
        Some(old.clone())
    );
    let mut smaller = old;
    smaller.state = Payload::copy_from_slice(&[3; 8]);
    store
        .publish_parser_checkpoint(smaller.clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .latest_checkpoint(&smaller.source.source)
            .await
            .unwrap(),
        Some(smaller)
    );
}

#[tokio::test]
async fn runtime_composes_capture_and_atomic_output_with_bounded_admission() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let output = runtime
        .create_stream(&StreamId::new("runtime-journal-output").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("runtime-journal-enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("runtime-journal-input").unwrap(),
            incarnation: SourceIncarnation([19; 16]),
        },
        parser: parser(),
        output_stream: output.clone(),
    };
    runtime
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("runtime-journal-begin").unwrap(),
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
            bytes: Payload::copy_from_slice(b"frame\n"),
        })
        .await
        .unwrap();
    let appended = runtime
        .append_captured(
            &output,
            JournaledOutput {
                source: binding.source,
                position: DecodedPosition {
                    source_byte: 0,
                    item_index: 0,
                },
                event: event("runtime-item", b"decoded"),
            },
        )
        .await
        .unwrap();
    let page = runtime
        .read_after(
            &Cursor::new(output, 0),
            PageLimits {
                max_records: 1,
                max_bytes: 1024 * 1024,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].cursor, appended.record.cursor);
    let _ = runtime.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn runtime_rejects_journal_semaphore_ranges_before_opening_store() {
    let mut config = RuntimeConfig::default();
    config.journal.max_concurrent = Semaphore::MAX_PERMITS + 1;
    assert!(matches!(
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config).await,
        Err(Error::InvalidConfig(_))
    ));

    let mut config = RuntimeConfig::default();
    config.journal.admission_timeout = std::time::Duration::MAX;
    assert!(matches!(
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config).await,
        Err(Error::InvalidConfig(_))
    ));
}

#[test]
fn cleanup_budget_must_fit_every_accepted_journal_row_shape() {
    let mut config = SourceJournalStoreConfig::default();
    config.storage.max_segment_bytes = 1;
    config.cleanup.max_bytes = 900;
    assert!(matches!(
        config.validate(),
        Err(JournalError::InvalidConfig(_))
    ));
}

#[test]
fn newline_checkpoint_rejects_impossible_partial_without_mutating_decoder() {
    let mut decoder = newline();
    let step = decoder.decode(
        b"old",
        DecodeBudget {
            max_items: 1,
            max_bytes: 16,
            max_work_units: 16,
        },
    );
    assert_eq!(step.consumed_bytes, 3);
    let before = decoder.checkpoint_state(1024).unwrap();
    let mut malformed = before.as_bytes().to_vec();
    *malformed.last_mut().unwrap() = b'\n';
    let parser = decoder.parser();
    assert!(matches!(
        decoder.restore_state(&parser, &malformed, 1024),
        Err(CheckpointError::InvalidState(_))
    ));
    assert_eq!(decoder.checkpoint_state(1024).unwrap(), before);
}

#[tokio::test]
async fn journal_ingestion_replays_after_output_before_checkpoint_and_restores_split_frame() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let output = runtime
        .create_stream(&StreamId::new("journal-ingestion-output").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("journal-ingestion-enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("journal-ingestion-input").unwrap(),
            incarnation: SourceIncarnation([31; 16]),
        },
        parser: newline().parser(),
        output_stream: output.clone(),
    };
    let service =
        JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
    service
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("journal-ingestion-begin").unwrap(),
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
                event: event("output-0", b"one"),
            },
        )
        .await
        .unwrap();
    runtime
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new("journal-ingestion-generation-two").unwrap(),
            stream: output.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    assert_eq!(
        runtime.latest_checkpoint(&binding.source).await.unwrap(),
        None
    );

    let replayed = service
        .recover_captured(
            &binding,
            newline(),
            |frame: ByteFrame, position: DecodedPosition| {
                Ok(event(
                    &format!("output-{}", position.item_index),
                    frame.as_bytes(),
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(replayed.next_item_index, 2);
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
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].event.payload.as_bytes(), b"one");
    assert_eq!(page.records[1].event.payload.as_bytes(), b"two");
    assert!(runtime
        .lookup_generated(
            &output,
            RetryGeneration::FIRST,
            &EventId::new("output-0").unwrap(),
        )
        .await
        .unwrap()
        .is_some());
    assert!(runtime
        .lookup_generated(
            &output,
            RetryGeneration::new(2),
            &EventId::new("output-1").unwrap(),
        )
        .await
        .unwrap()
        .is_some());

    let split_binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("split-frame-input").unwrap(),
            incarnation: SourceIncarnation([32; 16]),
        },
        parser: newline().parser(),
        output_stream: output.clone(),
    };
    service
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("split-frame-begin").unwrap(),
            binding: split_binding.clone(),
        })
        .await
        .unwrap();
    runtime
        .capture_segment(RawSegment {
            start: SourcePosition {
                source: split_binding.source.clone(),
                offset: 0,
            },
            bytes: Payload::copy_from_slice(b"par"),
        })
        .await
        .unwrap();
    let partial = service
        .recover_captured(
            &split_binding,
            newline(),
            |frame: ByteFrame, position: DecodedPosition| {
                Ok(event(
                    &format!("split-{}", position.item_index),
                    frame.as_bytes(),
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(partial.captured_offset, 3);
    assert_eq!(partial.next_item_index, 0);
    runtime
        .capture_segment(RawSegment {
            start: SourcePosition {
                source: split_binding.source.clone(),
                offset: 3,
            },
            bytes: Payload::copy_from_slice(b"tial\n"),
        })
        .await
        .unwrap();
    let resumed = service
        .recover_captured(
            &split_binding,
            newline(),
            |frame: ByteFrame, position: DecodedPosition| {
                Ok(event(
                    &format!("split-{}", position.item_index),
                    frame.as_bytes(),
                ))
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.next_item_index, 1);
    assert_eq!(
        runtime
            .lookup_generated(
                &output,
                RetryGeneration::new(2),
                &EventId::new("split-0").unwrap(),
            )
            .await
            .unwrap()
            .unwrap()
            .event
            .payload
            .as_bytes(),
        b"partial"
    );
    let _ = runtime.shutdown(std::time::Duration::from_secs(1)).await;
}

#[tokio::test]
async fn journal_ingestion_drains_valid_zero_consumption_buffered_outputs() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let output = runtime
        .create_stream(&StreamId::new("buffered-output-stream").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("buffered-output-enable").unwrap(),
            stream: output.clone(),
        })
        .await
        .unwrap();
    let decoder = BufferedOutputDecoder(0);
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("buffered-output-source").unwrap(),
            incarnation: SourceIncarnation([44; 16]),
        },
        parser: decoder.parser(),
        output_stream: output.clone(),
    };
    let drive = JournalDriveConfig {
        max_output_commits: 1,
        ..JournalDriveConfig::default()
    };
    let service = JournalIngestionService::new(runtime.clone(), drive).unwrap();
    service
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("buffered-output-begin").unwrap(),
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
            bytes: Payload::copy_from_slice(b"x"),
        })
        .await
        .unwrap();
    let first_drive = service
        .recover_captured(
            &binding,
            decoder,
            |bytes: Vec<u8>, position: DecodedPosition| {
                Ok(event(&format!("buffered-{}", position.item_index), &bytes))
            },
        )
        .await;
    assert!(matches!(
        first_drive,
        Err(JournalIngestionError::WorkLimitReached(
            JournalDriveProgress {
                captured_offset: 1,
                next_item_index: 1,
                committed_outputs: 1,
                ..
            }
        ))
    ));
    let second_drive = service
        .recover_captured(
            &binding,
            BufferedOutputDecoder(0),
            |bytes: Vec<u8>, position: DecodedPosition| {
                Ok(event(&format!("buffered-{}", position.item_index), &bytes))
            },
        )
        .await;
    assert!(matches!(
        second_drive,
        Err(JournalIngestionError::WorkLimitReached(
            JournalDriveProgress {
                captured_offset: 1,
                next_item_index: 2,
                committed_outputs: 1,
                ..
            }
        ))
    ));
    let progress = service
        .recover_captured(
            &binding,
            BufferedOutputDecoder(0),
            |bytes: Vec<u8>, position: DecodedPosition| {
                Ok(event(&format!("buffered-{}", position.item_index), &bytes))
            },
        )
        .await
        .unwrap();
    assert_eq!(progress.next_item_index, 2);
    assert_eq!(progress.captured_offset, 1);
    assert!(progress.complete_capture);
    let page = runtime
        .read_after(
            &Cursor::new(output, 0),
            PageLimits {
                max_records: 2,
                max_bytes: 1024 * 1024,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.records[0].event.payload.as_bytes(), b"first");
    assert_eq!(page.records[1].event.payload.as_bytes(), b"second");
    let _ = runtime.shutdown(std::time::Duration::from_secs(1)).await;
}
