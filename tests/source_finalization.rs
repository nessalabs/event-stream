#![cfg(feature = "source-journal")]
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;

#[tokio::test]
async fn memory_seal_and_finish_preserve_exact_retries_and_reject_new_work() {
    for input in [b"".as_slice(), b"abc".as_slice()] {
        let store = MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap();
        let stream = store
            .create_if_absent(&StreamId::new("output").unwrap())
            .await
            .unwrap();
        store
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new("enable").unwrap(),
                stream: stream.clone(),
            })
            .await
            .unwrap();
        let binding = SourceBinding {
            source: SourceKey {
                id: SourceId::new("input").unwrap(),
                incarnation: SourceIncarnation([1; 16]),
            },
            parser: ParserRef {
                id: ParserId::new("fixture").unwrap(),
                version: 1,
            },
            output_stream: stream.clone(),
        };
        store
            .begin_source(BeginSource {
                operation_id: JournalOperationId::new("begin").unwrap(),
                binding: binding.clone(),
            })
            .await
            .unwrap();
        let segment = RawSegment {
            start: SourcePosition {
                source: binding.source.clone(),
                offset: 0,
            },
            bytes: Payload::copy_from_slice(input),
        };
        let capture = if input.is_empty() {
            None
        } else {
            Some(store.capture_segment(segment.clone()).await.unwrap())
        };
        let end = SourcePosition {
            source: binding.source.clone(),
            offset: input.len() as u64,
        };
        let checkpoint = ParserCheckpoint {
            source: end.clone(),
            parser: binding.parser.clone(),
            state: Payload::copy_from_slice(b"finished"),
            next_item_index: 0,
            output_stream: stream.clone(),
            committed_output: None,
        };
        store
            .publish_parser_checkpoint(checkpoint.clone())
            .await
            .unwrap();
        assert!(matches!(
            store
                .finish_source(FinishSource {
                    checkpoint: checkpoint.clone()
                })
                .await,
            Err(JournalError::CheckpointConflict { .. })
        ));
        assert!(matches!(
            store
                .seal_source(SealSource {
                    end: SourcePosition {
                        offset: end.offset + 1,
                        ..end.clone()
                    }
                })
                .await,
            Err(JournalError::InvalidInput(_))
        ));
        let seal = SealSource { end: end.clone() };
        let receipt = store.seal_source(seal.clone()).await.unwrap();
        assert_eq!(store.seal_source(seal).await.unwrap(), receipt);
        assert!(
            !store
                .source_finalization(&binding.source)
                .await
                .unwrap()
                .parser_finished
        );
        if let Some(capture) = capture {
            assert_eq!(store.capture_segment(segment).await.unwrap(), capture);
        }
        assert!(matches!(
            store
                .capture_segment(RawSegment {
                    start: end.clone(),
                    bytes: Payload::copy_from_slice(b"extra")
                })
                .await,
            Err(JournalError::SourceSealed { .. })
        ));
        let request = FinishSource {
            checkpoint: checkpoint.clone(),
        };
        let completed = store.finish_source(request.clone()).await.unwrap();
        assert_eq!(store.finish_source(request).await.unwrap(), completed);
        assert_eq!(
            store.source_finalization(&binding.source).await.unwrap(),
            SourceFinalizationStatus {
                sealed_end: Some(end.offset),
                parser_finished: true
            }
        );
        store
            .publish_parser_checkpoint(checkpoint.clone())
            .await
            .unwrap();
        let mut changed = checkpoint.clone();
        changed.state = Payload::copy_from_slice(b"different");
        assert!(matches!(
            store.publish_parser_checkpoint(changed.clone()).await,
            Err(JournalError::CheckpointConflict { .. })
        ));
        assert!(matches!(
            store
                .finish_source(FinishSource {
                    checkpoint: changed
                })
                .await,
            Err(JournalError::CheckpointConflict { .. })
        ));
        assert!(matches!(
            store
                .append_captured(
                    &stream,
                    JournaledOutput {
                        source: binding.source.clone(),
                        position: DecodedPosition {
                            source_byte: end.offset,
                            item_index: 0
                        },
                        event: NewEvent {
                            id: EventId::new("late").unwrap(),
                            schema: SchemaRef {
                                id: SchemaId::new("bytes").unwrap(),
                                version: 1
                            },
                            payload: Payload::copy_from_slice(b"late")
                        },
                    }
                )
                .await,
            Err(JournalError::SourceSealed { .. })
        ));
        store.close().await.unwrap();
    }
}

#[tokio::test]
async fn runtime_routes_finalization_and_rejects_work_after_shutdown() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let stream = runtime
        .create_stream(&StreamId::new("runtime-output").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("runtime-input").unwrap(),
            incarnation: SourceIncarnation([2; 16]),
        },
        parser: ParserRef {
            id: ParserId::new("fixture").unwrap(),
            version: 1,
        },
        output_stream: stream.clone(),
    };
    runtime
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin").unwrap(),
            binding: binding.clone(),
        })
        .await
        .unwrap();
    let checkpoint = ParserCheckpoint {
        source: SourcePosition {
            source: binding.source.clone(),
            offset: 0,
        },
        parser: binding.parser,
        state: Payload::copy_from_slice(b"finished"),
        next_item_index: 0,
        output_stream: stream,
        committed_output: None,
    };
    runtime
        .publish_parser_checkpoint(checkpoint.clone())
        .await
        .unwrap();
    let seal = SealSource {
        end: checkpoint.source.clone(),
    };
    runtime.seal_source(seal.clone()).await.unwrap();
    let finish = FinishSource { checkpoint };
    runtime.finish_source(finish.clone()).await.unwrap();
    assert_eq!(
        runtime.source_finalization(&binding.source).await.unwrap(),
        SourceFinalizationStatus {
            sealed_end: Some(0),
            parser_finished: true
        }
    );
    runtime
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert!(matches!(
        runtime.seal_source(seal).await,
        Err(JournalError::Closed)
    ));
    assert!(matches!(
        runtime.finish_source(finish).await,
        Err(JournalError::Closed)
    ));
    assert!(matches!(
        runtime.source_finalization(&binding.source).await,
        Err(JournalError::Closed)
    ));
}

#[test]
fn newline_finished_checkpoint_replays_eof_without_reemitting_output() {
    use event_stream::ingestion::*;
    let config = NewlineFramerConfig {
        max_frame_bytes: 32,
        emit_empty_frames: false,
        crlf: CrLfPolicy::PreserveCarriageReturn,
        final_line: FinalLinePolicy::EmitUnterminated,
    };
    let budget = DecodeBudget {
        max_items: 2,
        max_bytes: 32,
        max_work_units: 32,
    };
    let mut decoder = NewlineFramer::new(config.clone()).unwrap();
    assert!(decoder.decode(b"last", budget).items.is_empty());
    let live = decoder.checkpoint_state(64).unwrap();
    let mut old = NewlineFramer::new(config.clone()).unwrap();
    old.restore_state(&decoder.parser(), live.as_bytes(), 32)
        .unwrap();
    let final_step = old.finish(budget);
    assert_eq!(final_step.state, DecodeState::Finished);
    assert_eq!(final_step.items[0].item.as_bytes(), b"last");
    let complete = old.checkpoint_state(64).unwrap();
    let mut reopened = NewlineFramer::new(config).unwrap();
    reopened
        .restore_state(&old.parser(), complete.as_bytes(), 32)
        .unwrap();
    let repeated = reopened.finish(budget);
    assert_eq!(repeated.state, DecodeState::Finished);
    assert!(repeated.items.is_empty());
    assert_eq!(reopened.checkpoint_state(64).unwrap(), complete);
    let mut bad = complete.as_bytes().to_vec();
    bad[12] = 0; // Frame start no longer equals the finished byte position.
    assert!(reopened.restore_state(&old.parser(), &bad, 32).is_err());
    let mut rejected = NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 32,
        emit_empty_frames: false,
        crlf: CrLfPolicy::PreserveCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .unwrap();
    rejected.decode(b"last", budget);
    assert!(matches!(
        rejected.finish(budget).state,
        DecodeState::Failed(_)
    ));
    assert!(rejected.checkpoint_state(64).is_err());
}

#[tokio::test]
async fn journal_finish_requires_seal_and_recovers_final_frame_mapping_failure() {
    use event_stream::ingestion::*;
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let stream = runtime
        .create_stream(&StreamId::new("decoded").unwrap())
        .await
        .unwrap();
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable").unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    let decoder = || {
        NewlineFramer::new(NewlineFramerConfig {
            max_frame_bytes: 32,
            emit_empty_frames: false,
            crlf: CrLfPolicy::PreserveCarriageReturn,
            final_line: FinalLinePolicy::EmitUnterminated,
        })
        .unwrap()
    };
    let binding = SourceBinding {
        source: SourceKey {
            id: SourceId::new("raw").unwrap(),
            incarnation: SourceIncarnation([3; 16]),
        },
        parser: decoder().parser(),
        output_stream: stream.clone(),
    };
    runtime
        .begin_source(BeginSource {
            operation_id: JournalOperationId::new("begin").unwrap(),
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
            bytes: Payload::copy_from_slice(b"a\nb"),
        })
        .await
        .unwrap();
    let map =
        |frame: ByteFrame, position: DecodedPosition| -> std::result::Result<NewEvent, String> {
            Ok(NewEvent {
                id: EventId::new(format!("item-{}", position.item_index)).unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("line").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(frame.as_bytes()),
            })
        };
    let service =
        JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
    assert!(matches!(
        service.finish_captured(&binding, decoder(), map).await,
        Err(JournalIngestionError::SourceNotSealed)
    ));
    let caught_up = service
        .recover_captured(&binding, decoder(), map)
        .await
        .unwrap();
    assert!(caught_up.complete_capture);
    assert_eq!(
        caught_up.next_item_index, 1,
        "captured tail must not emit an unterminated frame"
    );
    runtime
        .seal_source(SealSource {
            end: SourcePosition {
                source: binding.source.clone(),
                offset: 3,
            },
        })
        .await
        .unwrap();
    assert!(matches!(
        service
            .finish_captured(
                &binding,
                decoder(),
                |_: ByteFrame, _: DecodedPosition| -> std::result::Result<NewEvent, String> {
                    Err("injected mapping failure".into())
                }
            )
            .await,
        Err(JournalIngestionError::Mapping(_))
    ));
    assert!(
        !runtime
            .source_finalization(&binding.source)
            .await
            .unwrap()
            .parser_finished
    );
    let done = service
        .finish_captured(&binding, decoder(), map)
        .await
        .unwrap();
    assert!(done.parser_finished);
    assert_eq!(done.recovery.next_item_index, 2);
    assert_eq!(done.recovery.committed_outputs, 1);
    let repeated = service
        .finish_captured(&binding, decoder(), map)
        .await
        .unwrap();
    assert_eq!(repeated.recovery.committed_outputs, 0);
    let history = runtime
        .read_after(
            &Cursor::new(stream, 0),
            PageLimits {
                max_records: 4,
                max_bytes: RuntimeConfig::default().reads.page.max_bytes,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(history.records.len(), 2);
    assert_eq!(history.records[0].event.payload.as_bytes(), b"a");
    assert_eq!(history.records[1].event.payload.as_bytes(), b"b");
    runtime
        .shutdown(std::time::Duration::from_secs(2))
        .await
        .unwrap();
}
