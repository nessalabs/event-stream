#![cfg(all(feature = "sqlite", feature = "source-journal"))]
use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::ingestion::*;
use event_stream::*;
use std::time::Duration;

fn decoder() -> NewlineFramer {
    NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 32,
        emit_empty_frames: false,
        crlf: CrLfPolicy::PreserveCarriageReturn,
        final_line: FinalLinePolicy::EmitUnterminated,
    })
    .unwrap()
}
fn map(frame: ByteFrame, position: DecodedPosition) -> std::result::Result<NewEvent, String> {
    Ok(NewEvent {
        id: EventId::new(format!("line-{}", position.item_index)).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("line").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(frame.as_bytes()),
    })
}

#[tokio::test]
async fn sqlite_finalization_is_bounded_and_survives_reopen() {
    for (input, expected) in [
        (b"".as_slice(), vec![]),
        (
            b"a\nb\nlast".as_slice(),
            vec![b"a".as_slice(), b"b".as_slice(), b"last".as_slice()],
        ),
    ] {
        let directory =
            std::env::temp_dir().join(format!("sqlite-finish-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("events.db");
        let runtime =
            Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
                .await
                .unwrap();
        let stream = runtime
            .create_stream(&StreamId::new("output").unwrap())
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
                id: SourceId::new("input").unwrap(),
                incarnation: SourceIncarnation([7; 16]),
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
        if !input.is_empty() {
            runtime
                .capture_segment(RawSegment {
                    start: SourcePosition {
                        source: binding.source.clone(),
                        offset: 0,
                    },
                    bytes: Payload::copy_from_slice(input),
                })
                .await
                .unwrap();
        }
        runtime
            .seal_source(SealSource {
                end: SourcePosition {
                    source: binding.source.clone(),
                    offset: input.len() as u64,
                },
            })
            .await
            .unwrap();
        runtime.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(runtime);
        let runtime =
            Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
                .await
                .unwrap();
        let service = JournalIngestionService::new(
            runtime.clone(),
            JournalDriveConfig {
                max_output_commits: 1,
                max_steps: 2,
                ..Default::default()
            },
        )
        .unwrap();
        let mut completed = false;
        let mut stopped = 0;
        let mut outputs = 0;
        for _ in 0..8 {
            match service.finish_captured(&binding, decoder(), map).await {
                Ok(progress) => {
                    assert!(progress.parser_finished);
                    assert!(progress.recovery.committed_outputs <= 1);
                    outputs += progress.recovery.committed_outputs;
                    completed = true;
                    break;
                }
                Err(JournalIngestionError::WorkLimitReached(progress)) => {
                    assert!(progress.committed_outputs <= 1);
                    outputs += progress.committed_outputs;
                    stopped += 1;
                    assert!(
                        !runtime
                            .source_finalization(&binding.source)
                            .await
                            .unwrap()
                            .parser_finished
                    );
                }
                other => panic!("unexpected finalization result: {other:?}"),
            }
        }
        assert!(completed, "bounded calls failed to complete");
        assert_eq!(outputs, expected.len());
        if !input.is_empty() {
            assert!(stopped >= 2);
        }
        drop(service);
        runtime.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(runtime);
        let runtime =
            Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
                .await
                .unwrap();
        let service =
            JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
        let replay = service
            .finish_captured(&binding, decoder(), map)
            .await
            .unwrap();
        assert!(replay.parser_finished);
        assert_eq!(replay.recovery.committed_outputs, 0);
        let page = runtime
            .read_after(
                &Cursor::new(stream, 0),
                PageLimits {
                    max_records: 8,
                    max_bytes: RuntimeConfig::default().reads.page.max_bytes,
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), expected.len());
        for (index, (record, expected)) in page.records.iter().zip(expected).enumerate() {
            assert_eq!(record.cursor.offset, index as u64 + 1);
            assert_eq!(record.event.payload.as_bytes(), expected);
        }
        let checkpoint = runtime
            .latest_checkpoint(&binding.source)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint.source.offset, input.len() as u64);
        assert!(checkpoint.state.as_bytes().starts_with(b"NLCF1"));
        drop(service);
        runtime.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(runtime);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn sqlite_eof_kill_child() {
    let Some(path) = std::env::var_os("EVENT_STREAM_EOF_CHILD") else {
        return;
    };
    let stage = std::env::var("EVENT_STREAM_EOF_STAGE").unwrap();
    let runtime = Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
        .await
        .unwrap();
    let stream = runtime
        .create_stream(&StreamId::new("output").unwrap())
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
            id: SourceId::new("input").unwrap(),
            incarnation: SourceIncarnation([7; 16]),
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
            bytes: Payload::copy_from_slice(b"a\nb\nlast"),
        })
        .await
        .unwrap();
    let end = SourcePosition {
        source: binding.source.clone(),
        offset: 8,
    };
    runtime
        .seal_source(SealSource { end: end.clone() })
        .await
        .unwrap();
    if stage != "seal" {
        let service =
            JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
        let progress = service
            .recover_captured(&binding, decoder(), map)
            .await
            .unwrap();
        assert_eq!(progress.next_item_index, 2);
        let checkpoint = runtime
            .latest_checkpoint(&binding.source)
            .await
            .unwrap()
            .unwrap();
        let mut final_decoder = decoder();
        final_decoder
            .restore_state(&binding.parser, checkpoint.state.as_bytes(), 32)
            .unwrap();
        let step = final_decoder.finish(DecodeBudget {
            max_items: 1,
            max_bytes: 32,
            max_work_units: 32,
        });
        assert_eq!(step.state, DecodeState::Finished);
        let decoded = step.items.into_iter().next().unwrap();
        let position = DecodedPosition {
            source_byte: decoded.source_byte,
            item_index: 2,
        };
        let receipt = runtime
            .append_captured(
                &stream,
                JournaledOutput {
                    source: binding.source.clone(),
                    position,
                    event: map(decoded.item, position).unwrap(),
                },
            )
            .await
            .unwrap();
        if stage != "output" {
            let checkpoint = ParserCheckpoint {
                source: end,
                parser: binding.parser.clone(),
                state: final_decoder.checkpoint_state(64).unwrap(),
                next_item_index: 3,
                output_stream: stream,
                committed_output: Some(receipt.record.cursor.clone()),
            };
            runtime
                .publish_parser_checkpoint(checkpoint.clone())
                .await
                .unwrap();
            if stage == "finished" {
                runtime
                    .finish_source(FinishSource { checkpoint })
                    .await
                    .unwrap();
            }
        }
    }
    std::fs::write(
        std::env::var_os("EVENT_STREAM_EOF_READY").unwrap(),
        b"committed",
    )
    .unwrap();
    std::future::pending::<()>().await;
}

struct KillChild(std::process::Child);
impl Drop for KillChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn sqlite_eof_recovers_four_process_kill_boundaries() {
    for stage in ["seal", "output", "checkpoint", "finished"] {
        let directory =
            std::env::temp_dir().join(format!("sqlite-eof-kill-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("events.db");
        let ready = directory.join("ready");
        let mut child = KillChild(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "sqlite_eof_kill_child", "--nocapture"])
                .env("EVENT_STREAM_EOF_CHILD", &path)
                .env("EVENT_STREAM_EOF_STAGE", stage)
                .env("EVENT_STREAM_EOF_READY", &ready)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !ready.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child exited before {stage}"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "child did not reach {stage}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        child.0.kill().unwrap();
        let exit = child.0.wait().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(exit.signal(), Some(9));
        }
        assert!(!exit.success());
        let runtime =
            Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default())
                .await
                .unwrap();
        let stream = runtime
            .create_stream(&StreamId::new("output").unwrap())
            .await
            .unwrap();
        let binding = SourceBinding {
            source: SourceKey {
                id: SourceId::new("input").unwrap(),
                incarnation: SourceIncarnation([7; 16]),
            },
            parser: decoder().parser(),
            output_stream: stream.clone(),
        };
        let state = runtime.source_finalization(&binding.source).await.unwrap();
        assert_eq!(state.sealed_end, Some(8));
        assert_eq!(state.parser_finished, stage == "finished");
        let checkpoint = runtime.latest_checkpoint(&binding.source).await.unwrap();
        assert_eq!(
            checkpoint.as_ref().map(|cp| cp.next_item_index),
            match stage {
                "seal" => None,
                "output" => Some(2),
                _ => Some(3),
            }
        );
        let service =
            JournalIngestionService::new(runtime.clone(), JournalDriveConfig::default()).unwrap();
        let recovered = service
            .finish_captured(&binding, decoder(), map)
            .await
            .unwrap();
        assert!(recovered.parser_finished);
        let page = runtime
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
        assert_eq!(page.records.len(), 3);
        for (index, (record, expected)) in page
            .records
            .iter()
            .zip([b"a".as_slice(), b"b".as_slice(), b"last".as_slice()])
            .enumerate()
        {
            assert_eq!(record.cursor.offset, index as u64 + 1);
            assert_eq!(record.event.payload.as_bytes(), expected);
        }
        assert_eq!(
            service
                .finish_captured(&binding, decoder(), map)
                .await
                .unwrap()
                .recovery
                .committed_outputs,
            0
        );
        drop(service);
        runtime.shutdown(Duration::from_secs(2)).await.unwrap();
        drop(runtime);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
