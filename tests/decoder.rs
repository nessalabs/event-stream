#![cfg(feature = "codec")]

use async_trait::async_trait;
use event_stream::{ingestion::*, *};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

fn key() -> StreamKey {
    StreamKey {
        id: StreamId::new("decoded").unwrap(),
        incarnation: IncarnationId([7; 16]),
    }
}

fn framer(max: usize, final_line: FinalLinePolicy) -> NewlineFramer {
    NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: max,
        emit_empty_frames: true,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line,
    })
    .unwrap()
}

fn budget() -> DecodeBudget {
    DecodeBudget {
        max_items: 32,
        max_bytes: 4096,
        max_work_units: 4096,
    }
}

fn decode_chunks(chunks: &[&[u8]]) -> Vec<Vec<u8>> {
    let mut decoder = framer(64, FinalLinePolicy::EmitUnterminated);
    let mut output = Vec::new();
    for chunk in chunks {
        let mut offset = 0;
        while offset < chunk.len() {
            let step = decoder.decode(&chunk[offset..], budget());
            offset += step.consumed_bytes;
            output.extend(
                step.items
                    .into_iter()
                    .map(|item| item.item.as_bytes().to_vec()),
            );
            assert!(!matches!(step.state, DecodeState::Failed(_)));
        }
    }
    let step = decoder.finish(budget());
    output.extend(
        step.items
            .into_iter()
            .map(|item| item.item.as_bytes().to_vec()),
    );
    assert_eq!(step.state, DecodeState::Finished);
    output
}

#[test]
fn newline_output_is_identical_for_every_partition() {
    let input = b"a\r\n\n\xff\xef\xbb\xbfz";
    let expected = vec![b"a".to_vec(), Vec::new(), b"\xff\xef\xbb\xbfz".to_vec()];
    for mask in 0..(1usize << (input.len() - 1)) {
        let mut chunks = Vec::new();
        let mut start = 0;
        for boundary in 1..input.len() {
            if mask & (1 << (boundary - 1)) != 0 {
                chunks.push(&input[start..boundary]);
                start = boundary;
            }
        }
        chunks.push(&input[start..]);
        assert_eq!(decode_chunks(&chunks), expected, "partition mask {mask}");
    }
    assert_eq!(decode_chunks(&[b"", input, b""]), expected);
}

#[test]
fn larger_seeded_chunk_partitions_match_unsplit_decoding() {
    let mut input = Vec::new();
    for index in 0..64u8 {
        input.extend_from_slice(format!("row-{index:02}-").as_bytes());
        input.extend_from_slice(&[index, 0xff]);
        input.extend_from_slice(if index % 3 == 0 { b"\r\n" } else { b"\n" });
    }
    let expected = decode_chunks(&[&input]);

    for seed in 0..128u64 {
        let mut state = seed.wrapping_add(1);
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < input.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let length = 1 + ((state >> 32) as usize % 23);
            let end = offset.saturating_add(length).min(input.len());
            chunks.push(&input[offset..end]);
            offset = end;
        }
        assert_eq!(decode_chunks(&chunks), expected, "partition seed {seed}");
    }
}

#[test]
fn newline_policies_are_exact_at_crlf_empty_and_eof_boundaries() {
    let mut preserve = NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 8,
        emit_empty_frames: false,
        crlf: CrLfPolicy::PreserveCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .unwrap();
    let step = preserve.decode(b"x\r\n\n", budget());
    assert_eq!(step.items.len(), 1);
    assert_eq!(step.items[0].item.as_bytes(), b"x\r");
    assert_eq!(preserve.finish(budget()).state, DecodeState::Finished);

    let mut reject = framer(4, FinalLinePolicy::RejectUnterminated);
    reject.decode(b"tail", budget());
    assert!(
        matches!(reject.finish(budget()).state, DecodeState::Failed(DecodeFailure { class, .. }) if class == "truncated_input")
    );

    let mut oversized = framer(2, FinalLinePolicy::EmitUnterminated);
    let step = oversized.decode(b"abc\n", budget());
    assert_eq!(step.consumed_bytes, 2);
    assert!(
        matches!(step.state, DecodeState::Failed(DecodeFailure { class, source_byte: Some(2) }) if class == "frame_too_large")
    );
}

#[derive(Default)]
struct RecordingSink {
    events: Mutex<Vec<NewEvent>>,
    fail_once: AtomicBool,
}

#[derive(Default)]
struct CountingSink(std::sync::atomic::AtomicUsize);

#[async_trait]
impl EventSink for CountingSink {
    async fn append(&self, _: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        let offset = self.0.fetch_add(1, Ordering::Relaxed) as u64 + 1;
        Ok(AppendReceipt {
            record: Arc::new(Record {
                cursor: Cursor::new(key(), offset),
                event,
            }),
            kind: AppendKind::Inserted,
        })
    }
}

#[async_trait]
impl EventSink for RecordingSink {
    async fn append(&self, _: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        if self.fail_once.swap(false, Ordering::SeqCst) {
            return Err(Error::StoreWriteFailed("injected".into()));
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(AppendReceipt {
            record: Arc::new(Record {
                cursor: Cursor::new(key(), self.events.lock().unwrap().len() as u64),
                event,
            }),
            kind: AppendKind::Inserted,
        })
    }
}

fn mapper(frame: ByteFrame, position: DecodedPosition) -> std::result::Result<NewEvent, String> {
    Ok(NewEvent {
        id: EventId::new(format!(
            "source-{}-{}",
            position.source_byte, position.item_index
        ))
        .unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("test.frame").unwrap(),
            version: 1,
        },
        payload: frame.into_payload(),
    })
}

#[tokio::test]
async fn driver_maps_and_commits_one_item_at_a_time_with_stable_positions() {
    let sink = Arc::new(RecordingSink::default());
    let service = IngestionService::new(sink.clone(), IngestionConfig::default()).unwrap();
    let mut session = service
        .try_start(key(), framer(64, FinalLinePolicy::EmitUnterminated), mapper)
        .unwrap();
    session.push_chunk(b"one\ntw").await.unwrap();
    session.push_chunk(b"o\nthree").await.unwrap();
    session.finish().await.unwrap();
    let events = sink.events.lock().unwrap();
    assert_eq!(
        events
            .iter()
            .map(|e| e.payload.as_bytes())
            .collect::<Vec<_>>(),
        vec![b"one".as_slice(), b"two".as_slice(), b"three".as_slice()]
    );
    assert_eq!(
        events.iter().map(|e| e.id.as_str()).collect::<Vec<_>>(),
        vec!["source-0-0", "source-4-1", "source-8-2"]
    );
}

#[tokio::test]
async fn default_drive_budget_accepts_a_full_chunk_of_empty_frames() {
    let sink = Arc::new(CountingSink::default());
    let service = IngestionService::new(sink.clone(), IngestionConfig::default()).unwrap();
    let decoder = NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: 1,
        emit_empty_frames: true,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })
    .unwrap();
    let mut session = service.try_start(key(), decoder, mapper).unwrap();
    let input = vec![b'\n'; IngestionConfig::default().max_chunk_bytes];

    session.push_chunk(&input).await.unwrap();
    session.finish().await.unwrap();
    assert_eq!(sink.0.load(Ordering::Relaxed), input.len());
}

#[tokio::test]
async fn valid_items_commit_before_a_later_parse_failure() {
    let sink = Arc::new(RecordingSink::default());
    let service = IngestionService::new(sink.clone(), IngestionConfig::default()).unwrap();
    let mut session = service
        .try_start(key(), framer(3, FinalLinePolicy::EmitUnterminated), mapper)
        .unwrap();
    let error = session.push_chunk(b"ok\nabcd").await.unwrap_err();
    assert!(
        matches!(error, IngestionError::Decode { failure: DecodeFailure { class, .. }, .. } if class == "frame_too_large")
    );
    assert_eq!(sink.events.lock().unwrap().len(), 1);
    assert_eq!(sink.events.lock().unwrap()[0].payload.as_bytes(), b"ok");
    assert_eq!(session.retained_input_bytes(), 0);
    assert!(matches!(
        session.push_chunk(b"later\n").await,
        Err(IngestionError::AlreadyTerminal)
    ));
}

#[tokio::test]
async fn append_failure_retains_the_exact_pending_id_for_resolution() {
    let sink = Arc::new(RecordingSink {
        events: Mutex::new(Vec::new()),
        fail_once: AtomicBool::new(true),
    });
    let service = IngestionService::new(sink.clone(), IngestionConfig::default()).unwrap();
    let mut session = service
        .try_start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper)
        .unwrap();
    let error = session.push_chunk(b"one\n").await.unwrap_err();
    let pending = session.pending_event_id().unwrap().clone();
    assert!(
        matches!(error, IngestionError::Append { pending_event_id, .. } if pending_event_id == pending)
    );
    assert!(matches!(
        session.push_chunk(b"two\n").await,
        Err(IngestionError::PendingAppendUnresolved { .. })
    ));
    assert_eq!(
        session.retry_pending().await.unwrap().record.event.id,
        pending
    );
    assert!(session.pending_event_id().is_none());
}

#[derive(Clone, Copy)]
enum BadMode {
    Overconsume,
    TooMuchWork,
    TooMuchOutput,
    ZeroProgress,
    FinishedEarly,
}
struct BadDecoder(BadMode);
impl IncrementalDecoder for BadDecoder {
    type Item = ();
    fn name(&self) -> &'static str {
        "malicious"
    }
    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 1,
            max_retained_bytes: 1,
        }
    }
    fn decode(&mut self, input: &[u8], budget: DecodeBudget) -> DecodeStep<()> {
        match self.0 {
            BadMode::Overconsume => DecodeStep {
                consumed_bytes: input.len() + 1,
                work_units: 1,
                items: vec![],
                state: DecodeState::NeedInput,
            },
            BadMode::TooMuchWork => DecodeStep {
                consumed_bytes: 1,
                work_units: budget.max_work_units + 1,
                items: vec![],
                state: DecodeState::NeedInput,
            },
            BadMode::TooMuchOutput => DecodeStep {
                consumed_bytes: 1,
                work_units: 1,
                items: vec![DecodedItem {
                    item: (),
                    accounted_bytes: budget.max_bytes + 1,
                    source_byte: 0,
                }],
                state: DecodeState::OutputReady,
            },
            BadMode::ZeroProgress => DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: vec![],
                state: DecodeState::OutputReady,
            },
            BadMode::FinishedEarly => DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: vec![],
                state: DecodeState::Finished,
            },
        }
    }
    fn finish(&mut self, _: DecodeBudget) -> DecodeStep<()> {
        DecodeStep {
            consumed_bytes: 0,
            work_units: 0,
            items: vec![],
            state: DecodeState::Finished,
        }
    }
}

#[tokio::test]
async fn malicious_decoder_contract_violations_stop_the_session() {
    for mode in [
        BadMode::Overconsume,
        BadMode::TooMuchWork,
        BadMode::TooMuchOutput,
        BadMode::ZeroProgress,
        BadMode::FinishedEarly,
    ] {
        let sink = Arc::new(RecordingSink::default());
        let service = IngestionService::new(sink, IngestionConfig::default()).unwrap();
        let mut session = service
            .try_start(key(), BadDecoder(mode), |_: (), _: DecodedPosition| {
                Err("unused".into())
            })
            .unwrap();
        assert!(matches!(
            session.push_chunk(b"x").await,
            Err(IngestionError::DecoderContract { .. })
        ));
        assert!(matches!(
            session.push_chunk(b"x").await,
            Err(IngestionError::AlreadyTerminal)
        ));
    }
}

struct PausedSink {
    entered: Notify,
    release: Notify,
    block_once: AtomicBool,
    events: Mutex<Vec<NewEvent>>,
}

#[async_trait]
impl EventSink for PausedSink {
    async fn append(&self, _: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        if self.block_once.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.events.lock().unwrap().push(event.clone());
        Ok(AppendReceipt {
            record: Arc::new(Record {
                cursor: Cursor::new(key(), self.events.lock().unwrap().len() as u64),
                event,
            }),
            kind: AppendKind::Inserted,
        })
    }
}

#[tokio::test]
async fn cancelled_sink_wait_requires_retry_of_the_same_pending_event() {
    let sink = Arc::new(PausedSink {
        entered: Notify::new(),
        release: Notify::new(),
        block_once: AtomicBool::new(true),
        events: Mutex::new(Vec::new()),
    });
    let service = IngestionService::new(sink.clone(), IngestionConfig::default()).unwrap();
    let mut session = service
        .try_start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper)
        .unwrap();
    let mut push = Box::pin(session.push_chunk(b"first\nsecond\n"));
    tokio::select! {
        _ = sink.entered.notified() => {}
        result = &mut push => panic!("append unexpectedly completed: {result:?}"),
    }
    drop(push);
    let pending = session.pending_event_id().unwrap().clone();
    assert_eq!(pending.as_str(), "source-0-0");
    assert!(matches!(
        session.push_chunk(b"later\n").await,
        Err(IngestionError::PendingAppendUnresolved { event_id }) if event_id == pending
    ));
    let retry = session.retry_pending().await.unwrap();
    assert_eq!(retry.record.event.id, pending);
    // The already-decoded second item remains ordered after the resolved first item.
    session.push_chunk(b"").await.unwrap();
    session.finish().await.unwrap();
    assert_eq!(
        sink.events
            .lock()
            .unwrap()
            .iter()
            .map(|event| event.id.as_str())
            .collect::<Vec<_>>(),
        vec!["source-0-0", "source-6-1"]
    );
}

#[tokio::test]
async fn decoder_session_count_and_declared_state_bytes_are_bounded() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_sessions: 1,
        max_session_waiters: 1,
        max_decoder_retained_bytes: 16,
        max_total_decoder_bytes: 32,
        session_admission_timeout: std::time::Duration::from_millis(10),
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink, config).unwrap();
    let _first = service
        .try_start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper)
        .unwrap();
    assert!(matches!(
        service.try_start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper),
        Err(IngestionError::Overloaded)
    ));
    assert!(matches!(
        service
            .start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper)
            .await,
        Err(IngestionError::AdmissionTimeout)
    ));
}

#[tokio::test]
async fn terminal_session_releases_admission_while_its_handle_is_retained() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_sessions: 1,
        max_session_waiters: 1,
        max_decoder_retained_bytes: 16,
        max_total_decoder_bytes: 16,
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink, config).unwrap();
    let mut finished = service
        .try_start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper)
        .unwrap();
    finished.finish().await.unwrap();

    let _next = service
        .try_start(key(), framer(16, FinalLinePolicy::EmitUnterminated), mapper)
        .expect("a retained terminal handle must not occupy active capacity");
    assert!(matches!(
        finished.push_chunk(b"later\n").await,
        Err(IngestionError::AlreadyTerminal)
    ));
}

struct RetainingDecoder;
impl IncrementalDecoder for RetainingDecoder {
    type Item = ();
    fn name(&self) -> &'static str {
        "retaining"
    }
    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 1,
            max_retained_bytes: 1,
        }
    }
    fn decode(&mut self, _: &[u8], _: DecodeBudget) -> DecodeStep<()> {
        DecodeStep {
            consumed_bytes: 0,
            work_units: 0,
            items: vec![],
            state: DecodeState::NeedInput,
        }
    }
    fn finish(&mut self, _: DecodeBudget) -> DecodeStep<()> {
        unreachable!()
    }
}

struct RepeatingBufferedDecoder {
    remaining: usize,
    finish_only: bool,
}

impl IncrementalDecoder for RepeatingBufferedDecoder {
    type Item = u8;

    fn name(&self) -> &'static str {
        "repeating-buffered"
    }

    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 1,
            max_retained_bytes: 1,
        }
    }

    fn decode(&mut self, input: &[u8], _: DecodeBudget) -> DecodeStep<Self::Item> {
        if self.finish_only || self.remaining == 0 {
            return DecodeStep {
                consumed_bytes: input.len(),
                work_units: input.len(),
                items: Vec::new(),
                state: DecodeState::NeedInput,
            };
        }
        self.emit_or_finish()
    }

    fn finish(&mut self, _: DecodeBudget) -> DecodeStep<Self::Item> {
        self.emit_or_finish()
    }
}

impl RepeatingBufferedDecoder {
    fn emit_or_finish(&mut self) -> DecodeStep<u8> {
        if self.remaining == 0 {
            DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: Vec::new(),
                state: DecodeState::Finished,
            }
        } else {
            self.remaining -= 1;
            DecodeStep {
                consumed_bytes: 0,
                work_units: 1,
                items: vec![DecodedItem {
                    item: self.remaining as u8,
                    accounted_bytes: 1,
                    source_byte: 0,
                }],
                state: DecodeState::OutputReady,
            }
        }
    }
}

struct ExpandingDecoder;

impl IncrementalDecoder for ExpandingDecoder {
    type Item = u8;

    fn name(&self) -> &'static str {
        "expanding"
    }

    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 1,
            max_retained_bytes: 1,
        }
    }

    fn decode(&mut self, _: &[u8], _: DecodeBudget) -> DecodeStep<Self::Item> {
        DecodeStep {
            consumed_bytes: 1,
            work_units: 1,
            items: vec![
                DecodedItem {
                    item: 1,
                    accounted_bytes: 1,
                    source_byte: 0,
                },
                DecodedItem {
                    item: 2,
                    accounted_bytes: 1,
                    source_byte: 0,
                },
            ],
            state: DecodeState::OutputReady,
        }
    }

    fn finish(&mut self, _: DecodeBudget) -> DecodeStep<Self::Item> {
        unreachable!()
    }
}

struct CumulativeWorkDecoder;

impl IncrementalDecoder for CumulativeWorkDecoder {
    type Item = u8;

    fn name(&self) -> &'static str {
        "cumulative-work"
    }

    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: 1,
            max_retained_bytes: 1,
        }
    }

    fn decode(&mut self, _: &[u8], budget: DecodeBudget) -> DecodeStep<Self::Item> {
        DecodeStep {
            consumed_bytes: 1,
            work_units: budget.max_work_units,
            items: Vec::new(),
            state: DecodeState::OutputReady,
        }
    }

    fn finish(&mut self, _: DecodeBudget) -> DecodeStep<Self::Item> {
        unreachable!()
    }
}

fn byte_mapper(byte: u8, position: DecodedPosition) -> std::result::Result<NewEvent, String> {
    Ok(NewEvent {
        id: EventId::new(format!("buffered-{}-{byte}", position.item_index)).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("test.buffered").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(&[byte]),
    })
}

#[tokio::test]
async fn repeated_output_without_input_progress_is_finite() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_chunk_bytes: 1,
        max_retained_input_bytes: 4,
        max_output_items_per_step: 4,
        max_output_bytes_per_step: 4,
        max_work_units_per_step: 4,
        drive_budget: DecodeDriveBudget {
            max_steps: 4,
            max_items: 16,
            max_output_bytes: 16,
            max_work_units: 16,
        },
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink.clone(), config).unwrap();
    let mut session = service
        .try_start(
            key(),
            RepeatingBufferedDecoder {
                remaining: usize::MAX,
                finish_only: false,
            },
            byte_mapper,
        )
        .unwrap();

    assert!(matches!(
        session.push_chunk(b"x").await,
        Err(IngestionError::DecoderContract {
            reason: "cumulative drive budget exceeded",
            ..
        })
    ));
    assert_eq!(sink.events.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn finite_buffered_output_is_valid_during_decode_and_finish() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_output_items_per_step: 4,
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink.clone(), config).unwrap();
    let mut decode_session = service
        .try_start(
            key(),
            RepeatingBufferedDecoder {
                remaining: 3,
                finish_only: false,
            },
            byte_mapper,
        )
        .unwrap();
    decode_session.push_chunk(b"x").await.unwrap();
    assert_eq!(sink.events.lock().unwrap().len(), 3);
    drop(decode_session);

    let mut finish_session = service
        .try_start(
            key(),
            RepeatingBufferedDecoder {
                remaining: 3,
                finish_only: true,
            },
            byte_mapper,
        )
        .unwrap();
    finish_session.push_chunk(b"x").await.unwrap();
    finish_session.finish().await.unwrap();
    assert_eq!(sink.events.lock().unwrap().len(), 6);
}

#[tokio::test]
async fn expanding_output_uses_an_independent_drive_budget() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_chunk_bytes: 4,
        max_retained_input_bytes: 4,
        max_output_items_per_step: 2,
        max_output_bytes_per_step: 2,
        max_work_units_per_step: 4,
        drive_budget: DecodeDriveBudget {
            max_steps: 5,
            max_items: 8,
            max_output_bytes: 8,
            max_work_units: 8,
        },
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink.clone(), config).unwrap();
    let mut expansion = service
        .try_start(key(), ExpandingDecoder, byte_mapper)
        .unwrap();
    expansion.push_chunk(b"1234").await.unwrap();
    assert_eq!(sink.events.lock().unwrap().len(), 8);
}

#[tokio::test]
async fn cumulative_work_is_bounded_across_progressing_steps() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_chunk_bytes: 4,
        max_retained_input_bytes: 4,
        max_output_items_per_step: 4,
        max_output_bytes_per_step: 4,
        max_work_units_per_step: 4,
        drive_budget: DecodeDriveBudget {
            max_steps: 4,
            max_items: 4,
            max_output_bytes: 4,
            max_work_units: 8,
        },
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink, config).unwrap();
    let mut work = service
        .try_start(key(), CumulativeWorkDecoder, byte_mapper)
        .unwrap();
    assert!(matches!(
        work.push_chunk(b"1234").await,
        Err(IngestionError::DecoderContract {
            reason: "cumulative drive budget exceeded",
            ..
        })
    ));
}

#[tokio::test]
async fn driver_bounds_chunks_and_unconsumed_input() {
    let sink = Arc::new(RecordingSink::default());
    let config = IngestionConfig {
        max_chunk_bytes: 4,
        max_retained_input_bytes: 4,
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(sink, config).unwrap();
    let mut session = service
        .try_start(key(), RetainingDecoder, |_: (), _: DecodedPosition| {
            Err("unused".into())
        })
        .unwrap();
    assert!(matches!(
        session.push_chunk(b"12345").await,
        Err(IngestionError::ChunkTooLarge)
    ));
    session.push_chunk(b"1234").await.unwrap();
    assert!(matches!(
        session.push_chunk(b"x").await,
        Err(IngestionError::RetainedInputExceeded)
    ));
}
