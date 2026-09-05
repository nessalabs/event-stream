//! Optional bounded byte decoding and ingestion through `EventSink`.

#[cfg(feature = "source-journal")]
mod journal;
#[cfg(feature = "source-journal")]
pub use journal::*;

pub use crate::domain::DecodedPosition;
use crate::{domain::*, EventSink};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeBudget {
    pub max_items: usize,
    pub max_bytes: usize,
    pub max_work_units: usize,
}

/// Total work one `push_chunk` or `finish` call may cause across decode steps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecodeDriveBudget {
    pub max_steps: usize,
    pub max_items: usize,
    pub max_output_bytes: usize,
    pub max_work_units: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecoderCapabilities {
    pub max_item_bytes: usize,
    /// Maximum allocation retained inside the decoder between calls.
    pub max_retained_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedItem<T> {
    pub item: T,
    /// Retained allocation charged while this item waits for mapping.
    pub accounted_bytes: usize,
    pub source_byte: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeFailure {
    pub class: String,
    pub source_byte: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecodeState {
    NeedInput,
    OutputReady,
    Finished,
    Failed(DecodeFailure),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodeStep<T> {
    pub consumed_bytes: usize,
    pub work_units: usize,
    pub items: Vec<DecodedItem<T>>,
    pub state: DecodeState,
}

pub trait IncrementalDecoder: Send {
    type Item: Send;
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> DecoderCapabilities;
    fn decode(&mut self, input: &[u8], budget: DecodeBudget) -> DecodeStep<Self::Item>;
    fn finish(&mut self, budget: DecodeBudget) -> DecodeStep<Self::Item>;
}

#[cfg(feature = "source-journal")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    InvalidParser,
    CapacityExceeded,
    InvalidState(String),
}

#[cfg(feature = "source-journal")]
pub trait CheckpointDecoder: IncrementalDecoder {
    fn parser(&self) -> ParserRef;
    fn checkpoint_state(&self, max_bytes: usize) -> Result<Payload, CheckpointError>;
    fn restore_state(
        &mut self,
        parser: &ParserRef,
        state: &[u8],
        max_work_units: usize,
    ) -> Result<(), CheckpointError>;
}

pub trait EventMapper<I>: Send {
    fn map(&mut self, item: I, position: DecodedPosition) -> std::result::Result<NewEvent, String>;
}

impl<I, F> EventMapper<I> for F
where
    F: FnMut(I, DecodedPosition) -> std::result::Result<NewEvent, String> + Send,
{
    fn map(&mut self, item: I, position: DecodedPosition) -> std::result::Result<NewEvent, String> {
        self(item, position)
    }
}

#[derive(Clone, Debug)]
pub struct IngestionConfig {
    pub max_sessions: usize,
    pub max_session_waiters: usize,
    pub session_admission_timeout: Duration,
    pub max_chunk_bytes: usize,
    pub max_retained_input_bytes: usize,
    pub max_decoder_retained_bytes: usize,
    pub max_total_decoder_bytes: usize,
    pub max_output_items_per_step: usize,
    pub max_output_bytes_per_step: usize,
    pub max_work_units_per_step: usize,
    pub drive_budget: DecodeDriveBudget,
    pub max_mapper_error_bytes: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_sessions: 64,
            max_session_waiters: 64,
            session_admission_timeout: Duration::from_secs(5),
            max_chunk_bytes: 64 * 1024,
            max_retained_input_bytes: 1024 * 1024,
            max_decoder_retained_bytes: 1024 * 1024,
            max_total_decoder_bytes: 16 * 1024 * 1024,
            max_output_items_per_step: 128,
            max_output_bytes_per_step: 1024 * 1024,
            max_work_units_per_step: 64 * 1024,
            drive_budget: DecodeDriveBudget {
                max_steps: 1024,
                max_items: 64 * 1024,
                max_output_bytes: 16 * 1024 * 1024,
                max_work_units: 1024 * 1024,
            },
            max_mapper_error_bytes: 256,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IngestionError {
    InvalidConfig(String),
    Overloaded,
    AdmissionTimeout,
    ChunkTooLarge,
    RetainedInputExceeded,
    DecoderContract {
        decoder: &'static str,
        reason: &'static str,
    },
    Decode {
        decoder: &'static str,
        failure: DecodeFailure,
    },
    Mapping {
        detail: String,
    },
    Append {
        error: crate::Error,
        pending_event_id: EventId,
    },
    PendingAppendUnresolved {
        event_id: EventId,
    },
    AlreadyTerminal,
}

impl std::fmt::Display for IngestionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for IngestionError {}

#[derive(Clone)]
pub struct IngestionService<S: EventSink + 'static> {
    sink: Arc<S>,
    config: IngestionConfig,
    sessions: Arc<Semaphore>,
    waiters: Arc<Semaphore>,
    decoder_bytes: Arc<Semaphore>,
}

impl<S: EventSink + 'static> IngestionService<S> {
    pub fn new(sink: Arc<S>, config: IngestionConfig) -> Result<Self, IngestionError> {
        validate_config(&config)?;
        Ok(Self {
            sink,
            sessions: Arc::new(Semaphore::new(config.max_sessions)),
            waiters: Arc::new(Semaphore::new(config.max_session_waiters)),
            decoder_bytes: Arc::new(Semaphore::new(config.max_total_decoder_bytes)),
            config,
        })
    }

    pub fn try_start<D, M>(
        &self,
        stream: StreamKey,
        decoder: D,
        mapper: M,
    ) -> Result<IngestionSession<S, D, M>, IngestionError>
    where
        D: IncrementalDecoder,
        M: EventMapper<D::Item>,
    {
        let decoder_bytes = self.reserve_decoder_bytes(&decoder)?;
        let permit = self
            .sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| IngestionError::Overloaded)?;
        self.build(stream, decoder, mapper, permit, decoder_bytes)
    }

    pub async fn start<D, M>(
        &self,
        stream: StreamKey,
        decoder: D,
        mapper: M,
    ) -> Result<IngestionSession<S, D, M>, IngestionError>
    where
        D: IncrementalDecoder,
        M: EventMapper<D::Item>,
    {
        let decoder_bytes = self.reserve_decoder_bytes(&decoder)?;
        let _waiter = self
            .waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| IngestionError::Overloaded)?;
        let permit = tokio::time::timeout(
            self.config.session_admission_timeout,
            self.sessions.clone().acquire_owned(),
        )
        .await
        .map_err(|_| IngestionError::AdmissionTimeout)?
        .map_err(|_| IngestionError::Overloaded)?;
        self.build(stream, decoder, mapper, permit, decoder_bytes)
    }

    fn build<D, M>(
        &self,
        stream: StreamKey,
        decoder: D,
        mapper: M,
        permit: OwnedSemaphorePermit,
        decoder_bytes: OwnedSemaphorePermit,
    ) -> Result<IngestionSession<S, D, M>, IngestionError>
    where
        D: IncrementalDecoder,
        M: EventMapper<D::Item>,
    {
        if decoder.capabilities().max_item_bytes == 0
            || decoder.capabilities().max_item_bytes > self.config.max_output_bytes_per_step
        {
            return Err(IngestionError::InvalidConfig(
                "decoder item limit must fit one output step".into(),
            ));
        }
        Ok(IngestionSession {
            sink: self.sink.clone(),
            stream,
            decoder: Some(decoder),
            mapper: Some(mapper),
            config: self.config.clone(),
            permit: Some(permit),
            decoder_bytes: Some(decoder_bytes),
            input: Vec::new(),
            input_start: 0,
            source_consumed: 0,
            next_item_index: 0,
            pending_items: VecDeque::new(),
            pending_failure: None,
            pending_event: None,
            terminal: false,
        })
    }

    fn reserve_decoder_bytes<D: IncrementalDecoder>(
        &self,
        decoder: &D,
    ) -> Result<OwnedSemaphorePermit, IngestionError> {
        let bytes = decoder.capabilities().max_retained_bytes;
        if bytes == 0 || bytes > self.config.max_decoder_retained_bytes || bytes > u32::MAX as usize
        {
            return Err(IngestionError::InvalidConfig(
                "decoder retained-state limit is invalid".into(),
            ));
        }
        self.decoder_bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| IngestionError::Overloaded)
    }
}

pub struct IngestionSession<S: EventSink + 'static, D: IncrementalDecoder, M: EventMapper<D::Item>>
{
    sink: Arc<S>,
    stream: StreamKey,
    decoder: Option<D>,
    mapper: Option<M>,
    config: IngestionConfig,
    permit: Option<OwnedSemaphorePermit>,
    decoder_bytes: Option<OwnedSemaphorePermit>,
    input: Vec<u8>,
    input_start: usize,
    source_consumed: u64,
    next_item_index: u64,
    pending_items: VecDeque<DecodedItem<D::Item>>,
    pending_failure: Option<DecodeFailure>,
    pending_event: Option<NewEvent>,
    terminal: bool,
}

impl<S, D, M> IngestionSession<S, D, M>
where
    S: EventSink + 'static,
    D: IncrementalDecoder,
    M: EventMapper<D::Item>,
{
    pub fn pending_event_id(&self) -> Option<&EventId> {
        self.pending_event.as_ref().map(|e| &e.id)
    }
    pub fn retained_input_bytes(&self) -> usize {
        self.input.capacity()
    }

    pub async fn push_chunk(&mut self, chunk: &[u8]) -> Result<(), IngestionError> {
        if let Some(event) = &self.pending_event {
            return Err(IngestionError::PendingAppendUnresolved {
                event_id: event.id.clone(),
            });
        }
        if self.terminal {
            return Err(IngestionError::AlreadyTerminal);
        }
        if chunk.len() > self.config.max_chunk_bytes {
            return Err(IngestionError::ChunkTooLarge);
        }
        self.compact();
        let retained = self
            .input
            .len()
            .checked_add(chunk.len())
            .ok_or(IngestionError::RetainedInputExceeded)?;
        if retained > self.config.max_retained_input_bytes {
            return Err(IngestionError::RetainedInputExceeded);
        }
        self.input.reserve_exact(chunk.len());
        self.input.extend_from_slice(chunk);
        let result = self.drive(false).await;
        self.compact();
        if result.is_err() {
            self.stop();
        }
        result
    }

    pub async fn finish(&mut self) -> Result<(), IngestionError> {
        if let Some(event) = &self.pending_event {
            return Err(IngestionError::PendingAppendUnresolved {
                event_id: event.id.clone(),
            });
        }
        if self.terminal {
            return Err(IngestionError::AlreadyTerminal);
        }
        if let Err(error) = self.drive(true).await {
            self.stop();
            return Err(error);
        }
        if !self.terminal {
            let decoder = self
                .decoder
                .as_ref()
                .map_or("decoder", IncrementalDecoder::name);
            self.stop();
            return Err(IngestionError::DecoderContract {
                decoder,
                reason: "finish did not reach a terminal state",
            });
        }
        self.stop();
        Ok(())
    }

    pub async fn retry_pending(&mut self) -> Result<crate::AppendReceipt, IngestionError> {
        let event = self
            .pending_event
            .clone()
            .ok_or(IngestionError::AlreadyTerminal)?;
        match self.sink.append(&self.stream, event.clone()).await {
            Ok(receipt) => {
                self.pending_event = None;
                Ok(receipt)
            }
            Err(error) => Err(IngestionError::Append {
                error,
                pending_event_id: event.id,
            }),
        }
    }

    async fn drive(&mut self, finishing: bool) -> Result<(), IngestionError> {
        let mut decode_steps = 0usize;
        let mut emitted_items = 0usize;
        let mut emitted_bytes = 0usize;
        let mut work_units = 0usize;
        loop {
            self.commit_pending().await?;
            if let Some(failure) = self.pending_failure.take() {
                self.terminal = true;
                return Err(IngestionError::Decode {
                    decoder: self.decoder.as_ref().expect("active decoder").name(),
                    failure,
                });
            }
            let budget = DecodeBudget {
                max_items: self.config.max_output_items_per_step,
                max_bytes: self.config.max_output_bytes_per_step,
                max_work_units: self.config.max_work_units_per_step,
            };
            let available = &self.input[self.input_start..];
            let decoder = self.decoder.as_mut().expect("active decoder");
            let decoder_name = decoder.name();
            let step = if finishing && available.is_empty() {
                decoder.finish(budget)
            } else if available.is_empty() {
                return Ok(());
            } else {
                decoder.decode(available, budget)
            };
            validate_step(decoder_name, available.len(), budget, &step)?;
            decode_steps = decode_steps
                .checked_add(1)
                .ok_or(IngestionError::DecoderContract {
                    decoder: decoder_name,
                    reason: "drive step accounting overflow",
                })?;
            emitted_items = emitted_items.checked_add(step.items.len()).ok_or(
                IngestionError::DecoderContract {
                    decoder: decoder_name,
                    reason: "drive item accounting overflow",
                },
            )?;
            let step_bytes = step
                .items
                .iter()
                .try_fold(0usize, |sum, item| sum.checked_add(item.accounted_bytes));
            emitted_bytes = emitted_bytes
                .checked_add(step_bytes.ok_or(IngestionError::DecoderContract {
                    decoder: decoder_name,
                    reason: "drive output accounting overflow",
                })?)
                .ok_or(IngestionError::DecoderContract {
                    decoder: decoder_name,
                    reason: "drive output accounting overflow",
                })?;
            work_units =
                work_units
                    .checked_add(step.work_units)
                    .ok_or(IngestionError::DecoderContract {
                        decoder: decoder_name,
                        reason: "drive work accounting overflow",
                    })?;
            let drive_budget = self.config.drive_budget;
            if decode_steps > drive_budget.max_steps
                || emitted_items > drive_budget.max_items
                || emitted_bytes > drive_budget.max_output_bytes
                || work_units > drive_budget.max_work_units
            {
                return Err(IngestionError::DecoderContract {
                    decoder: decoder_name,
                    reason: "cumulative drive budget exceeded",
                });
            }
            self.input_start += step.consumed_bytes;
            self.source_consumed = self
                .source_consumed
                .checked_add(step.consumed_bytes as u64)
                .ok_or(IngestionError::DecoderContract {
                    decoder: decoder_name,
                    reason: "source position overflow",
                })?;
            self.pending_items.extend(step.items);
            match step.state {
                DecodeState::Failed(failure) => self.pending_failure = Some(failure),
                DecodeState::Finished => self.terminal = true,
                DecodeState::NeedInput if self.input_start == self.input.len() => {
                    self.commit_pending().await?;
                    if finishing {
                        continue;
                    }
                    return Ok(());
                }
                DecodeState::NeedInput => {
                    self.commit_pending().await?;
                    return Ok(());
                }
                DecodeState::OutputReady => {}
            }
            self.commit_pending().await?;
            if self.pending_failure.is_some() {
                continue;
            }
            if self.terminal {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    }

    async fn commit_pending(&mut self) -> Result<(), IngestionError> {
        if let Some(event) = &self.pending_event {
            return Err(IngestionError::PendingAppendUnresolved {
                event_id: event.id.clone(),
            });
        }
        while let Some(decoded) = self.pending_items.pop_front() {
            let position = DecodedPosition {
                source_byte: decoded.source_byte,
                item_index: self.next_item_index,
            };
            self.next_item_index =
                self.next_item_index
                    .checked_add(1)
                    .ok_or(IngestionError::DecoderContract {
                        decoder: self.decoder.as_ref().expect("active decoder").name(),
                        reason: "item position overflow",
                    })?;
            let event = self
                .mapper
                .as_mut()
                .expect("active mapper")
                .map(decoded.item, position)
                .map_err(|detail| IngestionError::Mapping {
                    detail: bounded_detail(detail, self.config.max_mapper_error_bytes),
                })?;
            self.pending_event = Some(event.clone());
            match self.sink.append(&self.stream, event.clone()).await {
                Ok(_) => self.pending_event = None,
                Err(error) => {
                    self.terminal = true;
                    return Err(IngestionError::Append {
                        error,
                        pending_event_id: event.id,
                    });
                }
            }
        }
        Ok(())
    }

    fn compact(&mut self) {
        if self.input_start == 0 {
            return;
        }
        self.input.drain(..self.input_start);
        self.input_start = 0;
        self.input.shrink_to_fit();
    }

    fn stop(&mut self) {
        self.terminal = true;
        self.input.clear();
        self.input.shrink_to_fit();
        self.input_start = 0;
        self.pending_items = VecDeque::new();
        self.pending_failure = None;
        self.decoder.take();
        self.mapper.take();
        self.decoder_bytes.take();
        self.permit.take();
    }
}

fn validate_config(config: &IngestionConfig) -> Result<(), IngestionError> {
    if [
        config.max_sessions,
        config.max_session_waiters,
        config.max_chunk_bytes,
        config.max_retained_input_bytes,
        config.max_decoder_retained_bytes,
        config.max_total_decoder_bytes,
        config.max_output_items_per_step,
        config.max_output_bytes_per_step,
        config.max_work_units_per_step,
        config.drive_budget.max_steps,
        config.drive_budget.max_items,
        config.drive_budget.max_output_bytes,
        config.drive_budget.max_work_units,
        config.max_mapper_error_bytes,
    ]
    .contains(&0)
        || config.session_admission_timeout.is_zero()
    {
        return Err(IngestionError::InvalidConfig(
            "ingestion limits must be nonzero".into(),
        ));
    }
    if config.max_chunk_bytes > config.max_retained_input_bytes {
        return Err(IngestionError::InvalidConfig(
            "one chunk must fit retained input".into(),
        ));
    }
    if config.max_output_items_per_step > config.drive_budget.max_items
        || config.max_output_bytes_per_step > config.drive_budget.max_output_bytes
        || config.max_work_units_per_step > config.drive_budget.max_work_units
    {
        return Err(IngestionError::InvalidConfig(
            "one decoder step must fit the cumulative drive budget".into(),
        ));
    }
    if config.max_decoder_retained_bytes > config.max_total_decoder_bytes
        || config.max_total_decoder_bytes > u32::MAX as usize
    {
        return Err(IngestionError::InvalidConfig(
            "decoder byte limits do not fit the global budget".into(),
        ));
    }
    Ok(())
}

fn validate_step<T>(
    decoder: &'static str,
    input_len: usize,
    budget: DecodeBudget,
    step: &DecodeStep<T>,
) -> Result<(), IngestionError> {
    if step.consumed_bytes > input_len {
        return Err(IngestionError::DecoderContract {
            decoder,
            reason: "consumed beyond supplied input",
        });
    }
    if step.work_units > budget.max_work_units {
        return Err(IngestionError::DecoderContract {
            decoder,
            reason: "work budget exceeded",
        });
    }
    if step.items.len() > budget.max_items {
        return Err(IngestionError::DecoderContract {
            decoder,
            reason: "item count budget exceeded",
        });
    }
    let bytes = step
        .items
        .iter()
        .try_fold(0usize, |sum, item| sum.checked_add(item.accounted_bytes))
        .ok_or(IngestionError::DecoderContract {
            decoder,
            reason: "output byte accounting overflow",
        })?;
    if bytes > budget.max_bytes {
        return Err(IngestionError::DecoderContract {
            decoder,
            reason: "output byte budget exceeded",
        });
    }
    if step.consumed_bytes == 0
        && step.items.is_empty()
        && matches!(step.state, DecodeState::OutputReady)
    {
        return Err(IngestionError::DecoderContract {
            decoder,
            reason: "decoder made no progress",
        });
    }
    if matches!(step.state, DecodeState::Finished) && step.consumed_bytes != input_len {
        return Err(IngestionError::DecoderContract {
            decoder,
            reason: "decoder finished with unconsumed input",
        });
    }
    Ok(())
}

fn bounded_detail(mut detail: String, max: usize) -> String {
    if detail.len() > max {
        let mut boundary = max;
        while boundary > 0 && !detail.is_char_boundary(boundary) {
            boundary -= 1;
        }
        detail.truncate(boundary);
    }
    detail
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrLfPolicy {
    PreserveCarriageReturn,
    StripCarriageReturn,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalLinePolicy {
    RejectUnterminated,
    EmitUnterminated,
}

#[derive(Clone, Debug)]
pub struct NewlineFramerConfig {
    pub max_frame_bytes: usize,
    pub emit_empty_frames: bool,
    pub crlf: CrLfPolicy,
    pub final_line: FinalLinePolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteFrame(Arc<[u8]>);
impl ByteFrame {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
    pub fn into_payload(self) -> Payload {
        Payload::copy_from_slice(&self.0)
    }
}

pub struct NewlineFramer {
    config: NewlineFramerConfig,
    partial: Vec<u8>,
    frame_start: u64,
    position: u64,
    terminal: bool,
    finished: bool,
}

impl NewlineFramer {
    pub fn new(config: NewlineFramerConfig) -> Result<Self, IngestionError> {
        if config.max_frame_bytes == 0 {
            return Err(IngestionError::InvalidConfig(
                "frame limit must be nonzero".into(),
            ));
        }
        Ok(Self {
            config,
            partial: Vec::new(),
            frame_start: 0,
            position: 0,
            terminal: false,
            finished: false,
        })
    }

    fn failed(
        &mut self,
        class: &str,
        at: u64,
        consumed: usize,
        work: usize,
        items: Vec<DecodedItem<ByteFrame>>,
    ) -> DecodeStep<ByteFrame> {
        self.terminal = true;
        DecodeStep {
            consumed_bytes: consumed,
            work_units: work,
            items,
            state: DecodeState::Failed(DecodeFailure {
                class: class.into(),
                source_byte: Some(at),
            }),
        }
    }
}

impl IncrementalDecoder for NewlineFramer {
    type Item = ByteFrame;
    fn name(&self) -> &'static str {
        "newline-bytes"
    }
    fn capabilities(&self) -> DecoderCapabilities {
        DecoderCapabilities {
            max_item_bytes: self.config.max_frame_bytes,
            max_retained_bytes: self.config.max_frame_bytes,
        }
    }

    fn decode(&mut self, input: &[u8], budget: DecodeBudget) -> DecodeStep<Self::Item> {
        if self.terminal {
            return DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: Vec::new(),
                state: DecodeState::Failed(DecodeFailure {
                    class: "already_terminal".into(),
                    source_byte: Some(self.position),
                }),
            };
        }
        let mut consumed = 0usize;
        let mut work = 0usize;
        let mut output_bytes = 0usize;
        let mut items = Vec::new();
        while consumed < input.len() && work < budget.max_work_units {
            let byte = input[consumed];
            work += 1;
            if byte == b'\n' {
                let strip_cr = self.config.crlf == CrLfPolicy::StripCarriageReturn
                    && self.partial.last() == Some(&b'\r');
                let frame_len = self.partial.len() - usize::from(strip_cr);
                let emits = self.config.emit_empty_frames || frame_len > 0;
                if emits
                    && (items.len() == budget.max_items
                        || output_bytes
                            .checked_add(frame_len)
                            .is_none_or(|n| n > budget.max_bytes))
                {
                    break;
                }
                if strip_cr {
                    self.partial.pop();
                }
                if emits {
                    let bytes: Arc<[u8]> = Arc::from(self.partial.as_slice());
                    output_bytes += bytes.len();
                    items.push(DecodedItem {
                        accounted_bytes: bytes.len(),
                        item: ByteFrame(bytes),
                        source_byte: self.frame_start,
                    });
                }
                self.partial.clear();
                consumed += 1;
                self.position += 1;
                self.frame_start = self.position;
                continue;
            }
            if self.partial.len() >= self.config.max_frame_bytes {
                let at = self.position;
                return self.failed("frame_too_large", at, consumed, work, items);
            }
            self.partial.push(byte);
            consumed += 1;
            self.position += 1;
        }
        let state = if consumed < input.len() {
            DecodeState::OutputReady
        } else {
            DecodeState::NeedInput
        };
        DecodeStep {
            consumed_bytes: consumed,
            work_units: work,
            items,
            state,
        }
    }

    fn finish(&mut self, budget: DecodeBudget) -> DecodeStep<Self::Item> {
        if self.finished {
            return DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: Vec::new(),
                state: DecodeState::Finished,
            };
        }
        if self.terminal {
            return DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: Vec::new(),
                state: DecodeState::Failed(DecodeFailure {
                    class: "already_terminal".into(),
                    source_byte: Some(self.position),
                }),
            };
        }
        if self.partial.is_empty() {
            self.terminal = true;
            self.finished = true;
            return DecodeStep {
                consumed_bytes: 0,
                work_units: 0,
                items: Vec::new(),
                state: DecodeState::Finished,
            };
        }
        match self.config.final_line {
            FinalLinePolicy::RejectUnterminated => {
                self.failed("truncated_input", self.position, 0, 0, Vec::new())
            }
            FinalLinePolicy::EmitUnterminated => {
                if budget.max_items == 0 || self.partial.len() > budget.max_bytes {
                    return DecodeStep {
                        consumed_bytes: 0,
                        work_units: 0,
                        items: Vec::new(),
                        state: DecodeState::OutputReady,
                    };
                }
                let bytes: Arc<[u8]> = Arc::from(self.partial.as_slice());
                let item = DecodedItem {
                    accounted_bytes: bytes.len(),
                    item: ByteFrame(bytes),
                    source_byte: self.frame_start,
                };
                self.partial.clear();
                self.frame_start = self.position;
                self.terminal = true;
                self.finished = true;
                DecodeStep {
                    consumed_bytes: 0,
                    work_units: 0,
                    items: vec![item],
                    state: DecodeState::Finished,
                }
            }
        }
    }
}

#[cfg(feature = "source-journal")]
impl CheckpointDecoder for NewlineFramer {
    fn parser(&self) -> ParserRef {
        let id = format!(
            "newline-v1:max={}:empty={}:crlf={}:eof={}",
            self.config.max_frame_bytes,
            u8::from(self.config.emit_empty_frames),
            match self.config.crlf {
                CrLfPolicy::PreserveCarriageReturn => "keep",
                CrLfPolicy::StripCarriageReturn => "strip",
            },
            match self.config.final_line {
                FinalLinePolicy::RejectUnterminated => "reject",
                FinalLinePolicy::EmitUnterminated => "emit",
            }
        );
        ParserRef {
            id: ParserId::new(id).expect("newline parser identity is bounded"),
            version: 1,
        }
    }

    fn checkpoint_state(&self, max_bytes: usize) -> Result<Payload, CheckpointError> {
        if self.terminal && !self.finished {
            return Err(CheckpointError::InvalidState(
                "terminal decoder state cannot be resumed".into(),
            ));
        }
        let size = 5usize
            .checked_add(8 + 8 + 8)
            .and_then(|value| value.checked_add(self.partial.len()))
            .ok_or(CheckpointError::CapacityExceeded)?;
        if size > max_bytes {
            return Err(CheckpointError::CapacityExceeded);
        }
        let mut state = Vec::with_capacity(size);
        state.extend_from_slice(if self.finished { b"NLCF1" } else { b"NLCP1" });
        state.extend_from_slice(&self.frame_start.to_be_bytes());
        state.extend_from_slice(&self.position.to_be_bytes());
        state.extend_from_slice(&(self.partial.len() as u64).to_be_bytes());
        state.extend_from_slice(&self.partial);
        Ok(Payload::copy_from_slice(&state))
    }

    fn restore_state(
        &mut self,
        parser: &ParserRef,
        state: &[u8],
        max_work_units: usize,
    ) -> Result<(), CheckpointError> {
        if parser != &self.parser() {
            return Err(CheckpointError::InvalidParser);
        }
        const HEADER: usize = 5 + 8 + 8 + 8;
        if state.len() < HEADER || (&state[..5] != b"NLCP1" && &state[..5] != b"NLCF1") {
            return Err(CheckpointError::InvalidState(
                "invalid newline checkpoint header".into(),
            ));
        }
        let finished = &state[..5] == b"NLCF1";
        let frame_start = u64::from_be_bytes(state[5..13].try_into().unwrap());
        let position = u64::from_be_bytes(state[13..21].try_into().unwrap());
        let partial_len = usize::try_from(u64::from_be_bytes(state[21..29].try_into().unwrap()))
            .map_err(|_| CheckpointError::InvalidState("checkpoint length overflow".into()))?;
        let expected_len = HEADER
            .checked_add(partial_len)
            .ok_or_else(|| CheckpointError::InvalidState("checkpoint length overflow".into()))?;
        if (finished && (partial_len != 0 || frame_start != position))
            || partial_len > self.config.max_frame_bytes
            || partial_len > max_work_units
            || state.len() != expected_len
            || position < frame_start
            || position - frame_start != partial_len as u64
            || state[HEADER..].contains(&b'\n')
        {
            return Err(CheckpointError::InvalidState(
                "invalid newline checkpoint positions".into(),
            ));
        }
        self.partial.clear();
        self.partial.extend_from_slice(&state[HEADER..]);
        self.frame_start = frame_start;
        self.position = position;
        self.terminal = finished;
        self.finished = finished;
        Ok(())
    }
}
