use super::{
    bounded_detail, validate_step, CheckpointDecoder, CheckpointError, DecodeBudget, DecodeState,
    EventMapper,
};
use crate::{
    BeginSource, BeginSourceReceipt, JournalError, JournaledOutput, ParserCheckpoint,
    RawPageLimits, Runtime, SourceBinding, SourceJournalStore,
};
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalDriveConfig {
    pub max_sessions: usize,
    pub max_session_bytes: usize,
    pub raw_page: RawPageLimits,
    pub decode: DecodeBudget,
    pub max_steps: usize,
    pub max_output_commits: usize,
    pub max_checkpoint_state_bytes: usize,
    pub max_restore_work_units: usize,
    pub max_mapping_error_bytes: usize,
}

impl Default for JournalDriveConfig {
    fn default() -> Self {
        Self {
            max_sessions: 8,
            max_session_bytes: 16 * 1024 * 1024,
            raw_page: RawPageLimits {
                max_segments: 16,
                max_bytes: 64 * 1024,
            },
            decode: DecodeBudget {
                max_items: 128,
                max_bytes: 1024 * 1024,
                max_work_units: 64 * 1024,
            },
            max_steps: 1024,
            max_output_commits: 64 * 1024,
            max_checkpoint_state_bytes: 64 * 1024,
            max_restore_work_units: 64 * 1024,
            max_mapping_error_bytes: 256,
        }
    }
}

impl JournalDriveConfig {
    pub fn validate(&self) -> Result<(), JournalIngestionError> {
        if self.raw_page.max_segments == 0
            || self.raw_page.max_bytes == 0
            || self.decode.max_items == 0
            || self.decode.max_bytes == 0
            || self.decode.max_work_units == 0
            || self.max_steps == 0
            || self.max_output_commits == 0
            || self.max_checkpoint_state_bytes == 0
            || self.max_restore_work_units == 0
            || self.max_mapping_error_bytes == 0
            || self.max_sessions == 0
            || self.max_sessions > Semaphore::MAX_PERMITS
            || self.max_session_bytes == 0
            || self.max_session_bytes > u32::MAX as usize
        {
            return Err(JournalIngestionError::InvalidConfig(
                "journal drive limits must be nonzero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalDriveProgress {
    pub captured_offset: u64,
    pub next_item_index: u64,
    pub committed_outputs: usize,
    pub complete_capture: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalIngestionError {
    InvalidConfig(String),
    BindingMismatch,
    SourceNotSealed,
    Overloaded,
    DecoderContract(&'static str),
    Checkpoint(CheckpointError),
    Mapping(String),
    Journal(JournalError),
    WorkLimitReached(JournalDriveProgress),
}

impl std::fmt::Display for JournalIngestionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(error) => write!(formatter, "{error}"),
            Self::Mapping(detail) => write!(formatter, "mapping failed: {detail}"),
            other => write!(formatter, "{other:?}"),
        }
    }
}

impl std::error::Error for JournalIngestionError {}

impl From<JournalError> for JournalIngestionError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

#[derive(Clone)]
pub struct JournalIngestionService<S: SourceJournalStore> {
    runtime: Runtime<S>,
    config: JournalDriveConfig,
    sessions: Arc<Semaphore>,
    session_bytes: Arc<Semaphore>,
}

impl<S: SourceJournalStore> JournalIngestionService<S> {
    pub fn new(
        runtime: Runtime<S>,
        config: JournalDriveConfig,
    ) -> Result<Self, JournalIngestionError> {
        config.validate()?;
        Ok(Self {
            runtime,
            sessions: Arc::new(Semaphore::new(config.max_sessions)),
            session_bytes: Arc::new(Semaphore::new(config.max_session_bytes)),
            config,
        })
    }

    pub async fn begin_source(
        &self,
        request: BeginSource,
    ) -> Result<BeginSourceReceipt, JournalIngestionError> {
        Ok(self.runtime.begin_source(request).await?)
    }

    /// Drives only bytes already acknowledged by the source journal. Source acknowledgement may
    /// happen after `capture_segment` succeeds; decoded output is never produced from uncaptured
    /// input.
    pub async fn recover_captured<D, M>(
        &self,
        binding: &SourceBinding,
        decoder: D,
        mapper: M,
    ) -> Result<JournalDriveProgress, JournalIngestionError>
    where
        D: CheckpointDecoder,
        M: EventMapper<D::Item>,
    {
        self.drive_captured(binding, decoder, mapper, None).await
    }

    async fn drive_captured<D, M>(
        &self,
        binding: &SourceBinding,
        mut decoder: D,
        mut mapper: M,
        sealed_end: Option<u64>,
    ) -> Result<JournalDriveProgress, JournalIngestionError>
    where
        D: CheckpointDecoder,
        M: EventMapper<D::Item>,
    {
        let session_charge = self
            .config
            .raw_page
            .max_bytes
            .checked_add(self.config.decode.max_bytes)
            .and_then(|value| value.checked_add(self.config.max_checkpoint_state_bytes))
            .and_then(|value| value.checked_add(decoder.capabilities().max_retained_bytes))
            .and_then(|value| value.checked_add(1024))
            .ok_or(JournalIngestionError::InvalidConfig(
                "journal session byte charge overflow".into(),
            ))?;
        if session_charge > self.config.max_session_bytes || session_charge > u32::MAX as usize {
            return Err(JournalIngestionError::InvalidConfig(
                "one journal session exceeds its global byte budget".into(),
            ));
        }
        let _session = self
            .sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| JournalIngestionError::Overloaded)?;
        let _session_bytes = self
            .session_bytes
            .clone()
            .try_acquire_many_owned(session_charge as u32)
            .map_err(|_| JournalIngestionError::Overloaded)?;
        let status = self.runtime.source_status(&binding.source).await?;
        if status.binding != *binding || decoder.parser() != binding.parser {
            return Err(JournalIngestionError::BindingMismatch);
        }
        let checkpoint = self.runtime.latest_checkpoint(&binding.source).await?;
        let restored_checkpoint = checkpoint.is_some();
        let (mut offset, mut next_item_index, mut committed_output) =
            if let Some(checkpoint) = checkpoint {
                if checkpoint.parser != binding.parser
                    || checkpoint.output_stream != binding.output_stream
                    || checkpoint.source.source != binding.source
                {
                    return Err(JournalIngestionError::BindingMismatch);
                }
                decoder
                    .restore_state(
                        &checkpoint.parser,
                        checkpoint.state.as_bytes(),
                        self.config.max_restore_work_units,
                    )
                    .map_err(JournalIngestionError::Checkpoint)?;
                (
                    checkpoint.source.offset,
                    checkpoint.next_item_index,
                    checkpoint.committed_output,
                )
            } else {
                (0, 0, None)
            };
        let mut steps = 0usize;
        let mut committed_outputs = 0usize;
        // A saved decoder state can contain output that was not yet emitted. Probe it before
        // declaring an empty captured tail complete.
        let mut drain_decoder_output = restored_checkpoint;
        loop {
            if steps == self.config.max_steps {
                return Err(JournalIngestionError::WorkLimitReached(
                    JournalDriveProgress {
                        captured_offset: offset,
                        next_item_index,
                        committed_outputs,
                        complete_capture: false,
                    },
                ));
            }
            let finishing = sealed_end == Some(offset);
            let page = if drain_decoder_output || finishing {
                None
            } else {
                Some(
                    self.runtime
                        .read_captured(&binding.source, offset, self.config.raw_page)
                        .await?,
                )
            };
            if let Some(page) = &page {
                if page.bytes.is_empty() {
                    if sealed_end.is_some() {
                        return Err(JournalIngestionError::DecoderContract(
                            "captured input ended before its sealed boundary",
                        ));
                    }
                    return Ok(JournalDriveProgress {
                        captured_offset: offset,
                        next_item_index,
                        committed_outputs,
                        complete_capture: page.complete,
                    });
                }
            }
            let input = page.as_ref().map_or(&[][..], |page| page.bytes.as_bytes());
            let remaining_outputs = self.config.max_output_commits - committed_outputs;
            if remaining_outputs == 0 {
                return Err(JournalIngestionError::WorkLimitReached(
                    JournalDriveProgress {
                        captured_offset: offset,
                        next_item_index,
                        committed_outputs,
                        complete_capture: false,
                    },
                ));
            }
            let decode_budget = DecodeBudget {
                max_items: self.config.decode.max_items.min(remaining_outputs),
                ..self.config.decode
            };
            let step = if finishing {
                decoder.finish(decode_budget)
            } else {
                decoder.decode(input, decode_budget)
            };
            validate_step(decoder.name(), input.len(), decode_budget, &step)
                .map_err(|_| JournalIngestionError::DecoderContract("invalid decode step"))?;
            let finished = finishing && matches!(step.state, DecodeState::Finished);
            if finishing && matches!(step.state, DecodeState::NeedInput) {
                return Err(JournalIngestionError::DecoderContract(
                    "finish requested more input after source seal",
                ));
            }
            if step.consumed_bytes == 0
                && step.items.is_empty()
                && !finished
                && !(finishing
                    && step.work_units > 0
                    && matches!(step.state, DecodeState::OutputReady))
            {
                if drain_decoder_output && matches!(step.state, DecodeState::NeedInput) {
                    drain_decoder_output = false;
                    continue;
                }
                return Err(JournalIngestionError::DecoderContract(
                    "decoder made no progress over captured bytes",
                ));
            }
            let next_offset = offset.checked_add(step.consumed_bytes as u64).ok_or(
                JournalIngestionError::DecoderContract("captured position overflow"),
            )?;
            let output_ready = matches!(step.state, DecodeState::OutputReady);
            if committed_outputs
                .checked_add(step.items.len())
                .is_none_or(|total| total > self.config.max_output_commits)
            {
                return Err(JournalIngestionError::WorkLimitReached(
                    JournalDriveProgress {
                        captured_offset: offset,
                        next_item_index,
                        committed_outputs,
                        complete_capture: false,
                    },
                ));
            }
            for decoded in step.items {
                let position = crate::DecodedPosition {
                    source_byte: decoded.source_byte,
                    item_index: next_item_index,
                };
                let event = mapper.map(decoded.item, position).map_err(|detail| {
                    JournalIngestionError::Mapping(bounded_detail(
                        detail,
                        self.config.max_mapping_error_bytes,
                    ))
                })?;
                let receipt = self
                    .runtime
                    .append_captured(
                        &binding.output_stream,
                        JournaledOutput {
                            source: binding.source.clone(),
                            position,
                            event,
                        },
                    )
                    .await?;
                next_item_index = next_item_index.checked_add(1).ok_or(
                    JournalIngestionError::DecoderContract("output item index overflow"),
                )?;
                committed_outputs += 1;
                committed_output = Some(receipt.record.cursor.clone());
            }
            if matches!(step.state, DecodeState::Failed(_)) {
                return Err(JournalIngestionError::DecoderContract("decoder failed"));
            }
            let state = decoder
                .checkpoint_state(self.config.max_checkpoint_state_bytes)
                .map_err(JournalIngestionError::Checkpoint)?;
            self.runtime
                .publish_parser_checkpoint(ParserCheckpoint {
                    source: crate::SourcePosition {
                        source: binding.source.clone(),
                        offset: next_offset,
                    },
                    parser: binding.parser.clone(),
                    state,
                    next_item_index,
                    output_stream: binding.output_stream.clone(),
                    committed_output: committed_output.clone(),
                })
                .await?;
            offset = next_offset;
            steps += 1;
            drain_decoder_output = output_ready;
            if finished
                || (sealed_end.is_none()
                    && page.as_ref().is_some_and(|page| page.complete)
                    && page.as_ref().is_some_and(|page| offset == page.next_offset)
                    && !output_ready)
            {
                return Ok(JournalDriveProgress {
                    captured_offset: offset,
                    next_item_index,
                    committed_outputs,
                    complete_capture: true,
                });
            }
            if committed_outputs == self.config.max_output_commits {
                return Err(JournalIngestionError::WorkLimitReached(
                    JournalDriveProgress {
                        captured_offset: offset,
                        next_item_index,
                        committed_outputs,
                        complete_capture: false,
                    },
                ));
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalFinishProgress {
    pub recovery: JournalDriveProgress,
    pub parser_finished: bool,
}

impl<S: crate::SourceFinalizationStore> JournalIngestionService<S> {
    /// Finish a durably sealed source. An empty current capture alone is not EOF.
    /// Repeated calls reconcile committed output/checkpoints before marking completion.
    pub async fn finish_captured<D, M>(
        &self,
        binding: &SourceBinding,
        decoder: D,
        mapper: M,
    ) -> Result<JournalFinishProgress, JournalIngestionError>
    where
        D: CheckpointDecoder,
        M: EventMapper<D::Item>,
    {
        let finalization = self.runtime.source_finalization(&binding.source).await?;
        // Read progress after the finished flag so a concurrent completion cannot
        // pair a new flag with an older checkpoint observation.
        let status = self.runtime.source_status(&binding.source).await?;
        if status.binding != *binding || decoder.parser() != binding.parser {
            return Err(JournalIngestionError::BindingMismatch);
        }
        let end = finalization
            .sealed_end
            .ok_or(JournalIngestionError::SourceNotSealed)?;
        if finalization.parser_finished {
            return Ok(JournalFinishProgress {
                recovery: JournalDriveProgress {
                    captured_offset: status.checkpoint_offset,
                    next_item_index: status.next_item_index,
                    committed_outputs: 0,
                    complete_capture: true,
                },
                parser_finished: true,
            });
        }
        let recovery = self
            .drive_captured(binding, decoder, mapper, Some(end))
            .await?;
        let checkpoint = self
            .runtime
            .latest_checkpoint(&binding.source)
            .await?
            .ok_or(JournalIngestionError::DecoderContract(
                "finished decoder has no checkpoint",
            ))?;
        self.runtime
            .finish_source(crate::FinishSource { checkpoint })
            .await?;
        Ok(JournalFinishProgress {
            recovery,
            parser_finished: true,
        })
    }
}
