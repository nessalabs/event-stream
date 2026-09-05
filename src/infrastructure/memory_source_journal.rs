use super::memory::{MemoryStore, State, StreamEntry};
use crate::{application::*, domain::*};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const RECEIPT_OVERHEAD: usize = 160;
const MARKER_OVERHEAD: u64 = 192;
const SEGMENT_OVERHEAD: usize = 128;

#[derive(Debug)]
pub(super) struct MemoryJournalSource {
    binding: SourceBinding,
    captured_end: u64,
    sealed_end: Option<u64>,
    parser_finished: bool,
    receipt_floor: u64,
    segments: BTreeMap<u64, Payload>,
    receipts: BTreeMap<u64, CaptureReceipt>,
    markers: BTreeMap<u64, OutputMarker>,
    next_item_index: u64,
    last_marker_source_byte: Option<u64>,
    checkpoint: Option<ParserCheckpoint>,
}

#[derive(Clone, Debug)]
pub(super) enum MemoryJournalReceipt {
    Begin(BeginSourceReceipt),
    ReceiptFloor(AdvanceCaptureReceiptFloorReceipt),
}

impl MemoryStore {
    fn journal_lock(&self) -> JournalResult<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| JournalError::CorruptStorage("memory store lock poisoned".into()))
    }

    fn journal_source<'a>(
        state: &'a State,
        source: &SourceKey,
    ) -> JournalResult<&'a MemoryJournalSource> {
        if state.closed {
            return Err(JournalError::Closed);
        }
        state
            .journal_sources
            .get(source)
            .ok_or_else(|| JournalError::StaleSource {
                current: state
                    .current_journal_sources
                    .get(&source.id)
                    .cloned()
                    .map(Box::new),
            })
    }

    fn progress(source: &MemoryJournalSource) -> SourceProgress {
        let checkpoint = source.checkpoint.as_ref();
        SourceProgress {
            binding: source.binding.clone(),
            captured_end: source.captured_end,
            capture_receipt_floor: source.receipt_floor,
            checkpoint_offset: checkpoint.map_or(0, |value| value.source.offset),
            next_item_index: checkpoint.map_or(0, |value| value.next_item_index),
            committed_output: checkpoint.and_then(|value| value.committed_output.clone()),
        }
    }

    fn capture_digest(bytes: &[u8]) -> CaptureDigest {
        CaptureDigest(Sha256::digest(bytes).into())
    }

    fn checkpoint_charge(checkpoint: &ParserCheckpoint) -> Option<u64> {
        u64::try_from(checkpoint.state.len())
            .ok()?
            .checked_add(256)?
            .checked_add(checkpoint.source.source.id.as_str().len() as u64)?
            .checked_add(checkpoint.parser.id.as_str().len() as u64)?
            .checked_add(checkpoint.output_stream.id.as_str().len() as u64)?
            .checked_add(
                checkpoint
                    .committed_output
                    .as_ref()
                    .map_or(0, |cursor| cursor.stream.id.as_str().len() as u64),
            )
    }

    fn capture_receipt_charge(source: &SourceKey) -> Option<usize> {
        source
            .id
            .as_str()
            .len()
            .checked_mul(2)?
            .checked_add(RECEIPT_OVERHEAD)
    }

    pub(super) fn minimum_journal_generation(
        state: &State,
        stream: &StreamKey,
    ) -> Option<RetryGeneration> {
        state
            .journal_generation_pins
            .get(stream)
            .and_then(|generations| generations.first_key_value().map(|(value, _)| *value))
    }

    fn enqueue_journal_cleanup(state: &mut State, source: &SourceKey) {
        if state.journal_cleanup_set.insert(source.clone()) {
            state.journal_cleanup_queue.push_back(source.clone());
        }
    }

    fn marker_charge(binding: &SourceBinding, marker: &OutputMarker) -> Option<u64> {
        MARKER_OVERHEAD
            .checked_add(marker.event_id.as_str().len() as u64)?
            .checked_add(binding.source.id.as_str().len() as u64)?
            .checked_add(binding.output_stream.id.as_str().len() as u64)
    }

    fn output_error(
        error: RetentionError,
        output_stream: &StreamKey,
        output: &JournaledOutput,
    ) -> JournalError {
        match error {
            RetentionError::Closed => JournalError::Closed,
            RetentionError::CapacityExceeded => JournalError::CapacityExceeded,
            RetentionError::RetryGenerationExpired { .. }
            | RetentionError::LegacyRetryNotFound { .. } => JournalError::RetryGenerationExpired,
            RetentionError::IdempotencyConflict { .. } => JournalError::OutputConflict {
                source: output.source.clone(),
                item_index: output.position.item_index,
            },
            RetentionError::GeneratedAppendUnknown(_) => JournalError::OutputUnknown {
                output_stream: Box::new(output_stream.clone()),
                output: Box::new(output.clone()),
            },
            RetentionError::CorruptStorage(detail) => JournalError::CorruptStorage(detail),
            other => JournalError::StorageFailure(other.to_string()),
        }
    }
}

#[async_trait]
impl SourceJournalStore for MemoryStore {
    async fn begin_source(&self, request: BeginSource) -> JournalResult<BeginSourceReceipt> {
        let mut state = self.journal_lock()?;
        if let Some(prior) = state.journal_operation_receipts.get(&request.operation_id) {
            return match prior {
                MemoryJournalReceipt::Begin(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(JournalError::OperationConflict {
                    operation_id: request.operation_id,
                }),
            };
        }
        if state.operation_ids.contains(request.operation_id.as_str()) {
            return Err(JournalError::OperationConflict {
                operation_id: request.operation_id,
            });
        }
        if state.journal_sources.contains_key(&request.binding.source) {
            return Err(JournalError::SourceBindingConflict {
                source: request.binding.source,
            });
        }
        let receipt_charge = request
            .operation_id
            .as_str()
            .len()
            .checked_add(request.binding.source.id.as_str().len().saturating_mul(4))
            .and_then(|value| value.checked_add(request.binding.parser.id.as_str().len()))
            .and_then(|value| {
                value.checked_add(
                    request
                        .binding
                        .output_stream
                        .id
                        .as_str()
                        .len()
                        .saturating_mul(2),
                )
            })
            .and_then(|value| value.checked_add(RECEIPT_OVERHEAD))
            .ok_or(JournalError::CapacityExceeded)?;
        let config = &self.options.source_journal;
        if state.journal_sources.len() >= config.storage.max_sources
            || state.journal_receipt_rows >= config.receipts.max_source_receipts
            || state
                .journal_receipt_bytes
                .checked_add(receipt_charge)
                .is_none_or(|value| value > config.receipts.max_receipt_bytes)
        {
            return Err(JournalError::CapacityExceeded);
        }
        let source = MemoryJournalSource {
            binding: request.binding.clone(),
            captured_end: 0,
            sealed_end: None,
            parser_finished: false,
            receipt_floor: 0,
            segments: BTreeMap::new(),
            receipts: BTreeMap::new(),
            markers: BTreeMap::new(),
            next_item_index: 0,
            last_marker_source_byte: None,
            checkpoint: None,
        };
        let receipt = BeginSourceReceipt {
            request: request.clone(),
            progress: Self::progress(&source),
        };
        state.current_journal_sources.insert(
            request.binding.source.id.clone(),
            request.binding.source.clone(),
        );
        state
            .journal_sources
            .insert(request.binding.source.clone(), source);
        state.journal_receipt_bytes += receipt_charge;
        state.journal_receipt_rows += 1;
        state.journal_operation_receipts.insert(
            request.operation_id.clone(),
            MemoryJournalReceipt::Begin(receipt.clone()),
        );
        state
            .operation_ids
            .insert(request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn capture_segment(&self, segment: RawSegment) -> JournalResult<CaptureReceipt> {
        if segment.bytes.is_empty()
            || segment.bytes.len() > self.options.source_journal.storage.max_segment_bytes
        {
            return Err(JournalError::InvalidInput(
                "captured segment size is outside configured bounds".into(),
            ));
        }
        let end = segment
            .start
            .offset
            .checked_add(segment.bytes.len() as u64)
            .ok_or_else(|| JournalError::InvalidInput("captured position overflow".into()))?;
        let digest = Self::capture_digest(segment.bytes.as_bytes());
        let mut state = self.journal_lock()?;
        let source = Self::journal_source(&state, &segment.start.source)?;
        if segment.start.offset < source.receipt_floor {
            return Err(JournalError::CaptureReceiptExpired {
                floor: source.receipt_floor,
            });
        }
        if let Some(prior) = source.receipts.get(&segment.start.offset) {
            return if prior.end == end && prior.digest == digest {
                Ok(prior.clone())
            } else {
                Err(JournalError::CaptureConflict {
                    start: segment.start,
                })
            };
        }
        if let Some(end) = source.sealed_end {
            return Err(JournalError::SourceSealed { end });
        }
        if segment.start.offset != source.captured_end {
            return Err(JournalError::CaptureConflict {
                start: segment.start,
            });
        }
        let receipt_charge = Self::capture_receipt_charge(&segment.start.source)
            .ok_or(JournalError::CapacityExceeded)?;
        let config = &self.options.source_journal;
        if state.journal_segment_rows >= config.storage.max_segments
            || state
                .journal_captured_bytes
                .checked_add(segment.bytes.len() as u64)
                .is_none_or(|value| value > config.storage.max_captured_bytes)
            || state.journal_receipt_rows >= config.receipts.max_source_receipts
            || state
                .journal_receipt_bytes
                .checked_add(receipt_charge)
                .is_none_or(|value| value > config.receipts.max_receipt_bytes)
        {
            return Err(JournalError::CapacityExceeded);
        }
        let receipt = CaptureReceipt {
            start: segment.start.clone(),
            end,
            digest,
        };
        let source = state
            .journal_sources
            .get_mut(&segment.start.source)
            .unwrap();
        source
            .segments
            .insert(segment.start.offset, segment.bytes.clone());
        source
            .receipts
            .insert(segment.start.offset, receipt.clone());
        source.captured_end = end;
        state.journal_segment_rows += 1;
        state.journal_captured_bytes += segment.bytes.len() as u64;
        state.journal_receipt_rows += 1;
        state.journal_receipt_bytes += receipt_charge;
        Ok(receipt)
    }

    async fn advance_capture_receipt_floor(
        &self,
        request: AdvanceCaptureReceiptFloor,
    ) -> JournalResult<AdvanceCaptureReceiptFloorReceipt> {
        let mut state = self.journal_lock()?;
        if let Some(prior) = state.journal_operation_receipts.get(&request.operation_id) {
            return match prior {
                MemoryJournalReceipt::ReceiptFloor(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(JournalError::OperationConflict {
                    operation_id: request.operation_id,
                }),
            };
        }
        if state.operation_ids.contains(request.operation_id.as_str()) {
            return Err(JournalError::OperationConflict {
                operation_id: request.operation_id,
            });
        }
        let source = Self::journal_source(&state, &request.source)?;
        if request.expected_floor != source.receipt_floor {
            return Err(JournalError::StaleCaptureReceiptFloor {
                current: source.receipt_floor,
            });
        }
        if request.new_floor < request.expected_floor || request.new_floor > source.captured_end {
            return Err(JournalError::InvalidInput(
                "capture receipt floor is outside captured history".into(),
            ));
        }
        let charge = request
            .operation_id
            .as_str()
            .len()
            .checked_add(request.source.id.as_str().len().saturating_mul(3))
            .and_then(|value| value.checked_add(RECEIPT_OVERHEAD))
            .ok_or(JournalError::CapacityExceeded)?;
        if state.journal_receipt_rows >= self.options.source_journal.receipts.max_source_receipts
            || state
                .journal_receipt_bytes
                .checked_add(charge)
                .is_none_or(|value| value > self.options.source_journal.receipts.max_receipt_bytes)
        {
            return Err(JournalError::CapacityExceeded);
        }
        let source = state.journal_sources.get_mut(&request.source).unwrap();
        source.receipt_floor = request.new_floor;
        let receipt = AdvanceCaptureReceiptFloorReceipt {
            request,
            progress: Self::progress(source),
        };
        Self::enqueue_journal_cleanup(&mut state, &receipt.request.source);
        state.journal_receipt_bytes += charge;
        state.journal_receipt_rows += 1;
        state.journal_operation_receipts.insert(
            receipt.request.operation_id.clone(),
            MemoryJournalReceipt::ReceiptFloor(receipt.clone()),
        );
        state
            .operation_ids
            .insert(receipt.request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn append_captured(
        &self,
        output_stream: &StreamKey,
        output: JournaledOutput,
    ) -> JournalResult<AppendReceipt> {
        let mut state = self.journal_lock()?;
        let source = Self::journal_source(&state, &output.source)?;
        if source.binding.output_stream != *output_stream
            || output.position.source_byte > source.captured_end
        {
            return Err(JournalError::InvalidInput(
                "journal output is outside its immutable binding".into(),
            ));
        }
        if let Some(marker) = source.markers.get(&output.position.item_index).cloned() {
            if marker.position != output.position || marker.event_id != output.event.id {
                return Err(JournalError::OutputConflict {
                    source: output.source,
                    item_index: output.position.item_index,
                });
            }
            let record = match state.streams.get(&output_stream.id) {
                Some(StreamEntry::Active(history)) if history.key == *output_stream => history
                    .generated_event_offsets
                    .get(&(marker.retry_generation, output.event.id.clone()))
                    .and_then(|offset| history.records.get(offset))
                    .cloned(),
                _ => None,
            }
            .ok_or_else(|| {
                JournalError::CorruptStorage("journal marker has no generated retry record".into())
            })?;
            if record.cursor != marker.committed || record.event != output.event {
                return Err(JournalError::CorruptStorage(
                    "journal marker differs from its generated retry record".into(),
                ));
            }
            return Ok(AppendReceipt {
                kind: AppendKind::Deduplicated,
                record,
            });
        }
        if source.parser_finished {
            return Err(JournalError::SourceSealed {
                end: source.captured_end,
            });
        }
        if output.position.item_index != source.next_item_index {
            return Err(JournalError::OutputConflict {
                source: output.source,
                item_index: output.position.item_index,
            });
        }
        if source
            .last_marker_source_byte
            .is_some_and(|previous| output.position.source_byte < previous)
        {
            return Err(JournalError::OutputConflict {
                source: output.source,
                item_index: output.position.item_index,
            });
        }
        let next_item_index = source
            .next_item_index
            .checked_add(1)
            .ok_or(JournalError::CapacityExceeded)?;
        let retry_generation = match state.streams.get(&output_stream.id) {
            Some(StreamEntry::Active(history)) if history.key == *output_stream => {
                match history.retry_policy {
                    RetryPolicyState::Generational { current, .. } => current,
                    RetryPolicyState::Lifetime => {
                        return Err(JournalError::InvalidInput(
                            "journal output requires an enabled retry policy".into(),
                        ));
                    }
                }
            }
            _ => {
                return Err(JournalError::InvalidInput(
                    "journal output stream is unavailable".into(),
                ));
            }
        };
        let marker_charge = MARKER_OVERHEAD
            .checked_add(output.event.id.as_str().len() as u64)
            .and_then(|value| value.checked_add(output.source.id.as_str().len() as u64))
            .and_then(|value| value.checked_add(output_stream.id.as_str().len() as u64))
            .ok_or(JournalError::CapacityExceeded)?;
        let config = &self.options.source_journal.storage;
        if state.journal_marker_rows >= config.max_output_markers
            || state
                .journal_marker_bytes
                .checked_add(marker_charge)
                .is_none_or(|value| value > config.max_marker_bytes)
        {
            return Err(JournalError::CapacityExceeded);
        }
        let event_id = output.event.id.clone();
        let receipt = self
            .append_generated_in_state(
                &mut state,
                output_stream,
                GeneratedEvent {
                    generation: retry_generation,
                    event: output.event.clone(),
                },
            )
            .map_err(|error| Self::output_error(error, output_stream, &output))?;
        let marker = OutputMarker {
            source: output.source.clone(),
            position: output.position,
            retry_generation,
            event_id,
            committed: receipt.record.cursor.clone(),
        };
        let source = state.journal_sources.get_mut(&output.source).unwrap();
        source.markers.insert(output.position.item_index, marker);
        source.next_item_index = next_item_index;
        source.last_marker_source_byte = Some(output.position.source_byte);
        *state
            .journal_generation_pins
            .entry(output_stream.clone())
            .or_default()
            .entry(retry_generation)
            .or_default() += 1;
        state.journal_marker_rows += 1;
        state.journal_marker_bytes += marker_charge;
        Ok(receipt)
    }

    async fn publish_parser_checkpoint(
        &self,
        checkpoint: ParserCheckpoint,
    ) -> JournalResult<CheckpointReceipt> {
        if checkpoint.state.len()
            > self
                .options
                .source_journal
                .storage
                .max_checkpoint_state_bytes
        {
            return Err(JournalError::CapacityExceeded);
        }
        let mut state = self.journal_lock()?;
        let source = Self::journal_source(&state, &checkpoint.source.source)?;
        if checkpoint.parser != source.binding.parser
            || checkpoint.output_stream != source.binding.output_stream
            || checkpoint.source.offset > source.captured_end
        {
            return Err(JournalError::CheckpointConflict {
                source: checkpoint.source.source,
            });
        }
        if source.checkpoint.as_ref() == Some(&checkpoint) {
            return Ok(CheckpointReceipt { checkpoint });
        }
        if source.parser_finished {
            return Err(JournalError::CheckpointConflict {
                source: checkpoint.source.source,
            });
        }
        if source.checkpoint.is_none()
            && state.journal_checkpoint_count >= self.options.source_journal.storage.max_checkpoints
        {
            return Err(JournalError::CapacityExceeded);
        }
        let checkpoint_charge =
            Self::checkpoint_charge(&checkpoint).ok_or(JournalError::CapacityExceeded)?;
        let prior_checkpoint_charge = source
            .checkpoint
            .as_ref()
            .and_then(Self::checkpoint_charge)
            .unwrap_or(0);
        if state
            .journal_checkpoint_bytes
            .checked_sub(prior_checkpoint_charge)
            .and_then(|value| value.checked_add(checkpoint_charge))
            .is_none_or(|value| value > self.options.source_journal.storage.max_staging_bytes)
        {
            return Err(JournalError::CapacityExceeded);
        }
        let old_offset = source
            .checkpoint
            .as_ref()
            .map_or(0, |value| value.source.offset);
        let old_index = source
            .checkpoint
            .as_ref()
            .map_or(0, |value| value.next_item_index);
        let old_committed = source
            .checkpoint
            .as_ref()
            .and_then(|value| value.committed_output.clone());
        if checkpoint.source.offset < old_offset
            || checkpoint.next_item_index < old_index
            || checkpoint.next_item_index > source.next_item_index
        {
            return Err(JournalError::CheckpointConflict {
                source: checkpoint.source.source,
            });
        }
        let expected_committed =
            if checkpoint.next_item_index == old_index {
                old_committed
            } else {
                let mut last = None;
                for index in old_index..checkpoint.next_item_index {
                    let marker = source.markers.get(&index).ok_or_else(|| {
                        JournalError::CheckpointConflict {
                            source: checkpoint.source.source.clone(),
                        }
                    })?;
                    if marker.position.source_byte > checkpoint.source.offset {
                        return Err(JournalError::CheckpointConflict {
                            source: checkpoint.source.source,
                        });
                    }
                    last = Some(marker.committed.clone());
                }
                last
            };
        if checkpoint.committed_output != expected_committed {
            return Err(JournalError::CheckpointConflict {
                source: checkpoint.source.source,
            });
        }
        let new_checkpoint = state
            .journal_sources
            .get(&checkpoint.source.source)
            .is_some_and(|source| source.checkpoint.is_none());
        let source = state
            .journal_sources
            .get_mut(&checkpoint.source.source)
            .unwrap();
        source.checkpoint = Some(checkpoint.clone());
        if new_checkpoint {
            state.journal_checkpoint_count += 1;
        }
        state.journal_checkpoint_bytes =
            state.journal_checkpoint_bytes - prior_checkpoint_charge + checkpoint_charge;
        Self::enqueue_journal_cleanup(&mut state, &checkpoint.source.source);
        Ok(CheckpointReceipt { checkpoint })
    }

    async fn source_status(&self, source: &SourceKey) -> JournalResult<SourceProgress> {
        let state = self.journal_lock()?;
        Ok(Self::progress(Self::journal_source(&state, source)?))
    }

    async fn read_captured(
        &self,
        source: &SourceKey,
        offset: u64,
        limits: RawPageLimits,
    ) -> JournalResult<RawPage> {
        if limits.max_segments == 0
            || limits.max_bytes == 0
            || limits.max_segments > self.options.source_journal.cleanup.max_segment_rows
            || limits.max_bytes > self.options.source_journal.cleanup.max_bytes
        {
            return Err(JournalError::InvalidInput(
                "raw page limits are outside configured bounds".into(),
            ));
        }
        let state = self.journal_lock()?;
        let journal = Self::journal_source(&state, source)?;
        if offset > journal.captured_end {
            return Err(JournalError::InvalidInput(
                "raw page starts after captured end".into(),
            ));
        }
        if offset == journal.captured_end {
            return Ok(RawPage {
                start: SourcePosition {
                    source: source.clone(),
                    offset,
                },
                bytes: Payload::copy_from_slice(&[]),
                next_offset: offset,
                complete: true,
            });
        }
        let available = journal
            .segments
            .range(..=offset)
            .next_back()
            .filter(|(start, bytes)| offset < **start + bytes.len() as u64)
            .map(|(start, _)| *start)
            .or_else(|| {
                journal
                    .segments
                    .range(offset..)
                    .next()
                    .map(|(start, _)| *start)
            })
            .ok_or(JournalError::MissingCapturedHistory {
                available_from: journal.captured_end,
            })?;
        if available > offset {
            return Err(JournalError::MissingCapturedHistory {
                available_from: available,
            });
        }
        let remaining = usize::try_from(journal.captured_end - offset).unwrap_or(usize::MAX);
        let mut bytes = Vec::with_capacity(limits.max_bytes.min(remaining));
        let mut position = offset;
        let mut rows = 0;
        while rows < limits.max_segments && bytes.len() < limits.max_bytes {
            let (&start, segment) = journal
                .segments
                .range(..=position)
                .next_back()
                .filter(|(start, segment)| position < **start + segment.len() as u64)
                .ok_or_else(|| JournalError::CorruptStorage("captured byte gap".into()))?;
            let inside = (position - start) as usize;
            let take = (segment.len() - inside).min(limits.max_bytes - bytes.len());
            bytes.extend_from_slice(&segment.as_bytes()[inside..inside + take]);
            position += take as u64;
            rows += 1;
            if take + inside < segment.len() || position == journal.captured_end {
                break;
            }
        }
        Ok(RawPage {
            start: SourcePosition {
                source: source.clone(),
                offset,
            },
            bytes: Payload::copy_from_slice(&bytes),
            next_offset: position,
            complete: position == journal.captured_end,
        })
    }

    async fn latest_checkpoint(
        &self,
        source: &SourceKey,
    ) -> JournalResult<Option<ParserCheckpoint>> {
        let state = self.journal_lock()?;
        Ok(Self::journal_source(&state, source)?.checkpoint.clone())
    }

    async fn cleanup_captured(
        &self,
        limits: JournalCleanupLimits,
    ) -> JournalResult<JournalCleanupProgress> {
        if limits.max_segment_rows == 0
            || limits.max_marker_rows == 0
            || limits.max_receipt_rows == 0
            || limits.max_bytes == 0
        {
            return Err(JournalError::InvalidInput(
                "journal cleanup limits must be nonzero".into(),
            ));
        }
        let mut state = self.journal_lock()?;
        if limits.max_segment_rows > self.options.source_journal.cleanup.max_segment_rows
            || limits.max_marker_rows > self.options.source_journal.cleanup.max_marker_rows
            || limits.max_receipt_rows > self.options.source_journal.cleanup.max_receipt_rows
            || limits.max_bytes > self.options.source_journal.cleanup.max_bytes
        {
            return Err(JournalError::CapacityExceeded);
        }
        let mut removed_segment_rows = 0;
        let mut removed_marker_rows = 0;
        let mut removed_receipt_rows = 0;
        let mut removed_bytes = 0usize;
        let mut freed_segment_bytes = 0u64;
        let mut freed_marker_bytes = 0u64;
        let mut freed_receipt_bytes = 0usize;
        let mut released_pins = Vec::new();
        while let Some(key) = state.journal_cleanup_queue.pop_front() {
            state.journal_cleanup_set.remove(&key);
            let before_rows = removed_segment_rows + removed_marker_rows + removed_receipt_rows;
            let still_eligible = {
                let journal = state.journal_sources.get_mut(&key).unwrap();
                let checkpoint_offset = journal.checkpoint.as_ref().map_or(0, |v| v.source.offset);
                while removed_segment_rows < limits.max_segment_rows {
                    let Some((&start, payload)) = journal.segments.first_key_value() else {
                        break;
                    };
                    let end = start + payload.len() as u64;
                    let charge = payload.len() + SEGMENT_OVERHEAD + key.id.as_str().len();
                    if end > checkpoint_offset || removed_bytes + charge > limits.max_bytes {
                        break;
                    }
                    let payload = journal.segments.remove(&start).unwrap();
                    removed_bytes += charge;
                    removed_segment_rows += 1;
                    freed_segment_bytes += payload.len() as u64;
                }
                let checkpoint_index = journal.checkpoint.as_ref().map_or(0, |v| v.next_item_index);
                while removed_marker_rows < limits.max_marker_rows {
                    let Some((&index, marker)) = journal.markers.first_key_value() else {
                        break;
                    };
                    if index >= checkpoint_index {
                        break;
                    }
                    let charge = Self::marker_charge(&journal.binding, marker)
                        .ok_or(JournalError::CapacityExceeded)?;
                    if removed_bytes + charge as usize > limits.max_bytes {
                        break;
                    }
                    let marker = journal.markers.remove(&index).unwrap();
                    released_pins.push((
                        journal.binding.output_stream.clone(),
                        marker.retry_generation,
                    ));
                    removed_bytes += charge as usize;
                    removed_marker_rows += 1;
                    freed_marker_bytes += charge;
                }
                while removed_receipt_rows < limits.max_receipt_rows {
                    let Some((&start, receipt)) = journal.receipts.first_key_value() else {
                        break;
                    };
                    if receipt.end > journal.receipt_floor {
                        break;
                    }
                    let charge =
                        Self::capture_receipt_charge(&key).ok_or(JournalError::CapacityExceeded)?;
                    if removed_bytes + charge > limits.max_bytes {
                        break;
                    }
                    journal.receipts.remove(&start);
                    removed_bytes += charge;
                    removed_receipt_rows += 1;
                    freed_receipt_bytes += charge;
                }
                journal
                    .segments
                    .first_key_value()
                    .is_some_and(|(start, bytes)| *start + bytes.len() as u64 <= checkpoint_offset)
                    || journal
                        .markers
                        .first_key_value()
                        .is_some_and(|(index, _)| *index < checkpoint_index)
                    || journal
                        .receipts
                        .first_key_value()
                        .is_some_and(|(_, receipt)| receipt.end <= journal.receipt_floor)
            };
            if still_eligible {
                Self::enqueue_journal_cleanup(&mut state, &key);
            }
            let after_rows = removed_segment_rows + removed_marker_rows + removed_receipt_rows;
            if after_rows == before_rows
                || removed_segment_rows == limits.max_segment_rows
                || removed_marker_rows == limits.max_marker_rows
                || removed_receipt_rows == limits.max_receipt_rows
                || removed_bytes == limits.max_bytes
            {
                break;
            }
        }
        state.journal_segment_rows -= removed_segment_rows;
        state.journal_captured_bytes -= freed_segment_bytes;
        state.journal_marker_rows -= removed_marker_rows;
        state.journal_marker_bytes -= freed_marker_bytes;
        state.journal_receipt_rows -= removed_receipt_rows;
        state.journal_receipt_bytes -= freed_receipt_bytes;
        for (stream, generation) in released_pins {
            if let Some(generations) = state.journal_generation_pins.get_mut(&stream) {
                if let Some(count) = generations.get_mut(&generation) {
                    *count -= 1;
                    if *count == 0 {
                        generations.remove(&generation);
                    }
                }
                if generations.is_empty() {
                    state.journal_generation_pins.remove(&stream);
                }
            }
        }
        let remaining = !state.journal_cleanup_queue.is_empty();
        Ok(JournalCleanupProgress {
            removed_segment_rows,
            removed_marker_rows,
            removed_receipt_rows,
            removed_bytes,
            remaining,
        })
    }
}

#[async_trait]
impl SourceFinalizationStore for MemoryStore {
    async fn seal_source(&self, request: SealSource) -> JournalResult<SealSourceReceipt> {
        let mut state = self.journal_lock()?;
        let source = Self::journal_source(&state, &request.end.source)?;
        if request.end.offset != source.captured_end {
            return Err(JournalError::InvalidInput(
                "seal must equal captured end".into(),
            ));
        }
        state
            .journal_sources
            .get_mut(&request.end.source)
            .unwrap()
            .sealed_end = Some(request.end.offset);
        Ok(SealSourceReceipt { request })
    }

    async fn finish_source(&self, request: FinishSource) -> JournalResult<FinishSourceReceipt> {
        let mut state = self.journal_lock()?;
        let source = Self::journal_source(&state, &request.checkpoint.source.source)?;
        if source.sealed_end != Some(request.checkpoint.source.offset)
            || source.checkpoint.as_ref() != Some(&request.checkpoint)
        {
            return Err(JournalError::CheckpointConflict {
                source: request.checkpoint.source.source,
            });
        }
        state
            .journal_sources
            .get_mut(&request.checkpoint.source.source)
            .unwrap()
            .parser_finished = true;
        Ok(FinishSourceReceipt { request })
    }

    async fn source_finalization(
        &self,
        source: &SourceKey,
    ) -> JournalResult<SourceFinalizationStatus> {
        let state = self.journal_lock()?;
        let source = Self::journal_source(&state, source)?;
        Ok(SourceFinalizationStatus {
            sealed_end: source.sealed_end,
            parser_finished: source.parser_finished,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn marker_retry_does_not_recreate_a_missing_generated_identity() {
        let store = MemoryStore::open(super::super::MemoryStoreOptions::default())
            .await
            .unwrap();
        let output = store
            .create_if_absent(&StreamId::new("corrupt-marker-output").unwrap())
            .await
            .unwrap();
        store
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new("corrupt-marker-enable").unwrap(),
                stream: output.clone(),
            })
            .await
            .unwrap();
        let source = SourceKey {
            id: SourceId::new("corrupt-marker-source").unwrap(),
            incarnation: SourceIncarnation([77; 16]),
        };
        store
            .begin_source(BeginSource {
                operation_id: JournalOperationId::new("corrupt-marker-begin").unwrap(),
                binding: SourceBinding {
                    source: source.clone(),
                    parser: ParserRef {
                        id: ParserId::new("corrupt-marker-parser").unwrap(),
                        version: 1,
                    },
                    output_stream: output.clone(),
                },
            })
            .await
            .unwrap();
        store
            .capture_segment(RawSegment {
                start: SourcePosition {
                    source: source.clone(),
                    offset: 0,
                },
                bytes: Payload::copy_from_slice(b"x"),
            })
            .await
            .unwrap();
        let request = JournaledOutput {
            source: source.clone(),
            position: DecodedPosition {
                source_byte: 0,
                item_index: 0,
            },
            event: NewEvent {
                id: EventId::new("corrupt-marker-event").unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("corrupt-marker-schema").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(b"value"),
            },
        };
        store
            .append_captured(&output, request.clone())
            .await
            .unwrap();
        {
            let mut state = store.state.lock().unwrap();
            let StreamEntry::Active(history) = state.streams.get_mut(&output.id).unwrap() else {
                unreachable!()
            };
            history
                .generated_event_offsets
                .remove(&(RetryGeneration::FIRST, request.event.id.clone()));
        }
        assert!(matches!(
            store.append_captured(&output, request).await,
            Err(JournalError::CorruptStorage(_))
        ));
        let state = store.state.lock().unwrap();
        let StreamEntry::Active(history) = state.streams.get(&output.id).unwrap() else {
            unreachable!()
        };
        assert_eq!(history.tail, 1);
        assert!(history.generated_event_offsets.is_empty());
    }
}
