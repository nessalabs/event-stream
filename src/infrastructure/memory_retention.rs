use super::memory::{MemoryStore, State, StreamEntry, StreamHistory};
use crate::{application::*, domain::*};
use async_trait::async_trait;
use std::sync::Arc;

const RECEIPT_OVERHEAD: usize = 256;
const STORED_RECORD_OVERHEAD: usize = 256;

#[derive(Clone, Debug)]
pub(super) enum MemoryRetentionReceipt {
    Enable(EnableRetryPolicyReceipt),
    Advance(AdvanceRetryGenerationReceipt),
    Expire(ExpireRetryGenerationsReceipt),
    Floor(AdvanceRetentionFloorReceipt),
}

impl MemoryStore {
    fn retention_lock(&self) -> RetentionResult<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| RetentionError::CorruptStorage("memory store lock poisoned".into()))
    }

    fn retention_history<'a>(
        state: &'a State,
        stream: &StreamKey,
    ) -> RetentionResult<&'a StreamHistory> {
        if state.closed {
            return Err(RetentionError::Closed);
        }
        match state.streams.get(&stream.id) {
            None | Some(StreamEntry::Unavailable(_)) => {
                Err(RetentionError::StaleIncarnation { current: None })
            }
            Some(StreamEntry::Active(history)) if history.key != *stream => {
                Err(RetentionError::StaleIncarnation {
                    current: Some(Box::new(history.key.clone())),
                })
            }
            Some(StreamEntry::Active(history)) => Ok(history),
        }
    }

    fn retention_history_mut<'a>(
        state: &'a mut State,
        stream: &StreamKey,
    ) -> RetentionResult<&'a mut StreamHistory> {
        Self::retention_history(state, stream)?;
        match state.streams.get_mut(&stream.id) {
            Some(StreamEntry::Active(history)) => Ok(history),
            _ => unreachable!("validated active stream"),
        }
    }

    fn retention_status_for(history: &StreamHistory) -> RetentionStatus {
        RetentionStatus {
            bounds: Bounds {
                floor: Cursor::new(history.key.clone(), history.floor),
                tail: Cursor::new(history.key.clone(), history.tail),
            },
            retry_policy: history.retry_policy.clone(),
        }
    }

    fn receipt_charge(operation: &RetentionOperationId, stream: &StreamKey) -> Option<usize> {
        operation
            .as_str()
            .len()
            .checked_add(stream.id.as_str().len())?
            .checked_add(RECEIPT_OVERHEAD)
    }

    fn reserve_receipt(
        &self,
        state: &State,
        operation: &RetentionOperationId,
        stream: &StreamKey,
    ) -> RetentionResult<usize> {
        if state.operation_ids.contains(operation.as_str()) {
            return Err(RetentionError::OperationConflict {
                operation_id: operation.clone(),
            });
        }
        let charge =
            Self::receipt_charge(operation, stream).ok_or(RetentionError::CapacityExceeded)?;
        if state.retention_receipts.len() >= self.options.retention.operations.max_receipts
            || state
                .retention_receipt_bytes
                .checked_add(charge)
                .is_none_or(|total| total > self.options.retention.operations.max_receipt_bytes)
        {
            return Err(RetentionError::CapacityExceeded);
        }
        Ok(charge)
    }

    fn enqueue_cleanup(&self, state: &mut State, stream: &StreamKey) -> RetentionResult<()> {
        if state.pending_retention_cleanup_set.contains(stream) {
            return Ok(());
        }
        if state.pending_retention_cleanup.len()
            >= self.options.retention.operations.max_pending_cleanup_ranges
        {
            return Err(RetentionError::CapacityExceeded);
        }
        state.pending_retention_cleanup.push_back(stream.clone());
        state.pending_retention_cleanup_set.insert(stream.clone());
        Ok(())
    }

    fn stored_record_bytes(record: &Record) -> RetentionResult<usize> {
        record
            .event
            .accounted_bytes()
            .checked_add(STORED_RECORD_OVERHEAD)
            .ok_or(RetentionError::CapacityExceeded)
    }

    pub(super) fn append_generated_in_state(
        &self,
        state: &mut State,
        stream: &StreamKey,
        event: GeneratedEvent,
    ) -> RetentionResult<AppendReceipt> {
        let event_bytes = event.event.accounted_bytes();
        if event_bytes > self.options.max_record_bytes {
            return Err(RetentionError::InvalidInput(
                "event exceeds record limit".into(),
            ));
        }
        let stored_bytes = event_bytes
            .checked_add(STORED_RECORD_OVERHEAD)
            .ok_or(RetentionError::CapacityExceeded)?;
        let history = Self::retention_history(state, stream)?;
        let (oldest, current) = match history.retry_policy {
            RetryPolicyState::Lifetime => return Err(RetentionError::RetryPolicyRequired),
            RetryPolicyState::Generational {
                oldest_accepted,
                current,
            } => (oldest_accepted, current),
        };
        if event.generation < oldest {
            return Err(RetentionError::RetryGenerationExpired {
                oldest_accepted: oldest,
            });
        }
        if event.generation > current {
            return Err(RetentionError::RetryGenerationAhead { current });
        }
        let identity = (event.generation, event.event.id.clone());
        let prior = if event.generation == RetryGeneration::LEGACY {
            history
                .event_offsets
                .get(&event.event.id)
                .and_then(|offset| history.records.get(offset))
                .cloned()
                .or_else(|| {
                    history
                        .retained_retry_records
                        .get(&(RetryGeneration::LEGACY, event.event.id.clone()))
                        .cloned()
                })
        } else {
            history
                .generated_event_offsets
                .get(&identity)
                .and_then(|offset| history.records.get(offset))
                .cloned()
                .or_else(|| history.retained_retry_records.get(&identity).cloned())
        };
        if let Some(record) = prior {
            if record.event.schema != event.event.schema
                || record.event.payload != event.event.payload
            {
                return Err(RetentionError::IdempotencyConflict {
                    identity: Box::new(GeneratedEventIdentity {
                        stream: stream.clone(),
                        generation: event.generation,
                        event_id: event.event.id,
                    }),
                });
            }
            return Ok(AppendReceipt {
                record,
                kind: AppendKind::Deduplicated,
            });
        }
        if event.generation == RetryGeneration::LEGACY {
            return Err(RetentionError::LegacyRetryNotFound {
                event_id: event.event.id,
            });
        }
        if state.history_records >= self.options.max_history_records
            || state
                .history_bytes
                .checked_add(stored_bytes)
                .is_none_or(|total| total > self.options.max_history_bytes)
            || state.retry_receipt_rows >= self.options.retention.receipts.max_rows
            || state
                .retry_receipt_bytes
                .checked_add(stored_bytes as u64)
                .is_none_or(|total| total > self.options.retention.receipts.max_bytes)
        {
            return Err(RetentionError::CapacityExceeded);
        }
        #[cfg(feature = "replication")]
        let replication_now = self
            .preflight_replication_append(state, stream, stored_bytes)
            .map_err(|error| match error {
                crate::application::Error::ReplicaBacklogExceeded {
                    replica,
                    limit_bytes,
                } => RetentionError::ReplicaBacklogExceeded {
                    replica,
                    limit_bytes,
                },
                crate::application::Error::ReplicaBacklogExpired { replica } => {
                    RetentionError::ReplicaBacklogExpired { replica }
                }
                crate::application::Error::ReplicationClockRollback => {
                    RetentionError::ReplicationClockRollback
                }
                crate::application::Error::CapacityExceeded => RetentionError::CapacityExceeded,
                other => RetentionError::StorageFailure(other.to_string()),
            })?;
        let offset = history
            .tail
            .checked_add(1)
            .ok_or_else(|| RetentionError::InvalidInput("stream offset overflow".into()))?;
        let record = Arc::new(Record {
            cursor: Cursor::new(stream.clone(), offset),
            event: event.event,
        });
        let history = Self::retention_history_mut(state, stream)?;
        history.generated_event_offsets.insert(identity, offset);
        history.record_generations.insert(offset, event.generation);
        history.records.insert(offset, record.clone());
        history.tail = offset;
        history.logical_bytes = history
            .logical_bytes
            .checked_add(stored_bytes as u64)
            .ok_or(RetentionError::CapacityExceeded)?;
        state.history_records += 1;
        state.history_bytes += stored_bytes;
        state.retry_receipt_rows += 1;
        state.retry_receipt_bytes += stored_bytes as u64;
        #[cfg(feature = "replication")]
        Self::apply_replication_append(
            state,
            stream,
            record.cursor.offset,
            stored_bytes,
            replication_now,
        );
        Ok(AppendReceipt {
            record,
            kind: AppendKind::Inserted,
        })
    }
}

#[async_trait]
impl RetentionStore for MemoryStore {
    async fn retention_status(&self, stream: &StreamKey) -> RetentionResult<RetentionStatus> {
        let state = self.retention_lock()?;
        Ok(Self::retention_status_for(Self::retention_history(
            &state, stream,
        )?))
    }

    async fn enable_retry_policy(
        &self,
        request: EnableRetryPolicy,
    ) -> RetentionResult<EnableRetryPolicyReceipt> {
        let mut state = self.retention_lock()?;
        if let Some(prior) = state.retention_receipts.get(&request.operation_id) {
            return match prior {
                MemoryRetentionReceipt::Enable(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(RetentionError::OperationConflict {
                    operation_id: request.operation_id,
                }),
            };
        }
        let charge = self.reserve_receipt(&state, &request.operation_id, &request.stream)?;
        let history = Self::retention_history(&state, &request.stream)?;
        if !matches!(history.retry_policy, RetryPolicyState::Lifetime) {
            return Err(RetentionError::InvalidInput(
                "retry policy is already generational".into(),
            ));
        }
        let legacy_rows = history.records.len();
        let legacy_bytes = history.logical_bytes;
        if state
            .retry_receipt_rows
            .checked_add(legacy_rows)
            .is_none_or(|total| total > self.options.retention.receipts.max_rows)
            || state
                .retry_receipt_bytes
                .checked_add(legacy_bytes)
                .is_none_or(|total| total > self.options.retention.receipts.max_bytes)
        {
            return Err(RetentionError::CapacityExceeded);
        }
        let history = Self::retention_history_mut(&mut state, &request.stream)?;
        history.retry_policy = RetryPolicyState::Generational {
            oldest_accepted: RetryGeneration::LEGACY,
            current: RetryGeneration::FIRST,
        };
        let receipt = EnableRetryPolicyReceipt {
            request: request.clone(),
            status: Self::retention_status_for(history),
        };
        state.retry_receipt_rows += legacy_rows;
        state.retry_receipt_bytes += legacy_bytes;
        state.retention_receipt_bytes += charge;
        state.retention_receipts.insert(
            request.operation_id.clone(),
            MemoryRetentionReceipt::Enable(receipt.clone()),
        );
        state
            .operation_ids
            .insert(request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn advance_retry_generation(
        &self,
        request: AdvanceRetryGeneration,
    ) -> RetentionResult<AdvanceRetryGenerationReceipt> {
        let mut state = self.retention_lock()?;
        if let Some(prior) = state.retention_receipts.get(&request.operation_id) {
            return match prior {
                MemoryRetentionReceipt::Advance(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(RetentionError::OperationConflict {
                    operation_id: request.operation_id,
                }),
            };
        }
        let charge = self.reserve_receipt(&state, &request.operation_id, &request.stream)?;
        let history = Self::retention_history_mut(&mut state, &request.stream)?;
        let (oldest_accepted, current) = match history.retry_policy {
            RetryPolicyState::Lifetime => return Err(RetentionError::RetryPolicyRequired),
            RetryPolicyState::Generational {
                oldest_accepted,
                current,
            } => (oldest_accepted, current),
        };
        if current != request.expected_current {
            return Err(RetentionError::StaleGeneration { current });
        }
        let next = current.checked_next().ok_or_else(|| {
            RetentionError::InvalidInput("retry generation cannot overflow".into())
        })?;
        history.retry_policy = RetryPolicyState::Generational {
            oldest_accepted,
            current: next,
        };
        let receipt = AdvanceRetryGenerationReceipt {
            request: request.clone(),
            status: Self::retention_status_for(history),
        };
        state.retention_receipt_bytes += charge;
        state.retention_receipts.insert(
            request.operation_id.clone(),
            MemoryRetentionReceipt::Advance(receipt.clone()),
        );
        state
            .operation_ids
            .insert(request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn expire_retry_generations(
        &self,
        request: ExpireRetryGenerations,
    ) -> RetentionResult<ExpireRetryGenerationsReceipt> {
        let mut state = self.retention_lock()?;
        if let Some(prior) = state.retention_receipts.get(&request.operation_id) {
            return match prior {
                MemoryRetentionReceipt::Expire(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(RetentionError::OperationConflict {
                    operation_id: request.operation_id,
                }),
            };
        }
        let charge = self.reserve_receipt(&state, &request.operation_id, &request.stream)?;
        let history = Self::retention_history(&state, &request.stream)?;
        let (oldest, current) = match history.retry_policy {
            RetryPolicyState::Lifetime => return Err(RetentionError::RetryPolicyRequired),
            RetryPolicyState::Generational {
                oldest_accepted,
                current,
            } => (oldest_accepted, current),
        };
        if oldest != request.expected_oldest {
            return Err(RetentionError::StaleGeneration { current: oldest });
        }
        if request.retain_from < oldest || request.retain_from > current {
            return Err(RetentionError::InvalidInput(
                "retry expiry must move within the accepted generation range".into(),
            ));
        }
        #[cfg(feature = "source-journal")]
        if let Some(required) = MemoryStore::minimum_journal_generation(&state, &request.stream) {
            if request.retain_from > required {
                return Err(RetentionError::JournalProtectionActive {
                    oldest_required: required,
                });
            }
        }
        if request.retain_from > oldest {
            self.enqueue_cleanup(&mut state, &request.stream)?;
        }
        let history = Self::retention_history_mut(&mut state, &request.stream)?;
        history.retry_policy = RetryPolicyState::Generational {
            oldest_accepted: request.retain_from,
            current,
        };
        let receipt = ExpireRetryGenerationsReceipt {
            request: request.clone(),
            status: Self::retention_status_for(history),
        };
        state.retention_receipt_bytes += charge;
        state.retention_receipts.insert(
            request.operation_id.clone(),
            MemoryRetentionReceipt::Expire(receipt.clone()),
        );
        state
            .operation_ids
            .insert(request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn append_generated(
        &self,
        stream: &StreamKey,
        event: GeneratedEvent,
    ) -> RetentionResult<AppendReceipt> {
        let mut state = self.retention_lock()?;
        self.append_generated_in_state(&mut state, stream, event)
    }

    async fn lookup_generated(
        &self,
        stream: &StreamKey,
        generation: RetryGeneration,
        event_id: &EventId,
    ) -> RetentionResult<Option<Arc<Record>>> {
        let state = self.retention_lock()?;
        let history = Self::retention_history(&state, stream)?;
        let (oldest, current) = match history.retry_policy {
            RetryPolicyState::Lifetime => return Err(RetentionError::RetryPolicyRequired),
            RetryPolicyState::Generational {
                oldest_accepted,
                current,
            } => (oldest_accepted, current),
        };
        if generation < oldest {
            return Err(RetentionError::RetryGenerationExpired {
                oldest_accepted: oldest,
            });
        }
        if generation > current {
            return Err(RetentionError::RetryGenerationAhead { current });
        }
        if generation == RetryGeneration::LEGACY {
            return Ok(history
                .event_offsets
                .get(event_id)
                .and_then(|offset| history.records.get(offset))
                .cloned()
                .or_else(|| {
                    history
                        .retained_retry_records
                        .get(&(RetryGeneration::LEGACY, event_id.clone()))
                        .cloned()
                }));
        }
        let identity = (generation, event_id.clone());
        Ok(history
            .generated_event_offsets
            .get(&identity)
            .and_then(|offset| history.records.get(offset))
            .cloned()
            .or_else(|| history.retained_retry_records.get(&identity).cloned()))
    }

    async fn advance_retention_floor(
        &self,
        request: AdvanceRetentionFloor,
    ) -> RetentionResult<AdvanceRetentionFloorReceipt> {
        if request.expected_floor.version != CURSOR_VERSION
            || request.new_floor.version != CURSOR_VERSION
            || request.expected_floor.stream != request.stream
            || request.new_floor.stream != request.stream
        {
            return Err(RetentionError::InvalidInput(
                "retention cursors must identify the requested stream".into(),
            ));
        }
        let mut state = self.retention_lock()?;
        if let Some(prior) = state.retention_receipts.get(&request.operation_id) {
            return match prior {
                MemoryRetentionReceipt::Floor(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(RetentionError::OperationConflict {
                    operation_id: request.operation_id,
                }),
            };
        }
        let charge = self.reserve_receipt(&state, &request.operation_id, &request.stream)?;
        let history = Self::retention_history(&state, &request.stream)?;
        if matches!(history.retry_policy, RetryPolicyState::Lifetime) {
            return Err(RetentionError::RetryPolicyRequired);
        }
        if request.expected_floor.offset != history.floor {
            return Err(RetentionError::StaleFloor {
                current: Box::new(Cursor::new(request.stream.clone(), history.floor)),
            });
        }
        if request.new_floor.offset < history.floor || request.new_floor.offset > history.tail {
            return Err(RetentionError::InvalidInput(
                "retention floor must be monotonic and no greater than tail".into(),
            ));
        }
        #[cfg(feature = "replication")]
        if let Some(protected) = Self::minimum_replica_offset(&state, &request.stream) {
            if request.new_floor.offset > protected {
                return Err(RetentionError::ReplicaProtectionActive {
                    maximum_floor: Box::new(Cursor::new(request.stream.clone(), protected)),
                });
            }
        }
        let now = self.options.snapshot_clock.now();
        let maximum = state
            .recovery_leases
            .values()
            .filter(|lease| lease.stream == request.stream && lease.expires.0 > now.0)
            .map(|lease| lease.covered)
            .min();
        if maximum.is_some_and(|covered| request.new_floor.offset > covered) {
            return Err(RetentionError::RecoveryProtectionActive {
                maximum_floor: Box::new(Cursor::new(request.stream.clone(), maximum.unwrap())),
            });
        }
        if request.new_floor.offset > history.floor {
            self.enqueue_cleanup(&mut state, &request.stream)?;
        }
        let history = Self::retention_history_mut(&mut state, &request.stream)?;
        history.floor = request.new_floor.offset;
        let receipt = AdvanceRetentionFloorReceipt {
            request: request.clone(),
            status: Self::retention_status_for(history),
        };
        state.retention_receipt_bytes += charge;
        state.retention_receipts.insert(
            request.operation_id.clone(),
            MemoryRetentionReceipt::Floor(receipt.clone()),
        );
        state
            .operation_ids
            .insert(request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn cleanup_retention(
        &self,
        limits: RetentionCleanupLimits,
    ) -> RetentionResult<RetentionCleanupProgress> {
        if limits.max_event_rows == 0 || limits.max_retry_rows == 0 || limits.max_bytes == 0 {
            return Err(RetentionError::InvalidInput(
                "cleanup limits must be nonzero".into(),
            ));
        }
        if self
            .options
            .max_record_bytes
            .checked_add(STORED_RECORD_OVERHEAD)
            .is_none_or(|maximum| maximum > limits.max_bytes)
        {
            return Err(RetentionError::InvalidInput(
                "cleanup byte limit must fit one maximum-size stored record".into(),
            ));
        }
        let mut state = self.retention_lock()?;
        if state.closed {
            return Err(RetentionError::Closed);
        }
        let Some(stream) = state.pending_retention_cleanup.pop_front() else {
            return Ok(RetentionCleanupProgress {
                removed_event_rows: 0,
                removed_retry_rows: 0,
                removed_bytes: 0,
                remaining: false,
            });
        };
        state.pending_retention_cleanup_set.remove(&stream);
        let mut removed_event_rows: usize = 0;
        let mut removed_retry_rows: usize = 0;
        let mut removed_bytes: usize = 0;
        let mut released_retry_rows: usize = 0;
        let mut released_retry_bytes = 0u64;
        let mut released_history_bytes = 0usize;
        let mut more = false;
        #[cfg(feature = "replication")]
        let mut last_removed_offset = None;
        if let Ok(history) = Self::retention_history_mut(&mut state, &stream) {
            let oldest = match history.retry_policy {
                RetryPolicyState::Lifetime => RetryGeneration::LEGACY,
                RetryPolicyState::Generational {
                    oldest_accepted, ..
                } => oldest_accepted,
            };
            while removed_retry_rows < limits.max_retry_rows {
                let expired = history
                    .generated_event_offsets
                    .first_key_value()
                    .filter(|((generation, _), _)| *generation < oldest)
                    .map(|(identity, _)| identity.clone());
                let Some(identity) = expired else { break };
                let offset = history.generated_event_offsets[&identity];
                let record = history
                    .records
                    .get(&offset)
                    .or_else(|| history.retained_retry_records.get(&identity));
                let bytes = record
                    .map(Arc::as_ref)
                    .map(Self::stored_record_bytes)
                    .transpose()?
                    .unwrap_or(0);
                if removed_bytes
                    .checked_add(bytes)
                    .is_none_or(|n| n > limits.max_bytes)
                {
                    break;
                }
                history.generated_event_offsets.remove(&identity);
                history.retained_retry_records.remove(&identity);
                removed_retry_rows += 1;
                removed_bytes += bytes;
                released_retry_rows += 1;
                released_retry_bytes += bytes as u64;
            }
            if oldest > RetryGeneration::LEGACY {
                while removed_retry_rows < limits.max_retry_rows {
                    let Some(event_id) = history.event_offsets.keys().next().cloned() else {
                        break;
                    };
                    let offset = history.event_offsets[&event_id];
                    let bytes = history
                        .records
                        .get(&offset)
                        .or_else(|| {
                            history
                                .retained_retry_records
                                .get(&(RetryGeneration::LEGACY, event_id.clone()))
                        })
                        .map(|record| Self::stored_record_bytes(record))
                        .transpose()?
                        .unwrap_or(0);
                    if removed_bytes
                        .checked_add(bytes)
                        .is_none_or(|n| n > limits.max_bytes)
                    {
                        break;
                    }
                    history.event_offsets.remove(&event_id);
                    history
                        .retained_retry_records
                        .remove(&(RetryGeneration::LEGACY, event_id));
                    removed_retry_rows += 1;
                    removed_bytes += bytes;
                    released_retry_rows += 1;
                    released_retry_bytes += bytes as u64;
                }
            }
            while removed_event_rows < limits.max_event_rows {
                let Some((&offset, record)) = history.records.first_key_value() else {
                    break;
                };
                if offset > history.floor {
                    break;
                }
                let bytes = Self::stored_record_bytes(record)?;
                if removed_bytes
                    .checked_add(bytes)
                    .is_none_or(|n| n > limits.max_bytes)
                {
                    break;
                }
                let generation = history
                    .record_generations
                    .get(&offset)
                    .copied()
                    .unwrap_or(RetryGeneration::LEGACY);
                let keep_retry = matches!(history.retry_policy, RetryPolicyState::Generational { oldest_accepted, .. } if generation >= oldest_accepted);
                let record = history.records.remove(&offset).unwrap();
                #[cfg(feature = "replication")]
                {
                    last_removed_offset = Some(offset);
                }
                if keep_retry {
                    history
                        .retained_retry_records
                        .insert((generation, record.event.id.clone()), record.clone());
                }
                history.record_generations.remove(&offset);
                history.logical_bytes = history.logical_bytes.saturating_sub(bytes as u64);
                released_history_bytes += bytes;
                removed_event_rows += 1;
                removed_bytes += bytes;
            }
            more = history
                .records
                .first_key_value()
                .is_some_and(|(&offset, _)| offset <= history.floor)
                || history
                    .generated_event_offsets
                    .first_key_value()
                    .is_some_and(|((generation, _), _)| *generation < oldest)
                || (oldest > RetryGeneration::LEGACY && !history.event_offsets.is_empty());
        }
        #[cfg(feature = "replication")]
        if let (Some(times), Some(last_removed)) = (
            state.replication_commit_times.get_mut(&stream),
            last_removed_offset,
        ) {
            // Match timestamp reclamation to physical history reclamation.
            // Never scan the retained suffix or allocate a list of removed keys.
            for _ in 0..removed_event_rows {
                if times
                    .first_key_value()
                    .is_none_or(|(&offset, _)| offset > last_removed)
                {
                    break;
                }
                times.pop_first();
            }
        }
        state.history_records = state.history_records.saturating_sub(removed_event_rows);
        state.history_bytes = state.history_bytes.saturating_sub(released_history_bytes);
        state.retry_receipt_rows = state.retry_receipt_rows.saturating_sub(released_retry_rows);
        state.retry_receipt_bytes = state
            .retry_receipt_bytes
            .saturating_sub(released_retry_bytes);
        if more {
            self.enqueue_cleanup(&mut state, &stream)?;
        }
        Ok(RetentionCleanupProgress {
            removed_event_rows,
            removed_retry_rows,
            removed_bytes,
            remaining: !state.pending_retention_cleanup.is_empty(),
        })
    }
}

#[cfg(all(test, feature = "replication"))]
mod timestamp_cleanup_tests {
    use super::*;
    use crate::infrastructure::MemoryStoreOptions;

    #[tokio::test]
    async fn one_row_cleanup_preserves_timestamp_suffix_and_unreclaimed_prefix() {
        let store = MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap();
        let stream = store
            .create_if_absent(&StreamId::new("cleanup-times").unwrap())
            .await
            .unwrap();
        for offset in 1..=4096 {
            store
                .append_atomic(
                    &stream,
                    NewEvent {
                        id: EventId::new(format!("event-{offset}")).unwrap(),
                        schema: SchemaRef {
                            id: SchemaId::new("cleanup-test").unwrap(),
                            version: 1,
                        },
                        payload: Payload::copy_from_slice(b"x"),
                    },
                )
                .await
                .unwrap();
        }
        let original_times = store.state.lock().unwrap().replication_commit_times[&stream].clone();
        store
            .enable_retry_policy(EnableRetryPolicy {
                operation_id: RetentionOperationId::new("enable").unwrap(),
                stream: stream.clone(),
            })
            .await
            .unwrap();
        store
            .advance_retention_floor(AdvanceRetentionFloor {
                operation_id: RetentionOperationId::new("floor").unwrap(),
                stream: stream.clone(),
                expected_floor: Cursor::new(stream.clone(), 0),
                new_floor: Cursor::new(stream.clone(), 4),
            })
            .await
            .unwrap();
        for removed in 1..=4u64 {
            let progress = store
                .cleanup_retention(RetentionCleanupLimits {
                    max_event_rows: 1,
                    max_retry_rows: 1,
                    max_bytes: 2 * 1024 * 1024,
                })
                .await
                .unwrap();
            assert_eq!(progress.removed_event_rows, 1);
            let state = store.state.lock().unwrap();
            let times = &state.replication_commit_times[&stream];
            assert_eq!(times.len(), 4096 - removed as usize);
            assert_eq!(
                times.first_key_value().map(|(offset, _)| *offset),
                Some(removed + 1)
            );
            assert!(times
                .iter()
                .all(|(offset, at)| original_times.get(offset) == Some(at)));
            let history = MemoryStore::retention_history(&state, &stream).unwrap();
            assert!(history.records.keys().eq(times.keys()));
        }
        let empty = store
            .cleanup_retention(RetentionCleanupLimits {
                max_event_rows: 1,
                max_retry_rows: 1,
                max_bytes: 2 * 1024 * 1024,
            })
            .await
            .unwrap();
        assert_eq!(empty.removed_event_rows, 0);
        assert!(!empty.remaining);
        store.close().await.unwrap();
    }
}
