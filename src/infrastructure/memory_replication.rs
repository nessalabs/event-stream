use super::MemoryStore;
use crate::{application::*, domain::*};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

const STORED_RECORD_OVERHEAD: usize = 256;
const RECEIPT_OVERHEAD: usize = 192;

#[derive(Debug)]
pub(super) struct MemoryOriginReplica {
    status: ReplicaStatus,
    max_backlog_bytes: u64,
    max_backlog_age: std::time::Duration,
    pending: Option<ReplicaBatch>,
    bootstrap: Option<MemoryOriginBootstrap>,
}

#[derive(Debug)]
struct MemoryOriginBootstrap {
    request: BeginOriginBootstrap,
    suffix_records: usize,
    suffix_bytes: u64,
    recovery_lease: RecoveryLeaseId,
}

#[derive(Clone, Debug)]
pub(super) enum MemoryOriginReceipt {
    Attach(AttachReplicaReceipt),
    Prepare(PrepareReplicaBatchReceipt),
    Acknowledge(AcknowledgeReplicaBatchReceipt),
    Detach(DetachReplicaReceipt),
    BeginBootstrap {
        receipt: BeginOriginBootstrapReceipt,
        completed: bool,
    },
    AcknowledgeBootstrap(AcknowledgeOriginBootstrapReceipt),
}

#[derive(Debug)]
pub(super) struct MemoryReplicaBootstrap {
    request: ReplicaBootstrap,
    state: ReplicaBootstrapState,
    chunks: BTreeMap<u64, Payload>,
    accepted_bytes: u64,
    suffix_records: BTreeMap<u64, Arc<Record>>,
    suffix_batches: std::collections::HashMap<BatchId, ReplicaBatch>,
    suffix_bytes: u64,
    verification: Option<MemoryReplicaBootstrapVerification>,
    publish: Option<(PublishReplicaBootstrap, ReplicaBootstrapReceipt)>,
    abort: Option<(AbortReplicaBootstrap, AbortReplicaBootstrapReceipt)>,
    readers: usize,
}

#[derive(Debug)]
struct MemoryReplicaBootstrapVerification {
    snapshot_offset: u64,
    suffix_offset: u64,
    suffix_records: usize,
    suffix_bytes: u64,
    hasher: Sha256,
}

#[derive(Debug)]
pub(super) struct MemoryReplicaReadLease {
    bootstrap: BootstrapId,
    expires_at: DurableTimestampMillis,
}

fn record_charge(record: &Record) -> ReplicationResult<usize> {
    record
        .event
        .accounted_bytes()
        .checked_add(STORED_RECORD_OVERHEAD)
        .ok_or(ReplicationError::CapacityExceeded)
}

fn enqueue_replica_cleanup(state: &mut super::State, id: BootstrapId) {
    if state.replica_cleanup_set.insert(id) {
        state.replica_cleanup_queue.push_back(id);
    }
}

fn release_replica_reader(state: &mut super::State, id: BootstrapId) -> ReplicationResult<()> {
    let Some(bootstrap) = state.replica_bootstraps.get_mut(&id) else {
        return Ok(());
    };
    bootstrap.readers = bootstrap.readers.checked_sub(1).ok_or_else(|| {
        ReplicationError::CorruptStorage("published bootstrap reader counter underflow".into())
    })?;
    let enqueue = bootstrap.readers == 0
        && state
            .published_replica_bootstraps
            .get(&bootstrap.request.stream)
            != Some(&id);
    if enqueue {
        enqueue_replica_cleanup(state, id);
    }
    Ok(())
}

fn batch_payload_charge(batch: &ReplicaBatch) -> ReplicationResult<usize> {
    batch.records.iter().try_fold(0usize, |total, record| {
        total
            .checked_add(record_charge(record)?)
            .ok_or(ReplicationError::CapacityExceeded)
    })
}

fn batch_receipt_charge(batch: &ReplicaBatch) -> ReplicationResult<usize> {
    batch
        .records
        .len()
        .checked_mul(std::mem::size_of::<Arc<Record>>())
        .and_then(|bytes| {
            bytes.checked_add(
                batch
                    .after
                    .stream
                    .stream
                    .id
                    .as_str()
                    .len()
                    .saturating_mul(2),
            )
        })
        .and_then(|bytes| bytes.checked_add(RECEIPT_OVERHEAD))
        .ok_or(ReplicationError::CapacityExceeded)
}

fn bootstrap_receipt_charge(request: &ReplicaBootstrap) -> ReplicationResult<usize> {
    request
        .snapshot
        .accounted_bytes()
        .and_then(|bytes| bytes.checked_add(request.operation_id.as_str().len()))
        .and_then(|bytes| bytes.checked_add(request.replica.as_str().len()))
        .and_then(|bytes| bytes.checked_add(request.stream.stream.id.as_str().len() * 2))
        .and_then(|bytes| bytes.checked_add(RECEIPT_OVERHEAD))
        .ok_or(ReplicationError::CapacityExceeded)
}

fn validate_batch(
    batch: &ReplicaBatch,
    max_records: usize,
    max_bytes: usize,
) -> ReplicationResult<usize> {
    if batch.records.is_empty() || batch.records.len() > max_records {
        return Err(ReplicationError::InvalidInput(
            "replica batch record count is outside its finite limit".into(),
        ));
    }
    let bytes = batch_payload_charge(batch)?;
    if bytes > max_bytes {
        return Err(ReplicationError::CapacityExceeded);
    }
    for (index, record) in batch.records.iter().enumerate() {
        let distance = u64::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| ReplicationError::InvalidInput("replica cursor overflow".into()))?;
        let expected = batch
            .after
            .offset
            .checked_add(distance)
            .ok_or_else(|| ReplicationError::InvalidInput("replica cursor overflow".into()))?;
        if record.cursor.version != CURSOR_VERSION
            || record.cursor.stream != batch.after.stream.stream
            || record.cursor.offset != expected
        {
            return Err(ReplicationError::InvalidInput(
                "replica batch must be a contiguous prefix for one exact stream lifetime".into(),
            ));
        }
    }
    Ok(bytes)
}

fn origin_history<'a>(
    state: &'a super::State,
    origin: OriginId,
    stream: &StreamKey,
) -> ReplicationResult<&'a super::StreamHistory> {
    if state.closed {
        return Err(ReplicationError::Closed);
    }
    if state.replication_origin != origin {
        return Err(ReplicationError::InvalidInput(
            "origin identity does not name this store".into(),
        ));
    }
    match state.streams.get(&stream.id) {
        Some(super::StreamEntry::Active(history)) if &history.key == stream => Ok(history),
        Some(super::StreamEntry::Active(history)) => Err(ReplicationError::InvalidInput(format!(
            "stream incarnation was replaced by {:?}",
            history.key.incarnation
        ))),
        _ => Err(ReplicationError::InvalidInput(
            "origin stream is unavailable".into(),
        )),
    }
}

fn backlog_after(
    history: &super::StreamHistory,
    acknowledged: u64,
) -> ReplicationResult<(usize, u64)> {
    let mut rows = 0usize;
    let mut bytes = 0u64;
    let first = acknowledged.checked_add(1);
    if let Some(first) = first {
        for record in history.records.range(first..).map(|(_, record)| record) {
            rows = rows
                .checked_add(1)
                .ok_or(ReplicationError::CapacityExceeded)?;
            bytes = bytes
                .checked_add(
                    u64::try_from(record_charge(record)?)
                        .map_err(|_| ReplicationError::CapacityExceeded)?,
                )
                .ok_or(ReplicationError::CapacityExceeded)?;
        }
    }
    Ok((rows, bytes))
}

fn origin_receipt_charge(
    operation: &ReplicationOperationId,
    stream: &OriginStream,
) -> ReplicationResult<usize> {
    operation
        .as_str()
        .len()
        .checked_add(stream.stream.id.as_str().len())
        .and_then(|bytes| bytes.checked_add(RECEIPT_OVERHEAD))
        .ok_or(ReplicationError::CapacityExceeded)
}

fn destination_tail(state: &super::State, stream: &OriginStream) -> u64 {
    state
        .replica_histories
        .get(stream)
        .and_then(BTreeMap::last_key_value)
        .map(|(&offset, _)| offset)
        .or_else(|| {
            state
                .published_replica_bootstraps
                .get(stream)
                .and_then(|id| state.replica_bootstraps.get(id))
                .and_then(|bootstrap| bootstrap.publish.as_ref())
                .map(|(_, receipt)| receipt.committed_through.offset)
        })
        .unwrap_or(0)
}

impl MemoryStore {
    pub(super) fn minimum_replica_offset(state: &super::State, stream: &StreamKey) -> Option<u64> {
        state
            .origin_replicas_by_stream
            .get(stream)
            .and_then(|pairs| {
                pairs
                    .iter()
                    .filter_map(|pair| state.origin_replicas.get(pair))
                    .filter(|replica| {
                        matches!(
                            replica.status.mode,
                            ReplicaMode::Required | ReplicaMode::Bootstrapping
                        )
                    })
                    .map(|replica| replica.status.acknowledged.offset)
                    .min()
            })
    }

    pub(super) fn required_replica(state: &super::State, stream: &StreamKey) -> Option<ReplicaId> {
        state
            .origin_replicas_by_stream
            .get(stream)
            .and_then(|pairs| {
                pairs.iter().find_map(|pair| {
                    state
                        .origin_replicas
                        .get(pair)
                        .filter(|replica| {
                            matches!(
                                replica.status.mode,
                                ReplicaMode::Required | ReplicaMode::Bootstrapping
                            )
                        })
                        .map(|replica| replica.status.replica.clone())
                })
            })
    }

    fn reserve_origin_receipt(
        &self,
        state: &super::State,
        operation: &ReplicationOperationId,
        stream: &OriginStream,
        extra_bytes: usize,
    ) -> ReplicationResult<usize> {
        let charge = origin_receipt_charge(operation, stream)?
            .checked_add(extra_bytes)
            .ok_or(ReplicationError::CapacityExceeded)?;
        if state.origin_replication_receipts.len()
            >= self.options.replication.receipts.max_operation_receipts
            || state
                .origin_replication_receipt_bytes
                .checked_add(charge)
                .is_none_or(|bytes| {
                    bytes
                        > self
                            .options
                            .replication
                            .receipts
                            .max_operation_receipt_bytes
                })
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        Ok(charge)
    }

    pub(super) fn preflight_replication_append(
        &self,
        state: &super::State,
        stream: &StreamKey,
        charge: usize,
    ) -> crate::application::Result<DurableTimestampMillis> {
        let now = self.options.replication_clock.now();
        if state
            .last_replication_clock
            .is_some_and(|last| now.0 < last.0)
        {
            return Err(crate::application::Error::ReplicationClockRollback);
        }
        let charge =
            u64::try_from(charge).map_err(|_| crate::application::Error::CapacityExceeded)?;
        let Some(pairs) = state.origin_replicas_by_stream.get(stream) else {
            return Ok(now);
        };
        for pair in pairs {
            let replica = state
                .origin_replicas
                .get(pair)
                .expect("replica stream index points to attachment");
            if !matches!(
                replica.status.mode,
                ReplicaMode::Required | ReplicaMode::Bootstrapping
            ) {
                continue;
            }
            if replica
                .status
                .backlog_bytes
                .checked_add(charge)
                .is_none_or(|bytes| bytes > replica.max_backlog_bytes)
            {
                return Err(crate::application::Error::ReplicaBacklogExceeded {
                    replica: replica.status.replica.clone(),
                    limit_bytes: replica.max_backlog_bytes,
                });
            }
            if let Some(oldest) = replica.status.oldest_backlog_at {
                let max_age =
                    u64::try_from(replica.max_backlog_age.as_millis()).unwrap_or(u64::MAX);
                if now.0.saturating_sub(oldest.0) > max_age {
                    return Err(crate::application::Error::ReplicaBacklogExpired {
                        replica: replica.status.replica.clone(),
                    });
                }
            }
        }
        Ok(now)
    }

    pub(super) fn apply_replication_append(
        state: &mut super::State,
        stream: &StreamKey,
        offset: u64,
        charge: usize,
        now: DurableTimestampMillis,
    ) {
        let charge = u64::try_from(charge).expect("append preflight checked charge range");
        state
            .replication_commit_times
            .entry(stream.clone())
            .or_default()
            .insert(offset, now);
        let (index, replicas) = (&state.origin_replicas_by_stream, &mut state.origin_replicas);
        if let Some(pairs) = index.get(stream) {
            for pair in pairs {
                let replica = replicas
                    .get_mut(pair)
                    .expect("replica stream index points to attachment");
                if !matches!(
                    replica.status.mode,
                    ReplicaMode::Required | ReplicaMode::Bootstrapping
                ) {
                    continue;
                }
                replica.status.backlog_records = replica
                    .status
                    .backlog_records
                    .checked_add(1)
                    .expect("append preflight checked replica backlog record count");
                replica.status.backlog_bytes = replica
                    .status
                    .backlog_bytes
                    .checked_add(charge)
                    .expect("append preflight checked replica backlog bytes");
                replica.status.oldest_backlog_at.get_or_insert(now);
            }
        }
        state.last_replication_clock = Some(now);
    }
}

#[async_trait]
impl ReplicationOriginStore for MemoryStore {
    async fn origin_identity(&self) -> ReplicationResult<OriginId> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        Ok(state.replication_origin)
    }

    async fn attach_replica(
        &self,
        request: AttachReplica,
    ) -> ReplicationResult<AttachReplicaReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if let Some(prior) = state.origin_replication_receipts.get(&request.operation_id) {
            return match prior {
                MemoryOriginReceipt::Attach(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                )),
            };
        }
        if request.max_backlog_bytes == 0 || request.max_backlog_age.is_zero() {
            return Err(ReplicationError::InvalidInput(
                "replica backlog limits must be nonzero".into(),
            ));
        }
        let now = self.options.replication_clock.now();
        if state
            .last_replication_clock
            .is_some_and(|last| now.0 < last.0)
        {
            return Err(ReplicationError::ClockRollback {
                last: state.last_replication_clock.unwrap(),
                observed: now,
            });
        }
        let history = origin_history(&state, request.stream.origin, &request.stream.stream)?;
        let (mode, acknowledged, backlog_records, backlog_bytes, oldest_backlog_at) = match request
            .start
        {
            ReplicaStart::FromBeginning => {
                if history.floor != 0 {
                    return Err(ReplicationError::NeedsBootstrap);
                }
                let (rows, bytes) = backlog_after(history, 0)?;
                if bytes > request.max_backlog_bytes {
                    return Err(ReplicationError::BacklogExceeded {
                        limit_bytes: request.max_backlog_bytes,
                    });
                }
                let oldest = history
                    .records
                    .first_key_value()
                    .map(|(&offset, _)| {
                        state
                            .replication_commit_times
                            .get(&request.stream.stream)
                            .and_then(|times| times.get(&offset))
                            .copied()
                            .ok_or_else(|| {
                                ReplicationError::CorruptStorage(
                                    "origin record is missing its replication commit time".into(),
                                )
                            })
                    })
                    .transpose()?;
                (ReplicaMode::Required, 0, rows, bytes, oldest)
            }
            ReplicaStart::NeedsBootstrap => (
                ReplicaMode::DetachedNeedsBootstrap,
                history.tail,
                0,
                0,
                None,
            ),
        };
        let pair = (request.replica.clone(), request.stream.clone());
        if state.origin_replicas.contains_key(&pair) {
            return Err(ReplicationError::InvalidInput(
                "replica is already attached to this origin stream".into(),
            ));
        }
        let stream_count = state
            .origin_replicas
            .keys()
            .filter(|(_, stream)| stream == &request.stream)
            .count();
        if state.origin_replicas.len() >= self.options.replication.attachments.max_replicas
            || stream_count >= self.options.replication.attachments.max_replicas_per_stream
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        let charge =
            self.reserve_origin_receipt(&state, &request.operation_id, &request.stream, 0)?;
        let status = ReplicaStatus {
            replica: request.replica.clone(),
            stream: request.stream.clone(),
            destination_epoch: request.destination_epoch,
            acknowledged: ReplicaPosition {
                stream: request.stream.clone(),
                offset: acknowledged,
            },
            backlog_records,
            backlog_bytes,
            oldest_backlog_at,
            mode,
            pending_batch: None,
        };
        let receipt = AttachReplicaReceipt {
            request: request.clone(),
            status: status.clone(),
        };
        state.last_replication_clock = Some(now);
        state.origin_replicas.insert(
            pair.clone(),
            MemoryOriginReplica {
                status,
                max_backlog_bytes: request.max_backlog_bytes,
                max_backlog_age: request.max_backlog_age,
                pending: None,
                bootstrap: None,
            },
        );
        state
            .origin_replicas_by_stream
            .entry(request.stream.stream.clone())
            .or_default()
            .push(pair);
        state.origin_replication_receipt_bytes += charge;
        state.origin_replication_receipts.insert(
            request.operation_id.clone(),
            MemoryOriginReceipt::Attach(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn replica_status(
        &self,
        replica: &ReplicaId,
        stream: &OriginStream,
    ) -> ReplicationResult<ReplicaStatus> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        state
            .origin_replicas
            .get(&(replica.clone(), stream.clone()))
            .map(|replica| replica.status.clone())
            .ok_or_else(|| ReplicationError::NotFound {
                replica: replica.clone(),
            })
    }

    async fn prepare_replica_batch(
        &self,
        request: PrepareReplicaBatch,
    ) -> ReplicationResult<PrepareReplicaBatchReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if let Some(prior) = state.origin_replication_receipts.get(&request.operation_id) {
            return match prior {
                MemoryOriginReceipt::Prepare(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                )),
            };
        }
        if request.limits.max_records == 0
            || request.limits.max_bytes == 0
            || request.limits.max_records > self.options.replication.batches.max_batch_records
            || request.limits.max_bytes > self.options.replication.batches.max_batch_bytes
        {
            return Err(ReplicationError::InvalidInput(
                "replica batch limits exceed configured limits".into(),
            ));
        }
        let pair = (request.replica.clone(), request.stream.clone());
        let replica =
            state
                .origin_replicas
                .get(&pair)
                .ok_or_else(|| ReplicationError::NotFound {
                    replica: request.replica.clone(),
                })?;
        if replica.status.mode != ReplicaMode::Required {
            return Err(ReplicationError::NeedsBootstrap);
        }
        if request.expected_after != replica.status.acknowledged {
            return Err(ReplicationError::StaleProgress {
                current: Box::new(replica.status.acknowledged.clone()),
            });
        }
        if let Some(pending) = &replica.pending {
            return if pending.id == request.batch_id {
                Err(ReplicationError::BatchConflict {
                    batch: request.batch_id,
                })
            } else {
                Err(ReplicationError::CapacityExceeded)
            };
        }
        let history = origin_history(&state, request.stream.origin, &request.stream.stream)?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        if request.expected_after.offset < history.tail {
            let first = request
                .expected_after
                .offset
                .checked_add(1)
                .ok_or_else(|| ReplicationError::InvalidInput("replica cursor overflow".into()))?;
            for record in history.records.range(first..).map(|(_, record)| record) {
                let charge = record_charge(record)?;
                if records.is_empty() && charge > request.limits.max_bytes {
                    return Err(ReplicationError::CapacityExceeded);
                }
                if records.len() == request.limits.max_records
                    || bytes
                        .checked_add(charge)
                        .is_none_or(|n| n > request.limits.max_bytes)
                {
                    break;
                }
                bytes += charge;
                records.push(Arc::clone(record));
            }
        }
        let batch = (!records.is_empty()).then(|| ReplicaBatch {
            id: request.batch_id,
            destination_epoch: replica.status.destination_epoch,
            after: request.expected_after.clone(),
            records,
        });
        let retained = batch.as_ref().map_or(0, |batch| {
            batch.records.len() * std::mem::size_of::<Arc<Record>>() + bytes
        });
        if batch.is_some()
            && (state
                .origin_pending_batch_bytes
                .checked_add(retained)
                .is_none_or(|n| n > self.options.replication.batches.max_pending_batch_bytes)
                || state
                    .origin_replicas
                    .values()
                    .filter(|replica| replica.pending.is_some())
                    .count()
                    >= self.options.replication.batches.max_pending_batches)
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        let receipt_extra = retained;
        let charge = self.reserve_origin_receipt(
            &state,
            &request.operation_id,
            &request.stream,
            receipt_extra,
        )?;
        let replica = state.origin_replicas.get_mut(&pair).unwrap();
        replica.pending = batch.clone();
        replica.status.pending_batch = batch.as_ref().map(|batch| batch.id);
        let receipt = PrepareReplicaBatchReceipt {
            request: request.clone(),
            batch,
            status: replica.status.clone(),
        };
        state.origin_pending_batch_bytes += retained;
        state.origin_replication_receipt_bytes += charge;
        state.origin_replication_receipts.insert(
            request.operation_id.clone(),
            MemoryOriginReceipt::Prepare(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn acknowledge_replica_batch(
        &self,
        request: AcknowledgeReplicaBatch,
    ) -> ReplicationResult<AcknowledgeReplicaBatchReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if let Some(prior) = state.origin_replication_receipts.get(&request.operation_id) {
            return match prior {
                MemoryOriginReceipt::Acknowledge(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                )),
            };
        }
        let pair = (
            request.replica.clone(),
            request.expected_after.stream.clone(),
        );
        let replica =
            state
                .origin_replicas
                .get(&pair)
                .ok_or_else(|| ReplicationError::NotFound {
                    replica: request.replica.clone(),
                })?;
        let pending = replica
            .pending
            .as_ref()
            .ok_or_else(|| ReplicationError::InvalidInput("replica has no pending batch".into()))?;
        let expected_through = ReplicaPosition {
            stream: pending.after.stream.clone(),
            offset: pending
                .records
                .last()
                .expect("pending batch is nonempty")
                .cursor
                .offset,
        };
        if request.expected_after != replica.status.acknowledged
            || request.receipt.batch != pending.id
            || request.receipt.destination_epoch != replica.status.destination_epoch
            || request.receipt.committed_through != expected_through
        {
            return Err(ReplicationError::InvalidReceipt(
                "replica receipt does not match the exact pending batch".into(),
            ));
        }
        let pending_charge = pending.records.len() * std::mem::size_of::<Arc<Record>>()
            + batch_payload_charge(pending)?;
        let acknowledged_records = pending.records.len();
        let acknowledged_bytes = u64::try_from(batch_payload_charge(pending)?)
            .map_err(|_| ReplicationError::CapacityExceeded)?;
        let _history = origin_history(
            &state,
            request.expected_after.stream.origin,
            &request.expected_after.stream.stream,
        )?;
        let next_oldest = expected_through.offset.checked_add(1).and_then(|offset| {
            state
                .replication_commit_times
                .get(&request.expected_after.stream.stream)
                .and_then(|times| times.get(&offset))
                .copied()
        });
        let charge = self.reserve_origin_receipt(
            &state,
            &request.operation_id,
            &request.expected_after.stream,
            0,
        )?;
        let replica = state.origin_replicas.get_mut(&pair).unwrap();
        replica.pending = None;
        replica.bootstrap = None;
        replica.status.pending_batch = None;
        replica.status.acknowledged = expected_through;
        replica.status.backlog_records = replica
            .status
            .backlog_records
            .checked_sub(acknowledged_records)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("replica backlog row underflow".into())
            })?;
        replica.status.backlog_bytes = replica
            .status
            .backlog_bytes
            .checked_sub(acknowledged_bytes)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("replica backlog byte underflow".into())
            })?;
        replica.status.oldest_backlog_at = next_oldest;
        let receipt = AcknowledgeReplicaBatchReceipt {
            request: request.clone(),
            status: replica.status.clone(),
        };
        state.origin_pending_batch_bytes = state
            .origin_pending_batch_bytes
            .checked_sub(pending_charge)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("pending batch byte underflow".into())
            })?;
        state.origin_replication_receipt_bytes += charge;
        state.origin_replication_receipts.insert(
            request.operation_id.clone(),
            MemoryOriginReceipt::Acknowledge(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn detach_replica(
        &self,
        request: DetachReplica,
    ) -> ReplicationResult<DetachReplicaReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if let Some(prior) = state.origin_replication_receipts.get(&request.operation_id) {
            return match prior {
                MemoryOriginReceipt::Detach(receipt) if receipt.request == request => {
                    Ok(receipt.clone())
                }
                _ => Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                )),
            };
        }
        let pair = (request.replica.clone(), request.stream.clone());
        let replica =
            state
                .origin_replicas
                .get(&pair)
                .ok_or_else(|| ReplicationError::NotFound {
                    replica: request.replica.clone(),
                })?;
        let pending_charge = replica.pending.as_ref().map_or(Ok(0), |pending| {
            batch_payload_charge(pending)
                .map(|bytes| bytes + pending.records.len() * std::mem::size_of::<Arc<Record>>())
        })?;
        let bootstrap_lease = replica
            .bootstrap
            .as_ref()
            .map(|bootstrap| bootstrap.recovery_lease);
        let charge =
            self.reserve_origin_receipt(&state, &request.operation_id, &request.stream, 0)?;
        let replica = state.origin_replicas.get_mut(&pair).unwrap();
        replica.pending = None;
        replica.status.pending_batch = None;
        replica.status.mode = ReplicaMode::DetachedNeedsBootstrap;
        replica.status.backlog_records = 0;
        replica.status.backlog_bytes = 0;
        replica.status.oldest_backlog_at = None;
        replica.bootstrap = None;
        let status = replica.status.clone();
        if let Some(lease) = bootstrap_lease {
            state.recovery_leases.remove(&lease);
        }
        let receipt = DetachReplicaReceipt {
            request: request.clone(),
            status,
        };
        state.origin_pending_batch_bytes = state
            .origin_pending_batch_bytes
            .checked_sub(pending_charge)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("pending batch byte underflow".into())
            })?;
        state.origin_replication_receipt_bytes += charge;
        state.origin_replication_receipts.insert(
            request.operation_id.clone(),
            MemoryOriginReceipt::Detach(receipt.clone()),
        );
        Ok(receipt)
    }
}

#[async_trait]
impl ReplicationStore for MemoryStore {
    async fn begin_origin_bootstrap(
        &self,
        request: BeginOriginBootstrap,
    ) -> ReplicationResult<BeginOriginBootstrapReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if let Some(prior) = state
            .origin_replication_receipts
            .get(&request.operation_id)
            .cloned()
        {
            let MemoryOriginReceipt::BeginBootstrap { receipt, completed } = prior else {
                return Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                ));
            };
            if receipt.request != request {
                return Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                ));
            }
            if completed {
                return Ok(receipt);
            }
            let pair = (request.replica.clone(), request.stream.clone());
            // An old retry must never detach a newer bootstrap attempt.
            if state
                .origin_replicas
                .get(&pair)
                .and_then(|replica| replica.bootstrap.as_ref())
                .is_none_or(|bootstrap| bootstrap.request != request)
            {
                return Err(ReplicationError::BootstrapProtectionExpired {
                    id: request.bootstrap_id,
                });
            }
            let lease = state
                .origin_replicas
                .get(&pair)
                .and_then(|replica| replica.bootstrap.as_ref())
                .map(|bootstrap| bootstrap.recovery_lease);
            let now = self.options.snapshot_clock.now();
            if lease
                .and_then(|lease| state.recovery_leases.get(&lease))
                .is_some_and(|lease| lease.expires.0 > now.0)
            {
                return Ok(receipt);
            }
            if let Some(lease) = lease {
                state.recovery_leases.remove(&lease);
            }
            if let Some(replica) = state.origin_replicas.get_mut(&pair) {
                replica.bootstrap = None;
                replica.pending = None;
                replica.status.mode = ReplicaMode::DetachedNeedsBootstrap;
                replica.status.pending_batch = None;
                replica.status.backlog_records = 0;
                replica.status.backlog_bytes = 0;
                replica.status.oldest_backlog_at = None;
            }
            return Err(ReplicationError::BootstrapProtectionExpired {
                id: request.bootstrap_id,
            });
        }
        if request.captured_tail.stream != request.stream
            || request.snapshot.covered.stream != request.stream.stream
            || request.snapshot.covered.offset > request.captured_tail.offset
        {
            return Err(ReplicationError::InvalidInput(
                "origin bootstrap boundaries must name one ordered stream".into(),
            ));
        }
        let snapshot = Self::published_snapshot_for_replication(&state, request.snapshot.id)
            .ok_or_else(|| ReplicationError::InvalidInput("snapshot is not published".into()))?;
        if snapshot != request.snapshot {
            return Err(ReplicationError::InvalidInput(
                "bootstrap descriptor does not match the published snapshot".into(),
            ));
        }
        let history = origin_history(&state, request.stream.origin, &request.stream.stream)?;
        if request.captured_tail.offset > history.tail {
            return Err(ReplicationError::StaleProgress {
                current: Box::new(ReplicaPosition {
                    stream: request.stream.clone(),
                    offset: history.tail,
                }),
            });
        }
        let mut suffix_records = 0usize;
        let mut suffix_bytes = 0u64;
        let first = request.snapshot.covered.offset.checked_add(1);
        if request.snapshot.covered.offset < request.captured_tail.offset {
            let first = first.ok_or_else(|| {
                ReplicationError::InvalidInput("bootstrap suffix cursor overflow".into())
            })?;
            for record in history
                .records
                .range(first..=request.captured_tail.offset)
                .map(|(_, record)| record)
            {
                suffix_records = suffix_records
                    .checked_add(1)
                    .ok_or(ReplicationError::CapacityExceeded)?;
                suffix_bytes = suffix_bytes
                    .checked_add(
                        u64::try_from(record_charge(record)?)
                            .map_err(|_| ReplicationError::CapacityExceeded)?,
                    )
                    .ok_or(ReplicationError::CapacityExceeded)?;
            }
        }
        let pair = (request.replica.clone(), request.stream.clone());
        let replica =
            state
                .origin_replicas
                .get(&pair)
                .ok_or_else(|| ReplicationError::NotFound {
                    replica: request.replica.clone(),
                })?;
        if replica.status.mode != ReplicaMode::DetachedNeedsBootstrap {
            return Err(ReplicationError::InvalidInput(
                "replica is not waiting for bootstrap".into(),
            ));
        }
        if request.destination_epoch != replica.status.destination_epoch
            || suffix_bytes > replica.max_backlog_bytes
        {
            return if request.destination_epoch != replica.status.destination_epoch {
                Err(ReplicationError::DestinationReplaced {
                    current: replica.status.destination_epoch,
                })
            } else {
                Err(ReplicationError::BacklogExceeded {
                    limit_bytes: replica.max_backlog_bytes,
                })
            };
        }
        let charge = self.reserve_origin_receipt(
            &state,
            &request.operation_id,
            &request.stream,
            request
                .snapshot
                .accounted_bytes()
                .ok_or(ReplicationError::CapacityExceeded)?,
        )?;
        let oldest = request
            .snapshot
            .covered
            .offset
            .checked_add(1)
            .and_then(|offset| {
                state
                    .replication_commit_times
                    .get(&request.stream.stream)
                    .and_then(|times| times.get(&offset))
                    .copied()
            });
        let (recovery_lease, protection_expires_at) =
            self.acquire_replication_snapshot_lease(&mut state, request.snapshot.id)?;
        let replica = state.origin_replicas.get_mut(&pair).unwrap();
        replica.status.mode = ReplicaMode::Bootstrapping;
        replica.status.acknowledged = ReplicaPosition {
            stream: request.stream.clone(),
            offset: request.snapshot.covered.offset,
        };
        replica.status.backlog_records = suffix_records;
        replica.status.backlog_bytes = suffix_bytes;
        replica.status.oldest_backlog_at = oldest;
        replica.bootstrap = Some(MemoryOriginBootstrap {
            request: request.clone(),
            suffix_records,
            suffix_bytes,
            recovery_lease,
        });
        let receipt = BeginOriginBootstrapReceipt {
            request: request.clone(),
            status: replica.status.clone(),
            protection_expires_at,
        };
        state.origin_replication_receipt_bytes += charge;
        state.origin_replication_receipts.insert(
            request.operation_id.clone(),
            MemoryOriginReceipt::BeginBootstrap {
                receipt: receipt.clone(),
                completed: false,
            },
        );
        Ok(receipt)
    }

    async fn acknowledge_origin_bootstrap(
        &self,
        request: AcknowledgeOriginBootstrap,
    ) -> ReplicationResult<AcknowledgeOriginBootstrapReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if let Some(prior) = state.origin_replication_receipts.get(&request.operation_id) {
            return match prior {
                MemoryOriginReceipt::AcknowledgeBootstrap(receipt)
                    if receipt.request == request =>
                {
                    Ok(receipt.clone())
                }
                _ => Err(ReplicationError::InvalidInput(
                    "replication operation identity was reused".into(),
                )),
            };
        }
        let pair = (request.replica.clone(), request.stream.clone());
        let replica =
            state
                .origin_replicas
                .get(&pair)
                .ok_or_else(|| ReplicationError::NotFound {
                    replica: request.replica.clone(),
                })?;
        let bootstrap = replica.bootstrap.as_ref().ok_or_else(|| {
            ReplicationError::InvalidInput("replica has no active bootstrap".into())
        })?;
        let recovery_lease = bootstrap.recovery_lease;
        let now = self.options.snapshot_clock.now();
        if state
            .recovery_leases
            .get(&recovery_lease)
            .is_none_or(|lease| lease.expires.0 <= now.0)
        {
            state.recovery_leases.remove(&recovery_lease);
            let replica = state.origin_replicas.get_mut(&pair).unwrap();
            replica.bootstrap = None;
            replica.pending = None;
            replica.status.mode = ReplicaMode::DetachedNeedsBootstrap;
            replica.status.pending_batch = None;
            replica.status.backlog_records = 0;
            replica.status.backlog_bytes = 0;
            replica.status.oldest_backlog_at = None;
            return Err(ReplicationError::BootstrapProtectionExpired {
                id: request.receipt.request.id,
            });
        }
        if request.receipt.request.stream != bootstrap.request.stream
            || request.receipt.request.id != bootstrap.request.bootstrap_id
            || request.receipt.request.operation_id != bootstrap.request.destination_operation_id
            || request.receipt.request.replica != request.replica
            || request.receipt.request.destination_epoch != bootstrap.request.destination_epoch
            || request.receipt.request.snapshot != bootstrap.request.snapshot
            || request.receipt.request.through != bootstrap.request.captured_tail
            || request.receipt.committed_through != bootstrap.request.captured_tail
        {
            return Err(ReplicationError::InvalidReceipt(
                "bootstrap receipt does not match the exact active attempt".into(),
            ));
        }
        let begin_operation_id = bootstrap.request.operation_id.clone();
        let suffix_records = bootstrap.suffix_records;
        let suffix_bytes = bootstrap.suffix_bytes;
        let next_oldest = request
            .receipt
            .committed_through
            .offset
            .checked_add(1)
            .and_then(|offset| {
                state
                    .replication_commit_times
                    .get(&request.stream.stream)
                    .and_then(|times| times.get(&offset))
                    .copied()
            });
        let charge =
            self.reserve_origin_receipt(&state, &request.operation_id, &request.stream, 0)?;
        let replica = state.origin_replicas.get_mut(&pair).unwrap();
        replica.status.mode = ReplicaMode::Required;
        replica.status.acknowledged = request.receipt.committed_through.clone();
        replica.status.backlog_records = replica
            .status
            .backlog_records
            .checked_sub(suffix_records)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("bootstrap backlog row underflow".into())
            })?;
        replica.status.backlog_bytes = replica
            .status
            .backlog_bytes
            .checked_sub(suffix_bytes)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("bootstrap backlog byte underflow".into())
            })?;
        replica.status.oldest_backlog_at = next_oldest;
        replica.bootstrap = None;
        let status = replica.status.clone();
        state.recovery_leases.remove(&recovery_lease);
        let receipt = AcknowledgeOriginBootstrapReceipt {
            request: request.clone(),
            status,
        };
        if let Some(MemoryOriginReceipt::BeginBootstrap { completed, .. }) = state
            .origin_replication_receipts
            .get_mut(&begin_operation_id)
        {
            *completed = true;
        }
        state.origin_replication_receipt_bytes += charge;
        state.origin_replication_receipts.insert(
            request.operation_id.clone(),
            MemoryOriginReceipt::AcknowledgeBootstrap(receipt.clone()),
        );
        Ok(receipt)
    }

    async fn cleanup_replication(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress> {
        if limits.max_receipt_rows == 0 || limits.max_staging_rows == 0 || limits.max_bytes == 0 {
            return Err(ReplicationError::InvalidInput(
                "replication cleanup limits must be nonzero".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let detached: Vec<_> = state
            .origin_replicas
            .iter()
            .filter(|(_, replica)| {
                replica.status.mode == ReplicaMode::DetachedNeedsBootstrap
                    && replica.pending.is_none()
                    && replica.bootstrap.is_none()
            })
            .take(limits.max_staging_rows)
            .map(|(pair, _)| pair.clone())
            .collect();
        let mut removed = 0usize;
        let mut bytes = 0usize;
        for pair in detached {
            let charge = pair
                .0
                .as_str()
                .len()
                .checked_add(pair.1.stream.id.as_str().len())
                .and_then(|n| n.checked_add(RECEIPT_OVERHEAD))
                .ok_or(ReplicationError::CapacityExceeded)?;
            if bytes
                .checked_add(charge)
                .is_none_or(|n| n > limits.max_bytes)
            {
                break;
            }
            state.origin_replicas.remove(&pair);
            if let Some(pairs) = state.origin_replicas_by_stream.get_mut(&pair.1.stream) {
                pairs.retain(|candidate| candidate != &pair);
                if pairs.is_empty() {
                    state.origin_replicas_by_stream.remove(&pair.1.stream);
                }
            }
            removed += 1;
            bytes += charge;
        }
        let remaining = state.origin_replicas.values().any(|replica| {
            replica.status.mode == ReplicaMode::DetachedNeedsBootstrap
                && replica.pending.is_none()
                && replica.bootstrap.is_none()
        });
        Ok(ReplicaCleanupProgress {
            removed_receipt_rows: 0,
            removed_staging_rows: removed,
            removed_bytes: bytes,
            remaining,
        })
    }
}

#[async_trait]
impl ReplicaBatchDestinationStore for MemoryStore {
    async fn destination_epoch(&self) -> ReplicationResult<DestinationEpoch> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        Ok(state.replica_destination_epoch)
    }

    async fn commit_replica_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        let bytes = validate_batch(
            &batch,
            self.options.replica_destination.storage.max_batch_records,
            self.options.replica_destination.storage.max_batch_bytes,
        )?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if batch.destination_epoch != state.replica_destination_epoch {
            return Err(ReplicationError::DestinationReplaced {
                current: state.replica_destination_epoch,
            });
        }
        if batch.after.offset
            < state
                .replica_receipt_floors
                .get(&batch.after.stream)
                .copied()
                .unwrap_or(0)
        {
            return Err(ReplicationError::ReceiptExpired);
        }
        if let Some((prior, receipt)) = state.replica_receipts.get(&batch.id) {
            if prior == &batch {
                return Ok(receipt.clone());
            }
            return Err(ReplicationError::BatchConflict { batch: batch.id });
        }
        let current = destination_tail(&state, &batch.after.stream);
        if current != batch.after.offset {
            return Err(ReplicationError::StaleProgress {
                current: Box::new(ReplicaPosition {
                    stream: batch.after.stream.clone(),
                    offset: current,
                }),
            });
        }
        let new_records = batch.records.len();
        let new_history_bytes = state
            .replica_history_bytes
            .checked_add(u64::try_from(bytes).map_err(|_| ReplicationError::CapacityExceeded)?)
            .ok_or(ReplicationError::CapacityExceeded)?;
        let receipt_bytes = batch_receipt_charge(&batch)?;
        if state.replica_histories.len()
            + usize::from(!state.replica_histories.contains_key(&batch.after.stream))
            > self.options.replica_destination.storage.max_origin_streams
            || state
                .replica_history_records
                .checked_add(new_records)
                .is_none_or(|n| n > self.options.replica_destination.storage.max_history_records)
            || new_history_bytes > self.options.replica_destination.storage.max_history_bytes
            || state
                .replica_receipts
                .len()
                .checked_add(state.replica_floor_receipts.len())
                .is_none_or(|rows| {
                    rows >= self.options.replica_destination.receipts.max_batch_receipts
                })
            || state
                .replica_receipt_bytes
                .checked_add(receipt_bytes)
                .is_none_or(|n| {
                    n > self
                        .options
                        .replica_destination
                        .receipts
                        .max_batch_receipt_bytes
                })
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        let committed_through = ReplicaPosition {
            stream: batch.after.stream.clone(),
            offset: batch
                .records
                .last()
                .expect("nonempty validated batch")
                .cursor
                .offset,
        };
        let receipt = ReplicaReceipt {
            batch: batch.id,
            destination_epoch: batch.destination_epoch,
            committed_through,
        };
        let history = state
            .replica_histories
            .entry(batch.after.stream.clone())
            .or_default();
        for record in &batch.records {
            history.insert(record.cursor.offset, Arc::clone(record));
        }
        state.replica_history_records += new_records;
        state.replica_history_bytes = new_history_bytes;
        state.replica_receipt_bytes += receipt_bytes;
        let mut stored_records = Vec::with_capacity(batch.records.len());
        stored_records.extend(batch.records.iter().cloned());
        let stored_batch = ReplicaBatch {
            id: batch.id,
            destination_epoch: batch.destination_epoch,
            after: batch.after,
            records: stored_records,
        };
        state
            .replica_receipts
            .insert(stored_batch.id, (stored_batch, receipt.clone()));
        Ok(receipt)
    }

    async fn read_replica_after(
        &self,
        after: &ReplicaPosition,
        limits: ReplicaBatchLimits,
    ) -> ReplicationResult<ReplicaPage> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(ReplicationError::InvalidInput(
                "replica page limits must be nonzero".into(),
            ));
        }
        if limits.max_records > self.options.replica_destination.storage.max_batch_records
            || limits.max_bytes > self.options.replica_destination.storage.max_batch_bytes
        {
            return Err(ReplicationError::InvalidInput(
                "replica page limits exceed configured store limits".into(),
            ));
        }
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let history = state.replica_histories.get(&after.stream);
        let tail = destination_tail(&state, &after.stream);
        if after.offset > tail {
            return Err(ReplicationError::StaleProgress {
                current: Box::new(ReplicaPosition {
                    stream: after.stream.clone(),
                    offset: tail,
                }),
            });
        }
        let mut records = Vec::new();
        let mut bytes = 0usize;
        if let Some(history) = history.filter(|_| after.offset < tail) {
            let first = after
                .offset
                .checked_add(1)
                .ok_or_else(|| ReplicationError::InvalidInput("replica cursor overflow".into()))?;
            for (&offset, record) in history.range(first..) {
                let charge = record_charge(record)?;
                if records.is_empty() && charge > limits.max_bytes {
                    return Err(ReplicationError::CapacityExceeded);
                }
                if records.len() == limits.max_records
                    || bytes
                        .checked_add(charge)
                        .is_none_or(|n| n > limits.max_bytes)
                {
                    break;
                }
                let expected = first
                    .checked_add(u64::try_from(records.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| {
                        ReplicationError::CorruptStorage("replica cursor overflow".into())
                    })?;
                if offset != expected {
                    return Err(ReplicationError::CorruptStorage(
                        "replica history contains a gap".into(),
                    ));
                }
                bytes += charge;
                records.push(Arc::clone(record));
            }
        }
        let next_offset = records
            .last()
            .map_or(after.offset, |record| record.cursor.offset);
        Ok(ReplicaPage {
            after: after.clone(),
            records,
            next: ReplicaPosition {
                stream: after.stream.clone(),
                offset: next_offset,
            },
            complete: next_offset == tail,
        })
    }

    async fn advance_replica_receipt_floor(
        &self,
        request: AdvanceReplicaReceiptFloor,
    ) -> ReplicationResult<AdvanceReplicaReceiptFloorReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if request.destination_epoch != state.replica_destination_epoch {
            return Err(ReplicationError::DestinationReplaced {
                current: state.replica_destination_epoch,
            });
        }
        if let Some(prior) = state.replica_floor_receipts.get(&request.operation_id) {
            return if prior.request == request {
                Ok(prior.clone())
            } else {
                Err(ReplicationError::InvalidInput(
                    "replica receipt-floor operation identity was reused".into(),
                ))
            };
        }
        if !state.replica_histories.contains_key(&request.stream) {
            return Err(ReplicationError::InvalidInput(
                "cannot create a receipt floor for an unknown origin stream".into(),
            ));
        }
        let current = state
            .replica_receipt_floors
            .get(&request.stream)
            .copied()
            .unwrap_or(0);
        if request.expected_floor.stream != request.stream
            || request.new_floor.stream != request.stream
            || request.expected_floor.offset != current
            || request.new_floor.offset < current
        {
            return Err(ReplicationError::StaleProgress {
                current: Box::new(ReplicaPosition {
                    stream: request.stream.clone(),
                    offset: current,
                }),
            });
        }
        let tail = destination_tail(&state, &request.stream);
        if request.new_floor.offset > tail {
            return Err(ReplicationError::InvalidInput(
                "replica receipt floor is beyond committed history".into(),
            ));
        }
        let receipt = AdvanceReplicaReceiptFloorReceipt {
            request: request.clone(),
        };
        let receipt_charge = request
            .stream
            .stream
            .id
            .as_str()
            .len()
            .checked_add(
                request
                    .operation_id
                    .as_str()
                    .len()
                    .saturating_add(RECEIPT_OVERHEAD),
            )
            .ok_or(ReplicationError::CapacityExceeded)?;
        if state
            .replica_receipts
            .len()
            .checked_add(state.replica_floor_receipts.len())
            .and_then(|rows| rows.checked_add(1))
            .is_none_or(|rows| rows > self.options.replica_destination.receipts.max_batch_receipts)
            || state
                .replica_receipt_bytes
                .checked_add(receipt_charge)
                .is_none_or(|bytes| {
                    bytes
                        > self
                            .options
                            .replica_destination
                            .receipts
                            .max_batch_receipt_bytes
                })
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        state
            .replica_receipt_floors
            .insert(request.stream.clone(), request.new_floor.offset);
        state
            .replica_floor_receipts
            .insert(request.operation_id.clone(), receipt.clone());
        state.replica_receipt_bytes += receipt_charge;
        Ok(receipt)
    }

    async fn close_replica_destination(&self) -> ReplicationResult<()> {
        self.state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?
            .closed = true;
        Ok(())
    }
}

#[async_trait]
impl ReplicaDestinationStore for MemoryStore {
    async fn begin_replica_bootstrap(
        &self,
        request: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt> {
        if request.snapshot.covered.stream != request.stream.stream
            || request.through.stream != request.stream
            || request.snapshot.covered.offset > request.through.offset
        {
            return Err(ReplicationError::InvalidInput(
                "bootstrap snapshot and tail must name one ordered origin stream".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if request.destination_epoch != state.replica_destination_epoch {
            return Err(ReplicationError::DestinationReplaced {
                current: state.replica_destination_epoch,
            });
        }
        if let Some(prior) = state.replica_bootstraps.get(&request.id) {
            return if prior.request == request {
                Ok(BeginReplicaBootstrapReceipt {
                    request,
                    state: prior.state,
                    accepted_bytes: prior.accepted_bytes,
                })
            } else {
                Err(ReplicationError::BootstrapUnknown(Box::new(request)))
            };
        }
        let staging_count = state
            .replica_bootstraps
            .values()
            .filter(|bootstrap| {
                matches!(
                    bootstrap.state,
                    ReplicaBootstrapState::Staging | ReplicaBootstrapState::Verified
                )
            })
            .count();
        let receipt_charge = bootstrap_receipt_charge(&request)?;
        if staging_count
            >= self
                .options
                .replica_destination
                .staging
                .max_staging_bootstraps
            || state
                .replica_bootstraps
                .len()
                .checked_add(state.replica_receipts.len())
                .and_then(|rows| rows.checked_add(state.replica_floor_receipts.len()))
                .is_none_or(|rows| {
                    rows >= self.options.replica_destination.receipts.max_batch_receipts
                })
            || state
                .replica_receipt_bytes
                .checked_add(receipt_charge)
                .is_none_or(|bytes| {
                    bytes
                        > self
                            .options
                            .replica_destination
                            .receipts
                            .max_batch_receipt_bytes
                })
            || request.snapshot.content_bytes
                > self.options.replica_destination.staging.max_staging_bytes
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        state.replica_bootstraps.insert(
            request.id,
            MemoryReplicaBootstrap {
                request: request.clone(),
                state: ReplicaBootstrapState::Staging,
                chunks: BTreeMap::new(),
                accepted_bytes: 0,
                suffix_records: BTreeMap::new(),
                suffix_batches: std::collections::HashMap::new(),
                suffix_bytes: 0,
                verification: None,
                publish: None,
                abort: None,
                readers: 0,
            },
        );
        state.replica_receipt_bytes += receipt_charge;
        Ok(BeginReplicaBootstrapReceipt {
            request,
            state: ReplicaBootstrapState::Staging,
            accepted_bytes: 0,
        })
    }

    async fn put_replica_bootstrap_chunk(
        &self,
        chunk: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
        if chunk.chunk.bytes.is_empty()
            || chunk.chunk.bytes.len() > self.options.replica_destination.staging.max_chunk_bytes
        {
            return Err(ReplicationError::InvalidInput(
                "bootstrap chunk is empty or exceeds the configured chunk limit".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let bootstrap = state
            .replica_bootstraps
            .get(&chunk.id)
            .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
        if let Some(prior) = bootstrap.chunks.get(&chunk.chunk.offset) {
            return if prior == &chunk.chunk.bytes {
                Ok(ReplicaBootstrapChunkReceipt {
                    id: chunk.id,
                    offset: chunk.chunk.offset,
                    end: chunk.chunk.offset + u64::try_from(prior.len()).unwrap_or(u64::MAX),
                })
            } else {
                Err(ReplicationError::InvalidInput(
                    "bootstrap chunk offset was reused with different bytes".into(),
                ))
            };
        }
        if bootstrap.state != ReplicaBootstrapState::Staging
            || chunk.chunk.offset != bootstrap.accepted_bytes
        {
            return Err(ReplicationError::InvalidInput(
                "bootstrap chunks must append contiguously while staging".into(),
            ));
        }
        let end = chunk
            .chunk
            .offset
            .checked_add(
                u64::try_from(chunk.chunk.bytes.len())
                    .map_err(|_| ReplicationError::CapacityExceeded)?,
            )
            .ok_or(ReplicationError::CapacityExceeded)?;
        if end > bootstrap.request.snapshot.content_bytes
            || state.replica_staging_chunks
                >= self.options.replica_destination.staging.max_staging_chunks
            || state
                .replica_staging_bytes
                .checked_add(u64::try_from(chunk.chunk.bytes.len()).unwrap_or(u64::MAX))
                .is_none_or(|bytes| {
                    bytes > self.options.replica_destination.staging.max_staging_bytes
                })
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        let bytes = chunk.chunk.bytes.len();
        let bootstrap = state.replica_bootstraps.get_mut(&chunk.id).unwrap();
        bootstrap
            .chunks
            .insert(chunk.chunk.offset, chunk.chunk.bytes);
        bootstrap.accepted_bytes = end;
        state.replica_staging_chunks += 1;
        state.replica_staging_bytes += u64::try_from(bytes).unwrap();
        Ok(ReplicaBootstrapChunkReceipt {
            id: chunk.id,
            offset: chunk.chunk.offset,
            end,
        })
    }

    async fn put_replica_bootstrap_batch(
        &self,
        request: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt> {
        let bytes = validate_batch(
            &request.batch,
            self.options.replica_destination.storage.max_batch_records,
            self.options.replica_destination.storage.max_batch_bytes,
        )?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let bootstrap = state
            .replica_bootstraps
            .get(&request.id)
            .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
        if let Some(prior) = bootstrap.suffix_batches.get(&request.batch.id) {
            return if prior == &request.batch {
                Ok(ReplicaBootstrapBatchReceipt {
                    id: request.id,
                    committed_through: ReplicaPosition {
                        stream: request.batch.after.stream.clone(),
                        offset: request.batch.records.last().unwrap().cursor.offset,
                    },
                })
            } else {
                Err(ReplicationError::BootstrapBatchUnknown(Box::new(request)))
            };
        }
        let expected = bootstrap
            .suffix_records
            .last_key_value()
            .map_or(bootstrap.request.snapshot.covered.offset, |(&offset, _)| {
                offset
            });
        let end = request.batch.records.last().unwrap().cursor.offset;
        if bootstrap.state != ReplicaBootstrapState::Staging
            || request.batch.destination_epoch != bootstrap.request.destination_epoch
            || request.batch.after.stream != bootstrap.request.stream
            || request.batch.after.offset != expected
            || end > bootstrap.request.through.offset
        {
            return Err(ReplicationError::InvalidInput(
                "bootstrap suffix must append contiguously through its captured tail".into(),
            ));
        }
        let bytes_u64 = u64::try_from(bytes).map_err(|_| ReplicationError::CapacityExceeded)?;
        if state
            .replica_staging_records
            .checked_add(request.batch.records.len())
            .is_none_or(|rows| rows > self.options.replica_destination.staging.max_staging_records)
            || state
                .replica_staging_record_bytes
                .checked_add(bytes_u64)
                .is_none_or(|total| {
                    total
                        > self
                            .options
                            .replica_destination
                            .staging
                            .max_staging_record_bytes
                })
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        let rows = request.batch.records.len();
        let bootstrap = state.replica_bootstraps.get_mut(&request.id).unwrap();
        for record in &request.batch.records {
            bootstrap
                .suffix_records
                .insert(record.cursor.offset, Arc::clone(record));
        }
        bootstrap.suffix_bytes += bytes_u64;
        bootstrap
            .suffix_batches
            .insert(request.batch.id, request.batch.clone());
        state.replica_staging_records += rows;
        state.replica_staging_record_bytes += bytes_u64;
        Ok(ReplicaBootstrapBatchReceipt {
            id: request.id,
            committed_through: ReplicaPosition {
                stream: request.batch.after.stream,
                offset: end,
            },
        })
    }

    async fn verify_replica_bootstrap_step(
        &self,
        request: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress> {
        if request.limits.max_chunks == 0
            || request.limits.max_records == 0
            || request.limits.max_bytes == 0
        {
            return Err(ReplicationError::InvalidInput(
                "bootstrap verification limits must be nonzero".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let bootstrap = state
            .replica_bootstraps
            .get_mut(&request.id)
            .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
        if bootstrap.state == ReplicaBootstrapState::Verified
            || bootstrap.state == ReplicaBootstrapState::Published
        {
            return Ok(ReplicaBootstrapVerificationProgress {
                id: request.id,
                verified_snapshot_bytes: bootstrap.request.snapshot.content_bytes,
                verified_suffix_records: bootstrap.suffix_records.len(),
                verified_suffix_bytes: bootstrap.suffix_bytes,
                complete: true,
            });
        }
        if bootstrap.state != ReplicaBootstrapState::Staging
            || bootstrap.accepted_bytes != bootstrap.request.snapshot.content_bytes
        {
            return Err(ReplicationError::InvalidInput(
                "bootstrap content is incomplete or aborted".into(),
            ));
        }
        let verification =
            bootstrap
                .verification
                .get_or_insert_with(|| MemoryReplicaBootstrapVerification {
                    snapshot_offset: 0,
                    suffix_offset: bootstrap.request.snapshot.covered.offset,
                    suffix_records: 0,
                    suffix_bytes: 0,
                    hasher: Sha256::new(),
                });
        let mut used_bytes = 0usize;
        let mut used_chunks = 0usize;
        let mut used_records = 0usize;
        while verification.snapshot_offset < bootstrap.request.snapshot.content_bytes
            && used_chunks < request.limits.max_chunks
        {
            let bytes = bootstrap
                .chunks
                .get(&verification.snapshot_offset)
                .ok_or_else(|| {
                    ReplicationError::CorruptStorage(
                        "bootstrap snapshot contains a byte gap".into(),
                    )
                })?;
            if used_bytes
                .checked_add(bytes.len())
                .is_none_or(|total| total > request.limits.max_bytes)
            {
                if used_chunks == 0 {
                    return Err(ReplicationError::InvalidInput(
                        "verification byte limit cannot fit the next snapshot chunk".into(),
                    ));
                }
                break;
            }
            verification.hasher.update(bytes.as_bytes());
            verification.snapshot_offset += u64::try_from(bytes.len()).unwrap();
            used_bytes += bytes.len();
            used_chunks += 1;
        }
        if verification.snapshot_offset == bootstrap.request.snapshot.content_bytes {
            while verification.suffix_offset < bootstrap.request.through.offset
                && used_records < request.limits.max_records
            {
                let next = verification.suffix_offset.checked_add(1).ok_or_else(|| {
                    ReplicationError::CorruptStorage("bootstrap suffix cursor overflow".into())
                })?;
                let record = bootstrap.suffix_records.get(&next).ok_or_else(|| {
                    ReplicationError::CorruptStorage("bootstrap suffix contains a gap".into())
                })?;
                let bytes = record_charge(record)?;
                if used_bytes
                    .checked_add(bytes)
                    .is_none_or(|total| total > request.limits.max_bytes)
                {
                    if used_bytes == 0 {
                        return Err(ReplicationError::InvalidInput(
                            "verification byte limit cannot fit the next suffix record".into(),
                        ));
                    }
                    break;
                }
                verification.suffix_offset = next;
                verification.suffix_records += 1;
                used_records += 1;
                verification.suffix_bytes += u64::try_from(bytes).unwrap();
                used_bytes += bytes;
            }
        }
        let complete = verification.snapshot_offset == bootstrap.request.snapshot.content_bytes
            && verification.suffix_offset == bootstrap.request.through.offset;
        if complete {
            let digest: [u8; 32] = verification.hasher.clone().finalize().into();
            if SnapshotDigest(digest) != bootstrap.request.snapshot.digest {
                return Err(ReplicationError::InvalidInput(
                    "bootstrap snapshot digest does not match its descriptor".into(),
                ));
            }
            bootstrap.state = ReplicaBootstrapState::Verified;
        }
        Ok(ReplicaBootstrapVerificationProgress {
            id: request.id,
            verified_snapshot_bytes: verification.snapshot_offset,
            verified_suffix_records: verification.suffix_records,
            verified_suffix_bytes: verification.suffix_bytes,
            complete,
        })
    }

    async fn publish_replica_bootstrap(
        &self,
        request: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if request.destination_epoch != state.replica_destination_epoch {
            return Err(ReplicationError::DestinationReplaced {
                current: state.replica_destination_epoch,
            });
        }
        let bootstrap = state
            .replica_bootstraps
            .get(&request.id)
            .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
        if let Some((prior, receipt)) = &bootstrap.publish {
            return if prior == &request {
                Ok(receipt.clone())
            } else {
                Err(ReplicationError::BootstrapPublishUnknown(Box::new(request)))
            };
        }
        if bootstrap.state != ReplicaBootstrapState::Verified {
            return Err(ReplicationError::InvalidInput(
                "bootstrap must finish bounded verification before publication".into(),
            ));
        }
        let stream = bootstrap.request.stream.clone();
        let snapshot_bytes = bootstrap.request.snapshot.content_bytes;
        let staged_chunks = bootstrap.chunks.len();
        let staged_bytes = bootstrap.accepted_bytes;
        let staged_records = bootstrap.suffix_records.len();
        let staged_record_bytes = bootstrap.suffix_bytes;
        let suffix = bootstrap.suffix_records.clone();
        let suffix_bytes = bootstrap.suffix_bytes;
        let original = bootstrap.request.clone();
        let prior_history_bytes = state
            .replica_histories
            .get(&stream)
            .map(|records| {
                records.values().try_fold(0u64, |total, record| {
                    total
                        .checked_add(
                            u64::try_from(record_charge(record)?)
                                .map_err(|_| ReplicationError::CapacityExceeded)?,
                        )
                        .ok_or(ReplicationError::CapacityExceeded)
                })
            })
            .transpose()?
            .unwrap_or(0);
        let prior_rows = state
            .replica_histories
            .get(&stream)
            .map_or(0, BTreeMap::len);
        let next_history_bytes = state
            .replica_history_bytes
            .checked_sub(prior_history_bytes)
            .and_then(|bytes| bytes.checked_add(suffix_bytes))
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("replica history byte counter is invalid".into())
            })?;
        let next_rows = state
            .replica_history_records
            .checked_sub(prior_rows)
            .and_then(|rows| rows.checked_add(suffix.len()))
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("replica history row counter is invalid".into())
            })?;
        let next_snapshot_bytes = state
            .replica_published_snapshot_bytes
            .checked_add(snapshot_bytes)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage(
                    "published snapshot byte counter is invalid".into(),
                )
            })?;
        let new_stream = !state.replica_histories.contains_key(&stream);
        if next_rows > self.options.replica_destination.storage.max_history_records
            || next_history_bytes > self.options.replica_destination.storage.max_history_bytes
            || next_snapshot_bytes
                > self
                    .options
                    .replica_destination
                    .storage
                    .max_published_snapshot_bytes
            || (new_stream
                && state.replica_histories.len()
                    >= self.options.replica_destination.storage.max_origin_streams)
            || (!state.published_replica_bootstraps.contains_key(&stream)
                && state.published_replica_bootstraps.len()
                    >= self
                        .options
                        .replica_destination
                        .storage
                        .max_published_bootstraps)
        {
            return Err(ReplicationError::CapacityExceeded);
        }
        let receipt = ReplicaBootstrapReceipt {
            request: original,
            committed_through: ReplicaPosition {
                stream: stream.clone(),
                offset: bootstrap.request.through.offset,
            },
        };
        state.replica_histories.insert(stream.clone(), suffix);
        state.replica_history_records = next_rows;
        state.replica_history_bytes = next_history_bytes;
        state.replica_published_snapshot_bytes = next_snapshot_bytes;
        let replaced = state
            .published_replica_bootstraps
            .insert(stream, request.id);
        if let Some(replaced) = replaced {
            if replaced != request.id {
                enqueue_replica_cleanup(&mut state, replaced);
            }
        }
        state.replica_staging_chunks = state
            .replica_staging_chunks
            .checked_sub(staged_chunks)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("bootstrap staging chunk underflow".into())
            })?;
        state.replica_staging_bytes = state
            .replica_staging_bytes
            .checked_sub(staged_bytes)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("bootstrap staging byte underflow".into())
            })?;
        state.replica_staging_records = state
            .replica_staging_records
            .checked_sub(staged_records)
            .ok_or_else(|| {
            ReplicationError::CorruptStorage("bootstrap staging row underflow".into())
        })?;
        state.replica_staging_record_bytes = state
            .replica_staging_record_bytes
            .checked_sub(staged_record_bytes)
            .ok_or_else(|| {
                ReplicationError::CorruptStorage("bootstrap staging record byte underflow".into())
            })?;
        let bootstrap = state.replica_bootstraps.get_mut(&request.id).unwrap();
        bootstrap.suffix_records.clear();
        bootstrap.suffix_batches.clear();
        bootstrap.suffix_bytes = 0;
        bootstrap.state = ReplicaBootstrapState::Published;
        bootstrap.publish = Some((request, receipt.clone()));
        Ok(receipt)
    }

    async fn published_replica_bootstrap(
        &self,
        stream: &OriginStream,
    ) -> ReplicationResult<Option<PublishedReplicaBootstrap>> {
        let state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        Ok(state
            .published_replica_bootstraps
            .get(stream)
            .and_then(|id| state.replica_bootstraps.get(id))
            .and_then(|bootstrap| bootstrap.publish.as_ref())
            .map(|(_, receipt)| PublishedReplicaBootstrap {
                request: receipt.request.clone(),
                committed_through: receipt.committed_through.clone(),
            }))
    }

    async fn acquire_replica_bootstrap_read(
        &self,
        stream: &OriginStream,
        lifetime: std::time::Duration,
    ) -> ReplicationResult<ReplicaBootstrapReadPlan> {
        if lifetime.is_zero() || lifetime > self.options.replica_destination.reads.max_lifetime {
            return Err(ReplicationError::InvalidInput(
                "replica bootstrap read lifetime is outside configured bounds".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let now = self.options.replication_clock.now();
        if state
            .last_replication_clock
            .is_some_and(|last| now.0 < last.0)
        {
            return Err(ReplicationError::ClockRollback {
                last: state.last_replication_clock.unwrap(),
                observed: now,
            });
        }
        let bootstrap = *state
            .published_replica_bootstraps
            .get(stream)
            .ok_or_else(|| {
                ReplicationError::InvalidInput("published bootstrap is unknown".into())
            })?;
        let published = state
            .replica_bootstraps
            .get(&bootstrap)
            .and_then(|bootstrap| bootstrap.publish.as_ref())
            .map(|(_, receipt)| PublishedReplicaBootstrap {
                request: receipt.request.clone(),
                committed_through: receipt.committed_through.clone(),
            })
            .ok_or_else(|| {
                ReplicationError::CorruptStorage(
                    "published bootstrap index has no publication receipt".into(),
                )
            })?;
        if let Some((&(expires_at, expired_lease), _)) =
            state.replica_read_lease_expiries.first_key_value()
        {
            if expires_at.0 <= now.0 {
                state
                    .replica_read_lease_expiries
                    .remove(&(expires_at, expired_lease));
                if let Some(expired) = state.replica_read_leases.remove(&expired_lease) {
                    release_replica_reader(&mut state, expired.bootstrap)?;
                }
            }
        }
        if state.replica_read_leases.len() >= self.options.replica_destination.reads.max_leases {
            return Err(ReplicationError::CapacityExceeded);
        }
        let lifetime_millis = u64::try_from(lifetime.as_millis()).map_err(|_| {
            ReplicationError::InvalidInput(
                "replica bootstrap read lifetime is outside configured bounds".into(),
            )
        })?;
        let expires_at =
            DurableTimestampMillis(now.0.checked_add(lifetime_millis).ok_or_else(|| {
                ReplicationError::InvalidInput("read lease expiry overflow".into())
            })?);
        let lease = (0..4)
            .map(|_| ReplicaReadLeaseId(*uuid::Uuid::new_v4().as_bytes()))
            .find(|id| !state.replica_read_leases.contains_key(id))
            .ok_or_else(|| {
                ReplicationError::StorageFailure("read lease identity collision".into())
            })?;
        state.replica_read_leases.insert(
            lease,
            MemoryReplicaReadLease {
                bootstrap,
                expires_at,
            },
        );
        state
            .replica_read_lease_expiries
            .insert((expires_at, lease), ());
        state
            .replica_bootstraps
            .get_mut(&bootstrap)
            .expect("published bootstrap disappeared while locked")
            .readers += 1;
        state.last_replication_clock = Some(now);
        Ok(ReplicaBootstrapReadPlan {
            lease,
            published,
            expires_at,
        })
    }

    async fn read_replica_bootstrap_bytes(
        &self,
        lease: ReplicaReadLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> ReplicationResult<ReplicaBootstrapBytePage> {
        if max_bytes == 0 || max_bytes > self.options.replica_destination.reads.max_bytes_per_page {
            return Err(ReplicationError::InvalidInput(
                "bootstrap read limit is outside configured bounds".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let now = self.options.replication_clock.now();
        if state
            .last_replication_clock
            .is_some_and(|last| now.0 < last.0)
        {
            return Err(ReplicationError::ClockRollback {
                last: state.last_replication_clock.unwrap(),
                observed: now,
            });
        }
        let read_lease = state
            .replica_read_leases
            .get(&lease)
            .ok_or(ReplicationError::ReadLeaseExpired { lease })?;
        if now.0 >= read_lease.expires_at.0 {
            let expires_at = read_lease.expires_at;
            let expired = state.replica_read_leases.remove(&lease).unwrap();
            release_replica_reader(&mut state, expired.bootstrap)?;
            state
                .replica_read_lease_expiries
                .remove(&(expires_at, lease));
            state.last_replication_clock = Some(now);
            return Err(ReplicationError::ReadLeaseExpired { lease });
        }
        let id = read_lease.bootstrap;
        state.last_replication_clock = Some(now);
        let bootstrap = state
            .replica_bootstraps
            .get(&id)
            .filter(|bootstrap| bootstrap.state == ReplicaBootstrapState::Published)
            .ok_or_else(|| {
                ReplicationError::InvalidInput("published bootstrap is unknown".into())
            })?;
        if offset > bootstrap.request.snapshot.content_bytes {
            return Err(ReplicationError::InvalidInput(
                "bootstrap byte offset is beyond content".into(),
            ));
        }
        let available = usize::try_from(bootstrap.request.snapshot.content_bytes - offset)
            .unwrap_or(usize::MAX)
            .min(max_bytes);
        let mut output = Vec::with_capacity(available);
        let mut position = offset;
        while output.len() < available {
            let (&start, bytes) =
                bootstrap
                    .chunks
                    .range(..=position)
                    .next_back()
                    .ok_or_else(|| {
                        ReplicationError::CorruptStorage("bootstrap snapshot contains a gap".into())
                    })?;
            let inside = usize::try_from(position - start).map_err(|_| {
                ReplicationError::CorruptStorage("bootstrap chunk offset overflow".into())
            })?;
            if inside >= bytes.len() {
                return Err(ReplicationError::CorruptStorage(
                    "bootstrap snapshot contains a gap".into(),
                ));
            }
            let count = (available - output.len()).min(bytes.len() - inside);
            output.extend_from_slice(&bytes.as_bytes()[inside..inside + count]);
            position += u64::try_from(count).unwrap();
        }
        Ok(ReplicaBootstrapBytePage {
            id,
            offset,
            bytes: Payload::copy_from_slice(&output),
            next_offset: position,
            complete: position == bootstrap.request.snapshot.content_bytes,
        })
    }

    async fn release_replica_bootstrap_read(
        &self,
        lease: ReplicaReadLeaseId,
    ) -> ReplicationResult<ReplicaReadRelease> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        Ok(
            if let Some(released) = state.replica_read_leases.remove(&lease) {
                state
                    .replica_read_lease_expiries
                    .remove(&(released.expires_at, lease));
                release_replica_reader(&mut state, released.bootstrap)?;
                ReplicaReadRelease::Released
            } else {
                ReplicaReadRelease::AlreadyReleased
            },
        )
    }

    async fn abort_replica_bootstrap(
        &self,
        request: AbortReplicaBootstrap,
    ) -> ReplicationResult<AbortReplicaBootstrapReceipt> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        if request.destination_epoch != state.replica_destination_epoch {
            return Err(ReplicationError::DestinationReplaced {
                current: state.replica_destination_epoch,
            });
        }
        let bootstrap = state
            .replica_bootstraps
            .get_mut(&request.id)
            .ok_or_else(|| ReplicationError::InvalidInput("bootstrap is unknown".into()))?;
        if let Some((prior, receipt)) = &bootstrap.abort {
            return if prior == &request {
                Ok(receipt.clone())
            } else {
                Err(ReplicationError::BootstrapAbortUnknown(Box::new(request)))
            };
        }
        if bootstrap.state == ReplicaBootstrapState::Published {
            return Err(ReplicationError::InvalidInput(
                "published bootstrap cannot be aborted".into(),
            ));
        }
        bootstrap.state = ReplicaBootstrapState::Aborted;
        let receipt = AbortReplicaBootstrapReceipt {
            request: request.clone(),
            state: ReplicaBootstrapState::Aborted,
        };
        bootstrap.abort = Some((request, receipt.clone()));
        enqueue_replica_cleanup(&mut state, receipt.request.id);
        Ok(receipt)
    }

    async fn cleanup_replica_destination(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress> {
        if limits.max_receipt_rows == 0 || limits.max_staging_rows == 0 || limits.max_bytes == 0 {
            return Err(ReplicationError::InvalidInput(
                "replica cleanup limits must be nonzero".into(),
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ReplicationError::CorruptStorage("memory store lock poisoned".into()))?;
        if state.closed {
            return Err(ReplicationError::Closed);
        }
        let now = self.options.replication_clock.now();
        if state
            .last_replication_clock
            .is_some_and(|last| now.0 < last.0)
        {
            return Err(ReplicationError::ClockRollback {
                last: state.last_replication_clock.unwrap(),
                observed: now,
            });
        }
        state.last_replication_clock = Some(now);
        let mut removed_receipt_rows = 0usize;
        let mut removed_staging_rows = 0usize;
        let mut removed_bytes = 0usize;
        let expired: Vec<BatchId> = state
            .replica_receipts
            .iter()
            .filter(|(_, (_, receipt))| {
                state
                    .replica_receipt_floors
                    .get(&receipt.committed_through.stream)
                    .is_some_and(|floor| receipt.committed_through.offset <= *floor)
            })
            .take(limits.max_receipt_rows)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            let Some((batch, _)) = state.replica_receipts.get(&id) else {
                continue;
            };
            let charge = batch_receipt_charge(batch)?;
            if removed_bytes
                .checked_add(charge)
                .is_none_or(|bytes| bytes > limits.max_bytes)
            {
                break;
            }
            state.replica_receipts.remove(&id);
            state.replica_receipt_bytes = state
                .replica_receipt_bytes
                .checked_sub(charge)
                .ok_or_else(|| {
                    ReplicationError::CorruptStorage(
                        "replica receipt byte counter underflow".into(),
                    )
                })?;
            removed_receipt_rows += 1;
            removed_bytes += charge;
        }
        let lease_charge = std::mem::size_of::<MemoryReplicaReadLease>()
            .checked_add(std::mem::size_of::<ReplicaReadLeaseId>())
            .ok_or(ReplicationError::CapacityExceeded)?;
        while removed_staging_rows < limits.max_staging_rows {
            let Some((&(expires_at, lease), _)) =
                state.replica_read_lease_expiries.first_key_value()
            else {
                break;
            };
            if expires_at.0 > now.0 {
                break;
            }
            if removed_bytes
                .checked_add(lease_charge)
                .is_none_or(|bytes| bytes > limits.max_bytes)
            {
                break;
            }
            state
                .replica_read_lease_expiries
                .remove(&(expires_at, lease));
            let Some(expired) = state.replica_read_leases.remove(&lease) else {
                return Err(ReplicationError::CorruptStorage(
                    "replica read lease expiry index is invalid".into(),
                ));
            };
            release_replica_reader(&mut state, expired.bootstrap)?;
            removed_staging_rows += 1;
            removed_bytes += lease_charge;
        }
        enum CleanupPiece {
            AbortedChunk(BootstrapId, u64, usize),
            AbortedRecord(BootstrapId, u64, usize),
            RetiredPublishedChunk(BootstrapId, u64, usize),
        }
        while removed_staging_rows < limits.max_staging_rows {
            let Some(id) = state.replica_cleanup_queue.pop_front() else {
                break;
            };
            state.replica_cleanup_set.remove(&id);
            let piece = state.replica_bootstraps.get(&id).and_then(|bootstrap| {
                if bootstrap.state == ReplicaBootstrapState::Aborted {
                    if let Some((&offset, bytes)) = bootstrap.chunks.first_key_value() {
                        return Some(CleanupPiece::AbortedChunk(id, offset, bytes.len()));
                    }
                    return bootstrap
                        .suffix_records
                        .first_key_value()
                        .map(|(&offset, record)| {
                            CleanupPiece::AbortedRecord(
                                id,
                                offset,
                                record_charge(record).unwrap_or(usize::MAX),
                            )
                        });
                }
                let is_current = state
                    .published_replica_bootstraps
                    .get(&bootstrap.request.stream)
                    == Some(&id);
                if bootstrap.state == ReplicaBootstrapState::Published
                    && !is_current
                    && bootstrap.readers == 0
                {
                    return bootstrap.chunks.first_key_value().map(|(&offset, bytes)| {
                        CleanupPiece::RetiredPublishedChunk(id, offset, bytes.len())
                    });
                }
                None
            });
            let Some(piece) = piece else {
                continue;
            };
            let charge = match piece {
                CleanupPiece::AbortedChunk(_, _, bytes)
                | CleanupPiece::AbortedRecord(_, _, bytes)
                | CleanupPiece::RetiredPublishedChunk(_, _, bytes) => bytes,
            };
            if removed_bytes
                .checked_add(charge)
                .is_none_or(|bytes| bytes > limits.max_bytes)
            {
                enqueue_replica_cleanup(&mut state, id);
                break;
            }
            match piece {
                CleanupPiece::AbortedChunk(id, offset, bytes) => {
                    state
                        .replica_bootstraps
                        .get_mut(&id)
                        .unwrap()
                        .chunks
                        .remove(&offset);
                    state.replica_staging_chunks =
                        state.replica_staging_chunks.checked_sub(1).ok_or_else(|| {
                            ReplicationError::CorruptStorage(
                                "bootstrap staging chunk underflow".into(),
                            )
                        })?;
                    state.replica_staging_bytes = state
                        .replica_staging_bytes
                        .checked_sub(u64::try_from(bytes).unwrap())
                        .ok_or_else(|| {
                            ReplicationError::CorruptStorage(
                                "bootstrap staging byte underflow".into(),
                            )
                        })?;
                }
                CleanupPiece::AbortedRecord(id, offset, bytes) => {
                    let bootstrap = state.replica_bootstraps.get_mut(&id).unwrap();
                    bootstrap.suffix_records.remove(&offset);
                    bootstrap.suffix_bytes = bootstrap
                        .suffix_bytes
                        .checked_sub(u64::try_from(bytes).unwrap())
                        .ok_or_else(|| {
                            ReplicationError::CorruptStorage(
                                "bootstrap suffix byte underflow".into(),
                            )
                        })?;
                    state.replica_staging_records = state
                        .replica_staging_records
                        .checked_sub(1)
                        .ok_or_else(|| {
                            ReplicationError::CorruptStorage(
                                "bootstrap staging row underflow".into(),
                            )
                        })?;
                    state.replica_staging_record_bytes = state
                        .replica_staging_record_bytes
                        .checked_sub(u64::try_from(bytes).unwrap())
                        .ok_or_else(|| {
                            ReplicationError::CorruptStorage(
                                "bootstrap staging record byte underflow".into(),
                            )
                        })?;
                }
                CleanupPiece::RetiredPublishedChunk(id, offset, bytes) => {
                    state
                        .replica_bootstraps
                        .get_mut(&id)
                        .unwrap()
                        .chunks
                        .remove(&offset);
                    state.replica_published_snapshot_bytes = state
                        .replica_published_snapshot_bytes
                        .checked_sub(u64::try_from(bytes).unwrap())
                        .ok_or_else(|| {
                            ReplicationError::CorruptStorage(
                                "published bootstrap byte underflow".into(),
                            )
                        })?;
                }
            }
            removed_staging_rows += 1;
            removed_bytes += charge;
            let still_eligible = state.replica_bootstraps.get(&id).is_some_and(|bootstrap| {
                (bootstrap.state == ReplicaBootstrapState::Aborted
                    && (!bootstrap.chunks.is_empty() || !bootstrap.suffix_records.is_empty()))
                    || (bootstrap.state == ReplicaBootstrapState::Published
                        && state
                            .published_replica_bootstraps
                            .get(&bootstrap.request.stream)
                            != Some(&id)
                        && bootstrap.readers == 0
                        && !bootstrap.chunks.is_empty())
            });
            if still_eligible {
                enqueue_replica_cleanup(&mut state, id);
            }
        }
        let remaining = state.replica_receipts.iter().any(|(_, (_, receipt))| {
            state
                .replica_receipt_floors
                .get(&receipt.committed_through.stream)
                .is_some_and(|floor| receipt.committed_through.offset <= *floor)
        }) || !state.replica_cleanup_queue.is_empty()
            || state
                .replica_read_lease_expiries
                .first_key_value()
                .is_some_and(|(&(expires_at, _), _)| expires_at.0 <= now.0);
        Ok(ReplicaCleanupProgress {
            removed_receipt_rows,
            removed_staging_rows,
            removed_bytes,
            remaining,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug)]
    struct TestReplicationClock(AtomicU64);

    impl DurableReplicationClock for TestReplicationClock {
        fn now(&self) -> DurableTimestampMillis {
            DurableTimestampMillis(self.0.load(Ordering::SeqCst))
        }
    }

    fn stream(origin: OriginId) -> OriginStream {
        OriginStream {
            origin,
            stream: StreamKey {
                id: StreamId::new("replicated").unwrap(),
                incarnation: IncarnationId([7; 16]),
            },
        }
    }

    fn record(stream: &OriginStream, offset: u64, value: u8) -> Arc<Record> {
        Arc::new(Record {
            cursor: Cursor::new(stream.stream.clone(), offset),
            event: NewEvent {
                id: EventId::new(format!("event-{offset}")).unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("test.bytes").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(&[value]),
            },
        })
    }

    fn event(id: &str, value: u8) -> NewEvent {
        NewEvent {
            id: EventId::new(id).unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("test.bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(&[value]),
        }
    }

    #[tokio::test]
    async fn origin_prepares_one_stable_batch_and_acknowledges_exact_receipt() {
        let store = MemoryStore::open(Default::default()).await.unwrap();
        let key = store
            .create_if_absent(&StreamId::new("origin").unwrap())
            .await
            .unwrap();
        store.append_atomic(&key, event("one", 1)).await.unwrap();
        let origin = store.origin_identity().await.unwrap();
        let origin_stream = OriginStream {
            origin,
            stream: key.clone(),
        };
        let replica = ReplicaId::new("replica-a").unwrap();
        let destination_epoch = DestinationEpoch([6; 16]);
        let attach = AttachReplica {
            operation_id: ReplicationOperationId::new("attach-a").unwrap(),
            replica: replica.clone(),
            stream: origin_stream.clone(),
            destination_epoch,
            max_backlog_bytes: 10_000,
            max_backlog_age: std::time::Duration::from_secs(60),
            start: ReplicaStart::FromBeginning,
        };
        assert_eq!(
            store
                .attach_replica(attach)
                .await
                .unwrap()
                .status
                .backlog_records,
            1
        );
        store.append_atomic(&key, event("two", 2)).await.unwrap();
        let request = PrepareReplicaBatch {
            operation_id: ReplicationOperationId::new("prepare-a").unwrap(),
            batch_id: BatchId([7; 16]),
            replica: replica.clone(),
            stream: origin_stream.clone(),
            expected_after: ReplicaPosition {
                stream: origin_stream.clone(),
                offset: 0,
            },
            limits: ReplicaBatchLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        };
        let prepared = store.prepare_replica_batch(request.clone()).await.unwrap();
        assert_eq!(prepared.batch.as_ref().unwrap().records.len(), 2);
        assert_eq!(
            store.prepare_replica_batch(request).await.unwrap(),
            prepared
        );
        let batch = prepared.batch.unwrap();
        let acknowledge = AcknowledgeReplicaBatch {
            operation_id: ReplicationOperationId::new("ack-a").unwrap(),
            replica,
            expected_after: batch.after.clone(),
            receipt: ReplicaReceipt {
                batch: batch.id,
                destination_epoch,
                committed_through: ReplicaPosition {
                    stream: origin_stream,
                    offset: 2,
                },
            },
        };
        let acknowledged = store
            .acknowledge_replica_batch(acknowledge.clone())
            .await
            .unwrap();
        assert_eq!(acknowledged.status.acknowledged.offset, 2);
        assert_eq!(acknowledged.status.backlog_records, 0);
        assert_eq!(
            store.acknowledge_replica_batch(acknowledge).await.unwrap(),
            acknowledged
        );
    }

    #[tokio::test]
    async fn required_replica_backlog_rejects_a_new_local_commit_atomically() {
        let store = MemoryStore::open(Default::default()).await.unwrap();
        let key = store
            .create_if_absent(&StreamId::new("bounded-origin").unwrap())
            .await
            .unwrap();
        let first = event("one", 1);
        let charge = first.accounted_bytes() + STORED_RECORD_OVERHEAD;
        store.append_atomic(&key, first).await.unwrap();
        let origin_stream = OriginStream {
            origin: store.origin_identity().await.unwrap(),
            stream: key.clone(),
        };
        store
            .attach_replica(AttachReplica {
                operation_id: ReplicationOperationId::new("attach-bounded").unwrap(),
                replica: ReplicaId::new("bounded").unwrap(),
                stream: origin_stream,
                destination_epoch: DestinationEpoch([2; 16]),
                max_backlog_bytes: u64::try_from(charge).unwrap(),
                max_backlog_age: std::time::Duration::from_secs(60),
                start: ReplicaStart::FromBeginning,
            })
            .await
            .unwrap();
        assert!(matches!(
            store.append_atomic(&key, event("two", 2)).await,
            Err(crate::application::Error::ReplicaBacklogExceeded { .. })
        ));
        assert_eq!(store.bounds(&key).await.unwrap().tail.offset, 1);
    }

    #[tokio::test]
    async fn origin_bootstrap_binds_published_snapshot_attempt_and_captured_tail() {
        let store = MemoryStore::open(Default::default()).await.unwrap();
        let key = store
            .create_if_absent(&StreamId::new("bootstrap-origin").unwrap())
            .await
            .unwrap();
        store.append_atomic(&key, event("one", 1)).await.unwrap();
        store.append_atomic(&key, event("two", 2)).await.unwrap();
        let bytes = b"state-through-one";
        let descriptor = SnapshotDescriptor {
            id: SnapshotId([51; 16]),
            covered: Cursor::new(key.clone(), 1),
            schema: SchemaRef {
                id: SchemaId::new("app.state").unwrap(),
                version: 1,
            },
            content_bytes: u64::try_from(bytes.len()).unwrap(),
            digest: SnapshotDigest(Sha256::digest(bytes).into()),
        };
        store.begin_snapshot(descriptor.clone()).await.unwrap();
        store
            .put_snapshot_chunk(
                descriptor.id,
                SnapshotChunk {
                    offset: 0,
                    bytes: Payload::copy_from_slice(bytes),
                },
            )
            .await
            .unwrap();
        store
            .verify_snapshot_step(
                descriptor.id,
                VerificationLimits {
                    max_chunks: 1,
                    max_bytes: 1024,
                },
            )
            .await
            .unwrap();
        store.publish_snapshot(descriptor.id).await.unwrap();
        let stream = OriginStream {
            origin: store.origin_identity().await.unwrap(),
            stream: key.clone(),
        };
        let replica = ReplicaId::new("bootstrap-origin-replica").unwrap();
        let epoch = DestinationEpoch([52; 16]);
        store
            .attach_replica(AttachReplica {
                operation_id: ReplicationOperationId::new("attach-bootstrap").unwrap(),
                replica: replica.clone(),
                stream: stream.clone(),
                destination_epoch: epoch,
                max_backlog_bytes: 4096,
                max_backlog_age: std::time::Duration::from_secs(60),
                start: ReplicaStart::NeedsBootstrap,
            })
            .await
            .unwrap();
        let begin = BeginOriginBootstrap {
            operation_id: ReplicationOperationId::new("begin-origin-bootstrap").unwrap(),
            bootstrap_id: BootstrapId([53; 16]),
            destination_operation_id: ReplicationOperationId::new("begin-destination-bootstrap")
                .unwrap(),
            replica: replica.clone(),
            stream: stream.clone(),
            destination_epoch: epoch,
            snapshot: descriptor.clone(),
            captured_tail: ReplicaPosition {
                stream: stream.clone(),
                offset: 2,
            },
        };
        let begun = store.begin_origin_bootstrap(begin.clone()).await.unwrap();
        assert_eq!(begun.status.mode, ReplicaMode::Bootstrapping);
        assert_eq!(begun.status.backlog_records, 1);
        let destination_request = ReplicaBootstrap {
            operation_id: begin.destination_operation_id.clone(),
            id: begin.bootstrap_id,
            replica: replica.clone(),
            destination_epoch: epoch,
            stream: stream.clone(),
            snapshot: descriptor,
            through: begin.captured_tail.clone(),
        };
        let receipt = store
            .acknowledge_origin_bootstrap(AcknowledgeOriginBootstrap {
                operation_id: ReplicationOperationId::new("ack-origin-bootstrap").unwrap(),
                replica,
                stream,
                receipt: ReplicaBootstrapReceipt {
                    request: destination_request,
                    committed_through: begin.captured_tail,
                },
            })
            .await
            .unwrap();
        assert_eq!(receipt.status.mode, ReplicaMode::Required);
        assert_eq!(receipt.status.acknowledged.offset, 2);
        assert_eq!(receipt.status.backlog_records, 0);
    }

    #[tokio::test]
    async fn destination_commits_one_exact_bounded_batch_and_reconciles_it() {
        let store = MemoryStore::open(Default::default()).await.unwrap();
        let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
            .await
            .unwrap();
        let origin_stream = stream(OriginId([3; 16]));
        let batch = ReplicaBatch {
            id: BatchId([4; 16]),
            destination_epoch: epoch,
            after: ReplicaPosition {
                stream: origin_stream.clone(),
                offset: 0,
            },
            records: vec![record(&origin_stream, 1, 1), record(&origin_stream, 2, 2)],
        };
        let receipt = store.commit_replica_batch(batch.clone()).await.unwrap();
        assert_eq!(receipt.committed_through.offset, 2);
        assert_eq!(
            store.commit_replica_batch(batch.clone()).await.unwrap(),
            receipt
        );

        let mut conflict = batch.clone();
        conflict.records[1] = record(&origin_stream, 2, 9);
        assert!(matches!(
            store.commit_replica_batch(conflict).await,
            Err(ReplicationError::BatchConflict { .. })
        ));
        let page = store
            .read_replica_after(
                &batch.after,
                ReplicaBatchLimits {
                    max_records: 2,
                    max_bytes: 1024,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), 2);
        assert_eq!(page.records[1].event.payload.as_bytes(), &[2]);
        assert!(page.complete);

        let mut unsupported = record(&origin_stream, 3, 3);
        Arc::make_mut(&mut unsupported).cursor.version = CURSOR_VERSION + 1;
        assert!(matches!(
            store
                .commit_replica_batch(ReplicaBatch {
                    id: BatchId([5; 16]),
                    destination_epoch: epoch,
                    after: ReplicaPosition {
                        stream: origin_stream.clone(),
                        offset: 2,
                    },
                    records: vec![unsupported],
                })
                .await,
            Err(ReplicationError::InvalidInput(_))
        ));
        assert!(matches!(
            store
                .read_replica_after(
                    &ReplicaPosition {
                        stream: origin_stream.clone(),
                        offset: 2,
                    },
                    ReplicaBatchLimits {
                        max_records: store.options.replica_destination.storage.max_batch_records
                            + 1,
                        max_bytes: 1024,
                    },
                )
                .await,
            Err(ReplicationError::InvalidInput(_))
        ));

        let floor = AdvanceReplicaReceiptFloor {
            operation_id: ReplicationOperationId::new("floor-1").unwrap(),
            stream: origin_stream.clone(),
            destination_epoch: epoch,
            expected_floor: ReplicaPosition {
                stream: origin_stream.clone(),
                offset: 0,
            },
            new_floor: ReplicaPosition {
                stream: origin_stream,
                offset: 2,
            },
        };
        let first = store
            .advance_replica_receipt_floor(floor.clone())
            .await
            .unwrap();
        assert_eq!(
            store.advance_replica_receipt_floor(floor).await.unwrap(),
            first
        );
        assert!(matches!(
            store.commit_replica_batch(batch).await,
            Err(ReplicationError::ReceiptExpired)
        ));
    }

    #[tokio::test]
    async fn destination_accepts_a_final_max_offset_and_reads_at_tail() {
        let store = MemoryStore::open(Default::default()).await.unwrap();
        let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
            .await
            .unwrap();
        let origin_stream = stream(OriginId([8; 16]));
        {
            let mut state = store.state.lock().unwrap();
            state
                .replica_histories
                .entry(origin_stream.clone())
                .or_default()
                .insert(u64::MAX - 1, record(&origin_stream, u64::MAX - 1, 1));
            state.replica_history_records = 1;
        }
        let batch = ReplicaBatch {
            id: BatchId([9; 16]),
            destination_epoch: epoch,
            after: ReplicaPosition {
                stream: origin_stream.clone(),
                offset: u64::MAX - 1,
            },
            records: vec![record(&origin_stream, u64::MAX, 2)],
        };
        store.commit_replica_batch(batch).await.unwrap();
        let page = store
            .read_replica_after(
                &ReplicaPosition {
                    stream: origin_stream,
                    offset: u64::MAX,
                },
                ReplicaBatchLimits {
                    max_records: 1,
                    max_bytes: 1024,
                },
            )
            .await
            .unwrap();
        assert!(page.records.is_empty());
        assert!(page.complete);
    }

    #[tokio::test]
    async fn destination_bootstrap_verifies_in_steps_and_publishes_snapshot_plus_suffix() {
        let clock = Arc::new(TestReplicationClock(AtomicU64::new(0)));
        let options = super::super::MemoryStoreOptions {
            replication_clock: clock.clone(),
            ..Default::default()
        };
        let store = MemoryStore::open(options).await.unwrap();
        let epoch = ReplicaBatchDestinationStore::destination_epoch(&store)
            .await
            .unwrap();
        let origin_stream = stream(OriginId([12; 16]));
        let snapshot_bytes = b"opaque-state";
        let digest: [u8; 32] = Sha256::digest(snapshot_bytes).into();
        let bootstrap = ReplicaBootstrap {
            operation_id: ReplicationOperationId::new("bootstrap-1").unwrap(),
            id: BootstrapId([13; 16]),
            replica: ReplicaId::new("bootstrap-replica").unwrap(),
            destination_epoch: epoch,
            stream: origin_stream.clone(),
            snapshot: SnapshotDescriptor {
                id: SnapshotId([14; 16]),
                covered: Cursor::new(origin_stream.stream.clone(), 2),
                schema: SchemaRef {
                    id: SchemaId::new("app.state").unwrap(),
                    version: 1,
                },
                content_bytes: u64::try_from(snapshot_bytes.len()).unwrap(),
                digest: SnapshotDigest(digest),
            },
            through: ReplicaPosition {
                stream: origin_stream.clone(),
                offset: 3,
            },
        };
        store
            .begin_replica_bootstrap(bootstrap.clone())
            .await
            .unwrap();
        store
            .put_replica_bootstrap_chunk(ReplicaBootstrapChunk {
                id: bootstrap.id,
                chunk: SnapshotChunk {
                    offset: 0,
                    bytes: Payload::copy_from_slice(snapshot_bytes),
                },
            })
            .await
            .unwrap();
        store
            .put_replica_bootstrap_batch(ReplicaBootstrapBatch {
                id: bootstrap.id,
                batch: ReplicaBatch {
                    id: BatchId([15; 16]),
                    destination_epoch: epoch,
                    after: ReplicaPosition {
                        stream: origin_stream.clone(),
                        offset: 2,
                    },
                    records: vec![record(&origin_stream, 3, 3)],
                },
            })
            .await
            .unwrap();
        let verification = store
            .verify_replica_bootstrap_step(VerifyReplicaBootstrap {
                id: bootstrap.id,
                limits: ReplicaBootstrapVerificationLimits {
                    max_chunks: 1,
                    max_records: 1,
                    max_bytes: 4096,
                },
            })
            .await
            .unwrap();
        assert!(verification.complete);
        let published = store
            .publish_replica_bootstrap(PublishReplicaBootstrap {
                operation_id: ReplicationOperationId::new("publish-1").unwrap(),
                id: bootstrap.id,
                destination_epoch: epoch,
            })
            .await
            .unwrap();
        assert_eq!(published.committed_through.offset, 3);
        assert_eq!(
            store
                .published_replica_bootstrap(&origin_stream)
                .await
                .unwrap()
                .unwrap()
                .request,
            bootstrap
        );
        let read = store
            .acquire_replica_bootstrap_read(&bootstrap.stream, std::time::Duration::from_secs(30))
            .await
            .unwrap();
        let bytes = store
            .read_replica_bootstrap_bytes(read.lease, 0, 64)
            .await
            .unwrap();
        assert_eq!(bytes.bytes.as_bytes(), snapshot_bytes);
        assert!(bytes.complete);
        assert_eq!(
            store
                .release_replica_bootstrap_read(read.lease)
                .await
                .unwrap(),
            ReplicaReadRelease::Released
        );
        let expiring = store
            .acquire_replica_bootstrap_read(&bootstrap.stream, std::time::Duration::from_millis(1))
            .await
            .unwrap();
        clock.0.store(1, Ordering::SeqCst);
        assert_eq!(
            store
                .read_replica_bootstrap_bytes(expiring.lease, 0, 1)
                .await,
            Err(ReplicationError::ReadLeaseExpired {
                lease: expiring.lease
            })
        );
        let page = store
            .read_replica_after(
                &ReplicaPosition {
                    stream: origin_stream,
                    offset: 2,
                },
                ReplicaBatchLimits {
                    max_records: 1,
                    max_bytes: 1024,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records[0].cursor.offset, 3);
        assert!(page.complete);
    }

    #[tokio::test]
    async fn application_driver_transfers_snapshot_and_suffix_between_real_stores() {
        let origin = Arc::new(MemoryStore::open(Default::default()).await.unwrap());
        let destination = Arc::new(MemoryStore::open(Default::default()).await.unwrap());
        let key = origin
            .create_if_absent(&StreamId::new("driver-bootstrap").unwrap())
            .await
            .unwrap();
        origin.append_atomic(&key, event("a", 1)).await.unwrap();
        origin.append_atomic(&key, event("b", 2)).await.unwrap();
        let snapshot_bytes = b"state-a";
        let snapshot = SnapshotDescriptor {
            id: SnapshotId([21; 16]),
            covered: Cursor::new(key.clone(), 1),
            schema: SchemaRef {
                id: SchemaId::new("app.state").unwrap(),
                version: 1,
            },
            content_bytes: u64::try_from(snapshot_bytes.len()).unwrap(),
            digest: SnapshotDigest(Sha256::digest(snapshot_bytes).into()),
        };
        origin.begin_snapshot(snapshot.clone()).await.unwrap();
        origin
            .put_snapshot_chunk(
                snapshot.id,
                SnapshotChunk {
                    offset: 0,
                    bytes: Payload::copy_from_slice(snapshot_bytes),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            origin
                .verify_snapshot_step(
                    snapshot.id,
                    VerificationLimits {
                        max_chunks: 1,
                        max_bytes: 4096,
                    },
                )
                .await
                .unwrap()
                .state,
            SnapshotUploadState::Verified
        );
        origin.publish_snapshot(snapshot.id).await.unwrap();
        let stream = OriginStream {
            origin: origin.origin_identity().await.unwrap(),
            stream: key,
        };
        let replica = ReplicaId::new("driver-destination").unwrap();
        let epoch = destination.destination_epoch().await.unwrap();
        origin
            .attach_replica(AttachReplica {
                operation_id: ReplicationOperationId::new("driver-attach").unwrap(),
                replica: replica.clone(),
                stream: stream.clone(),
                destination_epoch: epoch,
                max_backlog_bytes: 64 * 1024,
                max_backlog_age: std::time::Duration::from_secs(60),
                start: ReplicaStart::NeedsBootstrap,
            })
            .await
            .unwrap();
        let driver = ReplicationDriver::open(
            origin.clone(),
            destination.clone(),
            ReplicationDriverConfig {
                max_concurrent: 1,
                max_in_flight_bytes: 128 * 1024,
            },
        )
        .unwrap();
        let receipt = driver
            .bootstrap_once(
                BeginOriginBootstrap {
                    operation_id: ReplicationOperationId::new("driver-origin-begin").unwrap(),
                    bootstrap_id: BootstrapId([22; 16]),
                    destination_operation_id: ReplicationOperationId::new(
                        "driver-destination-begin",
                    )
                    .unwrap(),
                    replica: replica.clone(),
                    stream: stream.clone(),
                    destination_epoch: epoch,
                    snapshot: snapshot.clone(),
                    captured_tail: ReplicaPosition {
                        stream: stream.clone(),
                        offset: 2,
                    },
                },
                ReplicationOperationId::new("driver-publish").unwrap(),
                ReplicationOperationId::new("driver-ack").unwrap(),
                ReplicationBootstrapDriveLimits {
                    recovery_lifetime: std::time::Duration::from_secs(30),
                    snapshot_page_bytes: 4,
                    suffix_page: PageLimits {
                        max_records: 1,
                        max_bytes: 4096,
                    },
                    verification: ReplicaBootstrapVerificationLimits {
                        max_chunks: 1,
                        max_records: 1,
                        max_bytes: 4096,
                    },
                },
            )
            .await
            .unwrap();
        assert_eq!(receipt.acknowledged.status.acknowledged.offset, 2);
        let page = destination
            .read_replica_after(
                &ReplicaPosition { stream, offset: 1 },
                ReplicaBatchLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0].event.payload.as_bytes(), &[2]);
    }
}
