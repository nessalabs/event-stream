use super::{
    Error, EventReader, EventRuntime, EventSink, EventStore, EventSubscription, LifecycleStore,
    PersistenceProfile, Result, RuntimeConfig, ShutdownReport, StartPosition, StoreCapabilities,
    SubscriptionOptions, UnresolvedAppend,
};
use crate::domain::*;
use async_trait::async_trait;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex, Weak,
    },
    time::{Duration, Instant},
};
use tokio::sync::{oneshot, Mutex, Notify, OwnedSemaphorePermit, Semaphore};

#[cfg(feature = "replication")]
use super::{
    replication_bootstrap_drive_charge, replication_drive_charge, AttachReplica,
    AttachReplicaReceipt, BeginOriginBootstrap, DetachReplica, DetachReplicaReceipt,
    PrepareReplicaBatch, ReplicaBootstrapTransport, ReplicaCleanupLimits, ReplicaCleanupProgress,
    ReplicaStatus, ReplicaTransport, ReplicationBootstrapDriveLimits,
    ReplicationBootstrapDriveReceipt, ReplicationDriveReceipt, ReplicationDriver,
    ReplicationDriverConfig, ReplicationError, ReplicationOriginStore, ReplicationResult,
    ReplicationStore,
};
#[cfg(feature = "source-journal")]
use super::{
    AdvanceCaptureReceiptFloorReceipt, BeginSourceReceipt, CaptureReceipt, CheckpointReceipt,
    FinishSourceReceipt, JournalCleanupLimits, JournalCleanupProgress, JournalError, JournalResult,
    RawPage, RawPageLimits, SealSourceReceipt, SourceFinalizationStatus, SourceFinalizationStore,
    SourceJournalStore, SourceProgress,
};
#[cfg(feature = "retention")]
use super::{
    AdvanceRetentionFloorReceipt, AdvanceRetryGenerationReceipt, EnableRetryPolicyReceipt,
    ExpireRetryGenerationsReceipt, RetentionCleanupLimits, RetentionCleanupProgress,
    RetentionError, RetentionResult, RetentionStatus, RetentionStore,
};
#[cfg(feature = "snapshots")]
use super::{
    RecoveryRelease, SnapshotAbortReceipt, SnapshotBytePage, SnapshotCleanupLimits,
    SnapshotCleanupProgress, SnapshotError, SnapshotPage, SnapshotResult, SnapshotStore,
    SnapshotUploadPage, VerificationLimits,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeLifecycle {
    Ready,
    Draining,
    Faulted,
    Closed,
}

#[cfg(feature = "source-journal")]
impl<S: SourceFinalizationStore> Runtime<S> {
    pub async fn seal_source(&self, request: SealSource) -> JournalResult<SealSourceReceipt> {
        let bytes = request
            .end
            .source
            .id
            .as_str()
            .len()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::SealUnknown(Box::new(request.clone()));
        let store = self.inner.store.clone();
        self.journal_operation(
            bytes,
            fallback,
            async move { store.seal_source(request).await },
        )
        .await
    }

    pub async fn finish_source(&self, request: FinishSource) -> JournalResult<FinishSourceReceipt> {
        let checkpoint = &request.checkpoint;
        let identifiers = checkpoint.source.source.id.as_str().len()
            + checkpoint.parser.id.as_str().len()
            + checkpoint.output_stream.id.as_str().len()
            + checkpoint
                .committed_output
                .as_ref()
                .map_or(0, |cursor| cursor.stream.id.as_str().len());
        let bytes = checkpoint
            .state
            .len()
            .checked_add(identifiers)
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(1024))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::FinishUnknown(Box::new(request.clone()));
        let store = self.inner.store.clone();
        self.journal_operation(bytes, fallback, async move {
            store.finish_source(request).await
        })
        .await
    }

    pub async fn source_finalization(
        &self,
        source: &SourceKey,
    ) -> JournalResult<SourceFinalizationStatus> {
        let source = source.clone();
        let store = self.inner.store.clone();
        self.journal_operation(
            1024,
            JournalError::StorageFailure("source finalization status task stopped".into()),
            async move { store.source_finalization(&source).await },
        )
        .await
    }
}

#[cfg(feature = "source-journal")]
impl<S: SourceJournalStore> Runtime<S> {
    pub async fn begin_source(&self, request: BeginSource) -> JournalResult<BeginSourceReceipt> {
        let bytes = request
            .operation_id
            .as_str()
            .len()
            .checked_add(request.binding.source.id.as_str().len())
            .and_then(|value| value.checked_add(request.binding.parser.id.as_str().len()))
            .and_then(|value| value.checked_add(request.binding.output_stream.id.as_str().len()))
            .and_then(|value| value.checked_mul(2))
            .and_then(|value| value.checked_add(512))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::BeginUnknown(Box::new(request.clone()));
        let store = self.inner.store.clone();
        self.journal_operation(
            bytes,
            fallback,
            async move { store.begin_source(request).await },
        )
        .await
    }

    pub async fn capture_segment(&self, segment: RawSegment) -> JournalResult<CaptureReceipt> {
        let bytes = segment
            .bytes
            .len()
            .checked_add(segment.start.source.id.as_str().len().saturating_mul(2))
            .and_then(|value| value.checked_add(384))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::CaptureUnknown(Box::new(segment.clone()));
        let store = self.inner.store.clone();
        self.journal_operation(bytes, fallback, async move {
            store.capture_segment(segment).await
        })
        .await
    }

    pub async fn advance_capture_receipt_floor(
        &self,
        request: AdvanceCaptureReceiptFloor,
    ) -> JournalResult<AdvanceCaptureReceiptFloorReceipt> {
        let bytes = request
            .operation_id
            .as_str()
            .len()
            .checked_add(request.source.id.as_str().len())
            .and_then(|value| value.checked_add(384))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::AdvanceCaptureReceiptFloorUnknown(Box::new(request.clone()));
        let store = self.inner.store.clone();
        self.journal_operation(bytes, fallback, async move {
            store.advance_capture_receipt_floor(request).await
        })
        .await
    }

    pub async fn append_captured(
        &self,
        output_stream: &StreamKey,
        output: JournaledOutput,
    ) -> JournalResult<AppendReceipt> {
        if output.event.accounted_bytes() > self.inner.config.events.max_bytes {
            return Err(JournalError::InvalidInput(
                "event exceeds the runtime event limit".into(),
            ));
        }
        let bytes = output
            .event
            .accounted_bytes()
            .checked_add(output.source.id.as_str().len())
            .and_then(|value| value.checked_add(output_stream.id.as_str().len()))
            .and_then(|value| value.checked_add(output.event.id.as_str().len()))
            .and_then(|value| value.checked_add(output.event.schema.id.as_str().len()))
            .and_then(|value| value.checked_add(output.source.id.as_str().len()))
            .and_then(|value| value.checked_add(output_stream.id.as_str().len().saturating_mul(2)))
            .and_then(|value| value.checked_add(1024))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::OutputUnknown {
            output_stream: Box::new(output_stream.clone()),
            output: Box::new(output.clone()),
        };
        let stream = output_stream.clone();
        let expected = output.event.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.journal_operation(bytes, fallback, async move {
            let anchor = retention_coordinator(inner.clone(), stream.clone())
                .await
                .map_err(JournalError::from)?;
            let _gate = anchor.coordinator.write_gate.lock().await;
            retention_name_available(&inner, &stream.id)
                .await
                .map_err(JournalError::from)?;
            let receipt = store.append_captured(&stream, output).await?;
            if receipt.record.cursor.version != CURSOR_VERSION
                || receipt.record.cursor.stream != stream
                || receipt.record.cursor.offset == 0
                || receipt.record.event != expected
            {
                return Err(JournalError::CorruptStorage(
                    "journal append returned a different record".into(),
                ));
            }
            observe_retention_commit(&anchor.coordinator, receipt.record.clone());
            Ok(receipt)
        })
        .await
    }

    pub async fn publish_parser_checkpoint(
        &self,
        checkpoint: ParserCheckpoint,
    ) -> JournalResult<CheckpointReceipt> {
        let bytes = checkpoint
            .state
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(1024))
            .ok_or(JournalError::CapacityExceeded)?;
        let fallback = JournalError::CheckpointUnknown(Box::new(checkpoint.clone()));
        let store = self.inner.store.clone();
        self.journal_operation(bytes, fallback, async move {
            store.publish_parser_checkpoint(checkpoint).await
        })
        .await
    }

    pub async fn source_status(&self, source: &SourceKey) -> JournalResult<SourceProgress> {
        let store = self.inner.store.clone();
        let source = source.clone();
        self.journal_operation(
            1024,
            JournalError::StorageFailure("source status task stopped".into()),
            async move { store.source_status(&source).await },
        )
        .await
    }

    pub async fn read_captured(
        &self,
        source: &SourceKey,
        offset: u64,
        limits: RawPageLimits,
    ) -> JournalResult<RawPage> {
        let bytes = limits
            .max_bytes
            .checked_add(512)
            .ok_or(JournalError::CapacityExceeded)?;
        let store = self.inner.store.clone();
        let source = source.clone();
        self.journal_operation(
            bytes,
            JournalError::StorageFailure("captured read task stopped".into()),
            async move { store.read_captured(&source, offset, limits).await },
        )
        .await
    }

    pub async fn latest_checkpoint(
        &self,
        source: &SourceKey,
    ) -> JournalResult<Option<ParserCheckpoint>> {
        let bytes = self
            .inner
            .config
            .journal
            .max_in_flight_bytes
            .min(self.inner.config.journal.max_waiter_bytes);
        let store = self.inner.store.clone();
        let source = source.clone();
        self.journal_operation(
            bytes,
            JournalError::StorageFailure("checkpoint read task stopped".into()),
            async move { store.latest_checkpoint(&source).await },
        )
        .await
    }

    pub async fn cleanup_captured(
        &self,
        limits: JournalCleanupLimits,
    ) -> JournalResult<JournalCleanupProgress> {
        let store = self.inner.store.clone();
        self.journal_operation(
            128,
            JournalError::StorageFailure("journal cleanup task stopped".into()),
            async move { store.cleanup_captured(limits).await },
        )
        .await
    }

    async fn journal_operation<T, F>(
        &self,
        retained_bytes: usize,
        uncertain: JournalError,
        operation: F,
    ) -> JournalResult<T>
    where
        T: Send + 'static,
        F: Future<Output = JournalResult<T>> + Send + 'static,
    {
        let bytes = retained_bytes.max(1);
        if bytes > self.inner.config.journal.max_waiter_bytes
            || bytes > self.inner.config.journal.max_in_flight_bytes
            || bytes > u32::MAX as usize
        {
            return Err(JournalError::CapacityExceeded);
        }
        let deadline: tokio::time::Instant = Instant::now()
            .checked_add(self.inner.config.journal.admission_timeout)
            .ok_or_else(|| JournalError::InvalidConfig("journal deadline overflow".into()))?
            .into();
        let waiter = self
            .inner
            .journal_waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| JournalError::Overloaded)?;
        let waiter_bytes = self
            .inner
            .journal_waiter_bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| JournalError::Overloaded)?;
        let in_flight_bytes = journal_permit(
            self.inner.clone(),
            self.inner.journal_in_flight_bytes.clone(),
            bytes as u32,
            deadline,
        )
        .await?;
        let slot = journal_permit(
            self.inner.clone(),
            self.inner.journal_slots.clone(),
            1,
            deadline,
        )
        .await?;
        {
            let mut state = self.inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(JournalError::Closed);
            }
            state.active_io += 1;
        }
        drop(waiter_bytes);
        drop(waiter);
        let inner = self.inner.clone();
        let executor = inner.executor.clone();
        let receiver_fallback = uncertain.clone();
        let (sender, receiver) = oneshot::channel();
        executor.spawn(async move {
            let result = match tokio::spawn(operation).await {
                Ok(result) => result,
                Err(_) => Err(uncertain),
            };
            let _slot = slot;
            let _in_flight_bytes = in_flight_bytes;
            let mut state = inner.state.lock().await;
            state.active_io = state.active_io.saturating_sub(1);
            drop(state);
            inner.changed.notify_waiters();
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(receiver_fallback))
    }
}

#[cfg(feature = "replication")]
impl<S: ReplicationOriginStore> Runtime<S> {
    pub async fn origin_identity(&self) -> ReplicationResult<OriginId> {
        let store = self.inner.store.clone();
        self.replication_operation(512, || {
            (
                ReplicationError::StorageFailure("origin identity task stopped".into()),
                async move { ReplicationOriginStore::origin_identity(store.as_ref()).await },
            )
        })
        .await
    }

    pub async fn attach_replica(
        &self,
        request: AttachReplica,
    ) -> ReplicationResult<AttachReplicaReceipt> {
        let bytes = replication_request_charge(
            [
                request.operation_id.as_str().len(),
                request.replica.as_str().len(),
                request.stream.stream.id.as_str().len(),
            ],
            4,
            1024,
        )?;
        let store = self.inner.store.clone();
        self.replication_operation(bytes, move || {
            let fallback = ReplicationError::AttachUnknown(Box::new(request.clone()));
            (fallback, async move {
                ReplicationOriginStore::attach_replica(store.as_ref(), request).await
            })
        })
        .await
    }

    pub async fn replica_status(
        &self,
        replica: &ReplicaId,
        stream: &OriginStream,
    ) -> ReplicationResult<ReplicaStatus> {
        let bytes = replication_request_charge(
            [replica.as_str().len(), stream.stream.id.as_str().len()],
            4,
            768,
        )?;
        let store = self.inner.store.clone();
        let replica = replica.clone();
        let stream = stream.clone();
        self.replication_operation(bytes, move || {
            (
                ReplicationError::StorageFailure("replica status task stopped".into()),
                async move {
                    ReplicationOriginStore::replica_status(store.as_ref(), &replica, &stream).await
                },
            )
        })
        .await
    }

    pub async fn detach_replica(
        &self,
        request: DetachReplica,
    ) -> ReplicationResult<DetachReplicaReceipt> {
        let bytes = replication_request_charge(
            [
                request.operation_id.as_str().len(),
                request.replica.as_str().len(),
                request.stream.stream.id.as_str().len(),
            ],
            4,
            1024,
        )?;
        let store = self.inner.store.clone();
        self.replication_operation(bytes, move || {
            let fallback = ReplicationError::DetachUnknown(Box::new(request.clone()));
            (fallback, async move {
                ReplicationOriginStore::detach_replica(store.as_ref(), request).await
            })
        })
        .await
    }

    pub async fn replicate_once<T: ReplicaTransport>(
        &self,
        transport: Arc<T>,
        prepare: PrepareReplicaBatch,
        acknowledge_operation_id: ReplicationOperationId,
    ) -> ReplicationResult<ReplicationDriveReceipt> {
        if prepare.limits.max_records == 0 || prepare.limits.max_bytes == 0 {
            return Err(ReplicationError::InvalidInput(
                "replication drive limits must be nonzero".into(),
            ));
        }
        let driver_bytes = replication_drive_charge(&prepare, &acknowledge_operation_id)
            .map_err(runtime_replication_charge_error)?;
        let wrapper_bytes = replication_request_charge(
            [
                prepare.operation_id.as_str().len(),
                prepare.replica.as_str().len(),
                prepare.stream.stream.id.as_str().len(),
                prepare.expected_after.stream.stream.id.as_str().len(),
                acknowledge_operation_id.as_str().len(),
            ],
            6,
            2048,
        )?;
        let bytes = usize::try_from(driver_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(wrapper_bytes))
            .ok_or(ReplicationError::CapacityExceeded)?;
        let origin = self.inner.store.clone();
        self.replication_operation(bytes, move || {
            let fallback = ReplicationError::DriveUnknown {
                prepare: Box::new(prepare.clone()),
                acknowledge_operation_id: acknowledge_operation_id.clone(),
            };
            let child_fallback = fallback.clone();
            let operation = async move {
                let driver = Arc::new(ReplicationDriver::open(
                    origin,
                    transport,
                    ReplicationDriverConfig {
                        max_concurrent: 1,
                        max_in_flight_bytes: driver_bytes as usize,
                    },
                )?);
                let child_driver = driver.clone();
                let child = tokio::spawn(async move {
                    child_driver
                        .replicate_once(prepare, acknowledge_operation_id)
                        .await
                });
                let result = child.await.unwrap_or(Err(child_fallback));
                driver.wait_closed().await;
                result
            };
            (fallback, operation)
        })
        .await
    }

    async fn replication_operation<T, F, Fut>(
        &self,
        retained_bytes: usize,
        make_operation: F,
    ) -> ReplicationResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> (ReplicationError, Fut),
        Fut: Future<Output = ReplicationResult<T>> + Send + 'static,
    {
        let bytes = retained_bytes.max(1);
        if bytes > self.inner.config.replication.max_in_flight_bytes || bytes > u32::MAX as usize {
            return Err(ReplicationError::CapacityExceeded);
        }
        // Closed takes precedence over occupied permits. The second check below
        // closes the race between this observation and fail-fast acquisition.
        {
            let state = self.inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(ReplicationError::Closed);
            }
        }
        let slot = self
            .inner
            .replication_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReplicationError::Overloaded)?;
        let byte_permit = self
            .inner
            .replication_in_flight_bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| ReplicationError::Overloaded)?;
        {
            let mut state = self.inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(ReplicationError::Closed);
            }
            state.active_io += 1;
        }
        // There is no await between registering active I/O and handing the
        // operation to the runtime-owned task.
        let (fallback, operation) = make_operation();
        let receiver_fallback = fallback.clone();
        let inner = self.inner.clone();
        let executor = inner.executor.clone();
        let (sender, receiver) = oneshot::channel();
        executor.spawn(async move {
            let result = tokio::spawn(operation).await.unwrap_or(Err(fallback));
            let _permits = (slot, byte_permit);
            let mut state = inner.state.lock().await;
            state.active_io = state.active_io.saturating_sub(1);
            drop(state);
            inner.changed.notify_waiters();
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(receiver_fallback))
    }
}

#[cfg(feature = "replication")]
impl<S: ReplicationStore> Runtime<S> {
    pub async fn cleanup_replication(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress> {
        if limits.max_receipt_rows == 0 || limits.max_staging_rows == 0 || limits.max_bytes == 0 {
            return Err(ReplicationError::InvalidInput(
                "replication cleanup limits must be nonzero".into(),
            ));
        }
        let bytes = limits
            .max_bytes
            .checked_add(512)
            .ok_or(ReplicationError::CapacityExceeded)?;
        let store = self.inner.store.clone();
        self.replication_operation(bytes, move || {
            (
                ReplicationError::StorageFailure("replication cleanup task stopped".into()),
                async move { ReplicationStore::cleanup_replication(store.as_ref(), limits).await },
            )
        })
        .await
    }

    pub async fn bootstrap_replica_once<T>(
        &self,
        transport: Arc<T>,
        begin: BeginOriginBootstrap,
        publish_operation_id: ReplicationOperationId,
        acknowledge_operation_id: ReplicationOperationId,
        limits: ReplicationBootstrapDriveLimits,
    ) -> ReplicationResult<ReplicationBootstrapDriveReceipt>
    where
        T: ReplicaTransport + ReplicaBootstrapTransport,
    {
        if limits.recovery_lifetime.is_zero()
            || limits.snapshot_page_bytes == 0
            || limits.suffix_page.max_records == 0
            || limits.suffix_page.max_bytes == 0
            || limits.verification.max_chunks == 0
            || limits.verification.max_records == 0
            || limits.verification.max_bytes == 0
        {
            return Err(ReplicationError::InvalidInput(
                "replication bootstrap drive limits must be nonzero".into(),
            ));
        }
        let driver_bytes = replication_bootstrap_drive_charge(
            &begin,
            &publish_operation_id,
            &acknowledge_operation_id,
            &limits,
        )
        .map_err(runtime_replication_charge_error)?;
        let wrapper_bytes = replication_request_charge(
            [
                begin.operation_id.as_str().len(),
                begin.destination_operation_id.as_str().len(),
                begin.replica.as_str().len(),
                begin.stream.stream.id.as_str().len(),
                begin.snapshot.covered.stream.id.as_str().len(),
                begin.snapshot.schema.id.as_str().len(),
                begin.captured_tail.stream.stream.id.as_str().len(),
                publish_operation_id.as_str().len(),
                acknowledge_operation_id.as_str().len(),
            ],
            8,
            4096,
        )?;
        let bytes = usize::try_from(driver_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(wrapper_bytes))
            .ok_or(ReplicationError::CapacityExceeded)?;
        let origin = self.inner.store.clone();
        self.replication_operation(bytes, move || {
            let fallback = ReplicationError::BootstrapDriveUnknown(Box::new(begin.clone()));
            let child_fallback = fallback.clone();
            let operation = async move {
                let driver = Arc::new(ReplicationDriver::open(
                    origin,
                    transport,
                    ReplicationDriverConfig {
                        max_concurrent: 1,
                        max_in_flight_bytes: driver_bytes as usize,
                    },
                )?);
                let child_driver = driver.clone();
                let child = tokio::spawn(async move {
                    child_driver
                        .bootstrap_once(
                            begin,
                            publish_operation_id,
                            acknowledge_operation_id,
                            limits,
                        )
                        .await
                });
                let result = child.await.unwrap_or(Err(child_fallback));
                driver.wait_closed().await;
                result
            };
            (fallback, operation)
        })
        .await
    }
}

#[cfg(feature = "replication")]
fn replication_request_charge<const N: usize>(
    identifier_lengths: [usize; N],
    retained_copies: usize,
    fixed: usize,
) -> ReplicationResult<usize> {
    identifier_lengths
        .into_iter()
        .try_fold(fixed, |bytes, length| {
            length
                .checked_mul(retained_copies)
                .and_then(|length| bytes.checked_add(length))
        })
        .ok_or(ReplicationError::CapacityExceeded)
}

#[cfg(feature = "replication")]
fn runtime_replication_charge_error(error: ReplicationError) -> ReplicationError {
    match error {
        // The standalone driver reports an unrepresentable semaphore charge as
        // invalid input. At the Runtime boundary it is a request that cannot fit
        // the configured aggregate capacity.
        ReplicationError::InvalidInput(_) => ReplicationError::CapacityExceeded,
        other => other,
    }
}

#[derive(Clone, Debug)]
pub enum DiagnosticKind {
    AppendAccepted,
    AppendInserted,
    AppendDeduplicated,
    AppendRejected,
    ReadPage,
    SubscriberEnded,
    RuntimeFaulted,
    Shutdown,
}

#[derive(Clone, Debug)]
pub struct DiagnosticSample {
    pub kind: DiagnosticKind,
    pub detail: &'static str,
}

#[derive(Clone, Debug)]
pub struct RuntimeDiagnostics {
    pub lifecycle: RuntimeLifecycle,
    pub persistence: PersistenceProfile,
    pub accepts_writes: bool,
    pub queued_appends: usize,
    pub queued_append_bytes: usize,
    pub admission_waiters: usize,
    pub active_subscriptions: usize,
    /// Append calls admitted into the owned runtime queue since open.
    pub append_accepted: u64,
    /// Append calls rejected before admission since open.
    pub append_rejected: u64,
    /// Accepted calls completed with an inserted receipt since open.
    pub append_inserted: u64,
    /// Accepted calls completed with a deduplicated receipt since open.
    pub append_deduplicated: u64,
    /// Accepted calls completed with an error since open.
    pub append_failed: u64,
    pub peak_queued_appends: usize,
    pub peak_queued_append_bytes: usize,
    pub samples: Vec<DiagnosticSample>,
}

pub struct Runtime<S: EventStore> {
    inner: Arc<Inner<S>>,
}

impl<S: EventStore> Clone for Runtime<S> {
    fn clone(&self) -> Self {
        self.inner.handles.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S: EventStore> Drop for Runtime<S> {
    fn drop(&mut self) {
        if self.inner.handles.fetch_sub(1, Ordering::AcqRel) != 1
            || self.inner.drop_started.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let handle = self.inner.executor.clone();
        let inner = self.inner.clone();
        handle.spawn(async move {
            let mut state = inner.state.lock().await;
            if matches!(
                state.lifecycle,
                RuntimeLifecycle::Ready | RuntimeLifecycle::Faulted
            ) {
                state.lifecycle = RuntimeLifecycle::Draining;
                end_all_subscriptions(&mut state, Error::Closed);
            }
            drop(state);
            inner.changed.notify_waiters();
            spawn_finalizer(inner);
        });
    }
}

struct Inner<S: EventStore> {
    executor: tokio::runtime::Handle,
    store: Arc<S>,
    capabilities: StoreCapabilities,
    config: RuntimeConfig,
    state: Mutex<State>,
    changed: Notify,
    read_slots: Arc<Semaphore>,
    read_waiters: Arc<Semaphore>,
    page_bytes: Arc<Semaphore>,
    #[cfg(feature = "snapshots")]
    snapshot_slots: Arc<Semaphore>,
    #[cfg(feature = "snapshots")]
    snapshot_waiters: Arc<Semaphore>,
    #[cfg(feature = "snapshots")]
    snapshot_waiter_bytes: Arc<Semaphore>,
    #[cfg(feature = "snapshots")]
    snapshot_in_flight_bytes: Arc<Semaphore>,
    #[cfg(feature = "retention")]
    retention_slots: Arc<Semaphore>,
    #[cfg(feature = "retention")]
    retention_waiters: Arc<Semaphore>,
    #[cfg(feature = "retention")]
    retention_waiter_bytes: Arc<Semaphore>,
    #[cfg(feature = "retention")]
    retention_in_flight_bytes: Arc<Semaphore>,
    #[cfg(feature = "source-journal")]
    journal_slots: Arc<Semaphore>,
    #[cfg(feature = "source-journal")]
    journal_waiters: Arc<Semaphore>,
    #[cfg(feature = "source-journal")]
    journal_waiter_bytes: Arc<Semaphore>,
    #[cfg(feature = "source-journal")]
    journal_in_flight_bytes: Arc<Semaphore>,
    #[cfg(feature = "replication")]
    replication_slots: Arc<Semaphore>,
    #[cfg(feature = "replication")]
    replication_in_flight_bytes: Arc<Semaphore>,
    handles: AtomicUsize,
    drop_started: AtomicBool,
}

struct State {
    lifecycle: RuntimeLifecycle,
    fault: Option<String>,
    coordinators: HashMap<StreamKey, Arc<Coordinator>>,
    ready: VecDeque<StreamKey>,
    queued_count: usize,
    queued_bytes: usize,
    waiter_count: usize,
    waiter_bytes: usize,
    subscriptions: usize,
    active_io: usize,
    maintenance_reserved: usize,
    maintenance_reserved_bytes: usize,
    maintenance_active: usize,
    cleanup_active: usize,
    lifecycle_in_flight: HashMap<StreamId, LifecycleRequest>,
    unresolved_lifecycle: HashMap<StreamId, LifecycleRequest>,
    unresolved: HashMap<(StreamKey, EventId), usize>,
    append_accepted: u64,
    append_rejected: u64,
    append_inserted: u64,
    append_deduplicated: u64,
    append_failed: u64,
    peak_queued_appends: usize,
    peak_queued_append_bytes: usize,
    close_started: bool,
    samples: VecDeque<DiagnosticSample>,
}

struct Coordinator {
    key: StreamKey,
    write_gate: Arc<Mutex<()>>,
    state: StdMutex<CoordinatorState>,
    wake: Notify,
}

struct CoordinatorState {
    queue: VecDeque<PendingAppend>,
    queued_bytes: usize,
    scheduled: bool,
    in_flight: usize,
    maintenance_in_flight: usize,
    observed_floor: u64,
    observed_tail: u64,
    generation: u64,
    subscriptions: Vec<Option<Arc<SubscriptionShared>>>,
    free_subscription_slots: Vec<usize>,
    sweep_index: usize,
    subscription_reservations: usize,
}

struct PendingAppend {
    event: NewEvent,
    bytes: usize,
    result: oneshot::Sender<Result<AppendReceipt>>,
}

struct SubscriptionShared {
    state: StdMutex<SharedSubscriptionState>,
}

struct SharedSubscriptionState {
    last_delivered: Cursor,
    created: Instant,
    behind_since: Option<Instant>,
    ended: Option<Error>,
    terminal_delivered: bool,
    registered: bool,
    membership_slot: Option<NonZeroUsize>,
    buffer: VecDeque<Arc<Record>>,
    page_permit: Option<OwnedSemaphorePermit>,
    max_lag_records: u64,
    max_lag_duration: Duration,
    catch_up_grace: Duration,
}

impl SharedSubscriptionState {
    fn install_membership(&mut self, slot: NonZeroUsize) {
        assert!(
            self.membership_slot.is_none() && !self.registered,
            "subscription membership can only be installed once"
        );
        self.membership_slot = Some(slot);
        self.registered = true;
    }

    fn begin_membership_removal(&mut self) -> bool {
        if !self.registered {
            return false;
        }
        assert!(
            self.membership_slot.is_some(),
            "registered subscription must have an indexed membership"
        );
        self.registered = false;
        true
    }

    fn membership_index(&self) -> Option<usize> {
        self.membership_slot.map(|slot| slot.get() - 1)
    }

    fn remove_membership(&mut self) -> bool {
        self.registered = false;
        self.membership_slot.take().is_some()
    }
}

struct AdmissionWaiterGuard<S: EventStore> {
    inner: Arc<Inner<S>>,
    bytes: usize,
    coordinator: StreamKey,
    active: bool,
}

#[cfg(feature = "retention")]
struct RetentionCoordinatorGuard<S: EventStore> {
    inner: Arc<Inner<S>>,
    coordinator: Arc<Coordinator>,
}

#[cfg(feature = "retention")]
impl<S: EventStore> Drop for RetentionCoordinatorGuard<S> {
    fn drop(&mut self) {
        let inner = self.inner.clone();
        let coordinator = self.coordinator.clone();
        let executor = inner.executor.clone();
        executor.spawn(async move {
            let mut state = inner.state.lock().await;
            {
                let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                cs.maintenance_in_flight = cs.maintenance_in_flight.saturating_sub(1);
            }
            cleanup_coordinator(&mut state, &coordinator.key);
            drop(state);
            inner.changed.notify_waiters();
        });
    }
}

struct SubscriptionReservationGuard<S: EventStore> {
    inner: Arc<Inner<S>>,
    coordinator: Arc<Coordinator>,
    active: bool,
}

impl<S: EventStore> SubscriptionReservationGuard<S> {
    fn disarm(&mut self) {
        self.active = false;
    }
}

impl<S: EventStore> Drop for SubscriptionReservationGuard<S> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let inner = self.inner.clone();
        let coordinator = self.coordinator.clone();
        let executor = inner.executor.clone();
        executor.spawn(async move {
            let mut state = inner.state.lock().await;
            state.subscriptions = state.subscriptions.saturating_sub(1);
            let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
            cs.subscription_reservations = cs.subscription_reservations.saturating_sub(1);
            drop(cs);
            cleanup_coordinator(&mut state, &coordinator.key);
            inner.changed.notify_waiters();
        });
    }
}

impl<S: EventStore> AdmissionWaiterGuard<S> {
    fn disarm(&mut self) {
        self.active = false;
    }
}

impl<S: EventStore> Drop for AdmissionWaiterGuard<S> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let inner = self.inner.clone();
        let bytes = self.bytes;
        let key = self.coordinator.clone();
        let executor = inner.executor.clone();
        executor.spawn(async move {
            let mut state = inner.state.lock().await;
            state.waiter_count = state.waiter_count.saturating_sub(1);
            state.waiter_bytes = state.waiter_bytes.saturating_sub(bytes);
            cleanup_coordinator(&mut state, &key);
            inner.changed.notify_waiters();
        });
    }
}

pub struct RuntimeSubscription<S: EventStore> {
    inner: Arc<Inner<S>>,
    coordinator: Arc<Coordinator>,
    shared: Arc<SubscriptionShared>,
    last_delivered: Cursor,
    replay_high: u64,
    options: SubscriptionOptions,
}

impl RuntimeConfig {
    fn validate_shape(&self) -> Result<()> {
        #[cfg(feature = "replication")]
        self.replication
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "snapshots")]
        self.snapshots
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "snapshots")]
        if self.snapshots.max_concurrent > Semaphore::MAX_PERMITS
            || self.snapshots.max_waiters > Semaphore::MAX_PERMITS
            || Instant::now()
                .checked_add(self.snapshots.admission_timeout)
                .is_none()
        {
            return Err(Error::InvalidConfig(
                "snapshot admission limits exceed runtime ranges".into(),
            ));
        }
        #[cfg(feature = "retention")]
        self.retention
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "retention")]
        if self.retention.max_concurrent > Semaphore::MAX_PERMITS
            || self.retention.max_waiters > Semaphore::MAX_PERMITS
            || Instant::now()
                .checked_add(self.retention.admission_timeout)
                .is_none()
        {
            return Err(Error::InvalidConfig(
                "retention admission limits exceed runtime ranges".into(),
            ));
        }
        #[cfg(feature = "source-journal")]
        self.journal
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "source-journal")]
        if self.journal.max_concurrent > Semaphore::MAX_PERMITS
            || self.journal.max_waiters > Semaphore::MAX_PERMITS
            || Instant::now()
                .checked_add(self.journal.admission_timeout)
                .is_none()
        {
            return Err(Error::InvalidConfig(
                "journal admission limits exceed runtime ranges".into(),
            ));
        }
        let nonzero = [
            self.events.max_bytes,
            self.appends.max_queued,
            self.appends.max_queued_bytes,
            self.appends.max_queued_per_stream,
            self.appends.max_queued_bytes_per_stream,
            self.appends.max_waiters,
            self.appends.max_waiter_bytes,
            self.scheduling.max_coordinators,
            self.scheduling.storage_workers,
            self.scheduling.max_appends_per_stream_turn,
            self.reads.max_concurrent,
            self.reads.max_waiters,
            self.subscriptions.max_total,
            self.subscriptions.max_per_stream,
            self.reads.page.max_records,
            self.reads.page.max_bytes,
            self.reads.max_buffered_page_bytes,
            self.subscriptions.checks_per_sweep,
            self.diagnostics.capacity,
            self.maintenance.max_operations,
            self.maintenance.max_operation_bytes,
            self.maintenance.max_cleanup_operations,
            self.maintenance.cleanup.max_records,
            self.maintenance.cleanup.max_bytes,
        ];
        if nonzero.contains(&0)
            || self.appends.admission_timeout.is_zero()
            || self.subscriptions.sweep_interval.is_zero()
            || self.reads.admission_timeout.is_zero()
        {
            return Err(Error::InvalidConfig(
                "runtime limits must be nonzero".into(),
            ));
        }
        if self.appends.max_queued_per_stream > self.appends.max_queued
            || self.appends.max_queued_bytes_per_stream > self.appends.max_queued_bytes
            || self.subscriptions.max_per_stream > self.subscriptions.max_total
        {
            return Err(Error::InvalidConfig(
                "per-stream limits must fit global limits".into(),
            ));
        }
        if self.events.max_bytes > self.appends.max_queued_bytes_per_stream
            || self.events.max_bytes > self.appends.max_queued_bytes
            || self.events.max_bytes > self.appends.max_waiter_bytes
        {
            return Err(Error::InvalidConfig(
                "one maximum-size event must fit every append byte budget".into(),
            ));
        }
        if self
            .events
            .max_bytes
            .checked_add(256)
            .is_none_or(|maximum| maximum > self.maintenance.cleanup.max_bytes)
            || self.maintenance.max_cleanup_operations > self.maintenance.max_operations
        {
            return Err(Error::InvalidConfig(
                "maintenance limits cannot admit one maximum-size cleanup record".into(),
            ));
        }
        if self.reads.max_buffered_page_bytes > u32::MAX as usize {
            return Err(Error::InvalidConfig(
                "buffered page byte budget exceeds semaphore range".into(),
            ));
        }
        if self.subscriptions.max_total == usize::MAX
            || self.subscriptions.max_per_stream == usize::MAX
        {
            return Err(Error::InvalidConfig(
                "subscription limits exceed indexed membership range".into(),
            ));
        }
        Ok(())
    }

    fn validate(&self, capabilities: &StoreCapabilities) -> Result<()> {
        self.validate_shape()?;
        if self.events.max_bytes > capabilities.max_record_bytes {
            return Err(Error::InvalidConfig(
                "runtime event limit exceeds store capability".into(),
            ));
        }
        if capabilities.persistence < self.events.minimum_persistence {
            return Err(Error::InvalidConfig(
                "store persistence is weaker than the required profile".into(),
            ));
        }
        if self.reads.page.max_bytes < self.events.max_bytes
            || self.reads.max_buffered_page_bytes < self.reads.page.max_bytes
        {
            return Err(Error::InvalidConfig(
                "a page must fit one maximum-size event".into(),
            ));
        }
        if self.scheduling.storage_workers > capabilities.max_concurrent_writes
            || self.reads.max_concurrent > capabilities.max_concurrent_reads
        {
            return Err(Error::InvalidConfig(
                "runtime storage concurrency exceeds store capability".into(),
            ));
        }
        Ok(())
    }
}

impl<S: EventStore> Runtime<S> {
    pub async fn open(options: S::Options, config: RuntimeConfig) -> Result<Self> {
        config.validate_shape()?;
        let executor = tokio::runtime::Handle::current();
        let store = Arc::new(S::open(options).await?);
        let capabilities = store.capabilities();
        if let Err(error) = config.validate(&capabilities) {
            let _ = store.close().await;
            return Err(error);
        }
        let inner = Arc::new(Inner {
            executor,
            store,
            capabilities,
            read_slots: Arc::new(Semaphore::new(config.reads.max_concurrent)),
            read_waiters: Arc::new(Semaphore::new(config.reads.max_waiters)),
            page_bytes: Arc::new(Semaphore::new(config.reads.max_buffered_page_bytes)),
            #[cfg(feature = "snapshots")]
            snapshot_slots: Arc::new(Semaphore::new(config.snapshots.max_concurrent)),
            #[cfg(feature = "snapshots")]
            snapshot_waiters: Arc::new(Semaphore::new(config.snapshots.max_waiters)),
            #[cfg(feature = "snapshots")]
            snapshot_waiter_bytes: Arc::new(Semaphore::new(config.snapshots.max_waiter_bytes)),
            #[cfg(feature = "snapshots")]
            snapshot_in_flight_bytes: Arc::new(Semaphore::new(
                config.snapshots.max_in_flight_chunk_bytes,
            )),
            #[cfg(feature = "retention")]
            retention_slots: Arc::new(Semaphore::new(config.retention.max_concurrent)),
            #[cfg(feature = "retention")]
            retention_waiters: Arc::new(Semaphore::new(config.retention.max_waiters)),
            #[cfg(feature = "retention")]
            retention_waiter_bytes: Arc::new(Semaphore::new(config.retention.max_waiter_bytes)),
            #[cfg(feature = "retention")]
            retention_in_flight_bytes: Arc::new(Semaphore::new(
                config.retention.max_in_flight_bytes,
            )),
            #[cfg(feature = "source-journal")]
            journal_slots: Arc::new(Semaphore::new(config.journal.max_concurrent)),
            #[cfg(feature = "source-journal")]
            journal_waiters: Arc::new(Semaphore::new(config.journal.max_waiters)),
            #[cfg(feature = "source-journal")]
            journal_waiter_bytes: Arc::new(Semaphore::new(config.journal.max_waiter_bytes)),
            #[cfg(feature = "source-journal")]
            journal_in_flight_bytes: Arc::new(Semaphore::new(config.journal.max_in_flight_bytes)),
            #[cfg(feature = "replication")]
            replication_slots: Arc::new(Semaphore::new(config.replication.max_concurrent)),
            #[cfg(feature = "replication")]
            replication_in_flight_bytes: Arc::new(Semaphore::new(
                config.replication.max_in_flight_bytes,
            )),
            handles: AtomicUsize::new(1),
            drop_started: AtomicBool::new(false),
            config,
            state: Mutex::new(State {
                lifecycle: RuntimeLifecycle::Ready,
                fault: None,
                coordinators: HashMap::new(),
                ready: VecDeque::new(),
                queued_count: 0,
                queued_bytes: 0,
                waiter_count: 0,
                waiter_bytes: 0,
                subscriptions: 0,
                active_io: 0,
                maintenance_reserved: 0,
                maintenance_reserved_bytes: 0,
                maintenance_active: 0,
                cleanup_active: 0,
                lifecycle_in_flight: HashMap::new(),
                unresolved_lifecycle: HashMap::new(),
                unresolved: HashMap::new(),
                append_accepted: 0,
                append_rejected: 0,
                append_inserted: 0,
                append_deduplicated: 0,
                append_failed: 0,
                peak_queued_appends: 0,
                peak_queued_append_bytes: 0,
                close_started: false,
                samples: VecDeque::new(),
            }),
            changed: Notify::new(),
        });
        for _ in 0..inner.config.scheduling.storage_workers {
            tokio::spawn(worker(inner.clone()));
        }
        tokio::spawn(subscription_sweeper(Arc::downgrade(&inner)));
        Ok(Self { inner })
    }

    pub async fn diagnostics(&self) -> RuntimeDiagnostics {
        let state = self.inner.state.lock().await;
        RuntimeDiagnostics {
            lifecycle: state.lifecycle,
            persistence: self.inner.capabilities.persistence,
            accepts_writes: state.lifecycle == RuntimeLifecycle::Ready,
            queued_appends: state.queued_count,
            queued_append_bytes: state.queued_bytes,
            admission_waiters: state.waiter_count,
            active_subscriptions: state.subscriptions,
            append_accepted: state.append_accepted,
            append_rejected: state.append_rejected,
            append_inserted: state.append_inserted,
            append_deduplicated: state.append_deduplicated,
            append_failed: state.append_failed,
            peak_queued_appends: state.peak_queued_appends,
            peak_queued_append_bytes: state.peak_queued_append_bytes,
            samples: state.samples.iter().cloned().collect(),
        }
    }

    async fn enqueue(
        &self,
        stream: &StreamKey,
        event: NewEvent,
        wait: bool,
    ) -> Result<AppendReceipt> {
        let bytes = event.accounted_bytes();
        if bytes > self.inner.config.events.max_bytes {
            let mut state = self.inner.state.lock().await;
            state.append_rejected = state.append_rejected.saturating_add(1);
            return Err(Error::PayloadTooLarge);
        }
        let deadline = Instant::now() + self.inner.config.appends.admission_timeout;
        let mut registered_waiter = false;
        let mut waiter_guard: Option<AdmissionWaiterGuard<S>> = None;
        loop {
            let notified = self.inner.changed.notified();
            let mut state = self.inner.state.lock().await;
            if let Err(error) = ensure_ready(&state) {
                state.append_rejected = state.append_rejected.saturating_add(1);
                return Err(error);
            }
            if let Some(error) = lifecycle_name_error(&state, &stream.id) {
                state.append_rejected = state.append_rejected.saturating_add(1);
                return Err(error);
            }
            let coordinator = match state.coordinators.get(stream).cloned() {
                Some(c) => c,
                None => {
                    if state.coordinators.len() >= self.inner.config.scheduling.max_coordinators {
                        if !wait {
                            state.append_rejected = state.append_rejected.saturating_add(1);
                            return Err(Error::Overloaded);
                        }
                        if !registered_waiter {
                            if let Err(error) =
                                register_waiter(&mut state, &self.inner.config, bytes)
                            {
                                state.append_rejected = state.append_rejected.saturating_add(1);
                                return Err(error);
                            }
                            registered_waiter = true;
                            waiter_guard = Some(AdmissionWaiterGuard {
                                inner: self.inner.clone(),
                                bytes,
                                coordinator: stream.clone(),
                                active: true,
                            });
                        }
                        drop(state);
                        if tokio::time::timeout_at(deadline.into(), notified)
                            .await
                            .is_err()
                        {
                            drop(waiter_guard.take());
                            let mut state = self.inner.state.lock().await;
                            state.append_rejected = state.append_rejected.saturating_add(1);
                            return Err(Error::AdmissionTimeout);
                        }
                        continue;
                    }
                    let c = Arc::new(Coordinator {
                        key: stream.clone(),
                        write_gate: Arc::new(Mutex::new(())),
                        state: StdMutex::new(CoordinatorState {
                            queue: VecDeque::new(),
                            queued_bytes: 0,
                            scheduled: false,
                            in_flight: 0,
                            maintenance_in_flight: 0,
                            observed_floor: 0,
                            observed_tail: 0,
                            generation: 0,
                            subscriptions: Vec::new(),
                            free_subscription_slots: Vec::new(),
                            sweep_index: 0,
                            subscription_reservations: 0,
                        }),
                        wake: Notify::new(),
                    });
                    state.coordinators.insert(stream.clone(), c.clone());
                    c
                }
            };
            let accepted = {
                let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                let capacity = state.queued_count < self.inner.config.appends.max_queued
                    && state
                        .queued_bytes
                        .checked_add(bytes)
                        .is_some_and(|n| n <= self.inner.config.appends.max_queued_bytes)
                    && cs.queue.len() + cs.in_flight
                        < self.inner.config.appends.max_queued_per_stream
                    && cs.queued_bytes.checked_add(bytes).is_some_and(|n| {
                        n <= self.inner.config.appends.max_queued_bytes_per_stream
                    });
                if capacity {
                    if registered_waiter {
                        state.waiter_count -= 1;
                        state.waiter_bytes -= bytes;
                        if let Some(guard) = &mut waiter_guard {
                            guard.disarm();
                        }
                    }
                    let (sender, receiver) = oneshot::channel();
                    cs.queue.push_back(PendingAppend {
                        event: event.clone(),
                        bytes,
                        result: sender,
                    });
                    cs.queued_bytes += bytes;
                    state.queued_count += 1;
                    state.queued_bytes += bytes;
                    state.append_accepted = state.append_accepted.saturating_add(1);
                    state.peak_queued_appends = state.peak_queued_appends.max(state.queued_count);
                    state.peak_queued_append_bytes =
                        state.peak_queued_append_bytes.max(state.queued_bytes);
                    *state
                        .unresolved
                        .entry((stream.clone(), event.id.clone()))
                        .or_default() += 1;
                    if !cs.scheduled && cs.in_flight == 0 {
                        cs.scheduled = true;
                        state.ready.push_back(stream.clone());
                    }
                    Some(receiver)
                } else {
                    None
                }
            };
            if let Some(receiver) = accepted {
                sample(
                    &mut state,
                    &self.inner.config,
                    DiagnosticKind::AppendAccepted,
                    "accepted",
                );
                drop(state);
                self.inner.changed.notify_waiters();
                return receiver.await.unwrap_or(Err(Error::RuntimeFaulted(
                    "append worker stopped before reporting an outcome".into(),
                )));
            }
            if !wait {
                state.append_rejected = state.append_rejected.saturating_add(1);
                sample(
                    &mut state,
                    &self.inner.config,
                    DiagnosticKind::AppendRejected,
                    "overloaded",
                );
                cleanup_coordinator(&mut state, stream);
                return Err(Error::Overloaded);
            }
            if !registered_waiter {
                if let Err(error) = register_waiter(&mut state, &self.inner.config, bytes) {
                    state.append_rejected = state.append_rejected.saturating_add(1);
                    return Err(error);
                }
                registered_waiter = true;
                waiter_guard = Some(AdmissionWaiterGuard {
                    inner: self.inner.clone(),
                    bytes,
                    coordinator: stream.clone(),
                    active: true,
                });
            }
            drop(state);
            if tokio::time::timeout_at(deadline.into(), notified)
                .await
                .is_err()
            {
                drop(waiter_guard.take());
                let mut state = self.inner.state.lock().await;
                state.append_rejected = state.append_rejected.saturating_add(1);
                return Err(Error::AdmissionTimeout);
            }
        }
    }

    fn validate_cursor(cursor: &Cursor, stream: &StreamKey) -> Result<()> {
        if cursor.version != CURSOR_VERSION {
            return Err(Error::InvalidCursor("unsupported cursor version".into()));
        }
        if cursor.stream != *stream {
            return Err(Error::InvalidCursor(
                "cursor identifies another stream".into(),
            ));
        }
        Ok(())
    }

    fn validate_page(&self, page: PageLimits) -> Result<()> {
        if page.max_records == 0
            || page.max_bytes == 0
            || page.max_records > self.inner.config.reads.page.max_records
            || page.max_bytes > self.inner.config.reads.page.max_bytes
            || page.max_bytes < self.inner.config.events.max_bytes
        {
            return Err(Error::InvalidConfig(
                "page limits exceed runtime budget".into(),
            ));
        }
        Ok(())
    }

    async fn ensure_name_available(&self, id: &StreamId) -> Result<()> {
        let state = self.inner.state.lock().await;
        ensure_ready(&state)?;
        match lifecycle_name_error(&state, id) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

#[cfg(feature = "snapshots")]
impl<S: SnapshotStore> Runtime<S> {
    /// Begin or reconcile one immutable snapshot upload.
    pub async fn begin_snapshot(
        &self,
        descriptor: SnapshotDescriptor,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        Self::validate_cursor(&descriptor.covered, &descriptor.covered.stream)
            .map_err(snapshot_runtime_error)?;
        let descriptor_bytes = descriptor
            .accounted_bytes()
            .ok_or(SnapshotError::CapacityExceeded)?;
        let bytes = descriptor_bytes
            .checked_mul(3)
            .ok_or(SnapshotError::CapacityExceeded)?;
        let fallback = SnapshotError::BeginUnknown {
            descriptor: Box::new(descriptor.clone()),
        };
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        let name = descriptor.covered.stream.id.clone();
        self.snapshot_operation(bytes, fallback, async move {
            snapshot_name_available(&inner, &name).await?;
            store.begin_snapshot(descriptor).await
        })
        .await
    }

    /// Store or reconcile one bounded content chunk.
    pub async fn put_snapshot_chunk(
        &self,
        id: SnapshotId,
        chunk: SnapshotChunk,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        if chunk.bytes.is_empty() {
            return Err(SnapshotError::InvalidInput(
                "snapshot chunks must contain at least one byte".into(),
            ));
        }
        let bytes = chunk
            .accounted_bytes()
            .ok_or(SnapshotError::CapacityExceeded)?;
        let fallback = SnapshotError::ChunkUnknown {
            id,
            chunk: Box::new(chunk.clone()),
        };
        let store = self.inner.store.clone();
        self.snapshot_operation(bytes, fallback, async move {
            store.put_snapshot_chunk(id, chunk).await
        })
        .await
    }

    pub async fn snapshot_status(&self, id: SnapshotId) -> SnapshotResult<SnapshotUploadProgress> {
        let store = self.inner.store.clone();
        self.snapshot_operation(
            maximum_snapshot_descriptor_charge(),
            SnapshotError::StorageFailure("snapshot status task stopped".into()),
            async move { store.snapshot_status(id).await },
        )
        .await
    }

    /// Verify in bounded steps and publish. Once admitted, this work survives caller cancellation.
    pub async fn verify_and_publish_snapshot(
        &self,
        id: SnapshotId,
        limits: VerificationLimits,
    ) -> SnapshotResult<SnapshotDescriptor> {
        if limits.max_chunks == 0 || limits.max_bytes == 0 {
            return Err(SnapshotError::InvalidInput(
                "verification limits must be nonzero".into(),
            ));
        }
        let initial = self.snapshot_status(id).await?;
        if initial.descriptor.id != id {
            return Err(SnapshotError::CorruptStorage(
                "snapshot status returned another identity".into(),
            ));
        }
        let descriptor = initial.descriptor.clone();
        validate_snapshot_progress(&descriptor, &initial)?;
        let fallback = SnapshotError::PublicationUnknown {
            descriptor: Box::new(descriptor.clone()),
        };
        let store = self.inner.store.clone();
        let bytes = descriptor
            .accounted_bytes()
            .and_then(|bytes| bytes.checked_mul(4))
            .ok_or(SnapshotError::CapacityExceeded)?;
        self.snapshot_operation(bytes, fallback, async move {
            let mut previous = initial;
            loop {
                match previous.state {
                    SnapshotUploadState::Published => return Ok(descriptor),
                    SnapshotUploadState::Verified => {
                        let published = store.publish_snapshot(id).await?;
                        return if published == descriptor {
                            Ok(published)
                        } else {
                            Err(SnapshotError::CorruptStorage(
                                "snapshot publication returned another descriptor".into(),
                            ))
                        };
                    }
                    SnapshotUploadState::Aborted => {
                        return Err(SnapshotError::IncompleteUpload { id });
                    }
                    SnapshotUploadState::Uploading | SnapshotUploadState::Verifying => {}
                }
                let next = store.verify_snapshot_step(id, limits).await?;
                validate_snapshot_progress(&descriptor, &next)?;
                if next.state == SnapshotUploadState::Published {
                    return Ok(descriptor);
                }
                if next.state == SnapshotUploadState::Aborted {
                    return Err(SnapshotError::IncompleteUpload { id });
                }
                let legal = match (previous.state, next.state) {
                    (SnapshotUploadState::Uploading, SnapshotUploadState::Verifying) => {
                        next.verified_bytes >= previous.verified_bytes
                    }
                    (SnapshotUploadState::Uploading, SnapshotUploadState::Uploading)
                    | (SnapshotUploadState::Verifying, SnapshotUploadState::Verifying) => {
                        next.verified_bytes > previous.verified_bytes
                    }
                    (SnapshotUploadState::Uploading, SnapshotUploadState::Verified)
                    | (SnapshotUploadState::Verifying, SnapshotUploadState::Verified) => true,
                    _ => false,
                };
                if !legal || next.verified_bytes < previous.verified_bytes {
                    return Err(SnapshotError::CorruptStorage(
                        "snapshot verification returned an invalid transition".into(),
                    ));
                }
                previous = next;
                tokio::task::yield_now().await;
            }
        })
        .await
    }

    pub async fn abort_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotAbortReceipt> {
        let store = self.inner.store.clone();
        self.snapshot_operation(64, SnapshotError::AbortUnknown { id }, async move {
            store.abort_snapshot(id).await
        })
        .await
    }

    pub async fn cleanup_snapshot_staging(
        &self,
        limits: SnapshotCleanupLimits,
    ) -> SnapshotResult<SnapshotCleanupProgress> {
        if limits.max_rows == 0 || limits.max_bytes == 0 {
            return Err(SnapshotError::InvalidInput(
                "snapshot cleanup limits must be nonzero".into(),
            ));
        }
        let store = self.inner.store.clone();
        self.snapshot_operation(
            64,
            SnapshotError::StorageFailure("snapshot cleanup task stopped".into()),
            async move { store.cleanup_snapshot_staging(limits).await },
        )
        .await
    }

    pub async fn list_snapshots(
        &self,
        stream: &StreamKey,
        after: Option<SnapshotContinuation>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotPage> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(SnapshotError::InvalidInput(
                "snapshot page limits must be nonzero".into(),
            ));
        }
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        let stream = stream.clone();
        let name = stream.id.clone();
        let input_bytes = stream
            .id
            .as_str()
            .len()
            .checked_add(
                after
                    .as_ref()
                    .map_or(0, |position| position.covered.stream.id.as_str().len()),
            )
            .and_then(|bytes| bytes.checked_add(256))
            .and_then(|bytes| bytes.checked_add(limits.max_bytes))
            .ok_or(SnapshotError::CapacityExceeded)?;
        self.snapshot_operation(
            input_bytes,
            SnapshotError::StorageFailure("snapshot list task stopped".into()),
            async move {
                snapshot_name_available(&inner, &name).await?;
                store.list_snapshots(&stream, after, limits).await
            },
        )
        .await
    }

    pub async fn list_snapshot_uploads(
        &self,
        after: Option<SnapshotId>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(SnapshotError::InvalidInput(
                "snapshot upload page limits must be nonzero".into(),
            ));
        }
        let retained_bytes = limits
            .max_bytes
            .checked_add(64)
            .ok_or(SnapshotError::CapacityExceeded)?;
        let store = self.inner.store.clone();
        self.snapshot_operation(
            retained_bytes,
            SnapshotError::StorageFailure("snapshot upload list task stopped".into()),
            async move { store.list_snapshot_uploads(after, limits).await },
        )
        .await
    }

    pub async fn acquire_recovery(
        &self,
        id: SnapshotId,
        lifetime: Duration,
    ) -> SnapshotResult<RecoveryPlan> {
        if lifetime.is_zero() {
            return Err(SnapshotError::InvalidInput(
                "recovery lifetime must be nonzero".into(),
            ));
        }
        let store = self.inner.store.clone();
        self.snapshot_operation(
            128,
            SnapshotError::StorageFailure("recovery acquisition task stopped".into()),
            async move { store.acquire_recovery(id, lifetime).await },
        )
        .await
    }

    pub async fn read_snapshot_chunk(
        &self,
        lease: RecoveryLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> SnapshotResult<SnapshotBytePage> {
        if max_bytes == 0 {
            return Err(SnapshotError::InvalidInput(
                "snapshot read byte limit must be nonzero".into(),
            ));
        }
        let store = self.inner.store.clone();
        // A generic adapter may own a row buffer while it copies into Payload.
        // Admission covers both bounded buffers rather than assuming zero-copy.
        let retained_bytes = max_bytes
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(64))
            .ok_or(SnapshotError::CapacityExceeded)?;
        self.snapshot_operation(
            retained_bytes,
            SnapshotError::StorageFailure("snapshot content read task stopped".into()),
            async move { store.read_snapshot_chunk(lease, offset, max_bytes).await },
        )
        .await
    }

    pub async fn read_recovery_page(
        &self,
        lease: RecoveryLeaseId,
        after: u64,
        limits: PageLimits,
    ) -> SnapshotResult<Page> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(SnapshotError::InvalidInput(
                "recovery page limits must be nonzero".into(),
            ));
        }
        let store = self.inner.store.clone();
        let retained_bytes = limits
            .max_bytes
            .checked_add(128)
            .ok_or(SnapshotError::CapacityExceeded)?;
        self.snapshot_operation(
            retained_bytes,
            SnapshotError::StorageFailure("recovery history read task stopped".into()),
            async move { store.read_recovery_page(lease, after, limits).await },
        )
        .await
    }

    pub async fn release_recovery(
        &self,
        lease: RecoveryLeaseId,
    ) -> SnapshotResult<RecoveryRelease> {
        let store = self.inner.store.clone();
        self.snapshot_operation(
            32,
            SnapshotError::StorageFailure("recovery release task stopped".into()),
            async move { store.release_recovery(lease).await },
        )
        .await
    }

    async fn snapshot_operation<T, F>(
        &self,
        retained_bytes: usize,
        uncertain: SnapshotError,
        operation: F,
    ) -> SnapshotResult<T>
    where
        T: Send + 'static,
        F: Future<Output = SnapshotResult<T>> + Send + 'static,
    {
        let bytes = retained_bytes.max(1);
        if bytes > self.inner.config.snapshots.max_waiter_bytes
            || bytes > self.inner.config.snapshots.max_in_flight_chunk_bytes
            || bytes > u32::MAX as usize
        {
            return Err(SnapshotError::CapacityExceeded);
        }
        let deadline: tokio::time::Instant = Instant::now()
            .checked_add(self.inner.config.snapshots.admission_timeout)
            .ok_or_else(|| {
                SnapshotError::InvalidConfig("snapshot admission deadline overflow".into())
            })?
            .into();
        let waiter = self
            .inner
            .snapshot_waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| SnapshotError::Overloaded)?;
        let waiter_bytes = self
            .inner
            .snapshot_waiter_bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| SnapshotError::Overloaded)?;
        let in_flight_bytes = snapshot_permit(
            self.inner.clone(),
            self.inner.snapshot_in_flight_bytes.clone(),
            bytes as u32,
            deadline,
        )
        .await?;
        let slot = snapshot_permit(
            self.inner.clone(),
            self.inner.snapshot_slots.clone(),
            1,
            deadline,
        )
        .await?;
        {
            let mut state = self.inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(SnapshotError::Closed);
            }
            state.active_io += 1;
        }
        drop(waiter_bytes);
        drop(waiter);

        let inner = self.inner.clone();
        let executor = inner.executor.clone();
        let receiver_fallback = uncertain.clone();
        let (sender, receiver) = oneshot::channel();
        executor.spawn(async move {
            let result = match tokio::spawn(operation).await {
                Ok(result) => result,
                Err(_) => Err(uncertain),
            };
            let _slot = slot;
            let _in_flight_bytes = in_flight_bytes;
            let mut state = inner.state.lock().await;
            state.active_io = state.active_io.saturating_sub(1);
            drop(state);
            inner.changed.notify_waiters();
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(receiver_fallback))
    }
}

#[cfg(feature = "retention")]
impl<S: RetentionStore> Runtime<S> {
    pub async fn retention_status(&self, stream: &StreamKey) -> RetentionResult<RetentionStatus> {
        let stream = stream.clone();
        let name = stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        let bytes = name
            .as_str()
            .len()
            .checked_add(256)
            .ok_or(RetentionError::CapacityExceeded)?;
        self.retention_operation(
            bytes,
            RetentionError::StorageFailure("retention status task stopped".into()),
            async move {
                retention_name_available(&inner, &name).await?;
                store.retention_status(&stream).await
            },
        )
        .await
    }

    pub async fn enable_retry_policy(
        &self,
        request: EnableRetryPolicy,
    ) -> RetentionResult<EnableRetryPolicyReceipt> {
        let bytes = retention_request_charge(&request.operation_id, &request.stream)?;
        let fallback = RetentionError::EnableUnknown(Box::new(request.clone()));
        let name = request.stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.retention_operation(bytes, fallback, async move {
            let anchor = retention_coordinator(inner.clone(), request.stream.clone()).await?;
            let _gate = anchor.coordinator.write_gate.lock().await;
            retention_name_available(&inner, &name).await?;
            store.enable_retry_policy(request).await
        })
        .await
    }

    pub async fn advance_retry_generation(
        &self,
        request: AdvanceRetryGeneration,
    ) -> RetentionResult<AdvanceRetryGenerationReceipt> {
        let bytes = retention_request_charge(&request.operation_id, &request.stream)?;
        let fallback = RetentionError::AdvanceGenerationUnknown(Box::new(request.clone()));
        let name = request.stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.retention_operation(bytes, fallback, async move {
            let anchor = retention_coordinator(inner.clone(), request.stream.clone()).await?;
            let _gate = anchor.coordinator.write_gate.lock().await;
            retention_name_available(&inner, &name).await?;
            store.advance_retry_generation(request).await
        })
        .await
    }

    pub async fn expire_retry_generations(
        &self,
        request: ExpireRetryGenerations,
    ) -> RetentionResult<ExpireRetryGenerationsReceipt> {
        let bytes = retention_request_charge(&request.operation_id, &request.stream)?;
        let fallback = RetentionError::ExpireGenerationsUnknown(Box::new(request.clone()));
        let name = request.stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.retention_operation(bytes, fallback, async move {
            let anchor = retention_coordinator(inner.clone(), request.stream.clone()).await?;
            let _gate = anchor.coordinator.write_gate.lock().await;
            retention_name_available(&inner, &name).await?;
            store.expire_retry_generations(request).await
        })
        .await
    }

    pub async fn append_generated(
        &self,
        stream: &StreamKey,
        event: GeneratedEvent,
    ) -> RetentionResult<AppendReceipt> {
        if event.event.accounted_bytes() > self.inner.config.events.max_bytes {
            return Err(RetentionError::InvalidInput(
                "event exceeds the runtime event limit".into(),
            ));
        }
        let bytes = event
            .event
            .accounted_bytes()
            .checked_add(stream.id.as_str().len())
            .and_then(|bytes| bytes.checked_add(384))
            .ok_or(RetentionError::CapacityExceeded)?;
        let identity = GeneratedEventIdentity {
            stream: stream.clone(),
            generation: event.generation,
            event_id: event.event.id.clone(),
        };
        let fallback = RetentionError::GeneratedAppendUnknown(Box::new(identity));
        let stream = stream.clone();
        let name = stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.retention_operation(bytes, fallback, async move {
            let anchor = retention_coordinator(inner.clone(), stream.clone()).await?;
            let _gate = anchor.coordinator.write_gate.lock().await;
            retention_name_available(&inner, &name).await?;
            let expected = event.event.clone();
            let receipt = store.append_generated(&stream, event).await?;
            if receipt.record.cursor.version != CURSOR_VERSION
                || receipt.record.cursor.stream != stream
                || receipt.record.cursor.offset == 0
                || receipt.record.event != expected
            {
                return Err(RetentionError::CorruptStorage(
                    "generated append returned a different record".into(),
                ));
            }
            // A deduplicated retry may be the first successful observation of a
            // commit whose earlier acknowledgement was lost.
            observe_retention_commit(&anchor.coordinator, receipt.record.clone());
            Ok(receipt)
        })
        .await
    }

    pub async fn lookup_generated(
        &self,
        stream: &StreamKey,
        generation: RetryGeneration,
        event_id: &EventId,
    ) -> RetentionResult<Option<Arc<Record>>> {
        let bytes = stream
            .id
            .as_str()
            .len()
            .checked_add(event_id.as_str().len())
            .and_then(|bytes| bytes.checked_add(self.inner.config.events.max_bytes))
            .and_then(|bytes| bytes.checked_add(384))
            .ok_or(RetentionError::CapacityExceeded)?;
        let stream = stream.clone();
        let event_id = event_id.clone();
        let name = stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.retention_operation(
            bytes,
            RetentionError::StorageFailure("generated event lookup task stopped".into()),
            async move {
                retention_name_available(&inner, &name).await?;
                store.lookup_generated(&stream, generation, &event_id).await
            },
        )
        .await
    }

    pub async fn advance_retention_floor(
        &self,
        request: AdvanceRetentionFloor,
    ) -> RetentionResult<AdvanceRetentionFloorReceipt> {
        let bytes = retention_request_charge(&request.operation_id, &request.stream)?
            .checked_add(192)
            .ok_or(RetentionError::CapacityExceeded)?;
        let fallback = RetentionError::AdvanceFloorUnknown(Box::new(request.clone()));
        let name = request.stream.id.clone();
        let store = self.inner.store.clone();
        let inner = self.inner.clone();
        self.retention_operation(bytes, fallback, async move {
            let anchor = retention_coordinator(inner.clone(), request.stream.clone()).await?;
            let _gate = anchor.coordinator.write_gate.lock().await;
            retention_name_available(&inner, &name).await?;
            let expected = request.clone();
            let receipt = store.advance_retention_floor(request).await?;
            if receipt.request != expected
                || receipt.status.bounds.floor != expected.new_floor
                || receipt.status.bounds.tail.stream != expected.stream
            {
                return Err(RetentionError::CorruptStorage(
                    "floor mutation returned an invalid receipt".into(),
                ));
            }
            let mut state = inner.state.lock().await;
            {
                let mut cs = anchor
                    .coordinator
                    .state
                    .lock()
                    .expect("coordinator lock poisoned");
                cs.observed_floor = receipt.status.bounds.floor.offset;
                cs.observed_tail = cs.observed_tail.max(receipt.status.bounds.tail.offset);
                cs.generation = cs.generation.wrapping_add(1);
            }
            end_subscriptions_below_floor(
                &mut state,
                &anchor.coordinator,
                receipt.status.bounds.clone(),
            );
            anchor.coordinator.wake.notify_waiters();
            Ok(receipt)
        })
        .await
    }

    pub async fn cleanup_retention(
        &self,
        limits: RetentionCleanupLimits,
    ) -> RetentionResult<RetentionCleanupProgress> {
        if limits.max_event_rows == 0 || limits.max_retry_rows == 0 || limits.max_bytes == 0 {
            return Err(RetentionError::InvalidInput(
                "retention cleanup limits must be nonzero".into(),
            ));
        }
        let store = self.inner.store.clone();
        self.retention_operation(
            64,
            RetentionError::StorageFailure("retention cleanup task stopped".into()),
            async move { store.cleanup_retention(limits).await },
        )
        .await
    }

    async fn retention_operation<T, F>(
        &self,
        retained_bytes: usize,
        uncertain: RetentionError,
        operation: F,
    ) -> RetentionResult<T>
    where
        T: Send + 'static,
        F: Future<Output = RetentionResult<T>> + Send + 'static,
    {
        let bytes = retained_bytes.max(1);
        if bytes > self.inner.config.retention.max_waiter_bytes
            || bytes > self.inner.config.retention.max_in_flight_bytes
            || bytes > u32::MAX as usize
        {
            return Err(RetentionError::CapacityExceeded);
        }
        let deadline: tokio::time::Instant = Instant::now()
            .checked_add(self.inner.config.retention.admission_timeout)
            .ok_or_else(|| {
                RetentionError::InvalidConfig("retention admission deadline overflow".into())
            })?
            .into();
        let waiter = self
            .inner
            .retention_waiters
            .clone()
            .try_acquire_owned()
            .map_err(|_| RetentionError::Overloaded)?;
        let waiter_bytes = self
            .inner
            .retention_waiter_bytes
            .clone()
            .try_acquire_many_owned(bytes as u32)
            .map_err(|_| RetentionError::Overloaded)?;
        let in_flight_bytes = retention_permit(
            self.inner.clone(),
            self.inner.retention_in_flight_bytes.clone(),
            bytes as u32,
            deadline,
        )
        .await?;
        let slot = retention_permit(
            self.inner.clone(),
            self.inner.retention_slots.clone(),
            1,
            deadline,
        )
        .await?;
        {
            let mut state = self.inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(RetentionError::Closed);
            }
            state.active_io += 1;
        }
        drop(waiter_bytes);
        drop(waiter);

        let inner = self.inner.clone();
        let executor = inner.executor.clone();
        let receiver_fallback = uncertain.clone();
        let (sender, receiver) = oneshot::channel();
        executor.spawn(async move {
            let result = match tokio::spawn(operation).await {
                Ok(result) => result,
                Err(_) => Err(uncertain),
            };
            let _slot = slot;
            let _in_flight_bytes = in_flight_bytes;
            let mut state = inner.state.lock().await;
            state.active_io = state.active_io.saturating_sub(1);
            drop(state);
            inner.changed.notify_waiters();
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(receiver_fallback))
    }
}

#[cfg(feature = "snapshots")]
async fn snapshot_permit<S: EventStore>(
    inner: Arc<Inner<S>>,
    semaphore: Arc<Semaphore>,
    permits: u32,
    deadline: tokio::time::Instant,
) -> SnapshotResult<OwnedSemaphorePermit> {
    let acquisition = semaphore.acquire_many_owned(permits);
    tokio::pin!(acquisition);
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        {
            let state = inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(SnapshotError::Closed);
            }
        }
        tokio::select! {
            result = &mut acquisition => return result.map_err(|_| SnapshotError::Closed),
            _ = &mut timeout => return Err(SnapshotError::AdmissionTimeout),
            _ = changed => {}
        }
    }
}

#[cfg(feature = "snapshots")]
fn snapshot_runtime_error(error: Error) -> SnapshotError {
    match error {
        Error::Closed => SnapshotError::Closed,
        Error::Overloaded => SnapshotError::Overloaded,
        Error::AdmissionTimeout => SnapshotError::AdmissionTimeout,
        Error::StaleIncarnation { current } => SnapshotError::StaleIncarnation {
            current: match *current {
                StreamAvailability::Active(stream) => Some(Box::new(stream)),
                StreamAvailability::Unavailable(_) => None,
            },
        },
        Error::StreamUnavailable { .. } => SnapshotError::StaleIncarnation { current: None },
        Error::CursorAhead { tail } => SnapshotError::CursorAhead {
            tail: Box::new(tail),
        },
        Error::HistoryUnavailable { bounds } => SnapshotError::MissingHistory {
            floor: Box::new(bounds.floor),
        },
        Error::StoreCorrupt(detail) => SnapshotError::CorruptStorage(detail),
        other => SnapshotError::StorageFailure(other.to_string()),
    }
}

#[cfg(feature = "snapshots")]
async fn snapshot_name_available<S: EventStore>(
    inner: &Arc<Inner<S>>,
    id: &StreamId,
) -> SnapshotResult<()> {
    let state = inner.state.lock().await;
    if state.lifecycle != RuntimeLifecycle::Ready {
        return Err(SnapshotError::Closed);
    }
    match lifecycle_name_error(&state, id) {
        Some(error) => Err(snapshot_runtime_error(error)),
        None => Ok(()),
    }
}

#[cfg(feature = "snapshots")]
fn validate_snapshot_progress(
    expected: &SnapshotDescriptor,
    progress: &SnapshotUploadProgress,
) -> SnapshotResult<()> {
    let terminal = matches!(
        progress.state,
        SnapshotUploadState::Verified | SnapshotUploadState::Published
    );
    if progress.descriptor != *expected
        || progress.verified_bytes > progress.accepted_bytes
        || progress.accepted_bytes > expected.content_bytes
        || (terminal
            && (progress.accepted_bytes != expected.content_bytes
                || progress.verified_bytes != expected.content_bytes))
    {
        return Err(SnapshotError::CorruptStorage(
            "snapshot store returned invalid progress".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "snapshots")]
const fn maximum_snapshot_descriptor_charge() -> usize {
    SNAPSHOT_DESCRIPTOR_ENVELOPE_BYTES + (2 * MAX_IDENTIFIER_BYTES)
}

#[cfg(feature = "retention")]
fn retention_request_charge(
    operation: &RetentionOperationId,
    stream: &StreamKey,
) -> RetentionResult<usize> {
    operation
        .as_str()
        .len()
        .checked_add(stream.id.as_str().len())
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or(RetentionError::CapacityExceeded)
}

#[cfg(feature = "retention")]
async fn retention_name_available<S: EventStore>(
    inner: &Arc<Inner<S>>,
    id: &StreamId,
) -> RetentionResult<()> {
    let state = inner.state.lock().await;
    if state.lifecycle != RuntimeLifecycle::Ready {
        return Err(RetentionError::Closed);
    }
    match lifecycle_name_error(&state, id) {
        None => Ok(()),
        Some(Error::StaleIncarnation { current }) => Err(RetentionError::StaleIncarnation {
            current: match *current {
                StreamAvailability::Active(stream) => Some(Box::new(stream)),
                StreamAvailability::Unavailable(_) => None,
            },
        }),
        Some(Error::StreamUnavailable { .. } | Error::StreamNotFound) => {
            Err(RetentionError::StaleIncarnation { current: None })
        }
        Some(Error::Closed) => Err(RetentionError::Closed),
        Some(error) => Err(RetentionError::StorageFailure(error.to_string())),
    }
}

#[cfg(feature = "retention")]
async fn retention_permit<S: EventStore>(
    inner: Arc<Inner<S>>,
    semaphore: Arc<Semaphore>,
    permits: u32,
    deadline: tokio::time::Instant,
) -> RetentionResult<OwnedSemaphorePermit> {
    let acquisition = semaphore.acquire_many_owned(permits);
    tokio::pin!(acquisition);
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        {
            let state = inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(RetentionError::Closed);
            }
        }
        tokio::select! {
            result = &mut acquisition => return result.map_err(|_| RetentionError::Closed),
            _ = &mut timeout => return Err(RetentionError::AdmissionTimeout),
            _ = changed => {}
        }
    }
}

#[cfg(feature = "source-journal")]
async fn journal_permit<S: EventStore>(
    inner: Arc<Inner<S>>,
    semaphore: Arc<Semaphore>,
    permits: u32,
    deadline: tokio::time::Instant,
) -> JournalResult<OwnedSemaphorePermit> {
    let acquisition = semaphore.acquire_many_owned(permits);
    tokio::pin!(acquisition);
    let timeout = tokio::time::sleep_until(deadline);
    tokio::pin!(timeout);
    loop {
        let changed = inner.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        {
            let state = inner.state.lock().await;
            if state.lifecycle != RuntimeLifecycle::Ready {
                return Err(JournalError::Closed);
            }
        }
        tokio::select! {
            result = &mut acquisition => return result.map_err(|_| JournalError::Closed),
            _ = &mut timeout => return Err(JournalError::AdmissionTimeout),
            _ = changed => {}
        }
    }
}

#[cfg(feature = "retention")]
async fn retention_coordinator<S: EventStore>(
    inner: Arc<Inner<S>>,
    stream: StreamKey,
) -> RetentionResult<RetentionCoordinatorGuard<S>> {
    let coordinator = {
        let mut state = inner.state.lock().await;
        if state.lifecycle != RuntimeLifecycle::Ready {
            return Err(RetentionError::Closed);
        }
        if let Some(error) = lifecycle_name_error(&state, &stream.id) {
            return Err(match error {
                Error::LifecycleCommitUnknown { operation_id } => {
                    RetentionError::StorageFailure(format!(
                        "stream lifecycle outcome is unresolved: {}",
                        operation_id.as_str()
                    ))
                }
                other => RetentionError::StorageFailure(other.to_string()),
            });
        }
        let coordinator = match state.coordinators.get(&stream).cloned() {
            Some(coordinator) => coordinator,
            None => {
                if state.coordinators.len() >= inner.config.scheduling.max_coordinators {
                    return Err(RetentionError::Overloaded);
                }
                let coordinator = new_coordinator(stream.clone());
                state.coordinators.insert(stream, coordinator.clone());
                coordinator
            }
        };
        coordinator
            .state
            .lock()
            .expect("coordinator lock poisoned")
            .maintenance_in_flight += 1;
        coordinator
    };
    Ok(RetentionCoordinatorGuard { inner, coordinator })
}

#[cfg(feature = "retention")]
fn observe_retention_commit(coordinator: &Coordinator, record: Arc<Record>) {
    let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
    cs.observed_tail = cs.observed_tail.max(record.cursor.offset);
    cs.generation = cs.generation.wrapping_add(1);
    for subscription in cs.subscriptions.iter().flatten() {
        let mut ss = subscription
            .state
            .lock()
            .expect("subscription lock poisoned");
        if ss.registered && ss.last_delivered.offset < cs.observed_tail && ss.behind_since.is_none()
        {
            ss.behind_since = Some(Instant::now());
        }
    }
    coordinator.wake.notify_waiters();
}

#[cfg(feature = "retention")]
fn end_subscriptions_below_floor(
    state: &mut State,
    coordinator: &Arc<Coordinator>,
    bounds: Bounds,
) {
    let subscriptions: Vec<_> = coordinator
        .state
        .lock()
        .expect("coordinator lock poisoned")
        .subscriptions
        .iter()
        .flatten()
        .filter(|subscription| {
            subscription
                .state
                .lock()
                .expect("subscription lock poisoned")
                .last_delivered
                .offset
                < bounds.floor.offset
        })
        .cloned()
        .collect();
    for subscription in subscriptions {
        let last_delivered = subscription
            .state
            .lock()
            .expect("subscription lock poisoned")
            .last_delivered
            .clone();
        if end_subscription(
            &subscription,
            Error::SubscriberLagged {
                last_delivered,
                bounds: Box::new(bounds.clone()),
            },
        ) {
            remove_subscription_membership(state, coordinator, &subscription);
        }
    }
}

impl<S: LifecycleStore> Runtime<S> {
    /// Change the active lifetime for a stream name. Once admitted, the runtime
    /// owns the store call even if the caller drops this future.
    pub async fn change_lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleReceipt> {
        let operation_id = request.operation_id.clone();
        let bytes = request.receipt_charge().ok_or(Error::CapacityExceeded)?;
        if bytes > self.inner.config.maintenance.max_operation_bytes {
            return Err(Error::CapacityExceeded);
        }
        let coordinator = {
            let mut state = self.inner.state.lock().await;
            ensure_ready(&state)?;
            if let Some(existing) = state.lifecycle_in_flight.get(&request.expected.id) {
                return if existing == &request {
                    Err(Error::Overloaded)
                } else {
                    Err(Error::LifecycleCommitUnknown {
                        operation_id: existing.operation_id.clone(),
                    })
                };
            }
            let reuses_reservation = match state.unresolved_lifecycle.get(&request.expected.id) {
                Some(existing) if existing == &request => true,
                Some(existing) => {
                    return Err(Error::LifecycleCommitUnknown {
                        operation_id: existing.operation_id.clone(),
                    });
                }
                None => false,
            };
            if !reuses_reservation
                && (state.maintenance_reserved >= self.inner.config.maintenance.max_operations
                    || state
                        .maintenance_reserved_bytes
                        .checked_add(bytes)
                        .is_none_or(|total| {
                            total > self.inner.config.maintenance.max_operation_bytes
                        }))
            {
                return Err(Error::Overloaded);
            }
            let coordinator = match state.coordinators.get(&request.expected).cloned() {
                Some(coordinator) => coordinator,
                None => {
                    if state.coordinators.len() >= self.inner.config.scheduling.max_coordinators {
                        return Err(Error::Overloaded);
                    }
                    let coordinator = new_coordinator(request.expected.clone());
                    state
                        .coordinators
                        .insert(request.expected.clone(), coordinator.clone());
                    coordinator
                }
            };
            if !reuses_reservation {
                state.maintenance_reserved += 1;
                state.maintenance_reserved_bytes += bytes;
            }
            coordinator
                .state
                .lock()
                .expect("coordinator lock poisoned")
                .maintenance_in_flight += 1;
            state.maintenance_active += 1;
            state
                .lifecycle_in_flight
                .insert(request.expected.id.clone(), request.clone());
            coordinator
        };

        let (sender, receiver) = oneshot::channel();
        let inner = self.inner.clone();
        let executor = inner.executor.clone();
        executor.spawn(async move {
            let gate = coordinator.write_gate.clone().lock_owned().await;
            let store = inner.store.clone();
            let operation = request.clone();
            let raw_result =
                match tokio::spawn(async move { store.change_lifecycle(operation).await }).await {
                    Ok(result) => result,
                    Err(_) => Err(Error::LifecycleCommitUnknown {
                        operation_id: request.operation_id.clone(),
                    }),
                };
            let (result, contract_fault) = match raw_result {
                Ok(receipt) if lifecycle_receipt_is_valid(&request, &receipt) => {
                    (Ok(receipt), false)
                }
                Ok(_) => (
                    Err(Error::LifecycleCommitUnknown {
                        operation_id: request.operation_id.clone(),
                    }),
                    true,
                ),
                Err(Error::LifecycleCommitUnknown { operation_id })
                    if operation_id != request.operation_id =>
                {
                    (
                        Err(Error::LifecycleCommitUnknown {
                            operation_id: request.operation_id.clone(),
                        }),
                        true,
                    )
                }
                Err(error) => (Err(error), false),
            };
            let mut state = inner.state.lock().await;
            state.maintenance_active = state.maintenance_active.saturating_sub(1);
            state.lifecycle_in_flight.remove(&request.expected.id);
            {
                let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                cs.maintenance_in_flight = cs.maintenance_in_flight.saturating_sub(1);
            }
            match &result {
                Err(Error::LifecycleCommitUnknown { .. }) => {
                    state
                        .unresolved_lifecycle
                        .insert(request.expected.id.clone(), request.clone());
                    end_coordinator_subscriptions(
                        &mut state,
                        &coordinator,
                        Error::LifecycleCommitUnknown {
                            operation_id: request.operation_id.clone(),
                        },
                    );
                }
                Ok(receipt) => {
                    state.unresolved_lifecycle.remove(&request.expected.id);
                    release_maintenance_reservation(&mut state, bytes);
                    let terminal = match &receipt.replacement {
                        Some(replacement) => Error::StaleIncarnation {
                            current: Box::new(StreamAvailability::Active(replacement.clone())),
                        },
                        None => Error::StreamUnavailable {
                            last: Box::new(request.expected.clone()),
                        },
                    };
                    end_coordinator_subscriptions(&mut state, &coordinator, terminal);
                }
                Err(_) => {
                    state.unresolved_lifecycle.remove(&request.expected.id);
                    release_maintenance_reservation(&mut state, bytes);
                    if let Err(
                        error @ (Error::OwnershipLost
                        | Error::StoreCorrupt(_)
                        | Error::RuntimeFaulted(_)),
                    ) = &result
                    {
                        state.lifecycle = RuntimeLifecycle::Faulted;
                        state.fault = Some(error.to_string());
                        end_all_subscriptions(
                            &mut state,
                            Error::RuntimeFaulted("store can no longer continue safely".into()),
                        );
                    }
                }
            }
            if contract_fault {
                state.lifecycle = RuntimeLifecycle::Faulted;
                state.fault = Some("lifecycle store returned an invalid receipt".into());
                end_all_subscriptions(
                    &mut state,
                    Error::RuntimeFaulted("lifecycle store violated its receipt contract".into()),
                );
            }
            cleanup_coordinator(&mut state, &coordinator.key);
            drop(state);
            drop(gate);
            inner.changed.notify_waiters();
            let _ = sender.send(result);
        });
        receiver
            .await
            .unwrap_or(Err(Error::LifecycleCommitUnknown { operation_id }))
    }

    /// Remove one bounded prefix from retired history.
    pub async fn cleanup_retired(&self) -> Result<CleanupProgress> {
        {
            let mut state = self.inner.state.lock().await;
            ensure_ready(&state)?;
            if state.maintenance_reserved >= self.inner.config.maintenance.max_operations
                || state.cleanup_active >= self.inner.config.maintenance.max_cleanup_operations
            {
                return Err(Error::Overloaded);
            }
            state.maintenance_reserved += 1;
            state.maintenance_active += 1;
            state.cleanup_active += 1;
        }
        let (sender, receiver) = oneshot::channel();
        let inner = self.inner.clone();
        let limits = inner.config.maintenance.cleanup;
        let executor = inner.executor.clone();
        executor.spawn(async move {
            let store = inner.store.clone();
            let result =
                match tokio::spawn(async move { store.cleanup_retired(limits).await }).await {
                    Ok(result) => result,
                    Err(join) => Err(Error::RuntimeFaulted(format!(
                        "store cleanup task failed: {join}"
                    ))),
                };
            let mut state = inner.state.lock().await;
            state.maintenance_reserved = state.maintenance_reserved.saturating_sub(1);
            state.maintenance_active = state.maintenance_active.saturating_sub(1);
            state.cleanup_active = state.cleanup_active.saturating_sub(1);
            if let Err(
                error @ (Error::OwnershipLost | Error::StoreCorrupt(_) | Error::RuntimeFaulted(_)),
            ) = &result
            {
                state.lifecycle = RuntimeLifecycle::Faulted;
                state.fault = Some(error.to_string());
                end_all_subscriptions(
                    &mut state,
                    Error::RuntimeFaulted("store can no longer continue safely".into()),
                );
            }
            drop(state);
            inner.changed.notify_waiters();
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or_else(|_| {
            Err(Error::RuntimeFaulted(
                "cleanup owner stopped without reporting an outcome".into(),
            ))
        })
    }
}

#[async_trait]
impl<S: EventStore> EventSink for Runtime<S> {
    async fn append(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.enqueue(stream, event, true).await
    }
}

#[async_trait]
impl<S: EventStore> EventReader for Runtime<S> {
    async fn create_stream(&self, id: &StreamId) -> Result<StreamKey> {
        self.ensure_name_available(id).await?;
        let store = self.inner.store.clone();
        let owned_id = id.clone();
        let result = tracked_read(self.inner.clone(), 0, async move {
            store.create_if_absent(&owned_id).await
        })
        .await
        .map(|(value, _)| value)?;
        self.ensure_name_available(id).await?;
        Ok(result)
    }

    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        self.ensure_name_available(id).await?;
        let store = self.inner.store.clone();
        let owned_id = id.clone();
        let result = tracked_read(self.inner.clone(), 0, async move {
            store.find_stream(&owned_id).await
        })
        .await
        .map(|(value, _)| value)?;
        self.ensure_name_available(id).await?;
        Ok(result)
    }

    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.ensure_name_available(&stream.id).await?;
        let store = self.inner.store.clone();
        let stream = stream.clone();
        let result = tracked_read(
            self.inner.clone(),
            0,
            async move { store.bounds(&stream).await },
        )
        .await
        .map(|(value, _)| value)?;
        self.ensure_name_available(&result.tail.stream.id).await?;
        Ok(result)
    }

    async fn read_after(
        &self,
        after: &Cursor,
        limits: PageLimits,
        through: Option<&Cursor>,
    ) -> Result<Page> {
        Self::validate_cursor(after, &after.stream)?;
        self.ensure_name_available(&after.stream.id).await?;
        self.validate_page(limits)?;
        let bounds = self.bounds(&after.stream).await?;
        if after.offset < bounds.floor.offset {
            return Err(Error::HistoryUnavailable { bounds });
        }
        if after.offset > bounds.tail.offset {
            return Err(Error::CursorAhead { tail: bounds.tail });
        }
        let upper = match through {
            Some(cursor) => {
                Self::validate_cursor(cursor, &after.stream)?;
                if cursor.offset < after.offset {
                    return Err(Error::InvalidCursor("through precedes after".into()));
                }
                if cursor.offset > bounds.tail.offset {
                    return Err(Error::CursorAhead { tail: bounds.tail });
                }
                cursor.offset
            }
            None => bounds.tail.offset,
        };
        let store = self.inner.store.clone();
        let stream = after.stream.clone();
        let after_offset = after.offset;
        let (page, _page_permit) = tracked_read(self.inner.clone(), limits.max_bytes, async move {
            store.read_range(&stream, after_offset, upper, limits).await
        })
        .await?;
        self.ensure_name_available(&after.stream.id).await?;
        let mut state = self.inner.state.lock().await;
        sample(
            &mut state,
            &self.inner.config,
            DiagnosticKind::ReadPage,
            "history page",
        );
        Ok(page)
    }
}

#[async_trait]
impl<S: EventStore> EventRuntime for Runtime<S> {
    type Subscription = RuntimeSubscription<S>;

    async fn try_append(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.enqueue(stream, event, false).await
    }

    async fn subscribe(
        &self,
        stream: &StreamKey,
        options: SubscriptionOptions,
    ) -> Result<Self::Subscription> {
        self.validate_page(options.page)?;
        if options.max_lag_records == 0 || options.max_lag_duration.is_zero() {
            return Err(Error::InvalidConfig(
                "subscription lag limits must be nonzero".into(),
            ));
        }
        let coordinator = {
            let mut state = self.inner.state.lock().await;
            ensure_ready(&state)?;
            if let Some(error) = lifecycle_name_error(&state, &stream.id) {
                return Err(error);
            }
            if state.subscriptions >= self.inner.config.subscriptions.max_total {
                return Err(Error::Overloaded);
            }
            let coordinator = if let Some(c) = state.coordinators.get(stream).cloned() {
                c
            } else {
                if state.coordinators.len() >= self.inner.config.scheduling.max_coordinators {
                    return Err(Error::Overloaded);
                }
                let c = Arc::new(Coordinator {
                    key: stream.clone(),
                    write_gate: Arc::new(Mutex::new(())),
                    state: StdMutex::new(CoordinatorState {
                        queue: VecDeque::new(),
                        queued_bytes: 0,
                        scheduled: false,
                        in_flight: 0,
                        maintenance_in_flight: 0,
                        observed_floor: 0,
                        observed_tail: 0,
                        generation: 0,
                        subscriptions: Vec::new(),
                        free_subscription_slots: Vec::new(),
                        sweep_index: 0,
                        subscription_reservations: 0,
                    }),
                    wake: Notify::new(),
                });
                state.coordinators.insert(stream.clone(), c.clone());
                c
            };
            {
                let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                if cs.subscriptions.len() - cs.free_subscription_slots.len()
                    + cs.subscription_reservations
                    >= self.inner.config.subscriptions.max_per_stream
                {
                    return Err(Error::Overloaded);
                }
                cs.subscription_reservations += 1;
            }
            state.subscriptions += 1;
            coordinator
        };
        let mut reservation = SubscriptionReservationGuard {
            inner: self.inner.clone(),
            coordinator: coordinator.clone(),
            active: true,
        };
        let _gate = coordinator.write_gate.lock().await;
        {
            let state = self.inner.state.lock().await;
            ensure_ready(&state)?;
            if let Some(error) = lifecycle_name_error(&state, &stream.id) {
                return Err(error);
            }
        }
        let store = self.inner.store.clone();
        let bounds_stream = stream.clone();
        let bounds = tracked_read(self.inner.clone(), 0, async move {
            store.bounds(&bounds_stream).await
        })
        .await?
        .0;
        let start = match &options.start {
            StartPosition::Beginning => bounds.floor.clone(),
            StartPosition::Future => bounds.tail.clone(),
            StartPosition::After(cursor) => {
                Self::validate_cursor(cursor, stream)?;
                if cursor.offset < bounds.floor.offset {
                    return Err(Error::HistoryUnavailable { bounds });
                }
                if cursor.offset > bounds.tail.offset {
                    return Err(Error::CursorAhead { tail: bounds.tail });
                }
                cursor.clone()
            }
        };
        let shared = Arc::new(SubscriptionShared {
            state: StdMutex::new(SharedSubscriptionState {
                last_delivered: start.clone(),
                created: Instant::now(),
                behind_since: (bounds.tail.offset > start.offset).then(Instant::now),
                ended: None,
                terminal_delivered: false,
                registered: false,
                membership_slot: None,
                buffer: VecDeque::new(),
                page_permit: None,
                max_lag_records: options.max_lag_records,
                max_lag_duration: options.max_lag_duration,
                catch_up_grace: options.catch_up_grace,
            }),
        });
        {
            let state = self.inner.state.lock().await;
            ensure_ready(&state)?;
            let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
            cs.subscription_reservations -= 1;
            cs.observed_floor = bounds.floor.offset;
            cs.observed_tail = cs.observed_tail.max(bounds.tail.offset);
            let index = cs
                .free_subscription_slots
                .pop()
                .unwrap_or(cs.subscriptions.len());
            let slot = NonZeroUsize::new(index + 1).expect("subscription slot index is nonzero");
            {
                let mut ss = shared.state.lock().expect("subscription lock poisoned");
                ss.install_membership(slot);
            }
            if index == cs.subscriptions.len() {
                cs.subscriptions.push(Some(shared.clone()));
            } else {
                debug_assert!(cs.subscriptions[index].is_none());
                cs.subscriptions[index] = Some(shared.clone());
            }
            reservation.disarm();
        }
        drop(_gate);
        Ok(RuntimeSubscription {
            inner: self.inner.clone(),
            coordinator,
            shared,
            last_delivered: start,
            replay_high: bounds.tail.offset,
            options,
        })
    }

    async fn shutdown(&self, deadline: Duration) -> Result<ShutdownReport> {
        {
            let mut state = self.inner.state.lock().await;
            match state.lifecycle {
                RuntimeLifecycle::Closed => {
                    return Ok(ShutdownReport {
                        closed: true,
                        unresolved: unresolved_appends(&state),
                        unresolved_lifecycle: unresolved_lifecycle(&state),
                        unfinished_cleanup: state.cleanup_active,
                    })
                }
                RuntimeLifecycle::Faulted | RuntimeLifecycle::Ready => {
                    state.lifecycle = RuntimeLifecycle::Draining
                }
                RuntimeLifecycle::Draining => {}
            }
            end_all_subscriptions(&mut state, Error::Closed);
            sample(
                &mut state,
                &self.inner.config,
                DiagnosticKind::Shutdown,
                "draining",
            );
        }
        self.inner.changed.notify_waiters();
        // This task owns close even if the caller drops or times out shutdown.
        spawn_finalizer(self.inner.clone());
        let until = Instant::now() + deadline;
        loop {
            let notified = self.inner.changed.notified();
            {
                let state = self.inner.state.lock().await;
                if state.lifecycle == RuntimeLifecycle::Closed {
                    return Ok(ShutdownReport {
                        closed: true,
                        unresolved: unresolved_appends(&state),
                        unresolved_lifecycle: unresolved_lifecycle(&state),
                        unfinished_cleanup: state.cleanup_active,
                    });
                }
                if state.lifecycle == RuntimeLifecycle::Faulted && state.close_started {
                    return Err(Error::RuntimeFaulted(
                        state
                            .fault
                            .clone()
                            .unwrap_or_else(|| "store close failed".into()),
                    ));
                }
            }
            if tokio::time::timeout_at(until.into(), notified)
                .await
                .is_err()
            {
                let state = self.inner.state.lock().await;
                let report = ShutdownReport {
                    closed: false,
                    unresolved: unresolved_appends(&state),
                    unresolved_lifecycle: unresolved_lifecycle(&state),
                    unfinished_cleanup: state.cleanup_active,
                };
                drop(state);
                return Ok(report);
            }
        }
    }
}

#[async_trait]
impl<S: EventStore> EventSubscription for RuntimeSubscription<S> {
    async fn next(&mut self) -> Option<Result<Arc<Record>>> {
        loop {
            let lifecycle_error = {
                let state = self.inner.state.lock().await;
                lifecycle_name_error(&state, &self.coordinator.key.id)
            };
            if let Some(error) = lifecycle_error {
                self.end(error);
            }
            {
                let mut shared = self
                    .shared
                    .state
                    .lock()
                    .expect("subscription lock poisoned");
                if let Some(error) = shared.ended.clone() {
                    release_subscription_page(&mut shared);
                    if !shared.terminal_delivered {
                        shared.terminal_delivered = true;
                        return Some(Err(error));
                    }
                    return None;
                }
                if let Some(record) = shared.buffer.pop_front() {
                    self.last_delivered = record.cursor.clone();
                    shared.last_delivered = record.cursor.clone();
                    if shared.buffer.is_empty() {
                        release_subscription_page(&mut shared);
                    }
                    return Some(Ok(record));
                }
            }
            if let Some(error) = lag_error(
                &self.shared,
                &self.options,
                self.coordinator_observed_bounds(),
            ) {
                self.end(error);
                continue;
            }
            let generation_before = self
                .coordinator
                .state
                .lock()
                .expect("coordinator lock poisoned")
                .generation;
            let store = self.inner.store.clone();
            let stream = self.coordinator.key.clone();
            let mut bounds_call = Box::pin(tracked_read(self.inner.clone(), 0, async move {
                store.bounds(&stream).await
            }));
            let mut wake_observed = false;
            let bounds_result = loop {
                let wake = self.coordinator.wake.notified();
                tokio::pin!(wake);
                wake.as_mut().enable();
                tokio::select! {
                    result = &mut bounds_call => break result.map(|(bounds, _)| bounds),
                    _ = &mut wake => {
                        if subscription_ended(&self.shared) { break Err(Error::Closed); }
                        wake_observed = true;
                    }
                }
            };
            let bounds = match bounds_result {
                Ok(bounds) => bounds,
                Err(error) => {
                    if !subscription_ended(&self.shared) {
                        self.end(error);
                    }
                    continue;
                }
            };
            let through = if self.last_delivered.offset < self.replay_high {
                self.replay_high
            } else {
                bounds.tail.offset
            };
            if through > self.last_delivered.offset {
                let store = self.inner.store.clone();
                let stream = self.coordinator.key.clone();
                let after = self.last_delivered.offset;
                let limits = self.options.page;
                let mut page_call = Box::pin(tracked_read(
                    self.inner.clone(),
                    limits.max_bytes,
                    async move { store.read_range(&stream, after, through, limits).await },
                ));
                let page_result = loop {
                    tokio::select! {
                        result = &mut page_call => break result,
                        _ = self.coordinator.wake.notified() => {
                            if subscription_ended(&self.shared) { break Err(Error::Closed); }
                        }
                    }
                };
                match page_result {
                    Ok((page, _)) if page.records.is_empty() => {
                        self.end(Error::StoreCorrupt(
                            "empty subscription page below requested through cursor".into(),
                        ));
                    }
                    Ok((page, permit)) => {
                        let mut shared = self
                            .shared
                            .state
                            .lock()
                            .expect("subscription lock poisoned");
                        if shared.ended.is_none() {
                            shared.buffer = page.records.into();
                            if !shared.buffer.is_empty() {
                                shared.page_permit = permit;
                            }
                        }
                    }
                    Err(error) => self.end(error),
                }
                continue;
            }
            if wake_observed {
                continue;
            }
            // Register before the final generation check. A commit either changes
            // generation or wakes this enabled waiter, so the transition to sleep
            // cannot lose a notification.
            let notified = self.coordinator.wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let generation_now = self
                .coordinator
                .state
                .lock()
                .expect("coordinator lock poisoned")
                .generation;
            if generation_now != generation_before || subscription_ended(&self.shared) {
                continue;
            }
            notified.await;
        }
    }

    fn last_delivered(&self) -> &Cursor {
        &self.last_delivered
    }
}

impl<S: EventStore> RuntimeSubscription<S> {
    fn end(&self, error: Error) {
        if end_subscription(&self.shared, error) {
            schedule_subscription_deregistration(
                self.inner.clone(),
                self.coordinator.clone(),
                self.shared.clone(),
            );
        }
    }

    fn coordinator_observed_bounds(&self) -> Bounds {
        let cs = self
            .coordinator
            .state
            .lock()
            .expect("coordinator lock poisoned");
        Bounds {
            floor: Cursor::new(self.coordinator.key.clone(), cs.observed_floor),
            tail: Cursor::new(self.coordinator.key.clone(), cs.observed_tail),
        }
    }
}

impl<S: EventStore> Drop for RuntimeSubscription<S> {
    fn drop(&mut self) {
        let was_registered = {
            let mut shared = self
                .shared
                .state
                .lock()
                .expect("subscription lock poisoned");
            release_subscription_page(&mut shared);
            shared.begin_membership_removal()
        };
        if was_registered {
            schedule_subscription_deregistration(
                self.inner.clone(),
                self.coordinator.clone(),
                self.shared.clone(),
            );
        }
    }
}

async fn tracked_read<S, T, F>(
    inner: Arc<Inner<S>>,
    page_bytes: usize,
    operation: F,
) -> Result<(T, Option<OwnedSemaphorePermit>)>
where
    S: EventStore,
    T: Send + 'static,
    F: Future<Output = Result<T>> + Send + 'static,
{
    let waiter = inner
        .read_waiters
        .clone()
        .try_acquire_owned()
        .map_err(|_| Error::Overloaded)?;
    let until = Instant::now() + inner.config.reads.admission_timeout;
    let (read_permit, page_permit) = tokio::time::timeout_at(until.into(), async {
        {
            let state = inner.state.lock().await;
            ensure_ready(&state)?;
        }
        // Page reads acquire resources in one order and retain each acquired
        // permit. Keeping the same queued acquire future across lifecycle wakes
        // preserves semaphore fairness instead of handing a permit to the next
        // waiter and racing to reacquire it.
        let page_permit = if page_bytes == 0 {
            None
        } else {
            let page = inner
                .page_bytes
                .clone()
                .acquire_many_owned(page_bytes as u32);
            tokio::pin!(page);
            Some(loop {
                let changed = inner.changed.notified();
                tokio::pin!(changed);
                match tokio::select! {
                    permit = &mut page => Some(permit.map_err(|_| Error::Closed)?),
                    _ = &mut changed => None,
                } {
                    Some(permit) => break permit,
                    None => {
                        let state = inner.state.lock().await;
                        ensure_ready(&state)?;
                    }
                }
            })
        };
        let read = inner.read_slots.clone().acquire_owned();
        tokio::pin!(read);
        let read_permit = loop {
            let changed = inner.changed.notified();
            tokio::pin!(changed);
            match tokio::select! {
                permit = &mut read => Some(permit.map_err(|_| Error::Closed)?),
                _ = &mut changed => None,
            } {
                Some(permit) => break permit,
                None => {
                    let state = inner.state.lock().await;
                    ensure_ready(&state)?;
                }
            }
        };
        Ok::<_, Error>((read_permit, page_permit))
    })
    .await
    .map_err(|_| Error::AdmissionTimeout)??;
    {
        let mut state = inner.state.lock().await;
        ensure_ready(&state)?;
        state.active_io += 1;
    }
    drop(waiter);
    let (sender, receiver) = oneshot::channel();
    let completion_inner = inner.clone();
    tokio::spawn(async move {
        let result = match tokio::spawn(operation).await {
            Ok(result) => result,
            Err(join) => Err(Error::RuntimeFaulted(format!(
                "store read task failed: {join}"
            ))),
        };
        drop(read_permit);
        let mut state = completion_inner.state.lock().await;
        state.active_io = state.active_io.saturating_sub(1);
        if let Err(
            error @ (Error::OwnershipLost | Error::StoreCorrupt(_) | Error::RuntimeFaulted(_)),
        ) = &result
        {
            let detail = error.to_string();
            state.fault.get_or_insert(detail);
            if state.lifecycle == RuntimeLifecycle::Ready {
                state.lifecycle = RuntimeLifecycle::Faulted;
            }
            end_all_subscriptions(
                &mut state,
                Error::RuntimeFaulted("store can no longer continue safely".into()),
            );
        }
        drop(state);
        completion_inner.changed.notify_waiters();
        let result = result.map(|value| (value, page_permit));
        let _ = sender.send(result);
    });
    receiver.await.unwrap_or_else(|_| {
        Err(Error::RuntimeFaulted(
            "tracked storage operation stopped without an outcome".into(),
        ))
    })
}

async fn worker<S: EventStore>(inner: Arc<Inner<S>>) {
    loop {
        let notified = inner.changed.notified();
        let coordinator = {
            let mut state = inner.state.lock().await;
            match state.ready.pop_front() {
                Some(key) => state.coordinators.get(&key).cloned(),
                None if matches!(state.lifecycle, RuntimeLifecycle::Closed) => return,
                None => None,
            }
        };
        let Some(coordinator) = coordinator else {
            notified.await;
            continue;
        };
        for _ in 0..inner.config.scheduling.max_appends_per_stream_turn {
            let pending = {
                let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                let pending = cs.queue.pop_front();
                if pending.is_some() {
                    cs.in_flight += 1;
                }
                pending
            };
            let Some(pending) = pending else {
                break;
            };
            let prior_fault = {
                let mut state = inner.state.lock().await;
                state.active_io += 1;
                state
                    .fault
                    .as_ref()
                    .map(|fault| Error::RuntimeFaulted(fault.clone()))
            };
            let result = if let Some(error) = prior_fault {
                Err(error)
            } else {
                let gate = coordinator.write_gate.clone().lock_owned().await;
                let blocked = {
                    let state = inner.state.lock().await;
                    lifecycle_name_error(&state, &coordinator.key.id)
                };
                let result = if let Some(error) = blocked {
                    Err(error)
                } else {
                    let store = inner.store.clone();
                    let stream = coordinator.key.clone();
                    let event = pending.event.clone();
                    match tokio::spawn(async move { store.append_atomic(&stream, event).await })
                        .await
                    {
                        Ok(result) => result,
                        Err(join) => Err(Error::RuntimeFaulted(format!(
                            "store append task failed: {join}"
                        ))),
                    }
                };
                drop(gate);
                result
            };
            let mut resolved_record = None;
            let final_result = if matches!(result, Err(Error::CommitUnknown { .. })) {
                let store = inner.store.clone();
                let stream = coordinator.key.clone();
                let event_id = pending.event.id.clone();
                let lookup =
                    tokio::spawn(async move { store.lookup_event(&stream, &event_id).await }).await;
                match lookup {
                    Ok(Ok(Some(record))) if record.event == pending.event => {
                        resolved_record = Some(record);
                        result
                    }
                    Ok(Ok(Some(_))) => Err(Error::IdempotencyConflict {
                        event_id: pending.event.id.clone(),
                    }),
                    Ok(Ok(None) | Err(_)) | Err(_) => result,
                }
            } else {
                result
            };
            {
                let mut state = inner.state.lock().await;
                state.active_io -= 1;
                state.queued_count -= 1;
                state.queued_bytes -= pending.bytes;
                let committed = {
                    let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                    cs.in_flight -= 1;
                    cs.queued_bytes -= pending.bytes;
                    let committed = match &final_result {
                        Ok(receipt) if receipt.kind == AppendKind::Inserted => {
                            Some(receipt.record.clone())
                        }
                        _ => resolved_record,
                    };
                    if let Some(ref record) = committed {
                        cs.observed_tail = cs.observed_tail.max(record.cursor.offset);
                        cs.generation = cs.generation.wrapping_add(1);
                        for sub in cs.subscriptions.iter().flatten() {
                            let mut ss = sub.state.lock().expect("subscription lock poisoned");
                            if ss.registered
                                && ss.last_delivered.offset < cs.observed_tail
                                && ss.behind_since.is_none()
                            {
                                ss.behind_since = Some(Instant::now());
                            }
                        }
                        coordinator.wake.notify_waiters();
                    }
                    committed
                };
                if !matches!(final_result, Err(Error::CommitUnknown { .. })) || committed.is_some()
                {
                    let unresolved_key = (coordinator.key.clone(), pending.event.id.clone());
                    if let Some(count) = state.unresolved.get_mut(&unresolved_key) {
                        *count -= 1;
                        if *count == 0 {
                            state.unresolved.remove(&unresolved_key);
                        }
                    }
                } else {
                    state.lifecycle = RuntimeLifecycle::Faulted;
                    state.fault = Some("an uncertain commit could not be resolved".into());
                    sample(
                        &mut state,
                        &inner.config,
                        DiagnosticKind::RuntimeFaulted,
                        "commit unknown",
                    );
                    end_all_subscriptions(
                        &mut state,
                        Error::RuntimeFaulted("an uncertain commit could not be resolved".into()),
                    );
                }
                if matches!(
                    final_result,
                    Err(Error::OwnershipLost | Error::StoreCorrupt(_) | Error::RuntimeFaulted(_))
                ) {
                    state.lifecycle = RuntimeLifecycle::Faulted;
                    state.fault = Some(final_result.as_ref().unwrap_err().to_string());
                    sample(
                        &mut state,
                        &inner.config,
                        DiagnosticKind::RuntimeFaulted,
                        "store fault",
                    );
                    end_all_subscriptions(
                        &mut state,
                        Error::RuntimeFaulted("store can no longer continue safely".into()),
                    );
                }
                match &final_result {
                    Ok(r) if r.kind == AppendKind::Inserted => {
                        state.append_inserted = state.append_inserted.saturating_add(1);
                        sample(
                            &mut state,
                            &inner.config,
                            DiagnosticKind::AppendInserted,
                            "inserted",
                        );
                    }
                    Ok(_) => {
                        state.append_deduplicated = state.append_deduplicated.saturating_add(1);
                        sample(
                            &mut state,
                            &inner.config,
                            DiagnosticKind::AppendDeduplicated,
                            "deduplicated",
                        );
                    }
                    Err(_) => {
                        state.append_failed = state.append_failed.saturating_add(1);
                        sample(
                            &mut state,
                            &inner.config,
                            DiagnosticKind::AppendRejected,
                            "append failed",
                        );
                    }
                }
            }
            let _ = pending.result.send(final_result);
            inner.changed.notify_waiters();
        }
        let mut state = inner.state.lock().await;
        let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
        if !cs.queue.is_empty() {
            state.ready.push_back(coordinator.key.clone());
        } else {
            cs.scheduled = false;
        }
        drop(cs);
        cleanup_coordinator(&mut state, &coordinator.key);
        drop(state);
        inner.changed.notify_waiters();
    }
}

fn select_subscriptions_for_sweep(
    state: &mut CoordinatorState,
    budget: usize,
) -> (Vec<Arc<SubscriptionShared>>, usize) {
    if state.subscriptions.is_empty() || budget == 0 {
        return (Vec::new(), 0);
    }
    let visited = budget.min(state.subscriptions.len());
    let start = state.sweep_index.min(state.subscriptions.len() - 1);
    let selected = (0..visited)
        .filter_map(|offset| {
            state.subscriptions[(start + offset) % state.subscriptions.len()].clone()
        })
        .collect();
    state.sweep_index = (start + visited) % state.subscriptions.len();
    (selected, visited)
}

async fn subscription_sweeper<S: EventStore>(inner: Weak<Inner<S>>) {
    let Some(first) = inner.upgrade() else {
        return;
    };
    let interval = first.config.subscriptions.sweep_interval;
    drop(first);
    let mut cursor = 0usize;
    loop {
        tokio::time::sleep(interval).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        let coordinators: Vec<_> = {
            let state = inner.state.lock().await;
            if state.lifecycle == RuntimeLifecycle::Closed {
                return;
            }
            state.coordinators.values().cloned().collect()
        };
        if coordinators.is_empty() {
            continue;
        }
        let mut remaining = inner.config.subscriptions.checks_per_sweep;
        let mut checked_coordinators = 0usize;
        while remaining > 0 && checked_coordinators < coordinators.len() {
            let coordinator = &coordinators[cursor % coordinators.len()];
            cursor = cursor.wrapping_add(1);
            checked_coordinators += 1;
            let (floor, tail, subscriptions) = {
                let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
                let (subscriptions, visited) = select_subscriptions_for_sweep(&mut cs, remaining);
                remaining -= visited;
                (cs.observed_floor, cs.observed_tail, subscriptions)
            };
            let mut expired = Vec::new();
            for subscription in subscriptions {
                let mut ss = subscription
                    .state
                    .lock()
                    .expect("subscription lock poisoned");
                if tail == ss.last_delivered.offset {
                    ss.behind_since = None;
                } else if ss.behind_since.is_none() {
                    ss.behind_since = Some(Instant::now());
                }
                let now = Instant::now();
                let behind = tail.saturating_sub(ss.last_delivered.offset);
                let lag_duration = ss
                    .behind_since
                    .map_or(Duration::ZERO, |since| now.duration_since(since));
                if ss.ended.is_none()
                    && now.duration_since(ss.created) >= ss.catch_up_grace
                    && (behind > ss.max_lag_records || lag_duration > ss.max_lag_duration)
                {
                    let bounds = Bounds {
                        floor: Cursor::new(coordinator.key.clone(), floor),
                        tail: Cursor::new(coordinator.key.clone(), tail),
                    };
                    ss.ended = Some(Error::SubscriberLagged {
                        last_delivered: ss.last_delivered.clone(),
                        bounds: Box::new(bounds),
                    });
                    release_subscription_page(&mut ss);
                    if ss.begin_membership_removal() {
                        expired.push(subscription.clone());
                    }
                    coordinator.wake.notify_waiters();
                }
            }
            if !expired.is_empty() {
                let mut state = inner.state.lock().await;
                for subscription in expired {
                    remove_subscription_membership(&mut state, coordinator, &subscription);
                }
                sample(
                    &mut state,
                    &inner.config,
                    DiagnosticKind::SubscriberEnded,
                    "lagged",
                );
            }
        }
    }
}

fn lag_error(
    shared: &SubscriptionShared,
    options: &SubscriptionOptions,
    bounds: Bounds,
) -> Option<Error> {
    let now = Instant::now();
    let mut ss = shared.state.lock().expect("subscription lock poisoned");
    if bounds.tail.offset <= ss.last_delivered.offset {
        ss.behind_since = None;
        return None;
    }
    let behind = bounds.tail.offset - ss.last_delivered.offset;
    let since = *ss.behind_since.get_or_insert(now);
    if now.duration_since(ss.created) >= options.catch_up_grace
        && (behind > options.max_lag_records
            || now.duration_since(since) > options.max_lag_duration)
    {
        Some(Error::SubscriberLagged {
            last_delivered: ss.last_delivered.clone(),
            bounds: Box::new(bounds),
        })
    } else {
        None
    }
}

fn end_subscription(shared: &SubscriptionShared, error: Error) -> bool {
    let mut ss = shared.state.lock().expect("subscription lock poisoned");
    if ss.ended.is_none() {
        ss.ended = Some(error);
    }
    release_subscription_page(&mut ss);
    ss.begin_membership_removal()
}

fn schedule_subscription_deregistration<S: EventStore>(
    inner: Arc<Inner<S>>,
    coordinator: Arc<Coordinator>,
    shared: Arc<SubscriptionShared>,
) {
    let executor = inner.executor.clone();
    executor.spawn(async move {
        let mut state = inner.state.lock().await;
        remove_subscription_membership(&mut state, &coordinator, &shared);
        inner.changed.notify_waiters();
    });
}

fn remove_subscription_membership(
    state: &mut State,
    coordinator: &Coordinator,
    shared: &Arc<SubscriptionShared>,
) -> bool {
    let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
    let slot = {
        let ss = shared.state.lock().expect("subscription lock poisoned");
        let Some(slot) = ss.membership_index() else {
            return false;
        };
        slot
    };
    assert!(
        cs.subscriptions
            .get(slot)
            .and_then(Option::as_ref)
            .is_some_and(|entry| Arc::ptr_eq(entry, shared)),
        "subscription membership slot must identify its owner"
    );
    let removed = cs.subscriptions[slot]
        .take()
        .expect("validated subscription slot must be occupied");
    debug_assert!(Arc::ptr_eq(&removed, shared));
    {
        let mut ss = shared.state.lock().expect("subscription lock poisoned");
        assert!(ss.remove_membership());
    }
    cs.free_subscription_slots.push(slot);
    if cs.free_subscription_slots.len() == cs.subscriptions.len() {
        cs.subscriptions = Vec::new();
        cs.free_subscription_slots = Vec::new();
        cs.sweep_index = 0;
    }
    drop(cs);
    state.subscriptions = state
        .subscriptions
        .checked_sub(1)
        .expect("indexed membership count must include the removed subscription");
    cleanup_coordinator(state, &coordinator.key);
    true
}

fn release_subscription_page(state: &mut SharedSubscriptionState) {
    state.buffer = VecDeque::new();
    state.page_permit.take();
}

fn subscription_ended(shared: &SubscriptionShared) -> bool {
    shared
        .state
        .lock()
        .expect("subscription lock poisoned")
        .ended
        .is_some()
}

fn register_waiter(state: &mut State, config: &RuntimeConfig, bytes: usize) -> Result<()> {
    if state.waiter_count >= config.appends.max_waiters
        || state
            .waiter_bytes
            .checked_add(bytes)
            .is_none_or(|n| n > config.appends.max_waiter_bytes)
    {
        return Err(Error::Overloaded);
    }
    state.waiter_count += 1;
    state.waiter_bytes += bytes;
    Ok(())
}

fn new_coordinator(key: StreamKey) -> Arc<Coordinator> {
    Arc::new(Coordinator {
        key,
        write_gate: Arc::new(Mutex::new(())),
        state: StdMutex::new(CoordinatorState {
            queue: VecDeque::new(),
            queued_bytes: 0,
            scheduled: false,
            in_flight: 0,
            maintenance_in_flight: 0,
            observed_floor: 0,
            observed_tail: 0,
            generation: 0,
            subscriptions: Vec::new(),
            free_subscription_slots: Vec::new(),
            sweep_index: 0,
            subscription_reservations: 0,
        }),
        wake: Notify::new(),
    })
}

fn lifecycle_name_error(state: &State, id: &StreamId) -> Option<Error> {
    state
        .unresolved_lifecycle
        .get(id)
        .map(|request| Error::LifecycleCommitUnknown {
            operation_id: request.operation_id.clone(),
        })
}

fn lifecycle_receipt_is_valid(request: &LifecycleRequest, receipt: &LifecycleReceipt) -> bool {
    if &receipt.request != request {
        return false;
    }
    match (request.action, &receipt.replacement) {
        (LifecycleAction::Delete, None) => true,
        (LifecycleAction::Reset, Some(replacement)) => {
            replacement.id == request.expected.id
                && replacement.incarnation != request.expected.incarnation
        }
        _ => false,
    }
}

fn release_maintenance_reservation(state: &mut State, bytes: usize) {
    state.maintenance_reserved = state
        .maintenance_reserved
        .checked_sub(1)
        .expect("maintenance reservation count must cover accepted operation");
    state.maintenance_reserved_bytes = state
        .maintenance_reserved_bytes
        .checked_sub(bytes)
        .expect("maintenance byte reservation must cover accepted operation");
}

fn ensure_ready(state: &State) -> Result<()> {
    match state.lifecycle {
        RuntimeLifecycle::Ready => Ok(()),
        RuntimeLifecycle::Faulted => Err(Error::RuntimeFaulted(
            state
                .fault
                .clone()
                .unwrap_or_else(|| "runtime faulted".into()),
        )),
        RuntimeLifecycle::Draining | RuntimeLifecycle::Closed => Err(Error::Closed),
    }
}

fn cleanup_coordinator(state: &mut State, key: &StreamKey) {
    let remove = state.coordinators.get(key).is_some_and(|coordinator| {
        let cs = coordinator.state.lock().expect("coordinator lock poisoned");
        cs.queue.is_empty()
            && cs.in_flight == 0
            && cs.maintenance_in_flight == 0
            && cs.subscriptions.is_empty()
            && cs.subscription_reservations == 0
            && !cs.scheduled
            && !state.unresolved_lifecycle.contains_key(&key.id)
            && !state.lifecycle_in_flight.contains_key(&key.id)
    });
    if remove {
        state.coordinators.remove(key);
    }
}

fn end_coordinator_subscriptions(state: &mut State, coordinator: &Coordinator, error: Error) {
    let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
    let mut ended = 0usize;
    for subscription in cs.subscriptions.iter().flatten() {
        let mut ss = subscription
            .state
            .lock()
            .expect("subscription lock poisoned");
        if ss.ended.is_none() {
            ss.ended = Some(error.clone());
        }
        release_subscription_page(&mut ss);
        if ss.remove_membership() {
            ended += 1;
        }
    }
    cs.subscriptions = Vec::new();
    cs.free_subscription_slots = Vec::new();
    cs.sweep_index = 0;
    coordinator.wake.notify_waiters();
    drop(cs);
    state.subscriptions = state
        .subscriptions
        .checked_sub(ended)
        .expect("global subscription count must include coordinator memberships");
    cleanup_coordinator(state, &coordinator.key);
}

fn end_all_subscriptions(state: &mut State, error: Error) {
    let mut ended = 0usize;
    for coordinator in state.coordinators.values() {
        let mut cs = coordinator.state.lock().expect("coordinator lock poisoned");
        for subscription in cs.subscriptions.iter().flatten() {
            let mut ss = subscription
                .state
                .lock()
                .expect("subscription lock poisoned");
            if ss.ended.is_none() {
                ss.ended = Some(error.clone());
            }
            release_subscription_page(&mut ss);
            if ss.remove_membership() {
                ended += 1;
            }
        }
        cs.subscriptions = Vec::new();
        cs.free_subscription_slots = Vec::new();
        cs.sweep_index = 0;
        coordinator.wake.notify_waiters();
    }
    state.subscriptions = state
        .subscriptions
        .checked_sub(ended)
        .expect("global subscription count must include every indexed membership");
}

fn sample(state: &mut State, config: &RuntimeConfig, kind: DiagnosticKind, detail: &'static str) {
    if state.samples.len() == config.diagnostics.capacity {
        state.samples.pop_front();
    }
    state.samples.push_back(DiagnosticSample { kind, detail });
}

fn unresolved_appends(state: &State) -> Vec<UnresolvedAppend> {
    state
        .unresolved
        .keys()
        .map(|(stream, event_id)| UnresolvedAppend {
            stream: stream.clone(),
            event_id: event_id.clone(),
        })
        .collect()
}

fn unresolved_lifecycle(state: &State) -> Vec<LifecycleRequest> {
    let mut requests: Vec<_> = state.unresolved_lifecycle.values().cloned().collect();
    for request in state.lifecycle_in_flight.values() {
        if !requests.contains(request) {
            requests.push(request.clone());
        }
    }
    requests.sort_by(|left, right| {
        left.operation_id
            .cmp(&right.operation_id)
            .then_with(|| left.expected.id.cmp(&right.expected.id))
    });
    requests
}

fn spawn_finalizer<S: EventStore>(inner: Arc<Inner<S>>) {
    let executor = inner.executor.clone();
    executor.spawn(async move {
        loop {
            let notified = inner.changed.notified();
            let should_close = {
                let mut state = inner.state.lock().await;
                if state.lifecycle == RuntimeLifecycle::Closed || state.close_started {
                    return;
                }
                if state.queued_count == 0 && state.active_io == 0 && state.maintenance_active == 0
                {
                    state.close_started = true;
                    true
                } else {
                    false
                }
            };
            if should_close {
                let store = inner.store.clone();
                let result = match tokio::spawn(async move { store.close().await }).await {
                    Ok(result) => result,
                    Err(join) => Err(Error::RuntimeFaulted(format!(
                        "store close task failed: {join}"
                    ))),
                };
                let mut state = inner.state.lock().await;
                if result.is_ok() {
                    state.lifecycle = RuntimeLifecycle::Closed;
                } else {
                    state.lifecycle = RuntimeLifecycle::Faulted;
                    state.fault = result.err().map(|e| e.to_string());
                }
                inner.changed.notify_waiters();
                return;
            }
            notified.await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::{MemoryStore, MemoryStoreOptions};

    fn test_event(id: &str) -> NewEvent {
        NewEvent {
            id: EventId::new(id).unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("test.bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(b"one"),
        }
    }

    fn test_subscription(start: StartPosition, page: PageLimits) -> SubscriptionOptions {
        SubscriptionOptions {
            start,
            page,
            max_lag_records: 100,
            max_lag_duration: Duration::from_secs(1),
            catch_up_grace: Duration::ZERO,
        }
    }

    #[tokio::test]
    async fn draining_last_record_releases_page_capacity_and_allocation() {
        let config = {
            let mut config = RuntimeConfig::default();
            config.events.max_bytes = 1024;
            config.reads.page.max_records = 4;
            config.reads.page.max_bytes = 4096;
            config.reads.max_buffered_page_bytes = 4096;
            config
        };
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config.clone())
            .await
            .unwrap();
        let key = runtime
            .create_stream(&StreamId::new("drained-page").unwrap())
            .await
            .unwrap();
        for number in 0..4 {
            runtime
                .append(&key, test_event(&format!("event-{number}")))
                .await
                .unwrap();
        }
        let mut subscription = runtime
            .subscribe(
                &key,
                test_subscription(
                    StartPosition::Beginning,
                    PageLimits {
                        max_records: 4,
                        max_bytes: 4096,
                    },
                ),
            )
            .await
            .unwrap();

        for _ in 0..3 {
            subscription.next().await.unwrap().unwrap();
        }
        {
            let state = subscription.shared.state.lock().unwrap();
            assert_eq!(state.buffer.len(), 1);
            assert!(state.buffer.capacity() > 0);
            assert!(state.page_permit.is_some());
        }
        assert_eq!(runtime.inner.page_bytes.available_permits(), 0);

        subscription.next().await.unwrap().unwrap();
        let state = subscription.shared.state.lock().unwrap();
        assert!(state.buffer.is_empty());
        assert_eq!(state.buffer.capacity(), 0);
        assert!(state.page_permit.is_none());
        assert_eq!(
            runtime.inner.page_bytes.available_permits(),
            config.reads.max_buffered_page_bytes
        );
    }

    #[derive(Clone)]
    struct CompleteEmptyOptions {
        reads: Arc<AtomicUsize>,
    }

    struct CompleteEmptyStore {
        key: StreamKey,
        reads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl EventStore for CompleteEmptyStore {
        type Options = CompleteEmptyOptions;

        async fn open(options: Self::Options) -> Result<Self> {
            Ok(Self {
                key: StreamKey {
                    id: StreamId::new("complete-empty").unwrap(),
                    incarnation: IncarnationId([9; 16]),
                },
                reads: options.reads,
            })
        }

        fn capabilities(&self) -> StoreCapabilities {
            StoreCapabilities {
                persistence: PersistenceProfile::Ephemeral,
                format_version: 1,
                max_record_bytes: 1024 * 1024,
                max_concurrent_reads: 1,
                max_concurrent_writes: 1,
                ownership: "complete-empty test store",
            }
        }

        async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
            if id == &self.key.id {
                Ok(self.key.clone())
            } else {
                Err(Error::StreamNotFound)
            }
        }

        async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
            Ok((id == &self.key.id).then(|| self.key.clone()))
        }

        async fn append_atomic(&self, _: &StreamKey, _: NewEvent) -> Result<AppendReceipt> {
            Err(Error::StoreWriteFailed("read-only test store".into()))
        }

        async fn lookup_event(&self, _: &StreamKey, _: &EventId) -> Result<Option<Arc<Record>>> {
            Ok(None)
        }

        async fn bounds(&self, _: &StreamKey) -> Result<Bounds> {
            Ok(Bounds {
                floor: Cursor::new(self.key.clone(), 0),
                tail: Cursor::new(self.key.clone(), 1),
            })
        }

        async fn read_range(
            &self,
            _: &StreamKey,
            after: u64,
            through: u64,
            _: PageLimits,
        ) -> Result<Page> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let records = Vec::with_capacity(256);
            Ok(Page {
                records,
                next_after: Cursor::new(self.key.clone(), after),
                through: Cursor::new(self.key.clone(), through),
                complete: true,
            })
        }

        async fn close(&self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn complete_empty_page_is_terminal_and_releases_subscription_quota() {
        let reads = Arc::new(AtomicUsize::new(0));
        let config = {
            let mut config = RuntimeConfig::default();
            config.events.max_bytes = 1024;
            config.subscriptions.max_total = 1;
            config.subscriptions.max_per_stream = 1;
            config.reads.page.max_bytes = 4096;
            config.reads.max_buffered_page_bytes = 4096;
            config
        };
        let runtime = Runtime::<CompleteEmptyStore>::open(
            CompleteEmptyOptions {
                reads: reads.clone(),
            },
            config.clone(),
        )
        .await
        .unwrap();
        let key = runtime
            .create_stream(&StreamId::new("complete-empty").unwrap())
            .await
            .unwrap();
        let options = test_subscription(
            StartPosition::Beginning,
            PageLimits {
                max_records: 4,
                max_bytes: 4096,
            },
        );
        let mut first = runtime.subscribe(&key, options.clone()).await.unwrap();

        let error = tokio::time::timeout(Duration::from_millis(250), first.next())
            .await
            .expect("empty page must not cause a retry loop")
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(error, Error::StoreCorrupt(message) if message.contains("empty subscription page"))
        );
        assert!(first.next().await.is_none());
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        {
            let state = first.shared.state.lock().unwrap();
            assert!(!state.registered);
            assert_eq!(state.buffer.capacity(), 0);
            assert!(state.page_permit.is_none());
        }
        assert_eq!(
            runtime.inner.page_bytes.available_permits(),
            config.reads.max_buffered_page_bytes
        );

        let second = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match runtime.subscribe(&key, options.clone()).await {
                    Ok(subscription) => break subscription,
                    Err(Error::Overloaded) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected second subscription error: {error}"),
                }
            }
        })
        .await
        .expect("terminal subscription must release its admission slot");
        assert_eq!(reads.load(Ordering::Relaxed), 1);
        drop(second);
        drop(first);
    }

    #[tokio::test]
    async fn ending_subscription_releases_page_capacity_and_allocation() {
        let page_bytes = Arc::new(Semaphore::new(32));
        let permit = page_bytes.clone().acquire_many_owned(16).await.unwrap();
        let stream = StreamKey {
            id: StreamId::new("terminal-capacity").unwrap(),
            incarnation: IncarnationId([1; 16]),
        };
        let mut buffer = VecDeque::with_capacity(32);
        buffer.push_back(Arc::new(Record {
            cursor: Cursor::new(stream.clone(), 1),
            event: NewEvent {
                id: EventId::new("one").unwrap(),
                schema: SchemaRef {
                    id: SchemaId::new("test.bytes").unwrap(),
                    version: 1,
                },
                payload: Payload::copy_from_slice(b"one"),
            },
        }));
        let shared = SubscriptionShared {
            state: StdMutex::new(SharedSubscriptionState {
                last_delivered: Cursor::new(stream, 0),
                created: Instant::now(),
                behind_since: None,
                ended: None,
                terminal_delivered: false,
                registered: true,
                membership_slot: NonZeroUsize::new(1),
                buffer,
                page_permit: Some(permit),
                max_lag_records: 1,
                max_lag_duration: Duration::from_secs(1),
                catch_up_grace: Duration::ZERO,
            }),
        };

        assert!(end_subscription(&shared, Error::Closed));

        let state = shared.state.lock().unwrap();
        assert_eq!(state.buffer.capacity(), 0);
        assert!(state.page_permit.is_none());
        assert!(!state.registered);
        assert!(matches!(state.ended, Some(Error::Closed)));
        assert_eq!(page_bytes.available_permits(), 32);
    }

    fn assert_indexed_membership(coordinator: &Coordinator) {
        let cs = coordinator.state.lock().unwrap();
        let free = cs
            .free_subscription_slots
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(free.len(), cs.free_subscription_slots.len());
        assert_eq!(
            cs.subscriptions.iter().flatten().count() + free.len(),
            cs.subscriptions.len()
        );
        for index in &free {
            assert!(cs.subscriptions[*index].is_none());
        }
        for (index, subscription) in cs
            .subscriptions
            .iter()
            .enumerate()
            .filter_map(|(index, subscription)| subscription.as_ref().map(|value| (index, value)))
        {
            let state = subscription.state.lock().unwrap();
            assert_eq!(state.membership_index(), Some(index));
            assert!(state.registered);
        }
        assert!(
            cs.subscriptions.is_empty() || cs.sweep_index < cs.subscriptions.len(),
            "sweep index must remain inside a nonempty membership vector"
        );
    }

    fn sweep_test_subscription(index: usize) -> Arc<SubscriptionShared> {
        let stream = StreamKey {
            id: StreamId::new("sweep-selection").unwrap(),
            incarnation: IncarnationId([1; 16]),
        };
        Arc::new(SubscriptionShared {
            state: StdMutex::new(SharedSubscriptionState {
                last_delivered: Cursor::new(stream, index as u64),
                created: Instant::now(),
                behind_since: None,
                ended: None,
                terminal_delivered: false,
                registered: true,
                membership_slot: NonZeroUsize::new(index + 1),
                buffer: VecDeque::new(),
                page_permit: None,
                max_lag_records: 1,
                max_lag_duration: Duration::from_secs(1),
                catch_up_grace: Duration::ZERO,
            }),
        })
    }

    #[test]
    fn stable_sweep_cursor_visits_survivors_during_slot_zero_churn() {
        const COUNT: usize = 128;
        const BATCH: usize = 64;
        let mut state = CoordinatorState {
            queue: VecDeque::new(),
            queued_bytes: 0,
            scheduled: false,
            in_flight: 0,
            maintenance_in_flight: 0,
            observed_floor: 0,
            observed_tail: 0,
            generation: 0,
            subscriptions: (0..COUNT)
                .map(|index| Some(sweep_test_subscription(index)))
                .collect(),
            free_subscription_slots: Vec::new(),
            sweep_index: 0,
            subscription_reservations: 0,
        };
        let expected = state
            .subscriptions
            .iter()
            .skip(1)
            .flatten()
            .map(|subscription| Arc::as_ptr(subscription) as usize)
            .collect::<std::collections::HashSet<_>>();
        let mut visited = std::collections::HashSet::new();
        for turn in 0..3 {
            let (selected, physical_slots) = select_subscriptions_for_sweep(&mut state, BATCH);
            assert_eq!(physical_slots, BATCH);
            visited.extend(
                selected
                    .iter()
                    .map(|subscription| Arc::as_ptr(subscription) as usize),
            );
            state.subscriptions[0] = Some(sweep_test_subscription(COUNT + turn));
        }
        assert!(expected.is_subset(&visited));
    }

    #[tokio::test]
    async fn indexed_membership_removal_reuses_the_stable_slot() {
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), {
            let mut config = RuntimeConfig::default();
            config.events.max_bytes = 1024;
            config.subscriptions.max_total = 4;
            config.subscriptions.max_per_stream = 4;
            config
        })
        .await
        .unwrap();
        let key = runtime
            .create_stream(&StreamId::new("indexed-membership").unwrap())
            .await
            .unwrap();
        let mut subscriptions = Vec::new();
        for _ in 0..4 {
            subscriptions.push(
                runtime
                    .subscribe(
                        &key,
                        test_subscription(
                            StartPosition::Future,
                            PageLimits {
                                max_records: 4,
                                max_bytes: 4096,
                            },
                        ),
                    )
                    .await
                    .unwrap(),
            );
        }
        let coordinator = subscriptions[0].coordinator.clone();
        assert_indexed_membership(&coordinator);

        let unmoved = subscriptions[3].shared.clone();
        drop(subscriptions.remove(1));
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.diagnostics().await.active_subscriptions != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_indexed_membership(&coordinator);
        assert_eq!(unmoved.state.lock().unwrap().membership_index(), Some(3));

        let replacement = runtime
            .subscribe(
                &key,
                test_subscription(
                    StartPosition::Future,
                    PageLimits {
                        max_records: 4,
                        max_bytes: 4096,
                    },
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            replacement.shared.state.lock().unwrap().membership_index(),
            Some(1)
        );
        subscriptions.push(replacement);

        drop(subscriptions.remove(0));
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.diagnostics().await.active_subscriptions != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_indexed_membership(&coordinator);
        assert!(
            runtime
                .shutdown(Duration::from_secs(1))
                .await
                .unwrap()
                .closed
        );
        for subscription in &subscriptions {
            let state = subscription.shared.state.lock().unwrap();
            assert!(!state.registered);
            assert!(state.membership_slot.is_none());
        }
    }

    #[tokio::test]
    async fn indexed_membership_matches_reference_set_across_generated_removals() {
        const COUNT: usize = 32;
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), {
            let mut config = RuntimeConfig::default();
            config.events.max_bytes = 1024;
            config.subscriptions.max_total = COUNT;
            config.subscriptions.max_per_stream = COUNT;
            config
        })
        .await
        .unwrap();
        let key = runtime
            .create_stream(&StreamId::new("membership-model").unwrap())
            .await
            .unwrap();
        let mut handles = Vec::new();
        for _ in 0..COUNT {
            handles.push(Some(
                runtime
                    .subscribe(
                        &key,
                        test_subscription(
                            StartPosition::Future,
                            PageLimits {
                                max_records: 4,
                                max_bytes: 4096,
                            },
                        ),
                    )
                    .await
                    .unwrap(),
            ));
        }
        let coordinator = handles[0].as_ref().unwrap().coordinator.clone();
        let mut remaining = COUNT;
        let mut seed = 0x5eed_u64;
        while remaining > 1 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let mut index = (seed as usize) % COUNT;
            while handles[index].is_none() {
                index = (index + 1) % COUNT;
            }
            drop(handles[index].take());
            remaining -= 1;
            tokio::time::timeout(Duration::from_secs(1), async {
                while runtime.diagnostics().await.active_subscriptions != remaining {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_indexed_membership(&coordinator);
            let expected = handles
                .iter()
                .flatten()
                .map(|subscription| Arc::as_ptr(&subscription.shared) as usize)
                .collect::<std::collections::HashSet<_>>();
            let actual = coordinator
                .state
                .lock()
                .unwrap()
                .subscriptions
                .iter()
                .flatten()
                .map(|subscription| Arc::as_ptr(subscription) as usize)
                .collect::<std::collections::HashSet<_>>();
            assert_eq!(actual, expected);
        }
        assert!(
            runtime
                .shutdown(Duration::from_secs(1))
                .await
                .unwrap()
                .closed
        );
    }

    #[tokio::test]
    async fn shutdown_owns_live_and_pending_membership_exactly_once() {
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), {
            let mut config = RuntimeConfig::default();
            config.events.max_bytes = 1024;
            config.subscriptions.max_total = 2;
            config.subscriptions.max_per_stream = 2;
            config
        })
        .await
        .unwrap();
        let key = runtime
            .create_stream(&StreamId::new("membership-shutdown").unwrap())
            .await
            .unwrap();
        let pending = runtime
            .subscribe(
                &key,
                test_subscription(
                    StartPosition::Future,
                    PageLimits {
                        max_records: 4,
                        max_bytes: 4096,
                    },
                ),
            )
            .await
            .unwrap();
        let live = runtime
            .subscribe(
                &key,
                test_subscription(
                    StartPosition::Future,
                    PageLimits {
                        max_records: 4,
                        max_bytes: 4096,
                    },
                ),
            )
            .await
            .unwrap();
        assert!(pending
            .shared
            .state
            .lock()
            .unwrap()
            .begin_membership_removal());

        assert!(
            runtime
                .shutdown(Duration::from_secs(1))
                .await
                .unwrap()
                .closed
        );
        assert_eq!(runtime.diagnostics().await.active_subscriptions, 0);
        for subscription in [&pending, &live] {
            let state = subscription.shared.state.lock().unwrap();
            assert!(!state.registered);
            assert!(state.membership_slot.is_none());
        }
        drop(pending);
        drop(live);
        tokio::task::yield_now().await;
        assert_eq!(runtime.diagnostics().await.active_subscriptions, 0);
    }

    #[cfg(feature = "source-journal")]
    #[tokio::test]
    async fn journal_permit_keeps_its_queue_position_across_unrelated_notifications() {
        let config = RuntimeConfig {
            journal: crate::application::JournalAdmissionConfig {
                max_concurrent: 1,
                ..crate::application::JournalAdmissionConfig::default()
            },
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
            .await
            .unwrap();
        let held = runtime
            .inner
            .journal_slots
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let (order_tx, mut order_rx) = tokio::sync::mpsc::unbounded_channel();
        let (release_first_tx, release_first_rx) = oneshot::channel();
        let first_inner = runtime.inner.clone();
        let first = tokio::spawn(async move {
            let permit = journal_permit(
                first_inner.clone(),
                first_inner.journal_slots.clone(),
                1,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
            order_tx.send(1).unwrap();
            let _ = release_first_rx.await;
            drop(permit);
        });
        tokio::task::yield_now().await;
        let (second_tx, mut second_rx) = oneshot::channel();
        let second_inner = runtime.inner.clone();
        let second = tokio::spawn(async move {
            let permit = journal_permit(
                second_inner.clone(),
                second_inner.journal_slots.clone(),
                1,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
            .unwrap();
            second_tx.send(()).unwrap();
            drop(permit);
        });
        for _ in 0..64 {
            runtime.inner.changed.notify_waiters();
            tokio::task::yield_now().await;
        }
        drop(held);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), order_rx.recv())
                .await
                .unwrap(),
            Some(1)
        );
        assert!(second_rx.try_recv().is_err());
        release_first_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), second_rx)
            .await
            .unwrap()
            .unwrap();
        first.await.unwrap();
        second.await.unwrap();
        let _ = runtime.shutdown(Duration::from_secs(1)).await;
    }

    #[cfg(feature = "source-journal")]
    #[tokio::test]
    async fn cancelled_and_closed_journal_waiters_release_all_permits() {
        let config = RuntimeConfig {
            journal: crate::application::JournalAdmissionConfig {
                max_concurrent: 1,
                ..crate::application::JournalAdmissionConfig::default()
            },
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
            .await
            .unwrap();
        let held = runtime
            .inner
            .journal_slots
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let cancelled_inner = runtime.inner.clone();
        let cancelled = tokio::spawn(async move {
            journal_permit(
                cancelled_inner.clone(),
                cancelled_inner.journal_slots.clone(),
                1,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        drop(held);
        let permit = runtime
            .inner
            .journal_slots
            .clone()
            .try_acquire_owned()
            .expect("cancelled waiter releases its queue reservation");
        drop(permit);

        let held = runtime
            .inner
            .journal_slots
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let closed_inner = runtime.inner.clone();
        let closed = tokio::spawn(async move {
            journal_permit(
                closed_inner.clone(),
                closed_inner.journal_slots.clone(),
                1,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await
        });
        tokio::task::yield_now().await;
        assert!(
            runtime
                .shutdown(Duration::from_secs(1))
                .await
                .unwrap()
                .closed
        );
        assert!(matches!(closed.await.unwrap(), Err(JournalError::Closed)));
        drop(held);
        assert_eq!(runtime.inner.journal_slots.available_permits(), 1);
    }
}
