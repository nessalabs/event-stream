use super::{MonotonicTick, RetentionStore, SnapshotStore};
use crate::domain::*;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{oneshot, Notify, Semaphore};

pub type ReplicationResult<T> = std::result::Result<T, ReplicationError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaMode {
    Required,
    DetachedNeedsBootstrap,
    Bootstrapping,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaStart {
    /// Protect and transfer complete retained history from offset zero.
    FromBeginning,
    /// Attach without claiming an existing prefix is replayable.
    NeedsBootstrap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaStatus {
    pub replica: ReplicaId,
    pub stream: OriginStream,
    pub destination_epoch: DestinationEpoch,
    pub acknowledged: ReplicaPosition,
    pub backlog_records: usize,
    pub backlog_bytes: u64,
    /// Persisted wall-clock observation for the oldest unacknowledged record.
    pub oldest_backlog_at: Option<DurableTimestampMillis>,
    pub mode: ReplicaMode,
    pub pending_batch: Option<BatchId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachReplica {
    pub operation_id: ReplicationOperationId,
    pub replica: ReplicaId,
    pub stream: OriginStream,
    pub destination_epoch: DestinationEpoch,
    pub max_backlog_bytes: u64,
    pub max_backlog_age: Duration,
    pub start: ReplicaStart,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttachReplicaReceipt {
    pub request: AttachReplica,
    pub status: ReplicaStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetachReplica {
    pub operation_id: ReplicationOperationId,
    pub replica: ReplicaId,
    pub stream: OriginStream,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetachReplicaReceipt {
    pub request: DetachReplica,
    pub status: ReplicaStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcknowledgeReplicaBatch {
    pub operation_id: ReplicationOperationId,
    pub replica: ReplicaId,
    pub expected_after: ReplicaPosition,
    pub receipt: ReplicaReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcknowledgeReplicaBatchReceipt {
    pub request: AcknowledgeReplicaBatch,
    pub status: ReplicaStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaBatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareReplicaBatch {
    pub operation_id: ReplicationOperationId,
    pub batch_id: BatchId,
    pub replica: ReplicaId,
    pub stream: OriginStream,
    pub expected_after: ReplicaPosition,
    pub limits: ReplicaBatchLimits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareReplicaBatchReceipt {
    pub request: PrepareReplicaBatch,
    pub batch: Option<ReplicaBatch>,
    pub status: ReplicaStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceReplicaReceiptFloor {
    pub operation_id: ReplicationOperationId,
    pub stream: OriginStream,
    pub destination_epoch: DestinationEpoch,
    pub expected_floor: ReplicaPosition,
    pub new_floor: ReplicaPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceReplicaReceiptFloorReceipt {
    pub request: AdvanceReplicaReceiptFloor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrap {
    pub operation_id: ReplicationOperationId,
    pub id: BootstrapId,
    pub replica: ReplicaId,
    pub destination_epoch: DestinationEpoch,
    pub stream: OriginStream,
    pub snapshot: SnapshotDescriptor,
    pub through: ReplicaPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapChunk {
    pub id: BootstrapId,
    pub chunk: SnapshotChunk,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapBatch {
    pub id: BootstrapId,
    pub batch: ReplicaBatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapBatchReceipt {
    pub id: BootstrapId,
    pub committed_through: ReplicaPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapReceipt {
    pub request: ReplicaBootstrap,
    pub committed_through: ReplicaPosition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaBootstrapState {
    Staging,
    Verified,
    Published,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginReplicaBootstrapReceipt {
    pub request: ReplicaBootstrap,
    pub state: ReplicaBootstrapState,
    pub accepted_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapChunkReceipt {
    pub id: BootstrapId,
    pub offset: u64,
    pub end: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapVerificationLimits {
    pub max_chunks: usize,
    pub max_records: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifyReplicaBootstrap {
    pub id: BootstrapId,
    pub limits: ReplicaBootstrapVerificationLimits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapVerificationProgress {
    pub id: BootstrapId,
    pub verified_snapshot_bytes: u64,
    pub verified_suffix_records: usize,
    pub verified_suffix_bytes: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishReplicaBootstrap {
    pub operation_id: ReplicationOperationId,
    pub id: BootstrapId,
    pub destination_epoch: DestinationEpoch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedReplicaBootstrap {
    pub request: ReplicaBootstrap,
    pub committed_through: ReplicaPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapBytePage {
    pub id: BootstrapId,
    pub offset: u64,
    pub bytes: Payload,
    pub next_offset: u64,
    pub complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaBootstrapReadPlan {
    pub lease: ReplicaReadLeaseId,
    pub published: PublishedReplicaBootstrap,
    pub expires_at: DurableTimestampMillis,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaReadRelease {
    Released,
    AlreadyReleased,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginOriginBootstrap {
    pub operation_id: ReplicationOperationId,
    pub bootstrap_id: BootstrapId,
    pub destination_operation_id: ReplicationOperationId,
    pub replica: ReplicaId,
    pub stream: OriginStream,
    pub destination_epoch: DestinationEpoch,
    pub snapshot: SnapshotDescriptor,
    pub captured_tail: ReplicaPosition,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeginOriginBootstrapReceipt {
    pub request: BeginOriginBootstrap,
    pub status: ReplicaStatus,
    /// The origin's snapshot and suffix protection is valid before this exclusive deadline.
    pub protection_expires_at: MonotonicTick,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcknowledgeOriginBootstrap {
    pub operation_id: ReplicationOperationId,
    pub replica: ReplicaId,
    pub stream: OriginStream,
    pub receipt: ReplicaBootstrapReceipt,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcknowledgeOriginBootstrapReceipt {
    pub request: AcknowledgeOriginBootstrap,
    pub status: ReplicaStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortReplicaBootstrapReceipt {
    pub request: AbortReplicaBootstrap,
    pub state: ReplicaBootstrapState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AbortReplicaBootstrap {
    pub operation_id: ReplicationOperationId,
    pub id: BootstrapId,
    pub destination_epoch: DestinationEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaCleanupLimits {
    pub max_receipt_rows: usize,
    pub max_staging_rows: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaCleanupProgress {
    pub removed_receipt_rows: usize,
    pub removed_staging_rows: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}

#[derive(Clone, Debug)]
pub struct ReplicationStoreConfig {
    pub attachments: ReplicaAttachmentLimits,
    pub batches: ReplicaPendingBatchLimits,
    pub receipts: ReplicaReceiptLimits,
    pub cleanup: ReplicaCleanupLimits,
}

#[derive(Clone, Debug)]
pub struct ReplicaAttachmentLimits {
    pub max_replicas: usize,
    pub max_replicas_per_stream: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicaPendingBatchLimits {
    pub max_pending_batches: usize,
    pub max_pending_batch_bytes: usize,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicaReceiptLimits {
    pub max_operation_receipts: usize,
    pub max_operation_receipt_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicaDestinationConfig {
    pub storage: ReplicaDestinationStorageLimits,
    pub receipts: ReplicaDestinationReceiptLimits,
    pub staging: ReplicaDestinationStagingLimits,
    pub reads: ReplicaDestinationReadLimits,
    pub cleanup: ReplicaCleanupLimits,
}

#[derive(Clone, Debug)]
pub struct ReplicaDestinationStorageLimits {
    pub max_origin_streams: usize,
    pub max_history_records: usize,
    pub max_history_bytes: u64,
    pub max_published_bootstraps: usize,
    pub max_published_snapshot_bytes: u64,
    pub max_batch_records: usize,
    pub max_batch_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicaDestinationReceiptLimits {
    pub max_batch_receipts: usize,
    pub max_batch_receipt_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicaDestinationStagingLimits {
    pub max_staging_bootstraps: usize,
    pub max_staging_chunks: usize,
    pub max_chunk_bytes: usize,
    pub max_staging_bytes: u64,
    pub max_staging_records: usize,
    pub max_staging_record_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct ReplicaDestinationReadLimits {
    pub max_leases: usize,
    pub max_lifetime: Duration,
    pub max_bytes_per_page: usize,
}

impl Default for ReplicationStoreConfig {
    fn default() -> Self {
        Self {
            attachments: ReplicaAttachmentLimits {
                max_replicas: 1024,
                max_replicas_per_stream: 16,
            },
            batches: ReplicaPendingBatchLimits {
                max_pending_batches: 1024,
                max_pending_batch_bytes: 64 * 1024 * 1024,
                max_batch_records: 256,
                max_batch_bytes: 2 * 1024 * 1024,
            },
            receipts: ReplicaReceiptLimits {
                max_operation_receipts: 4096,
                max_operation_receipt_bytes: 4 * 1024 * 1024,
            },
            cleanup: ReplicaCleanupLimits {
                max_receipt_rows: 256,
                max_staging_rows: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl Default for ReplicaDestinationConfig {
    fn default() -> Self {
        Self {
            storage: ReplicaDestinationStorageLimits {
                max_origin_streams: 10_000,
                max_history_records: 100_000,
                max_history_bytes: 1024 * 1024 * 1024,
                max_published_bootstraps: 1024,
                max_published_snapshot_bytes: 1024 * 1024 * 1024,
                max_batch_records: 256,
                max_batch_bytes: 2 * 1024 * 1024,
            },
            receipts: ReplicaDestinationReceiptLimits {
                max_batch_receipts: 4096,
                max_batch_receipt_bytes: 4 * 1024 * 1024,
            },
            staging: ReplicaDestinationStagingLimits {
                max_staging_bootstraps: 16,
                max_staging_chunks: 1024 * 1024,
                max_chunk_bytes: 64 * 1024,
                max_staging_bytes: 256 * 1024 * 1024,
                max_staging_records: 100_000,
                max_staging_record_bytes: 1024 * 1024 * 1024,
            },
            reads: ReplicaDestinationReadLimits {
                max_leases: 64,
                max_lifetime: Duration::from_secs(5 * 60),
                max_bytes_per_page: 64 * 1024,
            },
            cleanup: ReplicaCleanupLimits {
                max_receipt_rows: 256,
                max_staging_rows: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl ReplicationStoreConfig {
    pub fn validate(&self) -> ReplicationResult<()> {
        if self.attachments.max_replicas == 0
            || self.attachments.max_replicas_per_stream == 0
            || self.batches.max_pending_batches == 0
            || self.batches.max_pending_batch_bytes == 0
            || self.batches.max_batch_records == 0
            || self.batches.max_batch_bytes == 0
            || self.receipts.max_operation_receipts == 0
            || self.receipts.max_operation_receipt_bytes == 0
            || self.cleanup.max_receipt_rows == 0
            || self.cleanup.max_staging_rows == 0
            || self.cleanup.max_bytes == 0
        {
            return Err(ReplicationError::InvalidConfig(
                "origin replication limits must be nonzero and finite".into(),
            ));
        }
        Ok(())
    }
}

impl ReplicaDestinationConfig {
    pub fn validate(&self) -> ReplicationResult<()> {
        if self.storage.max_origin_streams == 0
            || self.storage.max_history_records == 0
            || self.storage.max_history_bytes == 0
            || self.storage.max_published_bootstraps == 0
            || self.storage.max_published_snapshot_bytes == 0
            || self.storage.max_batch_records == 0
            || self.storage.max_batch_bytes == 0
            || self.receipts.max_batch_receipts == 0
            || self.receipts.max_batch_receipt_bytes == 0
            || self.staging.max_staging_bootstraps == 0
            || self.staging.max_staging_chunks == 0
            || self.staging.max_chunk_bytes == 0
            || self.staging.max_staging_bytes == 0
            || self.staging.max_staging_records == 0
            || self.staging.max_staging_record_bytes == 0
            || self.reads.max_leases == 0
            || self.reads.max_lifetime.is_zero()
            || self.reads.max_lifetime.as_millis() > u128::from(u64::MAX)
            || self.reads.max_bytes_per_page == 0
            || self.cleanup.max_receipt_rows == 0
            || self.cleanup.max_staging_rows == 0
            || self.cleanup.max_bytes == 0
        {
            return Err(ReplicationError::InvalidConfig(
                "destination replication limits must be nonzero and finite".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicationError {
    InvalidConfig(String),
    InvalidInput(String),
    NotFound {
        replica: ReplicaId,
    },
    StaleProgress {
        current: Box<ReplicaPosition>,
    },
    DestinationReplaced {
        current: DestinationEpoch,
    },
    ClockRollback {
        last: DurableTimestampMillis,
        observed: DurableTimestampMillis,
    },
    NeedsBootstrap,
    BacklogExceeded {
        limit_bytes: u64,
    },
    BatchConflict {
        batch: BatchId,
    },
    ReceiptExpired,
    InvalidReceipt(String),
    CapacityExceeded,
    Overloaded,
    AdmissionTimeout,
    Closed,
    CorruptStorage(String),
    StorageFailure(String),
    CommitUnknown(Box<ReplicaBatch>),
    AttachUnknown(Box<AttachReplica>),
    PrepareUnknown(Box<PrepareReplicaBatch>),
    AcknowledgeUnknown(Box<AcknowledgeReplicaBatch>),
    DetachUnknown(Box<DetachReplica>),
    ReceiptFloorUnknown(Box<AdvanceReplicaReceiptFloor>),
    BootstrapUnknown(Box<ReplicaBootstrap>),
    BootstrapBatchUnknown(Box<ReplicaBootstrapBatch>),
    BootstrapPublishUnknown(Box<PublishReplicaBootstrap>),
    BootstrapAbortUnknown(Box<AbortReplicaBootstrap>),
    ReadLeaseExpired {
        lease: ReplicaReadLeaseId,
    },
    BootstrapProtectionExpired {
        id: BootstrapId,
    },
    DriveUnknown {
        prepare: Box<PrepareReplicaBatch>,
        acknowledge_operation_id: ReplicationOperationId,
    },
    BootstrapDriveUnknown(Box<BeginOriginBootstrap>),
}

impl std::fmt::Display for ReplicationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommitUnknown(batch) => write!(
                formatter,
                "replica batch result unknown for {} records after offset {}",
                batch.records.len(),
                batch.after.offset
            ),
            Self::AcknowledgeUnknown(request) => write!(
                formatter,
                "replica acknowledgement result unknown for {} after offset {}",
                request.replica.as_str(),
                request.expected_after.offset
            ),
            Self::BootstrapUnknown(request) => write!(
                formatter,
                "replica bootstrap result unknown for {} through offset {}",
                request.replica.as_str(),
                request.through.offset
            ),
            other => write!(formatter, "{other:?}"),
        }
    }
}

impl std::error::Error for ReplicationError {}

/// Origin-side state. Implementations update replay protection and acknowledged progress under
/// the same ownership boundary as retention and local append backlog accounting.
#[async_trait]
pub trait ReplicationOriginStore: RetentionStore {
    async fn origin_identity(&self) -> ReplicationResult<OriginId>;
    async fn attach_replica(
        &self,
        request: AttachReplica,
    ) -> ReplicationResult<AttachReplicaReceipt>;
    async fn replica_status(
        &self,
        replica: &ReplicaId,
        stream: &OriginStream,
    ) -> ReplicationResult<ReplicaStatus>;
    async fn prepare_replica_batch(
        &self,
        request: PrepareReplicaBatch,
    ) -> ReplicationResult<PrepareReplicaBatchReceipt>;
    async fn acknowledge_replica_batch(
        &self,
        request: AcknowledgeReplicaBatch,
    ) -> ReplicationResult<AcknowledgeReplicaBatchReceipt>;
    async fn detach_replica(
        &self,
        request: DetachReplica,
    ) -> ReplicationResult<DetachReplicaReceipt>;
}

#[async_trait]
pub trait ReplicationStore: ReplicationOriginStore + SnapshotStore {
    async fn begin_origin_bootstrap(
        &self,
        request: BeginOriginBootstrap,
    ) -> ReplicationResult<BeginOriginBootstrapReceipt>;
    async fn acknowledge_origin_bootstrap(
        &self,
        request: AcknowledgeOriginBootstrap,
    ) -> ReplicationResult<AcknowledgeOriginBootstrapReceipt>;
    async fn cleanup_replication(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress>;
}

/// Destination-side atomic records-plus-receipt publication.
#[async_trait]
pub trait ReplicaBatchDestinationStore: Send + Sync + 'static {
    async fn destination_epoch(&self) -> ReplicationResult<DestinationEpoch>;
    async fn commit_replica_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt>;
    async fn read_replica_after(
        &self,
        after: &ReplicaPosition,
        limits: ReplicaBatchLimits,
    ) -> ReplicationResult<ReplicaPage>;
    async fn advance_replica_receipt_floor(
        &self,
        request: AdvanceReplicaReceiptFloor,
    ) -> ReplicationResult<AdvanceReplicaReceiptFloorReceipt>;
    async fn close_replica_destination(&self) -> ReplicationResult<()>;
}

#[async_trait]
pub trait ReplicaDestinationStore: ReplicaBatchDestinationStore {
    async fn begin_replica_bootstrap(
        &self,
        request: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt>;
    async fn put_replica_bootstrap_chunk(
        &self,
        chunk: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt>;
    async fn put_replica_bootstrap_batch(
        &self,
        batch: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt>;
    async fn verify_replica_bootstrap_step(
        &self,
        request: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress>;
    async fn publish_replica_bootstrap(
        &self,
        request: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt>;
    async fn published_replica_bootstrap(
        &self,
        stream: &OriginStream,
    ) -> ReplicationResult<Option<PublishedReplicaBootstrap>>;
    async fn acquire_replica_bootstrap_read(
        &self,
        stream: &OriginStream,
        lifetime: Duration,
    ) -> ReplicationResult<ReplicaBootstrapReadPlan>;
    async fn read_replica_bootstrap_bytes(
        &self,
        lease: ReplicaReadLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> ReplicationResult<ReplicaBootstrapBytePage>;
    async fn release_replica_bootstrap_read(
        &self,
        lease: ReplicaReadLeaseId,
    ) -> ReplicationResult<ReplicaReadRelease>;
    async fn abort_replica_bootstrap(
        &self,
        request: AbortReplicaBootstrap,
    ) -> ReplicationResult<AbortReplicaBootstrapReceipt>;
    async fn cleanup_replica_destination(
        &self,
        limits: ReplicaCleanupLimits,
    ) -> ReplicationResult<ReplicaCleanupProgress>;
}

pub trait DurableReplicationClock: std::fmt::Debug + Send + Sync + 'static {
    fn now(&self) -> DurableTimestampMillis;
}

/// The application injects an authenticated transport. The library retains the request until the
/// returned future finishes or transfers ownership to another bounded task.
#[async_trait]
pub trait ReplicaTransport: Send + Sync + 'static {
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt>;
}

#[async_trait]
impl<D> ReplicaTransport for D
where
    D: ReplicaBatchDestinationStore,
{
    async fn send_batch(&self, batch: ReplicaBatch) -> ReplicationResult<ReplicaReceipt> {
        self.commit_replica_batch(batch).await
    }
}

#[async_trait]
pub trait ReplicaBootstrapTransport: Send + Sync + 'static {
    async fn begin_bootstrap(
        &self,
        request: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt>;
    async fn put_bootstrap_chunk(
        &self,
        chunk: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt>;
    async fn put_bootstrap_batch(
        &self,
        batch: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt>;
    async fn verify_bootstrap(
        &self,
        request: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress>;
    async fn publish_bootstrap(
        &self,
        request: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt>;
}

#[async_trait]
impl<D> ReplicaBootstrapTransport for D
where
    D: ReplicaDestinationStore,
{
    async fn begin_bootstrap(
        &self,
        request: ReplicaBootstrap,
    ) -> ReplicationResult<BeginReplicaBootstrapReceipt> {
        self.begin_replica_bootstrap(request).await
    }

    async fn put_bootstrap_chunk(
        &self,
        chunk: ReplicaBootstrapChunk,
    ) -> ReplicationResult<ReplicaBootstrapChunkReceipt> {
        self.put_replica_bootstrap_chunk(chunk).await
    }

    async fn put_bootstrap_batch(
        &self,
        batch: ReplicaBootstrapBatch,
    ) -> ReplicationResult<ReplicaBootstrapBatchReceipt> {
        self.put_replica_bootstrap_batch(batch).await
    }

    async fn verify_bootstrap(
        &self,
        request: VerifyReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapVerificationProgress> {
        self.verify_replica_bootstrap_step(request).await
    }

    async fn publish_bootstrap(
        &self,
        request: PublishReplicaBootstrap,
    ) -> ReplicationResult<ReplicaBootstrapReceipt> {
        self.publish_replica_bootstrap(request).await
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationBootstrapDriveLimits {
    pub recovery_lifetime: Duration,
    pub snapshot_page_bytes: usize,
    pub suffix_page: PageLimits,
    pub verification: ReplicaBootstrapVerificationLimits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationBootstrapDriveReceipt {
    pub origin: BeginOriginBootstrapReceipt,
    pub destination: ReplicaBootstrapReceipt,
    pub acknowledged: AcknowledgeOriginBootstrapReceipt,
}

#[derive(Clone, Debug)]
pub struct ReplicationDriverConfig {
    pub max_concurrent: usize,
    pub max_in_flight_bytes: usize,
}

/// Aggregate admission limits for replication work owned by [`Runtime`](super::Runtime).
/// Runtime admission is immediate, so this boundary does not retain a waiter queue.
#[derive(Clone, Debug)]
pub struct RuntimeReplicationConfig {
    pub max_concurrent: usize,
    pub max_in_flight_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct ReplicationRetryPolicy {
    pub max_attempts: usize,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub deadline: Duration,
}

impl ReplicationRetryPolicy {
    pub fn validate(&self) -> ReplicationResult<()> {
        if self.max_attempts == 0
            || self.initial_delay.is_zero()
            || self.max_delay < self.initial_delay
            || self.deadline.is_zero()
        {
            return Err(ReplicationError::InvalidConfig(
                "replication retry limits must be nonzero and ordered".into(),
            ));
        }
        Ok(())
    }
}

impl Default for ReplicationDriverConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 16,
            max_in_flight_bytes: 32 * 1024 * 1024,
        }
    }
}

impl Default for RuntimeReplicationConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_in_flight_bytes: 8 * 1024 * 1024,
        }
    }
}

impl ReplicationDriverConfig {
    pub fn validate(&self) -> ReplicationResult<()> {
        if self.max_concurrent == 0
            || self.max_concurrent > Semaphore::MAX_PERMITS
            || self.max_in_flight_bytes == 0
            || self.max_in_flight_bytes > u32::MAX as usize
        {
            return Err(ReplicationError::InvalidConfig(
                "replication driver limits must be nonzero and supported by admission control"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl RuntimeReplicationConfig {
    pub fn validate(&self) -> ReplicationResult<()> {
        if self.max_concurrent == 0
            || self.max_concurrent > Semaphore::MAX_PERMITS
            || self.max_in_flight_bytes == 0
            || self.max_in_flight_bytes > u32::MAX as usize
        {
            return Err(ReplicationError::InvalidConfig(
                "runtime replication limits must be nonzero and supported by admission control"
                    .into(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn replication_drive_charge(
    prepare: &PrepareReplicaBatch,
    acknowledge_operation_id: &ReplicationOperationId,
) -> ReplicationResult<u32> {
    let identifier_bytes = prepare
        .operation_id
        .as_str()
        .len()
        .checked_add(prepare.replica.as_str().len())
        .and_then(|bytes| bytes.checked_add(prepare.stream.stream.id.as_str().len()))
        .and_then(|bytes| bytes.checked_mul(8))
        .and_then(|bytes| {
            bytes.checked_add(acknowledge_operation_id.as_str().len().saturating_mul(4))
        })
        .ok_or(ReplicationError::CapacityExceeded)?;
    let charge = prepare
        .limits
        .max_records
        .checked_mul(std::mem::size_of::<Arc<Record>>() * 4)
        .and_then(|bytes| bytes.checked_add(prepare.limits.max_bytes))
        .and_then(|bytes| bytes.checked_add(identifier_bytes))
        .and_then(|bytes| bytes.checked_add(1024))
        .ok_or(ReplicationError::CapacityExceeded)?;
    u32::try_from(charge).map_err(|_| {
        ReplicationError::InvalidInput("replication drive byte limit is unsupported".into())
    })
}

pub(crate) fn replication_bootstrap_drive_charge(
    begin: &BeginOriginBootstrap,
    publish_operation_id: &ReplicationOperationId,
    acknowledge_operation_id: &ReplicationOperationId,
    limits: &ReplicationBootstrapDriveLimits,
) -> ReplicationResult<u32> {
    const IDENTIFIER_COPY_BOUND: usize = 8;
    const BEGIN_STRUCT_COPY_BOUND: usize = 8;
    const RESULT_STRUCT_COPY_BOUND: usize = 2;

    let buffer_charge = limits
        .snapshot_page_bytes
        .checked_add(limits.suffix_page.max_bytes)
        .and_then(|bytes| {
            limits
                .suffix_page
                .max_records
                .checked_mul(std::mem::size_of::<Arc<Record>>() * 3)
                .and_then(|metadata| bytes.checked_add(metadata))
        })
        .ok_or(ReplicationError::CapacityExceeded)?;
    // These are separate Box<str> allocations even when validation requires
    // the cursor streams to identify the same logical stream. The copy bound
    // covers the input, unknown-result fallback, origin and destination
    // requests in flight, and the nested requests retained by final receipts.
    let identifier_bytes = [
        begin.operation_id.as_str().len(),
        begin.destination_operation_id.as_str().len(),
        begin.replica.as_str().len(),
        begin.stream.stream.id.as_str().len(),
        begin.snapshot.covered.stream.id.as_str().len(),
        begin.snapshot.schema.id.as_str().len(),
        begin.captured_tail.stream.stream.id.as_str().len(),
        publish_operation_id.as_str().len(),
        acknowledge_operation_id.as_str().len(),
    ]
    .into_iter()
    .try_fold(0usize, |total, length| total.checked_add(length))
    .ok_or(ReplicationError::CapacityExceeded)?
    .checked_mul(IDENTIFIER_COPY_BOUND)
    .ok_or(ReplicationError::CapacityExceeded)?;
    // Eight Begin-sized shallow structures bound the original/fallback, the
    // origin request, destination request and their receipt-owned copies. Two
    // result/error structures cover the value awaiting oneshot delivery and
    // its receiver fallback. The fixed remainder covers the task, permits,
    // channel and Arc handles; page and identifier allocations are charged
    // separately above.
    let structure_bytes = std::mem::size_of::<BeginOriginBootstrap>()
        .checked_mul(BEGIN_STRUCT_COPY_BOUND)
        .and_then(|bytes| {
            std::mem::size_of::<ReplicationBootstrapDriveReceipt>()
                .checked_mul(RESULT_STRUCT_COPY_BOUND)
                .and_then(|result| bytes.checked_add(result))
        })
        .and_then(|bytes| {
            std::mem::size_of::<ReplicationError>()
                .checked_mul(2)
                .and_then(|errors| bytes.checked_add(errors))
        })
        .and_then(|bytes| bytes.checked_add(2048))
        .ok_or(ReplicationError::CapacityExceeded)?;
    let charge = buffer_charge
        .checked_add(identifier_bytes)
        .and_then(|bytes| bytes.checked_add(structure_bytes))
        .ok_or(ReplicationError::CapacityExceeded)?;
    u32::try_from(charge)
        .map_err(|_| ReplicationError::InvalidInput("bootstrap drive charge is unsupported".into()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationDriveReceipt {
    pub prepared: PrepareReplicaBatchReceipt,
    pub remote: Option<ReplicaReceipt>,
    pub acknowledged: Option<AcknowledgeReplicaBatchReceipt>,
}

/// Drives one bounded prepared batch through an injected transport. Once admitted, an owned task
/// keeps the batch and permits until the transport and exact origin acknowledgement finish.
#[derive(Debug)]
pub struct ReplicationDriver<O, T> {
    origin: Arc<O>,
    transport: Arc<T>,
    concurrent: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    closed: Arc<AtomicBool>,
    active: Arc<std::sync::atomic::AtomicUsize>,
    idle: Arc<Notify>,
}

impl<O, T> Clone for ReplicationDriver<O, T> {
    fn clone(&self) -> Self {
        Self {
            origin: self.origin.clone(),
            transport: self.transport.clone(),
            concurrent: self.concurrent.clone(),
            bytes: self.bytes.clone(),
            closed: self.closed.clone(),
            active: self.active.clone(),
            idle: self.idle.clone(),
        }
    }
}

impl<O, T> ReplicationDriver<O, T>
where
    O: ReplicationOriginStore,
    T: ReplicaTransport,
{
    pub fn open(
        origin: Arc<O>,
        transport: Arc<T>,
        config: ReplicationDriverConfig,
    ) -> ReplicationResult<Self> {
        config.validate()?;
        Ok(Self {
            origin,
            transport,
            concurrent: Arc::new(Semaphore::new(config.max_concurrent)),
            bytes: Arc::new(Semaphore::new(config.max_in_flight_bytes)),
            closed: Arc::new(AtomicBool::new(false)),
            active: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            idle: Arc::new(Notify::new()),
        })
    }

    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        self.concurrent.close();
        self.bytes.close();
    }

    pub async fn wait_closed(&self) {
        self.close();
        loop {
            let notified = self.idle.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    pub async fn replicate_once(
        &self,
        prepare: PrepareReplicaBatch,
        acknowledge_operation_id: ReplicationOperationId,
    ) -> ReplicationResult<ReplicationDriveReceipt> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ReplicationError::Closed);
        }
        if prepare.limits.max_records == 0 || prepare.limits.max_bytes == 0 {
            return Err(ReplicationError::InvalidInput(
                "replication drive limits must be nonzero".into(),
            ));
        }
        let bytes = replication_drive_charge(&prepare, &acknowledge_operation_id)?;
        let concurrent = self
            .concurrent
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReplicationError::Overloaded)?;
        let byte_permit = self
            .bytes
            .clone()
            .try_acquire_many_owned(bytes)
            .map_err(|_| ReplicationError::Overloaded)?;
        // Register before the final close check. Sequential consistency across the
        // two atomics prevents shutdown from seeing zero while this task sees open.
        self.active.fetch_add(1, Ordering::SeqCst);
        let active = ActiveReplicationDrive {
            active: self.active.clone(),
            idle: self.idle.clone(),
        };
        if self.closed.load(Ordering::SeqCst) {
            return Err(ReplicationError::Closed);
        }
        let origin = self.origin.clone();
        let transport = self.transport.clone();
        let fallback = ReplicationError::DriveUnknown {
            prepare: Box::new(prepare.clone()),
            acknowledge_operation_id: acknowledge_operation_id.clone(),
        };
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = drive_once(
                origin.as_ref(),
                transport.as_ref(),
                prepare,
                acknowledge_operation_id,
            )
            .await;
            let _owned_permits = (concurrent, byte_permit);
            let _active = active;
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(fallback))
    }

    pub async fn replicate_with_retry(
        &self,
        prepare: PrepareReplicaBatch,
        acknowledge_operation_id: ReplicationOperationId,
        policy: ReplicationRetryPolicy,
    ) -> ReplicationResult<ReplicationDriveReceipt> {
        policy.validate()?;
        let deadline = tokio::time::Instant::now()
            .checked_add(policy.deadline)
            .ok_or_else(|| ReplicationError::InvalidConfig("retry deadline overflows".into()))?;
        let fallback = || ReplicationError::DriveUnknown {
            prepare: Box::new(prepare.clone()),
            acknowledge_operation_id: acknowledge_operation_id.clone(),
        };
        let mut delay = policy.initial_delay;
        for attempt in 0..policy.max_attempts {
            let result = tokio::time::timeout_at(
                deadline,
                self.replicate_once(prepare.clone(), acknowledge_operation_id.clone()),
            )
            .await
            .unwrap_or_else(|_| Err(fallback()));
            match result {
                Ok(receipt) => return Ok(receipt),
                Err(error)
                    if attempt + 1 < policy.max_attempts && retryable_replication(&error) =>
                {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(error);
                    }
                    if tokio::time::timeout_at(deadline, tokio::time::sleep(delay))
                        .await
                        .is_err()
                    {
                        return Err(fallback());
                    }
                    delay = delay.saturating_mul(2).min(policy.max_delay);
                }
                Err(error) => return Err(error),
            }
        }
        Err(fallback())
    }
}

fn retryable_replication(error: &ReplicationError) -> bool {
    matches!(
        error,
        ReplicationError::CommitUnknown(_)
            | ReplicationError::PrepareUnknown(_)
            | ReplicationError::AcknowledgeUnknown(_)
            | ReplicationError::DriveUnknown { .. }
            | ReplicationError::StorageFailure(_)
    )
}

impl<O, T> ReplicationDriver<O, T>
where
    O: ReplicationStore,
    T: ReplicaTransport + ReplicaBootstrapTransport,
{
    pub async fn bootstrap_once(
        &self,
        begin: BeginOriginBootstrap,
        publish_operation_id: ReplicationOperationId,
        acknowledge_operation_id: ReplicationOperationId,
        limits: ReplicationBootstrapDriveLimits,
    ) -> ReplicationResult<ReplicationBootstrapDriveReceipt> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ReplicationError::Closed);
        }
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
        let byte_permits = replication_bootstrap_drive_charge(
            &begin,
            &publish_operation_id,
            &acknowledge_operation_id,
            &limits,
        )?;
        let concurrent = self
            .concurrent
            .clone()
            .try_acquire_owned()
            .map_err(|_| ReplicationError::Overloaded)?;
        let bytes = self
            .bytes
            .clone()
            .try_acquire_many_owned(byte_permits)
            .map_err(|_| ReplicationError::Overloaded)?;
        // Register before the final close check. Sequential consistency across the
        // two atomics prevents shutdown from seeing zero while this task sees open.
        self.active.fetch_add(1, Ordering::SeqCst);
        let active = ActiveReplicationDrive {
            active: self.active.clone(),
            idle: self.idle.clone(),
        };
        if self.closed.load(Ordering::SeqCst) {
            return Err(ReplicationError::Closed);
        }
        let origin = self.origin.clone();
        let transport = self.transport.clone();
        let fallback = ReplicationError::BootstrapDriveUnknown(Box::new(begin.clone()));
        let (sender, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = drive_bootstrap(
                origin.as_ref(),
                transport.as_ref(),
                begin,
                publish_operation_id,
                acknowledge_operation_id,
                limits,
            )
            .await;
            let _owned_permits = (concurrent, bytes);
            let _active = active;
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(fallback))
    }
}

struct ActiveReplicationDrive {
    active: Arc<std::sync::atomic::AtomicUsize>,
    idle: Arc<Notify>,
}

impl Drop for ActiveReplicationDrive {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.idle.notify_waiters();
    }
}

async fn drive_once<O, T>(
    origin: &O,
    transport: &T,
    prepare: PrepareReplicaBatch,
    acknowledge_operation_id: ReplicationOperationId,
) -> ReplicationResult<ReplicationDriveReceipt>
where
    O: ReplicationOriginStore,
    T: ReplicaTransport,
{
    let prepared = origin.prepare_replica_batch(prepare).await?;
    let Some(batch) = prepared.batch.clone() else {
        return Ok(ReplicationDriveReceipt {
            prepared,
            remote: None,
            acknowledged: None,
        });
    };
    let receipt = transport.send_batch(batch.clone()).await?;
    let expected_offset = batch
        .records
        .last()
        .map_or(batch.after.offset, |record| record.cursor.offset);
    if receipt.batch != batch.id
        || receipt.destination_epoch != batch.destination_epoch
        || receipt.committed_through.stream != batch.after.stream
        || receipt.committed_through.offset != expected_offset
    {
        return Err(ReplicationError::InvalidReceipt(
            "transport receipt does not match the exact prepared batch".into(),
        ));
    }
    let acknowledged = origin
        .acknowledge_replica_batch(AcknowledgeReplicaBatch {
            operation_id: acknowledge_operation_id,
            replica: prepared.request.replica.clone(),
            expected_after: prepared.request.expected_after.clone(),
            receipt: receipt.clone(),
        })
        .await?;
    Ok(ReplicationDriveReceipt {
        prepared,
        remote: Some(receipt),
        acknowledged: Some(acknowledged),
    })
}

async fn drive_bootstrap<O, T>(
    origin: &O,
    transport: &T,
    begin: BeginOriginBootstrap,
    publish_operation_id: ReplicationOperationId,
    acknowledge_operation_id: ReplicationOperationId,
    limits: ReplicationBootstrapDriveLimits,
) -> ReplicationResult<ReplicationBootstrapDriveReceipt>
where
    O: ReplicationStore,
    T: ReplicaBootstrapTransport,
{
    let begun = origin.begin_origin_bootstrap(begin.clone()).await?;
    let destination_request = ReplicaBootstrap {
        operation_id: begin.destination_operation_id.clone(),
        id: begin.bootstrap_id,
        replica: begin.replica.clone(),
        destination_epoch: begin.destination_epoch,
        stream: begin.stream.clone(),
        snapshot: begin.snapshot.clone(),
        through: begin.captured_tail.clone(),
    };
    let destination_state = transport
        .begin_bootstrap(destination_request.clone())
        .await?;
    if destination_state.request != destination_request {
        return Err(ReplicationError::InvalidReceipt(
            "bootstrap begin receipt does not match the requested transfer".into(),
        ));
    }
    // Once verified, retry publication and acknowledgement directly. Re-uploading would
    // require history that may already have been reclaimed after a successful acknowledgement.
    if matches!(
        destination_state.state,
        ReplicaBootstrapState::Verified | ReplicaBootstrapState::Published
    ) {
        let destination = transport
            .publish_bootstrap(PublishReplicaBootstrap {
                operation_id: publish_operation_id,
                id: begin.bootstrap_id,
                destination_epoch: begin.destination_epoch,
            })
            .await?;
        let acknowledged = origin
            .acknowledge_origin_bootstrap(AcknowledgeOriginBootstrap {
                operation_id: acknowledge_operation_id,
                replica: begin.replica,
                stream: begin.stream,
                receipt: destination.clone(),
            })
            .await?;
        return Ok(ReplicationBootstrapDriveReceipt {
            origin: begun,
            destination,
            acknowledged,
        });
    }
    let recovery = origin
        .acquire_recovery(begin.snapshot.id, limits.recovery_lifetime)
        .await
        .map_err(|error| ReplicationError::StorageFailure(error.to_string()))?;
    let transfer = async {
        let mut offset = 0u64;
        while offset < begin.snapshot.content_bytes {
            let page = origin
                .read_snapshot_chunk(recovery.lease, offset, limits.snapshot_page_bytes)
                .await
                .map_err(|error| ReplicationError::StorageFailure(error.to_string()))?;
            if page.next_offset <= offset || page.next_offset > begin.snapshot.content_bytes {
                return Err(ReplicationError::CorruptStorage(
                    "snapshot reader made invalid bootstrap progress".into(),
                ));
            }
            transport
                .put_bootstrap_chunk(ReplicaBootstrapChunk {
                    id: begin.bootstrap_id,
                    chunk: SnapshotChunk {
                        offset,
                        bytes: page.bytes,
                    },
                })
                .await?;
            offset = page.next_offset;
        }
        let mut after = begin.snapshot.covered.offset;
        while after < begin.captured_tail.offset {
            let page = origin
                .read_recovery_page(recovery.lease, after, limits.suffix_page)
                .await
                .map_err(|error| ReplicationError::StorageFailure(error.to_string()))?;
            let records: Vec<_> = page
                .records
                .into_iter()
                .take_while(|record| record.cursor.offset <= begin.captured_tail.offset)
                .collect();
            let Some(last_offset) = records.last().map(|record| record.cursor.offset) else {
                return Err(ReplicationError::CorruptStorage(
                    "origin recovery ended before the captured bootstrap tail".into(),
                ));
            };
            let mut hasher = Sha256::new();
            hasher.update(begin.bootstrap_id.0);
            hasher.update(after.to_be_bytes());
            let digest: [u8; 32] = hasher.finalize().into();
            let mut batch_id = [0u8; 16];
            batch_id.copy_from_slice(&digest[..16]);
            transport
                .put_bootstrap_batch(ReplicaBootstrapBatch {
                    id: begin.bootstrap_id,
                    batch: ReplicaBatch {
                        id: BatchId(batch_id),
                        destination_epoch: begin.destination_epoch,
                        after: ReplicaPosition {
                            stream: begin.stream.clone(),
                            offset: after,
                        },
                        records,
                    },
                })
                .await?;
            after = last_offset;
        }
        let mut prior = None;
        loop {
            let progress = transport
                .verify_bootstrap(VerifyReplicaBootstrap {
                    id: begin.bootstrap_id,
                    limits: limits.verification,
                })
                .await?;
            if progress.complete {
                break;
            }
            let current = (
                progress.verified_snapshot_bytes,
                progress.verified_suffix_records,
            );
            if prior == Some(current) {
                return Err(ReplicationError::CorruptStorage(
                    "bootstrap verification made no bounded progress".into(),
                ));
            }
            prior = Some(current);
        }
        transport
            .publish_bootstrap(PublishReplicaBootstrap {
                operation_id: publish_operation_id,
                id: begin.bootstrap_id,
                destination_epoch: begin.destination_epoch,
            })
            .await
    }
    .await;
    let release = origin.release_recovery(recovery.lease).await;
    let destination = transfer?;
    release.map_err(|error| ReplicationError::StorageFailure(error.to_string()))?;
    let acknowledged = origin
        .acknowledge_origin_bootstrap(AcknowledgeOriginBootstrap {
            operation_id: acknowledge_operation_id,
            replica: begin.replica,
            stream: begin.stream,
            receipt: destination.clone(),
        })
        .await?;
    Ok(ReplicationBootstrapDriveReceipt {
        origin: begun,
        destination,
        acknowledged,
    })
}
