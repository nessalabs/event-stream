use super::EventStore;
use crate::domain::*;
use async_trait::async_trait;
use std::{fmt::Debug, time::Duration};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MonotonicTick(pub u64);

impl MonotonicTick {
    /// Adds a duration to this owner-relative nanosecond tick.
    pub fn checked_add(self, duration: Duration) -> Option<Self> {
        let nanos = u64::try_from(duration.as_nanos()).ok()?;
        self.0.checked_add(nanos).map(Self)
    }
}

pub trait MonotonicClock: Debug + Send + Sync + 'static {
    fn now(&self) -> MonotonicTick;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VerificationLimits {
    pub max_chunks: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct SnapshotPage {
    pub entries: Vec<SnapshotDescriptor>,
    pub next_after: Option<SnapshotContinuation>,
    pub complete: bool,
}

#[derive(Clone, Debug)]
pub struct SnapshotUploadPage {
    pub entries: Vec<SnapshotUploadProgress>,
    pub next_after: Option<SnapshotId>,
    pub complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotBytePage {
    pub snapshot: SnapshotId,
    pub offset: u64,
    pub bytes: Payload,
    pub next_offset: u64,
    pub complete: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotAbortReceipt {
    pub id: SnapshotId,
    pub already_aborted: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCleanupLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCleanupProgress {
    pub removed_snapshots: usize,
    pub removed_chunks: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryRelease {
    Released,
    AlreadyReleased,
}

#[derive(Clone, Debug)]
pub struct SnapshotAdmissionConfig {
    pub max_concurrent: usize,
    pub max_waiters: usize,
    pub max_waiter_bytes: usize,
    pub max_in_flight_chunk_bytes: usize,
    pub admission_timeout: Duration,
}

impl Default for SnapshotAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 8,
            max_waiters: 64,
            max_waiter_bytes: 16 * 1024 * 1024,
            max_in_flight_chunk_bytes: 8 * 1024 * 1024,
            admission_timeout: Duration::from_secs(5),
        }
    }
}

impl SnapshotAdmissionConfig {
    pub fn validate(&self) -> SnapshotResult<()> {
        if self.max_concurrent == 0
            || self.max_waiters == 0
            || self.max_waiter_bytes == 0
            || self.max_in_flight_chunk_bytes == 0
            || self.admission_timeout.is_zero()
            || self.max_waiter_bytes > u32::MAX as usize
            || self.max_in_flight_chunk_bytes > u32::MAX as usize
        {
            return Err(SnapshotError::InvalidConfig(
                "snapshot admission limits must be finite".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SnapshotStoreConfig {
    pub storage: SnapshotStorageConfig,
    pub verification: SnapshotVerificationConfig,
    pub recovery: SnapshotRecoveryConfig,
    pub cleanup: SnapshotCleanupLimits,
}

#[derive(Clone, Debug)]
pub struct SnapshotStorageConfig {
    pub max_snapshots: usize,
    pub max_published_bytes: u64,
    pub max_staging_snapshots: usize,
    pub max_staging_bytes: u64,
    pub max_chunk_bytes: usize,
    pub max_chunks: usize,
    pub max_chunk_metadata_bytes: usize,
    pub max_descriptor_metadata_bytes: usize,
    pub max_receipts: usize,
    pub max_receipt_bytes: usize,
    pub max_list_page: PageLimits,
}

#[derive(Clone, Debug)]
pub struct SnapshotVerificationConfig {
    pub max_active: usize,
    pub max_chunks_per_step: usize,
    pub max_bytes_per_step: usize,
}

#[derive(Clone, Debug)]
pub struct SnapshotRecoveryConfig {
    pub max_leases: usize,
    pub max_lifetime: Duration,
    pub max_chunk_bytes: usize,
    pub max_page: PageLimits,
}

impl Default for SnapshotStorageConfig {
    fn default() -> Self {
        Self {
            max_snapshots: 1024,
            max_published_bytes: 1024 * 1024 * 1024,
            max_staging_snapshots: 16,
            max_staging_bytes: 256 * 1024 * 1024,
            max_chunk_bytes: 64 * 1024,
            max_chunks: 1024 * 1024,
            max_chunk_metadata_bytes: 64 * 1024 * 1024,
            max_descriptor_metadata_bytes: 4 * 1024 * 1024,
            max_receipts: 4096,
            max_receipt_bytes: 4 * 1024 * 1024,
            max_list_page: PageLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl Default for SnapshotVerificationConfig {
    fn default() -> Self {
        Self {
            max_active: 4,
            max_chunks_per_step: 256,
            max_bytes_per_step: 2 * 1024 * 1024,
        }
    }
}

impl Default for SnapshotRecoveryConfig {
    fn default() -> Self {
        Self {
            max_leases: 64,
            max_lifetime: Duration::from_secs(5 * 60),
            max_chunk_bytes: 64 * 1024,
            max_page: PageLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl Default for SnapshotStoreConfig {
    fn default() -> Self {
        Self {
            storage: SnapshotStorageConfig::default(),
            verification: SnapshotVerificationConfig::default(),
            recovery: SnapshotRecoveryConfig::default(),
            cleanup: SnapshotCleanupLimits {
                max_rows: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl SnapshotStoreConfig {
    pub fn validate(&self) -> SnapshotResult<()> {
        let maximum_chunk = u64::try_from(self.storage.max_chunk_bytes).map_err(|_| {
            SnapshotError::InvalidConfig("snapshot chunk limit exceeds u64 range".into())
        })?;
        let cleanup_chunk_charge = self
            .storage
            .max_chunk_bytes
            .checked_add(SNAPSHOT_CHUNK_ENVELOPE_BYTES)
            .ok_or_else(|| {
                SnapshotError::InvalidConfig("snapshot cleanup charge overflows".into())
            })?;
        if self.storage.max_snapshots == 0
            || self.storage.max_published_bytes == 0
            || self.storage.max_staging_snapshots == 0
            || self.storage.max_staging_bytes == 0
            || self.storage.max_chunk_bytes == 0
            || self.storage.max_chunks == 0
            || self.storage.max_chunk_metadata_bytes == 0
            || self.storage.max_descriptor_metadata_bytes == 0
            || self.storage.max_receipts == 0
            || self.storage.max_receipt_bytes == 0
            || self.storage.max_list_page.max_records == 0
            || self.storage.max_list_page.max_bytes == 0
            || self.verification.max_active == 0
            || self.verification.max_chunks_per_step == 0
            || self.verification.max_bytes_per_step == 0
            || self.recovery.max_leases == 0
            || self.recovery.max_lifetime.is_zero()
            || self.recovery.max_lifetime.as_nanos() > u128::from(u64::MAX)
            || self.recovery.max_chunk_bytes == 0
            || self.recovery.max_page.max_records == 0
            || self.recovery.max_page.max_bytes == 0
            || self.cleanup.max_rows == 0
            || self.cleanup.max_bytes < cleanup_chunk_charge
            || maximum_chunk > self.storage.max_staging_bytes
        {
            return Err(SnapshotError::InvalidConfig(
                "snapshot store limits must be finite and internally compatible".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    InvalidConfig(String),
    InvalidInput(String),
    Unsupported,
    NotFound {
        id: SnapshotId,
    },
    Closed,
    Overloaded,
    AdmissionTimeout,
    StaleIncarnation {
        current: Option<Box<StreamKey>>,
    },
    CursorAhead {
        tail: Box<Cursor>,
    },
    MissingHistory {
        floor: Box<Cursor>,
    },
    OperationConflict {
        id: SnapshotId,
    },
    CapacityExceeded,
    IncompleteUpload {
        id: SnapshotId,
    },
    ChecksumMismatch {
        id: SnapshotId,
    },
    ExpiredProtection {
        lease: RecoveryLeaseId,
    },
    StorageFailure(String),
    CorruptStorage(String),
    BeginUnknown {
        descriptor: Box<SnapshotDescriptor>,
    },
    ChunkUnknown {
        id: SnapshotId,
        chunk: Box<SnapshotChunk>,
    },
    AbortUnknown {
        id: SnapshotId,
    },
    PublicationUnknown {
        descriptor: Box<SnapshotDescriptor>,
    },
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for SnapshotError {}

pub type SnapshotResult<T> = std::result::Result<T, SnapshotError>;

#[async_trait]
pub trait SnapshotStore: EventStore {
    async fn begin_snapshot(
        &self,
        descriptor: SnapshotDescriptor,
    ) -> SnapshotResult<SnapshotUploadProgress>;
    async fn put_snapshot_chunk(
        &self,
        id: SnapshotId,
        chunk: SnapshotChunk,
    ) -> SnapshotResult<SnapshotUploadProgress>;
    async fn snapshot_status(&self, id: SnapshotId) -> SnapshotResult<SnapshotUploadProgress>;
    async fn verify_snapshot_step(
        &self,
        id: SnapshotId,
        limits: VerificationLimits,
    ) -> SnapshotResult<SnapshotUploadProgress>;
    async fn publish_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotDescriptor>;
    async fn abort_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotAbortReceipt>;
    async fn cleanup_snapshot_staging(
        &self,
        limits: SnapshotCleanupLimits,
    ) -> SnapshotResult<SnapshotCleanupProgress>;
    async fn list_snapshot_uploads(
        &self,
        after: Option<SnapshotId>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage>;
    async fn list_snapshots(
        &self,
        stream: &StreamKey,
        after: Option<SnapshotContinuation>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotPage>;
    async fn acquire_recovery(
        &self,
        id: SnapshotId,
        lifetime: Duration,
    ) -> SnapshotResult<RecoveryPlan>;
    async fn read_snapshot_chunk(
        &self,
        lease: RecoveryLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> SnapshotResult<SnapshotBytePage>;
    async fn read_recovery_page(
        &self,
        lease: RecoveryLeaseId,
        after: u64,
        limits: PageLimits,
    ) -> SnapshotResult<Page>;
    async fn release_recovery(&self, lease: RecoveryLeaseId) -> SnapshotResult<RecoveryRelease>;
}
