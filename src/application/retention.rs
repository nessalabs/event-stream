use super::SnapshotStore;
use crate::domain::*;
use async_trait::async_trait;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetentionStatus {
    pub bounds: Bounds,
    pub retry_policy: RetryPolicyState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnableRetryPolicyReceipt {
    pub request: EnableRetryPolicy,
    pub status: RetentionStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceRetryGenerationReceipt {
    pub request: AdvanceRetryGeneration,
    pub status: RetentionStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExpireRetryGenerationsReceipt {
    pub request: ExpireRetryGenerations,
    pub status: RetentionStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceRetentionFloorReceipt {
    pub request: AdvanceRetentionFloor,
    pub status: RetentionStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionCleanupLimits {
    pub max_event_rows: usize,
    pub max_retry_rows: usize,
    pub max_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetentionCleanupProgress {
    pub removed_event_rows: usize,
    pub removed_retry_rows: usize,
    pub removed_bytes: usize,
    pub remaining: bool,
}

#[derive(Clone, Debug)]
pub struct RetentionStoreConfig {
    pub operations: RetentionOperationLimits,
    pub receipts: RetryReceiptLimits,
    pub cleanup: RetentionCleanupLimits,
}

#[derive(Clone, Debug)]
pub struct RetentionOperationLimits {
    pub max_receipts: usize,
    pub max_receipt_bytes: usize,
    pub max_pending_cleanup_ranges: usize,
}

#[derive(Clone, Debug)]
pub struct RetryReceiptLimits {
    pub max_rows: usize,
    pub max_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct RetentionAdmissionConfig {
    pub max_concurrent: usize,
    pub max_waiters: usize,
    pub max_waiter_bytes: usize,
    pub max_in_flight_bytes: usize,
    pub admission_timeout: Duration,
}

impl Default for RetentionStoreConfig {
    fn default() -> Self {
        Self {
            operations: RetentionOperationLimits {
                max_receipts: 4096,
                max_receipt_bytes: 4 * 1024 * 1024,
                max_pending_cleanup_ranges: 1024,
            },
            receipts: RetryReceiptLimits {
                max_rows: 100_000,
                max_bytes: 1024 * 1024 * 1024,
            },
            cleanup: RetentionCleanupLimits {
                max_event_rows: 256,
                max_retry_rows: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        }
    }
}

impl Default for RetentionAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 4,
            max_waiters: 32,
            max_waiter_bytes: 4 * 1024 * 1024,
            max_in_flight_bytes: 4 * 1024 * 1024,
            admission_timeout: Duration::from_secs(5),
        }
    }
}

impl RetentionStoreConfig {
    pub fn validate(&self) -> RetentionResult<()> {
        if self.operations.max_receipts == 0
            || self.operations.max_receipt_bytes == 0
            || self.operations.max_pending_cleanup_ranges == 0
            || self.receipts.max_rows == 0
            || self.receipts.max_bytes == 0
            || self.cleanup.max_event_rows == 0
            || self.cleanup.max_retry_rows == 0
            || self.cleanup.max_bytes == 0
        {
            return Err(RetentionError::InvalidConfig(
                "retention store limits must be nonzero and finite".into(),
            ));
        }
        Ok(())
    }
}

impl RetentionAdmissionConfig {
    pub fn validate(&self) -> RetentionResult<()> {
        if self.max_concurrent == 0
            || self.max_waiters == 0
            || self.max_waiter_bytes == 0
            || self.max_in_flight_bytes == 0
            || self.max_waiter_bytes > u32::MAX as usize
            || self.max_in_flight_bytes > u32::MAX as usize
            || self.admission_timeout.is_zero()
            || self.admission_timeout.as_nanos() > u128::from(u64::MAX)
        {
            return Err(RetentionError::InvalidConfig(
                "retention admission limits must be finite".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RetentionError {
    InvalidConfig(String),
    InvalidInput(String),
    StaleIncarnation {
        current: Option<Box<StreamKey>>,
    },
    StaleFloor {
        current: Box<Cursor>,
    },
    StaleGeneration {
        current: RetryGeneration,
    },
    RecoveryProtectionActive {
        maximum_floor: Box<Cursor>,
    },
    #[cfg(feature = "replication")]
    ReplicaProtectionActive {
        maximum_floor: Box<Cursor>,
    },
    JournalProtectionActive {
        oldest_required: RetryGeneration,
    },
    RetryGenerationExpired {
        oldest_accepted: RetryGeneration,
    },
    RetryGenerationAhead {
        current: RetryGeneration,
    },
    LegacyRetryNotFound {
        event_id: EventId,
    },
    RetryPolicyRequired,
    #[cfg(feature = "replication")]
    ReplicaBacklogExceeded {
        replica: ReplicaId,
        limit_bytes: u64,
    },
    #[cfg(feature = "replication")]
    ReplicaBacklogExpired {
        replica: ReplicaId,
    },
    #[cfg(feature = "replication")]
    ReplicationClockRollback,
    OperationConflict {
        operation_id: RetentionOperationId,
    },
    IdempotencyConflict {
        identity: Box<GeneratedEventIdentity>,
    },
    CapacityExceeded,
    Overloaded,
    AdmissionTimeout,
    Closed,
    CorruptStorage(String),
    StorageFailure(String),
    EnableUnknown(Box<EnableRetryPolicy>),
    AdvanceGenerationUnknown(Box<AdvanceRetryGeneration>),
    ExpireGenerationsUnknown(Box<ExpireRetryGenerations>),
    AdvanceFloorUnknown(Box<AdvanceRetentionFloor>),
    GeneratedAppendUnknown(Box<GeneratedEventIdentity>),
}

impl std::fmt::Display for RetentionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RetentionError {}

pub type RetentionResult<T> = std::result::Result<T, RetentionError>;

#[async_trait]
pub trait RetentionStore: SnapshotStore {
    async fn retention_status(&self, stream: &StreamKey) -> RetentionResult<RetentionStatus>;
    async fn enable_retry_policy(
        &self,
        request: EnableRetryPolicy,
    ) -> RetentionResult<EnableRetryPolicyReceipt>;
    async fn advance_retry_generation(
        &self,
        request: AdvanceRetryGeneration,
    ) -> RetentionResult<AdvanceRetryGenerationReceipt>;
    async fn expire_retry_generations(
        &self,
        request: ExpireRetryGenerations,
    ) -> RetentionResult<ExpireRetryGenerationsReceipt>;
    async fn append_generated(
        &self,
        stream: &StreamKey,
        event: GeneratedEvent,
    ) -> RetentionResult<AppendReceipt>;
    async fn lookup_generated(
        &self,
        stream: &StreamKey,
        generation: RetryGeneration,
        event_id: &EventId,
    ) -> RetentionResult<Option<std::sync::Arc<Record>>>;
    async fn advance_retention_floor(
        &self,
        request: AdvanceRetentionFloor,
    ) -> RetentionResult<AdvanceRetentionFloorReceipt>;
    async fn cleanup_retention(
        &self,
        limits: RetentionCleanupLimits,
    ) -> RetentionResult<RetentionCleanupProgress>;
}
