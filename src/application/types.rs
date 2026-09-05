use crate::domain::*;
use std::time::Duration;
#[derive(Clone, Debug)]
pub enum StartPosition {
    Beginning,
    After(Cursor),
    Future,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum PersistenceProfile {
    Ephemeral,
    ProcessRestart,
    PowerLoss,
}
#[derive(Clone, Debug)]
pub struct StoreCapabilities {
    pub persistence: PersistenceProfile,
    pub format_version: u32,
    pub max_record_bytes: usize,
    pub max_concurrent_reads: usize,
    pub max_concurrent_writes: usize,
    pub ownership: &'static str,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Error {
    InvalidIdentifier,
    InvalidConfig(String),
    PayloadTooLarge,
    InvalidCursor(String),
    CursorAhead {
        tail: Cursor,
    },
    HistoryUnavailable {
        bounds: Bounds,
    },
    StreamNotFound,
    StaleIncarnation {
        current: Box<StreamAvailability>,
    },
    StreamUnavailable {
        last: Box<StreamKey>,
    },
    LifecycleConflict {
        operation_id: LifecycleOperationId,
    },
    LifecycleCommitUnknown {
        operation_id: LifecycleOperationId,
    },
    IdempotencyConflict {
        event_id: EventId,
    },
    #[cfg(feature = "retention")]
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
    #[cfg(feature = "replication")]
    ReplicaProtectionActive {
        replica: ReplicaId,
    },
    OffsetOverflow,
    Overloaded,
    CapacityExceeded,
    AdmissionTimeout,
    SubscriberLagged {
        last_delivered: Cursor,
        bounds: Box<Bounds>,
    },
    StoreWriteFailed(String),
    CommitUnknown {
        event_id: EventId,
    },
    StoreInUse,
    OwnershipLost,
    StoreCorrupt(String),
    UnsupportedFormat(u32),
    Closed,
    RuntimeFaulted(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnresolvedAppend {
    pub stream: StreamKey,
    pub event_id: EventId,
}
#[derive(Clone, Debug)]
pub struct ShutdownReport {
    pub closed: bool,
    pub unresolved: Vec<UnresolvedAppend>,
    pub unresolved_lifecycle: Vec<LifecycleRequest>,
    pub unfinished_cleanup: usize,
}
#[derive(Clone, Debug)]
pub struct SubscriptionOptions {
    pub start: StartPosition,
    pub page: PageLimits,
    pub max_lag_records: u64,
    pub max_lag_duration: Duration,
    pub catch_up_grace: Duration,
}

impl From<ValidationError> for Error {
    fn from(_: ValidationError) -> Self {
        Self::InvalidIdentifier
    }
}
