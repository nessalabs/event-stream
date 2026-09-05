#[cfg(feature = "retention")]
use crate::application::RetentionStoreConfig;
#[cfg(feature = "source-journal")]
use crate::application::SourceJournalStoreConfig;
#[cfg(feature = "replication")]
use crate::application::{ReplicaDestinationConfig, ReplicationStoreConfig};
use crate::{
    application::{
        Error, EventStore, LifecycleStore, PersistenceProfile, Result, StoreCapabilities,
    },
    domain::*,
};
use async_trait::async_trait;
#[cfg(feature = "snapshots")]
use sha2::{Digest, Sha256};
#[cfg(feature = "replication")]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "snapshots")]
use std::{
    collections::BTreeSet,
    ops::Bound::{Excluded, Unbounded},
    time::{Duration, Instant},
};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::{Arc, Mutex},
};

#[cfg(feature = "snapshots")]
use crate::application::{
    MonotonicClock, MonotonicTick, RecoveryRelease, SnapshotAbortReceipt, SnapshotBytePage,
    SnapshotCleanupLimits, SnapshotCleanupProgress, SnapshotError, SnapshotPage, SnapshotResult,
    SnapshotStore, SnapshotStoreConfig, SnapshotUploadPage, VerificationLimits,
};

/// Finite storage limits for one ephemeral store instance.
#[derive(Clone, Debug)]
pub struct MemoryStoreOptions {
    pub max_record_bytes: usize,
    pub max_history_records: usize,
    pub max_history_bytes: usize,
    pub max_streams: usize,
    pub max_stream_metadata_bytes: usize,
    pub max_lifecycle_receipts: usize,
    pub max_lifecycle_receipt_bytes: usize,
    pub max_retired_lifetimes: usize,
    pub max_retired_metadata_bytes: usize,
    #[cfg(feature = "snapshots")]
    pub snapshots: SnapshotStoreConfig,
    #[cfg(feature = "snapshots")]
    pub snapshot_clock: Arc<dyn MonotonicClock>,
    #[cfg(feature = "retention")]
    pub retention: RetentionStoreConfig,
    #[cfg(feature = "source-journal")]
    pub source_journal: SourceJournalStoreConfig,
    #[cfg(feature = "replication")]
    pub replication: ReplicationStoreConfig,
    #[cfg(feature = "replication")]
    pub replica_destination: ReplicaDestinationConfig,
    #[cfg(feature = "replication")]
    pub replication_clock: Arc<dyn crate::application::DurableReplicationClock>,
}

#[cfg(feature = "snapshots")]
#[derive(Debug)]
pub struct ProcessMonotonicClock {
    origin: Instant,
}

#[cfg(feature = "snapshots")]
impl Default for ProcessMonotonicClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

#[cfg(feature = "snapshots")]
impl MonotonicClock for ProcessMonotonicClock {
    fn now(&self) -> MonotonicTick {
        MonotonicTick(u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX))
    }
}

impl Default for MemoryStoreOptions {
    fn default() -> Self {
        Self {
            max_record_bytes: 1024 * 1024,
            max_history_records: 100_000,
            max_history_bytes: 64 * 1024 * 1024,
            max_streams: 10_000,
            max_stream_metadata_bytes: 4 * 1024 * 1024,
            max_lifecycle_receipts: 10_000,
            max_lifecycle_receipt_bytes: 4 * 1024 * 1024,
            max_retired_lifetimes: 10_000,
            max_retired_metadata_bytes: 4 * 1024 * 1024,
            #[cfg(feature = "snapshots")]
            snapshots: SnapshotStoreConfig::default(),
            #[cfg(feature = "snapshots")]
            snapshot_clock: Arc::new(ProcessMonotonicClock::default()),
            #[cfg(feature = "retention")]
            retention: RetentionStoreConfig::default(),
            #[cfg(feature = "source-journal")]
            source_journal: SourceJournalStoreConfig::default(),
            #[cfg(feature = "replication")]
            replication: ReplicationStoreConfig::default(),
            #[cfg(feature = "replication")]
            replica_destination: ReplicaDestinationConfig::default(),
            #[cfg(feature = "replication")]
            replication_clock: Arc::new(ProcessDurableReplicationClock),
        }
    }
}

#[cfg(feature = "replication")]
#[derive(Debug)]
pub struct ProcessDurableReplicationClock;

#[cfg(feature = "replication")]
impl crate::application::DurableReplicationClock for ProcessDurableReplicationClock {
    fn now(&self) -> DurableTimestampMillis {
        DurableTimestampMillis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .ok()
                .and_then(|duration| u64::try_from(duration.as_millis()).ok())
                .unwrap_or(0),
        )
    }
}

#[derive(Debug)]
pub struct MemoryStore {
    pub(super) options: MemoryStoreOptions,
    pub(super) state: Mutex<State>,
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) streams: HashMap<StreamId, StreamEntry>,
    retired: VecDeque<StreamHistory>,
    pub(super) history_records: usize,
    pub(super) history_bytes: usize,
    stream_metadata_bytes: usize,
    lifecycle_receipts: HashMap<LifecycleOperationId, LifecycleReceipt>,
    #[cfg(feature = "retention")]
    pub(super) operation_ids: std::collections::HashSet<Box<str>>,
    lifecycle_receipt_bytes: usize,
    retired_lifetimes: usize,
    retired_metadata_bytes: usize,
    #[cfg(feature = "snapshots")]
    snapshots: HashMap<SnapshotId, MemorySnapshot>,
    #[cfg(feature = "snapshots")]
    staging_snapshots: BTreeSet<SnapshotId>,
    #[cfg(feature = "snapshots")]
    aborted_staging_snapshots: BTreeSet<SnapshotId>,
    #[cfg(feature = "snapshots")]
    aborted_snapshots: HashMap<SnapshotId, SnapshotDescriptor>,
    #[cfg(feature = "snapshots")]
    published_snapshots: HashMap<StreamKey, BTreeMap<(u64, SnapshotId), SnapshotId>>,
    #[cfg(feature = "snapshots")]
    pub(super) recovery_leases: HashMap<RecoveryLeaseId, MemoryRecoveryLease>,
    #[cfg(feature = "snapshots")]
    staged_count: usize,
    #[cfg(feature = "snapshots")]
    staged_bytes: u64,
    #[cfg(feature = "snapshots")]
    published_count: usize,
    #[cfg(feature = "snapshots")]
    published_bytes: u64,
    #[cfg(feature = "snapshots")]
    snapshot_chunk_count: usize,
    #[cfg(feature = "snapshots")]
    snapshot_chunk_metadata_bytes: usize,
    #[cfg(feature = "snapshots")]
    snapshot_receipt_bytes: usize,
    #[cfg(feature = "snapshots")]
    snapshot_receipt_count: usize,
    #[cfg(feature = "snapshots")]
    snapshot_descriptor_metadata_bytes: usize,
    #[cfg(feature = "snapshots")]
    active_verifications: usize,
    #[cfg(feature = "retention")]
    pub(super) retention_receipts:
        HashMap<RetentionOperationId, super::memory_retention::MemoryRetentionReceipt>,
    #[cfg(feature = "retention")]
    pub(super) retention_receipt_bytes: usize,
    #[cfg(feature = "retention")]
    pub(super) pending_retention_cleanup: VecDeque<StreamKey>,
    #[cfg(feature = "retention")]
    pub(super) pending_retention_cleanup_set: std::collections::HashSet<StreamKey>,
    #[cfg(feature = "retention")]
    pub(super) retry_receipt_rows: usize,
    #[cfg(feature = "retention")]
    pub(super) retry_receipt_bytes: u64,
    #[cfg(feature = "source-journal")]
    pub(super) journal_sources:
        HashMap<SourceKey, super::memory_source_journal::MemoryJournalSource>,
    #[cfg(feature = "source-journal")]
    pub(super) current_journal_sources: HashMap<SourceId, SourceKey>,
    #[cfg(feature = "source-journal")]
    pub(super) journal_operation_receipts:
        HashMap<JournalOperationId, super::memory_source_journal::MemoryJournalReceipt>,
    #[cfg(feature = "source-journal")]
    pub(super) journal_captured_bytes: u64,
    #[cfg(feature = "source-journal")]
    pub(super) journal_segment_rows: usize,
    #[cfg(feature = "source-journal")]
    pub(super) journal_receipt_bytes: usize,
    #[cfg(feature = "source-journal")]
    pub(super) journal_receipt_rows: usize,
    #[cfg(feature = "source-journal")]
    pub(super) journal_marker_bytes: u64,
    #[cfg(feature = "source-journal")]
    pub(super) journal_marker_rows: usize,
    #[cfg(feature = "source-journal")]
    pub(super) journal_checkpoint_count: usize,
    #[cfg(feature = "source-journal")]
    pub(super) journal_checkpoint_bytes: u64,
    #[cfg(feature = "source-journal")]
    pub(super) journal_cleanup_queue: VecDeque<SourceKey>,
    #[cfg(feature = "source-journal")]
    pub(super) journal_cleanup_set: std::collections::HashSet<SourceKey>,
    #[cfg(feature = "source-journal")]
    pub(super) journal_generation_pins: HashMap<StreamKey, BTreeMap<RetryGeneration, usize>>,
    #[cfg(feature = "replication")]
    pub(super) replication_origin: OriginId,
    #[cfg(feature = "replication")]
    pub(super) replica_destination_epoch: DestinationEpoch,
    #[cfg(feature = "replication")]
    pub(super) replica_histories: HashMap<OriginStream, BTreeMap<u64, Arc<Record>>>,
    #[cfg(feature = "replication")]
    pub(super) replica_history_records: usize,
    #[cfg(feature = "replication")]
    pub(super) replica_history_bytes: u64,
    #[cfg(feature = "replication")]
    pub(super) replica_receipts: HashMap<BatchId, (ReplicaBatch, crate::domain::ReplicaReceipt)>,
    #[cfg(feature = "replication")]
    pub(super) replica_receipt_bytes: usize,
    #[cfg(feature = "replication")]
    pub(super) replica_receipt_floors: HashMap<OriginStream, u64>,
    #[cfg(feature = "replication")]
    pub(super) replica_floor_receipts:
        HashMap<ReplicationOperationId, crate::application::AdvanceReplicaReceiptFloorReceipt>,
    #[cfg(feature = "replication")]
    pub(super) replica_bootstraps:
        HashMap<BootstrapId, super::memory_replication::MemoryReplicaBootstrap>,
    #[cfg(feature = "replication")]
    pub(super) published_replica_bootstraps: HashMap<OriginStream, BootstrapId>,
    #[cfg(feature = "replication")]
    pub(super) replica_staging_chunks: usize,
    #[cfg(feature = "replication")]
    pub(super) replica_staging_bytes: u64,
    #[cfg(feature = "replication")]
    pub(super) replica_staging_records: usize,
    #[cfg(feature = "replication")]
    pub(super) replica_staging_record_bytes: u64,
    #[cfg(feature = "replication")]
    pub(super) replica_published_snapshot_bytes: u64,
    #[cfg(feature = "replication")]
    pub(super) replica_read_leases:
        HashMap<ReplicaReadLeaseId, super::memory_replication::MemoryReplicaReadLease>,
    #[cfg(feature = "replication")]
    pub(super) replica_read_lease_expiries:
        BTreeMap<(DurableTimestampMillis, ReplicaReadLeaseId), ()>,
    #[cfg(feature = "replication")]
    pub(super) replica_cleanup_queue: VecDeque<BootstrapId>,
    #[cfg(feature = "replication")]
    pub(super) replica_cleanup_set: std::collections::HashSet<BootstrapId>,
    #[cfg(feature = "replication")]
    pub(super) origin_replicas:
        HashMap<(ReplicaId, OriginStream), super::memory_replication::MemoryOriginReplica>,
    #[cfg(feature = "replication")]
    pub(super) origin_replicas_by_stream: HashMap<StreamKey, Vec<(ReplicaId, OriginStream)>>,
    #[cfg(feature = "replication")]
    pub(super) origin_replication_receipts:
        HashMap<ReplicationOperationId, super::memory_replication::MemoryOriginReceipt>,
    #[cfg(feature = "replication")]
    pub(super) origin_pending_batch_bytes: usize,
    #[cfg(feature = "replication")]
    pub(super) origin_replication_receipt_bytes: usize,
    #[cfg(feature = "replication")]
    pub(super) last_replication_clock: Option<DurableTimestampMillis>,
    #[cfg(feature = "replication")]
    pub(super) replication_commit_times: HashMap<StreamKey, BTreeMap<u64, DurableTimestampMillis>>,
    pub(super) closed: bool,
}

#[derive(Debug)]
pub(super) struct StreamHistory {
    pub(super) key: StreamKey,
    pub(super) records: BTreeMap<u64, Arc<Record>>,
    pub(super) event_offsets: HashMap<EventId, u64>,
    pub(super) tail: u64,
    #[cfg(feature = "retention")]
    pub(super) floor: u64,
    #[cfg(feature = "retention")]
    pub(super) retry_policy: RetryPolicyState,
    #[cfg(feature = "retention")]
    pub(super) generated_event_offsets: BTreeMap<(RetryGeneration, EventId), u64>,
    #[cfg(feature = "retention")]
    pub(super) record_generations: BTreeMap<u64, RetryGeneration>,
    #[cfg(feature = "retention")]
    pub(super) retained_retry_records: BTreeMap<(RetryGeneration, EventId), Arc<Record>>,
    #[cfg(feature = "retention")]
    pub(super) logical_bytes: u64,
}

#[derive(Debug)]
pub(super) enum StreamEntry {
    Active(StreamHistory),
    Unavailable(StreamKey),
}

#[cfg(feature = "snapshots")]
#[derive(Debug)]
struct MemorySnapshot {
    descriptor: SnapshotDescriptor,
    chunks: BTreeMap<u64, Payload>,
    accepted_bytes: u64,
    state: SnapshotUploadState,
    verification: Option<MemoryVerification>,
    checksum_failed: bool,
    receipt_reserved: bool,
}

#[cfg(feature = "snapshots")]
#[derive(Debug)]
struct MemoryVerification {
    next_offset: u64,
    hasher: Sha256,
}

#[cfg(feature = "snapshots")]
#[derive(Debug)]
pub(super) struct MemoryRecoveryLease {
    snapshot: SnapshotId,
    pub(super) stream: StreamKey,
    pub(super) covered: u64,
    pub(super) through: u64,
    pub(super) expires: MonotonicTick,
}

#[cfg(feature = "snapshots")]
impl MemorySnapshot {
    fn progress(&self) -> SnapshotUploadProgress {
        SnapshotUploadProgress {
            descriptor: self.descriptor.clone(),
            accepted_bytes: self.accepted_bytes,
            verified_bytes: self.verification.as_ref().map_or_else(
                || {
                    if matches!(
                        self.state,
                        SnapshotUploadState::Verified | SnapshotUploadState::Published
                    ) {
                        self.descriptor.content_bytes
                    } else {
                        0
                    }
                },
                |verification| verification.next_offset,
            ),
            state: self.state,
        }
    }
}

#[cfg(feature = "snapshots")]
fn snapshot_store_error(error: Error) -> SnapshotError {
    match error {
        Error::Closed => SnapshotError::Closed,
        Error::StreamNotFound => SnapshotError::StaleIncarnation { current: None },
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
        Error::CapacityExceeded => SnapshotError::CapacityExceeded,
        Error::StoreCorrupt(detail) => SnapshotError::CorruptStorage(detail),
        other => SnapshotError::StorageFailure(other.to_string()),
    }
}

#[cfg(feature = "snapshots")]
fn snapshot_chunk_at(
    chunks: &BTreeMap<u64, Payload>,
    offset: u64,
) -> SnapshotResult<(u64, Payload)> {
    let (&start, payload) = chunks
        .range(..=offset)
        .next_back()
        .ok_or_else(|| SnapshotError::CorruptStorage("snapshot has a byte gap".into()))?;
    let inside = usize::try_from(offset - start)
        .map_err(|_| SnapshotError::CorruptStorage("chunk offset overflow".into()))?;
    if inside < payload.len() {
        return Ok((start, payload.clone()));
    }
    chunks
        .get_key_value(&offset)
        .map(|(&start, payload)| (start, payload.clone()))
        .ok_or_else(|| SnapshotError::CorruptStorage("snapshot has a byte gap".into()))
}

impl MemoryStore {
    #[cfg(feature = "replication")]
    pub(super) fn published_snapshot_for_replication(
        state: &State,
        id: SnapshotId,
    ) -> Option<SnapshotDescriptor> {
        state
            .snapshots
            .get(&id)
            .filter(|snapshot| snapshot.state == SnapshotUploadState::Published)
            .map(|snapshot| snapshot.descriptor.clone())
    }

    #[cfg(feature = "replication")]
    pub(super) fn acquire_replication_snapshot_lease(
        &self,
        state: &mut State,
        id: SnapshotId,
    ) -> crate::application::ReplicationResult<(RecoveryLeaseId, MonotonicTick)> {
        let now = self.options.snapshot_clock.now();
        let expires = now
            .checked_add(self.options.snapshots.recovery.max_lifetime)
            .ok_or_else(|| {
                crate::application::ReplicationError::InvalidConfig(
                    "snapshot recovery lifetime overflows its clock".into(),
                )
            })?;
        state
            .recovery_leases
            .retain(|_, lease| lease.expires.0 > now.0);
        if state.recovery_leases.len() >= self.options.snapshots.recovery.max_leases {
            return Err(crate::application::ReplicationError::CapacityExceeded);
        }
        let snapshot = state
            .snapshots
            .get(&id)
            .filter(|snapshot| snapshot.state == SnapshotUploadState::Published)
            .ok_or_else(|| {
                crate::application::ReplicationError::InvalidInput(
                    "snapshot is not published".into(),
                )
            })?;
        let descriptor = snapshot.descriptor.clone();
        let history = Self::history(state, &descriptor.covered.stream).map_err(|error| {
            crate::application::ReplicationError::StorageFailure(error.to_string())
        })?;
        let lease = (0..4)
            .map(|_| RecoveryLeaseId(*uuid::Uuid::new_v4().as_bytes()))
            .find(|lease| !state.recovery_leases.contains_key(lease))
            .ok_or_else(|| {
                crate::application::ReplicationError::StorageFailure(
                    "snapshot recovery lease identity collision".into(),
                )
            })?;
        state.recovery_leases.insert(
            lease,
            MemoryRecoveryLease {
                snapshot: id,
                stream: descriptor.covered.stream,
                covered: descriptor.covered.offset,
                through: history.tail,
                expires,
            },
        );
        Ok((lease, expires))
    }

    pub(super) fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::StoreCorrupt("memory store lock poisoned".into()))
    }

    fn validate_options(options: &MemoryStoreOptions) -> Result<()> {
        if options.max_record_bytes == 0
            || options.max_history_records == 0
            || options.max_history_bytes == 0
            || options.max_streams == 0
            || options.max_stream_metadata_bytes == 0
            || options.max_lifecycle_receipts == 0
            || options.max_lifecycle_receipt_bytes == 0
            || options.max_retired_lifetimes == 0
            || options.max_retired_metadata_bytes == 0
        {
            return Err(Error::InvalidConfig(
                "memory store limits must all be nonzero".into(),
            ));
        }
        if options
            .max_record_bytes
            .checked_add(256)
            .is_none_or(|bytes| bytes > options.max_history_bytes)
        {
            return Err(Error::InvalidConfig(
                "one maximum-size record and its index overhead must fit history bytes".into(),
            ));
        }
        #[cfg(feature = "snapshots")]
        options
            .snapshots
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "retention")]
        options
            .retention
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "source-journal")]
        options
            .source_journal
            .validate()
            .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        #[cfg(feature = "replication")]
        {
            options
                .replication
                .validate()
                .map_err(|error| Error::InvalidConfig(error.to_string()))?;
            options
                .replica_destination
                .validate()
                .map_err(|error| Error::InvalidConfig(error.to_string()))?;
        }
        Ok(())
    }

    pub(super) fn history<'a>(state: &'a State, stream: &StreamKey) -> Result<&'a StreamHistory> {
        if state.closed {
            return Err(Error::Closed);
        }
        let entry = state.streams.get(&stream.id).ok_or(Error::StreamNotFound)?;
        match entry {
            StreamEntry::Unavailable(last) => Err(Error::StreamUnavailable {
                last: Box::new(last.clone()),
            }),
            StreamEntry::Active(history) if &history.key != stream => {
                Err(Error::StaleIncarnation {
                    current: Box::new(StreamAvailability::Active(history.key.clone())),
                })
            }
            StreamEntry::Active(history) => Ok(history),
        }
    }
}

#[async_trait]
impl EventStore for MemoryStore {
    type Options = MemoryStoreOptions;

    async fn open(options: Self::Options) -> Result<Self> {
        Self::validate_options(&options)?;
        Ok(Self {
            options,
            state: Mutex::new(State {
                streams: HashMap::new(),
                retired: VecDeque::new(),
                history_records: 0,
                history_bytes: 0,
                stream_metadata_bytes: 0,
                lifecycle_receipts: HashMap::new(),
                #[cfg(feature = "retention")]
                operation_ids: std::collections::HashSet::new(),
                lifecycle_receipt_bytes: 0,
                retired_lifetimes: 0,
                retired_metadata_bytes: 0,
                #[cfg(feature = "snapshots")]
                snapshots: HashMap::new(),
                #[cfg(feature = "snapshots")]
                staging_snapshots: BTreeSet::new(),
                #[cfg(feature = "snapshots")]
                aborted_staging_snapshots: BTreeSet::new(),
                #[cfg(feature = "snapshots")]
                aborted_snapshots: HashMap::new(),
                #[cfg(feature = "snapshots")]
                published_snapshots: HashMap::new(),
                #[cfg(feature = "snapshots")]
                recovery_leases: HashMap::new(),
                #[cfg(feature = "snapshots")]
                staged_count: 0,
                #[cfg(feature = "snapshots")]
                staged_bytes: 0,
                #[cfg(feature = "snapshots")]
                published_count: 0,
                #[cfg(feature = "snapshots")]
                published_bytes: 0,
                #[cfg(feature = "snapshots")]
                snapshot_chunk_count: 0,
                #[cfg(feature = "snapshots")]
                snapshot_chunk_metadata_bytes: 0,
                #[cfg(feature = "snapshots")]
                snapshot_receipt_bytes: 0,
                #[cfg(feature = "snapshots")]
                snapshot_receipt_count: 0,
                #[cfg(feature = "snapshots")]
                snapshot_descriptor_metadata_bytes: 0,
                #[cfg(feature = "snapshots")]
                active_verifications: 0,
                #[cfg(feature = "retention")]
                retention_receipts: HashMap::new(),
                #[cfg(feature = "retention")]
                retention_receipt_bytes: 0,
                #[cfg(feature = "retention")]
                pending_retention_cleanup: VecDeque::new(),
                #[cfg(feature = "retention")]
                pending_retention_cleanup_set: std::collections::HashSet::new(),
                #[cfg(feature = "retention")]
                retry_receipt_rows: 0,
                #[cfg(feature = "retention")]
                retry_receipt_bytes: 0,
                #[cfg(feature = "source-journal")]
                journal_sources: HashMap::new(),
                #[cfg(feature = "source-journal")]
                current_journal_sources: HashMap::new(),
                #[cfg(feature = "source-journal")]
                journal_operation_receipts: HashMap::new(),
                #[cfg(feature = "source-journal")]
                journal_captured_bytes: 0,
                #[cfg(feature = "source-journal")]
                journal_segment_rows: 0,
                #[cfg(feature = "source-journal")]
                journal_receipt_bytes: 0,
                #[cfg(feature = "source-journal")]
                journal_receipt_rows: 0,
                #[cfg(feature = "source-journal")]
                journal_marker_bytes: 0,
                #[cfg(feature = "source-journal")]
                journal_marker_rows: 0,
                #[cfg(feature = "source-journal")]
                journal_checkpoint_count: 0,
                #[cfg(feature = "source-journal")]
                journal_checkpoint_bytes: 0,
                #[cfg(feature = "source-journal")]
                journal_cleanup_queue: VecDeque::new(),
                #[cfg(feature = "source-journal")]
                journal_cleanup_set: std::collections::HashSet::new(),
                #[cfg(feature = "source-journal")]
                journal_generation_pins: HashMap::new(),
                #[cfg(feature = "replication")]
                replication_origin: OriginId(*uuid::Uuid::new_v4().as_bytes()),
                #[cfg(feature = "replication")]
                replica_destination_epoch: DestinationEpoch(*uuid::Uuid::new_v4().as_bytes()),
                #[cfg(feature = "replication")]
                replica_histories: HashMap::new(),
                #[cfg(feature = "replication")]
                replica_history_records: 0,
                #[cfg(feature = "replication")]
                replica_history_bytes: 0,
                #[cfg(feature = "replication")]
                replica_receipts: HashMap::new(),
                #[cfg(feature = "replication")]
                replica_receipt_bytes: 0,
                #[cfg(feature = "replication")]
                replica_receipt_floors: HashMap::new(),
                #[cfg(feature = "replication")]
                replica_floor_receipts: HashMap::new(),
                #[cfg(feature = "replication")]
                replica_bootstraps: HashMap::new(),
                #[cfg(feature = "replication")]
                published_replica_bootstraps: HashMap::new(),
                #[cfg(feature = "replication")]
                replica_staging_chunks: 0,
                #[cfg(feature = "replication")]
                replica_staging_bytes: 0,
                #[cfg(feature = "replication")]
                replica_staging_records: 0,
                #[cfg(feature = "replication")]
                replica_staging_record_bytes: 0,
                #[cfg(feature = "replication")]
                replica_published_snapshot_bytes: 0,
                #[cfg(feature = "replication")]
                replica_read_leases: HashMap::new(),
                #[cfg(feature = "replication")]
                replica_read_lease_expiries: BTreeMap::new(),
                #[cfg(feature = "replication")]
                replica_cleanup_queue: VecDeque::new(),
                #[cfg(feature = "replication")]
                replica_cleanup_set: std::collections::HashSet::new(),
                #[cfg(feature = "replication")]
                origin_replicas: HashMap::new(),
                #[cfg(feature = "replication")]
                origin_replicas_by_stream: HashMap::new(),
                #[cfg(feature = "replication")]
                origin_replication_receipts: HashMap::new(),
                #[cfg(feature = "replication")]
                origin_pending_batch_bytes: 0,
                #[cfg(feature = "replication")]
                origin_replication_receipt_bytes: 0,
                #[cfg(feature = "replication")]
                last_replication_clock: None,
                #[cfg(feature = "replication")]
                replication_commit_times: HashMap::new(),
                closed: false,
            }),
        })
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistence: PersistenceProfile::Ephemeral,
            format_version: 1,
            max_record_bytes: self.options.max_record_bytes,
            max_concurrent_reads: usize::MAX,
            max_concurrent_writes: usize::MAX,
            ownership: "one in-memory store instance",
        }
    }

    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(Error::Closed);
        }
        if let Some(entry) = state.streams.get(id) {
            return match entry {
                StreamEntry::Active(history) => Ok(history.key.clone()),
                StreamEntry::Unavailable(last) => Err(Error::StreamUnavailable {
                    last: Box::new(last.clone()),
                }),
            };
        }
        let metadata_bytes = id
            .as_str()
            .len()
            .checked_add(256)
            .ok_or(Error::CapacityExceeded)?;
        if state.streams.len() >= self.options.max_streams
            || state
                .stream_metadata_bytes
                .checked_add(metadata_bytes)
                .is_none_or(|bytes| bytes > self.options.max_stream_metadata_bytes)
        {
            return Err(Error::CapacityExceeded);
        }
        let key = StreamKey {
            id: id.clone(),
            incarnation: super::new_incarnation(),
        };
        state.streams.insert(
            id.clone(),
            StreamEntry::Active(StreamHistory {
                key: key.clone(),
                records: BTreeMap::new(),
                event_offsets: HashMap::new(),
                tail: 0,
                #[cfg(feature = "retention")]
                floor: 0,
                #[cfg(feature = "retention")]
                retry_policy: RetryPolicyState::Lifetime,
                #[cfg(feature = "retention")]
                generated_event_offsets: BTreeMap::new(),
                #[cfg(feature = "retention")]
                record_generations: BTreeMap::new(),
                #[cfg(feature = "retention")]
                retained_retry_records: BTreeMap::new(),
                #[cfg(feature = "retention")]
                logical_bytes: 0,
            }),
        );
        state.stream_metadata_bytes += metadata_bytes;
        Ok(key)
    }

    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        let event_bytes = event.accounted_bytes();
        if event_bytes > self.options.max_record_bytes {
            return Err(Error::PayloadTooLarge);
        }
        let mut state = self.lock()?;
        let history = Self::history(&state, stream)?;
        #[cfg(feature = "retention")]
        if !matches!(history.retry_policy, RetryPolicyState::Lifetime) {
            return Err(Error::RetryPolicyRequired);
        }
        if let Some(offset) = history.event_offsets.get(&event.id) {
            let original = history.records.get(offset).ok_or_else(|| {
                Error::StoreCorrupt("event identity points to a missing record".into())
            })?;
            if original.event.schema != event.schema || original.event.payload != event.payload {
                return Err(Error::IdempotencyConflict {
                    event_id: event.id.clone(),
                });
            }
            return Ok(AppendReceipt {
                record: original.clone(),
                kind: AppendKind::Deduplicated,
            });
        }
        let stored_bytes = event_bytes
            .checked_add(256)
            .ok_or(Error::CapacityExceeded)?;
        #[cfg(feature = "replication")]
        let replication_now = self.preflight_replication_append(&state, stream, stored_bytes)?;
        if state.history_records >= self.options.max_history_records
            || state
                .history_bytes
                .checked_add(stored_bytes)
                .filter(|total| *total <= self.options.max_history_bytes)
                .is_none()
        {
            return Err(Error::CapacityExceeded);
        }
        let offset = history.tail.checked_add(1).ok_or(Error::OffsetOverflow)?;
        let record = Arc::new(Record {
            cursor: Cursor::new(stream.clone(), offset),
            event,
        });
        let history = state
            .streams
            .get_mut(&stream.id)
            .and_then(|entry| match entry {
                StreamEntry::Active(history) => Some(history),
                StreamEntry::Unavailable(_) => None,
            })
            .expect("validated active history above");
        history
            .event_offsets
            .insert(record.event.id.clone(), offset);
        history.records.insert(offset, record.clone());
        history.tail = offset;
        #[cfg(feature = "retention")]
        {
            history.logical_bytes = history
                .logical_bytes
                .checked_add(stored_bytes as u64)
                .ok_or(Error::CapacityExceeded)?;
        }
        state.history_records += 1;
        state.history_bytes += stored_bytes;
        #[cfg(feature = "replication")]
        Self::apply_replication_append(
            &mut state,
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

    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        let state = self.lock()?;
        let history = Self::history(&state, stream)?;
        #[cfg(feature = "retention")]
        if !matches!(history.retry_policy, RetryPolicyState::Lifetime) {
            return Err(Error::RetryPolicyRequired);
        }
        match history.event_offsets.get(id) {
            Some(offset) => history
                .records
                .get(offset)
                .cloned()
                .map(Some)
                .ok_or_else(|| Error::StoreCorrupt("event lookup has no record".into())),
            None => Ok(None),
        }
    }

    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        let state = self.lock()?;
        let history = Self::history(&state, stream)?;
        Ok(Bounds {
            floor: Cursor::new(stream.clone(), {
                #[cfg(feature = "retention")]
                {
                    history.floor
                }
                #[cfg(not(feature = "retention"))]
                {
                    0
                }
            }),
            tail: Cursor::new(stream.clone(), history.tail),
        })
    }

    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(Error::InvalidConfig("page limits must be nonzero".into()));
        }
        let state = self.lock()?;
        let history = Self::history(&state, stream)?;
        let bounds = Bounds {
            floor: Cursor::new(stream.clone(), {
                #[cfg(feature = "retention")]
                {
                    history.floor
                }
                #[cfg(not(feature = "retention"))]
                {
                    0
                }
            }),
            tail: Cursor::new(stream.clone(), history.tail),
        };
        #[cfg(feature = "retention")]
        if after < history.floor {
            return Err(Error::HistoryUnavailable { bounds });
        }
        if after > history.tail {
            return Err(Error::CursorAhead { tail: bounds.tail });
        }
        if through > history.tail {
            return Err(Error::CursorAhead { tail: bounds.tail });
        }
        if after > through {
            return Err(Error::InvalidConfig("after is beyond through".into()));
        }

        if after == through {
            return Ok(Page {
                records: Vec::new(),
                next_after: Cursor::new(stream.clone(), after),
                through: Cursor::new(stream.clone(), through),
                complete: true,
            });
        }
        let available = usize::try_from(through - after).unwrap_or(usize::MAX);
        let mut records = Vec::with_capacity(limits.max_records.min(available));
        let mut bytes = 0usize;
        let first = after.checked_add(1).ok_or(Error::OffsetOverflow)?;
        let mut expected = first;
        let mut range = history.records.range(first..=through);
        loop {
            let Some((&offset, record)) = range.next() else {
                return Err(Error::StoreCorrupt(format!(
                    "missing committed record at offset {expected}"
                )));
            };
            if offset != expected {
                return Err(Error::StoreCorrupt(format!(
                    "missing committed record at offset {expected}"
                )));
            }
            let record_bytes = record.event.accounted_bytes();
            if records.is_empty() && record_bytes > limits.max_bytes {
                return Err(Error::CapacityExceeded);
            }
            if records.len() == limits.max_records
                || bytes
                    .checked_add(record_bytes)
                    .is_none_or(|n| n > limits.max_bytes)
            {
                break;
            }
            bytes += record_bytes;
            records.push(record.clone());
            if offset == through {
                break;
            }
            expected = offset.checked_add(1).ok_or(Error::OffsetOverflow)?;
        }
        let next_offset = records.last().map_or(after, |r| r.cursor.offset);
        Ok(Page {
            records,
            next_after: Cursor::new(stream.clone(), next_offset),
            through: Cursor::new(stream.clone(), through),
            complete: next_offset == through,
        })
    }

    async fn close(&self) -> Result<()> {
        self.lock()?.closed = true;
        Ok(())
    }
}

#[async_trait]
impl LifecycleStore for MemoryStore {
    async fn change_lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleReceipt> {
        let receipt_charge = request.receipt_charge().ok_or(Error::CapacityExceeded)?;
        let mut state = self.lock()?;
        if state.closed {
            return Err(Error::Closed);
        }
        if let Some(receipt) = state.lifecycle_receipts.get(&request.operation_id) {
            if receipt.request == request {
                return Ok(receipt.clone());
            }
            return Err(Error::LifecycleConflict {
                operation_id: request.operation_id.clone(),
            });
        }
        #[cfg(feature = "retention")]
        if state.operation_ids.contains(request.operation_id.as_str()) {
            return Err(Error::LifecycleConflict {
                operation_id: request.operation_id.clone(),
            });
        }
        let availability = match state.streams.get(&request.expected.id) {
            Some(StreamEntry::Active(history)) => StreamAvailability::Active(history.key.clone()),
            Some(StreamEntry::Unavailable(last)) => StreamAvailability::Unavailable(last.clone()),
            None => return Err(Error::StreamNotFound),
        };
        match &availability {
            StreamAvailability::Active(current) | StreamAvailability::Unavailable(current)
                if current != &request.expected =>
            {
                return Err(Error::StaleIncarnation {
                    current: Box::new(availability),
                });
            }
            _ => {}
        }
        #[cfg(feature = "replication")]
        if let Some(replica) = Self::required_replica(&state, &request.expected) {
            return Err(Error::ReplicaProtectionActive { replica });
        }
        if state.lifecycle_receipts.len() >= self.options.max_lifecycle_receipts
            || state
                .lifecycle_receipt_bytes
                .checked_add(receipt_charge)
                .is_none_or(|total| total > self.options.max_lifecycle_receipt_bytes)
        {
            return Err(Error::CapacityExceeded);
        }
        let retires_active = matches!(availability, StreamAvailability::Active(_));
        let retired_charge = request
            .expected
            .id
            .as_str()
            .len()
            .checked_add(256)
            .ok_or(Error::CapacityExceeded)?;
        if retires_active
            && (state.retired_lifetimes >= self.options.max_retired_lifetimes
                || state
                    .retired_metadata_bytes
                    .checked_add(retired_charge)
                    .is_none_or(|total| total > self.options.max_retired_metadata_bytes))
        {
            return Err(Error::CapacityExceeded);
        }

        let replacement = match request.action {
            LifecycleAction::Delete => None,
            LifecycleAction::Reset => Some(StreamKey {
                id: request.expected.id.clone(),
                incarnation: super::new_incarnation(),
            }),
        };
        let receipt = LifecycleReceipt {
            request: request.clone(),
            replacement: replacement.clone(),
        };
        let prior = state
            .streams
            .remove(&request.expected.id)
            .expect("validated stream name above");
        if let StreamEntry::Active(history) = prior {
            #[cfg(feature = "replication")]
            state.replication_commit_times.remove(&history.key);
            state.retired.push_back(history);
        }
        let next = match replacement {
            Some(key) => StreamEntry::Active(StreamHistory {
                key,
                records: BTreeMap::new(),
                event_offsets: HashMap::new(),
                tail: 0,
                #[cfg(feature = "retention")]
                floor: 0,
                #[cfg(feature = "retention")]
                retry_policy: RetryPolicyState::Lifetime,
                #[cfg(feature = "retention")]
                generated_event_offsets: BTreeMap::new(),
                #[cfg(feature = "retention")]
                record_generations: BTreeMap::new(),
                #[cfg(feature = "retention")]
                retained_retry_records: BTreeMap::new(),
                #[cfg(feature = "retention")]
                logical_bytes: 0,
            }),
            None => StreamEntry::Unavailable(request.expected.clone()),
        };
        state.streams.insert(request.expected.id.clone(), next);
        if retires_active {
            state.retired_lifetimes += 1;
            state.retired_metadata_bytes += retired_charge;
        }
        state.lifecycle_receipt_bytes += receipt_charge;
        state
            .lifecycle_receipts
            .insert(request.operation_id.clone(), receipt.clone());
        #[cfg(feature = "retention")]
        state
            .operation_ids
            .insert(request.operation_id.as_str().into());
        Ok(receipt)
    }

    async fn cleanup_retired(&self, limits: CleanupLimits) -> Result<CleanupProgress> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(Error::InvalidConfig(
                "cleanup limits must be nonzero".into(),
            ));
        }
        if self
            .options
            .max_record_bytes
            .checked_add(256)
            .is_none_or(|maximum| maximum > limits.max_bytes)
        {
            return Err(Error::InvalidConfig(
                "cleanup bytes must fit one maximum-size stored record".into(),
            ));
        }
        let mut state = self.lock()?;
        if state.closed {
            return Err(Error::Closed);
        }
        #[cfg(feature = "snapshots")]
        {
            let now = self.options.snapshot_clock.now();
            state
                .recovery_leases
                .retain(|_, lease| lease.expires.0 > now.0);
        }
        if state.retired.is_empty() {
            return Ok(CleanupProgress {
                stream: None,
                removed_records: 0,
                removed_bytes: 0,
                remaining: false,
            });
        };
        let mut removed_records = 0usize;
        let mut removed_bytes = 0usize;
        #[cfg(feature = "retention")]
        let mut removed_history_records = 0usize;
        #[cfg(feature = "retention")]
        let mut removed_history_bytes = 0usize;
        #[cfg(feature = "retention")]
        let mut released_retry_rows = 0usize;
        #[cfg(feature = "retention")]
        let mut released_retry_bytes = 0u64;
        #[cfg(feature = "snapshots")]
        let maximum_removable = {
            let stream = &state.retired.front().expect("retired lifetime exists").key;
            state
                .recovery_leases
                .values()
                .filter(|lease| &lease.stream == stream)
                .map(|lease| lease.covered)
                .min()
                .unwrap_or(u64::MAX)
        };
        let (stream, finalized, retired_charge) = {
            let history = state
                .retired
                .front_mut()
                .expect("selected retired lifetime exists");
            let stream = history.key.clone();
            while removed_records < limits.max_records {
                let Some((&offset, record)) = history.records.first_key_value() else {
                    break;
                };
                #[cfg(feature = "snapshots")]
                if offset > maximum_removable {
                    break;
                }
                let charge = record
                    .event
                    .accounted_bytes()
                    .checked_add(256)
                    .ok_or(Error::CapacityExceeded)?;
                if removed_records == 0 && charge > limits.max_bytes {
                    return Err(Error::CapacityExceeded);
                }
                if removed_bytes
                    .checked_add(charge)
                    .is_none_or(|total| total > limits.max_bytes)
                {
                    break;
                }
                let event_id = record.event.id.clone();
                #[cfg(feature = "retention")]
                let generation = history
                    .record_generations
                    .get(&offset)
                    .copied()
                    .unwrap_or(RetryGeneration::LEGACY);
                #[cfg(feature = "retention")]
                let retry_reserved =
                    matches!(history.retry_policy, RetryPolicyState::Generational { .. })
                        && if generation == RetryGeneration::LEGACY {
                            history.event_offsets.contains_key(&event_id)
                        } else {
                            history
                                .generated_event_offsets
                                .contains_key(&(generation, event_id.clone()))
                        };
                history.records.remove(&offset);
                history.event_offsets.remove(&event_id);
                #[cfg(feature = "retention")]
                {
                    history.record_generations.remove(&offset);
                    history
                        .generated_event_offsets
                        .remove(&(generation, event_id.clone()));
                    history.logical_bytes = history.logical_bytes.saturating_sub(charge as u64);
                    removed_history_records += 1;
                    removed_history_bytes += charge;
                    if retry_reserved {
                        released_retry_rows += 1;
                        released_retry_bytes += charge as u64;
                    }
                }
                removed_records += 1;
                removed_bytes += charge;
            }
            #[cfg(feature = "retention")]
            while removed_records < limits.max_records {
                let Some((identity, record)) = history.retained_retry_records.first_key_value()
                else {
                    break;
                };
                let identity = identity.clone();
                let charge = record
                    .event
                    .accounted_bytes()
                    .checked_add(256)
                    .ok_or(Error::CapacityExceeded)?;
                if removed_records == 0 && charge > limits.max_bytes {
                    return Err(Error::CapacityExceeded);
                }
                if removed_bytes
                    .checked_add(charge)
                    .is_none_or(|total| total > limits.max_bytes)
                {
                    break;
                }
                history.retained_retry_records.remove(&identity);
                if identity.0 == RetryGeneration::LEGACY {
                    history.event_offsets.remove(&identity.1);
                } else {
                    history.generated_event_offsets.remove(&identity);
                }
                released_retry_rows += 1;
                released_retry_bytes += charge as u64;
                removed_records += 1;
                removed_bytes += charge;
            }
            #[cfg(feature = "retention")]
            let finalized = history.records.is_empty()
                && history.retained_retry_records.is_empty()
                && history.event_offsets.is_empty()
                && history.generated_event_offsets.is_empty()
                && history.record_generations.is_empty();
            #[cfg(not(feature = "retention"))]
            let finalized = history.records.is_empty();
            if finalized {
                state.retired.pop_front();
            }
            let retired_charge = stream
                .id
                .as_str()
                .len()
                .checked_add(256)
                .ok_or(Error::CapacityExceeded)?;
            (stream, finalized, retired_charge)
        };
        #[cfg(feature = "retention")]
        let history_rows_removed = removed_history_records;
        #[cfg(not(feature = "retention"))]
        let history_rows_removed = removed_records;
        state.history_records = state
            .history_records
            .checked_sub(history_rows_removed)
            .ok_or_else(|| Error::StoreCorrupt("cleanup record accounting underflow".into()))?;
        state.history_bytes = state
            .history_bytes
            .checked_sub({
                #[cfg(feature = "retention")]
                {
                    removed_history_bytes
                }
                #[cfg(not(feature = "retention"))]
                {
                    removed_bytes
                }
            })
            .ok_or_else(|| Error::StoreCorrupt("cleanup byte accounting underflow".into()))?;
        #[cfg(feature = "retention")]
        {
            state.retry_receipt_rows = state
                .retry_receipt_rows
                .checked_sub(released_retry_rows)
                .ok_or_else(|| Error::StoreCorrupt("retry row accounting underflow".into()))?;
            state.retry_receipt_bytes = state
                .retry_receipt_bytes
                .checked_sub(released_retry_bytes)
                .ok_or_else(|| Error::StoreCorrupt("retry byte accounting underflow".into()))?;
        }
        if finalized {
            state.retired_lifetimes = state
                .retired_lifetimes
                .checked_sub(1)
                .ok_or_else(|| Error::StoreCorrupt("retired count accounting underflow".into()))?;
            state.retired_metadata_bytes = state
                .retired_metadata_bytes
                .checked_sub(retired_charge)
                .ok_or_else(|| Error::StoreCorrupt("retired byte accounting underflow".into()))?;
        }
        let remaining = !state.retired.is_empty();
        Ok(CleanupProgress {
            stream: Some(stream),
            removed_records,
            removed_bytes,
            remaining,
        })
    }
}

#[cfg(feature = "snapshots")]
#[async_trait]
impl SnapshotStore for MemoryStore {
    async fn begin_snapshot(
        &self,
        descriptor: SnapshotDescriptor,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        if descriptor.covered.version != CURSOR_VERSION {
            return Err(SnapshotError::InvalidInput(
                "snapshot cursor version is unsupported".into(),
            ));
        }
        let descriptor_charge = descriptor
            .accounted_bytes()
            .ok_or(SnapshotError::CapacityExceeded)?;
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        if let Some(snapshot) = state.snapshots.get(&descriptor.id) {
            return if snapshot.descriptor == descriptor {
                Ok(snapshot.progress())
            } else {
                Err(SnapshotError::OperationConflict { id: descriptor.id })
            };
        }
        if let Some(aborted) = state.aborted_snapshots.get(&descriptor.id) {
            return if aborted == &descriptor {
                Ok(SnapshotUploadProgress {
                    descriptor,
                    accepted_bytes: 0,
                    verified_bytes: 0,
                    state: SnapshotUploadState::Aborted,
                })
            } else {
                Err(SnapshotError::OperationConflict { id: descriptor.id })
            };
        }
        let history =
            Self::history(&state, &descriptor.covered.stream).map_err(snapshot_store_error)?;
        if descriptor.covered.offset > history.tail {
            return Err(SnapshotError::CursorAhead {
                tail: Box::new(Cursor::new(history.key.clone(), history.tail)),
            });
        }
        let config = &self.options.snapshots.storage;
        if state.staged_count >= config.max_staging_snapshots
            || descriptor.content_bytes > config.max_staging_bytes
            || state
                .staged_bytes
                .checked_add(descriptor.content_bytes)
                .is_none_or(|total| total > config.max_staging_bytes)
            || state
                .snapshot_descriptor_metadata_bytes
                .checked_add(descriptor_charge)
                .is_none_or(|total| total > config.max_descriptor_metadata_bytes)
        {
            return Err(SnapshotError::CapacityExceeded);
        }
        let progress = SnapshotUploadProgress {
            descriptor: descriptor.clone(),
            accepted_bytes: 0,
            verified_bytes: 0,
            state: SnapshotUploadState::Uploading,
        };
        state.snapshots.insert(
            descriptor.id,
            MemorySnapshot {
                descriptor,
                chunks: BTreeMap::new(),
                accepted_bytes: 0,
                state: SnapshotUploadState::Uploading,
                verification: None,
                checksum_failed: false,
                receipt_reserved: false,
            },
        );
        state.staging_snapshots.insert(progress.descriptor.id);
        state.staged_count += 1;
        state.staged_bytes += progress.descriptor.content_bytes;
        state.snapshot_descriptor_metadata_bytes += descriptor_charge;
        Ok(progress)
    }

    async fn put_snapshot_chunk(
        &self,
        id: SnapshotId,
        chunk: SnapshotChunk,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        if chunk.bytes.is_empty()
            || chunk.bytes.len() > self.options.snapshots.storage.max_chunk_bytes
        {
            return Err(SnapshotError::InvalidInput(
                "snapshot chunk size is outside configured bounds".into(),
            ));
        }
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let snapshot = state
            .snapshots
            .get(&id)
            .ok_or(SnapshotError::NotFound { id })?;
        if let Some(existing) = snapshot.chunks.get(&chunk.offset) {
            return if existing == &chunk.bytes {
                Ok(snapshot.progress())
            } else {
                Err(SnapshotError::OperationConflict { id })
            };
        }
        if snapshot.state != SnapshotUploadState::Uploading {
            return Err(SnapshotError::IncompleteUpload { id });
        }
        if chunk.offset != snapshot.accepted_bytes {
            return Err(SnapshotError::InvalidInput(
                "snapshot chunks must be contiguous".into(),
            ));
        }
        let chunk_len =
            u64::try_from(chunk.bytes.len()).map_err(|_| SnapshotError::CapacityExceeded)?;
        let next = chunk
            .offset
            .checked_add(chunk_len)
            .ok_or(SnapshotError::CapacityExceeded)?;
        if next > snapshot.descriptor.content_bytes {
            return Err(SnapshotError::InvalidInput(
                "snapshot chunk exceeds declared content length".into(),
            ));
        }
        let metadata = SNAPSHOT_CHUNK_ENVELOPE_BYTES;
        let config = &self.options.snapshots.storage;
        if state.snapshot_chunk_count >= config.max_chunks
            || state
                .snapshot_chunk_metadata_bytes
                .checked_add(metadata)
                .is_none_or(|total| total > config.max_chunk_metadata_bytes)
        {
            return Err(SnapshotError::CapacityExceeded);
        }
        let snapshot = state
            .snapshots
            .get_mut(&id)
            .expect("snapshot checked above");
        snapshot.chunks.insert(chunk.offset, chunk.bytes);
        snapshot.accepted_bytes = next;
        let progress = snapshot.progress();
        state.snapshot_chunk_count += 1;
        state.snapshot_chunk_metadata_bytes += metadata;
        Ok(progress)
    }

    async fn snapshot_status(&self, id: SnapshotId) -> SnapshotResult<SnapshotUploadProgress> {
        let state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        if let Some(snapshot) = state.snapshots.get(&id) {
            return Ok(snapshot.progress());
        }
        state
            .aborted_snapshots
            .get(&id)
            .cloned()
            .map(|descriptor| SnapshotUploadProgress {
                descriptor,
                accepted_bytes: 0,
                verified_bytes: 0,
                state: SnapshotUploadState::Aborted,
            })
            .ok_or(SnapshotError::NotFound { id })
    }

    async fn verify_snapshot_step(
        &self,
        id: SnapshotId,
        limits: VerificationLimits,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        let verification_config = &self.options.snapshots.verification;
        if limits.max_chunks == 0
            || limits.max_bytes == 0
            || limits.max_chunks > verification_config.max_chunks_per_step
            || limits.max_bytes > verification_config.max_bytes_per_step
        {
            return Err(SnapshotError::InvalidInput(
                "verification step exceeds configured limits".into(),
            ));
        }
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let snapshot = state
            .snapshots
            .get(&id)
            .ok_or(SnapshotError::NotFound { id })?;
        if snapshot.checksum_failed {
            return Err(SnapshotError::ChecksumMismatch { id });
        }
        if matches!(
            snapshot.state,
            SnapshotUploadState::Verified | SnapshotUploadState::Published
        ) {
            return Ok(snapshot.progress());
        }
        if snapshot.accepted_bytes != snapshot.descriptor.content_bytes {
            return Err(SnapshotError::IncompleteUpload { id });
        }
        if snapshot.verification.is_none()
            && state.active_verifications >= verification_config.max_active
        {
            return Err(SnapshotError::CapacityExceeded);
        }
        if snapshot.verification.is_none() {
            state.active_verifications += 1;
            let snapshot = state.snapshots.get_mut(&id).expect("snapshot exists");
            snapshot.state = SnapshotUploadState::Verifying;
            snapshot.verification = Some(MemoryVerification {
                next_offset: 0,
                hasher: Sha256::new(),
            });
        }

        let mut chunks = 0usize;
        let mut bytes = 0usize;
        while chunks < limits.max_chunks && bytes < limits.max_bytes {
            let snapshot = state.snapshots.get(&id).expect("snapshot exists");
            let next_offset = snapshot
                .verification
                .as_ref()
                .expect("verification active")
                .next_offset;
            if next_offset == snapshot.descriptor.content_bytes {
                break;
            }
            let (chunk_offset, payload) = snapshot_chunk_at(&snapshot.chunks, next_offset)?;
            let inside = usize::try_from(next_offset - chunk_offset)
                .map_err(|_| SnapshotError::CorruptStorage("chunk offset overflow".into()))?;
            let take = (payload.len() - inside).min(limits.max_bytes - bytes);
            let snapshot = state.snapshots.get_mut(&id).expect("snapshot exists");
            let verification = snapshot.verification.as_mut().expect("verification active");
            verification
                .hasher
                .update(&payload.as_bytes()[inside..inside + take]);
            verification.next_offset = verification
                .next_offset
                .checked_add(u64::try_from(take).map_err(|_| SnapshotError::CapacityExceeded)?)
                .ok_or(SnapshotError::CapacityExceeded)?;
            bytes += take;
            chunks += 1;
        }

        let finished = state
            .snapshots
            .get(&id)
            .and_then(|snapshot| snapshot.verification.as_ref())
            .is_some_and(|verification| {
                verification.next_offset == state.snapshots[&id].descriptor.content_bytes
            });
        if finished {
            let snapshot = state.snapshots.get_mut(&id).expect("snapshot exists");
            let verification = snapshot.verification.take().expect("verification exists");
            let actual: [u8; 32] = verification.hasher.finalize().into();
            state.active_verifications = state.active_verifications.saturating_sub(1);
            let snapshot = state.snapshots.get_mut(&id).expect("snapshot exists");
            if actual != snapshot.descriptor.digest.0 {
                snapshot.checksum_failed = true;
                return Err(SnapshotError::ChecksumMismatch { id });
            }
            snapshot.state = SnapshotUploadState::Verified;
        }
        Ok(state.snapshots[&id].progress())
    }

    async fn publish_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotDescriptor> {
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let snapshot = state
            .snapshots
            .get(&id)
            .ok_or(SnapshotError::NotFound { id })?;
        if snapshot.state == SnapshotUploadState::Published {
            return Ok(snapshot.descriptor.clone());
        }
        if snapshot.state != SnapshotUploadState::Verified {
            return Err(SnapshotError::IncompleteUpload { id });
        }
        let descriptor = snapshot.descriptor.clone();
        let history =
            Self::history(&state, &descriptor.covered.stream).map_err(snapshot_store_error)?;
        if descriptor.covered.offset > history.tail {
            return Err(SnapshotError::CursorAhead {
                tail: Box::new(Cursor::new(history.key.clone(), history.tail)),
            });
        }
        let storage = &self.options.snapshots.storage;
        if state.published_count >= storage.max_snapshots
            || state
                .published_bytes
                .checked_add(descriptor.content_bytes)
                .is_none_or(|total| total > storage.max_published_bytes)
        {
            return Err(SnapshotError::CapacityExceeded);
        }
        state.snapshots.get_mut(&id).expect("snapshot exists").state =
            SnapshotUploadState::Published;
        state.staging_snapshots.remove(&id);
        state.staged_count -= 1;
        state.staged_bytes -= descriptor.content_bytes;
        state.published_count += 1;
        state.published_bytes += descriptor.content_bytes;
        state
            .published_snapshots
            .entry(descriptor.covered.stream.clone())
            .or_default()
            .insert((descriptor.covered.offset, id), id);
        Ok(descriptor)
    }

    async fn abort_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotAbortReceipt> {
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        if state.aborted_snapshots.contains_key(&id)
            || state
                .snapshots
                .get(&id)
                .is_some_and(|snapshot| snapshot.state == SnapshotUploadState::Aborted)
        {
            return Ok(SnapshotAbortReceipt {
                id,
                already_aborted: true,
            });
        }
        let snapshot = state
            .snapshots
            .get(&id)
            .ok_or(SnapshotError::NotFound { id })?;
        if snapshot.state == SnapshotUploadState::Published {
            return Err(SnapshotError::OperationConflict { id });
        }
        let receipt_charge = snapshot
            .descriptor
            .accounted_bytes()
            .ok_or(SnapshotError::CapacityExceeded)?;
        let storage = &self.options.snapshots.storage;
        if state.snapshot_receipt_count >= storage.max_receipts
            || state
                .snapshot_receipt_bytes
                .checked_add(receipt_charge)
                .is_none_or(|total| total > storage.max_receipt_bytes)
        {
            return Err(SnapshotError::CapacityExceeded);
        }
        let snapshot = state.snapshots.get_mut(&id).expect("snapshot exists");
        if snapshot.verification.take().is_some() {
            state.active_verifications = state.active_verifications.saturating_sub(1);
        }
        let snapshot = state.snapshots.get_mut(&id).expect("snapshot exists");
        snapshot.state = SnapshotUploadState::Aborted;
        snapshot.receipt_reserved = true;
        state.aborted_staging_snapshots.insert(id);
        state.snapshot_receipt_count += 1;
        state.snapshot_receipt_bytes += receipt_charge;
        Ok(SnapshotAbortReceipt {
            id,
            already_aborted: false,
        })
    }

    async fn cleanup_snapshot_staging(
        &self,
        limits: SnapshotCleanupLimits,
    ) -> SnapshotResult<SnapshotCleanupProgress> {
        if limits.max_rows == 0
            || limits.max_bytes == 0
            || limits.max_rows > self.options.snapshots.cleanup.max_rows
            || limits.max_bytes > self.options.snapshots.cleanup.max_bytes
        {
            return Err(SnapshotError::InvalidInput(
                "snapshot cleanup exceeds configured limits".into(),
            ));
        }
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let candidate = state.aborted_staging_snapshots.first().copied();
        let Some(id) = candidate else {
            return Ok(SnapshotCleanupProgress {
                removed_snapshots: 0,
                removed_chunks: 0,
                removed_bytes: 0,
                remaining: false,
            });
        };
        let mut removed_chunks = 0usize;
        let mut removed_bytes = 0usize;
        while removed_chunks < limits.max_rows {
            let next = state.snapshots[&id]
                .chunks
                .first_key_value()
                .map(|(offset, payload)| (*offset, payload.clone()));
            let Some((offset, payload)) = next else { break };
            let charge = payload
                .len()
                .checked_add(SNAPSHOT_CHUNK_ENVELOPE_BYTES)
                .ok_or(SnapshotError::CapacityExceeded)?;
            if removed_bytes
                .checked_add(charge)
                .is_none_or(|total| total > limits.max_bytes)
            {
                if removed_chunks == 0 {
                    return Err(SnapshotError::CapacityExceeded);
                }
                break;
            }
            state
                .snapshots
                .get_mut(&id)
                .expect("snapshot exists")
                .chunks
                .remove(&offset);
            state.snapshot_chunk_count -= 1;
            state.snapshot_chunk_metadata_bytes -= SNAPSHOT_CHUNK_ENVELOPE_BYTES;
            removed_chunks += 1;
            removed_bytes += charge;
        }
        let mut removed_snapshots = 0usize;
        if removed_chunks < limits.max_rows && state.snapshots[&id].chunks.is_empty() {
            let snapshot = state.snapshots.remove(&id).expect("snapshot exists");
            state.staging_snapshots.remove(&id);
            state.aborted_staging_snapshots.remove(&id);
            state.staged_count -= 1;
            state.staged_bytes -= snapshot.descriptor.content_bytes;
            state.aborted_snapshots.insert(id, snapshot.descriptor);
            removed_snapshots = 1;
        }
        let remaining = !state.aborted_staging_snapshots.is_empty();
        Ok(SnapshotCleanupProgress {
            removed_snapshots,
            removed_chunks,
            removed_bytes,
            remaining,
        })
    }

    async fn list_snapshots(
        &self,
        stream: &StreamKey,
        after: Option<SnapshotContinuation>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotPage> {
        let maximum = self.options.snapshots.storage.max_list_page;
        if limits.max_records == 0
            || limits.max_bytes == 0
            || limits.max_records > maximum.max_records
            || limits.max_bytes > maximum.max_bytes
        {
            return Err(SnapshotError::InvalidInput(
                "snapshot page exceeds configured limits".into(),
            ));
        }
        let state = self.lock().map_err(snapshot_store_error)?;
        Self::history(&state, stream).map_err(snapshot_store_error)?;
        if let Some(continuation) = &after {
            if continuation.covered.stream != *stream {
                return Err(SnapshotError::InvalidInput(
                    "snapshot continuation identifies another stream".into(),
                ));
            }
        }
        let start = after.as_ref().map(|value| (value.covered.offset, value.id));
        let Some(index) = state.published_snapshots.get(stream) else {
            return Ok(SnapshotPage {
                entries: Vec::new(),
                next_after: None,
                complete: true,
            });
        };
        let lower = start.map_or(Unbounded, Excluded);
        let mut candidates = index.range((lower, Unbounded)).peekable();
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        while let Some((_, id)) = candidates.peek().copied() {
            if entries.len() == limits.max_records {
                break;
            }
            let descriptor = &state.snapshots[id].descriptor;
            let charge = descriptor
                .accounted_bytes()
                .ok_or(SnapshotError::CapacityExceeded)?;
            if entries.is_empty() && charge > limits.max_bytes {
                return Err(SnapshotError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > limits.max_bytes)
            {
                break;
            }
            bytes += charge;
            entries.push(descriptor.clone());
            candidates.next();
        }
        let complete = candidates.peek().is_none();
        let next_after = entries.last().map(|descriptor| SnapshotContinuation {
            covered: descriptor.covered.clone(),
            id: descriptor.id,
        });
        Ok(SnapshotPage {
            entries,
            next_after,
            complete,
        })
    }

    async fn list_snapshot_uploads(
        &self,
        after: Option<SnapshotId>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage> {
        let maximum = self.options.snapshots.storage.max_list_page;
        if limits.max_records == 0
            || limits.max_bytes == 0
            || limits.max_records > maximum.max_records
            || limits.max_bytes > maximum.max_bytes
        {
            return Err(SnapshotError::InvalidInput(
                "snapshot upload page exceeds configured limits".into(),
            ));
        }
        let state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let lower = after.map_or(Unbounded, Excluded);
        let mut candidates = state.staging_snapshots.range((lower, Unbounded)).peekable();
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        while let Some(id) = candidates.peek().copied() {
            if entries.len() == limits.max_records {
                break;
            }
            let snapshot = &state.snapshots[id];
            let charge = snapshot
                .descriptor
                .accounted_bytes()
                .and_then(|bytes| bytes.checked_add(64))
                .ok_or(SnapshotError::CapacityExceeded)?;
            if entries.is_empty() && charge > limits.max_bytes {
                return Err(SnapshotError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > limits.max_bytes)
            {
                break;
            }
            bytes += charge;
            entries.push(snapshot.progress());
            candidates.next();
        }
        let complete = candidates.peek().is_none();
        let next_after = entries.last().map(|entry| entry.descriptor.id);
        Ok(SnapshotUploadPage {
            entries,
            next_after,
            complete,
        })
    }

    async fn acquire_recovery(
        &self,
        id: SnapshotId,
        lifetime: Duration,
    ) -> SnapshotResult<RecoveryPlan> {
        let maximum = self.options.snapshots.recovery.max_lifetime;
        if lifetime.is_zero() || lifetime > maximum {
            return Err(SnapshotError::InvalidInput(
                "recovery lifetime exceeds configured limit".into(),
            ));
        }
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let now = self.options.snapshot_clock.now();
        let expires = now.checked_add(lifetime).ok_or_else(|| {
            SnapshotError::InvalidInput("recovery lifetime overflows clock".into())
        })?;
        state
            .recovery_leases
            .retain(|_, lease| lease.expires.0 > now.0);
        if state.recovery_leases.len() >= self.options.snapshots.recovery.max_leases {
            return Err(SnapshotError::CapacityExceeded);
        }
        let snapshot = state
            .snapshots
            .get(&id)
            .filter(|snapshot| snapshot.state == SnapshotUploadState::Published)
            .ok_or(SnapshotError::NotFound { id })?;
        let descriptor = snapshot.descriptor.clone();
        let history =
            Self::history(&state, &descriptor.covered.stream).map_err(snapshot_store_error)?;
        let through = history.tail;
        let lease = loop {
            let lease = RecoveryLeaseId(*uuid::Uuid::new_v4().as_bytes());
            if !state.recovery_leases.contains_key(&lease) {
                break lease;
            }
        };
        state.recovery_leases.insert(
            lease,
            MemoryRecoveryLease {
                snapshot: id,
                stream: descriptor.covered.stream.clone(),
                covered: descriptor.covered.offset,
                through,
                expires,
            },
        );
        Ok(RecoveryPlan {
            lease,
            snapshot: descriptor.clone(),
            through: Cursor::new(descriptor.covered.stream, through),
        })
    }

    async fn read_snapshot_chunk(
        &self,
        lease: RecoveryLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> SnapshotResult<SnapshotBytePage> {
        if max_bytes == 0 || max_bytes > self.options.snapshots.recovery.max_chunk_bytes {
            return Err(SnapshotError::InvalidInput(
                "snapshot read exceeds configured byte limit".into(),
            ));
        }
        let state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let now = self.options.snapshot_clock.now();
        let protection = state
            .recovery_leases
            .get(&lease)
            .filter(|protection| protection.expires.0 > now.0)
            .ok_or(SnapshotError::ExpiredProtection { lease })?;
        let snapshot =
            state
                .snapshots
                .get(&protection.snapshot)
                .ok_or(SnapshotError::CorruptStorage(
                    "recovery lease refers to a missing snapshot".into(),
                ))?;
        if offset > snapshot.descriptor.content_bytes {
            return Err(SnapshotError::InvalidInput(
                "snapshot byte offset is beyond content".into(),
            ));
        }
        if offset == snapshot.descriptor.content_bytes {
            return Ok(SnapshotBytePage {
                snapshot: snapshot.descriptor.id,
                offset,
                bytes: Payload::copy_from_slice(&[]),
                next_offset: offset,
                complete: true,
            });
        }
        let mut output = Vec::with_capacity(max_bytes);
        let mut next = offset;
        while output.len() < max_bytes && next < snapshot.descriptor.content_bytes {
            let (chunk_offset, payload) = snapshot_chunk_at(&snapshot.chunks, next)?;
            let inside = usize::try_from(next - chunk_offset)
                .map_err(|_| SnapshotError::CorruptStorage("chunk offset overflow".into()))?;
            let take = (payload.len() - inside).min(max_bytes - output.len());
            output.extend_from_slice(&payload.as_bytes()[inside..inside + take]);
            next = next
                .checked_add(u64::try_from(take).map_err(|_| SnapshotError::CapacityExceeded)?)
                .ok_or(SnapshotError::CapacityExceeded)?;
        }
        Ok(SnapshotBytePage {
            snapshot: snapshot.descriptor.id,
            offset,
            bytes: Payload::copy_from_slice(&output),
            next_offset: next,
            complete: next == snapshot.descriptor.content_bytes,
        })
    }

    async fn read_recovery_page(
        &self,
        lease: RecoveryLeaseId,
        after: u64,
        limits: PageLimits,
    ) -> SnapshotResult<Page> {
        let maximum = self.options.snapshots.recovery.max_page;
        if limits.max_records == 0
            || limits.max_bytes == 0
            || limits.max_records > maximum.max_records
            || limits.max_bytes > maximum.max_bytes
        {
            return Err(SnapshotError::InvalidInput(
                "recovery page exceeds configured limits".into(),
            ));
        }
        let state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        let now = self.options.snapshot_clock.now();
        let protection = state
            .recovery_leases
            .get(&lease)
            .filter(|protection| protection.expires.0 > now.0)
            .ok_or(SnapshotError::ExpiredProtection { lease })?;
        if after < protection.covered || after > protection.through {
            return Err(SnapshotError::InvalidInput(
                "recovery cursor is outside the protected range".into(),
            ));
        }
        if after == protection.through {
            return Ok(Page {
                records: Vec::new(),
                next_after: Cursor::new(protection.stream.clone(), after),
                through: Cursor::new(protection.stream.clone(), protection.through),
                complete: true,
            });
        }
        let history = state
            .streams
            .get(&protection.stream.id)
            .and_then(|entry| match entry {
                StreamEntry::Active(history) if history.key == protection.stream => Some(history),
                _ => None,
            })
            .or_else(|| {
                state
                    .retired
                    .iter()
                    .find(|history| history.key == protection.stream)
            })
            .ok_or_else(|| SnapshotError::MissingHistory {
                floor: Box::new(Cursor::new(protection.stream.clone(), after)),
            })?;
        let mut records = Vec::new();
        let mut bytes = 0usize;
        let mut expected = after
            .checked_add(1)
            .ok_or_else(|| SnapshotError::CorruptStorage("event offset overflow".into()))?;
        while expected <= protection.through && records.len() < limits.max_records {
            let record =
                history
                    .records
                    .get(&expected)
                    .ok_or_else(|| SnapshotError::MissingHistory {
                        floor: Box::new(Cursor::new(protection.stream.clone(), expected)),
                    })?;
            let charge = record.event.accounted_bytes();
            if records.is_empty() && charge > limits.max_bytes {
                return Err(SnapshotError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > limits.max_bytes)
            {
                break;
            }
            bytes += charge;
            records.push(record.clone());
            if expected == protection.through {
                break;
            }
            expected = expected
                .checked_add(1)
                .ok_or_else(|| SnapshotError::CorruptStorage("event offset overflow".into()))?;
        }
        let next = records.last().map_or(after, |record| record.cursor.offset);
        Ok(Page {
            records,
            next_after: Cursor::new(protection.stream.clone(), next),
            through: Cursor::new(protection.stream.clone(), protection.through),
            complete: next == protection.through,
        })
    }

    async fn release_recovery(&self, lease: RecoveryLeaseId) -> SnapshotResult<RecoveryRelease> {
        let mut state = self.lock().map_err(snapshot_store_error)?;
        if state.closed {
            return Err(SnapshotError::Closed);
        }
        Ok(if state.recovery_leases.remove(&lease).is_some() {
            RecoveryRelease::Released
        } else {
            RecoveryRelease::AlreadyReleased
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn append_rejects_offset_overflow_without_mutating_history() {
        let store = MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap();
        let key = store
            .create_if_absent(&StreamId::new("overflow").unwrap())
            .await
            .unwrap();
        {
            let mut state = store.state.lock().unwrap();
            let StreamEntry::Active(history) = state.streams.get_mut(&key.id).unwrap() else {
                panic!("fixture stream must be active");
            };
            history.tail = u64::MAX;
        }
        let event = NewEvent {
            id: EventId::new("overflow-event").unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("test.bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(b"unchanged"),
        };

        assert!(matches!(
            store.append_atomic(&key, event.clone()).await,
            Err(Error::OffsetOverflow)
        ));
        assert_eq!(store.lookup_event(&key, &event.id).await.unwrap(), None);
        let state = store.state.lock().unwrap();
        assert_eq!(state.history_records, 0);
        assert_eq!(state.history_bytes, 0);
        let StreamEntry::Active(history) = state.streams.get(&key.id).unwrap() else {
            panic!("fixture stream must be active");
        };
        assert!(history.records.is_empty());
    }

    #[tokio::test]
    async fn range_read_reports_middle_and_suffix_holes() {
        let store = MemoryStore::open(MemoryStoreOptions::default())
            .await
            .unwrap();
        let key = store
            .create_if_absent(&StreamId::new("corrupt-range").unwrap())
            .await
            .unwrap();
        for index in 1..=3 {
            store
                .append_atomic(
                    &key,
                    NewEvent {
                        id: EventId::new(format!("event-{index}")).unwrap(),
                        schema: SchemaRef {
                            id: SchemaId::new("test.bytes").unwrap(),
                            version: 1,
                        },
                        payload: Payload::copy_from_slice(&[index]),
                    },
                )
                .await
                .unwrap();
        }

        {
            let mut state = store.state.lock().unwrap();
            let StreamEntry::Active(history) = state.streams.get_mut(&key.id).unwrap() else {
                panic!("fixture stream must be active");
            };
            history.records.remove(&2);
        }
        assert!(matches!(
            store
                .read_range(
                    &key,
                    0,
                    3,
                    PageLimits {
                        max_records: 3,
                        max_bytes: 1024,
                    },
                )
                .await,
            Err(Error::StoreCorrupt(detail)) if detail.contains("offset 2")
        ));

        {
            let mut state = store.state.lock().unwrap();
            let StreamEntry::Active(history) = state.streams.get_mut(&key.id).unwrap() else {
                panic!("fixture stream must be active");
            };
            history.records.insert(
                2,
                Arc::new(Record {
                    cursor: Cursor::new(key.clone(), 2),
                    event: NewEvent {
                        id: EventId::new("event-2").unwrap(),
                        schema: SchemaRef {
                            id: SchemaId::new("test.bytes").unwrap(),
                            version: 1,
                        },
                        payload: Payload::copy_from_slice(&[2]),
                    },
                }),
            );
            history.records.remove(&3);
        }
        assert!(matches!(
            store
                .read_range(
                    &key,
                    0,
                    3,
                    PageLimits {
                        max_records: 3,
                        max_bytes: 1024,
                    },
                )
                .await,
            Err(Error::StoreCorrupt(detail)) if detail.contains("offset 3")
        ));
    }
}
