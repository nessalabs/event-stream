use crate::{
    application::{
        AppendBatch, Error, EventStore, LifecycleStore, PersistenceProfile, Result,
        StoreCapabilities,
    },
    domain::*,
};
use async_trait::async_trait;
use fs2::FileExt;
use rusqlite::{
    params, types::ValueRef, Connection, ErrorCode, OptionalExtension, TransactionBehavior,
};
use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex, OnceLock,
    },
    thread,
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Notify};

#[cfg(feature = "replication")]
use super::sqlite_replication::{ReplicationCommand, SqliteReplicationState};
#[cfg(feature = "retention")]
use super::sqlite_retention::RetentionCommand;
#[cfg(feature = "snapshots")]
use super::sqlite_snapshot::{SnapshotCommand, SqliteSnapshotState};
#[cfg(feature = "source-journal")]
use super::sqlite_source_journal::JournalCommand;
#[cfg(feature = "retention")]
use crate::application::RetentionStoreConfig;
#[cfg(feature = "source-journal")]
use crate::application::SourceJournalStoreConfig;
#[cfg(feature = "replication")]
use crate::application::{
    DurableReplicationClock, ReplicaDestinationConfig, ReplicationStoreConfig,
};
#[cfg(feature = "snapshots")]
use crate::application::{MonotonicClock, SnapshotStoreConfig};

pub const SQLITE_FORMAT_VERSION: u32 = 2;
const OPEN: u8 = 0;
const CLOSING: u8 = 1;
const CLOSED: u8 = 2;
const FAULTED: u8 = 3;
const LOOKUP_RECORD_SQL: &str = "SELECT octet_length(event_id),octet_length(schema_id),octet_length(payload),
    CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?3 THEN event_id END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?3 THEN schema_id END,
    CASE WHEN typeof(schema_version)='integer' THEN schema_version END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?3 THEN payload END
    FROM event_records WHERE stream_key=?1 AND event_id=?2";
#[cfg(not(feature = "retention"))]
const READ_RECORD_SQL: &str = "SELECT octet_length(event_id),octet_length(schema_id),octet_length(payload),
    CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN event_id END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN schema_id END,
    CASE WHEN typeof(schema_version)='integer' THEN schema_version END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN payload END
    FROM event_records WHERE stream_key=?1 AND offset>?2 AND offset<=?3 ORDER BY offset LIMIT ?4";
#[cfg(feature = "retention")]
const READ_RECORD_SQL: &str = "SELECT octet_length(event_id),octet_length(schema_id),octet_length(payload),
    CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN event_id END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN schema_id END,
    CASE WHEN typeof(schema_version)='integer' THEN schema_version END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN payload END,
    offset
    FROM event_records WHERE stream_key=?1 AND offset>?2 AND offset<=?3
    UNION ALL
    SELECT octet_length(event_id),octet_length(schema_id),octet_length(payload),
    CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN event_id END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN schema_id END,
    CASE WHEN typeof(schema_version)='integer' THEN schema_version END,
    CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(payload)='blob'
        AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
        AND octet_length(payload)+octet_length(event_id)+octet_length(schema_id)+128<=?5 THEN payload END,
    offset
    FROM retention_generated_records WHERE stream_key=?1 AND offset>?2 AND offset<=?3
    ORDER BY 9 LIMIT ?4";

#[derive(Clone, Debug)]
pub struct SqliteOptions {
    pub path: PathBuf,
    pub max_record_bytes: usize,
    pub worker_queue_capacity: usize,
    pub max_page_records: usize,
    pub max_page_bytes: usize,
    pub sqlite_cache_kib: u32,
    /// SQLite database-page quota. Journal files may temporarily use more disk space.
    pub max_database_pages: u32,
    pub busy_timeout: Duration,
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
    pub replication_clock: Arc<dyn DurableReplicationClock>,
    /// Deterministic one-shot failure hook for adapter boundary tests.
    #[doc(hidden)]
    pub failure_injection: Option<SqliteFailureInjection>,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteFailureInjection {
    BeforeRecordInsert,
    AfterRecordInsert,
    BeforeCommit,
    AfterCommitAcknowledgementLost,
    PauseAfterCommitAcknowledgementLost(Duration),
    PauseBeforeCommit(Duration),
    BeforeLifecycleCommit,
    AfterLifecycleCommitAcknowledgementLost,
    BeforeCleanupCommit,
    BeforeMigrationCommit,
    BeforeSnapshotCommit,
    AfterSnapshotCommitAcknowledgementLost,
    PauseAfterSnapshotCommitAcknowledgementLost(Duration),
    BeforeRetentionCommit,
    AfterRetentionCommitAcknowledgementLost,
    BeforeJournalOutputCommit,
    AfterJournalOutputCommitAcknowledgementLost,
    PauseAfterJournalOutputCommitAcknowledgementLost(Duration),
    BeforeJournalCaptureCommit,
    AfterJournalCaptureCommitAcknowledgementLost,
    BeforeJournalCheckpointCommit,
    AfterJournalCheckpointCommitAcknowledgementLost,
    BeforeJournalSealCommit,
    AfterJournalSealCommitAcknowledgementLost,
    BeforeJournalFinishCommit,
    AfterJournalFinishCommitAcknowledgementLost,
    BeforeReplicaCommit,
    AfterReplicaCommitAcknowledgementLost,
}
impl SqliteOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            max_record_bytes: 1024 * 1024,
            worker_queue_capacity: 256,
            max_page_records: 1024,
            max_page_bytes: 4 * 1024 * 1024,
            sqlite_cache_kib: 4096,
            max_database_pages: 262_144,
            busy_timeout: Duration::from_secs(5),
            max_lifecycle_receipts: 10_000,
            max_lifecycle_receipt_bytes: 4 * 1024 * 1024,
            max_retired_lifetimes: 10_000,
            max_retired_metadata_bytes: 4 * 1024 * 1024,
            #[cfg(feature = "snapshots")]
            snapshots: SnapshotStoreConfig::default(),
            #[cfg(feature = "snapshots")]
            snapshot_clock: Arc::new(super::ProcessMonotonicClock::default()),
            #[cfg(feature = "retention")]
            retention: RetentionStoreConfig::default(),
            #[cfg(feature = "source-journal")]
            source_journal: SourceJournalStoreConfig::default(),
            #[cfg(feature = "replication")]
            replication: ReplicationStoreConfig::default(),
            #[cfg(feature = "replication")]
            replica_destination: ReplicaDestinationConfig::default(),
            #[cfg(feature = "replication")]
            replication_clock: Arc::new(super::ProcessDurableReplicationClock),
            failure_injection: None,
        }
    }
    fn validate(&self) -> Result<()> {
        if self.max_record_bytes == 0 {
            return Err(invalid("max_record_bytes must be positive"));
        }
        if self.worker_queue_capacity == 0 {
            return Err(invalid("worker_queue_capacity must be positive"));
        }
        if self.max_page_records == 0 || self.max_page_bytes == 0 {
            return Err(invalid("SQLite page limits must be positive"));
        }
        if self.sqlite_cache_kib == 0 || self.sqlite_cache_kib > i32::MAX as u32 {
            return Err(invalid("sqlite_cache_kib is out of range"));
        }
        if self.max_database_pages == 0 || self.max_database_pages > i32::MAX as u32 {
            return Err(invalid("max_database_pages is out of range"));
        }
        if self.busy_timeout.is_zero() {
            return Err(invalid("busy_timeout must be positive"));
        }
        if self.busy_timeout.as_millis() > i32::MAX as u128 {
            return Err(invalid("busy_timeout is out of range"));
        }
        if self.max_lifecycle_receipts == 0
            || self.max_lifecycle_receipt_bytes == 0
            || self.max_retired_lifetimes == 0
            || self.max_retired_metadata_bytes == 0
        {
            return Err(invalid("SQLite lifecycle limits must be positive"));
        }
        if self.max_lifecycle_receipts > i64::MAX as usize
            || self.max_lifecycle_receipt_bytes > i64::MAX as usize
            || self.max_retired_lifetimes > i64::MAX as usize
            || self.max_retired_metadata_bytes > i64::MAX as usize
        {
            return Err(invalid(
                "SQLite lifecycle limits exceed SQLite INTEGER range",
            ));
        }
        #[cfg(feature = "snapshots")]
        self.snapshots
            .validate()
            .map_err(|error| invalid(&error.to_string()))?;
        #[cfg(feature = "retention")]
        self.retention
            .validate()
            .map_err(|error| invalid(&error.to_string()))?;
        #[cfg(feature = "source-journal")]
        self.source_journal
            .validate()
            .map_err(|error| invalid(&error.to_string()))?;
        #[cfg(feature = "replication")]
        {
            self.replication
                .validate()
                .map_err(|error| invalid(&error.to_string()))?;
            self.replica_destination
                .validate()
                .map_err(|error| invalid(&error.to_string()))?;
        }
        Ok(())
    }
}
fn invalid(message: &str) -> Error {
    Error::InvalidConfig(message.into())
}

/// A single-worker SQLite adapter with bounded queues and caches.
///
/// The verified profile covers committed data across normal restart and process
/// termination. It does not claim survival across device power loss.
pub struct SqliteStore {
    inner: Arc<Inner>,
    capabilities: StoreCapabilities,
    pub(super) limits: Limits,
    #[cfg(feature = "snapshots")]
    pub(super) snapshot_config: SnapshotStoreConfig,
}
struct Inner {
    tx: mpsc::Sender<Command>,
    gate: Mutex<()>,
    shared: Arc<Shared>,
}
struct Shared {
    state: AtomicU8,
    stopped: Notify,
}
#[derive(Clone, Copy)]
pub(super) struct Limits {
    pub(super) record: usize,
    page_records: usize,
    page_bytes: usize,
}
pub(super) enum Command {
    Create(StreamId, oneshot::Sender<Result<StreamKey>>),
    Find(StreamId, oneshot::Sender<Result<Option<StreamKey>>>),
    Append(StreamKey, NewEvent, oneshot::Sender<Result<AppendReceipt>>),
    AppendBatch(
        Vec<(StreamKey, NewEvent)>,
        oneshot::Sender<Vec<Result<AppendReceipt>>>,
    ),
    Lookup(
        StreamKey,
        EventId,
        oneshot::Sender<Result<Option<Arc<Record>>>>,
    ),
    Bounds(StreamKey, oneshot::Sender<Result<Bounds>>),
    Read(
        StreamKey,
        u64,
        u64,
        PageLimits,
        oneshot::Sender<Result<Page>>,
    ),
    Lifecycle(LifecycleRequest, oneshot::Sender<Result<LifecycleReceipt>>),
    Cleanup(CleanupLimits, oneshot::Sender<Result<CleanupProgress>>),
    #[cfg(feature = "snapshots")]
    Snapshot(SnapshotCommand),
    #[cfg(feature = "retention")]
    Retention(RetentionCommand),
    #[cfg(feature = "source-journal")]
    Journal(JournalCommand),
    #[cfg(feature = "replication")]
    Replication(Box<ReplicationCommand>),
    Close(oneshot::Sender<Result<()>>),
}

pub(crate) struct Ownership {
    path: PathBuf,
    file: File,
}
static OWNED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
impl Ownership {
    pub(crate) fn acquire(path: &Path) -> Result<Self> {
        Self::acquire_with_alias_policy(path, false)
    }

    pub(crate) fn acquire_restore_destination(path: &Path) -> Result<Self> {
        Self::acquire_with_alias_policy(path, true)
    }

    fn acquire_with_alias_policy(path: &Path, allow_hardlink: bool) -> Result<Self> {
        let path = canonical_db(path, allow_hardlink)?;
        let registry = OWNED.get_or_init(|| Mutex::new(HashSet::new()));
        if !registry
            .lock()
            .map_err(|_| Error::StoreInUse)?
            .insert(path.clone())
        {
            return Err(Error::StoreInUse);
        }
        let result = (|| {
            let name = path
                .file_name()
                .and_then(|v| v.to_str())
                .ok_or_else(|| invalid("database file name must be UTF-8"))?;
            let lock_path = path.with_file_name(format!(".{name}.event-stream.lock"));
            if lock_path.exists()
                && std::fs::symlink_metadata(&lock_path)
                    .map_err(|e| Error::StoreWriteFailed(format!("inspect ownership lock: {e}")))?
                    .file_type()
                    .is_symlink()
            {
                return Err(invalid("ownership lock may not be a symbolic link"));
            }
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(lock_path)
                .map_err(|e| Error::StoreWriteFailed(format!("open ownership lock: {e}")))?;
            file.try_lock_exclusive().map_err(|_| Error::StoreInUse)?;
            Ok(Self {
                path: path.clone(),
                file,
            })
        })();
        if result.is_err() {
            remove_owned(&path);
        }
        result
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}
impl Drop for Ownership {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
        remove_owned(&self.path);
    }
}
fn remove_owned(path: &Path) {
    if let Some(registry) = OWNED.get() {
        if let Ok(mut paths) = registry.lock() {
            paths.remove(path);
        }
    }
}
fn canonical_db(path: &Path, allow_hardlink: bool) -> Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| invalid("database path needs a file name"))?;
    if !allow_hardlink {
        let reserved_owner = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| {
                let body = name
                    .strip_prefix(".event-stream-restore-")?
                    .strip_suffix(".sqlite3")?;
                let (operation, request) = body.split_once('-')?;
                let hexadecimal = |value: &str| {
                    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
                };
                if !hexadecimal(operation) || !hexadecimal(request) {
                    return None;
                }
                Some(path.with_file_name(format!(".event-stream-restore-{operation}.owner")))
            });
        if reserved_owner.is_some_and(|owner| owner.exists()) {
            return Err(invalid(
                "database path is reserved by an incomplete restore",
            ));
        }
    }
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path)
            .map_err(|e| Error::StoreWriteFailed(format!("inspect database: {e}")))?;
        if metadata.file_type().is_symlink() {
            return Err(invalid("database path may not be a symbolic link"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if !allow_hardlink && metadata.nlink() != 1 {
                return Err(invalid("hard-linked database files are unsupported"));
            }
        }
    }
    let parent = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|e| Error::InvalidConfig(format!("database parent is unavailable: {e}")))?;
    Ok(parent.join(name))
}

#[async_trait]
impl EventStore for SqliteStore {
    type Options = SqliteOptions;
    async fn open(options: Self::Options) -> Result<Self> {
        options.validate()?;
        let ownership = Ownership::acquire(&options.path)?;
        let path = ownership.path.clone();
        let limits = Limits {
            record: options.max_record_bytes,
            page_records: options.max_page_records,
            page_bytes: options.max_page_bytes,
        };
        let (tx, rx) = mpsc::channel(options.worker_queue_capacity);
        let shared = Arc::new(Shared {
            state: AtomicU8::new(OPEN),
            stopped: Notify::new(),
        });
        let inner = Arc::new(Inner {
            tx,
            gate: Mutex::new(()),
            shared: shared.clone(),
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        #[cfg(feature = "snapshots")]
        let snapshot_config = options.snapshots.clone();
        thread::Builder::new()
            .name("event-stream-sqlite".into())
            .spawn(move || worker(path, options, ownership, rx, shared, ready_tx))
            .map_err(|e| Error::StoreWriteFailed(format!("start SQLite worker: {e}")))?;
        ready_rx
            .await
            .map_err(|_| Error::StoreWriteFailed("SQLite worker stopped during open".into()))??;
        Ok(Self {
            inner,
            limits,
            capabilities: StoreCapabilities {
                persistence: PersistenceProfile::ProcessRestart,
                format_version: SQLITE_FORMAT_VERSION,
                max_record_bytes: limits.record,
                max_concurrent_reads: 1,
                max_concurrent_writes: 1,
                ownership: "exclusive in-process registry and OS file lock through worker shutdown",
            },
            #[cfg(feature = "snapshots")]
            snapshot_config,
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.capabilities.clone()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Create(id.clone(), tx))?;
        recv(rx).await
    }
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Find(id.clone(), tx))?;
        recv(rx).await
    }
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        if event.accounted_bytes() > self.limits.record {
            return Err(Error::PayloadTooLarge);
        }
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Append(stream.clone(), event, tx))?;
        recv(rx).await
    }
    async fn append_batch(&self, batch: &AppendBatch) -> Vec<Result<AppendReceipt>> {
        let count = batch.items().len();
        if batch.accounted_bytes() > self.limits.record {
            return vec![Err(Error::CapacityExceeded); count];
        }
        let items = batch
            .items()
            .iter()
            .map(|item| (item.stream.clone(), item.event.clone()))
            .collect();
        let (tx, rx) = oneshot::channel();
        if let Err(error) = self.submit(Command::AppendBatch(items, tx)) {
            return vec![Err(error); count];
        }
        recv_append_batch(rx, batch).await
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Lookup(stream.clone(), id.clone(), tx))?;
        recv(rx).await
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Bounds(stream.clone(), tx))?;
        recv(rx).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(invalid("page limits must be positive"));
        }
        if limits.max_records > self.limits.page_records
            || limits.max_bytes > self.limits.page_bytes
        {
            return Err(Error::CapacityExceeded);
        }
        if after > through {
            return Err(invalid("after must not exceed through"));
        }
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Read(stream.clone(), after, through, limits, tx))?;
        recv(rx).await
    }
    async fn close(&self) -> Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let first = {
            let _gate = self.inner.gate.lock().map_err(|_| Error::OwnershipLost)?;
            let first = self
                .inner
                .shared
                .state
                .compare_exchange(OPEN, CLOSING, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            if first {
                match self.inner.tx.try_send(Command::Close(reply_tx)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(command)) => {
                        // The detached sender makes shutdown cancellation-safe. Once
                        // state changes to CLOSING, a dropped close future cannot
                        // prevent the close command from following accepted work.
                        let sender = self.inner.tx.clone();
                        thread::spawn(move || {
                            let _ = sender.blocking_send(command);
                        });
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return self.closed_result(),
                }
            }
            first
        };
        if first {
            return recv(reply_rx).await;
        }
        self.wait_stopped().await;
        self.closed_result()
    }
}

#[async_trait]
impl LifecycleStore for SqliteStore {
    async fn change_lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleReceipt> {
        request.receipt_charge().ok_or(Error::CapacityExceeded)?;
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Lifecycle(request, tx))?;
        recv(rx).await
    }

    async fn cleanup_retired(&self, limits: CleanupLimits) -> Result<CleanupProgress> {
        if limits.max_records == 0 || limits.max_bytes == 0 {
            return Err(invalid("cleanup limits must be nonzero"));
        }
        if self
            .limits
            .record
            .checked_add(256)
            .is_none_or(|maximum| maximum > limits.max_bytes)
        {
            return Err(invalid(
                "cleanup bytes must fit one maximum-size stored record",
            ));
        }
        if limits.max_records > self.limits.page_records
            || limits.max_bytes > self.limits.page_bytes
        {
            return Err(Error::CapacityExceeded);
        }
        let (tx, rx) = oneshot::channel();
        self.submit(Command::Cleanup(limits, tx))?;
        recv(rx).await
    }
}
impl SqliteStore {
    pub(super) fn submit(&self, command: Command) -> Result<()> {
        let _gate = self.inner.gate.lock().map_err(|_| Error::OwnershipLost)?;
        match self.inner.shared.state.load(Ordering::Acquire) {
            OPEN => self.inner.tx.try_send(command).map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => Error::Overloaded,
                _ => Error::OwnershipLost,
            }),
            CLOSING | CLOSED => Err(Error::Closed),
            _ => Err(Error::OwnershipLost),
        }
    }
    async fn wait_stopped(&self) {
        loop {
            let notified = self.inner.shared.stopped.notified();
            if matches!(
                self.inner.shared.state.load(Ordering::Acquire),
                CLOSED | FAULTED
            ) {
                return;
            }
            notified.await;
        }
    }
    fn closed_result(&self) -> Result<()> {
        match self.inner.shared.state.load(Ordering::Acquire) {
            CLOSED => Ok(()),
            FAULTED => Err(Error::OwnershipLost),
            _ => Err(Error::Closed),
        }
    }
}
pub(super) async fn recv<T>(rx: oneshot::Receiver<Result<T>>) -> Result<T> {
    rx.await.unwrap_or(Err(Error::OwnershipLost))
}

fn worker(
    path: PathBuf,
    options: SqliteOptions,
    ownership: Ownership,
    mut rx: mpsc::Receiver<Command>,
    shared: Arc<Shared>,
    ready: oneshot::Sender<Result<()>>,
) {
    let conn = match open_connection(&path, &options) {
        Ok(c) => {
            let _ = ready.send(Ok(()));
            c
        }
        Err(e) => {
            let _ = ready.send(Err(e));
            shared.state.store(FAULTED, Ordering::Release);
            shared.stopped.notify_waiters();
            return;
        }
    };
    let mut conn = Some(conn);
    let mut failure_injection = options.failure_injection;
    #[cfg(feature = "snapshots")]
    let mut snapshot_state = SqliteSnapshotState::new();
    #[cfg(feature = "replication")]
    let mut replication_state = SqliteReplicationState::new();
    let mut close_reply = None;
    let mut fatal = false;
    while let Some(command) = rx.blocking_recv() {
        match command {
            Command::Create(id, tx) => {
                let _ = tx.send(create_stream(conn.as_mut().unwrap(), id));
            }
            Command::Find(id, tx) => {
                let _ = tx.send(find_stream(conn.as_ref().unwrap(), &id));
            }
            Command::Append(stream, event, tx) => {
                let (r, f) = append(
                    conn.as_mut().unwrap(),
                    stream,
                    event,
                    &mut failure_injection,
                    &options,
                );
                let _ = tx.send(r);
                if f {
                    fatal = true;
                    break;
                }
            }
            Command::AppendBatch(items, tx) => {
                let (results, failed_fatally) = append_batch(
                    conn.as_mut().unwrap(),
                    items,
                    &mut failure_injection,
                    &options,
                );
                let _ = tx.send(results);
                if failed_fatally {
                    fatal = true;
                    break;
                }
            }
            Command::Lookup(stream, id, tx) => {
                let _ = tx.send(lookup(
                    conn.as_ref().unwrap(),
                    &stream,
                    &id,
                    options.max_record_bytes,
                ));
            }
            Command::Bounds(stream, tx) => {
                let _ = tx.send(bounds(conn.as_ref().unwrap(), &stream));
            }
            Command::Read(stream, after, through, limits, tx) => {
                let _ = tx.send(read_page(
                    conn.as_ref().unwrap(),
                    &stream,
                    after,
                    through,
                    limits,
                    options.max_record_bytes,
                ));
            }
            Command::Lifecycle(request, tx) => {
                let _ = tx.send(change_lifecycle(
                    conn.as_mut().unwrap(),
                    request,
                    &options,
                    &mut failure_injection,
                ));
            }
            Command::Cleanup(limits, tx) => {
                let _ = tx.send(cleanup_retired(
                    conn.as_mut().unwrap(),
                    limits,
                    &mut failure_injection,
                    #[cfg(feature = "snapshots")]
                    &mut snapshot_state,
                    #[cfg(feature = "snapshots")]
                    &options,
                ));
            }
            #[cfg(feature = "snapshots")]
            Command::Snapshot(command) => {
                super::sqlite_snapshot::handle_snapshot_command(
                    conn.as_mut().unwrap(),
                    command,
                    &options,
                    &mut snapshot_state,
                    &mut failure_injection,
                );
            }
            #[cfg(feature = "retention")]
            Command::Retention(command) => {
                super::sqlite_retention::handle_retention_command(
                    conn.as_mut().unwrap(),
                    command,
                    &options,
                    &mut snapshot_state,
                    &mut failure_injection,
                );
            }
            #[cfg(feature = "source-journal")]
            Command::Journal(command) => {
                super::sqlite_source_journal::handle_journal_command(
                    conn.as_mut().unwrap(),
                    command,
                    &options,
                    &mut failure_injection,
                );
            }
            #[cfg(feature = "replication")]
            Command::Replication(command) => {
                super::sqlite_replication::handle_replication_command(
                    conn.as_mut().unwrap(),
                    *command,
                    &options,
                    &mut replication_state,
                    &mut snapshot_state,
                    &mut failure_injection,
                );
            }
            Command::Close(tx) => {
                close_reply = Some(tx);
                break;
            }
        }
    }
    if fatal {
        while let Ok(command) = rx.try_recv() {
            fail(command);
        }
    }
    let close_result = conn
        .take()
        .unwrap()
        .close()
        .map_err(|(_, e)| write_error("close", e));
    drop(ownership);
    shared.state.store(
        if fatal || close_result.is_err() {
            FAULTED
        } else {
            CLOSED
        },
        Ordering::Release,
    );
    shared.stopped.notify_waiters();
    if let Some(tx) = close_reply {
        let _ = tx.send(close_result);
    }
}
fn fail(command: Command) {
    let e = || Error::RuntimeFaulted("SQLite connection cannot be safely reused".into());
    match command {
        Command::Create(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::Find(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::Append(_, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::AppendBatch(items, tx) => {
            let _ = tx.send(vec![Err(e()); items.len()]);
        }
        Command::Lookup(_, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::Bounds(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::Read(_, _, _, _, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::Lifecycle(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        Command::Cleanup(_, tx) => {
            let _ = tx.send(Err(e()));
        }
        #[cfg(feature = "snapshots")]
        Command::Snapshot(command) => super::sqlite_snapshot::fail_snapshot_command(command),
        #[cfg(feature = "retention")]
        Command::Retention(command) => super::sqlite_retention::fail_retention_command(command),
        #[cfg(feature = "source-journal")]
        Command::Journal(command) => super::sqlite_source_journal::fail_journal_command(command),
        #[cfg(feature = "replication")]
        Command::Replication(command) => {
            super::sqlite_replication::fail_replication_command(*command)
        }
        Command::Close(tx) => {
            let _ = tx.send(Err(e()));
        }
    }
}

fn open_connection(path: &Path, options: &SqliteOptions) -> Result<Connection> {
    let conn = Connection::open(path).map_err(|e| write_error("open database", e))?;
    conn.set_prepared_statement_cache_capacity(8);
    conn.busy_timeout(options.busy_timeout)
        .map_err(|e| write_error("set busy timeout", e))?;
    let encoding: String = conn
        .pragma_query_value(None, "encoding", |row| row.get(0))
        .map_err(|e| corrupt("read database encoding", e))?;
    if !encoding.eq_ignore_ascii_case("UTF-8") {
        return Err(Error::StoreCorrupt(format!(
            "storage format requires UTF-8 database encoding, found {encoding}"
        )));
    }
    // Reject foreign and newer formats before pragmas that persistently change
    // database state. Opening SQLite may still perform its own crash recovery.
    let existing_version = inspect_existing_format(&conn)?;
    conn.pragma_update(None, "journal_mode", "DELETE")
        .map_err(|e| write_error("set journal mode", e))?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|e| write_error("set synchronous mode", e))?;
    conn.pragma_update(None, "foreign_keys", true)
        .map_err(|e| write_error("enable foreign keys", e))?;
    conn.pragma_update(None, "cache_size", -(options.sqlite_cache_kib as i64))
        .map_err(|e| write_error("set cache size", e))?;
    conn.pragma_update(None, "max_page_count", options.max_database_pages)
        .map_err(|e| write_error("set database quota", e))?;
    conn.pragma_update(None, "temp_store", "MEMORY")
        .map_err(|e| write_error("set temp store", e))?;
    initialize_schema(&conn, existing_version, options.failure_injection)?;
    validate_lifecycle_counters(&conn)?;
    #[cfg(feature = "snapshots")]
    super::sqlite_snapshot::initialize_snapshot_schema(&conn)?;
    #[cfg(feature = "retention")]
    super::sqlite_retention::initialize_retention_schema(&conn)?;
    #[cfg(feature = "source-journal")]
    super::sqlite_source_journal::initialize_journal_schema(&conn)?;
    #[cfg(feature = "replication")]
    super::sqlite_replication::initialize_replication_schema(&conn, options)?;
    #[cfg(not(feature = "source-journal"))]
    reject_source_journal_schema(&conn)?;
    #[cfg(not(feature = "retention"))]
    reject_retention_schema(&conn)?;
    #[cfg(not(feature = "replication"))]
    reject_replication_schema(&conn)?;
    verify_configuration(&conn, options)?;
    Ok(conn)
}

#[cfg(not(feature = "replication"))]
fn reject_replication_schema(conn: &Connection) -> Result<()> {
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE 'replication_%'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect replication schema", error))?;
    if count == 0 {
        Ok(())
    } else {
        Err(Error::StoreCorrupt(
            "database contains replication state but replication support is disabled".into(),
        ))
    }
}

#[cfg(not(feature = "source-journal"))]
fn reject_source_journal_schema(conn: &Connection) -> Result<()> {
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('journal_metadata','journal_sources','journal_segments','journal_capture_receipts',
              'journal_markers','journal_operations')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect source journal schema", error))?;
    if count == 0 {
        Ok(())
    } else {
        Err(Error::StoreCorrupt(
            "database contains source-journal state but source-journal support is disabled".into(),
        ))
    }
}

#[cfg(not(feature = "retention"))]
fn reject_retention_schema(conn: &Connection) -> Result<()> {
    let count: u32 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('retention_metadata','retention_streams','retention_retry_identities',
              'retention_generated_records','retention_receipts','retention_cleanup')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| corrupt("inspect retention schema", error))?;
    if count != 0 {
        return Err(Error::StoreCorrupt(
            "database contains retention state but retention support is disabled".into(),
        ));
    }
    Ok(())
}
fn initialize_schema(
    conn: &Connection,
    existing_version: Option<u32>,
    failure_injection: Option<SqliteFailureInjection>,
) -> Result<()> {
    if existing_version == Some(SQLITE_FORMAT_VERSION) {
        return ensure_restore_extension(conn, failure_injection);
    }
    if existing_version == Some(1) {
        return migrate_format_one(conn, failure_injection);
    }
    // ADR 0003's reproducible probe is examples/sqlite_schema_probe.rs. Results
    // vary with payload size, and no workload mix is selected. The ordinary
    // rowid table is the simpler SQLite baseline when neither layout wins all
    // measured cases.
    conn.execute_batch("BEGIN IMMEDIATE;
      CREATE TABLE IF NOT EXISTS event_stream_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),format_version INTEGER NOT NULL,lifecycle_receipt_count INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_count>=0),lifecycle_receipt_bytes INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_bytes>=0),retired_lifetime_count INTEGER NOT NULL DEFAULT 0 CHECK(retired_lifetime_count>=0),retired_metadata_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retired_metadata_bytes>=0),restore_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(restore_incomplete IN (0,1,2)));
      CREATE TABLE IF NOT EXISTS event_streams(stream_key INTEGER PRIMARY KEY,public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),floor BLOB NOT NULL CHECK(length(floor)=8),tail BLOB NOT NULL CHECK(length(tail)=8),retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),UNIQUE(public_id,incarnation));
      CREATE TABLE IF NOT EXISTS event_records(stream_key INTEGER NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),event_id TEXT NOT NULL,schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(stream_key,offset),UNIQUE(stream_key,event_id),FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
      CREATE TABLE IF NOT EXISTS event_stream_names(public_id TEXT PRIMARY KEY,latest_incarnation BLOB NOT NULL CHECK(length(latest_incarnation)=16),active_stream_key INTEGER);
      CREATE TABLE IF NOT EXISTS lifecycle_receipts(operation_id TEXT PRIMARY KEY,action INTEGER NOT NULL CHECK(action IN (0,1)),expected_public_id TEXT NOT NULL,expected_incarnation BLOB NOT NULL CHECK(length(expected_incarnation)=16),replacement_incarnation BLOB CHECK(replacement_incarnation IS NULL OR length(replacement_incarnation)=16),charge INTEGER NOT NULL CHECK(charge>=0));
      CREATE TABLE IF NOT EXISTS restore_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),operation_id TEXT NOT NULL,backup_identity BLOB NOT NULL CHECK(length(backup_identity)=32),destination TEXT NOT NULL,mapping_count INTEGER NOT NULL CHECK(mapping_count>=0));
      CREATE TABLE IF NOT EXISTS restore_mappings(old_public_id TEXT NOT NULL,old_incarnation BLOB NOT NULL CHECK(length(old_incarnation)=16),new_incarnation BLOB NOT NULL CHECK(length(new_incarnation)=16),PRIMARY KEY(old_public_id,old_incarnation));
      CREATE INDEX IF NOT EXISTS event_streams_retired_idx ON event_streams(retired,stream_key);
      INSERT OR IGNORE INTO event_stream_metadata(singleton,format_version) VALUES(1,2); COMMIT;")
        .map_err(|e| write_error("initialize schema", e))?;
    Ok(())
}

fn ensure_restore_extension(
    conn: &Connection,
    failure_injection: Option<SqliteFailureInjection>,
) -> Result<()> {
    if has_restore_extension(conn)? {
        return Ok(());
    }
    let extension = (|| {
        conn.execute_batch(
            "BEGIN IMMEDIATE;
             ALTER TABLE event_stream_metadata ADD COLUMN restore_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(restore_incomplete IN (0,1,2));
             CREATE TABLE restore_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),operation_id TEXT NOT NULL,backup_identity BLOB NOT NULL CHECK(length(backup_identity)=32),destination TEXT NOT NULL,mapping_count INTEGER NOT NULL CHECK(mapping_count>=0));
             CREATE TABLE restore_mappings(old_public_id TEXT NOT NULL,old_incarnation BLOB NOT NULL CHECK(length(old_incarnation)=16),new_incarnation BLOB NOT NULL CHECK(length(new_incarnation)=16),PRIMARY KEY(old_public_id,old_incarnation));",
        )
        .map_err(|e| write_error("extend format 2 restore schema", e))?;
        if matches!(
            failure_injection,
            Some(SqliteFailureInjection::BeforeMigrationCommit)
        ) {
            return Err(Error::StoreWriteFailed(
                "injected failure before format 2 extension commit".into(),
            ));
        }
        conn.execute_batch("COMMIT;")
            .map_err(|e| write_error("commit format 2 restore extension", e))
    })();
    if extension.is_err() && !conn.is_autocommit() {
        let _ = conn.execute_batch("ROLLBACK;");
    }
    extension
}

fn has_restore_extension(conn: &Connection) -> Result<bool> {
    let restore_tables: u32 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('restore_metadata','restore_mappings')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| corrupt("inspect restore schema", e))?;
    let restore_column: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('event_stream_metadata') WHERE name='restore_incomplete')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| corrupt("inspect restore state column", e))?;
    match (restore_tables, restore_column) {
        (0, false) => Ok(false),
        (2, true) => Ok(true),
        _ => Err(Error::StoreCorrupt(
            "format 2 restore schema is only partially present".into(),
        )),
    }
}

fn migrate_format_one(
    conn: &Connection,
    failure_injection: Option<SqliteFailureInjection>,
) -> Result<()> {
    conn.execute_batch("PRAGMA foreign_keys=OFF;")
        .map_err(|e| write_error("disable foreign keys for migration", e))?;
    let migration = (|| {
        conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE event_streams_v2(stream_key INTEGER PRIMARY KEY,public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),floor BLOB NOT NULL CHECK(length(floor)=8),tail BLOB NOT NULL CHECK(length(tail)=8),retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),UNIQUE(public_id,incarnation));
         INSERT INTO event_streams_v2(stream_key,public_id,incarnation,floor,tail,retired) SELECT stream_key,public_id,incarnation,floor,tail,0 FROM event_streams;
         DROP TABLE event_streams;
         ALTER TABLE event_streams_v2 RENAME TO event_streams;
         CREATE TABLE event_stream_names(public_id TEXT PRIMARY KEY,latest_incarnation BLOB NOT NULL CHECK(length(latest_incarnation)=16),active_stream_key INTEGER);
         INSERT INTO event_stream_names(public_id,latest_incarnation,active_stream_key) SELECT public_id,incarnation,stream_key FROM event_streams;
         CREATE TABLE lifecycle_receipts(operation_id TEXT PRIMARY KEY,action INTEGER NOT NULL CHECK(action IN (0,1)),expected_public_id TEXT NOT NULL,expected_incarnation BLOB NOT NULL CHECK(length(expected_incarnation)=16),replacement_incarnation BLOB CHECK(replacement_incarnation IS NULL OR length(replacement_incarnation)=16),charge INTEGER NOT NULL CHECK(charge>=0));
         CREATE INDEX event_streams_retired_idx ON event_streams(retired,stream_key);
         ALTER TABLE event_stream_metadata ADD COLUMN lifecycle_receipt_count INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_count>=0);
         ALTER TABLE event_stream_metadata ADD COLUMN lifecycle_receipt_bytes INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_bytes>=0);
         ALTER TABLE event_stream_metadata ADD COLUMN retired_lifetime_count INTEGER NOT NULL DEFAULT 0 CHECK(retired_lifetime_count>=0);
         ALTER TABLE event_stream_metadata ADD COLUMN retired_metadata_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retired_metadata_bytes>=0);
         ALTER TABLE event_stream_metadata ADD COLUMN restore_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(restore_incomplete IN (0,1,2));
         CREATE TABLE restore_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),operation_id TEXT NOT NULL,backup_identity BLOB NOT NULL CHECK(length(backup_identity)=32),destination TEXT NOT NULL,mapping_count INTEGER NOT NULL CHECK(mapping_count>=0));
         CREATE TABLE restore_mappings(old_public_id TEXT NOT NULL,old_incarnation BLOB NOT NULL CHECK(length(old_incarnation)=16),new_incarnation BLOB NOT NULL CHECK(length(new_incarnation)=16),PRIMARY KEY(old_public_id,old_incarnation));
         UPDATE event_stream_metadata SET format_version=2 WHERE singleton=1;",
        )
        .map_err(|e| write_error("migrate format 1 to format 2", e))?;
        let violation: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                [],
                |r| r.get(0),
            )
            .map_err(|e| corrupt("check migrated foreign keys", e))?;
        if violation {
            return Err(Error::StoreCorrupt(
                "format migration produced a foreign-key violation".into(),
            ));
        }
        if matches!(
            failure_injection,
            Some(SqliteFailureInjection::BeforeMigrationCommit)
        ) {
            return Err(Error::StoreWriteFailed(
                "injected failure before migration commit".into(),
            ));
        }
        conn.execute_batch("COMMIT;")
            .map_err(|e| write_error("commit format migration", e))?;
        Ok(())
    })();
    if migration.is_err() && !conn.is_autocommit() {
        let _ = conn.execute_batch("ROLLBACK;");
    }
    let restore = conn.execute_batch("PRAGMA foreign_keys=ON;");
    match (migration, restore) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(write_error("restore foreign keys after migration", error)),
        (Err(error), Err(restore)) => Err(Error::StoreCorrupt(format!(
            "migration failed: {error}; restoring foreign keys also failed: {restore}"
        ))),
    }
}

/// Returns true when this is an existing supported event-stream database.
pub(crate) fn inspect_existing_format(conn: &Connection) -> Result<Option<u32>> {
    inspect_existing_format_state(conn, None)
}

pub(crate) fn inspect_restore_format(conn: &Connection, expected_state: i64) -> Result<()> {
    if inspect_existing_format_state(conn, Some(expected_state))? != Some(SQLITE_FORMAT_VERSION) {
        return Err(Error::StoreCorrupt(
            "restore staging has an unsupported storage format".into(),
        ));
    }
    Ok(())
}

fn inspect_existing_format_state(
    conn: &Connection,
    expected_restore_state: Option<i64>,
) -> Result<Option<u32>> {
    let metadata_exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='event_stream_metadata')",
            [],
            |row| row.get(0),
        )
        .map_err(|e| corrupt("inspect storage format", e))?;
    if metadata_exists {
        let version: Option<i64> = conn
            .query_row(
                "SELECT CASE WHEN typeof(format_version)='integer' THEN format_version END FROM event_stream_metadata WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(|e| corrupt("read format version", e))?;
        let version = version
            .ok_or_else(|| Error::StoreCorrupt("format version is not an integer".into()))?;
        let version = u32::try_from(version)
            .map_err(|_| Error::StoreCorrupt("format version is out of range".into()))?;
        if version != 1 && version != SQLITE_FORMAT_VERSION {
            return Err(Error::UnsupportedFormat(version));
        }
        let base_tables: u32 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('event_stream_metadata','event_streams','event_records','event_stream_names','lifecycle_receipts')",
                [],
                |row| row.get(0),
            )
            .map_err(|e| corrupt("validate storage schema", e))?;
        let expected_tables = if version == 1 { 3 } else { 5 };
        if base_tables != expected_tables {
            return Err(Error::StoreCorrupt(
                "format metadata exists but required tables are missing".into(),
            ));
        }
        let extended = version == SQLITE_FORMAT_VERSION && has_restore_extension(conn)?;
        if expected_restore_state.is_some() && !extended {
            return Err(Error::StoreCorrupt(
                "restore staging is missing its schema extension".into(),
            ));
        }
        if extended {
            let incomplete: Option<i64> = conn
                .query_row(
                    "SELECT CASE WHEN typeof(restore_incomplete)='integer' THEN restore_incomplete END FROM event_stream_metadata WHERE singleton=1",
                    [],
                    |row| row.get(0),
                )
                .map_err(|e| corrupt("read restore state", e))?;
            let expected = expected_restore_state.unwrap_or(0);
            if incomplete != Some(expected) {
                return Err(Error::StoreCorrupt(
                    "restore database is in an unexpected publication state".into(),
                ));
            }
        }
        return Ok(Some(version));
    }
    let application_tables: u32 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )
        .map_err(|e| corrupt("inspect empty database", e))?;
    if application_tables != 0 {
        return Err(Error::StoreCorrupt(
            "database has tables but no event-stream format metadata".into(),
        ));
    }
    Ok(None)
}
fn verify_configuration(conn: &Connection, options: &SqliteOptions) -> Result<()> {
    let engine: String = conn
        .query_row("SELECT sqlite_version()", [], |row| row.get(0))
        .map_err(|e| write_error("read SQLite engine version", e))?;
    if engine != rusqlite::version() {
        return Err(Error::InvalidConfig(format!(
            "SQLite engine version mismatch: connection={engine}, binding={}",
            rusqlite::version()
        )));
    }
    let journal: String = conn
        .pragma_query_value(None, "journal_mode", |r| r.get(0))
        .map_err(|e| write_error("read journal mode", e))?;
    let sync: i64 = conn
        .pragma_query_value(None, "synchronous", |r| r.get(0))
        .map_err(|e| write_error("read synchronous", e))?;
    let fk: i64 = conn
        .pragma_query_value(None, "foreign_keys", |r| r.get(0))
        .map_err(|e| write_error("read foreign keys", e))?;
    let cache_size: i64 = conn
        .pragma_query_value(None, "cache_size", |r| r.get(0))
        .map_err(|e| write_error("read cache size", e))?;
    let max_pages: u32 = conn
        .pragma_query_value(None, "max_page_count", |r| r.get(0))
        .map_err(|e| write_error("read database quota", e))?;
    let temp_store: i64 = conn
        .pragma_query_value(None, "temp_store", |r| r.get(0))
        .map_err(|e| write_error("read temp store", e))?;
    let busy_timeout: i64 = conn
        .pragma_query_value(None, "busy_timeout", |r| r.get(0))
        .map_err(|e| write_error("read busy timeout", e))?;
    if max_pages != options.max_database_pages {
        return Err(Error::CapacityExceeded);
    }
    if !journal.eq_ignore_ascii_case("delete")
        || sync != 2
        || fk != 1
        || cache_size != -(options.sqlite_cache_kib as i64)
        || temp_store != 2
        || busy_timeout != options.busy_timeout.as_millis() as i64
    {
        return Err(Error::InvalidConfig(format!("effective SQLite settings differ: journal_mode={journal}, synchronous={sync}, foreign_keys={fk}, cache_size={cache_size}, temp_store={temp_store}, busy_timeout={busy_timeout}")));
    }
    Ok(())
}

pub(crate) fn validate_lifecycle_counters(conn: &Connection) -> Result<()> {
    let stored: (i64, i64, i64, i64) = conn
        .query_row(
            "SELECT lifecycle_receipt_count,lifecycle_receipt_bytes,retired_lifetime_count,retired_metadata_bytes FROM event_stream_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(|e| corrupt("read lifecycle accounting", e))?;
    let actual_receipts: (i64, i64, i64) = conn
        .query_row(
            "SELECT count(*),
                    coalesce(sum(CASE WHEN typeof(charge)='integer' AND charge>=0 THEN charge END),0),
                    coalesce(sum(CASE WHEN typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256
                                           AND typeof(expected_public_id)='text' AND octet_length(expected_public_id) BETWEEN 1 AND 256
                                           AND typeof(expected_incarnation)='blob' AND length(expected_incarnation)=16
                                           AND typeof(action)='integer' AND action IN (0,1)
                                           AND ((action=0 AND replacement_incarnation IS NULL)
                                                OR (action=1 AND typeof(replacement_incarnation)='blob' AND length(replacement_incarnation)=16 AND replacement_incarnation!=expected_incarnation))
                                           AND typeof(charge)='integer'
                                           AND charge=octet_length(operation_id)+octet_length(expected_public_id)+256
                                      THEN 0 ELSE 1 END),0)
             FROM lifecycle_receipts",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| corrupt("verify lifecycle receipt accounting", e))?;
    let actual_retired: (i64, i64, i64) = conn
        .query_row(
            "SELECT count(*),coalesce(sum(octet_length(public_id)+256),0),
                    coalesce(sum(CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                                           AND typeof(incarnation)='blob' AND length(incarnation)=16
                                           AND typeof(floor)='blob' AND length(floor)=8
                                           AND typeof(tail)='blob' AND length(tail)=8 AND floor<=tail
                                      THEN 0 ELSE 1 END),0)
             FROM event_streams WHERE retired=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .map_err(|e| corrupt("verify retired lifetime accounting", e))?;
    if stored.0 < 0
        || stored.1 < 0
        || stored.2 < 0
        || stored.3 < 0
        || actual_receipts.2 != 0
        || actual_retired.2 != 0
        || (stored.0, stored.1) != (actual_receipts.0, actual_receipts.1)
        || (stored.2, stored.3) != (actual_retired.0, actual_retired.1)
    {
        return Err(Error::StoreCorrupt(
            "lifecycle accounting counters do not match stored metadata".into(),
        ));
    }
    Ok(())
}

fn create_stream(conn: &mut Connection, id: StreamId) -> Result<StreamKey> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write_error("begin stream creation", e))?;
    if let Some((incarnation, active_key)) = name_row(&tx, &id)? {
        let key = StreamKey { id, incarnation };
        if active_key.is_some() {
            stream_row(&tx, &key)?;
        }
        tx.rollback()
            .map_err(|e| write_error("finish existing stream lookup", e))?;
        return if active_key.is_some() {
            Ok(key)
        } else {
            Err(Error::StreamUnavailable {
                last: Box::new(key),
            })
        };
    }
    let incarnation = super::new_incarnation();
    tx.execute(
        "INSERT INTO event_streams(public_id,incarnation,floor,tail,retired) VALUES(?1,?2,?3,?3,0)",
        params![id.as_str(), incarnation.0.as_slice(), be(0).as_slice()],
    )
    .map_err(|e| write_error("create stream lifetime", e))?;
    let stream_key = tx.last_insert_rowid();
    tx.execute(
        "INSERT INTO event_stream_names(public_id,latest_incarnation,active_stream_key) VALUES(?1,?2,?3)",
        params![id.as_str(), incarnation.0.as_slice(), stream_key],
    )
    .map_err(|e| write_error("create stream name", e))?;
    tx.commit()
        .map_err(|e| write_error("commit stream creation", e))?;
    Ok(StreamKey { id, incarnation })
}

fn find_stream(conn: &Connection, id: &StreamId) -> Result<Option<StreamKey>> {
    let Some((incarnation, active_key)) = name_row(conn, id)? else {
        return Ok(None);
    };
    let key = StreamKey {
        id: id.clone(),
        incarnation,
    };
    if active_key.is_none() {
        return Err(Error::StreamUnavailable {
            last: Box::new(key),
        });
    }
    stream_row(conn, &key)?;
    Ok(Some(key))
}

fn name_row(conn: &Connection, id: &StreamId) -> Result<Option<(IncarnationId, Option<i64>)>> {
    let row = conn
        .query_row(
            "SELECT CASE WHEN typeof(latest_incarnation)='blob' AND length(latest_incarnation)=16 THEN latest_incarnation END,
                    CASE WHEN active_stream_key IS NULL OR typeof(active_stream_key)='integer' THEN 1 ELSE 0 END,
                    active_stream_key
             FROM event_stream_names WHERE public_id=?1",
            params![id.as_str()],
            |r| {
                Ok((
                    r.get::<_, Option<Vec<u8>>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                ))
            },
        )
        .optional()
        .map_err(|e| corrupt("read stream", e))?;
    row.map(|(incarnation, active_valid, active)| {
        let incarnation = incarnation.ok_or_else(|| {
            Error::StoreCorrupt("latest incarnation is not a 16-byte blob".into())
        })?;
        if active_valid != 1 {
            return Err(Error::StoreCorrupt(
                "active stream key is not an integer or null".into(),
            ));
        }
        Ok((decode_incarnation(&incarnation)?, active))
    })
    .transpose()
}
pub(super) fn stream_row(conn: &Connection, stream: &StreamKey) -> Result<(i64, u64, u64)> {
    let (current_incarnation, active_key) =
        name_row(conn, &stream.id)?.ok_or(Error::StreamNotFound)?;
    let current = StreamKey {
        id: stream.id.clone(),
        incarnation: current_incarnation,
    };
    let Some(active_key) = active_key else {
        return Err(Error::StreamUnavailable {
            last: Box::new(current),
        });
    };
    if &current != stream {
        return Err(Error::StaleIncarnation {
            current: Box::new(StreamAvailability::Active(current)),
        });
    }
    let row = conn
        .query_row(
            "SELECT stream_key,
                    CASE WHEN length(incarnation)=16 THEN incarnation END,
                    CASE WHEN length(floor)=8 THEN floor END,
                    CASE WHEN length(tail)=8 THEN tail END,
                    retired
             FROM event_streams WHERE stream_key=?1 AND public_id=?2",
            params![active_key, stream.id.as_str()],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                    r.get::<_, Vec<u8>>(3)?,
                    r.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()
        .map_err(|e| corrupt("read stream state", e))?
        .ok_or_else(|| {
            Error::StoreCorrupt("active name points to a missing or mismatched lifetime".into())
        })?;
    if decode_incarnation(&row.1)? != stream.incarnation || row.4 != 0 {
        return Err(Error::StoreCorrupt(
            "active name does not identify an active lifetime".into(),
        ));
    }
    let floor = decode_offset(&row.2)?;
    let tail = decode_offset(&row.3)?;
    if floor > tail {
        return Err(Error::StoreCorrupt(
            "stream floor is greater than its tail".into(),
        ));
    }
    Ok((row.0, floor, tail))
}

fn read_lifecycle_receipt(
    conn: &Connection,
    operation_id: &LifecycleOperationId,
) -> Result<Option<LifecycleReceipt>> {
    let row = conn
        .query_row(
            "SELECT action,
                    CASE WHEN typeof(expected_public_id)='text' AND octet_length(expected_public_id) BETWEEN 1 AND 256 THEN expected_public_id END,
                    CASE WHEN typeof(expected_incarnation)='blob' AND length(expected_incarnation)=16 THEN expected_incarnation END,
                    CASE WHEN replacement_incarnation IS NULL OR (typeof(replacement_incarnation)='blob' AND length(replacement_incarnation)=16) THEN 1 ELSE 0 END,
                    replacement_incarnation,
                    CASE WHEN typeof(charge)='integer' AND charge>=0 THEN charge END
             FROM lifecycle_receipts WHERE operation_id=?1",
            params![operation_id.as_str()],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, Option<Vec<u8>>>(4)?,
                    r.get::<_, Option<i64>>(5)?,
                ))
            },
        )
        .optional()
        .map_err(|e| corrupt("read lifecycle receipt", e))?;
    row.map(
        |(action, public_id, expected, replacement_valid, replacement, charge)| {
            if replacement_valid != 1 {
                return Err(Error::StoreCorrupt(
                    "receipt replacement incarnation is invalid".into(),
                ));
            }
            let id = StreamId::new(public_id.ok_or_else(|| {
                Error::StoreCorrupt("receipt stream identifier is invalid".into())
            })?)
            .map_err(|_| Error::StoreCorrupt("receipt stream identifier is invalid".into()))?;
            let expected = StreamKey {
                id: id.clone(),
                incarnation: decode_incarnation(&expected.ok_or_else(|| {
                    Error::StoreCorrupt("receipt expected incarnation is invalid".into())
                })?)?,
            };
            let action = match action {
                0 => LifecycleAction::Delete,
                1 => LifecycleAction::Reset,
                _ => return Err(Error::StoreCorrupt("receipt action is invalid".into())),
            };
            let stored_charge = usize::try_from(
                charge.ok_or_else(|| Error::StoreCorrupt("receipt charge is invalid".into()))?,
            )
            .map_err(|_| Error::StoreCorrupt("receipt charge is invalid".into()))?;
            let replacement = replacement
                .map(|bytes| {
                    Ok::<StreamKey, Error>(StreamKey {
                        id,
                        incarnation: decode_incarnation(&bytes)?,
                    })
                })
                .transpose()?;
            let receipt = LifecycleReceipt {
                request: LifecycleRequest {
                    operation_id: operation_id.clone(),
                    expected,
                    action,
                },
                replacement,
            };
            if receipt.request.receipt_charge() != Some(stored_charge)
                || matches!(
                    (&receipt.request.action, &receipt.replacement),
                    (LifecycleAction::Delete, Some(_)) | (LifecycleAction::Reset, None)
                )
                || receipt.replacement.as_ref().is_some_and(|replacement| {
                    replacement.id != receipt.request.expected.id
                        || replacement.incarnation == receipt.request.expected.incarnation
                })
            {
                return Err(Error::StoreCorrupt(
                    "stored lifecycle receipt is internally inconsistent".into(),
                ));
            }
            Ok(receipt)
        },
    )
    .transpose()
}

fn change_lifecycle(
    conn: &mut Connection,
    request: LifecycleRequest,
    options: &SqliteOptions,
    failure_injection: &mut Option<SqliteFailureInjection>,
) -> Result<LifecycleReceipt> {
    let receipt_charge = request.receipt_charge().ok_or(Error::CapacityExceeded)?;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write_error("begin lifecycle change", e))?;
    if let Some(receipt) = read_lifecycle_receipt(&tx, &request.operation_id)? {
        tx.rollback()
            .map_err(|e| write_error("finish lifecycle retry", e))?;
        return if receipt.request == request {
            Ok(receipt)
        } else {
            Err(Error::LifecycleConflict {
                operation_id: request.operation_id,
            })
        };
    }
    let (latest, active_key) = name_row(&tx, &request.expected.id)?.ok_or(Error::StreamNotFound)?;
    let current = StreamKey {
        id: request.expected.id.clone(),
        incarnation: latest,
    };
    let availability = if active_key.is_some() {
        StreamAvailability::Active(current.clone())
    } else {
        StreamAvailability::Unavailable(current.clone())
    };
    if current != request.expected {
        return Err(Error::StaleIncarnation {
            current: Box::new(availability),
        });
    }
    if active_key.is_some() {
        let (validated_key, _, _) = stream_row(&tx, &request.expected)?;
        if Some(validated_key) != active_key {
            return Err(Error::StoreCorrupt(
                "active name points to a different lifetime".into(),
            ));
        }
    }
    #[cfg(feature = "replication")]
    if let Some(replica) = tx
        .query_row(
            "SELECT replica_id FROM replication_origin_replicas WHERE public_id=?1 AND incarnation=?2 AND mode=0 LIMIT 1",
            params![
                request.expected.id.as_str(),
                request.expected.incarnation.0.as_slice()
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| corrupt("read lifecycle replica protection", error))?
    {
        return Err(Error::ReplicaProtectionActive {
            replica: ReplicaId::new(replica)
                .map_err(|error| Error::StoreCorrupt(error.to_string()))?,
        });
    }

    let (receipt_count, receipt_bytes, retired_count, retired_bytes): (i64, i64, i64, i64) = tx
        .query_row(
            "SELECT CASE WHEN typeof(lifecycle_receipt_count)='integer' AND lifecycle_receipt_count>=0 THEN lifecycle_receipt_count END,
                    CASE WHEN typeof(lifecycle_receipt_bytes)='integer' AND lifecycle_receipt_bytes>=0 THEN lifecycle_receipt_bytes END,
                    CASE WHEN typeof(retired_lifetime_count)='integer' AND retired_lifetime_count>=0 THEN retired_lifetime_count END,
                    CASE WHEN typeof(retired_metadata_bytes)='integer' AND retired_metadata_bytes>=0 THEN retired_metadata_bytes END
             FROM event_stream_metadata WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .map_err(|e| corrupt("read lifecycle quota counters", e))?;
    let receipt_count = usize::try_from(receipt_count)
        .map_err(|_| Error::StoreCorrupt("receipt count is invalid".into()))?;
    let receipt_bytes = usize::try_from(receipt_bytes)
        .map_err(|_| Error::StoreCorrupt("receipt byte count is invalid".into()))?;
    if receipt_count >= options.max_lifecycle_receipts
        || receipt_bytes
            .checked_add(receipt_charge)
            .is_none_or(|total| total > options.max_lifecycle_receipt_bytes)
    {
        return Err(Error::CapacityExceeded);
    }

    let retires_active = active_key.is_some();
    let retired_charge = request
        .expected
        .id
        .as_str()
        .len()
        .checked_add(256)
        .ok_or(Error::CapacityExceeded)?;
    if retires_active {
        let retired_count = usize::try_from(retired_count)
            .map_err(|_| Error::StoreCorrupt("retired count is invalid".into()))?;
        let retired_bytes = usize::try_from(retired_bytes)
            .map_err(|_| Error::StoreCorrupt("retired byte count is invalid".into()))?;
        if retired_count >= options.max_retired_lifetimes
            || retired_bytes
                .checked_add(retired_charge)
                .is_none_or(|total| total > options.max_retired_metadata_bytes)
        {
            return Err(Error::CapacityExceeded);
        }
        let changed = tx
            .execute(
                "UPDATE event_streams SET retired=1 WHERE stream_key=?1 AND retired=0",
                params![active_key],
            )
            .map_err(|e| write_error("retire stream lifetime", e))?;
        if changed != 1 {
            return Err(Error::StoreCorrupt(
                "active lifetime could not be retired".into(),
            ));
        }
    }

    let replacement = match request.action {
        LifecycleAction::Delete => {
            tx.execute(
                "UPDATE event_stream_names SET active_stream_key=NULL WHERE public_id=?1",
                params![request.expected.id.as_str()],
            )
            .map_err(|e| write_error("mark stream unavailable", e))?;
            None
        }
        LifecycleAction::Reset => {
            let incarnation = super::new_incarnation();
            tx.execute(
                "INSERT INTO event_streams(public_id,incarnation,floor,tail,retired) VALUES(?1,?2,?3,?3,0)",
                params![request.expected.id.as_str(), incarnation.0.as_slice(), be(0).as_slice()],
            )
            .map_err(|e| write_error("create replacement lifetime", e))?;
            let key = tx.last_insert_rowid();
            tx.execute(
                "UPDATE event_stream_names SET latest_incarnation=?1,active_stream_key=?2 WHERE public_id=?3",
                params![incarnation.0.as_slice(), key, request.expected.id.as_str()],
            )
            .map_err(|e| write_error("activate replacement lifetime", e))?;
            Some(StreamKey {
                id: request.expected.id.clone(),
                incarnation,
            })
        }
    };
    let action = match request.action {
        LifecycleAction::Delete => 0,
        LifecycleAction::Reset => 1,
    };
    tx.execute(
        "INSERT INTO lifecycle_receipts(operation_id,action,expected_public_id,expected_incarnation,replacement_incarnation,charge) VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            request.operation_id.as_str(),
            action,
            request.expected.id.as_str(),
            request.expected.incarnation.0.as_slice(),
            replacement.as_ref().map(|key| key.incarnation.0.as_slice()),
            i64::try_from(receipt_charge).map_err(|_| Error::CapacityExceeded)?,
        ],
    )
    .map_err(|e| write_error("store lifecycle receipt", e))?;
    let retired_increment = if retires_active { 1_i64 } else { 0 };
    let retired_byte_increment = if retires_active {
        i64::try_from(retired_charge).map_err(|_| Error::CapacityExceeded)?
    } else {
        0
    };
    let changed = tx
        .execute(
            "UPDATE event_stream_metadata
             SET lifecycle_receipt_count=lifecycle_receipt_count+1,
                 lifecycle_receipt_bytes=lifecycle_receipt_bytes+?1,
                 retired_lifetime_count=retired_lifetime_count+?2,
                 retired_metadata_bytes=retired_metadata_bytes+?3
             WHERE singleton=1",
            params![
                i64::try_from(receipt_charge).map_err(|_| Error::CapacityExceeded)?,
                retired_increment,
                retired_byte_increment,
            ],
        )
        .map_err(|e| write_error("update lifecycle quota counters", e))?;
    if changed != 1 {
        return Err(Error::StoreCorrupt(
            "lifecycle metadata row is missing".into(),
        ));
    }
    let receipt = LifecycleReceipt {
        request: request.clone(),
        replacement,
    };
    if matches!(
        failure_injection,
        Some(SqliteFailureInjection::BeforeLifecycleCommit)
    ) {
        *failure_injection = None;
        return injected_rollback(tx, "before lifecycle commit");
    }
    match tx.commit() {
        Ok(()) => {
            if matches!(
                failure_injection,
                Some(SqliteFailureInjection::AfterLifecycleCommitAcknowledgementLost)
            ) {
                *failure_injection = None;
                match read_lifecycle_receipt(conn, &request.operation_id) {
                    Ok(Some(stored)) if stored == receipt => Ok(stored),
                    _ => Err(Error::LifecycleCommitUnknown {
                        operation_id: request.operation_id,
                    }),
                }
            } else {
                Ok(receipt)
            }
        }
        Err(_) => match read_lifecycle_receipt(conn, &request.operation_id) {
            Ok(Some(stored)) if stored == receipt => Ok(stored),
            _ => Err(Error::LifecycleCommitUnknown {
                operation_id: request.operation_id,
            }),
        },
    }
}

fn cleanup_retired(
    conn: &mut Connection,
    limits: CleanupLimits,
    failure_injection: &mut Option<SqliteFailureInjection>,
    #[cfg(feature = "snapshots")] snapshot_state: &mut SqliteSnapshotState,
    #[cfg(feature = "snapshots")] options: &SqliteOptions,
) -> Result<CleanupProgress> {
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| write_error("begin retired cleanup", e))?;
    let lifetime = tx
        .query_row(
            "SELECT stream_key,
                    CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                    CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,
                    CASE WHEN typeof(floor)='blob' AND length(floor)=8 THEN floor END,
                    CASE WHEN typeof(tail)='blob' AND length(tail)=8 THEN tail END
             FROM event_streams WHERE retired=1 ORDER BY stream_key LIMIT 1",
            [],
            |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                    r.get::<_, Option<Vec<u8>>>(3)?,
                    r.get::<_, Option<Vec<u8>>>(4)?,
                ))
            },
        )
        .optional()
        .map_err(|e| corrupt("select retired lifetime", e))?;
    let Some((stream_key, public_id, incarnation, floor, tail)) = lifetime else {
        tx.rollback()
            .map_err(|e| write_error("finish empty cleanup", e))?;
        return Ok(CleanupProgress {
            stream: None,
            removed_records: 0,
            removed_bytes: 0,
            remaining: false,
        });
    };
    let stream =
        StreamKey {
            id: StreamId::new(public_id.ok_or_else(|| {
                Error::StoreCorrupt("retired stream identifier is invalid".into())
            })?)
            .map_err(|_| Error::StoreCorrupt("retired stream identifier is invalid".into()))?,
            incarnation: decode_incarnation(
                &incarnation
                    .ok_or_else(|| Error::StoreCorrupt("retired incarnation is invalid".into()))?,
            )?,
        };
    let mut expected = decode_offset(
        &floor.ok_or_else(|| Error::StoreCorrupt("retired floor is invalid".into()))?,
    )?;
    let tail =
        decode_offset(&tail.ok_or_else(|| Error::StoreCorrupt("retired tail is invalid".into()))?)?;
    if expected > tail {
        return Err(Error::StoreCorrupt(
            "retired floor is greater than its tail".into(),
        ));
    }
    let mut removed_records = 0usize;
    let mut removed_bytes = 0usize;
    #[cfg(feature = "snapshots")]
    let maximum_removable =
        snapshot_state.maximum_cleanup_offset(&stream, options.snapshot_clock.now());
    #[cfg(feature = "retention")]
    let retired_record_sql =
        "SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
                CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(schema_version)='integer' AND typeof(payload)='blob'
                          AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
                     THEN octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384 END
         FROM (SELECT offset,event_id,schema_id,schema_version,payload FROM event_records WHERE stream_key=?1
               UNION ALL SELECT offset,event_id,schema_id,schema_version,payload FROM retention_generated_records WHERE stream_key=?1)
         ORDER BY offset LIMIT 1";
    #[cfg(not(feature = "retention"))]
    let retired_record_sql =
        "SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
                CASE WHEN typeof(event_id)='text' AND typeof(schema_id)='text' AND typeof(schema_version)='integer' AND typeof(payload)='blob'
                          AND octet_length(event_id) BETWEEN 1 AND 256 AND octet_length(schema_id) BETWEEN 1 AND 256
                     THEN octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384 END
         FROM event_records WHERE stream_key=?1 ORDER BY offset LIMIT 1";
    while removed_records < limits.max_records {
        let record = tx
            .query_row(retired_record_sql, params![stream_key], |r| {
                Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, Option<i64>>(1)?))
            })
            .optional()
            .map_err(|e| corrupt("read retired record charge", e))?;
        let Some((offset, charge)) = record else {
            break;
        };
        let offset = decode_offset(
            &offset
                .ok_or_else(|| Error::StoreCorrupt("retired record offset is invalid".into()))?,
        )?;
        #[cfg(feature = "snapshots")]
        if offset > maximum_removable {
            break;
        }
        let next = expected.checked_add(1).ok_or(Error::OffsetOverflow)?;
        if offset != next {
            return Err(Error::StoreCorrupt(format!(
                "retired history gap: expected {next}, found {offset}"
            )));
        }
        let charge = usize::try_from(
            charge.ok_or_else(|| Error::StoreCorrupt("retired record charge is invalid".into()))?,
        )
        .map_err(|_| Error::StoreCorrupt("retired record charge is invalid".into()))?;
        if removed_records == 0 && charge > limits.max_bytes {
            return Err(Error::CapacityExceeded);
        }
        if removed_bytes
            .checked_add(charge)
            .is_none_or(|total| total > limits.max_bytes)
        {
            break;
        }
        #[cfg(feature = "retention")]
        super::sqlite_retention::remove_retry_identity_for_retired(&tx, stream_key, offset)?;
        let deleted = tx
            .execute(
                "DELETE FROM event_records WHERE stream_key=?1 AND offset=?2",
                params![stream_key, be(offset).as_slice()],
            )
            .map_err(|e| write_error("delete retired record", e))?;
        #[cfg(not(feature = "retention"))]
        let _ = deleted;
        #[cfg(feature = "retention")]
        if deleted == 0 {
            tx.execute(
                "DELETE FROM retention_generated_records WHERE stream_key=?1 AND offset=?2",
                params![stream_key, be(offset).as_slice()],
            )
            .map_err(|e| write_error("delete retired generated record", e))?;
        }
        tx.execute(
            "UPDATE event_streams SET floor=?1 WHERE stream_key=?2",
            params![be(offset).as_slice(), stream_key],
        )
        .map_err(|e| write_error("advance retired floor", e))?;
        expected = offset;
        removed_records += 1;
        removed_bytes = removed_bytes
            .checked_add(charge)
            .ok_or(Error::CapacityExceeded)?;
    }
    #[cfg(feature = "retention")]
    let records_left_sql = "SELECT EXISTS(SELECT 1 FROM event_records WHERE stream_key=?1
                           UNION ALL SELECT 1 FROM retention_generated_records WHERE stream_key=?1)";
    #[cfg(not(feature = "retention"))]
    let records_left_sql = "SELECT EXISTS(SELECT 1 FROM event_records WHERE stream_key=?1)";
    let records_left: bool = tx
        .query_row(records_left_sql, params![stream_key], |r| r.get(0))
        .map_err(|e| corrupt("check retired lifetime completion", e))?;
    if !records_left && expected != tail {
        return Err(Error::StoreCorrupt(format!(
            "retired history ended at {expected} before tail {tail}"
        )));
    }
    if !records_left {
        #[cfg(feature = "retention")]
        super::sqlite_retention::remove_retired_retention_state(&tx, stream_key)?;
        let retired_charge = stream
            .id
            .as_str()
            .len()
            .checked_add(256)
            .ok_or(Error::CapacityExceeded)?;
        tx.execute(
            "DELETE FROM event_streams WHERE stream_key=?1 AND retired=1",
            params![stream_key],
        )
        .map_err(|e| write_error("finalize retired lifetime", e))?;
        let changed = tx
            .execute(
                "UPDATE event_stream_metadata
                 SET retired_lifetime_count=retired_lifetime_count-1,
                     retired_metadata_bytes=retired_metadata_bytes-?1
                 WHERE singleton=1 AND retired_lifetime_count>=1 AND retired_metadata_bytes>=?1",
                params![i64::try_from(retired_charge).map_err(|_| Error::CapacityExceeded)?],
            )
            .map_err(|e| write_error("update retired quota counters", e))?;
        if changed != 1 {
            return Err(Error::StoreCorrupt(
                "retired quota counters do not match stored lifetimes".into(),
            ));
        }
    }
    let remaining: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM event_streams WHERE retired=1)",
            [],
            |r| r.get(0),
        )
        .map_err(|e| corrupt("check remaining retired history", e))?;
    if matches!(
        failure_injection,
        Some(SqliteFailureInjection::BeforeCleanupCommit)
    ) {
        *failure_injection = None;
        return injected_rollback(tx, "before cleanup commit");
    }
    tx.commit()
        .map_err(|e| write_error("commit retired cleanup", e))?;
    Ok(CleanupProgress {
        stream: Some(stream),
        removed_records,
        removed_bytes,
        remaining,
    })
}
// Stage one event using the caller's transaction. Never commit or roll back here.
// An error may follow writes (including replication metadata); the owner must
// explicitly roll back before continuing or publishing any receipt.
fn stage_append(
    conn: &rusqlite::Transaction<'_>,
    stream: &StreamKey,
    event: NewEvent,
    failure_injection: &mut Option<SqliteFailureInjection>,
    options: &SqliteOptions,
) -> Result<AppendReceipt> {
    let (key, _, tail) = stream_row(conn, stream)?;
    #[cfg(feature = "retention")]
    if super::sqlite_retention::retry_policy_enabled(conn, key)? {
        return Err(Error::RetryPolicyRequired);
    }
    if let Some(record) = lookup_key(conn, stream, key, &event.id, options.max_record_bytes)? {
        let same = record.event.schema == event.schema && record.event.payload == event.payload;
        return if same {
            Ok(AppendReceipt {
                record,
                kind: AppendKind::Deduplicated,
            })
        } else {
            Err(Error::IdempotencyConflict { event_id: event.id })
        };
    }
    let offset = tail.checked_add(1).ok_or(Error::OffsetOverflow)?;
    #[cfg(feature = "replication")]
    let (replication_now, replication_charge) =
        super::sqlite_replication::preflight_replication_append(
            conn, stream, key, &event, options,
        )?;
    if matches!(
        failure_injection,
        Some(SqliteFailureInjection::BeforeRecordInsert)
    ) {
        *failure_injection = None;
        return Err(Error::StoreWriteFailed(
            "injected failure before record insert".into(),
        ));
    }
    if let Err(e)=conn.execute("INSERT INTO event_records(stream_key,offset,event_id,schema_id,schema_version,payload) VALUES(?1,?2,?3,?4,?5,?6)",params![key,be(offset).as_slice(),event.id.as_str(),event.schema.id.as_str(),i64::from(event.schema.version),event.payload.as_bytes()]){return Err(write_error("insert event",e));}
    if matches!(
        failure_injection,
        Some(SqliteFailureInjection::AfterRecordInsert)
    ) {
        *failure_injection = None;
        return Err(Error::StoreWriteFailed(
            "injected failure after record insert".into(),
        ));
    }
    if let Err(e) = conn.execute(
        "UPDATE event_streams SET tail=?1 WHERE stream_key=?2",
        params![be(offset).as_slice(), key],
    ) {
        return Err(write_error("update tail", e));
    }
    #[cfg(feature = "replication")]
    super::sqlite_replication::apply_replication_append(
        conn,
        stream,
        key,
        offset,
        replication_charge,
        replication_now,
    )?;
    Ok(AppendReceipt {
        record: Arc::new(Record {
            cursor: Cursor::new(stream.clone(), offset),
            event,
        }),
        kind: AppendKind::Inserted,
    })
}

fn append(
    conn: &mut Connection,
    stream: StreamKey,
    event: NewEvent,
    failure_injection: &mut Option<SqliteFailureInjection>,
    options: &SqliteOptions,
) -> (Result<AppendReceipt>, bool) {
    let retry_id = event.id.clone();
    let mut expected_record = None;
    let result = (|| {
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| write_error("begin append", e))?;
        let receipt = match stage_append(&tx, &stream, event, failure_injection, options) {
            Ok(receipt) => receipt,
            Err(error) => return rollback_error(tx, error),
        };
        if receipt.kind == AppendKind::Deduplicated {
            tx.rollback()
                .map_err(|e| Error::StoreCorrupt(format!("finish retry rollback failed: {e}")))?;
            return Ok(receipt);
        }
        if matches!(
            failure_injection,
            Some(SqliteFailureInjection::BeforeCommit)
        ) {
            *failure_injection = None;
            return injected_rollback(tx, "after tail update");
        }
        if let Some(SqliteFailureInjection::PauseBeforeCommit(delay)) = *failure_injection {
            *failure_injection = None;
            thread::sleep(delay);
        }
        expected_record = Some(receipt.record.clone());
        tx.commit()
            .map(|_| receipt)
            .and_then(|receipt| {
                if matches!(
                    failure_injection,
                    Some(SqliteFailureInjection::AfterCommitAcknowledgementLost)
                ) {
                    *failure_injection = None;
                    Err(rusqlite::Error::InvalidQuery)
                } else if let Some(SqliteFailureInjection::PauseAfterCommitAcknowledgementLost(
                    delay,
                )) = *failure_injection
                {
                    *failure_injection = None;
                    thread::sleep(delay);
                    Err(rusqlite::Error::InvalidQuery)
                } else {
                    Ok(receipt)
                }
            })
            .map_err(|_| Error::CommitUnknown {
                event_id: retry_id.clone(),
            })
    })();
    match result {
        Err(Error::CommitUnknown { .. }) => {
            // Never mistake this connection's uncommitted rows for durable rows.
            if !conn.is_autocommit() {
                return (Err(Error::CommitUnknown { event_id: retry_id }), true);
            }
            match lookup(conn, &stream, &retry_id, options.max_record_bytes) {
                Ok(Some(record)) if Some(&record) == expected_record.as_ref() => (
                    Ok(AppendReceipt {
                        record,
                        kind: AppendKind::Inserted,
                    }),
                    false,
                ),
                Ok(Some(_)) => (
                    Err(Error::StoreCorrupt(
                        "reconciled append differs from staged record".into(),
                    )),
                    true,
                ),
                Ok(None) => (
                    Err(Error::StoreWriteFailed(
                        "append commit failed and no record committed".into(),
                    )),
                    false,
                ),
                Err(_) => (Err(Error::CommitUnknown { event_id: retry_id }), true),
            }
        }
        other @ Err(Error::StoreCorrupt(_)) => (other, true),
        other => (other, false),
    }
}

fn append_batch(
    conn: &mut Connection,
    items: Vec<(StreamKey, NewEvent)>,
    failure_injection: &mut Option<SqliteFailureInjection>,
    options: &SqliteOptions,
) -> (Vec<Result<AppendReceipt>>, bool) {
    let count = items.len();
    let event_ids: Vec<_> = items.iter().map(|(_, event)| event.id.clone()).collect();
    let tx = match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => tx,
        Err(error) => return batch_failure(count, write_error("begin append batch", error), false),
    };
    let mut outcomes = Vec::with_capacity(count);
    let mut inserted = Vec::new();

    for (stream, event) in items {
        if let Err(error) = tx.execute_batch("SAVEPOINT append_batch_item") {
            return abort_batch(tx, count, write_error("create append savepoint", error));
        }
        match stage_append(&tx, &stream, event, failure_injection, options) {
            Ok(receipt) => {
                if let Err(error) = tx.execute_batch("RELEASE append_batch_item") {
                    return abort_batch(tx, count, write_error("release append savepoint", error));
                }
                if receipt.kind == AppendKind::Inserted {
                    inserted.push(receipt.record.clone());
                }
                outcomes.push(Ok(receipt));
            }
            Err(error) if tx.is_autocommit() => return abort_batch(tx, count, error),
            Err(error) if isolated_append_error(&error) => {
                if let Err(rollback) =
                    tx.execute_batch("ROLLBACK TO append_batch_item; RELEASE append_batch_item")
                {
                    return abort_batch(
                        tx,
                        count,
                        Error::StoreCorrupt(format!(
                            "{error}; append savepoint rollback failed: {rollback}"
                        )),
                    );
                }
                outcomes.push(Err(error));
            }
            Err(error) => return abort_batch(tx, count, error),
        }
    }

    if matches!(
        failure_injection,
        Some(SqliteFailureInjection::BeforeCommit)
    ) {
        *failure_injection = None;
        let error = injected_rollback::<()>(tx, "after batched tail updates").unwrap_err();
        let fatal = matches!(error, Error::StoreCorrupt(_));
        return batch_failure(count, error, fatal);
    }
    if let Some(SqliteFailureInjection::PauseBeforeCommit(delay)) = *failure_injection {
        *failure_injection = None;
        thread::sleep(delay);
    }

    let commit_unknown = match tx.commit() {
        Ok(()) => {
            if matches!(
                failure_injection,
                Some(SqliteFailureInjection::AfterCommitAcknowledgementLost)
            ) {
                *failure_injection = None;
                true
            } else if let Some(SqliteFailureInjection::PauseAfterCommitAcknowledgementLost(delay)) =
                *failure_injection
            {
                *failure_injection = None;
                thread::sleep(delay);
                true
            } else {
                false
            }
        }
        Err(_) => true,
    };
    if !commit_unknown {
        return (outcomes, false);
    }

    // A live transaction could expose its own uncommitted writes as though they
    // were durable. Fault the worker rather than using those rows as evidence.
    if !conn.is_autocommit() {
        return (batch_commit_unknown(event_ids), true);
    }
    if inserted.is_empty() {
        return (outcomes, false);
    }

    let mut present = 0usize;
    for expected in &inserted {
        let stream = &expected.cursor.stream;
        match lookup(conn, stream, &expected.event.id, options.max_record_bytes) {
            Ok(Some(record)) if record == *expected => present += 1,
            Ok(Some(_)) => {
                return batch_failure(
                    count,
                    Error::StoreCorrupt(
                        "reconciled append batch record differs from staged record".into(),
                    ),
                    true,
                )
            }
            Ok(None) => {}
            Err(_) => return (batch_commit_unknown(event_ids), true),
        }
    }
    if present == inserted.len() {
        (outcomes, false)
    } else if present == 0 {
        batch_failure(
            count,
            Error::StoreWriteFailed("append batch commit failed and no records committed".into()),
            false,
        )
    } else {
        batch_failure(
            count,
            Error::StoreCorrupt(
                "append batch commit reconciliation found a partial transaction".into(),
            ),
            true,
        )
    }
}

// Only known per-event rejections are safe to isolate. Unknown/future failures
// abort the group rather than being accidentally treated as a benign rejection.
fn isolated_append_error(error: &Error) -> bool {
    match error {
        Error::StreamNotFound
        | Error::StaleIncarnation { .. }
        | Error::StreamUnavailable { .. }
        | Error::IdempotencyConflict { .. }
        | Error::OffsetOverflow
        | Error::CapacityExceeded => true,
        #[cfg(feature = "retention")]
        Error::RetryPolicyRequired => true,
        #[cfg(feature = "replication")]
        Error::ReplicaBacklogExceeded { .. }
        | Error::ReplicaBacklogExpired { .. }
        | Error::ReplicationClockRollback => true,
        _ => false,
    }
}

fn abort_batch(
    tx: rusqlite::Transaction<'_>,
    count: usize,
    original: Error,
) -> (Vec<Result<AppendReceipt>>, bool) {
    let error = rollback_error::<()>(tx, original).unwrap_err();
    let fatal = matches!(error, Error::StoreCorrupt(_));
    batch_failure(count, error, fatal)
}

fn batch_failure(count: usize, error: Error, fatal: bool) -> (Vec<Result<AppendReceipt>>, bool) {
    let fatal = fatal
        || matches!(
            error,
            Error::StoreCorrupt(_) | Error::OwnershipLost | Error::RuntimeFaulted(_)
        );
    let error = if fatal {
        error
    } else {
        Error::StoreWriteFailed(format!("append batch rolled back: {error}"))
    };
    (vec![Err(error); count], fatal)
}

async fn recv_append_batch(
    rx: oneshot::Receiver<Vec<Result<AppendReceipt>>>,
    batch: &AppendBatch,
) -> Vec<Result<AppendReceipt>> {
    rx.await.unwrap_or_else(|_| {
        // The command was accepted. A lost sender cannot prove rollback;
        // preserve every original identity for reconciliation by the caller.
        batch
            .items()
            .iter()
            .map(|item| {
                Err(Error::CommitUnknown {
                    event_id: item.event.id.clone(),
                })
            })
            .collect()
    })
}

fn batch_commit_unknown(event_ids: Vec<EventId>) -> Vec<Result<AppendReceipt>> {
    event_ids
        .into_iter()
        .map(|event_id| Err(Error::CommitUnknown { event_id }))
        .collect()
}

fn injected_rollback<T>(tx: rusqlite::Transaction<'_>, position: &str) -> Result<T> {
    tx.rollback()
        .map_err(|e| Error::StoreCorrupt(format!("injected rollback failed at {position}: {e}")))?;
    Err(Error::StoreWriteFailed(format!(
        "injected failure {position}"
    )))
}
fn rollback_error<T>(tx: rusqlite::Transaction<'_>, original: Error) -> Result<T> {
    // SQLITE_FULL and some I/O failures can make SQLite roll back before the
    // statement returns. Autocommit then proves that no transaction remains.
    if tx.is_autocommit() {
        return Err(original);
    }
    match tx.rollback() {
        Ok(()) => Err(original),
        Err(e) => Err(Error::StoreCorrupt(format!(
            "{original}; rollback also failed: {e}"
        ))),
    }
}
fn lookup(
    conn: &Connection,
    stream: &StreamKey,
    id: &EventId,
    max_record_bytes: usize,
) -> Result<Option<Arc<Record>>> {
    let (key, _, _) = stream_row(conn, stream)?;
    #[cfg(feature = "retention")]
    if super::sqlite_retention::retry_policy_enabled(conn, key)? {
        return Err(Error::RetryPolicyRequired);
    }
    lookup_key(conn, stream, key, id, max_record_bytes)
}
fn lookup_key(
    conn: &Connection,
    stream: &StreamKey,
    key: i64,
    id: &EventId,
    max_record_bytes: usize,
) -> Result<Option<Arc<Record>>> {
    conn.query_row(
        LOOKUP_RECORD_SQL,
        params![key, id.as_str(), record_limit(max_record_bytes)],
        |r| decode_row(r, stream, max_record_bytes),
    )
    .optional()
    .map_err(|e| corrupt("lookup event", e))?
    .transpose()
    .map(|record| record.map(Arc::new))
}
fn bounds(conn: &Connection, stream: &StreamKey) -> Result<Bounds> {
    let (_, floor, tail) = stream_row(conn, stream)?;
    Ok(Bounds {
        floor: Cursor::new(stream.clone(), floor),
        tail: Cursor::new(stream.clone(), tail),
    })
}
pub(super) fn read_page(
    conn: &Connection,
    stream: &StreamKey,
    after: u64,
    through: u64,
    limits: PageLimits,
    max_record_bytes: usize,
) -> Result<Page> {
    let (key, floor, tail) = stream_row(conn, stream)?;
    read_page_exact(
        conn,
        stream,
        StoredRange { key, floor, tail },
        after,
        through,
        limits,
        max_record_bytes,
    )
}

pub(super) struct StoredRange {
    pub(super) key: i64,
    pub(super) floor: u64,
    pub(super) tail: u64,
}

pub(super) fn read_page_exact(
    conn: &Connection,
    stream: &StreamKey,
    stored: StoredRange,
    after: u64,
    through: u64,
    limits: PageLimits,
    max_record_bytes: usize,
) -> Result<Page> {
    let StoredRange { key, floor, tail } = stored;
    let bounds = Bounds {
        floor: Cursor::new(stream.clone(), floor),
        tail: Cursor::new(stream.clone(), tail),
    };
    if after < floor {
        return Err(Error::HistoryUnavailable { bounds });
    }
    if after > tail || through > tail {
        return Err(Error::CursorAhead { tail: bounds.tail });
    }
    if after == through {
        let c = Cursor::new(stream.clone(), after);
        return Ok(Page {
            records: vec![],
            next_after: c,
            through: Cursor::new(stream.clone(), through),
            complete: true,
        });
    }
    let mut stmt = conn
        .prepare_cached(READ_RECORD_SQL)
        .map_err(|e| corrupt("prepare range", e))?;
    let limit = i64::try_from(limits.max_records).map_err(|_| Error::CapacityExceeded)?;
    let mut rows = stmt
        .query(params![
            key,
            be(after).as_slice(),
            be(through).as_slice(),
            limit,
            record_limit(max_record_bytes),
        ])
        .map_err(|e| corrupt("start range", e))?;
    let mut records = Vec::with_capacity(limits.max_records.min(64));
    let mut bytes = 0usize;
    let mut byte_limited = false;
    let mut expected = after.checked_add(1).ok_or(Error::OffsetOverflow)?;
    while let Some(row) = rows.next().map_err(|e| corrupt("step range", e))? {
        let record = Arc::new(
            decode_row(row, stream, max_record_bytes)
                .map_err(|e| corrupt("decode range row", e))??,
        );
        if record.cursor.offset != expected {
            return Err(Error::StoreCorrupt(format!(
                "history gap: expected {expected}, found {}",
                record.cursor.offset
            )));
        }
        let size = record.event.accounted_bytes();
        if bytes.saturating_add(size) > limits.max_bytes {
            if records.is_empty() {
                return Err(Error::CapacityExceeded);
            }
            byte_limited = true;
            break;
        }
        bytes += size;
        if record.cursor.offset != u64::MAX {
            expected = record.cursor.offset + 1;
        }
        records.push(record);
    }
    if records.is_empty() {
        return Err(Error::StoreCorrupt(format!(
            "missing record after {after} through {through}"
        )));
    }
    let next = records.last().unwrap().cursor.offset;
    if next != through && records.len() < limits.max_records && !byte_limited {
        return Err(Error::StoreCorrupt(format!(
            "missing record after {next} through {through}"
        )));
    }
    Ok(Page {
        records,
        next_after: Cursor::new(stream.clone(), next),
        through: Cursor::new(stream.clone(), through),
        complete: next == through,
    })
}
fn decode_row(
    row: &rusqlite::Row<'_>,
    stream: &StreamKey,
    max_record_bytes: usize,
) -> rusqlite::Result<Result<Record>> {
    let event_len: i64 = row.get(0)?;
    let schema_len: i64 = row.get(1)?;
    let payload_len: i64 = row.get(2)?;
    let offset = row.get_ref(3)?;
    let event = row.get_ref(4)?;
    let schema = row.get_ref(5)?;
    let version: Option<i64> = row.get(6)?;
    let payload = row.get_ref(7)?;
    Ok((|| {
        let event_len = usize::try_from(event_len)
            .map_err(|_| Error::StoreCorrupt("negative event identifier length".into()))?;
        let schema_len = usize::try_from(schema_len)
            .map_err(|_| Error::StoreCorrupt("negative schema identifier length".into()))?;
        let payload_len = usize::try_from(payload_len)
            .map_err(|_| Error::StoreCorrupt("negative payload length".into()))?;
        if event_len == 0
            || event_len > MAX_IDENTIFIER_BYTES
            || schema_len == 0
            || schema_len > MAX_IDENTIFIER_BYTES
        {
            return Err(Error::StoreCorrupt(
                "stored identifier length is invalid".into(),
            ));
        }
        if payload_len
            .saturating_add(event_len)
            .saturating_add(schema_len)
            .saturating_add(128)
            > max_record_bytes
        {
            return Err(Error::StoreCorrupt(
                "stored record exceeds the configured allocation bound".into(),
            ));
        }
        let offset = match offset {
            ValueRef::Blob(bytes) if bytes.len() == 8 => bytes,
            _ => {
                return Err(Error::StoreCorrupt(
                    "offset is not an eight-byte blob".into(),
                ))
            }
        };
        let event = match event {
            ValueRef::Text(bytes) => std::str::from_utf8(bytes)
                .map_err(|_| Error::StoreCorrupt("stored event identifier is not UTF-8".into()))?,
            _ => {
                return Err(Error::StoreCorrupt(
                    "stored event identifier failed allocation guards".into(),
                ))
            }
        };
        let schema = match schema {
            ValueRef::Text(bytes) => std::str::from_utf8(bytes)
                .map_err(|_| Error::StoreCorrupt("stored schema identifier is not UTF-8".into()))?,
            _ => {
                return Err(Error::StoreCorrupt(
                    "stored schema identifier failed allocation guards".into(),
                ))
            }
        };
        let version = version
            .ok_or_else(|| Error::StoreCorrupt("schema version is not an integer".into()))?;
        let payload = match payload {
            ValueRef::Blob(bytes) => bytes,
            _ => {
                return Err(Error::StoreCorrupt(
                    "stored payload failed allocation guards".into(),
                ))
            }
        };
        Ok(Record {
            cursor: Cursor::new(stream.clone(), decode_offset(offset)?),
            event: NewEvent {
                id: EventId::new(event).map_err(|_| {
                    Error::StoreCorrupt("stored event identifier is invalid".into())
                })?,
                schema: SchemaRef {
                    id: SchemaId::new(schema).map_err(|_| {
                        Error::StoreCorrupt("stored schema identifier is invalid".into())
                    })?,
                    version: u32::try_from(version)
                        .map_err(|_| Error::StoreCorrupt("schema version out of range".into()))?,
                },
                payload: Payload::copy_from_slice(payload),
            },
        })
    })())
}
pub(super) fn be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}
fn record_limit(max_record_bytes: usize) -> i64 {
    i64::try_from(max_record_bytes).unwrap_or(i64::MAX)
}
pub(super) fn decode_offset(v: &[u8]) -> Result<u64> {
    Ok(u64::from_be_bytes(v.try_into().map_err(|_| {
        Error::StoreCorrupt("offset is not eight bytes".into())
    })?))
}
fn decode_incarnation(v: &[u8]) -> Result<IncarnationId> {
    Ok(IncarnationId(v.try_into().map_err(|_| {
        Error::StoreCorrupt("incarnation is not 16 bytes".into())
    })?))
}
fn corrupt(action: &str, e: rusqlite::Error) -> Error {
    Error::StoreCorrupt(format!("{action}: {e}"))
}
fn write_error(action: &str, e: rusqlite::Error) -> Error {
    if let rusqlite::Error::SqliteFailure(code, _) = &e {
        if matches!(
            code.code,
            ErrorCode::DatabaseCorrupt | ErrorCode::NotADatabase
        ) {
            return corrupt(action, e);
        }
        if code.code == ErrorCode::DiskFull {
            return Error::CapacityExceeded;
        }
    }
    Error::StoreWriteFailed(format!("{action}: {e}"))
}

#[cfg(all(test, feature = "retention"))]
mod retention_query_plan_tests {
    use super::*;

    #[test]
    fn production_merged_replay_uses_both_offset_indexes_without_temp_sort() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE event_records(stream_key INTEGER,offset BLOB,event_id TEXT,schema_id TEXT,schema_version INTEGER,payload BLOB,PRIMARY KEY(stream_key,offset));
             CREATE TABLE retention_generated_records(stream_key INTEGER,generation BLOB,offset BLOB,event_id TEXT,schema_id TEXT,schema_version INTEGER,payload BLOB,UNIQUE(stream_key,offset));",
        )
        .unwrap();
        let mut stmt = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {READ_RECORD_SQL}"))
            .unwrap();
        let details = stmt
            .query_map(
                params![1_i64, be(0).as_slice(), be(3).as_slice(), 3_i64, 4096_i64],
                |row| row.get::<_, String>(3),
            )
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            details
                .iter()
                .any(|line| line.contains("event_records") && line.contains("INDEX")),
            "{details:?}"
        );
        assert!(
            details.iter().any(|line| {
                line.contains("retention_generated_records") && line.contains("INDEX")
            }),
            "{details:?}"
        );
        assert!(
            details.iter().all(|line| !line.contains("TEMP B-TREE")),
            "{details:?}"
        );
    }
}

#[cfg(test)]
mod append_staging_tests {
    use super::*;

    struct Database(PathBuf);
    impl Database {
        fn new() -> Self {
            let dir =
                std::env::temp_dir().join(format!("event-stream-stage-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&dir).unwrap();
            Self(dir)
        }
        fn path(&self) -> PathBuf {
            self.0.join("events.db")
        }
    }
    impl Drop for Database {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn event(id: &str, bytes: &[u8]) -> NewEvent {
        NewEvent {
            id: EventId::new(id).unwrap(),
            schema: SchemaRef {
                id: SchemaId::new("stage.bytes").unwrap(),
                version: 1,
            },
            payload: Payload::copy_from_slice(bytes),
        }
    }

    #[tokio::test]
    async fn closed_batch_reply_retains_each_uncertain_identity() {
        let stream = StreamKey {
            id: StreamId::new("reply").unwrap(),
            incarnation: IncarnationId([1; 16]),
        };
        let batch = AppendBatch::new(
            ["a", "b"]
                .into_iter()
                .map(|id| crate::AppendRequest {
                    stream: stream.clone(),
                    event: event(id, b"x"),
                })
                .collect(),
            crate::AppendBatchLimits {
                max_records: 2,
                max_bytes: 8192,
            },
        )
        .unwrap();
        let (sender, receiver) = oneshot::channel();
        drop(sender);
        let outcomes = recv_append_batch(receiver, &batch).await;
        batch.validate_results(&outcomes).unwrap();
        for (item, outcome) in batch.items().iter().zip(outcomes) {
            assert_eq!(
                outcome,
                Err(Error::CommitUnknown {
                    event_id: item.event.id.clone()
                })
            );
        }
    }

    #[test]
    fn staging_dedup_and_conflict_leave_outer_transaction_in_control() {
        let db = Database::new();
        let options = SqliteOptions::new(db.path());
        let mut conn = open_connection(&db.path(), &options).unwrap();
        let stream = create_stream(&mut conn, StreamId::new("staged").unwrap()).unwrap();
        let observer = Connection::open(db.path()).unwrap();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        let first = stage_append(&tx, &stream, event("a", b"first"), &mut None, &options).unwrap();
        let retry = stage_append(&tx, &stream, event("a", b"first"), &mut None, &options).unwrap();
        assert_eq!(retry.kind, AppendKind::Deduplicated);
        assert_eq!(first.record, retry.record);
        assert!(matches!(
            stage_append(&tx, &stream, event("a", b"changed"), &mut None, &options),
            Err(Error::IdempotencyConflict { .. })
        ));
        let second =
            stage_append(&tx, &stream, event("b", b"second"), &mut None, &options).unwrap();
        assert_eq!(second.record.cursor.offset, 2);
        assert!(!tx.is_autocommit());
        assert_eq!(bounds(&observer, &stream).unwrap().tail.offset, 0);
        tx.commit().unwrap();
        assert_eq!(bounds(&observer, &stream).unwrap().tail.offset, 2);
        assert_eq!(
            lookup(
                &observer,
                &stream,
                &EventId::new("b").unwrap(),
                options.max_record_bytes
            )
            .unwrap()
            .unwrap(),
            second.record
        );
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        stage_append(
            &tx,
            &stream,
            event("c", b"rolled back"),
            &mut None,
            &options,
        )
        .unwrap();
        let staged_retry = stage_append(
            &tx,
            &stream,
            event("c", b"rolled back"),
            &mut None,
            &options,
        )
        .unwrap();
        assert_eq!(staged_retry.kind, AppendKind::Deduplicated);
        let rollback: Result<()> =
            rollback_error(tx, Error::StoreWriteFailed("abort group".into()));
        assert!(matches!(rollback, Err(Error::StoreWriteFailed(_))));
        assert_eq!(bounds(&observer, &stream).unwrap().tail.offset, 2);
        assert!(lookup(
            &observer,
            &stream,
            &EventId::new("c").unwrap(),
            options.max_record_bytes
        )
        .unwrap()
        .is_none());
        drop(observer);
        drop(conn);
        let reopened = open_connection(&db.path(), &options).unwrap();
        assert_eq!(
            lookup(
                &reopened,
                &stream,
                &EventId::new("a").unwrap(),
                options.max_record_bytes
            )
            .unwrap()
            .unwrap(),
            first.record
        );
    }

    #[cfg(feature = "replication")]
    #[test]
    fn failure_after_replication_clock_write_rolls_back_record_tail_and_clock() {
        let db = Database::new();
        let options = SqliteOptions::new(db.path());
        let mut conn = open_connection(&db.path(), &options).unwrap();
        let stream = create_stream(&mut conn, StreamId::new("rollback").unwrap()).unwrap();
        let before: Option<Vec<u8>> = conn
            .query_row(
                "SELECT last_clock FROM replication_metadata WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        // FAIL preserves the statement's earlier changes until the owner rolls back.
        conn.execute_batch("CREATE TRIGGER fail_clock AFTER UPDATE ON replication_metadata BEGIN SELECT RAISE(FAIL, 'clock fault'); END;").unwrap();
        let (outcome, fatal) = append(
            &mut conn,
            stream.clone(),
            event("a", b"uncommitted"),
            &mut None,
            &options,
        );
        assert!(matches!(outcome, Err(Error::StoreWriteFailed(_))));
        assert!(!fatal);
        assert!(conn.is_autocommit());
        let after: Option<Vec<u8>> = conn
            .query_row(
                "SELECT last_clock FROM replication_metadata WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(before, after);
        assert_eq!(bounds(&conn, &stream).unwrap().tail.offset, 0);
        assert!(lookup(
            &conn,
            &stream,
            &EventId::new("a").unwrap(),
            options.max_record_bytes
        )
        .unwrap()
        .is_none());
        conn.execute_batch("DROP TRIGGER fail_clock").unwrap();
        let (outcome, fatal) = append(
            &mut conn,
            stream.clone(),
            event("a", b"committed"),
            &mut None,
            &options,
        );
        assert!(!fatal);
        let receipt = outcome.unwrap();
        assert_eq!(receipt.record.cursor.offset, 1);
        drop(conn);
        let reopened = open_connection(&db.path(), &options).unwrap();
        assert_eq!(
            lookup(
                &reopened,
                &stream,
                &EventId::new("a").unwrap(),
                options.max_record_bytes
            )
            .unwrap()
            .unwrap(),
            receipt.record
        );
    }
}
