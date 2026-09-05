#[cfg(feature = "replication")]
use super::sqlite::decode_offset;
use super::sqlite::{
    inspect_existing_format, inspect_restore_format, validate_lifecycle_counters, Ownership,
};
use crate::{
    application::{
        MappingPage, RestoreBackend, RestoreConfig, RestoreError, RestoreManager, RestoreReceipt,
        RestoreRequest, RestoreResult,
    },
    domain::*,
};
use async_trait::async_trait;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
#[cfg(any(feature = "replication", feature = "source-journal"))]
use rusqlite::{params_from_iter, types::Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

pub type SqliteRestoreManager = RestoreManager<SqliteRestoreBackend>;
#[cfg(feature = "source-journal")]
type JournalCounters = (i64, i64, Vec<u8>, i64, Vec<u8>, i64, Vec<u8>, i64, i64);

#[derive(Clone, Debug)]
pub struct SqliteRestoreBackend {
    root: PathBuf,
    failure: Arc<AtomicU8>,
    #[cfg(feature = "test-support")]
    pause_marker: Option<Arc<PathBuf>>,
    #[cfg(feature = "test-support")]
    observer: Option<Arc<dyn SqliteRestoreObserver>>,
}

#[doc(hidden)]
#[cfg(feature = "test-support")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteRestoreStage {
    SourceValidated,
    OwnerReserved,
    StagingCreated,
    IdentityMappingsImported,
    LifetimesImported,
    RecordPageCommitted,
    RecordsImported,
    NamesImported,
    ReceiptsImported,
    RelationalValidationCompleted,
    StagingCompleted,
    StagingSynced,
    DestinationPublished,
}

#[doc(hidden)]
#[cfg(feature = "test-support")]
pub trait SqliteRestoreObserver: std::fmt::Debug + Send + Sync + 'static {
    fn observe(&self, stage: SqliteRestoreStage);
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteRestoreFailureInjection {
    AfterOwnerReservation,
    AfterStagingFileReservation,
    AfterStagingCompletion,
    AfterDestinationLink,
    AfterPublishedMarker,
    #[cfg(feature = "test-support")]
    DuringCleanupUnlink,
    #[cfg(feature = "test-support")]
    DuringCleanupDirectorySync,
}

impl SqliteRestoreBackend {
    pub fn new(root: impl AsRef<Path>) -> RestoreResult<Self> {
        let root = root
            .as_ref()
            .canonicalize()
            .map_err(|error| RestoreError::InvalidConfig(format!("restore root: {error}")))?;
        let metadata = std::fs::symlink_metadata(&root)
            .map_err(|error| RestoreError::InvalidConfig(format!("restore root: {error}")))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(RestoreError::InvalidConfig(
                "restore root must be a real directory".into(),
            ));
        }
        Ok(Self {
            root,
            failure: Arc::new(AtomicU8::new(0)),
            #[cfg(feature = "test-support")]
            pause_marker: None,
            #[cfg(feature = "test-support")]
            observer: None,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    #[doc(hidden)]
    pub fn with_failure_injection(self, failure: SqliteRestoreFailureInjection) -> Self {
        self.failure.store(failure as u8 + 1, Ordering::Release);
        self
    }

    #[doc(hidden)]
    #[cfg(feature = "test-support")]
    pub fn with_pause_injection(
        mut self,
        boundary: SqliteRestoreFailureInjection,
        marker: impl Into<PathBuf>,
    ) -> Self {
        self.failure.store(boundary as u8 + 1, Ordering::Release);
        self.pause_marker = Some(Arc::new(marker.into()));
        self
    }

    #[doc(hidden)]
    #[cfg(feature = "test-support")]
    pub fn with_observer(mut self, observer: Arc<dyn SqliteRestoreObserver>) -> Self {
        self.observer = Some(observer);
        self
    }
}

#[async_trait]
impl RestoreBackend for SqliteRestoreBackend {
    async fn inspect_backup(
        &self,
        source: PathBuf,
        config: RestoreConfig,
    ) -> RestoreResult<BackupIdentity> {
        #[cfg(feature = "test-support")]
        let observer = self.observer.clone();
        tokio::task::spawn_blocking(move || {
            let identity = inspect_backup_blocking(&source, &config)?;
            #[cfg(feature = "test-support")]
            observe(observer.as_deref(), SqliteRestoreStage::SourceValidated);
            Ok(identity)
        })
        .await
        .map_err(|_| RestoreError::StorageFailure("backup inspection worker panicked".into()))?
    }

    async fn restore(
        &self,
        request: RestoreRequest,
        config: RestoreConfig,
    ) -> RestoreResult<RestoreReceipt> {
        let root = self.root.clone();
        let failure = self.failure.clone();
        #[cfg(feature = "test-support")]
        let pause_marker = self.pause_marker.clone();
        #[cfg(feature = "test-support")]
        let observer = self.observer.clone();
        #[cfg(not(feature = "test-support"))]
        let pause_marker: Option<Arc<PathBuf>> = None;
        let panic_request = request.clone();
        tokio::task::spawn_blocking(move || {
            restore_blocking(
                &root,
                &request,
                &config,
                &failure,
                pause_marker.as_deref().map(PathBuf::as_path),
                #[cfg(feature = "test-support")]
                observer.as_deref(),
            )
        })
        .await
        .map_err(|_| RestoreError::PublicationUnknown {
            operation_id: panic_request.operation_id,
            destination: Box::new(panic_request.destination),
        })?
    }

    async fn read_mapping(
        &self,
        receipt: RestoreReceipt,
        after: Option<StreamKey>,
        limits: PageLimits,
        config: RestoreConfig,
    ) -> RestoreResult<MappingPage> {
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            read_mapping_blocking(&root, &receipt, after.as_ref(), limits, &config)
        })
        .await
        .map_err(|_| RestoreError::StorageFailure("mapping reader worker panicked".into()))?
    }

    async fn cleanup_staging(
        &self,
        request: RestoreRequest,
        config: RestoreConfig,
    ) -> RestoreResult<()> {
        let root = self.root.clone();
        let failure = self.failure.clone();
        tokio::task::spawn_blocking(move || {
            cleanup_staging_blocking(&root, &request, &config, &failure)
        })
        .await
        .map_err(|_| RestoreError::StorageFailure("staging cleanup worker panicked".into()))?
    }
}

fn map_store_error(error: crate::application::Error) -> RestoreError {
    match error {
        crate::application::Error::StoreInUse => RestoreError::OwnershipConflict,
        crate::application::Error::UnsupportedFormat(version) => {
            RestoreError::UnsupportedBackup(version)
        }
        crate::application::Error::StoreCorrupt(message) => RestoreError::CorruptBackup(message),
        other => RestoreError::StorageFailure(other.to_string()),
    }
}

fn inspect_backup_blocking(source: &Path, config: &RestoreConfig) -> RestoreResult<BackupIdentity> {
    let ownership = Ownership::acquire(source).map_err(map_store_error)?;
    let (_connection, identity) = open_locked_source(ownership.path(), config)?;
    Ok(identity)
}

fn open_locked_source(
    source: &Path,
    config: &RestoreConfig,
) -> RestoreResult<(Connection, BackupIdentity)> {
    reject_recovery_sidecars(source)?;
    let metadata = std::fs::metadata(source)
        .map_err(|error| RestoreError::StorageFailure(format!("inspect backup: {error}")))?;
    if !metadata.is_file() || metadata.len() > config.max_source_bytes {
        return Err(RestoreError::CapacityExceeded);
    }
    let conn = Connection::open_with_flags(
        source,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| RestoreError::CorruptBackup(format!("open backup read-only: {error}")))?;
    let cache_kib = config.copy_buffer_bytes.div_ceil(1024).clamp(1, 16 * 1024);
    conn.pragma_update(None, "query_only", true)
        .map_err(|error| RestoreError::CorruptBackup(format!("make backup query-only: {error}")))?;
    conn.pragma_update(
        None,
        "cache_size",
        -i64::try_from(cache_kib).map_err(|_| RestoreError::CapacityExceeded)?,
    )
    .map_err(|error| RestoreError::CorruptBackup(format!("bound backup cache: {error}")))?;
    conn.pragma_update(None, "temp_store", "FILE")
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("bound backup temp memory: {error}"))
        })?;
    conn.execute_batch("BEGIN;")
        .map_err(|error| RestoreError::CorruptBackup(format!("begin backup snapshot: {error}")))?;
    conn.query_row("SELECT count(*) FROM sqlite_schema", [], |row| {
        row.get::<_, u64>(0)
    })
    .map_err(|error| RestoreError::CorruptBackup(format!("establish backup snapshot: {error}")))?;
    let identity = hash_file(source, config.copy_buffer_bytes, config.max_source_bytes)?;
    match inspect_existing_format(&conn).map_err(map_store_error)? {
        Some(2) => {}
        Some(version) => return Err(RestoreError::UnsupportedBackup(version)),
        None => {
            return Err(RestoreError::CorruptBackup(
                "backup has no storage format".into(),
            ))
        }
    }
    validate_lifecycle_counters(&conn).map_err(map_store_error)?;
    let journal: String = conn
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .map_err(|error| RestoreError::CorruptBackup(format!("read journal mode: {error}")))?;
    if !journal.eq_ignore_ascii_case("delete") {
        return Err(RestoreError::CorruptBackup(
            "backup must use DELETE journal mode".into(),
        ));
    }
    let quick: String = conn
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|error| RestoreError::CorruptBackup(format!("SQLite quick_check: {error}")))?;
    if quick != "ok" {
        return Err(RestoreError::CorruptBackup(format!(
            "SQLite quick_check failed: {quick}"
        )));
    }
    let foreign_violation: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("foreign-key check: {error}")))?;
    if foreign_violation {
        return Err(RestoreError::CorruptBackup(
            "backup contains a foreign-key violation".into(),
        ));
    }
    let orphan_record: bool = conn
        .query_row(
            "SELECT EXISTS(
               SELECT 1 FROM event_records AS r
               WHERE NOT EXISTS(
                 SELECT 1 FROM event_streams AS s WHERE s.stream_key=r.stream_key
               )
               LIMIT 1
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("check record stream ownership: {error}"))
        })?;
    if orphan_record {
        return Err(RestoreError::CorruptBackup(
            "backup contains a record without a stream lifetime".into(),
        ));
    }
    Ok((conn, identity))
}

fn reject_recovery_sidecars(source: &Path) -> RestoreResult<()> {
    for suffix in ["-wal", "-journal"] {
        let mut sidecar = source.as_os_str().to_os_string();
        sidecar.push(suffix);
        let path = PathBuf::from(sidecar);
        match std::fs::metadata(&path) {
            Ok(metadata) if metadata.len() > 0 => {
                return Err(RestoreError::CorruptBackup(format!(
                    "backup has a nonempty recovery sidecar: {}",
                    path.display()
                )))
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(RestoreError::StorageFailure(format!(
                    "inspect recovery sidecar: {error}"
                )))
            }
        }
    }
    Ok(())
}

fn hash_file(path: &Path, buffer_bytes: usize, max_bytes: u64) -> RestoreResult<BackupIdentity> {
    let mut file = File::open(path).map_err(|error| {
        RestoreError::StorageFailure(format!("open backup for hashing: {error}"))
    })?;
    let mut buffer = vec![0; buffer_bytes];
    let mut total = 0_u64;
    let mut hash = Sha256::new();
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| RestoreError::StorageFailure(format!("hash backup: {error}")))?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .filter(|total| *total <= max_bytes)
            .ok_or(RestoreError::CapacityExceeded)?;
        hash.update(&buffer[..read]);
    }
    Ok(BackupIdentity(hash.finalize().into()))
}

fn resolve_destination(root: &Path, relative: &Path) -> RestoreResult<PathBuf> {
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(RestoreError::InvalidConfig(
            "destination must remain inside the restore root".into(),
        ));
    }
    let name = relative
        .file_name()
        .ok_or_else(|| RestoreError::InvalidConfig("destination needs a file name".into()))?;
    let parent = root.join(relative).parent().unwrap_or(root).to_path_buf();
    let parent = parent.canonicalize().map_err(|error| {
        RestoreError::InvalidConfig(format!("destination parent is unavailable: {error}"))
    })?;
    if !parent.starts_with(root) {
        return Err(RestoreError::InvalidConfig(
            "destination parent escapes the restore root".into(),
        ));
    }
    Ok(parent.join(name))
}

fn staging_path(destination: &Path, request: &RestoreRequest) -> RestoreResult<PathBuf> {
    destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| RestoreError::InvalidConfig("destination name must be UTF-8".into()))?;
    let operation = operation_hash(destination, &request.operation_id);
    let request_hash = request_hash(destination, request);
    Ok(destination.with_file_name(format!(
        ".event-stream-restore-{operation}-{request_hash}.sqlite3"
    )))
}

fn request_hash(destination: &Path, request: &RestoreRequest) -> String {
    let mut request_hash = Sha256::new();
    update_path_hash(&mut request_hash, destination);
    request_hash.update([0]);
    request_hash.update(request.operation_id.as_str().as_bytes());
    request_hash.update([0]);
    request_hash.update(request.backup_identity.0);
    hex_prefix(&request_hash.finalize(), 32)
}

fn owner_path(destination: &Path, operation_id: &RestoreOperationId) -> PathBuf {
    destination.with_file_name(format!(
        ".event-stream-restore-{}.owner",
        operation_hash(destination, operation_id)
    ))
}

fn operation_hash(destination: &Path, operation_id: &RestoreOperationId) -> String {
    let mut hash = Sha256::new();
    update_path_hash(&mut hash, destination);
    hash.update([0]);
    hash.update(operation_id.as_str().as_bytes());
    hex_prefix(&hash.finalize(), 32)
}

#[cfg(unix)]
fn update_path_hash(hash: &mut Sha256, path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    hash.update(path.as_os_str().as_bytes());
}

#[cfg(not(unix))]
fn update_path_hash(hash: &mut Sha256, path: &Path) {
    hash.update(path.as_os_str().to_string_lossy().as_bytes());
}

fn ensure_owner_reservation(
    request: &RestoreRequest,
    destination: &Path,
    max_staging_bytes: u64,
    failure: &AtomicU8,
    pause_marker: Option<&Path>,
) -> RestoreResult<usize> {
    let owner = owner_path(destination, &request.operation_id);
    match std::fs::create_dir(&owner) {
        Ok(()) => {
            sync_directory(destination.parent().unwrap())?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let metadata = std::fs::symlink_metadata(&owner).map_err(|error| {
                RestoreError::StorageFailure(format!("inspect restore owner directory: {error}"))
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(RestoreError::StagingCleanupFailed {
                    paths: vec![owner.clone()],
                });
            }
        }
        Err(error) => {
            return Err(RestoreError::StorageFailure(format!(
                "reserve restore owner directory: {error}"
            )))
        }
    }
    let charge = request
        .operation_id
        .as_str()
        .len()
        .checked_add(destination.as_os_str().len())
        .and_then(|value| value.checked_add(512))
        .ok_or(RestoreError::CapacityExceeded)?;
    if u64::try_from(charge).map_err(|_| RestoreError::CapacityExceeded)? > max_staging_bytes {
        return Err(RestoreError::CapacityExceeded);
    }
    let expected = owner.join(format!("request-{}", request_hash(destination, request)));
    let entries = bounded_directory_entries(&owner)?;
    if entries.is_empty() {
        std::fs::create_dir(&expected).map_err(|error| {
            RestoreError::StorageFailure(format!("bind restore request identity: {error}"))
        })?;
        sync_directory(&owner)?;
        sync_directory(destination.parent().unwrap())?;
        if consume_or_pause(
            failure,
            SqliteRestoreFailureInjection::AfterOwnerReservation,
            pause_marker,
        ) {
            return Err(RestoreError::StorageFailure(
                "injected failure after restore owner reservation".into(),
            ));
        }
    } else if entries.len() != 1 || entries[0] != expected {
        return Err(RestoreError::RequestConflict {
            operation_id: request.operation_id.clone(),
        });
    }
    validate_owner_reservation(request, destination)?;
    Ok(charge)
}

fn validate_owner_reservation(request: &RestoreRequest, destination: &Path) -> RestoreResult<()> {
    let owner = owner_path(destination, &request.operation_id);
    let owner_metadata = std::fs::symlink_metadata(&owner).map_err(|error| {
        RestoreError::StorageFailure(format!("inspect restore owner directory: {error}"))
    })?;
    if owner_metadata.file_type().is_symlink() || !owner_metadata.is_dir() {
        return Err(RestoreError::StagingCleanupFailed { paths: vec![owner] });
    }
    let expected = owner.join(format!("request-{}", request_hash(destination, request)));
    let entries = bounded_directory_entries(&owner)?;
    if entries.len() != 1 || entries[0] != expected {
        return Err(RestoreError::RequestConflict {
            operation_id: request.operation_id.clone(),
        });
    }
    let metadata = std::fs::symlink_metadata(&expected).map_err(|error| {
        RestoreError::StorageFailure(format!("inspect restore request binding: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RestoreError::StagingCleanupFailed {
            paths: vec![expected],
        });
    }
    let unexpected = bounded_directory_entries(&expected)?;
    if !unexpected.is_empty() {
        return Err(RestoreError::StagingCleanupFailed { paths: unexpected });
    }
    Ok(())
}

fn bounded_directory_entries(directory: &Path) -> RestoreResult<Vec<PathBuf>> {
    let mut entries = Vec::with_capacity(2);
    for entry in std::fs::read_dir(directory).map_err(|error| {
        RestoreError::StorageFailure(format!("inspect restore reservation: {error}"))
    })? {
        entries.push(
            entry
                .map_err(|error| {
                    RestoreError::StorageFailure(format!("read restore reservation: {error}"))
                })?
                .path(),
        );
        if entries.len() == 2 {
            break;
        }
    }
    Ok(entries)
}

fn remove_owner_files(destination: &Path, request: &RestoreRequest) -> RestoreResult<()> {
    let owner = owner_path(destination, &request.operation_id);
    if !path_entry_exists(&owner)? {
        return Ok(());
    }
    validate_owner_reservation(request, destination)?;
    let binding = owner.join(format!("request-{}", request_hash(destination, request)));
    std::fs::remove_dir(&binding).map_err(|_| RestoreError::StagingCleanupFailed {
        paths: vec![binding],
    })?;
    std::fs::remove_dir(&owner)
        .map_err(|_| RestoreError::StagingCleanupFailed { paths: vec![owner] })?;
    sync_directory(destination.parent().unwrap())
}

fn path_entry_exists(path: &Path) -> RestoreResult<bool> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(RestoreError::StorageFailure(format!(
            "inspect restore control path {}: {error}",
            path.display()
        ))),
    }
}

fn hex_prefix(bytes: &[u8], count: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(count * 2);
    for byte in bytes.iter().take(count) {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

fn restore_blocking(
    root: &Path,
    request: &RestoreRequest,
    config: &RestoreConfig,
    failure: &AtomicU8,
    pause_marker: Option<&Path>,
    #[cfg(feature = "test-support")] observer: Option<&dyn SqliteRestoreObserver>,
) -> RestoreResult<RestoreReceipt> {
    let destination = resolve_destination(root, &request.destination)?;
    let _destination_ownership =
        Ownership::acquire_restore_destination(&destination).map_err(map_store_error)?;
    let staging = staging_path(&destination, request)?;

    if destination.exists() {
        let receipt = read_published_receipt(&destination, request, true)?;
        finish_staging_alias_if_owned(&staging, &destination, request)?;
        return Ok(receipt);
    }
    let owner = owner_path(&destination, &request.operation_id);
    let mut staging_present = path_entry_exists(&staging)?;
    if staging_present {
        validate_staging_file_boundary(&staging)?;
    }
    if staging_present
        && path_entry_exists(&owner)?
        && std::fs::metadata(&staging)
            .map_err(|error| {
                RestoreError::StorageFailure(format!("inspect reserved staging file: {error}"))
            })?
            .len()
            == 0
    {
        validate_owner_reservation(request, &destination)?;
        std::fs::remove_file(&staging).map_err(|error| {
            RestoreError::StorageFailure(format!("remove empty reserved staging file: {error}"))
        })?;
        sync_directory(destination.parent().unwrap())?;
        staging_present = false;
    }
    if staging_present {
        let incomplete = validate_staging_owner(&staging, request, &destination)?;
        ensure_owner_reservation(
            request,
            &destination,
            config.max_staging_bytes,
            failure,
            pause_marker,
        )?;
        #[cfg(feature = "test-support")]
        observe(observer, SqliteRestoreStage::OwnerReserved);
        if incomplete {
            return Err(RestoreError::IncompleteStaging {
                operation_id: request.operation_id.clone(),
                destination: Box::new(destination),
            });
        }
        validate_stored_mapping_count(
            &Connection::open_with_flags(
                &staging,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "open completed staging for validation: {error}"
                ))
            })?,
        )?;
        sync_file(&staging)?;
        publish_no_replace(&staging, &destination, request, failure, pause_marker)?;
        return read_published_receipt(&destination, request, true);
    }

    let source_ownership = Ownership::acquire(&request.source).map_err(map_store_error)?;
    let (source, actual) = open_locked_source(source_ownership.path(), config)?;
    if actual != request.backup_identity {
        return Err(RestoreError::IdentityMismatch {
            expected: request.backup_identity,
            actual,
        });
    }
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::SourceValidated);

    let owner_charge = ensure_owner_reservation(
        request,
        &destination,
        config.max_staging_bytes,
        failure,
        pause_marker,
    )?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::OwnerReserved);

    let mut target = create_incomplete_staging(
        &staging,
        request,
        &destination,
        config,
        owner_charge,
        failure,
        pause_marker,
    )?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::StagingCreated);
    let result = (|| {
        import_all(
            &source,
            &mut target,
            request,
            config,
            #[cfg(feature = "test-support")]
            observer,
        )?;
        validate_import(&target)?;
        #[cfg(feature = "test-support")]
        observe(observer, SqliteRestoreStage::RelationalValidationCompleted);
        complete_staging(&mut target, request, &destination)?;
        validate_stored_mapping_count(&target)?;
        #[cfg(feature = "test-support")]
        observe(observer, SqliteRestoreStage::StagingCompleted);
        enforce_staging_limit(&target, config.max_staging_bytes)?;
        Ok(())
    })();
    if let Err(error) = result {
        drop(target);
        return Err(error);
    }
    target
        .close()
        .map_err(|(_, error)| RestoreError::StorageFailure(format!("close staging: {error}")))?;
    sync_file(&staging)?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::StagingSynced);
    if consume_or_pause(
        failure,
        SqliteRestoreFailureInjection::AfterStagingCompletion,
        pause_marker,
    ) {
        return Err(RestoreError::StorageFailure(
            "injected failure after staging completion".into(),
        ));
    }
    publish_no_replace(&staging, &destination, request, failure, pause_marker)?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::DestinationPublished);
    read_published_receipt(&destination, request, true)
}

#[cfg(feature = "test-support")]
fn observe(observer: Option<&dyn SqliteRestoreObserver>, stage: SqliteRestoreStage) {
    if let Some(observer) = observer {
        observer.observe(stage);
    }
}

fn open_read_only(path: &Path) -> RestoreResult<Connection> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| RestoreError::CorruptBackup(format!("open backup read-only: {error}")))
}

fn create_incomplete_staging(
    path: &Path,
    request: &RestoreRequest,
    destination: &Path,
    config: &RestoreConfig,
    owner_charge: usize,
    failure: &AtomicU8,
    pause_marker: Option<&Path>,
) -> RestoreResult<Connection> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| RestoreError::StorageFailure(format!("reserve staging: {error}")))?;
    if consume_or_pause(
        failure,
        SqliteRestoreFailureInjection::AfterStagingFileReservation,
        pause_marker,
    ) {
        return Err(RestoreError::StorageFailure(
            "injected failure after staging file reservation".into(),
        ));
    }
    let flags = OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let conn = Connection::open_with_flags(path, flags)
        .map_err(|error| RestoreError::StorageFailure(format!("create staging: {error}")))?;
    conn.pragma_update(None, "journal_mode", "DELETE")
        .map_err(|error| RestoreError::StorageFailure(format!("set staging journal: {error}")))?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(|error| RestoreError::StorageFailure(format!("set staging sync: {error}")))?;
    conn.pragma_update(None, "foreign_keys", true)
        .map_err(|error| {
            RestoreError::StorageFailure(format!("set staging foreign keys: {error}"))
        })?;
    let cache_kib = config.copy_buffer_bytes.div_ceil(1024).clamp(1, 16 * 1024);
    conn.pragma_update(
        None,
        "cache_size",
        -i64::try_from(cache_kib).map_err(|_| RestoreError::CapacityExceeded)?,
    )
    .map_err(|error| RestoreError::StorageFailure(format!("bound staging cache: {error}")))?;
    conn.pragma_update(None, "temp_store", "FILE")
        .map_err(|error| {
            RestoreError::StorageFailure(format!("bound staging temp memory: {error}"))
        })?;
    let page_size: u64 = conn
        .pragma_query_value(None, "page_size", |row| row.get(0))
        .map_err(|error| {
            RestoreError::StorageFailure(format!("read staging page size: {error}"))
        })?;
    let database_bytes = config
        .max_staging_bytes
        .checked_sub(u64::try_from(owner_charge).map_err(|_| RestoreError::CapacityExceeded)?)
        .ok_or(RestoreError::CapacityExceeded)?;
    let pages = database_bytes / page_size;
    if pages == 0 || pages > i32::MAX as u64 {
        return Err(RestoreError::InvalidConfig(
            "staging byte limit cannot be represented as SQLite pages".into(),
        ));
    }
    conn.pragma_update(None, "max_page_count", pages)
        .map_err(|error| {
            RestoreError::StorageFailure(format!("set staging page limit: {error}"))
        })?;
    let destination = destination
        .to_str()
        .ok_or_else(|| RestoreError::InvalidConfig("canonical destination must be UTF-8".into()))?;
    let schema_result = conn.execute_batch(
        "BEGIN IMMEDIATE;
         CREATE TABLE event_stream_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),format_version INTEGER NOT NULL,lifecycle_receipt_count INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_count>=0),lifecycle_receipt_bytes INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_receipt_bytes>=0),retired_lifetime_count INTEGER NOT NULL DEFAULT 0 CHECK(retired_lifetime_count>=0),retired_metadata_bytes INTEGER NOT NULL DEFAULT 0 CHECK(retired_metadata_bytes>=0),restore_incomplete INTEGER NOT NULL DEFAULT 1 CHECK(restore_incomplete IN (0,1,2)));
         CREATE TABLE event_streams(stream_key INTEGER PRIMARY KEY,public_id TEXT NOT NULL,incarnation BLOB NOT NULL CHECK(length(incarnation)=16),floor BLOB NOT NULL CHECK(length(floor)=8),tail BLOB NOT NULL CHECK(length(tail)=8),retired INTEGER NOT NULL DEFAULT 0 CHECK(retired IN (0,1)),UNIQUE(public_id,incarnation));
         CREATE TABLE event_records(stream_key INTEGER NOT NULL,offset BLOB NOT NULL CHECK(length(offset)=8),event_id TEXT NOT NULL,schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(stream_key,offset),UNIQUE(stream_key,event_id),FOREIGN KEY(stream_key) REFERENCES event_streams(stream_key));
         CREATE TABLE event_stream_names(public_id TEXT PRIMARY KEY,latest_incarnation BLOB NOT NULL CHECK(length(latest_incarnation)=16),active_stream_key INTEGER);
         CREATE TABLE lifecycle_receipts(operation_id TEXT PRIMARY KEY,action INTEGER NOT NULL CHECK(action IN (0,1)),expected_public_id TEXT NOT NULL,expected_incarnation BLOB NOT NULL CHECK(length(expected_incarnation)=16),replacement_incarnation BLOB CHECK(replacement_incarnation IS NULL OR length(replacement_incarnation)=16),charge INTEGER NOT NULL CHECK(charge>=0));
         CREATE TABLE restore_metadata(singleton INTEGER PRIMARY KEY CHECK(singleton=1),operation_id TEXT NOT NULL,backup_identity BLOB NOT NULL CHECK(length(backup_identity)=32),destination TEXT NOT NULL,mapping_count INTEGER NOT NULL CHECK(mapping_count>=0));
         CREATE TABLE restore_mappings(old_public_id TEXT NOT NULL,old_incarnation BLOB NOT NULL CHECK(length(old_incarnation)=16),new_incarnation BLOB NOT NULL CHECK(length(new_incarnation)=16),PRIMARY KEY(old_public_id,old_incarnation));
         CREATE TABLE restore_lifetime_keys(old_stream_key INTEGER PRIMARY KEY,new_stream_key INTEGER NOT NULL UNIQUE);
         CREATE INDEX event_streams_retired_idx ON event_streams(retired,stream_key);",
    );
    if let Err(error) = schema_result {
        return Err(map_sql_write("create staging schema", error));
    }
    #[cfg(feature = "snapshots")]
    conn.execute_batch(
        "CREATE TABLE snapshot_metadata(
           singleton INTEGER PRIMARY KEY CHECK(singleton=1),
           staged_count INTEGER NOT NULL CHECK(staged_count>=0), staged_bytes INTEGER NOT NULL CHECK(staged_bytes>=0),
           published_count INTEGER NOT NULL CHECK(published_count>=0), published_bytes INTEGER NOT NULL CHECK(published_bytes>=0),
           chunk_count INTEGER NOT NULL CHECK(chunk_count>=0), chunk_metadata_bytes INTEGER NOT NULL CHECK(chunk_metadata_bytes>=0),
           descriptor_metadata_bytes INTEGER NOT NULL CHECK(descriptor_metadata_bytes>=0),
           receipt_count INTEGER NOT NULL CHECK(receipt_count>=0), receipt_bytes INTEGER NOT NULL CHECK(receipt_bytes>=0));
         CREATE TABLE snapshots(
           snapshot_id BLOB PRIMARY KEY CHECK(typeof(snapshot_id)='blob' AND length(snapshot_id)=16),
           public_id TEXT NOT NULL, incarnation BLOB NOT NULL CHECK(typeof(incarnation)='blob' AND length(incarnation)=16),
           covered BLOB NOT NULL CHECK(typeof(covered)='blob' AND length(covered)=8), schema_id TEXT NOT NULL,
           schema_version INTEGER NOT NULL, content_bytes BLOB NOT NULL CHECK(typeof(content_bytes)='blob' AND length(content_bytes)=8),
           digest BLOB NOT NULL CHECK(typeof(digest)='blob' AND length(digest)=32), state INTEGER NOT NULL CHECK(state BETWEEN 0 AND 4),
           accepted_bytes BLOB NOT NULL CHECK(typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8),
           verified_bytes BLOB NOT NULL CHECK(typeof(verified_bytes)='blob' AND length(verified_bytes)=8),
           checksum_failed INTEGER NOT NULL DEFAULT 0 CHECK(checksum_failed IN (0,1)), cleaned INTEGER NOT NULL DEFAULT 0 CHECK(cleaned IN (0,1)),
           descriptor_charge INTEGER NOT NULL CHECK(descriptor_charge>=0), receipt_reserved INTEGER NOT NULL DEFAULT 0 CHECK(receipt_reserved IN (0,1)));
         CREATE TABLE snapshot_chunks(snapshot_id BLOB NOT NULL,offset BLOB NOT NULL CHECK(typeof(offset)='blob' AND length(offset)=8),bytes BLOB NOT NULL,
           PRIMARY KEY(snapshot_id,offset),FOREIGN KEY(snapshot_id) REFERENCES snapshots(snapshot_id));
         CREATE INDEX snapshots_published_idx ON snapshots(public_id,incarnation,state,covered,snapshot_id);
         CREATE INDEX snapshots_cleanup_idx ON snapshots(state,cleaned,snapshot_id);
         INSERT INTO snapshot_metadata VALUES(1,0,0,0,0,0,0,0,0,0);",
    )
    .map_err(|error| map_sql_write("create staging snapshot schema", error))?;
    conn.execute(
        "INSERT INTO event_stream_metadata(singleton,format_version,restore_incomplete) VALUES(1,2,1)",
        [],
    )
    .map_err(|error| map_sql_write("mark staging incomplete", error))?;
    conn.execute(
        "INSERT INTO restore_metadata(singleton,operation_id,backup_identity,destination,mapping_count) VALUES(1,?1,?2,?3,0)",
        params![request.operation_id.as_str(), request.backup_identity.0.as_slice(), destination],
    )
    .map_err(|error| map_sql_write("store restore identity", error))?;
    conn.execute_batch("COMMIT;")
        .map_err(|error| map_sql_write("commit incomplete staging marker", error))?;
    #[cfg(feature = "retention")]
    super::sqlite_retention::initialize_retention_schema(&conn).map_err(|error| match error {
        crate::application::Error::CapacityExceeded => RestoreError::CapacityExceeded,
        error => RestoreError::StorageFailure(format!("create staging retention schema: {error}")),
    })?;
    #[cfg(feature = "source-journal")]
    super::sqlite_source_journal::initialize_journal_schema(&conn).map_err(
        |error| match error {
            crate::application::Error::CapacityExceeded => RestoreError::CapacityExceeded,
            error => RestoreError::StorageFailure(format!(
                "create staging source-journal schema: {error}"
            )),
        },
    )?;
    #[cfg(feature = "replication")]
    super::sqlite_replication::initialize_replication_schema(
        &conn,
        &super::sqlite::SqliteOptions::new(path),
    )
    .map_err(|error| match error {
        crate::application::Error::CapacityExceeded => RestoreError::CapacityExceeded,
        error => {
            RestoreError::StorageFailure(format!("create staging replication schema: {error}"))
        }
    })?;
    sync_file(path)?;
    Ok(conn)
}

fn map_sql_write(action: &str, error: rusqlite::Error) -> RestoreError {
    if let rusqlite::Error::SqliteFailure(code, _) = &error {
        if code.code == rusqlite::ErrorCode::DiskFull {
            return RestoreError::CapacityExceeded;
        }
    }
    RestoreError::StorageFailure(format!("{action}: {error}"))
}

fn enforce_staging_limit(conn: &Connection, max_bytes: u64) -> RestoreResult<()> {
    let (pages, page_size): (u64, u64) = conn
        .query_row(
            "SELECT page_count,page_size FROM pragma_page_count(),pragma_page_size()",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(|error| RestoreError::StorageFailure(format!("measure staging: {error}")))?;
    pages
        .checked_mul(page_size)
        .filter(|bytes| *bytes <= max_bytes)
        .ok_or(RestoreError::CapacityExceeded)?;
    Ok(())
}

fn import_all(
    source: &Connection,
    target: &mut Connection,
    request: &RestoreRequest,
    config: &RestoreConfig,
    #[cfg(feature = "test-support")] observer: Option<&dyn SqliteRestoreObserver>,
) -> RestoreResult<()> {
    let has_replication = replication_data_present(source)?;
    #[cfg(not(feature = "replication"))]
    if has_replication {
        return Err(RestoreError::UnsupportedBackup(2));
    }
    #[cfg(not(feature = "source-journal"))]
    if journal_data_present(source)? {
        return Err(RestoreError::UnsupportedBackup(2));
    }
    #[cfg(feature = "replication")]
    if has_replication {
        validate_source_replication_origin(source)?;
    }
    let mapping_count = import_identity_mappings(source, target, config)?;
    #[cfg(feature = "replication")]
    let mapping_count = if has_replication {
        mapping_count
            .checked_add(import_replication_identity_mappings(
                source, target, config,
            )?)
            .ok_or(RestoreError::CapacityExceeded)?
    } else {
        mapping_count
    };
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::IdentityMappingsImported);
    import_lifetimes(source, target, config)?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::LifetimesImported);
    import_records(
        source,
        target,
        config,
        #[cfg(feature = "test-support")]
        observer,
    )?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::RecordsImported);
    import_names(source, target, config)?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::NamesImported);
    import_receipts(source, target, config)?;
    #[cfg(feature = "test-support")]
    observe(observer, SqliteRestoreStage::ReceiptsImported);
    import_snapshots(source, target, config)?;
    import_retention(source, target, config)?;
    #[cfg(feature = "source-journal")]
    import_journal(source, target, config)?;
    #[cfg(feature = "replication")]
    if has_replication {
        import_replication(source, target, config)?;
    }
    let counters: (i64, i64, i64, i64) = source
        .query_row(
            "SELECT lifecycle_receipt_count,lifecycle_receipt_bytes,retired_lifetime_count,retired_metadata_bytes FROM event_stream_metadata WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("read source counters: {error}")))?;
    target
        .execute(
            "UPDATE event_stream_metadata SET lifecycle_receipt_count=?1,lifecycle_receipt_bytes=?2,retired_lifetime_count=?3,retired_metadata_bytes=?4 WHERE singleton=1",
            params![counters.0, counters.1, counters.2, counters.3],
        )
        .map_err(|error| map_sql_write("copy lifecycle counters", error))?;
    target
        .execute(
            "UPDATE restore_metadata SET mapping_count=?1 WHERE singleton=1 AND operation_id=?2",
            params![
                i64::try_from(mapping_count).map_err(|_| RestoreError::CapacityExceeded)?,
                request.operation_id.as_str()
            ],
        )
        .map_err(|error| map_sql_write("store mapping count", error))?;
    Ok(())
}

#[cfg(feature = "replication")]
fn validate_source_replication_origin(source: &Connection) -> RestoreResult<()> {
    let has_bootstraps: bool = source
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema
                           WHERE type='table' AND name='replication_origin_bootstraps')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("inspect origin bootstrap schema: {error}"))
        })?;
    let base_identity_mismatch: bool = source
        .query_row(
            "SELECT EXISTS(
               SELECT 1 FROM replication_origin_replicas r,replication_metadata m
                WHERE m.singleton=1 AND r.origin_id<>m.origin_id
               UNION ALL
               SELECT 1 FROM replication_origin_operations o,replication_metadata m
                WHERE m.singleton=1 AND o.origin_id<>m.origin_id
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("audit origin replication identities: {error}"))
        })?;
    let bootstrap_identity_mismatch = if has_bootstraps {
        source
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM replication_origin_bootstraps b,replication_metadata m
                    WHERE m.singleton=1 AND b.origin_id<>m.origin_id
                   UNION ALL
                   SELECT 1 FROM replication_origin_bootstrap_receipts r,replication_metadata m
                    WHERE m.singleton=1 AND r.origin_id<>m.origin_id
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "audit origin bootstrap receipt identities: {error}"
                ))
            })?
    } else {
        false
    };
    if base_identity_mismatch || bootstrap_identity_mismatch {
        return Err(RestoreError::CorruptBackup(
            "origin replication row does not belong to the source origin".into(),
        ));
    }
    if has_bootstraps {
        let active_bootstrap_mismatch: bool = source
            .query_row(
                "SELECT EXISTS(
                   SELECT 1
                   FROM replication_origin_bootstraps b
                   LEFT JOIN replication_origin_replicas r
                     ON r.replica_id=b.replica_id AND r.origin_id=b.origin_id
                    AND r.public_id=b.public_id AND r.incarnation=b.incarnation
                   LEFT JOIN replication_origin_bootstrap_receipts x
                     ON x.bootstrap_id=b.bootstrap_id AND x.kind=0
                   LEFT JOIN snapshots s ON s.snapshot_id=b.snapshot_id
                   WHERE r.mode IS NULL OR r.mode<>2
                      OR x.operation_id IS NULL OR x.completed<>0
                      OR x.destination_operation_id<>b.destination_operation_id
                      OR x.replica_id<>b.replica_id OR x.origin_id<>b.origin_id
                      OR x.public_id<>b.public_id OR x.incarnation<>b.incarnation
                      OR x.destination_epoch<>b.destination_epoch
                      OR x.snapshot_id<>b.snapshot_id OR x.covered<>b.covered
                      OR x.schema_id<>b.schema_id OR x.schema_version<>b.schema_version
                      OR x.content_bytes<>b.content_bytes OR x.digest<>b.digest
                      OR x.captured_tail<>b.captured_tail
                      OR x.protection_expires<>b.protection_expires
                      OR s.snapshot_id IS NULL OR s.state<>3
                      OR s.public_id<>b.public_id OR s.incarnation<>b.incarnation
                      OR s.covered<>b.covered OR s.schema_id<>b.schema_id
                      OR s.schema_version<>b.schema_version
                      OR s.content_bytes<>b.content_bytes OR s.digest<>b.digest
                   UNION ALL
                   SELECT 1 FROM replication_origin_replicas r
                    WHERE r.mode=2 AND NOT EXISTS(
                      SELECT 1 FROM replication_origin_bootstraps b
                       WHERE b.replica_id=r.replica_id AND b.origin_id=r.origin_id
                         AND b.public_id=r.public_id AND b.incarnation=r.incarnation)
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("audit active origin bootstrap state: {error}"))
            })?;
        if active_bootstrap_mismatch {
            return Err(RestoreError::CorruptBackup(
                "active origin bootstrap state is inconsistent".into(),
            ));
        }
    }

    let include_retention = retention_schema_present(source)?;
    let record_union = if include_retention {
        "SELECT stream_key,offset,event_id,schema_id,payload FROM event_records
         UNION ALL
         SELECT stream_key,offset,event_id,schema_id,payload FROM retention_generated_records"
    } else {
        "SELECT stream_key,offset,event_id,schema_id,payload FROM event_records"
    };
    let mut replicas = source
        .prepare(&format!(
            "WITH all_records AS ({record_union})
             SELECT r.mode,
                    CASE WHEN r.pending_batch IS NULL OR
                                   (typeof(r.pending_batch)='blob' AND length(r.pending_batch)=16)
                         THEN r.pending_batch END,
                    r.backlog_records,
                    CASE WHEN typeof(r.backlog_bytes)='blob' AND length(r.backlog_bytes)=8 THEN r.backlog_bytes END,
                    CASE WHEN r.oldest_backlog_at IS NULL OR
                                   (typeof(r.oldest_backlog_at)='blob' AND length(r.oldest_backlog_at)=8)
                         THEN r.oldest_backlog_at END,
                    count(e.offset),
                    coalesce(sum(octet_length(e.event_id)+octet_length(e.schema_id)+octet_length(e.payload)+384),0),
                    count(t.committed_at),s.stream_key,min(t.committed_at)
             FROM replication_origin_replicas r
             LEFT JOIN event_streams s ON s.public_id=r.public_id AND s.incarnation=r.incarnation
             LEFT JOIN all_records e ON e.stream_key=s.stream_key AND e.offset>r.acknowledged
             LEFT JOIN replication_origin_record_times t ON t.stream_key=e.stream_key AND t.offset=e.offset
             GROUP BY r.rowid"
        ))
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("prepare origin backlog audit: {error}"))
        })?;
    let mut rows = replicas.query([]).map_err(|error| {
        RestoreError::CorruptBackup(format!("query origin backlog audit: {error}"))
    })?;
    while let Some(row) = rows.next().map_err(|error| {
        RestoreError::CorruptBackup(format!("step origin backlog audit: {error}"))
    })? {
        let mode: i64 = row.get(0).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin replica mode: {error}"))
        })?;
        let pending: Option<Vec<u8>> = row.get(1).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin pending batch: {error}"))
        })?;
        let stored_records: i64 = row.get(2).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin backlog count: {error}"))
        })?;
        let stored_bytes: Option<Vec<u8>> = row.get(3).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin backlog bytes: {error}"))
        })?;
        let oldest: Option<Vec<u8>> = row.get(4).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin backlog timestamp: {error}"))
        })?;
        let actual_records: i64 = row.get(5).map_err(|error| {
            RestoreError::CorruptBackup(format!("read actual origin backlog count: {error}"))
        })?;
        let actual_bytes: i64 = row.get(6).map_err(|error| {
            RestoreError::CorruptBackup(format!("read actual origin backlog bytes: {error}"))
        })?;
        let timed_records: i64 = row.get(7).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin backlog timestamps: {error}"))
        })?;
        let stream_key: Option<i64> = row.get(8).map_err(|error| {
            RestoreError::CorruptBackup(format!("read origin backlog lifetime: {error}"))
        })?;
        let actual_oldest: Option<Vec<u8>> = row.get(9).map_err(|error| {
            RestoreError::CorruptBackup(format!("read actual origin backlog timestamp: {error}"))
        })?;
        let stored_bytes = stored_bytes
            .as_deref()
            .and_then(|bytes| decode_offset(bytes).ok());
        let invalid = match mode {
            1 => {
                stored_records != 0
                    || stored_bytes != Some(0)
                    || oldest.is_some()
                    || pending.is_some()
            }
            0 | 2 => {
                stream_key.is_none()
                    || stored_records != actual_records
                    || stored_bytes != u64::try_from(actual_bytes).ok()
                    || timed_records != actual_records
                    || oldest != actual_oldest
            }
            _ => true,
        };
        if invalid {
            return Err(RestoreError::CorruptBackup(
                "origin replica backlog accounting does not match retained history".into(),
            ));
        }
    }
    drop(rows);
    drop(replicas);

    let mut after: Option<i64> = None;
    loop {
        type Prepared = (i64, Vec<u8>, Vec<u8>, i64, Vec<u8>, i64, Vec<u8>, i64, bool);
        let prepared: Option<Prepared> = source
            .query_row(
                "SELECT o.rowid,
                        CASE WHEN typeof(o.expected_after)='blob' AND length(o.expected_after)=8 THEN o.expected_after END,
                        CASE WHEN typeof(o.result_through)='blob' AND length(o.result_through)=8 THEN o.result_through END,
                        o.limit_records,
                        CASE WHEN typeof(o.limit_bytes)='blob' AND length(o.limit_bytes)=8 THEN o.limit_bytes END,
                        o.result_batch_records,
                        CASE WHEN typeof(o.result_batch_bytes)='blob' AND length(o.result_batch_bytes)=8 THEN o.result_batch_bytes END,
                        s.stream_key,
                        EXISTS(SELECT 1 FROM replication_origin_replicas r
                               WHERE r.replica_id=o.replica_id AND r.origin_id=o.origin_id
                                 AND r.public_id=o.public_id AND r.incarnation=o.incarnation
                                 AND r.pending_batch=o.batch_id)
                 FROM replication_origin_operations o
                 LEFT JOIN event_streams s
                   ON s.public_id=o.public_id AND s.incarnation=o.incarnation
                 WHERE o.kind=1 AND (?1 IS NULL OR o.rowid>?1)
                 ORDER BY o.rowid LIMIT 1",
                params![after],
                |row| {
                    Ok((
                        row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                        row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?,
                        row.get(8)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("read prepared replication receipt: {error}"))
            })?;
        let Some((
            rowid,
            expected,
            through,
            limit_records,
            limit_bytes,
            result_records,
            result_bytes,
            stream_key,
            active,
        )) = prepared
        else {
            break;
        };
        after = Some(rowid);
        let expected = decode_offset(&expected).map_err(|error| {
            RestoreError::CorruptBackup(format!("invalid prepared batch start: {error}"))
        })?;
        let through = decode_offset(&through).map_err(|error| {
            RestoreError::CorruptBackup(format!("invalid prepared batch end: {error}"))
        })?;
        let limit_records = usize::try_from(limit_records).map_err(|_| {
            RestoreError::CorruptBackup("invalid prepared batch record limit".into())
        })?;
        let limit_bytes = decode_offset(&limit_bytes).map_err(|error| {
            RestoreError::CorruptBackup(format!("invalid prepared batch byte limit: {error}"))
        })?;
        let result_records = usize::try_from(result_records).map_err(|_| {
            RestoreError::CorruptBackup("invalid prepared batch record count".into())
        })?;
        let result_bytes = decode_offset(&result_bytes).map_err(|error| {
            RestoreError::CorruptBackup(format!("invalid prepared batch byte count: {error}"))
        })?;
        if through < expected {
            return Err(RestoreError::CorruptBackup(
                "prepared replication batch has an inverted range".into(),
            ));
        }
        if result_records != usize::try_from(through - expected).unwrap_or(usize::MAX)
            || result_records > limit_records
            || result_bytes > limit_bytes
        {
            return Err(RestoreError::CorruptBackup(
                "prepared replication receipt does not match retained history".into(),
            ));
        }
        if active {
            let aggregate_sql = format!(
                "SELECT count(*),coalesce(sum(octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384),0)
                 FROM ({record_union}) WHERE stream_key=?1 AND offset>?2 AND offset<=?3"
            );
            let (actual_records, actual_bytes): (i64, i64) = source
                .query_row(
                    &aggregate_sql,
                    params![
                        stream_key,
                        expected.to_be_bytes().as_slice(),
                        through.to_be_bytes().as_slice()
                    ],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(|error| {
                    RestoreError::CorruptBackup(format!(
                        "audit active prepared replication batch: {error}"
                    ))
                })?;
            if usize::try_from(actual_records).ok() != Some(result_records)
                || u64::try_from(actual_bytes).ok() != Some(result_bytes)
            {
                return Err(RestoreError::CorruptBackup(
                    "active prepared replication receipt does not match retained history".into(),
                ));
            }
        }
    }
    Ok(())
}

fn replication_data_present(source: &Connection) -> RestoreResult<bool> {
    let count: u32 = source
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('replication_metadata','replication_origin_replicas','replication_origin_record_times',
              'replication_origin_operations','replication_destination_streams',
              'replication_destination_records','replication_destination_receipts',
              'replication_destination_floor_receipts','replication_destination_accounting',
              'replication_origin_bootstraps','replication_origin_bootstrap_receipts','replication_destination_bootstraps',
              'replication_destination_bootstrap_chunks','replication_destination_bootstrap_records',
              'replication_destination_bootstrap_batch_receipts',
              'replication_destination_published_bootstraps')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("inspect replication schema: {error}"))
        })?;
    match count {
        0 => Ok(false),
        8 | 9 => source
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas UNION ALL
                               SELECT 1 FROM replication_origin_record_times UNION ALL
                               SELECT 1 FROM replication_origin_operations UNION ALL
                               SELECT 1 FROM replication_destination_streams UNION ALL
                               SELECT 1 FROM replication_destination_records UNION ALL
                               SELECT 1 FROM replication_destination_receipts UNION ALL
                               SELECT 1 FROM replication_destination_floor_receipts)",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("inspect replication rows: {error}"))
            }),
        16 => source
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas UNION ALL
                               SELECT 1 FROM replication_origin_record_times UNION ALL
                               SELECT 1 FROM replication_origin_operations UNION ALL
                               SELECT 1 FROM replication_destination_streams UNION ALL
                               SELECT 1 FROM replication_destination_records UNION ALL
                               SELECT 1 FROM replication_destination_receipts UNION ALL
                               SELECT 1 FROM replication_destination_floor_receipts UNION ALL
                               SELECT 1 FROM replication_origin_bootstraps UNION ALL
                               SELECT 1 FROM replication_origin_bootstrap_receipts UNION ALL
                               SELECT 1 FROM replication_destination_bootstraps UNION ALL
                               SELECT 1 FROM replication_destination_bootstrap_chunks UNION ALL
                               SELECT 1 FROM replication_destination_bootstrap_records UNION ALL
                               SELECT 1 FROM replication_destination_bootstrap_batch_receipts UNION ALL
                               SELECT 1 FROM replication_destination_published_bootstraps)",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("inspect replication rows: {error}"))
            }),
        _ => Err(RestoreError::CorruptBackup(
            "replication schema is only partially present".into(),
        )),
    }
}

#[cfg(feature = "replication")]
fn import_replication_identity_mappings(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<u64> {
    let has_bootstrap_schema: bool = source
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='replication_origin_bootstrap_receipts')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("inspect origin receipt identity schema: {error}")))?;
    let sql = if has_bootstrap_schema {
        "SELECT octet_length(public_id),
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
         FROM (SELECT public_id,incarnation FROM replication_origin_replicas
               UNION SELECT public_id,incarnation FROM replication_origin_operations
               UNION SELECT public_id,incarnation FROM replication_origin_bootstrap_receipts)
         ORDER BY public_id,incarnation"
    } else {
        "SELECT octet_length(public_id),
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
         FROM (SELECT public_id,incarnation FROM replication_origin_replicas
               UNION SELECT public_id,incarnation FROM replication_origin_operations)
         ORDER BY public_id,incarnation"
    };
    let mut statement = source.prepare(sql).map_err(|error| {
        RestoreError::CorruptBackup(format!("prepare origin replication identities: {error}"))
    })?;
    let mut rows = statement.query([]).map_err(|error| {
        RestoreError::CorruptBackup(format!("query origin replication identities: {error}"))
    })?;
    let mut count = 0u64;
    let mut pending = None;
    loop {
        let mut batch = Vec::with_capacity(config.page.max_records.min(64));
        let mut bytes = 0usize;
        while batch.len() < config.page.max_records {
            let item = if let Some(value) = pending.take() {
                value
            } else {
                let Some(row) = rows.next().map_err(|error| {
                    RestoreError::CorruptBackup(format!(
                        "step origin replication identities: {error}"
                    ))
                })?
                else {
                    break;
                };
                let length: i64 = row.get(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read origin identity length: {error}"))
                })?;
                let public: Option<String> = row.get(1).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read origin identity name: {error}"))
                })?;
                let incarnation: Option<Vec<u8>> = row.get(2).map_err(|error| {
                    RestoreError::CorruptBackup(format!(
                        "read origin identity incarnation: {error}"
                    ))
                })?;
                let public = StreamId::new(public.ok_or_else(|| {
                    RestoreError::CorruptBackup("invalid origin replication stream name".into())
                })?)
                .map_err(|_| {
                    RestoreError::CorruptBackup("invalid origin replication stream name".into())
                })?;
                (
                    public,
                    fixed::<16>(incarnation, "origin replication incarnation")?,
                    usize::try_from(length).map_err(|_| {
                        RestoreError::CorruptBackup(
                            "invalid origin replication stream length".into(),
                        )
                    })?,
                )
            };
            let charge = item
                .2
                .checked_mul(2)
                .and_then(|n| n.checked_add(128))
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|n| n > config.page.max_bytes)
            {
                pending = Some(item);
                break;
            }
            bytes += charge;
            batch.push(item);
        }
        if batch.is_empty() {
            break;
        }
        let tx = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write("begin origin replication mapping page", error))?;
        for (public, old, _) in &batch {
            let changed=tx.execute("INSERT OR IGNORE INTO restore_mappings(old_public_id,old_incarnation,new_incarnation) VALUES(?1,?2,?3)",params![public.as_str(),old.as_slice(),super::new_incarnation().0.as_slice()]).map_err(|error|map_sql_write("insert origin replication identity mapping",error))?;
            count = count
                .checked_add(changed as u64)
                .ok_or(RestoreError::CapacityExceeded)?;
        }
        tx.commit()
            .map_err(|error| map_sql_write("commit origin replication mapping page", error))?;
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(count)
}

#[cfg(feature = "replication")]
fn copy_replication_rows(
    source: &Connection,
    target: &mut Connection,
    select: &str,
    insert: &str,
    columns: usize,
    config: &RestoreConfig,
    label: &str,
) -> RestoreResult<()> {
    let mut after: Option<i64> = None;
    loop {
        let mut statement = source
            .prepare(select)
            .map_err(|error| RestoreError::CorruptBackup(format!("prepare {label}: {error}")))?;
        let mut query_values = vec![
            after.map_or(Value::Null, Value::Integer),
            Value::Integer(
                i64::try_from(config.page.max_records)
                    .map_err(|_| RestoreError::CapacityExceeded)?,
            ),
        ];
        if statement.parameter_count() == 3 {
            query_values.push(Value::Integer(
                i64::try_from(config.page.max_bytes).map_err(|_| RestoreError::CapacityExceeded)?,
            ));
        }
        let mut rows = statement
            .query(params_from_iter(query_values.iter()))
            .map_err(|error| RestoreError::CorruptBackup(format!("query {label}: {error}")))?;
        let mut batch: Vec<(i64, Vec<Value>)> = Vec::with_capacity(config.page.max_records.min(64));
        let mut bytes = 0usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| RestoreError::CorruptBackup(format!("step {label}: {error}")))?
        {
            let rowid: i64 = row.get(0).map_err(|error| {
                RestoreError::CorruptBackup(format!("read {label} row identity: {error}"))
            })?;
            let charge: i64 = row.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read {label} row charge: {error}"))
            })?;
            let valid: i64 = row.get(2).map_err(|error| {
                RestoreError::CorruptBackup(format!("read {label} validity: {error}"))
            })?;
            let charge = usize::try_from(charge)
                .map_err(|_| RestoreError::CorruptBackup(format!("invalid {label} row charge")))?;
            if valid != 1 {
                return Err(RestoreError::CorruptBackup(format!("invalid {label} row")));
            }
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > config.page.max_bytes)
            {
                break;
            }
            let mut values = Vec::with_capacity(columns);
            for column in 0..columns {
                values.push(row.get::<_, Value>(3 + column).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read {label} value: {error}"))
                })?);
            }
            bytes += charge;
            batch.push((rowid, values));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write(&format!("begin {label} page"), error))?;
        for (_, values) in &batch {
            transaction
                .execute(insert, params_from_iter(values.iter()))
                .map_err(|error| map_sql_write(&format!("insert {label}"), error))?;
        }
        transaction
            .commit()
            .map_err(|error| map_sql_write(&format!("commit {label} page"), error))?;
        after = Some(batch.last().unwrap().0);
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(())
}

#[cfg(feature = "replication")]
fn import_replication(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let mut origin_rows: bool = source
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_origin_replicas UNION ALL
                           SELECT 1 FROM replication_origin_record_times UNION ALL
                           SELECT 1 FROM replication_origin_operations)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!(
                "inspect origin replication restore state: {error}"
            ))
        })?;
    let has_bootstrap_schema: bool = source
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='replication_origin_bootstraps')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("inspect replication bootstrap schema: {error}")))?;
    let has_completed_receipt_column = if has_bootstrap_schema {
        source
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM pragma_table_info('replication_origin_bootstrap_receipts')
                   WHERE name='completed'
                 )",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "inspect origin bootstrap receipt schema: {error}"
                ))
            })?
    } else {
        false
    };
    if has_bootstrap_schema {
        let bootstrap_origin_rows: bool = source.query_row(
            "SELECT EXISTS(SELECT 1 FROM replication_origin_bootstraps UNION ALL SELECT 1 FROM replication_origin_bootstrap_receipts)",
            [],
            |row| row.get::<_, bool>(0),
        ).map_err(|error|RestoreError::CorruptBackup(format!("inspect origin bootstrap restore state: {error}")))?;
        origin_rows |= bootstrap_origin_rows;
    }
    if origin_rows {
        import_replication_origin(
            source,
            target,
            config,
            has_bootstrap_schema,
            has_completed_receipt_column,
        )?;
    }

    copy_replication_rows(
        source,
        target,
        "SELECT rowid,
                octet_length(public_id)+96,
                typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(floor)='blob' AND length(floor)=8
                  AND typeof(tail)='blob' AND length(tail)=8,
                origin_id,public_id,incarnation,floor,tail
         FROM replication_destination_streams
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_streams VALUES(?1,?2,?3,?4,?5)",
        5,
        config,
        "replication destination stream",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,
                octet_length(public_id)+octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384,
                typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(offset)='blob' AND length(offset)=8
                  AND typeof(event_id)='text' AND octet_length(event_id) BETWEEN 1 AND 256
                  AND typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256
                  AND typeof(schema_version)='integer'
                  AND typeof(payload)='blob' AND octet_length(payload)<=?3,
                origin_id,public_id,incarnation,offset,event_id,schema_id,schema_version,
                CASE WHEN typeof(payload)='blob' AND octet_length(payload)<=?3 THEN payload END
         FROM replication_destination_records
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_records VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        8,
        config,
        "replication destination record",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,octet_length(public_id)+208,
                typeof(batch_id)='blob' AND length(batch_id)=16
                  AND typeof(destination_epoch)='blob' AND length(destination_epoch)=16
                  AND typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(after_offset)='blob' AND length(after_offset)=8
                  AND typeof(through_offset)='blob' AND length(through_offset)=8,
                batch_id,destination_epoch,origin_id,public_id,incarnation,after_offset,through_offset
         FROM replication_destination_receipts
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_receipts VALUES(?1,?2,?3,?4,?5,?6,?7)",
        7,
        config,
        "replication destination receipt",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,octet_length(operation_id)+octet_length(public_id)+208,
                typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256
                  AND typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(destination_epoch)='blob' AND length(destination_epoch)=16
                  AND typeof(expected_floor)='blob' AND length(expected_floor)=8
                  AND typeof(new_floor)='blob' AND length(new_floor)=8,
                operation_id,origin_id,public_id,incarnation,destination_epoch,expected_floor,new_floor
         FROM replication_destination_floor_receipts
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_floor_receipts VALUES(?1,?2,?3,?4,?5,?6,?7)",
        7,
        config,
        "replication destination floor receipt",
    )?;
    if has_bootstrap_schema {
        // Restore aborts unfinished uploads. Check their original counters before
        // that state change makes them eligible for partial cleanup.
        validate_unpublished_replica_chunks(source, config)?;
        import_replication_bootstraps(source, target, config)?;
        validate_restored_replication_bootstraps(target, config)?;
    }
    super::sqlite_replication::rebuild_destination_accounting(target).map_err(|error| {
        RestoreError::CorruptBackup(format!("rebuild restored replication accounting: {error}"))
    })?;
    Ok(())
}

#[cfg(feature = "replication")]
fn import_replication_origin(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
    has_bootstrap_schema: bool,
    has_completed_receipt_column: bool,
) -> RestoreResult<()> {
    copy_replication_rows(
        source,target,
        "SELECT rowid,octet_length(replica_id)+octet_length(public_id)+320,
                typeof(replica_id)='text' AND octet_length(replica_id) BETWEEN 1 AND 256
                  AND typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(destination_epoch)='blob' AND length(destination_epoch)=16
                  AND typeof(acknowledged)='blob' AND length(acknowledged)=8
                  AND typeof(max_backlog_bytes)='blob' AND length(max_backlog_bytes)=8
                  AND typeof(max_backlog_age_ms)='blob' AND length(max_backlog_age_ms)=8
                  AND typeof(backlog_records)='integer' AND backlog_records>=0
                  AND typeof(backlog_bytes)='blob' AND length(backlog_bytes)=8
                  AND (oldest_backlog_at IS NULL OR (typeof(oldest_backlog_at)='blob' AND length(oldest_backlog_at)=8))
                  AND typeof(mode)='integer' AND mode BETWEEN 0 AND 2
                  AND (pending_batch IS NULL OR (typeof(pending_batch)='blob' AND length(pending_batch)=16)),
                replica_id,origin_id,public_id,incarnation,destination_epoch,acknowledged,
                max_backlog_bytes,max_backlog_age_ms,backlog_records,backlog_bytes,oldest_backlog_at,mode,pending_batch
         FROM replication_origin_replicas WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_origin_replicas VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",13,config,"replication origin replica")?;
    copy_replication_rows(
        source,target,
        "SELECT rowid,80,
                typeof(stream_key)='integer' AND typeof(offset)='blob' AND length(offset)=8
                  AND typeof(committed_at)='blob' AND length(committed_at)=8,
                stream_key,offset,committed_at
         FROM replication_origin_record_times WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_origin_record_times VALUES(?1,?2,?3)",3,config,"replication origin timestamp")?;
    copy_replication_rows(
        source,target,
        "SELECT rowid,octet_length(operation_id)+octet_length(replica_id)+octet_length(public_id)+512,
                typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256
                  AND typeof(kind)='integer' AND kind BETWEEN 0 AND 3
                  AND typeof(replica_id)='text' AND octet_length(replica_id) BETWEEN 1 AND 256
                  AND typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND (batch_id IS NULL OR (typeof(batch_id)='blob' AND length(batch_id)=16))
                  AND (expected_after IS NULL OR (typeof(expected_after)='blob' AND length(expected_after)=8))
                  AND (result_through IS NULL OR (typeof(result_through)='blob' AND length(result_through)=8))
                  AND typeof(destination_epoch)='blob' AND length(destination_epoch)=16
                  AND (max_backlog_bytes IS NULL OR (typeof(max_backlog_bytes)='blob' AND length(max_backlog_bytes)=8))
                  AND (max_backlog_age_ms IS NULL OR (typeof(max_backlog_age_ms)='blob' AND length(max_backlog_age_ms)=8))
                  AND (start_mode IS NULL OR (typeof(start_mode)='integer' AND start_mode IN(0,1)))
                  AND (limit_records IS NULL OR (typeof(limit_records)='integer' AND limit_records>=0))
                  AND (limit_bytes IS NULL OR (typeof(limit_bytes)='blob' AND length(limit_bytes)=8))
                  AND (status_ack IS NULL OR (typeof(status_ack)='blob' AND length(status_ack)=8))
                  AND (status_backlog_records IS NULL OR (typeof(status_backlog_records)='integer' AND status_backlog_records>=0))
                  AND (status_backlog_bytes IS NULL OR (typeof(status_backlog_bytes)='blob' AND length(status_backlog_bytes)=8))
                  AND (result_batch_records IS NULL OR (typeof(result_batch_records)='integer' AND result_batch_records>=0))
                  AND (result_batch_bytes IS NULL OR (typeof(result_batch_bytes)='blob' AND length(result_batch_bytes)=8))
                  AND (status_oldest IS NULL OR (typeof(status_oldest)='blob' AND length(status_oldest)=8))
                  AND (status_mode IS NULL OR (typeof(status_mode)='integer' AND status_mode BETWEEN 0 AND 2))
                  AND (status_pending IS NULL OR (typeof(status_pending)='blob' AND length(status_pending)=16)),
                operation_id,kind,replica_id,origin_id,public_id,incarnation,batch_id,expected_after,result_through,destination_epoch,
                max_backlog_bytes,max_backlog_age_ms,start_mode,limit_records,limit_bytes,status_ack,status_backlog_records,
                status_backlog_bytes,result_batch_records,result_batch_bytes,status_oldest,status_mode,status_pending
         FROM replication_origin_operations WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_origin_operations VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)",23,config,"replication origin operation")?;
    if has_bootstrap_schema {
        let completed_validity = if has_completed_receipt_column {
            "AND typeof(completed)='integer' AND completed IN(0,1)"
        } else {
            ""
        };
        let completed_value = if has_completed_receipt_column {
            "completed"
        } else {
            "0"
        };
        let select = format!(
            "SELECT rowid,octet_length(operation_id)+octet_length(destination_operation_id)+octet_length(replica_id)+octet_length(public_id)+octet_length(schema_id)+640,
                    typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256
                      AND typeof(kind)='integer' AND kind IN(0,1)
                      AND typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16
                      AND typeof(destination_operation_id)='text' AND octet_length(destination_operation_id) BETWEEN 1 AND 256
                      AND typeof(replica_id)='text' AND octet_length(replica_id) BETWEEN 1 AND 256
                      AND typeof(origin_id)='blob' AND length(origin_id)=16
                      AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                      AND typeof(incarnation)='blob' AND length(incarnation)=16
                      AND typeof(destination_epoch)='blob' AND length(destination_epoch)=16
                      AND typeof(snapshot_id)='blob' AND length(snapshot_id)=16
                      AND typeof(covered)='blob' AND length(covered)=8
                      AND typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256
                      AND typeof(schema_version)='integer'
                      AND typeof(content_bytes)='blob' AND length(content_bytes)=8
                      AND typeof(digest)='blob' AND length(digest)=32
                      AND typeof(captured_tail)='blob' AND length(captured_tail)=8
                      AND typeof(status_ack)='blob' AND length(status_ack)=8
                      AND typeof(status_backlog_records)='integer' AND status_backlog_records>=0
                      AND typeof(status_backlog_bytes)='blob' AND length(status_backlog_bytes)=8
                      AND (status_oldest IS NULL OR (typeof(status_oldest)='blob' AND length(status_oldest)=8))
                      AND typeof(status_mode)='integer' AND status_mode BETWEEN 0 AND 2
                      AND (protection_expires IS NULL OR (typeof(protection_expires)='blob' AND length(protection_expires)=8))
                      {completed_validity},
                    operation_id,kind,bootstrap_id,destination_operation_id,replica_id,origin_id,public_id,incarnation,
                    destination_epoch,snapshot_id,covered,schema_id,schema_version,content_bytes,digest,captured_tail,status_ack,
                    status_backlog_records,status_backlog_bytes,status_oldest,status_mode,protection_expires,{completed_value}
             FROM replication_origin_bootstrap_receipts WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2"
        );
        copy_replication_rows(
            source,target,
            &select,
            "INSERT INTO replication_origin_bootstrap_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23)",23,config,"replication origin bootstrap receipt")?;
    }
    let tx = target
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| map_sql_write("begin origin replication identity rewrite", error))?;
    let new_origin: Vec<u8> = tx
        .query_row(
            "SELECT origin_id FROM replication_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| map_sql_write("read restored origin identity", error))?;
    tx.execute("UPDATE replication_origin_record_times SET stream_key=(SELECT new_stream_key FROM restore_lifetime_keys WHERE old_stream_key=replication_origin_record_times.stream_key)",[]).map_err(|error|map_sql_write("rewrite origin timestamp lifetime",error))?;
    for table in [
        "replication_origin_replicas",
        "replication_origin_operations",
        "replication_origin_bootstrap_receipts",
    ] {
        if table == "replication_origin_bootstrap_receipts" && !has_bootstrap_schema {
            continue;
        }
        tx.execute(&format!("UPDATE {table} SET origin_id=?1,incarnation=(SELECT new_incarnation FROM restore_mappings WHERE old_public_id={table}.public_id AND old_incarnation={table}.incarnation)"),[new_origin.as_slice()]).map_err(|error|map_sql_write("rewrite origin replication identity",error))?;
    }
    tx.execute("UPDATE replication_origin_replicas SET acknowledged=zeroblob(8),backlog_records=0,backlog_bytes=zeroblob(8),oldest_backlog_at=NULL,mode=1,pending_batch=NULL",[]).map_err(|error|map_sql_write("detach restored origin replicas",error))?;
    tx.execute("UPDATE replication_origin_bootstrap_receipts SET completed=CASE WHEN completed=1 THEN 1 ELSE 0 END",[]).map_err(|error|map_sql_write("normalize restored origin bootstrap receipts",error))?;
    tx.commit()
        .map_err(|error| map_sql_write("commit origin replication identity rewrite", error))?;
    enforce_staging_limit(target, config.max_staging_bytes)?;
    Ok(())
}

#[cfg(feature = "replication")]
fn import_replication_bootstraps(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,
                octet_length(operation_id)+octet_length(replica_id)+octet_length(public_id)+octet_length(schema_id)+512,
                typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16
                  AND typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256
                  AND typeof(replica_id)='text' AND octet_length(replica_id) BETWEEN 1 AND 256
                  AND typeof(destination_epoch)='blob' AND length(destination_epoch)=16
                  AND typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(snapshot_id)='blob' AND length(snapshot_id)=16
                  AND typeof(covered)='blob' AND length(covered)=8
                  AND typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256
                  AND typeof(schema_version)='integer'
                  AND typeof(content_bytes)='blob' AND length(content_bytes)=8
                  AND typeof(digest)='blob' AND length(digest)=32
                  AND typeof(through_offset)='blob' AND length(through_offset)=8
                  AND typeof(state)='integer' AND state BETWEEN 0 AND 3
                  AND typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8
                  AND typeof(accepted_records)='integer' AND accepted_records>=0
                  AND typeof(accepted_record_bytes)='blob' AND length(accepted_record_bytes)=8
                  AND (publish_operation_id IS NULL OR (typeof(publish_operation_id)='text' AND octet_length(publish_operation_id) BETWEEN 1 AND 256))
                  AND (abort_operation_id IS NULL OR (typeof(abort_operation_id)='text' AND octet_length(abort_operation_id) BETWEEN 1 AND 256)),
                bootstrap_id,operation_id,replica_id,destination_epoch,origin_id,public_id,incarnation,
                snapshot_id,covered,schema_id,schema_version,content_bytes,digest,through_offset,state,
                accepted_bytes,accepted_records,accepted_record_bytes,publish_operation_id,abort_operation_id
         FROM replication_destination_bootstraps
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_bootstraps VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20)",
        20,
        config,
        "replication destination bootstrap",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,octet_length(bytes)+80,
                typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16
                  AND typeof(offset)='blob' AND length(offset)=8
                  AND typeof(bytes)='blob' AND octet_length(bytes)<=?3,
                bootstrap_id,offset,
                CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END
         FROM replication_destination_bootstrap_chunks
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_bootstrap_chunks VALUES(?1,?2,?3)",
        3,
        config,
        "replication bootstrap chunk",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384,
                typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16
                  AND typeof(offset)='blob' AND length(offset)=8
                  AND typeof(event_id)='text' AND octet_length(event_id) BETWEEN 1 AND 256
                  AND typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256
                  AND typeof(schema_version)='integer'
                  AND typeof(payload)='blob' AND octet_length(payload)<=?3,
                bootstrap_id,offset,event_id,schema_id,schema_version,
                CASE WHEN typeof(payload)='blob' AND octet_length(payload)<=?3 THEN payload END
         FROM replication_destination_bootstrap_records
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_bootstrap_records VALUES(?1,?2,?3,?4,?5,?6)",
        6,
        config,
        "replication bootstrap record",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,96,
                typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16
                  AND typeof(batch_id)='blob' AND length(batch_id)=16
                  AND typeof(after_offset)='blob' AND length(after_offset)=8
                  AND typeof(through_offset)='blob' AND length(through_offset)=8,
                bootstrap_id,batch_id,after_offset,through_offset
         FROM replication_destination_bootstrap_batch_receipts
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_bootstrap_batch_receipts VALUES(?1,?2,?3,?4)",
        4,
        config,
        "replication bootstrap batch receipt",
    )?;
    copy_replication_rows(
        source,
        target,
        "SELECT rowid,octet_length(public_id)+112,
                typeof(origin_id)='blob' AND length(origin_id)=16
                  AND typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256
                  AND typeof(incarnation)='blob' AND length(incarnation)=16
                  AND typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16,
                origin_id,public_id,incarnation,bootstrap_id
         FROM replication_destination_published_bootstraps
         WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO replication_destination_published_bootstraps VALUES(?1,?2,?3,?4)",
        4,
        config,
        "published replication bootstrap",
    )?;
    target
        .execute(
            "UPDATE replication_destination_bootstraps SET state=3
             WHERE state<>2",
            [],
        )
        .map_err(|error| map_sql_write("retire restored incomplete bootstraps", error))?;
    enforce_staging_limit(target, config.max_staging_bytes)?;
    Ok(())
}

// Partial uploads require a contiguous accepted prefix. Verified uploads also
// require their complete digest. Retired and aborted uploads may have been cleaned.
#[cfg(feature = "replication")]
fn validate_unpublished_replica_chunks(
    target: &Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let max = i64::try_from(config.page.max_bytes).map_err(|_| RestoreError::CapacityExceeded)?;
    let corrupt = |error: rusqlite::Error| {
        RestoreError::CorruptBackup(format!("validate unfinished replica chunks: {error}"))
    };
    let mut uploads = target.prepare(
        "SELECT CASE WHEN typeof(bootstrap_id)='blob' AND length(bootstrap_id)=16 THEN bootstrap_id END,
                CASE WHEN typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8 THEN accepted_bytes END,
                CASE WHEN typeof(content_bytes)='blob' AND length(content_bytes)=8 THEN content_bytes END,
                state, CASE WHEN typeof(digest)='blob' AND length(digest)=32 THEN digest END
         FROM replication_destination_bootstraps WHERE state IN(0,1) ORDER BY bootstrap_id",
    ).map_err(corrupt)?;
    let mut rows = uploads.query([]).map_err(corrupt)?;
    let mut chunks = target
        .prepare(
            "SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
                    CASE WHEN typeof(bytes)='blob' THEN octet_length(bytes) END,
                    CASE WHEN ?2 AND typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END
             FROM replication_destination_bootstrap_chunks WHERE bootstrap_id=?1 ORDER BY offset",
        )
        .map_err(corrupt)?;
    while let Some(row) = rows.next().map_err(corrupt)? {
        let id = fixed::<16>(row.get(0).map_err(corrupt)?, "unfinished replica identity")?;
        let accepted = u64::from_be_bytes(fixed::<8>(
            row.get(1).map_err(corrupt)?,
            "accepted replica bytes",
        )?);
        let content = u64::from_be_bytes(fixed::<8>(
            row.get(2).map_err(corrupt)?,
            "replica content bytes",
        )?);
        let verified = row.get::<_, i64>(3).map_err(corrupt)? == 1;
        let digest = fixed::<32>(row.get(4).map_err(corrupt)?, "replica snapshot digest")?;
        let mut hasher = Sha256::new();
        let mut chunk_rows = chunks
            .query(params![id.as_slice(), verified, max])
            .map_err(corrupt)?;
        let mut next = 0u64;
        while let Some(chunk) = chunk_rows.next().map_err(corrupt)? {
            let offset = u64::from_be_bytes(fixed::<8>(
                chunk.get(0).map_err(corrupt)?,
                "replica chunk offset",
            )?);
            let length: i64 = chunk.get(1).map_err(corrupt)?;
            if offset != next || length <= 0 {
                return Err(RestoreError::CorruptBackup(
                    "unfinished replica chunks are not contiguous".into(),
                ));
            }
            next = next.checked_add(length as u64).ok_or_else(|| {
                RestoreError::CorruptBackup("unfinished replica byte count overflow".into())
            })?;
            if verified {
                let bytes: Option<Vec<u8>> = chunk.get(2).map_err(corrupt)?;
                hasher.update(bytes.ok_or(RestoreError::CapacityExceeded)?);
            }
        }
        if verified && (next != content || <[u8; 32]>::from(hasher.finalize()) != digest) {
            return Err(RestoreError::CorruptBackup(
                "verified replica snapshot digest or length is invalid".into(),
            ));
        }
        if next != accepted || next > content {
            return Err(RestoreError::CorruptBackup(
                "unfinished replica byte count differs from stored chunks".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "replication")]
fn validate_restored_replication_bootstraps(
    target: &Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    type Published = (
        Vec<u8>,
        Vec<u8>,
        String,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        Vec<u8>,
        i64,
        Vec<u8>,
    );
    let max = i64::try_from(config.page.max_bytes).map_err(|_| RestoreError::CapacityExceeded)?;
    let invalid_pointer: bool = target
        .query_row(
            "SELECT EXISTS(
               SELECT 1
               FROM replication_destination_published_bootstraps p
               JOIN replication_destination_bootstraps b USING(bootstrap_id)
               WHERE b.state<>2
             )",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!(
                "validate published bootstrap pointer state: {error}"
            ))
        })?;
    if invalid_pointer {
        return Err(RestoreError::CorruptBackup(
            "published bootstrap pointer references a nonpublished bootstrap".into(),
        ));
    }
    let mut after: Option<Vec<u8>> = None;
    loop {
        let published: Option<Published> = target.query_row(
            "SELECT b.bootstrap_id,b.origin_id,b.public_id,b.incarnation,b.content_bytes,b.digest,
                    b.covered,b.accepted_records,b.accepted_record_bytes
             FROM replication_destination_published_bootstraps p
             JOIN replication_destination_bootstraps b USING(bootstrap_id)
             WHERE (?1 IS NULL OR b.bootstrap_id>?1)
             ORDER BY b.bootstrap_id LIMIT 1",
            params![after],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?)),
        ).optional().map_err(|error|RestoreError::CorruptBackup(format!("read published bootstrap validation row: {error}")))?;
        let Some((
            id,
            origin,
            public,
            incarnation,
            content,
            digest,
            covered,
            accepted_records,
            accepted_record_bytes,
        )) = published
        else {
            break;
        };
        after = Some(id.clone());
        let id = fixed::<16>(Some(id), "published bootstrap identity")?;
        let origin = fixed::<16>(Some(origin), "published bootstrap origin")?;
        let incarnation = fixed::<16>(Some(incarnation), "published bootstrap incarnation")?;
        let content = decode_offset(&fixed::<8>(
            Some(content),
            "published bootstrap content length",
        )?)
        .map_err(|error| {
            RestoreError::CorruptBackup(format!(
                "invalid published bootstrap content length: {error}"
            ))
        })?;
        let digest = fixed::<32>(Some(digest), "published bootstrap digest")?;
        let covered = decode_offset(&fixed::<8>(
            Some(covered),
            "published bootstrap covered cursor",
        )?)
        .map_err(|error| {
            RestoreError::CorruptBackup(format!(
                "invalid published bootstrap covered cursor: {error}"
            ))
        })?;
        let accepted_records = usize::try_from(accepted_records).map_err(|_| {
            RestoreError::CorruptBackup("invalid published bootstrap record count".into())
        })?;
        let accepted_record_bytes = decode_offset(&fixed::<8>(
            Some(accepted_record_bytes),
            "published bootstrap record bytes",
        )?)
        .map_err(|error| {
            RestoreError::CorruptBackup(format!(
                "invalid published bootstrap record bytes: {error}"
            ))
        })?;
        let mut hasher = Sha256::new();
        let mut next = 0u64;
        let mut chunks=target.prepare("SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?2 THEN bytes END FROM replication_destination_bootstrap_chunks WHERE bootstrap_id=?1 ORDER BY offset").map_err(|error|RestoreError::CorruptBackup(format!("prepare bootstrap chunk validation: {error}")))?;
        let mut chunk_rows = chunks.query(params![id.as_slice(), max]).map_err(|error| {
            RestoreError::CorruptBackup(format!("query bootstrap chunk validation: {error}"))
        })?;
        while let Some(row) = chunk_rows.next().map_err(|error| {
            RestoreError::CorruptBackup(format!("step bootstrap chunk validation: {error}"))
        })? {
            let offset = decode_offset(&fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read bootstrap chunk offset: {error}"))
                })?,
                "bootstrap chunk offset",
            )?)
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("invalid bootstrap chunk offset: {error}"))
            })?;
            let bytes: Option<Vec<u8>> = row.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read bootstrap chunk bytes: {error}"))
            })?;
            let bytes = bytes.ok_or(RestoreError::CapacityExceeded)?;
            if offset != next || bytes.is_empty() {
                return Err(RestoreError::CorruptBackup(
                    "published bootstrap snapshot is not contiguous".into(),
                ));
            }
            next = next
                .checked_add(bytes.len() as u64)
                .ok_or(RestoreError::CapacityExceeded)?;
            if next > content {
                return Err(RestoreError::CorruptBackup(
                    "published bootstrap snapshot exceeds its descriptor".into(),
                ));
            }
            hasher.update(&bytes);
        }
        drop(chunk_rows);
        drop(chunks);
        if next != content || <[u8; 32]>::from(hasher.finalize()) != digest {
            return Err(RestoreError::CorruptBackup(
                "published bootstrap snapshot digest or length is invalid".into(),
            ));
        }
        let through:Vec<u8>=target.query_row("SELECT through_offset FROM replication_destination_bootstraps WHERE bootstrap_id=?1",[id.as_slice()],|row|row.get(0)).map_err(|error|RestoreError::CorruptBackup(format!("read published bootstrap tail: {error}")))?;
        let through = decode_offset(&fixed::<8>(Some(through), "published bootstrap tail")?)
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("invalid published bootstrap tail: {error}"))
            })?;
        let mut expected = covered;
        let mut count = 0usize;
        let mut record_bytes = 0u64;
        let mut records=target.prepare("SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384 FROM replication_destination_bootstrap_records WHERE bootstrap_id=?1 ORDER BY offset").map_err(|error|RestoreError::CorruptBackup(format!("prepare bootstrap suffix validation: {error}")))?;
        let mut record_rows = records.query([id.as_slice()]).map_err(|error| {
            RestoreError::CorruptBackup(format!("query bootstrap suffix validation: {error}"))
        })?;
        while let Some(row) = record_rows.next().map_err(|error| {
            RestoreError::CorruptBackup(format!("step bootstrap suffix validation: {error}"))
        })? {
            let offset = decode_offset(&fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read bootstrap suffix offset: {error}"))
                })?,
                "bootstrap suffix offset",
            )?)
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("invalid bootstrap suffix offset: {error}"))
            })?;
            expected = expected
                .checked_add(1)
                .ok_or(RestoreError::CapacityExceeded)?;
            if offset != expected {
                return Err(RestoreError::CorruptBackup(
                    "published bootstrap suffix is not contiguous".into(),
                ));
            }
            count = count.checked_add(1).ok_or(RestoreError::CapacityExceeded)?;
            let charge: i64 = row.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read bootstrap suffix charge: {error}"))
            })?;
            record_bytes = record_bytes
                .checked_add(u64::try_from(charge).map_err(|_| {
                    RestoreError::CorruptBackup("invalid bootstrap suffix charge".into())
                })?)
                .ok_or(RestoreError::CapacityExceeded)?;
        }
        drop(record_rows);
        drop(records);
        if count > 0
            && (expected != through
                || count != accepted_records
                || record_bytes != accepted_record_bytes)
        {
            return Err(RestoreError::CorruptBackup(
                "published bootstrap suffix metadata is inconsistent".into(),
            ));
        }
        let original_records = usize::try_from(through.checked_sub(covered).ok_or_else(|| {
            RestoreError::CorruptBackup("published bootstrap has an inverted suffix".into())
        })?)
        .map_err(|_| RestoreError::CapacityExceeded)?;
        if accepted_records != original_records {
            return Err(RestoreError::CorruptBackup(
                "published bootstrap record count differs from its range".into(),
            ));
        }
        let (floor, tail): (Vec<u8>, Vec<u8>) = target
            .query_row(
                "SELECT floor,tail FROM replication_destination_streams
                 WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3",
                params![origin.as_slice(), public, incarnation.as_slice()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "read published bootstrap retained bounds: {error}"
                ))
            })?;
        let floor = decode_offset(&fixed::<8>(Some(floor), "published stream floor")?).map_err(
            |error| RestoreError::CorruptBackup(format!("invalid published stream floor: {error}")),
        )?;
        let tail =
            decode_offset(&fixed::<8>(Some(tail), "published stream tail")?).map_err(|error| {
                RestoreError::CorruptBackup(format!("invalid published stream tail: {error}"))
            })?;
        if tail < through {
            return Err(RestoreError::CorruptBackup(
                "published stream ends before its bootstrap receipt".into(),
            ));
        }
        let retained_after = covered.max(floor).min(through);
        let mut retained_expected = retained_after;
        let mut retained_count = 0usize;
        let mut retained_bytes = 0u64;
        let mut retained = target
            .prepare(
                "SELECT CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
                    octet_length(event_id)+octet_length(schema_id)+octet_length(payload)+384
             FROM replication_destination_records
             WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3
               AND offset>?4 AND offset<=?5 ORDER BY offset",
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "prepare published retained suffix validation: {error}"
                ))
            })?;
        let mut retained_rows = retained
            .query(params![
                origin.as_slice(),
                public,
                incarnation.as_slice(),
                retained_after.to_be_bytes().as_slice(),
                through.to_be_bytes().as_slice()
            ])
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "query published retained suffix validation: {error}"
                ))
            })?;
        while let Some(row) = retained_rows.next().map_err(|error| {
            RestoreError::CorruptBackup(format!(
                "step published retained suffix validation: {error}"
            ))
        })? {
            let offset = decode_offset(&fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!(
                        "read published retained suffix offset: {error}"
                    ))
                })?,
                "published retained suffix offset",
            )?)
            .map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "invalid published retained suffix offset: {error}"
                ))
            })?;
            retained_expected = retained_expected
                .checked_add(1)
                .ok_or(RestoreError::CapacityExceeded)?;
            if offset != retained_expected {
                return Err(RestoreError::CorruptBackup(
                    "published retained suffix is not contiguous".into(),
                ));
            }
            retained_count = retained_count
                .checked_add(1)
                .ok_or(RestoreError::CapacityExceeded)?;
            let charge: i64 = row.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!(
                    "read published retained suffix charge: {error}"
                ))
            })?;
            retained_bytes = retained_bytes
                .checked_add(u64::try_from(charge).map_err(|_| {
                    RestoreError::CorruptBackup("invalid published retained suffix charge".into())
                })?)
                .ok_or(RestoreError::CapacityExceeded)?;
        }
        drop(retained_rows);
        drop(retained);
        let expected_retained = usize::try_from(through.saturating_sub(retained_after))
            .map_err(|_| RestoreError::CapacityExceeded)?;
        if retained_expected != through
            || retained_count != expected_retained
            || (floor <= covered && retained_bytes != accepted_record_bytes)
            || retained_bytes > accepted_record_bytes
        {
            return Err(RestoreError::CorruptBackup(
                "published retained suffix metadata is inconsistent".into(),
            ));
        }
        let pointer:bool=target.query_row("SELECT EXISTS(SELECT 1 FROM replication_destination_published_bootstraps WHERE origin_id=?1 AND public_id=?2 AND incarnation=?3 AND bootstrap_id=?4)",params![origin.as_slice(),&public,incarnation.as_slice(),id.as_slice()],|row|row.get(0)).map_err(|error|RestoreError::CorruptBackup(format!("validate published bootstrap pointer: {error}")))?;
        let mismatch = if count == 0 {
            false
        } else {
            target.query_row("SELECT EXISTS(SELECT 1 FROM replication_destination_bootstrap_records b LEFT JOIN replication_destination_records r ON r.origin_id=?2 AND r.public_id=?3 AND r.incarnation=?4 AND r.offset=b.offset AND r.event_id=b.event_id AND r.schema_id=b.schema_id AND r.schema_version=b.schema_version AND r.payload=b.payload WHERE b.bootstrap_id=?1 AND b.offset>?5 AND b.offset<=?6 AND r.offset IS NULL UNION ALL SELECT 1 FROM replication_destination_records r LEFT JOIN replication_destination_bootstrap_records b ON b.bootstrap_id=?1 AND b.offset=r.offset AND b.event_id=r.event_id AND b.schema_id=r.schema_id AND b.schema_version=r.schema_version AND b.payload=r.payload WHERE r.origin_id=?2 AND r.public_id=?3 AND r.incarnation=?4 AND r.offset>?5 AND r.offset<=?6 AND b.offset IS NULL)",params![id.as_slice(),origin.as_slice(),&public,incarnation.as_slice(),retained_after.to_be_bytes().as_slice(),through.to_be_bytes().as_slice()],|row|row.get(0)).map_err(|error|RestoreError::CorruptBackup(format!("validate published bootstrap suffix publication: {error}")))?
        };
        if !pointer || mismatch {
            return Err(RestoreError::CorruptBackup(
                "published bootstrap pointer or history is inconsistent".into(),
            ));
        }
    }
    Ok(())
}

fn journal_data_present(source: &Connection) -> RestoreResult<bool> {
    let count: u32 = source
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('journal_metadata','journal_sources','journal_segments','journal_capture_receipts',
              'journal_markers','journal_operations')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("inspect source journal schema: {error}"))
        })?;
    match count {
        0 => Ok(false),
        6 => source
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM journal_sources UNION ALL
                               SELECT 1 FROM journal_segments UNION ALL
                               SELECT 1 FROM journal_capture_receipts UNION ALL
                               SELECT 1 FROM journal_markers UNION ALL
                               SELECT 1 FROM journal_operations)",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("inspect source journal rows: {error}"))
            }),
        _ => Err(RestoreError::CorruptBackup(
            "source journal schema is only partially present".into(),
        )),
    }
}

fn import_identity_mappings(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<u64> {
    let mut count = 0_u64;
    #[cfg(feature = "snapshots")]
    let include_snapshots = snapshot_schema_present(source)?;
    #[cfg(feature = "retention")]
    let include_retention = retention_schema_present(source)?;
    #[cfg(all(feature = "snapshots", feature = "retention"))]
    let identity_sql = if include_snapshots && include_retention {
        "SELECT octet_length(public_id),
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
         FROM (
           SELECT public_id,incarnation FROM event_streams
           UNION SELECT public_id,latest_incarnation FROM event_stream_names
           UNION SELECT expected_public_id,expected_incarnation FROM lifecycle_receipts
           UNION SELECT expected_public_id,replacement_incarnation FROM lifecycle_receipts WHERE replacement_incarnation IS NOT NULL
           UNION SELECT public_id,incarnation FROM snapshots
           UNION SELECT public_id,incarnation FROM retention_receipts
         ) ORDER BY public_id,incarnation"
    } else if include_snapshots {
        "SELECT octet_length(public_id),
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
         FROM (
           SELECT public_id,incarnation FROM event_streams
           UNION SELECT public_id,latest_incarnation FROM event_stream_names
           UNION SELECT expected_public_id,expected_incarnation FROM lifecycle_receipts
           UNION SELECT expected_public_id,replacement_incarnation FROM lifecycle_receipts WHERE replacement_incarnation IS NOT NULL
           UNION SELECT public_id,incarnation FROM snapshots
         ) ORDER BY public_id,incarnation"
    } else if include_retention {
        "SELECT octet_length(public_id),
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
         FROM (
           SELECT public_id,incarnation FROM event_streams
           UNION SELECT public_id,latest_incarnation FROM event_stream_names
           UNION SELECT expected_public_id,expected_incarnation FROM lifecycle_receipts
           UNION SELECT expected_public_id,replacement_incarnation FROM lifecycle_receipts WHERE replacement_incarnation IS NOT NULL
           UNION SELECT public_id,incarnation FROM retention_receipts
         ) ORDER BY public_id,incarnation"
    } else {
        IDENTITY_UNION_SQL
    };
    #[cfg(all(feature = "snapshots", not(feature = "retention")))]
    let identity_sql = if include_snapshots {
        "SELECT octet_length(public_id),
                CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
         FROM (
           SELECT public_id,incarnation FROM event_streams
           UNION SELECT public_id,latest_incarnation FROM event_stream_names
           UNION SELECT expected_public_id,expected_incarnation FROM lifecycle_receipts
           UNION SELECT expected_public_id,replacement_incarnation FROM lifecycle_receipts WHERE replacement_incarnation IS NOT NULL
           UNION SELECT public_id,incarnation FROM snapshots
         ) ORDER BY public_id,incarnation"
    } else {
        IDENTITY_UNION_SQL
    };
    #[cfg(all(not(feature = "snapshots"), not(feature = "retention")))]
    let identity_sql = IDENTITY_UNION_SQL;
    let mut statement = source
        .prepare(identity_sql)
        .map_err(|error| RestoreError::CorruptBackup(format!("prepare identity union: {error}")))?;
    let mut rows = statement
        .query([])
        .map_err(|error| RestoreError::CorruptBackup(format!("query identity union: {error}")))?;
    let mut pending = None;
    loop {
        let mut batch = Vec::with_capacity(config.page.max_records.min(256));
        let mut bytes = 0usize;
        while batch.len() < config.page.max_records {
            let item = if let Some(item) = pending.take() {
                item
            } else {
                let Some(row) = rows.next().map_err(|error| {
                    RestoreError::CorruptBackup(format!("step identity union: {error}"))
                })?
                else {
                    break;
                };
                let length: i64 = row.get(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read identity length: {error}"))
                })?;
                let id: Option<String> = row.get(1).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read mapped stream name: {error}"))
                })?;
                let incarnation: Option<Vec<u8>> = row.get(2).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read mapped incarnation: {error}"))
                })?;
                let length = usize::try_from(length).map_err(|_| {
                    RestoreError::CorruptBackup("invalid stream-name length".into())
                })?;
                let id = StreamId::new(id.ok_or_else(|| {
                    RestoreError::CorruptBackup("invalid stream name in identity union".into())
                })?)
                .map_err(|_| RestoreError::CorruptBackup("invalid stream name".into()))?;
                let incarnation = fixed::<16>(incarnation, "incarnation")?;
                (id, incarnation, length)
            };
            let (id, incarnation, length) = item;
            let charge = length
                .checked_mul(2)
                .and_then(|value| value.checked_add(128))
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > config.page.max_bytes)
            {
                pending = Some((id, incarnation, length));
                break;
            }
            bytes += charge;
            batch.push((id, incarnation));
        }
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write("begin mapping page", error))?;
        for (id, old) in &batch {
            let new = super::new_incarnation();
            transaction
                .execute(
                    "INSERT INTO restore_mappings(old_public_id,old_incarnation,new_incarnation) VALUES(?1,?2,?3)",
                    params![id.as_str(), old.as_slice(), new.0.as_slice()],
                )
                .map_err(|error| map_sql_write("insert incarnation mapping", error))?;
        }
        transaction
            .commit()
            .map_err(|error| map_sql_write("commit mapping page", error))?;
        count = count
            .checked_add(batch.len() as u64)
            .ok_or(RestoreError::CapacityExceeded)?;
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(count)
}

const IDENTITY_UNION_SQL: &str =
    "SELECT octet_length(public_id),
            CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
            CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END
     FROM (
       SELECT public_id,incarnation FROM event_streams
       UNION SELECT public_id,latest_incarnation FROM event_stream_names
       UNION SELECT expected_public_id,expected_incarnation FROM lifecycle_receipts
       UNION SELECT expected_public_id,replacement_incarnation FROM lifecycle_receipts WHERE replacement_incarnation IS NOT NULL
     ) ORDER BY public_id,incarnation";

fn mapped_incarnation(target: &Connection, id: &str, old: &[u8; 16]) -> RestoreResult<[u8; 16]> {
    let value: Vec<u8> = target
        .query_row(
            "SELECT new_incarnation FROM restore_mappings WHERE old_public_id=?1 AND old_incarnation=?2",
            params![id, old.as_slice()],
            |row| row.get(0),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("missing identity mapping: {error}")))?;
    fixed(Some(value), "mapped incarnation")
}

fn import_lifetimes(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let mut after = 0_i64;
    loop {
        let mut statement = source.prepare(
            "SELECT stream_key,
                    CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                    CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,
                    CASE WHEN typeof(floor)='blob' AND length(floor)=8 THEN floor END,
                    CASE WHEN typeof(tail)='blob' AND length(tail)=8 THEN tail END,
                    CASE WHEN typeof(retired)='integer' AND retired IN (0,1) THEN retired END
             FROM event_streams WHERE stream_key>?1 ORDER BY stream_key LIMIT ?2",
        ).map_err(|error| RestoreError::CorruptBackup(format!("prepare lifetimes: {error}")))?;
        let mut rows = statement
            .query(params![
                after,
                i64::try_from(config.page.max_records)
                    .map_err(|_| RestoreError::CapacityExceeded)?
            ])
            .map_err(|error| RestoreError::CorruptBackup(format!("query lifetimes: {error}")))?;
        let mut batch = Vec::with_capacity(config.page.max_records.min(256));
        let mut bytes = 0usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| RestoreError::CorruptBackup(format!("step lifetimes: {error}")))?
        {
            let key: i64 = row.get(0).map_err(|error| {
                RestoreError::CorruptBackup(format!("read lifetime key: {error}"))
            })?;
            let id: Option<String> = row.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read lifetime name: {error}"))
            })?;
            let old: Option<Vec<u8>> = row.get(2).map_err(|error| {
                RestoreError::CorruptBackup(format!("read lifetime incarnation: {error}"))
            })?;
            let floor: Option<Vec<u8>> = row.get(3).map_err(|error| {
                RestoreError::CorruptBackup(format!("read lifetime floor: {error}"))
            })?;
            let tail: Option<Vec<u8>> = row.get(4).map_err(|error| {
                RestoreError::CorruptBackup(format!("read lifetime tail: {error}"))
            })?;
            let retired: Option<i64> = row.get(5).map_err(|error| {
                RestoreError::CorruptBackup(format!("read retired flag: {error}"))
            })?;
            let id = StreamId::new(
                id.ok_or_else(|| RestoreError::CorruptBackup("invalid lifetime name".into()))?,
            )
            .map_err(|_| RestoreError::CorruptBackup("invalid lifetime name".into()))?;
            let old = fixed::<16>(old, "lifetime incarnation")?;
            let floor = fixed::<8>(floor, "lifetime floor")?;
            let tail = fixed::<8>(tail, "lifetime tail")?;
            if floor > tail || retired.is_none() {
                return Err(RestoreError::CorruptBackup(
                    "invalid lifetime metadata".into(),
                ));
            }
            let charge = id
                .as_str()
                .len()
                .checked_add(256)
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > config.page.max_bytes)
            {
                break;
            }
            bytes += charge;
            batch.push((key, id, old, floor, tail, retired.unwrap()));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write("begin lifetime page", error))?;
        for (old_key, id, old, floor, tail, retired) in &batch {
            let new = mapped_incarnation(&transaction, id.as_str(), old)?;
            transaction.execute(
                "INSERT INTO event_streams(public_id,incarnation,floor,tail,retired) VALUES(?1,?2,?3,?4,?5)",
                params![id.as_str(),new.as_slice(),floor.as_slice(),tail.as_slice(),retired],
            ).map_err(|error| map_sql_write("insert restored lifetime", error))?;
            let new_key = transaction.last_insert_rowid();
            transaction.execute("INSERT INTO restore_lifetime_keys(old_stream_key,new_stream_key) VALUES(?1,?2)", params![old_key,new_key])
                .map_err(|error| map_sql_write("map lifetime key", error))?;
        }
        transaction
            .commit()
            .map_err(|error| map_sql_write("commit lifetime page", error))?;
        after = batch.last().unwrap().0;
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(())
}

fn fixed<const N: usize>(value: Option<Vec<u8>>, label: &str) -> RestoreResult<[u8; N]> {
    value
        .ok_or_else(|| RestoreError::CorruptBackup(format!("invalid {label}")))?
        .try_into()
        .map_err(|_| RestoreError::CorruptBackup(format!("invalid {label}")))
}

fn import_records(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
    #[cfg(feature = "test-support")] observer: Option<&dyn SqliteRestoreObserver>,
) -> RestoreResult<()> {
    struct Lifetime {
        old_key: i64,
        new_key: i64,
        floor: u64,
        tail: u64,
        next: u64,
    }

    type PendingRecord = (i64, i64, [u8; 8], [u8; 8], String, String, i64, Vec<u8>);

    fn finish_lifetime(lifetime: &Lifetime) -> RestoreResult<()> {
        if lifetime.next != lifetime.tail {
            return Err(RestoreError::CorruptBackup(
                "record history ends before declared tail".into(),
            ));
        }
        Ok(())
    }

    fn flush(
        target: &mut Connection,
        batch: &mut Vec<PendingRecord>,
        batch_bytes: &mut usize,
        config: &RestoreConfig,
        #[cfg(feature = "test-support")] observer: Option<&dyn SqliteRestoreObserver>,
    ) -> RestoreResult<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write("begin record page", error))?;
        for (source, new_key, generation, offset, event, schema, version, payload) in batch.iter() {
            #[cfg(not(feature = "retention"))]
            let _ = generation;
            if *source == 0 {
                transaction.execute(
                    "INSERT INTO event_records(stream_key,offset,event_id,schema_id,schema_version,payload) VALUES(?1,?2,?3,?4,?5,?6)",
                    params![new_key,offset.as_slice(),event,schema,version,payload],
                ).map_err(|error| map_sql_write("insert restored record", error))?;
            } else {
                #[cfg(feature = "retention")]
                transaction.execute(
                    "INSERT INTO retention_generated_records(stream_key,generation,offset,event_id,schema_id,schema_version,payload) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![new_key,generation.as_slice(),offset.as_slice(),event,schema,version,payload],
                ).map_err(|error| map_sql_write("insert restored generated record", error))?;
                #[cfg(not(feature = "retention"))]
                return Err(RestoreError::UnsupportedBackup(2));
            }
        }
        transaction
            .commit()
            .map_err(|error| map_sql_write("commit record page", error))?;
        #[cfg(feature = "test-support")]
        observe(observer, SqliteRestoreStage::RecordPageCommitted);
        enforce_staging_limit(target, config.max_staging_bytes)?;
        batch.clear();
        *batch_bytes = 0;
        Ok(())
    }

    let payload_limit =
        i64::try_from(config.page.max_bytes).map_err(|_| RestoreError::CapacityExceeded)?;
    let include_retention = retention_schema_present(source)?;
    let record_sql = if include_retention {
        "SELECT s.stream_key,
                CASE WHEN typeof(s.floor)='blob' AND length(s.floor)=8 THEN s.floor END,
                CASE WHEN typeof(s.tail)='blob' AND length(s.tail)=8 THEN s.tail END,
                r.identity,r.source,r.generation,
                octet_length(r.event_id),octet_length(r.schema_id),octet_length(r.payload),
                CASE WHEN typeof(r.offset)='blob' AND length(r.offset)=8 THEN r.offset END,
                CASE WHEN typeof(r.event_id)='text' AND octet_length(r.event_id) BETWEEN 1 AND 256 THEN r.event_id END,
                CASE WHEN typeof(r.schema_id)='text' AND octet_length(r.schema_id) BETWEEN 1 AND 256 THEN r.schema_id END,
                CASE WHEN typeof(r.schema_version)='integer' THEN r.schema_version END,
                CASE WHEN typeof(r.payload)='blob' AND octet_length(r.payload)<=?1 THEN r.payload END
         FROM event_streams AS s LEFT JOIN (
           SELECT rowid identity,0 source,zeroblob(8) generation,stream_key,offset,event_id,schema_id,schema_version,payload FROM event_records
           UNION ALL
           SELECT rowid identity,1 source,generation,stream_key,offset,event_id,schema_id,schema_version,payload FROM retention_generated_records
         ) AS r ON r.stream_key=s.stream_key ORDER BY s.stream_key,r.offset"
    } else {
        "SELECT s.stream_key,
                CASE WHEN typeof(s.floor)='blob' AND length(s.floor)=8 THEN s.floor END,
                CASE WHEN typeof(s.tail)='blob' AND length(s.tail)=8 THEN s.tail END,
                r.rowid,0,zeroblob(8),
                octet_length(r.event_id),octet_length(r.schema_id),octet_length(r.payload),
                CASE WHEN typeof(r.offset)='blob' AND length(r.offset)=8 THEN r.offset END,
                CASE WHEN typeof(r.event_id)='text' AND octet_length(r.event_id) BETWEEN 1 AND 256 THEN r.event_id END,
                CASE WHEN typeof(r.schema_id)='text' AND octet_length(r.schema_id) BETWEEN 1 AND 256 THEN r.schema_id END,
                CASE WHEN typeof(r.schema_version)='integer' THEN r.schema_version END,
                CASE WHEN typeof(r.payload)='blob' AND octet_length(r.payload)<=?1 THEN r.payload END
         FROM event_streams AS s LEFT JOIN event_records AS r ON r.stream_key=s.stream_key
         ORDER BY s.stream_key,r.offset"
    };
    let mut statement = source
        .prepare(record_sql)
        .map_err(|error| RestoreError::CorruptBackup(format!("prepare record import: {error}")))?;
    let mut rows = statement
        .query(params![payload_limit])
        .map_err(|error| RestoreError::CorruptBackup(format!("query record import: {error}")))?;
    let mut current: Option<Lifetime> = None;
    let mut batch = Vec::with_capacity(config.page.max_records.min(256));
    let mut batch_bytes = 0usize;

    while let Some(row) = rows
        .next()
        .map_err(|error| RestoreError::CorruptBackup(format!("step record import: {error}")))?
    {
        let old_key: i64 = row.get(0).map_err(|error| {
            RestoreError::CorruptBackup(format!("read record lifetime key: {error}"))
        })?;
        if current
            .as_ref()
            .is_none_or(|state| state.old_key != old_key)
        {
            if let Some(state) = current.as_ref() {
                finish_lifetime(state)?;
            }
            let floor = u64::from_be_bytes(fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(1).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read record floor: {error}"))
                })?,
                "record floor",
            )?);
            let tail = u64::from_be_bytes(fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(2).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read record tail: {error}"))
                })?,
                "record tail",
            )?);
            if floor > tail {
                return Err(RestoreError::CorruptBackup(
                    "record floor exceeds tail".into(),
                ));
            }
            let new_key = target
                .query_row(
                    "SELECT new_stream_key FROM restore_lifetime_keys WHERE old_stream_key=?1",
                    params![old_key],
                    |target_row| target_row.get(0),
                )
                .map_err(|error| {
                    RestoreError::CorruptBackup(format!("missing lifetime key mapping: {error}"))
                })?;
            current = Some(Lifetime {
                old_key,
                new_key,
                floor,
                tail,
                next: floor,
            });
        }
        let state = current.as_mut().expect("record lifetime initialized");
        let record_rowid: Option<i64> = row.get(3).map_err(|error| {
            RestoreError::CorruptBackup(format!("read record row identifier: {error}"))
        })?;
        if record_rowid.is_none() {
            if state.floor != state.tail {
                return Err(RestoreError::CorruptBackup(
                    "record history ends before declared tail".into(),
                ));
            }
            continue;
        }

        let source_kind: i64 = row
            .get(4)
            .map_err(|error| RestoreError::CorruptBackup(format!("read record source: {error}")))?;
        let generation = fixed::<8>(
            row.get::<_, Option<Vec<u8>>>(5).map_err(|error| {
                RestoreError::CorruptBackup(format!("read record generation: {error}"))
            })?,
            "record generation",
        )?;
        if !matches!(source_kind, 0 | 1)
            || (source_kind == 0 && generation != [0; 8])
            || (source_kind == 1 && generation == [0; 8])
        {
            return Err(RestoreError::CorruptBackup(
                "invalid record generation source".into(),
            ));
        }
        let event_len =
            usize::try_from(row.get::<_, i64>(6).map_err(|error| {
                RestoreError::CorruptBackup(format!("read event length: {error}"))
            })?)
            .map_err(|_| RestoreError::CorruptBackup("invalid event length".into()))?;
        let schema_len = usize::try_from(row.get::<_, i64>(7).map_err(|error| {
            RestoreError::CorruptBackup(format!("read schema length: {error}"))
        })?)
        .map_err(|_| RestoreError::CorruptBackup("invalid schema length".into()))?;
        let payload_len = usize::try_from(row.get::<_, i64>(8).map_err(|error| {
            RestoreError::CorruptBackup(format!("read payload length: {error}"))
        })?)
        .map_err(|_| RestoreError::CorruptBackup("invalid payload length".into()))?;
        let charge = event_len
            .checked_add(schema_len)
            .and_then(|value| value.checked_add(payload_len))
            .and_then(|value| value.checked_add(128))
            .ok_or(RestoreError::CapacityExceeded)?;
        if charge > config.page.max_bytes {
            return Err(RestoreError::CapacityExceeded);
        }
        if batch.len() == config.page.max_records
            || batch_bytes
                .checked_add(charge)
                .is_none_or(|total| total > config.page.max_bytes)
        {
            flush(
                target,
                &mut batch,
                &mut batch_bytes,
                config,
                #[cfg(feature = "test-support")]
                observer,
            )?;
        }

        let offset = fixed::<8>(
            row.get::<_, Option<Vec<u8>>>(9).map_err(|error| {
                RestoreError::CorruptBackup(format!("read record offset: {error}"))
            })?,
            "record offset",
        )?;
        let offset_value = u64::from_be_bytes(offset);
        let retained_prefix = offset_value <= state.floor;
        if !retained_prefix {
            let expected = state
                .next
                .checked_add(1)
                .ok_or_else(|| RestoreError::CorruptBackup("record offset overflow".into()))?;
            if offset_value != expected || offset_value > state.tail {
                return Err(RestoreError::CorruptBackup(
                    "invalid or noncontiguous visible record history".into(),
                ));
            }
        }
        let event = row
            .get::<_, Option<String>>(10)
            .map_err(|error| RestoreError::CorruptBackup(format!("read event ID: {error}")))?;
        let schema = row
            .get::<_, Option<String>>(11)
            .map_err(|error| RestoreError::CorruptBackup(format!("read schema ID: {error}")))?;
        let version = row.get::<_, Option<i64>>(12).map_err(|error| {
            RestoreError::CorruptBackup(format!("read schema version: {error}"))
        })?;
        let payload = row.get::<_, Option<Vec<u8>>>(13).map_err(|error| {
            RestoreError::CorruptBackup(format!("read record payload: {error}"))
        })?;
        let (event, schema, version, payload) = match (event, schema, version, payload) {
            (Some(event), Some(schema), Some(version), Some(payload)) => {
                let version = u32::try_from(version).map_err(|_| {
                    RestoreError::CorruptBackup("invalid restored schema version".into())
                })?;
                (event, schema, i64::from(version), payload)
            }
            _ => {
                return Err(RestoreError::CorruptBackup(
                    "invalid restored record value".into(),
                ))
            }
        };
        if retained_prefix {
            let valid_hidden_prefix: bool = include_retention
                && source
                    .query_row(
                        "SELECT
                           EXISTS(
                             SELECT 1 FROM retention_retry_identities
                             WHERE stream_key=?1 AND generation=?2 AND event_id=?3 AND offset=?4
                           )
                           OR EXISTS(
                             SELECT 1 FROM retention_streams AS policy
                             JOIN retention_cleanup AS cleanup USING(stream_key)
                             WHERE policy.stream_key=?1 AND policy.oldest_generation>?2
                           )",
                        params![old_key, generation.as_slice(), &event, offset.as_slice()],
                        |query_row| query_row.get(0),
                    )
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!(
                            "validate retained retry prefix: {error}"
                        ))
                    })?;
            if !valid_hidden_prefix {
                return Err(RestoreError::CorruptBackup(
                    "record below the replay floor is neither retryable nor pending bounded cleanup"
                        .into(),
                ));
            }
        }
        batch_bytes = batch_bytes
            .checked_add(charge)
            .ok_or(RestoreError::CapacityExceeded)?;
        batch.push((
            source_kind,
            state.new_key,
            generation,
            offset,
            event,
            schema,
            version,
            payload,
        ));
        if !retained_prefix {
            state.next = offset_value;
        }
    }
    if let Some(state) = current.as_ref() {
        finish_lifetime(state)?;
    }
    drop(rows);
    drop(statement);
    flush(
        target,
        &mut batch,
        &mut batch_bytes,
        config,
        #[cfg(feature = "test-support")]
        observer,
    )
}

fn import_names(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let mut after = String::new();
    loop {
        let mut statement = source.prepare(
            "SELECT CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                    CASE WHEN typeof(latest_incarnation)='blob' AND length(latest_incarnation)=16 THEN latest_incarnation END,
                    active_stream_key
             FROM event_stream_names WHERE public_id>?1 ORDER BY public_id LIMIT ?2",
        ).map_err(|error| RestoreError::CorruptBackup(format!("prepare names: {error}")))?;
        let mut rows = statement
            .query(params![
                after,
                i64::try_from(config.page.max_records)
                    .map_err(|_| RestoreError::CapacityExceeded)?
            ])
            .map_err(|error| RestoreError::CorruptBackup(format!("query names: {error}")))?;
        let mut batch = Vec::with_capacity(config.page.max_records.min(256));
        let mut bytes = 0usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| RestoreError::CorruptBackup(format!("step names: {error}")))?
        {
            let id: Option<String> = row
                .get(0)
                .map_err(|error| RestoreError::CorruptBackup(format!("read name: {error}")))?;
            let incarnation = fixed::<16>(
                row.get::<_, Option<Vec<u8>>>(1).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read latest incarnation: {error}"))
                })?,
                "latest incarnation",
            )?;
            let active: Option<i64> = row.get(2).map_err(|error| {
                RestoreError::CorruptBackup(format!("read active lifetime: {error}"))
            })?;
            let id = StreamId::new(
                id.ok_or_else(|| RestoreError::CorruptBackup("invalid stream name".into()))?,
            )
            .map_err(|_| RestoreError::CorruptBackup("invalid stream name".into()))?;
            let charge = id
                .as_str()
                .len()
                .checked_add(256)
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|v| v > config.page.max_bytes)
            {
                break;
            }
            bytes += charge;
            batch.push((id, incarnation, active));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write("begin name page", e))?;
        for (id, old, active) in &batch {
            let new = mapped_incarnation(&transaction, id.as_str(), old)?;
            let new_active = active
                .map(|key| {
                    transaction.query_row(
                        "SELECT new_stream_key FROM restore_lifetime_keys WHERE old_stream_key=?1",
                        params![key],
                        |r| r.get::<_, i64>(0),
                    )
                })
                .transpose()
                .map_err(|e| {
                    RestoreError::CorruptBackup(format!("missing active lifetime mapping: {e}"))
                })?;
            transaction.execute("INSERT INTO event_stream_names(public_id,latest_incarnation,active_stream_key) VALUES(?1,?2,?3)",params![id.as_str(),new.as_slice(),new_active]).map_err(|e|map_sql_write("insert restored name",e))?;
        }
        transaction
            .commit()
            .map_err(|e| map_sql_write("commit name page", e))?;
        after = batch.last().unwrap().0.as_str().to_owned();
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(())
}

fn import_receipts(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let mut after = String::new();
    loop {
        let mut statement=source.prepare("SELECT octet_length(operation_id),octet_length(expected_public_id),
                    CASE WHEN typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END,
                    CASE WHEN typeof(action)='integer' AND action IN (0,1) THEN action END,
                    CASE WHEN typeof(expected_public_id)='text' AND octet_length(expected_public_id) BETWEEN 1 AND 256 THEN expected_public_id END,
                    CASE WHEN typeof(expected_incarnation)='blob' AND length(expected_incarnation)=16 THEN expected_incarnation END,
                    CASE WHEN replacement_incarnation IS NULL OR (typeof(replacement_incarnation)='blob' AND length(replacement_incarnation)=16) THEN 1 ELSE 0 END,
                    CASE WHEN typeof(replacement_incarnation)='blob' AND length(replacement_incarnation)=16 THEN replacement_incarnation END,
                    CASE WHEN typeof(charge)='integer' AND charge>=0 THEN charge END
                 FROM lifecycle_receipts WHERE operation_id>?1 ORDER BY operation_id LIMIT ?2")
            .map_err(|e|RestoreError::CorruptBackup(format!("prepare receipts: {e}")))?;
        let mut rows = statement
            .query(params![
                after,
                i64::try_from(config.page.max_records)
                    .map_err(|_| RestoreError::CapacityExceeded)?
            ])
            .map_err(|e| RestoreError::CorruptBackup(format!("query receipts: {e}")))?;
        let mut batch = Vec::with_capacity(config.page.max_records.min(256));
        let mut bytes = 0usize;
        while let Some(row) = rows
            .next()
            .map_err(|e| RestoreError::CorruptBackup(format!("step receipts: {e}")))?
        {
            let op_len: i64 = row.get(0).map_err(|e| {
                RestoreError::CorruptBackup(format!("read receipt operation length: {e}"))
            })?;
            let id_len: i64 = row.get(1).map_err(|e| {
                RestoreError::CorruptBackup(format!("read receipt name length: {e}"))
            })?;
            let op: Option<String> = row.get(2).map_err(|e| {
                RestoreError::CorruptBackup(format!("read restore receipt op: {e}"))
            })?;
            let action: Option<i64> = row.get(3).map_err(|e| {
                RestoreError::CorruptBackup(format!("read restore receipt action: {e}"))
            })?;
            let id: Option<String> = row.get(4).map_err(|e| {
                RestoreError::CorruptBackup(format!("read restore receipt name: {e}"))
            })?;
            let expected = fixed::<16>(
                row.get::<_, Option<Vec<u8>>>(5).map_err(|e| {
                    RestoreError::CorruptBackup(format!("read expected incarnation: {e}"))
                })?,
                "expected incarnation",
            )?;
            let replacement_valid: i64 = row.get(6).map_err(|e| {
                RestoreError::CorruptBackup(format!("read replacement validity: {e}"))
            })?;
            let replacement: Option<Vec<u8>> = row.get(7).map_err(|e| {
                RestoreError::CorruptBackup(format!("read replacement incarnation: {e}"))
            })?;
            let charge: Option<i64> = row
                .get(8)
                .map_err(|e| RestoreError::CorruptBackup(format!("read receipt charge: {e}")))?;
            let logical = usize::try_from(op_len)
                .ok()
                .and_then(|op| {
                    usize::try_from(id_len)
                        .ok()
                        .and_then(|id| op.checked_add(id))
                })
                .and_then(|v| v.checked_add(256))
                .ok_or_else(|| {
                    RestoreError::CorruptBackup("invalid lifecycle receipt lengths".into())
                })?;
            if batch.is_empty() && logical > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(logical)
                .is_none_or(|v| v > config.page.max_bytes)
            {
                break;
            }
            let op = op.ok_or_else(|| {
                RestoreError::CorruptBackup("invalid lifecycle operation ID".into())
            })?;
            let id = id.ok_or_else(|| {
                RestoreError::CorruptBackup("invalid lifecycle stream name".into())
            })?;
            let action = action
                .ok_or_else(|| RestoreError::CorruptBackup("invalid lifecycle action".into()))?;
            let charge = charge.ok_or_else(|| {
                RestoreError::CorruptBackup("invalid lifecycle receipt charge".into())
            })?;
            if replacement_valid != 1
                || (action == 0 && replacement.is_some())
                || (action == 1 && replacement.is_none())
                || RestoreOperationId::new(&op).is_err()
                || StreamId::new(&id).is_err()
                || usize::try_from(charge).ok() != Some(logical)
            {
                return Err(RestoreError::CorruptBackup(
                    "invalid lifecycle receipt".into(),
                ));
            }
            bytes += logical;
            batch.push((op, action, id, expected, replacement, charge));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write("begin receipt page", e))?;
        for (op, action, id, expected, replacement, charge) in &batch {
            let new_expected = mapped_incarnation(&transaction, id, expected)?;
            let new_replacement = replacement
                .as_ref()
                .map(|old| {
                    fixed::<16>(Some(old.clone()), "replacement incarnation")
                        .and_then(|old| mapped_incarnation(&transaction, id, &old))
                })
                .transpose()?;
            transaction.execute("INSERT INTO lifecycle_receipts(operation_id,action,expected_public_id,expected_incarnation,replacement_incarnation,charge) VALUES(?1,?2,?3,?4,?5,?6)",params![op,action,id,new_expected.as_slice(),new_replacement.as_ref().map(|v|v.as_slice()),charge]).map_err(|e|map_sql_write("insert restored receipt",e))?;
        }
        transaction
            .commit()
            .map_err(|e| map_sql_write("commit receipt page", e))?;
        after = batch.last().unwrap().0.clone();
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(())
}

fn snapshot_schema_present(source: &Connection) -> RestoreResult<bool> {
    let count: u32 = source
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN ('snapshot_metadata','snapshots','snapshot_chunks')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("inspect snapshot schema: {error}")))?;
    match count {
        0 => Ok(false),
        3 => Ok(true),
        _ => Err(RestoreError::CorruptBackup(
            "snapshot schema is only partially present".into(),
        )),
    }
}

fn retention_schema_present(source: &Connection) -> RestoreResult<bool> {
    let count: u32 = source
        .query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name IN
             ('retention_metadata','retention_streams','retention_retry_identities',
              'retention_generated_records','retention_receipts','retention_cleanup')",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("inspect retention schema: {error}"))
        })?;
    match count {
        0 => Ok(false),
        6 => Ok(true),
        _ => Err(RestoreError::CorruptBackup(
            "retention schema is only partially present".into(),
        )),
    }
}

#[cfg(not(feature = "retention"))]
fn import_retention(
    source: &Connection,
    _target: &mut Connection,
    _config: &RestoreConfig,
) -> RestoreResult<()> {
    if retention_schema_present(source)? {
        return Err(RestoreError::UnsupportedBackup(2));
    }
    Ok(())
}

#[cfg(feature = "retention")]
fn import_retention(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    if !retention_schema_present(source)? {
        return Ok(());
    }
    super::sqlite_retention::validate_retention_counters(source).map_err(|error| {
        RestoreError::CorruptBackup(format!("invalid retention accounting: {error}"))
    })?;
    let bounded_limit = |maximum_charge: usize| -> RestoreResult<i64> {
        let by_bytes = config.page.max_bytes / maximum_charge;
        i64::try_from(config.page.max_records.min(by_bytes))
            .ok()
            .filter(|limit| *limit > 0)
            .ok_or(RestoreError::CapacityExceeded)
    };
    let policy_limit = bounded_limit(80)?;
    let mut after = i64::MIN;
    loop {
        let batch = {
            let mut stmt=source.prepare("SELECT stream_key,CASE WHEN typeof(oldest_generation)='blob' AND length(oldest_generation)=8 THEN oldest_generation END,CASE WHEN typeof(current_generation)='blob' AND length(current_generation)=8 THEN current_generation END FROM retention_streams WHERE stream_key>?1 ORDER BY stream_key LIMIT ?2").map_err(|e|RestoreError::CorruptBackup(format!("prepare retry policies: {e}")))?;
            let rows = stmt
                .query_map(params![after, policy_limit], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, Option<Vec<u8>>>(1)?,
                        r.get::<_, Option<Vec<u8>>>(2)?,
                    ))
                })
                .map_err(|e| RestoreError::CorruptBackup(format!("read retry policies: {e}")))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| RestoreError::CorruptBackup(format!("decode retry policies: {e}")))?;
            rows
        };
        if batch.is_empty() {
            break;
        }
        let tx = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write("begin retry policy page", e))?;
        for (old_key, oldest, current) in &batch {
            let oldest = fixed::<8>(oldest.clone(), "oldest retry generation")?;
            let current = fixed::<8>(current.clone(), "current retry generation")?;
            if oldest > current {
                return Err(RestoreError::CorruptBackup(
                    "oldest retry generation exceeds current".into(),
                ));
            }
            let new_key: i64 = tx
                .query_row(
                    "SELECT new_stream_key FROM restore_lifetime_keys WHERE old_stream_key=?1",
                    [old_key],
                    |r| r.get(0),
                )
                .map_err(|e| {
                    RestoreError::CorruptBackup(format!("map retry policy lifetime: {e}"))
                })?;
            tx.execute(
                "INSERT INTO retention_streams VALUES(?1,?2,?3)",
                params![new_key, oldest.as_slice(), current.as_slice()],
            )
            .map_err(|e| map_sql_write("insert retry policy", e))?;
        }
        tx.commit()
            .map_err(|e| map_sql_write("commit retry policy page", e))?;
        after = batch.last().unwrap().0;
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    let identity_limit = bounded_limit(336)?;
    let mut after: Option<i64> = None;
    loop {
        type Identity = (
            i64,
            i64,
            Option<Vec<u8>>,
            Option<String>,
            Option<Vec<u8>>,
            Option<i64>,
        );
        let batch: Vec<Identity> = {
            let mut stmt=source.prepare("SELECT rowid,stream_key,CASE WHEN typeof(generation)='blob' AND length(generation)=8 THEN generation END,CASE WHEN typeof(event_id)='text' AND octet_length(event_id) BETWEEN 1 AND 256 THEN event_id END,CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,CASE WHEN typeof(charge)='integer' AND charge>=0 THEN charge END FROM retention_retry_identities WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2").map_err(|e|RestoreError::CorruptBackup(format!("prepare retry identities: {e}")))?;
            let rows = stmt
                .query_map(params![after, identity_limit], |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                })
                .map_err(|e| RestoreError::CorruptBackup(format!("read retry identities: {e}")))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    RestoreError::CorruptBackup(format!("decode retry identities: {e}"))
                })?;
            rows
        };
        if batch.is_empty() {
            break;
        }
        let tx = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write("begin retry identity page", e))?;
        for (_, old_key, generation, event, offset, charge) in &batch {
            let generation = fixed::<8>(generation.clone(), "retry identity generation")?;
            let offset = fixed::<8>(offset.clone(), "retry identity offset")?;
            let event = event
                .as_ref()
                .ok_or_else(|| RestoreError::CorruptBackup("invalid retry event ID".into()))?;
            let charge = charge.ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retry identity charge".into())
            })?;
            let new_key: i64 = tx
                .query_row(
                    "SELECT new_stream_key FROM restore_lifetime_keys WHERE old_stream_key=?1",
                    [old_key],
                    |r| r.get(0),
                )
                .map_err(|e| {
                    RestoreError::CorruptBackup(format!("map retry identity lifetime: {e}"))
                })?;
            tx.execute(
                "INSERT INTO retention_retry_identities VALUES(?1,?2,?3,?4,?5)",
                params![
                    new_key,
                    generation.as_slice(),
                    event,
                    offset.as_slice(),
                    charge
                ],
            )
            .map_err(|e| map_sql_write("insert retry identity", e))?;
        }
        tx.commit()
            .map_err(|e| map_sql_write("commit retry identity page", e))?;
        after = Some(batch.last().unwrap().0);
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    let mut after = String::new();
    loop {
        type Receipt = (
            Option<String>,
            Option<i64>,
            Option<String>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<Vec<u8>>,
            Option<i64>,
        );
        let mut batch: Vec<Receipt> = Vec::with_capacity(config.page.max_records.min(64));
        let mut batch_bytes = 0usize;
        let mut cursor = after.clone();
        while batch.len() < config.page.max_records {
            let row: Option<Receipt> = source.query_row("SELECT CASE WHEN typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256 THEN operation_id END,CASE WHEN typeof(kind)='integer' AND kind BETWEEN 0 AND 3 THEN kind END,CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,CASE WHEN argument_one IS NULL OR (typeof(argument_one)='blob' AND length(argument_one)=8) THEN argument_one END,CASE WHEN argument_two IS NULL OR (typeof(argument_two)='blob' AND length(argument_two)=8) THEN argument_two END,CASE WHEN typeof(result_floor)='blob' AND length(result_floor)=8 THEN result_floor END,CASE WHEN typeof(result_tail)='blob' AND length(result_tail)=8 THEN result_tail END,CASE WHEN typeof(result_oldest)='blob' AND length(result_oldest)=8 THEN result_oldest END,CASE WHEN typeof(result_current)='blob' AND length(result_current)=8 THEN result_current END,CASE WHEN typeof(charge)='integer' AND charge>=0 THEN charge END FROM retention_receipts WHERE operation_id>?1 ORDER BY operation_id LIMIT 1",[cursor.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?))).optional().map_err(|e|RestoreError::CorruptBackup(format!("read retention receipt: {e}")))?;
            let Some(row) = row else { break };
            let operation = row.0.as_ref().ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention operation ID".into())
            })?;
            let public = row.2.as_ref().ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention receipt stream ID".into())
            })?;
            let charge = operation
                .len()
                .checked_add(public.len())
                .and_then(|n| n.checked_add(16 + 8 * 6 + 128))
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if batch_bytes
                .checked_add(charge)
                .is_none_or(|n| n > config.page.max_bytes)
            {
                break;
            }
            batch_bytes += charge;
            cursor = operation.clone();
            batch.push(row);
        }
        if batch.is_empty() {
            break;
        }
        let tx = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write("begin retention receipt page", e))?;
        for (op, kind, id, old, one, two, floor, tail, oldest, current, charge) in &batch {
            let op = op.as_ref().ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention operation ID".into())
            })?;
            let kind = kind.ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention receipt kind".into())
            })?;
            let id = id.as_ref().ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention receipt stream ID".into())
            })?;
            let old = fixed::<16>(old.clone(), "retention receipt incarnation")?;
            let floor = fixed::<8>(floor.clone(), "retention receipt result floor")?;
            let tail = fixed::<8>(tail.clone(), "retention receipt result tail")?;
            let oldest = fixed::<8>(oldest.clone(), "retention receipt result oldest")?;
            let current = fixed::<8>(current.clone(), "retention receipt result current")?;
            let charge = charge.ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention receipt charge".into())
            })?;
            let new = mapped_incarnation(&tx, id, &old)?;
            tx.execute(
                "INSERT INTO retention_receipts VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    op,
                    kind,
                    id,
                    new.as_slice(),
                    one,
                    two,
                    floor.as_slice(),
                    tail.as_slice(),
                    oldest.as_slice(),
                    current.as_slice(),
                    charge
                ],
            )
            .map_err(|e| map_sql_write("insert retention receipt", e))?;
        }
        tx.commit()
            .map_err(|e| map_sql_write("commit retention receipt page", e))?;
        after =
            batch.last().unwrap().0.clone().ok_or_else(|| {
                RestoreError::CorruptBackup("invalid retention operation ID".into())
            })?;
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    let cleanup_limit = bounded_limit(64)?;
    let mut after = i64::MIN;
    loop {
        let keys = {
            let mut stmt = source
                .prepare("SELECT stream_key FROM retention_cleanup WHERE stream_key>?1 ORDER BY stream_key LIMIT ?2")
                .map_err(|e| RestoreError::CorruptBackup(format!("prepare retention cleanup: {e}")))?;
            let rows = stmt
                .query_map(params![after, cleanup_limit], |r| r.get::<_, i64>(0))
                .map_err(|e| RestoreError::CorruptBackup(format!("read retention cleanup: {e}")))?
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    RestoreError::CorruptBackup(format!("decode retention cleanup: {e}"))
                })?;
            rows
        };
        if keys.is_empty() {
            break;
        }
        let tx = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write("begin retention cleanup page", e))?;
        for old_key in &keys {
            let new_key: i64 = tx
                .query_row(
                    "SELECT new_stream_key FROM restore_lifetime_keys WHERE old_stream_key=?1",
                    [old_key],
                    |r| r.get(0),
                )
                .map_err(|e| RestoreError::CorruptBackup(format!("map cleanup lifetime: {e}")))?;
            tx.execute("INSERT INTO retention_cleanup VALUES(?1)", [new_key])
                .map_err(|e| map_sql_write("insert retention cleanup", e))?;
        }
        tx.commit()
            .map_err(|e| map_sql_write("commit retention cleanup page", e))?;
        after = *keys.last().unwrap();
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    let counters:(i64,i64,i64,Vec<u8>,i64)=source.query_row("SELECT receipt_count,receipt_bytes,retry_rows,retry_bytes,pending_count FROM retention_metadata WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).map_err(|e|RestoreError::CorruptBackup(format!("read retention counters: {e}")))?;
    target.execute("UPDATE retention_metadata SET receipt_count=?1,receipt_bytes=?2,retry_rows=?3,retry_bytes=?4,pending_count=?5 WHERE singleton=1",params![counters.0,counters.1,counters.2,counters.3,counters.4]).map_err(|e|map_sql_write("copy retention counters",e))?;
    super::sqlite_retention::validate_retention_counters(target).map_err(|error| {
        RestoreError::CorruptBackup(format!("invalid restored retention accounting: {error}"))
    })?;
    Ok(())
}

#[cfg(feature = "source-journal")]
fn import_journal(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    if !journal_data_present(source)? {
        return Ok(());
    }
    super::sqlite_source_journal::validate_journal_counters(source).map_err(|error| {
        RestoreError::CorruptBackup(format!("invalid source journal accounting: {error}"))
    })?;
    let finalization_columns: i64 = source.query_row(
        "SELECT count(*) FROM pragma_table_info('journal_sources') WHERE name IN('sealed_end','parser_finished')",
        [], |row| row.get(0),
    ).map_err(|error| RestoreError::CorruptBackup(format!("inspect source finalization schema: {error}")))?;
    if !matches!(finalization_columns, 0 | 2) {
        return Err(RestoreError::CorruptBackup(
            "source finalization schema is partial".into(),
        ));
    }
    let (finalization_validity, finalization_values) = if finalization_columns == 2 {
        (
            "AND (sealed_end IS NULL OR (typeof(sealed_end)='blob' AND length(sealed_end)=8 AND sealed_end=captured_end)) AND typeof(parser_finished)='integer' AND parser_finished IN(0,1) AND (parser_finished=0 OR (sealed_end IS NOT NULL AND checkpoint_offset=sealed_end))",
            "sealed_end,parser_finished",
        )
    } else {
        ("", "NULL,0")
    };
    let source_select = format!(
        "SELECT rowid,octet_length(source_id)+octet_length(parser_id)+octet_length(output_public_id)+coalesce(octet_length(checkpoint_state),0)+512,typeof(source_id)='text' AND octet_length(source_id) BETWEEN 1 AND 256 AND typeof(source_incarnation)='blob' AND length(source_incarnation)=16 AND typeof(parser_id)='text' AND octet_length(parser_id) BETWEEN 1 AND 256 AND typeof(parser_version)='integer' AND parser_version BETWEEN 0 AND 4294967295 AND typeof(output_public_id)='text' AND octet_length(output_public_id) BETWEEN 1 AND 256 AND typeof(output_incarnation)='blob' AND length(output_incarnation)=16 AND typeof(captured_end)='blob' AND length(captured_end)=8 AND typeof(receipt_floor)='blob' AND length(receipt_floor)=8 AND typeof(next_item_index)='blob' AND length(next_item_index)=8 AND (last_source_byte IS NULL OR (typeof(last_source_byte)='blob' AND length(last_source_byte)=8)) AND ((checkpoint_offset IS NULL AND checkpoint_state IS NULL AND checkpoint_next_index IS NULL AND checkpoint_committed IS NULL AND checkpoint_charge=0) OR (typeof(checkpoint_offset)='blob' AND length(checkpoint_offset)=8 AND typeof(checkpoint_state)='blob' AND typeof(checkpoint_next_index)='blob' AND length(checkpoint_next_index)=8 AND (checkpoint_committed IS NULL OR (typeof(checkpoint_committed)='blob' AND length(checkpoint_committed)=8)) AND typeof(checkpoint_charge)='integer' AND checkpoint_charge=octet_length(checkpoint_state)+256+octet_length(source_id)+octet_length(parser_id)+octet_length(output_public_id))) {finalization_validity},source_id,source_incarnation,parser_id,parser_version,output_public_id,output_incarnation,captured_end,receipt_floor,next_item_index,last_source_byte,checkpoint_offset,checkpoint_state,checkpoint_next_index,checkpoint_committed,checkpoint_charge,{finalization_values} FROM journal_sources WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2"
    );
    copy_journal_rows(source, target,
        &source_select,
        "INSERT INTO journal_sources VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)", Some((4,5)), "source binding", config)?;
    copy_journal_rows(source, target,
        "SELECT rowid,octet_length(source_id)+octet_length(bytes)+160,typeof(source_id)='text' AND octet_length(source_id) BETWEEN 1 AND 256 AND typeof(source_incarnation)='blob' AND length(source_incarnation)=16 AND typeof(start)='blob' AND length(start)=8 AND typeof(end)='blob' AND length(end)=8 AND start<end AND typeof(bytes)='blob',source_id,source_incarnation,start,end,bytes FROM journal_segments WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO journal_segments VALUES(?1,?2,?3,?4,?5)", None, "captured segment", config)?;
    copy_journal_rows(source, target,
        "SELECT rowid,2*octet_length(source_id)+160,typeof(source_id)='text' AND octet_length(source_id) BETWEEN 1 AND 256 AND typeof(source_incarnation)='blob' AND length(source_incarnation)=16 AND typeof(start)='blob' AND length(start)=8 AND typeof(end)='blob' AND length(end)=8 AND typeof(digest)='blob' AND length(digest)=32 AND typeof(charge)='integer' AND charge=2*octet_length(source_id)+160,source_id,source_incarnation,start,end,digest,charge FROM journal_capture_receipts WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO journal_capture_receipts VALUES(?1,?2,?3,?4,?5,?6)", None, "capture receipt", config)?;
    copy_journal_rows(source, target,
        "SELECT m.rowid,octet_length(m.source_id)+octet_length(m.event_id)+octet_length(s.output_public_id)+192,typeof(m.source_id)='text' AND octet_length(m.source_id) BETWEEN 1 AND 256 AND typeof(m.source_incarnation)='blob' AND length(m.source_incarnation)=16 AND typeof(m.item_index)='blob' AND length(m.item_index)=8 AND typeof(m.source_byte)='blob' AND length(m.source_byte)=8 AND typeof(m.generation)='blob' AND length(m.generation)=8 AND typeof(m.event_id)='text' AND octet_length(m.event_id) BETWEEN 1 AND 256 AND typeof(m.committed_offset)='blob' AND length(m.committed_offset)=8 AND typeof(m.charge)='integer' AND m.charge=octet_length(m.source_id)+octet_length(m.event_id)+octet_length(s.output_public_id)+192,m.source_id,m.source_incarnation,m.item_index,m.source_byte,m.generation,m.event_id,m.committed_offset,m.charge FROM journal_markers m JOIN journal_sources s USING(source_id,source_incarnation) WHERE (?1 IS NULL OR m.rowid>?1) ORDER BY m.rowid LIMIT ?2",
        "INSERT INTO journal_markers VALUES(?1,?2,?3,?4,?5,?6,?7,?8)", None, "output marker", config)?;
    copy_journal_rows(source, target,
        "SELECT rowid,CASE kind WHEN 0 THEN octet_length(operation_id)+4*octet_length(source_id)+octet_length(parser_id)+2*octet_length(output_public_id)+160 ELSE octet_length(operation_id)+3*octet_length(source_id)+160 END,typeof(operation_id)='text' AND octet_length(operation_id) BETWEEN 1 AND 256 AND kind IN(0,1) AND typeof(source_id)='text' AND octet_length(source_id) BETWEEN 1 AND 256 AND typeof(source_incarnation)='blob' AND length(source_incarnation)=16 AND (argument_one IS NULL OR (typeof(argument_one)='blob' AND length(argument_one)=8)) AND (argument_two IS NULL OR (typeof(argument_two)='blob' AND length(argument_two)=8)) AND ((kind=0 AND typeof(parser_id)='text' AND octet_length(parser_id) BETWEEN 1 AND 256 AND typeof(parser_version)='integer' AND parser_version BETWEEN 0 AND 4294967295 AND typeof(output_public_id)='text' AND octet_length(output_public_id) BETWEEN 1 AND 256 AND typeof(output_incarnation)='blob' AND length(output_incarnation)=16 AND charge=octet_length(operation_id)+4*octet_length(source_id)+octet_length(parser_id)+2*octet_length(output_public_id)+160) OR (kind=1 AND parser_id IS NULL AND parser_version IS NULL AND output_public_id IS NULL AND output_incarnation IS NULL AND charge=octet_length(operation_id)+3*octet_length(source_id)+160)) AND typeof(result_captured_end)='blob' AND length(result_captured_end)=8 AND typeof(result_receipt_floor)='blob' AND length(result_receipt_floor)=8 AND typeof(result_checkpoint_offset)='blob' AND length(result_checkpoint_offset)=8 AND typeof(result_next_item_index)='blob' AND length(result_next_item_index)=8 AND (result_committed IS NULL OR (typeof(result_committed)='blob' AND length(result_committed)=8)) AND typeof(charge)='integer' AND charge>=0,operation_id,kind,source_id,source_incarnation,argument_one,argument_two,parser_id,parser_version,output_public_id,output_incarnation,result_captured_end,result_receipt_floor,result_checkpoint_offset,result_next_item_index,result_committed,charge FROM journal_operations WHERE (?1 IS NULL OR rowid>?1) ORDER BY rowid LIMIT ?2",
        "INSERT INTO journal_operations VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)", Some((8,9)), "journal operation receipt", config)?;
    let counters:JournalCounters=source.query_row("SELECT source_count,segment_count,captured_bytes,marker_count,marker_bytes,checkpoint_count,checkpoint_bytes,receipt_count,receipt_bytes FROM journal_metadata WHERE singleton=1",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).map_err(|e|RestoreError::CorruptBackup(format!("read source journal counters: {e}")))?;
    target.execute("UPDATE journal_metadata SET source_count=?1,segment_count=?2,captured_bytes=?3,marker_count=?4,marker_bytes=?5,checkpoint_count=?6,checkpoint_bytes=?7,receipt_count=?8,receipt_bytes=?9 WHERE singleton=1",params![counters.0,counters.1,counters.2,counters.3,counters.4,counters.5,counters.6,counters.7,counters.8]).map_err(|e|map_sql_write("copy source journal counters",e))?;
    super::sqlite_source_journal::validate_journal_counters(target)
        .map_err(|e| RestoreError::CorruptBackup(format!("validate restored source journal: {e}")))
}

#[cfg(feature = "source-journal")]
fn copy_journal_rows(
    source: &Connection,
    target: &mut Connection,
    select_sql: &str,
    insert_sql: &str,
    mapping_columns: Option<(usize, usize)>,
    label: &str,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let mut after: Option<i64> = None;
    loop {
        let mut statement = source
            .prepare(select_sql)
            .map_err(|e| RestoreError::CorruptBackup(format!("prepare {label}: {e}")))?;
        let limit =
            i64::try_from(config.page.max_records).map_err(|_| RestoreError::CapacityExceeded)?;
        let column_count = statement.column_count();
        let mut rows = statement
            .query(params![after, limit])
            .map_err(|e| RestoreError::CorruptBackup(format!("query {label}: {e}")))?;
        let mut batch: Vec<(i64, Vec<Value>)> =
            Vec::with_capacity(config.page.max_records.min(256));
        let mut page_bytes = 0usize;
        while let Some(row) = rows
            .next()
            .map_err(|e| RestoreError::CorruptBackup(format!("step {label}: {e}")))?
        {
            let rowid: i64 = row
                .get(0)
                .map_err(|e| RestoreError::CorruptBackup(format!("read {label} rowid: {e}")))?;
            let charge: Option<i64> = row
                .get(1)
                .map_err(|e| RestoreError::CorruptBackup(format!("read {label} charge: {e}")))?;
            let valid: i64 = row
                .get(2)
                .map_err(|e| RestoreError::CorruptBackup(format!("read {label} validity: {e}")))?;
            let charge =
                usize::try_from(charge.ok_or_else(|| {
                    RestoreError::CorruptBackup(format!("invalid {label} charge"))
                })?)
                .map_err(|_| RestoreError::CorruptBackup(format!("invalid {label} charge")))?;
            if valid != 1 {
                return Err(RestoreError::CorruptBackup(format!("invalid {label}")));
            }
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if page_bytes
                .checked_add(charge)
                .is_none_or(|sum| sum > config.page.max_bytes)
            {
                break;
            }
            let mut values = Vec::with_capacity(column_count.saturating_sub(3));
            for column in 3..column_count {
                values
                    .push(row.get(column).map_err(|e| {
                        RestoreError::CorruptBackup(format!("decode {label}: {e}"))
                    })?);
            }
            page_bytes += charge;
            batch.push((rowid, values));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let tx = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| map_sql_write(&format!("begin {label} page"), e))?;
        for (_, values) in &mut batch {
            if let Some((id_column, incarnation_column)) = mapping_columns {
                if !matches!(values[incarnation_column], Value::Null) {
                    let Value::Text(public_id) = &values[id_column] else {
                        return Err(RestoreError::CorruptBackup(format!(
                            "invalid {label} stream"
                        )));
                    };
                    let public_id = public_id.clone();
                    let Value::Blob(old) = &values[incarnation_column] else {
                        return Err(RestoreError::CorruptBackup(format!(
                            "invalid {label} incarnation"
                        )));
                    };
                    let old = fixed::<16>(Some(old.clone()), "journal output incarnation")?;
                    values[incarnation_column] =
                        Value::Blob(mapped_incarnation(&tx, &public_id, &old)?.to_vec());
                }
            }
            tx.execute(insert_sql, params_from_iter(values.iter()))
                .map_err(|e| map_sql_write(&format!("insert {label}"), e))?;
        }
        tx.commit()
            .map_err(|e| map_sql_write(&format!("commit {label} page"), e))?;
        after = Some(batch.last().unwrap().0);
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }
    Ok(())
}

#[cfg(not(feature = "snapshots"))]
fn import_snapshots(
    source: &Connection,
    _target: &mut Connection,
    _config: &RestoreConfig,
) -> RestoreResult<()> {
    if snapshot_schema_present(source)? {
        return Err(RestoreError::UnsupportedBackup(2));
    }
    Ok(())
}

#[cfg(feature = "snapshots")]
fn import_snapshots(
    source: &Connection,
    target: &mut Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    if !snapshot_schema_present(source)? {
        return Ok(());
    }
    super::sqlite_snapshot::validate_snapshot_counters(source).map_err(|error| {
        RestoreError::CorruptBackup(format!("invalid snapshot accounting: {error}"))
    })?;

    let mut after: Option<[u8; 16]> = None;
    loop {
        let mut statement = source.prepare(
            "SELECT CASE WHEN typeof(snapshot_id)='blob' AND length(snapshot_id)=16 THEN snapshot_id END,
                    CASE WHEN typeof(public_id)='text' AND octet_length(public_id) BETWEEN 1 AND 256 THEN public_id END,
                    CASE WHEN typeof(incarnation)='blob' AND length(incarnation)=16 THEN incarnation END,
                    CASE WHEN typeof(covered)='blob' AND length(covered)=8 THEN covered END,
                    CASE WHEN typeof(schema_id)='text' AND octet_length(schema_id) BETWEEN 1 AND 256 THEN schema_id END,
                    CASE WHEN typeof(schema_version)='integer' THEN schema_version END,
                    CASE WHEN typeof(content_bytes)='blob' AND length(content_bytes)=8 THEN content_bytes END,
                    CASE WHEN typeof(digest)='blob' AND length(digest)=32 THEN digest END,
                    CASE WHEN typeof(state)='integer' AND state BETWEEN 0 AND 4 THEN state END,
                    CASE WHEN typeof(accepted_bytes)='blob' AND length(accepted_bytes)=8 THEN accepted_bytes END,
                    CASE WHEN typeof(verified_bytes)='blob' AND length(verified_bytes)=8 THEN verified_bytes END,
                    CASE WHEN typeof(checksum_failed)='integer' AND checksum_failed IN (0,1) THEN checksum_failed END,
                    CASE WHEN typeof(cleaned)='integer' AND cleaned IN (0,1) THEN cleaned END,
                    CASE WHEN typeof(descriptor_charge)='integer' AND descriptor_charge>=0 THEN descriptor_charge END,
                    CASE WHEN typeof(receipt_reserved)='integer' AND receipt_reserved IN (0,1) THEN receipt_reserved END
             FROM snapshots WHERE (?1 IS NULL OR snapshot_id>?1) ORDER BY snapshot_id LIMIT ?2",
        ).map_err(|error| RestoreError::CorruptBackup(format!("prepare snapshots: {error}")))?;
        let limit =
            i64::try_from(config.page.max_records).map_err(|_| RestoreError::CapacityExceeded)?;
        let mut rows = statement
            .query(params![after.as_ref().map(|value| value.as_slice()), limit])
            .map_err(|error| RestoreError::CorruptBackup(format!("query snapshots: {error}")))?;
        let mut batch = Vec::with_capacity(config.page.max_records.min(256));
        let mut batch_bytes = 0usize;
        while let Some(row) = rows
            .next()
            .map_err(|error| RestoreError::CorruptBackup(format!("step snapshots: {error}")))?
        {
            let id = fixed::<16>(
                row.get::<_, Option<Vec<u8>>>(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read snapshot ID: {error}"))
                })?,
                "snapshot ID",
            )?;
            let public_id: Option<String> = row.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot stream: {error}"))
            })?;
            let public_id = public_id
                .ok_or_else(|| RestoreError::CorruptBackup("invalid snapshot stream".into()))?;
            let old_incarnation = fixed::<16>(
                row.get::<_, Option<Vec<u8>>>(2).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read snapshot incarnation: {error}"))
                })?,
                "snapshot incarnation",
            )?;
            let new_incarnation = mapped_incarnation(target, &public_id, &old_incarnation)?;
            let covered = fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(3).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read snapshot cursor: {error}"))
                })?,
                "snapshot cursor",
            )?;
            let schema_id: Option<String> = row.get(4).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot schema: {error}"))
            })?;
            let schema_id = schema_id
                .ok_or_else(|| RestoreError::CorruptBackup("invalid snapshot schema".into()))?;
            let values = (
                row.get::<_, Option<i64>>(5)
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!(
                            "read snapshot schema version: {error}"
                        ))
                    })?
                    .ok_or_else(|| {
                        RestoreError::CorruptBackup("invalid snapshot schema version".into())
                    })?,
                fixed::<8>(
                    row.get::<_, Option<Vec<u8>>>(6).map_err(|error| {
                        RestoreError::CorruptBackup(format!("read snapshot length: {error}"))
                    })?,
                    "snapshot length",
                )?,
                fixed::<32>(
                    row.get::<_, Option<Vec<u8>>>(7).map_err(|error| {
                        RestoreError::CorruptBackup(format!("read snapshot digest: {error}"))
                    })?,
                    "snapshot digest",
                )?,
                row.get::<_, Option<i64>>(8)
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!("read snapshot state: {error}"))
                    })?
                    .ok_or_else(|| RestoreError::CorruptBackup("invalid snapshot state".into()))?,
                fixed::<8>(
                    row.get::<_, Option<Vec<u8>>>(9).map_err(|error| {
                        RestoreError::CorruptBackup(format!("read accepted bytes: {error}"))
                    })?,
                    "accepted bytes",
                )?,
                fixed::<8>(
                    row.get::<_, Option<Vec<u8>>>(10).map_err(|error| {
                        RestoreError::CorruptBackup(format!("read verified bytes: {error}"))
                    })?,
                    "verified bytes",
                )?,
                row.get::<_, Option<i64>>(11)
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!("read checksum state: {error}"))
                    })?
                    .ok_or_else(|| RestoreError::CorruptBackup("invalid checksum state".into()))?,
                row.get::<_, Option<i64>>(12)
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!("read cleaned state: {error}"))
                    })?
                    .ok_or_else(|| RestoreError::CorruptBackup("invalid cleaned state".into()))?,
                row.get::<_, Option<i64>>(13)
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!("read descriptor charge: {error}"))
                    })?
                    .ok_or_else(|| {
                        RestoreError::CorruptBackup("invalid descriptor charge".into())
                    })?,
                row.get::<_, Option<i64>>(14)
                    .map_err(|error| {
                        RestoreError::CorruptBackup(format!("read receipt reservation: {error}"))
                    })?
                    .ok_or_else(|| {
                        RestoreError::CorruptBackup("invalid receipt reservation".into())
                    })?,
            );
            let charge = public_id
                .len()
                .checked_add(schema_id.len())
                .and_then(|value| value.checked_add(256))
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if batch_bytes
                .checked_add(charge)
                .is_none_or(|total| total > config.page.max_bytes)
            {
                break;
            }
            batch_bytes += charge;
            batch.push((id, public_id, new_incarnation, covered, schema_id, values));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write("begin snapshot descriptor page", error))?;
        for (id, public_id, incarnation, covered, schema_id, values) in &batch {
            transaction.execute(
                "INSERT INTO snapshots VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15)",
                params![id.as_slice(), public_id, incarnation.as_slice(), covered.as_slice(), schema_id, values.0, values.1.as_slice(), values.2.as_slice(), values.3, values.4.as_slice(), values.5.as_slice(), values.6, values.7, values.8, values.9],
            ).map_err(|error| map_sql_write("insert restored snapshot", error))?;
        }
        transaction
            .commit()
            .map_err(|error| map_sql_write("commit snapshot descriptor page", error))?;
        after = Some(batch.last().expect("nonempty snapshot batch").0);
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }

    let mut after_id: Option<[u8; 16]> = None;
    let mut after_offset = [0_u8; 8];
    loop {
        let mut statement = source.prepare(
            "SELECT CASE WHEN typeof(snapshot_id)='blob' AND length(snapshot_id)=16 THEN snapshot_id END,
                    CASE WHEN typeof(offset)='blob' AND length(offset)=8 THEN offset END,
                    CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?3 THEN bytes END
             FROM snapshot_chunks
             WHERE ?1 IS NULL OR snapshot_id>?1 OR (snapshot_id=?1 AND offset>?2)
             ORDER BY snapshot_id,offset LIMIT ?4",
        ).map_err(|error| RestoreError::CorruptBackup(format!("prepare snapshot chunks: {error}")))?;
        let mut rows = statement
            .query(params![
                after_id.as_ref().map(|value| value.as_slice()),
                after_offset.as_slice(),
                i64::try_from(config.page.max_bytes).map_err(|_| RestoreError::CapacityExceeded)?,
                i64::try_from(config.page.max_records)
                    .map_err(|_| RestoreError::CapacityExceeded)?
            ])
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("query snapshot chunks: {error}"))
            })?;
        let mut batch = Vec::with_capacity(config.page.max_records.min(256));
        let mut bytes = 0usize;
        while let Some(row) = rows.next().map_err(|error| {
            RestoreError::CorruptBackup(format!("step snapshot chunks: {error}"))
        })? {
            let id = fixed::<16>(
                row.get::<_, Option<Vec<u8>>>(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read chunk snapshot ID: {error}"))
                })?,
                "chunk snapshot ID",
            )?;
            let offset = fixed::<8>(
                row.get::<_, Option<Vec<u8>>>(1).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read chunk offset: {error}"))
                })?,
                "chunk offset",
            )?;
            let data: Option<Vec<u8>> = row.get(2).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot chunk: {error}"))
            })?;
            let data =
                data.ok_or_else(|| RestoreError::CorruptBackup("invalid snapshot chunk".into()))?;
            let charge = data
                .len()
                .checked_add(64)
                .ok_or(RestoreError::CapacityExceeded)?;
            if batch.is_empty() && charge > config.page.max_bytes {
                return Err(RestoreError::CapacityExceeded);
            }
            if bytes
                .checked_add(charge)
                .is_none_or(|total| total > config.page.max_bytes)
            {
                break;
            }
            bytes += charge;
            batch.push((id, offset, data));
        }
        drop(rows);
        drop(statement);
        if batch.is_empty() {
            break;
        }
        let transaction = target
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| map_sql_write("begin snapshot chunk page", error))?;
        for (id, offset, data) in &batch {
            transaction
                .execute(
                    "INSERT INTO snapshot_chunks VALUES(?1,?2,?3)",
                    params![id.as_slice(), offset.as_slice(), data],
                )
                .map_err(|error| map_sql_write("insert restored snapshot chunk", error))?;
        }
        transaction
            .commit()
            .map_err(|error| map_sql_write("commit snapshot chunk page", error))?;
        let last = batch.last().expect("nonempty chunk batch");
        after_id = Some(last.0);
        after_offset = last.1;
        enforce_staging_limit(target, config.max_staging_bytes)?;
    }

    let counters: (i64,i64,i64,i64,i64,i64,i64,i64,i64) = source.query_row(
        "SELECT staged_count,staged_bytes,published_count,published_bytes,chunk_count,chunk_metadata_bytes,descriptor_metadata_bytes,receipt_count,receipt_bytes FROM snapshot_metadata WHERE singleton=1",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?,row.get(8)?)),
    ).map_err(|error| RestoreError::CorruptBackup(format!("read snapshot counters: {error}")))?;
    target.execute("UPDATE snapshot_metadata SET staged_count=?1,staged_bytes=?2,published_count=?3,published_bytes=?4,chunk_count=?5,chunk_metadata_bytes=?6,descriptor_metadata_bytes=?7,receipt_count=?8,receipt_bytes=?9 WHERE singleton=1", params![counters.0,counters.1,counters.2,counters.3,counters.4,counters.5,counters.6,counters.7,counters.8]).map_err(|error| map_sql_write("copy snapshot counters", error))?;
    validate_restored_snapshot_semantics(target, config)?;
    super::sqlite_snapshot::validate_snapshot_counters(target).map_err(|error| {
        RestoreError::CorruptBackup(format!("restored snapshot accounting is invalid: {error}"))
    })?;
    Ok(())
}

#[cfg(feature = "snapshots")]
fn validate_restored_snapshot_semantics(
    target: &Connection,
    config: &RestoreConfig,
) -> RestoreResult<()> {
    let mut statement = target
        .prepare(
            "SELECT snapshot_id,public_id,incarnation,covered,schema_id,schema_version,
                content_bytes,digest,state,accepted_bytes,verified_bytes,checksum_failed,
                cleaned,descriptor_charge,receipt_reserved
         FROM snapshots ORDER BY snapshot_id",
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("prepare restored snapshot validation: {error}"))
        })?;
    let mut rows = statement.query([]).map_err(|error| {
        RestoreError::CorruptBackup(format!("query restored snapshots: {error}"))
    })?;
    while let Some(row) = rows
        .next()
        .map_err(|error| RestoreError::CorruptBackup(format!("step restored snapshots: {error}")))?
    {
        let id = fixed::<16>(
            Some(row.get::<_, Vec<u8>>(0).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot ID: {error}"))
            })?),
            "snapshot ID",
        )?;
        let public: String = row.get(1).map_err(|error| {
            RestoreError::CorruptBackup(format!("read snapshot stream: {error}"))
        })?;
        let incarnation = fixed::<16>(
            Some(row.get::<_, Vec<u8>>(2).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot incarnation: {error}"))
            })?),
            "snapshot incarnation",
        )?;
        let covered = decode_restore_u64(
            row.get(3).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot cursor: {error}"))
            })?,
            "snapshot cursor",
        )?;
        let schema: String = row.get(4).map_err(|error| {
            RestoreError::CorruptBackup(format!("read snapshot schema: {error}"))
        })?;
        let version: i64 = row.get(5).map_err(|error| {
            RestoreError::CorruptBackup(format!("read snapshot schema version: {error}"))
        })?;
        u32::try_from(version).map_err(|_| {
            RestoreError::CorruptBackup("snapshot schema version is out of range".into())
        })?;
        let content = decode_restore_u64(
            row.get(6).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot length: {error}"))
            })?,
            "snapshot length",
        )?;
        let digest = fixed::<32>(
            Some(row.get::<_, Vec<u8>>(7).map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot digest: {error}"))
            })?),
            "snapshot digest",
        )?;
        let state: i64 = row.get(8).map_err(|error| {
            RestoreError::CorruptBackup(format!("read snapshot state: {error}"))
        })?;
        let accepted = decode_restore_u64(
            row.get(9).map_err(|error| {
                RestoreError::CorruptBackup(format!("read accepted bytes: {error}"))
            })?,
            "accepted bytes",
        )?;
        let verified = decode_restore_u64(
            row.get(10).map_err(|error| {
                RestoreError::CorruptBackup(format!("read verified bytes: {error}"))
            })?,
            "verified bytes",
        )?;
        let checksum_failed: i64 = row.get(11).map_err(|error| {
            RestoreError::CorruptBackup(format!("read checksum state: {error}"))
        })?;
        let cleaned: i64 = row
            .get(12)
            .map_err(|error| RestoreError::CorruptBackup(format!("read cleaned state: {error}")))?;
        let charge: i64 = row.get(13).map_err(|error| {
            RestoreError::CorruptBackup(format!("read descriptor charge: {error}"))
        })?;
        let receipt: i64 = row
            .get(14)
            .map_err(|error| RestoreError::CorruptBackup(format!("read receipt state: {error}")))?;
        let expected_charge = public
            .len()
            .checked_add(schema.len())
            .and_then(|value| value.checked_add(SNAPSHOT_DESCRIPTOR_ENVELOPE_BYTES))
            .ok_or(RestoreError::CapacityExceeded)?;
        if charge != i64::try_from(expected_charge).map_err(|_| RestoreError::CapacityExceeded)?
            || accepted > content
            || verified > accepted
            || !(0..=4).contains(&state)
            || !(0..=1).contains(&checksum_failed)
            || !(0..=1).contains(&cleaned)
            || !(0..=1).contains(&receipt)
        {
            return Err(RestoreError::CorruptBackup(
                "snapshot progress metadata is inconsistent".into(),
            ));
        }
        let bounds: Option<(Vec<u8>, Vec<u8>)> = target
            .query_row(
                "SELECT floor,tail FROM event_streams WHERE public_id=?1 AND incarnation=?2",
                params![public, incarnation.as_slice()],
                |bounds| Ok((bounds.get(0)?, bounds.get(1)?)),
            )
            .optional()
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("read snapshot stream bounds: {error}"))
            })?;
        if let Some((floor, tail)) = bounds {
            decode_restore_u64(floor, "snapshot stream floor")?;
            if covered > decode_restore_u64(tail, "snapshot stream tail")? {
                return Err(RestoreError::CorruptBackup(
                    "snapshot cursor is outside its stream bounds".into(),
                ));
            }
        }

        let mut chunks_statement = target.prepare(
            "SELECT offset,CASE WHEN typeof(bytes)='blob' AND octet_length(bytes)<=?2 THEN bytes END
             FROM snapshot_chunks WHERE snapshot_id=?1 ORDER BY offset",
        ).map_err(|error| RestoreError::CorruptBackup(format!("prepare restored snapshot chunks: {error}")))?;
        let mut chunks = chunks_statement
            .query(params![
                id.as_slice(),
                i64::try_from(config.page.max_bytes).map_err(|_| RestoreError::CapacityExceeded)?
            ])
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("query restored snapshot chunks: {error}"))
            })?;
        let mut next = 0_u64;
        let mut saw_chunk = false;
        let mut hasher = Sha256::new();
        while let Some(chunk) = chunks.next().map_err(|error| {
            RestoreError::CorruptBackup(format!("step restored snapshot chunks: {error}"))
        })? {
            let offset = decode_restore_u64(
                chunk.get(0).map_err(|error| {
                    RestoreError::CorruptBackup(format!("read chunk offset: {error}"))
                })?,
                "chunk offset",
            )?;
            let bytes: Option<Vec<u8>> = chunk.get(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read chunk bytes: {error}"))
            })?;
            let bytes = bytes.ok_or(RestoreError::CapacityExceeded)?;
            if !saw_chunk && state == 4 && cleaned == 0 {
                next = offset;
            }
            if bytes.is_empty() || offset != next {
                return Err(RestoreError::CorruptBackup(
                    "snapshot chunks are empty or noncontiguous".into(),
                ));
            }
            saw_chunk = true;
            next = next
                .checked_add(bytes.len() as u64)
                .ok_or(RestoreError::CapacityExceeded)?;
            if next > accepted {
                return Err(RestoreError::CorruptBackup(
                    "snapshot chunks exceed accepted bytes".into(),
                ));
            }
            hasher.update(&bytes);
        }
        if cleaned == 1 {
            if state != 4 || accepted != 0 || verified != 0 || next != 0 || receipt != 1 {
                return Err(RestoreError::CorruptBackup(
                    "cleaned snapshot receipt is inconsistent".into(),
                ));
            }
        } else if next != accepted && !(state == 4 && !saw_chunk) {
            return Err(RestoreError::CorruptBackup(
                "snapshot chunks do not cover accepted bytes".into(),
            ));
        }
        if state == 2 || state == 3 {
            if accepted != content
                || verified != content
                || checksum_failed != 0
                || cleaned != 0
                || hasher.finalize().as_slice() != digest
            {
                return Err(RestoreError::CorruptBackup(
                    "published or verified snapshot has invalid content".into(),
                ));
            }
        } else if state == 0 && verified != 0 {
            return Err(RestoreError::CorruptBackup(
                "uploading snapshot has verified bytes".into(),
            ));
        } else if state == 4 && receipt != 1 {
            return Err(RestoreError::CorruptBackup(
                "aborted snapshot is missing its durable receipt".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "snapshots")]
fn decode_restore_u64(value: Vec<u8>, label: &str) -> RestoreResult<u64> {
    Ok(u64::from_be_bytes(fixed::<8>(Some(value), label)?))
}

fn validate_import(target: &Connection) -> RestoreResult<()> {
    validate_lifecycle_counters(target).map_err(map_store_error)?;
    let foreign_violation: bool = target
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("validate restored foreign keys: {error}"))
        })?;
    if foreign_violation {
        return Err(RestoreError::CorruptBackup(
            "restored database contains a foreign-key violation".into(),
        ));
    }
    let quick: String = target
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("validate restored SQLite: {error}"))
        })?;
    if quick != "ok" {
        return Err(RestoreError::CorruptBackup(format!(
            "restored SQLite quick_check failed: {quick}"
        )));
    }
    Ok(())
}

fn complete_staging(
    target: &mut Connection,
    request: &RestoreRequest,
    destination: &Path,
) -> RestoreResult<()> {
    let destination = destination
        .to_str()
        .ok_or_else(|| RestoreError::InvalidConfig("canonical destination must be UTF-8".into()))?;
    let transaction = target
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| map_sql_write("begin staging completion", error))?;
    let matches: bool = transaction
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM restore_metadata WHERE singleton=1 AND operation_id=?1 AND backup_identity=?2 AND destination=?3)",
            params![request.operation_id.as_str(),request.backup_identity.0.as_slice(),destination],
            |row| row.get(0),
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("validate restore identity: {error}")))?;
    if !matches {
        return Err(RestoreError::RequestConflict {
            operation_id: request.operation_id.clone(),
        });
    }
    transaction
        .execute("DROP TABLE restore_lifetime_keys", [])
        .map_err(|error| map_sql_write("drop staging key map", error))?;
    transaction
        .execute(
            "UPDATE event_stream_metadata SET restore_incomplete=2 WHERE singleton=1 AND restore_incomplete=1",
            [],
        )
        .map_err(|error| map_sql_write("mark restore complete", error))?;
    transaction
        .commit()
        .map_err(|error| map_sql_write("commit staging completion", error))
}

fn read_published_receipt(
    destination: &Path,
    request: &RestoreRequest,
    recover_publication: bool,
) -> RestoreResult<RestoreReceipt> {
    let conn = open_read_only(destination)?;
    let row: Option<(Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT CASE WHEN typeof(mapping_count)='integer' AND mapping_count>=0 THEN mapping_count END,
                    CASE WHEN typeof(restore_incomplete)='integer' AND restore_incomplete BETWEEN 0 AND 2 THEN restore_incomplete END
             FROM restore_metadata,event_stream_metadata
             WHERE restore_metadata.singleton=1 AND event_stream_metadata.singleton=1
               AND operation_id=?1 AND backup_identity=?2 AND destination=?3",
            params![
                request.operation_id.as_str(),
                request.backup_identity.0.as_slice(),
                destination.to_str()
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|_| RestoreError::DestinationExists(Box::new(destination.to_owned())))?;
    let Some((mapping_count, state)) = row else {
        let same_operation: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM restore_metadata WHERE operation_id=?1)",
                params![request.operation_id.as_str()],
                |row| row.get(0),
            )
            .unwrap_or(false);
        if same_operation {
            return Err(RestoreError::RequestConflict {
                operation_id: request.operation_id.clone(),
            });
        }
        return Err(RestoreError::DestinationExists(Box::new(
            destination.to_owned(),
        )));
    };
    let mapping_count =
        mapping_count.ok_or_else(|| RestoreError::CorruptBackup("invalid mapping count".into()))?;
    let state = state.ok_or_else(|| RestoreError::CorruptBackup("invalid restore state".into()))?;
    if recover_publication {
        validate_mapping_count(&conn, mapping_count)?;
    }
    drop(conn);
    match state {
        0 => {
            let conn = open_read_only(destination)?;
            if inspect_existing_format(&conn).map_err(map_store_error)? != Some(2) {
                return Err(RestoreError::CorruptBackup(
                    "published restore format is invalid".into(),
                ));
            }
            if recover_publication {
                validate_lifecycle_counters(&conn).map_err(map_store_error)?;
            }
        }
        2 if recover_publication => {
            let conn = open_read_only(destination)?;
            inspect_restore_format(&conn, 2).map_err(map_store_error)?;
            validate_lifecycle_counters(&conn).map_err(map_store_error)?;
            let quick: String = conn
                .query_row("PRAGMA quick_check", [], |row| row.get(0))
                .map_err(|error| {
                    RestoreError::CorruptBackup(format!(
                        "check completed restore before publication recovery: {error}"
                    ))
                })?;
            if quick != "ok" {
                return Err(RestoreError::CorruptBackup(
                    "completed restore failed integrity validation".into(),
                ));
            }
            let foreign_violation: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check)",
                    [],
                    |row| row.get(0),
                )
                .map_err(|error| {
                    RestoreError::CorruptBackup(format!(
                        "check completed restore references: {error}"
                    ))
                })?;
            if foreign_violation {
                return Err(RestoreError::CorruptBackup(
                    "completed restore has a foreign-key violation".into(),
                ));
            }
            drop(conn);
            finalize_published_marker(destination).map_err(|_| {
                RestoreError::PublicationUnknown {
                    operation_id: request.operation_id.clone(),
                    destination: Box::new(destination.to_owned()),
                }
            })?;
        }
        _ => {
            return Err(RestoreError::PublicationUnknown {
                operation_id: request.operation_id.clone(),
                destination: Box::new(destination.to_owned()),
            })
        }
    }
    Ok(RestoreReceipt {
        operation_id: request.operation_id.clone(),
        backup_identity: request.backup_identity,
        destination: destination.to_owned(),
        mapping_count: u64::try_from(mapping_count)
            .map_err(|_| RestoreError::CorruptBackup("invalid mapping count".into()))?,
    })
}

fn validate_staging_owner(
    staging: &Path,
    request: &RestoreRequest,
    destination: &Path,
) -> RestoreResult<bool> {
    validate_staging_file_boundary(staging)?;
    // A failed import can leave a hot DELETE journal. Validation owns the
    // deterministic staging name, so it opens read-write to let SQLite roll
    // that transaction back before reading the last committed owner marker.
    let conn = Connection::open_with_flags(
        staging,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        RestoreError::CorruptBackup(format!("recover staging for validation: {error}"))
    })?;
    let row: Option<i64> = conn
        .query_row(
            "SELECT restore_incomplete FROM restore_metadata,event_stream_metadata
             WHERE restore_metadata.singleton=1 AND event_stream_metadata.singleton=1
               AND operation_id=?1 AND backup_identity=?2 AND destination=?3",
            params![
                request.operation_id.as_str(),
                request.backup_identity.0.as_slice(),
                destination.to_str()
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| RestoreError::CorruptBackup(format!("read staging identity: {error}")))?;
    let Some(incomplete) = row else {
        return Err(RestoreError::RequestConflict {
            operation_id: request.operation_id.clone(),
        });
    };
    if !matches!(incomplete, 1 | 2) {
        return Err(RestoreError::CorruptBackup("invalid staging state".into()));
    }
    Ok(incomplete == 1)
}

fn validate_stored_mapping_count(conn: &Connection) -> RestoreResult<()> {
    let expected: Option<i64> = conn
        .query_row(
            "SELECT CASE WHEN typeof(mapping_count)='integer' AND mapping_count>=0 THEN mapping_count END
             FROM restore_metadata WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("read restored mapping count: {error}"))
        })?;
    validate_mapping_count(
        conn,
        expected.ok_or_else(|| RestoreError::CorruptBackup("invalid mapping count".into()))?,
    )
}

fn validate_mapping_count(conn: &Connection, expected: i64) -> RestoreResult<()> {
    let actual: i64 = conn
        .query_row("SELECT COUNT(*) FROM restore_mappings", [], |row| {
            row.get(0)
        })
        .map_err(|error| {
            RestoreError::CorruptBackup(format!("count restored mappings: {error}"))
        })?;
    if actual != expected {
        return Err(RestoreError::CorruptBackup(
            "restore mapping count does not match mapping rows".into(),
        ));
    }
    Ok(())
}

fn validate_staging_file_boundary(staging: &Path) -> RestoreResult<()> {
    let metadata = std::fs::symlink_metadata(staging).map_err(|error| {
        RestoreError::StorageFailure(format!("inspect restore staging file: {error}"))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RestoreError::StagingCleanupFailed {
            paths: vec![staging.to_owned()],
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(RestoreError::StagingCleanupFailed {
                paths: vec![staging.to_owned()],
            });
        }
    }
    Ok(())
}

fn publish_no_replace(
    staging: &Path,
    destination: &Path,
    request: &RestoreRequest,
    failure: &AtomicU8,
    pause_marker: Option<&Path>,
) -> RestoreResult<()> {
    match std::fs::hard_link(staging, destination) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(RestoreError::DestinationExists(Box::new(
                destination.to_owned(),
            )))
        }
        Err(error) => {
            return Err(RestoreError::StorageFailure(format!(
                "publish destination without replacement: {error}"
            )))
        }
    }
    if consume_or_pause(
        failure,
        SqliteRestoreFailureInjection::AfterDestinationLink,
        pause_marker,
    ) {
        return Err(RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination.to_owned()),
        });
    }
    if sync_file(destination).is_err()
        || sync_directory(destination.parent().unwrap()).is_err()
        || finalize_published_marker(destination).is_err()
    {
        return Err(RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination.to_owned()),
        });
    }
    if consume_or_pause(
        failure,
        SqliteRestoreFailureInjection::AfterPublishedMarker,
        pause_marker,
    ) {
        return Err(RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination.to_owned()),
        });
    }
    if std::fs::remove_file(staging).is_err()
        || sync_directory(destination.parent().unwrap()).is_err()
    {
        return Err(RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination.to_owned()),
        });
    }
    remove_owner_files(destination, request).map_err(|_| RestoreError::PublicationUnknown {
        operation_id: request.operation_id.clone(),
        destination: Box::new(destination.to_owned()),
    })?;
    Ok(())
}

fn consume_or_pause(
    failure: &AtomicU8,
    expected: SqliteRestoreFailureInjection,
    pause_marker: Option<&Path>,
) -> bool {
    let consumed = failure
        .compare_exchange(expected as u8 + 1, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok();
    if consumed {
        #[cfg(feature = "test-support")]
        if let Some(marker) = pause_marker {
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(marker)
                .expect("test-only restore pause marker must be creatable");
            file.sync_all()
                .expect("test-only restore pause marker must be durable");
            loop {
                std::thread::park();
            }
        }
        #[cfg(not(feature = "test-support"))]
        let _ = pause_marker;
    }
    consumed
}

fn finalize_published_marker(destination: &Path) -> RestoreResult<()> {
    let conn = Connection::open_with_flags(
        destination,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| RestoreError::StorageFailure(format!("open published restore: {error}")))?;
    let changed = conn
        .execute(
            "UPDATE event_stream_metadata SET restore_incomplete=0 WHERE singleton=1 AND restore_incomplete=2",
            [],
        )
        .map_err(|error| map_sql_write("mark restore published", error))?;
    if changed != 1 {
        let state: i64 = conn
            .query_row(
                "SELECT restore_incomplete FROM event_stream_metadata WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(|error| {
                RestoreError::CorruptBackup(format!("read publication state: {error}"))
            })?;
        if state != 0 {
            return Err(RestoreError::CorruptBackup(
                "published restore has an invalid state".into(),
            ));
        }
    }
    conn.close().map_err(|(_, error)| {
        RestoreError::StorageFailure(format!("close published restore: {error}"))
    })?;
    sync_file(destination)
}

fn finish_staging_alias_if_owned(
    staging: &Path,
    destination: &Path,
    request: &RestoreRequest,
) -> RestoreResult<()> {
    if !staging.exists() {
        return remove_owner_files(destination, request).map_err(|_| {
            RestoreError::PublicationUnknown {
                operation_id: request.operation_id.clone(),
                destination: Box::new(destination.to_owned()),
            }
        });
    }
    if same_file(staging, destination)? {
        std::fs::remove_file(staging).map_err(|_| RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination.to_owned()),
        })?;
        sync_directory(destination.parent().unwrap()).map_err(|_| {
            RestoreError::PublicationUnknown {
                operation_id: request.operation_id.clone(),
                destination: Box::new(destination.to_owned()),
            }
        })?;
        remove_owner_files(destination, request).map_err(|_| RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination.to_owned()),
        })?;
        return Ok(());
    }
    Err(RestoreError::IncompleteStaging {
        operation_id: request.operation_id.clone(),
        destination: Box::new(destination.to_owned()),
    })
}

#[cfg(unix)]
fn same_file(left: &Path, right: &Path) -> RestoreResult<bool> {
    use std::os::unix::fs::MetadataExt;
    let left = std::fs::metadata(left)
        .map_err(|error| RestoreError::StorageFailure(format!("inspect staging alias: {error}")))?;
    let right = std::fs::metadata(right).map_err(|error| {
        RestoreError::StorageFailure(format!("inspect destination alias: {error}"))
    })?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn same_file(_left: &Path, _right: &Path) -> RestoreResult<bool> {
    Ok(false)
}

fn sync_file(path: &Path) -> RestoreResult<()> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            RestoreError::StorageFailure(format!("synchronize {}: {error}", path.display()))
        })
}

fn sync_directory(path: &Path) -> RestoreResult<()> {
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| {
            RestoreError::StorageFailure(format!(
                "synchronize directory {}: {error}",
                path.display()
            ))
        })
}

fn read_mapping_blocking(
    root: &Path,
    receipt: &RestoreReceipt,
    after: Option<&StreamKey>,
    limits: PageLimits,
    _config: &RestoreConfig,
) -> RestoreResult<MappingPage> {
    let destination = resolve_receipt_destination(root, &receipt.destination)?;
    let _ownership = Ownership::acquire(&destination).map_err(map_store_error)?;
    let request = RestoreRequest {
        operation_id: receipt.operation_id.clone(),
        backup_identity: receipt.backup_identity,
        source: PathBuf::from("unused-for-mapping"),
        destination: destination.clone(),
    };
    let stored = read_published_receipt(&destination, &request, false)?;
    if &stored != receipt {
        return Err(RestoreError::RequestConflict {
            operation_id: receipt.operation_id.clone(),
        });
    }
    let conn = open_read_only(&destination)?;
    let (after_id, after_incarnation) = after
        .map(|key| (key.id.as_str(), key.incarnation.0))
        .unwrap_or(("", [0; 16]));
    let mut statement = conn
        .prepare(
            "SELECT CASE WHEN typeof(old_public_id)='text' AND octet_length(old_public_id) BETWEEN 1 AND 256 THEN old_public_id END,
                    CASE WHEN typeof(old_incarnation)='blob' AND length(old_incarnation)=16 THEN old_incarnation END,
                    CASE WHEN typeof(new_incarnation)='blob' AND length(new_incarnation)=16 THEN new_incarnation END
             FROM restore_mappings
         WHERE (old_public_id,old_incarnation)>(?1,?2)
         ORDER BY old_public_id,old_incarnation LIMIT ?3",
        )
        .map_err(|error| RestoreError::CorruptBackup(format!("prepare mapping page: {error}")))?;
    let mut rows = statement
        .query(params![
            after_id,
            after_incarnation.as_slice(),
            i64::try_from(limits.max_records).map_err(|_| RestoreError::CapacityExceeded)?
        ])
        .map_err(|error| RestoreError::CorruptBackup(format!("query mapping page: {error}")))?;
    let mut entries = Vec::with_capacity(limits.max_records.min(256));
    let mut bytes = 0usize;
    while let Some(row) = rows
        .next()
        .map_err(|error| RestoreError::CorruptBackup(format!("step mapping page: {error}")))?
    {
        let id: Option<String> = row
            .get(0)
            .map_err(|error| RestoreError::CorruptBackup(format!("read mapped name: {error}")))?;
        let old = fixed::<16>(
            row.get::<_, Option<Vec<u8>>>(1).map_err(|error| {
                RestoreError::CorruptBackup(format!("read old mapping: {error}"))
            })?,
            "old mapping",
        )?;
        let new = fixed::<16>(
            row.get::<_, Option<Vec<u8>>>(2).map_err(|error| {
                RestoreError::CorruptBackup(format!("read new mapping: {error}"))
            })?,
            "new mapping",
        )?;
        if old == new {
            return Err(RestoreError::CorruptBackup(
                "restored incarnation was not refreshed".into(),
            ));
        }
        let id = StreamId::new(
            id.ok_or_else(|| RestoreError::CorruptBackup("invalid mapping name".into()))?,
        )
        .map_err(|_| RestoreError::CorruptBackup("invalid mapping name".into()))?;
        let entry = IncarnationMapping {
            old: StreamKey {
                id: id.clone(),
                incarnation: IncarnationId(old),
            },
            new: StreamKey {
                id,
                incarnation: IncarnationId(new),
            },
        };
        let charge = entry
            .accounted_bytes()
            .ok_or(RestoreError::CapacityExceeded)?;
        if entries.is_empty() && charge > limits.max_bytes {
            return Err(RestoreError::CapacityExceeded);
        }
        if bytes
            .checked_add(charge)
            .is_none_or(|v| v > limits.max_bytes)
        {
            break;
        }
        bytes += charge;
        entries.push(entry);
    }
    drop(rows);
    drop(statement);
    let next_after = entries.last().map(|entry| entry.old.clone());
    let complete=match &next_after {
        None=>true,
        Some(last)=>!conn.query_row("SELECT EXISTS(SELECT 1 FROM restore_mappings WHERE (old_public_id,old_incarnation)>(?1,?2))",params![last.id.as_str(),last.incarnation.0.as_slice()],|r|r.get::<_,bool>(0)).map_err(|e|RestoreError::CorruptBackup(format!("check mapping completion: {e}")))?,
    };
    Ok(MappingPage {
        entries,
        next_after,
        complete,
    })
}

fn resolve_receipt_destination(root: &Path, path: &Path) -> RestoreResult<PathBuf> {
    if !path.is_absolute() {
        return resolve_destination(root, path);
    }
    let name = path.file_name().ok_or_else(|| {
        RestoreError::InvalidConfig("mapping destination needs a file name".into())
    })?;
    let parent = path
        .parent()
        .ok_or_else(|| RestoreError::InvalidConfig("mapping destination has no parent".into()))?
        .canonicalize()
        .map_err(|error| {
            RestoreError::InvalidConfig(format!("mapping destination parent: {error}"))
        })?;
    if !parent.starts_with(root) {
        return Err(RestoreError::InvalidConfig(
            "mapping destination escapes root".into(),
        ));
    }
    Ok(parent.join(name))
}

fn cleanup_staging_blocking(
    root: &Path,
    request: &RestoreRequest,
    _config: &RestoreConfig,
    failure: &AtomicU8,
) -> RestoreResult<()> {
    #[cfg(not(feature = "test-support"))]
    let _ = failure;
    let destination = resolve_destination(root, &request.destination)?;
    let _ownership =
        Ownership::acquire_restore_destination(&destination).map_err(map_store_error)?;
    let staging = staging_path(&destination, request)?;
    let staging_present = path_entry_exists(&staging)?;
    let owner = owner_path(&destination, &request.operation_id);
    let binding = owner.join(format!("request-{}", request_hash(&destination, request)));
    if destination.exists() {
        return Err(RestoreError::PublicationUnknown {
            operation_id: request.operation_id.clone(),
            destination: Box::new(destination),
        });
    }
    if !path_entry_exists(&owner)? {
        if staging_present {
            validate_staging_owner(&staging, request, &destination)?;
        } else {
            return Ok(());
        }
    } else {
        let metadata = std::fs::symlink_metadata(&owner).map_err(|error| {
            RestoreError::StorageFailure(format!("inspect restore owner directory: {error}"))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(RestoreError::StagingCleanupFailed {
                paths: vec![owner.clone()],
            });
        }
        let entries = bounded_directory_entries(&owner)?;
        if entries.len() > 1 || entries.first().is_some_and(|path| path != &binding) {
            return Err(RestoreError::RequestConflict {
                operation_id: request.operation_id.clone(),
            });
        }
        if entries.is_empty() && staging_present {
            return Err(RestoreError::StagingCleanupFailed {
                paths: vec![staging],
            });
        }
        if path_entry_exists(&binding)? {
            let metadata = std::fs::symlink_metadata(&binding).map_err(|error| {
                RestoreError::StorageFailure(format!("inspect restore request binding: {error}"))
            })?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(RestoreError::StagingCleanupFailed {
                    paths: vec![binding.clone()],
                });
            }
            let unexpected = bounded_directory_entries(&binding)?;
            if !unexpected.is_empty() {
                return Err(RestoreError::StagingCleanupFailed { paths: unexpected });
            }
        }
        if staging_present {
            validate_staging_file_boundary(&staging)?;
            if std::fs::symlink_metadata(&staging)
                .map_err(|error| {
                    RestoreError::StorageFailure(format!(
                        "inspect owned restore staging file: {error}"
                    ))
                })?
                .len()
                > 0
            {
                validate_staging_owner(&staging, request, &destination)?;
            }
        }
    }
    let mut failed = Vec::new();
    for path in [
        staging.clone(),
        PathBuf::from(format!("{}-journal", staging.display())),
        PathBuf::from(format!("{}-wal", staging.display())),
        PathBuf::from(format!("{}-shm", staging.display())),
    ] {
        #[cfg(feature = "test-support")]
        if path == staging
            && consume_or_pause(
                failure,
                SqliteRestoreFailureInjection::DuringCleanupUnlink,
                None,
            )
        {
            failed.push(path);
            continue;
        }
        if let Err(error) = std::fs::remove_file(&path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                failed.push(path)
            }
        }
    }
    if !failed.is_empty() {
        return Err(RestoreError::StagingCleanupFailed { paths: failed });
    }
    if let Err(error) = std::fs::remove_dir(&binding) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(RestoreError::StagingCleanupFailed {
                paths: vec![binding],
            });
        }
    }
    if let Err(error) = std::fs::remove_dir(&owner) {
        if error.kind() != std::io::ErrorKind::NotFound {
            return Err(RestoreError::StagingCleanupFailed { paths: vec![owner] });
        }
    }
    #[cfg(feature = "test-support")]
    if consume_or_pause(
        failure,
        SqliteRestoreFailureInjection::DuringCleanupDirectorySync,
        None,
    ) {
        return Err(RestoreError::StagingCleanupFailed {
            paths: vec![destination.parent().unwrap().to_owned()],
        });
    }
    sync_directory(destination.parent().unwrap())?;
    Ok(())
}
