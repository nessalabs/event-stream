use crate::domain::*;
use async_trait::async_trait;
use std::{
    future::Future,
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{oneshot, Semaphore};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreRequest {
    pub operation_id: RestoreOperationId,
    pub backup_identity: BackupIdentity,
    pub source: PathBuf,
    pub destination: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreReceipt {
    pub operation_id: RestoreOperationId,
    pub backup_identity: BackupIdentity,
    pub destination: PathBuf,
    pub mapping_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MappingPage {
    pub entries: Vec<IncarnationMapping>,
    pub next_after: Option<StreamKey>,
    pub complete: bool,
}

#[derive(Clone, Debug)]
pub struct RestoreConfig {
    pub max_operations: usize,
    pub max_source_bytes: u64,
    pub max_staging_bytes: u64,
    pub copy_buffer_bytes: usize,
    pub page: PageLimits,
    pub max_path_bytes: usize,
}

impl Default for RestoreConfig {
    fn default() -> Self {
        Self {
            max_operations: 1,
            max_source_bytes: 1024 * 1024 * 1024,
            max_staging_bytes: 1024 * 1024 * 1024,
            copy_buffer_bytes: 64 * 1024,
            page: PageLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
            max_path_bytes: 4096,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreError {
    InvalidConfig(String),
    Overloaded,
    OwnershipConflict,
    CorruptBackup(String),
    UnsupportedBackup(u32),
    IdentityMismatch {
        expected: BackupIdentity,
        actual: BackupIdentity,
    },
    RequestConflict {
        operation_id: RestoreOperationId,
    },
    IncompleteStaging {
        operation_id: RestoreOperationId,
        destination: Box<PathBuf>,
    },
    DestinationExists(Box<PathBuf>),
    CapacityExceeded,
    StorageFailure(String),
    PublicationUnknown {
        operation_id: RestoreOperationId,
        destination: Box<PathBuf>,
    },
    StagingCleanupFailed {
        paths: Vec<PathBuf>,
    },
}

impl std::fmt::Display for RestoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RestoreError {}

pub type RestoreResult<T> = std::result::Result<T, RestoreError>;

#[async_trait]
pub trait RestoreBackend: Send + Sync + 'static {
    async fn inspect_backup(
        &self,
        source: PathBuf,
        config: RestoreConfig,
    ) -> RestoreResult<BackupIdentity>;

    async fn restore(
        &self,
        request: RestoreRequest,
        config: RestoreConfig,
    ) -> RestoreResult<RestoreReceipt>;

    async fn read_mapping(
        &self,
        receipt: RestoreReceipt,
        after: Option<StreamKey>,
        limits: PageLimits,
        config: RestoreConfig,
    ) -> RestoreResult<MappingPage>;

    async fn cleanup_staging(
        &self,
        request: RestoreRequest,
        config: RestoreConfig,
    ) -> RestoreResult<()>;
}

pub struct RestoreManager<B: RestoreBackend> {
    backend: Arc<B>,
    config: RestoreConfig,
    operations: Arc<Semaphore>,
    executor: tokio::runtime::Handle,
}

impl<B: RestoreBackend> Clone for RestoreManager<B> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            config: self.config.clone(),
            operations: self.operations.clone(),
            executor: self.executor.clone(),
        }
    }
}

impl<B: RestoreBackend> RestoreManager<B> {
    pub fn new(backend: B, config: RestoreConfig) -> RestoreResult<Self> {
        validate_config(&config)?;
        let executor = tokio::runtime::Handle::try_current().map_err(|_| {
            RestoreError::InvalidConfig("restore manager requires a Tokio runtime".into())
        })?;
        Ok(Self {
            backend: Arc::new(backend),
            operations: Arc::new(Semaphore::new(config.max_operations)),
            config,
            executor,
        })
    }

    pub async fn inspect_backup(&self, source: PathBuf) -> RestoreResult<BackupIdentity> {
        validate_path(&source, self.config.max_path_bytes, false)?;
        let backend = self.backend.clone();
        let config = self.config.clone();
        self.run_owned(
            async move { backend.inspect_backup(source, config).await },
            || RestoreError::StorageFailure("backup inspection task panicked".into()),
        )
        .await
    }

    pub async fn restore(&self, request: RestoreRequest) -> RestoreResult<RestoreReceipt> {
        validate_request(&request, &self.config)?;
        let backend = self.backend.clone();
        let config = self.config.clone();
        let panic_request = request.clone();
        let expected = request.clone();
        let receipt = self
            .run_owned(
                async move { backend.restore(request, config).await },
                move || RestoreError::PublicationUnknown {
                    operation_id: panic_request.operation_id,
                    destination: Box::new(panic_request.destination),
                },
            )
            .await?;
        if receipt.operation_id != expected.operation_id
            || receipt.backup_identity != expected.backup_identity
            || path_bytes(&receipt.destination) > self.config.max_path_bytes
        {
            return Err(RestoreError::PublicationUnknown {
                operation_id: expected.operation_id,
                destination: Box::new(expected.destination),
            });
        }
        Ok(receipt)
    }

    pub async fn read_mapping(
        &self,
        receipt: RestoreReceipt,
        after: Option<StreamKey>,
        limits: PageLimits,
    ) -> RestoreResult<MappingPage> {
        validate_path(&receipt.destination, self.config.max_path_bytes, false)?;
        validate_page_limits(limits, &self.config)?;
        let backend = self.backend.clone();
        let config = self.config.clone();
        let expected_after = after.clone();
        let page = self
            .run_owned(
                async move { backend.read_mapping(receipt, after, limits, config).await },
                || RestoreError::StorageFailure("mapping read task panicked".into()),
            )
            .await?;
        validate_mapping_page(&page, expected_after.as_ref(), limits)?;
        Ok(page)
    }

    pub async fn cleanup_staging(&self, request: RestoreRequest) -> RestoreResult<()> {
        validate_request(&request, &self.config)?;
        let backend = self.backend.clone();
        let config = self.config.clone();
        self.run_owned(
            async move { backend.cleanup_staging(request, config).await },
            || RestoreError::StorageFailure("staging cleanup task panicked".into()),
        )
        .await
    }

    async fn run_owned<T, F, P>(&self, operation: F, panic_error: P) -> RestoreResult<T>
    where
        T: Send + 'static,
        F: Future<Output = RestoreResult<T>> + Send + 'static,
        P: FnOnce() -> RestoreError + Send + 'static,
    {
        let permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| RestoreError::Overloaded)?;
        let (sender, receiver) = oneshot::channel();
        let owner_error = panic_error();
        let receiver_error = owner_error.clone();
        self.executor.spawn(async move {
            let result = match tokio::spawn(operation).await {
                Ok(result) => result,
                Err(_) => Err(owner_error),
            };
            drop(permit);
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(receiver_error))
    }
}

fn validate_config(config: &RestoreConfig) -> RestoreResult<()> {
    if config.max_operations == 0
        || config.max_operations > Semaphore::MAX_PERMITS
        || config.max_source_bytes == 0
        || config.max_staging_bytes == 0
        || config.copy_buffer_bytes == 0
        || config.page.max_records == 0
        || config.page.max_bytes == 0
        || config.max_path_bytes == 0
    {
        return Err(RestoreError::InvalidConfig(
            "restore limits must be finite and nonzero".into(),
        ));
    }
    if config.copy_buffer_bytes as u64 > config.max_source_bytes
        || config.copy_buffer_bytes as u64 > config.max_staging_bytes
    {
        return Err(RestoreError::InvalidConfig(
            "one copy buffer must fit source and staging limits".into(),
        ));
    }
    if config.page.max_bytes < 640 {
        return Err(RestoreError::InvalidConfig(
            "one maximum-size incarnation mapping must fit a page".into(),
        ));
    }
    Ok(())
}

fn validate_request(request: &RestoreRequest, config: &RestoreConfig) -> RestoreResult<()> {
    validate_path(&request.source, config.max_path_bytes, false)?;
    validate_path(&request.destination, config.max_path_bytes, true)
}

fn validate_page_limits(limits: PageLimits, config: &RestoreConfig) -> RestoreResult<()> {
    if limits.max_records == 0
        || limits.max_bytes == 0
        || limits.max_records > config.page.max_records
        || limits.max_bytes > config.page.max_bytes
        || limits.max_bytes < 640
    {
        return Err(RestoreError::InvalidConfig(
            "mapping page limits exceed the configured boundary".into(),
        ));
    }
    Ok(())
}

fn validate_path(path: &Path, max_bytes: usize, relative: bool) -> RestoreResult<()> {
    let bytes = path_bytes(path);
    if bytes == 0 || bytes > max_bytes {
        return Err(RestoreError::InvalidConfig(
            "restore path is empty or exceeds max_path_bytes".into(),
        ));
    }
    if relative
        && (path.is_absolute()
            || path.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            }))
    {
        return Err(RestoreError::InvalidConfig(
            "restore destination must be a relative path inside the backend root".into(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn path_bytes(path: &Path) -> usize {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().len()
}

#[cfg(not(unix))]
fn path_bytes(path: &Path) -> usize {
    path.as_os_str().to_string_lossy().len()
}

fn validate_mapping_page(
    page: &MappingPage,
    after: Option<&StreamKey>,
    limits: PageLimits,
) -> RestoreResult<()> {
    if page.entries.len() > limits.max_records {
        return Err(RestoreError::CorruptBackup(
            "mapping page exceeds its record limit".into(),
        ));
    }
    let mut bytes = 0usize;
    let mut previous = after;
    for entry in &page.entries {
        let charge = entry
            .accounted_bytes()
            .ok_or(RestoreError::CapacityExceeded)?;
        bytes = bytes
            .checked_add(charge)
            .filter(|total| *total <= limits.max_bytes)
            .ok_or_else(|| {
                RestoreError::CorruptBackup("mapping page exceeds its byte limit".into())
            })?;
        if entry.old.id != entry.new.id
            || entry.old.incarnation == entry.new.incarnation
            || previous.is_some_and(|cursor| mapping_key(&entry.old) <= mapping_key(cursor))
        {
            return Err(RestoreError::CorruptBackup(
                "mapping entries are not strict fresh-identity order".into(),
            ));
        }
        previous = Some(&entry.old);
    }
    let expected_next = page.entries.last().map(|entry| &entry.old);
    if page.next_after.as_ref() != expected_next || (!page.complete && page.entries.is_empty()) {
        return Err(RestoreError::CorruptBackup(
            "mapping page does not make cursor progress".into(),
        ));
    }
    Ok(())
}

fn mapping_key(stream: &StreamKey) -> (&[u8], &[u8; 16]) {
    (stream.id.as_str().as_bytes(), &stream.incarnation.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Clone)]
    struct Controls {
        entered: Arc<Notify>,
        release: Arc<Notify>,
        pause_restore: Arc<AtomicBool>,
        panic_restore: Arc<AtomicBool>,
        bad_mapping: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
    }

    struct Backend(Controls);

    #[async_trait]
    impl RestoreBackend for Backend {
        async fn inspect_backup(
            &self,
            _source: PathBuf,
            _config: RestoreConfig,
        ) -> RestoreResult<BackupIdentity> {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            Ok(BackupIdentity([7; 32]))
        }

        async fn restore(
            &self,
            request: RestoreRequest,
            _config: RestoreConfig,
        ) -> RestoreResult<RestoreReceipt> {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            if self.0.pause_restore.swap(false, Ordering::SeqCst) {
                self.0.entered.notify_one();
                self.0.release.notified().await;
            }
            assert!(
                !self.0.panic_restore.swap(false, Ordering::SeqCst),
                "injected panic"
            );
            Ok(RestoreReceipt {
                operation_id: request.operation_id,
                backup_identity: request.backup_identity,
                destination: PathBuf::from("/canonical/published.sqlite3"),
                mapping_count: 0,
            })
        }

        async fn read_mapping(
            &self,
            _receipt: RestoreReceipt,
            _after: Option<StreamKey>,
            _limits: PageLimits,
            _config: RestoreConfig,
        ) -> RestoreResult<MappingPage> {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            if self.0.bad_mapping.swap(false, Ordering::SeqCst) {
                let old = StreamKey {
                    id: StreamId::new("old-name").unwrap(),
                    incarnation: IncarnationId([1; 16]),
                };
                let new = StreamKey {
                    id: StreamId::new("different-name").unwrap(),
                    incarnation: IncarnationId([2; 16]),
                };
                return Ok(MappingPage {
                    next_after: Some(old.clone()),
                    entries: vec![IncarnationMapping { old, new }],
                    complete: true,
                });
            }
            Ok(MappingPage {
                entries: Vec::new(),
                next_after: None,
                complete: true,
            })
        }

        async fn cleanup_staging(
            &self,
            _request: RestoreRequest,
            _config: RestoreConfig,
        ) -> RestoreResult<()> {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn controls() -> Controls {
        Controls {
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            pause_restore: Arc::new(AtomicBool::new(false)),
            panic_restore: Arc::new(AtomicBool::new(false)),
            bad_mapping: Arc::new(AtomicBool::new(false)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn request() -> RestoreRequest {
        RestoreRequest {
            operation_id: RestoreOperationId::new("restore-1").unwrap(),
            backup_identity: BackupIdentity([7; 32]),
            source: PathBuf::from("/backups/source.sqlite3"),
            destination: PathBuf::from("published.sqlite3"),
        }
    }

    #[tokio::test]
    async fn cancelled_restore_keeps_its_owned_slot_until_backend_finishes() {
        let controls = controls();
        controls.pause_restore.store(true, Ordering::SeqCst);
        let manager =
            RestoreManager::new(Backend(controls.clone()), RestoreConfig::default()).unwrap();
        let accepted = {
            let manager = manager.clone();
            tokio::spawn(async move { manager.restore(request()).await })
        };
        controls.entered.notified().await;
        accepted.abort();
        assert_eq!(
            manager
                .inspect_backup(PathBuf::from("/backups/other.sqlite3"))
                .await,
            Err(RestoreError::Overloaded)
        );
        controls.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match manager
                    .inspect_backup(PathBuf::from("/backups/other.sqlite3"))
                    .await
                {
                    Ok(identity) => break identity,
                    Err(RestoreError::Overloaded) => tokio::task::yield_now().await,
                    Err(error) => panic!("unexpected inspection result: {error}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(controls.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn restore_panic_is_an_exact_uncertain_publication() {
        let controls = controls();
        controls.panic_restore.store(true, Ordering::SeqCst);
        let manager = RestoreManager::new(Backend(controls), RestoreConfig::default()).unwrap();
        let request = request();
        assert_eq!(
            manager.restore(request.clone()).await,
            Err(RestoreError::PublicationUnknown {
                operation_id: request.operation_id,
                destination: Box::new(request.destination),
            })
        );
    }

    #[tokio::test]
    async fn limits_and_paths_are_rejected_before_backend_admission() {
        let controls = controls();
        let invalid = RestoreConfig {
            max_operations: 0,
            ..RestoreConfig::default()
        };
        assert!(matches!(
            RestoreManager::new(Backend(controls.clone()), invalid),
            Err(RestoreError::InvalidConfig(_))
        ));
        let manager =
            RestoreManager::new(Backend(controls.clone()), RestoreConfig::default()).unwrap();
        let mut escaping = request();
        escaping.destination = PathBuf::from("../outside.sqlite3");
        assert!(matches!(
            manager.restore(escaping).await,
            Err(RestoreError::InvalidConfig(_))
        ));
        assert_eq!(controls.calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn malformed_mapping_page_is_rejected_at_the_application_boundary() {
        let controls = controls();
        controls.bad_mapping.store(true, Ordering::SeqCst);
        let manager = RestoreManager::new(Backend(controls), RestoreConfig::default()).unwrap();
        let request = request();
        let receipt = RestoreReceipt {
            operation_id: request.operation_id,
            backup_identity: request.backup_identity,
            destination: PathBuf::from("/canonical/published.sqlite3"),
            mapping_count: 1,
        };
        assert!(matches!(
            manager
                .read_mapping(receipt, None, RestoreConfig::default().page)
                .await,
            Err(RestoreError::CorruptBackup(_))
        ));
    }
}
