#![cfg(feature = "snapshots")]

use async_trait::async_trait;
use event_stream::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Debug)]
struct SnapshotControl {
    entered: AtomicUsize,
    completed: AtomicUsize,
    entered_notify: Notify,
    release: Semaphore,
    behavior: SnapshotBehavior,
    published: AtomicUsize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SnapshotBehavior {
    Paused,
    PanicBegin,
    BadTerminal,
    NoProgress,
    PublishedOnStep,
    AbortedOnStep,
}

impl SnapshotControl {
    fn paused() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            release: Semaphore::new(0),
            behavior: SnapshotBehavior::Paused,
            published: AtomicUsize::new(0),
        })
    }

    fn panicking() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            release: Semaphore::new(0),
            behavior: SnapshotBehavior::PanicBegin,
            published: AtomicUsize::new(0),
        })
    }

    fn malformed(behavior: SnapshotBehavior) -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            release: Semaphore::new(0),
            behavior,
            published: AtomicUsize::new(0),
        })
    }

    async fn wait_for_entries(&self, expected: usize) {
        while self.entered.load(Ordering::Acquire) < expected {
            tokio::task::yield_now().await;
        }
    }
}

struct PausedSnapshotStore {
    control: Arc<SnapshotControl>,
    stream: StreamKey,
}

#[async_trait]
impl EventStore for PausedSnapshotStore {
    type Options = Arc<SnapshotControl>;

    async fn open(control: Self::Options) -> Result<Self> {
        Ok(Self {
            control,
            stream: StreamKey {
                id: StreamId::new("snapshots").unwrap(),
                incarnation: IncarnationId([7; 16]),
            },
        })
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistence: PersistenceProfile::Ephemeral,
            format_version: 1,
            max_record_bytes: 1024 * 1024,
            max_concurrent_reads: 8,
            max_concurrent_writes: 8,
            ownership: "paused snapshot contract store",
        }
    }

    async fn create_if_absent(&self, _: &StreamId) -> Result<StreamKey> {
        Ok(self.stream.clone())
    }

    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        Ok((id == &self.stream.id).then(|| self.stream.clone()))
    }

    async fn append_atomic(&self, _: &StreamKey, _: NewEvent) -> Result<AppendReceipt> {
        Err(Error::StoreWriteFailed("unused".into()))
    }

    async fn lookup_event(&self, _: &StreamKey, _: &EventId) -> Result<Option<Arc<Record>>> {
        Ok(None)
    }

    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        Ok(Bounds {
            floor: Cursor::new(stream.clone(), 0),
            tail: Cursor::new(stream.clone(), 0),
        })
    }

    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        _: u64,
        _: PageLimits,
    ) -> Result<Page> {
        Ok(Page {
            records: Vec::new(),
            next_after: Cursor::new(stream.clone(), after),
            through: Cursor::new(stream.clone(), after),
            complete: true,
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl SnapshotStore for PausedSnapshotStore {
    async fn begin_snapshot(
        &self,
        descriptor: SnapshotDescriptor,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        self.control.entered.fetch_add(1, Ordering::AcqRel);
        self.control.entered_notify.notify_waiters();
        assert_ne!(
            self.control.behavior,
            SnapshotBehavior::PanicBegin,
            "injected snapshot panic"
        );
        let permit = self.control.release.acquire().await.unwrap();
        permit.forget();
        self.control.completed.fetch_add(1, Ordering::AcqRel);
        Ok(SnapshotUploadProgress {
            descriptor,
            accepted_bytes: 0,
            verified_bytes: 0,
            state: SnapshotUploadState::Uploading,
        })
    }

    async fn put_snapshot_chunk(
        &self,
        id: SnapshotId,
        _: SnapshotChunk,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        Err(SnapshotError::NotFound { id })
    }

    async fn snapshot_status(&self, id: SnapshotId) -> SnapshotResult<SnapshotUploadProgress> {
        match self.control.behavior {
            SnapshotBehavior::BadTerminal => Ok(progress(id, SnapshotUploadState::Verified, 0)),
            SnapshotBehavior::NoProgress
            | SnapshotBehavior::PublishedOnStep
            | SnapshotBehavior::AbortedOnStep => {
                Ok(progress(id, SnapshotUploadState::Verifying, 0))
            }
            _ => Err(SnapshotError::NotFound { id }),
        }
    }

    async fn verify_snapshot_step(
        &self,
        id: SnapshotId,
        _: VerificationLimits,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        match self.control.behavior {
            SnapshotBehavior::NoProgress => Ok(progress(id, SnapshotUploadState::Verifying, 0)),
            SnapshotBehavior::PublishedOnStep => {
                Ok(progress(id, SnapshotUploadState::Published, 1))
            }
            SnapshotBehavior::AbortedOnStep => Ok(progress(id, SnapshotUploadState::Aborted, 0)),
            _ => Err(SnapshotError::NotFound { id }),
        }
    }

    async fn publish_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotDescriptor> {
        self.control.published.fetch_add(1, Ordering::AcqRel);
        Err(SnapshotError::NotFound { id })
    }

    async fn abort_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotAbortReceipt> {
        Err(SnapshotError::NotFound { id })
    }

    async fn cleanup_snapshot_staging(
        &self,
        _: SnapshotCleanupLimits,
    ) -> SnapshotResult<SnapshotCleanupProgress> {
        Ok(SnapshotCleanupProgress {
            removed_snapshots: 0,
            removed_chunks: 0,
            removed_bytes: 0,
            remaining: false,
        })
    }

    async fn list_snapshot_uploads(
        &self,
        _: Option<SnapshotId>,
        _: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage> {
        Ok(SnapshotUploadPage {
            entries: Vec::new(),
            next_after: None,
            complete: true,
        })
    }

    async fn list_snapshots(
        &self,
        _: &StreamKey,
        _: Option<SnapshotContinuation>,
        _: PageLimits,
    ) -> SnapshotResult<SnapshotPage> {
        Ok(SnapshotPage {
            entries: Vec::new(),
            next_after: None,
            complete: true,
        })
    }

    async fn acquire_recovery(&self, id: SnapshotId, _: Duration) -> SnapshotResult<RecoveryPlan> {
        Err(SnapshotError::NotFound { id })
    }

    async fn read_snapshot_chunk(
        &self,
        lease: RecoveryLeaseId,
        _: u64,
        _: usize,
    ) -> SnapshotResult<SnapshotBytePage> {
        Err(SnapshotError::ExpiredProtection { lease })
    }

    async fn read_recovery_page(
        &self,
        lease: RecoveryLeaseId,
        _: u64,
        _: PageLimits,
    ) -> SnapshotResult<Page> {
        Err(SnapshotError::ExpiredProtection { lease })
    }

    async fn release_recovery(&self, lease: RecoveryLeaseId) -> SnapshotResult<RecoveryRelease> {
        Err(SnapshotError::ExpiredProtection { lease })
    }
}

fn descriptor() -> SnapshotDescriptor {
    SnapshotDescriptor {
        id: SnapshotId::from_bytes([9; 16]),
        covered: Cursor::new(
            StreamKey {
                id: StreamId::new("snapshots").unwrap(),
                incarnation: IncarnationId([7; 16]),
            },
            0,
        ),
        schema: SchemaRef {
            id: SchemaId::new("state.v1").unwrap(),
            version: 1,
        },
        content_bytes: 0,
        digest: SnapshotDigest::from_bytes([0; 32]),
    }
}

fn progress(
    id: SnapshotId,
    state: SnapshotUploadState,
    verified_bytes: u64,
) -> SnapshotUploadProgress {
    let mut descriptor = descriptor();
    descriptor.id = id;
    descriptor.content_bytes = 1;
    SnapshotUploadProgress {
        descriptor,
        accepted_bytes: 1,
        verified_bytes,
        state,
    }
}

#[tokio::test]
async fn cancelled_accepted_snapshot_remains_owned_through_shutdown() {
    let control = SnapshotControl::paused();
    let runtime = Runtime::<PausedSnapshotStore>::open(control.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let caller = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.begin_snapshot(descriptor()).await })
    };
    control.wait_for_entries(1).await;
    caller.abort();

    let mut shutdown = Box::pin(runtime.shutdown(Duration::from_secs(1)));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    control.release.add_permits(1);
    assert!(shutdown.await.unwrap().closed);
    assert_eq!(control.completed.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn shutdown_wakes_snapshot_operation_waiting_for_capacity() {
    let control = SnapshotControl::paused();
    let mut config = RuntimeConfig::default();
    config.snapshots.max_concurrent = 1;
    let runtime = Runtime::<PausedSnapshotStore>::open(control.clone(), config)
        .await
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.begin_snapshot(descriptor()).await })
    };
    control.wait_for_entries(1).await;

    let waiting = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.begin_snapshot(descriptor()).await })
    };
    tokio::task::yield_now().await;
    assert!(!waiting.is_finished());
    let shutdown = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown(Duration::from_secs(1)).await })
    };
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(100), waiting)
            .await
            .expect("shutdown wakes capacity wait")
            .unwrap()
            .unwrap_err(),
        SnapshotError::Closed
    );
    assert_eq!(control.entered.load(Ordering::Acquire), 1);
    control.release.add_permits(1);
    first.await.unwrap().unwrap();
    assert!(shutdown.await.unwrap().unwrap().closed);
}

#[tokio::test]
async fn snapshot_panic_returns_exact_unknown_identity_without_faulting_executor() {
    let control = SnapshotControl::panicking();
    let runtime = Runtime::<PausedSnapshotStore>::open(control, RuntimeConfig::default())
        .await
        .unwrap();
    let expected = descriptor();
    let error = runtime.begin_snapshot(expected.clone()).await.unwrap_err();
    assert_eq!(
        error,
        SnapshotError::BeginUnknown {
            descriptor: Box::new(expected)
        }
    );
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn snapshot_admission_rejects_excess_retained_bytes_before_store_entry() {
    let control = SnapshotControl::paused();
    let mut config = RuntimeConfig::default();
    config.snapshots.max_waiter_bytes = 128;
    config.snapshots.max_in_flight_chunk_bytes = 128;
    let runtime = Runtime::<PausedSnapshotStore>::open(control.clone(), config)
        .await
        .unwrap();
    let chunk = SnapshotChunk {
        offset: 0,
        bytes: Payload::copy_from_slice(&[1; 129]),
    };
    assert_eq!(
        runtime
            .put_snapshot_chunk(SnapshotId::from_bytes([3; 16]), chunk)
            .await
            .unwrap_err(),
        SnapshotError::CapacityExceeded
    );
    assert_eq!(control.entered.load(Ordering::Acquire), 0);
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn snapshot_waiter_count_is_bounded_while_one_operation_is_paused() {
    let control = SnapshotControl::paused();
    let mut config = RuntimeConfig::default();
    config.snapshots.max_concurrent = 1;
    config.snapshots.max_waiters = 1;
    config.snapshots.admission_timeout = Duration::from_secs(1);
    let runtime = Runtime::<PausedSnapshotStore>::open(control.clone(), config)
        .await
        .unwrap();

    let first = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.begin_snapshot(descriptor()).await })
    };
    control.wait_for_entries(1).await;

    let mut callers = Vec::new();
    for index in 0..8u8 {
        let runtime = runtime.clone();
        callers.push(tokio::spawn(async move {
            let mut value = descriptor();
            value.id = SnapshotId::from_bytes([index.saturating_add(20); 16]);
            runtime.begin_snapshot(value).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if callers.iter().filter(|caller| caller.is_finished()).count() == 7 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    control.release.add_permits(2);
    first.await.unwrap().unwrap();
    let mut inserted = 0;
    let mut overloaded = 0;
    for caller in callers {
        match caller.await.unwrap() {
            Ok(_) => inserted += 1,
            Err(SnapshotError::Overloaded) => overloaded += 1,
            other => panic!("unexpected admission outcome: {other:?}"),
        }
    }
    assert_eq!((inserted, overloaded), (1, 7));
    assert_eq!(control.completed.load(Ordering::Acquire), 2);
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn snapshot_admission_rejects_platform_maxima_before_opening_store() {
    let control = SnapshotControl::paused();
    let mut excessive_count = RuntimeConfig::default();
    excessive_count.snapshots.max_concurrent = Semaphore::MAX_PERMITS + 1;
    assert!(matches!(
        Runtime::<PausedSnapshotStore>::open(control.clone(), excessive_count).await,
        Err(Error::InvalidConfig(_))
    ));
    assert_eq!(control.entered.load(Ordering::Acquire), 0);

    let mut excessive_deadline = RuntimeConfig::default();
    excessive_deadline.snapshots.admission_timeout = Duration::MAX;
    assert!(matches!(
        Runtime::<PausedSnapshotStore>::open(control, excessive_deadline).await,
        Err(Error::InvalidConfig(_))
    ));
}

#[tokio::test]
async fn malformed_verification_progress_never_reaches_publication_or_loops() {
    for behavior in [SnapshotBehavior::BadTerminal, SnapshotBehavior::NoProgress] {
        let control = SnapshotControl::malformed(behavior);
        let runtime =
            Runtime::<PausedSnapshotStore>::open(control.clone(), RuntimeConfig::default())
                .await
                .unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            runtime.verify_and_publish_snapshot(
                SnapshotId::from_bytes([9; 16]),
                VerificationLimits {
                    max_chunks: 1,
                    max_bytes: 1,
                },
            ),
        )
        .await
        .expect("malformed progress must terminate")
        .unwrap_err();
        assert!(matches!(result, SnapshotError::CorruptStorage(_)));
        assert_eq!(control.published.load(Ordering::Acquire), 0);
        assert!(
            runtime
                .shutdown(Duration::from_secs(1))
                .await
                .unwrap()
                .closed
        );
    }
}

#[tokio::test]
async fn verification_reconciles_concurrent_publish_and_abort_winners() {
    let published = SnapshotControl::malformed(SnapshotBehavior::PublishedOnStep);
    let runtime = Runtime::<PausedSnapshotStore>::open(published.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let descriptor = runtime
        .verify_and_publish_snapshot(
            SnapshotId::from_bytes([9; 16]),
            VerificationLimits {
                max_chunks: 1,
                max_bytes: 1,
            },
        )
        .await
        .unwrap();
    assert_eq!(descriptor.id, SnapshotId::from_bytes([9; 16]));
    assert_eq!(published.published.load(Ordering::Acquire), 0);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();

    let aborted = SnapshotControl::malformed(SnapshotBehavior::AbortedOnStep);
    let runtime = Runtime::<PausedSnapshotStore>::open(aborted, RuntimeConfig::default())
        .await
        .unwrap();
    assert_eq!(
        runtime
            .verify_and_publish_snapshot(
                SnapshotId::from_bytes([9; 16]),
                VerificationLimits {
                    max_chunks: 1,
                    max_bytes: 1,
                },
            )
            .await
            .unwrap_err(),
        SnapshotError::IncompleteUpload {
            id: SnapshotId::from_bytes([9; 16])
        }
    );
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}
