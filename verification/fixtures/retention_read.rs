// Shared bounded scenario. Stream operations use the production Runtime and a real
// SQLite store. ReadGate only controls ordering at the EventStore port boundary.
use async_trait::async_trait;
use event_stream::{
    infrastructure::{SqliteOptions, SqliteStore},
    *,
};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

const RECORDS: u64 = 3;

#[derive(Debug)]
pub struct Evidence {
    pub pending_read_was_history_unavailable: bool,
    pub pending_read_floor: u64,
    pub pending_read_tail: u64,
    pub completed_offsets: Vec<u64>,
    pub completed_page_complete: bool,
    pub completed_page_through: u64,
    pub retained_payloads_after_cleanup: Vec<Vec<u8>>,
    pub removed_event_rows: usize,
    pub cleanup_remaining: bool,
}

#[derive(Clone)]
struct ReadGate {
    before_entered: Arc<Semaphore>,
    before_release: Arc<Semaphore>,
    after_entered: Arc<Semaphore>,
    after_release: Arc<Semaphore>,
}

impl ReadGate {
    fn new() -> Self {
        Self {
            before_entered: Arc::new(Semaphore::new(0)),
            before_release: Arc::new(Semaphore::new(0)),
            after_entered: Arc::new(Semaphore::new(0)),
            after_release: Arc::new(Semaphore::new(0)),
        }
    }

    async fn wait(permit: &Semaphore) {
        permit.acquire().await.unwrap().forget();
    }
}

struct GateOptions {
    sqlite: SqliteOptions,
    gate: ReadGate,
}

struct GatedSqlite {
    sqlite: SqliteStore,
    gate: ReadGate,
}

#[async_trait]
impl EventStore for GatedSqlite {
    type Options = GateOptions;

    async fn open(options: Self::Options) -> Result<Self> {
        Ok(Self {
            sqlite: SqliteStore::open(options.sqlite).await?,
            gate: options.gate,
        })
    }

    fn capabilities(&self) -> StoreCapabilities {
        self.sqlite.capabilities()
    }

    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.sqlite.create_if_absent(id).await
    }

    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.sqlite.append_atomic(stream, event).await
    }

    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.sqlite.lookup_event(stream, id).await
    }

    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.sqlite.bounds(stream).await
    }

    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        if stream.id.as_str() == "read-before-cleanup" {
            self.gate.before_entered.add_permits(1);
            ReadGate::wait(&self.gate.before_release).await;
        }
        let page = self
            .sqlite
            .read_range(stream, after, through, limits)
            .await?;
        if stream.id.as_str() == "read-after-sqlite" {
            self.gate.after_entered.add_permits(1);
            ReadGate::wait(&self.gate.after_release).await;
        }
        Ok(page)
    }

    async fn close(&self) -> Result<()> {
        self.sqlite.close().await
    }
}

#[async_trait]
impl SnapshotStore for GatedSqlite {
    async fn begin_snapshot(
        &self,
        descriptor: SnapshotDescriptor,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        self.sqlite.begin_snapshot(descriptor).await
    }

    async fn put_snapshot_chunk(
        &self,
        id: SnapshotId,
        chunk: SnapshotChunk,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        self.sqlite.put_snapshot_chunk(id, chunk).await
    }

    async fn snapshot_status(&self, id: SnapshotId) -> SnapshotResult<SnapshotUploadProgress> {
        self.sqlite.snapshot_status(id).await
    }

    async fn verify_snapshot_step(
        &self,
        id: SnapshotId,
        limits: VerificationLimits,
    ) -> SnapshotResult<SnapshotUploadProgress> {
        self.sqlite.verify_snapshot_step(id, limits).await
    }

    async fn publish_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotDescriptor> {
        self.sqlite.publish_snapshot(id).await
    }

    async fn abort_snapshot(&self, id: SnapshotId) -> SnapshotResult<SnapshotAbortReceipt> {
        self.sqlite.abort_snapshot(id).await
    }

    async fn cleanup_snapshot_staging(
        &self,
        limits: SnapshotCleanupLimits,
    ) -> SnapshotResult<SnapshotCleanupProgress> {
        self.sqlite.cleanup_snapshot_staging(limits).await
    }

    async fn list_snapshot_uploads(
        &self,
        after: Option<SnapshotId>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotUploadPage> {
        self.sqlite.list_snapshot_uploads(after, limits).await
    }

    async fn list_snapshots(
        &self,
        stream: &StreamKey,
        after: Option<SnapshotContinuation>,
        limits: PageLimits,
    ) -> SnapshotResult<SnapshotPage> {
        self.sqlite.list_snapshots(stream, after, limits).await
    }

    async fn acquire_recovery(
        &self,
        id: SnapshotId,
        lifetime: Duration,
    ) -> SnapshotResult<RecoveryPlan> {
        self.sqlite.acquire_recovery(id, lifetime).await
    }

    async fn read_snapshot_chunk(
        &self,
        lease: RecoveryLeaseId,
        offset: u64,
        max_bytes: usize,
    ) -> SnapshotResult<SnapshotBytePage> {
        self.sqlite
            .read_snapshot_chunk(lease, offset, max_bytes)
            .await
    }

    async fn read_recovery_page(
        &self,
        lease: RecoveryLeaseId,
        after: u64,
        limits: PageLimits,
    ) -> SnapshotResult<Page> {
        self.sqlite.read_recovery_page(lease, after, limits).await
    }

    async fn release_recovery(&self, lease: RecoveryLeaseId) -> SnapshotResult<RecoveryRelease> {
        self.sqlite.release_recovery(lease).await
    }
}

#[async_trait]
impl RetentionStore for GatedSqlite {
    async fn retention_status(&self, stream: &StreamKey) -> RetentionResult<RetentionStatus> {
        self.sqlite.retention_status(stream).await
    }

    async fn enable_retry_policy(
        &self,
        request: EnableRetryPolicy,
    ) -> RetentionResult<EnableRetryPolicyReceipt> {
        self.sqlite.enable_retry_policy(request).await
    }

    async fn advance_retry_generation(
        &self,
        request: AdvanceRetryGeneration,
    ) -> RetentionResult<AdvanceRetryGenerationReceipt> {
        self.sqlite.advance_retry_generation(request).await
    }

    async fn expire_retry_generations(
        &self,
        request: ExpireRetryGenerations,
    ) -> RetentionResult<ExpireRetryGenerationsReceipt> {
        self.sqlite.expire_retry_generations(request).await
    }

    async fn append_generated(
        &self,
        stream: &StreamKey,
        event: GeneratedEvent,
    ) -> RetentionResult<AppendReceipt> {
        self.sqlite.append_generated(stream, event).await
    }

    async fn lookup_generated(
        &self,
        stream: &StreamKey,
        generation: RetryGeneration,
        event_id: &EventId,
    ) -> RetentionResult<Option<Arc<Record>>> {
        self.sqlite
            .lookup_generated(stream, generation, event_id)
            .await
    }

    async fn advance_retention_floor(
        &self,
        request: AdvanceRetentionFloor,
    ) -> RetentionResult<AdvanceRetentionFloorReceipt> {
        self.sqlite.advance_retention_floor(request).await
    }

    async fn cleanup_retention(
        &self,
        limits: RetentionCleanupLimits,
    ) -> RetentionResult<RetentionCleanupProgress> {
        self.sqlite.cleanup_retention(limits).await
    }
}

fn event(stream: &str, offset: u64) -> NewEvent {
    NewEvent {
        id: EventId::new(format!("{stream}-{offset}")).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("retention-read.event").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(format!("{stream}:{offset}").as_bytes()),
    }
}

fn page_limits() -> PageLimits {
    PageLimits {
        max_records: 8,
        max_bytes: 16 * 1024,
    }
}

async fn seed(runtime: &Runtime<GatedSqlite>, name: &str) -> StreamKey {
    let stream = runtime
        .create_stream(&StreamId::new(name).unwrap())
        .await
        .unwrap();
    for offset in 1..=RECORDS {
        runtime.append(&stream, event(name, offset)).await.unwrap();
    }
    runtime
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new(format!("enable-{name}")).unwrap(),
            stream: stream.clone(),
        })
        .await
        .unwrap();
    runtime
        .advance_retry_generation(AdvanceRetryGeneration {
            operation_id: RetentionOperationId::new(format!("advance-{name}")).unwrap(),
            stream: stream.clone(),
            expected_current: RetryGeneration::FIRST,
        })
        .await
        .unwrap();
    runtime
        .expire_retry_generations(ExpireRetryGenerations {
            operation_id: RetentionOperationId::new(format!("expire-{name}")).unwrap(),
            stream: stream.clone(),
            expected_oldest: RetryGeneration::LEGACY,
            retain_from: RetryGeneration::new(2),
        })
        .await
        .unwrap();
    stream
}

async fn advance_floor(runtime: &Runtime<GatedSqlite>, stream: &StreamKey) {
    runtime
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new(format!("floor-{}", stream.id.as_str()))
                .unwrap(),
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), RECORDS),
        })
        .await
        .unwrap();
}

async fn cleanup(runtime: &Runtime<GatedSqlite>) -> RetentionCleanupProgress {
    runtime
        .cleanup_retention(RetentionCleanupLimits {
            max_event_rows: 16,
            max_retry_rows: 16,
            max_bytes: 2 * 1024 * 1024,
        })
        .await
        .unwrap()
}

pub async fn run(directory: &Path) -> Evidence {
    std::fs::create_dir(directory).unwrap();
    let gate = ReadGate::new();
    let config = RuntimeConfig {
        events: EventConfig {
            max_bytes: 4096,
            minimum_persistence: PersistenceProfile::ProcessRestart,
        },
        ..RuntimeConfig::default()
    };
    let runtime = Runtime::<GatedSqlite>::open(
        GateOptions {
            sqlite: SqliteOptions::new(directory.join("retention-read.db")),
            gate: gate.clone(),
        },
        config,
    )
    .await
    .unwrap();

    // Schedule 1: bounds are captured, then the floor and physical cleanup move
    // before SQLite begins the page read. The production adapter must reject it.
    let before = seed(&runtime, "read-before-cleanup").await;
    let pending = tokio::spawn({
        let runtime = runtime.clone();
        let after = Cursor::new(before.clone(), 0);
        let through = Cursor::new(before.clone(), RECORDS);
        async move {
            runtime
                .read_after(&after, page_limits(), Some(&through))
                .await
        }
    });
    ReadGate::wait(&gate.before_entered).await;
    advance_floor(&runtime, &before).await;
    let first_cleanup = cleanup(&runtime).await;
    gate.before_release.add_permits(1);
    let pending_result = pending.await.unwrap();
    let (pending_read_was_history_unavailable, pending_read_floor, pending_read_tail) =
        match pending_result {
            Err(Error::HistoryUnavailable { bounds }) => {
                (true, bounds.floor.offset, bounds.tail.offset)
            }
            other => panic!(
                "pending read returned neither an explicit history error nor a page: {other:?}"
            ),
        };

    // Schedule 2: SQLite builds the complete page first. Cleanup then removes the
    // rows while the port holds the owned Page. Releasing the gate must return the
    // complete page, whose Arc-backed records remain usable after cleanup.
    let after_sqlite = seed(&runtime, "read-after-sqlite").await;
    let completed = tokio::spawn({
        let runtime = runtime.clone();
        let after = Cursor::new(after_sqlite.clone(), 0);
        let through = Cursor::new(after_sqlite.clone(), RECORDS);
        async move {
            runtime
                .read_after(&after, page_limits(), Some(&through))
                .await
        }
    });
    ReadGate::wait(&gate.after_entered).await;
    advance_floor(&runtime, &after_sqlite).await;
    let second_cleanup = cleanup(&runtime).await;
    gate.after_release.add_permits(1);
    let page = completed.await.unwrap().unwrap();
    let completed_offsets = page
        .records
        .iter()
        .map(|record| record.cursor.offset)
        .collect();
    let retained_payloads_after_cleanup = page
        .records
        .iter()
        .map(|record| record.event.payload.as_bytes().to_vec())
        .collect();

    assert!(pending_read_was_history_unavailable);
    assert_eq!((pending_read_floor, pending_read_tail), (RECORDS, RECORDS));
    assert_eq!(completed_offsets, vec![1, 2, 3]);
    assert!(page.complete);
    assert_eq!(page.through.offset, RECORDS);
    assert_eq!(
        retained_payloads_after_cleanup,
        vec![
            b"read-after-sqlite:1".to_vec(),
            b"read-after-sqlite:2".to_vec(),
            b"read-after-sqlite:3".to_vec(),
        ]
    );
    assert_eq!(
        first_cleanup.removed_event_rows + second_cleanup.removed_event_rows,
        6
    );
    assert!(!first_cleanup.remaining && !second_cleanup.remaining);

    let report = runtime.shutdown(Duration::from_secs(5)).await.unwrap();
    assert!(report.closed && report.unresolved.is_empty());

    Evidence {
        pending_read_was_history_unavailable,
        pending_read_floor,
        pending_read_tail,
        completed_offsets,
        completed_page_complete: page.complete,
        completed_page_through: page.through.offset,
        retained_payloads_after_cleanup,
        removed_event_rows: first_cleanup.removed_event_rows + second_cleanup.removed_event_rows,
        cleanup_remaining: first_cleanup.remaining || second_cleanup.remaining,
    }
}
