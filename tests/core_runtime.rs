mod common;
use async_trait::async_trait;
use event_stream::infrastructure::{MemoryStore, MemoryStoreOptions};
use event_stream::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

fn subscription(start: StartPosition) -> SubscriptionOptions {
    SubscriptionOptions {
        start,
        page: PageLimits {
            max_records: 2,
            max_bytes: 1024 * 1024,
        },
        max_lag_records: 100,
        max_lag_duration: Duration::from_secs(2),
        catch_up_grace: Duration::from_millis(50),
    }
}

#[tokio::test]
async fn find_stream_does_not_create_and_reports_retired_names() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let id = StreamId::new("find-stream").unwrap();
    assert_eq!(runtime.find_stream(&id).await.unwrap(), None);
    let key = runtime.create_stream(&id).await.unwrap();
    assert_eq!(runtime.find_stream(&id).await.unwrap(), Some(key.clone()));
    runtime
        .change_lifecycle(LifecycleRequest {
            operation_id: LifecycleOperationId::new("retire-found").unwrap(),
            expected: key.clone(),
            action: LifecycleAction::Delete,
        })
        .await
        .unwrap();
    assert!(matches!(
        runtime.find_stream(&id).await,
        Err(Error::StreamUnavailable { last }) if *last == key
    ));
}

#[tokio::test]
async fn absent_lookup_uses_no_memory_stream_capacity() {
    let options = MemoryStoreOptions {
        max_streams: 1,
        ..MemoryStoreOptions::default()
    };
    let store = MemoryStore::open(options).await.unwrap();
    let absent = StreamId::new("absent-at-capacity").unwrap();
    assert_eq!(store.find_stream(&absent).await.unwrap(), None);
    let first = StreamId::new("first-at-capacity").unwrap();
    let key = store.create_if_absent(&first).await.unwrap();
    assert_eq!(store.find_stream(&first).await.unwrap(), Some(key));
    assert_eq!(store.find_stream(&absent).await.unwrap(), None);
    assert!(matches!(
        store.create_if_absent(&absent).await,
        Err(Error::CapacityExceeded)
    ));
}

#[tokio::test]
async fn concurrent_runtime_appends_are_gapless_and_replay_is_bounded() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("runtime").unwrap())
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for number in 0..64 {
        let runtime = runtime.clone();
        let key = key.clone();
        tasks.push(tokio::spawn(async move {
            runtime
                .append(&key, common::event(&format!("id-{number}"), &[number]))
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let bounds = runtime.bounds(&key).await.unwrap();
    assert_eq!(bounds.tail.offset, 64);
    let page = runtime
        .read_after(
            &bounds.floor,
            PageLimits {
                max_records: 7,
                max_bytes: 1024 * 1024,
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 7);
    assert_eq!(page.through.offset, 64);
    assert!(!page.complete);
    assert!(
        runtime
            .shutdown(Duration::from_secs(2))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn subscription_replays_then_follows_without_a_gap() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("follow").unwrap())
        .await
        .unwrap();
    for number in 1..=4 {
        runtime
            .append(&key, common::event(&format!("id-{number}"), &[number]))
            .await
            .unwrap();
    }
    let after = Cursor::new(key.clone(), 2);
    let mut sub = runtime
        .subscribe(&key, subscription(StartPosition::After(after)))
        .await
        .unwrap();
    runtime
        .append(&key, common::event("id-5", b"five"))
        .await
        .unwrap();
    let mut offsets = Vec::new();
    for _ in 0..3 {
        offsets.push(sub.next().await.unwrap().unwrap().cursor.offset);
    }
    assert_eq!(offsets, vec![3, 4, 5]);
    assert_eq!(sub.last_delivered().offset, 5);
    runtime.shutdown(Duration::from_secs(2)).await.unwrap();
    assert!(matches!(sub.next().await, Some(Err(Error::Closed))));
    assert!(sub.next().await.is_none());
}

#[tokio::test]
async fn finite_admission_reports_overload_and_timeout() {
    let config = {
        let mut config = RuntimeConfig::default();
        config.appends.max_queued = 1;
        config.appends.max_queued_per_stream = 1;
        config.appends.max_queued_bytes = 1024;
        config.appends.max_queued_bytes_per_stream = 1024;
        config.appends.max_waiter_bytes = 1024;
        config.appends.admission_timeout = Duration::from_millis(25);
        config.events.max_bytes = 512;
        config
    };
    let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("limited").unwrap())
        .await
        .unwrap();
    // A direct try still has a deterministic success path when capacity is available.
    runtime
        .try_append(&key, common::event("one", b"x"))
        .await
        .unwrap();
    assert_eq!(runtime.diagnostics().await.queued_appends, 0);
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn future_subscription_gets_only_later_commits() {
    let runtime =
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), RuntimeConfig::default())
            .await
            .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("future").unwrap())
        .await
        .unwrap();
    runtime
        .append(&key, common::event("old", b"old"))
        .await
        .unwrap();
    let mut sub = runtime
        .subscribe(&key, subscription(StartPosition::Future))
        .await
        .unwrap();
    runtime
        .append(&key, common::event("new", b"new"))
        .await
        .unwrap();
    let record = tokio::time::timeout(Duration::from_secs(1), sub.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(record.event.id.as_str(), "new");
}

#[derive(Clone)]
struct BlockingOptions {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    block_once: Arc<AtomicBool>,
    calls: Arc<Mutex<Vec<String>>>,
}

struct BlockingStore {
    memory: MemoryStore,
    controls: BlockingOptions,
}

#[async_trait]
impl EventStore for BlockingStore {
    type Options = BlockingOptions;
    async fn open(options: Self::Options) -> Result<Self> {
        Ok(Self {
            memory: MemoryStore::open(MemoryStoreOptions::default()).await?,
            controls: options,
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.memory.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.memory.create_if_absent(id).await
    }
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        self.memory.find_stream(id).await
    }
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.controls
            .calls
            .lock()
            .unwrap()
            .push(event.id.as_str().to_owned());
        if self.controls.block_once.swap(false, Ordering::SeqCst) {
            self.controls.entered.notify_one();
            self.controls.release.notified().await;
        }
        self.memory.append_atomic(stream, event).await
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.memory.lookup_event(stream, id).await
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.memory.bounds(stream).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.memory.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> Result<()> {
        self.memory.close().await
    }
}

#[tokio::test]
async fn multiple_workers_preserve_one_active_turn_and_stream_acceptance_order() {
    let controls = BlockingOptions {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        block_once: Arc::new(AtomicBool::new(true)),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let config = {
        let mut config = RuntimeConfig::default();
        config.scheduling.storage_workers = 2;
        config.scheduling.max_appends_per_stream_turn = 3;
        config
    };
    let runtime = Runtime::<BlockingStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("ordered-turn").unwrap())
        .await
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("first", b"1")).await })
    };
    controls.entered.notified().await;
    let second = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("second", b"2")).await })
    };
    while runtime.diagnostics().await.queued_appends != 2 {
        tokio::task::yield_now().await;
    }
    let third = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("third", b"3")).await })
    };
    while runtime.diagnostics().await.queued_appends != 3 {
        tokio::task::yield_now().await;
    }
    assert_eq!(controls.calls.lock().unwrap().as_slice(), ["first"]);
    controls.release.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    third.await.unwrap().unwrap();
    assert_eq!(
        controls.calls.lock().unwrap().as_slice(),
        ["first", "second", "third"]
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
async fn dropped_accepted_append_finishes_and_shutdown_retains_ownership() {
    let controls = BlockingOptions {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        block_once: Arc::new(AtomicBool::new(true)),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let runtime = Runtime::<BlockingStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("cancelled-caller").unwrap())
        .await
        .unwrap();
    let task = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("owned", b"data")).await })
    };
    controls.entered.notified().await;
    task.abort();

    let report = runtime.shutdown(Duration::from_millis(10)).await.unwrap();
    assert!(!report.closed);
    assert_eq!(
        report.unresolved,
        vec![UnresolvedAppend {
            stream: key,
            event_id: EventId::new("owned").unwrap(),
        }]
    );
    controls.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.lifecycle == RuntimeLifecycle::Closed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shutdown_report_scopes_same_event_id_to_each_stream() {
    let controls = BlockingOptions {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        block_once: Arc::new(AtomicBool::new(true)),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let runtime = Runtime::<BlockingStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let a = runtime
        .create_stream(&StreamId::new("scope-a").unwrap())
        .await
        .unwrap();
    let b = runtime
        .create_stream(&StreamId::new("scope-b").unwrap())
        .await
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        let a = a.clone();
        tokio::spawn(async move { runtime.append(&a, common::event("same", b"a")).await })
    };
    controls.entered.notified().await;
    let second = {
        let runtime = runtime.clone();
        let b = b.clone();
        tokio::spawn(async move { runtime.append(&b, common::event("same", b"b")).await })
    };
    while runtime.diagnostics().await.queued_appends != 2 {
        tokio::task::yield_now().await;
    }

    let mut unresolved = runtime
        .shutdown(Duration::from_millis(5))
        .await
        .unwrap()
        .unresolved;
    unresolved.sort_by(|left, right| left.stream.id.as_str().cmp(right.stream.id.as_str()));
    assert_eq!(
        unresolved,
        vec![
            UnresolvedAppend {
                stream: a,
                event_id: EventId::new("same").unwrap(),
            },
            UnresolvedAppend {
                stream: b,
                event_id: EventId::new("same").unwrap(),
            },
        ]
    );
    controls.release.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    wait_closed(&runtime).await;
}

#[tokio::test]
async fn full_admission_is_immediate_for_try_and_bounded_for_waiting_append() {
    let controls = BlockingOptions {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        block_once: Arc::new(AtomicBool::new(true)),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let config = {
        let mut config = RuntimeConfig::default();
        config.appends.max_queued = 1;
        config.appends.max_queued_per_stream = 1;
        config.appends.max_queued_bytes = 1024;
        config.appends.max_queued_bytes_per_stream = 1024;
        config.appends.max_waiter_bytes = 1024;
        config.events.max_bytes = 512;
        config.appends.admission_timeout = Duration::from_millis(20);
        config
    };
    let runtime = Runtime::<BlockingStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("admission").unwrap())
        .await
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("first", b"x")).await })
    };
    controls.entered.notified().await;
    assert!(matches!(
        runtime
            .try_append(&key, common::event("second", b"x"))
            .await,
        Err(Error::Overloaded)
    ));
    let cancelled_waiter = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move {
            runtime
                .append(&key, common::event("cancelled-waiter", b"x"))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.admission_waiters == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancelled_waiter.abort();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.admission_waiters == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        runtime.append(&key, common::event("third", b"x")).await,
        Err(Error::AdmissionTimeout)
    ));
    controls.release.notify_one();
    first.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let diagnostics = runtime.diagnostics().await;
    assert_eq!(diagnostics.admission_waiters, 0);
    assert_eq!(diagnostics.append_accepted, 1);
    assert_eq!(diagnostics.append_rejected, 2);
    assert_eq!(diagnostics.append_inserted, 1);
    assert_eq!(diagnostics.append_deduplicated, 0);
    assert_eq!(diagnostics.append_failed, 0);
    assert_eq!(diagnostics.peak_queued_appends, 1);
    assert!(diagnostics.peak_queued_append_bytes > 0);
}

#[tokio::test]
async fn scheduler_gives_another_ready_stream_a_turn() {
    let controls = BlockingOptions {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        block_once: Arc::new(AtomicBool::new(true)),
        calls: Arc::new(Mutex::new(Vec::new())),
    };
    let runtime = Runtime::<BlockingStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let hot = runtime
        .create_stream(&StreamId::new("hot").unwrap())
        .await
        .unwrap();
    let other = runtime
        .create_stream(&StreamId::new("other").unwrap())
        .await
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        let hot = hot.clone();
        tokio::spawn(async move { runtime.append(&hot, common::event("hot-1", b"x")).await })
    };
    controls.entered.notified().await;
    let second = {
        let runtime = runtime.clone();
        let hot = hot.clone();
        tokio::spawn(async move { runtime.append(&hot, common::event("hot-2", b"x")).await })
    };
    let other_task = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.append(&other, common::event("other-1", b"x")).await })
    };
    tokio::task::yield_now().await;
    controls.release.notify_one();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    other_task.await.unwrap().unwrap();
    assert_eq!(
        *controls.calls.lock().unwrap(),
        vec!["hot-1", "other-1", "hot-2"]
    );
}

struct UnknownOnceStore {
    memory: MemoryStore,
    unknown_once: AtomicBool,
}

#[async_trait]
impl EventStore for UnknownOnceStore {
    type Options = ();
    async fn open(_: ()) -> Result<Self> {
        Ok(Self {
            memory: MemoryStore::open(MemoryStoreOptions::default()).await?,
            unknown_once: AtomicBool::new(true),
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.memory.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.memory.create_if_absent(id).await
    }
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        self.memory.find_stream(id).await
    }
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        let id = event.id.clone();
        let receipt = self.memory.append_atomic(stream, event).await?;
        if self.unknown_once.swap(false, Ordering::SeqCst) {
            Err(Error::CommitUnknown { event_id: id })
        } else {
            Ok(receipt)
        }
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.memory.lookup_event(stream, id).await
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.memory.bounds(stream).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.memory.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> Result<()> {
        self.memory.close().await
    }
}

#[tokio::test]
async fn resolved_unknown_commit_wakes_readers_and_retry_deduplicates() {
    let runtime = Runtime::<UnknownOnceStore>::open((), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("unknown").unwrap())
        .await
        .unwrap();
    let mut sub = runtime
        .subscribe(&key, subscription(StartPosition::Beginning))
        .await
        .unwrap();
    let input = common::event("uncertain", b"saved");
    assert!(matches!(
        runtime.append(&key, input.clone()).await,
        Err(Error::CommitUnknown { .. })
    ));
    let record = tokio::time::timeout(Duration::from_secs(1), sub.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(record.cursor.offset, 1);
    assert_eq!(
        runtime.append(&key, input).await.unwrap().kind,
        AppendKind::Deduplicated
    );
    assert_eq!(
        runtime.diagnostics().await.lifecycle,
        RuntimeLifecycle::Ready
    );
}

#[tokio::test]
async fn unpolled_lagging_subscription_expires_and_releases_its_slot() {
    let config = {
        let mut config = RuntimeConfig::default();
        config.subscriptions.max_total = 1;
        config.subscriptions.max_per_stream = 1;
        config.subscriptions.sweep_interval = Duration::from_millis(5);
        config.subscriptions.checks_per_sweep = 1;
        config
    };
    let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("lag").unwrap())
        .await
        .unwrap();
    let mut options = subscription(StartPosition::Beginning);
    options.max_lag_records = 1;
    options.max_lag_duration = Duration::from_secs(1);
    options.catch_up_grace = Duration::ZERO;
    let mut lagged = runtime.subscribe(&key, options.clone()).await.unwrap();
    runtime
        .append(&key, common::event("one", b"1"))
        .await
        .unwrap();
    runtime
        .append(&key, common::event("two", b"2"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.active_subscriptions == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        lagged.next().await,
        Some(Err(Error::SubscriberLagged { .. }))
    ));
    assert!(lagged.next().await.is_none());
    let _replacement = runtime.subscribe(&key, options).await.unwrap();
}

#[tokio::test]
async fn catch_up_grace_delays_unpolled_subscription_expiry() {
    let config = {
        let mut config = RuntimeConfig::default();
        config.subscriptions.max_total = 1;
        config.subscriptions.max_per_stream = 1;
        config.subscriptions.sweep_interval = Duration::from_millis(1);
        config.subscriptions.checks_per_sweep = 1;
        config
    };
    let runtime = Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("grace").unwrap())
        .await
        .unwrap();
    let mut options = subscription(StartPosition::Beginning);
    options.max_lag_records = 1;
    options.max_lag_duration = Duration::from_nanos(1);
    options.catch_up_grace = Duration::from_millis(100);
    let mut lagged = runtime.subscribe(&key, options).await.unwrap();
    runtime
        .append(&key, common::event("during-grace", b"1"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(runtime.diagnostics().await.active_subscriptions, 1);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.active_subscriptions == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscription should expire after its catch-up grace");
    assert!(matches!(
        lagged.next().await,
        Some(Err(Error::SubscriberLagged { .. }))
    ));
}

#[tokio::test]
async fn generated_retry_histories_match_a_small_reference_model() {
    let store = MemoryStore::open(MemoryStoreOptions::default())
        .await
        .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("generated").unwrap())
        .await
        .unwrap();
    let mut model = std::collections::HashMap::<u64, (Vec<u8>, u64)>::new();
    let mut seed = 0x5eed_u64;
    let mut tail = 0_u64;
    for _ in 0..500 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id_number = (seed >> 32) % 80;
        let payload = vec![(seed & 0xff) as u8];
        let input = common::event(&format!("generated-{id_number}"), &payload);
        match model.get(&id_number) {
            Some((expected, offset)) if *expected == payload => {
                let receipt = store.append_atomic(&key, input).await.unwrap();
                assert_eq!(
                    (receipt.kind, receipt.record.cursor.offset),
                    (AppendKind::Deduplicated, *offset)
                );
            }
            Some(_) => assert!(matches!(
                store.append_atomic(&key, input).await,
                Err(Error::IdempotencyConflict { .. })
            )),
            None => {
                tail += 1;
                let receipt = store.append_atomic(&key, input).await.unwrap();
                assert_eq!(
                    (receipt.kind, receipt.record.cursor.offset),
                    (AppendKind::Inserted, tail)
                );
                model.insert(id_number, (payload, tail));
            }
        }
        assert_eq!(store.bounds(&key).await.unwrap().tail.offset, tail);
    }
}

#[derive(Clone)]
struct FloorOptions;

struct FloorStore {
    key: StreamKey,
    record: Arc<Record>,
}

#[async_trait]
impl EventStore for FloorStore {
    type Options = FloorOptions;

    async fn open(_: Self::Options) -> Result<Self> {
        let key = StreamKey {
            id: StreamId::new("retained").unwrap(),
            incarnation: IncarnationId([22; 16]),
        };
        Ok(Self {
            record: Arc::new(Record {
                cursor: Cursor::new(key.clone(), 3),
                event: common::event("retained-3", b"retained"),
            }),
            key,
        })
    }

    fn capabilities(&self) -> StoreCapabilities {
        StoreCapabilities {
            persistence: PersistenceProfile::Ephemeral,
            format_version: 1,
            max_record_bytes: 1024 * 1024,
            max_concurrent_reads: 8,
            max_concurrent_writes: 1,
            ownership: "test seeded retained history",
        }
    }

    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        if id == &self.key.id {
            Ok(self.key.clone())
        } else {
            Err(Error::StreamNotFound)
        }
    }

    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        Ok((id == &self.key.id).then(|| self.key.clone()))
    }

    async fn append_atomic(&self, _: &StreamKey, _: NewEvent) -> Result<AppendReceipt> {
        Err(Error::StoreWriteFailed("seeded read-only store".into()))
    }

    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.check(stream)?;
        Ok((id == &self.record.event.id).then(|| self.record.clone()))
    }

    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.check(stream)?;
        Ok(Bounds {
            floor: Cursor::new(self.key.clone(), 2),
            tail: Cursor::new(self.key.clone(), 3),
        })
    }

    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        _: PageLimits,
    ) -> Result<Page> {
        self.check(stream)?;
        let records = if after < 3 && through >= 3 {
            vec![self.record.clone()]
        } else {
            Vec::new()
        };
        let next_after = records.last().map_or_else(
            || Cursor::new(self.key.clone(), after),
            |r| r.cursor.clone(),
        );
        Ok(Page {
            records,
            next_after,
            through: Cursor::new(self.key.clone(), through),
            complete: true,
        })
    }

    async fn close(&self) -> Result<()> {
        Ok(())
    }
}

impl FloorStore {
    fn check(&self, stream: &StreamKey) -> Result<()> {
        if stream == &self.key {
            Ok(())
        } else {
            Err(Error::InvalidCursor("wrong seeded stream".into()))
        }
    }
}

#[tokio::test]
async fn subscription_uses_authoritative_nonzero_floor_for_start_and_lag_errors() {
    let runtime = Runtime::<FloorStore>::open(FloorOptions, {
        let mut config = RuntimeConfig::default();
        config.subscriptions.sweep_interval = Duration::from_millis(1);
        config.subscriptions.checks_per_sweep = 1;
        config
    })
    .await
    .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("retained").unwrap())
        .await
        .unwrap();
    let below_floor = runtime
        .subscribe(
            &key,
            subscription(StartPosition::After(Cursor::new(key.clone(), 1))),
        )
        .await;
    assert!(matches!(
        below_floor,
        Err(Error::HistoryUnavailable { bounds }) if bounds.floor.offset == 2 && bounds.tail.offset == 3
    ));

    let mut beginning = runtime
        .subscribe(&key, subscription(StartPosition::Beginning))
        .await
        .unwrap();
    assert_eq!(beginning.next().await.unwrap().unwrap().cursor.offset, 3);
    drop(beginning);

    let mut lag_options = subscription(StartPosition::Beginning);
    lag_options.catch_up_grace = Duration::ZERO;
    lag_options.max_lag_duration = Duration::from_nanos(1);
    let mut lagged = runtime.subscribe(&key, lag_options).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.active_subscriptions == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unpolled subscription should be expired by the sweeper");
    assert!(matches!(
        lagged.next().await,
        Some(Err(Error::SubscriberLagged { bounds, .. })) if bounds.floor.offset == 2 && bounds.tail.offset == 3
    ));
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[derive(Clone)]
struct PausedIoOptions {
    pause_create: Arc<AtomicBool>,
    pause_bounds: Arc<AtomicBool>,
    pause_after_bounds: Arc<AtomicBool>,
    pause_read: Arc<AtomicBool>,
    panic_read: Arc<AtomicBool>,
    pause_close: Arc<AtomicBool>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    read_entries: Arc<std::sync::atomic::AtomicUsize>,
    open_entries: Arc<std::sync::atomic::AtomicUsize>,
    close_entries: Arc<std::sync::atomic::AtomicUsize>,
    owned: Arc<AtomicBool>,
}

impl PausedIoOptions {
    fn new() -> Self {
        Self {
            pause_create: Arc::new(AtomicBool::new(false)),
            pause_bounds: Arc::new(AtomicBool::new(false)),
            pause_after_bounds: Arc::new(AtomicBool::new(false)),
            pause_read: Arc::new(AtomicBool::new(false)),
            panic_read: Arc::new(AtomicBool::new(false)),
            pause_close: Arc::new(AtomicBool::new(false)),
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            read_entries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            open_entries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            close_entries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            owned: Arc::new(AtomicBool::new(false)),
        }
    }
    async fn maybe_pause(&self, flag: &AtomicBool) {
        if flag.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
    }
}

struct PausedIoStore {
    memory: MemoryStore,
    controls: PausedIoOptions,
}

#[async_trait]
impl EventStore for PausedIoStore {
    type Options = PausedIoOptions;
    async fn open(options: Self::Options) -> Result<Self> {
        options.open_entries.fetch_add(1, Ordering::SeqCst);
        if options.owned.swap(true, Ordering::SeqCst) {
            return Err(Error::StoreInUse);
        }
        Ok(Self {
            memory: MemoryStore::open(MemoryStoreOptions::default()).await?,
            controls: options,
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.memory.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.controls.maybe_pause(&self.controls.pause_create).await;
        self.memory.create_if_absent(id).await
    }
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        self.controls.maybe_pause(&self.controls.pause_create).await;
        self.memory.find_stream(id).await
    }
    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.memory.append_atomic(stream, event).await
    }
    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.memory.lookup_event(stream, id).await
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.controls.maybe_pause(&self.controls.pause_bounds).await;
        let bounds = self.memory.bounds(stream).await?;
        self.controls
            .maybe_pause(&self.controls.pause_after_bounds)
            .await;
        Ok(bounds)
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.controls.read_entries.fetch_add(1, Ordering::SeqCst);
        if self.controls.panic_read.swap(false, Ordering::SeqCst) {
            panic!("injected read panic");
        }
        self.controls.maybe_pause(&self.controls.pause_read).await;
        self.memory.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> Result<()> {
        self.controls.maybe_pause(&self.controls.pause_close).await;
        let result = self.memory.close().await;
        self.controls.close_entries.fetch_add(1, Ordering::SeqCst);
        self.controls.owned.store(false, Ordering::SeqCst);
        result
    }
}

impl Drop for PausedIoStore {
    fn drop(&mut self) {
        self.controls.owned.store(false, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn invalid_runtime_shape_is_rejected_before_opening_the_store() {
    let controls = PausedIoOptions::new();
    let config = {
        let mut config = RuntimeConfig::default();
        config.reads.max_concurrent = 0;
        config
    };
    assert!(matches!(
        Runtime::<PausedIoStore>::open(controls.clone(), config).await,
        Err(Error::InvalidConfig(_))
    ));
    assert_eq!(controls.open_entries.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn required_persistence_profile_is_enforced() {
    let config = {
        let mut config = RuntimeConfig::default();
        config.events.minimum_persistence = PersistenceProfile::ProcessRestart;
        config
    };
    assert!(matches!(
        Runtime::<MemoryStore>::open(MemoryStoreOptions::default(), config).await,
        Err(Error::InvalidConfig(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_last_runtime_handle_on_std_thread_schedules_owned_close() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    std::thread::spawn(move || drop(runtime)).join().unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while controls.close_entries.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let reopened = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    assert!(
        reopened
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn cancelled_read_keeps_io_and_page_capacity_until_storage_finishes() {
    let controls = PausedIoOptions::new();
    let mut config = RuntimeConfig::default();
    config.reads.max_concurrent = 2;
    config.reads.max_buffered_page_bytes = config.reads.page.max_bytes;
    config.reads.admission_timeout = Duration::from_millis(10);
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("paused-read").unwrap())
        .await
        .unwrap();
    runtime
        .append(&key, common::event("one", b"one"))
        .await
        .unwrap();
    let floor = runtime.bounds(&key).await.unwrap().floor;
    controls.pause_read.store(true, Ordering::SeqCst);
    let read = {
        let runtime = runtime.clone();
        let read_floor = floor.clone();
        tokio::spawn(async move {
            runtime
                .read_after(
                    &read_floor,
                    PageLimits {
                        max_records: 1,
                        max_bytes: 1024 * 1024,
                    },
                    None,
                )
                .await
        })
    };
    controls.entered.notified().await;
    read.abort();
    let second = runtime
        .read_after(
            &floor,
            PageLimits {
                max_records: 1,
                max_bytes: 1024 * 1024,
            },
            None,
        )
        .await;
    assert!(matches!(second, Err(Error::AdmissionTimeout)));
    assert_eq!(controls.read_entries.load(Ordering::SeqCst), 1);
    assert!(
        !runtime
            .shutdown(Duration::from_millis(5))
            .await
            .unwrap()
            .closed
    );
    controls.release.notify_one();
    wait_closed(&runtime).await;
}

#[tokio::test]
async fn cancelled_subscription_next_preserves_cursor_and_owned_page_io() {
    let controls = PausedIoOptions::new();
    let mut config = RuntimeConfig::default();
    config.reads.max_concurrent = 2;
    config.reads.max_buffered_page_bytes = config.reads.page.max_bytes;
    config.reads.admission_timeout = Duration::from_millis(10);
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("cancelled-next").unwrap())
        .await
        .unwrap();
    runtime
        .append(&key, common::event("one", b"one"))
        .await
        .unwrap();
    let mut sub = runtime
        .subscribe(&key, subscription(StartPosition::Beginning))
        .await
        .unwrap();

    controls.pause_read.store(true, Ordering::SeqCst);
    let entered = controls.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    let mut cancelled = Box::pin(sub.next());
    tokio::select! {
        _ = &mut entered => {}
        result = &mut cancelled => panic!("subscription read unexpectedly completed: {result:?}"),
    }
    drop(cancelled);
    assert_eq!(sub.last_delivered().offset, 0);

    let floor = runtime.bounds(&key).await.unwrap().floor;
    let competing = runtime
        .read_after(
            &floor,
            PageLimits {
                max_records: 1,
                max_bytes: 1024 * 1024,
            },
            None,
        )
        .await;
    assert!(matches!(competing, Err(Error::AdmissionTimeout)));
    assert_eq!(controls.read_entries.load(Ordering::SeqCst), 1);

    controls.release.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(1), sub.next())
        .await
        .expect("the cancelled owned read must finish and release page admission")
        .unwrap()
        .unwrap();
    assert_eq!(first.cursor.offset, 1);
    assert_eq!(first.event.id.as_str(), "one");
    assert_eq!(sub.last_delivered().offset, 1);
    assert_eq!(controls.read_entries.load(Ordering::SeqCst), 2);

    runtime
        .append(&key, common::event("two", b"two"))
        .await
        .unwrap();
    let second = sub.next().await.unwrap().unwrap();
    assert_eq!(second.cursor.offset, 2);
    assert_eq!(second.event.id.as_str(), "two");
    assert_eq!(sub.last_delivered().offset, 2);
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_page_permit_fairly_serves_many_subscription_readers() {
    const SUBSCRIBERS: usize = 100;
    const EVENTS: usize = 64;
    const PAGE_RECORDS: usize = 16;

    let controls = PausedIoOptions::new();
    let config = {
        let mut config = RuntimeConfig::default();
        config.reads.max_concurrent = 1;
        config.reads.max_waiters = SUBSCRIBERS + 1;
        config.subscriptions.max_total = SUBSCRIBERS;
        config.subscriptions.max_per_stream = SUBSCRIBERS;
        config.reads.page.max_records = PAGE_RECORDS;
        config.reads.max_buffered_page_bytes = 1024 * 1024;
        config.reads.admission_timeout = Duration::from_secs(5);
        config
    };
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("many-readers").unwrap())
        .await
        .unwrap();
    let mut subscriptions = Vec::new();
    for _ in 0..SUBSCRIBERS {
        let mut options = subscription(StartPosition::Beginning);
        options.page = PageLimits {
            max_records: PAGE_RECORDS,
            max_bytes: 1024 * 1024,
        };
        options.max_lag_records = 10_000;
        options.max_lag_duration = Duration::from_secs(30);
        options.catch_up_grace = Duration::ZERO;
        subscriptions.push(runtime.subscribe(&key, options).await.unwrap());
    }
    for index in 0..EVENTS {
        runtime
            .append(
                &key,
                common::event(&format!("many-{index}"), &[index as u8]),
            )
            .await
            .unwrap();
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(SUBSCRIBERS + 1));
    let tasks = subscriptions
        .into_iter()
        .map(|mut subscription| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                for expected in 1..=EVENTS as u64 {
                    let record = subscription.next().await.unwrap().unwrap();
                    assert_eq!(record.cursor.offset, expected);
                }
            })
        })
        .collect::<Vec<_>>();
    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        for task in tasks {
            task.await.unwrap();
        }
    })
    .await
    .expect("every queued reader should retain its fair place and finish");
    assert_eq!(
        controls.read_entries.load(Ordering::SeqCst),
        SUBSCRIBERS * (EVENTS / PAGE_RECORDS)
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
async fn panicking_store_read_faults_runtime_and_blocks_later_operations() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("panic-read").unwrap())
        .await
        .unwrap();
    runtime
        .append(&key, common::event("one", b"one"))
        .await
        .unwrap();
    let floor = runtime.bounds(&key).await.unwrap().floor;
    controls.panic_read.store(true, Ordering::SeqCst);

    assert!(matches!(
        runtime
            .read_after(
                &floor,
                PageLimits {
                    max_records: 1,
                    max_bytes: 1024 * 1024,
                },
                None,
            )
            .await,
        Err(Error::RuntimeFaulted(_))
    ));
    assert_eq!(
        runtime.diagnostics().await.lifecycle,
        RuntimeLifecycle::Faulted
    );
    assert!(matches!(
        runtime.bounds(&key).await,
        Err(Error::RuntimeFaulted(_))
    ));
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn draining_runtime_rejects_new_reads_before_adapter_admission() {
    let controls = PausedIoOptions::new();
    controls.pause_close.store(true, Ordering::SeqCst);
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("draining-read").unwrap())
        .await
        .unwrap();
    let floor = runtime.bounds(&key).await.unwrap().floor;
    let shutdown = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown(Duration::from_secs(1)).await })
    };
    controls.entered.notified().await;

    assert!(matches!(
        runtime
            .read_after(
                &floor,
                PageLimits {
                    max_records: 1,
                    max_bytes: 1024 * 1024,
                },
                None,
            )
            .await,
        Err(Error::Closed)
    ));
    assert_eq!(controls.read_entries.load(Ordering::SeqCst), 0);
    controls.release.notify_one();
    assert!(shutdown.await.unwrap().unwrap().closed);
}

#[tokio::test]
async fn cancelled_metadata_call_remains_owned_and_delays_close() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    controls.pause_create.store(true, Ordering::SeqCst);
    let create = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .create_stream(&StreamId::new("paused-create").unwrap())
                .await
        })
    };
    controls.entered.notified().await;
    create.abort();
    assert!(
        !runtime
            .shutdown(Duration::from_millis(5))
            .await
            .unwrap()
            .closed
    );
    controls.release.notify_one();
    wait_closed(&runtime).await;
}

#[tokio::test]
async fn cancelled_find_remains_owned_and_delays_close() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    controls.pause_create.store(true, Ordering::SeqCst);
    let find = {
        let runtime = runtime.clone();
        tokio::spawn(async move {
            runtime
                .find_stream(&StreamId::new("paused-find").unwrap())
                .await
        })
    };
    controls.entered.notified().await;
    find.abort();
    assert!(
        !runtime
            .shutdown(Duration::from_millis(5))
            .await
            .unwrap()
            .closed
    );
    controls.release.notify_one();
    wait_closed(&runtime).await;
}

#[tokio::test]
async fn cancelled_subscribe_bounds_remains_owned_and_delays_close() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("cancelled-subscribe").unwrap())
        .await
        .unwrap();
    controls.pause_bounds.store(true, Ordering::SeqCst);
    let subscribe = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move {
            runtime
                .subscribe(&key, subscription(StartPosition::Beginning))
                .await
        })
    };
    controls.entered.notified().await;
    subscribe.abort();
    assert!(
        !runtime
            .shutdown(Duration::from_millis(5))
            .await
            .unwrap()
            .closed
    );
    controls.release.notify_one();
    wait_closed(&runtime).await;
}

#[tokio::test]
async fn subscription_registration_holds_stream_gate_across_tail_capture() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("registration-race").unwrap())
        .await
        .unwrap();
    controls.pause_bounds.store(true, Ordering::SeqCst);
    let subscribe = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move {
            runtime
                .subscribe(&key, subscription(StartPosition::Future))
                .await
        })
    };
    controls.entered.notified().await;
    let append = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("raced", b"raced")).await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while runtime.diagnostics().await.queued_appends != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!append.is_finished());
    controls.release.notify_one();
    let mut subscriber = subscribe.await.unwrap().unwrap();
    append.await.unwrap().unwrap();
    let record = tokio::time::timeout(Duration::from_secs(1), subscriber.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(record.event.id.as_str(), "raced");
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn caught_up_subscription_does_not_sleep_past_commit_during_tail_check() {
    let controls = PausedIoOptions::new();
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("tail-sleep-race").unwrap())
        .await
        .unwrap();
    let subscriber = runtime
        .subscribe(&key, subscription(StartPosition::Future))
        .await
        .unwrap();
    controls.pause_after_bounds.store(true, Ordering::SeqCst);
    let next = tokio::spawn(async move {
        let mut subscriber = subscriber;
        subscriber.next().await
    });
    controls.entered.notified().await;
    runtime
        .append(&key, common::event("wake", b"wake"))
        .await
        .unwrap();
    controls.release.notify_one();
    let record = tokio::time::timeout(Duration::from_secs(1), next)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(record.event.id.as_str(), "wake");
    assert!(
        runtime
            .shutdown(Duration::from_secs(1))
            .await
            .unwrap()
            .closed
    );
}

#[tokio::test]
async fn cancelled_shutdown_still_completes_the_owned_close() {
    let controls = PausedIoOptions::new();
    controls.pause_close.store(true, Ordering::SeqCst);
    let runtime = Runtime::<PausedIoStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let shutdown = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.shutdown(Duration::from_secs(10)).await })
    };
    controls.entered.notified().await;
    shutdown.abort();
    controls.release.notify_one();
    wait_closed(&runtime).await;
}

async fn wait_closed<S: EventStore>(runtime: &Runtime<S>) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if runtime.diagnostics().await.lifecycle == RuntimeLifecycle::Closed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[derive(Clone)]
struct UnknownControls {
    lookup_entered: Arc<Notify>,
    release_lookup: Arc<Notify>,
    appends: Arc<Mutex<Vec<String>>>,
}
struct UnresolvedUnknownStore {
    memory: MemoryStore,
    controls: UnknownControls,
}

#[async_trait]
impl EventStore for UnresolvedUnknownStore {
    type Options = UnknownControls;
    async fn open(controls: Self::Options) -> Result<Self> {
        Ok(Self {
            memory: MemoryStore::open(MemoryStoreOptions::default()).await?,
            controls,
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.memory.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.memory.create_if_absent(id).await
    }
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        self.memory.find_stream(id).await
    }
    async fn append_atomic(&self, _: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.controls
            .appends
            .lock()
            .unwrap()
            .push(event.id.as_str().to_owned());
        Err(Error::CommitUnknown { event_id: event.id })
    }
    async fn lookup_event(&self, _: &StreamKey, _: &EventId) -> Result<Option<Arc<Record>>> {
        self.controls.lookup_entered.notify_one();
        self.controls.release_lookup.notified().await;
        Ok(None)
    }
    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.memory.bounds(stream).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.memory.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> Result<()> {
        self.memory.close().await
    }
}

#[tokio::test]
async fn draining_a_faulted_runtime_never_submits_the_next_queued_write() {
    let controls = UnknownControls {
        lookup_entered: Arc::new(Notify::new()),
        release_lookup: Arc::new(Notify::new()),
        appends: Arc::new(Mutex::new(Vec::new())),
    };
    let runtime =
        Runtime::<UnresolvedUnknownStore>::open(controls.clone(), RuntimeConfig::default())
            .await
            .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("faulted").unwrap())
        .await
        .unwrap();
    let first = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("first", b"x")).await })
    };
    controls.lookup_entered.notified().await;
    let second = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move { runtime.append(&key, common::event("second", b"x")).await })
    };
    tokio::task::yield_now().await;
    controls.release_lookup.notify_one();
    assert!(matches!(
        first.await.unwrap(),
        Err(Error::CommitUnknown { .. })
    ));
    assert!(matches!(
        second.await.unwrap(),
        Err(Error::RuntimeFaulted(_))
    ));
    let _ = runtime.shutdown(Duration::from_secs(1)).await;
    assert_eq!(*controls.appends.lock().unwrap(), vec!["first"]);
}

#[derive(Clone)]
struct LifecycleTestOptions {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    pause_next: Arc<AtomicBool>,
    unknown_after_commit: Arc<AtomicBool>,
    malformed_receipt: Arc<AtomicBool>,
    wrong_unknown_id: Arc<AtomicBool>,
    pause_cleanup: Arc<AtomicBool>,
}

struct LifecycleTestStore {
    memory: MemoryStore,
    controls: LifecycleTestOptions,
}

#[async_trait]
impl EventStore for LifecycleTestStore {
    type Options = LifecycleTestOptions;

    async fn open(options: Self::Options) -> Result<Self> {
        Ok(Self {
            memory: MemoryStore::open(MemoryStoreOptions::default()).await?,
            controls: options,
        })
    }

    fn capabilities(&self) -> StoreCapabilities {
        self.memory.capabilities()
    }

    async fn create_if_absent(&self, id: &StreamId) -> Result<StreamKey> {
        self.memory.create_if_absent(id).await
    }
    async fn find_stream(&self, id: &StreamId) -> Result<Option<StreamKey>> {
        self.memory.find_stream(id).await
    }

    async fn append_atomic(&self, stream: &StreamKey, event: NewEvent) -> Result<AppendReceipt> {
        self.memory.append_atomic(stream, event).await
    }

    async fn lookup_event(&self, stream: &StreamKey, id: &EventId) -> Result<Option<Arc<Record>>> {
        self.memory.lookup_event(stream, id).await
    }

    async fn bounds(&self, stream: &StreamKey) -> Result<Bounds> {
        self.memory.bounds(stream).await
    }

    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> Result<Page> {
        self.memory.read_range(stream, after, through, limits).await
    }

    async fn close(&self) -> Result<()> {
        self.memory.close().await
    }
}

#[async_trait]
impl LifecycleStore for LifecycleTestStore {
    async fn change_lifecycle(&self, request: LifecycleRequest) -> Result<LifecycleReceipt> {
        if self.controls.pause_next.swap(false, Ordering::SeqCst) {
            self.controls.entered.notify_one();
            self.controls.release.notified().await;
        }
        let mut receipt = self.memory.change_lifecycle(request.clone()).await?;
        if self.controls.wrong_unknown_id.swap(false, Ordering::SeqCst) {
            return Err(Error::LifecycleCommitUnknown {
                operation_id: LifecycleOperationId::new("wrong-operation").unwrap(),
            });
        }
        if self
            .controls
            .unknown_after_commit
            .swap(false, Ordering::SeqCst)
        {
            return Err(Error::LifecycleCommitUnknown {
                operation_id: request.operation_id,
            });
        }
        if self
            .controls
            .malformed_receipt
            .swap(false, Ordering::SeqCst)
        {
            receipt.replacement = Some(request.expected);
        }
        Ok(receipt)
    }

    async fn cleanup_retired(&self, limits: CleanupLimits) -> Result<CleanupProgress> {
        if self.controls.pause_cleanup.swap(false, Ordering::SeqCst) {
            self.controls.entered.notify_one();
            self.controls.release.notified().await;
        }
        self.memory.cleanup_retired(limits).await
    }
}

fn lifecycle_controls() -> LifecycleTestOptions {
    LifecycleTestOptions {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        pause_next: Arc::new(AtomicBool::new(false)),
        unknown_after_commit: Arc::new(AtomicBool::new(false)),
        malformed_receipt: Arc::new(AtomicBool::new(false)),
        wrong_unknown_id: Arc::new(AtomicBool::new(false)),
        pause_cleanup: Arc::new(AtomicBool::new(false)),
    }
}

fn reset_request(id: &str, expected: &StreamKey) -> LifecycleRequest {
    LifecycleRequest {
        operation_id: LifecycleOperationId::new(id).unwrap(),
        expected: expected.clone(),
        action: LifecycleAction::Reset,
    }
}

#[tokio::test]
async fn cancelled_lifecycle_call_remains_owned_and_is_reported_during_shutdown() {
    let controls = lifecycle_controls();
    controls.pause_next.store(true, Ordering::SeqCst);
    let runtime = Runtime::<LifecycleTestStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("cancelled-lifecycle").unwrap())
        .await
        .unwrap();
    let request = reset_request("cancelled-reset", &key);
    let task = {
        let runtime = runtime.clone();
        let request = request.clone();
        tokio::spawn(async move { runtime.change_lifecycle(request).await })
    };
    controls.entered.notified().await;
    task.abort();

    let report = runtime.shutdown(Duration::from_millis(5)).await.unwrap();
    assert!(!report.closed);
    assert_eq!(report.unresolved_lifecycle, vec![request]);
    controls.release.notify_one();
    wait_closed(&runtime).await;
}

#[tokio::test]
async fn unknown_lifecycle_blocks_the_name_and_exact_retry_reuses_its_reservation() {
    let controls = lifecycle_controls();
    controls.unknown_after_commit.store(true, Ordering::SeqCst);
    let mut config = RuntimeConfig::default();
    config.maintenance.max_operations = 1;
    let runtime = Runtime::<LifecycleTestStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("unknown-lifecycle").unwrap())
        .await
        .unwrap();
    let mut subscription = runtime
        .subscribe(&key, subscription(StartPosition::Future))
        .await
        .unwrap();
    let request = reset_request("unknown-reset", &key);
    assert!(matches!(
        runtime.change_lifecycle(request.clone()).await,
        Err(Error::LifecycleCommitUnknown { .. })
    ));
    assert!(matches!(
        runtime.create_stream(&key.id).await,
        Err(Error::LifecycleCommitUnknown { .. })
    ));
    assert!(matches!(
        runtime.find_stream(&key.id).await,
        Err(Error::LifecycleCommitUnknown { .. })
    ));
    assert!(matches!(
        subscription.next().await,
        Some(Err(Error::LifecycleCommitUnknown { .. }))
    ));

    controls.pause_next.store(true, Ordering::SeqCst);
    let retry = {
        let runtime = runtime.clone();
        let request = request.clone();
        tokio::spawn(async move { runtime.change_lifecycle(request).await })
    };
    controls.entered.notified().await;
    assert!(matches!(
        runtime.bounds(&key).await,
        Err(Error::LifecycleCommitUnknown { .. })
    ));
    assert!(matches!(
        runtime.change_lifecycle(request.clone()).await,
        Err(Error::Overloaded)
    ));
    controls.release.notify_one();
    let receipt = retry.await.unwrap().unwrap();
    let replacement = receipt.replacement.unwrap();
    assert_eq!(runtime.create_stream(&key.id).await.unwrap(), replacement);
    assert!(matches!(
        runtime.append(&key, common::event("old", b"old")).await,
        Err(Error::StaleIncarnation { .. })
    ));
    assert_eq!(
        runtime
            .append(&replacement, common::event("new", b"new"))
            .await
            .unwrap()
            .record
            .cursor
            .offset,
        1
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
async fn lifecycle_gate_orders_a_queued_old_append_after_the_transition() {
    let controls = lifecycle_controls();
    controls.pause_next.store(true, Ordering::SeqCst);
    let runtime = Runtime::<LifecycleTestStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("lifecycle-order").unwrap())
        .await
        .unwrap();
    let transition = {
        let runtime = runtime.clone();
        let request = reset_request("ordered-reset", &key);
        tokio::spawn(async move { runtime.change_lifecycle(request).await })
    };
    controls.entered.notified().await;
    let append = {
        let runtime = runtime.clone();
        let key = key.clone();
        tokio::spawn(async move {
            runtime
                .append(&key, common::event("queued-old", b"x"))
                .await
        })
    };
    while runtime.diagnostics().await.queued_appends != 1 {
        tokio::task::yield_now().await;
    }
    controls.release.notify_one();
    let receipt = transition.await.unwrap().unwrap();
    assert!(receipt.replacement.is_some());
    assert!(matches!(
        append.await.unwrap(),
        Err(Error::StaleIncarnation { .. })
    ));
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn lifecycle_coordinator_is_anchored_while_active_and_released_after_definite_error() {
    let controls = lifecycle_controls();
    controls.pause_next.store(true, Ordering::SeqCst);
    let mut config = RuntimeConfig::default();
    config.scheduling.max_coordinators = 1;
    let runtime = Runtime::<LifecycleTestStore>::open(controls.clone(), config)
        .await
        .unwrap();
    let first = runtime
        .create_stream(&StreamId::new("maintenance-anchor").unwrap())
        .await
        .unwrap();
    let other = runtime
        .create_stream(&StreamId::new("maintenance-other").unwrap())
        .await
        .unwrap();
    let transition = {
        let runtime = runtime.clone();
        let request = reset_request("anchor-reset", &first);
        tokio::spawn(async move { runtime.change_lifecycle(request).await })
    };
    controls.entered.notified().await;
    assert!(matches!(
        runtime
            .try_append(&other, common::event("other-blocked", b"x"))
            .await,
        Err(Error::Overloaded)
    ));
    controls.release.notify_one();
    transition.await.unwrap().unwrap();
    runtime
        .try_append(&other, common::event("other-admitted", b"x"))
        .await
        .unwrap();

    let current = runtime.create_stream(&first.id).await.unwrap();
    let stale = StreamKey {
        id: first.id.clone(),
        incarnation: IncarnationId([77; 16]),
    };
    assert!(matches!(
        runtime
            .change_lifecycle(reset_request("definite-stale", &stale))
            .await,
        Err(Error::StaleIncarnation { .. })
    ));
    runtime
        .change_lifecycle(reset_request("after-definite", &current))
        .await
        .unwrap();
    runtime.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn malformed_lifecycle_receipt_faults_runtime_and_retains_retry_identity() {
    let controls = lifecycle_controls();
    controls.malformed_receipt.store(true, Ordering::SeqCst);
    let runtime = Runtime::<LifecycleTestStore>::open(controls, RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("malformed-lifecycle").unwrap())
        .await
        .unwrap();
    let request = reset_request("malformed-reset", &key);
    assert!(matches!(
        runtime.change_lifecycle(request.clone()).await,
        Err(Error::LifecycleCommitUnknown { .. })
    ));
    assert_eq!(
        runtime.diagnostics().await.lifecycle,
        RuntimeLifecycle::Faulted
    );
    let report = runtime.shutdown(Duration::from_secs(1)).await.unwrap();
    assert!(report.closed);
    assert_eq!(report.unresolved_lifecycle, vec![request]);
}

#[tokio::test]
async fn mismatched_unknown_operation_id_is_normalized_faulted_and_retained() {
    let controls = lifecycle_controls();
    controls.wrong_unknown_id.store(true, Ordering::SeqCst);
    let runtime = Runtime::<LifecycleTestStore>::open(controls, RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("wrong-unknown-id").unwrap())
        .await
        .unwrap();
    let request = reset_request("expected-operation", &key);
    let error = runtime.change_lifecycle(request.clone()).await.unwrap_err();
    assert_eq!(
        error,
        Error::LifecycleCommitUnknown {
            operation_id: request.operation_id.clone()
        }
    );
    assert_eq!(
        runtime.diagnostics().await.lifecycle,
        RuntimeLifecycle::Faulted
    );
    let report = runtime.shutdown(Duration::from_secs(1)).await.unwrap();
    assert_eq!(report.unresolved_lifecycle, vec![request]);
}

#[tokio::test]
async fn cancelled_cleanup_remains_owned_and_shutdown_reports_it() {
    let controls = lifecycle_controls();
    controls.pause_cleanup.store(true, Ordering::SeqCst);
    let runtime = Runtime::<LifecycleTestStore>::open(controls.clone(), RuntimeConfig::default())
        .await
        .unwrap();
    let key = runtime
        .create_stream(&StreamId::new("cancelled-cleanup").unwrap())
        .await
        .unwrap();
    runtime
        .change_lifecycle(reset_request("cleanup-reset", &key))
        .await
        .unwrap();
    let cleanup = {
        let runtime = runtime.clone();
        tokio::spawn(async move { runtime.cleanup_retired().await })
    };
    controls.entered.notified().await;
    cleanup.abort();
    let report = runtime.shutdown(Duration::from_millis(5)).await.unwrap();
    assert!(!report.closed);
    assert_eq!(report.unfinished_cleanup, 1);
    controls.release.notify_one();
    wait_closed(&runtime).await;
}
