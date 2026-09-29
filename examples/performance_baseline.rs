//! Bounded end-to-end baseline. JSON is emitted after each timed region.
#![allow(unexpected_cfgs)]

use async_trait::async_trait;
#[cfg(feature = "sqlite")]
use event_stream::infrastructure::{SqliteOptions, SqliteStore};
#[cfg(feature = "codec")]
use event_stream::ingestion::{
    CrLfPolicy, DecodedPosition, FinalLinePolicy, IngestionConfig, IngestionService, NewlineFramer,
    NewlineFramerConfig,
};
use event_stream::{
    infrastructure::{MemoryStore, MemoryStoreOptions},
    AppendKind, AppendReceipt, Bounds, Error, EventId, EventReader, EventRuntime, EventSink,
    EventStore, EventSubscription, NewEvent, Page, PageLimits, Payload, Runtime, RuntimeConfig,
    SchemaId, SchemaRef, StartPosition, StoreCapabilities, StreamId, StreamKey,
    SubscriptionOptions,
};
#[cfg(not(performance_no_alloc))]
use std::alloc::{GlobalAlloc, Layout};
#[cfg(not(performance_no_alloc))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    alloc::System,
    collections::{HashMap, VecDeque},
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const MAX_PAYLOAD_BYTES: usize = 1_048_000;
const MAX_PRODUCERS: usize = 64;
const MAX_STREAMS: usize = 100_000;
const MAX_SUBSCRIBERS: usize = 100_000;
const MAX_LIVE_SUBSCRIBERS: usize = 100;
const MAX_OFFERS: usize = 1_000_000;
const MAX_AGENT_BURST_TASKS: usize = 100_000;
const MAX_SUSTAINED_GENERATOR_TASKS: usize = 256;
const MAX_REPETITIONS: usize = 20;
const MAX_LATENCY_SAMPLES: usize = 100_000;
const WARM_PAGE_RECORDS: usize = 256;
const MAX_LARGE_HISTORY_BYTES: u64 = 64 * 1024 * 1024 * 1024;

#[cfg(not(performance_no_alloc))]
struct CountingAllocator;
#[cfg(not(performance_no_alloc))]
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(not(performance_no_alloc))]
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
#[cfg(not(performance_no_alloc))]
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

#[cfg(not(performance_no_alloc))]
#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;
#[cfg(performance_no_alloc)]
#[global_allocator]
static GLOBAL_ALLOCATOR: System = System;

// This profiling wrapper delegates every operation to the standard system
// allocator. It changes observation overhead only; production library code does
// not select a custom allocator.
#[cfg(not(performance_no_alloc))]
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }
    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, old, new_size) };
        if !replacement.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            DEALLOCATED_BYTES.fetch_add(old.size() as u64, Ordering::Relaxed);
        }
        replacement
    }
}

#[derive(Clone, Copy)]
struct AllocationSnapshot {
    count: u64,
    allocated: u64,
    deallocated: u64,
}
impl AllocationSnapshot {
    fn read() -> Self {
        #[cfg(performance_no_alloc)]
        {
            return Self {
                count: 0,
                allocated: 0,
                deallocated: 0,
            };
        }
        #[cfg(not(performance_no_alloc))]
        Self {
            count: ALLOCATIONS.load(Ordering::Relaxed),
            allocated: ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated: DEALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }
}

fn allocation_metric(value: u64) -> String {
    if cfg!(performance_no_alloc) {
        "null".into()
    } else {
        value.to_string()
    }
}

fn allocation_delta(value: i128) -> String {
    if cfg!(performance_no_alloc) {
        "null".into()
    } else {
        value.to_string()
    }
}

#[derive(Clone)]
struct Args {
    store: String,
    scenario: String,
    payload: usize,
    producers: usize,
    streams: usize,
    events: usize,
    repetitions: usize,
    large_history_bytes: Option<u64>,
    subscribers: usize,
    offer_interval_us: u64,
}

#[derive(Default)]
struct Outcome {
    offered: u64,
    generator_rejected: u64,
    store_entered: u64,
    rejected: u64,
    failed: u64,
    runtime_accepted: u64,
    runtime_rejected: u64,
    runtime_failed: u64,
    peak_queued_appends: usize,
    peak_queued_append_bytes: usize,
    inserted: u64,
    deduplicated: u64,
    replayed: u64,
    decoded_bytes: u64,
    expected_decoder_failures: u64,
    delivered: u64,
    append_tasks: usize,
    peak_generator_tasks: usize,
    subscriber_tasks: usize,
    all_outcome_latencies_ns: Vec<u64>,
    successful_receipt_latencies_ns: Vec<u64>,
    rejected_latencies_ns: Vec<u64>,
    schedule_lateness_ns: Vec<u64>,
    queue_wait_ns: Vec<u64>,
    store_service_ns: Vec<u64>,
    store_return_to_delivery_ns: Vec<u64>,
    commit_to_delivery_upper_ns: Vec<u64>,
    elapsed_ns: u128,
    storage_bytes: Option<u64>,
    cpu_user_us: Option<i64>,
    cpu_system_us: Option<i64>,
    max_rss_bytes: Option<u64>,
    baseline_rss_bytes: Option<u64>,
    startup_rss_bytes: Option<u64>,
    settled_rss_bytes: Option<u64>,
    post_shutdown_rss_bytes: Option<u64>,
    post_drop_rss_bytes: Option<u64>,
    voluntary_switches: Option<i64>,
    involuntary_switches: Option<i64>,
    minor_faults: Option<i64>,
    major_faults: Option<i64>,
    allocation_count: u64,
    allocated_bytes: u64,
    deallocated_bytes: u64,
    setup_elapsed_ns: u128,
    subscription_registration_elapsed_ns: u128,
    post_registration_rss_bytes: Option<u64>,
    post_registration_threads: Option<u64>,
    post_registration_virtual_bytes: Option<u64>,
    setup_allocation_count: u64,
    setup_allocated_bytes: u64,
    setup_deallocated_bytes: u64,
    registration_allocation_count: u64,
    registration_allocated_bytes: u64,
    registration_deallocated_bytes: u64,
    warm_preload_elapsed_ns: u128,
    warm_drain_elapsed_ns: u128,
    pre_warm_drain_rust_requested_live_bytes: i128,
    post_warm_drain_rust_requested_live_bytes: i128,
    warm_drain_rust_requested_live_delta_bytes: i128,
    post_warm_drain_rss_bytes: Option<u64>,
    baseline_threads: Option<u64>,
    startup_threads: Option<u64>,
    settled_threads: Option<u64>,
    post_shutdown_threads: Option<u64>,
    baseline_virtual_bytes: Option<u64>,
    startup_virtual_bytes: Option<u64>,
    settled_virtual_bytes: Option<u64>,
    post_shutdown_virtual_bytes: Option<u64>,
    post_drop_threads: Option<u64>,
    post_drop_virtual_bytes: Option<u64>,
    shutdown_closed: bool,
    shutdown_unresolved: usize,
    burst_ready_tasks: usize,
    burst_ready_rss_bytes: Option<u64>,
    burst_ready_virtual_bytes: Option<u64>,
    burst_ready_rust_requested_live_delta_bytes: Option<i128>,
    shutdown_elapsed_ns: u128,
    shutdown_cpu_user_us: Option<i64>,
    shutdown_cpu_system_us: Option<i64>,
    shutdown_allocation_count: u64,
    shutdown_allocated_bytes: u64,
    shutdown_deallocated_bytes: u64,
    shutdown_queued_appends: usize,
    shutdown_admission_waiters: usize,
    shutdown_active_subscriptions: usize,
    settled_active_subscriptions: usize,
    population_cycles: Vec<PopulationCycle>,
    population_verification_elapsed_ns: u128,
    population_successful_stream_coverage: usize,
    population_uncovered_streams: usize,
    population_uncovered_stream_indices: Vec<usize>,
    population_state_vector_capacity_bytes: usize,
}

#[derive(Clone, Default)]
struct PopulationCycle {
    offered: u64,
    accepted: u64,
    runtime_rejected: u64,
    generator_rejected: u64,
    failed: u64,
}

struct PopulationState {
    receipt_offsets: Vec<u64>,
    cycles: Vec<PopulationCycle>,
}

impl PopulationState {
    fn new(total_offers: usize, streams: usize) -> Self {
        let mut cycles = vec![PopulationCycle::default(); total_offers / streams];
        for cycle in &mut cycles {
            cycle.offered = streams as u64;
        }
        Self {
            receipt_offsets: vec![0; total_offers],
            cycles,
        }
    }
}

#[derive(Clone, Copy)]
enum AgentBurstDisposition {
    EventBuildFailed,
    Inserted,
    Deduplicated,
    Rejected,
    Failed,
}

#[derive(Clone, Copy)]
struct AgentBurstOutcome {
    disposition: AgentBurstDisposition,
    latency_ns: u64,
    sampled: bool,
}

#[derive(Clone)]
struct Probe {
    inner: Arc<Mutex<ProbeState>>,
}

struct ProbeState {
    origin: Instant,
    pending: HashMap<(StreamKey, EventId), VecDeque<(Instant, bool)>>,
    queue_wait_ns: Vec<u64>,
    store_service_ns: Vec<u64>,
    store_return_to_delivery_ns: Vec<u64>,
    commit_to_delivery_upper_ns: Vec<u64>,
    committed: HashMap<(StreamKey, EventId), (Instant, Instant, usize, bool)>,
    delivery_consumers: usize,
    store_entered: u64,
    sample_cap: usize,
    sample_modulus: u64,
}

struct StoreTiming {
    started: Instant,
    sampled: bool,
    key: (StreamKey, EventId),
}

impl Probe {
    fn new(total_offers: usize, sample_cap: usize, delivery_consumers: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProbeState {
                origin: Instant::now(),
                pending: HashMap::new(),
                queue_wait_ns: Vec::with_capacity(sample_cap),
                store_service_ns: Vec::with_capacity(sample_cap),
                store_return_to_delivery_ns: Vec::with_capacity(sample_cap),
                commit_to_delivery_upper_ns: Vec::with_capacity(sample_cap),
                committed: HashMap::new(),
                delivery_consumers,
                store_entered: 0,
                sample_cap,
                sample_modulus: total_offers.div_ceil(sample_cap).max(1) as u64,
            })),
        }
    }

    fn register(&self, stream: &StreamKey, id: &EventId) {
        let mut state = self.inner.lock().expect("performance probe poisoned");
        let sampled = stable_hash(stream, id) % state.sample_modulus == 0;
        state
            .pending
            .entry((stream.clone(), id.clone()))
            .or_default()
            .push_back((Instant::now(), sampled));
    }

    fn cancel(&self, stream: &StreamKey, id: &EventId) {
        let mut state = self.inner.lock().expect("performance probe poisoned");
        let key = (stream.clone(), id.clone());
        if let Some(times) = state.pending.get_mut(&key) {
            times.pop_front();
            if times.is_empty() {
                state.pending.remove(&key);
            }
        }
    }

    fn store_started(&self, stream: &StreamKey, id: &EventId) -> StoreTiming {
        let now = Instant::now();
        let mut state = self.inner.lock().expect("performance probe poisoned");
        state.store_entered += 1;
        let key = (stream.clone(), id.clone());
        let submitted = state.pending.get_mut(&key).and_then(VecDeque::pop_front);
        if state.pending.get(&key).is_some_and(VecDeque::is_empty) {
            state.pending.remove(&key);
        }
        if state.queue_wait_ns.len() < state.sample_cap {
            if let Some((submitted, true)) = submitted {
                state.queue_wait_ns.push(ns(submitted.elapsed()));
            }
        }
        StoreTiming {
            started: now,
            sampled: submitted.is_some_and(|(_, sampled)| sampled),
            key,
        }
    }

    fn store_finished(&self, timing: StoreTiming, committed: bool) {
        let finished = Instant::now();
        let mut state = self.inner.lock().expect("performance probe poisoned");
        if timing.sampled && state.store_service_ns.len() < state.sample_cap {
            state
                .store_service_ns
                .push(ns(finished.duration_since(timing.started)));
        }
        if committed && state.delivery_consumers != 0 {
            let delivery_consumers = state.delivery_consumers;
            state.committed.insert(
                timing.key,
                (timing.started, finished, delivery_consumers, timing.sampled),
            );
        }
    }

    fn delivered(&self, record: &event_stream::Record) -> bool {
        let now = Instant::now();
        let key = (record.cursor.stream.clone(), record.event.id.clone());
        let mut state = self.inner.lock().expect("performance probe poisoned");
        let (return_elapsed, upper_elapsed, sampled, remove) = {
            let Some((store_started, store_returned, remaining, sampled)) =
                state.committed.get_mut(&key)
            else {
                return false;
            };
            let return_elapsed = ns(now
                .checked_duration_since(*store_returned)
                .unwrap_or_default());
            let upper_elapsed = ns(now.duration_since(*store_started));
            *remaining -= 1;
            (return_elapsed, upper_elapsed, *sampled, *remaining == 0)
        };
        if sampled && state.store_return_to_delivery_ns.len() < state.sample_cap {
            state.store_return_to_delivery_ns.push(return_elapsed);
            state.commit_to_delivery_upper_ns.push(upper_elapsed);
        }
        if remove {
            state.committed.remove(&key);
        }
        true
    }

    fn take(&self) -> (u64, Vec<u64>, Vec<u64>, Vec<u64>, Vec<u64>, usize) {
        let mut state = self.inner.lock().expect("performance probe poisoned");
        let _probe_lifetime = state.origin.elapsed();
        let store_entered = std::mem::take(&mut state.store_entered);
        (
            store_entered,
            std::mem::take(&mut state.queue_wait_ns),
            std::mem::take(&mut state.store_service_ns),
            std::mem::take(&mut state.store_return_to_delivery_ns),
            std::mem::take(&mut state.commit_to_delivery_upper_ns),
            state.committed.len(),
        )
    }
}

struct ProbedOptions<O> {
    inner: O,
    probe: Probe,
}

struct ProbedStore<S> {
    inner: S,
    probe: Probe,
}

#[async_trait]
impl<S: EventStore> EventStore for ProbedStore<S> {
    type Options = ProbedOptions<S::Options>;

    async fn open(options: Self::Options) -> event_stream::Result<Self> {
        Ok(Self {
            inner: S::open(options.inner).await?,
            probe: options.probe,
        })
    }
    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }
    async fn create_if_absent(&self, id: &StreamId) -> event_stream::Result<StreamKey> {
        self.inner.create_if_absent(id).await
    }
    async fn find_stream(&self, id: &StreamId) -> event_stream::Result<Option<StreamKey>> {
        self.inner.find_stream(id).await
    }
    async fn append_atomic(
        &self,
        stream: &StreamKey,
        event: NewEvent,
    ) -> event_stream::Result<AppendReceipt> {
        let timing = self.probe.store_started(stream, &event.id);
        let result = self.inner.append_atomic(stream, event).await;
        self.probe.store_finished(timing, result.is_ok());
        result
    }
    async fn lookup_event(
        &self,
        stream: &StreamKey,
        id: &EventId,
    ) -> event_stream::Result<Option<Arc<event_stream::Record>>> {
        self.inner.lookup_event(stream, id).await
    }
    async fn bounds(&self, stream: &StreamKey) -> event_stream::Result<Bounds> {
        self.inner.bounds(stream).await
    }
    async fn read_range(
        &self,
        stream: &StreamKey,
        after: u64,
        through: u64,
        limits: PageLimits,
    ) -> event_stream::Result<Page> {
        self.inner.read_range(stream, after, through, limits).await
    }
    async fn close(&self) -> event_stream::Result<()> {
        self.inner.close().await
    }
}

struct TempDirectory(PathBuf);
impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    println!("{{\"kind\":\"environment\",\"source_sha256\":\"{}\",\"input_digest\":\"{}\",\"os\":\"{}\",\"arch\":\"{}\",\"system\":\"{}\",\"cpu\":\"{}\",\"logical_cpus\":{},\"rustc\":\"{}\",\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"payload_bytes\":{},\"producers\":{},\"streams\":{},\"subscribers\":{},\"events_per_producer\":{},\"offer_interval_us\":{},\"repetitions\":{},\"large_history_bytes\":{},\"latency_sample_cap\":{},\"latency_sampling\":\"systematic_fixture_index_for_callers;fnv1a_stream_event_identity_for_store_stages\",\"max_rss_scope\":\"process_lifetime_peak\",\"queue_timing_scope\":\"submission_to_EventStore_entry\",\"store_timing_scope\":\"EventStore_append_atomic\",\"schedule_lateness_scope\":\"actual_offer_start_minus_requested_monotonic_deadline\"}}",
        option_env!("PERFORMANCE_SOURCE_SHA256").unwrap_or("not-provided"), option_env!("PERFORMANCE_INPUT_DIGEST").unwrap_or("not-provided"), std::env::consts::OS, std::env::consts::ARCH,
        json_escape(&command_output("uname", &["-a"])), json_escape(&command_output("sysctl", &["-n", "machdep.cpu.brand_string"])),
        std::thread::available_parallelism().map_or(0, usize::from), json_escape(&command_output("rustc", &["--version"])),
        args.store, args.scenario, logical_agents(&args), args.payload, args.producers, args.streams, args.subscribers, args.events, args.offer_interval_us, args.repetitions,
        args.large_history_bytes.map_or("null".into(), |v| v.to_string()), MAX_LATENCY_SAMPLES);
    println!("{{\"kind\":\"config\",\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"build_profile\":\"{}\",\"instrumentation\":\"{}\",\"allocation_scope\":{},\"generator_max_tasks\":{},\"generator_semantics\":{},\"max_event_bytes\":1048576,\"max_queued_appends\":{},\"max_stream_queued_appends\":{},\"max_queued_append_bytes\":67108864,\"max_stream_queued_bytes\":67108864,\"max_page_bytes\":2097152,\"max_buffered_page_bytes\":8388608,\"max_coordinators\":{},\"max_subscriptions\":{},\"max_stream_subscriptions\":{},\"subscriber_sweep_interval_ms\":{},\"subscribers_per_sweep\":64,\"idle_observation_ms\":{},\"sqlite_engine\":{},\"sqlite_journal_mode\":{},\"sqlite_synchronous\":{},\"sqlite_cache_kib\":{}}}",
        args.store, args.scenario, logical_agents(&args),
        if cfg!(debug_assertions) { "debug" } else { "release" },
        if cfg!(performance_no_alloc) { "stage_timestamps_without_allocator_counters" } else { "Rust_System_GlobalAlloc_atomic_counters_and_stage_timestamps" },
        if cfg!(performance_no_alloc) { "null" } else { "\"Rust_GlobalAlloc_including_harness_and_Tokio_excluding_SQLite_C_allocator\"" },
        if matches!(args.scenario.as_str(), "sustained" | "population_sustained") { MAX_SUSTAINED_GENERATOR_TASKS.to_string() } else { "null".into() },
        if args.scenario == "sustained" { "\"deadline_major_synchronized_producer_offers_bounded_try_append_v1\"" } else if args.scenario == "population_sustained" { "\"deadline_major_synchronized_producer_offers_rotating_population_v1\"" } else { "null" },
        if args.scenario == "overload" { 1 } else { 1024 },
        if args.scenario == "overload" { 1 } else { 128 },
        args.streams.max(1024), args.subscribers.max(1024),
        if args.scenario == "subscription_scale" { 128 } else { args.subscribers.max(128) },
        if matches!(
            args.scenario.as_str(),
            "idle" | "idle_scale" | "subscription_scale" | "warmed_subscription_scale"
        ) { 250 } else { 10 },
        if args.scenario == "idle" { "250" } else if matches!(args.scenario.as_str(), "idle_scale" | "subscription_scale" | "warmed_subscription_scale") { "10000" } else { "null" },
        if args.store == "sqlite" { format!("\"{}\"", rusqlite_version()) } else { "null".into() },
        if args.store == "sqlite" { "\"DELETE\"" } else { "null" },
        if args.store == "sqlite" { "\"FULL\"" } else { "null" },
        if args.store == "sqlite" { "4096" } else { "null" });
    if args.scenario == "population_sustained" {
        let offers = total_offers(&args).expect("validated population offer count");
        println!(
            "{{\"kind\":\"population_config\",\"store\":\"{}\",\"scenario\":\"{}\",\"scenario_config_fingerprint\":\"population_sustained_offer_i_times_producers_plus_p_v1\",\"population_size\":{},\"population_cycles\":{},\"population_total_offers\":{},\"nominal_mean_aggregate_offer_interval_us\":{:.6},\"nominal_mean_stream_revisit_interval_us\":{:.6},\"stream_revisit_interval_formula\":\"streams_times_offer_interval_divided_by_producers\",\"receipt_ledger_vector_capacity_bytes\":{},\"cycle_counter_vector_capacity_bytes\":{},\"population_state_vector_capacity_bytes\":{},\"population_state_vector_capacity_cap_bytes\":{},\"population_state_scope\":\"vector_element_capacity_only;cursor_offsets_indexed_by_offer_plus_per_cycle_counters;excludes_wrappers_samples_and_replay_expected_vectors\"}}",
            args.store,
            args.scenario,
            args.streams,
            offers / args.streams,
            offers,
            args.offer_interval_us as f64 / args.producers as f64,
            args.streams as f64 * args.offer_interval_us as f64 / args.producers as f64,
            offers * std::mem::size_of::<u64>(),
            (offers / args.streams) * std::mem::size_of::<PopulationCycle>(),
            offers * std::mem::size_of::<u64>()
                + (offers / args.streams) * std::mem::size_of::<PopulationCycle>(),
            MAX_OFFERS
                * (std::mem::size_of::<u64>() + std::mem::size_of::<PopulationCycle>()),
        );
    }
    for repetition in 1..=args.repetitions {
        let outcome = match args.store.as_str() {
            "memory" => {
                if args.large_history_bytes.is_some() {
                    return Err("--large-history-bytes requires --store sqlite".into());
                }
                run::<MemoryStore>(
                    MemoryStoreOptions {
                        max_record_bytes: 1024 * 1024,
                        max_history_records: total_offers(&args).unwrap_or(100_000).max(100_000),
                        max_history_bytes: 256 * 1024 * 1024,
                        max_streams: args.streams,
                        max_stream_metadata_bytes: args
                            .streams
                            .saturating_mul(512)
                            .max(4 * 1024 * 1024),
                        ..MemoryStoreOptions::default()
                    },
                    &args,
                    None,
                )
                .await?
            }
            #[cfg(feature = "sqlite")]
            "sqlite" => {
                let dir = std::env::temp_dir()
                    .join(format!("event-stream-performance-{}", uuid::Uuid::new_v4()));
                std::fs::create_dir(&dir)?;
                let _cleanup = TempDirectory(dir.clone());
                let path = dir.join("events.sqlite3");
                if let Some(target) = args.large_history_bytes {
                    ensure_free_space(&dir, target)?;
                    preload_sqlite(&path, &args, target).await?;
                }
                run::<SqliteStore>(sqlite_options(&path, &args)?, &args, Some(path)).await?
            }
            _ => return Err("store must be memory or sqlite (with --features sqlite)".into()),
        };
        print_outcome(repetition, &args, outcome);
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
fn rusqlite_version() -> &'static str {
    rusqlite::version()
}
#[cfg(not(feature = "sqlite"))]
fn rusqlite_version() -> &'static str {
    "unavailable"
}

fn command_output(program: &str, args: &[&str]) -> String {
    std::process::Command::new(program)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or_else(|| "unavailable".into(), |output| output.trim().into())
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character < ' ' => {
                use std::fmt::Write;
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped
}

async fn run<S: EventStore>(
    options: S::Options,
    args: &Args,
    storage_path: Option<PathBuf>,
) -> event_stream::Result<Outcome> {
    let baseline_rss_bytes = current_rss_bytes();
    let baseline_threads = current_thread_count();
    let baseline_virtual_bytes = current_process_metric("vsz", 1024);
    let setup_allocations_before = AllocationSnapshot::read();
    let setup_started = Instant::now();
    let config = {
        let mut config = RuntimeConfig::default();
        config.appends.max_queued = if args.scenario == "overload" { 1 } else { 1024 };
        config.appends.max_queued_per_stream = if args.scenario == "overload" { 1 } else { 128 };
        config.appends.max_queued_bytes = 64 * 1024 * 1024;
        config.appends.max_queued_bytes_per_stream = 64 * 1024 * 1024;
        config.reads.page.max_bytes = 2 * 1024 * 1024;
        config.reads.max_buffered_page_bytes = 8 * 1024 * 1024;
        config.scheduling.max_coordinators = args.streams.max(1024);
        config.subscriptions.max_total = args.subscribers.max(1024);
        config.subscriptions.max_per_stream = if args.scenario == "subscription_scale" {
            128
        } else {
            args.subscribers.max(128)
        };
        config.reads.admission_timeout = Duration::from_secs(60);
        config.subscriptions.sweep_interval = if matches!(
            args.scenario.as_str(),
            "idle" | "idle_scale" | "subscription_scale" | "warmed_subscription_scale"
        ) {
            Duration::from_millis(250)
        } else {
            Duration::from_millis(10)
        };
        config
    };
    let probe = Probe::new(
        total_offers(args).unwrap_or(MAX_OFFERS),
        MAX_LATENCY_SAMPLES,
        if args.scenario == "live_delivery" {
            args.subscribers
        } else {
            0
        },
    );
    let runtime = Arc::new(
        Runtime::<ProbedStore<S>>::open(
            ProbedOptions {
                inner: options,
                probe: probe.clone(),
            },
            config,
        )
        .await?,
    );
    let mut streams = Vec::with_capacity(args.streams);
    for index in 0..args.streams {
        streams.push(
            runtime
                .create_stream(&StreamId::new(format!("stream-{index}"))?)
                .await?,
        );
    }
    let streams = Arc::new(streams);
    let warm_expected_ids = if args.scenario == "warmed_subscription_scale" {
        (0..WARM_PAGE_RECORDS)
            .map(|index| format!("warm-{index}"))
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let startup_rss_bytes = current_rss_bytes();
    let startup_threads = current_thread_count();
    let startup_virtual_bytes = current_process_metric("vsz", 1024);
    let setup_allocations_after = AllocationSnapshot::read();
    let setup_elapsed_ns = setup_started.elapsed().as_nanos();

    let warm_preload_started = Instant::now();
    if args.scenario == "warmed_subscription_scale" {
        for expected_id in &warm_expected_ids {
            runtime
                .append(&streams[0], make_event(expected_id.clone(), args.payload)?)
                .await?;
        }
        let _ = probe.take();
    }
    let warm_preload_elapsed_ns = if args.scenario == "warmed_subscription_scale" {
        warm_preload_started.elapsed().as_nanos()
    } else {
        0
    };

    if matches!(args.scenario.as_str(), "replay_append" | "replay_only")
        && args.large_history_bytes.is_none()
    {
        for index in 0..args.events {
            let event = make_event(format!("history-{index}"), args.payload)?;
            let probe_registered = args.scenario != "duplicates";
            if probe_registered {
                probe.register(&streams[0], &event.id);
            }
            runtime.append(&streams[0], event).await?;
        }
        let _ = probe.take();
    }
    let mut subscriptions = Vec::new();
    let registration_allocations_before = AllocationSnapshot::read();
    let subscription_registration_started = Instant::now();
    if matches!(
        args.scenario.as_str(),
        "stalled" | "live_delivery" | "subscription_scale" | "warmed_subscription_scale"
    ) {
        for index in 0..args.subscribers {
            subscriptions.push(
                runtime
                    .subscribe(
                        &streams[if matches!(
                            args.scenario.as_str(),
                            "subscription_scale" | "warmed_subscription_scale"
                        ) {
                            index % streams.len()
                        } else {
                            0
                        }],
                        SubscriptionOptions {
                            start: StartPosition::Beginning,
                            page: PageLimits {
                                max_records: if args.scenario == "warmed_subscription_scale" {
                                    WARM_PAGE_RECORDS
                                } else {
                                    16
                                },
                                max_bytes: 2 * 1024 * 1024,
                            },
                            max_lag_records: if args.scenario == "stalled" {
                                1
                            } else {
                                MAX_OFFERS as u64
                            },
                            max_lag_duration: if args.scenario == "stalled" {
                                Duration::from_millis(20)
                            } else if args.scenario == "warmed_subscription_scale" {
                                Duration::from_secs(300)
                            } else {
                                Duration::from_secs(30)
                            },
                            catch_up_grace: Duration::ZERO,
                        },
                    )
                    .await?,
            );
        }
    }
    let subscription_registration_elapsed_ns =
        subscription_registration_started.elapsed().as_nanos();
    let registration_allocations_after = AllocationSnapshot::read();
    let post_registration_rss_bytes = current_rss_bytes();
    let post_registration_threads = current_thread_count();
    let post_registration_virtual_bytes = current_process_metric("vsz", 1024);
    let warm_drain_allocations_before = AllocationSnapshot::read();
    let warm_drain_started = Instant::now();
    if args.scenario == "warmed_subscription_scale" {
        for subscription in &mut subscriptions {
            for (index, expected_id) in warm_expected_ids.iter().enumerate() {
                let record = tokio::time::timeout(Duration::from_secs(300), subscription.next())
                    .await
                    .map_err(|_| {
                        Error::RuntimeFaulted("warmed subscription drain timed out".into())
                    })?
                    .ok_or_else(|| {
                        Error::RuntimeFaulted("warmed subscription ended early".into())
                    })??;
                let expected_offset = index as u64 + 1;
                if record.cursor.offset != expected_offset
                    || record.cursor.stream != streams[0]
                    || record.event.id.as_str() != expected_id
                {
                    return Err(Error::RuntimeFaulted(
                        "warmed subscription returned the wrong record".into(),
                    ));
                }
            }
        }
        tokio::task::yield_now().await;
    }
    let warm_drain_elapsed_ns = if args.scenario == "warmed_subscription_scale" {
        warm_drain_started.elapsed().as_nanos()
    } else {
        0
    };
    let warm_drain_allocations_after = AllocationSnapshot::read();
    let post_warm_drain_rss_bytes = current_rss_bytes();

    // This ledger is harness-owned memory. It is allocated before the active
    // measurement and retained through verification so its fixed cost is explicit.
    let population_state = if args.scenario == "population_sustained" {
        let offers = total_offers(args).expect("validated population offer count");
        Some(Arc::new(Mutex::new(PopulationState::new(
            offers,
            args.streams,
        ))))
    } else {
        None
    };

    let diagnostics_before = runtime.diagnostics().await;
    let usage_before = Usage::read();
    let allocations_before = AllocationSnapshot::read();
    let started = Instant::now();
    let start_barrier = Arc::new(tokio::sync::Barrier::new(
        if args.scenario == "replay_append" {
            2
        } else {
            1
        },
    ));
    let replay_task = if matches!(args.scenario.as_str(), "replay_append" | "replay_only") {
        let runtime = runtime.clone();
        let stream = streams[0].clone();
        let start_barrier = start_barrier.clone();
        Some(tokio::spawn(async move {
            let through = runtime.bounds(&stream).await?.tail;
            let mut cursor = event_stream::Cursor::new(stream, 0);
            let mut count = 0_u64;
            start_barrier.wait().await;
            loop {
                let page = runtime
                    .read_after(
                        &cursor,
                        PageLimits {
                            max_records: 32,
                            max_bytes: 2 * 1024 * 1024,
                        },
                        Some(&through),
                    )
                    .await?;
                count += page.records.len() as u64;
                cursor = page.next_after;
                tokio::task::yield_now().await;
                if page.complete {
                    break;
                }
            }
            Ok::<u64, Error>(count)
        }))
    } else {
        None
    };
    let delivery_tasks = if args.scenario == "live_delivery" {
        let expected = args.producers.saturating_mul(args.events);
        subscriptions
            .drain(..)
            .map(|mut subscription| {
                let probe = probe.clone();
                tokio::spawn(async move {
                    let mut delivered = 0_u64;
                    while delivered < expected as u64 {
                        let record = subscription.next().await.ok_or_else(|| {
                            Error::RuntimeFaulted("live subscription ended early".into())
                        })??;
                        let expected_offset = delivered + 1;
                        if record.cursor.offset != expected_offset {
                            return Err(Error::RuntimeFaulted(format!(
                                "live delivery expected offset {expected_offset}, got {}",
                                record.cursor.offset
                            )));
                        }
                        if !probe.delivered(&record) {
                            return Err(Error::RuntimeFaulted(
                                "delivery did not match an acknowledged store return".into(),
                            ));
                        }
                        delivered += 1;
                    }
                    Ok::<u64, Error>(delivered)
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let mut tasks = Vec::new();
    let mut burst_tasks = Vec::new();
    let mut outcome = Outcome {
        baseline_rss_bytes,
        baseline_threads,
        baseline_virtual_bytes,
        startup_threads,
        startup_virtual_bytes,
        setup_allocation_count: setup_allocations_after
            .count
            .saturating_sub(setup_allocations_before.count),
        setup_allocated_bytes: setup_allocations_after
            .allocated
            .saturating_sub(setup_allocations_before.allocated),
        setup_deallocated_bytes: setup_allocations_after
            .deallocated
            .saturating_sub(setup_allocations_before.deallocated),
        registration_allocation_count: registration_allocations_after
            .count
            .saturating_sub(registration_allocations_before.count),
        registration_allocated_bytes: registration_allocations_after
            .allocated
            .saturating_sub(registration_allocations_before.allocated),
        registration_deallocated_bytes: registration_allocations_after
            .deallocated
            .saturating_sub(registration_allocations_before.deallocated),
        warm_preload_elapsed_ns,
        warm_drain_elapsed_ns,
        pre_warm_drain_rust_requested_live_bytes: signed_delta(
            warm_drain_allocations_before.allocated,
            warm_drain_allocations_before.deallocated,
        ),
        post_warm_drain_rust_requested_live_bytes: signed_delta(
            warm_drain_allocations_after.allocated,
            warm_drain_allocations_after.deallocated,
        ),
        warm_drain_rust_requested_live_delta_bytes: signed_delta(
            warm_drain_allocations_after
                .allocated
                .saturating_sub(warm_drain_allocations_before.allocated),
            warm_drain_allocations_after
                .deallocated
                .saturating_sub(warm_drain_allocations_before.deallocated),
        ),
        post_warm_drain_rss_bytes,
        subscription_registration_elapsed_ns,
        post_registration_rss_bytes,
        post_registration_threads,
        post_registration_virtual_bytes,
        ..Outcome::default()
    };
    let burst_barrier = Arc::new(tokio::sync::Barrier::new(
        if args.scenario == "agent_burst" {
            total_offers(args).unwrap_or(0).saturating_add(1)
        } else {
            1
        },
    ));
    if matches!(
        args.scenario.as_str(),
        "decoder" | "decoder_tiny" | "decoder_malformed"
    ) {
        #[cfg(feature = "codec")]
        {
            let (committed, expected_decoder_failures) =
                run_decoder(runtime.clone(), streams[0].clone(), args, &probe)
                    .await
                    .map_err(|error| Error::RuntimeFaulted(error.to_string()))?;
            outcome.offered = committed;
            outcome.inserted = committed;
            outcome.expected_decoder_failures = expected_decoder_failures;
            outcome.decoded_bytes = committed.saturating_mul(args.payload as u64 + 1);
        }
        #[cfg(not(feature = "codec"))]
        return Err(Error::InvalidConfig(
            "decoder scenario needs the codec feature".into(),
        ));
    } else if !matches!(
        args.scenario.as_str(),
        "idle" | "idle_scale" | "replay_only" | "subscription_scale" | "warmed_subscription_scale"
    ) {
        if args.scenario == "replay_append" {
            start_barrier.wait().await;
        }
        let open_loop = args.scenario == "overload";
        if args.scenario == "population_sustained" {
            let population_runtime = runtime.clone();
            let population_streams = streams.clone();
            let population_probe = probe.clone();
            let population_state_for_tasks = population_state
                .as_ref()
                .expect("population state initialized")
                .clone();
            let population_state_for_rejections = population_state_for_tasks.clone();
            let payload_bytes = args.payload;
            let producer_count = args.producers;
            let event_count = args.events;
            let stream_count = args.streams;
            let population = run_sustained_generator_with_rejections(
                producer_count,
                event_count,
                Duration::from_micros(args.offer_interval_us),
                MAX_SUSTAINED_GENERATOR_TASKS,
                move |producer, event_index, scheduled| {
                    let runtime = population_runtime.clone();
                    let streams = population_streams.clone();
                    let probe = population_probe.clone();
                    let state = population_state_for_tasks.clone();
                    async move {
                        let mut outcome = Outcome::default();
                        let offer_started = Instant::now();
                        let offer_index =
                            population_offer_index(producer, event_index, producer_count);
                        let (stream_index, cycle) = population_mapping(offer_index, stream_count);
                        let stream = &streams[stream_index];
                        let id = population_event_id(stream_index, cycle);
                        let event = match make_event(id.clone(), payload_bytes) {
                            Ok(event) => event,
                            Err(_) => {
                                outcome.failed += 1;
                                state.lock().expect("population state poisoned").cycles[cycle]
                                    .failed += 1;
                                return outcome;
                            }
                        };
                        probe.register(stream, &event.id);
                        let sampled =
                            should_sample(producer, event_index, event_count, producer_count);
                        if sampled {
                            outcome.schedule_lateness_ns.push(ns(offer_started
                                .checked_duration_since(scheduled)
                                .unwrap_or(Duration::ZERO)));
                        }
                        let call_started = Instant::now();
                        let result = runtime.try_append(stream, event).await;
                        let elapsed = ns(call_started.elapsed());
                        if sampled {
                            outcome.all_outcome_latencies_ns.push(elapsed);
                        }
                        match result {
                            Ok(receipt) => {
                                if receipt.kind != AppendKind::Inserted
                                    || receipt.record.cursor.stream != *stream
                                {
                                    outcome.failed += 1;
                                    state.lock().expect("population state poisoned").cycles
                                        [cycle]
                                        .failed += 1;
                                    return outcome;
                                }
                                let mut state = state.lock().expect("population state poisoned");
                                state.receipt_offsets[offer_index] = receipt.record.cursor.offset;
                                state.cycles[cycle].accepted += 1;
                                drop(state);
                                if sampled {
                                    outcome.successful_receipt_latencies_ns.push(elapsed);
                                }
                                outcome.inserted += 1;
                            }
                            Err(Error::Overloaded | Error::AdmissionTimeout) => {
                                probe
                                    .cancel(stream, &EventId::new(id).expect("valid benchmark id"));
                                outcome.rejected += 1;
                                state.lock().expect("population state poisoned").cycles[cycle]
                                    .runtime_rejected += 1;
                                if sampled {
                                    outcome.rejected_latencies_ns.push(elapsed);
                                }
                            }
                            Err(_) => {
                                probe
                                    .cancel(stream, &EventId::new(id).expect("valid benchmark id"));
                                outcome.failed += 1;
                                state.lock().expect("population state poisoned").cycles[cycle]
                                    .failed += 1;
                            }
                        }
                        outcome
                    }
                },
                move |producer, event_index| {
                    let offer_index = population_offer_index(producer, event_index, producer_count);
                    let (_, cycle) = population_mapping(offer_index, stream_count);
                    population_state_for_rejections
                        .lock()
                        .expect("population state poisoned")
                        .cycles[cycle]
                        .generator_rejected += 1;
                },
            )
            .await?;
            merge(&mut outcome, population);
        } else if args.scenario == "sustained" {
            let sustained_runtime = runtime.clone();
            let sustained_streams = streams.clone();
            let sustained_probe = probe.clone();
            let payload_bytes = args.payload;
            let producer_count = args.producers;
            let event_count = args.events;
            let sustained = run_sustained_generator(
                producer_count,
                event_count,
                Duration::from_micros(args.offer_interval_us),
                MAX_SUSTAINED_GENERATOR_TASKS,
                move |producer, event_index, scheduled| {
                    let runtime = sustained_runtime.clone();
                    let stream = sustained_streams[producer % sustained_streams.len()].clone();
                    let probe = sustained_probe.clone();
                    async move {
                        let mut outcome = Outcome::default();
                        let offer_started = Instant::now();
                        let id = format!("p{producer}-{event_index}");
                        let event = match make_event(id.clone(), payload_bytes) {
                            Ok(event) => event,
                            Err(_) => {
                                outcome.failed += 1;
                                return outcome;
                            }
                        };
                        probe.register(&stream, &event.id);
                        let sampled =
                            should_sample(producer, event_index, event_count, producer_count);
                        if sampled {
                            outcome.schedule_lateness_ns.push(ns(offer_started
                                .checked_duration_since(scheduled)
                                .unwrap_or(Duration::ZERO)));
                        }
                        let call_started = Instant::now();
                        let result = runtime.try_append(&stream, event).await;
                        let elapsed = ns(call_started.elapsed());
                        if sampled {
                            outcome.all_outcome_latencies_ns.push(elapsed);
                        }
                        match result {
                            Ok(receipt) => {
                                if sampled {
                                    outcome.successful_receipt_latencies_ns.push(elapsed);
                                }
                                if receipt.kind == AppendKind::Inserted {
                                    outcome.inserted += 1;
                                } else {
                                    outcome.deduplicated += 1;
                                }
                            }
                            Err(Error::Overloaded | Error::AdmissionTimeout) => {
                                probe.cancel(
                                    &stream,
                                    &EventId::new(id).expect("valid benchmark id"),
                                );
                                outcome.rejected += 1;
                                if sampled {
                                    outcome.rejected_latencies_ns.push(elapsed);
                                }
                            }
                            Err(_) => outcome.failed += 1,
                        }
                        outcome
                    }
                },
            )
            .await?;
            merge(&mut outcome, sustained);
        } else if args.scenario == "agent_burst" {
            for stream_index in 0..streams.len() {
                for index in 0..args.events {
                    let runtime = runtime.clone();
                    let stream = streams[stream_index].clone();
                    let probe = probe.clone();
                    let barrier = burst_barrier.clone();
                    let payload_bytes = args.payload;
                    let sampled = should_sample(stream_index, index, args.events, args.streams);
                    burst_tasks.push(tokio::spawn(async move {
                        barrier.wait().await;
                        barrier.wait().await;
                        let id = format!("agent-{stream_index}-{index}");
                        let event = match make_event(id.clone(), payload_bytes) {
                            Ok(event) => event,
                            Err(_) => {
                                return AgentBurstOutcome {
                                    disposition: AgentBurstDisposition::EventBuildFailed,
                                    latency_ns: 0,
                                    sampled: false,
                                };
                            }
                        };
                        probe.register(&stream, &event.id);
                        let call_started = Instant::now();
                        let result = runtime.try_append(&stream, event).await;
                        let elapsed = ns(call_started.elapsed());
                        let disposition = match result {
                            Ok(receipt) => {
                                if receipt.kind == AppendKind::Inserted {
                                    AgentBurstDisposition::Inserted
                                } else {
                                    AgentBurstDisposition::Deduplicated
                                }
                            }
                            Err(Error::Overloaded | Error::AdmissionTimeout) => {
                                probe.cancel(
                                    &stream,
                                    &EventId::new(id).expect("valid benchmark id"),
                                );
                                AgentBurstDisposition::Rejected
                            }
                            Err(_) => AgentBurstDisposition::Failed,
                        };
                        AgentBurstOutcome {
                            disposition,
                            latency_ns: elapsed,
                            sampled,
                        }
                    }));
                }
            }
        } else if args.scenario == "active_streams" {
            for worker in 0..args.producers.min(streams.len()) {
                let runtime = runtime.clone();
                let streams = streams.clone();
                let args = args.clone();
                let probe = probe.clone();
                tasks.push(tokio::spawn(async move {
                    let mut outcome = Outcome::default();
                    for stream_index in (worker..streams.len()).step_by(args.producers) {
                        let stream = &streams[stream_index];
                        for index in 0..args.events {
                            outcome.offered += 1;
                            let id = format!("agent-{stream_index}-{index}");
                            let event = match make_event(id.clone(), args.payload) {
                                Ok(event) => event,
                                Err(_) => {
                                    outcome.failed += 1;
                                    continue;
                                }
                            };
                            probe.register(stream, &event.id);
                            let call_started = Instant::now();
                            let result = runtime.append(stream, event).await;
                            let elapsed = ns(call_started.elapsed());
                            if should_sample(stream_index, index, args.events, args.streams) {
                                outcome.all_outcome_latencies_ns.push(elapsed);
                            }
                            match result {
                                Ok(receipt) => {
                                    if should_sample(stream_index, index, args.events, args.streams)
                                    {
                                        outcome.successful_receipt_latencies_ns.push(elapsed);
                                    }
                                    if receipt.kind == AppendKind::Inserted {
                                        outcome.inserted += 1;
                                    } else {
                                        outcome.deduplicated += 1;
                                    }
                                }
                                Err(Error::Overloaded | Error::AdmissionTimeout) => {
                                    probe.cancel(
                                        stream,
                                        &EventId::new(id).expect("valid benchmark id"),
                                    );
                                    outcome.rejected += 1;
                                }
                                Err(_) => outcome.failed += 1,
                            }
                        }
                    }
                    outcome
                }));
            }
        } else {
            for producer in 0..args.producers {
                let runtime = runtime.clone();
                let stream = streams[producer % streams.len()].clone();
                let args = args.clone();
                let producer_events = args.events;
                let probe = probe.clone();
                let spawn_one = move |index: usize| {
                    let runtime = runtime.clone();
                    let stream = stream.clone();
                    let args = args.clone();
                    let probe = probe.clone();
                    tokio::spawn(async move {
                        let mut outcome = Outcome::default();
                        let range = if open_loop {
                            index..index + 1
                        } else {
                            0..args.events
                        };
                        for index in range {
                            outcome.offered += 1;
                            let id = if args.scenario == "duplicates" {
                                "duplicate".to_owned()
                            } else {
                                format!("p{producer}-{index}")
                            };
                            let event = match make_event(id.clone(), args.payload) {
                                Ok(event) => event,
                                Err(_) => {
                                    outcome.failed += 1;
                                    continue;
                                }
                            };
                            let probe_registered = args.scenario != "duplicates";
                            if probe_registered {
                                probe.register(&stream, &event.id);
                            }
                            let call_started = Instant::now();
                            let result = if args.scenario == "overload" {
                                runtime.try_append(&stream, event).await
                            } else {
                                runtime.append(&stream, event).await
                            };
                            let elapsed = ns(call_started.elapsed());
                            if should_sample(producer, index, args.events, args.producers) {
                                outcome.all_outcome_latencies_ns.push(elapsed);
                            }
                            match result {
                                Ok(receipt) => {
                                    if should_sample(producer, index, args.events, args.producers) {
                                        outcome.successful_receipt_latencies_ns.push(elapsed);
                                    }
                                    if receipt.kind == AppendKind::Inserted {
                                        outcome.inserted += 1
                                    } else {
                                        outcome.deduplicated += 1
                                    }
                                }
                                Err(Error::Overloaded | Error::AdmissionTimeout) => {
                                    if probe_registered {
                                        probe.cancel(
                                            &stream,
                                            &EventId::new(id).expect("valid benchmark id"),
                                        );
                                    }
                                    outcome.rejected += 1;
                                    if should_sample(producer, index, args.events, args.producers) {
                                        outcome.rejected_latencies_ns.push(elapsed);
                                    }
                                }
                                Err(_) => outcome.failed += 1,
                            }
                        }
                        outcome
                    })
                };
                if open_loop {
                    for index in 0..producer_events {
                        tasks.push(spawn_one(index));
                    }
                } else {
                    tasks.push(spawn_one(0));
                }
            }
        }
    } else if matches!(
        args.scenario.as_str(),
        "idle" | "idle_scale" | "subscription_scale" | "warmed_subscription_scale"
    ) {
        tokio::time::sleep(if args.scenario == "idle" {
            Duration::from_millis(250)
        } else {
            Duration::from_secs(10)
        })
        .await;
    }
    if args.scenario == "agent_burst" {
        let expected = total_offers(args).unwrap_or(0);
        tokio::time::timeout(Duration::from_secs(120), burst_barrier.wait())
            .await
            .map_err(|_| Error::RuntimeFaulted("agent burst readiness timed out".into()))?;
        outcome.burst_ready_tasks = expected;
        outcome.burst_ready_rss_bytes = current_rss_bytes();
        outcome.burst_ready_virtual_bytes = current_process_metric("vsz", 1024);
        let ready_allocations = AllocationSnapshot::read();
        outcome.burst_ready_rust_requested_live_delta_bytes = Some(signed_delta(
            ready_allocations
                .allocated
                .saturating_sub(allocations_before.allocated),
            ready_allocations
                .deallocated
                .saturating_sub(allocations_before.deallocated),
        ));
        tokio::time::timeout(Duration::from_secs(120), burst_barrier.wait())
            .await
            .map_err(|_| Error::RuntimeFaulted("agent burst release timed out".into()))?;
    }
    outcome.append_tasks += tasks.len() + burst_tasks.len();
    outcome.subscriber_tasks = delivery_tasks.len();
    for task in tasks {
        merge(
            &mut outcome,
            task.await
                .map_err(|e| Error::RuntimeFaulted(e.to_string()))?,
        );
    }
    for task in burst_tasks {
        merge_agent_burst(
            &mut outcome,
            task.await
                .map_err(|e| Error::RuntimeFaulted(e.to_string()))?,
        );
    }
    if let Some(task) = replay_task {
        outcome.replayed = task
            .await
            .map_err(|e| Error::RuntimeFaulted(e.to_string()))??;
    }
    for task in delivery_tasks {
        outcome.delivered += tokio::time::timeout(Duration::from_secs(120), task)
            .await
            .map_err(|_| Error::RuntimeFaulted("live delivery timed out".into()))?
            .map_err(|error| Error::RuntimeFaulted(error.to_string()))??;
    }
    let active_elapsed_ns = started.elapsed().as_nanos();
    let diagnostics = runtime.diagnostics().await;
    outcome.runtime_accepted = diagnostics
        .append_accepted
        .saturating_sub(diagnostics_before.append_accepted);
    outcome.runtime_rejected = diagnostics
        .append_rejected
        .saturating_sub(diagnostics_before.append_rejected);
    outcome.runtime_failed = diagnostics
        .append_failed
        .saturating_sub(diagnostics_before.append_failed);
    outcome.settled_active_subscriptions = diagnostics.active_subscriptions;
    outcome.peak_queued_appends = diagnostics.peak_queued_appends;
    outcome.peak_queued_append_bytes = diagnostics.peak_queued_append_bytes;
    outcome.elapsed_ns = active_elapsed_ns;
    let (
        store_entered,
        queue_wait_ns,
        store_service_ns,
        store_return_to_delivery_ns,
        commit_to_delivery_upper_ns,
        unmatched_deliveries,
    ) = probe.take();
    outcome.store_entered = store_entered;
    outcome.queue_wait_ns = queue_wait_ns;
    outcome.store_service_ns = store_service_ns;
    outcome.store_return_to_delivery_ns = store_return_to_delivery_ns;
    outcome.commit_to_delivery_upper_ns = commit_to_delivery_upper_ns;
    outcome.startup_rss_bytes = startup_rss_bytes;
    outcome.setup_elapsed_ns = setup_elapsed_ns;
    let allocations_after = AllocationSnapshot::read();
    outcome.allocation_count = allocations_after
        .count
        .saturating_sub(allocations_before.count);
    outcome.allocated_bytes = allocations_after
        .allocated
        .saturating_sub(allocations_before.allocated);
    outcome.deallocated_bytes = allocations_after
        .deallocated
        .saturating_sub(allocations_before.deallocated);
    let usage_after = Usage::read();
    if let Some(usage_after) = usage_after {
        usage_after.delta_into(usage_before, &mut outcome);
    }
    if let Some(state) = &population_state {
        let (receipt_offsets, cycles, receipt_state_bytes) = {
            let mut state = state.lock().expect("population state poisoned");
            let receipt_state_bytes = state.receipt_offsets.capacity() * std::mem::size_of::<u64>()
                + state.cycles.capacity() * std::mem::size_of::<PopulationCycle>();
            (
                std::mem::take(&mut state.receipt_offsets),
                std::mem::take(&mut state.cycles),
                receipt_state_bytes,
            )
        };
        let verification_started = Instant::now();
        let (coverage, uncovered_stream_indices) = verify_population(
            runtime.as_ref(),
            streams.as_ref(),
            &receipt_offsets,
            args.payload,
        )
        .await?;
        outcome.population_verification_elapsed_ns = verification_started.elapsed().as_nanos();
        outcome.population_successful_stream_coverage = coverage;
        outcome.population_uncovered_streams = args.streams.saturating_sub(coverage);
        outcome.population_uncovered_stream_indices = uncovered_stream_indices;
        outcome.population_state_vector_capacity_bytes = receipt_state_bytes;
        outcome.population_cycles = cycles;
    }
    outcome.settled_rss_bytes = current_rss_bytes();
    outcome.settled_threads = current_thread_count();
    outcome.settled_virtual_bytes = current_process_metric("vsz", 1024);
    let shutdown_usage_before = Usage::read();
    let shutdown_allocations_before = AllocationSnapshot::read();
    let shutdown_started = Instant::now();
    drop(subscriptions);
    let shutdown_report = runtime.shutdown(Duration::from_secs(10)).await?;
    outcome.shutdown_elapsed_ns = shutdown_started.elapsed().as_nanos();
    outcome.shutdown_closed = shutdown_report.closed;
    outcome.shutdown_unresolved = shutdown_report.unresolved.len();
    let shutdown_allocations_after = AllocationSnapshot::read();
    outcome.shutdown_allocation_count = shutdown_allocations_after
        .count
        .saturating_sub(shutdown_allocations_before.count);
    outcome.shutdown_allocated_bytes = shutdown_allocations_after
        .allocated
        .saturating_sub(shutdown_allocations_before.allocated);
    outcome.shutdown_deallocated_bytes = shutdown_allocations_after
        .deallocated
        .saturating_sub(shutdown_allocations_before.deallocated);
    if let Some(shutdown_usage_after) = Usage::read() {
        let mut shutdown_usage = Outcome::default();
        shutdown_usage_after.delta_into(shutdown_usage_before, &mut shutdown_usage);
        outcome.shutdown_cpu_user_us = shutdown_usage.cpu_user_us;
        outcome.shutdown_cpu_system_us = shutdown_usage.cpu_system_us;
    }
    let shutdown_diagnostics = runtime.diagnostics().await;
    outcome.shutdown_queued_appends = shutdown_diagnostics.queued_appends;
    outcome.shutdown_admission_waiters = shutdown_diagnostics.admission_waiters;
    outcome.shutdown_active_subscriptions = shutdown_diagnostics.active_subscriptions;
    outcome.post_shutdown_rss_bytes = current_rss_bytes();
    outcome.post_shutdown_threads = current_thread_count();
    outcome.post_shutdown_virtual_bytes = current_process_metric("vsz", 1024);
    if !outcome.shutdown_closed || outcome.shutdown_unresolved != 0 {
        return Err(Error::RuntimeFaulted(format!(
            "shutdown closed={}, unresolved={}",
            outcome.shutdown_closed, outcome.shutdown_unresolved
        )));
    }
    drop(streams);
    drop(runtime);
    drop(probe);
    outcome.post_drop_rss_bytes = current_rss_bytes();
    outcome.post_drop_threads = current_thread_count();
    outcome.post_drop_virtual_bytes = current_process_metric("vsz", 1024);
    outcome.storage_bytes =
        storage_path.and_then(|path| std::fs::metadata(path).ok().map(|m| m.len()));
    if outcome.failed != 0 {
        return Err(Error::RuntimeFaulted(format!(
            "performance iteration had {} unexpected failures",
            outcome.failed
        )));
    }
    if matches!(args.scenario.as_str(), "sustained" | "population_sustained") {
        let completed = outcome
            .inserted
            .saturating_add(outcome.deduplicated)
            .saturating_add(outcome.rejected)
            .saturating_add(outcome.failed);
        if outcome.offered != completed.saturating_add(outcome.generator_rejected)
            || outcome.append_tasks as u64 != completed
        {
            return Err(Error::RuntimeFaulted(format!(
                "{} accounting mismatch: offered={}, spawned={}, completed={}, generator_rejected={}",
                args.scenario,
                outcome.offered, outcome.append_tasks, completed, outcome.generator_rejected
            )));
        }
    }
    if args.scenario == "population_sustained" {
        for (cycle, counters) in outcome.population_cycles.iter().enumerate() {
            let completed = counters
                .accepted
                .saturating_add(counters.runtime_rejected)
                .saturating_add(counters.generator_rejected)
                .saturating_add(counters.failed);
            if counters.offered != completed {
                return Err(Error::RuntimeFaulted(format!(
                    "population cycle {cycle} accounting mismatch: offered={}, accounted={completed}",
                    counters.offered
                )));
            }
        }
    }
    if args.scenario == "live_delivery"
        && outcome.delivered != outcome.inserted.saturating_mul(args.subscribers as u64)
    {
        return Err(Error::RuntimeFaulted(format!(
            "delivered {} records, expected {}",
            outcome.delivered,
            outcome.inserted.saturating_mul(args.subscribers as u64)
        )));
    }
    if args.scenario == "live_delivery" && unmatched_deliveries != 0 {
        return Err(Error::RuntimeFaulted(format!(
            "{unmatched_deliveries} acknowledged records were not delivered to every subscriber"
        )));
    }
    Ok(outcome)
}

fn make_event(id: String, payload_bytes: usize) -> event_stream::Result<NewEvent> {
    Ok(NewEvent {
        id: event_stream::EventId::new(id)?,
        schema: SchemaRef {
            id: SchemaId::new("bench.bytes")?,
            version: 1,
        },
        payload: Payload::copy_from_slice(&vec![0x5a; payload_bytes]),
    })
}

async fn verify_population<S: EventStore>(
    runtime: &Runtime<ProbedStore<S>>,
    streams: &[StreamKey],
    receipt_offsets: &[u64],
    payload_bytes: usize,
) -> event_stream::Result<(usize, Vec<usize>)> {
    let stream_count = streams.len();
    let mut successful_stream_coverage = 0;
    let mut uncovered_stream_indices = Vec::new();
    for (stream_index, stream) in streams.iter().enumerate() {
        let expected = population_expected_for_stream(receipt_offsets, stream_count, stream_index);
        if !expected.is_empty() {
            successful_stream_coverage += 1;
        } else {
            uncovered_stream_indices.push(stream_index);
        }

        let bounds = runtime.bounds(stream).await?;
        if bounds.tail.offset != expected.len() as u64 {
            return Err(Error::RuntimeFaulted(format!(
                "population stream {stream_index} retained {} records, expected {} accepted receipts",
                bounds.tail.offset,
                expected.len()
            )));
        }
        let mut after = event_stream::Cursor::new(stream.clone(), 0);
        let mut verified = 0;
        while verified < expected.len() {
            let page = runtime
                .read_after(
                    &after,
                    PageLimits {
                        max_records: 256,
                        max_bytes: 2 * 1024 * 1024,
                    },
                    Some(&bounds.tail),
                )
                .await?;
            if page.records.is_empty() {
                return Err(Error::RuntimeFaulted(format!(
                    "population stream {stream_index} replay ended after {verified} of {} records",
                    expected.len()
                )));
            }
            for record in &page.records {
                let (expected_offset, cycle) = expected.get(verified).ok_or_else(|| {
                    Error::RuntimeFaulted(format!(
                        "population stream {stream_index} replay returned an extra record"
                    ))
                })?;
                verify_population_record(
                    record,
                    stream,
                    *expected_offset,
                    stream_index,
                    *cycle,
                    payload_bytes,
                )?;
                verified += 1;
            }
            after = page.next_after;
        }
    }
    Ok((successful_stream_coverage, uncovered_stream_indices))
}

fn population_expected_for_stream(
    receipt_offsets: &[u64],
    stream_count: usize,
    stream_index: usize,
) -> Vec<(u64, usize)> {
    let mut expected = (stream_index..receipt_offsets.len())
        .step_by(stream_count)
        .filter_map(|offer_index| {
            let offset = receipt_offsets[offer_index];
            (offset != 0).then_some((offset, offer_index / stream_count))
        })
        .collect::<Vec<_>>();
    expected.sort_unstable_by_key(|(offset, _)| *offset);
    expected
}

fn verify_population_record(
    record: &event_stream::Record,
    stream: &StreamKey,
    expected_offset: u64,
    stream_index: usize,
    cycle: usize,
    payload_bytes: usize,
) -> event_stream::Result<()> {
    let expected_id = population_event_id(stream_index, cycle);
    if record.cursor.stream != *stream
        || record.cursor.offset != expected_offset
        || record.event.id.as_str() != expected_id
        || record.event.schema.id.as_str() != "bench.bytes"
        || record.event.schema.version != 1
        || record.event.payload.len() != payload_bytes
        || record
            .event
            .payload
            .as_bytes()
            .iter()
            .any(|byte| *byte != 0x5a)
    {
        return Err(Error::RuntimeFaulted(format!(
            "population verification mismatch for stream {stream_index}, cycle {cycle}, expected cursor {expected_offset} and ID {expected_id}"
        )));
    }
    Ok(())
}

fn merge(target: &mut Outcome, mut source: Outcome) {
    target.offered += source.offered;
    target.generator_rejected += source.generator_rejected;
    target.store_entered += source.store_entered;
    target.rejected += source.rejected;
    target.failed += source.failed;
    target.inserted += source.inserted;
    target.deduplicated += source.deduplicated;
    target.append_tasks += source.append_tasks;
    target.peak_generator_tasks = target.peak_generator_tasks.max(source.peak_generator_tasks);
    target
        .all_outcome_latencies_ns
        .append(&mut source.all_outcome_latencies_ns);
    target
        .successful_receipt_latencies_ns
        .append(&mut source.successful_receipt_latencies_ns);
    target
        .rejected_latencies_ns
        .append(&mut source.rejected_latencies_ns);
    target
        .schedule_lateness_ns
        .append(&mut source.schedule_lateness_ns);
}

async fn run_sustained_generator<F, Fut>(
    producers: usize,
    events: usize,
    offer_interval: Duration,
    task_limit: usize,
    operation: F,
) -> event_stream::Result<Outcome>
where
    F: Fn(usize, usize, Instant) -> Fut,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    run_sustained_generator_with_rejections(
        producers,
        events,
        offer_interval,
        task_limit,
        operation,
        |_, _| {},
    )
    .await
}

async fn run_sustained_generator_with_rejections<F, Fut, R>(
    producers: usize,
    events: usize,
    offer_interval: Duration,
    task_limit: usize,
    operation: F,
    rejected: R,
) -> event_stream::Result<Outcome>
where
    F: Fn(usize, usize, Instant) -> Fut,
    Fut: Future<Output = Outcome> + Send + 'static,
    R: Fn(usize, usize),
{
    if task_limit == 0 {
        return Err(Error::InvalidConfig(
            "sustained generator task limit must be positive".into(),
        ));
    }
    let schedule_started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    let mut outcome = Outcome::default();

    for event_index in 0..events {
        let offset = offer_interval
            .checked_mul(event_index as u32)
            .ok_or_else(|| Error::InvalidConfig("sustained schedule duration overflow".into()))?;
        let scheduled = schedule_started
            .checked_add(offset)
            .ok_or_else(|| Error::InvalidConfig("sustained schedule deadline overflow".into()))?;
        tokio::time::sleep_until(tokio::time::Instant::from_std(scheduled)).await;

        for producer in 0..producers {
            while let Some(result) = tasks.try_join_next() {
                merge(
                    &mut outcome,
                    result.map_err(|error| Error::RuntimeFaulted(error.to_string()))?,
                );
            }

            outcome.offered += 1;
            if tasks.len() == task_limit {
                outcome.generator_rejected += 1;
                rejected(producer, event_index);
                continue;
            }

            tasks.spawn(operation(producer, event_index, scheduled));
            outcome.append_tasks += 1;
            outcome.peak_generator_tasks = outcome.peak_generator_tasks.max(tasks.len());
        }
    }

    while let Some(result) = tasks.join_next().await {
        merge(
            &mut outcome,
            result.map_err(|error| Error::RuntimeFaulted(error.to_string()))?,
        );
    }
    Ok(outcome)
}

fn population_offer_index(producer: usize, event_index: usize, producers: usize) -> usize {
    event_index * producers + producer
}

fn population_mapping(offer_index: usize, streams: usize) -> (usize, usize) {
    (offer_index % streams, offer_index / streams)
}

fn population_event_id(stream_index: usize, cycle: usize) -> String {
    format!("population-{stream_index}-{cycle}")
}

fn merge_agent_burst(target: &mut Outcome, source: AgentBurstOutcome) {
    target.offered += 1;
    match source.disposition {
        AgentBurstDisposition::EventBuildFailed | AgentBurstDisposition::Failed => {
            target.failed += 1;
        }
        AgentBurstDisposition::Inserted => target.inserted += 1,
        AgentBurstDisposition::Deduplicated => target.deduplicated += 1,
        AgentBurstDisposition::Rejected => target.rejected += 1,
    }
    if source.sampled {
        let elapsed = source.latency_ns;
        target.all_outcome_latencies_ns.push(elapsed);
        match source.disposition {
            AgentBurstDisposition::Inserted | AgentBurstDisposition::Deduplicated => {
                target.successful_receipt_latencies_ns.push(elapsed);
            }
            AgentBurstDisposition::Rejected => target.rejected_latencies_ns.push(elapsed),
            AgentBurstDisposition::EventBuildFailed | AgentBurstDisposition::Failed => {}
        }
    }
}

#[cfg(feature = "codec")]
async fn run_decoder<S: EventStore>(
    runtime: Arc<Runtime<ProbedStore<S>>>,
    stream: event_stream::StreamKey,
    args: &Args,
    probe: &Probe,
) -> std::result::Result<(u64, u64), Box<dyn std::error::Error>> {
    let config = IngestionConfig {
        max_chunk_bytes: (args.payload + 1).min(64 * 1024),
        max_retained_input_bytes: args.payload + 1,
        max_decoder_retained_bytes: args.payload,
        max_total_decoder_bytes: args.payload + 1,
        max_output_bytes_per_step: args.payload + 1,
        ..IngestionConfig::default()
    };
    let service = IngestionService::new(runtime.clone(), config)?;
    let decoder = NewlineFramer::new(NewlineFramerConfig {
        max_frame_bytes: args.payload,
        emit_empty_frames: false,
        crlf: CrLfPolicy::StripCarriageReturn,
        final_line: FinalLinePolicy::RejectUnterminated,
    })?;
    let mut session = service.try_start(stream.clone(), decoder, {
        let probe = probe.clone();
        let probe_stream = stream.clone();
        move |frame: event_stream::ingestion::ByteFrame, position: DecodedPosition| {
            let id = EventId::new(format!("decoded-{}", position.item_index))
                .map_err(|error| error.to_string())?;
            probe.register(&probe_stream, &id);
            Ok(NewEvent {
                id,
                schema: SchemaRef {
                    id: SchemaId::new("bench.decoded").map_err(|error| error.to_string())?,
                    version: 1,
                },
                payload: frame.into_payload(),
            })
        }
    })?;
    let chunk_bytes = if args.scenario == "decoder_tiny" {
        1
    } else {
        64 * 1024
    };
    let mut chunk = vec![0x5a_u8; args.payload.min(chunk_bytes)];
    let input_events = if args.scenario == "decoder_malformed" {
        1
    } else {
        args.events
    };
    for _ in 0..input_events {
        let mut remaining = args.payload;
        while remaining != 0 {
            let take = remaining.min(chunk_bytes);
            if chunk.len() != take {
                chunk.resize(take, 0x5a);
            }
            session.push_chunk(&chunk).await?;
            remaining -= take;
        }
        if args.scenario != "decoder_malformed" {
            session.push_chunk(b"\n").await?;
        }
    }
    let expected_failure = if args.scenario == "decoder_malformed" {
        if session.finish().await.is_ok() {
            return Err("malformed decoder input unexpectedly succeeded".into());
        }
        1
    } else {
        session.finish().await?;
        0
    };
    Ok((runtime.bounds(&stream).await?.tail.offset, expected_failure))
}

fn print_outcome(repetition: usize, args: &Args, mut result: Outcome) {
    result.all_outcome_latencies_ns.sort_unstable();
    result.successful_receipt_latencies_ns.sort_unstable();
    result.rejected_latencies_ns.sort_unstable();
    result.schedule_lateness_ns.sort_unstable();
    result.queue_wait_ns.sort_unstable();
    result.store_service_ns.sort_unstable();
    result.store_return_to_delivery_ns.sort_unstable();
    result.commit_to_delivery_upper_ns.sort_unstable();
    let rate = if result.elapsed_ns == 0 {
        0.0
    } else {
        result.inserted as f64 * 1e9 / result.elapsed_ns as f64
    };
    let all = percentile_triplet(&result.all_outcome_latencies_ns);
    let success = percentile_triplet(&result.successful_receipt_latencies_ns);
    let rejected = percentile_triplet(&result.rejected_latencies_ns);
    let schedule_lateness = percentile_triplet(&result.schedule_lateness_ns);
    let queue = percentile_triplet(&result.queue_wait_ns);
    let store = percentile_triplet(&result.store_service_ns);
    let delivery = percentile_triplet(&result.store_return_to_delivery_ns);
    let delivery_upper = percentile_triplet(&result.commit_to_delivery_upper_ns);
    println!("{{\"kind\":\"sample\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"payload_bytes\":{},\"producers\":{},\"logical_agents\":{},\"append_tasks\":{},\"peak_generator_tasks\":{},\"generator_rejected\":{},\"subscriber_tasks\":{},\"streams\":{},\"subscribers\":{},\"settled_active_subscriptions\":{},\"events_per_producer\":{},\"offer_interval_us\":{},\"large_history_bytes\":{},\"setup_elapsed_ns\":{},\"offered\":{},\"runtime_accepted\":{},\"runtime_rejected\":{},\"runtime_failed\":{},\"store_entered\":{},\"caller_rejected\":{},\"caller_failed\":{},\"successful_receipts\":{},\"inserted\":{},\"deduplicated\":{},\"replayed\":{},\"delivered\":{},\"decoded_bytes\":{},\"elapsed_ns\":{},\"events_per_second\":{rate:.3},\"all_outcome_latency_samples\":{},\"all_outcome_latency_p50_ns\":{},\"all_outcome_latency_p95_ns\":{},\"all_outcome_latency_p99_ns\":{},\"successful_receipt_samples\":{},\"successful_receipt_p50_ns\":{},\"successful_receipt_p95_ns\":{},\"successful_receipt_p99_ns\":{},\"rejected_samples\":{},\"rejected_p50_ns\":{},\"rejected_p95_ns\":{},\"rejected_p99_ns\":{},\"schedule_lateness_samples\":{},\"schedule_lateness_p50_ns\":{},\"schedule_lateness_p95_ns\":{},\"schedule_lateness_p99_ns\":{},\"queue_wait_samples\":{},\"queue_wait_p50_ns\":{},\"queue_wait_p95_ns\":{},\"queue_wait_p99_ns\":{},\"store_service_samples\":{},\"store_service_p50_ns\":{},\"store_service_p95_ns\":{},\"store_service_p99_ns\":{},\"store_return_to_delivery_samples\":{},\"store_return_to_delivery_p50_ns\":{},\"store_return_to_delivery_p95_ns\":{},\"store_return_to_delivery_p99_ns\":{},\"cpu_user_us\":{},\"cpu_system_us\":{},\"max_rss_bytes\":{},\"max_rss_scope\":\"process_lifetime_peak\",\"startup_rss_bytes\":{},\"settled_rss_bytes\":{},\"post_shutdown_rss_bytes\":{},\"voluntary_context_switches\":{},\"involuntary_context_switches\":{},\"minor_faults\":{},\"major_faults\":{},\"storage_bytes\":{},\"rust_allocation_count\":{},\"rust_allocated_bytes\":{},\"rust_deallocated_bytes\":{},\"rust_allocation_scope\":{},\"shutdown_elapsed_ns\":{},\"shutdown_cpu_user_us\":{},\"shutdown_cpu_system_us\":{},\"shutdown_allocation_count\":{},\"shutdown_allocated_bytes\":{},\"shutdown_deallocated_bytes\":{},\"shutdown_queued_appends\":{},\"shutdown_admission_waiters\":{},\"shutdown_active_subscriptions\":{},\"copied_bytes\":null,\"peak_queued_appends\":{},\"peak_queued_append_bytes\":{},\"flush_count\":null}}",
        args.store,args.scenario,args.payload,args.producers,logical_agents(args),result.append_tasks,result.peak_generator_tasks,result.generator_rejected,result.subscriber_tasks,args.streams,args.subscribers,result.settled_active_subscriptions,args.events,args.offer_interval_us,args.large_history_bytes.map_or("null".into(),|v|v.to_string()),result.setup_elapsed_ns,result.offered,result.runtime_accepted,result.runtime_rejected,result.runtime_failed,result.store_entered,result.rejected,result.failed,result.inserted + result.deduplicated,result.inserted,result.deduplicated,result.replayed,result.delivered,result.decoded_bytes,result.elapsed_ns,
        result.all_outcome_latencies_ns.len(),opt(all.0),opt(all.1),opt(all.2),result.successful_receipt_latencies_ns.len(),opt(success.0),opt(success.1),opt(success.2),result.rejected_latencies_ns.len(),opt(rejected.0),opt(rejected.1),opt(rejected.2),result.schedule_lateness_ns.len(),opt(schedule_lateness.0),opt(schedule_lateness.1),opt(schedule_lateness.2),result.queue_wait_ns.len(),opt(queue.0),opt(queue.1),opt(queue.2),result.store_service_ns.len(),opt(store.0),opt(store.1),opt(store.2),
        result.store_return_to_delivery_ns.len(),opt(delivery.0),opt(delivery.1),opt(delivery.2),
        opt_i(result.cpu_user_us),opt_i(result.cpu_system_us),opt_u(result.max_rss_bytes),opt_u(result.startup_rss_bytes),opt_u(result.settled_rss_bytes),opt_u(result.post_shutdown_rss_bytes),opt_i(result.voluntary_switches),opt_i(result.involuntary_switches),opt_i(result.minor_faults),opt_i(result.major_faults),opt_u(result.storage_bytes),allocation_metric(result.allocation_count),allocation_metric(result.allocated_bytes),allocation_metric(result.deallocated_bytes),if cfg!(performance_no_alloc) { "null" } else { "\"GlobalAlloc_traffic_not_peak;excludes_SQLite_C_allocator\"" },result.shutdown_elapsed_ns,opt_i(result.shutdown_cpu_user_us),opt_i(result.shutdown_cpu_system_us),allocation_metric(result.shutdown_allocation_count),allocation_metric(result.shutdown_allocated_bytes),allocation_metric(result.shutdown_deallocated_bytes),result.shutdown_queued_appends,result.shutdown_admission_waiters,result.shutdown_active_subscriptions,result.peak_queued_appends,result.peak_queued_append_bytes);
    println!(
        "{{\"kind\":\"delivery_bounds\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"streams\":{},\"subscribers\":{},\"physical_commit_to_delivery_bound\":\"lower=max(0,delivery-store_return);upper=delivery-store_entry\",\"identity_misses\":0,\"samples\":{},\"upper_p50_ns\":{},\"upper_p95_ns\":{},\"upper_p99_ns\":{}}}",
        args.store,
        args.scenario,
        logical_agents(args),
        args.streams,
        args.subscribers,
        result.commit_to_delivery_upper_ns.len(),
        opt(delivery_upper.0),
        opt(delivery_upper.1),
        opt(delivery_upper.2)
    );
    println!(
        "{{\"kind\":\"resources\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"subscription_registration_elapsed_ns\":{},\"baseline_rss_bytes\":{},\"startup_rss_bytes\":{},\"post_registration_rss_bytes\":{},\"settled_rss_bytes\":{},\"post_shutdown_before_handle_drop_rss_bytes\":{},\"post_drop_before_profiler_release_rss_bytes\":{},\"baseline_threads\":{},\"startup_threads\":{},\"post_registration_threads\":{},\"settled_threads\":{},\"post_shutdown_before_handle_drop_threads\":{},\"post_drop_before_profiler_release_threads\":{},\"baseline_virtual_bytes\":{},\"startup_virtual_bytes\":{},\"post_registration_virtual_bytes\":{},\"settled_virtual_bytes\":{},\"post_shutdown_before_handle_drop_virtual_bytes\":{},\"post_drop_before_profiler_release_virtual_bytes\":{},\"setup_rust_allocation_count\":{},\"setup_rust_allocated_bytes\":{},\"setup_rust_deallocated_bytes\":{},\"setup_rust_requested_live_delta_bytes\":{},\"registration_rust_allocation_count\":{},\"registration_rust_allocated_bytes\":{},\"registration_rust_deallocated_bytes\":{},\"registration_rust_requested_live_delta_bytes\":{},\"timed_rust_requested_live_delta_bytes\":{},\"teardown_rust_requested_live_delta_bytes\":{},\"shutdown_closed\":{},\"shutdown_unresolved\":{},\"burst_ready_tasks\":{},\"burst_ready_rss_bytes\":{},\"burst_ready_virtual_bytes\":{},\"burst_ready_rust_requested_live_delta_bytes\":{},\"teardown_scope\":\"subscription_handle_drop_plus_Runtime_shutdown\",\"scope\":\"process_points_and_Rust_GlobalAlloc_requested_bytes;excludes_SQLite_C_allocator_and_allocator_metadata\"}}",
        args.store,
        args.scenario,
        logical_agents(args),
        result.subscription_registration_elapsed_ns,
        opt_u(result.baseline_rss_bytes),
        opt_u(result.startup_rss_bytes),
        opt_u(result.post_registration_rss_bytes),
        opt_u(result.settled_rss_bytes),
        opt_u(result.post_shutdown_rss_bytes),
        opt_u(result.post_drop_rss_bytes),
        opt_u(result.baseline_threads),
        opt_u(result.startup_threads),
        opt_u(result.post_registration_threads),
        opt_u(result.settled_threads),
        opt_u(result.post_shutdown_threads),
        opt_u(result.post_drop_threads),
        opt_u(result.baseline_virtual_bytes),
        opt_u(result.startup_virtual_bytes),
        opt_u(result.post_registration_virtual_bytes),
        opt_u(result.settled_virtual_bytes),
        opt_u(result.post_shutdown_virtual_bytes),
        opt_u(result.post_drop_virtual_bytes),
        allocation_metric(result.setup_allocation_count),
        allocation_metric(result.setup_allocated_bytes),
        allocation_metric(result.setup_deallocated_bytes),
        allocation_delta(signed_delta(result.setup_allocated_bytes, result.setup_deallocated_bytes)),
        allocation_metric(result.registration_allocation_count),
        allocation_metric(result.registration_allocated_bytes),
        allocation_metric(result.registration_deallocated_bytes),
        allocation_delta(signed_delta(
            result.registration_allocated_bytes,
            result.registration_deallocated_bytes
        )),
        allocation_delta(signed_delta(result.allocated_bytes, result.deallocated_bytes)),
        allocation_delta(signed_delta(
            result.shutdown_allocated_bytes,
            result.shutdown_deallocated_bytes
        )),
        result.shutdown_closed,
        result.shutdown_unresolved,
        result.burst_ready_tasks,
        opt_u(result.burst_ready_rss_bytes),
        opt_u(result.burst_ready_virtual_bytes),
        if cfg!(performance_no_alloc) {
            "null".into()
        } else {
            result
                .burst_ready_rust_requested_live_delta_bytes
                .map_or("null".into(), |value| value.to_string())
        }
    );
    if args.scenario == "warmed_subscription_scale" {
        println!(
            "{{\"kind\":\"warmup\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"streams\":{},\"subscribers\":{},\"records_per_subscription\":{},\"preload_elapsed_ns\":{},\"pre_drain_rss_bytes\":{},\"pre_drain_rust_requested_live_bytes\":{},\"drain_elapsed_ns\":{},\"post_drain_rss_bytes\":{},\"post_drain_rust_requested_live_bytes\":{},\"drain_rust_requested_live_delta_bytes\":{},\"drain_order\":\"sequential_subscribers\",\"drain_completion\":\"every_subscription_returned_256_exact_records_and_empty_buffer_released_synchronously\"}}",
            args.store,
            args.scenario,
            logical_agents(args),
            args.streams,
            args.subscribers,
            WARM_PAGE_RECORDS,
            result.warm_preload_elapsed_ns,
            opt_u(result.post_registration_rss_bytes),
            allocation_delta(result.pre_warm_drain_rust_requested_live_bytes),
            result.warm_drain_elapsed_ns,
            opt_u(result.post_warm_drain_rss_bytes),
            allocation_delta(result.post_warm_drain_rust_requested_live_bytes),
            allocation_delta(result.warm_drain_rust_requested_live_delta_bytes)
        );
    }
    if args.scenario == "population_sustained" {
        for (cycle, counters) in result.population_cycles.iter().enumerate() {
            println!(
                "{{\"kind\":\"population_cycle\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"cycle\":{},\"offered\":{},\"accepted\":{},\"runtime_rejected\":{},\"generator_rejected\":{},\"failed\":{}}}",
                args.store,
                args.scenario,
                cycle,
                counters.offered,
                counters.accepted,
                counters.runtime_rejected,
                counters.generator_rejected,
                counters.failed,
            );
        }
        println!(
            "{{\"kind\":\"population_verification\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"duration_ns\":{},\"successful_stream_coverage\":{},\"uncovered_streams\":{},\"uncovered_stream_indices\":{:?},\"accepted_receipts\":{},\"population_state_vector_capacity_bytes\":{},\"population_state_vector_capacity_cap_bytes\":{},\"population_state_scope\":\"vector_element_capacity_only;excludes_wrappers_samples_and_replay_expected_vectors\",\"page_max_records\":256,\"page_max_bytes\":2097152,\"scope\":\"after_active_timing_before_shutdown\",\"verified\":true}}",
            args.store,
            args.scenario,
            result.population_verification_elapsed_ns,
            result.population_successful_stream_coverage,
            result.population_uncovered_streams,
            result.population_uncovered_stream_indices,
            result.inserted + result.deduplicated,
            result.population_state_vector_capacity_bytes,
            MAX_OFFERS
                * (std::mem::size_of::<u64>() + std::mem::size_of::<PopulationCycle>()),
        );
    }
    println!(
        "{{\"kind\":\"decoder_result\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"streams\":{},\"subscribers\":{},\"expected_decoder_failures\":{}}}",
        args.store, args.scenario, logical_agents(args), args.streams, args.subscribers, result.expected_decoder_failures
    );
    let release_before = AllocationSnapshot::read();
    drop(std::mem::take(&mut result.all_outcome_latencies_ns));
    drop(std::mem::take(&mut result.successful_receipt_latencies_ns));
    drop(std::mem::take(&mut result.rejected_latencies_ns));
    drop(std::mem::take(&mut result.schedule_lateness_ns));
    drop(std::mem::take(&mut result.queue_wait_ns));
    drop(std::mem::take(&mut result.store_service_ns));
    drop(std::mem::take(&mut result.store_return_to_delivery_ns));
    drop(std::mem::take(&mut result.commit_to_delivery_upper_ns));
    std::thread::sleep(Duration::from_millis(10));
    let release_after = AllocationSnapshot::read();
    println!(
        "{{\"kind\":\"reclamation\",\"repetition\":{repetition},\"store\":\"{}\",\"scenario\":\"{}\",\"logical_agents\":{},\"after_shutdown_handle_drop_and_profiler_buffer_release_rss_bytes\":{},\"threads\":{},\"virtual_bytes\":{},\"rust_requested_live_bytes\":{},\"rust_requested_live_delta_during_buffer_release_bytes\":{},\"quiescence_wait_ms\":10,\"scope\":\"bounded_observation;RSS_may_include_allocator_retention;requested_live_excludes_allocator_metadata_and_SQLite_C\"}}",
        args.store,
        args.scenario,
        logical_agents(args),
        opt_u(current_rss_bytes()),
        opt_u(current_thread_count()),
        opt_u(current_process_metric("vsz", 1024)),
        allocation_delta(signed_delta(release_after.allocated, release_after.deallocated)),
        allocation_delta(signed_delta(
            release_after.allocated.saturating_sub(release_before.allocated),
            release_after
                .deallocated
                .saturating_sub(release_before.deallocated)
        ))
    );
}
fn percentile_triplet(values: &[u64]) -> (Option<u64>, Option<u64>, Option<u64>) {
    (
        percentile(values, 50),
        percentile(values, 95),
        percentile(values, 99),
    )
}
fn percentile(values: &[u64], percentile: usize) -> Option<u64> {
    if values.is_empty() {
        None
    } else {
        let rank = values.len().saturating_mul(percentile).div_ceil(100);
        Some(values[rank.saturating_sub(1).min(values.len() - 1)])
    }
}
fn opt(v: Option<u64>) -> String {
    opt_u(v)
}
fn opt_u(v: Option<u64>) -> String {
    v.map_or("null".into(), |v| v.to_string())
}
fn opt_i(v: Option<i64>) -> String {
    v.map_or("null".into(), |v| v.to_string())
}

fn signed_delta(allocated: u64, deallocated: u64) -> i128 {
    i128::from(allocated) - i128::from(deallocated)
}

fn parse_args() -> std::result::Result<Args, Box<dyn std::error::Error>> {
    let mut args = Args {
        store: "memory".into(),
        scenario: "append".into(),
        payload: 128,
        producers: 1,
        streams: 1,
        events: 64,
        repetitions: 3,
        large_history_bytes: None,
        subscribers: 1,
        offer_interval_us: 1_000,
    };
    let values: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < values.len() {
        let value = values.get(i + 1).ok_or("option needs a value")?;
        match values[i].as_str() {
            "--store" => args.store = value.clone(),
            "--scenario" => args.scenario = value.clone(),
            "--payload-bytes" => args.payload = value.parse()?,
            "--producers" => args.producers = value.parse()?,
            "--streams" => args.streams = value.parse()?,
            "--events" => args.events = value.parse()?,
            "--repetitions" => args.repetitions = value.parse()?,
            "--large-history-bytes" => args.large_history_bytes = Some(value.parse()?),
            "--subscribers" => args.subscribers = value.parse()?,
            "--offer-interval-us" => args.offer_interval_us = value.parse()?,
            _ => return Err(format!("unknown option {}", values[i]).into()),
        }
        i += 2;
    }
    if args.payload == 0
        || args.producers == 0
        || args.streams == 0
        || args.events == 0
        || args.repetitions == 0
        || args.subscribers == 0
    {
        return Err("numeric options must be positive".into());
    }
    let offers = total_offers(&args).ok_or("offer count overflows")?;
    if args.payload > MAX_PAYLOAD_BYTES {
        return Err(format!("payload must be at most {MAX_PAYLOAD_BYTES} bytes").into());
    }
    if args.producers > MAX_PRODUCERS
        || args.streams > MAX_STREAMS
        || offers > MAX_OFFERS
        || args.repetitions > MAX_REPETITIONS
        || args.subscribers > MAX_SUBSCRIBERS
    {
        return Err(format!(
            "limits: producers <= {MAX_PRODUCERS}, streams <= {MAX_STREAMS}, subscribers <= {MAX_SUBSCRIBERS}, total offers <= {MAX_OFFERS}, repetitions <= {MAX_REPETITIONS}"
        ).into());
    }
    if args.scenario == "overload" && offers > 16_384 {
        return Err("overload is limited to 16384 concurrently scheduled calls".into());
    }
    if args.scenario == "agent_burst" && offers > MAX_AGENT_BURST_TASKS {
        return Err(
            format!("agent_burst is limited to {MAX_AGENT_BURST_TASKS} Tokio tasks").into(),
        );
    }
    if args.scenario == "live_delivery"
        && (args.subscribers > MAX_LIVE_SUBSCRIBERS
            || args.streams != 1
            || offers
                .checked_mul(args.subscribers)
                .is_none_or(|deliveries| deliveries > MAX_LATENCY_SAMPLES))
    {
        return Err(
            "live_delivery requires one stream and at most 100000 event-subscriber deliveries"
                .into(),
        );
    }
    if args.scenario == "subscription_scale" && args.streams != args.subscribers {
        return Err("subscription scale requires one retained subscription per stream".into());
    }
    if args.scenario == "warmed_subscription_scale" && args.streams != 1 {
        return Err("warmed subscription scale requires one shared stream".into());
    }
    if args.scenario == "population_sustained"
        && (offers % args.streams != 0 || offers / args.streams < 2)
    {
        return Err(
            "population_sustained requires total offers divisible by streams and at least two complete population cycles"
                .into(),
        );
    }
    if args
        .large_history_bytes
        .is_some_and(|bytes| bytes == 0 || bytes > MAX_LARGE_HISTORY_BYTES)
    {
        return Err(format!("large history must be 1..={MAX_LARGE_HISTORY_BYTES} bytes").into());
    }
    if !matches!(
        args.scenario.as_str(),
        "append"
            | "overload"
            | "sustained"
            | "population_sustained"
            | "duplicates"
            | "replay_append"
            | "replay_only"
            | "live_delivery"
            | "stalled"
            | "idle"
            | "idle_scale"
            | "decoder"
            | "decoder_tiny"
            | "decoder_malformed"
            | "active_streams"
            | "agent_burst"
            | "subscription_scale"
            | "warmed_subscription_scale"
    ) {
        return Err("unknown scenario".into());
    }
    Ok(args)
}

fn total_offers(args: &Args) -> Option<usize> {
    if matches!(
        args.scenario.as_str(),
        "idle" | "idle_scale" | "subscription_scale" | "warmed_subscription_scale"
    ) {
        return Some(0);
    }
    if matches!(args.scenario.as_str(), "active_streams" | "agent_burst") {
        args.streams.checked_mul(args.events)
    } else {
        args.producers.checked_mul(args.events)
    }
}

fn logical_agents(args: &Args) -> usize {
    if args.scenario == "warmed_subscription_scale" {
        return args.subscribers;
    }
    if matches!(
        args.scenario.as_str(),
        "idle"
            | "idle_scale"
            | "active_streams"
            | "agent_burst"
            | "subscription_scale"
            | "population_sustained"
    ) {
        args.streams
    } else {
        args.producers
    }
}

fn ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn should_sample(producer: usize, index: usize, events: usize, producers: usize) -> bool {
    let total_index = producer.saturating_mul(events).saturating_add(index);
    let total = events.saturating_mul(producers);
    let stride = total.div_ceil(MAX_LATENCY_SAMPLES).max(1);
    total_index % stride == 0
}

fn stable_hash(stream: &StreamKey, id: &EventId) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in stream
        .id
        .as_str()
        .bytes()
        .chain(std::iter::once(0))
        .chain(id.as_str().bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn current_rss_bytes() -> Option<u64> {
    current_process_metric("rss", 1024)
}

fn current_thread_count() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-M", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let lines = output.stdout.split(|byte| *byte == b'\n').count();
    u64::try_from(lines.saturating_sub(2)).ok()
}

fn current_process_metric(field: &str, multiplier: u64) -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args([
            "-o",
            &format!("{field}="),
            "-p",
            &std::process::id().to_string(),
        ])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = std::str::from_utf8(&output.stdout)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()?;
    value.checked_mul(multiplier)
}

#[cfg(feature = "sqlite")]
fn sqlite_options(
    path: &std::path::Path,
    args: &Args,
) -> std::result::Result<SqliteOptions, Box<dyn std::error::Error>> {
    let mut options = SqliteOptions::new(path);
    if let Some(bytes) = args.large_history_bytes {
        let pages = bytes
            .saturating_mul(3)
            .div_ceil(4096)
            .saturating_add(16_384);
        options.max_database_pages = u32::try_from(pages.min(i32::MAX as u64))?;
    }
    Ok(options)
}

#[cfg(feature = "sqlite")]
fn ensure_free_space(
    path: &std::path::Path,
    target: u64,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(path.as_ptr(), &mut stats) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let available = (stats.f_bavail as u64).saturating_mul(stats.f_frsize as u64);
    let required = target
        .saturating_mul(2)
        .saturating_add(2 * 1024 * 1024 * 1024);
    if available < required {
        return Err(format!(
            "large-history run needs {required} free bytes; {available} available"
        )
        .into());
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
async fn preload_sqlite(
    path: &std::path::Path,
    args: &Args,
    target: u64,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut preload_args = args.clone();
    preload_args.large_history_bytes = Some(target);
    let config = {
        let mut config = RuntimeConfig::default();
        config.appends.max_queued_bytes = 64 * 1024 * 1024;
        config.appends.max_queued_bytes_per_stream = 64 * 1024 * 1024;
        config
    };
    let runtime =
        Runtime::<SqliteStore>::open(sqlite_options(path, &preload_args)?, config).await?;
    let stream = runtime.create_stream(&StreamId::new("stream-0")?).await?;
    let logical_record_bytes = args.payload.saturating_add(192).max(1) as u64;
    let records = target.div_ceil(logical_record_bytes);
    for index in 0..records {
        runtime
            .append(&stream, make_event(format!("large-{index}"), args.payload)?)
            .await?;
    }
    runtime.shutdown(Duration::from_secs(30)).await?;
    Ok(())
}

#[derive(Clone, Copy)]
struct Usage {
    user_us: i64,
    system_us: i64,
    max_rss: u64,
    nvcsw: i64,
    nivcsw: i64,
    minflt: i64,
    majflt: i64,
}
impl Usage {
    fn read() -> Option<Self> {
        unsafe {
            let mut r: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut r) != 0 {
                return None;
            }
            Some(Self {
                user_us: tv_us(r.ru_utime),
                system_us: tv_us(r.ru_stime),
                max_rss: rss_bytes(r.ru_maxrss),
                nvcsw: r.ru_nvcsw,
                nivcsw: r.ru_nivcsw,
                minflt: r.ru_minflt,
                majflt: r.ru_majflt,
            })
        }
    }
    fn delta_into(self, before: Option<Self>, out: &mut Outcome) {
        if let Some(before) = before {
            out.cpu_user_us = Some(self.user_us - before.user_us);
            out.cpu_system_us = Some(self.system_us - before.system_us);
            out.max_rss_bytes = Some(self.max_rss);
            out.voluntary_switches = Some(self.nvcsw - before.nvcsw);
            out.involuntary_switches = Some(self.nivcsw - before.nivcsw);
            out.minor_faults = Some(self.minflt - before.minflt);
            out.major_faults = Some(self.majflt - before.majflt)
        }
    }
}
// tv_usec is i32 on macOS and i64 on 64-bit Linux; keep the portable conversion.
#[allow(clippy::useless_conversion)]
fn tv_us(value: libc::timeval) -> i64 {
    value
        .tv_sec
        .saturating_mul(1_000_000)
        .saturating_add(i64::from(value.tv_usec))
}
#[cfg(target_os = "macos")]
fn rss_bytes(value: i64) -> u64 {
    value.max(0) as u64
}
#[cfg(not(target_os = "macos"))]
fn rss_bytes(value: i64) -> u64 {
    (value.max(0) as u64).saturating_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::{
        make_event, population_expected_for_stream, population_mapping, population_offer_index,
        run_sustained_generator, verify_population_record, AgentBurstOutcome, Outcome,
    };
    use event_stream::{Cursor, IncarnationId, Record, StreamId, StreamKey};
    use std::{sync::Arc, time::Duration};

    #[test]
    fn agent_burst_task_result_stays_compact() {
        let compact = std::mem::size_of::<AgentBurstOutcome>();
        let general = std::mem::size_of::<Outcome>();
        println!("agent_burst_task_result_bytes={compact} general_outcome_bytes={general}");
        assert!(compact <= 2 * std::mem::size_of::<usize>());
        assert!(compact.saturating_mul(16) < general);
    }

    #[test]
    fn population_mapping_covers_every_stream_once_per_cycle_with_nondivisible_width() {
        let streams = 5;
        let producers = 3;
        let events = 10;
        let mut visits = vec![vec![0; streams]; producers * events / streams];
        for event_index in 0..events {
            for producer in 0..producers {
                let offer = population_offer_index(producer, event_index, producers);
                let (stream, cycle) = population_mapping(offer, streams);
                visits[cycle][stream] += 1;
            }
        }
        assert_eq!(visits.len(), 6);
        assert_eq!(visits.iter().flatten().sum::<usize>(), producers * events);
        assert!(visits.iter().all(|cycle| cycle == &[1, 1, 1, 1, 1]));
    }

    #[test]
    fn population_record_verifier_rejects_wrong_cursor_id_and_payload() {
        let stream = StreamKey {
            id: StreamId::new("stream-2").unwrap(),
            incarnation: IncarnationId([7; 16]),
        };
        let correct = Record {
            cursor: Cursor::new(stream.clone(), 4),
            event: make_event("population-2-1".into(), 3).unwrap(),
        };
        assert!(verify_population_record(&correct, &stream, 4, 2, 1, 3).is_ok());
        assert!(verify_population_record(&correct, &stream, 5, 2, 1, 3).is_err());
        assert!(verify_population_record(&correct, &stream, 4, 2, 0, 3).is_err());

        let wrong_payload = Record {
            cursor: Cursor::new(stream.clone(), 4),
            event: make_event("population-2-1".into(), 2).unwrap(),
        };
        assert!(verify_population_record(&wrong_payload, &stream, 4, 2, 1, 3).is_err());
    }

    #[test]
    fn population_expected_records_follow_receipt_cursor_order() {
        // Stream zero's second cycle committed first. Replay must follow its
        // returned cursor offsets rather than offer or task completion order.
        let receipt_offsets = [2, 1, 1, 2];
        assert_eq!(
            population_expected_for_stream(&receipt_offsets, 2, 0),
            vec![(1, 1), (2, 0)]
        );
    }

    #[tokio::test]
    async fn sustained_generator_conserves_offers_at_hard_task_cap() {
        let task_limit = 2;
        let release_barrier = Arc::new(tokio::sync::Barrier::new(task_limit + 1));
        let operation_barrier = release_barrier.clone();
        let generator = tokio::spawn(async move {
            run_sustained_generator(6, 1, Duration::ZERO, task_limit, move |_, _, _| {
                let barrier = operation_barrier.clone();
                async move {
                    barrier.wait().await;
                    Outcome {
                        inserted: 1,
                        ..Outcome::default()
                    }
                }
            })
            .await
        });

        tokio::time::timeout(Duration::from_secs(1), release_barrier.wait())
            .await
            .expect("two append tasks should reach the release barrier");
        let outcome = tokio::time::timeout(Duration::from_secs(1), generator)
            .await
            .expect("generator should finish after append tasks are released")
            .expect("generator task should join")
            .unwrap();

        assert_eq!(outcome.offered, 6);
        assert_eq!(outcome.append_tasks, task_limit);
        assert_eq!(outcome.peak_generator_tasks, task_limit);
        assert_eq!(outcome.generator_rejected, 4);
        assert_eq!(outcome.inserted, 2);
        assert_eq!(
            outcome.offered,
            outcome.inserted + outcome.generator_rejected
        );
    }

    #[tokio::test]
    async fn sustained_generator_rejects_zero_task_limit() {
        let result = run_sustained_generator(1, 1, Duration::ZERO, 0, |_, _, _| async {
            Outcome::default()
        })
        .await;
        assert!(matches!(result, Err(event_stream::Error::InvalidConfig(_))));
    }
}
