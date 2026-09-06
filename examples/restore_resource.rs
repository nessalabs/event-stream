//! Bounded restore resource diagnostic. Fixture creation is outside the timed restore.
#![cfg_attr(
    not(all(feature = "sqlite", feature = "test-support")),
    allow(dead_code, unused_imports)
)]

#[cfg(all(feature = "sqlite", feature = "test-support"))]
mod enabled {
    use event_stream::{
        infrastructure::{
            install_sqlite_vfs_recorder, reset_sqlite_vfs_recorder, sqlite_vfs_snapshot,
            SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager, SqliteRestoreObserver,
            SqliteRestoreStage, SqliteStore, SqliteVfsCategorySnapshot, SqliteVfsSnapshot,
        },
        EventStore, PageLimits, RestoreConfig, RestoreOperationId, RestoreRequest,
    };
    use rusqlite::{params, Connection, TransactionBehavior};
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        error::Error,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    const PAYLOAD_BYTES: usize = 128;
    const MAX_STREAMS: usize = 100_000;
    const MAX_DEEP_RECORDS: usize = 100_000;
    const MAX_RECEIPTS: usize = 10_000;
    const STAGE_COUNT: usize = 13;
    type ReceiptRow = (i64, String, Vec<u8>, Vec<u8>, i64);
    type RetiredRecordRow = (Vec<u8>, Vec<u8>, i64, Vec<u8>, String, String, i64, Vec<u8>);

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FixtureShape {
        Wide,
        Deep,
        ReceiptHeavy,
    }

    impl FixtureShape {
        fn name(self) -> &'static str {
            match self {
                Self::Wide => "wide",
                Self::Deep => "deep",
                Self::ReceiptHeavy => "receipt_heavy",
            }
        }

        fn counts(self, population: usize) -> (usize, usize, usize) {
            match self {
                Self::Wide => (population, population, 0),
                Self::Deep => (1, population, 0),
                Self::ReceiptHeavy => (population * 2, population, population),
            }
        }
    }

    struct CountingAllocator;
    static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
    static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
    static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

    unsafe impl GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc(layout) };
            if !pointer.is_null() {
                ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
                ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            }
            pointer
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let pointer = unsafe { System.alloc_zeroed(layout) };
            if !pointer.is_null() {
                ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
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
                ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
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
            Self {
                count: ALLOCATION_COUNT.load(Ordering::Relaxed),
                allocated: ALLOCATED_BYTES.load(Ordering::Relaxed),
                deallocated: DEALLOCATED_BYTES.load(Ordering::Relaxed),
            }
        }

        fn live(self) -> u64 {
            self.allocated.saturating_sub(self.deallocated)
        }
    }

    #[global_allocator]
    static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

    #[derive(Debug)]
    struct StageObserver {
        root: PathBuf,
        origin: Mutex<Option<Instant>>,
        counts: [AtomicU64; STAGE_COUNT],
        last_ns: [AtomicU64; STAGE_COUNT],
        sizes: Mutex<[ExactSizes; STAGE_COUNT]>,
    }

    impl StageObserver {
        fn new(root: PathBuf) -> Self {
            Self {
                root,
                origin: Mutex::new(None),
                counts: [const { AtomicU64::new(0) }; STAGE_COUNT],
                last_ns: [const { AtomicU64::new(0) }; STAGE_COUNT],
                sizes: Mutex::new([ExactSizes::default(); STAGE_COUNT]),
            }
        }

        fn start(&self) {
            *self.origin.lock().expect("observer lock") = Some(Instant::now());
            for value in self.counts.iter().chain(self.last_ns.iter()) {
                value.store(0, Ordering::Relaxed);
            }
            *self.sizes.lock().expect("observer size lock") = [ExactSizes::default(); STAGE_COUNT];
        }

        fn stop(&self) {
            *self.origin.lock().expect("observer lock") = None;
        }

        fn encoded(&self) -> String {
            let mut fields = Vec::with_capacity(STAGE_COUNT);
            let sizes = self.sizes.lock().expect("observer size lock");
            for (index, name) in STAGE_NAMES.iter().enumerate() {
                let size = sizes[index];
                fields.push(format!(
                    "\"{name}\":{{\"count\":{},\"last_elapsed_ns\":{},\"staging_bytes\":{},\"staging_allocated_bytes\":{},\"journal_bytes\":{},\"journal_allocated_bytes\":{},\"owner_directory_count\":{},\"owner_allocated_bytes\":{},\"request_directory_count\":{},\"request_allocated_bytes\":{}}}",
                    self.counts[index].load(Ordering::Relaxed),
                    self.last_ns[index].load(Ordering::Relaxed),
                    size.staging,
                    size.staging_allocated,
                    size.journal,
                    size.journal_allocated,
                    size.owner_directories,
                    size.owner_allocated,
                    size.request_directories,
                    size.request_allocated,
                ));
            }
            format!("{{{}}}", fields.join(","))
        }
    }

    impl SqliteRestoreObserver for StageObserver {
        fn observe(&self, stage: SqliteRestoreStage) {
            let index = stage_index(stage);
            let Some(origin) = *self.origin.lock().expect("observer lock") else {
                return;
            };
            self.counts[index].fetch_add(1, Ordering::Relaxed);
            self.last_ns[index].store(ns(origin.elapsed()), Ordering::Relaxed);
            self.sizes.lock().expect("observer size lock")[index] =
                exact_restore_file_sizes(&self.root);
        }
    }

    const STAGE_NAMES: [&str; STAGE_COUNT] = [
        "source_validated",
        "owner_reserved",
        "staging_created",
        "identity_mappings_imported",
        "lifetimes_imported",
        "record_page_committed",
        "records_imported",
        "names_imported",
        "receipts_imported",
        "relational_validation_completed",
        "staging_completed",
        "staging_synced",
        "destination_published",
    ];

    fn stage_index(stage: SqliteRestoreStage) -> usize {
        match stage {
            SqliteRestoreStage::SourceValidated => 0,
            SqliteRestoreStage::OwnerReserved => 1,
            SqliteRestoreStage::StagingCreated => 2,
            SqliteRestoreStage::IdentityMappingsImported => 3,
            SqliteRestoreStage::LifetimesImported => 4,
            SqliteRestoreStage::RecordPageCommitted => 5,
            SqliteRestoreStage::RecordsImported => 6,
            SqliteRestoreStage::NamesImported => 7,
            SqliteRestoreStage::ReceiptsImported => 8,
            SqliteRestoreStage::RelationalValidationCompleted => 9,
            SqliteRestoreStage::StagingCompleted => 10,
            SqliteRestoreStage::StagingSynced => 11,
            SqliteRestoreStage::DestinationPublished => 12,
        }
    }

    #[derive(Default)]
    struct SamplePeaks {
        rss: AtomicU64,
        rust_live: AtomicU64,
        staging: AtomicU64,
        staging_allocated: AtomicU64,
        journal: AtomicU64,
        journal_allocated: AtomicU64,
        owner_directories: AtomicU64,
        owner_allocated: AtomicU64,
        request_directories: AtomicU64,
        request_allocated: AtomicU64,
        samples: AtomicU64,
    }

    struct Sampler {
        stop: Arc<AtomicBool>,
        peaks: Arc<SamplePeaks>,
        worker: Option<thread::JoinHandle<()>>,
    }

    impl Sampler {
        fn start(root: PathBuf) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let peaks = Arc::new(SamplePeaks::default());
            let worker_stop = stop.clone();
            let worker_peaks = peaks.clone();
            let worker = thread::spawn(move || {
                while !worker_stop.load(Ordering::Acquire) {
                    sample_files(&root, &worker_peaks);
                    if let Some(rss) = current_rss_bytes() {
                        update_max(&worker_peaks.rss, rss);
                    }
                    update_max(&worker_peaks.rust_live, AllocationSnapshot::read().live());
                    worker_peaks.samples.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(Duration::from_millis(1));
                }
                sample_files(&root, &worker_peaks);
            });
            Self {
                stop,
                peaks,
                worker: Some(worker),
            }
        }

        fn finish(mut self) -> Arc<SamplePeaks> {
            self.stop.store(true, Ordering::Release);
            self.worker.take().unwrap().join().unwrap();
            self.peaks
        }
    }

    fn update_max(target: &AtomicU64, value: u64) {
        target.fetch_max(value, Ordering::Relaxed);
    }

    fn sample_files(root: &Path, peaks: &SamplePeaks) {
        let Ok(entries) = std::fs::read_dir(root) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if name.ends_with(".sqlite3-journal") {
                update_max(
                    &peaks.journal,
                    entry.metadata().map(|value| value.len()).unwrap_or(0),
                );
                update_max(&peaks.journal_allocated, allocated_size(&path));
            } else if name.starts_with(".event-stream-restore-") && name.ends_with(".owner") {
                update_max(&peaks.owner_directories, 1);
                update_max(&peaks.owner_allocated, allocated_size(&path));
                let mut request_count = 0_u64;
                let mut request_allocated = 0_u64;
                for request in std::fs::read_dir(&path)
                    .into_iter()
                    .flatten()
                    .take(2)
                    .flatten()
                {
                    request_count += 1;
                    request_allocated =
                        request_allocated.saturating_add(allocated_size(&request.path()));
                }
                update_max(&peaks.request_directories, request_count);
                update_max(&peaks.request_allocated, request_allocated);
            } else if name.starts_with(".event-stream-restore-") && name.ends_with(".sqlite3") {
                update_max(&peaks.staging, file_size(&path));
                update_max(&peaks.staging_allocated, allocated_size(&path));
            }
        }
    }

    fn file_size(path: &Path) -> u64 {
        std::fs::metadata(path)
            .map(|value| value.len())
            .unwrap_or(0)
    }

    #[cfg(unix)]
    fn allocated_size(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::symlink_metadata(path)
            .map(|metadata| metadata.blocks().saturating_mul(512))
            .unwrap_or(0)
    }

    #[cfg(not(unix))]
    fn allocated_size(_path: &Path) -> u64 {
        0
    }

    #[cfg(target_os = "macos")]
    #[allow(deprecated)] // libc exposes the stable Mach call but marks its binding deprecated.
    fn current_rss_bytes() -> Option<u64> {
        unsafe {
            let mut info: libc::mach_task_basic_info_data_t = std::mem::zeroed();
            let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;
            let result = libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO,
                (&mut info as *mut libc::mach_task_basic_info_data_t).cast(),
                &mut count,
            );
            (result == 0).then_some(info.resident_size)
        }
    }

    #[cfg(target_os = "linux")]
    fn current_rss_bytes() -> Option<u64> {
        let stat = std::fs::read_to_string("/proc/self/statm").ok()?;
        let pages = stat.split_whitespace().nth(1)?.parse::<u64>().ok()?;
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        (page > 0).then(|| pages.saturating_mul(page as u64))
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn current_rss_bytes() -> Option<u64> {
        None
    }

    #[cfg(target_os = "macos")]
    fn process_disk_io_bytes() -> Option<(u64, u64)> {
        unsafe {
            let mut usage: libc::rusage_info_v2 = std::mem::zeroed();
            let result = libc::proc_pid_rusage(
                libc::getpid(),
                libc::RUSAGE_INFO_V2,
                (&mut usage as *mut libc::rusage_info_v2).cast(),
            );
            (result == 0).then_some((usage.ri_diskio_bytesread, usage.ri_diskio_byteswritten))
        }
    }

    #[cfg(target_os = "linux")]
    fn process_disk_io_bytes() -> Option<(u64, u64)> {
        let values = std::fs::read_to_string("/proc/self/io").ok()?;
        let mut read = None;
        let mut written = None;
        for line in values.lines() {
            if let Some(value) = line.strip_prefix("read_bytes: ") {
                read = value.parse().ok();
            } else if let Some(value) = line.strip_prefix("write_bytes: ") {
                written = value.parse().ok();
            }
        }
        Some((read?, written?))
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn process_disk_io_bytes() -> Option<(u64, u64)> {
        None
    }

    #[derive(Clone, Copy)]
    struct Usage {
        user_us: i64,
        system_us: i64,
        max_rss: u64,
        voluntary: i64,
        involuntary: i64,
        minor_faults: i64,
        major_faults: i64,
        disk_read_bytes: Option<u64>,
        disk_written_bytes: Option<u64>,
    }

    impl Usage {
        fn read() -> Result<Self, Box<dyn Error>> {
            unsafe {
                let mut value: libc::rusage = std::mem::zeroed();
                if libc::getrusage(libc::RUSAGE_SELF, &mut value) != 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                let disk_io = process_disk_io_bytes();
                Ok(Self {
                    user_us: timeval_us(value.ru_utime),
                    system_us: timeval_us(value.ru_stime),
                    max_rss: max_rss_bytes(value.ru_maxrss),
                    voluntary: value.ru_nvcsw,
                    involuntary: value.ru_nivcsw,
                    minor_faults: value.ru_minflt,
                    major_faults: value.ru_majflt,
                    disk_read_bytes: disk_io.map(|value| value.0),
                    disk_written_bytes: disk_io.map(|value| value.1),
                })
            }
        }
    }

    pub async fn run() -> Result<(), Box<dyn Error>> {
        let (shape, population, stage_observer_enabled, vfs_recorder_enabled) = parse_args()?;
        let (identities, records, receipts) = shape.counts(population);
        let duplicate_vfs_install_rejected = if vfs_recorder_enabled {
            install_sqlite_vfs_recorder()?;
            install_sqlite_vfs_recorder().is_err()
        } else {
            false
        };
        let root = std::env::temp_dir().join(format!(
            "event-stream-restore-resource-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        std::fs::create_dir(&root)?;
        let source = root.join("source.sqlite3");
        create_fixture(&source, shape, population)
            .await
            .map_err(|error| format!("create fixture: {error}"))?;

        let observer = Arc::new(StageObserver::new(root.clone()));
        let mut backend = SqliteRestoreBackend::new(&root)?;
        if stage_observer_enabled {
            backend = backend.with_observer(observer.clone());
        }
        let config = RestoreConfig {
            max_operations: 1,
            max_source_bytes: 512 * 1024 * 1024,
            max_staging_bytes: 512 * 1024 * 1024,
            copy_buffer_bytes: 64 * 1024,
            page: PageLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
            max_path_bytes: 4096,
        };
        let manager = SqliteRestoreManager::new(backend, config)?;
        let identity = manager
            .inspect_backup(source.clone())
            .await
            .map_err(|error| format!("inspect fixture: {error}"))?;
        let source_bytes = std::fs::metadata(&source)?.len();
        let rss_before = current_rss_bytes();
        let allocations_before = AllocationSnapshot::read();
        let usage_before = Usage::read()?;
        if vfs_recorder_enabled {
            reset_sqlite_vfs_recorder()?;
        }
        let sampler = Sampler::start(root.clone());
        observer.start();
        let started = Instant::now();
        let receipt = tokio::time::timeout(
            Duration::from_secs(15 * 60),
            manager.restore(RestoreRequest {
                operation_id: RestoreOperationId::new("resource-measurement")?,
                backup_identity: identity,
                source: source.clone(),
                destination: PathBuf::from("restored.sqlite3"),
            }),
        )
        .await
        .map_err(|_| "restore watchdog elapsed")?
        .map_err(|error| format!("timed restore: {error}"))?;
        let elapsed = started.elapsed();
        let vfs = vfs_recorder_enabled.then(sqlite_vfs_snapshot);
        observer.stop();
        let usage_after = Usage::read()?;
        let allocations_after = AllocationSnapshot::read();
        let peaks = sampler.finish();
        let rss_after = current_rss_bytes();
        let rust_live_post_sampler = AllocationSnapshot::read().live();

        let validation = validate_restore(&manager, &receipt, shape, population).await?;
        let retry = manager
            .restore(RestoreRequest {
                operation_id: RestoreOperationId::new("resource-measurement")?,
                backup_identity: identity,
                source: source.clone(),
                destination: PathBuf::from("restored.sqlite3"),
            })
            .await?;
        if retry != receipt {
            return Err("restore retry changed its receipt".into());
        }
        let repeated_identity = manager.inspect_backup(source.clone()).await?;
        if repeated_identity != identity {
            return Err("source identity changed".into());
        }
        let destination_bytes = std::fs::metadata(&receipt.destination)?.len();
        let exact = exact_restore_file_sizes(&root);
        let allocation_count = allocations_after
            .count
            .saturating_sub(allocations_before.count);
        let allocated_bytes = allocations_after
            .allocated
            .saturating_sub(allocations_before.allocated);
        let deallocated_bytes = allocations_after
            .deallocated
            .saturating_sub(allocations_before.deallocated);
        let disk_read_bytes =
            option_delta(usage_after.disk_read_bytes, usage_before.disk_read_bytes);
        let disk_written_bytes = option_delta(
            usage_after.disk_written_bytes,
            usage_before.disk_written_bytes,
        );
        let rss_before = option_value(rss_before);
        let rss_after = option_value(rss_after);
        println!(
            "{{\"kind\":\"restore_resource_sample\",\"fixture_shape\":\"{}\",\"population\":{population},\"identities\":{identities},\"streams\":{},\"records\":{records},\"lifecycle_receipts\":{receipts},\"payload_bytes\":{PAYLOAD_BYTES},\"stage_observer_enabled\":{stage_observer_enabled},\"vfs_recorder_enabled\":{vfs_recorder_enabled},\"duplicate_vfs_install_rejected\":{duplicate_vfs_install_rejected},\"sampler_enabled\":true,\"allocator_counters_enabled\":true,\"max_operations\":1,\"max_source_bytes\":536870912,\"max_staging_page_bytes\":536870912,\"copy_buffer_bytes\":65536,\"page_max_records\":256,\"page_max_bytes\":2097152,\"tokio_worker_threads\":2,\"watchdog_seconds\":900,\"timed_scope\":\"restore_call_including_sampler_and_optional_stage_observer_and_vfs_recorder_fixture_and_validation_excluded\",\"elapsed_ns\":{},\"cpu_user_us\":{},\"cpu_system_us\":{},\"max_rss_bytes\":{},\"max_rss_scope\":\"process_lifetime_including_fixture\",\"sampled_peak_rss_bytes\":{},\"rss_before_restore_bytes\":{rss_before},\"rss_post_operation_bytes\":{rss_after},\"voluntary_context_switches\":{},\"involuntary_context_switches\":{},\"minor_faults\":{},\"major_faults\":{},\"process_disk_read_bytes\":{disk_read_bytes},\"process_disk_written_bytes\":{disk_written_bytes},\"process_disk_io_scope\":\"whole_process_delta_not_physical_device_or_restore_file_specific\",\"sqlite_vfs\":{},\"rust_allocation_count\":{allocation_count},\"rust_allocated_bytes\":{allocated_bytes},\"rust_deallocated_bytes\":{deallocated_bytes},\"rust_live_bytes_before\":{},\"rust_live_bytes_after_before_sampler_stop\":{},\"rust_live_bytes_post_sampler_stop\":{rust_live_post_sampler},\"sampled_peak_rust_live_bytes\":{},\"sampler_samples\":{},\"source_bytes\":{source_bytes},\"sampled_peak_staging_bytes\":{},\"sampled_peak_staging_allocated_bytes\":{},\"sampled_peak_journal_bytes\":{},\"sampled_peak_journal_allocated_bytes\":{},\"sampled_peak_owner_directory_count\":{},\"sampled_peak_owner_allocated_bytes\":{},\"sampled_peak_request_directory_count\":{},\"sampled_peak_request_allocated_bytes\":{},\"final_staging_bytes\":{},\"final_staging_allocated_bytes\":{},\"final_journal_bytes\":{},\"final_journal_allocated_bytes\":{},\"final_owner_directory_count\":{},\"final_owner_allocated_bytes\":{},\"final_request_directory_count\":{},\"final_request_allocated_bytes\":{},\"published_bytes\":{destination_bytes},\"mapping_pages\":{},\"max_mapping_logical_bytes\":{},\"max_mapping_vector_capacity\":{},\"source_sha256\":\"{}\",\"stage_observations\":{},\"correctness\":\"exact_mapping_replay_receipts_retry_source_identity\"}}",
            shape.name(),
            match shape { FixtureShape::Deep => 1, _ => population },
            ns(elapsed),
            usage_after.user_us.saturating_sub(usage_before.user_us),
            usage_after.system_us.saturating_sub(usage_before.system_us),
            usage_after.max_rss,
            peaks.rss.load(Ordering::Relaxed),
            usage_after.voluntary.saturating_sub(usage_before.voluntary),
            usage_after.involuntary.saturating_sub(usage_before.involuntary),
            usage_after.minor_faults.saturating_sub(usage_before.minor_faults),
            usage_after.major_faults.saturating_sub(usage_before.major_faults),
            encode_vfs(vfs),
            allocations_before.live(),
            allocations_after.live(),
            peaks.rust_live.load(Ordering::Relaxed),
            peaks.samples.load(Ordering::Relaxed),
            peaks.staging.load(Ordering::Relaxed),
            peaks.staging_allocated.load(Ordering::Relaxed),
            peaks.journal.load(Ordering::Relaxed),
            peaks.journal_allocated.load(Ordering::Relaxed),
            peaks.owner_directories.load(Ordering::Relaxed),
            peaks.owner_allocated.load(Ordering::Relaxed),
            peaks.request_directories.load(Ordering::Relaxed),
            peaks.request_allocated.load(Ordering::Relaxed),
            exact.staging,
            exact.staging_allocated,
            exact.journal,
            exact.journal_allocated,
            exact.owner_directories,
            exact.owner_allocated,
            exact.request_directories,
            exact.request_allocated,
            validation.pages,
            validation.max_logical_bytes,
            validation.max_capacity,
            identity.to_hex(),
            observer.encoded(),
        );
        std::fs::remove_dir_all(root)?;
        Ok(())
    }

    fn parse_args() -> Result<(FixtureShape, usize, bool, bool), Box<dyn Error>> {
        let mut shape = None;
        let mut population = None;
        let mut stage_observer = None;
        let mut vfs_recorder = None;
        let mut args = std::env::args().skip(1);
        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--streams" => {
                    population = Some(args.next().ok_or("missing --streams value")?.parse()?);
                    shape.get_or_insert(FixtureShape::Wide);
                }
                "--population" => {
                    population = Some(args.next().ok_or("missing --population value")?.parse()?);
                }
                "--shape" => {
                    shape = Some(match args.next().as_deref() {
                        Some("wide") => FixtureShape::Wide,
                        Some("deep") => FixtureShape::Deep,
                        Some("receipt-heavy") => FixtureShape::ReceiptHeavy,
                        _ => return Err("--shape needs wide, deep, or receipt-heavy".into()),
                    });
                }
                "--stage-observer" => {
                    stage_observer = Some(match args.next().as_deref() {
                        Some("true") => true,
                        Some("false") => false,
                        _ => return Err("--stage-observer needs true or false".into()),
                    })
                }
                "--vfs-recorder" => {
                    vfs_recorder = Some(match args.next().as_deref() {
                        Some("true") => true,
                        Some("false") => false,
                        _ => return Err("--vfs-recorder needs true or false".into()),
                    })
                }
                _ => return Err(format!("unknown argument: {argument}").into()),
            }
        }
        let shape = shape.unwrap_or(FixtureShape::Wide);
        let population = population.ok_or("--population or --streams is required")?;
        let valid = match shape {
            FixtureShape::Wide => {
                matches!(population, 1_000 | 10_000 | 100_000) && population <= MAX_STREAMS
            }
            FixtureShape::Deep => population == MAX_DEEP_RECORDS,
            FixtureShape::ReceiptHeavy => population == MAX_RECEIPTS,
        };
        if !valid {
            return Err("unsupported bounded shape/population combination".into());
        }
        Ok((
            shape,
            population,
            stage_observer.ok_or("--stage-observer is required")?,
            vfs_recorder.unwrap_or(false),
        ))
    }

    async fn create_fixture(
        path: &Path,
        shape: FixtureShape,
        population: usize,
    ) -> Result<(), Box<dyn Error>> {
        let store = SqliteStore::open(SqliteOptions::new(path)).await?;
        store.close().await?;
        drop(store);
        let mut connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "DELETE")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", true)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let payload = [0x5a_u8; PAYLOAD_BYTES];
        let stream_count = match shape {
            FixtureShape::Wide => population,
            FixtureShape::Deep => 1,
            FixtureShape::ReceiptHeavy => population,
        };
        let mut receipt_bytes = 0usize;
        for index in 0..stream_count {
            let name = format!("stream-{index:06}");
            let incarnation_index = if shape == FixtureShape::ReceiptHeavy {
                index * 2
            } else {
                index
            };
            let incarnation = fixture_incarnation(incarnation_index);
            let tail = if shape == FixtureShape::Deep {
                population
            } else {
                1
            };
            transaction.execute(
                "INSERT INTO event_streams(public_id,incarnation,floor,tail,retired) VALUES(?1,?2,?3,?4,?5)",
                params![name, incarnation.as_slice(), 0_u64.to_be_bytes().as_slice(), (tail as u64).to_be_bytes().as_slice(), i64::from(shape == FixtureShape::ReceiptHeavy)],
            )?;
            let key = transaction.last_insert_rowid();
            let active_key;
            let latest;
            if shape == FixtureShape::ReceiptHeavy {
                let replacement = fixture_incarnation(index * 2 + 1);
                transaction.execute(
                    "INSERT INTO event_streams(public_id,incarnation,floor,tail,retired) VALUES(?1,?2,?3,?3,0)",
                    params![name, replacement.as_slice(), 0_u64.to_be_bytes().as_slice()],
                )?;
                active_key = transaction.last_insert_rowid();
                latest = replacement;
                let operation = format!("receipt-{index:06}");
                let charge = operation
                    .len()
                    .checked_add(name.len())
                    .and_then(|v| v.checked_add(256))
                    .ok_or("receipt charge overflow")?;
                receipt_bytes = receipt_bytes
                    .checked_add(charge)
                    .ok_or("receipt total overflow")?;
                transaction.execute(
                    "INSERT INTO lifecycle_receipts(operation_id,action,expected_public_id,expected_incarnation,replacement_incarnation,charge) VALUES(?1,1,?2,?3,?4,?5)",
                    params![operation, name, incarnation.as_slice(), replacement.as_slice(), charge as i64],
                )?;
            } else {
                active_key = key;
                latest = incarnation;
            }
            transaction.execute(
                "INSERT INTO event_stream_names(public_id,latest_incarnation,active_stream_key) VALUES(?1,?2,?3)",
                params![name, latest.as_slice(), active_key],
            )?;
            let record_count = if shape == FixtureShape::Deep {
                population
            } else {
                1
            };
            for offset in 1..=record_count {
                let event = if shape == FixtureShape::Deep {
                    format!("event-{offset:06}")
                } else {
                    format!("event-{index:06}")
                };
                transaction.execute(
                    "INSERT INTO event_records(stream_key,offset,event_id,schema_id,schema_version,payload) VALUES(?1,?2,?3,'restore.resource',1,?4)",
                    params![key, (offset as u64).to_be_bytes().as_slice(), event, payload.as_slice()],
                )?;
            }
        }
        if shape == FixtureShape::ReceiptHeavy {
            let retired_bytes = (0..population)
                .try_fold(0usize, |total, index| {
                    total.checked_add(format!("stream-{index:06}").len() + 256)
                })
                .ok_or("retired total overflow")?;
            transaction.execute(
                "UPDATE event_stream_metadata SET lifecycle_receipt_count=?1,lifecycle_receipt_bytes=?2,retired_lifetime_count=?1,retired_metadata_bytes=?3 WHERE singleton=1",
                params![population as i64, receipt_bytes as i64, retired_bytes as i64],
            )?;
        }
        transaction.commit()?;
        drop(connection);
        let store = SqliteStore::open(SqliteOptions::new(path)).await?;
        store.close().await?;
        drop(store);
        Ok(())
    }

    fn fixture_incarnation(index: usize) -> [u8; 16] {
        let mut value = [0x7c; 16];
        value[..8].copy_from_slice(&(index as u64).to_be_bytes());
        value
    }

    struct Validation {
        pages: usize,
        max_logical_bytes: usize,
        max_capacity: usize,
    }

    async fn validate_restore(
        manager: &SqliteRestoreManager,
        receipt: &event_stream::RestoreReceipt,
        shape: FixtureShape,
        population: usize,
    ) -> Result<Validation, Box<dyn Error>> {
        let mut after = None;
        let mut mappings = 0usize;
        let mut pages = 0usize;
        let mut max_logical_bytes = 0usize;
        let mut max_capacity = 0usize;
        let mut pending_receipt_original: Option<(usize, event_stream::StreamKey)> = None;
        let receipt_connection = if shape == FixtureShape::ReceiptHeavy {
            Some(Connection::open_with_flags(
                &receipt.destination,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?)
        } else {
            None
        };
        loop {
            let page = manager
                .read_mapping(
                    receipt.clone(),
                    after.clone(),
                    PageLimits {
                        max_records: 256,
                        max_bytes: 2 * 1024 * 1024,
                    },
                )
                .await?;
            if page.entries.is_empty() && !page.complete {
                return Err("mapping page made no progress".into());
            }
            pages += 1;
            max_capacity = max_capacity.max(page.entries.capacity());
            let logical_bytes = page
                .entries
                .iter()
                .try_fold(0usize, |total, mapping| {
                    total.checked_add(mapping.accounted_bytes()?)
                })
                .ok_or("mapping logical-byte overflow")?;
            max_logical_bytes = max_logical_bytes.max(logical_bytes);
            let restored = if shape == FixtureShape::ReceiptHeavy {
                None
            } else {
                Some(SqliteStore::open(SqliteOptions::new(&receipt.destination)).await?)
            };
            for mapping in &page.entries {
                let stream_index = match shape {
                    FixtureShape::ReceiptHeavy => mappings / 2,
                    _ => mappings,
                };
                let incarnation_index = match shape {
                    FixtureShape::ReceiptHeavy => mappings,
                    _ => stream_index,
                };
                let expected_name = format!("stream-{stream_index:06}");
                if mapping.old.id.as_str() != expected_name
                    || mapping.new.id != mapping.old.id
                    || mapping.old.incarnation.0 != fixture_incarnation(incarnation_index)
                    || mapping.new.incarnation == mapping.old.incarnation
                {
                    return Err(format!("mapping {mappings} is not exact").into());
                }
                match shape {
                    FixtureShape::Deep => {
                        validate_deep_records(restored.as_ref().unwrap(), &mapping.new, population)
                            .await?;
                    }
                    FixtureShape::Wide => {
                        validate_one_record(restored.as_ref().unwrap(), &mapping.new, stream_index)
                            .await?;
                    }
                    FixtureShape::ReceiptHeavy if mappings % 2 == 0 => {
                        pending_receipt_original = Some((stream_index, mapping.new.clone()));
                    }
                    FixtureShape::ReceiptHeavy => {
                        let (pending_index, original) = pending_receipt_original
                            .take()
                            .ok_or("receipt replacement mapping lacks its original")?;
                        if pending_index != stream_index {
                            return Err("receipt identity pair is not adjacent".into());
                        }
                        validate_receipt_row(
                            receipt_connection.as_ref().unwrap(),
                            stream_index,
                            &original,
                            &mapping.new,
                        )?;
                    }
                }
                mappings += 1;
            }
            if let Some(restored) = restored {
                restored.close().await?;
                drop(restored);
            }
            after = page.next_after;
            if page.complete {
                break;
            }
        }
        if pending_receipt_original.is_some() {
            return Err("receipt identity pair is incomplete".into());
        }
        let expected_mappings = shape.counts(population).0;
        if mappings != expected_mappings || receipt.mapping_count != expected_mappings as u64 {
            return Err(
                format!("expected {expected_mappings} mappings, observed {mappings}").into(),
            );
        }
        Ok(Validation {
            pages,
            max_logical_bytes,
            max_capacity,
        })
    }

    async fn validate_one_record(
        store: &SqliteStore,
        stream: &event_stream::StreamKey,
        index: usize,
    ) -> Result<(), Box<dyn Error>> {
        let records = store
            .read_range(
                stream,
                0,
                1,
                PageLimits {
                    max_records: 1,
                    max_bytes: 4096,
                },
            )
            .await?;
        if records.records.len() != 1
            || records.records[0].cursor.offset != 1
            || records.records[0].event.id.as_str() != format!("event-{index:06}")
            || records.records[0].event.schema.id.as_str() != "restore.resource"
            || records.records[0].event.schema.version != 1
            || records.records[0].event.payload.as_bytes() != [0x5a; PAYLOAD_BYTES]
        {
            return Err(format!("record {index} is not exact").into());
        }
        Ok(())
    }

    async fn validate_deep_records(
        store: &SqliteStore,
        stream: &event_stream::StreamKey,
        record_count: usize,
    ) -> Result<(), Box<dyn Error>> {
        let mut after = 0u64;
        while after < record_count as u64 {
            let through = (after + 256).min(record_count as u64);
            let records = store
                .read_range(
                    stream,
                    after,
                    through,
                    PageLimits {
                        max_records: 256,
                        max_bytes: 256 * 1024,
                    },
                )
                .await?;
            if records.records.len() != (through - after) as usize {
                return Err("deep replay page count is not exact".into());
            }
            for (index, record) in records.records.iter().enumerate() {
                let offset = after + index as u64 + 1;
                if record.cursor.offset != offset
                    || record.event.id.as_str() != format!("event-{offset:06}")
                    || record.event.schema.id.as_str() != "restore.resource"
                    || record.event.schema.version != 1
                    || record.event.payload.as_bytes() != [0x5a; PAYLOAD_BYTES]
                {
                    return Err(format!("deep record {offset} is not exact").into());
                }
            }
            after = through;
        }
        Ok(())
    }

    fn validate_receipt_row(
        connection: &Connection,
        index: usize,
        original: &event_stream::StreamKey,
        replacement: &event_stream::StreamKey,
    ) -> Result<(), Box<dyn Error>> {
        let operation = format!("receipt-{index:06}");
        let (action, name, expected, restored_replacement, charge): ReceiptRow = connection.query_row(
            "SELECT action,expected_public_id,expected_incarnation,replacement_incarnation,charge FROM lifecycle_receipts WHERE operation_id=?1",
            [operation.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )?;
        let expected_charge = operation.len() + original.id.as_str().len() + 256;
        if action != 1
            || name != original.id.as_str()
            || expected.as_slice() != original.incarnation.0
            || restored_replacement.as_slice() != replacement.incarnation.0
            || charge != expected_charge as i64
        {
            return Err(format!("receipt {index} is not exact").into());
        }
        let (floor, tail, retired, offset, event, schema, version, payload): RetiredRecordRow = connection.query_row(
            "SELECT s.floor,s.tail,s.retired,r.offset,r.event_id,r.schema_id,r.schema_version,r.payload
             FROM event_streams s JOIN event_records r ON r.stream_key=s.stream_key
             WHERE s.public_id=?1 AND s.incarnation=?2",
            params![original.id.as_str(), original.incarnation.0.as_slice()],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
        )?;
        if floor.as_slice() != 0_u64.to_be_bytes()
            || tail.as_slice() != 1_u64.to_be_bytes()
            || retired != 1
            || offset.as_slice() != 1_u64.to_be_bytes()
            || event != format!("event-{index:06}")
            || schema != "restore.resource"
            || version != 1
            || payload.as_slice() != [0x5a; PAYLOAD_BYTES]
        {
            return Err(format!("retired record {index} is not exact").into());
        }
        Ok(())
    }

    #[derive(Clone, Copy, Debug, Default)]
    struct ExactSizes {
        staging: u64,
        staging_allocated: u64,
        journal: u64,
        journal_allocated: u64,
        owner_directories: u64,
        owner_allocated: u64,
        request_directories: u64,
        request_allocated: u64,
    }

    fn exact_restore_file_sizes(root: &Path) -> ExactSizes {
        let mut sizes = ExactSizes::default();
        let Ok(entries) = std::fs::read_dir(root) else {
            return sizes;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
                continue;
            };
            if name.ends_with(".sqlite3-journal") {
                sizes.journal = sizes.journal.saturating_add(file_size(&path));
                sizes.journal_allocated = sizes
                    .journal_allocated
                    .saturating_add(allocated_size(&path));
            } else if name.starts_with(".event-stream-restore-") && name.ends_with(".owner") {
                sizes.owner_directories = sizes.owner_directories.saturating_add(1);
                sizes.owner_allocated = sizes.owner_allocated.saturating_add(allocated_size(&path));
                for request in std::fs::read_dir(&path)
                    .into_iter()
                    .flatten()
                    .take(2)
                    .flatten()
                {
                    sizes.request_directories = sizes.request_directories.saturating_add(1);
                    sizes.request_allocated = sizes
                        .request_allocated
                        .saturating_add(allocated_size(&request.path()));
                }
            } else if name.starts_with(".event-stream-restore-") && name.ends_with(".sqlite3") {
                sizes.staging = sizes.staging.saturating_add(file_size(&path));
                sizes.staging_allocated = sizes
                    .staging_allocated
                    .saturating_add(allocated_size(&path));
            }
        }
        sizes
    }

    fn ns(duration: Duration) -> u64 {
        duration.as_nanos().min(u128::from(u64::MAX)) as u64
    }

    fn encode_vfs(snapshot: Option<SqliteVfsSnapshot>) -> String {
        let Some(snapshot) = snapshot else {
            return "{\"available\":false,\"reason\":\"recorder_disabled\"}".into();
        };
        format!(
            "{{\"available\":true,\"scope\":\"SQLite_VFS_callbacks_during_restore_only_excludes_std_file_source_hashing_and_counts_xFetch_separately\",\"byte_scope\":\"requested_callback_bytes_including_non_OK_calls_not_physical_or_filesystem_allocated_bytes\",\"source\":{},\"target\":{},\"source_journal_or_wal\":{},\"target_journal_or_wal\":{},\"temporary\":{},\"other\":{},\"temporary_live_logical_bytes\":{},\"temporary_peak_logical_bytes\":{},\"optional_method_mismatches\":{},\"underlying_vfs_interface_version\":{},\"recorder_vfs_interface_version\":{},\"vfs_interface_note\":\"recorder_preserves_the_underlying_VFS_version_and_reports_any_per_file_SHM_or_fetch_surface_mismatch\",\"temporary_zero_open_interpretation\":\"zero_observed_VFS_temp_files_does_not_measure_SQLite_in_memory_temp_work_and_xDelete_without_open_flags_can_be_classified_other\"}}",
            encode_vfs_category(snapshot.source),
            encode_vfs_category(snapshot.target),
            encode_vfs_category(snapshot.source_journal),
            encode_vfs_category(snapshot.target_journal),
            encode_vfs_category(snapshot.temporary),
            encode_vfs_category(snapshot.other),
            snapshot.temporary_live_logical_bytes,
            snapshot.temporary_peak_logical_bytes,
            snapshot.optional_method_mismatches,
            snapshot.underlying_vfs_interface_version,
            snapshot.recorder_vfs_interface_version,
        )
    }

    fn encode_vfs_category(value: SqliteVfsCategorySnapshot) -> String {
        format!(
            "{{\"opens\":{},\"deletes\":{},\"read_calls\":{},\"read_requested_bytes\":{},\"write_calls\":{},\"write_requested_bytes\":{},\"truncate_calls\":{},\"sync_calls\":{},\"sync_elapsed_ns\":{},\"fetch_calls\":{},\"non_ok_callbacks\":{},\"short_read_callbacks\":{}}}",
            value.opens,
            value.deletes,
            value.read_calls,
            value.read_bytes,
            value.write_calls,
            value.write_bytes,
            value.truncate_calls,
            value.sync_calls,
            value.sync_nanos,
            value.fetch_calls,
            value.non_ok_callbacks,
            value.short_read_callbacks,
        )
    }

    fn option_delta(after: Option<u64>, before: Option<u64>) -> String {
        match (after, before) {
            (Some(after), Some(before)) => after.saturating_sub(before).to_string(),
            _ => "null".into(),
        }
    }

    fn option_value(value: Option<u64>) -> String {
        value.map_or_else(|| "null".into(), |value| value.to_string())
    }

    // tv_usec is i32 on macOS and i64 on 64-bit Linux; keep the portable conversion.
    #[allow(clippy::useless_conversion)]
    fn timeval_us(value: libc::timeval) -> i64 {
        value
            .tv_sec
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(value.tv_usec))
    }

    #[cfg(target_os = "macos")]
    fn max_rss_bytes(value: i64) -> u64 {
        value.max(0) as u64
    }

    #[cfg(not(target_os = "macos"))]
    fn max_rss_bytes(value: i64) -> u64 {
        (value.max(0) as u64).saturating_mul(1024)
    }
}

#[cfg(all(feature = "sqlite", feature = "test-support"))]
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    enabled::run().await
}

#[cfg(not(all(feature = "sqlite", feature = "test-support")))]
fn main() {
    eprintln!("restore_resource requires --features sqlite,test-support");
    std::process::exit(2);
}
