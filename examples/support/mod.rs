use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

struct CountingAllocator;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let replacement = unsafe { System.realloc(pointer, old, new_size) };
        if !replacement.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED.fetch_add(new_size as u64, Ordering::Relaxed);
            DEALLOCATED.fetch_add(old.size() as u64, Ordering::Relaxed);
        }
        replacement
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

#[derive(Clone, Copy)]
pub struct AllocationSnapshot {
    pub count: u64,
    pub allocated: u64,
    pub deallocated: u64,
}

impl AllocationSnapshot {
    pub fn read() -> Self {
        Self {
            count: ALLOCATIONS.load(Ordering::Relaxed),
            allocated: ALLOCATED.load(Ordering::Relaxed),
            deallocated: DEALLOCATED.load(Ordering::Relaxed),
        }
    }

    pub fn live(self) -> u64 {
        self.allocated.saturating_sub(self.deallocated)
    }
}

pub struct Sampler {
    stop: Arc<AtomicBool>,
    peak_rss: Arc<AtomicU64>,
    peak_live: Arc<AtomicU64>,
    samples: Arc<AtomicU64>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Sampler {
    pub fn start(enabled: bool) -> Option<Self> {
        enabled.then(|| {
            let stop = Arc::new(AtomicBool::new(false));
            let peak_rss = Arc::new(AtomicU64::new(0));
            let peak_live = Arc::new(AtomicU64::new(0));
            let samples = Arc::new(AtomicU64::new(0));
            let worker = {
                let stop = stop.clone();
                let peak_rss = peak_rss.clone();
                let peak_live = peak_live.clone();
                let samples = samples.clone();
                thread::spawn(move || {
                    while !stop.load(Ordering::Acquire) {
                        if let Some(rss) = current_rss_bytes() {
                            peak_rss.fetch_max(rss, Ordering::Relaxed);
                        }
                        peak_live.fetch_max(AllocationSnapshot::read().live(), Ordering::Relaxed);
                        samples.fetch_add(1, Ordering::Relaxed);
                        thread::sleep(Duration::from_millis(1));
                    }
                })
            };
            Self {
                stop,
                peak_rss,
                peak_live,
                samples,
                worker: Some(worker),
            }
        })
    }

    pub fn finish(mut self) -> SamplePeaks {
        self.stop.store(true, Ordering::Release);
        self.worker.take().unwrap().join().unwrap();
        SamplePeaks {
            rss: self.peak_rss.load(Ordering::Relaxed),
            rust_live: self.peak_live.load(Ordering::Relaxed),
            samples: self.samples.load(Ordering::Relaxed),
        }
    }
}

pub struct SamplePeaks {
    pub rss: u64,
    pub rust_live: u64,
    pub samples: u64,
}

#[derive(Clone, Copy)]
pub struct Usage {
    pub user_us: i64,
    pub system_us: i64,
    pub max_rss: u64,
}

impl Usage {
    pub fn read() -> std::io::Result<Self> {
        unsafe {
            let mut value: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut value) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Self {
                user_us: timeval_us(value.ru_utime),
                system_us: timeval_us(value.ru_stime),
                max_rss: max_rss_bytes(value.ru_maxrss),
            })
        }
    }
}

#[cfg(target_os = "macos")]
#[allow(deprecated)]
pub fn current_rss_bytes() -> Option<u64> {
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
pub fn current_rss_bytes() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages = stat.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page > 0).then(|| pages.saturating_mul(page as u64))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub fn current_rss_bytes() -> Option<u64> {
    None
}

fn timeval_us(value: libc::timeval) -> i64 {
    value
        .tv_sec
        .saturating_mul(1_000_000)
        .saturating_add(value.tv_usec as i64)
}

#[cfg(target_os = "macos")]
fn max_rss_bytes(value: libc::c_long) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(not(target_os = "macos"))]
fn max_rss_bytes(value: libc::c_long) -> u64 {
    u64::try_from(value).unwrap_or(0).saturating_mul(1024)
}
