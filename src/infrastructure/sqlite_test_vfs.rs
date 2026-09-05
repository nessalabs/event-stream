//! Opt-in SQLite VFS recorder for bounded evidence harnesses.
//!
//! This module is compiled only with `test-support`. It is never installed by
//! the production adapters. The forwarding shape is shared with the reviewed
//! SQLite fault VFS used by the integration tests.

use rusqlite::ffi;
use std::{
    ffi::{c_char, c_int, c_void, CStr},
    ptr,
    sync::{
        atomic::{AtomicPtr, AtomicU64, Ordering},
        OnceLock,
    },
    time::Instant,
};

const CATEGORY_COUNT: usize = 6;
const SOURCE: usize = 0;
const TARGET: usize = 1;
const SOURCE_JOURNAL: usize = 2;
const TARGET_JOURNAL: usize = 3;
const TEMP: usize = 4;
const OTHER: usize = 5;
const MAX_SYNC_TIMELINE_EVENTS: usize = 2048;
static VFS_NAME: &[u8] = b"event_stream_restore_record_vfs\0";
static REAL_VFS: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(ptr::null_mut());
static REAL_VFS_VERSION: AtomicU64 = AtomicU64::new(0);
static OPEN_FILES: AtomicU64 = AtomicU64::new(0);
static OBSERVATION_EPOCH: OnceLock<Instant> = OnceLock::new();
static SYNC_TIMELINE_COUNT: AtomicU64 = AtomicU64::new(0);

struct SyncTimelineSlot {
    category: AtomicU64,
    start_ns: AtomicU64,
    end_ns: AtomicU64,
}

impl SyncTimelineSlot {
    const fn new() -> Self {
        Self {
            category: AtomicU64::new(0),
            start_ns: AtomicU64::new(0),
            end_ns: AtomicU64::new(0),
        }
    }
}

static SYNC_TIMELINE: [SyncTimelineSlot; MAX_SYNC_TIMELINE_EVENTS] =
    [const { SyncTimelineSlot::new() }; MAX_SYNC_TIMELINE_EVENTS];

struct Counters {
    opens: AtomicU64,
    deletes: AtomicU64,
    read_calls: AtomicU64,
    read_bytes: AtomicU64,
    write_calls: AtomicU64,
    write_bytes: AtomicU64,
    truncate_calls: AtomicU64,
    sync_calls: AtomicU64,
    sync_nanos: AtomicU64,
    fetch_calls: AtomicU64,
    non_ok_callbacks: AtomicU64,
    short_read_callbacks: AtomicU64,
}

impl Counters {
    const fn new() -> Self {
        Self {
            opens: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            read_calls: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            write_bytes: AtomicU64::new(0),
            truncate_calls: AtomicU64::new(0),
            sync_calls: AtomicU64::new(0),
            sync_nanos: AtomicU64::new(0),
            fetch_calls: AtomicU64::new(0),
            non_ok_callbacks: AtomicU64::new(0),
            short_read_callbacks: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        for value in [
            &self.opens,
            &self.deletes,
            &self.read_calls,
            &self.read_bytes,
            &self.write_calls,
            &self.write_bytes,
            &self.truncate_calls,
            &self.sync_calls,
            &self.sync_nanos,
            &self.fetch_calls,
            &self.non_ok_callbacks,
            &self.short_read_callbacks,
        ] {
            value.store(0, Ordering::Relaxed);
        }
    }

    fn snapshot(&self) -> SqliteVfsCategorySnapshot {
        SqliteVfsCategorySnapshot {
            opens: self.opens.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            read_calls: self.read_calls.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_calls: self.write_calls.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
            truncate_calls: self.truncate_calls.load(Ordering::Relaxed),
            sync_calls: self.sync_calls.load(Ordering::Relaxed),
            sync_nanos: self.sync_nanos.load(Ordering::Relaxed),
            fetch_calls: self.fetch_calls.load(Ordering::Relaxed),
            non_ok_callbacks: self.non_ok_callbacks.load(Ordering::Relaxed),
            short_read_callbacks: self.short_read_callbacks.load(Ordering::Relaxed),
        }
    }
}

static COUNTERS: [Counters; CATEGORY_COUNT] = [const { Counters::new() }; CATEGORY_COUNT];
static TEMP_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static TEMP_PEAK_BYTES: AtomicU64 = AtomicU64::new(0);
static OPTIONAL_METHOD_MISMATCHES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteVfsCategorySnapshot {
    pub opens: u64,
    pub deletes: u64,
    pub read_calls: u64,
    pub read_bytes: u64,
    pub write_calls: u64,
    pub write_bytes: u64,
    pub truncate_calls: u64,
    pub sync_calls: u64,
    pub sync_nanos: u64,
    pub fetch_calls: u64,
    pub non_ok_callbacks: u64,
    pub short_read_callbacks: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteVfsSnapshot {
    pub source: SqliteVfsCategorySnapshot,
    pub target: SqliteVfsCategorySnapshot,
    pub source_journal: SqliteVfsCategorySnapshot,
    pub target_journal: SqliteVfsCategorySnapshot,
    pub temporary: SqliteVfsCategorySnapshot,
    pub other: SqliteVfsCategorySnapshot,
    pub temporary_live_logical_bytes: u64,
    pub temporary_peak_logical_bytes: u64,
    pub optional_method_mismatches: u64,
    pub underlying_vfs_interface_version: u64,
    pub recorder_vfs_interface_version: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct SqliteVfsSyncEvent {
    pub sequence: u64,
    pub category: u8,
    pub start_ns: u64,
    pub end_ns: u64,
}

#[derive(Debug, Default)]
pub struct SqliteVfsSyncTimeline {
    pub events: Vec<SqliteVfsSyncEvent>,
    pub dropped: u64,
}

/// Returns nanoseconds from one process-local epoch shared with sync events.
pub fn sqlite_vfs_observation_nanos() -> u64 {
    u64::try_from(
        OBSERVATION_EPOCH
            .get_or_init(Instant::now)
            .elapsed()
            .as_nanos(),
    )
    .unwrap_or(u64::MAX)
}

pub fn sqlite_vfs_sync_timeline_position() -> u64 {
    SYNC_TIMELINE_COUNT.load(Ordering::Acquire)
}

/// Copies the bounded sync callback timeline beginning at `after`.
pub fn sqlite_vfs_sync_timeline_since(after: u64) -> SqliteVfsSyncTimeline {
    let through = SYNC_TIMELINE_COUNT.load(Ordering::Acquire);
    let available_through = through.min(MAX_SYNC_TIMELINE_EVENTS as u64);
    let available_after = after.min(available_through);
    let mut events = Vec::with_capacity((available_through - available_after) as usize);
    for sequence in available_after..available_through {
        let slot = &SYNC_TIMELINE[sequence as usize];
        let end_ns = slot.end_ns.load(Ordering::Acquire);
        if end_ns == 0 {
            continue;
        }
        events.push(SqliteVfsSyncEvent {
            sequence,
            category: slot.category.load(Ordering::Relaxed) as u8,
            start_ns: slot.start_ns.load(Ordering::Relaxed),
            end_ns,
        });
    }
    let requested = through.saturating_sub(after);
    SqliteVfsSyncTimeline {
        dropped: requested.saturating_sub(events.len() as u64),
        events,
    }
}

/// Clears counters while no SQLite file is open through this VFS.
///
/// The caller must also prevent a concurrent open until this returns. The
/// recorder rejects reset when an observed handle is still live so per-file
/// logical sizes cannot be subtracted from a new counter generation.
pub fn reset_sqlite_vfs_recorder() -> Result<(), String> {
    if OPEN_FILES.load(Ordering::SeqCst) != 0 {
        return Err("reset SQLite recorder while a file is open".into());
    }
    for counters in &COUNTERS {
        counters.reset();
    }
    TEMP_LIVE_BYTES.store(0, Ordering::Relaxed);
    TEMP_PEAK_BYTES.store(0, Ordering::Relaxed);
    OPTIONAL_METHOD_MISMATCHES.store(0, Ordering::Relaxed);
    SYNC_TIMELINE_COUNT.store(0, Ordering::Release);
    Ok(())
}

pub fn sqlite_vfs_snapshot() -> SqliteVfsSnapshot {
    SqliteVfsSnapshot {
        source: COUNTERS[SOURCE].snapshot(),
        target: COUNTERS[TARGET].snapshot(),
        source_journal: COUNTERS[SOURCE_JOURNAL].snapshot(),
        target_journal: COUNTERS[TARGET_JOURNAL].snapshot(),
        temporary: COUNTERS[TEMP].snapshot(),
        other: COUNTERS[OTHER].snapshot(),
        temporary_live_logical_bytes: TEMP_LIVE_BYTES.load(Ordering::Relaxed),
        temporary_peak_logical_bytes: TEMP_PEAK_BYTES.load(Ordering::Relaxed),
        optional_method_mismatches: OPTIONAL_METHOD_MISMATCHES.load(Ordering::Relaxed),
        underlying_vfs_interface_version: REAL_VFS_VERSION.load(Ordering::Relaxed),
        recorder_vfs_interface_version: REAL_VFS_VERSION.load(Ordering::Relaxed),
    }
}

/// Installs the recorder as the process-default SQLite VFS.
///
/// Call once, before opening any SQLite connection in the fresh harness
/// process. The registration intentionally leaks its process-lifetime VFS.
pub fn install_sqlite_vfs_recorder() -> Result<(), String> {
    unsafe {
        if ffi::sqlite3_initialize() != ffi::SQLITE_OK {
            return Err("initialize SQLite before VFS registration".into());
        }
        let real = ffi::sqlite3_vfs_find(ptr::null());
        if real.is_null() {
            return Err("find SQLite default VFS".into());
        }
        if REAL_VFS
            .compare_exchange(ptr::null_mut(), real, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("SQLite recorder VFS is already installed".into());
        }
        REAL_VFS_VERSION.store((*real).iVersion as u64, Ordering::Relaxed);
        if std::mem::size_of::<WrappedFile>() % std::mem::align_of::<ffi::sqlite3_file>() != 0 {
            return Err("SQLite recorder file wrapper alignment".into());
        }
        let mut wrapper = Box::new(ptr::read(real));
        wrapper.zName = VFS_NAME.as_ptr().cast::<c_char>();
        wrapper.szOsFile = wrapper
            .szOsFile
            .checked_add(std::mem::size_of::<WrappedFile>() as c_int)
            .ok_or("SQLite recorder file wrapper size")?;
        wrapper.xOpen = Some(vfs_open);
        wrapper.xDelete = Some(vfs_delete);
        wrapper.xAccess = Some(vfs_access);
        wrapper.xFullPathname = Some(vfs_full_path);
        wrapper.xDlOpen = Some(vfs_dl_open);
        wrapper.xDlError = Some(vfs_dl_error);
        wrapper.xDlSym = Some(vfs_dl_sym);
        wrapper.xDlClose = Some(vfs_dl_close);
        wrapper.xRandomness = Some(vfs_random);
        wrapper.xSleep = Some(vfs_sleep);
        wrapper.xCurrentTime = Some(vfs_time);
        wrapper.xGetLastError = Some(vfs_last_error);
        wrapper.xCurrentTimeInt64 = (*real).xCurrentTimeInt64.map(|_| vfs_time_i64 as _);
        wrapper.xSetSystemCall = (*real).xSetSystemCall.map(|_| vfs_set_system_call as _);
        wrapper.xGetSystemCall = (*real).xGetSystemCall.map(|_| vfs_get_system_call as _);
        wrapper.xNextSystemCall = (*real).xNextSystemCall.map(|_| vfs_next_system_call as _);
        let wrapper = Box::into_raw(wrapper);
        if ffi::sqlite3_vfs_register(wrapper, 1) != ffi::SQLITE_OK {
            drop(Box::from_raw(wrapper));
            REAL_VFS.store(ptr::null_mut(), Ordering::SeqCst);
            return Err("register SQLite recorder VFS".into());
        }
    }
    reset_sqlite_vfs_recorder()?;
    Ok(())
}

#[repr(C)]
struct WrappedFile {
    base: ffi::sqlite3_file,
    category: c_int,
    logical_size: AtomicU64,
    methods: ffi::sqlite3_io_methods,
}

unsafe fn wrapped(file: *mut ffi::sqlite3_file) -> *mut WrappedFile {
    file.cast()
}

unsafe fn real_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    file.cast::<u8>()
        .add(std::mem::size_of::<WrappedFile>())
        .cast()
}

unsafe fn methods(file: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    &*(*real_file(file)).pMethods
}

unsafe fn category(file: *mut ffi::sqlite3_file) -> usize {
    (*wrapped(file)).category as usize
}

fn record_result(category: usize, result: c_int) -> c_int {
    if result != ffi::SQLITE_OK {
        COUNTERS[category]
            .non_ok_callbacks
            .fetch_add(1, Ordering::Relaxed);
        if result == ffi::SQLITE_IOERR_SHORT_READ {
            COUNTERS[category]
                .short_read_callbacks
                .fetch_add(1, Ordering::Relaxed);
        }
    }
    result
}

fn adjust_temp_size(file: *mut ffi::sqlite3_file, new_size: u64) {
    unsafe {
        if category(file) != TEMP {
            return;
        }
        let old = (*wrapped(file))
            .logical_size
            .swap(new_size, Ordering::Relaxed);
        let live = if new_size >= old {
            TEMP_LIVE_BYTES
                .fetch_add(new_size - old, Ordering::Relaxed)
                .saturating_add(new_size - old)
        } else {
            TEMP_LIVE_BYTES
                .fetch_sub(old - new_size, Ordering::Relaxed)
                .saturating_sub(old - new_size)
        };
        TEMP_PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
    }
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let category = category(file);
    if category == TEMP {
        adjust_temp_size(file, 0);
    }
    let result = (methods(file).xClose.unwrap())(real_file(file));
    if result == ffi::SQLITE_OK {
        OPEN_FILES.fetch_sub(1, Ordering::SeqCst);
    }
    record_result(category, result)
}

unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    out: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let category = category(file);
    COUNTERS[category]
        .read_calls
        .fetch_add(1, Ordering::Relaxed);
    COUNTERS[category]
        .read_bytes
        .fetch_add(u64::try_from(amount).unwrap_or(0), Ordering::Relaxed);
    let result = (methods(file).xRead.unwrap())(real_file(file), out, amount, offset);
    record_result(category, result)
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let category = category(file);
    COUNTERS[category]
        .write_calls
        .fetch_add(1, Ordering::Relaxed);
    COUNTERS[category]
        .write_bytes
        .fetch_add(u64::try_from(amount).unwrap_or(0), Ordering::Relaxed);
    let result = (methods(file).xWrite.unwrap())(real_file(file), input, amount, offset);
    if result == ffi::SQLITE_OK && category == TEMP {
        let end = u64::try_from(offset)
            .ok()
            .and_then(|offset| offset.checked_add(u64::try_from(amount).ok()?))
            .unwrap_or(u64::MAX);
        let old = (*wrapped(file)).logical_size.load(Ordering::Relaxed);
        if end > old {
            adjust_temp_size(file, end);
        }
    }
    record_result(category, result)
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    let category = category(file);
    COUNTERS[category]
        .truncate_calls
        .fetch_add(1, Ordering::Relaxed);
    let result = (methods(file).xTruncate.unwrap())(real_file(file), size);
    if result == ffi::SQLITE_OK && category == TEMP {
        adjust_temp_size(file, u64::try_from(size).unwrap_or(0));
    }
    record_result(category, result)
}

unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    let category = category(file);
    COUNTERS[category]
        .sync_calls
        .fetch_add(1, Ordering::Relaxed);
    let sequence = SYNC_TIMELINE_COUNT.fetch_add(1, Ordering::AcqRel);
    let timeline_start = sqlite_vfs_observation_nanos();
    let started = Instant::now();
    let result = (methods(file).xSync.unwrap())(real_file(file), flags);
    let timeline_end = sqlite_vfs_observation_nanos();
    COUNTERS[category].sync_nanos.fetch_add(
        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        Ordering::Relaxed,
    );
    if sequence < MAX_SYNC_TIMELINE_EVENTS as u64 {
        let slot = &SYNC_TIMELINE[sequence as usize];
        slot.category.store(
            u64::try_from(category).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        slot.start_ns.store(timeline_start, Ordering::Relaxed);
        slot.end_ns.store(timeline_end.max(1), Ordering::Release);
    }
    record_result(category, result)
}

macro_rules! forward_io {
    ($name:ident($($arg:ident:$type:ty),*) -> $result:ty, $field:ident) => {
        unsafe extern "C" fn $name(file:*mut ffi::sqlite3_file,$($arg:$type),*)->$result {
            (methods(file).$field.unwrap())(real_file(file),$($arg),*)
        }
    };
}

forward_io!(x_file_size(size:*mut ffi::sqlite3_int64)->c_int,xFileSize);
forward_io!(x_lock(lock:c_int)->c_int,xLock);
forward_io!(x_unlock(lock:c_int)->c_int,xUnlock);
forward_io!(x_reserved(out:*mut c_int)->c_int,xCheckReservedLock);
forward_io!(x_control(op:c_int,arg:*mut c_void)->c_int,xFileControl);

unsafe extern "C" fn x_sector(file: *mut ffi::sqlite3_file) -> c_int {
    (methods(file).xSectorSize.unwrap())(real_file(file))
}
unsafe extern "C" fn x_device(file: *mut ffi::sqlite3_file) -> c_int {
    (methods(file).xDeviceCharacteristics.unwrap())(real_file(file))
}
unsafe extern "C" fn x_shm_map(
    file: *mut ffi::sqlite3_file,
    page: c_int,
    size: c_int,
    extend: c_int,
    out: *mut *mut c_void,
) -> c_int {
    (methods(file).xShmMap.unwrap())(real_file(file), page, size, extend, out)
}
unsafe extern "C" fn x_shm_lock(
    file: *mut ffi::sqlite3_file,
    offset: c_int,
    count: c_int,
    flags: c_int,
) -> c_int {
    (methods(file).xShmLock.unwrap())(real_file(file), offset, count, flags)
}
unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    (methods(file).xShmBarrier.unwrap())(real_file(file))
}
unsafe extern "C" fn x_shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    (methods(file).xShmUnmap.unwrap())(real_file(file), delete)
}
unsafe extern "C" fn x_fetch(
    file: *mut ffi::sqlite3_file,
    offset: ffi::sqlite3_int64,
    amount: c_int,
    out: *mut *mut c_void,
) -> c_int {
    let category = category(file);
    COUNTERS[category]
        .fetch_calls
        .fetch_add(1, Ordering::Relaxed);
    let result = (methods(file).xFetch.unwrap())(real_file(file), offset, amount, out);
    record_result(category, result)
}
unsafe extern "C" fn x_unfetch(
    file: *mut ffi::sqlite3_file,
    offset: ffi::sqlite3_int64,
    pointer: *mut c_void,
) -> c_int {
    (methods(file).xUnfetch.unwrap())(real_file(file), offset, pointer)
}

fn wrapper_methods(real: &ffi::sqlite3_io_methods) -> ffi::sqlite3_io_methods {
    let mut selected = *real;
    selected.xClose = Some(x_close);
    selected.xRead = Some(x_read);
    selected.xWrite = Some(x_write);
    selected.xTruncate = Some(x_truncate);
    selected.xSync = Some(x_sync);
    selected.xFileSize = Some(x_file_size);
    selected.xLock = Some(x_lock);
    selected.xUnlock = Some(x_unlock);
    selected.xCheckReservedLock = Some(x_reserved);
    selected.xFileControl = Some(x_control);
    selected.xSectorSize = Some(x_sector);
    selected.xDeviceCharacteristics = Some(x_device);
    selected.xShmMap = real.xShmMap.map(|_| x_shm_map as _);
    selected.xShmLock = real.xShmLock.map(|_| x_shm_lock as _);
    selected.xShmBarrier = real.xShmBarrier.map(|_| x_shm_barrier as _);
    selected.xShmUnmap = real.xShmUnmap.map(|_| x_shm_unmap as _);
    selected.xFetch = real.xFetch.map(|_| x_fetch as _);
    selected.xUnfetch = real.xUnfetch.map(|_| x_unfetch as _);
    selected
}

fn classify(name: *const c_char, flags: c_int) -> usize {
    let temporary = ffi::SQLITE_OPEN_TEMP_DB
        | ffi::SQLITE_OPEN_TEMP_JOURNAL
        | ffi::SQLITE_OPEN_TRANSIENT_DB
        | ffi::SQLITE_OPEN_SUBJOURNAL;
    if flags & temporary != 0 || name.is_null() {
        return TEMP;
    }
    let bytes = unsafe { CStr::from_ptr(name) }.to_bytes();
    let source = bytes
        .windows(b"source.sqlite3".len())
        .any(|value| value == b"source.sqlite3");
    let journal = flags & (ffi::SQLITE_OPEN_MAIN_JOURNAL | ffi::SQLITE_OPEN_WAL) != 0
        || bytes.ends_with(b"-journal")
        || bytes.ends_with(b"-wal");
    if journal {
        return if source {
            SOURCE_JOURNAL
        } else {
            TARGET_JOURNAL
        };
    }
    if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        return if source { SOURCE } else { TARGET };
    }
    OTHER
}

unsafe extern "C" fn vfs_open(
    _: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    (*file).pMethods = ptr::null();
    let category = classify(name, flags);
    (*wrapped(file)).category = category as c_int;
    ptr::write(&mut (*wrapped(file)).logical_size, AtomicU64::new(0));
    let real = REAL_VFS.load(Ordering::SeqCst);
    let result = ((*real).xOpen.unwrap())(real, name, real_file(file), flags, out_flags);
    if result == ffi::SQLITE_OK {
        let real_methods = methods(file);
        let selected = wrapper_methods(real_methods);
        let same_optional_surface = selected.iVersion == real_methods.iVersion
            && selected.xShmMap.is_some() == real_methods.xShmMap.is_some()
            && selected.xShmLock.is_some() == real_methods.xShmLock.is_some()
            && selected.xShmBarrier.is_some() == real_methods.xShmBarrier.is_some()
            && selected.xShmUnmap.is_some() == real_methods.xShmUnmap.is_some()
            && selected.xFetch.is_some() == real_methods.xFetch.is_some()
            && selected.xUnfetch.is_some() == real_methods.xUnfetch.is_some();
        if !same_optional_surface {
            OPTIONAL_METHOD_MISMATCHES.fetch_add(1, Ordering::Relaxed);
        }
        ptr::write(&mut (*wrapped(file)).methods, selected);
        (*file).pMethods = &raw const (*wrapped(file)).methods;
        COUNTERS[category].opens.fetch_add(1, Ordering::Relaxed);
        OPEN_FILES.fetch_add(1, Ordering::SeqCst);
        if category == TEMP {
            let mut size = 0;
            if (methods(file).xFileSize.unwrap())(real_file(file), &mut size) == ffi::SQLITE_OK {
                adjust_temp_size(file, u64::try_from(size).unwrap_or(0));
            }
        }
    } else {
        COUNTERS[category]
            .non_ok_callbacks
            .fetch_add(1, Ordering::Relaxed);
        if !(*real_file(file)).pMethods.is_null() {
            ((*(*real_file(file)).pMethods).xClose.unwrap())(real_file(file));
            (*real_file(file)).pMethods = ptr::null();
        }
    }
    result
}

unsafe fn real_vfs() -> *mut ffi::sqlite3_vfs {
    REAL_VFS.load(Ordering::SeqCst)
}

unsafe extern "C" fn vfs_delete(
    _: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync: c_int,
) -> c_int {
    let category = classify(name, 0);
    COUNTERS[category].deletes.fetch_add(1, Ordering::Relaxed);
    let real = real_vfs();
    record_result(category, ((*real).xDelete.unwrap())(real, name, sync))
}

unsafe extern "C" fn vfs_access(
    _: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    out: *mut c_int,
) -> c_int {
    let real = real_vfs();
    ((*real).xAccess.unwrap())(real, name, flags, out)
}

unsafe extern "C" fn vfs_full_path(
    _: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    let real = real_vfs();
    ((*real).xFullPathname.unwrap())(real, name, size, out)
}

unsafe extern "C" fn vfs_dl_open(_: *mut ffi::sqlite3_vfs, name: *const c_char) -> *mut c_void {
    let real = real_vfs();
    ((*real).xDlOpen.unwrap())(real, name)
}

unsafe extern "C" fn vfs_dl_error(_: *mut ffi::sqlite3_vfs, size: c_int, out: *mut c_char) {
    let real = real_vfs();
    ((*real).xDlError.unwrap())(real, size, out)
}

unsafe extern "C" fn vfs_dl_sym(
    _: *mut ffi::sqlite3_vfs,
    handle: *mut c_void,
    name: *const c_char,
) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
    let real = real_vfs();
    ((*real).xDlSym.unwrap())(real, handle, name)
}

unsafe extern "C" fn vfs_dl_close(_: *mut ffi::sqlite3_vfs, handle: *mut c_void) {
    let real = real_vfs();
    ((*real).xDlClose.unwrap())(real, handle)
}

unsafe extern "C" fn vfs_random(_: *mut ffi::sqlite3_vfs, size: c_int, out: *mut c_char) -> c_int {
    let real = real_vfs();
    ((*real).xRandomness.unwrap())(real, size, out)
}

unsafe extern "C" fn vfs_sleep(_: *mut ffi::sqlite3_vfs, micros: c_int) -> c_int {
    let real = real_vfs();
    ((*real).xSleep.unwrap())(real, micros)
}

unsafe extern "C" fn vfs_time(_: *mut ffi::sqlite3_vfs, out: *mut f64) -> c_int {
    let real = real_vfs();
    ((*real).xCurrentTime.unwrap())(real, out)
}

unsafe extern "C" fn vfs_last_error(
    _: *mut ffi::sqlite3_vfs,
    size: c_int,
    out: *mut c_char,
) -> c_int {
    let real = real_vfs();
    ((*real).xGetLastError.unwrap())(real, size, out)
}

unsafe extern "C" fn vfs_time_i64(_: *mut ffi::sqlite3_vfs, out: *mut ffi::sqlite3_int64) -> c_int {
    let real = real_vfs();
    ((*real).xCurrentTimeInt64.unwrap())(real, out)
}

unsafe extern "C" fn vfs_set_system_call(
    _: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    call: ffi::sqlite3_syscall_ptr,
) -> c_int {
    let real = real_vfs();
    ((*real).xSetSystemCall.unwrap())(real, name, call)
}

unsafe extern "C" fn vfs_get_system_call(
    _: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> ffi::sqlite3_syscall_ptr {
    let real = real_vfs();
    ((*real).xGetSystemCall.unwrap())(real, name)
}

unsafe extern "C" fn vfs_next_system_call(
    _: *mut ffi::sqlite3_vfs,
    name: *const c_char,
) -> *const c_char {
    let real = real_vfs();
    ((*real).xNextSystemCall.unwrap())(real, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_optional_methods_are_not_advertised() {
        let mut real: ffi::sqlite3_io_methods = unsafe { std::mem::zeroed() };
        real.iVersion = 3;
        real.xShmMap = Some(x_shm_map);
        real.xShmBarrier = Some(x_shm_barrier);
        real.xFetch = None;
        real.xUnfetch = Some(x_unfetch);

        let selected = wrapper_methods(&real);
        assert_eq!(selected.iVersion, 3);
        assert!(selected.xShmMap.is_some());
        assert!(selected.xShmLock.is_none());
        assert!(selected.xShmBarrier.is_some());
        assert!(selected.xFetch.is_none());
        assert!(selected.xUnfetch.is_some());
    }
}
