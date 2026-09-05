#![cfg(feature = "sqlite")]

//! Test-only forwarding VFS. Unsafe code is confined to SQLite's C ABI boundary;
//! every callback forwards to the bundled default VFS after translating the file
//! pointer. Production code and filesystem behavior are not replaced.

use event_stream::{
    infrastructure::{
        SqliteFailureInjection, SqliteOptions, SqliteRestoreBackend, SqliteRestoreManager,
        SqliteStore,
    },
    *,
};
use rusqlite::{ffi, Connection};
use std::{
    ffi::{c_char, c_int, c_void},
    fs,
    path::PathBuf,
    ptr,
    sync::{
        atomic::{AtomicI64, AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering},
        Once,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(target_os = "macos")]
use std::{io::Write, os::unix::fs::MetadataExt, process::Command};

const NONE: u8 = 0;
const FAIL_WRITE_ONCE: u8 = 1;
const FAIL_SYNC_ONCE: u8 = 2;
const FAIL_MAIN_WRITES: u8 = 3;
const FAIL_ROLLBACK_TRUNCATE: u8 = 4;
static FAULT: AtomicU8 = AtomicU8::new(NONE);
static WRITES: AtomicUsize = AtomicUsize::new(0);
static SYNCS: AtomicUsize = AtomicUsize::new(0);
static WRITE_FAULTS: AtomicUsize = AtomicUsize::new(0);
static SYNC_FAULTS: AtomicUsize = AtomicUsize::new(0);
static MAIN_WRITE_CALLS: AtomicUsize = AtomicUsize::new(0);
static MAIN_WRITE_BYTES: AtomicU64 = AtomicU64::new(0);
static MAIN_WRITE_SUCCESSES: AtomicUsize = AtomicUsize::new(0);
static JOURNAL_WRITE_CALLS: AtomicUsize = AtomicUsize::new(0);
static JOURNAL_WRITE_BYTES: AtomicU64 = AtomicU64::new(0);
static MAIN_SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
static MAIN_SYNC_NANOS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_SYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
static JOURNAL_SYNC_NANOS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_SYNC_SUCCESSES: AtomicUsize = AtomicUsize::new(0);
static ROLLBACK_HEADER_STEP: AtomicU8 = AtomicU8::new(0);
static ROLLBACK_HEADER_DB_PAGES: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_HEADER_PAGE_SIZE: AtomicU64 = AtomicU64::new(0);
static ROLLBACK_EXPECTED_SIZE: AtomicI64 = AtomicI64::new(-1);
static ROLLBACK_BASE_MAIN_WRITES: AtomicUsize = AtomicUsize::new(0);
static ROLLBACK_BASE_JOURNAL_SYNCS: AtomicUsize = AtomicUsize::new(0);
static ROLLBACK_TRUNCATE_FAULTS: AtomicUsize = AtomicUsize::new(0);
static REAL_VFS: AtomicPtr<ffi::sqlite3_vfs> = AtomicPtr::new(ptr::null_mut());
static INSTALL: Once = Once::new();
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static VFS_NAME: &[u8] = b"event_stream_fault_vfs\0";

struct FaultReset;
impl FaultReset {
    fn arm(mode: u8) -> Self {
        assert_eq!(FAULT.swap(mode, Ordering::SeqCst), NONE);
        Self
    }
}
impl Drop for FaultReset {
    fn drop(&mut self) {
        FAULT.store(NONE, Ordering::SeqCst);
        ROLLBACK_HEADER_STEP.store(0, Ordering::SeqCst);
        ROLLBACK_EXPECTED_SIZE.store(-1, Ordering::SeqCst);
    }
}

#[repr(C)]
struct WrappedFile {
    base: ffi::sqlite3_file,
    flags: c_int,
}

unsafe fn real_file(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    file.cast::<u8>()
        .add(std::mem::size_of::<WrappedFile>())
        .cast()
}
unsafe fn file_flags(file: *mut ffi::sqlite3_file) -> c_int {
    (*(file.cast::<WrappedFile>())).flags
}
unsafe fn methods(file: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    &*(*real_file(file)).pMethods
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    (methods(file).xClose.unwrap())(real_file(file))
}
unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    out: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    let result = (methods(file).xRead.unwrap())(real_file(file), out, amount, offset);
    if result == ffi::SQLITE_OK
        && FAULT.load(Ordering::SeqCst) == FAIL_ROLLBACK_TRUNCATE
        && file_flags(file) & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0
        && amount == 4
    {
        let step = ROLLBACK_HEADER_STEP.load(Ordering::SeqCst);
        let expected_offset = [8_i64, 12, 16, 20, 24].get(usize::from(step)).copied();
        if expected_offset == Some(offset) {
            let bytes = std::slice::from_raw_parts(out.cast::<u8>(), 4);
            let value = u64::from(u32::from_be_bytes(bytes.try_into().unwrap()));
            if offset == 16 {
                ROLLBACK_HEADER_DB_PAGES.store(value, Ordering::SeqCst);
            } else if offset == 24 {
                ROLLBACK_HEADER_PAGE_SIZE.store(value, Ordering::SeqCst);
            }
            ROLLBACK_HEADER_STEP.store(step + 1, Ordering::SeqCst);
        } else if offset == 8 {
            ROLLBACK_HEADER_STEP.store(1, Ordering::SeqCst);
        } else {
            ROLLBACK_HEADER_STEP.store(0, Ordering::SeqCst);
        }
    }
    result
}
unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    input: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    WRITES.fetch_add(1, Ordering::SeqCst);
    let flags = file_flags(file);
    let bytes = u64::try_from(amount).unwrap_or(0);
    if flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        MAIN_WRITE_CALLS.fetch_add(1, Ordering::SeqCst);
        MAIN_WRITE_BYTES.fetch_add(bytes, Ordering::SeqCst);
    } else if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0 {
        JOURNAL_WRITE_CALLS.fetch_add(1, Ordering::SeqCst);
        JOURNAL_WRITE_BYTES.fetch_add(bytes, Ordering::SeqCst);
    }
    let mode = FAULT.load(Ordering::SeqCst);
    if (mode == FAIL_MAIN_WRITES && flags & ffi::SQLITE_OPEN_MAIN_DB != 0)
        || (mode == FAIL_WRITE_ONCE
            && FAULT
                .compare_exchange(FAIL_WRITE_ONCE, NONE, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok())
    {
        WRITE_FAULTS.fetch_add(1, Ordering::SeqCst);
        return ffi::SQLITE_FULL;
    }
    let result = (methods(file).xWrite.unwrap())(real_file(file), input, amount, offset);
    if result == ffi::SQLITE_OK && flags & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        MAIN_WRITE_SUCCESSES.fetch_add(1, Ordering::SeqCst);
    }
    result
}
unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    if FAULT.load(Ordering::SeqCst) == FAIL_ROLLBACK_TRUNCATE
        && file_flags(file) & ffi::SQLITE_OPEN_MAIN_DB != 0
        && ROLLBACK_HEADER_STEP.load(Ordering::SeqCst) == 5
        && size == ROLLBACK_EXPECTED_SIZE.load(Ordering::SeqCst)
        && ROLLBACK_HEADER_DB_PAGES
            .load(Ordering::SeqCst)
            .checked_mul(ROLLBACK_HEADER_PAGE_SIZE.load(Ordering::SeqCst))
            == u64::try_from(size).ok()
        && MAIN_WRITE_SUCCESSES.load(Ordering::SeqCst)
            > ROLLBACK_BASE_MAIN_WRITES.load(Ordering::SeqCst)
        && JOURNAL_SYNC_SUCCESSES.load(Ordering::SeqCst)
            > ROLLBACK_BASE_JOURNAL_SYNCS.load(Ordering::SeqCst)
    {
        ROLLBACK_TRUNCATE_FAULTS.fetch_add(1, Ordering::SeqCst);
        FAULT.store(NONE, Ordering::SeqCst);
        return ffi::SQLITE_IOERR_TRUNCATE;
    }
    (methods(file).xTruncate.unwrap())(real_file(file), size)
}
unsafe extern "C" fn x_sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    SYNCS.fetch_add(1, Ordering::SeqCst);
    let file_kind = file_flags(file);
    if file_kind & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        MAIN_SYNC_CALLS.fetch_add(1, Ordering::SeqCst);
    } else if file_kind & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0 {
        JOURNAL_SYNC_CALLS.fetch_add(1, Ordering::SeqCst);
    }
    if FAULT
        .compare_exchange(FAIL_SYNC_ONCE, NONE, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
    {
        SYNC_FAULTS.fetch_add(1, Ordering::SeqCst);
        return ffi::SQLITE_IOERR_FSYNC;
    }
    let started = Instant::now();
    let result = (methods(file).xSync.unwrap())(real_file(file), flags);
    let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    if file_kind & ffi::SQLITE_OPEN_MAIN_DB != 0 {
        MAIN_SYNC_NANOS.fetch_add(elapsed, Ordering::SeqCst);
    } else if file_kind & ffi::SQLITE_OPEN_MAIN_JOURNAL != 0 {
        JOURNAL_SYNC_NANOS.fetch_add(elapsed, Ordering::SeqCst);
        if result == ffi::SQLITE_OK {
            JOURNAL_SYNC_SUCCESSES.fetch_add(1, Ordering::SeqCst);
        }
    }
    result
}
unsafe extern "C" fn x_file_size(
    file: *mut ffi::sqlite3_file,
    size: *mut ffi::sqlite3_int64,
) -> c_int {
    (methods(file).xFileSize.unwrap())(real_file(file), size)
}
unsafe extern "C" fn x_lock(file: *mut ffi::sqlite3_file, lock: c_int) -> c_int {
    (methods(file).xLock.unwrap())(real_file(file), lock)
}
unsafe extern "C" fn x_unlock(file: *mut ffi::sqlite3_file, lock: c_int) -> c_int {
    (methods(file).xUnlock.unwrap())(real_file(file), lock)
}
unsafe extern "C" fn x_reserved(file: *mut ffi::sqlite3_file, out: *mut c_int) -> c_int {
    (methods(file).xCheckReservedLock.unwrap())(real_file(file), out)
}
unsafe extern "C" fn x_control(file: *mut ffi::sqlite3_file, op: c_int, arg: *mut c_void) -> c_int {
    (methods(file).xFileControl.unwrap())(real_file(file), op, arg)
}
unsafe extern "C" fn x_sector(file: *mut ffi::sqlite3_file) -> c_int {
    (methods(file).xSectorSize.unwrap())(real_file(file))
}
unsafe extern "C" fn x_device(file: *mut ffi::sqlite3_file) -> c_int {
    (methods(file).xDeviceCharacteristics.unwrap())(real_file(file))
}

static IO: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 1,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_reserved),
    xFileControl: Some(x_control),
    xSectorSize: Some(x_sector),
    xDeviceCharacteristics: Some(x_device),
    xShmMap: None,
    xShmLock: None,
    xShmBarrier: None,
    xShmUnmap: None,
    xFetch: None,
    xUnfetch: None,
};

unsafe extern "C" fn vfs_open(
    _: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    (*file).pMethods = ptr::null();
    (*(file.cast::<WrappedFile>())).flags = flags;
    let real = REAL_VFS.load(Ordering::SeqCst);
    let result = ((*real).xOpen.unwrap())(real, name, real_file(file), flags, out_flags);
    if result == ffi::SQLITE_OK {
        (*file).pMethods = &IO;
    } else if !(*real_file(file)).pMethods.is_null() {
        // https://www.sqlite.org/c3ref/vfs.html requires a valid or null
        // pMethods after failed xOpen, so close any initialized real handle.
        ((*(*real_file(file)).pMethods).xClose.unwrap())(real_file(file));
        (*file).pMethods = ptr::null();
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
    let real = real_vfs();
    ((*real).xDelete.unwrap())(real, name, sync)
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

unsafe fn install_fault_vfs() {
    assert_eq!(ffi::sqlite3_initialize(), ffi::SQLITE_OK);
    let real = ffi::sqlite3_vfs_find(ptr::null());
    assert!(!real.is_null());
    REAL_VFS.store(real, Ordering::SeqCst);
    assert!(std::mem::size_of::<WrappedFile>() % std::mem::align_of::<ffi::sqlite3_file>() == 0);
    let mut wrapper = Box::new(ptr::read(real));
    wrapper.zName = VFS_NAME.as_ptr().cast::<c_char>();
    wrapper.szOsFile = wrapper
        .szOsFile
        .checked_add(std::mem::size_of::<WrappedFile>() as c_int)
        .unwrap();
    wrapper.xOpen = Some(vfs_open);
    wrapper.iVersion = 1;
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
    wrapper.xCurrentTimeInt64 = None;
    wrapper.xSetSystemCall = None;
    wrapper.xGetSystemCall = None;
    wrapper.xNextSystemCall = None;
    // SQLite retains the registration for the process lifetime. REAL_VFS points
    // at SQLite's equally long-lived default VFS and IO is static, so delegated
    // file callbacks cannot outlive either target.
    let wrapper = Box::into_raw(wrapper);
    assert_eq!(ffi::sqlite3_vfs_register(wrapper, 1), ffi::SQLITE_OK);
}

struct TempDb {
    dir: PathBuf,
    path: PathBuf,
}
impl TempDb {
    fn new(name: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "event-stream-vfs-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        Self {
            path: dir.join("events.sqlite3"),
            dir,
        }
    }
}
impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

#[cfg(target_os = "macos")]
struct MountedImage {
    root: PathBuf,
    mount: PathBuf,
    attached: bool,
}
#[cfg(target_os = "macos")]
impl MountedImage {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "event-stream-full-image-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mount = root.join("mount");
        fs::create_dir_all(&mount).unwrap();
        let image = root.join("disk.dmg");
        let mut mounted = Self {
            root,
            mount,
            attached: false,
        };
        let created = Command::new("hdiutil")
            .args([
                "create",
                "-quiet",
                "-size",
                "64m",
                "-fs",
                "APFS",
                "-volname",
                "EventStreamFull",
            ])
            .arg(&image)
            .status()
            .unwrap();
        assert!(created.success(), "create private APFS image");
        let attached = Command::new("hdiutil")
            .args(["attach", "-quiet", "-nobrowse", "-mountpoint"])
            .arg(&mounted.mount)
            .arg(&image)
            .status()
            .unwrap();
        assert!(attached.success(), "attach private APFS image");
        mounted.attached = true;
        mounted
    }
}
#[cfg(target_os = "macos")]
impl Drop for MountedImage {
    fn drop(&mut self) {
        if self.attached {
            let detached = Command::new("hdiutil")
                .args(["detach", "-quiet"])
                .arg(&self.mount)
                .status()
                .is_ok_and(|status| status.success());
            if !detached {
                eprintln!(
                    "failed to detach private filesystem image; preserved at {}",
                    self.root.display()
                );
                return;
            }
        }
        if let Err(error) = fs::remove_dir_all(&self.root) {
            eprintln!(
                "failed to remove image directory {}: {error}",
                self.root.display()
            );
        }
    }
}

fn event(id: &str, bytes: &[u8]) -> NewEvent {
    NewEvent {
        id: EventId::new(id).unwrap(),
        schema: SchemaRef {
            id: SchemaId::new("fault.bytes").unwrap(),
            version: 1,
        },
        payload: Payload::copy_from_slice(bytes),
    }
}

fn grouped_batch(stream: &StreamKey, first: &[u8], second: &[u8]) -> AppendBatch {
    AppendBatch::new(
        vec![
            AppendRequest {
                stream: stream.clone(),
                event: event("group-a", first),
            },
            AppendRequest {
                stream: stream.clone(),
                event: event("group-a", first),
            },
            AppendRequest {
                stream: stream.clone(),
                event: event("group-b", second),
            },
        ],
        AppendBatchLimits {
            max_records: 3,
            max_bytes: 2 * 1024 * 1024,
        },
    )
    .unwrap()
}

async fn assert_recovered_group(
    store: &SqliteStore,
    stream: &StreamKey,
    batch: &AppendBatch,
    outcomes: &[Result<AppendReceipt>],
    first: &[u8],
    second: &[u8],
    schedule: &str,
) {
    batch.validate_results(outcomes).unwrap();
    let a = store
        .lookup_event(stream, &EventId::new("group-a").unwrap())
        .await
        .unwrap();
    let b = store
        .lookup_event(stream, &EventId::new("group-b").unwrap())
        .await
        .unwrap();
    match (a, b) {
        (None, None) => {
            assert!(
                outcomes.iter().all(Result::is_err),
                "{schedule}: rolled-back group returned a successful receipt: {outcomes:?}"
            );
            assert!(
                !outcomes.iter().any(|outcome| matches!(
                    outcome,
                    Ok(receipt) if receipt.kind == AppendKind::Deduplicated
                )),
                "{schedule}: same-group retry was reported as durable after rollback"
            );
            assert_eq!(store.bounds(stream).await.unwrap().tail.offset, 0);
        }
        (Some(a), Some(b)) => {
            assert_eq!(a.cursor.offset, 1, "{schedule}");
            assert_eq!(a.event, event("group-a", first), "{schedule}");
            assert_eq!(b.cursor.offset, 2, "{schedule}");
            assert_eq!(b.event, event("group-b", second), "{schedule}");
            let page = store
                .read_range(
                    stream,
                    0,
                    2,
                    PageLimits {
                        max_records: 2,
                        max_bytes: 2 * 1024 * 1024,
                    },
                )
                .await
                .unwrap();
            assert_eq!(page.records.as_slice(), [a, b], "{schedule}");
            assert!(page.complete, "{schedule}");
        }
        partial => panic!("{schedule}: grouped transaction recovered partially: {partial:?}"),
    }
}

#[tokio::test]
async fn successful_append_reports_normal_vfs_write_and_sync_scope() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });
    let db = TempDb::new("normal-vfs");
    let store = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("normal-vfs").unwrap())
        .await
        .unwrap();
    let main_writes_before = MAIN_WRITE_CALLS.load(Ordering::SeqCst);
    let main_bytes_before = MAIN_WRITE_BYTES.load(Ordering::SeqCst);
    let journal_writes_before = JOURNAL_WRITE_CALLS.load(Ordering::SeqCst);
    let journal_bytes_before = JOURNAL_WRITE_BYTES.load(Ordering::SeqCst);
    let main_syncs_before = MAIN_SYNC_CALLS.load(Ordering::SeqCst);
    let main_sync_ns_before = MAIN_SYNC_NANOS.load(Ordering::SeqCst);
    let journal_syncs_before = JOURNAL_SYNC_CALLS.load(Ordering::SeqCst);
    let journal_sync_ns_before = JOURNAL_SYNC_NANOS.load(Ordering::SeqCst);

    let receipt = store
        .append_atomic(&key, event("normal", &[0x5a; 8192]))
        .await
        .unwrap();
    assert_eq!(receipt.record.cursor.offset, 1);
    let main_writes = MAIN_WRITE_CALLS.load(Ordering::SeqCst) - main_writes_before;
    let main_bytes = MAIN_WRITE_BYTES.load(Ordering::SeqCst) - main_bytes_before;
    let journal_writes = JOURNAL_WRITE_CALLS.load(Ordering::SeqCst) - journal_writes_before;
    let journal_bytes = JOURNAL_WRITE_BYTES.load(Ordering::SeqCst) - journal_bytes_before;
    let main_syncs = MAIN_SYNC_CALLS.load(Ordering::SeqCst) - main_syncs_before;
    let main_sync_ns = MAIN_SYNC_NANOS.load(Ordering::SeqCst) - main_sync_ns_before;
    let journal_syncs = JOURNAL_SYNC_CALLS.load(Ordering::SeqCst) - journal_syncs_before;
    let journal_sync_ns = JOURNAL_SYNC_NANOS.load(Ordering::SeqCst) - journal_sync_ns_before;
    assert!(main_writes > 0 && main_bytes > 0);
    assert!(journal_writes > 0 && journal_bytes > 0);
    assert!(main_syncs > 0 && journal_syncs > 0);
    println!(
        "{{\"kind\":\"sqlite_vfs_append_trace\",\"scope\":\"forwarded SQLite VFS callbacks; not kernel syscalls or device flush completion\",\"payload_bytes\":8192,\"main_db_write_calls\":{main_writes},\"main_db_write_bytes\":{main_bytes},\"journal_write_calls\":{journal_writes},\"journal_write_bytes\":{journal_bytes},\"main_db_sync_calls\":{main_syncs},\"main_db_sync_callback_ns\":{main_sync_ns},\"journal_sync_calls\":{journal_syncs},\"journal_sync_callback_ns\":{journal_sync_ns}}}"
    );
    store.close().await.unwrap();

    let reopened = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    let recovered = reopened
        .lookup_event(&key, &EventId::new("normal").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered, receipt.record);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn forwarding_vfs_write_and_sync_faults_leave_resolvable_atomic_state() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });
    let db = TempDb::new("io");
    let store = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("faults").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&key, event("baseline", b"baseline"))
        .await
        .unwrap();

    let write_faults_before = WRITE_FAULTS.load(Ordering::SeqCst);
    let write = {
        let _fault = FaultReset::arm(FAIL_WRITE_ONCE);
        store
            .append_atomic(&key, event("write-fault", b"write"))
            .await
    };
    assert!(matches!(write, Err(Error::CapacityExceeded)));
    assert_eq!(WRITE_FAULTS.load(Ordering::SeqCst), write_faults_before + 1);
    assert_eq!(store.bounds(&key).await.unwrap().tail.offset, 1);
    assert!(store
        .lookup_event(&key, &EventId::new("write-fault").unwrap())
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .append_atomic(&key, event("write-fault", b"write"))
            .await
            .unwrap()
            .record
            .cursor
            .offset,
        2
    );

    let sync_faults_before = SYNC_FAULTS.load(Ordering::SeqCst);
    let sync = {
        let _fault = FaultReset::arm(FAIL_SYNC_ONCE);
        store
            .append_atomic(&key, event("sync-fault", b"sync"))
            .await
    };
    assert!(!matches!(sync, Err(Error::CommitUnknown { .. })));
    assert_eq!(SYNC_FAULTS.load(Ordering::SeqCst), sync_faults_before + 1);
    let found = store
        .lookup_event(&key, &EventId::new("sync-fault").unwrap())
        .await
        .unwrap();
    match (sync, found) {
        (Ok(receipt), Some(record)) => assert_eq!(receipt.record, record),
        (Err(Error::StoreWriteFailed(_)), None) => {}
        other => panic!("sync outcome did not match stored state: {other:?}"),
    }
    assert!(WRITES.load(Ordering::SeqCst) > 0 && SYNCS.load(Ordering::SeqCst) > 0);
    store.close().await.unwrap();
}

#[tokio::test]
async fn grouped_append_vfs_faults_recover_all_records_or_none() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });

    for (schedule, mode) in [
        ("write-once", FAIL_WRITE_ONCE),
        ("sync-once", FAIL_SYNC_ONCE),
        ("main-writes-and-rollback", FAIL_MAIN_WRITES),
    ] {
        let db = TempDb::new(schedule);
        let store = SqliteStore::open(SqliteOptions::new(&db.path))
            .await
            .unwrap();
        let stream = store
            .create_if_absent(&StreamId::new(format!("group-{schedule}")).unwrap())
            .await
            .unwrap();
        let batch = grouped_batch(&stream, b"first", b"second");
        let counter = if mode == FAIL_SYNC_ONCE {
            &SYNC_FAULTS
        } else {
            &WRITE_FAULTS
        };
        let faults_before = counter.load(Ordering::SeqCst);
        let fault = FaultReset::arm(mode);
        let outcomes = store.append_batch(&batch).await;
        assert!(
            counter.load(Ordering::SeqCst) > faults_before,
            "{schedule}: fault must actually fire"
        );
        drop(fault);
        let _ = store.close().await;
        drop(store);

        let reopened = SqliteStore::open(SqliteOptions::new(&db.path))
            .await
            .unwrap();
        assert_recovered_group(
            &reopened, &stream, &batch, &outcomes, b"first", b"second", schedule,
        )
        .await;
        reopened.close().await.unwrap();
    }

    let db = TempDb::new("group-rollback-truncate");
    let mut options = SqliteOptions::new(&db.path);
    options.max_record_bytes = 2 * 1024 * 1024;
    let initialized = SqliteStore::open(options.clone()).await.unwrap();
    let stream = initialized
        .create_if_absent(&StreamId::new("group-rollback-truncate").unwrap())
        .await
        .unwrap();
    initialized.close().await.unwrap();
    drop(initialized);

    let original_size = i64::try_from(fs::metadata(&db.path).unwrap().len()).unwrap();
    options.sqlite_cache_kib = 64;
    options.failure_injection = Some(SqliteFailureInjection::BeforeCommit);
    let store = SqliteStore::open(options).await.unwrap();
    ROLLBACK_HEADER_STEP.store(0, Ordering::SeqCst);
    ROLLBACK_HEADER_DB_PAGES.store(0, Ordering::SeqCst);
    ROLLBACK_HEADER_PAGE_SIZE.store(0, Ordering::SeqCst);
    ROLLBACK_EXPECTED_SIZE.store(original_size, Ordering::SeqCst);
    ROLLBACK_BASE_MAIN_WRITES.store(
        MAIN_WRITE_SUCCESSES.load(Ordering::SeqCst),
        Ordering::SeqCst,
    );
    ROLLBACK_BASE_JOURNAL_SYNCS.store(
        JOURNAL_SYNC_SUCCESSES.load(Ordering::SeqCst),
        Ordering::SeqCst,
    );
    let faults_before = ROLLBACK_TRUNCATE_FAULTS.load(Ordering::SeqCst);
    let first = vec![0xa5; 768 * 1024];
    let batch = grouped_batch(&stream, &first, b"second");
    let fault = FaultReset::arm(FAIL_ROLLBACK_TRUNCATE);
    let outcomes = store.append_batch(&batch).await;
    assert_eq!(
        ROLLBACK_TRUNCATE_FAULTS.load(Ordering::SeqCst),
        faults_before + 1,
        "the grouped rollback must reach the bounded truncate fault"
    );
    drop(fault);
    let _ = store.close().await;
    drop(store);

    let mut reopen_options = SqliteOptions::new(&db.path);
    reopen_options.max_record_bytes = 2 * 1024 * 1024;
    let reopened = SqliteStore::open(reopen_options).await.unwrap();
    assert_recovered_group(
        &reopened,
        &stream,
        &batch,
        &outcomes,
        &first,
        b"second",
        "rollback-truncate",
    )
    .await;
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn failed_main_file_write_and_rollback_recover_to_one_atomic_state() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });
    let db = TempDb::new("rollback");
    let store = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("rollback").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&key, event("baseline", b"baseline"))
        .await
        .unwrap();

    let faults_before = WRITE_FAULTS.load(Ordering::SeqCst);
    let outcome = {
        let _fault = FaultReset::arm(FAIL_MAIN_WRITES);
        store
            .append_atomic(&key, event("rollback-fault", b"atomic"))
            .await
    };
    let fired = WRITE_FAULTS.load(Ordering::SeqCst) - faults_before;
    assert!(
        fired >= 2,
        "at least two main-file write callbacks must fail during statement and failure handling"
    );
    assert!(matches!(
        &outcome,
        Err(Error::StoreWriteFailed(_)
            | Error::StoreCorrupt(_)
            | Error::CommitUnknown { .. }
            | Error::CapacityExceeded)
    ));
    let _ = store.close().await;
    drop(store);

    let reopened = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    let before_retry = reopened
        .lookup_event(&key, &EventId::new("rollback-fault").unwrap())
        .await
        .unwrap();
    if matches!(
        outcome,
        Err(Error::StoreWriteFailed(_) | Error::CapacityExceeded)
    ) {
        assert!(before_retry.is_none());
    }
    let retry = reopened
        .append_atomic(&key, event("rollback-fault", b"atomic"))
        .await
        .unwrap();
    assert_eq!(retry.record.cursor.offset, 2);
    assert_eq!(
        retry.kind,
        if before_retry.is_some() {
            AppendKind::Deduplicated
        } else {
            AppendKind::Inserted
        }
    );
    assert_eq!(reopened.bounds(&key).await.unwrap().tail.offset, 2);
    let page = reopened
        .read_range(
            &key,
            0,
            2,
            PageLimits {
                max_records: 2,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap();
    assert_eq!(page.records.len(), 2);
    assert_eq!(
        (
            page.records[0].cursor.offset,
            page.records[0].event.id.as_str(),
            page.records[0].event.schema.id.as_str(),
            page.records[0].event.payload.as_bytes(),
        ),
        (1, "baseline", "fault.bytes", b"baseline".as_slice())
    );
    assert_eq!(
        (
            page.records[1].cursor.offset,
            page.records[1].event.id.as_str(),
            page.records[1].event.payload.as_bytes(),
        ),
        (2, "rollback-fault", b"atomic".as_slice())
    );
    assert!(page.complete);
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn failed_rollback_truncate_reports_failure_and_preserves_committed_history() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });
    let db = TempDb::new("rollback-truncate");

    let initial = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    let key = initial
        .create_if_absent(&StreamId::new("rollback-truncate").unwrap())
        .await
        .unwrap();
    initial
        .append_atomic(&key, event("baseline", b"committed-before-fault"))
        .await
        .unwrap();
    initial.close().await.unwrap();
    drop(initial);

    let original_size = i64::try_from(fs::metadata(&db.path).unwrap().len()).unwrap();
    let mut options = SqliteOptions::new(&db.path);
    // A small cache forces dirty pages to spill before the injected rollback.
    // The rollback must then restore journaled pages and truncate the database.
    options.sqlite_cache_kib = 64;
    options.failure_injection = Some(SqliteFailureInjection::BeforeCommit);
    let store = SqliteStore::open(options).await.unwrap();
    let reopened_key = store
        .create_if_absent(&StreamId::new("rollback-truncate").unwrap())
        .await
        .unwrap();
    assert_eq!(reopened_key, key);

    ROLLBACK_HEADER_STEP.store(0, Ordering::SeqCst);
    ROLLBACK_HEADER_DB_PAGES.store(0, Ordering::SeqCst);
    ROLLBACK_HEADER_PAGE_SIZE.store(0, Ordering::SeqCst);
    ROLLBACK_EXPECTED_SIZE.store(original_size, Ordering::SeqCst);
    ROLLBACK_BASE_MAIN_WRITES.store(
        MAIN_WRITE_SUCCESSES.load(Ordering::SeqCst),
        Ordering::SeqCst,
    );
    ROLLBACK_BASE_JOURNAL_SYNCS.store(
        JOURNAL_SYNC_SUCCESSES.load(Ordering::SeqCst),
        Ordering::SeqCst,
    );
    let faults_before = ROLLBACK_TRUNCATE_FAULTS.load(Ordering::SeqCst);
    let fault = FaultReset::arm(FAIL_ROLLBACK_TRUNCATE);
    let attempted_payload = vec![0xa5; 768 * 1024];
    let attempted = store
        .append_atomic(&key, event("rollback-truncate-fault", &attempted_payload))
        .await;
    assert_eq!(
        ROLLBACK_TRUNCATE_FAULTS.load(Ordering::SeqCst),
        faults_before + 1,
        "the forwarding VFS must fail the classified rollback truncate"
    );
    assert_eq!(ROLLBACK_HEADER_STEP.load(Ordering::SeqCst), 5);
    assert_eq!(
        ROLLBACK_HEADER_DB_PAGES
            .load(Ordering::SeqCst)
            .checked_mul(ROLLBACK_HEADER_PAGE_SIZE.load(Ordering::SeqCst)),
        u64::try_from(original_size).ok(),
        "the parsed rollback header must describe the original database size"
    );
    assert!(
        matches!(
            attempted,
            Err(Error::StoreWriteFailed(_) | Error::StoreCorrupt(_))
        ),
        "the pre-commit append must report a definite failure: {attempted:?}"
    );
    drop(fault);
    let _ = store.close().await;
    drop(store);

    let recovered = SqliteStore::open(SqliteOptions::new(&db.path))
        .await
        .unwrap();
    assert_eq!(recovered.bounds(&key).await.unwrap().tail.offset, 1);
    let baseline = recovered
        .lookup_event(&key, &EventId::new("baseline").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(baseline.event.payload.as_bytes(), b"committed-before-fault");
    assert!(recovered
        .lookup_event(&key, &EventId::new("rollback-truncate-fault").unwrap())
        .await
        .unwrap()
        .is_none());
    let retry = recovered
        .append_atomic(&key, event("rollback-truncate-fault", &attempted_payload))
        .await
        .unwrap();
    assert_eq!(retry.kind, AppendKind::Inserted);
    assert_eq!(retry.record.cursor.offset, 2);
    recovered.close().await.unwrap();
}

#[tokio::test]
async fn logical_sqlite_page_quota_reports_full_without_losing_prior_history() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });
    let db = TempDb::new("full");
    let mut options = SqliteOptions::new(&db.path);
    options.sqlite_cache_kib = 64;
    options.busy_timeout = Duration::from_secs(1);
    let initialized = SqliteStore::open(options.clone()).await.unwrap();
    initialized.close().await.unwrap();
    let initialized_pages: u32 = Connection::open(&db.path)
        .unwrap()
        .pragma_query_value(None, "page_count", |row| row.get(0))
        .unwrap();
    options.max_database_pages = initialized_pages.checked_add(16).unwrap();
    let store = SqliteStore::open(options).await.unwrap();
    let key = store
        .create_if_absent(&StreamId::new("bounded-image").unwrap())
        .await
        .unwrap();
    let payload = vec![7; 2048];
    let mut committed = 0u64;
    for index in 0..128 {
        match store
            .append_atomic(&key, event(&format!("event-{index}"), &payload))
            .await
        {
            Ok(_) => committed += 1,
            Err(Error::StoreWriteFailed(_) | Error::CapacityExceeded) => break,
            other => panic!("unexpected full-image result: {other:?}"),
        }
    }
    assert!(committed > 0 && committed < 128);
    assert_eq!(store.bounds(&key).await.unwrap().tail.offset, committed);
    let retry = store
        .append_atomic(&key, event("event-0", &payload))
        .await
        .unwrap();
    assert_eq!(retry.kind, AppendKind::Deduplicated);
    assert_eq!(retry.record.cursor.offset, 1);
    store.close().await.unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn private_bounded_filesystem_exhaustion_preserves_history_and_retry() {
    let _test = TEST_LOCK.lock().await;
    INSTALL.call_once(|| unsafe { install_fault_vfs() });
    let image = MountedImage::new();
    assert_ne!(
        fs::metadata(&image.root).unwrap().dev(),
        fs::metadata(&image.mount).unwrap().dev(),
        "the test must use a separate mounted filesystem"
    );
    let filesystem = Command::new("df")
        .args(["-k"])
        .arg(&image.mount)
        .output()
        .unwrap();
    assert!(filesystem.status.success());
    let filesystem = String::from_utf8(filesystem.stdout).unwrap();
    let blocks_kib: u64 = filesystem
        .lines()
        .last()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        blocks_kib <= 128 * 1024,
        "filesystem image must stay bounded"
    );
    let database = image.mount.join("events.sqlite3");
    let store = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let key = store
        .create_if_absent(&StreamId::new("real-full-filesystem").unwrap())
        .await
        .unwrap();
    store
        .append_atomic(&key, event("baseline", b"durable-before-full"))
        .await
        .unwrap();

    let filler_path = image.mount.join("bounded-filler");
    let mut filler = fs::File::create(&filler_path).unwrap();
    let mut block = vec![0u8; 64 * 1024];
    let mut exhaustion_error = None;
    for index in 0..2048usize {
        block[0..8].copy_from_slice(&(index as u64).to_be_bytes());
        if let Err(error) = filler.write_all(&block) {
            exhaustion_error = error.raw_os_error();
            break;
        }
    }
    if exhaustion_error.is_none() {
        if let Err(error) = filler.sync_all() {
            exhaustion_error = error.raw_os_error();
        }
    }
    drop(filler);
    assert_eq!(
        exhaustion_error,
        Some(libc::ENOSPC),
        "the bounded image must fail specifically with ENOSPC"
    );

    let attempted = event("during-full", &vec![9; 512 * 1024]);
    let outcome = store.append_atomic(&key, attempted.clone()).await;
    assert!(matches!(
        &outcome,
        Err(Error::CapacityExceeded | Error::StoreWriteFailed(_) | Error::CommitUnknown { .. })
    ));
    fs::remove_file(&filler_path).unwrap();
    let _ = store.close().await;
    drop(store);

    let reopened = SqliteStore::open(SqliteOptions::new(&database))
        .await
        .unwrap();
    let baseline = reopened
        .lookup_event(&key, &EventId::new("baseline").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(baseline.cursor.offset, 1);
    assert_eq!(baseline.event.payload.as_bytes(), b"durable-before-full");
    let before_retry = reopened.lookup_event(&key, &attempted.id).await.unwrap();
    if matches!(
        outcome,
        Err(Error::StoreWriteFailed(_) | Error::CapacityExceeded)
    ) {
        assert!(before_retry.is_none());
    }
    let retry = reopened.append_atomic(&key, attempted).await.unwrap();
    assert_eq!(retry.record.cursor.offset, 2);
    assert_eq!(
        retry.kind,
        if before_retry.is_some() {
            AppendKind::Deduplicated
        } else {
            AppendKind::Inserted
        }
    );
    assert_eq!(reopened.bounds(&key).await.unwrap().tail.offset, 2);
    reopened.close().await.unwrap();
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn restore_on_a_real_full_filesystem_keeps_destination_unpublished_and_retries() {
    let _test = TEST_LOCK.lock().await;
    let source_db = TempDb::new("restore-source-full");
    let mut source_options = SqliteOptions::new(&source_db.path);
    source_options.max_record_bytes = 1024 * 1024;
    let source = SqliteStore::open(source_options).await.unwrap();
    let key = source
        .create_if_absent(&StreamId::new("restore-full").unwrap())
        .await
        .unwrap();
    let mut payload = vec![0x5a; 512 * 1024];
    for index in 0..32_u64 {
        payload[..8].copy_from_slice(&index.to_be_bytes());
        source
            .append_atomic(&key, event(&format!("restore-{index}"), &payload))
            .await
            .unwrap();
    }
    source.close().await.unwrap();

    let image = MountedImage::new();
    let reserve_path = image.mount.join("restore-reserve");
    let mut reserve = fs::File::create(&reserve_path).unwrap();
    let mut block = vec![0xa5; 64 * 1024];
    for _ in 0..64 {
        reserve.write_all(&block).unwrap();
    }
    reserve.sync_all().unwrap();
    drop(reserve);
    let filler_path = image.mount.join("restore-filler");
    let mut filler = fs::File::create(&filler_path).unwrap();
    let mut exhaustion_error = None;
    for index in 0..2048_u64 {
        block[..8].copy_from_slice(&index.to_be_bytes());
        if let Err(error) = filler.write_all(&block) {
            exhaustion_error = error.raw_os_error();
            break;
        }
    }
    if exhaustion_error.is_none() {
        if let Err(error) = filler.sync_all() {
            exhaustion_error = error.raw_os_error();
        }
    }
    drop(filler);
    assert_eq!(exhaustion_error, Some(libc::ENOSPC));
    fs::remove_file(&reserve_path).unwrap();

    let config = RestoreConfig {
        max_source_bytes: 64 * 1024 * 1024,
        max_staging_bytes: 48 * 1024 * 1024,
        ..RestoreConfig::default()
    };
    let manager = SqliteRestoreManager::new(
        SqliteRestoreBackend::new(&image.mount).unwrap(),
        config.clone(),
    )
    .unwrap();
    let identity = manager
        .inspect_backup(source_db.path.clone())
        .await
        .unwrap();
    let request = RestoreRequest {
        operation_id: RestoreOperationId::new("real-filesystem-full-restore").unwrap(),
        backup_identity: identity,
        source: source_db.path.clone(),
        destination: PathBuf::from("restored.sqlite3"),
    };
    let result = manager.restore(request.clone()).await;
    match result {
        Err(RestoreError::CapacityExceeded) => {}
        Err(RestoreError::StorageFailure(message))
            if message.contains("No space left on device") || message.contains("os error 28") => {}
        other => panic!("restore must fail specifically because storage is full: {other:?}"),
    }
    assert!(!image.mount.join("restored.sqlite3").exists());
    fs::remove_file(&filler_path).unwrap();
    manager.cleanup_staging(request.clone()).await.unwrap();

    let retry_manager =
        SqliteRestoreManager::new(SqliteRestoreBackend::new(&image.mount).unwrap(), config)
            .unwrap();
    let retry = retry_manager.restore(request.clone()).await.unwrap();
    let mapped = retry_manager
        .read_mapping(
            retry.clone(),
            None,
            PageLimits {
                max_records: 1,
                max_bytes: 4096,
            },
        )
        .await
        .unwrap()
        .entries
        .pop()
        .unwrap()
        .new;
    assert_ne!(mapped.incarnation, key.incarnation);
    let restored = SqliteStore::open(SqliteOptions::new(&retry.destination))
        .await
        .unwrap();
    assert_eq!(restored.bounds(&mapped).await.unwrap().tail.offset, 32);
    let mut after = 0;
    while after < 32 {
        let page = restored
            .read_range(
                &mapped,
                after,
                32,
                PageLimits {
                    max_records: 4,
                    max_bytes: 3 * 1024 * 1024,
                },
            )
            .await
            .unwrap();
        assert!(!page.records.is_empty());
        for record in page.records {
            let expected = after;
            assert_eq!(record.cursor.offset, expected + 1);
            assert_eq!(record.event.id.as_str(), format!("restore-{expected}"));
            assert_eq!(record.event.schema.id.as_str(), "fault.bytes");
            assert_eq!(record.event.schema.version, 1);
            payload[..8].copy_from_slice(&expected.to_be_bytes());
            assert_eq!(record.event.payload.as_bytes(), payload.as_slice());
            after = record.cursor.offset;
        }
    }
    assert_eq!(after, 32);
    restored.close().await.unwrap();
    assert_eq!(
        retry_manager
            .inspect_backup(source_db.path.clone())
            .await
            .unwrap(),
        identity,
        "the source artifact must remain byte-for-byte identical"
    );
}
