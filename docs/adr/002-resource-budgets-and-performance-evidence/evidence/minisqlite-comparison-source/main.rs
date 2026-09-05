use event_stream::infrastructure::{SqliteOptions, SqliteStore};
use event_stream::{
    EventReader, EventRuntime, EventStore, EventSubscription, NewEvent, PageLimits, Payload,
    Runtime, RuntimeConfig, SchemaId, SchemaRef, StartPosition, StreamId, SubscriptionOptions,
};
use minisqlite::{Connection as MiniConnection, Value};
use rusqlite::{params, Connection};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    env,
    error::Error,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct CountingAllocator;
static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) && !ptr.is_null() {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
            DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        System.dealloc(ptr, layout);
    }
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

const RECORDS: usize = 256;
const PAYLOAD_BYTES: usize = 128;
const PAGE_RECORDS: usize = 256;

#[derive(Clone, Copy)]
struct Usage {
    user_us: i128,
    system_us: i128,
    max_rss_bytes: i128,
}

fn usage() -> Usage {
    let mut value = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, value.as_mut_ptr()) };
    assert_eq!(rc, 0, "getrusage failed");
    let value = unsafe { value.assume_init() };
    let tv = |x: libc::timeval| i128::from(x.tv_sec) * 1_000_000 + i128::from(x.tv_usec);
    #[cfg(target_os = "macos")]
    let max_rss_bytes = i128::from(value.ru_maxrss);
    #[cfg(not(target_os = "macos"))]
    let max_rss_bytes = i128::from(value.ru_maxrss) * 1024;
    Usage {
        user_us: tv(value.ru_utime),
        system_us: tv(value.ru_stime),
        max_rss_bytes,
    }
}

fn unique_dir(mode: &str) -> Result<PathBuf, Box<dyn Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = env::temp_dir().join(format!(
        "event-stream-mini-{mode}-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir(&path)?;
    Ok(path)
}

fn seed_direct(path: &Path) -> Result<(), Box<dyn Error>> {
    let mut conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "DELETE")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.execute(
        "CREATE TABLE records(seq INTEGER PRIMARY KEY, payload BLOB NOT NULL)",
        [],
    )?;
    let tx = conn.transaction()?;
    {
        let mut statement = tx.prepare("INSERT INTO records(seq,payload) VALUES(?1,?2)")?;
        for seq in 0..RECORDS {
            statement.execute(params![seq as i64, vec![seq as u8; PAYLOAD_BYTES]])?;
        }
    }
    tx.commit()?;
    drop(conn);
    Ok(())
}

fn validate_direct_row(seq: usize, payload: &[u8]) -> Result<(), Box<dyn Error>> {
    if payload.len() != PAYLOAD_BYTES || payload.iter().any(|byte| *byte != seq as u8) {
        return Err(format!("payload mismatch at {seq}").into());
    }
    Ok(())
}

fn sqlite_replay(path: &Path, prepared: bool, population: usize) -> Result<usize, Box<dyn Error>> {
    let conn = Connection::open(path)?;
    let mut seen = 0usize;
    if prepared {
        let mut statement =
            conn.prepare("SELECT seq,payload FROM records WHERE seq>?1 ORDER BY seq LIMIT ?2")?;
        for _ in 0..population {
            let mut last = -1i64;
            while last < (RECORDS - 1) as i64 {
                let mut rows = statement.query(params![last, PAGE_RECORDS as i64])?;
                let mut page = 0;
                while let Some(row) = rows.next()? {
                    let seq: i64 = row.get(0)?;
                    let payload: Vec<u8> = row.get(1)?;
                    if seq != last + 1 {
                        return Err("noncontiguous SQLite row".into());
                    }
                    validate_direct_row(seq as usize, &payload)?;
                    last = seq;
                    page += 1;
                    seen += 1;
                }
                if page == 0 {
                    return Err("SQLite query made no progress".into());
                }
            }
        }
    } else {
        for _ in 0..population {
            let mut last = -1i64;
            while last < (RECORDS - 1) as i64 {
                let mut statement = conn.prepare(
                    "SELECT seq,payload FROM records WHERE seq>?1 ORDER BY seq LIMIT ?2",
                )?;
                let mut rows = statement.query(params![last, PAGE_RECORDS as i64])?;
                let mut page = 0;
                while let Some(row) = rows.next()? {
                    let seq: i64 = row.get(0)?;
                    let payload: Vec<u8> = row.get(1)?;
                    if seq != last + 1 {
                        return Err("noncontiguous SQLite row".into());
                    }
                    validate_direct_row(seq as usize, &payload)?;
                    last = seq;
                    page += 1;
                    seen += 1;
                }
                if page == 0 {
                    return Err("SQLite query made no progress".into());
                }
            }
        }
    }
    Ok(seen)
}

fn minisqlite_replay(path: &Path, population: usize) -> Result<usize, Box<dyn Error>> {
    let mut conn = MiniConnection::open(path)?;
    // Capability probe. Failure is reported explicitly rather than changing the workload.
    let probe = conn.query("SELECT octet_length(payload) FROM records WHERE seq=0")?;
    if !matches!(
        probe.rows.first().and_then(|row| row.first()),
        Some(Value::Integer(128))
    ) {
        return Err("MiniSQLite octet_length probe returned an unexpected result".into());
    }
    let mut seen = 0usize;
    for _ in 0..population {
        let mut last = -1i64;
        while last < (RECORDS - 1) as i64 {
            let sql = format!("SELECT seq,payload FROM records WHERE seq>{last} ORDER BY seq LIMIT {PAGE_RECORDS}");
            let result = conn.query(&sql)?;
            if result.rows.is_empty() {
                return Err("MiniSQLite query made no progress".into());
            }
            for row in result.rows {
                let (seq, payload) = match row.as_slice() {
                    [Value::Integer(seq), Value::Blob(payload)] => (*seq, payload.as_slice()),
                    _ => return Err("MiniSQLite returned unexpected value types".into()),
                };
                if seq != last + 1 {
                    return Err("noncontiguous MiniSQLite row".into());
                }
                validate_direct_row(seq as usize, payload)?;
                last = seq;
                seen += 1;
            }
        }
    }
    Ok(seen)
}

async fn seed_store(path: &Path) -> Result<(), Box<dyn Error>> {
    let store = SqliteStore::open(SqliteOptions::new(path)).await?;
    let stream = store.create_if_absent(&StreamId::new("replay")?).await?;
    for seq in 0..RECORDS {
        store
            .append_atomic(
                &stream,
                NewEvent {
                    id: event_stream::EventId::new(format!("event-{seq:03}"))?,
                    schema: SchemaRef {
                        id: SchemaId::new("diagnostic")?,
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(&vec![seq as u8; PAYLOAD_BYTES]),
                },
            )
            .await?;
    }
    store.close().await?;
    Ok(())
}

async fn store_replay(path: &Path, population: usize) -> Result<usize, Box<dyn Error>> {
    let store = SqliteStore::open(SqliteOptions::new(path)).await?;
    let stream = store.create_if_absent(&StreamId::new("replay")?).await?;
    let mut seen = 0usize;
    for _ in 0..population {
        let mut after = 0u64;
        while after < RECORDS as u64 {
            let page = store
                .read_range(
                    &stream,
                    after,
                    RECORDS as u64,
                    PageLimits {
                        max_records: PAGE_RECORDS,
                        max_bytes: 1024 * 1024,
                    },
                )
                .await?;
            if page.records.is_empty() {
                return Err("SqliteStore read made no progress".into());
            }
            for record in page.records {
                if record.cursor.offset != after + 1 {
                    return Err("noncontiguous SqliteStore record".into());
                }
                validate_direct_row(
                    (record.cursor.offset - 1) as usize,
                    record.event.payload.as_bytes(),
                )?;
                after = record.cursor.offset;
                seen += 1;
            }
        }
    }
    store.close().await?;
    Ok(seen)
}

async fn runtime_replay(path: &Path, population: usize) -> Result<usize, Box<dyn Error>> {
    let mut config = RuntimeConfig::default();
    config.subscriptions.max_total = population.max(1024);
    config.subscriptions.max_per_stream = population.max(128);
    config.reads.admission_timeout = Duration::from_secs(60);
    let runtime = Runtime::<SqliteStore>::open(SqliteOptions::new(path), config).await?;
    let stream = runtime.create_stream(&StreamId::new("replay")?).await?;
    let mut subscriptions = Vec::with_capacity(population);
    for _ in 0..population {
        subscriptions.push(
            runtime
                .subscribe(
                    &stream,
                    SubscriptionOptions {
                        start: StartPosition::Beginning,
                        page: PageLimits {
                            max_records: PAGE_RECORDS,
                            max_bytes: 1024 * 1024,
                        },
                        max_lag_records: 1024,
                        max_lag_duration: Duration::from_secs(300),
                        catch_up_grace: Duration::ZERO,
                    },
                )
                .await?,
        );
    }
    let mut seen = 0usize;
    for subscription in &mut subscriptions {
        for expected in 0..RECORDS {
            let record = tokio::time::timeout(Duration::from_secs(300), subscription.next())
                .await?
                .ok_or("runtime subscription ended early")??;
            if record.cursor.offset != expected as u64 + 1 {
                return Err("noncontiguous runtime record".into());
            }
            validate_direct_row(expected, record.event.payload.as_bytes())?;
            seen += 1;
        }
    }
    drop(subscriptions);
    runtime.shutdown(Duration::from_secs(30)).await?;
    Ok(seen)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn Error>> {
    let mode = env::args().nth(1).ok_or("mode required")?;
    let population: usize = env::args().nth(2).ok_or("population required")?.parse()?;
    if !matches!(population, 1_000 | 10_000 | 100_000) {
        return Err("population must be 1000, 10000, or 100000".into());
    }
    let dir = unique_dir(&mode)?;
    let db = dir.join("fixture.sqlite3");
    if mode.starts_with("sqlite-") || mode == "minisqlite" {
        seed_direct(&db)?;
    } else if mode == "store" || mode == "runtime" {
        seed_store(&db).await?;
    }
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    DEALLOCATED_BYTES.store(0, Ordering::Relaxed);
    let before = usage();
    TRACK_ALLOCATIONS.store(true, Ordering::SeqCst);
    let started = Instant::now();
    let seen = match mode.as_str() {
        "sqlite-prepared" => sqlite_replay(&db, true, population)?,
        "sqlite-unprepared" => sqlite_replay(&db, false, population)?,
        "minisqlite" => minisqlite_replay(&db, population)?,
        "store" => store_replay(&db, population).await?,
        "runtime" => runtime_replay(&db, population).await?,
        _ => return Err(format!("unknown mode {mode}").into()),
    };
    let elapsed_ns = started.elapsed().as_nanos();
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    let after = usage();
    let allocation_calls = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
    let deallocated_bytes = DEALLOCATED_BYTES.load(Ordering::Relaxed);
    if seen != RECORDS * population {
        return Err(format!("count mismatch: {seen}").into());
    }
    println!("{{\"schema_version\":1,\"mode\":\"{mode}\",\"records\":{RECORDS},\"payload_bytes\":{PAYLOAD_BYTES},\"page_records\":{PAGE_RECORDS},\"population\":{population},\"records_verified\":{seen},\"elapsed_ns\":{elapsed_ns},\"user_cpu_us\":{},\"system_cpu_us\":{},\"lifetime_peak_rss_bytes\":{},\"rust_allocation_calls\":{allocation_calls},\"rust_allocated_bytes\":{allocated_bytes},\"rust_deallocated_bytes\":{deallocated_bytes},\"journal_mode\":\"DELETE\",\"synchronous\":\"FULL\"}}", after.user_us-before.user_us, after.system_us-before.system_us, after.max_rss_bytes);
    Ok(())
}
