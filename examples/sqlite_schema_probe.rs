//! Machine-readable physical-layout comparison for ADR 0003.
//! This is engineering evidence, not a product benchmark or durability claim.

#[cfg(not(feature = "sqlite"))]
fn main() {
    eprintln!("run with --features sqlite");
}

#[cfg(feature = "sqlite")]
fn main() -> rusqlite::Result<()> {
    use rusqlite::{params, Connection};
    use std::{hint::black_box, time::Instant};
    const MAX_RECORD: usize = 1024 * 1024;
    let cases = [
        ("128", 128_usize, 8192_u64, 200_u64),
        ("1024", 1024, 4096, 200),
        ("16384", 16 * 1024, 512, 200),
        ("max", MAX_RECORD - 7 - 11 - 128, 8, 10),
    ];
    println!("{{\"kind\":\"environment\",\"sqlite_version\":\"{}\",\"os\":\"{}\",\"arch\":\"{}\",\"journal_mode\":\"delete\",\"synchronous\":\"full\",\"repetitions\":3}}", rusqlite::version(), std::env::consts::OS, std::env::consts::ARCH);
    for repetition in 1..=3 {
        for &(case_name, payload_bytes, records, query_iterations) in &cases {
            for without_rowid in [false, true] {
                let layout = if without_rowid {
                    "without_rowid"
                } else {
                    "rowid"
                };
                let path = std::env::temp_dir().join(format!(
                    "event-stream-layout-{layout}-{case_name}-{repetition}-{}.sqlite3",
                    uuid::Uuid::new_v4()
                ));
                let mut conn = Connection::open(&path)?;
                conn.pragma_update(None, "journal_mode", "DELETE")?;
                conn.pragma_update(None, "synchronous", "FULL")?;
                let suffix = if without_rowid { " WITHOUT ROWID" } else { "" };
                conn.execute_batch(&format!("CREATE TABLE records(stream_key INTEGER NOT NULL,offset BLOB NOT NULL,event_id TEXT NOT NULL,schema_id TEXT NOT NULL,schema_version INTEGER NOT NULL,payload BLOB NOT NULL,PRIMARY KEY(stream_key,offset),UNIQUE(stream_key,event_id)){suffix};"))?;
                let payload = vec![0x5a_u8; payload_bytes];
                let insert_started = Instant::now();
                let tx = conn.transaction()?;
                {
                    let mut insert = tx.prepare_cached("INSERT INTO records(stream_key,offset,event_id,schema_id,schema_version,payload) VALUES(?1,?2,?3,'probe.bytes',1,?4)")?;
                    for offset in 1..=records {
                        insert.execute(params![
                            1_i64,
                            offset.to_be_bytes().as_slice(),
                            format!("event-{offset}"),
                            payload.as_slice()
                        ])?;
                    }
                }
                tx.commit()?;
                let insert_ns = insert_started.elapsed().as_nanos();
                let middle = records / 2;
                let read_start = middle.saturating_sub(4);
                let read_end = (read_start + 8).min(records);
                let replay_started = Instant::now();
                for _ in 0..query_iterations {
                    let mut statement = conn.prepare_cached("SELECT length(event_id),length(schema_id),length(payload),offset,event_id,schema_id,schema_version,payload FROM records WHERE stream_key=?1 AND offset>?2 AND offset<=?3 ORDER BY offset LIMIT 8")?;
                    let mut rows = statement.query(params![
                        1_i64,
                        read_start.to_be_bytes().as_slice(),
                        read_end.to_be_bytes().as_slice()
                    ])?;
                    while let Some(row) = rows.next()? {
                        black_box(row.get::<_, Vec<u8>>(7)?);
                    }
                }
                let replay_ns = replay_started.elapsed().as_nanos() / u128::from(query_iterations);
                let retry_id = format!("event-{}", middle.max(1));
                let retry_started = Instant::now();
                for _ in 0..query_iterations {
                    let value: Vec<u8> = conn.query_row(
                        "SELECT payload FROM records WHERE stream_key=?1 AND event_id=?2",
                        params![1_i64, retry_id],
                        |row| row.get(0),
                    )?;
                    black_box(value);
                }
                let retry_ns = retry_started.elapsed().as_nanos() / u128::from(query_iterations);
                let page_count: u64 =
                    conn.pragma_query_value(None, "page_count", |row| row.get(0))?;
                let page_size: u64 =
                    conn.pragma_query_value(None, "page_size", |row| row.get(0))?;
                let table_bytes: u64 = conn.query_row(
                    "SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name='records'",
                    [],
                    |row| row.get(0),
                )?;
                let index_bytes: u64 = conn.query_row("SELECT coalesce(sum(pgsize),0) FROM dbstat WHERE name NOT IN ('records','sqlite_schema')", [], |row| row.get(0))?;
                let replay_plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT payload FROM records WHERE stream_key=?1 AND offset>?2 AND offset<=?3 ORDER BY offset LIMIT 8", params![1_i64, read_start.to_be_bytes().as_slice(), read_end.to_be_bytes().as_slice()], |row| row.get(3))?;
                let retry_plan: String = conn.query_row("EXPLAIN QUERY PLAN SELECT payload FROM records WHERE stream_key=?1 AND event_id=?2", params![1_i64, retry_id], |row| row.get(3))?;
                println!("{{\"kind\":\"sample\",\"repetition\":{repetition},\"layout\":\"{layout}\",\"case\":\"{case_name}\",\"payload_bytes\":{payload_bytes},\"records\":{records},\"query_iterations\":{query_iterations},\"database_bytes\":{},\"table_bytes\":{table_bytes},\"index_bytes\":{index_bytes},\"insert_transaction_ns\":{insert_ns},\"replay_ns_per_query\":{replay_ns},\"retry_ns_per_query\":{retry_ns},\"replay_plan\":\"{}\",\"retry_plan\":\"{}\"}}", page_count * page_size, json_escape(&replay_plan), json_escape(&retry_plan));
                drop(conn);
                let _ = std::fs::remove_file(path);
            }
        }
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
fn json_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
