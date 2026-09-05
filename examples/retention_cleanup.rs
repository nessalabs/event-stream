//! Diagnostic: bounded retention cleanup with a large surviving timestamp suffix.
mod support;
use event_stream::{
    infrastructure::{MemoryStore, MemoryStoreOptions},
    *,
};
use std::time::Instant;

const RECORDS: u64 = 100_000;
const FLOOR: u64 = 256;

#[tokio::main(flavor = "current_thread")]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::open(MemoryStoreOptions::default()).await?;
    let stream = store
        .create_if_absent(&StreamId::new("retention-cost")?)
        .await?;
    for offset in 1..=RECORDS {
        store
            .append_atomic(
                &stream,
                NewEvent {
                    id: EventId::new(format!("event-{offset}"))?,
                    schema: SchemaRef {
                        id: SchemaId::new("cleanup.bytes")?,
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(b"x"),
                },
            )
            .await?;
    }
    store
        .enable_retry_policy(EnableRetryPolicy {
            operation_id: RetentionOperationId::new("enable")?,
            stream: stream.clone(),
        })
        .await?;
    store
        .advance_retention_floor(AdvanceRetentionFloor {
            operation_id: RetentionOperationId::new("floor")?,
            stream: stream.clone(),
            expected_floor: Cursor::new(stream.clone(), 0),
            new_floor: Cursor::new(stream.clone(), FLOOR),
        })
        .await?;
    let before = support::Usage::read()?;
    let allocations = support::AllocationSnapshot::read();
    let sampler = support::Sampler::start(true).unwrap();
    let start = Instant::now();
    let mut removed = 0;
    for step in 0..FLOOR {
        let progress = store
            .cleanup_retention(RetentionCleanupLimits {
                max_event_rows: 1,
                max_retry_rows: 1,
                max_bytes: 2 * 1024 * 1024,
            })
            .await?;
        assert_eq!(progress.removed_event_rows, 1);
        assert_eq!(progress.remaining, step + 1 < FLOOR);
        removed += progress.removed_event_rows;
    }
    let elapsed_ns = start.elapsed().as_nanos();
    let after = support::Usage::read()?;
    let allocated_after = support::AllocationSnapshot::read();
    let peaks = sampler.finish();
    let rss = (peaks.samples > 0 && peaks.rss > 0)
        .then_some(peaks.rss)
        .map_or_else(|| "null".to_owned(), |value| value.to_string());
    let live = (peaks.samples > 0)
        .then_some(peaks.rust_live)
        .map_or_else(|| "null".to_owned(), |value| value.to_string());
    assert_eq!(removed as u64, FLOOR);
    let mut cursor = Cursor::new(stream.clone(), FLOOR);
    let tail = Cursor::new(stream, RECORDS);
    let mut verified = 0;
    while cursor.offset < RECORDS {
        let page = store
            .read_range(
                &cursor.stream,
                cursor.offset,
                RECORDS,
                PageLimits {
                    max_records: 1024,
                    max_bytes: 2 * 1024 * 1024,
                },
            )
            .await?;
        assert!(!page.records.is_empty());
        for record in &page.records {
            assert_eq!(record.cursor.offset, FLOOR + verified + 1);
            assert_eq!(
                record.event.id.as_str(),
                format!("event-{}", record.cursor.offset)
            );
            assert_eq!(record.event.payload.as_bytes(), b"x");
            verified += 1;
        }
        cursor = page.next_after;
    }
    assert_eq!(cursor, tail);
    assert_eq!(verified, RECORDS - FLOOR);
    store.close().await?;
    println!(concat!("{{\"records\":{},\"removed\":{},\"verified_suffix\":{},\"cleanup_calls\":{},",
        "\"elapsed_ns\":{},\"cpu_us\":{},\"allocated_bytes\":{},\"allocation_count\":{},",
        "\"sampled_rss_peak_bytes\":{},\"sampled_rust_live_peak_bytes\":{},\"rss_samples\":{},\"process_lifetime_max_rss_bytes\":{},",
        "\"scope\":\"256_one_row_cleanup_calls_excludes_setup_and_suffix_verification\",\"store\":\"memory_with_replication\"}}"),
        RECORDS, removed, verified, FLOOR, elapsed_ns,
        after.user_us - before.user_us + after.system_us - before.system_us,
        allocated_after.allocated - allocations.allocated, allocated_after.count - allocations.count,
        rss, live, peaks.samples, after.max_rss);
    Ok(())
}
