#![cfg(feature = "snapshots")]

#[path = "support/mod.rs"]
mod support;

use event_stream::{infrastructure::*, *};
use sha2::{Digest, Sha256};
use std::{
    error::Error,
    time::{Duration, Instant},
};
use support::{current_rss_bytes, AllocationSnapshot, Sampler, Usage};

type BenchResult<T> = std::result::Result<T, Box<dyn Error>>;

const CHUNK_BYTES: usize = 64 * 1024;
const EVENT_PAYLOAD_BYTES: usize = 128;
const COVERED: u64 = 128;
const TAIL: u64 = 256;

#[derive(Default)]
struct PhaseTimes {
    begin_ns: u128,
    upload_ns: u128,
    verify_publish_ns: u128,
    suffix_append_ns: u128,
    recovery_ns: u128,
    shutdown_ns: u128,
}

struct Outcome {
    times: PhaseTimes,
    descriptor: SnapshotDescriptor,
    content_reads: usize,
    suffix_records: usize,
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    phase_vfs: Option<[SqliteVfsSnapshot; 6]>,
}

struct ForegroundOutcome {
    elapsed_ns: u128,
    latencies_ns: Vec<u128>,
    append_intervals: Vec<AppendInterval>,
    accepted: u64,
    rejected: u64,
    failed: u64,
    committed: usize,
    snapshot_phase_ns: u128,
    snapshot_start_ns: u128,
    snapshot_end_ns: u128,
    overlapping_append_receipts: usize,
    allocation_before: AllocationSnapshot,
    allocation_after: AllocationSnapshot,
    usage_before: Usage,
    usage_after: Usage,
    rss_before: Option<u64>,
    rss_after: Option<u64>,
    peaks: Option<support::SamplePeaks>,
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    vfs_before: Option<SqliteVfsSnapshot>,
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    vfs_after: Option<SqliteVfsSnapshot>,
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    sync_timeline: Option<SqliteVfsSyncTimeline>,
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    sync_timeline_origin_ns: Option<u64>,
}

struct AppendInterval {
    producer: u64,
    sequence: u64,
    begin_ns: u128,
    end_ns: u128,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> BenchResult<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let store = value(&args, "--store").unwrap_or("memory");
    let content_bytes = value(&args, "--snapshot-bytes")
        .unwrap_or("1048576")
        .parse::<usize>()?;
    let instrumented = value(&args, "--instrumented").unwrap_or("true") == "true";
    let mode = value(&args, "--mode").unwrap_or("full");
    if ![1024 * 1024, 16 * 1024 * 1024, 64 * 1024 * 1024].contains(&content_bytes) {
        return Err("--snapshot-bytes must be 1, 16, or 64 MiB".into());
    }

    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    if store == "sqlite" && instrumented {
        install_sqlite_vfs_recorder().map_err(|error| format!("install recorder: {error}"))?;
        reset_sqlite_vfs_recorder().map_err(|error| format!("reset recorder: {error}"))?;
    }
    if mode != "full" {
        return run_foreground(store, mode, content_bytes, instrumented).await;
    }
    let allocation_before = AllocationSnapshot::read();
    let usage_before = Usage::read()?;
    let rss_before = current_rss_bytes();
    let sampler = Sampler::start(instrumented);
    let started = Instant::now();
    let (outcome, database_bytes) = match store {
        "memory" => {
            let runtime = Runtime::<MemoryStore>::open(
                MemoryStoreOptions::default(),
                RuntimeConfig::default(),
            )
            .await?;
            (exercise(runtime, content_bytes, false).await?, None)
        }
        "sqlite" => run_sqlite(content_bytes, instrumented).await?,
        _ => return Err("--store must be memory or sqlite".into()),
    };
    let elapsed = started.elapsed();
    let peaks = sampler.map(Sampler::finish);
    let rss_after = current_rss_bytes();
    let usage_after = Usage::read()?;
    let allocation_after = AllocationSnapshot::read();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs = (store == "sqlite" && instrumented).then(sqlite_vfs_snapshot);
    #[cfg(not(all(feature = "sqlite", feature = "test-support")))]
    let vfs: Option<()> = None;
    println!(
        concat!(
            "{{\"kind\":\"snapshot_resource_sample\",\"store\":\"{}\",",
            "\"snapshot_bytes\":{},\"chunk_bytes\":{},\"covered\":{},\"tail\":{},",
            "\"instrumented\":{},\"elapsed_ns\":{},\"begin_ns\":{},\"upload_ns\":{},",
            "\"verify_publish_ns\":{},\"suffix_append_ns\":{},\"recovery_ns\":{},\"shutdown_ns\":{},",
            "\"content_reads\":{},\"suffix_records\":{},\"snapshot_id\":\"{}\",",
            "\"allocation_count\":{},\"allocated_bytes\":{},\"deallocated_bytes\":{},",
            "\"rust_live_before\":{},\"rust_live_after\":{},\"sampled_peak_rust_live_bytes\":{},",
            "\"rss_before_bytes\":{},\"rss_after_bytes\":{},\"sampled_peak_rss_bytes\":{},",
            "\"max_rss_bytes\":{},\"cpu_user_us\":{},\"cpu_system_us\":{},",
            "\"sampler_samples\":{},\"database_bytes\":{},\"vfs\":{},\"phase_vfs\":{},",
            "\"aggregate_scope\":\"runtime_open_fixture_events_hash_snapshot_recovery_shutdown_and_for_sqlite_reopen_cleanup\",",
            "\"correctness\":\"exact_descriptor_content_suffix_replay_retry_release_and_empty_staging\"}}"
        ),
        store,
        content_bytes,
        CHUNK_BYTES,
        COVERED,
        TAIL,
        instrumented,
        elapsed.as_nanos(),
        outcome.times.begin_ns,
        outcome.times.upload_ns,
        outcome.times.verify_publish_ns,
        outcome.times.suffix_append_ns,
        outcome.times.recovery_ns,
        outcome.times.shutdown_ns,
        outcome.content_reads,
        outcome.suffix_records,
        hex(outcome.descriptor.id.as_bytes()),
        optional_u64(instrumented.then(|| allocation_after.count.saturating_sub(allocation_before.count))),
        optional_u64(instrumented.then(|| allocation_after.allocated.saturating_sub(allocation_before.allocated))),
        optional_u64(instrumented.then(|| allocation_after.deallocated.saturating_sub(allocation_before.deallocated))),
        optional_u64(instrumented.then(|| allocation_before.live())),
        optional_u64(instrumented.then(|| allocation_after.live())),
        optional_u64(peaks.as_ref().map(|value| value.rust_live)),
        optional_u64(rss_before),
        optional_u64(rss_after),
        optional_u64(peaks.as_ref().map(|value| value.rss)),
        usage_after.max_rss,
        usage_after.user_us.saturating_sub(usage_before.user_us),
        usage_after.system_us.saturating_sub(usage_before.system_us),
        peaks.as_ref().map_or(0, |value| value.samples),
        optional_u64(database_bytes),
        encode_vfs(vfs),
        encode_phase_vfs({
            #[cfg(all(feature = "sqlite", feature = "test-support"))]
            { outcome.phase_vfs }
            #[cfg(not(all(feature = "sqlite", feature = "test-support")))]
            { None }
        }),
    );
    Ok(())
}

async fn exercise<S: SnapshotStore>(
    runtime: Runtime<S>,
    content_bytes: usize,
    vfs_enabled: bool,
) -> BenchResult<Outcome> {
    let stream = runtime
        .create_stream(&StreamId::new("snapshot-resource")?)
        .await?;
    for offset in 1..=COVERED {
        runtime.append(&stream, fixture_event(offset)?).await?;
    }
    let covered_state = state_sum(1, COVERED);
    let digest = fixture_digest(content_bytes, covered_state);
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([23; 16]),
        covered: Cursor::new(stream.clone(), COVERED),
        schema: SchemaRef {
            id: SchemaId::new("resource.application-state")?,
            version: 1,
        },
        content_bytes: content_bytes as u64,
        digest,
    };
    let mut times = PhaseTimes::default();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let mut phase_vfs = [SqliteVfsSnapshot::default(); 6];
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let mut capture_phase = |index: usize, before: Option<SqliteVfsSnapshot>| -> BenchResult<()> {
        if let Some(before) = before {
            phase_vfs[index] = subtract_vfs(sqlite_vfs_snapshot(), before)?;
        }
        Ok(())
    };
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let phase_vfs_before = || vfs_enabled.then(sqlite_vfs_snapshot);
    let phase = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = phase_vfs_before();
    runtime.begin_snapshot(descriptor.clone()).await?;
    times.begin_ns = phase.elapsed().as_nanos();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    capture_phase(0, vfs_before)?;

    let phase = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = phase_vfs_before();
    let mut offset = 0usize;
    let mut first_chunk = None;
    while offset < content_bytes {
        let bytes = fixture_chunk(
            offset,
            (content_bytes - offset).min(CHUNK_BYTES),
            covered_state,
        );
        if offset == 0 {
            first_chunk = Some(bytes.clone());
        }
        runtime
            .put_snapshot_chunk(
                descriptor.id,
                SnapshotChunk {
                    offset: offset as u64,
                    bytes: Payload::copy_from_slice(&bytes),
                },
            )
            .await?;
        offset += bytes.len();
    }
    times.upload_ns = phase.elapsed().as_nanos();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    capture_phase(1, vfs_before)?;

    let phase = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = phase_vfs_before();
    let published = runtime
        .verify_and_publish_snapshot(
            descriptor.id,
            VerificationLimits {
                max_chunks: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        )
        .await?;
    if published != descriptor
        || runtime.begin_snapshot(descriptor.clone()).await?.state != SnapshotUploadState::Published
    {
        return Err("snapshot publication retry changed its descriptor".into());
    }
    let retry = runtime
        .put_snapshot_chunk(
            descriptor.id,
            SnapshotChunk {
                offset: 0,
                bytes: Payload::copy_from_slice(first_chunk.as_ref().unwrap()),
            },
        )
        .await?;
    if retry.state != SnapshotUploadState::Published {
        return Err("snapshot chunk retry did not reconcile publication".into());
    }
    times.verify_publish_ns = phase.elapsed().as_nanos();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    capture_phase(2, vfs_before)?;

    let phase = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = phase_vfs_before();
    for offset in (COVERED + 1)..=TAIL {
        runtime.append(&stream, fixture_event(offset)?).await?;
    }
    times.suffix_append_ns = phase.elapsed().as_nanos();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    capture_phase(3, vfs_before)?;

    let phase = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = phase_vfs_before();
    let plan = runtime
        .acquire_recovery(descriptor.id, Duration::from_secs(60))
        .await?;
    if plan.snapshot != descriptor || plan.through.offset != TAIL {
        return Err("recovery plan changed descriptor or captured tail".into());
    }
    let mut content_reads = 0usize;
    let mut byte_offset = 0usize;
    let mut recovered_prefix = [0u8; 8];
    while byte_offset < content_bytes {
        let page = runtime
            .read_snapshot_chunk(plan.lease, byte_offset as u64, CHUNK_BYTES)
            .await?;
        let expected = fixture_chunk(byte_offset, page.bytes.len(), covered_state);
        if page.offset != byte_offset as u64 || page.bytes.as_bytes() != expected {
            return Err("snapshot content page is not exact".into());
        }
        if byte_offset == 0 {
            recovered_prefix.copy_from_slice(&page.bytes.as_bytes()[..8]);
        }
        byte_offset = page.next_offset as usize;
        content_reads += 1;
        if page.complete {
            break;
        }
    }
    let mut recovered_state = u64::from_le_bytes(recovered_prefix);
    let mut after = COVERED;
    let mut suffix_records = 0usize;
    loop {
        let page = runtime
            .read_recovery_page(
                plan.lease,
                after,
                PageLimits {
                    max_records: 256,
                    max_bytes: 2 * 1024 * 1024,
                },
            )
            .await?;
        for record in &page.records {
            if record.cursor.offset != after + 1 {
                return Err("recovery suffix is not contiguous".into());
            }
            recovered_state =
                recovered_state.saturating_add(record.event.payload.as_bytes()[0] as u64);
            after = record.cursor.offset;
            suffix_records += 1;
        }
        if page.complete {
            break;
        }
    }
    if recovered_state != state_sum(1, TAIL) || suffix_records != (TAIL - COVERED) as usize {
        return Err("snapshot state plus suffix differs from complete replay".into());
    }
    if runtime.release_recovery(plan.lease).await? != RecoveryRelease::Released
        || !matches!(
            runtime.read_snapshot_chunk(plan.lease, 0, 1).await,
            Err(SnapshotError::ExpiredProtection { .. })
        )
    {
        return Err("released recovery lease remained usable".into());
    }
    let uploads = runtime
        .list_snapshot_uploads(
            None,
            PageLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        )
        .await?;
    if !uploads.entries.is_empty() || !uploads.complete {
        return Err("published snapshot remained in staging".into());
    }
    times.recovery_ns = phase.elapsed().as_nanos();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    capture_phase(4, vfs_before)?;
    let phase = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = phase_vfs_before();
    runtime.shutdown(Duration::from_secs(30)).await?;
    times.shutdown_ns = phase.elapsed().as_nanos();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    capture_phase(5, vfs_before)?;
    Ok(Outcome {
        times,
        descriptor,
        content_reads,
        suffix_records,
        #[cfg(all(feature = "sqlite", feature = "test-support"))]
        phase_vfs: vfs_enabled.then_some(phase_vfs),
    })
}

async fn run_foreground(
    store_name: &str,
    mode: &str,
    content_bytes: usize,
    instrumented: bool,
) -> BenchResult<()> {
    if ![
        "foreground_verify_control",
        "foreground_verify",
        "foreground_recovery_control",
        "foreground_recovery",
    ]
    .contains(&mode)
    {
        return Err("unsupported foreground mode".into());
    }
    let (outcome, database_bytes) = match store_name {
        "memory" => {
            let runtime = Runtime::<MemoryStore>::open(
                MemoryStoreOptions::default(),
                RuntimeConfig::default(),
            )
            .await?;
            (
                exercise_foreground(runtime, mode, content_bytes, instrumented).await?,
                None,
            )
        }
        "sqlite" => run_sqlite_foreground(mode, content_bytes, instrumented).await?,
        _ => return Err("--store must be memory or sqlite".into()),
    };
    let mut latencies = outcome.latencies_ns.clone();
    latencies.sort_unstable();
    let p50 = percentile(&latencies, 50);
    let p99 = percentile(&latencies, 99);
    let maximum = latencies.last().copied().unwrap_or(0);
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs = if store_name == "sqlite" {
        match (outcome.vfs_before, outcome.vfs_after) {
            (Some(before), Some(after)) => Some(subtract_vfs(after, before)?),
            _ => None,
        }
    } else {
        None
    };
    #[cfg(not(all(feature = "sqlite", feature = "test-support")))]
    let vfs: Option<()> = None;
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let sync_timeline_json =
        encode_sync_timeline(outcome.sync_timeline, outcome.sync_timeline_origin_ns);
    #[cfg(not(all(feature = "sqlite", feature = "test-support")))]
    let sync_timeline_json = "null".to_owned();
    println!(
        concat!(
            "{{\"kind\":\"snapshot_foreground_sample\",\"store\":\"{}\",",
            "\"mode\":\"{}\",\"snapshot_bytes\":{},\"chunk_bytes\":{},\"instrumented\":{},",
            "\"producers\":4,\"events_per_producer\":64,\"offered\":256,",
            "\"accepted\":{},\"rejected\":{},\"failed\":{},\"committed\":{},",
            "\"elapsed_ns\":{},\"append_p50_ns\":{},\"append_p99_ns\":{},\"append_max_ns\":{},",
            "\"append_latencies_ns\":{:?},\"append_intervals\":{},",
            "\"snapshot_phase_ns\":{},\"snapshot_start_ns\":{},\"snapshot_end_ns\":{},",
            "\"overlapping_append_receipts\":{},\"vfs_sync_timeline\":{},",
            "\"allocation_count\":{},\"allocated_bytes\":{},\"deallocated_bytes\":{},",
            "\"rust_live_before\":{},\"rust_live_after\":{},\"sampled_peak_rust_live_bytes\":{},",
            "\"rss_before_bytes\":{},\"rss_after_bytes\":{},\"sampled_peak_rss_bytes\":{},",
            "\"max_rss_bytes\":{},\"cpu_user_us\":{},\"cpu_system_us\":{},",
            "\"sampler_samples\":{},\"database_bytes\":{},\"vfs\":{},",
            "\"timing_scope\":\"barrier_release_through_snapshot_phase_and_all_closed_loop_append_receipts\",",
            "\"correctness\":\"256_unique_ids_contiguous_offsets_exact_count_conservation\"}}"
        ),
        store_name,
        mode,
        content_bytes,
        CHUNK_BYTES,
        instrumented,
        outcome.accepted,
        outcome.rejected,
        outcome.failed,
        outcome.committed,
        outcome.elapsed_ns,
        p50,
        p99,
        maximum,
        latencies,
        encode_append_intervals(&outcome.append_intervals),
        outcome.snapshot_phase_ns,
        outcome.snapshot_start_ns,
        outcome.snapshot_end_ns,
        outcome.overlapping_append_receipts,
        sync_timeline_json,
        optional_u64(instrumented.then(|| outcome.allocation_after.count.saturating_sub(outcome.allocation_before.count))),
        optional_u64(instrumented.then(|| outcome.allocation_after.allocated.saturating_sub(outcome.allocation_before.allocated))),
        optional_u64(instrumented.then(|| outcome.allocation_after.deallocated.saturating_sub(outcome.allocation_before.deallocated))),
        optional_u64(instrumented.then(|| outcome.allocation_before.live())),
        optional_u64(instrumented.then(|| outcome.allocation_after.live())),
        optional_u64(outcome.peaks.as_ref().map(|peak| peak.rust_live)),
        optional_u64(outcome.rss_before),
        optional_u64(outcome.rss_after),
        optional_u64(outcome.peaks.as_ref().map(|peak| peak.rss)),
        outcome.usage_after.max_rss,
        outcome.usage_after.user_us.saturating_sub(outcome.usage_before.user_us),
        outcome.usage_after.system_us.saturating_sub(outcome.usage_before.system_us),
        outcome.peaks.as_ref().map_or(0, |peak| peak.samples),
        optional_u64(database_bytes),
        encode_vfs(vfs),
    );
    Ok(())
}

async fn exercise_foreground<S: SnapshotStore>(
    runtime: Runtime<S>,
    mode: &str,
    content_bytes: usize,
    instrumented: bool,
) -> BenchResult<ForegroundOutcome> {
    let snapshot_stream = runtime
        .create_stream(&StreamId::new("snapshot-foreground-state")?)
        .await?;
    for offset in 1..=COVERED {
        runtime
            .append(&snapshot_stream, fixture_event(offset)?)
            .await?;
    }
    let state = state_sum(1, COVERED);
    let descriptor = SnapshotDescriptor {
        id: SnapshotId::from_bytes([31; 16]),
        covered: Cursor::new(snapshot_stream, COVERED),
        schema: SchemaRef {
            id: SchemaId::new("resource.application-state")?,
            version: 1,
        },
        content_bytes: content_bytes as u64,
        digest: fixture_digest(content_bytes, state),
    };
    runtime.begin_snapshot(descriptor.clone()).await?;
    let mut offset = 0usize;
    while offset < content_bytes {
        let bytes = fixture_chunk(offset, (content_bytes - offset).min(CHUNK_BYTES), state);
        runtime
            .put_snapshot_chunk(
                descriptor.id,
                SnapshotChunk {
                    offset: offset as u64,
                    bytes: Payload::copy_from_slice(&bytes),
                },
            )
            .await?;
        offset += bytes.len();
    }
    if mode.starts_with("foreground_recovery") {
        runtime
            .verify_and_publish_snapshot(
                descriptor.id,
                VerificationLimits {
                    max_chunks: 256,
                    max_bytes: 2 * 1024 * 1024,
                },
            )
            .await?;
    }
    let recovery = if mode.starts_with("foreground_recovery") {
        Some(
            runtime
                .acquire_recovery(descriptor.id, Duration::from_secs(60))
                .await?,
        )
    } else {
        None
    };
    let foreground = runtime
        .create_stream(&StreamId::new("snapshot-foreground-appends")?)
        .await?;
    let diagnostics_before = runtime.diagnostics().await;
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_before = instrumented.then(sqlite_vfs_snapshot);
    let allocation_before = AllocationSnapshot::read();
    let usage_before = Usage::read()?;
    let rss_before = current_rss_bytes();
    let sampler = Sampler::start(instrumented);
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(5));
    let started = Instant::now();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let sync_timeline_origin_ns = instrumented.then(sqlite_vfs_observation_nanos);
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let sync_timeline_start = instrumented.then(sqlite_vfs_sync_timeline_position);
    let mut producers = Vec::new();
    for producer in 0..4u64 {
        let runtime = runtime.clone();
        let stream = foreground.clone();
        let barrier = barrier.clone();
        let origin = started;
        producers.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut rows = Vec::with_capacity(64);
            for sequence in 0..64u64 {
                let event = NewEvent {
                    id: EventId::new(format!("foreground-{producer}-{sequence}"))?,
                    schema: SchemaRef {
                        id: SchemaId::new("resource.foreground")?,
                        version: 1,
                    },
                    payload: Payload::copy_from_slice(&[producer as u8; EVENT_PAYLOAD_BYTES]),
                };
                let begin_ns = origin.elapsed().as_nanos();
                let receipt = runtime.append(&stream, event).await?;
                let end_ns = origin.elapsed().as_nanos();
                rows.push((producer, sequence, begin_ns, end_ns, receipt));
            }
            Ok::<_, event_stream::Error>(rows)
        }));
    }
    barrier.wait().await;
    let snapshot_start_ns = started.elapsed().as_nanos();
    match mode {
        "foreground_verify_control" | "foreground_recovery_control" => {}
        "foreground_verify" => {
            let published = runtime
                .verify_and_publish_snapshot(
                    descriptor.id,
                    VerificationLimits {
                        max_chunks: 256,
                        max_bytes: 2 * 1024 * 1024,
                    },
                )
                .await?;
            if published != descriptor {
                return Err("foreground verification changed the descriptor".into());
            }
        }
        "foreground_recovery" => {
            let plan = recovery.as_ref().unwrap();
            let mut offset = 0u64;
            loop {
                let page = runtime
                    .read_snapshot_chunk(plan.lease, offset, CHUNK_BYTES)
                    .await?;
                if page.offset != offset
                    || page.bytes.as_bytes()
                        != fixture_chunk(offset as usize, page.bytes.len(), state)
                {
                    return Err("foreground recovery returned different snapshot bytes".into());
                }
                offset = page.next_offset;
                if page.complete {
                    if offset != descriptor.content_bytes {
                        return Err(
                            "foreground recovery completed at the wrong byte boundary".into()
                        );
                    }
                    break;
                }
            }
        }
        _ => unreachable!(),
    }
    let snapshot_end_ns = started.elapsed().as_nanos();
    let mut receipts = Vec::with_capacity(256);
    let mut latencies_ns = Vec::with_capacity(256);
    let mut append_intervals = Vec::with_capacity(256);
    let mut overlapping_append_receipts = 0usize;
    for producer in producers {
        for (producer, sequence, begin_ns, end_ns, receipt) in producer.await?? {
            latencies_ns.push(end_ns.saturating_sub(begin_ns));
            if !mode.ends_with("control")
                && begin_ns < snapshot_end_ns
                && end_ns > snapshot_start_ns
            {
                overlapping_append_receipts += 1;
            }
            append_intervals.push(AppendInterval {
                producer,
                sequence,
                begin_ns,
                end_ns,
            });
            receipts.push(receipt);
        }
    }
    let elapsed_ns = started.elapsed().as_nanos();
    let peaks = sampler.map(Sampler::finish);
    let rss_after = current_rss_bytes();
    let usage_after = Usage::read()?;
    let allocation_after = AllocationSnapshot::read();
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let vfs_after = instrumented.then(sqlite_vfs_snapshot);
    #[cfg(all(feature = "sqlite", feature = "test-support"))]
    let sync_timeline = sync_timeline_start.map(sqlite_vfs_sync_timeline_since);
    let diagnostics_after = runtime.diagnostics().await;
    let mut offsets: Vec<_> = receipts
        .iter()
        .map(|receipt| receipt.record.cursor.offset)
        .collect();
    offsets.sort_unstable();
    if receipts.len() != 256
        || offsets != (1..=256).collect::<Vec<_>>()
        || receipts
            .iter()
            .any(|receipt| receipt.kind != AppendKind::Inserted)
    {
        return Err("foreground receipts are not 256 unique contiguous inserts".into());
    }
    let accepted = diagnostics_after.append_accepted - diagnostics_before.append_accepted;
    let rejected = diagnostics_after.append_rejected - diagnostics_before.append_rejected;
    let failed = diagnostics_after.append_failed - diagnostics_before.append_failed;
    if accepted != 256 || rejected != 0 || failed != 0 {
        return Err("foreground runtime count conservation failed".into());
    }
    if let Some(plan) = recovery {
        runtime.release_recovery(plan.lease).await?;
    }
    if mode == "foreground_verify_control" {
        runtime.abort_snapshot(descriptor.id).await?;
    }
    runtime.shutdown(Duration::from_secs(30)).await?;
    Ok(ForegroundOutcome {
        elapsed_ns,
        latencies_ns,
        append_intervals,
        accepted,
        rejected,
        failed,
        committed: receipts.len(),
        snapshot_phase_ns: snapshot_end_ns.saturating_sub(snapshot_start_ns),
        snapshot_start_ns,
        snapshot_end_ns,
        overlapping_append_receipts,
        allocation_before,
        allocation_after,
        usage_before,
        usage_after,
        rss_before,
        rss_after,
        peaks,
        #[cfg(all(feature = "sqlite", feature = "test-support"))]
        vfs_before,
        #[cfg(all(feature = "sqlite", feature = "test-support"))]
        vfs_after,
        #[cfg(all(feature = "sqlite", feature = "test-support"))]
        sync_timeline,
        #[cfg(all(feature = "sqlite", feature = "test-support"))]
        sync_timeline_origin_ns,
    })
}

#[cfg(feature = "sqlite")]
async fn run_sqlite_foreground(
    mode: &str,
    content_bytes: usize,
    instrumented: bool,
) -> BenchResult<(ForegroundOutcome, Option<u64>)> {
    let root = std::env::temp_dir().join(format!(
        "event-stream-snapshot-foreground-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root)?;
    let path = root.join("store.sqlite3");
    let outcome = exercise_foreground(
        Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default()).await?,
        mode,
        content_bytes,
        instrumented,
    )
    .await?;
    let database_bytes = std::fs::metadata(&path)?.len();
    std::fs::remove_dir_all(root)?;
    Ok((outcome, Some(database_bytes)))
}

#[cfg(not(feature = "sqlite"))]
async fn run_sqlite_foreground(
    _: &str,
    _: usize,
    _: bool,
) -> BenchResult<(ForegroundOutcome, Option<u64>)> {
    Err("sqlite feature is required for --store sqlite".into())
}

#[cfg(feature = "sqlite")]
async fn run_sqlite(
    content_bytes: usize,
    instrumented: bool,
) -> BenchResult<(Outcome, Option<u64>)> {
    let root = std::env::temp_dir().join(format!(
        "event-stream-snapshot-resource-{}",
        uuid::Uuid::new_v4()
    ));
    std::fs::create_dir(&root)?;
    let path = root.join("store.sqlite3");
    let outcome = exercise(
        Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default()).await?,
        content_bytes,
        instrumented,
    )
    .await?;
    let database_bytes = std::fs::metadata(&path)?.len();
    let reopened =
        Runtime::<SqliteStore>::open(SqliteOptions::new(&path), RuntimeConfig::default()).await?;
    let plan = reopened
        .acquire_recovery(outcome.descriptor.id, Duration::from_secs(30))
        .await?;
    if plan.snapshot != outcome.descriptor || plan.through.offset != TAIL {
        return Err("SQLite reopen changed snapshot recovery plan".into());
    }
    let expected_state = state_sum(1, COVERED);
    let mut offset = 0usize;
    while offset < content_bytes {
        let page = reopened
            .read_snapshot_chunk(plan.lease, offset as u64, CHUNK_BYTES)
            .await?;
        if page.bytes.as_bytes() != fixture_chunk(offset, page.bytes.len(), expected_state) {
            return Err("SQLite reopen changed snapshot content".into());
        }
        offset = page.next_offset as usize;
        if page.complete {
            break;
        }
    }
    let suffix = reopened
        .read_recovery_page(
            plan.lease,
            COVERED,
            PageLimits {
                max_records: 256,
                max_bytes: 2 * 1024 * 1024,
            },
        )
        .await?;
    if !suffix.complete
        || suffix.next_after.offset != TAIL
        || suffix.records.len() != (TAIL - COVERED) as usize
        || !suffix
            .records
            .iter()
            .enumerate()
            .all(|(index, record)| record.cursor.offset == COVERED + index as u64 + 1)
    {
        return Err("SQLite reopen changed the protected suffix".into());
    }
    reopened.release_recovery(plan.lease).await?;
    reopened.shutdown(Duration::from_secs(30)).await?;
    std::fs::remove_dir_all(&root)?;
    Ok((outcome, Some(database_bytes)))
}

#[cfg(not(feature = "sqlite"))]
async fn run_sqlite(_: usize, _: bool) -> BenchResult<(Outcome, Option<u64>)> {
    Err("sqlite feature is required for --store sqlite".into())
}

fn fixture_event(offset: u64) -> Result<NewEvent> {
    Ok(NewEvent {
        id: EventId::new(format!("resource-{offset:03}"))?,
        schema: SchemaRef {
            id: SchemaId::new("resource.event")?,
            version: 1,
        },
        payload: Payload::copy_from_slice(&[(offset % 256) as u8; EVENT_PAYLOAD_BYTES]),
    })
}

fn state_sum(first: u64, last: u64) -> u64 {
    (first..=last).map(|offset| offset % 256).sum()
}

fn fixture_chunk(offset: usize, length: usize, state: u64) -> Vec<u8> {
    (offset..offset + length)
        .map(|position| {
            if position < 8 {
                state.to_le_bytes()[position]
            } else {
                ((position as u64 * 31 + 7) % 251) as u8
            }
        })
        .collect()
}

fn fixture_digest(content_bytes: usize, state: u64) -> SnapshotDigest {
    let mut hasher = Sha256::new();
    let mut offset = 0usize;
    while offset < content_bytes {
        let bytes = fixture_chunk(offset, (content_bytes - offset).min(CHUNK_BYTES), state);
        hasher.update(bytes);
        offset += CHUNK_BYTES.min(content_bytes - offset);
    }
    SnapshotDigest::from_bytes(hasher.finalize().into())
}

fn value<'a>(args: &'a [String], key: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|pair| pair[0] == key)
        .map(|pair| pair[1].as_str())
}

fn optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".into(), |value| value.to_string())
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len() - 1).saturating_mul(percentile) / 100;
    sorted[index]
}

fn encode_append_intervals(value: &[AppendInterval]) -> String {
    let mut encoded = String::from("[");
    for (index, value) in value.iter().enumerate() {
        if index != 0 {
            encoded.push(',');
        }
        encoded.push_str(&format!(
            "{{\"producer\":{},\"sequence\":{},\"begin_ns\":{},\"end_ns\":{}}}",
            value.producer, value.sequence, value.begin_ns, value.end_ns
        ));
    }
    encoded.push(']');
    encoded
}

#[cfg(all(feature = "sqlite", feature = "test-support"))]
fn encode_sync_timeline(value: Option<SqliteVfsSyncTimeline>, origin_ns: Option<u64>) -> String {
    let (Some(value), Some(origin_ns)) = (value, origin_ns) else {
        return "null".into();
    };
    let mut events = String::from("[");
    for (index, event) in value.events.iter().enumerate() {
        if index != 0 {
            events.push(',');
        }
        let category = match event.category {
            0 => "source",
            1 => "target",
            2 => "source_journal",
            3 => "target_journal",
            4 => "temporary",
            _ => "other",
        };
        events.push_str(&format!(
            "{{\"sequence\":{},\"category\":\"{}\",\"start_ns\":{},\"end_ns\":{}}}",
            event.sequence,
            category,
            event.start_ns.saturating_sub(origin_ns),
            event.end_ns.saturating_sub(origin_ns),
        ));
    }
    events.push(']');
    format!("{{\"events\":{events},\"dropped\":{}}}", value.dropped)
}

#[cfg(all(feature = "sqlite", feature = "test-support"))]
fn subtract_vfs(
    after: SqliteVfsSnapshot,
    before: SqliteVfsSnapshot,
) -> BenchResult<SqliteVfsSnapshot> {
    fn category(
        after: SqliteVfsCategorySnapshot,
        before: SqliteVfsCategorySnapshot,
    ) -> BenchResult<SqliteVfsCategorySnapshot> {
        macro_rules! difference {
            ($field:ident) => {
                after
                    .$field
                    .checked_sub(before.$field)
                    .ok_or_else(|| format!("VFS counter {} moved backwards", stringify!($field)))?
            };
        }
        Ok(SqliteVfsCategorySnapshot {
            opens: difference!(opens),
            deletes: difference!(deletes),
            read_calls: difference!(read_calls),
            read_bytes: difference!(read_bytes),
            write_calls: difference!(write_calls),
            write_bytes: difference!(write_bytes),
            truncate_calls: difference!(truncate_calls),
            sync_calls: difference!(sync_calls),
            sync_nanos: difference!(sync_nanos),
            fetch_calls: difference!(fetch_calls),
            non_ok_callbacks: difference!(non_ok_callbacks),
            short_read_callbacks: difference!(short_read_callbacks),
        })
    }
    Ok(SqliteVfsSnapshot {
        source: category(after.source, before.source)?,
        target: category(after.target, before.target)?,
        source_journal: category(after.source_journal, before.source_journal)?,
        target_journal: category(after.target_journal, before.target_journal)?,
        temporary: category(after.temporary, before.temporary)?,
        other: category(after.other, before.other)?,
        // Peak is run-wide. It is not a subtractable counter.
        temporary_peak_logical_bytes: after.temporary_peak_logical_bytes,
        temporary_live_logical_bytes: after.temporary_live_logical_bytes,
        optional_method_mismatches: after
            .optional_method_mismatches
            .checked_sub(before.optional_method_mismatches)
            .ok_or("VFS optional-method mismatch counter moved backwards")?,
        underlying_vfs_interface_version: after.underlying_vfs_interface_version,
        recorder_vfs_interface_version: after.recorder_vfs_interface_version,
    })
}

#[cfg(all(feature = "sqlite", feature = "test-support"))]
fn encode_vfs(value: Option<SqliteVfsSnapshot>) -> String {
    let Some(value) = value else {
        return "null".into();
    };
    fn category(value: SqliteVfsCategorySnapshot) -> String {
        format!(
            concat!(
                "{{\"opens\":{},\"deletes\":{},\"read_calls\":{},\"read_bytes\":{},",
                "\"write_calls\":{},\"write_bytes\":{},\"truncate_calls\":{},",
                "\"sync_calls\":{},\"sync_nanos\":{},\"fetch_calls\":{},",
                "\"non_ok_callbacks\":{},\"short_read_callbacks\":{}}}"
            ),
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
    format!(
        concat!(
            "{{\"source\":{},\"target\":{},\"source_journal\":{},",
            "\"target_journal\":{},\"temporary\":{},\"other\":{},",
            "\"temporary_live_logical_bytes\":{},\"temporary_peak_logical_bytes\":{},",
            "\"optional_method_mismatches\":{},\"underlying_vfs_interface_version\":{},",
            "\"recorder_vfs_interface_version\":{}}}"
        ),
        category(value.source),
        category(value.target),
        category(value.source_journal),
        category(value.target_journal),
        category(value.temporary),
        category(value.other),
        value.temporary_live_logical_bytes,
        value.temporary_peak_logical_bytes,
        value.optional_method_mismatches,
        value.underlying_vfs_interface_version,
        value.recorder_vfs_interface_version,
    )
}

#[cfg(all(feature = "sqlite", feature = "test-support"))]
fn encode_phase_vfs(value: Option<[SqliteVfsSnapshot; 6]>) -> String {
    let Some(value) = value else {
        return "null".into();
    };
    format!(
        "{{\"begin\":{},\"upload\":{},\"verify_publish\":{},\"suffix_append\":{},\"recovery\":{},\"shutdown\":{}}}",
        encode_vfs(Some(value[0])),
        encode_vfs(Some(value[1])),
        encode_vfs(Some(value[2])),
        encode_vfs(Some(value[3])),
        encode_vfs(Some(value[4])),
        encode_vfs(Some(value[5])),
    )
}

#[cfg(not(all(feature = "sqlite", feature = "test-support")))]
fn encode_vfs(_: Option<()>) -> String {
    "null".into()
}

#[cfg(not(all(feature = "sqlite", feature = "test-support")))]
fn encode_phase_vfs(_: Option<()>) -> String {
    "null".into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
