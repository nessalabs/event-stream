#[path = "../verification/fixtures/mixed_workload.rs"]
mod mixed;
mod support;

use std::{path::PathBuf, time::Instant};

fn percentile(values: &mut [u64], percent: usize) -> Option<u64> {
    values.sort_unstable();
    (!values.is_empty()).then(|| values[(values.len() * percent).div_ceil(100).saturating_sub(1)])
}
fn number(value: Option<u64>) -> String {
    value.map_or_else(|| "null".into(), |v| v.to_string())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 || !["control", "mixed"].contains(&args[0].as_str()) {
        return Err("usage: mixed_resource control|mixed SNAPSHOT_BYTES NEW_DIRECTORY".into());
    }
    let config = mixed::MixedConfig {
        mixed: args[0] == "mixed",
        offers: 512,
        interval_us: 1250,
        snapshot_bytes: args[1].parse()?,
    };
    let directory = PathBuf::from(&args[2]);
    let allocations = support::AllocationSnapshot::read();
    let usage = support::Usage::read()?;
    let sampler = support::Sampler::start(true).unwrap();
    let start = Instant::now();
    let mut result = mixed::run(&directory, config).await;
    let elapsed_ns = start.elapsed().as_nanos();
    let peaks = sampler.finish();
    let after = support::Usage::read()?;
    let allocation_after = support::AllocationSnapshot::read();
    let p50 = number(percentile(&mut result.receipt_ns, 50));
    let p95 = number(percentile(&mut result.receipt_ns, 95));
    let p99 = number(percentile(&mut result.receipt_ns, 99));
    let lateness = number(percentile(&mut result.lateness_ns, 99));
    println!(concat!("{{\"kind\":\"mixed_sample\",\"mode\":\"{}\",\"snapshot_bytes\":{},",
        "\"offered\":{},\"accepted\":{},\"runtime_rejected\":{},\"generator_rejected\":{},\"peak_tasks\":{},",
        "\"receipt_p50_ns\":{},\"receipt_p95_ns\":{},\"receipt_p99_ns\":{},\"schedule_lateness_p99_ns\":{},",
        "\"receipt_samples\":{},\"lateness_samples\":{},\"overlap_receipts\":{},\"maintenance_ns\":{},",
        "\"replay_pages\":{},\"removed_records\":{},\"replica_tail\":{},\"peak_runtime_queue\":{},",
        "\"elapsed_ns\":{},\"cpu_us\":{},\"rss_peak_bytes\":{},\"rust_live_peak_bytes\":{},\"allocation_count\":{},\"allocated_bytes\":{},\"rss_samples\":{},\"process_lifetime_max_rss_bytes\":{},",
        "\"measurement_scope\":\"whole_scenario_including_open_setup_verification_and_reopen\",",
        "\"receipt_scope\":\"actual_submission_to_receipt_excludes_schedule_delay\",",
        "\"memory_scope\":\"sampled_process_including_harness_SQLite_and_profiler\",",
        "\"durability\":\"SQLite_DELETE_FULL_ProcessRestart\",\"failures\":0}}"),
        args[0], config.snapshot_bytes, result.offered, result.accepted, result.runtime_rejected, result.generator_rejected, result.peak_tasks,
        p50, p95, p99, lateness, result.receipt_ns.len(), result.lateness_ns.len(), result.overlap_receipts, result.maintenance_ns,
        result.replay_pages, result.removed_records, result.replica_tail, result.peak_runtime_queue,
        elapsed_ns, after.user_us - usage.user_us + after.system_us - usage.system_us, peaks.rss, peaks.rust_live,
        allocation_after.count - allocations.count, allocation_after.allocated - allocations.allocated, peaks.samples, after.max_rss);
    Ok(())
}
