#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
evidence="$repo/docs/adr/001-event-stream/evidence"
provenance="$evidence/performance-instrumentation-provenance.json"
instrumented="$repo/target/release/examples/performance_baseline-instrumented"
control="$repo/target/release/examples/performance_baseline-no-alloc"
output="$evidence/performance-instrumentation-overhead.jsonl"
partial="$output.partial"

cd "$repo"
shasum -a 256 -c "$evidence/performance-build-inputs.sha256" >/dev/null
expected_instrumented=$(sed -n 's/.*"instrumented_binary_sha256": "\([0-9a-f]*\)".*/\1/p' "$provenance")
expected_control=$(sed -n 's/.*"control_binary_sha256": "\([0-9a-f]*\)".*/\1/p' "$provenance")
test "$expected_instrumented" = "$(shasum -a 256 "$instrumented" | awk '{print $1}')"
test "$expected_control" = "$(shasum -a 256 "$control" | awk '{print $1}')"
: > "$partial"

for store in memory sqlite; do
    for mode in instrumented control; do
        if test "$mode" = instrumented; then binary=$instrumented; else binary=$control; fi
        "$binary" --store "$store" --scenario append --payload-bytes 128 \
            --producers 8 --streams 8 --events 1000 --repetitions 5 >> "$partial"
    done
done

jq -e -c . "$partial" >/dev/null
python3 - "$partial" <<'PY'
import json
import statistics
import sys

path = sys.argv[1]
rows = [json.loads(line) for line in open(path, encoding="utf-8")]
samples = [row for row in rows if row["kind"] == "sample"]
assert len(samples) == 20, len(samples)
groups = {}
for row in rows:
    if row["kind"] == "config":
        key = (row["store"], row["instrumentation"])
        groups.setdefault(key, {})["config"] = row
for row in samples:
    mode = next(
        item["instrumentation"] for item in rows
        if item["kind"] == "config"
        and item["store"] == row["store"]
        and item["scenario"] == row["scenario"]
        and ((item["instrumentation"] == "stage_timestamps_without_allocator_counters")
             == (row["rust_allocation_count"] is None))
    )
    key = (row["store"], mode)
    groups.setdefault(key, {}).setdefault("samples", []).append(row)
    assert row["offered"] == row["runtime_accepted"] + row["runtime_rejected"]
    assert row["runtime_accepted"] == row["inserted"] + row["deduplicated"] + row["runtime_failed"]
    assert row["caller_failed"] == 0

comparisons = []
for store in ("memory", "sqlite"):
    measured = groups[(store, "Rust_System_GlobalAlloc_atomic_counters_and_stage_timestamps")]["samples"]
    control = groups[(store, "stage_timestamps_without_allocator_counters")]["samples"]
    assert len(measured) == len(control) == 5
    assert all(row["rust_allocation_count"] is not None for row in measured)
    assert all(row["rust_allocation_count"] is None for row in control)
    measured_elapsed = statistics.median(row["elapsed_ns"] for row in measured)
    control_elapsed = statistics.median(row["elapsed_ns"] for row in control)
    measured_cpu = statistics.median(row["cpu_user_us"] + row["cpu_system_us"] for row in measured)
    control_cpu = statistics.median(row["cpu_user_us"] + row["cpu_system_us"] for row in control)
    comparisons.append({
        "kind": "instrumentation_comparison",
        "store": store,
        "scenario": "append",
        "payload_bytes": 128,
        "producers": 8,
        "streams": 8,
        "events_per_producer": 1000,
        "repetitions_per_mode": 5,
        "instrumented_elapsed_median_ns": measured_elapsed,
        "control_elapsed_median_ns": control_elapsed,
        "elapsed_overhead_ratio": measured_elapsed / control_elapsed,
        "instrumented_cpu_median_us": measured_cpu,
        "control_cpu_median_us": control_cpu,
        "cpu_overhead_ratio": measured_cpu / control_cpu,
        "scope": "paired_sequential_repeated_process_modes;not_interleaved;stage_timestamps_present_in_both",
    })
with open(path, "a", encoding="utf-8") as output:
    for row in comparisons:
        output.write(json.dumps(row, separators=(",", ":")) + "\n")
print("validated 20 samples and wrote 2 allocator-instrumentation comparisons")
PY

shasum -a 256 -c "$evidence/performance-build-inputs.sha256" >/dev/null
test "$expected_instrumented" = "$(shasum -a 256 "$instrumented" | awk '{print $1}')"
test "$expected_control" = "$(shasum -a 256 "$control" | awk '{print $1}')"
mv "$partial" "$output"
shasum -a 256 "$output"
