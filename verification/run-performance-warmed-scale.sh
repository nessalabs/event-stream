#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
evidence="$repo/docs/adr/001-event-stream/evidence"
binary="$repo/target/release/examples/performance_baseline"
mode=${1:-bounded}

case "$mode" in
    bounded) populations="1000 10000" ;;
    full) populations="1000 10000 100000" ;;
    hundred-thousand) populations="100000" ;;
    *) echo "usage: $0 [bounded|full|hundred-thousand]" >&2; exit 2 ;;
esac

cd "$repo"
input_digest=$(shasum -a 256 "$evidence/performance-build-inputs.sha256" | awk '{print $1}')
archive="performance-source-$input_digest.tar"
(cd "$evidence" && shasum -a 256 -c "$archive.sha256" >/dev/null)
if test "$mode" = hundred-thousand; then
    output="$evidence/performance-warmed-subscription-scale-100k-$input_digest.jsonl"
else
    output="$evidence/performance-warmed-subscription-scale-$input_digest.jsonl"
fi
partial="$output.partial"
expected_binary=$(sed -n 's/.*"binary_sha256": "\([0-9a-f]*\)".*/\1/p' "$evidence/performance-build-provenance.json")
actual_binary=$(shasum -a 256 "$binary" | awk '{print $1}')
test "$expected_binary" = "$actual_binary"
: > "$partial"

run_case() {
    python3 -c '
import json, subprocess, sys
destination, program, *arguments = sys.argv[1:]
with open(destination, "ab", buffering=0) as output:
    process = subprocess.Popen(
        [program, *arguments], stdout=output, stderr=subprocess.PIPE,
    )
    try:
        _, stderr = process.communicate(timeout=900)
    except subprocess.TimeoutExpired:
        process.kill()
        _, stderr = process.communicate()
        output.write((json.dumps({
            "kind": "case_failure",
            "reason": "watchdog_timeout",
            "timeout_seconds": 900,
            "arguments": arguments,
        }, separators=(",", ":")) + "\n").encode())
        sys.stderr.buffer.write(stderr)
        raise SystemExit(124)
    if process.returncode != 0:
        output.write((json.dumps({
            "kind": "case_failure",
            "reason": "process_exit",
            "exit_code": process.returncode,
            "arguments": arguments,
        }, separators=(",", ":")) + "\n").encode())
        sys.stderr.buffer.write(stderr)
        raise SystemExit(process.returncode)
' "$partial" "$binary" "$@"
}

for store in memory sqlite; do
    for population in $populations; do
        run_case --store "$store" --scenario warmed_subscription_scale \
            --payload-bytes 128 --producers 1 --streams 1 \
            --subscribers "$population" --events 1 --repetitions 3
    done
done

jq -e -c . "$partial" >/dev/null
python3 - "$partial" "$mode" <<'PY'
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8")]
samples = [row for row in rows if row["kind"] == "sample"]
populations = {
    "bounded": [1_000, 10_000],
    "full": [1_000, 10_000, 100_000],
    "hundred-thousand": [100_000],
}[sys.argv[2]]
expected = {
    (store, population, repetition)
    for store in ("memory", "sqlite")
    for population in populations
    for repetition in (1, 2, 3)
}
observed = {
    (row["store"], row["logical_agents"], row["repetition"])
    for row in samples
}
assert len(samples) == len(expected), (len(samples), len(expected))
assert observed == expected, (expected - observed, observed - expected)
for row in samples:
    assert row["scenario"] == "warmed_subscription_scale"
    assert row["streams"] == 1
    assert row["subscribers"] == row["logical_agents"]
    assert row["settled_active_subscriptions"] == row["logical_agents"]
    assert row["offered"] == 0
    assert row["runtime_accepted"] == 0
    assert row["runtime_rejected"] == 0
    assert row["runtime_failed"] == 0
    assert row["shutdown_queued_appends"] == 0
    assert row["shutdown_admission_waiters"] == 0
    assert row["shutdown_active_subscriptions"] == 0

resources = [row for row in rows if row["kind"] == "resources"]
warmups = [row for row in rows if row["kind"] == "warmup"]
assert len(resources) == len(expected)
assert len(warmups) == len(expected)
for row in resources:
    assert row["shutdown_closed"] is True
    assert row["shutdown_unresolved"] == 0
for row in warmups:
    assert row["streams"] == 1
    assert row["subscribers"] == row["logical_agents"]
    assert row["records_per_subscription"] == 256
    assert row["drain_completion"] == (
        "every_subscription_returned_256_exact_records_and_"
        "empty_buffer_released_synchronously"
    )
print(f"validated {len(rows)} rows and {len(samples)} warmed samples")
PY

(cd "$evidence" && shasum -a 256 -c "$archive.sha256" >/dev/null)
actual_binary=$(shasum -a 256 "$binary" | awk '{print $1}')
test "$expected_binary" = "$actual_binary"
mv "$partial" "$output"
shasum -a 256 "$output"
