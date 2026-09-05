#!/bin/sh
set -eu

repo=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
evidence="$repo/docs/adr/001-event-stream/evidence"
binary="$repo/target/release/examples/performance_baseline"
output="$evidence/performance-scale.jsonl"
partial="$output.partial"
mode=${1:-bounded}

case "$mode" in
    bounded) populations="1000 10000" ;;
    full) populations="1000 10000 100000" ;;
    *) echo "usage: $0 [bounded|full]" >&2; exit 2 ;;
esac

cd "$repo"
shasum -a 256 -c "$evidence/performance-build-inputs.sha256" >/dev/null
expected=$(sed -n 's/.*"binary_sha256": "\([0-9a-f]*\)".*/\1/p' "$evidence/performance-build-provenance.json")
actual=$(shasum -a 256 "$binary" | awk '{print $1}')
test "$expected" = "$actual"
: > "$partial"

run_case() {
    python3 -c '
import subprocess, sys
destination, program, *arguments = sys.argv[1:]
try:
    result = subprocess.run(
        [program, *arguments], check=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, timeout=900,
    )
except subprocess.TimeoutExpired as error:
    sys.stderr.write(f"performance case exceeded 900 seconds: {arguments}\n")
    raise SystemExit(124) from error
except subprocess.CalledProcessError as error:
    sys.stderr.buffer.write(error.stderr)
    raise SystemExit(error.returncode) from error
with open(destination, "ab") as output:
    output.write(result.stdout)
' "$partial" "$binary" "$@"
}

for store in memory sqlite; do
    for population in $populations; do
        run_case --store "$store" --scenario idle_scale --payload-bytes 128 \
            --producers 1 --streams "$population" --events 1 --repetitions 3
        run_case --store "$store" --scenario subscription_scale --payload-bytes 128 \
            --producers 1 --streams "$population" --subscribers "$population" \
            --events 1 --repetitions 3
        run_case --store "$store" --scenario active_streams --payload-bytes 128 \
            --producers 64 --streams "$population" --events 1 --repetitions 3
        run_case --store "$store" --scenario agent_burst --payload-bytes 128 \
            --producers 64 --streams "$population" --events 1 --repetitions 3
    done
done

jq -e -c . "$partial" >/dev/null
python3 - "$partial" "$mode" <<'PY'
import json
import sys

rows = [json.loads(line) for line in open(sys.argv[1], encoding="utf-8")]
samples = [row for row in rows if row["kind"] == "sample"]
populations = [1_000, 10_000] + ([100_000] if sys.argv[2] == "full" else [])
expected = {
    (store, population, scenario, repetition)
    for store in ("memory", "sqlite")
    for population in populations
    for scenario in ("idle_scale", "subscription_scale", "active_streams", "agent_burst")
    for repetition in (1, 2, 3)
}
observed = {
    (row["store"], row["logical_agents"], row["scenario"], row["repetition"])
    for row in samples
}
assert len(samples) == len(expected), (len(samples), len(expected))
assert observed == expected, (expected - observed, observed - expected)
for row in samples:
    assert row["offered"] == row["runtime_accepted"] + row["runtime_rejected"]
    assert row["runtime_accepted"] == (
        row["inserted"] + row["deduplicated"] + row["runtime_failed"]
    )
    assert row["caller_failed"] == 0
    assert row["shutdown_queued_appends"] == 0
    assert row["shutdown_admission_waiters"] == 0
    assert row["shutdown_active_subscriptions"] == 0
    if row["scenario"] == "subscription_scale":
        assert row["offered"] == 0
        assert row["settled_active_subscriptions"] == row["logical_agents"]
    if row["scenario"] == "idle_scale":
        assert row["offered"] == 0
    if row["scenario"] == "active_streams":
        assert row["inserted"] == row["logical_agents"]
    if row["scenario"] == "agent_burst":
        assert row["append_tasks"] == row["logical_agents"]
resources = [row for row in rows if row["kind"] == "resources"]
assert len(resources) == len(expected)
for row in resources:
    assert row["shutdown_closed"] is True
    assert row["shutdown_unresolved"] == 0
    if row["scenario"] == "agent_burst":
        assert row["burst_ready_tasks"] == row["logical_agents"]
print(f"validated {len(rows)} rows and {len(samples)} samples")
PY

shasum -a 256 -c "$evidence/performance-build-inputs.sha256" >/dev/null
actual=$(shasum -a 256 "$binary" | awk '{print $1}')
test "$expected" = "$actual"
mv "$partial" "$output"
shasum -a 256 "$output"
