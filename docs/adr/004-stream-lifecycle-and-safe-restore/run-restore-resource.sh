#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "$0")/../../.." && pwd)"
output="${1:-$workspace/docs/adr/004-stream-lifecycle-and-safe-restore/evidence/restore-resource.jsonl}"
binary="$workspace/target/release/examples/restore_resource"
source_manifest_sha256="${RESTORE_SOURCE_MANIFEST_SHA256:-unrecorded}"

if [[ -e "$output" ]]; then
  echo "refusing to overwrite $output" >&2
  exit 2
fi
mkdir -p "$(dirname "$output")"

cd "$workspace"
cargo build --locked --release --features sqlite,test-support --example restore_resource
if command -v sha256sum >/dev/null 2>&1; then
  binary_hash_line="$(sha256sum "$binary")"
else
  binary_hash_line="$(shasum -a 256 "$binary")"
fi
binary_sha256="${binary_hash_line%% *}"
started_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf '{"kind":"restore_resource_environment","started_utc":"%s","binary_sha256":"%s","source_manifest_sha256":"%s","rustc":"%s","os":"%s","arch":"%s","schedule":"I1000,I10000,I100000,C1000,I10000,I100000,I1000,C10000,I100000,I1000,I10000,C100000","control_scope":"stage_observer_disabled_sampler_and_allocator_counters_retained","fresh_process_per_sample":true}\n' \
  "$started_utc" "$binary_sha256" "$source_manifest_sha256" "$(rustc --version)" "$(uname -s)" "$(uname -m)" > "$output"

schedule=(
  "1000 true" "10000 true" "100000 true" "1000 false"
  "10000 true" "100000 true" "1000 true" "10000 false"
  "100000 true" "1000 true" "10000 true" "100000 false"
)

execution_index=0
for cell in "${schedule[@]}"; do
  execution_index=$((execution_index + 1))
  read -r streams stage_observer <<< "$cell"
  python3 - "$binary" "$streams" "$stage_observer" "$output" "$execution_index" <<'PY'
import json
import subprocess
import sys

binary, streams, stage_observer, output, execution_index = sys.argv[1:]
try:
    completed = subprocess.run(
        [binary, "--streams", streams, "--stage-observer", stage_observer],
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        timeout=900,
        check=True,
    )
except subprocess.TimeoutExpired:
    with open(output, "a", encoding="utf-8") as evidence:
        evidence.write(
            '{"kind":"restore_resource_failure","execution_index":%s,"streams":%s,'
            '"stage_observer_enabled":%s,"failure":"process_watchdog_elapsed"}\n'
            % (execution_index, streams, stage_observer)
        )
    sys.exit(0)
except subprocess.CalledProcessError as error:
    with open(output, "a", encoding="utf-8") as evidence:
        evidence.write(
            '{"kind":"restore_resource_failure","execution_index":%s,"streams":%s,'
            '"stage_observer_enabled":%s,"failure":"process_exit_%s"}\n'
            % (execution_index, streams, stage_observer, error.returncode)
        )
    sys.exit(0)
lines = [line for line in completed.stdout.splitlines() if line]
if len(lines) != 1:
    raise RuntimeError(f"expected one JSON result, received {len(lines)} lines")
with open(output, "a", encoding="utf-8") as evidence:
    sample = json.loads(lines[0])
    sample["execution_index"] = int(execution_index)
    evidence.write(json.dumps(sample, separators=(",", ":"), sort_keys=True) + "\n")
PY
done

printf '{"kind":"restore_resource_completion","completed_utc":"%s","attempts":12}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$output"
echo "$output"
