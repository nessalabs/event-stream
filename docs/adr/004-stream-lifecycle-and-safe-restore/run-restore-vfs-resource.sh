#!/usr/bin/env bash
set -euo pipefail

workspace="$(cd "$(dirname "$0")/../../.." && pwd)"
output="${1:-$workspace/docs/adr/004-stream-lifecycle-and-safe-restore/evidence/restore-vfs-resource.jsonl}"
partial="$output.partial"
binary="${RESTORE_RESOURCE_BINARY:-$workspace/target/release/examples/restore_resource}"
source_manifest_sha256="${RESTORE_SOURCE_MANIFEST_SHA256:?set RESTORE_SOURCE_MANIFEST_SHA256}"
expected_binary_sha256="${RESTORE_BINARY_SHA256:?set RESTORE_BINARY_SHA256}"

if [[ -e "$output" || -e "$partial" ]]; then
  echo "refusing to overwrite $output or $partial" >&2
  exit 2
fi
if [[ ! -x "$binary" ]]; then
  echo "missing executable $binary" >&2
  exit 2
fi
mkdir -p "$(dirname "$output")"

if command -v sha256sum >/dev/null 2>&1; then
  binary_hash_line="$(sha256sum "$binary")"
else
  binary_hash_line="$(shasum -a 256 "$binary")"
fi
binary_sha256="${binary_hash_line%% *}"
if [[ "$binary_sha256" != "$expected_binary_sha256" ]]; then
  echo "binary hash mismatch" >&2
  exit 2
fi

printf '{"kind":"restore_vfs_resource_environment","started_utc":"%s","binary_sha256":"%s","source_manifest_sha256":"%s","rustc":"%s","os":"%s","arch":"%s","schedule":"deep-I,receipt-I,deep-C,receipt-C repeated three times","instrumented_scope":"stage observer plus VFS recorder","control_scope":"stage observer retained and VFS recorder disabled","fresh_process_per_sample":true}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$binary_sha256" "$source_manifest_sha256" "$(rustc --version)" "$(uname -s)" "$(uname -m)" > "$partial"

schedule=(
  "deep 100000 true" "receipt-heavy 10000 true"
  "deep 100000 false" "receipt-heavy 10000 false"
  "receipt-heavy 10000 true" "deep 100000 true"
  "receipt-heavy 10000 false" "deep 100000 false"
  "deep 100000 true" "receipt-heavy 10000 true"
  "deep 100000 false" "receipt-heavy 10000 false"
)

execution_index=0
for cell in "${schedule[@]}"; do
  execution_index=$((execution_index + 1))
  read -r shape population recorder <<< "$cell"
  python3 - "$binary" "$shape" "$population" "$recorder" "$partial" "$execution_index" <<'PY'
import json
import subprocess
import sys

binary, shape, population, recorder, output, execution_index = sys.argv[1:]
completed = subprocess.run(
    [binary, "--shape", shape, "--population", population,
     "--stage-observer", "true", "--vfs-recorder", recorder],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    timeout=900,
)
if completed.returncode != 0:
    with open(output, "a", encoding="utf-8") as evidence:
        evidence.write(json.dumps({
            "kind": "restore_vfs_resource_failure",
            "execution_index": int(execution_index),
            "fixture_shape": shape.replace("-", "_"),
            "population": int(population),
            "vfs_recorder_enabled": recorder == "true",
            "failure": f"process_exit_{completed.returncode}",
            "stderr": completed.stderr[-4096:],
        }, separators=(",", ":"), sort_keys=True) + "\n")
    raise SystemExit(1)
lines = [line for line in completed.stdout.splitlines() if line]
if len(lines) != 1:
    raise RuntimeError(f"expected one JSON result, received {len(lines)} lines")
sample = json.loads(lines[0])
sample["execution_index"] = int(execution_index)
with open(output, "a", encoding="utf-8") as evidence:
    evidence.write(json.dumps(sample, separators=(",", ":"), sort_keys=True) + "\n")
PY
done

python3 - "$partial" <<'PY'
import collections
import json
import sys

path = sys.argv[1]
rows = [json.loads(line) for line in open(path, encoding="utf-8")]
samples = [row for row in rows if row.get("kind") == "restore_resource_sample"]
if len(samples) != 12:
    raise SystemExit(f"expected 12 samples, observed {len(samples)}")
coverage = collections.Counter(
    (row["fixture_shape"], row["population"], row["vfs_recorder_enabled"])
    for row in samples
)
expected = {
    ("deep", 100000, True): 3,
    ("deep", 100000, False): 3,
    ("receipt_heavy", 10000, True): 3,
    ("receipt_heavy", 10000, False): 3,
}
if coverage != expected:
    raise SystemExit(f"coverage mismatch: {coverage}")
for row in samples:
    if row["correctness"] != "exact_mapping_replay_receipts_retry_source_identity":
        raise SystemExit("correctness marker mismatch")
    if row["vfs_recorder_enabled"]:
        vfs = row["sqlite_vfs"]
        if not vfs["available"] or vfs["temporary_live_logical_bytes"] != 0:
            raise SystemExit("VFS recorder availability or live-temp mismatch")
        if vfs["underlying_vfs_interface_version"] != vfs["recorder_vfs_interface_version"]:
            raise SystemExit("VFS interface version changed")
        if vfs["optional_method_mismatches"] != 0:
            raise SystemExit("per-file optional method surface changed")
PY

printf '{"kind":"restore_vfs_resource_completion","completed_utc":"%s","attempts":12}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$partial"
mv "$partial" "$output"
echo "$output"
