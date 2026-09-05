#!/usr/bin/env python3
"""Run and validate the fixed ADR 0005 resource matrix."""

import argparse, datetime, hashlib, json, os, platform, subprocess, sys
from pathlib import Path

SIZES = [1 << 20, 16 << 20, 64 << 20]
STORES = ["memory", "sqlite"]
FOREGROUND_MODES = ["foreground_verify_control", "foreground_verify",
                    "foreground_recovery_control", "foreground_recovery"]

def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()

def bounded_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    return value[-65536:]

def run_cell(binary, arguments, schedule_index, repetition, timeout):
    started = utc_now()
    command = [str(binary), *arguments]
    try:
        completed = subprocess.run(command, capture_output=True, text=True,
                                   timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        return {"kind": "snapshot_resource_failure", "schedule_index": schedule_index,
                "repetition": repetition, "started_utc": started, "ended_utc": utc_now(),
                "command": command, "failure": "whole_process_watchdog",
                "timeout_seconds": timeout, "stdout": bounded_text(error.stdout),
                "stderr": bounded_text(error.stderr)}
    if completed.returncode != 0:
        return {"kind": "snapshot_resource_failure", "schedule_index": schedule_index,
                "repetition": repetition, "started_utc": started, "ended_utc": utc_now(),
                "command": command, "failure": "child_exit", "exit_code": completed.returncode,
                "stdout": bounded_text(completed.stdout), "stderr": bounded_text(completed.stderr)}
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise RuntimeError(f"child emitted {len(lines)} nonempty stdout lines")
    row = json.loads(lines[0])
    expected = dict(zip(arguments[::2], arguments[1::2]))
    expected_kind = "snapshot_resource_sample" if expected["--mode"] == "full" else "snapshot_foreground_sample"
    if (row.get("kind") != expected_kind or row.get("store") != expected["--store"]
            or row.get("snapshot_bytes") != int(expected["--snapshot-bytes"])
            or row.get("instrumented") != (expected["--instrumented"] == "true")
            or (expected_kind == "snapshot_foreground_sample" and row.get("mode") != expected["--mode"])):
        raise RuntimeError("child row does not match its scheduled configuration")
    if expected_kind == "snapshot_foreground_sample" and (
            row.get("offered") != 256 or row.get("accepted") != 256
            or row.get("rejected") != 0 or row.get("failed") != 0
            or row.get("committed") != 256 or len(row.get("append_latencies_ns", [])) != 256
            or len(row.get("append_intervals", [])) != 256):
        raise RuntimeError("foreground row failed exact count conservation")
    if expected_kind == "snapshot_foreground_sample":
        intervals = row["append_intervals"]
        identities = {(entry.get("producer"), entry.get("sequence")) for entry in intervals}
        durations = sorted(entry.get("end_ns", -1) - entry.get("begin_ns", 0)
                           for entry in intervals)
        if (identities != {(producer, sequence) for producer in range(4) for sequence in range(64)}
                or any(entry.get("end_ns", -1) < entry.get("begin_ns", 0)
                       for entry in intervals)
                or durations != row["append_latencies_ns"]
                or row.get("snapshot_end_ns", -1) < row.get("snapshot_start_ns", 0)):
            raise RuntimeError("foreground row failed bounded timeline validation")
    if expected_kind == "snapshot_resource_sample" and (
            row.get("content_reads") != (int(expected["--snapshot-bytes"]) + 65535) // 65536
            or row.get("suffix_records") != 128
            or row.get("correctness") != "exact_descriptor_content_suffix_replay_retry_release_and_empty_staging"):
        raise RuntimeError("full snapshot row failed exact correctness checks")
    row.update(schedule_index=schedule_index, repetition=repetition,
               started_utc=started, ended_utc=utc_now(), command=command)
    return row

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--source-archive", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=900)
    args = parser.parse_args()
    binary, source_archive = args.binary.resolve(), args.source_archive.resolve()
    if not binary.is_file() or not source_archive.is_file():
        raise SystemExit("binary and source archive must both exist")
    if args.timeout_seconds <= 0 or args.timeout_seconds > 900:
        raise SystemExit("timeout must be in 1..=900 seconds")
    schedule = []
    for repetition in range(4):
        rotated = SIZES[repetition % 3:] + SIZES[:repetition % 3]
        for size in rotated:
            stores = STORES if repetition % 2 == 0 else list(reversed(STORES))
            for store in stores:
                schedule.append((repetition, ["--store", store, "--snapshot-bytes", str(size),
                    "--mode", "full", "--instrumented", "true" if repetition < 3 else "false"]))
    for repetition in range(3):
        for mode in FOREGROUND_MODES:
            stores = STORES if repetition % 2 == 0 else list(reversed(STORES))
            for store in stores:
                schedule.append((repetition, ["--store", store, "--snapshot-bytes", str(1 << 20),
                                              "--mode", mode, "--instrumented", "true"]))
    before_hash, source_hash, started = sha256(binary), sha256(source_archive), utc_now()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    partial = args.output.with_name(args.output.name + ".partial")
    rows = []
    with partial.open("x", encoding="utf-8") as output:
        for index, (repetition, command) in enumerate(schedule, 1):
            try:
                row = run_cell(binary, command, index, repetition, args.timeout_seconds)
            except Exception as error:
                row = {"kind": "snapshot_resource_failure", "schedule_index": index,
                       "repetition": repetition, "command": [str(binary), *command],
                       "started_utc": utc_now(), "ended_utc": utc_now(),
                       "failure": "collector_validation", "detail": str(error)}
            rows.append(row)
            output.write(json.dumps(row, sort_keys=True) + "\n")
            output.flush()
    failures = sum(row["kind"] == "snapshot_resource_failure" for row in rows)
    controls = {(row["store"], row["repetition"], row["mode"].removesuffix("_control")): row
                for row in rows if row.get("kind") == "snapshot_foreground_sample"
                and row["mode"].endswith("_control")}
    gate_failures = 0
    for row in rows:
        if row.get("kind") != "snapshot_foreground_sample" or row["mode"].endswith("_control"):
            continue
        control = controls.get((row["store"], row["repetition"], row["mode"]))
        if control is None:
            gate_failures += 1
            row["latency_gate"] = {"pass": False, "reason": "matched control unavailable"}
            continue
        p99_ok = row["append_p99_ns"] <= 2 * control["append_p99_ns"] + 5_000_000
        max_ok = row["append_max_ns"] <= 3 * control["append_max_ns"] + 20_000_000
        overlap_ok = row["overlapping_append_receipts"] > 0
        row["latency_gate"] = {"matched_control_schedule_index": control["schedule_index"],
            "p99_pass": p99_ok, "max_pass": max_ok, "overlap_observed": overlap_ok,
            "pass": p99_ok and max_ok and overlap_ok}
        gate_failures += not row["latency_gate"]["pass"]
    after_hash = sha256(binary)
    binary_changed = after_hash != before_hash
    failures += binary_changed
    with partial.open("a", encoding="utf-8") as output:
        for row in rows:
            if "latency_gate" in row:
                output.write(json.dumps({"kind": "snapshot_foreground_gate",
                    "schedule_index": row["schedule_index"], **row["latency_gate"]}, sort_keys=True) + "\n")
        output.write(json.dumps({"kind": "snapshot_resource_completion", "started_utc": started,
            "ended_utc": utc_now(), "rows": len(rows), "failures": failures,
            "latency_gate_failures": gate_failures, "binary_sha256_before": before_hash,
            "binary_sha256_after": after_hash, "binary_path": str(binary),
            "source_archive_path": str(source_archive), "source_archive_sha256": source_hash,
            "rustc_vv": subprocess.run(["rustc", "-Vv"], capture_output=True, text=True, check=True).stdout,
            "host": platform.platform(), "watchdog_seconds": args.timeout_seconds,
            "schedule": "24_full_interleaved_then_24_foreground_interleaved"}, sort_keys=True) + "\n")
        output.flush()
    if not failures and not gate_failures:
        os.link(partial, args.output)
        partial.unlink()
    return 1 if failures or gate_failures else 0

if __name__ == "__main__":
    sys.exit(main())
