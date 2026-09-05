#!/usr/bin/env python3
"""Derive review-only Home experiments from warmed benchmark JSONL files."""

import argparse
import hashlib
import json
import statistics
from pathlib import Path


def median(rows, key):
    values = [row[key] for _, row in rows if row.get(key) is not None]
    return int(statistics.median(values)) if values else None


def semantic_fingerprint(environment, config):
    environment = dict(environment)
    for key in ("kind", "source_sha256", "input_digest", "repetitions"):
        environment.pop(key, None)
    encoded = json.dumps(
        {"environment": environment, "config": config},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def load_groups(paths, expected_epoch):
    groups = []
    for path in paths:
        rows = [
            (line, json.loads(text))
            for line, text in enumerate(path.read_text().splitlines(), 1)
        ]
        current = None
        for line, row in rows:
            if row["kind"] == "environment":
                if row["input_digest"] != expected_epoch:
                    raise ValueError(f"{path}:{line}: unexpected input digest")
                current = {
                    "path": path,
                    "source_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "environment": row,
                    "environment_line": line,
                }
            elif row["kind"] == "config":
                if current is None:
                    raise ValueError(f"{path}:{line}: config before environment")
                current.update(
                    config=row,
                    config_line=line,
                    samples=[],
                    resources=[],
                    warmups=[],
                )
                groups.append(current)
            elif row["kind"] == "sample":
                current["samples"].append((line, row))
            elif row["kind"] == "resources":
                current["resources"].append((line, row))
            elif row["kind"] == "warmup":
                current["warmups"].append((line, row))
        for group in [item for item in groups if item["path"] == path]:
            if not (
                len(group["samples"])
                == len(group["resources"])
                == len(group["warmups"])
                == 3
            ):
                raise ValueError(f"{path}: each configuration needs three repetitions")
    return groups


def experiment_id(comparison_key, source_sha256, sample_key):
    identity = json.dumps(
        {
            "comparison_key": comparison_key,
            "source_sha256": source_sha256,
            "sample_key": sample_key,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(identity).hexdigest()[:16]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-epoch", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--collector-sha256", required=True)
    # Kept as an ignored compatibility argument for older invocation scripts.
    parser.add_argument("--comparison-home", type=Path)
    args = parser.parse_args()

    experiments = []
    for group in load_groups(args.input, args.source_epoch):
        environment = group["environment"]
        config = group["config"]
        samples = group["samples"]
        resources = group["resources"]
        warmups = group["warmups"]
        store = environment["store"]
        population = environment["logical_agents"]
        if environment["scenario"] != "warmed_subscription_scale":
            raise ValueError("only warmed_subscription_scale is supported")
        raw_fingerprint = semantic_fingerprint(environment, config)
        fingerprint = raw_fingerprint
        durability = "Ephemeral" if store == "memory" else "ProcessRestart_DELETE_FULL"
        common = {
            "workload": "warmed_subscription_scale",
            "store": store,
            "durability": durability,
            "payload_bytes": environment["payload_bytes"],
            "population": population,
            "producers": environment["producers"],
            "streams": environment["streams"],
            "subscribers": environment["subscribers"],
            "concurrency": 1,
            "page_records": warmups[0][1]["records_per_subscription"],
            "page_bytes": config["max_page_bytes"],
            "instrumentation": config["instrumentation"],
            "config_fingerprint": fingerprint,
        }
        line_key = {
            "aggregation": "median_per_metric; counts_are_medians_not_totals",
            "resources_lines": [line for line, _ in resources],
            "sample_lines": [line for line, _ in samples],
            "warmup_lines": [line for line, _ in warmups],
            "semantic_config_sha256": raw_fingerprint,
            "collector_sha256": args.collector_sha256,
        }
        phases = [
            (
                "timed idle",
                {**common, "measurement_phase": "timed_idle", "memory_scope": "settled_process_RSS", "cpu_scope": "process_user_plus_system"},
                {
                    "runtime_ns_median": median(samples, "elapsed_ns"),
                    "cpu_us_median": int(statistics.median(row["cpu_user_us"] + row["cpu_system_us"] for _, row in samples)),
                    "memory_bytes_median": median(samples, "settled_rss_bytes"),
                    "rust_live_bytes_median": None,
                    "rust_allocated_bytes_median": median(samples, "rust_allocated_bytes"),
                    "agents": population,
                    "accepted": median(samples, "runtime_accepted"),
                    "rejected": median(samples, "runtime_rejected"),
                    "failed": median(samples, "runtime_failed"),
                    "delivered": median(samples, "delivered"),
                },
            ),
            (
                "registration",
                {**common, "measurement_phase": "registration", "memory_scope": "post_registration_process_RSS", "cpu_scope": "not_measured"},
                {
                    "runtime_ns_median": median(resources, "subscription_registration_elapsed_ns"),
                    "cpu_us_median": None,
                    "memory_bytes_median": median(resources, "post_registration_rss_bytes"),
                    "rust_live_bytes_median": median(warmups, "pre_drain_rust_requested_live_bytes"),
                    "rust_allocated_bytes_median": median(resources, "registration_rust_allocated_bytes"),
                    "agents": population,
                    "accepted": None,
                    "rejected": None,
                    "failed": None,
                    "delivered": None,
                },
            ),
            (
                "sequential drain",
                {**common, "measurement_phase": "sequential_drain", "memory_scope": "post_drain_process_RSS", "cpu_scope": "not_measured"},
                {
                    "runtime_ns_median": median(warmups, "drain_elapsed_ns"),
                    "cpu_us_median": None,
                    "memory_bytes_median": median(warmups, "post_drain_rss_bytes"),
                    "rust_live_bytes_median": median(warmups, "post_drain_rust_requested_live_bytes"),
                    "rust_allocated_bytes_median": None,
                    "agents": population,
                    "accepted": None,
                    "rejected": None,
                    "failed": None,
                    "delivered": population * warmups[0][1]["records_per_subscription"],
                },
            ),
        ]
        for phase, comparison_key, metrics in phases:
            sample_key = {**line_key, "phase": comparison_key["measurement_phase"]}
            source_path = str(group["path"])
            source_sha256 = group["source_sha256"]
            experiments.append(
                {
                    "id": experiment_id(comparison_key, source_sha256, sample_key),
                    "label": f"warmed subscription scale · {store} · {population:,} · {phase}",
                    "status": "measured",
                    "comparison_key": comparison_key,
                    "sample_count": 3,
                    "metrics": metrics,
                    "provenance": {
                        "source_path": source_path,
                        "source_sha256": source_sha256,
                        "sample_key": json.dumps(sample_key, separators=(",", ":")),
                        "source_epoch": args.source_epoch,
                    },
                    "note": (
                        "Exact 256-record shared-stream fixture. Repetitions share a process. "
                        "Rust allocation metrics include harness/Tokio and exclude SQLite C. "
                        f"Measured binary SHA-256: {args.binary_sha256}."
                    ),
                }
            )
    args.output.write_text(json.dumps({"experiments": experiments}, indent=2) + "\n")
    print(f"wrote {len(experiments)} experiments to {args.output}")


if __name__ == "__main__":
    main()
