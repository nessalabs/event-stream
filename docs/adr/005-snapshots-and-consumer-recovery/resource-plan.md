# Snapshot resource qualification plan

Status: measurement contract. No result is recorded here.

The benchmark measures snapshot work separately from application projection
meaning. Its bytes are a deterministic payload fixture, not a claim that real
application state has the same shape.

Build one immutable-source executable, then pass that executable and its source
archive to the collector:

```sh
CARGO_TARGET_DIR=/tmp/event-stream-snapshot-target \
  cargo build --locked --release --features sqlite,snapshots,test-support \
  --example snapshot_resource
python3 examples/run_snapshot_resource.py \
  --binary /tmp/event-stream-snapshot-target/release/examples/snapshot_resource \
  --source-archive /path/to/snapshot-resource-source.tar \
  --output /path/to/snapshot-resource.jsonl
```

The collector creates `OUTPUT.partial` exclusively and flushes every child row.
It hard-links that file to `OUTPUT` only after all correctness and latency gates
pass, so it cannot overwrite an earlier result. Failed or interrupted work
keeps the partial evidence.

## Fixed matrix

One release executable runs in a fresh process for each cell. The collector
kills and waits for a child that exceeds 900 seconds and records that cell as a
failure. UI rendering and JSON aggregation stay outside every timed child.

| Dimension | Fixed values |
| --- | --- |
| Adapter | MemoryStore, SQLite |
| Snapshot content | 1 MiB, 16 MiB, 64 MiB |
| Stored chunk | 64 KiB |
| Event history | 128 records through the snapshot cursor, then 128 suffix records |
| Event payload | 128 deterministic bytes |
| Content read | at most 64 KiB per call |
| History page | at most 256 records and 2 MiB |
| Repetitions | 3 instrumented plus 1 instrumentation-control process per adapter and size |

The schedule interleaves adapters, sizes and repetitions. It records the exact
order. One source manifest, binary hash, compiler identity, host identity and
full configuration fingerprint accompany the raw rows. A smoke run cannot pass
the resource gate.

The fixture generates each content chunk twice: once to compute the expected
SHA-256 digest and once to upload it. It never builds the whole snapshot in one
buffer. Recovery compares every returned byte with the same generator and
checks the complete digest. It also checks descriptor identity and schema,
strict suffix offsets 129 through 256, application state equal to a complete
replay, exact begin/chunk/publication retries, an empty staging list after
publication and successful lease release.

## Foreground append comparison

This is a separate fixture. Four producers each submit 64 uniquely identified
128-byte events. Each producer waits for its receipt before submitting its next
event. All four start from one barrier with either bounded verification or
bounded recovery reads. The matched control performs the same 256 appends and
barrier work without snapshot I/O. There are three fresh-process repetitions
for each adapter, snapshot phase and control.

This is a closed-loop concurrency test. It does not model an independent
arrival rate and cannot qualify overload behavior by itself.

Every run must report exactly 256 offered, 256 accepted and 256 committed
events, zero rejected or failed events, 256 distinct IDs and one contiguous
cursor range. Latencies are caller-observed distributions. The initial release
gate is:

- append p99 no more than two times its matched control plus 5 ms;
- append maximum no more than three times its matched control plus 20 ms;
- snapshot work completes before the 900-second process watchdog.

A failed limit stays visible. It is not repaired by reducing the offered count,
changing SQLite synchronization, or comparing a different fixture. These
limits are provisional release budgets and may change only in a later numbered
evidence round with an explicit reason.

## Timed phases and resource fields

Record begin, upload, bounded verification, publication, lease acquisition,
content reads, suffix reads, release and shutdown separately. CPU windows use
process user plus system time. RSS fields include start, each phase boundary,
post-operation and lifetime peak. A post-operation RSS observation does not
prove allocator release.

Instrumented rows record Rust allocation/deallocation traffic and live/peak
Rust bytes for the whole process. Those counters exclude SQLite's C allocator.
SQLite test-support rows record VFS callback read/write byte counts, sync counts
and callback elapsed time, plus actual database, WAL, journal and SQLite temp
file sizes. VFS callbacks are not kernel syscall or physical-device traces.
MemoryStore rows leave disk fields null. File length is never presented as I/O.
The recorder is not reset while SQLite files are live. Each phase records
cumulative counters before and after and reports their checked difference.
Temporary-file peak is run-wide unless a phase-local sampler directly observes
it; cumulative peaks are never subtracted and relabeled as phase peaks.

The single observer-control process disables the RSS sampler and VFS recorder.
The process-wide Rust allocation counters remain enabled because switching
them on after earlier allocations would make live-byte accounting invalid. The
control diagnoses the sampler and VFS overhead; it does not measure allocation
counter overhead and does not replace the three correctness and latency
repetitions.

## Gates that do not depend on speed

All configured count and byte limits are finite and serialized with each row.
No request may exceed its configured admission, chunk, page, stored-content or
metadata quota. At the end of a successful cell, no upload remains in the
staging list, no recovery lease remains usable after release, and another
verification can acquire the bounded verifier slot. SQLite must reopen and
repeat the descriptor, content and suffix checks. Memory-only evidence never
counts as restart or durability evidence.

Working-memory analysis separates retained snapshot storage from transient
work. For MemoryStore, retained content is expected to grow with published
bytes. The reported transient estimate uses phase peak minus the greater of the
phase's start and settled retained-storage observations. It remains an
observation, not an allocator-capacity proof. The first matrix records this
value and leaves a numeric memory-release limit open until repeat variance is
known. That first run is diagnostic for the memory-release gate; it must not
invent a precise RSS guarantee from logical byte charges.
