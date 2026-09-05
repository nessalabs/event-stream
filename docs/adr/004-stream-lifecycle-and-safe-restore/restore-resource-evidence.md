# SQLite restore resource evidence

This report measures one bounded restore workload. It does not establish a
universal restore budget. The result covers a closed SQLite format-2 source,
one 128-byte record per active stream, and the limits below.

## Result

The complete 1,000, 10,000, and 100,000-stream matrix succeeded. Every sample
verified the source identity, the complete incarnation mapping, exact replay,
an exact receipt retry, and removal of the staging, journal, owner, and request
paths. Mapping pages and record transactions stayed bounded at 256 records.

| Streams | Instrumented elapsed median (range) | Control elapsed | CPU median (range) | Sampled peak RSS | Rust allocation traffic | Whole-process disk writes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 81.9 ms (78.6–84.1) | 65.5 ms | 36.1 ms (33.6–46.0) | 5.44 MiB | 0.85 MiB | 3.01 MiB |
| 10,000 | 320.2 ms (299.5–373.2) | 367.4 ms | 265.2 ms (263.9–295.2) | 7.98 MiB | 5.84 MiB | 34.13 MiB |
| 100,000 | 3.328 s (3.299–3.370) | 3.730 s | 3.158 s (3.122–3.187) | 8.73 MiB | 56.74 MiB | 394.69 MiB |

CPU is user plus system CPU for the process during the restore call. Rust
allocation traffic is cumulative requested bytes, not live memory. The 1 ms
sampled peak live Rust allocation was 84.4–85.4 KiB across the three sizes. It
is a sampled peak rather than a settled value, and it excludes SQLite's C
allocator and operating-system caches. The allocator and sampler were enabled
in both instrumented and control processes.

The table uses the highest current RSS observed by the 1 ms sampler during the
restore call. It includes Tokio, SQLite, and the harness, and the sampling
interval can miss a shorter peak. The raw `max_rss_bytes` field is different:
it is the operating system's process-lifetime high-water mark and includes
fixture creation. Each sample used a fresh process, so the three instrumented
repetitions do not inherit allocator state from one another.

The single control disables only the stage observer. It retains the sampler and
allocator counters. At 10,000 and 100,000 streams the one control was slower
than every instrumented repetition. At 1,000 it was faster. One control per
size cannot distinguish observer cost from run-order and host noise, so these
numbers do not support a precise observer-overhead estimate.

## Bounded work

The source and published sizes grew with the record count. The largest source
was 40.31 MiB and the published database was 52.20 MiB. The 100,000-stream
instrumented median wrote 394.69 MiB according to the process counter. That
counter includes all writes by the process and does not report physical-device
traffic or attribute bytes to a particular SQLite file.

| Streams | Mapping pages | Record commits | Max mapping logical bytes | Mapping vector capacity | Peak journal bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 4 | 4 | 39,424 | 256 | 58,920 median |
| 10,000 | 40 | 40 | 39,424 | 256 | 71,232 median |
| 100,000 | 391 | 391 | 39,424 | 256 | 95,824 median |

The page counts equal `ceil(streams / 256)`. The mapping vector never exceeded
256 entries. The observer records a record-page event only after its SQLite
transaction commits, so these counts also prove cross-stream batching rather
than one durable transaction per stream.

For the 100,000-stream instrumented samples, the median stage durations were:

| Stage | Median duration |
| --- | ---: |
| Source hash and validation | 131 ms |
| Owner reservation | 10.9 ms |
| Staging creation | 5.0 ms |
| Identity mapping import | 467 ms |
| Lifetime import | 881 ms |
| Record import | 1.252 s |
| Name import | 545 ms |
| Relational validation | 62 ms |
| Completion and staging sync | 7.0 ms |
| Publication | 16 ms |

These are differences between fixed cumulative observer boundaries. They are
not separate profiler spans and can include small observer and scheduling
costs. The source fixture was created before the timed restore call. JSON
serialization and UI rendering were outside the timed region.

The workload used one restore operation, a 512 MiB source limit, a 512 MiB
staging-page limit, a 64 KiB copy buffer, and mapping pages limited to 256
entries or 2 MiB. Each process had a 15-minute watchdog and two Tokio worker
threads. All processes completed in under four seconds of timed restore work.

## Provenance

The raw artifact is
[restore-resource-bba90501e0df45be.jsonl](evidence/restore-resource-bba90501e0df45be.jsonl).
It contains one environment row, 12 sample rows in execution order, and one
completion row. Its SHA-256 is
`c005e658ff6e3d44e1136ab064618d1e6c1e6fce761d3f7a71851b5480ea7e31`.

The source manifest is
[restore-resource-source-bba90501e0df45be4601f5666c25430fe8f626817a92867ebb77bd8807d9d330.manifest](evidence/restore-resource-source-bba90501e0df45be4601f5666c25430fe8f626817a92867ebb77bd8807d9d330.manifest).
It lists 38 exact inputs and has SHA-256
`bba90501e0df45be4601f5666c25430fe8f626817a92867ebb77bd8807d9d330`.
The matching source archive is
[restore-resource-source-bba90501e0df45be4601f5666c25430fe8f626817a92867ebb77bd8807d9d330.tar.gz](evidence/restore-resource-source-bba90501e0df45be4601f5666c25430fe8f626817a92867ebb77bd8807d9d330.tar.gz),
with SHA-256
`bae1cc801951656d73601b32624931ee03b2abcfdc4550e4ed63f3c37cb9f70a`.
The executed binary SHA-256 was
`54c7fee887437c6d2f9db6b3d320eb58c3fd904f6e191e5f39298be9847b7fcb`.

The artifact records Rust `1.98.1`, Darwin, and arm64. A separate
[host provenance record](evidence/restore-resource-host-after-run.json) was
captured on the same host after the run. It records an Apple M5, 24 GiB of
memory, 10 physical and logical CPUs, macOS 26.6 build 25G72, and the APFS
filesystem used by the temporary directory. Its SHA-256 is
`cda1734237b4e54b864c64341ae4e2e68654e247784fbe1f5cd39906109c81b2`.
Because it was captured afterward, it provides host context rather than a
cryptographic link to each sample. Host load and power state were not captured.
The executed binary itself is not archived. The source archive and manifest
make the implementation inputs reviewable, while the binary hash binds the raw
rows to the executable that ran.

## Limits of this evidence

The first matrix had no test VFS observer. A later paired matrix closes that
specific attribution gap below. Process read/write counters remain
whole-process deltas. Directory allocation reports zero bytes on this APFS host even though
the owner and request directories were observed, so directory counts and the
logical owner charge remain the useful reservation measures.

The first matrix has one record per stream. The later matrix covers deep and
receipt-heavy shapes. It does not cover larger payloads or a source near the
512 MiB limit. It does not test power loss. The real APFS `ENOSPC`, process
kill, cleanup-failure, and source-lock evidence is recorded separately in
[sqlite-restore-evidence.md](sqlite-restore-evidence.md).

Restore remains open for a near-limit artifact and power-loss qualification.
Injected failures and abrupt process termination provide process-restart
evidence, not power-loss evidence.

## VFS attribution for deep and receipt-heavy inputs

A second matrix used a test-support forwarding VFS. It ran in fresh processes
and compared three recorder-enabled samples with three controls for each shape.
The control retained the Rust allocator sampler and fixed stage observer. It
disabled only the VFS recorder.

| Shape | VFS median (range) | Control median (range) | Observed SQLite temp peak |
| --- | ---: | ---: | ---: |
| One stream, 100,000 records | 665.6 ms (642.6–733.3) | 640.3 ms (612.2–721.6) | 0 bytes |
| 10,000 receipts, 20,000 lifetime identities | 614.6 ms (613.4–779.8) | 621.4 ms (602.4–808.0) | 1,320,006 bytes |

The ranges overlap. These six paired groups do not show a stable timing cost
for the recorder. Timings include the stage observer in both modes. They are
regression evidence for this host and build, not a universal latency promise.

Callback counts repeated exactly across all three instrumented samples. The
deep shape made 20,468 source reads, 11,378 target-main writes, and 15,044
target-journal writes. It committed 391 record pages. The receipt-heavy shape
made 6,301 source reads, 5,764 target-main writes, and 10,147 target-journal
writes. It committed 40 record pages and read 79 mapping pages during the
outside-timing correctness check.

The receipt-heavy identity union opened one SQLite temporary file. Its exact
logical high-water size was 1,320,006 bytes in every repetition. The deep shape
opened no VFS temporary file. A zero VFS temporary-file count does not measure
SQLite's in-memory temporary work. The logical high-water counter follows
`xWrite`, `xTruncate`, and `xClose`; it is not filesystem-allocated space or
physical I/O.

The recorder counted requested `xRead` and `xWrite` bytes, including callbacks
that returned non-OK. It reports `SQLITE_IOERR_SHORT_READ` separately because
SQLite uses short reads normally for new and empty files. No other non-OK
callback occurred. Source hashing uses `std::File`, so its reads are outside the
VFS counters. `xFetch` has a separate count. Deletes without `xOpen` flags can
remain in the `other` category.

The recorder preserved VFS interface version 3 and each per-file nullable SHM
and fetch callback. Every sample reported zero optional-method mismatches. The
first attempted qualification used grouped optional-method tables and is kept
as a `.partial` artifact; it completed the work but failed its interface parity
gate. Its measurements are excluded here.

The qualified raw artifact is
[restore-vfs-resource-3d1b73ce5186ed2d.jsonl](evidence/restore-vfs-resource-3d1b73ce5186ed2d.jsonl),
SHA-256 `6c4d5a09c61524709f3850affb250ed0beefa59e7b583545b28b231278826ea6`.
Its [manifest](evidence/restore-vfs-resource-source-3d1b73ce5186ed2db406026e03bb25d4dcdd0066060cb2e68d6ae1d6dabeba49.manifest)
has SHA-256 `3d1b73ce5186ed2db406026e03bb25d4dcdd0066060cb2e68d6ae1d6dabeba49`.
The matching [source archive](evidence/restore-vfs-resource-source-3d1b73ce5186ed2db406026e03bb25d4dcdd0066060cb2e68d6ae1d6dabeba49.tar)
has SHA-256 `f3c14198b05853c464b67093e73d7b842af3af38b5fccbf85b197255e602eaf4`.
The executed binary SHA-256 was
`1e43465d6e8cada7e177f9664b03e19743d585cb26833ccbff40acd088cd99c4`.
The [pre-run provenance](evidence/restore-vfs-resource-provenance-3d1b73ce5186ed2d.json)
has SHA-256 `ce1d2129f457063624023c1872d410c86e19621de202a65dbb04e67268751297`.
This epoch is the earlier archived restore implementation plus only the
corrected test VFS recorder; concurrent ADR 0005 work in the live workspace was
not part of the executable.
