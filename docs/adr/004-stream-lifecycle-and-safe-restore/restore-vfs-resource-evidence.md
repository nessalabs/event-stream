# Restore file-I/O and deeper history evidence

The repeated restore run passed all 12 cases. This adds file-I/O attribution and
two history shapes to the earlier stream-count sweep. It does not qualify
snapshot recovery or establish concurrent-agent capacity.

| Fixture / instrumentation | Median restore time (ms) | Sampled peak RSS (MiB) | Sampled peak Rust live bytes |
| --- | ---: | ---: | ---: |
| Deep history · 100,000 records / 1 stream · VFS recorder | 665.57 | 7.20 | 87,691 |
| Deep history · 100,000 records / 1 stream · recorder-disabled control | 640.27 | 7.19 | 87,426 |
| Receipt-heavy · 10,000 names / 20,000 lifetimes · VFS recorder | 614.63 | 8.34 | 87,579 |
| Receipt-heavy · 10,000 names / 20,000 lifetimes · recorder-disabled control | 621.45 | 8.31 | 87,411 |

Each cell has three fresh-process repetitions. The deep fixture has one stream
with 100,000 records. The receipt-heavy fixture has 10,000 public names, 20,000
lifetimes, 10,000 records and 10,000 lifecycle receipts. Every payload is 128
bytes. Retired records are checked through bounded read-only SQL; current
lifetimes and retry receipts are also checked through the production adapter.

Both configurations retain the stage observer, 1 ms process sampler and Rust
allocation counters. The control disables only VFS recording. Timing includes
restore, publication and receipt return. Fixture construction and correctness
checks are outside timing. The ranges overlap; these data do not establish a
precise recorder overhead or an optimization speedup.

## Where SQLite did I/O

The counts below were identical in all three recorder-enabled repetitions.

| Shape | Source read callbacks | Target write callbacks | Journal write callbacks | Temporary logical-file peak |
| --- | ---: | ---: | ---: | ---: |
| Deep | 20,468 | 11,378 | 15,044 | 0 B |
| Receipt-heavy | 6,301 | 5,764 | 10,147 | 1,320,006 B |

The receipt-heavy sort opened one temporary file. It requested 1,320,006 bytes
of writes and the same number of read bytes. Its live logical size returned to
zero. Deep history opened no observed temporary files. This does not measure
SQLite's in-memory sort buffers.

VFS counters observe SQLite file callbacks. Byte counts are requested bytes,
including unsuccessful calls; they are not physical-device traffic or APFS
allocated space. Source hashing through Rust file reads is outside this VFS
measurement. Memory-mapped fetches have a separate counter. Every non-OK callback
in these runs was SQLite's expected short-read result; no other callback error
was observed. All final staging, journal and ownership artifacts were absent.

The recorder preserves VFS version 3 and each file's optional callbacks. The
first attempt incorrectly grouped optional callbacks. SQLite's no-lock methods
have a null shared-memory map callback while retaining other shared-memory
callbacks. Its 12 raw samples remain in the failed `.partial` artifact. They
were not reused in this report or Home. The corrected per-file method table
preserves each nullable callback independently.

## Provenance and practical limits

- [Passing raw JSONL](evidence/restore-vfs-resource-3d1b73ce5186ed2d.jsonl)
- [Pre-run provenance](evidence/restore-vfs-resource-provenance-3d1b73ce5186ed2d.json)
- [Exact source manifest](evidence/restore-vfs-resource-source-3d1b73ce5186ed2db406026e03bb25d4dcdd0066060cb2e68d6ae1d6dabeba49.manifest)
- [Archived inputs](evidence/restore-vfs-resource-source-3d1b73ce5186ed2db406026e03bb25d4dcdd0066060cb2e68d6ae1d6dabeba49.tar)
- [Collector](run-restore-vfs-resource.sh)
- [Failed recorder qualification](evidence/restore-vfs-resource-9fd701dfabc3f35e.jsonl.partial)

Raw SHA-256: `6c4d5a09c61524709f3850affb250ed0beefa59e7b583545b28b231278826ea6`.

The executed source is the archived restore checkpoint plus the recorder fix.
It was built in an isolated target directory while snapshot source development
continued separately. The root review checked all 24 archived inputs, archive,
manifest and executable hashes, the execution schedule, row counts, source
identity, bounded mapping pages, commit counts and final cleanup counters.

The host is Apple M5, 24 GiB RAM, macOS 26.6 and APFS with 4 KiB blocks. Host
configuration was captured earlier on the same host; load and power state during
these runs were not recorded. RSS is sampled current process memory during
restore, not the process-lifetime maximum also present in raw rows. Rust live
memory is sampled, not a settled value, and excludes SQLite's C allocator.

The runs use one restore operation, a 64 KiB copy buffer, 256-record / 2 MiB
pages and 512 MiB source and staging-page limits. Mapping and record-import
turns remain bounded. Full source validation and hashing still inspect the
artifact. This report does not claim constant-time restore or constant total
storage as history grows. ADR 0004's combined correctness, compatibility and
failure gates remain separate from this measurement.
