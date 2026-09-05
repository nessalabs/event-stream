# Borrowed SQLite row decoding

The small change removes allocations that hold data only long enough to copy it again. `decode_row` now borrows the guarded SQLite result values with `ValueRef`. It validates the borrowed types and UTF-8, then makes the one exact allocation required by each domain value.

Before the change, every record allocated an 8-byte offset `Vec`, two temporary `String` values, and a temporary payload `Vec`. The identifier constructors then copied both strings into `Box<str>`, and `Payload::copy_from_slice` copied the payload into `Arc<[u8]>`. The new path still allocates the final identifiers, payload, stream key, and `Arc<Record>`.

Both complete source snapshots build independently. Their manifests differ only at `src/infrastructure/sqlite.rs`, and that patch is limited to `decode_row`. Three repetitions were interleaved to reduce time-order bias. Each fresh process replayed one 256-record page with 128-byte payloads for 100,000 consumers. Every case verified 25.6 million offsets and payloads.

| Path | Metric | Before median | After median | Change |
|---|---|---:|---:|---:|
| `SqliteStore` | elapsed | 24.140 s | 23.457 s | -2.8% |
| `SqliteStore` | process CPU | 24.077 s | 23.303 s | -3.2% |
| `SqliteStore` | Rust allocation calls | 232,000,088 | 129,600,088 | -44.1% |
| `SqliteStore` | Rust allocated traffic | 11.884 GB | 7.916 GB | -33.4% |
| full runtime | elapsed | 38.006 s | 36.184 s | -4.8% |
| full runtime | process CPU | 39.479 s | 37.749 s | -4.4% |
| full runtime | Rust allocation calls | 313,001,067 | 210,601,020 | -32.7% |
| full runtime | Rust allocated traffic | 24.784 GB | 20.816 GB | -16.0% |

The allocation reduction is deterministic in these runs: exactly four temporary Rust allocations per decoded record disappear. Elapsed and CPU changes are smaller because SQL execution, final domain allocations, adapter task dispatch, and runtime subscription work remain. Peak RSS did not show a material reduction: the store median was unchanged at 4.44 MiB, while the runtime median moved from 84.0 MiB to 83.1 MiB. Peak RSS includes setup and allocator retention, so it does not measure the transient allocation traffic this patch targets.

The [raw paired results](evidence/sqlite-decode-borrowed-ab-100k.jsonl) and [provenance record](evidence/sqlite-decode-borrowed-ab-provenance.json) preserve every result, timer boundary, source archive hash, and binary hash.
