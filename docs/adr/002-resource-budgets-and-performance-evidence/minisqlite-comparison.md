# MiniSQLite replay diagnostic

This is one bounded diagnostic run. It compares read-path costs. It is not a durability qualification or a production dependency decision.

The fixture contains 256 rows with 128-byte payloads. Every query asks for one page of 256 rows. Each fresh process performs the workload for 1,000, 10,000, or 100,000 consumers. The harness checks every row number, payload byte, and total count. A 300-second watchdog preserves failures instead of shrinking the workload.

[MiniSQLite](https://github.com/cursor/minisqlite/tree/4a5c1341fdde38beaf438e0241c9f45833c4b870) is pinned to commit `4a5c1341fdde38beaf438e0241c9f45833c4b870`. Source inspection found that its disk store defaults to DELETE rollback-journal mode. Its commit path syncs the journal, writes and syncs the database, then invalidates the journal. This run only reads a database created by bundled SQLite. It does not test MiniSQLite commits, crash recovery, locking, or compatibility with the production event-store schema.

## What each row times

| Mode | Timed work |
|---|---|
| `sqlite-prepared` | Open bundled SQLite, reuse one prepared simple `SELECT`, decode integer plus blob. |
| `sqlite-unprepared` | Open bundled SQLite, prepare the same simple `SELECT` for every consumer, decode integer plus blob. |
| `minisqlite` | Open MiniSQLite, run one `octet_length` capability probe, format and parse a literal simple `SELECT` for every consumer, decode integer plus blob. MiniSQLite exposes no prepared-statement API. |
| `store` | Open `SqliteStore`, resolve the stream, call its guarded production `read_range` for every consumer, close it. |
| `runtime` | Open the runtime, resolve the stream, register every subscription, drain each subscription, drop the subscriptions, and shut down. This is a complete replay lifecycle. It is not comparable to the earlier warmed-drain phase. |

Fixture creation is outside the timer. Bundled SQLite creates every simple-query fixture with `journal_mode=DELETE` and `synchronous=FULL`. `SqliteStore` creates the production fixture with its normal ProcessRestart profile. The raw `journal_mode` and `synchronous` fields record fixture-seed settings; they do not claim that MiniSQLite applied those pragmas during this read-only run.

Rust allocation counters cover cumulative allocation traffic in the timed region for the whole Rust process. They exclude bundled SQLite's C allocator. Peak RSS is a process-lifetime high-water mark and includes fixture setup and allocator retention; it is not live Rust heap. CPU is the process user-plus-system delta across the timed region.

## Observed result

All 15 cases returned the exact expected records. At 100,000 consumers:

| Mode | Runtime | Rust allocation calls/record | Rust allocated bytes/record | Lifetime peak RSS |
|---|---:|---:|---:|---:|
| bundled SQLite, prepared | 2.505 s | 1.000 | 128.0 B | 3.31 MiB |
| bundled SQLite, unprepared | 2.681 s | 1.000 | 128.0 B | 3.33 MiB |
| MiniSQLite | 5.592 s | 4.281 | 518.3 B | 5.19 MiB |
| `SqliteStore` | 20.249 s | 9.063 | 464.2 B | 4.41 MiB |
| full runtime | 29.730 s | 12.227 | 968.1 B | 80.91 MiB |

The prepared and unprepared bundled SQLite results differ by about 7% here because each consumer executes one query. MiniSQLite is about 2.23 times the prepared simple-query runtime in this single run. `SqliteStore` is about 8.08 times the simple prepared query. The full runtime is about 11.87 times it and also retains 100,000 subscription objects during the run.

These ratios do not identify one engine bottleneck. The simple-query modes return one integer and one blob. The production adapter executes corruption guards and constructs a `Record`, cursor, identifiers, payload, and `Arc`. Source inspection shows additional temporary allocation and copying in `decode_row`: SQLite produces owned `String`/`Vec<u8>` values, identifier constructors copy them into exact `Box<str>` allocations, and `Payload::copy_from_slice` copies the payload into an `Arc<[u8]>`. The runtime then adds admission, subscription registration, page ownership, scheduling, and shutdown work.

The [raw JSONL](evidence/minisqlite-comparison.jsonl) and [provenance record](evidence/minisqlite-comparison-provenance.json) preserve the single-run measurements and identities. The archived [harness source](evidence/minisqlite-comparison-source/main.rs), [MiniSQLite source](evidence/minisqlite-source-4a5c1341.tar.gz), and [relevant event-stream source](evidence/minisqlite-event-stream-relevant-source.tar) make the executed comparison inspectable.

## Next change: remove temporary row buffers

The event record must own its bytes after the SQLite query moves to the next row.
It does not need an intermediate owned copy. The decoder can borrow the current
row while constructing the final record:

```text
Before: SQLite row -> temporary Vec/String -> owned Payload/identifier
After:  SQLite row -------------------------> owned Payload/identifier
```

The change also reads the eight-byte offset directly from the row. Type, length,
UTF-8, and configured allocation checks remain in place. This is not zero-copy
delivery: the final record still owns its payload and identifiers. Borrowed row
bytes never escape the decoder. No unsafe code or shared record cache is needed.

The [paired benchmark](sqlite-decode-borrowed-ab.md) completed three repetitions
per path and version. Store allocation calls fell by 44.1%; its median runtime
fell by 2.8%. Full-runtime median time fell by 4.8%. Peak memory did not show a
material reduction. These results support removing the temporary allocations;
they do not establish a general library throughput advantage.
