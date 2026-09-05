# Reproduce the diagnostic

This harness is intentionally outside the production workspace. MiniSQLite is pinned by commit and lockfile. The checked-in manifest records the path used for the measured build.

To build against another checkout, copy this directory to a temporary directory and replace the `event-stream` path without changing any other manifest or source field:

```sh
cp -R docs/adr/002-resource-budgets-and-performance-evidence/evidence/minisqlite-comparison-source /tmp/event-stream-minisqlite-repro
python3 - /tmp/event-stream-minisqlite-repro/Cargo.toml "$PWD" <<'PY'
from pathlib import Path
import sys
manifest = Path(sys.argv[1])
text = manifest.read_text()
start = 'path = "/Users/nessa/Documents/NessaLabs/event-stream"'
text = text.replace(start, f'path = "{sys.argv[2]}"')
manifest.write_text(text)
PY
CARGO_TARGET_DIR=/tmp/event-stream-minisqlite-repro-target \
  cargo build --manifest-path /tmp/event-stream-minisqlite-repro/Cargo.toml --release --locked
```

Run one fresh process per case. The measured matrix used every combination of the five modes and three populations:

```sh
/tmp/event-stream-minisqlite-repro-target/release/event-stream-minisqlite-diagnostic sqlite-prepared 1000
/tmp/event-stream-minisqlite-repro-target/release/event-stream-minisqlite-diagnostic sqlite-unprepared 1000
/tmp/event-stream-minisqlite-repro-target/release/event-stream-minisqlite-diagnostic minisqlite 1000
/tmp/event-stream-minisqlite-repro-target/release/event-stream-minisqlite-diagnostic store 1000
/tmp/event-stream-minisqlite-repro-target/release/event-stream-minisqlite-diagnostic runtime 1000
```

Repeat those commands with populations `10000` and `100000`. Apply a 300-second process watchdog. Do not run compilation or other timed benchmarks concurrently.

`minisqlite-event-stream-relevant-source.tar` preserves the exact production files on the measured read paths for source review. It is a scoped audit archive, not a standalone crate snapshot. The executed binary hash in the provenance record is the final build identity.
