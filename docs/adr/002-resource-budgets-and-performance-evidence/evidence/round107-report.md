# Round 107: batch contract before integration

Added bounded immutable append requests and one outcome per input to the store
port. The default implementation performs ordered individual appends. Runtime
scheduling and SQLite commit behavior are unchanged.

Five focused all-feature tests pass, including real SQLite reopen, mixed-stream
retry/conflict results, earlier success followed by capacity failure and later
retry, input resource limits, and rejected malformed responses. Four applicable
tests pass without optional features. Strict all-target Clippy, Rust 1.85 checks,
formatting, and all 19 verification tests pass. Logs and changed-source hashes
are retained beside this report.

See the [contract](../append-batch-contract.md) for ownership, logical byte
accounting, and remaining SQLite integration requirements. There are no fresh
performance measurements. Home round 107 explicitly says not measured.

The independent SQLite source review found that replication apply can mutate
clock state before a later error. Group transaction work must use complete
preflight or explicit item rollback, and distinguish retries of staged records
from retries of previously committed records. These are required implementation
steps, not properties established by the sequential fallback tests.
