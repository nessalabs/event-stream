# Round 110: grouped append process termination

A parent test now kills a real child process on both sides of a grouped SQLite
commit. The child calls the production `append_batch` port. No production code
or storage setting changes in this round.

Each schedule starts with an already committed baseline record. The group
contains two new event IDs and an exact retry of the first new ID. The child
uses existing test pause hooks; the parent waits for observable database state,
not a guessed sleep interval, then kills and reaps that child.

For the uncommitted schedule, a written rollback journal and a separate read-only
connection showing only the baseline prove that a write transaction is in
progress without externally visible new records. This does not identify the
exact last staging instruction. Recovery preserves the baseline and neither
new record. Retrying the group inserts both IDs once; its internal retry dedups.

For the committed schedule, the separate connection sees all three records while
the worker is paused after commit and before its response. No reply marker may
exist. After termination, reopening preserves exact IDs, schemas, payloads and
cursors. Retrying the entire group deduplicates every input without advancing
the tail. This tests process termination, not device power loss.

Both schedules pass with all features and with SQLite alone. Cargo reports two
tests per run: one parent test containing both schedules and one child entry
point that is inert when run normally. Do not interpret this as two independent
qualification repetitions. The process guard kills/reaps children even on test
failure, and each observation has a ten-second deadline.

Focused strict Clippy and Rust 1.85 checks pass. No fresh runtime latency or memory
measurements were taken. Runtime batching, sustained comparison, and broader
release resource gates remain open. Home round 110 is marked not measured.

All 19 verification tests and formatting checks pass after the Home update.
