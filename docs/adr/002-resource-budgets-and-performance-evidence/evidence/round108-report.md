# Round 108: SQLite transaction staging

SQLite append now separates staging from transaction ownership. The staging
helper requires a transaction reference. It never commits or rolls back and
may return an error after a mutation. Its owner explicitly rolls back every
staging error, including replication metadata errors. Failed retry rollback
faults the store instead of allowing reuse of an uncertain connection.

Uncertain commit reconciliation first checks that no transaction remains open.
It then compares the complete original staged record, including cursor and
bytes. Presence of an ID alone is insufficient proof of the expected commit.

The final all-feature library/integration run passes **330 tests**. This
includes two new real-transaction tests, 61 SQLite adapter tests, and seven
filesystem/VFS fault tests. The new tests exercise insert/retry/conflict staging,
external visibility before commit, rollback of a staged retry, and a replication
clock failure after mutation. The rollback test then appends successfully and
reopens the database to verify the committed record.

Strict all-target Clippy, Rust 1.85 checks, SQLite-only staging test, formatting,
and all 19 verification tests pass. Logs and changed-source hashes are retained
beside this report. Independent source review identified the retry rollback
exit and recommended the typed transaction boundary; both are addressed.

No performance samples were taken. Home round 108 is not measured. This is
preparation for grouped transactions, not a batch throughput result. Group-wide
rollback/unknown-commit outcomes, bounded runtime collection, and the controlled
performance comparison remain open. See the [contract](../append-batch-contract.md).
