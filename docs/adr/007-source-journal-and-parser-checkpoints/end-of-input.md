# Durable end of input

Status: Memory, SQLite, runtime and ingestion finish path implemented; broader resource and integration qualification pending.

A quiet source is not a finished source. Reaching the captured tail only means
there are no more acknowledged bytes to read right now. Applications must tell
the library when the source has actually ended. A socket disconnect alone may
mean reconnect, so the application decides whether it is terminal.

## Two durable steps

1. Seal the source at its exact captured end. This prevents later new capture.
2. Run the decoder's `finish` operation and durably mark the parser finished.

The gap between these steps is recoverable. Sealing does not say parsing
succeeded. An invalid final frame can leave a sealed source with an unfinished
parser and an explicit decoding error.

```text
captured bytes:  a \n b
byte boundary: 0 1 2 3

open                 sealed at 3             parser finished
more capture allowed -> no new capture       -> final checkpoint at 3
                        finish may emit b      no new decoded outputs
```

`recover_captured` continues to mean caught up with captured bytes. It must not
call `finish` without a durable seal. Its `complete_capture` field is not proof
of EOF. The finalization driver will report parser completion separately.

## Typed boundary

`SourceFinalizationStore` extends `SourceJournalStore`. It is a separate port so
an adapter cannot appear to support durable EOF just by implementing capture.
There are no default success implementations.

- `seal_source(SealSource { end: SourcePosition }) -> SealSourceReceipt`
- `finish_source(FinishSource { checkpoint: ParserCheckpoint }) -> FinishSourceReceipt`
- `source_finalization(&SourceKey) -> SourceFinalizationStatus`

A source lifetime can have one seal and one final checkpoint. Their identities
are the source lifetime and exact values; these operations do not allocate an
unbounded collection of operation IDs. The ordinary parser checkpoint remains
the only stored decoder-state payload. Finalization must not keep another copy.

## Seal transaction

The requested end must equal the currently captured end. A different boundary
returns `InvalidInput`; it must not truncate or skip input. Exact seal retries
return the original end. Capture and sealing use the same serialized storage
boundary. Capture either commits first and changes the end, or sealing commits
first and rejects the new segment with `SourceSealed`.

Existing capture retries still follow the normal receipt equality and expiry
rules. Sealing does not revive expired receipts. Reads and cleanup remain
available. Empty sources can be sealed at zero.

A lost commit acknowledgement returns `SealUnknown` with the exact request.
The caller retries it or reads finalization status. No final decoder output is
produced before the seal commits.

## Final parser transaction

The source must be sealed. The supplied checkpoint must equal the currently
stored checkpoint, including parser identity, output lifetime, state bytes,
item index and output cursor. Its source offset must equal the sealed end.
The store sets a finished flag atomically after those checks. It does not need
to interpret decoder state; the ingestion driver owns that validation.

Exact completion retries return the same receipt. A different checkpoint
returns `CheckpointConflict`. Once finished, new output and changed checkpoints
are rejected. Exact output and checkpoint retries remain governed by their
existing equality and expiry rules. A lost acknowledgement returns
`FinishUnknown` carrying the exact request.

The driver restores the existing checkpoint, drains captured bytes, calls
`finish` within the existing work/output/byte limits, commits each output with
its normal stable decoded position, publishes the finished decoder checkpoint,
and then calls `finish_source`. A crash after final output but before checkpoint
must deduplicate on restart. A crash after the checkpoint but before the finished
flag must reconcile completion without emitting another output. MoreWork is not
Finished. Final decode errors must stay explicit.

## Storage and restore

Use fixed per-source metadata: nullable sealed end and a finished flag. Charge
this metadata to the existing source budget. Do not create a worker, timer,
queue, or extra decoder-state allocation for each sealed source. Existing open
sources migrate with no seal and unfinished parser state.

Reopen validates seal equals captured end. Finished requires a matching final
checkpoint at that boundary. Restore preserves the seal and finished state,
rewrites existing source/output identities consistently, and rejects malformed
metadata before publication. Cleanup must preserve the final checkpoint and
must not turn a finished source back into an open one.

## Required evidence

Both adapters need shared tests for empty input, terminated and unterminated
frames, exact seal/finish retries, mismatched ends/checkpoints, capture-vs-seal
ordering, new output rejection, cleanup and bounded metadata. SQLite also needs
close/reopen, legacy migration, controlled restore, malformed backups and lost
acknowledgements. The integration crash matrix must include seal commit, final
output commit, final checkpoint commit and finished-flag commit. The native
scenario should use the same application finalization path.

This document does not establish those guarantees as implemented. Until the
adapter and driver tests pass, restart-safe finalization remains incomplete.

## Current checkpoint

Round 88 defines the public port and implements it in MemoryStore. The
[new adapter test](../../../tests/source_finalization.rs) covers empty and
nonempty sources, exact seal and finish retries, wrong ends, completion before
sealing, changed checkpoints, and new capture/output rejection.
[All 11 focused journal tests pass](evidence/round88-journal.log), as does
[strict source-journal library/test Clippy](evidence/round88-clippy.log).
This is adapter-state evidence. It does not yet prove the decoder finish driver,
SQLite persistence, or EOF crash recovery.

Round 89 routes seal, finish and status through Runtime's existing bounded
journal operations. Shutdown rejects all three. NewlineFramer can now persist a
successful terminal checkpoint and restore it without emitting the final frame
again. Failed terminal decoders still cannot publish checkpoints. Live `NLCP1`
checkpoints remain readable; successful EOF uses the same bounded layout with
an `NLCF1` tag, an empty partial frame, and equal frame-start/end positions.
Older readers reject the new tag explicitly. No checkpoint payload is duplicated
in the source finalization metadata.

[Thirty-four source-journal library and focused tests pass](evidence/round89-final.log),
including old live checkpoint restore, unterminated final-frame emission,
finished checkpoint replay, malformed finished state, and rejected final frames.
[Strict library/test Clippy passes](evidence/round89-clippy.log).
The ingestion driver's finish loop and SQLite qualification remain incomplete.

Round 90 adds `JournalIngestionService::finish_captured`. It requires a durable
seal and reuses the bounded recovery loop. At the sealed end it calls decoder
`finish`, commits mapped output through the existing journal identity path,
saves the finished checkpoint and marks the source complete. Repeated completed
calls return zero new output. `recover_captured` still does not infer EOF.

[Thirty-five source-journal tests pass](evidence/round90-tests.log), including
an unterminated final frame whose mapping fails and then succeeds on retry.
The test verifies exactly two stored records (`a`, `b`) and a single final-frame
commit after retry. [Strict library/test Clippy passes](evidence/round90-clippy.log).
Finalization returns `JournalFinishProgress`; its `parser_finished` flag is
separate from the existing caught-up-with-capture flag. SQLite crash boundaries,
empty-source driver coverage, capacity/failure schedules and native EOF callback
coverage remain to be qualified.

Round 91 independently tests SQLite finalization through the production ingestion
driver. Empty input completes. A multi-frame source finishes through calls limited
to one output each, then reopens with its exact checkpoint and history. A second
matrix kills real child processes after seal commit, final output commit,
finished-checkpoint commit and finished-flag commit. Each resumes through
`finish_captured` to the exact three outputs and an idempotent completion retry.
The parent bounds readiness waits and reaps children; Unix asserts SIGKILL.
These are post-commit process kills, not power-loss or arbitrary in-transaction
crash claims. [Both driver cases and the child harness pass](evidence/round91-kills.log).

The combined [302-test library/integration suite](evidence/round91-all-tests.log)
passes, including five SQLite adapter tests for legacy migration, controlled
restore, exact retry, pre-commit rollback, lost acknowledgement and corrupt
finalization metadata. [Strict all-feature/all-target Clippy passes](evidence/round91-clippy-final.log)
after moving the Memory implementation before its test module. Native EOF,
cleanup combinations, wider decoder contracts and resource qualification remain
separate work.

Round 92 connects durable EOF to the shared native/headless recovery fixture.
It verifies the finished source after cleanup and restart, alongside snapshot
replication and retained history. The [six-observation console report](../010-product-readiness-and-scope-closure/evidence/round92-full-recovery.json)
passes. This is headless execution of the native callback, not a visual inspection
of the running window. Resource and wider concurrency qualification remain open.
