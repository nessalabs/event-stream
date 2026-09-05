# 0007. Make source recovery optional and tie it to committed output

- **Date:** 2026-09-04
- **Status:** proposed; not implemented
- **Prerequisite:** [0006](../006-retention-compaction-and-retry-horizons/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

A process can crash after reading source bytes but before committing decoded events. Recovering output records alone cannot repair this gap. Some applications need stronger capture guarantees and can pay the extra storage cost.

## Decision

Provide an optional raw-input journal. Save observed input before acknowledging source progress. Associate it with a stable source identity and position. Keep versioned decoder checkpoints that describe state after an exact input position and output boundary.

Resume from a compatible checkpoint and replay captured input. Use stable output IDs so repeated decoding resolves already committed events. Publish checkpoint progress only after all output it covers is committed. No atomic multi-stream API is assumed: if input and output use separate commits, the recovery protocol must explicitly handle every gap.

Bound journal backlog, decoder state, and temporary output. Retain input until the checkpoint/output relationship makes it safe to reclaim. Pause or fail explicitly when a source cannot be captured within those limits. Bytes never observed or saved remain unrecoverable.

## Ordered work

1. Specify source identity, raw segment positions, codec/version identity, output IDs, and checkpoint format.
2. Write a state table for capture, decode, output commit, and checkpoint publication. Add a crash point between every pair.
3. Implement bounded restore and input cleanup using ADR 0006's resource rules.
4. Test one frame that emits several records, malformed suffixes, source rotation, incompatible parser versions, and lost source acknowledgements.
5. Measure the extra input writes and CPU separately from ordinary parsed-event ingestion.

## Completion evidence

- Replaying captured bytes after each injected crash produces no missing or duplicate committed output.
- Checkpoints never claim progress beyond committed output. Unsupported decoder state fails explicitly.
- Source rotation cannot reuse an old source identity accidentally.
- Journal growth and replay work remain bounded under a stalled output store.
- Documentation distinguishes capture durability from external-source availability and states the measured additional cost.

## Consequences

This path writes more data and adds checkpoint state. Keep it optional and out of the simple append path. It does not resume external actions or promise lossless data that never reached the journal.

## End-of-input completion work

[Durable end of input](end-of-input.md) defines the two-step seal and parser
completion contract. A captured tail is not EOF. The typed port and Memory
adapter exist; SQLite, runtime/ingestion finalization and failure qualification
remain in progress. Existing recovery evidence must not be read as proving EOF.
