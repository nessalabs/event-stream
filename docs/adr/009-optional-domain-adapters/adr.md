# 0009. Package domain integrations separately from the stream core

- **Date:** 2026-09-04
- **Status:** proposed; not implemented
- **Prerequisite:** [0008](../008-local-first-replication/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

Generic framing does not interpret a terminal screen or normalize a provider's event vocabulary. Reusable integrations can supply that work without making every stream consumer install their dependencies.

## Decision

Use optional companion packages for terminal interpretation and agent-event normalization. They consume the core's public interfaces and share its limits and recovery rules. They do not assign committed cursors, bypass storage, own the core scheduler, or turn the library into a network server.

Treat adapter selection as a decision gate, not an approved dependency. For terminals, compare transporting ordered bytes plus resize events with transporting a versioned screen-state representation. Define replay behavior before choosing an emulator. `libghostty-vt` remains an option to evaluate separately, not a planned installation.

For agent events, distinguish byte framing, provider-to-common-schema mapping, and application transport. Already parsed SDK objects bypass framing. Unknown required semantics must fail explicitly rather than disappearing from replay.

This ADR records the extension boundary. It does not commit a Ghostty or provider implementation before its own reviewed contract.

The [terminal proposal](terminal-proposal.md) defines the proposed input,
projection, memory and replay boundaries. The [agent-normalization proposal](agent-normalization-proposal.md)
defines provider mapping, stable identities, bounded state and fixture requirements.
Emulator and provider selection remain open.

## Ordered work

1. Write a focused terminal-adapter proposal covering output bytes, resize ordering, state limits, escape-sequence handling, replay side effects, and version compatibility.
2. Write an agent-normalization proposal with a versioned application schema and real provider fixtures. Keep provider lifecycle and tools outside this package.
3. If a proposal is selected, build its optional package and run the shared resource/ingestion tests with domain fixtures.
4. Verify replay does not repeat terminal responses or external actions inadvertently. Bound scrollback, parser state, raw diagnostics, and normalized output.
5. Confirm the core builds and operates without either integration.

## Completion evidence

- The extension boundary is documented with runnable examples of custom decoding and opaque payload transport.
- Each selected companion has a separate reviewed contract, deterministic fixtures, replay rules, and measured memory/CPU limits.
- An unselected candidate has an explicit disposition and revisit trigger, not a hidden dependency.
- No terminal/agent dependency enters the default core build.
- The prior discussion-only candidate evaluation has not been converted into a claim of implementation.

## Consequences

A common repository can host companion packages without putting their semantics in the core. This keeps the generic product small. Full terminal and provider implementations are separate work once their contracts are selected.

## Inspected example evidence

The [custom decoder](../../../examples/custom_decoder.rs) uses the production
`IngestionService` and Memory runtime. Its binary fixture includes NUL and invalid
UTF-8, split inside a four-byte frame. The example asserts both committed
payloads, stable event IDs, schema versions, cursor order and complete replay.
Its [round 44 run](evidence/round44-custom-decoder-final.log) passed. This proves
that extension seam and opaque transport for the fixture; it does not implement
a terminal emulator or provider normalization.

The [projection reconnect example](../../../examples/projection_reconnect.rs)
also [passed](evidence/round44-projection.log): replay and live state agree, and
a failed application update preserves the prior checkpoint. Companion selection,
real provider fixtures and complete roadmap qualification remain separate gates.
