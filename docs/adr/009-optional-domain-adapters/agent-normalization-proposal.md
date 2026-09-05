# Agent-event normalization proposal

Status: proposed boundary, not a selected provider implementation. No provider
fixtures or compatibility claims are established by this document.

## Preserve source meaning before mapping it

The application acquires bytes or already parsed SDK events. Framing separates
complete messages from bytes. A provider adapter interprets each message. A
normalizer maps its meaning into an application schema. These are separate
steps because an SDK object does not need byte framing and a framed message
does not necessarily have understood semantics.

```text
raw source bytes -> bounded framing -> provider event -> application event
already parsed SDK event --------------^                       |
                                                        durable append
```

The generic stream crate stores the application's schema ID, schema version and
opaque payload. It does not contain a provider union or choose the application's
meaning of completion. A companion can implement the ingestion interfaces and
use the public event sink. It cannot assign cursors or write directly to storage.

## Use stable identity and explicit scope

A source session has its own incarnation. Retries derive stable event IDs from
that source identity and a documented provider position or captured byte/item
position. A connection-local counter is insufficient if it resets after a
reconnect. When no durable source position exists, state that limitation and use
the captured journal boundary from ADR 0007 where appropriate.

Model incremental text as ordered deltas, not repeated copies of the whole
accumulated transcript. Associate deltas with an explicit message or content
part identity. Tool arguments can arrive in pieces and may not form valid JSON
until a declared boundary. Do not execute a tool merely because a partial
argument string currently parses.

Illustrative application schema; it is not a finalized public wire format:

```json
{
  "schema": "example.agent-event.v1",
  "payload": {
    "kind": "text_delta",
    "message_id": "message-7",
    "part_id": "part-0",
    "text": "hello"
  }
}
```

Other explicit kinds can describe message start/end, tool-argument deltas,
completed tool-call requests, provider errors and usage observations. A source
EOF is not automatically a successful message completion. Usage observations
must say whether they are incremental or cumulative; blindly adding cumulative
values would double-count them.

Choose numeric types from the field's meaning. Counts use checked integers.
Identifiers remain identifiers even if their text contains digits. If a wire
format carries integers beyond JavaScript's exact numeric range, preserve them
as decimal strings or an explicitly supported binary integer encoding. Do not
round a cursor or source position through a floating-point representation.

## Unknown input must remain visible

An adapter declares the event variants and schema versions it supports. Unknown
required semantics fail explicitly at their source position. An application may
choose an opaque-event path that preserves the bounded original payload, but
that is transport compatibility, not successful normalization.

Bound diagnostic bytes separately. Do not retain an unlimited raw response in
an error or log. Sensitive source content follows the application's logging
policy; the core does not need full payload logging to report a cursor or parse
failure.

A normalized event can retain a bounded reference to its captured source
position for debugging. Avoid copying the complete raw payload into every
normalized event by default. If raw capture is required, store it once in the
journal and account for its retention separately.

## Keep effects outside replay

Normalization is deterministic for a pinned adapter version and input. It does
not call tools, start processes, make network requests or update billing.
Consumers decide what actions to perform after observing committed events.
Their external idempotency boundary is separate from append deduplication.

A completed tool-call event describes a request. Replaying that record does not
by itself authorize another execution. The same rule applies to terminal
responses and provider reconnect callbacks.

## Bound every accumulation point

The companion declares maximum frame bytes, pending UTF-8/JSON bytes, open
message parts, pending tool calls, tool-argument bytes, emitted events per input
and retained output bytes. A frame that emits several events must yield when
its work/output budget is exhausted and retain only bounded state.

Do not concatenate the complete growing transcript on every text delta. Keep
bounded chunks or let the application projection own accumulation. Validate
finished structured arguments at the declared completion boundary, under a
separate size/depth policy. Prefer a small explicit state machine over a broad
framework of per-field traits.

Checkpoint state includes adapter version, source position, open item identities
and bounded unfinished data. It contains no SDK objects or live connections.
An incompatible adapter version rejects the checkpoint and requires an explicit
replay or migration decision.

## Selection and evidence

No provider adapter is selected yet. Select one only when the application names
a concrete provider protocol and supplies or permits acquisition of fixtures
with known provenance. Do not label invented JSON as a real provider fixture.
A versioned fixture inventory must state the protocol/SDK version, event types,
redactions and expected normalization.

Acceptance requires every input partition to produce identical ordered output,
including partial Unicode, split tool arguments, multiple events per frame,
errors, interrupted messages and unknown variants. Already parsed input must
produce equivalent normalized output without a redundant encode/decode round
trip. Snapshot/journal recovery must preserve unfinished state and output IDs.

Measure CPU per input MiB, allocated and copied bytes, peak retained state,
output expansion and overload behavior. Include long unfinished arguments and
many open parts. Keep the default core build free of provider dependencies.

The repository's custom-decoder example demonstrates the generic extension
seam. It is not provider compatibility evidence. A selected companion needs its
own runnable fixtures, reviewed schema and resource measurements before ADR 0009
can claim that integration complete.
