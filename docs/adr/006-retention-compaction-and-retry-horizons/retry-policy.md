# Retry policy required before retention implementation

Status: design constraint and proposed boundary. Not implemented.

## Forgetting an ID changes what we can know

The current append contract remembers event IDs for the stream lifetime. An
identical retry returns the committed record. Different content under the same
ID is a conflict. Deleting replay payloads does not remove this promise by itself.

After forgetting an arbitrary ID entirely, the store cannot distinguish an old
retry from a new event with that ID. A timeout does not supply that information.
Keeping only a checksum still keeps one entry per remembered ID; it does not
make the metadata bounded independently of event count.

```text
Earlier: ID "job-42" committed at cursor 10
Later:   both its record and retry metadata have been removed
Input:   ID "job-42"

The ID alone cannot tell us whether this is a delayed retry or new work.
```

Therefore ADR 0006 must not implement "delete old deduplication rows, then use
ordinary append". That would silently turn some retries into new events.

## Use an explicit retry generation for the bounded policy

The proposed bounded policy adds a durable retry generation to requests. A
generation is an integer identifying one period in which IDs can be retried.
The store rejects a request from an expired generation before looking up its ID.
This permits deletion of that generation's per-event retry metadata without
mistaking a delayed retry for new work.

Illustrative request, not a finalized Rust API:

```json
{
  "stream": "task-7",
  "incarnation": "...",
  "retry_generation": "4",
  "event_id": "job-42",
  "payload": "..."
}
```

With oldest accepted generation 5, that request returns an explicit expired-retry
error. The library never changes its generation to 5 automatically. Beginning
new work under generation 5 is an application decision and uses a new request
identity. The same content does not prove that an expired operation is safe to
repeat as an external action.

The existing append API has no generation. It must retain its original lifetime
retry semantics unless an explicit policy transition disables that API for the
affected stream. Once a stream uses expiring generations, an unqualified append
must fail with a policy-required error. It cannot silently choose the current
generation. This transition and its restart behavior need a typed contract before
implementation changes the schema.

## Keep replay deletion and retry expiration separate

Replay floor answers which event suffix is available. Retry generation answers
which request identities remain valid. They are separate boundaries.

Within an accepted generation, retry equality and its receipt remain exact even
if replay retention has removed the visible event record. The design must account
for the bytes needed to uphold that promise. If a receipt still returns a full
record, the retained retry data must contain its full content; a digest cannot
reconstruct it. A smaller receipt would require an explicit API decision, not a
quiet storage optimization.

Expiry must not invalidate accepted in-flight work or checkpoint recovery without
an explicit result. Snapshot recovery protection and required replica progress
constrain replay deletion. Captured-input recovery may also need to retain its
output request generation until its checkpoint is durable. These dependencies
need finite quotas and explicit exhaustion behavior.

## Decisions and proof required before coding

- Finalize the generation-bearing append request, receipt, lookup and conflict
  rules, including whether event IDs can be reused in a later generation.
- Define the atomic transition from lifetime retry to the bounded policy and
  how old clients fail. Never weaken the default foundation contract.
- Persist generation boundaries and prevent wraparound or reuse. Define what
  remains after reset, controlled restore and replication bootstrap.
- State exact metadata and payload retention costs. Measure the added per-record
  fields or indexes before choosing a storage layout.
- Test delayed retries before and after expiration, cancelled accepted appends,
  restart at every transition, snapshot protection and stalled captured-input
  output. Every expired request must fail explicitly without a new record.

This note closes an ambiguity in the roadmap; it does not mark retention or
retry expiration implemented. The final ADR 0006 contract must resolve these
items before adapters are built in parallel.
