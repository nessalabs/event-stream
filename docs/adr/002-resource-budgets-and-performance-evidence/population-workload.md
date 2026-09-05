# Sustain writes across an agent population

A population test must send work to every agent. Creating 100,000 streams while
writing to four of them measures stored metadata, not 100,000 active streams.
The population workload uses the bounded generator with an explicit
mapping from each offer to a stream.

For population N, generator width W and per-producer event index i:

```text
offer_index = i * W + producer_index
stream_index = offer_index % N
stream_event_index = offer_index / N
```

Every full cycle visits each stream once. The ID includes the stream event
index. The offered aggregate rate and each stream's revisit interval are
reported separately. For example, at 800 events/second, visiting 100,000 streams
takes 125 seconds. This is sparse activity across a large population. It is not
800 events/second per agent.

Keep at most 256 outstanding tasks regardless of N. Count generator rejections
separately. Increase the configured stream/coordinator allowance to the named
population; keep queue and worker limits fixed and report them. Setup must be
measured separately, since creating SQLite stream identities is real work.

A qualifying run must complete at least two population cycles. Record offered,
accepted, rejected and failed counts per cycle. Verify each stream's exact
committed event IDs and cursors with bounded replay, not only the total count.
Report uncovered streams explicitly if overload prevents their first success.
Do not classify a partially covered population as successful service to all N.

Use 1,000, 10,000 and 100,000 streams on Memory and SQLite. Before claiming a
supported profile, record aggregate/per-stream rates, observation duration,
queue peaks, receipt and scheduling latency, allocations, RSS, storage growth
and shutdown. Report setup cost, active history cost and memory after dropping
runtime/harness ownership separately. Retaining immutable history necessarily
uses space; it must not be mislabeled as a leak.

The `population_sustained` implementation now provides this mapping and exact
receipt/replay checks. Its measured qualification is recorded separately. The
original `sustained` scenario still maps producer p to stream p modulo N. Its
one-minute four-stream results cannot close the population gate.

## Typed verification state

The public benchmark scenario is `population_sustained`. The existing
`sustained` mapping remains unchanged. Require a whole number of cycles and at
least two cycles. Keep the existing one-million-offer maximum.

A compact receipt ledger is indexed by offer number. Zero means no committed
receipt was observed. A positive `u64` stores the actual returned cursor offset.
At most one million entries cost 8 MiB of element storage; allocated capacity
and counters are additional harness state and must be reported as such. This
ledger is measurement/verification state, not a runtime memory optimization.

Do not infer order from the cycle number. Task scheduling can change submission
order. Replay must match each event's original returned cursor offset, exact
ID and payload. Iterate each stream with bounded pages and check every accepted
identity exactly once. A failed verification invalidates the entire sample.

Keep history verification outside the timed arrival/commit region and report
its duration separately. Report per-cycle conservation even if the generator
rejects an offer before creating an append task. Such an offer must not acquire
a receipt latency sample. A stream counts as covered only after an actual
successful receipt, never because it was created or offered work.

The result reports receipt-ledger and cycle-counter vector capacities explicitly.
Those numbers exclude wrappers, latency samples and the temporary per-stream
expected-record vector. Completed generation moves its existing vectors into
verification instead of cloning the complete ledger. Nominal mean revisit times
are labeled as means: grouped producer deadlines can give uneven visit intervals
when the producer count does not divide the population.
