# Keep future arrivals out of memory

A long workload should not allocate one task for every future request. The
previous sustained harness did that. Its task count grew with the experiment's
duration even when the library was idle. That makes it unsuitable for measuring
steady resource use.

The new generator keeps a fixed maximum of 256 append tasks. It computes the
next deadline from the start time and event index. At each deadline, it offers
one event for each configured producer. This preserves the previous synchronized
arrival pattern. A slow receipt does not move future deadlines.

Before offering work, the generator collects completed tasks. If all 256 slots
are occupied, it rejects that offer at the generator boundary. It does not wait
for capacity or silently lower the offered rate. Generator rejection is separate
from a rejection returned by the runtime.

```text
computed deadline
      |
      v
collect ready results
      |
      +-- no task slot --> generator_rejected += 1
      |
      v
submit through Runtime::try_append
      |
      +-- accepted --> committed receipt
      +-- overloaded --> caller/runtime rejection
      +-- unexpected error --> failure
```

The outcome contract is:

```text
offered = successful receipts + runtime call rejections
        + failed calls + generator rejections

spawned append tasks = offered - generator rejections
peak outstanding append tasks <= 256
```

Scheduling delay begins at the intended deadline. Receipt latency begins when
the task actually submits its call. Generator rejections have no receipt latency
because they never call the runtime. Sampling uses the same producer/event
identity rule as the existing harness. A bounded sample cap remains necessary:
bounded tasks alone do not bound stored measurement vectors.

This is a harness change. A reduced memory sample does not imply a reduction in
library memory. The generator cap can change which offers reach the runtime
under overload. Comparisons must report those changed outcomes, and must not
call lower successful work a throughput improvement.

## Comparison before qualification

First compare fresh processes of the old and new generators at four producers,
four streams, 1,000 events per producer and 5,000 microseconds between each
producer's offers. Run three interleaved repetitions on Memory and SQLite with
unchanged event bytes, runtime limits, and SQLite durability. Record both binary
identities and source archives. Compare outcome counts before latency or memory.
No numerical release threshold is selected from these diagnostic runs.

Then use longer runs with the bounded generator. Scale qualifications must
specify how many streams receive sustained writes, the per-agent rate and the
aggregate rate. Four active producers with 100,000 stored stream names are not
100,000 active producers. The existing sustained scenario retains its configured
producer-to-stream mapping; a rotating-population workload needs a separate
explicit contract.
