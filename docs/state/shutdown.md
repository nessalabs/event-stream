---
id: "shutdown"
title: "Drain accepted work before reopening"
kind: "flow"
status: "implemented"
summary: "Shutdown timeout returns unresolved evidence while the finalizer retains ownership."
parent: "events"
sources: ["src/application/runtime.rs", "src/application/types.rs", "src/application/config.rs"]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
diagramDetails: [{"diagram": 0, "node": "Draining", "section": "Inspect the report before handing over ownership"}, {"diagram": 0, "node": "Closed", "section": "Inspect the report before handing over ownership"}]
---

# Stop admission without abandoning accepted operations

Call Runtime.shutdown with a deadline when the host is stopping. It moves to
Draining, rejects new work, ends subscriptions and starts an owned finalizer.
The finalizer continues if the caller cancels or times out its wait. A deadline
bounds the wait; it does not magically stop storage I/O.

```mermaid
stateDiagram-v2
    Ready --> Draining: Shutdown requested
    Faulted --> Draining: Shutdown attempts cleanup
    Draining --> Closed: Accepted work settles and store closes
    Draining --> Draining: Caller deadline returns unresolved report
    Draining --> Faulted: Store close fails
```

## Inspect the report before handing over ownership

A report with closed=false means close did not finish by the deadline. Preserve
unresolved append IDs, lifecycle requests and unfinished cleanup count. The old
owner can still own I/O; do not force-open another writer or delete ownership
files. Continue observing shutdown and reconcile named unknown operations through
the store's contract. Even closed=true may carry unresolved historical outcomes;
closed and every business effect known are different claims.

Dropping a handle or future is not cancellation of an accepted commit. The runtime
retains finite request/buffer/I/O reservations until owned work settles. Capacity
is charged across waiting and in-flight work, not merely visible queue length.
Default limits and diagnostics are policy settings, not qualified population
capacity. This page is source-reviewed; it has no model of scheduler progress,
shutdown timing, deadlocks or filesystem-close guarantees.
