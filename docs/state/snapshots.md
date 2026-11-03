---
id: "snapshots"
title: "Publish a snapshot and protect replay"
kind: "flow"
status: "implemented"
summary: "A snapshot becomes visible only after complete verified bytes;
  recovery has a finite lease."
parent: "events"
sources:
  [
    "src/domain/snapshot.rs",
    "src/application/snapshot.rs",
    "src/infrastructure/memory.rs",
    "src/infrastructure/sqlite_snapshot.rs"
  ]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
model:
  {
    "spec": "tla/SnapshotPublication.tla",
    "config": "tla/SnapshotPublication.cfg"
  }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Published",
        "section": "Publication and recovery are different steps"
      },
    {
        "diagram": 0,
        "node": "Released",
        "section": "Publication and recovery are different steps"
      }
  ]
modelCheck:
  sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
  checker: tlc-1.7.4
  date: 2026-11-03T09:27:29.496Z
  config: tla/SnapshotPublication.cfg
  modelDigest: 690c26b3ec55e8b7f1896bfb9db291d03c1f6f6c65b4adaf8689af9f0496c119
  states: 11
  held:
    - CompleteVerification
    - MatchingDigest
  broke: []
  unchecked: []
---
# Recover application state plus a retained suffix

Your application creates snapshot bytes and a descriptor naming the cursor they
cover. For example, a snapshot through 7 represents application state after event 7;
recovery applies records after 7 through the plan's captured tail. The store can
check size/digest and cursor identity, but cannot prove those bytes mean what your
application says they mean.

```mermaid
stateDiagram-v2
    Uploading --> Verifying: All declared bytes present
    Verifying --> Verified: Entire content hash matches
    Verifying --> Rejected: Digest mismatch
    Verified --> Published: Stream and capacity checks pass
    Published --> Protected: Acquire finite recovery lease
    Protected --> Released: Explicit release or expiry
```

## Publication and recovery are different steps

Upload bounded chunks under one descriptor, drive bounded verification steps, and
publish only Verified content. Repeating the same publication can return the same
descriptor. A failed or uncertain publication is not evidence that nothing became
visible; preserve the ID/request and query or retry through the owning API.

Acquire recovery before reading snapshot bytes and the protected suffix. Its plan
captures through and holds a finite lease. Read within that boundary, build your
application state, and release protection when finished. Lease expiry means
protection is gone; do not continue assuming cleanup is blocked. Reacquire a valid
plan and reconcile partial application work. Schema compatibility and external
projection installation remain application responsibilities.

SnapshotPublication models two independent uploaded chunks, two verified chunks
and matching/mismatching content digest. It checks publication needs complete
content verification and a matching digest. It excludes cryptographic strength,
serialization meaning, publication capacity and recovery lease timing.
