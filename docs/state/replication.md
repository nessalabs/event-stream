---
id: "replication"
title: "Copy committed history to a replica"
kind: "flow"
status: "implemented"
summary: "Destination commit and origin acknowledgement are separate;
  replication does not elect writers."
parent: "events"
sources:
  [
    "src/application/replication.rs",
    "src/domain/replication.rs",
    "src/infrastructure/memory_replication.rs",
    "src/infrastructure/sqlite_replication.rs"
  ]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
model:
  {
    "spec": "tla/ReplicaAcknowledgement.tla",
    "config": "tla/ReplicaAcknowledgement.cfg"
  }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Unknown",
        "section": "A lost reply can leave the copy ahead of its acknowledgement"
      },
    {
        "diagram": 0,
        "node": "Ack",
        "section": "A lost reply can leave the copy ahead of its acknowledgement"
      }
  ]
modelCheck:
  sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
  checker: tlc-1.7.4
  date: 2026-11-03T09:27:22.672Z
  config: tla/ReplicaAcknowledgement.cfg
  modelDigest: c4648fa8901987cac73827863c6cc3d9e45d434c28c9981035f7fefaabb2001e
  states: 6
  held:
    - DestinationBeforeAck
    - MatchingReceipt
  broke: []
  unchecked: []
---
# Catch up a separate durable copy

Attach a replica to one origin stream and drive bounded batches. The origin
prepares exact records; the transport commits them at the destination; a matching
receipt lets the origin advance its acknowledgement and release backlog capacity.
The host owns transport authentication, retry policy and remote availability.
A replica is a copy, not an elected replacement writer.

```mermaid
flowchart TD
    Prepare[Prepare exact origin batch] --> Send[Send to destination]
    Send --> Commit[Destination commits exact batch]
    Commit --> Receipt[Validate batch epoch stream and end position]
    Receipt --> Ack[Origin acknowledges]
    Send --> Unknown[Reply lost or uncertain]
    Unknown --> Retry[Retry exact operation identities]
    Retry --> Send
```

## A lost reply can leave the copy ahead of its acknowledgement

Do not release origin protection on send or transport success alone. The driver
validates batch ID, destination epoch, stream and exact committed-through position
before acknowledgement. Retain identities through an uncertain send/ack; exact
retry reconciles without inventing a new batch. Required replica backlog can
refuse new appends or retention/lifecycle work. Pause bounded source acquisition,
catch up and retry the rejected event with its original identity.

If a replica lacks a retained prefix, use snapshot-plus-suffix bootstrap rather
than pretend ordinary replay starts at the true beginning. Bootstrap uploads and
verifies data before publication, with separate retry/cleanup ownership. Restoring
an origin rotates identity and detaches old replication state; stale requests
must not silently enter the replacement. Runtime-owned transfer completion does
not close the caller-owned destination; close it only after source shutdown is
resolved. See the [two-store walkthrough](../operations.md).

ReplicaAcknowledgement models one batch, an independently matching or foreign
receipt and a separate destination-commit fact. A valid receipt is generated only after the modeled destination commits; this
is a trusted destination contract, not a second runtime query of its disk. The
model checks origin acknowledgement requires that commit and a matching receipt. Real transport authenticity, epochs,
bytes, bootstrap and failover are outside the abstraction; a malicious transport
is not made trustworthy by a Boolean model.
