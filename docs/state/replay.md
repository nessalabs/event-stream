---
id: "replay"
title: "Read and subscribe from a cursor"
kind: "flow"
status: "implemented"
summary: "Read after a position through an inclusive boundary, then follow live
  history."
parent: "events"
sources:
  [
    "src/application/runtime.rs",
    "src/application/types.rs",
    "src/domain/cursor_codec.rs",
    "src/application/ports.rs"
  ]
sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
diagramLinks: {}
model: { "spec": "tla/ReplayWindow.tla", "config": "tla/ReplayWindow.cfg" }
diagramDetails:
  [
    {
        "diagram": 0,
        "node": "Ended",
        "section": "Delivery is not application acknowledgement"
      },
    {
        "diagram": 0,
        "node": "Live",
        "section": "Delivery is not application acknowledgement"
      }
  ]
modelCheck:
  sourceRevision: "e16580f3d3602a1ff9966e89d4915be4e752aea0"
  checker: tlc-1.7.4
  date: 2026-11-03T09:27:19.302Z
  config: tla/ReplayWindow.cfg
  modelDigest: 6f1f13d1ba5300c5d914234f17ec2acf5663340e1f18ec94ea3d93bbd8708c34
  states: 48
  held:
    - RetainedStart
    - CommittedEnd
  broke: []
  unchecked: []
---
# Resume after the last event your application applied

If your saved cursor is 7 and the captured tail is 10, a range read asks for
`(7, 10]`: records 8, 9, 10, subject to page limits. Cursor 0 means before the first
record. Through equal to after is an empty range; through before after is invalid.
A future cursor is refused. A cursor below the retention floor returns
HistoryUnavailable rather than a silently shortened history.

```mermaid
stateDiagram-v2
    [*] --> Capture: Validate start and capture committed tail
    Capture --> Replay: Beginning or After saved cursor
    Capture --> Live: Future starts at captured tail
    Replay --> Live: Captured prefix delivered
    Live --> Live: Next committed record
    Replay --> Ended: Lag or lifecycle or store failure
    Live --> Ended: Lag or lifecycle or shutdown
```

## Delivery is not application acknowledgement

A subscription captures its replay boundary under the stream's write gate,
registers membership, then reads replay before live history. Future intentionally
skips all records committed at capture. Beginning means the current retained floor,
not necessarily the beginning of all historical time. Save a cursor with your
successfully applied application state; last_delivered only says the library
returned the record. The [projection example](../../examples/projection_reconnect.rs)
demonstrates not advancing application state on a rejected transition.

Cancelling pending next preserves delivery position. Slow subscriptions terminate
with SubscriberLagged and last-delivered/bounds evidence; they do not retain an
unlimited queue. Reconnect from the saved applied cursor if it remains retained,
or use a suitable snapshot recovery plan when old history is unavailable. One
terminal error is delivered, then None. None is not proof the application consumed
all historic events. Reset/delete also invalidate the old lifetime.

Cursor tokens encode version, name, incarnation and a full-width integer offset.
They are validated bounded encodings, not credentials or authorization grants.
Do not round offsets through a JavaScript floating-point number in a transport.

ReplayWindow models after/through/tail0..3 and a retention floor. It checks accepted
ranges never begin below the floor or end beyond captured history. It excludes
payload contiguity, subscription wakeup races, lag clocks and application commits.
