---- MODULE JournalCheckpoint ----
EXTENDS Naturals
VARIABLES captured, committed, saved, savedSource, savedItems, captureAtSave, commitAtSave
vars == <<captured, committed, saved, savedSource, savedItems, captureAtSave, commitAtSave>>
Init == /\ captured = 0 /\ committed = 0 /\ saved = FALSE /\ savedSource = 0 /\ savedItems = 0 /\ captureAtSave = 0 /\ commitAtSave = 0

Capture == /\ captured < 2 /\ captured' = captured + 1 /\ UNCHANGED <<committed, saved, savedSource, savedItems, captureAtSave, commitAtSave>>
CommitOutput == /\ committed < captured /\ committed' = committed + 1 /\ UNCHANGED <<captured, saved, savedSource, savedItems, captureAtSave, commitAtSave>>
Checkpoint == \E source \in 0..2, items \in 0..2:
              /\ source <= captured /\ items <= committed
              /\ saved' = TRUE /\ savedSource' = source /\ savedItems' = items
              /\ captureAtSave' = captured /\ commitAtSave' = committed
              /\ UNCHANGED <<captured, committed>>

Next == Capture \/ CommitOutput \/ Checkpoint
Spec == Init /\ [][Next]_vars
CapturedBeforeCheckpoint == saved => savedSource <= captureAtSave
OutputBeforeCheckpoint == saved => savedItems <= commitAtSave
====
