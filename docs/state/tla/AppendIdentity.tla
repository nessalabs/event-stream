---- MODULE AppendIdentity ----
EXTENDS Naturals
VARIABLES stored, payload, tail, accepted, offered
vars == <<stored, payload, tail, accepted, offered>>
Init == /\ stored = FALSE /\ payload = 1 /\ tail = 0 /\ accepted = FALSE /\ offered = 1

Insert == \E value \in 1..2:
          /\ ~stored /\ stored' = TRUE /\ payload' = value /\ tail' = 1 /\ accepted' = TRUE /\ offered' = value
Retry == \E value \in 1..2:
         /\ stored /\ tail < 2 /\ value = payload
         /\ accepted' = TRUE /\ offered' = value /\ tail' = tail
         /\ UNCHANGED <<stored, payload>>

Next == Insert \/ Retry
Spec == Init /\ [][Next]_vars
SameContent == accepted => offered = payload
OneStoredIdentity == tail <= 1
====
