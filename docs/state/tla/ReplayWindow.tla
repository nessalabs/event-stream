---- MODULE ReplayWindow ----
EXTENDS Naturals
VARIABLES floor, accepted, start, finish, checkedFloor
vars == <<floor, accepted, start, finish, checkedFloor>>
Init == /\ floor = 0 /\ accepted = FALSE /\ start = 0 /\ finish = 0 /\ checkedFloor = 0

AdvanceFloor == /\ floor < 2 /\ floor' = floor + 1 /\ UNCHANGED <<accepted, start, finish, checkedFloor>>
Read == \E after \in 0..3, through \in 0..4:
        /\ after >= floor /\ through >= after /\ through <= 3
        /\ accepted' = TRUE /\ start' = after /\ finish' = through /\ checkedFloor' = floor
        /\ UNCHANGED floor

Next == AdvanceFloor \/ Read
Spec == Init /\ [][Next]_vars
RetainedStart == accepted => start >= checkedFloor
CommittedEnd == accepted => finish <= 3
====
