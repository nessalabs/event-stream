---- MODULE RetentionProtection ----
EXTENDS Naturals
VARIABLES floor, recovery, replica, accepted, acceptedFloor, recoveryAtDecision, replicaAtDecision
vars == <<floor, recovery, replica, accepted, acceptedFloor, recoveryAtDecision, replicaAtDecision>>
Init == /\ floor = 0 /\ recovery = TRUE /\ replica = TRUE /\ accepted = FALSE /\ acceptedFloor = 0 /\ recoveryAtDecision = TRUE /\ replicaAtDecision = TRUE

ReleaseRecovery == /\ recovery /\ recovery' = FALSE /\ UNCHANGED <<floor, replica, accepted, acceptedFloor, recoveryAtDecision, replicaAtDecision>>
ReleaseReplica == /\ replica /\ replica' = FALSE /\ UNCHANGED <<floor, recovery, accepted, acceptedFloor, recoveryAtDecision, replicaAtDecision>>
Advance == \E next \in 0..2:
           /\ next >= floor /\ (~recovery \/ next <= 1) /\ (~replica \/ next <= 0)
           /\ floor' = next /\ accepted' = TRUE /\ acceptedFloor' = next
           /\ recoveryAtDecision' = recovery /\ replicaAtDecision' = replica
           /\ UNCHANGED <<recovery, replica>>

Next == ReleaseRecovery \/ ReleaseReplica \/ Advance
Spec == Init /\ [][Next]_vars
RecoveryProtected == (accepted /\ recoveryAtDecision) => acceptedFloor <= 1
ReplicaProtected == (accepted /\ replicaAtDecision) => acceptedFloor <= 0
====
