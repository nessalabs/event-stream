---- MODULE ReplicaAcknowledgement ----
EXTENDS Naturals
VARIABLES destinationCommitted, receiptReady, matches, acknowledged, commitAtAck, matchAtAck
vars == <<destinationCommitted, receiptReady, matches, acknowledged, commitAtAck, matchAtAck>>
Init == /\ destinationCommitted = FALSE /\ receiptReady = FALSE /\ matches = FALSE /\ acknowledged = FALSE /\ commitAtAck = FALSE /\ matchAtAck = FALSE

CommitDestination == /\ ~destinationCommitted /\ destinationCommitted' = TRUE /\ UNCHANGED <<receiptReady, matches, acknowledged, commitAtAck, matchAtAck>>
ValidReceipt == /\ ~receiptReady /\ destinationCommitted /\ receiptReady' = TRUE /\ matches' = TRUE
                /\ UNCHANGED <<destinationCommitted, acknowledged, commitAtAck, matchAtAck>>
ForeignReceipt == /\ ~receiptReady /\ receiptReady' = TRUE /\ matches' = FALSE
                  /\ UNCHANGED <<destinationCommitted, acknowledged, commitAtAck, matchAtAck>>
Acknowledge == /\ receiptReady /\ matches /\ ~acknowledged /\ acknowledged' = TRUE
               /\ commitAtAck' = destinationCommitted /\ matchAtAck' = matches
               /\ UNCHANGED <<destinationCommitted, receiptReady, matches>>

Next == CommitDestination \/ ValidReceipt \/ ForeignReceipt \/ Acknowledge
Spec == Init /\ [][Next]_vars
DestinationBeforeAck == acknowledged => commitAtAck
MatchingReceipt == acknowledged => matchAtAck
====
