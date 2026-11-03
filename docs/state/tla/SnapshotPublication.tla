---- MODULE SnapshotPublication ----
EXTENDS Naturals
VARIABLES uploaded, verified, digestMatches, published
vars == <<uploaded, verified, digestMatches, published>>
Init == /\ uploaded = 0 /\ verified = 0 /\ digestMatches \in BOOLEAN /\ published = FALSE

Upload == /\ uploaded < 2 /\ uploaded' = uploaded + 1 /\ UNCHANGED <<verified, digestMatches, published>>
Verify == /\ uploaded = 2 /\ verified < 2 /\ verified' = verified + 1 /\ UNCHANGED <<uploaded, digestMatches, published>>
Publish == /\ ~published /\ verified = 2 /\ digestMatches /\ published' = TRUE
           /\ UNCHANGED <<uploaded, verified, digestMatches>>

Next == Upload \/ Verify \/ Publish
Spec == Init /\ [][Next]_vars
CompleteVerification == published => (uploaded = 2 /\ verified = 2)
MatchingDigest == published => digestMatches
====
