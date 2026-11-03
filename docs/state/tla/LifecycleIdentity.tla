---- MODULE LifecycleIdentity ----
EXTENDS Naturals
VARIABLES current, receipt, savedAction, savedExpected, changed, changeExpected, changeCurrent, retried, retryAction, retryExpected
vars == <<current, receipt, savedAction, savedExpected, changed, changeExpected, changeCurrent, retried, retryAction, retryExpected>>
Init == /\ current = 1 /\ receipt = FALSE /\ savedAction = 1 /\ savedExpected = 1 /\ changed = FALSE /\ changeExpected = 1 /\ changeCurrent = 1 /\ retried = FALSE /\ retryAction = 1 /\ retryExpected = 1

Takeover == /\ current = 1 /\ ~receipt /\ current' = 2 /\ UNCHANGED <<receipt, savedAction, savedExpected, changed, changeExpected, changeCurrent, retried, retryAction, retryExpected>>
Change == \E expected \in 1..2, action \in 1..2:
          /\ ~receipt /\ expected = current
          /\ receipt' = TRUE /\ savedAction' = action /\ savedExpected' = expected
          /\ changed' = TRUE /\ changeExpected' = expected /\ changeCurrent' = current
          /\ UNCHANGED <<current, retried, retryAction, retryExpected>>
Retry == \E expected \in 1..2, action \in 1..2:
         /\ receipt /\ expected = savedExpected /\ action = savedAction
         /\ retried' = TRUE /\ retryAction' = action /\ retryExpected' = expected
         /\ UNCHANGED <<current, receipt, savedAction, savedExpected, changed, changeExpected, changeCurrent>>

Next == Takeover \/ Change \/ Retry
Spec == Init /\ [][Next]_vars
CurrentLifetime == changed => changeExpected = changeCurrent
ExactReceiptRequest == retried => (retryAction = savedAction /\ retryExpected = savedExpected)
====
