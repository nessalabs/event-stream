# Scenarios

Add one focused scenario per behavior. Start with the LLD acceptance areas:

- identity and ordering
- idempotency and retries
- replay-to-live races
- overload and slow consumers
- incremental decoder boundaries
- ownership, shutdown, and recovery
- projection equivalence between replay and live delivery

Keep scenarios deterministic and small enough that a reviewer can understand
the input and output from one screen.

