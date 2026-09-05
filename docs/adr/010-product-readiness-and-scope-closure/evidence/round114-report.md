# Round 114: documented operational commands

Executed the three commands in docs/operations.md using their documented
feature selections, rather than enabling every feature. Each durable example
received a fresh directory and retained its files for inspection. Commands,
outputs and directory paths are in [raw results](round114-operational.json).

All commands exited successfully:

- Memory live delivery and replay agree at offset 2; a failed application
  transition leaves the applied cursor unchanged.
- Local SQLite preserves 16 exact records through capacity rejection, shutdown,
  backup, restart and controlled restore. Exact retry deduplicates. The old
  cursor is rejected, and the restored stream appends at 17.
- Replication preserves four exact records through backlog rejection, restart,
  catch-up and retry.

The core CI matrix now invokes both durable command-line walkthroughs with fresh
runner-temporary paths. This complements the existing tests and catches command
or feature-selection regressions. Workflow YAML was checked locally; a remote
CI execution has not been observed.

These runs validate local procedures on this machine. They do not qualify
online backup, power loss, another filesystem/device, or automatic failover.
No production code or performance measurement changed. Source hashes and raw
results are retained. Home round 114 is marked not measured.
