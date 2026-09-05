# 0006. Remove history only through explicit recovery and retry policies

- **Date:** 2026-09-04
- **Status:** accepted; implementation in progress
- **Prerequisite:** [0005](../005-snapshots-and-consumer-recovery/adr.md)
- **Sequence:** [ADR roadmap](../README.md)

## Context

Disk use cannot grow forever, but deleting old records changes replay and retry guarantees. A stale reader must receive a clear error rather than an apparently complete suffix.

## Decision

Add opt-in prefix retention. Keep the meaning of `resume_floor`: all records after it are replayable through the tail. Publish floor changes consistently with logical removal. A concurrent read either gets its complete requested page or an explicit history error.

Separate replay retention from retry-ID lifetime. Choose and document a retry horizon before release of this feature. Preserve enough equality/receipt information for valid retries inside it. Specify the behavior outside it explicitly; do not turn an old retry into a new event silently.

The [retry-policy design constraint](retry-policy.md) explains why deleting an
arbitrary ID is insufficient and proposes explicit retry generations for the
bounded policy. The [implementation contract draft](contracts.md) makes the
generation, floor, cleanup and recovery-protection rules concrete. It is not an
implementation or a completion claim.

Coordinate retention with published snapshots, active recovery protection, and later replication acknowledgements. If protection prevents reclamation beyond the configured quota, stop growth or require an explicit recovery-policy change. Do not keep unlimited pinned history.

Compaction reclaims space in bounded steps. It must not renumber retained records or silently change payloads. Full database rewrites, if needed, run as explicit maintenance with stated temporary-space requirements.

## Ordered work

1. Define policy inputs, effective floor, recovery dependencies, and exact expired-retry errors.
2. Make floor changes and deletion progress recoverable. Keep cleanup separate from the short logical transition.
3. Bound rows/bytes/time per cleanup turn. Measure foreground latency during retention and database checkpoint/reclamation work.
4. Test readers whose page crosses a deletion boundary, abandoned protection, disk-full cleanup, and retry equality after old payload removal.
5. Publish a storage-growth example showing live records, snapshots, retry metadata, and temporary files separately.

## Completion evidence

- `after < resume_floor` fails without silently skipping required records.
- Retained history has no unexplained holes; cursors never change.
- Recovery and retry promises remain valid under the chosen policies and crash schedules.
- Cleanup progresses within its budget, survives restart, and cannot grow staging space without a limit.
- Space reclamation and its real filesystem effect are measured; deleting rows is not claimed to immediately shrink the file.

## Consequences

Retention needs more state than append-only storage. Keep each policy explicit and inspectable. Snapshotting, retry receipts, and replicas may retain bytes longer than the visible event history; report those costs.
