# Replication from an owned runtime

Status: implemented; focused ownership and recovery tests pass. Resource
qualification remains pending.

Applications must be able to ingest and replay events while replicating the
same store. `Runtime` owns that store exclusively. Exposing its raw adapter
would let a caller close it while appends or subscriptions still use it.
The runtime will instead expose bounded, scoped replication operations.

## Public operations

The existing `ReplicationDriver` remains available for applications that own
adapters directly. Runtime users get these additional methods under the
`replication` feature:

```text
Runtime<S: ReplicationOriginStore>
  origin_identity() -> OriginId
  attach_replica(AttachReplica) -> AttachReplicaReceipt
  replica_status(&ReplicaId, &OriginStream) -> ReplicaStatus
  detach_replica(DetachReplica) -> DetachReplicaReceipt
  replicate_once(Arc<T: ReplicaTransport>, PrepareReplicaBatch,
                 acknowledgement_operation_id) -> ReplicationDriveReceipt

Runtime<S: ReplicationStore>
  cleanup_replication(ReplicaCleanupLimits) -> ReplicaCleanupProgress
  bootstrap_replica_once(Arc<T: ReplicaTransport + ReplicaBootstrapTransport>, BeginOriginBootstrap,
                         publication_operation_id, acknowledgement_operation_id,
                         ReplicationBootstrapDriveLimits)
      -> ReplicationBootstrapDriveReceipt
```

These methods return `ReplicationResult`. They use the existing operation
identities and receipts. They do not invent a second replication algorithm or
allow a destination to write into its origin's history.

## Admit bounded work before spawning it

`RuntimeConfig.replication` uses `RuntimeReplicationConfig`: a maximum number of
concurrent operations and a maximum total number of in-flight bytes. Defaults
are four operations and 8 MiB. Both limits must be nonzero and representable by
the semaphore implementation.
The field names are `max_concurrent` and `max_in_flight_bytes`. Standalone
`ReplicationDriverConfig` defaults remain unchanged.

Admission is immediate. A call that cannot reserve its operation slot or bytes
returns `Overloaded`; there is no internal queue of replication waiters. A
request whose maximum charge cannot fit the configured total returns
`CapacityExceeded`. Invalid limits fail before I/O.

The charge includes bounded page buffers, retained record references, request
identifiers, fallback errors and task metadata. Reuse the driver's charge
calculation and add the runtime wrapper's retained state. Do not count only
payload bytes. Neither caller-facing wrapper nor helper may allocate a page
before reserving its maximum size.

This is a runtime work budget, not a whole-process memory limit. Retained store
history and SQLite caches have separate limits. A custom transport must also
bound its serialization buffers and any work it starts internally. Its method
must not report completion while untracked I/O still uses the request.

## Keep ownership through cancellation and shutdown

Once admitted, a task belongs to the runtime. Dropping the caller future drops
only its result receiver. The task keeps its permits and increments the same
active-I/O counter used by runtime shutdown.

```text
reserve slot + bytes -> verify runtime is ready -> register active I/O
                                                      |
                                             run production driver
                                                      |
                                      close driver and await its work
                                                      |
                                  release permits + unregister active I/O
```

Each scoped transfer uses the existing driver with one concurrent operation.
The runtime's shared limits bound the combined work of all scoped drivers.
The task must wait for the driver to finish before it releases active I/O,
including an error or unknown-result path. An outer timeout must not close the
origin while a driver's detached task still uses it.

A shutdown deadline bounds how long the caller waits. It does not authorize
abandoning a transaction or transport operation. If work is still running,
shutdown reports that the store has not closed. New operations fail with
`Closed`. The transport implementation owns its network timeouts; the runtime
does not claim it can forcibly stop arbitrary user I/O.

Read failures remain read failures. Stopped attach/detach and transfer tasks
report the existing request-bearing unknown-result error. Applications can
retry that same operation identity instead of guessing whether it committed.
Cleanup has no such receipt type; a stopped cleanup task reports `StorageFailure`.
Cleanup is a bounded repeatable maintenance operation, not an exact receipt retry.

## Verification required

- Real Memory and SQLite scenarios append through `Runtime`, replicate through
  its scoped API, and compare exact destination records after restart.
- A transport controlled by a test gate holds one admitted transfer while
  another transfer is rejected at the configured concurrency limit.
- A separate byte-limit case rejects before any transport call.
- Cancelling an admitted caller cannot release its capacity or let runtime
  shutdown close the store before the transport gate opens.
- Closing a runtime rejects identity/status and mutating replication calls.
- Snapshot bootstrap through the runtime preserves exact suffix and retry
  behavior without exposing the store.
- Feature-disabled builds add no replication permits or tasks.

The mixed workload depends on this boundary. The tests in
[`runtime_replication.rs`](../../../tests/runtime_replication.rs) exercise
runtime-owned append/transfer, cancellation with held transport, byte admission,
panic and exact retry, invalid configuration, shutdown rejection, snapshot
bootstrap, and SQLite reopen. The operational replicated example and shared
journal/replication recovery fixture also use this boundary. These correctness
checks do not establish resource qualification.

The standalone bootstrap driver now counts separately retained request identifiers,
shallow request/result structures, and bounded page buffers before starting I/O.
The runtime adds its own wrapper charge. Checked arithmetic rejects unsupported
sizes. Maximum-identifier tests verify that a previously undercharged request is
rejected before transport work, while an adequately budgeted transfer and its
exact retry still succeed. Defaults are unchanged. This conservative accounting
is not a measurement of allocator or process memory; resource qualification
still needs measured retained-state evidence.
