# ADR 0001 completion audit

Status: **in progress; not verified**. Completion means the first durable foundation, including the resource and storage evidence gates in ADRs 0002 and 0003. Lifecycle changes, snapshots, retention and replication retain their separate ADR 0004+ scope.

**Current priority:** [stability handoff](../../stability-handoff.md). Round 111
freezes optimization and removes unverified runtime batching. The checkpoints
below are historical evidence, not a current release-completion claim.


The indexed subscriber-membership and borrowed SQLite row-decoding changes have
measured evidence. Historical checkpoints below retain their original scope;
their test counts and source identities are not current-build claims.

## Historical independently checked checkpoints

Round 98 passes [317 library/integration tests plus two operational examples](../008-local-first-replication/evidence/round98-all-tests.log),
strict Clippy, formatting and Rust 1.85 all-target checks. Scoped replication
now shares the origin runtime's ownership and shutdown boundary. Seven new
tests exercise cancellation, admission, panic recovery, bootstrap and SQLite
restart. The shared full-recovery callback and all 19 verification tests pass.
[The report](../008-local-first-replication/evidence/round98-report.md) records
evidence and remaining resource qualification. No new performance samples or
native rendering evidence were collected.

Round 97 passes [310 library/integration tests plus two operational examples](../010-product-readiness-and-scope-closure/evidence/round97-all-tests.log),
[strict all-feature/all-target Clippy](../010-product-readiness-and-scope-closure/evidence/round97-clippy.log),
formatting and Rust 1.85 all-target checks. The [round report](../010-product-readiness-and-scope-closure/evidence/round97-report.md)
records the aggregate SQLite staging quota fix, migration/restore compatibility,
fault coverage, and runnable local/replicated operational recovery examples.
Home round 97 has no new resource measurements. Full mixed workloads, numerical
release budgets and current native rendering remain unverified.

Round 91 passes [302 all-feature library/integration tests](../007-source-journal-and-parser-checkpoints/evidence/round91-all-tests.log) and [strict all-feature/all-target Clippy](../007-source-journal-and-parser-checkpoints/evidence/round91-clippy-final.log). Durable EOF now has Memory/SQLite ports, runtime/ingestion, exact retries, migration/restore, injected rollback/ack loss, and four post-commit process-kill schedules. Resource gates and wider product integration remain open.


Round 86's [all-feature library and integration suite](../008-local-first-replication/evidence/round86-all-tests.log) passes **290 tests**, and [strict all-feature all-target Clippy](../008-local-first-replication/evidence/round86-clippy.log) passes. Verified-but-unpublished snapshots now have bounded digest validation before restore retires their state. SQLite chunk quota checks read a durable accounting row instead of scanning all chunks on each upload. Tests cover exact retries, physical capacity after abort, bounded cleanup, reopen, restore, and legacy accounting migration. These changes have no new performance measurements.


Round 85 passes [58 focused restore and cleanup tests](../008-local-first-replication/evidence/round85-validated.log) and [strict all-feature library Clippy](../008-local-first-replication/evidence/round85-clippy.log). These include mapped completed bootstrap receipts, nonempty replication restore kills, and partial-upload corruption. Source validation now rejects gaps and byte-counter mismatches before restore aborts unfinished uploads. Valid partial uploads still restore; old destination requests are rejected. Aborted receipt semantics, verified-but-unpublished content, broader resource qualification, and the full mixed workload remain open. No fresh performance samples were collected.

### Previous full-suite checkpoint

Round 78's [current all-feature library/integration suite](../008-local-first-replication/evidence/round78-all-tests.log) passes **273 tests**. [Strict all-feature all-target Clippy](../008-local-first-replication/evidence/round78-all-target-clippy.log) passes after test initializer cleanup. Import is implemented; qualification remains for mapped completed bootstrap receipts, malformed origin operation/counter metadata, restore kills with nonempty replication state, incomplete/aborted cleanup-state corruption, and scale resources.


Round 67's [all-feature library/integration run](../008-local-first-replication/evidence/round67-all-tests.log) passes **262 tests**. Both-SQLite driver bootstrap, completion retry after cleanup/reopen, and injected lost publication reply are exercised. Remaining cleanup/import and crash/resource gates are not qualified by this run.


Round 61's [all-feature library and integration run](../008-local-first-replication/evidence/round61-all-tests.log) passed **258 tests**. It includes the implemented Memory replication/bootstrap/driver paths, 60 SQLite tests, seven SQLite fault tests, and 36 restore tests. The five driver regressions include bounded retries, cancellation ownership and deadline/graceful shutdown. SQLite full bootstrap/import and roadmap resource qualification remain incomplete. This is a correctness checkpoint for the executed source, not release completion.

## Earlier checked checkpoints

Round 47’s [all-feature library and integration suite](../007-source-journal-and-parser-checkpoints/evidence/round47-integrated-tests.log)
passed **211 tests** for the features present then, before replication ports
were exported. It includes 48 SQLite integration tests, seven SQLite fault
tests, 34 restore tests and the source-snapshot locking test. These counts apply
to that compiled test set. A new [missing-output corruption test](../007-source-journal-and-parser-checkpoints/evidence/round47-missing-output.log)
was added afterward and failed at startup because its fixture did not preserve
retention counters. The [corrected live-corruption fixture](../007-source-journal-and-parser-checkpoints/evidence/round47-missing-output-corrected.log)
then passed independently: retry returns corruption and the missing row remains
absent. Both attempts are retained.

Round 49’s [five focused SQLite journal checks](../007-source-journal-and-parser-checkpoints/evidence/round49-sqlite-journal-settled.log)
pass with replication excluded while it is implemented. These include actual
parser close/reopen recovery and injected output-plus-marker transaction
rollback/lost-acknowledgement reconciliation. Close/reopen is not process-kill
coverage; capture/checkpoint crash boundaries remain open.

Round 50’s [journal restore accounting regression](../007-source-journal-and-parser-checkpoints/evidence/round50-journal-restore-charge.log)
rejects a forged zero receipt charge even when the aggregate counter was altered
to match. No destination is published. Other malformed journal shapes and
resource qualification remain separate gates.

The [parser recovery callback](../007-source-journal-and-parser-checkpoints/evidence/round47-parser-callback.json)
passes four assertions through the production Memory runtime and ingestion
service. It reconciles generation-one output, appends new generation-two output,
and publishes checkpoint byte 8/item 2/output 2. This constructs the interruption
boundary through public APIs; it is not a SQLite process-kill test.
The [retention callback](../007-source-journal-and-parser-checkpoints/evidence/round39-retention-callback.json)
and [custom binary decoder example](../009-optional-domain-adapters/evidence/round44-custom-decoder-final.log)
also have scoped passing evidence.

| Area | Inspected evidence | Still required |
| --- | --- | --- |
| Runtime, Memory and SQLite correctness | Round 91 combined run: 302 tests | Complete journal crash/failure schedules and later features |
| SQLite journal restore | Raw bytes survive receipt cleanup; output and markers map into restored history and deduplicate | Remaining malformed-row, checkpoint, low-byte and lifecycle combinations |
| Snapshot resources | Historical round 25 had one failed p99 gate. [Round 93](../005-snapshots-and-consumer-recovery/evidence/round93-report.md) passes 48 correctness runs and all 12 unchanged foreground gates | Establish repeatability/causality, memory budgets, independent arrivals and full mixed workloads |
| Restore resources | [Stream-count matrix](../004-stream-lifecycle-and-safe-restore/restore-resource-evidence.md) and [deep-history/VFS matrix](../004-stream-lifecycle-and-safe-restore/restore-vfs-resource-evidence.md) | Mixed workloads including new journal and replication paths |
| Native verification | Round 68 headless registry and saved SQLite receipt-loss callback pass; Home retains 115 experiments | Current native rendering verification; last successful capture showed 13 rounds/93 experiments |
| Compatibility | [Round 80 Rust 1.85 all-feature/all-target check](../008-local-first-replication/evidence/round80-msrv.log); round 78 strict all-target Clippy | Remaining feature combinations, final formatting and Clippy |

ADR 0008 implements typed replication boundaries, Memory and SQLite bootstrap,
reader expiry, bounded cleanup, and controlled restore with new identities.
Round 83 verifies the current restore suite plus strict origin/destination
corruption guards. Completed receipts can survive legitimate payload cleanup;
active pending batches must still match retained history. Remaining qualification
includes completed bootstrap receipt mapping, interrupted restore with nonempty
replication state, additional incomplete-upload corruption cases, and resource
measurements. The separate journal crash matrix covers three input/output/checkpoint
boundaries through the downstream replication path. It is not a restore-kill
matrix or a power-loss guarantee.
ADR 0009 has explicit terminal/provider proposals and runnable generic extension
examples. No emulator or provider companion is selected. ADR 0010’s full mixed
end-to-end workload and operational qualification remain unfinished. Passing the
current suite does not close these product requirements.

## Historical foundation checks

The orchestrator ran the latest subscription ownership changes with `cargo test --all-features --all-targets` on Rust 1.98.1 and `cargo +1.85.0 test --locked --all-features --all-targets` on Rust 1.85.0. Both passed **86 tests**: 5 private invariant tests, 3 memory, 27 runtime, 6 cursor, 16 decoder, 24 SQLite, 4 storage-fault and 1 projection example test. All example targets compiled. The feature-off suite passed 41 tests. Formatting and strict all-feature, all-target Clippy passed. These results establish correctness coverage; current-source performance qualification remains separate.

The separate GPUI crate passed **12 tests** on the latest core ownership changes. Its release executable passed all eight production scenario callbacks and both harness callbacks. All ten saved reports share fingerprint `fnv1a64:b65a960a6b57441e`. Tests also cover rejection of a mismatched displayed fixture, isolated report storage and concurrent report saves. Each callback uses the same registered runner as the UI. The native window was inspected with its workstream sidebar, selected scenario, status and Run control visible. This establishes native rendering and headless execution; it does not claim automated clicks through every control.

The new `tests/sqlite_faults.rs` suite independently passed **4 tests**. Its test-only VFS forwards calls to the bundled SQLite implementation. One-shot write and sync failures fire at actual VFS callbacks. Persistent main-database write faults fire repeatedly. Callback counts alone do not identify which failure occurred during rollback restoration; that exact stage is not independently proven. Close/reopen checks recover exact baseline and retry records, their cursors and gapless bounds. A separate test explicitly covers the SQLite page quota. The fourth test fills a private 64 MiB APFS image until the operating system returns ENOSPC, then reopens the database and checks prior history and retry recovery. The current stable and MSRV runs passed all four. Their timings are test execution times, not performance evidence. Both current combined runs include the tightened distinction between definitive failure and unknown commit.

The domain compiles independently using the standard library. Custom-store and custom-decoder examples have run. The projection example now receives valid events after subscription, reconnects from its applied cursor, compares live state against replay, and verifies an invalid application transition leaves both state and checkpoint unchanged.

## Correctness coverage

This maps inspected sources to LLD sections 3, 14 and 21. Passing one row does not substitute for the remaining rows.

| Area | Evidence inspected | Remaining proof |
| --- | --- | --- |
| Identity/order | Shared adapters check concurrent create, distinct concurrent appends, contiguous offsets and wrong incarnation; runtime checks accepted order across worker turns | Covered: stream-scoped IDs, memory overflow without mutation, SQLite signed-boundary and full-u64 cases |
| Idempotency | Fresh concurrent same-ID appends produce one insert and 31 exact retry receipts; concurrent schema conflicts are rejected; SQLite tests restart and lost acknowledgement | Passed in both current combined suites |
| Transactionality | Mutation hooks, child-process kills and VFS failures check atomic recovered state | Definitive-error assertions passed; stage-specific rollback failure remains open |
| Reads | Exclusive pages, fixed upper bounds, empty ranges, ahead cursors, page byte limits, internal holes and corrupt metadata | Floor-start, unpolled sweeper diagnostics and guarded SQL materialization independently verified |
| Subscriptions | Replay/follow, future-only start, registration gate, cancelled registration I/O and stale-tail-before-sleep race | Grace and release regressions pass; direct cancellation of subscription next remains open |
| Overload/lifecycle | Count/byte/waiter limits, tracked cancelled I/O, cancelled shutdown, faulted drain, idle subscriber expiry, final-handle drop and fair stream turns | Sustained measured read/write fairness and remaining resource matrix |
| Parsing | Exhaustive small-fixture partitions, byte preservation, CRLF/EOF policies, malicious decoder, cancellation and capacity bounds | Larger generated partitions pass; format-specific nesting belongs to custom decoders, not the built-in newline framer |
| Ownership/durability | Real SQLite child processes, alias rejection, close/drop ownership, process kills before/after commit and isolated APFS ENOSPC | macOS/APFS process restart is exercised; Linux and power loss are not qualified |
| Projection | Actual live delivery versus replay; failed apply does not advance application state/checkpoint | Current all-target suite passes the projection scenario |
| Compatibility | Golden/full-u64 cursor fixtures, malformed token generation, unknown schema round-trip and foreign/newer format refusal | Initial-format limits and refusal are documented and tested; no migration API is claimed |

The nonzero replay floor is now preserved in both polled and background-expired subscriber errors. The regression waits for the background sweep to release the subscription before reading its terminal error, and passed independently.

Earlier runtime findings are fixed and have regression tests: cancelled reads retain page capacity and I/O ownership; cancelled metadata and registration remain owned; read panics fault the runtime; draining rejects new reads; unresolved commits never resume queued writes; shutdown owns its close even if its caller disappears; one worker owns an entire per-stream scheduling turn.

## Performance evidence and provenance

A complete prior source checkpoint has an archived source manifest, executable and raw measurements. Its manifest begins `bd105171`, and its executable hash begins `84572e2f`. The orchestrator independently checked the archive against its manifest and executable digest. The scale matrix contains all 72 unique expected samples and matching resource rows. Caller counts balance, accepted work commits, and shutdown reports no unresolved work.

That checkpoint also completed baseline, extended, live fan-out, million-record and larger-than-RAM workloads. The live runs delivered exactly 340,992 records across 18 samples. The large database occupied 27,950,964,736 bytes on a host with 25,769,803,776 bytes of RAM. RSS excludes the kernel filesystem cache. These results do not establish cold-cache performance.

The latest subscription allocation and terminal-quota fixes change the production source identity. Prior results remain useful evidence for their archived source; they must not be relabeled as measurements of the new executable. Current-source warmed-subscription measurements and relevant regression workloads are being prepared. A warmed subscription has consumed a real page before memory is sampled.

See the [performance report](performance-evidence.md) for artifacts and workload-specific budgets. Budgets chosen after observing the matrix are calibrated regression thresholds, not acceptance limits chosen before the experiment. Repetitions in one process can retain allocator pages from earlier repetitions. Requested-live Rust bytes and process RSS answer different questions. SQLite C allocations, exact copied bytes and physical flush counts remain unavailable where the harness does not measure them.

## Allocation-guard finding

The identified allocation guards are fixed. SQLite record reads use byte lengths, enforce UTF-8 storage encoding, and guard output values with SQL CASE expressions before returning them. Tests cover multibyte and NUL-containing identifiers, oversized offsets/payloads, and malformed scalar types. The orchestrator inspected these guards and independently ran all 24 SQLite tests successfully. The indexed replay query still uses its declared index. The final benchmark build follows these changes and must validate its pre-run source manifest and binary identity.

## Subscriber performance regression

The longer live-delivery benchmark with 512 events, 100 subscribers and 16-record pages hit the 30-second lag guard at offset 80 of 512. This is a failed workload, not a reason to substitute a smaller passing fixture. Root review identified a possible contention loop in read admission: a waiting task acquires a semaphore permit, drops it, and retries acquisition. The issue was confirmed and fixed by retaining acquired permits and keeping queue position across wakes. Root independently reran the original 16-record-page workload: all 51,200 deliveries completed in approximately 0.409 seconds with memory and 2.486 seconds with SQLite, without failures or rejections. These are regression checks, not final repeated release measurements. A controlled many-reader test also checks actual adapter entries and completion.

## Decoder drive budget finding

The unbounded decoder drive has been fixed. `DecodeDriveBudget` independently limits total steps, emitted items, declared output bytes, and reported work per push or finish. Input buffering no longer determines allowed output expansion. Root independently passed all 16 decoder tests, including infinite-output rejection, valid expansion, buffered EOF, pending-event retry, terminal permit release, and a full 64 KiB chunk producing 65,536 empty newline frames.

The memory adapter now walks a tree range instead of looking up every record separately. Root passed its two private invariant tests and three integration tests, including missing middle and suffix records. The runtime's 27 tests also passed at this checkpoint. Performance measurements must follow the new build; no speedup is inferred from these correctness checks.

## Remaining release gates

- Complete the [scale review](scale-review.md). Existing stream-creation benchmarks do not establish 10,000 or 100,000 concurrent producers or subscriptions. Measure idle metadata, cleanup delay, overload recovery, and memory growth separately.
- The stage-specific rollback-truncate fault now passes its focused test. Include it in the full combined validation after the configuration migration. This proves the classified injected path, not every possible rollback failure.
- Complete the correctness gaps in the table without changing the original guarantees.
- Capture build identity before the final performance runs. Meet scoped numeric budgets with repeated results and explicit measurement limits.
- Finish the resource ownership/capacity audit, including allocations, retained memory and release paths.
- UI headless checks and all ten foundation callbacks passed at the recorded prior source checkpoint. Home was subsequently inspected with tables and charts. Refresh production callback fingerprints after the new source freeze.
- Stable/MSRV, feature-off, formatting and Clippy pass on current production. Run targeted new tests and final document-link checks after the remaining test/evidence edits settle.
- Name supported platforms from actual evidence. Linux CI is configured but no remote execution is claimed. Power loss is outside the current process-restart profile.

Operational behavior is documented in [running and recovering a local store](operations.md). The [release checklist](release-checks.md) records CI and publishing boundaries. The package has not been published. No missing measurement or unexecuted test counts as a pass.

## Drained and terminal subscription ownership

Consuming the last buffered record now releases the empty page allocation as well as its capacity permit. The next page replaces the buffer, so retaining those slots did not provide reuse. A private regression checks both the allocation capacity and available permits.

An empty store page for a known nonempty range now ends the subscription with one corruption error. A terminal subscription releases its admission slot even if the application retains the handle. The registration flag changes once, and executor-owned cleanup updates global accounting. Cancelling the caller cannot interrupt that bookkeeping between the two steps. A regression retains the terminal handle and successfully registers another subscription under a one-subscription limit.

## Current warmed-subscription smoke check

The orchestrator inspected the first current-source warmed sample and verified all 15 build inputs, the combined manifest and the executable hash. The input digest begins `c2a36ab8`; the executable digest begins `76995dfd`. One thousand subscribers on one stream each consumed 256 exact records before a ten-second idle interval. All 1,000 subscriptions remained active at the settled point, and shutdown closed with no unresolved work or registered subscriptions.

Requested-live Rust bytes changed from 760,921 before the drain to 761,113 afterward. Process RSS increased from 4,521,984 to 4,734,976 bytes. This single smoke sample supports the buffer-release regression and shows why requested-live allocations and RSS must remain separate. It is not the repeated scale qualification and does not establish a measured saving against an old executable.

## Home and latest scale evidence

Home now reads the bounded, typed `verification/evidence/home.json` catalog. The native window was inspected with the experiment table, separate runtime/RSS charts and recorded round status. Six focused tests cover the checked-in catalog, matching configurations, nulls, duplicate artifacts, zero baselines, ambiguous epochs and file-size limits. The orchestrator reran these tests and `just check-build` successfully. The complete verification suite passed 18 tests. The accepted UI executable SHA-256 is `73bb59580c38f18e0624ebeca80edde96f1e1feb1bbbef8e4076195029b6b815`.

The current catalog contains 46 experiment summaries. Each records source-file hash, sample lines, configuration identity and measurement phase. Old samples are not reassigned to a new work round. Missing measurements remain null. Charts only compare equal workload and measurement keys.

The completed warmed matrix covers 1,000, 10,000 and 100,000 subscribers on one stream for both stores. All repeated samples retained their named population and shut down with zero unresolved work. At 100,000 subscribers, median registration was 53.44 seconds for memory and 74.18 seconds for SQLite. Sequential drain delivered 25.6 million records per repetition. The requested-live change across drain was 192 bytes for memory and 1,668 bytes for SQLite in every repetition. These are live requested Rust bytes, not RSS or SQLite C allocations. Brief unrelated host activity was observed during the SQLite batch; timings are not isolated-host qualification.

The orchestrator independently checked the 100k sample coverage and counters, the source archive's 15 input hashes, and both instrumented/control executable hashes. That archived input manifest is `21b53c226641721ed8a1d1a95e056e57ea3eb90e9bd9ff3993ddddda1bb422d6`. The registration cost motivates the next optimization; no improvement from that new change is claimed yet.

## Current configuration and lifecycle work

`RuntimeConfig` now groups event policy, append admission, reads/pages, subscriptions, scheduling and diagnostics. The migration preserves the prior defaults. Stable subscription slots remove full membership scans from registration and removal. Focused tests pass; combined checks and new scale measurements are still pending at this checkpoint. Historical benchmark results above refer to their recorded source epochs.

The SQLite rollback test recognizes the journal-header read sequence and original database size before failing the actual rollback truncate callback. Reopen preserves the committed prefix and excludes the attempted pre-commit event. The fault is injected in the forwarding VFS against bundled SQLite.

The user has expanded implementation scope to the remaining roadmap. ADR 0004 now has a [typed lifecycle contract](../004-stream-lifecycle-and-safe-restore/contracts.md). Lifecycle, restore, snapshots, retention, parser recovery and replication are not completed by these foundation checks. ADR 0009 still requires explicit selection of optional domain integrations; no terminal engine has been selected.

## Stable-slot checkpoint verified

The orchestrator independently ran all-feature/all-target tests: 94 passed,
zero failed. Rust 1.85 all-feature/all-target checking also passed. The core
implementer separately passed strict Clippy and all 18 verification tests.
Those results precede the subsequent lifecycle implementation edits.

The `d85c4020` measurement archive and its 16 inputs/executable were independently
verified. Both new warmed artifacts passed sample coverage, population,
shutdown and failure-counter checks: 12 samples at 1k/10k and six at 100k.
Registration and idle CPU improved substantially; SQLite sequential replay was
slower and variable. The [scale review](scale-review.md#stable-membership-measurement)
records both outcomes. This is not full-product qualification.

## Lifecycle implementation checkpoint

Reset/delete, bounded retired-history cleanup, typed maintenance limits, owned
cancellation, uncertain-result reconciliation and subscriber termination are
implemented through `LifecycleStore`. Memory and SQLite share the lifecycle
contract tests. SQLite format 2 retains name identity separately from histories
and uses transactional lifecycle receipts and quota counters. The format-1
migration leaves the event-record table in place.

The orchestrator passed the 116-test integrated checkpoint. Two subsequent
SQLite validation fixes were checked with all 35 SQLite integration tests.
The final-source Rust 1.85 all-feature/all-target check passed. All 19 verification
tests and all 12 release-binary callbacks passed, including reset/stale cursor
and bounded cleanup. Their compiled fingerprint is `fnv1a64:5030decf8a720547`.
The native Home was inspected with 64 experiments and both improving and
regressing matched charts. Its release executable SHA-256 is
`09d3d5f3f4f6b015cea18fcaf0d9ce41b47a62962035574e6c0435a8eb07f594`.

One earlier verification run reported a SQLite accounting error at scenario
startup and an isolated rerun passed. The harness had a directory-reuse weakness:
timestamp-only names with `create_dir_all`. It now uses bounded atomic directory
creation with process ID and a monotonic counter. No observed collision was
established as that failure's cause. The settled-source verification suite and
all release callbacks subsequently passed. Preserve this distinction rather
than claiming a diagnosed database fault was repaired by changing a filename.

This checkpoint does not close ADR 0004: controlled restore and lifecycle resource
measurements remain required. ADRs 0005–0010 are still unfinished. The warmed
performance results belong to the earlier archived build, not this lifecycle
implementation.


## MiniSQLite diagnostic and restore checkpoint

The isolated [MiniSQLite comparison](../002-resource-budgets-and-performance-evidence/minisqlite-comparison.md) completed 15 single-run cases. The orchestrator verified every result count, the result and source archive hashes, and all twelve files in the scoped measured-path archive. Home round 6 contains these measurements with separate comparison keys; the catalog now contains 79 experiments. All seven Home evidence tests passed with the synchronized verification lockfile. These timings include setup inside the selected read path and must not be compared with the earlier warmed-drain phase.

The generic restore manager passed its focused tests. The orchestrator independently passed the first SQLite restore round-trip test after the staging-state fix. Publication interruption tests, format compatibility review and resource qualification remain open. This checkpoint does not qualify controlled restore or complete ADR 0004.


## Borrowed SQLite decoder and integrated restore checkpoint

The [paired decoder experiment](../002-resource-budgets-and-performance-evidence/sqlite-decode-borrowed-ab.md) passed twelve exact 100,000-consumer cases. The orchestrator verified both complete 19-file source manifests and archive hashes, confirmed that only the SQLite adapter differed, reproduced the reported medians, and checked the four matched Home points. Home now contains 83 experiments across eight local experiment rounds.

Removing temporary row allocations reduced store allocation calls by 44.1% and cumulative allocated bytes by 33.4%. Median elapsed time fell by 2.8% for the store and 4.8% for the complete runtime lifecycle. Three repetitions do not establish a universal speed gain. Peak RSS showed no material reduction.

The settled all-feature/all-target suite passed 130 tests. The focused restore suite passed eight tests, including publication error boundaries, source-independent retry, legacy format-2 compatibility and malformed history/mapping checks. An earlier run used the outdated expectation that a mapping read would return publication uncertainty; the stricter ownership boundary correctly refused the hard-linked destination first. The test now checks refusal and unchanged persisted state. The rerun passed. Rust 1.85 all-feature/all-target checking also passed. These are injected-error and reopen tests, not proof of actual process-kill recovery at each restore boundary. Restore resource and crash qualification remain open.


The final frozen checkpoint passed 132 core tests, 19 verification tests, and Rust 1.85 checking. Restore now has ten test functions, including a subprocess retry check. That child returns from the injected restore error before it is killed, so ownership has already unwound. This proves retry across processes for the persisted publication states; it does not prove abrupt termination inside an active restore. Genuine in-operation termination, storage-full, cleanup failures and large-resource measurements remain explicit gates.

## Active restore interruption and ownership checkpoint

The next integrated checkpoint passed 145 tests with all features and targets.
Rust 1.85 checking also passed with the same feature and target scope. These
results include 21 restore tests and a separate source-lock test. They supersede
the limited interruption evidence above; they do not reassign old performance
samples to the new implementation.

The process-kill test now pauses inside the actual restore worker, while it
still owns the operation and filesystem locks. It kills a child at five
bootstrap/publication boundaries. Each retry checks fresh identities and exact
record contents after reopening. The source remains available for the two
unfinished bootstrap cases. The three complete-publication cases recover after
the source is removed.

Restore rejects unbound empty staging files, dangling reservation links, and
staging symlink/hard-link substitutions. Cleanup uses the same ownership rules.
Injected cleanup failures report their exact paths. A real isolated APFS image
exercises storage exhaustion, an unpublished destination, explicit cleanup,
retry, and all 32 original 512 KiB payloads under fresh identities. The source
hash remains unchanged. A separate test holds the source read transaction and
checks that another library owner is rejected and a raw SQLite writer cannot
commit its update.

The repeated restore resource matrix is pending. Snapshot contracts now specify
store-owned quotas, bounded verification steps, exact pagination and expiring
recovery protection, but snapshot implementation has not started. ADRs
0005–0010 and the remaining resource/corruption gates are still required.

## Round 51 snapshot timeline diagnostic

[Six fresh release runs](../005-snapshots-and-consumer-recovery/evidence/round51-timeline-report.md) passed exact-record correctness. Verification append p99 was 3.19–3.45 ms; controls were 1.11–1.19 ms. The earlier 7.46 ms failure did not recur. New source and timeline instrumentation make this a separate diagnostic, not an improvement claim. The round 25 failure remains open. Home records two new three-sample diagnostic cells with distinct comparison settings.

Root independently passed both Memory replication batch contracts and the checked-in Home catalog validation. These checks cover the first origin/destination batch slice and evidence loading. They do not qualify bootstrap, durable replication, or native rendering.

## Round 52 independent recovery checks and replication regressions

Root passed [seven SQLite journal checks](../007-source-journal-and-parser-checkpoints/evidence/round52-journal.log) and [all 35 scoped SQLite restore checks](../007-source-journal-and-parser-checkpoints/evidence/round52-restore.log). The journal scope includes capture/checkpoint acknowledgement loss followed by reopen and actual process termination after output commit, with exact 96 KiB retry content. Capture/checkpoint process termination and the remaining journal gates are still open.

Two new independent replication regressions [initially failed](../008-local-first-replication/evidence/round52-replication-regressions-initial.log). A destination floor admitted an extra receipt beyond its configured count. A closed origin returned an exact attach receipt through its closed handle. Production fixes are assigned. The review also identified unrelated-replica scans per append, full remaining-history scans per acknowledgement, stale oldest-backlog timestamps and receipt-held payload accounting. ADR 0008 remains incomplete. No fresh performance samples were taken.

The two defects were then fixed. Root independently [reran both regressions and both Memory batch contracts](../008-local-first-replication/evidence/round52-replication-regressions-final.log): four passed. The Home catalog check also passed. Append now uses the affected stream’s attachment index, and acknowledgement subtracts the exact pending batch. These code changes are not fresh performance measurements; broader replication integration and resource gates remain open.

## Round 53 replication age and retention boundaries

Root added an injected-clock sequence that commits A at 100 ms and B at 200 ms. Acknowledging A at 300 ms must leave B's timestamp at 200 ms. The test also verifies retention protection before and after acknowledgement, unchanged counters on exact retry, age-based write rejection without tail advancement, and explicit detach releasing the protection. A second regression verifies attaching existing history preserves its original commit age.

[All four regressions and both Memory batch contracts passed](../008-local-first-replication/evidence/round53-replication-final.log). The [Rust 1.85 scoped replication check passed](../008-local-first-replication/evidence/round53-msrv.log) with four warnings from staged bootstrap work. This checks the Memory replication feature combination, not the unfinished SQLite replication integration or full end-to-end qualification. Home records round 53 without new performance samples.

The first SQLite batch implementation then reached a coherent checkpoint. Root independently [passed both SQLite origin/destination shared contracts](../008-local-first-replication/evidence/round53-sqlite-replication.log). This establishes the bounded base batch behavior only; restart, injected faults, corruption, quotas, controlled restore and bootstrap remain open. The Home catalog validation also passed.

## Round 54 exact SQLite replication reconciliation

Root added five tests using isolated real SQLite stores. The [initial run](../008-local-first-replication/evidence/round54-sqlite-regressions.log) exposed acknowledgement beyond the prepared extent and retries that enlarged a pending batch after a later append. After production fixes, [all five tests passed](../008-local-first-replication/evidence/round54-sqlite-settled.log). The scope also covers changed attach-request rejection, original pending identity/result after reopen, and missing replication metadata failing as corruption instead of generating a new origin.

The [first Memory destination bootstrap test passed](../008-local-first-replication/evidence/round54-memory-bootstrap-final.log). It stages one opaque snapshot chunk and one suffix record, verifies, publishes, and reads both back. It does not establish multi-step limits, replacement/read leases, cancellation, crash recovery, or full origin-to-destination bootstrap. Those remain open. No fresh performance measurements were made.

## Round 55 bounded bootstrap and empty suffix

Root added an independent three-chunk/three-record bootstrap test with one chunk and one record permitted per verification call. It verifies forward progress, hidden intermediate state, exact publication retry, cross-chunk byte reads and exact suffix cursors. [That scenario passed](../008-local-first-replication/evidence/round55-bootstrap.log).

A [second regression failed](../008-local-first-replication/evidence/round55-bootstrap-expanded.log): a snapshot-only bootstrap covering cursor 20 reports a tail of zero because the suffix map is empty. The required state is an empty complete page after 20, followed by accepting the next batch at 21. The same boundary must govern receipt-floor validation. Production correction is assigned; broader origin/SQLite bootstrap remains unfinished. No performance samples were taken.

After correction, root [passed eight Memory replication tests](../008-local-first-replication/evidence/round55-bootstrap-final.log): both bootstrap regressions, four age/resource/lifecycle regressions, and both base batch contracts. Snapshot-only recovery now reads at cursor 20 and accepts batch 21. Full origin bootstrap, published-read leases, receipt expiry, SQLite bootstrap and resource qualification remain open.

## Round 56 real origin-to-destination bootstrap transfer

Root [passed a two-Memory-store bootstrap integration and both destination bootstrap regressions](../008-local-first-replication/evidence/round56-origin-bootstrap-final.log). The integration reads actual stored snapshot pages and suffix records, obtains a real destination publication receipt, rejects a forged bootstrap identity, and acknowledges the matching receipt. An event committed during transfer remains in the backlog and is subsequently transferred and acknowledged through the normal batch path. Retention protection is checked before the test acquires a separate snapshot reader lease.

The byte-page regression now acquires and releases an explicit published replica read lease. This does not yet prove replacement/expiry behavior. Origin bootstrap still requires its own finite snapshot/suffix protection rather than relying on a caller's temporary recovery lease. SQLite bootstrap and the full resource/fault qualification remain incomplete. No fresh performance measurements were made.

## Round 57 published replica read leases

Root verified old readers retain original bytes across snapshot replacement, exact-deadline expiry rejects further reads, and release is idempotent. The [initial abandoned-reader cleanup test failed](../008-local-first-replication/evidence/round57-read-leases.log): an expired lease still occupied the only reader slot. An ordered expiry index now supports bounded reclamation. [All five Memory lease/bootstrap checks passed](../008-local-first-replication/evidence/round57-read-leases-final.log), and [a strengthened assertion passed](../008-local-first-replication/evidence/round57-cleanup-count.log) proving cleanup itself reports removal of the expired slot before a new acquisition.

Root also [passed four SQLite replication checks](../008-local-first-replication/evidence/round57-sqlite-durability.log): both base contracts, a lost destination commit acknowledgement followed by reopen/exact retry, and exact backlog/oldest-age updates. These do not establish full process-kill, corruption, restore, bootstrap or resource qualification. Home records round 57 without new performance measurements.

## Round 58 replacement capacity and aborted upload cleanup

Root added two resource regressions and [reproduced both failures](../008-local-first-replication/evidence/round58-cleanup-second.log). A third 8-byte publication cannot fit under a 16-byte budget even after replacing the first snapshot, releasing its reader, and cleaning. An aborted 12 KiB upload in three 4 KiB chunks makes no cleanup progress when each call permits one row and 8 KiB. Existing lease expiry/replacement checks pass. Production corrections are assigned for retired-content cleanup, bounded chunk removal and truthful accounting. Content cleanup must preserve or explicitly expire operation identities. No new performance samples were taken.

A broad root SQLite integration attempt [stopped during compilation](../008-local-first-replication/evidence/round58-sqlite-integration-build-in-progress.log) while the Memory cleanup refactor was in progress. No tests executed in that attempt, so it does not independently verify the agent-reported SQLite checkpoint. Cleanup remains under implementation.

## Round 59 cleanup fixes and integrated SQLite checkpoint

Root [passed all four lease/cleanup regressions](../008-local-first-replication/evidence/round59-cleanup.log). Replaced snapshot capacity is reusable and aborted uploads clean in bounded chunks. Root also [passed 99 SQLite integration tests](../008-local-first-replication/evidence/round59-sqlite-integrated.log): 58 SQLite tests, five independent replication regressions, and 36 restore tests. Restore currently rejects nonempty replication state explicitly; this prevents silent loss but does not complete replication restore.

The new [origin protection expiry regression failed](../008-local-first-replication/evidence/round59-origin-expiry.log): a first matching acknowledgement at the exclusive deadline becomes Required. The test uses an injected snapshot clock and a fabricated matching receipt specifically to isolate rejection behavior. The separate real two-store transfer still passes. A production expiry check is assigned. No performance samples were taken.

The origin expiry correction then landed. Root [passed both origin bootstrap tests with an explicit typed expiry assertion](../008-local-first-replication/evidence/round59-origin-expiry-typed.log). A first acknowledgement at the protection deadline returns `BootstrapProtectionExpired`; the real transfer path still succeeds. Full application coordinator, durable bootstrap/import and phase resource qualification remain open.

## Round 60 application replication driver

Root added and [passed three driver regressions](../008-local-first-replication/evidence/round60-driver.log). The transport commits through an actual Memory destination. Injected lost replies reconcile the exact stored record; invalid remote receipts leave origin progress unchanged. A blocked transport retains driver admission after its caller is cancelled, then completes origin acknowledgement when released. Closing denies new work. This verifies the bounded single-batch coordinator, not bootstrap transport, automatic retries, shutdown draining or network implementation.

Root also [passed SQLite generated-history replication](../008-local-first-replication/evidence/round60-generated-replication.log): both retry generations enter the batch, retention is blocked before acknowledgement, and reopened progress/counters match. Full durable replication import/bootstrap and resource qualification remain unfinished. No fresh performance measurements were taken.

## Round 61 driver shutdown ordering and bounded retry

Root corrected the race between the driver's final closed check and active-work registration. Registration now precedes that check with sequentially ordered atomics so graceful shutdown cannot return while an admitted task is about to start. [All five driver regressions pass](../008-local-first-replication/evidence/round61-driver.log), including new automatic retry and deadline/graceful-drain scenarios. The timeout test observes retained transport ownership until exact acknowledgement completes. [Strict replication library Clippy passes](../008-local-first-replication/evidence/round61-clippy.log). These scenarios do not establish every concurrent schedule or full bootstrap fault recovery. No performance measurements were taken.

Round 61 then completed the [full all-feature library and integration suite](../008-local-first-replication/evidence/round61-all-tests.log): 258 passed, zero failed. This includes the SQLite fault suite and source-locking restore test. It does not replace the failed snapshot latency gate or qualify unimplemented durable bootstrap/import.

Round 62 checked the native verification project headlessly: [all 19 tests pass](../008-local-first-replication/evidence/round62-verification-final.log). Its scenario check now uses the live UI registry, so newly registered callbacks are included automatically and unavailable callbacks must remain blocked. [Rust 1.85 all-feature library compilation](../008-local-first-replication/evidence/round62-msrv.log) also passes. The build contains warnings from incomplete SQLite bootstrap implementation. Compilation does not qualify those handlers. No new performance samples or native visual inspection were performed.

Round 63 exercises full application-driver bootstrap with multiple snapshot pages and an exact completion retry. It found and fixed two retry failures: completed origin protection was treated as expired, and the driver attempted to resend suffix data after destination publication. Completed Memory begin receipts now preserve completion; stale attempts cannot detach another active attempt. The driver skips transfer for verified or published destinations. [Eight focused bootstrap/driver tests pass](../008-local-first-replication/evidence/round63-bootstrap.log), including preserving a new local backlog record during retry. [Strict replication library Clippy passes](../008-local-first-replication/evidence/round63-clippy.log). SQLite parity and performance qualification remain open.

Round 64 verifies completed driver retry after retention physically removes both old source records. It also checks that retrying an expired begin leaves a replacement attempt and its protection deadline unchanged. A forged top-level origin stream in a bootstrap acknowledgement was accepted; Memory now rejects it before advancing progress. [The initial failing receipt test](../008-local-first-replication/evidence/round64-receipt-initial.log) is retained. [All 12 focused regressions pass](../008-local-first-replication/evidence/round64-bootstrap.log), as does [strict replication library Clippy](../008-local-first-replication/evidence/round64-clippy.log). SQLite parity and final resource qualification remain open.

Round 65 adds an independent application-driver test using a real Memory origin and real SQLite destination. It transfers multiple snapshot pages and a suffix, retries after the source deletes old history, preserves new origin backlog, and verifies published bytes and suffix cursor/payload after close/reopen. [The integration test passes](../008-local-first-replication/evidence/round65-sqlite-bootstrap-retry.log). This proves the exercised destination durability path, not crash-atomic publication, full SQLite origin recovery, abort/cleanup/import, or resource qualification. The initial compile log is retained as in-progress implementation evidence.

Round 66 adds a both-SQLite bootstrap driver test with retention and reopen of both databases. It first found an [origin receipt INSERT column mismatch](../008-local-first-replication/evidence/round66-sqlite-bootstrap-initial.log). After that fix, it finds [bootstrap backlog underflow](../008-local-first-replication/evidence/round66-sqlite-bootstrap.log): status currently hides counters for Bootstrapping mode. Append accounting and reopen validation also require that mode. The SQLite owner is correcting these paths. This new test is failing and does not establish durable origin completion. The earlier Memory-origin/SQLite-destination test continues to pass.

Round 67 resolves the both-SQLite driver failures. The test now advances and expires retry generation before asserting physical payload removal; retaining payloads while retry identities are valid is required by the SQLite storage model. [Both bootstrap driver tests pass](../008-local-first-replication/evidence/round67-sqlite-bootstrap.log), including a deliberately lost reply after actual destination publication and retry reconciliation. The full current suite passes 262 tests. This is not process-kill or power-loss evidence. No new performance measurement was made.

Round 68 registers “Remote receipt lost” in the verification UI. It calls the production driver and a real SQLite destination with an injected lost reply after commit, then closes/reopens the destination and retries the exact batch. Evidence shows the origin at cursor 0 while uncertain, cursor 1 after reconciliation, and one exact stored payload. [All 19 verification tests pass](../008-local-first-replication/evidence/round68-verification.log). The [headless scenario command also passes and saves evidence](../008-local-first-replication/evidence/round68-replica-retry.log). Native rendering was not visually inspected this round.

Round 69 adds [two passing independent SQLite cleanup tests](../008-local-first-replication/evidence/round69-cleanup.log). A 12 KiB aborted upload is reopened and reclaimed with one-row/8 KiB cleanup calls, retaining its exact abort receipt and releasing enough quota for another upload. A published replacement preserves an active reader of the old version; release and bounded cleanup allow a third 8-byte publication under a 16-byte snapshot quota. These establish exercised capacity and cleanup behavior, not latency, CPU, RSS, or process-kill guarantees. Controlled import and full qualification remain open.

Round 70 adds an abandoned SQLite reader test using an injected clock and a one-lease capacity. At the exact expiry deadline, bounded cleanup frees the slot and a new read succeeds. [All three independent SQLite cleanup tests pass](../008-local-first-replication/evidence/round70-cleanup.log). Source review also found full reader scans in acquisition and cleanup. Ordered expiry indexing is assigned to the SQLite owner; current passing tests do not qualify that path at scale. No fresh performance samples were collected.

Round 71 adds a [passing mixed SQLite integration test](../008-local-first-replication/evidence/round71-mixed.log). It captures raw newline input, commits one output without a checkpoint, closes/reopens, resumes parsing, and verifies the exact checkpoint. Those outputs feed snapshot-plus-suffix replication. The test acknowledges and cleans source journal history before expiring retry identities and deleting retained output, then reopens both stores and retries the completed bootstrap. Journal protection correctly rejected the initial attempt to expire still-required retry identities. This exercises a graceful stop at the actual commit gap, not a process kill or power loss. It does not close import or resource qualification.

Round 72 replaces the graceful first stop in the mixed integration test with a real child-process kill. The child signals only after raw capture and the first output have committed, with no parser checkpoint. The parent uses a bounded wait and kill/reap guard; Unix checks SIGKILL explicitly. [The mixed recovery test passes](../008-local-first-replication/evidence/round72-kill.log), including downstream replication and retention. This establishes that specific crash boundary, not arbitrary crash schedules or power-loss behavior.

Round 73 expands the mixed integration into three child-process kill schedules: after raw capture, after the first output before checkpoint, and after a full checkpoint. Before recovery it asserts 0/1/2 durable output records and the expected checkpoint presence. [All three schedules pass within the mixed test](../008-local-first-replication/evidence/round73-crash-matrix.log), including downstream replication, cleanup, retention and restart. These discrete schedules do not establish arbitrary instruction-level crash or power-loss behavior.

Round 74 independently passes [published destination restore and epoch rotation](../008-local-first-replication/evidence/round74-restore.log), but finds a blocking correctness defect in corrupt-backup handling. The new test replaces published snapshot chunks with same-length zero bytes; restore still returns success and publishes a destination. [The failing reproducer is retained](../008-local-first-replication/evidence/round74-corruption.log). Bounded digest and contiguous-content validation before publication is assigned to the SQLite owner. Successful normal roundtrip is insufficient to qualify restore.

Round 75 independently passes [four corrupt-backup rejection cases](../008-local-first-replication/evidence/round75-corruption.log): changed snapshot bytes, a missing chunk, a shifted chunk offset, and a missing published suffix record. [Normal published restore also passes](../008-local-first-replication/evidence/round75-valid-restore.log). The validator currently collects all published descriptors into one vector; bounded iteration is assigned to its owner. These passing correctness checks do not qualify validation memory growth.

Round 76 finds a restore regression in a valid archive: after publishing a replacement and cleaning the old snapshot bytes, validation still treats the retired descriptor as active content and rejects its digest/length. [Three cleanup tests pass and this new restore test fails](../008-local-first-replication/evidence/round76-retired.log). Validation must distinguish current published content from retained retired receipts. Separately, source inspection confirms published descriptor validation now fetches one row at a time by key rather than collecting the full set. The correctness fix remains assigned to the SQLite owner.

Round 77 independently passes [nine focused restore/cleanup checks](../008-local-first-replication/evidence/round77-restore-guards.log). Current-pointer validation accepts the valid cleaned-retired archive while retaining all four corrupt-current-content rejections. A new origin test imports two actual records and a pending replication batch: payloads survive, origin identity changes, the replica requires bootstrap, pending progress is cleared, and the old origin request is rejected. This is broader than empty-origin import, but does not close active bootstrap/receipt/lifetime rewrite or full failure/resource qualification.

Round 79 adds three origin metadata corruption fixtures. Restore currently accepts an attachment naming a foreign origin, an operation receipt naming a foreign origin, and a zero backlog count despite two pending records. [The valid case passes and all three rejection tests fail](../008-local-first-replication/evidence/round79-origin-corruption.log). Identity rewriting and progress reset hide corrupt source state; bounded read-only semantic validation must run before transformation. The SQLite owner is implementing this guard. Import is not qualified by the prior happy-path tests.

Round 80 adds malformed pending-batch bounds and count cases. Restore accepts a through cursor beyond the source tail and a result count inconsistent with the prepared batch; [the valid case passes and all five origin rejection cases fail](../008-local-first-replication/evidence/round80-origin.log). These are included in the pre-normalization audit work. [Rust 1.85 all-feature/all-target compilation passes](../008-local-first-replication/evidence/round80-msrv.log); compatibility does not establish import correctness.

Round 81 independently passes [ten origin/destination restore guards](../008-local-first-replication/evidence/round81-source-audit.log) after read-only source validation was added before normalization. Rejection assertions now require CorruptBackup, so unrelated failures cannot satisfy them. Review identified another required valid-history case: completed prepare receipts may remain after source payload cleanup. Exact content checks must not mistake those historical receipts for active pending batches. That regression is next; broad restore qualification remains open.

Round 82 adds a valid completed-receipt archive: two records are actually replicated and acknowledged, then retry expiry and retention remove their source payloads. Restore incorrectly compares the completed prepare receipt with deleted history and rejects the backup. [Six prior origin cases pass and this new valid case fails](../008-local-first-replication/evidence/round82-historical.log). Active pending batches need exact retained-history validation; historical completed receipts require a different check. The distinction is assigned to the SQLite owner.

Round 83 independently passes [48 restore checks](../008-local-first-replication/evidence/round83-restore.log): seven origin, four destination corruption, and 37 restore integration tests. Active pending receipts are checked against retained history; completed historical receipts retain internal range/count/limit validation without demanding deleted payloads. The current-status table is refreshed. Remaining qualification is named above; this checkpoint does not establish complete product readiness.

Round 84 adds three process-kill schedules for restore with nonempty pending replication state: after staging completion, destination linking, and publication marking. The same operation recovers and verifies two exact payloads, a new origin identity, detached replication state, and stale-origin rejection. [All nine origin restore test entries pass](../008-local-first-replication/evidence/round84-restore-kills.log), including the child harness entry. The parent bounds waiting and reaps its child; Unix requires SIGKILL. These schedules do not establish power-loss behavior or cover every internal commit.

Round 85 found [two invalid partial uploads were accepted](../008-local-first-replication/evidence/round85-partial-initial.log): a chunk gap and a false accepted-byte counter. The positive control initially expected a resumable staging state; restore correctly aborts unfinished work from the old destination epoch, so the control now checks that behavior. Validation must inspect the source before that state change. The added scan reads fixed-size identities and chunk lengths, without loading payloads or collecting all uploads. [Focused validation passes 58 tests](../008-local-first-replication/evidence/round85-validated.log); [source hashes](../008-local-first-replication/evidence/round85-source-hashes.json) identify the restore change and fixtures. This does not qualify every bootstrap state or establish resource improvements.

Round 86 adds a valid verified-but-unpublished snapshot control and a [failing changed-content reproducer](../008-local-first-replication/evidence/round86-initial.log). The fix hashes one chunk at a time under the restore page-byte bound. Partial uploads still avoid reading payload bytes. The full suite includes three new SQLite chunk-accounting tests. Counter updates share the chunk insertion/deletion transaction; aborted uploads consume quota until physical cleanup. Startup audits and legacy migration still scan stored chunks once. Remaining repeated scans include staging-bootstrap admission, staged-record admission, and publication quota calculation. [Source hashes](../008-local-first-replication/evidence/round86-source-hashes.json) identify this checkpoint. No new latency, CPU, or memory claim is made.

Round 87 enables the End-to-end recovery callback using a fixture shared with the SQLite process-kill test. The chain now drains a post-bootstrap event after restart and verifies cursor 3/backlog 0. [Three kill schedules pass](../010-product-readiness-and-scope-closure/evidence/round87-shared.log); [19 verification tests pass](../010-product-readiness-and-scope-closure/evidence/round87-verification.log); the [saved callback](../010-product-readiness-and-scope-closure/evidence/round87-full-recovery.json) passes five observations. Console stops use close/reopen. Source fingerprints now include the shared fixture and split SQLite/snapshot modules that were missing from the prior source list. No native visual or performance qualification is claimed.

Round 88 starts the missing durable EOF path with a [typed contract](../007-source-journal-and-parser-checkpoints/end-of-input.md) and Memory implementation. [Eleven journal checks pass](../007-source-journal-and-parser-checkpoints/evidence/round88-journal.log). Source sealing is distinct from parser completion. SQLite, runtime/decoder finalization and associated crash/resource evidence remain open; the prior combined recovery callback only proves recovery of captured bytes.

Round 89 adds bounded Runtime finalization operations and successful EOF checkpoint support in NewlineFramer. [Thirty-four source-journal tests pass](../007-source-journal-and-parser-checkpoints/evidence/round89-final.log), including shutdown rejection and zero-output repeat finish after checkpoint restore. This closes the decoder's inability to persist successful terminal state, but does not yet complete the ingestion finalization driver or SQLite EOF crash qualification. No resource samples were collected.

Round 90 implements the bounded ingestion finish path over SourceFinalizationStore. [Thirty-five source-journal tests pass](../007-source-journal-and-parser-checkpoints/evidence/round90-tests.log), including final-frame mapping failure/retry and exact completed replay. `complete_capture` remains distinct from parser EOF completion. SQLite, crash schedules, broader limits and native EOF qualification remain open; no new performance claim is made.

Round 91 adds independent SQLite EOF driver and process-kill tests. Empty input and one-output bounded calls complete across reopen. Four durable EOF boundaries recover exact history; repeated completion emits nothing. Full library/integration coverage now passes 302 tests. This is not power-loss or resource qualification, and the native combined callback has not yet been updated to exercise EOF.

Round 92 updates the shared full-recovery callback to seal and finish input and verify durable EOF after cleanup, retention, replication and SQLite reopen. [Six observed checks pass](../010-product-readiness-and-scope-closure/evidence/round92-full-recovery.json), together with the three process-kill schedules and 19 verification tests. No current native rendering or performance claim is made.

Round 93 runs a fresh archived release snapshot matrix: all 48 correctness processes and all 12 unchanged foreground gates pass. [The report](../005-snapshots-and-consumer-recovery/evidence/round93-report.md) gives scope, per-cell medians, raw provenance and remaining limits. Home now has 129 experiments including 14 new measured cells. The historical failed run is retained; this result does not establish the cause of that outlier or qualify 100,000 active agents. [All 19 verification tests](../005-snapshots-and-consumer-recovery/evidence/round93-home-check.log) pass after the data update.

Round 94 adds [18 archived simultaneous-request runs](../002-resource-budgets-and-performance-evidence/evidence/round94-report.md) at 1k/10k/100k tasks on both stores. All outcomes are accounted for, no failed calls occur, queues remain within 1024 and shutdown resolves work. Large bursts reject most requests; this does not qualify 100k successful active producers. Ready-task Rust allocation delta is about156MB at100k on both adapters; a large per-task harness Outcome is being reduced separately. Home now retains135 experiments including6 fresh scale cells. [All19 verification tests pass](../002-resource-budgets-and-performance-evidence/evidence/round94-home.log). Sustained/mixed workloads and explicit release budgets remain open.

Round 95 measures the compact caller/harness change against its archived baseline in 12 fresh paired processes. [The comparison](../002-resource-budgets-and-performance-evidence/evidence/round95-report.md) shows waiting-task Rust allocations falling by115.2MB at100k tasks; process RSS falls about110MiB on both stores. Production library source is identical. All 100k latency samples and accepted/rejected outcome counts are preserved; most requests still reject at the fixed1024queue, so this is not successful100k-producer capacity. The size guard, strict example Clippy and19 verification tests pass. Home now has139 experiments including4new paired cells.

Round 96 adds [12 scheduled-arrival diagnostic runs](../002-resource-budgets-and-performance-evidence/evidence/round96-report.md) using the archived round 95 binary. Both stores accept all 4,000 requests at a requested 800/s. At 16,000/s, Memory accepts all 16,384 requests; SQLite accepts a median 4,063 and rejects the rest. Every outcome and latency sample is accounted for, queues remain bounded, and shutdown resolves work. Receipt latency and scheduling lateness are reported separately. These short four-stream runs do not establish steady state, delivery performance, or 100k active-agent capacity. Home now contains 143 experiments across 96 rounds; all 19 verification tests pass. No production code changed.

Round 101 replaces planned snapshot-interruption and retention/read console
checks with shared real-SQLite fixtures. [The report](../010-product-readiness-and-scope-closure/evidence/round101-report.md)
links two passing integration tests and fresh headless callback evidence.
Snapshot interruption uses close/reopen; replay ordering uses injected port
gates around actual reads. Neither is labeled as power-loss or resource
qualification. No fresh performance samples were collected.

Round 102 bounds sustained-generator task retention independently of duration.
[The comparison](../002-resource-budgets-and-performance-evidence/evidence/round102-report.md)
retains twelve fresh runs with identical production code and all offers accepted.
Harness peak RSS falls, while measured receipt-p99 medians rise. It is not a
library speedup or a large-population qualification. Three benchmark tests,
strict Clippy, Rust 1.85 and all 19 verification tests pass.

Round 103 extends bounded scheduled arrivals to six one-minute processes.
[All 48,000 offers per run are accepted](../002-resource-budgets-and-performance-evidence/evidence/round103-report.md),
with resolved shutdown and bounded task occupancy. Memory receipt p99 stays
below 0.1 ms; SQLite ranges from 3.12 to 87.17 ms. The outlier is retained.
These four-stream results do not qualify a rotating 100k-agent population.

Round 104 implements rotating `population_sustained` with exact receipt-ledger
replay and per-cycle conservation. [Six fresh diagnostic samples](../002-resource-budgets-and-performance-evidence/evidence/round104-report.md)
cover1k/10k/100k streams on both stores. All offers succeed across two cycles
at 800 aggregate events/s; every stream is verified and shutdown resolves.
100k-stream RSS is187.27MiB Memory and24.14MiB SQLite; receipt p99 is0.111ms
and3.495ms respectively. These are single sparse-load samples, not repeated
qualification or arbitrary100k-agent throughput. A bounded-cleanup timestamp
scan remains a concrete follow-up.

Round 105 fixes the timestamp-index scan identified during population review.
[The report](../002-resource-budgets-and-performance-evidence/evidence/round105-report.md)
records 325 passing library/integration/operational tests and ten isolated
before/after samples. Median wall time for 256 one-row Memory cleanup calls
against 100k records falls from 119.449 ms to 0.099 ms, with identical surviving
history and measured allocation bytes. This is cleanup-specific evidence.
All 19 verification tests pass; broader release qualification remains open.

Round 106 adds [six repeated 10k-stream higher-rate diagnostics](../002-resource-budgets-and-performance-evidence/evidence/round106-report.md).
Memory accepts all 20k offers each run. SQLite reaches the 256-task generator
limit and loses offers before runtime submission; coverage is partial. Exact
accepted-history replay and shutdown pass on both stores. This profile is not
qualified for SQLite. Home retains both results, including rejections. Bounded
group commit evaluation and the wider release matrix remain open.

Round 107 establishes the [typed append-batch contract](../002-resource-budgets-and-performance-evidence/append-batch-contract.md)
and sequential fallback. Input order, bounds, per-item error preservation and
malformed response checks now have executable contracts. Runtime batching and
SQLite grouped transactions remain unimplemented; no speedup is claimed. Home
marks this round not measured.

Round 108 extracts SQLite append staging from transaction ownership. Every
staging failure now explicitly rolls back; retry rollback failure faults the
store. Unknown commits require autocommit mode and exact staged-record identity
before reconciliation succeeds. New real-transaction tests cover staged retry,
conflict, visibility, outer rollback, and a post-mutation replication failure.
See the [contract](../002-resource-budgets-and-performance-evidence/append-batch-contract.md).
Grouped transactions and runtime batching remain open; Home is not measured.

Round 109 implements bounded SQLite grouped appends behind the store port.
The aggregate request charge is checked before cloning and stays within the
existing per-command record-byte limit. Savepoints preserve valid peers across
known rejections; outer rollback and exact group reconciliation protect durable
outcomes. New tests include same-group/preexisting retries, replica backlog,
cancellation/close, and actual VFS fault schedules. Shared sync work is observed
but runtime batching, group process-kill tests and the performance matrix remain
open. Home records no new latency/memory measurements.

Round 110 adds [real grouped-append process-kill schedules](../002-resource-budgets-and-performance-evidence/evidence/round110-report.md).
Before-commit recovery keeps neither new record; after-commit recovery keeps
both exactly. The baseline survives both, and whole-group retry inserts or
deduplicates as appropriate without duplicate records. All-feature and SQLite-only
runs pass. Runtime batching and resource qualification remain open.

Round 111 freezes optimization per user direction and restores the tested
individual-append runtime/config from hash-verified source. The uncompiled batch
prototype is outside active source. All 352 all-feature/all-target tests pass.
See the [stability handoff](../../stability-handoff.md) for good-enough scope,
release blockers and explicit deferrals.

Round 112 runs one archived frozen-runtime SQLite stability screen. All 100k
offers succeed across 10k streams/ten cycles at 800 aggregate offers/s. Exact
replay and resolved shutdown pass; resource observations stay within predeclared
diagnostic ceilings. See the [report](../002-resource-budgets-and-performance-evidence/evidence/round112-report.md).
This does not close long-running mixed stability or release qualification.
