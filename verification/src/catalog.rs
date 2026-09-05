use crate::runner::{CheckResult, RunContext};
use serde_json::{json, Value};

pub type ScenarioFn = fn(&RunContext) -> CheckResult;

pub struct Scenario {
    pub id: &'static str,
    pub title: &'static str,
    pub purpose: &'static str,
    pub target: &'static str,
    pub evidence_scope: &'static str,
    pub source: &'static str,
    pub input: Value,
    pub expected: &'static str,
    pub run: Option<ScenarioFn>,
    pub blocked_reason: &'static str,
}

pub struct Workstream {
    pub number: usize,
    pub title: &'static str,
    pub folder: &'static str,
    pub scenarios: Vec<Scenario>,
}

impl Workstream {
    pub fn document(&self) -> String {
        format!("docs/adr/{}/adr.md", self.folder)
    }
}

fn planned(
    id: &'static str,
    title: &'static str,
    purpose: &'static str,
    target: &'static str,
    input: Value,
    expected: &'static str,
) -> Scenario {
    Scenario { id, title, purpose, target, evidence_scope: "Production contract; no implementation executed until registered.", source: "", input, expected, run: None,
        blocked_reason: "The production implementation and its scenario adapter are not built yet. This case will call that implementation directly when registered; no simulated pass is available." }
}

fn ready(
    id: &'static str,
    title: &'static str,
    purpose: &'static str,
    target: &'static str,
    input: Value,
    expected: &'static str,
    run: ScenarioFn,
) -> Scenario {
    ready_with_scope(
        id,
        title,
        purpose,
        target,
        input,
        expected,
        ReadyExecution {
            run,
            evidence_scope:
                "Production scenario using the public event-stream API with bounded deterministic input.",
        },
    )
}

struct ReadyExecution {
    run: ScenarioFn,
    evidence_scope: &'static str,
}

fn ready_with_scope(
    id: &'static str,
    title: &'static str,
    purpose: &'static str,
    target: &'static str,
    input: Value,
    expected: &'static str,
    execution: ReadyExecution,
) -> Scenario {
    Scenario {
        id,
        title,
        purpose,
        target,
        evidence_scope: execution.evidence_scope,
        source: "verification/src/scenarios.rs",
        input,
        expected,
        run: Some(execution.run),
        blocked_reason: "",
    }
}

pub fn workstreams() -> Vec<Workstream> {
    vec![
        Workstream { number: 1, title: "Event stream foundation", folder: "001-event-stream", scenarios: vec![
            Scenario { id: "roadmap-audit", title: "Roadmap & contract files", purpose: "Inspect the actual ADR files and verify that every workstream has its decision and completion contract.", target: "runner::audit_roadmap", source: "verification/src/runner.rs",
                evidence_scope: "Harness / documentation check. Does not verify the event-stream core.", input: json!({"root": "workspace", "directory": "docs/adr", "workstreams": 10, "max_file_bytes": 262144}), expected: "All 10 ADR files exist, use the matching number, and contain their required decision and completion sections.", run: Some(crate::runner::audit_roadmap), blocked_reason: "" },
            ready("record-order", "Committed record order", "Show the records returned by concurrent producers.", "Runtime<MemoryStore>::append", json!({"stream": "task-42", "producers": 8, "events_per_producer": 16}), "128 distinct committed records with contiguous cursors.", crate::scenarios::record_order),
            ready("retry-identity", "Retry & conflict", "Compare an identical retry with a changed payload under the same ID.", "MemoryStore::append_atomic", json!({"event_id": "output-7", "payloads": ["hello", "hello", "goodbye"]}), "Inserted, then deduplicated at the original cursor, then idempotency_conflict. Tail advances once.", crate::scenarios::retry_identity),
        ]},
        Workstream { number: 2, title: "Resources & performance", folder: "002-resource-budgets-and-performance-evidence", scenarios: vec![
            Scenario { id: "catalog-integrity", title: "Scenario wiring audit", purpose: "Check the live scenario registry used by this window and the headless runner.", target: "runner::audit_catalog → catalog::validate", source: "verification/src/catalog.rs",
                evidence_scope: "Harness / registry check. Does not verify the event-stream core.", input: json!({"registry": "catalog::workstreams", "require_unique_ids": true, "require_explicit_availability": true}), expected: "Every phase has scenarios with unique IDs, input, expected behavior, and either a real callable target or a blocking reason.", run: Some(crate::runner::audit_catalog), blocked_reason: "" },
            ready_with_scope("bounded-ingestion", "Queue & memory limits", "Inspect accepted and rejected work at capacity.", "Runtime<GateStore>::append / try_append", json!({"queue_records": 1, "queue_bytes": 32768, "payload_bytes": 1024, "offered_records": 128}), "One owned append completes, excess offers are explicitly overloaded, and all reservations are released.", ReadyExecution { run: crate::scenarios::bounded_ingestion, evidence_scope: "Production Runtime admission with a deterministic local GateStore failure adapter backed by MemoryStore. The gate pauses storage so queue capacity can be observed; it is not durability evidence." }),
            ready("replay-fairness", "Replay beside writes", "Run bounded replay pages beside foreground appends.", "Runtime<MemoryStore>::read_after + append", json!({"history_records": 64, "page_records": 8, "foreground_writes": 4}), "Replay and writes both progress. Report their wall times separately; CPU and memory remain unavailable.", crate::scenarios::replay_fairness),
        ]},
        Workstream { number: 3, title: "SQLite durability", folder: "003-sqlite-durability-and-storage-layout", scenarios: vec![
            ready("sqlite-reopen", "Commit → close → reopen", "Read back actual saved bytes and retry receipts after reopening SQLite.", "SqliteStore::append_atomic / close / open", json!({"database": "isolated scenario directory", "events": 100, "reopen": true}), "Every acknowledged record and its original cursor survive reopen.", crate::scenarios::sqlite_reopen),
            ready("sqlite-ownership", "Second owner rejected", "Attempt two opens against one isolated store.", "SqliteStore::open ownership guard", json!({"owners": 2, "same_store": true}), "Only the first owner succeeds. The second receives store_in_use, and ownership releases after close.", crate::scenarios::sqlite_ownership),
            ready("sqlite-unknown", "Lost commit acknowledgement", "Inject acknowledgement loss after a real SQLite commit, then retry the original ID.", "SqliteStore failure hook + commit recovery", json!({"fault": "after_commit_before_receipt", "retry_same_id": true}), "Recovery finds exactly one committed record with the original identity.", crate::scenarios::sqlite_unknown),
        ]},
        Workstream { number: 4, title: "Lifecycle & restore", folder: "004-stream-lifecycle-and-safe-restore", scenarios: vec![
            ready("reset-incarnation", "Reset & stale cursor", "Compare old and new stream identities after an explicit reset.", "Runtime<MemoryStore>::change_lifecycle", json!({"stream": "task-42", "old_records": 3, "operation_id": "reset-task-42"}), "New incarnation starts at zero. Old handles and cursors are rejected.", crate::scenarios::reset_incarnation),
            ready("bounded-lifecycle-cleanup", "Bounded retired cleanup", "Remove retired records through explicit bounded maintenance turns.", "Runtime<MemoryStore>::cleanup_retired", json!({"stream": "cleanup-task", "retired_records": 5, "records_per_turn": 2}), "Each turn removes at most two contiguous records. The active replacement remains writable.", crate::scenarios::bounded_lifecycle_cleanup),
            ready_with_scope("restore-boundary", "Restore older history", "Publish an offline SQLite backup under fresh stream identities and verify retry and restart behavior.", "SqliteRestoreManager + SqliteRestoreBackend", json!({"backup_records": 3, "mapping_page_records": 1, "retry_same_operation": true}), "The restore maps the old lifetime to a fresh incarnation, preserves exact history, deduplicates the restore operation, rejects the old identity, and remains writable after reopen.", ReadyExecution { run: crate::scenarios::restore_boundary, evidence_scope: "Production SQLite restore manager and backend with an isolated real database. This covers a successful logical import, publication, mapping page, identical retry, and reopen; fault-injection coverage remains in the SQLite restore tests." }),
        ]},
        Workstream { number: 5, title: "Snapshots & recovery", folder: "005-snapshots-and-consumer-recovery", scenarios: vec![
            ready_with_scope("snapshot-replay", "Snapshot + remaining events", "Compare restored state with complete replay.", "Runtime<MemoryStore> snapshot publication and protected recovery", json!({"covered_cursor": 2, "tail": 3, "schema": "example.application-state.v1"}), "Application-owned transcript and workflow state through cursor 2 plus protected offset 3 equals a complete replay.", ReadyExecution { run: crate::scenarios::snapshot_replay, evidence_scope: "Production Runtime and MemoryStore snapshot APIs with bounded deterministic input. The transcript/workflow projection and snapshot encoding are application-owned. This is not SQLite restart or crash-durability evidence." }),
            ready_with_scope("snapshot-partial", "Interrupted publication", "Reopen SQLite with partial uploads, resume one exactly, and reclaim the other.", "Shared production SQLite snapshot fixture", json!({"stop": "close_reopen_during_upload", "chunk_bytes": 4, "cleanup_rows_per_call": 1}), "Only the baseline is published after interruption. Retry preserves the accepted byte count. Resume publishes exact bytes; bounded cleanup removes the abandoned upload.", ReadyExecution { run: crate::scenarios::snapshot_partial, evidence_scope: "Real SQLite close/reopen and production snapshot APIs. This is not process-kill, power-loss or resource qualification." }),
        ]},
        Workstream { number: 6, title: "Retention & compaction", folder: "006-retention-compaction-and-retry-horizons", scenarios: vec![
            ready_with_scope("retention-floor", "Replay floor and retry horizon", "Show how a replay floor and retry-generation expiry affect visible history and exact retries.", "Runtime<MemoryStore> retention policy, generated append, floor, expiry, cleanup, and read_after", json!({"legacy_offset": 1, "generation_one_offset": 2, "floor": 2, "suffix_offset": 3}), "A generation-one retry survives floor cleanup until explicit expiry. Bounded cleanup then leaves exactly the generation-two suffix visible.", ReadyExecution { run: crate::scenarios::retention_floor, evidence_scope: "Production Runtime and MemoryStore retention APIs with bounded deterministic input. This verifies logical floor/expiry behavior and bounded in-memory cleanup; SQLite persistence and fault qualification remain separate." }),
            ready_with_scope("retention-race", "Cleanup during replay", "Control cleanup before SQLite reads and after it returns an owned page.", "Runtime with real SQLite and injected EventStore read gates", json!({"gate_positions": ["before_sqlite_read", "after_sqlite_page"], "records_per_stream": 3, "new_floor": 3}), "A delayed read returns HistoryUnavailable; an already-created page keeps every offset and payload after cleanup removes six records.", ReadyExecution { run: crate::scenarios::retention_race, evidence_scope: "Two deterministic port-boundary schedules around production SQLite reads. Gate injection controls scheduling only. This does not prove concurrent SQLite transactions, crash recovery or resource budgets." }),
        ]},
        Workstream { number: 7, title: "Source & parser recovery", folder: "007-source-journal-and-parser-checkpoints", scenarios: vec![
            ready("decoder-splits", "Every byte boundary", "Feed identical input through the production decoder using every single split and one-byte chunks.", "NewlineFramer::decode / finish", json!({"input": "{\"text\":\"hello\"}\n", "partitions": "unsplit, every single split, and one-byte chunks"}), "Identical byte output for every partition and explicit finished EOF.", crate::scenarios::decoder_splits),
            ready_with_scope("parser-checkpoint", "Capture → crash → resume", "Recover captured input after output commit and before checkpoint publication.", "JournalIngestionService + Runtime<MemoryStore>", json!({"fault": "after_output_commit_before_checkpoint", "rotate_generation_before_resume": true, "stable_output_ids": true}), "Replayed capture deduplicates the first output in its stored generation, commits the next output in the current generation, and publishes one exact checkpoint.", ReadyExecution { run: crate::scenarios::parser_checkpoint, evidence_scope: "Production Runtime, MemoryStore source journal, NewlineFramer checkpoint decoder, and JournalIngestionService with bounded deterministic input. The interruption boundary is constructed through public atomic output and checkpoint APIs. SQLite crash durability remains a separate gate." }),
        ]},
        Workstream { number: 8, title: "Local-first replication", folder: "008-local-first-replication", scenarios: vec![
            ready_with_scope("offline-replica", "Offline then catch up", "Delay replica transport while the origin continues writing, then catch up in bounded batches.", "ReplicationDriver + two MemoryStore instances", json!({"offline_events": 3, "batch_records": 2}), "Three local writes succeed before remote work. Two bounded driver turns catch the destination up in exact origin order.", ReadyExecution { run: crate::scenarios::offline_replica, evidence_scope: "Production MemoryStore origin and destination APIs plus the owned ReplicationDriver with finite deterministic input. This verifies local backlog and bounded in-memory catch-up; SQLite restart, transport fault and resource qualification remain separate." }),
            ready_with_scope("replica-retry", "Remote receipt lost", "Lose a reply after a real SQLite commit, reopen the destination, then retry the same batch.", "ReplicationDriver + MemoryStore origin + SQLite destination", json!({"fault": "after_remote_commit_before_receipt"}), "Origin remains at 0 until retry returns the durable receipt. Destination contains one exact record at cursor 1.", ReadyExecution { run: crate::scenarios::replica_retry, evidence_scope: "Production driver and real isolated SQLite database with injected acknowledgement loss after commit. Includes close/reopen and exact record verification. This is not process-kill or power-loss evidence." }),
        ]},
        Workstream { number: 9, title: "Optional domain adapters", folder: "009-optional-domain-adapters", scenarios: vec![
            planned("terminal-replay", "Terminal bytes & resize", "Inspect a selected terminal adapter's state under ordered bytes and resize input.", "Terminal companion adapter (not selected)", json!({"events": ["bytes", "resize", "bytes"], "engine": null}), "Replay behavior and side effects follow the selected adapter contract. No engine is assumed installed."),
            planned("agent-normalization", "Provider event mapping", "Compare real provider fixtures with a versioned common event schema.", "Agent companion normalizer (not selected)", json!({"provider_fixture": null, "schema_version": null}), "Known fixtures map deterministically. Unsupported required semantics fail explicitly."),
        ]},
        Workstream { number: 10, title: "Product readiness", folder: "010-product-readiness-and-scope-closure", scenarios: vec![
            ready_with_scope("full-recovery", "End-to-end recovery", "Follow captured input through SQLite restart, durable EOF, snapshot transfer, retention, and replica catch-up.", "Shared SQLite integration fixture through production runtime, ingestion, retention, and replication APIs", json!({"raw_input": "a\nb\n", "restart_after": "first_output_before_checkpoint", "stop": "close_reopen", "late_event": "c-after-completion"}), "Checkpoint reaches byte 4/item 2/output 2; sealed EOF survives cleanup and restart; two old records are reclaimed; snapshot and suffix survive restart; late output reaches replica cursor 3 with no backlog.", ReadyExecution { run: crate::scenarios::full_recovery, evidence_scope: "Two real isolated SQLite databases and the shared integration fixture. Console uses orderly close/reopen at the output/checkpoint gap. Separate integration tests run three process-kill schedules. This callback does not establish power-loss behavior or release resource budgets." }),
            ready_with_scope("mixed-workload", "Mixed workload progress", "Run scheduled foreground offers and replay while snapshot, retention, source cleanup, and replica recovery use the same production SQLite runtime.", "Shared mixed-workload fixture through production runtime, ingestion, retention, snapshot, and replication APIs", json!({"mixed": true, "offers": 128, "interval_us": 1250, "snapshot_bytes": 65536}), "All 128 offers have explicit outcomes; tasks and the runtime queue remain bounded; 64 replay calls complete and foreground receipts overlap maintenance; cleanup removes 32 records and the replica reaches cursor 33.", ReadyExecution { run: crate::scenarios::mixed_workload, evidence_scope: "Two real isolated SQLite databases and the shared bounded mixed-workload fixture. This verifies outcome conservation, bounded task and queue counts, foreground progress during maintenance, replay completion, cleanup, restart, and replica retry behavior. Reported wall-clock latency samples are correctness diagnostics only; this scenario does not qualify CPU, memory, latency, throughput, or release resource budgets." }),
            planned("release-budgets", "Complete workload budgets", "Compare the full supported workload with recorded release budgets.", "Headless benchmark suite", json!({"baseline": null, "budgets": null}), "Measured results meet recorded budgets. Missing measurements cannot pass."),
        ]},
    ]
}

pub fn validate(streams: &[Workstream]) -> Result<(), String> {
    let mut ids = std::collections::HashSet::new();
    for (index, stream) in streams.iter().enumerate() {
        if stream.number != index + 1 || stream.scenarios.is_empty() {
            return Err(format!(
                "Invalid workstream order or empty phase: {}",
                stream.title
            ));
        }
        for s in &stream.scenarios {
            if s.id.is_empty()
                || !s
                    .id
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
                || !ids.insert(s.id)
            {
                return Err(format!("Invalid or duplicate scenario ID: {}", s.id));
            }
            if s.target.is_empty()
                || s.expected.is_empty()
                || !s.input.is_object()
                || (s.run.is_none() && s.blocked_reason.is_empty())
                || (s.run.is_some() && s.source.is_empty())
            {
                return Err(format!("Incomplete scenario contract: {}", s.id));
            }
        }
    }
    Ok(())
}
