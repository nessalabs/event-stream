use crate::catalog::{self, Scenario};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub const MAX_REPORT_BYTES: u64 = 262_144;
static NEXT_RUN_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static NEXT_SAVE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub struct RunContext {
    pub workspace: PathBuf,
    pub input: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Observation {
    pub label: String,
    pub expected: String,
    pub actual: String,
    pub passed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CheckResult {
    pub summary: String,
    pub observations: Vec<Observation>,
    pub output: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Passed,
    Failed,
    Blocked,
}
impl Status {
    pub fn label(self) -> &'static str {
        match self {
            Self::Passed => "Passed",
            Self::Failed => "Failed",
            Self::Blocked => "Not implemented",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunReport {
    pub format_version: u32,
    pub scenario_id: String,
    pub run_id: String,
    pub started_unix_ms: u128,
    pub target: String,
    pub implementation_fingerprint: String,
    pub evidence_scope: String,
    pub input: Value,
    pub status: Status,
    pub elapsed_us: u128,
    pub cpu_ns: Option<u64>,
    pub peak_memory_bytes: Option<u64>,
    pub result: CheckResult,
}

/// Identity for the compiled runner/catalog, not a cryptographic integrity claim.
fn implementation_fingerprint() -> String {
    let mut hash = 0xcbf29ce484222325u64;
    for source in [
        include_bytes!("runner.rs").as_slice(),
        include_bytes!("catalog.rs").as_slice(),
        include_bytes!("scenarios.rs").as_slice(),
        include_bytes!("../../src/application/snapshot.rs").as_slice(),
        include_bytes!("../../src/domain/snapshot.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite_replication.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite_retention.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite_snapshot.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite_source_journal.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite_test_vfs.rs").as_slice(),
        include_bytes!("../fixtures/journal_replication.rs").as_slice(),
        include_bytes!("../fixtures/mixed_workload.rs").as_slice(),
        include_bytes!("../fixtures/retention_read.rs").as_slice(),
        include_bytes!("../fixtures/snapshot_interruption.rs").as_slice(),
        include_bytes!("../../src/application/runtime.rs").as_slice(),
        include_bytes!("../../src/application/restore.rs").as_slice(),
        include_bytes!("../../src/application/retention.rs").as_slice(),
        include_bytes!("../../src/application/source_journal.rs").as_slice(),
        include_bytes!("../../src/application/replication.rs").as_slice(),
        include_bytes!("../../src/application/config.rs").as_slice(),
        include_bytes!("../../src/application/ports.rs").as_slice(),
        include_bytes!("../../src/application/types.rs").as_slice(),
        include_bytes!("../../src/application/mod.rs").as_slice(),
        include_bytes!("../../src/infrastructure/memory.rs").as_slice(),
        include_bytes!("../../src/infrastructure/memory_retention.rs").as_slice(),
        include_bytes!("../../src/infrastructure/memory_source_journal.rs").as_slice(),
        include_bytes!("../../src/infrastructure/memory_replication.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite.rs").as_slice(),
        include_bytes!("../../src/infrastructure/sqlite_restore.rs").as_slice(),
        include_bytes!("../../src/infrastructure/mod.rs").as_slice(),
        include_bytes!("../../src/ingestion/mod.rs").as_slice(),
        include_bytes!("../../src/ingestion/journal.rs").as_slice(),
        include_bytes!("../../src/domain/model.rs").as_slice(),
        include_bytes!("../../src/domain/cursor_codec.rs").as_slice(),
        include_bytes!("../../src/domain/lifecycle.rs").as_slice(),
        include_bytes!("../../src/domain/restore.rs").as_slice(),
        include_bytes!("../../src/domain/retention.rs").as_slice(),
        include_bytes!("../../src/domain/source_journal.rs").as_slice(),
        include_bytes!("../../src/domain/replication.rs").as_slice(),
        include_bytes!("../../src/domain/mod.rs").as_slice(),
        include_bytes!("../../src/lib.rs").as_slice(),
        include_bytes!("../../Cargo.toml").as_slice(),
        include_bytes!("../../Cargo.lock").as_slice(),
        include_bytes!("../Cargo.toml").as_slice(),
        include_bytes!("../Cargo.lock").as_slice(),
    ] {
        for byte in source {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    format!("fnv1a64:{hash:016x}")
}

pub fn workspace() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace parent")
        .to_path_buf()
}

pub fn execute(scenario: &Scenario, workspace: &Path) -> RunReport {
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let clock = Instant::now();
    let (status, result) = if let Some(run) = scenario.run {
        // Only registered Rust callbacks execute. No shell or arbitrary commands from fixtures.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run(&RunContext {
                workspace: workspace.to_path_buf(),
                input: scenario.input.clone(),
            })
        })) {
            Ok(result) => {
                let status = if !result.observations.is_empty()
                    && result.observations.iter().all(|o| o.passed)
                {
                    Status::Passed
                } else {
                    Status::Failed
                };
                (status, result)
            }
            Err(_) => (
                Status::Failed,
                CheckResult {
                    summary: "Scenario callback panicked. No passing evidence recorded.".into(),
                    observations: vec![],
                    output: json!({"error": "callback_panicked"}),
                },
            ),
        }
    } else {
        (
            Status::Blocked,
            CheckResult {
                summary: scenario.blocked_reason.into(),
                observations: vec![],
                output: json!({"error": "implementation_unavailable"}),
            },
        )
    };
    RunReport {
        format_version: 1,
        scenario_id: scenario.id.into(),
        run_id: format!(
            "{}-{}-{}",
            started.as_nanos(),
            std::process::id(),
            NEXT_RUN_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ),
        started_unix_ms: started.as_millis(),
        target: scenario.target.into(),
        implementation_fingerprint: implementation_fingerprint(),
        evidence_scope: scenario.evidence_scope.into(),
        input: scenario.input.clone(),
        status,
        elapsed_us: clock.elapsed().as_micros(),
        cpu_ns: None,
        peak_memory_bytes: None,
        result,
    }
}

fn read_bounded(path: &Path) -> Result<String, String> {
    let file = fs::File::open(path).map_err(|e| e.to_string())?;
    let mut text = String::new();
    file.take(MAX_REPORT_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|e| e.to_string())?;
    if text.len() as u64 > MAX_REPORT_BYTES {
        return Err("File exceeds 256 KiB inspection limit".into());
    }
    Ok(text)
}

pub fn audit_roadmap(cx: &RunContext) -> CheckResult {
    let streams = catalog::workstreams();
    let mut observations = Vec::new();
    let mut files = Vec::new();
    for stream in streams {
        let relative = stream.document();
        let expected_title = format!("# {:04}.", stream.number);
        let required: &[&str] = if stream.number == 1 {
            &["## Context", "## Decision", "## Consequences"]
        } else {
            &[
                "## Context",
                "## Decision",
                "## Ordered work",
                "## Completion evidence",
                "## Consequences",
            ]
        };
        let (passed, actual, bytes) = match read_bounded(&cx.workspace.join(&relative)) {
            Ok(text) => {
                let missing: Vec<_> = required
                    .iter()
                    .filter(|heading| !text.lines().any(|line| line == **heading))
                    .collect();
                let valid = text.starts_with(&expected_title) && missing.is_empty();
                let actual = if valid {
                    format!(
                        "{} bytes · {} required sections found",
                        text.len(),
                        required.len()
                    )
                } else {
                    format!(
                        "Matching title: {}; missing sections: {missing:?}",
                        text.starts_with(&expected_title)
                    )
                };
                (valid, actual, Some(text.len()))
            }
            Err(error) => (false, error, None),
        };
        observations.push(Observation {
            label: format!("{:02} / {}", stream.number, stream.title),
            expected: format!(
                "{} and {} required sections",
                expected_title,
                required.len()
            ),
            actual: actual.clone(),
            passed,
        });
        files.push(json!({"path": relative, "bytes": bytes, "passed": passed, "observed": actual}));
    }
    let count = observations.iter().filter(|o| o.passed).count();
    CheckResult {
        summary: format!(
            "{count}/{} workstream documents satisfy their structure contract.",
            observations.len()
        ),
        observations,
        output: json!({"files": files}),
    }
}

pub fn audit_catalog(_: &RunContext) -> CheckResult {
    let streams = catalog::workstreams();
    let result = catalog::validate(&streams);
    let total: usize = streams.iter().map(|s| s.scenarios.len()).sum();
    let runnable = streams
        .iter()
        .flat_map(|s| &s.scenarios)
        .filter(|s| s.run.is_some())
        .count();
    CheckResult {
        summary: format!("{total} scenarios across {} workstreams; {runnable} runnable checks, {} awaiting implementation.", streams.len(), total - runnable),
        observations: vec![Observation { label: "Scenario registry".into(), expected: "Ordered workstreams, unique IDs, valid inputs, explicit runnable/blocked targets".into(), actual: result.as_ref().map(|_| "All catalog entries satisfy their contract".into()).unwrap_or_else(|e| e.clone()), passed: result.is_ok() }],
        output: json!({"workstreams": streams.len(), "scenarios": total, "runnable": runnable, "blocked": total-runnable, "entries": streams.iter().flat_map(|s| s.scenarios.iter().map(move |c| json!({"phase": s.number, "id": c.id, "target": c.target, "callable": c.run.is_some()}))).collect::<Vec<_>>() }),
    }
}

pub fn report_path(workspace: &Path, id: &str) -> PathBuf {
    workspace
        .join("verification/evidence/runs")
        .join(format!("{id}.json"))
}

pub fn save_report(workspace: &Path, report: &RunReport) -> Result<PathBuf, String> {
    if !report
        .scenario_id
        .bytes()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        || report.scenario_id.is_empty()
    {
        return Err("Invalid evidence ID".into());
    }
    let bytes = serde_json::to_vec_pretty(report).map_err(|e| e.to_string())?;
    if bytes.len() as u64 > MAX_REPORT_BYTES {
        return Err("Report exceeds 256 KiB evidence limit".into());
    }
    let path = report_path(workspace, &report.scenario_id);
    fs::create_dir_all(path.parent().unwrap()).map_err(|e| e.to_string())?;
    let temporary = path.with_extension(format!(
        "{}-{}.tmp",
        std::process::id(),
        NEXT_SAVE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    write_report_atomically(&path, &temporary, &bytes)?;
    Ok(path)
}

fn write_report_atomically(path: &Path, temporary: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary)
        .map_err(|e| e.to_string())?;
    let written = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| e.to_string());
    drop(file);
    if let Err(error) = written {
        let _ = fs::remove_file(temporary);
        return Err(error);
    }
    if let Err(error) = fs::rename(temporary, path) {
        let _ = fs::remove_file(temporary);
        return Err(error.to_string());
    }
    Ok(())
}

pub fn load_report(workspace: &Path, scenario: &Scenario) -> Result<Option<RunReport>, String> {
    let path = report_path(workspace, scenario.id);
    if !path.exists() {
        return Ok(None);
    }
    let report: RunReport = serde_json::from_str(&read_bounded(&path)?)
        .map_err(|e| format!("Unreadable saved evidence: {e}"))?;
    if report.format_version != 1
        || report.scenario_id != scenario.id
        || report.target != scenario.target
        || report.input != scenario.input
        || report.implementation_fingerprint != implementation_fingerprint()
    {
        return Err(
            "Saved evidence belongs to a different scenario contract; rerun this check.".into(),
        );
    }
    Ok(Some(report))
}

#[cfg(test)]
mod tests {
    use super::*;
    static NEXT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            for _ in 0..16 {
                let p = std::env::temp_dir().join(format!(
                    "stream-verification-test-{}-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos(),
                    NEXT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
                ));
                match fs::create_dir(&p) {
                    Ok(()) => return Self(p),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => panic!("create isolated test directory: {error}"),
                }
            }
            panic!("could not allocate an isolated test directory after 16 attempts")
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn roadmap_uses_real_files_and_reports_missing_file() {
        let streams = catalog::workstreams();
        let scenario = &streams[0].scenarios[0];
        assert_eq!(execute(scenario, &workspace()).status, Status::Passed);
        let temp = Temp::new();
        let report = execute(scenario, &temp.0);
        assert_eq!(report.status, Status::Failed);
        assert_eq!(report.result.observations.len(), 10);
    }
    #[test]
    fn duplicate_registry_ids_fail() {
        let mut streams = catalog::workstreams();
        streams[1].scenarios[0].id = streams[0].scenarios[0].id;
        assert!(catalog::validate(&streams).is_err());
    }
    #[test]
    fn unavailable_target_never_passes() {
        let streams = catalog::workstreams();
        let scenario = streams
            .iter()
            .flat_map(|stream| &stream.scenarios)
            .find(|scenario| scenario.run.is_none())
            .expect("catalog keeps at least one explicit future target");
        let report = execute(scenario, &workspace());
        assert_eq!(report.status, Status::Blocked);
        assert!(report.result.observations.is_empty());
        assert_eq!(report.cpu_ns, None);
    }
    #[test]
    fn saved_reports_are_separate_and_corruption_is_visible() {
        let temp = Temp::new();
        let streams = catalog::workstreams();
        let a = &streams[0].scenarios[0];
        let b = &streams[1].scenarios[0];
        let report = execute(a, &workspace());
        save_report(&temp.0, &report).unwrap();
        assert_eq!(
            load_report(&temp.0, a).unwrap().unwrap().run_id,
            report.run_id
        );
        assert!(load_report(&temp.0, b).unwrap().is_none());
        fs::write(report_path(&temp.0, a.id), "broken json").unwrap();
        assert!(load_report(&temp.0, a).is_err());
    }
    #[test]
    fn concurrent_same_scenario_saves_use_distinct_temporary_files() {
        let temp = Temp::new();
        let streams = catalog::workstreams();
        let scenario = &streams[0].scenarios[0];
        let first = execute(scenario, &workspace());
        let second = execute(scenario, &workspace());
        assert_ne!(first.run_id, second.run_id);

        std::thread::scope(|scope| {
            let left = scope.spawn(|| save_report(&temp.0, &first));
            let right = scope.spawn(|| save_report(&temp.0, &second));
            left.join().unwrap().unwrap();
            right.join().unwrap().unwrap();
        });

        let saved = load_report(&temp.0, scenario).unwrap().unwrap();
        assert!(saved.run_id == first.run_id || saved.run_id == second.run_id);
    }
    #[test]
    fn existing_unowned_temporary_file_is_never_removed_or_truncated() {
        let temp = Temp::new();
        let destination = temp.0.join("report.json");
        let occupied = temp.0.join("occupied.tmp");
        fs::write(&occupied, b"another writer owns this file").unwrap();

        assert!(write_report_atomically(&destination, &occupied, b"new report").is_err());
        assert_eq!(
            fs::read(&occupied).unwrap(),
            b"another writer owns this file"
        );
        assert!(!destination.exists());
    }
    #[test]
    fn changed_fixture_invalidates_saved_result() {
        let temp = Temp::new();
        let mut streams = catalog::workstreams();
        let scenario = &mut streams[0].scenarios[0];
        save_report(&temp.0, &execute(scenario, &workspace())).unwrap();
        scenario.input = json!({"changed":true});
        assert!(load_report(&temp.0, scenario).is_err());
    }
}

/// The UI depends on this seam, not on a database, subprocess, or test framework.
/// Alternate executors are useful for UI state tests; label their evidence honestly.
pub trait ScenarioExecutor: Send + Sync {
    fn run(&self, scenario_id: &str) -> Result<RunReport, String>;
}

pub struct RegisteredExecutor {
    pub workspace: PathBuf,
}
impl ScenarioExecutor for RegisteredExecutor {
    fn run(&self, scenario_id: &str) -> Result<RunReport, String> {
        let streams = catalog::workstreams();
        let scenario = streams
            .iter()
            .flat_map(|s| &s.scenarios)
            .find(|s| s.id == scenario_id)
            .ok_or_else(|| format!("Unknown scenario: {scenario_id}"))?;
        Ok(execute(scenario, &self.workspace))
    }
}

/// Storage is injected so tests can exercise failed writes without touching real evidence.
pub trait EvidenceStore: Send + Sync {
    fn save(&self, report: &RunReport) -> Result<PathBuf, String>;
    fn load(&self, scenario: &Scenario) -> Result<Option<RunReport>, String>;
}
pub struct FileEvidenceStore {
    pub workspace: PathBuf,
}
impl EvidenceStore for FileEvidenceStore {
    fn save(&self, report: &RunReport) -> Result<PathBuf, String> {
        save_report(&self.workspace, report)
    }
    fn load(&self, scenario: &Scenario) -> Result<Option<RunReport>, String> {
        load_report(&self.workspace, scenario)
    }
}

pub struct RunCompletion {
    pub report: RunReport,
    /// Saving failure does not erase the actual execution result.
    pub saved: Result<PathBuf, String>,
}

pub fn run_and_save(
    executor: &dyn ScenarioExecutor,
    evidence: &dyn EvidenceStore,
    id: &str,
) -> Result<RunCompletion, String> {
    let report = executor.run(id)?;
    let saved = if report.status == Status::Blocked {
        Err("No implementation executed; no evidence saved.".into())
    } else {
        evidence.save(&report)
    };
    Ok(RunCompletion { report, saved })
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    struct FailedEvidence;
    impl EvidenceStore for FailedEvidence {
        fn save(&self, _: &RunReport) -> Result<PathBuf, String> {
            Err("injected storage failure".into())
        }
        fn load(&self, _: &Scenario) -> Result<Option<RunReport>, String> {
            Ok(None)
        }
    }
    #[test]
    fn injected_save_failure_preserves_execution_result() {
        let executor = RegisteredExecutor {
            workspace: workspace(),
        };
        let result = run_and_save(&executor, &FailedEvidence, "catalog-integrity").unwrap();
        assert_eq!(result.report.status, Status::Passed);
        assert_eq!(result.saved.unwrap_err(), "injected storage failure");
    }
    #[test]
    fn unknown_scenario_is_rejected_before_execution() {
        let executor = RegisteredExecutor {
            workspace: workspace(),
        };
        assert!(executor.run("../../arbitrary-command").is_err());
    }
    #[test]
    fn every_generated_duplicate_id_is_rejected() {
        // Exhaustively generate a duplicate at every pair of positions in this finite registry.
        let base = catalog::workstreams();
        let positions: Vec<_> = base
            .iter()
            .enumerate()
            .flat_map(|(w, s)| (0..s.scenarios.len()).map(move |i| (w, i)))
            .collect();
        for (from_index, &(fw, fs)) in positions.iter().enumerate() {
            for &(tw, ts) in positions.iter().skip(from_index + 1) {
                let mut streams = catalog::workstreams();
                streams[tw].scenarios[ts].id = streams[fw].scenarios[fs].id;
                assert!(
                    catalog::validate(&streams).is_err(),
                    "duplicate at ({fw},{fs}) and ({tw},{ts})"
                );
            }
        }
    }

    #[test]
    fn every_registered_callback_passes_headlessly() {
        // Read the same registry as the UI so new Run buttons cannot escape this check.
        let streams = catalog::workstreams();
        for scenario in streams.iter().flat_map(|stream| &stream.scenarios) {
            let report = execute(scenario, &workspace());
            let expected = if scenario.run.is_some() {
                Status::Passed
            } else {
                Status::Blocked
            };
            assert_eq!(
                report.status, expected,
                "{}: {}",
                scenario.id, report.result.summary
            );
        }
    }

    #[test]
    fn production_callback_rejects_a_mismatched_displayed_fixture() {
        let mut streams = catalog::workstreams();
        let scenario = &mut streams[0].scenarios[1];
        scenario.input = json!({"changed": true});
        assert_eq!(execute(scenario, &workspace()).status, Status::Failed);
    }
}
