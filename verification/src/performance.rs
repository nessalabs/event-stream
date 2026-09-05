//! Bounded, typed input for the verification Home page.
//!
//! `home.json` is a small catalog derived from the retained raw evidence. The UI
//! never scans benchmark JSONL files or starts a benchmark.

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

pub const HOME_RELATIVE_PATH: &str = "verification/evidence/home.json";
const MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_ROUNDS: usize = 128;
const MAX_EXPERIMENTS_PER_ROUND: usize = 512;
const MAX_EXPERIMENTS: usize = 20_000;
const MAX_STRING_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HomeEvidence {
    pub schema_version: u32,
    pub generated_at_utc: String,
    pub rounds: Vec<ExperimentRound>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentRound {
    pub number: u32,
    pub summary: String,
    pub status: RoundStatus,
    pub build_id: Option<String>,
    pub experiments: Vec<Experiment>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RoundStatus {
    Measured,
    Partial,
    NotMeasured,
    PriorSource,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Experiment {
    pub id: String,
    pub label: String,
    pub status: ExperimentStatus,
    pub comparison_key: ComparisonKey,
    pub sample_count: u32,
    pub metrics: ExperimentMetrics,
    pub provenance: ExperimentProvenance,
    pub note: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentStatus {
    Measured,
    Partial,
    NotMeasured,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ComparisonKey {
    pub workload: String,
    pub store: String,
    pub durability: String,
    pub payload_bytes: u64,
    pub population: u64,
    pub producers: u32,
    pub streams: u32,
    pub subscribers: u32,
    pub concurrency: u32,
    pub page_records: u32,
    pub page_bytes: u64,
    pub measurement_phase: String,
    pub instrumentation: String,
    /// Hash of every semantic workload and runtime setting not represented above.
    pub config_fingerprint: String,
    /// For example, `settled_process_rss`.
    pub memory_scope: String,
    /// For example, `process_user_plus_system`.
    pub cpu_scope: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentMetrics {
    pub runtime_ns_median: Option<u64>,
    pub cpu_us_median: Option<u64>,
    pub memory_bytes_median: Option<u64>,
    pub rust_live_bytes_median: Option<u64>,
    #[serde(default)]
    pub rust_allocated_bytes_median: Option<u64>,
    pub agents: Option<u64>,
    pub accepted: Option<u64>,
    pub rejected: Option<u64>,
    pub failed: Option<u64>,
    pub delivered: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExperimentProvenance {
    pub source_path: Option<String>,
    pub source_sha256: Option<String>,
    pub sample_key: Option<String>,
    pub source_epoch: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ExperimentRow {
    pub number: usize,
    pub experiment_number: usize,
    pub round_number: u32,
    pub round_summary: Arc<str>,
    pub round_status: RoundStatus,
    pub build_id: Option<Arc<str>>,
    pub experiment: Experiment,
}

#[derive(Clone, Debug)]
pub struct ChartPoint {
    pub round_number: u32,
    pub value: u64,
    pub baseline_delta_percent: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct HomeViewModel {
    pub generated_at_utc: String,
    pub rounds: Vec<RoundSummary>,
    pub rows: Vec<ExperimentRow>,
    pub issues: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct RoundSummary {
    pub number: u32,
    pub summary: String,
    pub status: RoundStatus,
    pub build_id: Option<String>,
    pub experiment_count: usize,
}

impl HomeViewModel {
    pub fn load(root: &Path) -> Result<Self, String> {
        let path = root.join(HOME_RELATIVE_PATH);
        let metadata = fs::metadata(&path)
            .map_err(|error| format!("Could not read {}: {error}", path.display()))?;
        if metadata.len() > MAX_FILE_BYTES {
            return Err(format!(
                "Home evidence is {} bytes; the limit is {MAX_FILE_BYTES}",
                metadata.len()
            ));
        }
        let mut bytes = Vec::with_capacity(metadata.len().min(MAX_FILE_BYTES) as usize);
        File::open(&path)
            .map_err(|error| format!("Could not open {}: {error}", path.display()))?
            .take(MAX_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("Could not read {}: {error}", path.display()))?;
        if bytes.len() as u64 > MAX_FILE_BYTES {
            return Err(format!(
                "Home evidence exceeds {MAX_FILE_BYTES} bytes while reading"
            ));
        }
        let evidence: HomeEvidence = serde_json::from_slice(&bytes)
            .map_err(|error| format!("Invalid {}: {error}", path.display()))?;
        Self::from_evidence(root, evidence)
    }

    pub fn from_evidence(root: &Path, evidence: HomeEvidence) -> Result<Self, String> {
        validate(root, &evidence)?;
        let rounds = evidence
            .rounds
            .iter()
            .map(|round| RoundSummary {
                number: round.number,
                summary: round.summary.clone(),
                status: round.status,
                build_id: round.build_id.clone(),
                experiment_count: round.experiments.len(),
            })
            .collect();
        let mut rows = Vec::new();
        let mut issues = Vec::new();
        let mut seen_samples = HashSet::new();
        for round in evidence.rounds {
            let round_summary: Arc<str> = round.summary.into();
            let build_id = round.build_id.map(Arc::<str>::from);
            for (experiment_index, experiment) in round.experiments.into_iter().enumerate() {
                if experiment.status == ExperimentStatus::Measured {
                    let provenance = &experiment.provenance;
                    let identity = (
                        provenance.source_sha256.clone().unwrap_or_default(),
                        provenance.sample_key.clone().unwrap_or_default(),
                        experiment.comparison_key.clone(),
                    );
                    if !seen_samples.insert(identity) {
                        issues.push(format!(
                            "Experiment round {} / {} reuses an existing measured artifact and is excluded from charts",
                            round.number, experiment.id
                        ));
                    }
                }
                rows.push(ExperimentRow {
                    number: rows.len() + 1,
                    experiment_number: experiment_index + 1,
                    round_number: round.number,
                    round_summary: round_summary.clone(),
                    round_status: round.status,
                    build_id: build_id.clone(),
                    experiment,
                });
            }
        }
        Ok(Self {
            generated_at_utc: evidence.generated_at_utc,
            rounds,
            rows,
            issues,
        })
    }

    pub fn runtime_series(&self, selected: usize) -> Vec<ChartPoint> {
        self.series(selected, |metrics| metrics.runtime_ns_median)
    }

    pub fn memory_series(&self, selected: usize) -> Vec<ChartPoint> {
        self.series(selected, |metrics| metrics.memory_bytes_median)
    }

    /// Choose a useful first row without inventing a comparison. Prefer the
    /// newest round that has a real earlier match, then registration and the
    /// largest population within that round.
    pub fn default_row_index(&self) -> usize {
        let latest_comparable_round = self
            .rows
            .iter()
            .enumerate()
            .filter(|(index, row)| {
                row.experiment.status == ExperimentStatus::Measured
                    && self.has_earlier_comparison(*index)
            })
            .map(|(_, row)| row.round_number)
            .max();
        if let Some(round) = latest_comparable_round {
            return self
                .rows
                .iter()
                .enumerate()
                .filter(|(index, row)| {
                    row.round_number == round && self.has_earlier_comparison(*index)
                })
                .max_by_key(|(index, row)| {
                    (
                        row.experiment.comparison_key.measurement_phase == "registration",
                        row.experiment.comparison_key.population,
                        *index,
                    )
                })
                .map_or(0, |(index, _)| index);
        }
        self.rows
            .iter()
            .rposition(|row| row.experiment.status == ExperimentStatus::Measured)
            .unwrap_or(0)
    }

    fn has_earlier_comparison(&self, selected: usize) -> bool {
        let Some(row) = self.rows.get(selected) else {
            return false;
        };
        self.rows.iter().any(|candidate| {
            candidate.round_number < row.round_number
                && candidate.experiment.status == ExperimentStatus::Measured
                && candidate.experiment.comparison_key == row.experiment.comparison_key
                && (
                    candidate.experiment.provenance.source_sha256.as_deref(),
                    candidate.experiment.provenance.sample_key.as_deref(),
                ) != (
                    row.experiment.provenance.source_sha256.as_deref(),
                    row.experiment.provenance.sample_key.as_deref(),
                )
        })
    }

    fn series(
        &self,
        selected: usize,
        metric: impl Fn(&ExperimentMetrics) -> Option<u64>,
    ) -> Vec<ChartPoint> {
        let Some(selected_row) = self.rows.get(selected) else {
            return Vec::new();
        };
        let mut seen_artifacts = HashSet::new();
        let mut per_round: HashMap<u32, (u32, u64)> = HashMap::new();
        for row in &self.rows {
            if row.experiment.status != ExperimentStatus::Measured
                || row.experiment.comparison_key != selected_row.experiment.comparison_key
            {
                continue;
            }
            let provenance = &row.experiment.provenance;
            let artifact = (
                provenance.source_sha256.as_deref().unwrap_or_default(),
                provenance.sample_key.as_deref().unwrap_or_default(),
            );
            if !seen_artifacts.insert(artifact) {
                continue;
            }
            let Some(value) = metric(&row.experiment.metrics) else {
                continue;
            };
            per_round
                .entry(row.round_number)
                .and_modify(|current| {
                    if row.experiment.sample_count > current.0 {
                        *current = (row.experiment.sample_count, value);
                    }
                })
                .or_insert((row.experiment.sample_count, value));
        }
        let mut values: Vec<_> = per_round
            .into_iter()
            .map(|(round_number, (_, value))| (round_number, value))
            .collect();
        values.sort_unstable_by_key(|(round, _)| *round);
        let Some((_, baseline)) = values.first().copied() else {
            return Vec::new();
        };
        values
            .into_iter()
            .map(|(round_number, value)| ChartPoint {
                round_number,
                value,
                baseline_delta_percent: if baseline == 0 {
                    None
                } else {
                    Some(((value as f64 - baseline as f64) / baseline as f64) * 100.0)
                },
            })
            .collect()
    }
}

fn validate(root: &Path, evidence: &HomeEvidence) -> Result<(), String> {
    if evidence.schema_version != 1 {
        return Err(format!(
            "Unsupported Home evidence schema version {}",
            evidence.schema_version
        ));
    }
    check_string("generated_at_utc", &evidence.generated_at_utc)?;
    if evidence.rounds.len() > MAX_ROUNDS {
        return Err(format!("Home evidence exceeds {MAX_ROUNDS} rounds"));
    }
    let total: usize = evidence
        .rounds
        .iter()
        .map(|round| round.experiments.len())
        .sum();
    if total > MAX_EXPERIMENTS {
        return Err(format!(
            "Home evidence exceeds {MAX_EXPERIMENTS} experiments"
        ));
    }
    let mut previous = 0;
    let mut round_numbers = HashSet::new();
    let mut chart_epochs: HashMap<(u32, ComparisonKey), &str> = HashMap::new();
    for round in &evidence.rounds {
        if round.number == 0 || !round_numbers.insert(round.number) || round.number <= previous {
            return Err("Experiment round numbers must be positive, unique, and increasing".into());
        }
        previous = round.number;
        check_string("round summary", &round.summary)?;
        check_optional_string("build_id", round.build_id.as_deref())?;
        if round.experiments.len() > MAX_EXPERIMENTS_PER_ROUND {
            return Err(format!(
                "Experiment round {} exceeds {MAX_EXPERIMENTS_PER_ROUND} experiments",
                round.number
            ));
        }
        let mut ids = HashSet::new();
        for experiment in &round.experiments {
            if experiment.id.is_empty() || !ids.insert(&experiment.id) {
                return Err(format!(
                    "Experiment round {} has an empty or duplicate experiment id",
                    round.number
                ));
            }
            validate_experiment(root, round.number, experiment)?;
            if round.status == RoundStatus::NotMeasured
                && experiment.status == ExperimentStatus::Measured
            {
                return Err(format!(
                    "Not-measured experiment round {} cannot contain measured experiment {}",
                    round.number, experiment.id
                ));
            }
            if experiment.status == ExperimentStatus::Measured {
                let epoch = experiment
                    .provenance
                    .source_epoch
                    .as_deref()
                    .unwrap_or_default();
                if let Some(existing) =
                    chart_epochs.insert((round.number, experiment.comparison_key.clone()), epoch)
                {
                    if existing != epoch {
                        return Err(format!(
                            "Experiment round {} has multiple source epochs for comparison key {}",
                            round.number, experiment.comparison_key.config_fingerprint
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_experiment(root: &Path, round: u32, experiment: &Experiment) -> Result<(), String> {
    for (name, value) in [
        ("id", experiment.id.as_str()),
        ("label", experiment.label.as_str()),
        ("workload", experiment.comparison_key.workload.as_str()),
        ("store", experiment.comparison_key.store.as_str()),
        ("durability", experiment.comparison_key.durability.as_str()),
        (
            "measurement_phase",
            experiment.comparison_key.measurement_phase.as_str(),
        ),
        (
            "instrumentation",
            experiment.comparison_key.instrumentation.as_str(),
        ),
        (
            "memory_scope",
            experiment.comparison_key.memory_scope.as_str(),
        ),
        ("cpu_scope", experiment.comparison_key.cpu_scope.as_str()),
    ] {
        if value.is_empty() {
            return Err(format!(
                "Experiment round {round} / {} has an empty {name}",
                experiment.id
            ));
        }
        check_string(name, value)?;
    }
    validate_sha(
        "config_fingerprint",
        &experiment.comparison_key.config_fingerprint,
    )?;
    check_optional_string("note", experiment.note.as_deref())?;
    let provenance = &experiment.provenance;
    for (name, value) in [
        ("source_path", provenance.source_path.as_deref()),
        ("source_sha256", provenance.source_sha256.as_deref()),
        ("sample_key", provenance.sample_key.as_deref()),
        ("source_epoch", provenance.source_epoch.as_deref()),
    ] {
        check_optional_string(name, value)?;
    }
    if let Some(path) = provenance.source_path.as_deref() {
        validate_source_path(root, path)?;
    }
    if let Some(sha) = provenance.source_sha256.as_deref() {
        validate_sha("source_sha256", sha)?;
    }
    match experiment.status {
        ExperimentStatus::Measured => {
            if experiment.sample_count == 0 || !has_metric(&experiment.metrics) {
                return Err(format!(
                    "Measured experiment round {round} / {} needs samples and a measured value",
                    experiment.id
                ));
            }
            for (name, value) in [
                ("source_path", provenance.source_path.as_deref()),
                ("source_sha256", provenance.source_sha256.as_deref()),
                ("sample_key", provenance.sample_key.as_deref()),
                ("source_epoch", provenance.source_epoch.as_deref()),
            ] {
                if value.is_none_or(str::is_empty) {
                    return Err(format!(
                        "Measured experiment round {round} / {} needs {name}",
                        experiment.id
                    ));
                }
            }
        }
        ExperimentStatus::NotMeasured => {
            if experiment.sample_count != 0 || has_metric(&experiment.metrics) {
                return Err(format!(
                    "Not-measured experiment round {round} / {} cannot contain samples or metrics",
                    experiment.id
                ));
            }
        }
        ExperimentStatus::Partial => {}
    }
    Ok(())
}

fn validate_source_path(root: &Path, value: &str) -> Result<(), String> {
    let path = Path::new(value);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(format!(
            "Evidence source path must stay inside the workspace: {value}"
        ));
    }
    let joined: PathBuf = root.join(path);
    if !joined.is_file() {
        return Err(format!(
            "Evidence source does not exist: {}",
            joined.display()
        ));
    }
    Ok(())
}

fn validate_sha(name: &str, value: &str) -> Result<(), String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{name} must be 64 lowercase hexadecimal characters"
        ));
    }
    Ok(())
}

fn check_optional_string(name: &str, value: Option<&str>) -> Result<(), String> {
    if let Some(value) = value {
        check_string(name, value)?;
    }
    Ok(())
}

fn check_string(name: &str, value: &str) -> Result<(), String> {
    if value.len() > MAX_STRING_BYTES {
        return Err(format!("{name} exceeds {MAX_STRING_BYTES} bytes"));
    }
    Ok(())
}

fn has_metric(metrics: &ExperimentMetrics) -> bool {
    metrics.runtime_ns_median.is_some()
        || metrics.cpu_us_median.is_some()
        || metrics.memory_bytes_median.is_some()
        || metrics.rust_live_bytes_median.is_some()
        || metrics.rust_allocated_bytes_median.is_some()
        || metrics.agents.is_some()
        || metrics.accepted.is_some()
        || metrics.rejected.is_some()
        || metrics.failed.is_some()
        || metrics.delivered.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        sync::atomic::{AtomicU64, Ordering},
    };

    static TEMP_ID: AtomicU64 = AtomicU64::new(0);

    struct TempRoot(PathBuf);

    impl TempRoot {
        fn new() -> Self {
            let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("event-stream-home-{}-{id}", std::process::id()));
            fs::create_dir_all(path.join("verification/evidence")).unwrap();
            fs::write(path.join("source.jsonl"), b"{}\n").unwrap();
            Self(path)
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn key(fingerprint_byte: char) -> ComparisonKey {
        ComparisonKey {
            workload: "subscription_scale".into(),
            store: "memory".into(),
            durability: "Ephemeral".into(),
            payload_bytes: 128,
            population: 1_000,
            producers: 1,
            streams: 1_000,
            subscribers: 1_000,
            concurrency: 1,
            page_records: 16,
            page_bytes: 2_097_152,
            measurement_phase: "timed_idle".into(),
            instrumentation: "allocator_and_stage_timestamps".into(),
            config_fingerprint: std::iter::repeat_n(fingerprint_byte, 64).collect(),
            memory_scope: "settled_process_RSS".into(),
            cpu_scope: "process_user_plus_system".into(),
        }
    }

    fn measured(id: &str, key: ComparisonKey, epoch: &str, runtime: Option<u64>) -> Experiment {
        Experiment {
            id: id.into(),
            label: id.into(),
            status: ExperimentStatus::Measured,
            comparison_key: key,
            sample_count: 3,
            metrics: ExperimentMetrics {
                runtime_ns_median: runtime,
                cpu_us_median: None,
                memory_bytes_median: Some(1024),
                rust_live_bytes_median: None,
                rust_allocated_bytes_median: None,
                agents: Some(1_000),
                accepted: Some(0),
                rejected: Some(0),
                failed: Some(0),
                delivered: Some(0),
            },
            provenance: ExperimentProvenance {
                source_path: Some("source.jsonl".into()),
                source_sha256: Some("a".repeat(64)),
                sample_key: Some(id.into()),
                source_epoch: Some(epoch.into()),
            },
            note: None,
        }
    }

    fn evidence(rounds: Vec<ExperimentRound>) -> HomeEvidence {
        HomeEvidence {
            schema_version: 1,
            generated_at_utc: "2026-09-05T00:00:00Z".into(),
            rounds,
        }
    }

    fn round(number: u32, experiments: Vec<Experiment>) -> ExperimentRound {
        ExperimentRound {
            number,
            summary: format!("Round {number}"),
            status: RoundStatus::Measured,
            build_id: Some("b".repeat(64)),
            experiments,
        }
    }

    #[test]
    fn checked_in_home_catalog_loads_bounded_historical_evidence() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let model = HomeViewModel::load(root).unwrap();
        assert!(!model.rounds.is_empty());
        assert!(!model.rows.is_empty());
        assert!(model.rows.iter().any(|row| {
            row.experiment.status == ExperimentStatus::Measured
                && row.experiment.provenance.source_path.is_some()
                && row.experiment.provenance.source_sha256.is_some()
        }));
    }

    #[test]
    fn charts_require_exact_workload_and_config_and_preserve_nulls() {
        let root = TempRoot::new();
        let first = measured("first", key('a'), "epoch-a", Some(10));
        let mut incompatible = measured("other", key('b'), "epoch-b", Some(5));
        incompatible.metrics.cpu_us_median = None;
        let model = HomeViewModel::from_evidence(
            &root.0,
            evidence(vec![round(1, vec![first]), round(2, vec![incompatible])]),
        )
        .unwrap();
        assert_eq!(model.runtime_series(0).len(), 1);
        assert_eq!(model.rows[1].experiment.metrics.cpu_us_median, None);
    }

    #[test]
    fn duplicate_artifact_is_reported_and_excluded_from_chart() {
        let root = TempRoot::new();
        let first = measured("same", key('a'), "epoch-a", Some(10));
        let mut duplicate = measured("same", key('a'), "epoch-a", Some(8));
        duplicate.provenance.sample_key = first.provenance.sample_key.clone();
        let model = HomeViewModel::from_evidence(
            &root.0,
            evidence(vec![round(1, vec![first]), round(2, vec![duplicate])]),
        )
        .unwrap();
        assert_eq!(model.issues.len(), 1);
        assert_eq!(model.runtime_series(0).len(), 1);
    }

    #[test]
    fn zero_baseline_has_no_percentage_delta() {
        let root = TempRoot::new();
        let first = measured("zero", key('a'), "epoch-a", Some(0));
        let second = measured("later", key('a'), "epoch-b", Some(10));
        let model = HomeViewModel::from_evidence(
            &root.0,
            evidence(vec![round(1, vec![first]), round(2, vec![second])]),
        )
        .unwrap();
        let points = model.runtime_series(0);
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].baseline_delta_percent, None);
        assert_eq!(points[1].baseline_delta_percent, None);
    }

    #[test]
    fn default_selection_prefers_latest_comparable_registration_at_highest_population() {
        let root = TempRoot::new();
        let mut registration_small = key('a');
        registration_small.measurement_phase = "registration".into();
        let mut registration_large = registration_small.clone();
        registration_large.population = 10_000;
        registration_large.config_fingerprint = "b".repeat(64);
        let idle = key('c');
        let model = HomeViewModel::from_evidence(
            &root.0,
            evidence(vec![
                round(
                    1,
                    vec![
                        measured("small-before", registration_small.clone(), "one", Some(30)),
                        measured("large-before", registration_large.clone(), "one", Some(40)),
                        measured("idle-before", idle.clone(), "one", Some(10)),
                    ],
                ),
                round(
                    2,
                    vec![
                        measured("idle-after", idle, "two", Some(9)),
                        measured("small-after", registration_small, "two", Some(3)),
                        measured("large-after", registration_large, "two", Some(4)),
                    ],
                ),
            ]),
        )
        .unwrap();
        let selected = &model.rows[model.default_row_index()];
        assert_eq!(selected.round_number, 2);
        assert_eq!(selected.experiment.id, "large-after");
        assert_eq!(model.runtime_series(model.default_row_index()).len(), 2);
    }

    #[test]
    fn ambiguous_source_epochs_in_one_round_are_rejected() {
        let root = TempRoot::new();
        let first = measured("first", key('a'), "epoch-a", Some(10));
        let second = measured("second", key('a'), "epoch-b", Some(9));
        let error =
            HomeViewModel::from_evidence(&root.0, evidence(vec![round(1, vec![first, second])]))
                .unwrap_err();
        assert!(error.contains("multiple source epochs"));
    }

    #[test]
    fn oversized_catalog_is_refused_before_json_parsing() {
        let root = TempRoot::new();
        let path = root.0.join(HOME_RELATIVE_PATH);
        let file = fs::File::create(path).unwrap();
        file.set_len(MAX_FILE_BYTES + 1).unwrap();
        let error = HomeViewModel::load(&root.0).unwrap_err();
        assert!(error.contains("limit"));
    }
}
