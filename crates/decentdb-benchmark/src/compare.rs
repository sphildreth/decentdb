use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::artifacts::{RunManifest, RunSummary};
use crate::cli::CompareArgs;
use crate::targets::{load_targets_catalog, TargetCatalog, TargetDirection};
use crate::types::ScenarioResult;

const BASELINE_ARTIFACT_KIND: &str = "decentdb_benchmark_baseline_snapshot";
const BASELINE_SCHEMA_VERSION: u32 = 1;
const COMPARE_ARTIFACT_KIND: &str = "decentdb_benchmark_compare";
const COMPARE_SCHEMA_VERSION: u32 = 1;
const EPSILON: f64 = 1e-12;
const NOISE_DEFAULT_ABSOLUTE: f64 = 0.0;
const NOISE_DEFAULT_RELATIVE: f64 = 0.10;

const NOISE_BAND_DEFAULTS: &[(&str, f64, f64)] = &[
    ("durable_commit_single.txn_p95_us", 50.0, 0.10),
    ("durable_commit_single.commit_p95_us", 50.0, 0.10),
    ("durable_commit_batch.rows_per_sec", 5000.0, 0.10),
    ("durable_commit_batch.batch_commit_p95_us", 200.0, 0.10),
    ("point_lookup_warm.lookup_p95_us", 5.0, 0.15),
    ("point_lookup_cold.first_read_p95_us", 100.0, 0.15),
    ("point_lookup_cold.cold_batch_p95_ms", 5.0, 0.15),
    ("range_scan_warm.rows_per_sec", 5000.0, 0.10),
    ("checkpoint.checkpoint_ms", 10.0, 0.10),
    ("recovery_reopen.reopen_p95_ms", 10.0, 0.10),
    ("recovery_reopen.first_query_p95_ms", 0.05, 0.10),
    ("read_under_write.reader_p95_degradation_ratio", 0.05, 0.10),
    (
        "read_under_write.writer_throughput_degradation_ratio",
        0.05,
        0.10,
    ),
    ("storage_efficiency.space_amplification", 0.02, 0.05),
];

#[derive(Debug, Clone)]
pub(crate) struct ComparableRunInput {
    pub summary_path: PathBuf,
    pub manifest_path: Option<PathBuf>,
    pub snapshot: ComparableRunSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BaselineSnapshot {
    pub artifact_kind: String,
    pub schema_version: u32,
    pub baseline_name: String,
    pub created_unix_ms: u128,
    pub source_summary: String,
    pub source_manifest: Option<String>,
    pub snapshot: ComparableRunSnapshot,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ComparableRunSnapshot {
    pub run_id: String,
    pub profile: String,
    pub status: String,
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub build_profile: Option<String>,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub git_sha: Option<String>,
    pub git_branch: Option<String>,
    pub summary_warnings: Vec<String>,
    pub metrics: Vec<MetricPoint>,
    pub storage: StorageSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetricPoint {
    pub scenario: String,
    pub metric: String,
    pub value: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct StorageSnapshot {
    pub bytes_per_logical_row: Option<f64>,
    pub db_file_bytes: Option<f64>,
    pub wal_file_bytes_peak: Option<f64>,
    pub space_amplification: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompareArtifact {
    pub artifact_kind: String,
    pub schema_version: u32,
    pub compare_id: String,
    pub created_unix_ms: u128,
    pub output_path: String,
    pub targets_file: String,
    pub targets_format_version: u32,
    pub candidate_summary: String,
    pub baseline_source: BaselineSource,
    pub context: CompareContext,
    pub strictness: CompareStrictness,
    pub totals: CompareTotals,
    pub metrics: Vec<MetricComparison>,
    pub top_regressions: Vec<MetricTrend>,
    pub top_improvements: Vec<MetricTrend>,
    pub optimization_opportunities: Vec<OptimizationOpportunity>,
    pub storage: Option<StorageComparison>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BaselineSource {
    pub baseline_name: Option<String>,
    pub baseline_summary: Option<String>,
    pub baseline_snapshot: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompareContext {
    pub candidate: RunContext,
    pub baseline: RunContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunContext {
    pub run_id: String,
    pub profile: String,
    pub status: String,
    pub build_profile: Option<String>,
    pub os: Option<String>,
    pub arch: Option<String>,
    pub git_sha: Option<String>,
    pub git_branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompareStrictness {
    pub strict: bool,
    pub meaningful: bool,
    pub incompatible_context: bool,
    pub candidate_authoritative: bool,
    pub baseline_authoritative: bool,
    pub comparison_authoritative: bool,
    pub reasons: Vec<String>,
    pub target_expectations: TargetExpectations,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TargetExpectations {
    pub authoritative_build: Option<String>,
    pub authoritative_benchmark_profile: Option<String>,
    pub authoritative_host_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CompareTotals {
    pub total_metrics: usize,
    pub regressions: usize,
    pub improvements: usize,
    pub unchanged_within_noise: usize,
    pub missing_metric: usize,
    pub missing_target_metadata: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MetricComparisonStatus {
    Improvement,
    Regression,
    UnchangedWithinNoise,
    MissingMetric,
    MissingTargetMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetricComparison {
    pub metric_id: String,
    pub target_id: Option<String>,
    pub scenario: String,
    pub metric: String,
    pub display_name: Option<String>,
    pub status: MetricComparisonStatus,
    pub direction: Option<TargetDirection>,
    pub unit: Option<String>,
    pub current_value: Option<f64>,
    pub baseline_value: Option<f64>,
    pub target_value: Option<f64>,
    pub delta_value: Option<f64>,
    pub delta_percent: Option<f64>,
    pub directional_delta_percent: Option<f64>,
    pub noise_band: Option<f64>,
    pub absolute_threshold: Option<f64>,
    pub relative_threshold: Option<f64>,
    pub delta_vs_target_value: Option<f64>,
    pub delta_vs_target_percent: Option<f64>,
    pub gap_to_target_ratio: Option<f64>,
    pub regression_beyond_noise_ratio: Option<f64>,
    pub floor_value: Option<f64>,
    pub stretch_value: Option<f64>,
    pub expected_cache_mode: Option<String>,
    pub expected_durability_mode: Option<String>,
    pub weight: Option<f64>,
    pub priority: Option<String>,
    pub signature: bool,
    pub likely_owners: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetricTrend {
    pub metric_id: String,
    pub scenario: String,
    pub metric: String,
    pub delta_percent: f64,
    pub current_value: f64,
    pub baseline_value: f64,
    pub likely_owners: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OptimizationOpportunity {
    pub metric_id: String,
    pub scenario: String,
    pub metric: String,
    pub current_value: Option<f64>,
    pub baseline_value: Option<f64>,
    pub target_value: Option<f64>,
    pub direction: TargetDirection,
    pub delta_percent: Option<f64>,
    pub status_relative_to_noise: MetricComparisonStatus,
    pub priority_score: f64,
    pub likely_owners: Vec<String>,
    pub components: OpportunityScoreComponents,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OpportunityScoreComponents {
    pub regression_beyond_noise_ratio: f64,
    pub gap_to_target_ratio: f64,
    pub weight: f64,
    pub priority_boost: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StorageComparison {
    pub candidate: StorageSnapshot,
    pub baseline: StorageSnapshot,
    pub delta_percent: StorageSnapshot,
}

pub(crate) fn run_compare_command(args: CompareArgs) -> Result<()> {
    validate_compare_args(&args)?;

    let candidate = load_comparable_run(&args.candidate)?;
    let targets = load_targets_catalog(&args.targets)?;

    let (baseline_snapshot, baseline_source) = if let Some(path) = &args.baseline {
        let baseline = load_comparable_run(path)?;
        (
            baseline.snapshot,
            BaselineSource {
                baseline_name: None,
                baseline_summary: Some(path.display().to_string()),
                baseline_snapshot: None,
            },
        )
    } else {
        let baseline_name = args.baseline_name.clone().unwrap_or_default();
        let baseline_path = baseline_file_path(&args.artifact_root, &baseline_name)?;
        let baseline: BaselineSnapshot = read_json_file(&baseline_path)?;
        if baseline.artifact_kind != BASELINE_ARTIFACT_KIND {
            return Err(anyhow!(
                "baseline snapshot {} has unsupported artifact kind {}",
                baseline_path.display(),
                baseline.artifact_kind
            ));
        }
        (
            baseline.snapshot,
            BaselineSource {
                baseline_name: Some(baseline_name),
                baseline_summary: Some(baseline.source_summary),
                baseline_snapshot: Some(baseline_path.display().to_string()),
            },
        )
    };

    let created_unix_ms = unix_millis_now()?;
    let compare_id = make_compare_id(
        created_unix_ms,
        &candidate.snapshot.run_id,
        &baseline_snapshot.run_id,
    );
    let compares_dir = args.artifact_root.join("compares");
    fs::create_dir_all(&compares_dir)
        .with_context(|| format!("create compare dir {}", compares_dir.display()))?;
    let output_path = compares_dir.join(format!("{compare_id}.json"));

    let mut compare = build_compare_artifact(
        &targets,
        &args.targets,
        &candidate.snapshot,
        &baseline_snapshot,
        &baseline_source,
        &args.candidate,
    );
    compare.compare_id = compare_id;
    compare.created_unix_ms = created_unix_ms;
    compare.output_path = output_path.display().to_string();

    write_json_file(&output_path, &compare)?;
    println!("{}", serde_json::to_string_pretty(&compare)?);
    Ok(())
}

pub(crate) fn load_comparable_run(summary_path: &Path) -> Result<ComparableRunInput> {
    let summary: RunSummary = read_json_file(summary_path)?;
    let summary_dir = summary_path.parent().ok_or_else(|| {
        anyhow!(
            "summary path {} has no parent directory",
            summary_path.display()
        )
    })?;

    let manifest_path = summary_dir.join("manifest.json");
    let manifest = if manifest_path.exists() {
        Some(
            read_json_file::<RunManifest>(&manifest_path)
                .with_context(|| format!("read manifest {}", manifest_path.display()))?,
        )
    } else {
        None
    };

    let mut metric_points = Vec::new();
    let mut run_warnings = summary.warnings.clone();
    if manifest.is_none() {
        run_warnings.push(format!(
            "manifest {} not found; comparison context is partial",
            manifest_path.display()
        ));
    }

    let mut storage = StorageSnapshot::default();

    for scenario in &summary.scenarios {
        let scenario_path = resolve_scenario_path(summary_dir, scenario)?;
        let scenario_artifact: ScenarioResult = read_json_file(&scenario_path)
            .with_context(|| format!("read scenario artifact {}", scenario_path.display()))?;

        for (metric, value) in scenario_artifact.metrics {
            if let Some(number) = value.as_f64() {
                metric_points.push(MetricPoint {
                    scenario: scenario_artifact.scenario_id.as_str().to_string(),
                    metric: metric.clone(),
                    value: number,
                });

                if scenario_artifact.scenario_id.as_str() == "storage_efficiency" {
                    match metric.as_str() {
                        "bytes_per_logical_row" => storage.bytes_per_logical_row = Some(number),
                        "db_file_bytes" => storage.db_file_bytes = Some(number),
                        "wal_file_bytes_peak" => storage.wal_file_bytes_peak = Some(number),
                        "space_amplification" => storage.space_amplification = Some(number),
                        _ => {}
                    }
                }
            }
        }
    }

    let snapshot = ComparableRunSnapshot {
        run_id: summary.run_id,
        profile: summary.profile.as_str().to_string(),
        status: summary.status,
        started_unix_ms: summary.started_unix_ms,
        finished_unix_ms: summary.finished_unix_ms,
        build_profile: manifest
            .as_ref()
            .map(|value| value.environment.build_profile.clone()),
        os: manifest.as_ref().map(|value| value.environment.os.clone()),
        arch: manifest
            .as_ref()
            .map(|value| value.environment.arch.clone()),
        git_sha: manifest
            .as_ref()
            .and_then(|value| value.environment.git_sha.clone()),
        git_branch: manifest
            .as_ref()
            .and_then(|value| value.environment.git_branch.clone()),
        summary_warnings: run_warnings,
        metrics: metric_points,
        storage,
    };

    Ok(ComparableRunInput {
        summary_path: summary_path.to_path_buf(),
        manifest_path: manifest.map(|_| manifest_path),
        snapshot,
    })
}

pub(crate) fn baseline_snapshot_from_input(
    baseline_name: &str,
    input: &ComparableRunInput,
) -> Result<BaselineSnapshot> {
    validate_baseline_name(baseline_name)?;
    Ok(BaselineSnapshot {
        artifact_kind: BASELINE_ARTIFACT_KIND.to_string(),
        schema_version: BASELINE_SCHEMA_VERSION,
        baseline_name: baseline_name.to_string(),
        created_unix_ms: unix_millis_now()?,
        source_summary: input.summary_path.display().to_string(),
        source_manifest: input
            .manifest_path
            .as_ref()
            .map(|path| path.display().to_string()),
        snapshot: input.snapshot.clone(),
        warnings: input.snapshot.summary_warnings.clone(),
    })
}

pub(crate) fn baseline_file_path(artifact_root: &Path, baseline_name: &str) -> Result<PathBuf> {
    validate_baseline_name(baseline_name)?;
    Ok(artifact_root
        .join("baselines")
        .join(format!("{baseline_name}.json")))
}

pub(crate) fn read_compare_artifact(path: &Path) -> Result<CompareArtifact> {
    read_json_file(path)
}

fn build_compare_artifact(
    targets: &TargetCatalog,
    targets_path: &Path,
    candidate: &ComparableRunSnapshot,
    baseline: &ComparableRunSnapshot,
    baseline_source: &BaselineSource,
    candidate_summary_path: &Path,
) -> CompareArtifact {
    let mut target_by_key = BTreeMap::new();
    for target in &targets.metrics {
        target_by_key.insert(metric_id(&target.scenario, &target.metric), target);
    }

    let candidate_metrics = metrics_to_map(candidate);
    let baseline_metrics = metrics_to_map(baseline);

    let mut keys = BTreeSet::new();
    for key in candidate_metrics.keys() {
        keys.insert(key.clone());
    }
    for key in baseline_metrics.keys() {
        keys.insert(key.clone());
    }
    for key in target_by_key.keys() {
        keys.insert(key.clone());
    }

    let mut metrics = Vec::with_capacity(keys.len());
    let mut opportunities = Vec::new();
    let mut regressions = Vec::new();
    let mut improvements = Vec::new();
    let mut warnings = Vec::new();

    let mut totals = CompareTotals {
        total_metrics: 0,
        regressions: 0,
        improvements: 0,
        unchanged_within_noise: 0,
        missing_metric: 0,
        missing_target_metadata: 0,
    };

    for key in keys {
        totals.total_metrics += 1;

        let (scenario, metric) = split_metric_id(&key);
        let candidate_value = candidate_metrics.get(&key).copied();
        let baseline_value = baseline_metrics.get(&key).copied();
        let target = target_by_key.get(&key).copied();

        let mut comparison = MetricComparison {
            metric_id: key.clone(),
            target_id: target.map(|value| value.id.clone()),
            scenario: scenario.to_string(),
            metric: metric.to_string(),
            display_name: target.and_then(|value| value.display_name.clone()),
            status: MetricComparisonStatus::MissingTargetMetadata,
            direction: target.map(|value| value.direction),
            unit: target.map(|value| value.unit.clone()),
            current_value: candidate_value,
            baseline_value,
            target_value: target.and_then(|value| value.target),
            delta_value: None,
            delta_percent: None,
            directional_delta_percent: None,
            noise_band: None,
            absolute_threshold: None,
            relative_threshold: None,
            delta_vs_target_value: None,
            delta_vs_target_percent: None,
            gap_to_target_ratio: None,
            regression_beyond_noise_ratio: None,
            floor_value: target.and_then(|value| value.floor),
            stretch_value: target.and_then(|value| value.stretch),
            expected_cache_mode: target.and_then(|value| value.cache_mode.clone()),
            expected_durability_mode: target.and_then(|value| value.durability_mode.clone()),
            weight: target.map(|value| value.weight),
            priority: target.map(|value| value.priority.clone()),
            signature: target.is_some_and(|value| value.signature),
            likely_owners: target
                .map(|value| value.likely_owners.clone())
                .unwrap_or_default(),
        };

        let Some(target_metric) = target else {
            comparison.status = MetricComparisonStatus::MissingTargetMetadata;
            totals.missing_target_metadata += 1;
            metrics.push(comparison);
            continue;
        };

        if candidate_value.is_none() || baseline_value.is_none() {
            comparison.status = MetricComparisonStatus::MissingMetric;
            totals.missing_metric += 1;
            if candidate_value.is_none() {
                warnings.push(format!("candidate missing metric {}", key));
            }
            if baseline_value.is_none() {
                warnings.push(format!("baseline missing metric {}", key));
            }
            if let Some(current) = candidate_value {
                let gap_to_target_ratio =
                    normalized_target_gap(current, target_metric.target, target_metric.direction);
                comparison.gap_to_target_ratio = Some(gap_to_target_ratio);
                if let Some(target_value) = target_metric.target {
                    comparison.delta_vs_target_value = Some(current - target_value);
                    comparison.delta_vs_target_percent =
                        pct_delta(current - target_value, target_value);
                }
            }
            metrics.push(comparison);
            continue;
        }

        let current = candidate_value.unwrap_or_default();
        let previous = baseline_value.unwrap_or_default();

        let (absolute_threshold, relative_threshold) = noise_thresholds_for(&key);
        let noise_band = absolute_threshold.max(relative_threshold * previous.abs());
        comparison.absolute_threshold = Some(absolute_threshold);
        comparison.relative_threshold = Some(relative_threshold);
        comparison.noise_band = Some(noise_band);

        let delta = current - previous;
        let delta_pct = pct_delta(delta, previous);
        let directional_delta_ratio =
            directional_delta_ratio(current, previous, target_metric.direction);
        let directional_delta_percent = directional_delta_ratio * 100.0;
        let noise_ratio = noise_ratio(previous, noise_band);
        let abs_delta = delta.abs();

        comparison.delta_value = Some(delta);
        comparison.delta_percent = delta_pct;
        comparison.directional_delta_percent = Some(directional_delta_percent);

        let gap_to_target_ratio =
            normalized_target_gap(current, target_metric.target, target_metric.direction);
        comparison.gap_to_target_ratio = Some(gap_to_target_ratio);
        if let Some(target_value) = target_metric.target {
            comparison.delta_vs_target_value = Some(current - target_value);
            comparison.delta_vs_target_percent = pct_delta(current - target_value, target_value);
        }

        let regression_beyond_noise_ratio = (0.0_f64).max(-directional_delta_ratio - noise_ratio);
        comparison.regression_beyond_noise_ratio = Some(regression_beyond_noise_ratio);

        if abs_delta <= noise_band {
            comparison.status = MetricComparisonStatus::UnchangedWithinNoise;
            totals.unchanged_within_noise += 1;
        } else if directional_delta_ratio > 0.0 {
            comparison.status = MetricComparisonStatus::Improvement;
            totals.improvements += 1;
            improvements.push(MetricTrend {
                metric_id: key.clone(),
                scenario: scenario.to_string(),
                metric: metric.to_string(),
                delta_percent: directional_delta_percent,
                current_value: current,
                baseline_value: previous,
                likely_owners: target_metric.likely_owners.clone(),
            });
        } else {
            comparison.status = MetricComparisonStatus::Regression;
            totals.regressions += 1;
            regressions.push(MetricTrend {
                metric_id: key.clone(),
                scenario: scenario.to_string(),
                metric: metric.to_string(),
                delta_percent: directional_delta_percent,
                current_value: current,
                baseline_value: previous,
                likely_owners: target_metric.likely_owners.clone(),
            });
        }

        let priority_boost = priority_boost(&target_metric.priority, target_metric.signature);
        let severity = regression_beyond_noise_ratio.max(gap_to_target_ratio);
        if severity > 0.0 {
            let score = target_metric.weight * priority_boost * severity * 100.0;
            opportunities.push(OptimizationOpportunity {
                metric_id: key.clone(),
                scenario: scenario.to_string(),
                metric: metric.to_string(),
                current_value: Some(current),
                baseline_value: Some(previous),
                target_value: target_metric.target,
                direction: target_metric.direction,
                delta_percent: comparison.delta_percent,
                status_relative_to_noise: comparison.status,
                priority_score: score,
                likely_owners: target_metric.likely_owners.clone(),
                components: OpportunityScoreComponents {
                    regression_beyond_noise_ratio,
                    gap_to_target_ratio,
                    weight: target_metric.weight,
                    priority_boost,
                },
            });
        }

        metrics.push(comparison);
    }

    regressions.sort_by(|left, right| {
        right
            .delta_percent
            .total_cmp(&left.delta_percent)
            .then_with(|| left.metric_id.cmp(&right.metric_id))
    });
    improvements.sort_by(|left, right| {
        right
            .delta_percent
            .total_cmp(&left.delta_percent)
            .then_with(|| left.metric_id.cmp(&right.metric_id))
    });
    opportunities.sort_by(|left, right| {
        right
            .priority_score
            .total_cmp(&left.priority_score)
            .then_with(|| left.metric_id.cmp(&right.metric_id))
    });

    let strictness = compare_strictness(candidate, baseline, targets);
    if strictness.incompatible_context {
        warnings.extend(strictness.reasons.iter().cloned());
    }

    warnings.extend(
        candidate
            .summary_warnings
            .iter()
            .map(|warning| format!("candidate warning: {warning}")),
    );
    warnings.extend(
        baseline
            .summary_warnings
            .iter()
            .map(|warning| format!("baseline warning: {warning}")),
    );

    CompareArtifact {
        artifact_kind: COMPARE_ARTIFACT_KIND.to_string(),
        schema_version: COMPARE_SCHEMA_VERSION,
        compare_id: String::new(),
        created_unix_ms: 0,
        output_path: String::new(),
        targets_file: targets_path.display().to_string(),
        targets_format_version: targets.format_version,
        candidate_summary: candidate_summary_path.display().to_string(),
        baseline_source: baseline_source.clone(),
        context: CompareContext {
            candidate: snapshot_context(candidate),
            baseline: snapshot_context(baseline),
        },
        strictness,
        totals,
        metrics,
        top_regressions: regressions.into_iter().take(5).collect(),
        top_improvements: improvements.into_iter().take(5).collect(),
        optimization_opportunities: opportunities,
        storage: compare_storage(candidate, baseline),
        warnings,
    }
}

fn compare_storage(
    candidate: &ComparableRunSnapshot,
    baseline: &ComparableRunSnapshot,
) -> Option<StorageComparison> {
    if storage_empty(&candidate.storage) && storage_empty(&baseline.storage) {
        return None;
    }

    Some(StorageComparison {
        candidate: candidate.storage.clone(),
        baseline: baseline.storage.clone(),
        delta_percent: StorageSnapshot {
            bytes_per_logical_row: pct_pair(
                candidate.storage.bytes_per_logical_row,
                baseline.storage.bytes_per_logical_row,
            ),
            db_file_bytes: pct_pair(
                candidate.storage.db_file_bytes,
                baseline.storage.db_file_bytes,
            ),
            wal_file_bytes_peak: pct_pair(
                candidate.storage.wal_file_bytes_peak,
                baseline.storage.wal_file_bytes_peak,
            ),
            space_amplification: pct_pair(
                candidate.storage.space_amplification,
                baseline.storage.space_amplification,
            ),
        },
    })
}

fn pct_pair(candidate: Option<f64>, baseline: Option<f64>) -> Option<f64> {
    let candidate = candidate?;
    let baseline = baseline?;
    pct_delta(candidate - baseline, baseline)
}

fn storage_empty(storage: &StorageSnapshot) -> bool {
    storage.bytes_per_logical_row.is_none()
        && storage.db_file_bytes.is_none()
        && storage.wal_file_bytes_peak.is_none()
        && storage.space_amplification.is_none()
}

fn compare_strictness(
    candidate: &ComparableRunSnapshot,
    baseline: &ComparableRunSnapshot,
    targets: &TargetCatalog,
) -> CompareStrictness {
    let mut reasons = Vec::new();

    compare_optional(
        "profile",
        Some(&candidate.profile),
        Some(&baseline.profile),
        &mut reasons,
    );
    compare_optional(
        "build_profile",
        candidate.build_profile.as_deref(),
        baseline.build_profile.as_deref(),
        &mut reasons,
    );
    compare_optional(
        "os",
        candidate.os.as_deref(),
        baseline.os.as_deref(),
        &mut reasons,
    );
    compare_optional(
        "arch",
        candidate.arch.as_deref(),
        baseline.arch.as_deref(),
        &mut reasons,
    );
    compare_status("candidate", &candidate.status, &mut reasons);
    compare_status("baseline", &baseline.status, &mut reasons);

    let strict = reasons.is_empty();
    let candidate_authoritative = is_authoritative_snapshot(candidate, targets);
    let baseline_authoritative = is_authoritative_snapshot(baseline, targets);

    CompareStrictness {
        strict,
        meaningful: strict,
        incompatible_context: !strict,
        candidate_authoritative,
        baseline_authoritative,
        comparison_authoritative: strict && candidate_authoritative && baseline_authoritative,
        reasons,
        target_expectations: TargetExpectations {
            authoritative_build: targets.authoritative_build.clone(),
            authoritative_benchmark_profile: targets.authoritative_benchmark_profile.clone(),
            authoritative_host_class: targets.authoritative_host_class.clone(),
        },
    }
}

fn is_authoritative_snapshot(snapshot: &ComparableRunSnapshot, targets: &TargetCatalog) -> bool {
    let build_ok = targets
        .authoritative_build
        .as_deref()
        .is_none_or(|expected| snapshot.build_profile.as_deref() == Some(expected));
    let profile_ok = targets
        .authoritative_benchmark_profile
        .as_deref()
        .is_none_or(|expected| snapshot.profile == expected);
    build_ok && profile_ok
}

fn compare_optional(
    field: &str,
    candidate: Option<&str>,
    baseline: Option<&str>,
    reasons: &mut Vec<String>,
) {
    match (candidate, baseline) {
        (Some(left), Some(right)) if left == right => {}
        (Some(left), Some(right)) => reasons.push(format!(
            "comparison context mismatch: {field} candidate={left} baseline={right}"
        )),
        _ => reasons.push(format!(
            "comparison context missing: {field} candidate={:?} baseline={:?}",
            candidate, baseline
        )),
    }
}

fn compare_status(label: &str, status: &str, reasons: &mut Vec<String>) {
    if status != "passed" {
        reasons.push(format!(
            "comparison input is not a passed run: {label}.status={status}"
        ));
    }
}

fn snapshot_context(snapshot: &ComparableRunSnapshot) -> RunContext {
    RunContext {
        run_id: snapshot.run_id.clone(),
        profile: snapshot.profile.clone(),
        status: snapshot.status.clone(),
        build_profile: snapshot.build_profile.clone(),
        os: snapshot.os.clone(),
        arch: snapshot.arch.clone(),
        git_sha: snapshot.git_sha.clone(),
        git_branch: snapshot.git_branch.clone(),
    }
}

fn metrics_to_map(snapshot: &ComparableRunSnapshot) -> BTreeMap<String, f64> {
    let mut values = BTreeMap::new();
    for point in &snapshot.metrics {
        values.insert(metric_id(&point.scenario, &point.metric), point.value);
    }
    values
}

fn resolve_scenario_path(
    summary_dir: &Path,
    scenario: &crate::artifacts::ScenarioSummary,
) -> Result<PathBuf> {
    let declared = PathBuf::from(&scenario.artifact_file);
    if declared.exists() {
        return Ok(declared);
    }

    let candidate = summary_dir
        .join("scenarios")
        .join(format!("{}.json", scenario.scenario_id.as_str()));
    if candidate.exists() {
        return Ok(candidate);
    }

    Err(anyhow!(
        "scenario artifact not found for {} (declared path {}, fallback {})",
        scenario.scenario_id.as_str(),
        scenario.artifact_file,
        candidate.display()
    ))
}

fn validate_compare_args(args: &CompareArgs) -> Result<()> {
    match (&args.baseline, &args.baseline_name) {
        (Some(_), Some(_)) => Err(anyhow!(
            "provide either --baseline or --baseline-name, not both"
        )),
        (None, None) => Err(anyhow!(
            "missing baseline input; provide --baseline or --baseline-name"
        )),
        _ => Ok(()),
    }
}

fn validate_baseline_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow!("baseline name must not be empty"));
    }
    if name
        .chars()
        .all(|value| value.is_ascii_alphanumeric() || value == '-' || value == '_' || value == '.')
    {
        Ok(())
    } else {
        Err(anyhow!(
            "baseline name must match [A-Za-z0-9._-]+, got {}",
            name
        ))
    }
}

fn noise_thresholds_for(metric_id: &str) -> (f64, f64) {
    NOISE_BAND_DEFAULTS
        .iter()
        .find(|(name, _, _)| *name == metric_id)
        .map(|(_, absolute, relative)| (*absolute, *relative))
        .unwrap_or((NOISE_DEFAULT_ABSOLUTE, NOISE_DEFAULT_RELATIVE))
}

fn priority_boost(priority: &str, signature: bool) -> f64 {
    let mut boost = 1.0;
    if priority.eq_ignore_ascii_case("p0") {
        boost += 0.35;
    }
    if signature {
        boost += 0.25;
    }
    boost
}

fn directional_delta_ratio(candidate: f64, baseline: f64, direction: TargetDirection) -> f64 {
    let denom = baseline.abs().max(EPSILON);
    match direction {
        TargetDirection::SmallerIsBetter => (baseline - candidate) / denom,
        TargetDirection::LargerIsBetter => (candidate - baseline) / denom,
    }
}

fn normalized_target_gap(candidate: f64, target: Option<f64>, direction: TargetDirection) -> f64 {
    let Some(target) = target else {
        return 0.0;
    };

    let denom = target.abs().max(EPSILON);
    match direction {
        TargetDirection::SmallerIsBetter => ((candidate - target) / denom).max(0.0),
        TargetDirection::LargerIsBetter => ((target - candidate) / denom).max(0.0),
    }
}

fn noise_ratio(baseline: f64, noise_band: f64) -> f64 {
    noise_band / baseline.abs().max(EPSILON)
}

fn metric_id(scenario: &str, metric: &str) -> String {
    format!("{scenario}.{metric}")
}

fn split_metric_id(metric_id: &str) -> (&str, &str) {
    let Some((scenario, metric)) = metric_id.split_once('.') else {
        return ("unknown", metric_id);
    };
    (scenario, metric)
}

fn pct_delta(delta: f64, baseline: f64) -> Option<f64> {
    if baseline.abs() < EPSILON {
        None
    } else {
        Some((delta / baseline) * 100.0)
    }
}

fn make_compare_id(created_unix_ms: u128, candidate_run_id: &str, baseline_run_id: &str) -> String {
    format!(
        "unix-{created_unix_ms}-{}-vs-{}",
        sanitize_for_id(candidate_run_id),
        sanitize_for_id(baseline_run_id)
    )
}

fn sanitize_for_id(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn unix_millis_now() -> Result<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .with_context(|| "system clock is before UNIX_EPOCH".to_string())?
        .as_millis())
}

fn read_json_file<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read json {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse json {}", path.display()))
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("write json {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{
        baseline_snapshot_from_input, build_compare_artifact, compare_strictness, metric_id,
        noise_thresholds_for, BaselineSource, ComparableRunInput, ComparableRunSnapshot,
        MetricComparisonStatus, MetricPoint, StorageSnapshot,
    };
    use crate::targets::{TargetCatalog, TargetDirection, TargetMetricSpec};

    #[test]
    fn noise_thresholds_cover_space_amplification() {
        let (abs, rel) = noise_thresholds_for("storage_efficiency.space_amplification");
        assert_eq!(abs, 0.02);
        assert_eq!(rel, 0.05);
    }

    #[test]
    fn compare_marks_regressions_and_opportunities() {
        let candidate = ComparableRunSnapshot {
            run_id: "cand".to_string(),
            profile: "nightly".to_string(),
            status: "passed".to_string(),
            started_unix_ms: 1,
            finished_unix_ms: 2,
            build_profile: Some("release".to_string()),
            os: Some("linux".to_string()),
            arch: Some("x86_64".to_string()),
            git_sha: Some("abc".to_string()),
            git_branch: Some("main".to_string()),
            summary_warnings: Vec::new(),
            metrics: vec![MetricPoint {
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                value: 1200.0,
            }],
            storage: StorageSnapshot::default(),
        };

        let baseline = ComparableRunSnapshot {
            run_id: "base".to_string(),
            profile: "nightly".to_string(),
            status: "passed".to_string(),
            started_unix_ms: 1,
            finished_unix_ms: 2,
            build_profile: Some("release".to_string()),
            os: Some("linux".to_string()),
            arch: Some("x86_64".to_string()),
            git_sha: Some("abc".to_string()),
            git_branch: Some("main".to_string()),
            summary_warnings: Vec::new(),
            metrics: vec![MetricPoint {
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                value: 1000.0,
            }],
            storage: StorageSnapshot::default(),
        };

        let targets = TargetCatalog {
            format_version: 1,
            authoritative_build: Some("release".to_string()),
            authoritative_benchmark_profile: Some("nightly".to_string()),
            authoritative_host_class: None,
            metrics: vec![TargetMetricSpec {
                id: metric_id("durable_commit_single", "txn_p95_us"),
                display_name: None,
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                priority: "P0".to_string(),
                signature: true,
                direction: TargetDirection::SmallerIsBetter,
                unit: "microseconds".to_string(),
                weight: 1.0,
                floor: Some(2000.0),
                target: Some(800.0),
                stretch: None,
                cache_mode: None,
                durability_mode: None,
                likely_owners: vec!["wal".to_string()],
            }],
        };

        let artifact = build_compare_artifact(
            &targets,
            std::path::Path::new("benchmarks/targets.toml"),
            &candidate,
            &baseline,
            &BaselineSource {
                baseline_name: Some("local".to_string()),
                baseline_summary: Some("baseline-summary.json".to_string()),
                baseline_snapshot: Some("baseline.json".to_string()),
            },
            std::path::Path::new("candidate-summary.json"),
        );

        assert_eq!(artifact.totals.regressions, 1);
        assert_eq!(artifact.metrics.len(), 1);
        assert_eq!(
            artifact.metrics[0].status,
            MetricComparisonStatus::Regression
        );
        assert!(!artifact.optimization_opportunities.is_empty());
    }

    #[test]
    fn baseline_snapshot_copies_run_snapshot() {
        let input = ComparableRunInput {
            summary_path: std::path::PathBuf::from("/tmp/summary.json"),
            manifest_path: None,
            snapshot: ComparableRunSnapshot {
                run_id: "run-1".to_string(),
                profile: "smoke".to_string(),
                status: "passed".to_string(),
                started_unix_ms: 1,
                finished_unix_ms: 2,
                build_profile: Some("debug".to_string()),
                os: Some("linux".to_string()),
                arch: Some("x86_64".to_string()),
                git_sha: None,
                git_branch: None,
                summary_warnings: vec!["warning".to_string()],
                metrics: vec![MetricPoint {
                    scenario: "s".to_string(),
                    metric: "m".to_string(),
                    value: 1.0,
                }],
                storage: StorageSnapshot::default(),
            },
        };

        let snapshot = baseline_snapshot_from_input("local-smoke", &input).expect("snapshot");
        assert_eq!(snapshot.baseline_name, "local-smoke");
        assert_eq!(snapshot.snapshot.run_id, "run-1");
        assert_eq!(snapshot.source_summary, "/tmp/summary.json");
    }

    #[test]
    fn strictness_rejects_non_passed_inputs() {
        let candidate = ComparableRunSnapshot {
            run_id: "cand".to_string(),
            profile: "nightly".to_string(),
            status: "failed".to_string(),
            started_unix_ms: 1,
            finished_unix_ms: 2,
            build_profile: Some("release".to_string()),
            os: Some("linux".to_string()),
            arch: Some("x86_64".to_string()),
            git_sha: None,
            git_branch: None,
            summary_warnings: Vec::new(),
            metrics: Vec::new(),
            storage: StorageSnapshot::default(),
        };
        let baseline = ComparableRunSnapshot {
            run_id: "base".to_string(),
            profile: "nightly".to_string(),
            status: "passed".to_string(),
            started_unix_ms: 1,
            finished_unix_ms: 2,
            build_profile: Some("release".to_string()),
            os: Some("linux".to_string()),
            arch: Some("x86_64".to_string()),
            git_sha: None,
            git_branch: None,
            summary_warnings: Vec::new(),
            metrics: Vec::new(),
            storage: StorageSnapshot::default(),
        };
        let targets = TargetCatalog {
            format_version: 1,
            authoritative_build: Some("release".to_string()),
            authoritative_benchmark_profile: Some("nightly".to_string()),
            authoritative_host_class: None,
            metrics: Vec::new(),
        };

        let strictness = compare_strictness(&candidate, &baseline, &targets);

        assert!(!strictness.strict);
        assert!(!strictness.meaningful);
        assert!(strictness.incompatible_context);
        assert!(strictness
            .reasons
            .iter()
            .any(|reason| reason.contains("candidate.status=failed")));
    }
}
