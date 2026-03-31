use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::types::ScenarioResult;

pub(crate) const DEFAULT_TARGETS_PATH: &str = "benchmarks/targets.toml";

#[derive(Debug, Deserialize)]
struct TargetsFile {
    format_version: u32,
    metadata: Option<TargetsMetadata>,
    rating: RatingConfig,
    metric: Vec<TargetMetric>,
}

#[derive(Debug, Deserialize)]
struct TargetsMetadata {
    authoritative_host_class: Option<String>,
    authoritative_build: Option<String>,
    authoritative_benchmark_profile: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RatingConfig {
    gold_min_signature_metrics_at_stretch: usize,
    elite_min_signature_metrics_at_stretch: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct TargetMetric {
    id: String,
    display_name: Option<String>,
    scenario: String,
    metric: String,
    priority: String,
    signature: bool,
    direction: TargetDirection,
    unit: String,
    weight: f64,
    floor: Option<f64>,
    target: Option<f64>,
    stretch: Option<f64>,
    cache_mode: Option<String>,
    durability_mode: Option<String>,
    likely_owners: Vec<String>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TargetDirection {
    SmallerIsBetter,
    LargerIsBetter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunTargetAssessment {
    pub targets_file: String,
    pub format_version: u32,
    pub authoritative_context: bool,
    pub scope: String,
    pub overall_grade: Option<String>,
    pub matched_metrics: usize,
    pub total_metrics: usize,
    pub matched_signature_metrics: usize,
    pub total_signature_metrics: usize,
    pub gradeable_metrics: usize,
    pub metrics: Vec<MetricAssessment>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MetricAssessment {
    pub target_id: String,
    pub scenario: String,
    pub metric: String,
    pub priority: String,
    pub signature: bool,
    pub direction: TargetDirection,
    pub unit: String,
    pub weight: f64,
    pub status: String,
    pub current: Option<f64>,
    pub floor: Option<f64>,
    pub target: Option<f64>,
    pub stretch: Option<f64>,
    pub likely_owners: Vec<String>,
    pub cache_mode: Option<String>,
    pub durability_mode: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct TargetCatalog {
    pub format_version: u32,
    pub authoritative_build: Option<String>,
    pub authoritative_benchmark_profile: Option<String>,
    pub authoritative_host_class: Option<String>,
    pub metrics: Vec<TargetMetricSpec>,
}

#[derive(Debug, Clone)]
pub(crate) struct TargetMetricSpec {
    pub id: String,
    pub display_name: Option<String>,
    pub scenario: String,
    pub metric: String,
    pub priority: String,
    pub signature: bool,
    pub direction: TargetDirection,
    pub unit: String,
    pub weight: f64,
    pub floor: Option<f64>,
    pub target: Option<f64>,
    pub stretch: Option<f64>,
    pub cache_mode: Option<String>,
    pub durability_mode: Option<String>,
    pub likely_owners: Vec<String>,
}

pub(crate) fn default_targets_path() -> PathBuf {
    PathBuf::from(DEFAULT_TARGETS_PATH)
}

pub(crate) fn load_targets_catalog(targets_path: &Path) -> Result<TargetCatalog> {
    let contents = fs::read_to_string(targets_path)
        .with_context(|| format!("read targets file {}", targets_path.display()))?;
    let targets: TargetsFile = toml::from_str(&contents)
        .with_context(|| format!("parse targets file {}", targets_path.display()))?;
    let metrics = targets
        .metric
        .into_iter()
        .map(|metric| TargetMetricSpec {
            id: metric.id,
            display_name: metric.display_name,
            scenario: metric.scenario,
            metric: metric.metric,
            priority: metric.priority,
            signature: metric.signature,
            direction: metric.direction,
            unit: metric.unit,
            weight: metric.weight,
            floor: metric.floor,
            target: metric.target,
            stretch: metric.stretch,
            cache_mode: metric.cache_mode,
            durability_mode: metric.durability_mode,
            likely_owners: metric.likely_owners,
        })
        .collect();

    Ok(TargetCatalog {
        format_version: targets.format_version,
        authoritative_build: targets
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.authoritative_build.clone()),
        authoritative_benchmark_profile: targets
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.authoritative_benchmark_profile.clone()),
        authoritative_host_class: targets
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.authoritative_host_class.clone()),
        metrics,
    })
}

pub(crate) fn assess_run(
    targets_path: &Path,
    run_profile: &str,
    build_profile: &str,
    scenario_results: &[ScenarioResult],
) -> Result<RunTargetAssessment> {
    let contents = fs::read_to_string(targets_path)
        .with_context(|| format!("read targets file {}", targets_path.display()))?;
    let targets: TargetsFile = toml::from_str(&contents)
        .with_context(|| format!("parse targets file {}", targets_path.display()))?;

    let mut results_by_scenario = BTreeMap::new();
    for result in scenario_results {
        results_by_scenario.insert(result.scenario_id.as_str(), result);
    }

    let authoritative_context = targets
        .metadata
        .as_ref()
        .map(|metadata| {
            let build_matches = metadata
                .authoritative_build
                .as_deref()
                .is_none_or(|value| value == build_profile);
            let profile_matches = metadata
                .authoritative_benchmark_profile
                .as_deref()
                .is_none_or(|value| value == run_profile);
            build_matches && profile_matches
        })
        .unwrap_or(true);

    let total_metrics = targets.metric.len();
    let total_signature_metrics = targets
        .metric
        .iter()
        .filter(|metric| metric.signature)
        .count();
    let mut matched_metrics = 0_usize;
    let mut matched_signature_metrics = 0_usize;
    let mut gradeable_metrics = 0_usize;
    let mut warnings = Vec::new();
    let mut assessments = Vec::with_capacity(total_metrics);

    if !authoritative_context {
        let expected_build = targets
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.authoritative_build.as_deref())
            .unwrap_or("unspecified");
        let expected_profile = targets
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.authoritative_benchmark_profile.as_deref())
            .unwrap_or("unspecified");
        warnings.push(format!(
            "run is not in the authoritative grading context; expected build={} profile={}, got build={} profile={}",
            expected_build, expected_profile, build_profile, run_profile
        ));
    }

    let mut below_floor = 0_usize;
    let mut below_target = 0_usize;
    let mut signature_stretch_met = 0_usize;

    for target in &targets.metric {
        let Some(result) = results_by_scenario.get(target.scenario.as_str()) else {
            assessments.push(missing_assessment(target, "missing_scenario"));
            continue;
        };

        if let Some(expected_cache_mode) = &target.cache_mode {
            if result.cache_mode != *expected_cache_mode {
                warnings.push(format!(
                    "target {} expected cache_mode={}, run has {}",
                    target.id, expected_cache_mode, result.cache_mode
                ));
                assessments.push(missing_assessment(target, "cache_mode_mismatch"));
                continue;
            }
        }

        if let Some(expected_durability_mode) = &target.durability_mode {
            if result.durability_mode != *expected_durability_mode {
                warnings.push(format!(
                    "target {} expected durability_mode={}, run has {}",
                    target.id, expected_durability_mode, result.durability_mode
                ));
                assessments.push(missing_assessment(target, "durability_mode_mismatch"));
                continue;
            }
        }

        let Some(raw_value) = result.metrics.get(&target.metric) else {
            assessments.push(missing_assessment(target, "missing_metric"));
            continue;
        };

        let Some(current) = raw_value.as_f64() else {
            assessments.push(missing_assessment(target, "non_numeric_metric"));
            continue;
        };

        matched_metrics += 1;
        if target.signature {
            matched_signature_metrics += 1;
        }

        let status = metric_status(target, current);
        if matches!(
            status,
            MetricStatus::BelowFloor
                | MetricStatus::BelowTarget
                | MetricStatus::TargetMet
                | MetricStatus::StretchMet
        ) {
            gradeable_metrics += 1;
        }
        if matches!(status, MetricStatus::BelowFloor) {
            below_floor += 1;
        } else if matches!(status, MetricStatus::BelowTarget) {
            below_target += 1;
        } else if matches!(status, MetricStatus::StretchMet) && target.signature {
            signature_stretch_met += 1;
        }

        assessments.push(MetricAssessment {
            target_id: target.id.clone(),
            scenario: target.scenario.clone(),
            metric: target.metric.clone(),
            priority: target.priority.clone(),
            signature: target.signature,
            direction: target.direction,
            unit: target.unit.clone(),
            weight: target.weight,
            status: status.as_str().to_string(),
            current: Some(current),
            floor: target.floor,
            target: target.target,
            stretch: target.stretch,
            likely_owners: target.likely_owners.clone(),
            cache_mode: target.cache_mode.clone(),
            durability_mode: target.durability_mode.clone(),
        });
    }

    let scope_complete = gradeable_metrics == total_metrics;
    if !scope_complete {
        warnings.push(format!(
            "overall grade withheld because only {gradeable_metrics}/{total_metrics} target metrics were available for this run"
        ));
    }

    let overall_grade = if scope_complete {
        Some(if below_floor > 0 {
            "red".to_string()
        } else if below_target > 0 {
            "yellow".to_string()
        } else if signature_stretch_met >= targets.rating.elite_min_signature_metrics_at_stretch {
            "elite".to_string()
        } else if signature_stretch_met >= targets.rating.gold_min_signature_metrics_at_stretch {
            "gold".to_string()
        } else {
            "green".to_string()
        })
    } else {
        None
    };

    Ok(RunTargetAssessment {
        targets_file: targets_path.display().to_string(),
        format_version: targets.format_version,
        authoritative_context,
        scope: if scope_complete {
            "complete".to_string()
        } else {
            "partial".to_string()
        },
        overall_grade,
        matched_metrics,
        total_metrics,
        matched_signature_metrics,
        total_signature_metrics,
        gradeable_metrics,
        metrics: assessments,
        warnings,
    })
}

#[derive(Debug, Clone, Copy)]
enum MetricStatus {
    BelowFloor,
    BelowTarget,
    TargetMet,
    StretchMet,
}

impl MetricStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::BelowFloor => "below_floor",
            Self::BelowTarget => "below_target",
            Self::TargetMet => "target_met",
            Self::StretchMet => "stretch_met",
        }
    }
}

fn metric_status(target: &TargetMetric, current: f64) -> MetricStatus {
    match target.direction {
        TargetDirection::SmallerIsBetter => {
            if target.floor.is_some_and(|floor| current > floor) {
                MetricStatus::BelowFloor
            } else if target.target.is_some_and(|goal| current > goal) {
                MetricStatus::BelowTarget
            } else if target.stretch.is_some_and(|stretch| current <= stretch) {
                MetricStatus::StretchMet
            } else {
                MetricStatus::TargetMet
            }
        }
        TargetDirection::LargerIsBetter => {
            if target.floor.is_some_and(|floor| current < floor) {
                MetricStatus::BelowFloor
            } else if target.target.is_some_and(|goal| current < goal) {
                MetricStatus::BelowTarget
            } else if target.stretch.is_some_and(|stretch| current >= stretch) {
                MetricStatus::StretchMet
            } else {
                MetricStatus::TargetMet
            }
        }
    }
}

fn missing_assessment(target: &TargetMetric, status: &str) -> MetricAssessment {
    MetricAssessment {
        target_id: target.id.clone(),
        scenario: target.scenario.clone(),
        metric: target.metric.clone(),
        priority: target.priority.clone(),
        signature: target.signature,
        direction: target.direction,
        unit: target.unit.clone(),
        weight: target.weight,
        status: status.to_string(),
        current: None,
        floor: target.floor,
        target: target.target,
        stretch: target.stretch,
        likely_owners: target.likely_owners.clone(),
        cache_mode: target.cache_mode.clone(),
        durability_mode: target.durability_mode.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::TempDir;

    use super::assess_run;
    use crate::types::{ProfileKind, ScenarioId, ScenarioResult, ScenarioStatus};

    fn sample_result(
        scenario_id: ScenarioId,
        cache_mode: &str,
        durability_mode: &str,
        metrics: BTreeMap<String, serde_json::Value>,
    ) -> ScenarioResult {
        ScenarioResult {
            status: ScenarioStatus::Passed,
            error_class: None,
            scenario_id,
            profile: ProfileKind::Smoke,
            workload: scenario_id.default_workload().to_string(),
            durability_mode: durability_mode.to_string(),
            cache_mode: cache_mode.to_string(),
            trial_count: 1,
            metrics,
            warnings: Vec::new(),
            notes: Vec::new(),
            scale: serde_json::json!({}),
            histograms: None,
            vfs_stats: None,
            artifacts: Vec::new(),
        }
    }

    #[test]
    fn assess_run_is_partial_when_targets_are_missing() {
        let temp = TempDir::new().expect("tempdir");
        let targets = temp.path().join("targets.toml");
        std::fs::write(
            &targets,
            r#"
format_version = 1
[rating]
gold_min_signature_metrics_at_stretch = 1
elite_min_signature_metrics_at_stretch = 2

[[metric]]
id = "a"
scenario = "durable_commit_single"
metric = "txn_p95_us"
priority = "P0"
signature = true
direction = "smaller_is_better"
unit = "microseconds"
weight = 1.0
floor = 3000.0
target = 1500.0
stretch = 800.0
cache_mode = "real_fs"
durability_mode = "full"
likely_owners = ["wal"]

[[metric]]
id = "b"
scenario = "range_scan_warm"
metric = "rows_per_sec"
priority = "P0"
signature = true
direction = "larger_is_better"
unit = "rows_per_second"
weight = 1.0
floor = 1.0
target = 2.0
stretch = 3.0
cache_mode = "in_memory"
durability_mode = "n/a"
likely_owners = ["btree"]
"#,
        )
        .expect("write targets");

        let mut metrics = BTreeMap::new();
        metrics.insert("txn_p95_us".to_string(), serde_json::json!(1200.0));
        let result = sample_result(ScenarioId::DurableCommitSingle, "real_fs", "full", metrics);

        let assessment = assess_run(&targets, "smoke", "debug", &[result]).expect("assess run");
        assert_eq!(assessment.scope, "partial");
        assert!(assessment.overall_grade.is_none());
        assert_eq!(assessment.matched_metrics, 1);
    }

    #[test]
    fn assess_run_produces_green_grade_for_complete_target_meet() {
        let temp = TempDir::new().expect("tempdir");
        let targets = temp.path().join("targets.toml");
        std::fs::write(
            &targets,
            r#"
format_version = 1
[rating]
gold_min_signature_metrics_at_stretch = 2
elite_min_signature_metrics_at_stretch = 3

[[metric]]
id = "a"
scenario = "durable_commit_single"
metric = "txn_p95_us"
priority = "P0"
signature = true
direction = "smaller_is_better"
unit = "microseconds"
weight = 1.0
floor = 3000.0
target = 1500.0
stretch = 800.0
cache_mode = "real_fs"
durability_mode = "full"
likely_owners = ["wal"]
"#,
        )
        .expect("write targets");

        let mut metrics = BTreeMap::new();
        metrics.insert("txn_p95_us".to_string(), serde_json::json!(1200.0));
        let result = sample_result(ScenarioId::DurableCommitSingle, "real_fs", "full", metrics);

        let assessment = assess_run(&targets, "nightly", "release", &[result]).expect("assess run");
        assert_eq!(assessment.scope, "complete");
        assert_eq!(assessment.overall_grade.as_deref(), Some("green"));
        assert_eq!(assessment.metrics[0].status, "target_met");
    }
}
