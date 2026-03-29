use serde::{Deserialize, Serialize};

use crate::profiles::ResolvedProfile;
use crate::targets::RunTargetAssessment;
use crate::types::{ProfileKind, ScenarioId, ScenarioStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunManifest {
    pub run_id: String,
    pub started_unix_ms: u128,
    pub profile: ProfileKind,
    pub dry_run: bool,
    pub selected_scenarios: Vec<ScenarioId>,
    pub resolved_profile: ResolvedProfile,
    pub command_line: Vec<String>,
    pub paths: ManifestPaths,
    pub environment: EnvironmentCapture,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ManifestPaths {
    pub scratch_root: String,
    pub artifact_root: String,
    pub scratch_run_dir: String,
    pub run_dir: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EnvironmentCapture {
    pub benchmark_crate_version: String,
    pub build_profile: String,
    pub rustc_version: Option<String>,
    pub os: String,
    pub arch: String,
    pub git_sha: Option<String>,
    pub git_branch: Option<String>,
    pub hostname: Option<String>,
    pub cwd: String,
    pub logical_cores: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RunSummary {
    pub run_id: String,
    pub profile: ProfileKind,
    pub dry_run: bool,
    pub status: String,
    pub started_unix_ms: u128,
    pub finished_unix_ms: u128,
    pub scenario_count: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub scenarios: Vec<ScenarioSummary>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub target_assessment: Option<RunTargetAssessment>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScenarioSummary {
    pub scenario_id: ScenarioId,
    pub status: ScenarioStatus,
    pub error_class: Option<String>,
    pub artifact_file: String,
    pub headline_metrics: std::collections::BTreeMap<String, serde_json::Value>,
}
