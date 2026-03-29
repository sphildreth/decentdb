use std::collections::BTreeMap;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

use crate::cli::{InspectStorageArgs, InternalArgs, RunArgs};
use crate::profiles::{resolve_profile, ProfileOverrides, ResolvedProfile};
use crate::scenarios::{execute_internal_command, run_scenario};
use crate::storage_inspector::inspect_db_file;
use crate::targets::{assess_run, default_targets_path, RunTargetAssessment};
use crate::types::{ProfileKind, ScenarioId, ScenarioResult, ScenarioStatus};

#[derive(Debug)]
struct RunDirectories {
    scratch_root: PathBuf,
    artifact_root: PathBuf,
    scratch_run_dir: PathBuf,
    run_dir: PathBuf,
    scenario_dir: PathBuf,
    retained_artifact_dir: PathBuf,
}

impl RunDirectories {
    fn new(scratch_root: PathBuf, artifact_root: PathBuf, run_id: &str) -> Self {
        let scratch_run_dir = scratch_root.join(run_id);
        let run_dir = artifact_root.join("runs").join(run_id);
        let scenario_dir = run_dir.join("scenarios");
        let retained_artifact_dir = run_dir.join("artifacts");
        Self {
            scratch_root,
            artifact_root,
            scratch_run_dir,
            run_dir,
            scenario_dir,
            retained_artifact_dir,
        }
    }
}

#[derive(Debug, Serialize)]
struct RunManifest {
    run_id: String,
    started_unix_ms: u128,
    profile: ProfileKind,
    dry_run: bool,
    selected_scenarios: Vec<ScenarioId>,
    resolved_profile: ResolvedProfile,
    command_line: Vec<String>,
    paths: ManifestPaths,
    environment: EnvironmentCapture,
}

#[derive(Debug, Serialize)]
struct ManifestPaths {
    scratch_root: String,
    artifact_root: String,
    scratch_run_dir: String,
    run_dir: String,
}

#[derive(Debug, Serialize)]
struct EnvironmentCapture {
    benchmark_crate_version: String,
    build_profile: String,
    rustc_version: Option<String>,
    os: String,
    arch: String,
    git_sha: Option<String>,
    git_branch: Option<String>,
    hostname: Option<String>,
    cwd: String,
    logical_cores: Option<usize>,
}

#[derive(Debug, Serialize)]
struct RunSummary {
    run_id: String,
    profile: ProfileKind,
    dry_run: bool,
    status: String,
    started_unix_ms: u128,
    finished_unix_ms: u128,
    scenario_count: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    scenarios: Vec<ScenarioSummary>,
    warnings: Vec<String>,
    target_assessment: Option<RunTargetAssessment>,
}

#[derive(Debug, Serialize)]
struct ScenarioSummary {
    scenario_id: ScenarioId,
    status: ScenarioStatus,
    error_class: Option<String>,
    artifact_file: String,
    headline_metrics: BTreeMap<String, serde_json::Value>,
}

pub(crate) fn run_command(args: RunArgs) -> Result<()> {
    run_command_with_executor(args, run_scenario)
}

pub(crate) fn run_internal_command(args: InternalArgs) -> Result<()> {
    let output = execute_internal_command(args.command)?;
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

pub(crate) fn run_inspect_storage_command(args: InspectStorageArgs) -> Result<()> {
    let inspection = inspect_db_file(&args.db_path)?;
    let bytes = serde_json::to_vec_pretty(&inspection)?;
    if let Some(output_path) = args.output {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("create inspect-storage output dir {}", parent.display())
            })?;
        }
        fs::write(&output_path, bytes)
            .with_context(|| format!("write inspect-storage output {}", output_path.display()))?;
        println!("inspection={}", output_path.display());
    } else {
        println!("{}", String::from_utf8(bytes)?);
    }
    Ok(())
}

fn run_command_with_executor<F>(args: RunArgs, mut execute_scenario: F) -> Result<()>
where
    F: FnMut(ScenarioId, &ResolvedProfile, &Path) -> Result<ScenarioResult>,
{
    let selected_scenarios = resolve_scenarios(args.all, &args.scenario);
    let profile_overrides = ProfileOverrides::from_run_args(&args);
    let profile = resolve_profile(args.profile, &profile_overrides)?;
    let started_unix_ms = unix_millis(SystemTime::now())?;
    let run_id = make_run_id(started_unix_ms, profile.kind);

    let dirs = RunDirectories::new(args.scratch_root, args.artifact_root, &run_id);
    prepare_paths(&dirs)?;

    let manifest = RunManifest {
        run_id: run_id.clone(),
        started_unix_ms,
        profile: profile.kind,
        dry_run: args.dry_run,
        selected_scenarios: selected_scenarios.clone(),
        resolved_profile: profile.clone(),
        command_line: command_line(),
        paths: ManifestPaths {
            scratch_root: display_path(&dirs.scratch_root),
            artifact_root: display_path(&dirs.artifact_root),
            scratch_run_dir: display_path(&dirs.scratch_run_dir),
            run_dir: display_path(&dirs.run_dir),
        },
        environment: capture_environment()?,
    };
    write_json(&dirs.run_dir.join("manifest.json"), &manifest)?;

    let mut scenario_results = Vec::with_capacity(selected_scenarios.len());
    for scenario_id in &selected_scenarios {
        let scenario_file = dirs
            .scenario_dir
            .join(format!("{}.json", scenario_id.as_str()));
        let mut scenario_result = if args.dry_run {
            dry_run_result(*scenario_id, &profile)
        } else {
            let scenario_scratch = dirs.scratch_run_dir.join(scenario_id.as_str());
            match panic::catch_unwind(AssertUnwindSafe(|| {
                execute_scenario(*scenario_id, &profile, &scenario_scratch)
            })) {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => failed_result(
                    *scenario_id,
                    &profile,
                    classify_error(&error),
                    &format!("{error:#}"),
                ),
                Err(payload) => {
                    let message = panic_message(payload);
                    failed_result(
                        *scenario_id,
                        &profile,
                        "harness_panic",
                        &format!("benchmark runner panicked: {message}"),
                    )
                }
            }
        };
        promote_artifacts(&mut scenario_result, &dirs)?;
        write_json(&scenario_file, &scenario_result)?;
        scenario_results.push((scenario_result, scenario_file));
    }

    let finished_unix_ms = unix_millis(SystemTime::now())?;
    let mut summary = build_summary(
        &run_id,
        &profile,
        args.dry_run,
        started_unix_ms,
        finished_unix_ms,
        &scenario_results,
    );
    if !args.dry_run {
        let scenario_values = scenario_results
            .iter()
            .map(|(result, _)| result.clone())
            .collect::<Vec<_>>();
        let targets_path = default_targets_path();
        if targets_path.exists() {
            let assessment = assess_run(
                &targets_path,
                profile.kind.as_str(),
                build_profile(),
                &scenario_values,
            )?;
            if let Some(grade) = assessment.overall_grade.as_deref() {
                println!("grade={grade}");
            } else {
                println!("grade=partial");
            }
            summary.warnings.extend(assessment.warnings.iter().cloned());
            summary.target_assessment = Some(assessment);
        } else {
            summary.warnings.push(format!(
                "targets file {} not found; run was not graded",
                targets_path.display()
            ));
        }
    }
    write_json(&dirs.run_dir.join("summary.json"), &summary)?;

    println!("run_id={run_id}");
    println!("manifest={}", dirs.run_dir.join("manifest.json").display());
    println!("summary={}", dirs.run_dir.join("summary.json").display());
    if summary.failed > 0 {
        return Err(anyhow!(
            "benchmark run completed with {} failed scenario(s); inspect {}",
            summary.failed,
            dirs.run_dir.join("summary.json").display()
        ));
    }
    Ok(())
}

fn build_summary(
    run_id: &str,
    profile: &ResolvedProfile,
    dry_run: bool,
    started_unix_ms: u128,
    finished_unix_ms: u128,
    scenario_results: &[(ScenarioResult, PathBuf)],
) -> RunSummary {
    let mut passed = 0_usize;
    let mut failed = 0_usize;
    let mut skipped = 0_usize;
    let mut summary_rows = Vec::with_capacity(scenario_results.len());

    for (result, file_path) in scenario_results {
        match result.status {
            ScenarioStatus::Passed => passed += 1,
            ScenarioStatus::Failed => failed += 1,
            ScenarioStatus::Skipped => skipped += 1,
        }
        summary_rows.push(ScenarioSummary {
            scenario_id: result.scenario_id,
            status: result.status.clone(),
            error_class: result.error_class.clone(),
            artifact_file: display_path(file_path),
            headline_metrics: headline_metrics(result),
        });
    }

    let status = if dry_run {
        "dry_run".to_string()
    } else if failed > 0 {
        "incomplete".to_string()
    } else {
        "passed".to_string()
    };

    let mut warnings = Vec::new();
    if failed > 0 {
        warnings
            .push("one or more scenarios failed; inspect per-scenario JSON artifacts".to_string());
    }
    if dry_run {
        warnings.push("dry run did not execute scenarios".to_string());
    }

    RunSummary {
        run_id: run_id.to_string(),
        profile: profile.kind,
        dry_run,
        status,
        started_unix_ms,
        finished_unix_ms,
        scenario_count: scenario_results.len(),
        passed,
        failed,
        skipped,
        scenarios: summary_rows,
        warnings,
        target_assessment: None,
    }
}

fn headline_metrics(result: &ScenarioResult) -> BTreeMap<String, serde_json::Value> {
    let keys: &[&str] = match result.scenario_id {
        ScenarioId::DurableCommitSingle => &["commit_p95_us", "commits_per_sec"],
        ScenarioId::DurableCommitBatch => {
            &["batch_commit_p95_us", "rows_per_sec", "wal_growth_bytes"]
        }
        ScenarioId::PointLookupWarm => &["lookup_p95_us", "lookups_per_sec"],
        ScenarioId::PointLookupCold => &["first_read_p95_us", "cold_batch_p95_ms"],
        ScenarioId::RangeScanWarm => &["scan_p95_us", "rows_per_sec"],
        ScenarioId::Checkpoint => &[
            "checkpoint_ms",
            "wal_bytes_before_checkpoint",
            "wal_bytes_after_checkpoint",
        ],
        ScenarioId::RecoveryReopen => &["reopen_p95_ms", "first_query_p95_ms"],
        ScenarioId::ReadUnderWrite => &[
            "reader_p95_isolation_us",
            "reader_p95_under_write_us",
            "reader_p95_degradation_ratio",
            "writer_throughput_isolation_ops_per_sec",
            "writer_throughput_under_readers_ops_per_sec",
            "writer_throughput_degradation_ratio",
        ],
        ScenarioId::StorageEfficiency => &[
            "space_amplification",
            "bytes_per_logical_row",
            "db_file_bytes",
            "wal_file_bytes_peak",
        ],
        ScenarioId::MemoryFootprint => {
            &["rss_steady_bytes", "rss_peak_bytes", "memory_amplification"]
        }
    };

    let mut selected = BTreeMap::new();
    for key in keys {
        if let Some(value) = result.metrics.get(*key) {
            selected.insert((*key).to_string(), value.clone());
        }
    }
    selected
}

fn resolve_scenarios(all: bool, requested: &[ScenarioId]) -> Vec<ScenarioId> {
    if all || requested.is_empty() {
        return ScenarioId::ALL.to_vec();
    }

    let mut deduped = Vec::new();
    for scenario_id in requested {
        if !deduped.contains(scenario_id) {
            deduped.push(*scenario_id);
        }
    }
    deduped
}

fn dry_run_result(scenario_id: ScenarioId, profile: &ResolvedProfile) -> ScenarioResult {
    ScenarioResult {
        status: ScenarioStatus::Skipped,
        error_class: None,
        scenario_id,
        profile: profile.kind,
        workload: scenario_id.default_workload().to_string(),
        durability_mode: scenario_id.default_durability_mode().to_string(),
        cache_mode: scenario_id.default_cache_mode().to_string(),
        trial_count: profile.trials,
        metrics: BTreeMap::new(),
        warnings: Vec::new(),
        notes: vec!["dry run: scenario not executed".to_string()],
        scale: profile.scale_json(),
        histograms: None,
        vfs_stats: None,
        artifacts: Vec::new(),
    }
}

fn failed_result(
    scenario_id: ScenarioId,
    profile: &ResolvedProfile,
    error_class: &str,
    message: &str,
) -> ScenarioResult {
    ScenarioResult {
        status: ScenarioStatus::Failed,
        error_class: Some(error_class.to_string()),
        scenario_id,
        profile: profile.kind,
        workload: scenario_id.default_workload().to_string(),
        durability_mode: scenario_id.default_durability_mode().to_string(),
        cache_mode: scenario_id.default_cache_mode().to_string(),
        trial_count: profile.trials,
        metrics: BTreeMap::new(),
        warnings: Vec::new(),
        notes: vec![message.to_string()],
        scale: profile.scale_json(),
        histograms: None,
        vfs_stats: None,
        artifacts: Vec::new(),
    }
}

fn classify_error(error: &anyhow::Error) -> &'static str {
    let lower = format!("{error:#}").to_lowercase();
    if lower.contains("no space left on device") {
        "disk_full"
    } else if lower.contains("timed out") {
        "timeout"
    } else if lower.contains("permission denied")
        || lower.contains("read-only file system")
        || lower.contains("not a directory")
        || lower.contains("create scenario scratch")
        || lower.contains("create trial dir")
        || lower.contains("create storage trial dir")
        || lower.contains("read metadata for")
        || lower.contains("write json")
        || lower.contains("exceeds i64")
    {
        "harness_error"
    } else {
        "engine_error"
    }
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

fn prepare_paths(dirs: &RunDirectories) -> Result<()> {
    fs::create_dir_all(&dirs.scratch_root)
        .with_context(|| format!("create scratch root {}", dirs.scratch_root.display()))?;
    fs::create_dir_all(&dirs.artifact_root)
        .with_context(|| format!("create artifact root {}", dirs.artifact_root.display()))?;
    fs::create_dir_all(&dirs.scratch_run_dir)
        .with_context(|| format!("create scratch run dir {}", dirs.scratch_run_dir.display()))?;
    fs::create_dir_all(&dirs.scenario_dir)
        .with_context(|| format!("create scenario dir {}", dirs.scenario_dir.display()))?;
    fs::create_dir_all(&dirs.retained_artifact_dir).with_context(|| {
        format!(
            "create retained artifact dir {}",
            dirs.retained_artifact_dir.display()
        )
    })?;
    validate_writable(&dirs.run_dir)?;
    Ok(())
}

fn promote_artifacts(result: &mut ScenarioResult, dirs: &RunDirectories) -> Result<()> {
    if result.artifacts.is_empty() {
        return Ok(());
    }

    let scenario_dir = dirs.retained_artifact_dir.join(result.scenario_id.as_str());
    fs::create_dir_all(&scenario_dir)
        .with_context(|| format!("create scenario artifact dir {}", scenario_dir.display()))?;

    let original_artifacts = std::mem::take(&mut result.artifacts);
    let mut retained = Vec::with_capacity(original_artifacts.len());

    for artifact in original_artifacts {
        let source = PathBuf::from(&artifact);
        if !source.exists() {
            result.warnings.push(format!(
                "artifact {} was not found for retention; keeping original path reference",
                source.display()
            ));
            retained.push(artifact);
            continue;
        }

        let file_name = source.file_name().ok_or_else(|| {
            anyhow!(
                "artifact {} does not have a file name for retention",
                source.display()
            )
        })?;
        let destination = unique_retained_artifact_path(&scenario_dir, file_name);
        fs::copy(&source, &destination).with_context(|| {
            format!(
                "copy artifact {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        retained.push(display_path(&destination));
    }

    result.artifacts = retained;
    Ok(())
}

fn unique_retained_artifact_path(base_dir: &Path, file_name: &std::ffi::OsStr) -> PathBuf {
    let initial = base_dir.join(file_name);
    if !initial.exists() {
        return initial;
    }

    let stem = Path::new(file_name)
        .file_stem()
        .map(|value| value.to_string_lossy().to_string())
        .unwrap_or_else(|| "artifact".to_string());
    let ext = Path::new(file_name)
        .extension()
        .map(|value| value.to_string_lossy().to_string());

    for index in 1.. {
        let candidate_name = match &ext {
            Some(ext) => format!("{stem}-{index}.{ext}"),
            None => format!("{stem}-{index}"),
        };
        let candidate = base_dir.join(candidate_name);
        if !candidate.exists() {
            return candidate;
        }
    }

    unreachable!("artifact retention naming should always find a free path")
}

fn validate_writable(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("create writable directory {}", path.display()))?;
    let test_file = path.join(".write-check");
    fs::write(&test_file, b"ok")
        .with_context(|| format!("write test file {}", test_file.display()))?;
    fs::remove_file(&test_file)
        .with_context(|| format!("remove test file {}", test_file.display()))?;
    Ok(())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value)?;
    fs::write(path, bytes).with_context(|| format!("write json {}", path.display()))?;
    Ok(())
}

fn command_line() -> Vec<String> {
    env::args_os().map(os_to_string_lossy).collect()
}

fn capture_environment() -> Result<EnvironmentCapture> {
    let cwd = env::current_dir()
        .with_context(|| "resolve current directory".to_string())?
        .display()
        .to_string();
    Ok(EnvironmentCapture {
        benchmark_crate_version: env!("CARGO_PKG_VERSION").to_string(),
        build_profile: build_profile().to_string(),
        rustc_version: command_output("rustc", &["--version"]),
        os: env::consts::OS.to_string(),
        arch: env::consts::ARCH.to_string(),
        git_sha: command_output("git", &["rev-parse", "--short", "HEAD"]),
        git_branch: command_output("git", &["rev-parse", "--abbrev-ref", "HEAD"]),
        hostname: env::var("HOSTNAME")
            .ok()
            .or_else(|| env::var("COMPUTERNAME").ok()),
        cwd,
        logical_cores: std::thread::available_parallelism().ok().map(usize::from),
    })
}

fn command_output(binary: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(binary).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn unix_millis(time: SystemTime) -> Result<u128> {
    Ok(time
        .duration_since(UNIX_EPOCH)
        .with_context(|| "system clock is before UNIX_EPOCH".to_string())?
        .as_millis())
}

fn make_run_id(started_unix_ms: u128, profile: ProfileKind) -> String {
    let git_sha = command_output("git", &["rev-parse", "--short", "HEAD"])
        .unwrap_or_else(|| "nogit".to_string());
    format!("unix-{started_unix_ms}-{}-{git_sha}", profile.as_str())
}

fn display_path(path: &Path) -> String {
    path.display().to_string()
}

fn os_to_string_lossy(value: OsString) -> String {
    value.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::Path;

    use anyhow::anyhow;
    use serde_json::Value as JsonValue;
    use tempfile::TempDir;

    use super::{classify_error, resolve_scenarios, run_command, run_command_with_executor};
    use crate::cli::RunArgs;
    use crate::types::{ProfileKind, ScenarioId};

    #[test]
    fn resolve_scenarios_defaults_to_all_scenarios() {
        let resolved = resolve_scenarios(false, &[]);
        assert_eq!(resolved.len(), ScenarioId::ALL.len());
        assert_eq!(resolved[0], ScenarioId::DurableCommitSingle);
    }

    #[test]
    fn resolve_scenarios_dedupes_inputs() {
        let resolved = resolve_scenarios(
            false,
            &[
                ScenarioId::PointLookupWarm,
                ScenarioId::PointLookupWarm,
                ScenarioId::StorageEfficiency,
            ],
        );
        assert_eq!(
            resolved,
            vec![ScenarioId::PointLookupWarm, ScenarioId::StorageEfficiency]
        );
    }

    #[test]
    fn dry_run_writes_manifest_summary_and_scenarios() {
        let temp = TempDir::new().expect("tempdir");
        let scratch_root = temp.path().join("scratch");
        let artifact_root = temp.path().join("artifacts");
        let args = RunArgs {
            profile: ProfileKind::Smoke,
            scenario: vec![ScenarioId::DurableCommitSingle],
            all: false,
            dry_run: true,
            rows: None,
            point_reads: None,
            range_scan_rows: None,
            range_scans: None,
            durable_commits: None,
            batch_size: None,
            cold_batches: None,
            reader_threads: None,
            writer_ops: None,
            warmup_ops: None,
            trials: None,
            seed: None,
            scratch_root,
            artifact_root: artifact_root.clone(),
        };

        run_command(args).expect("dry run command");

        let runs_dir = artifact_root.join("runs");
        let mut run_entries = fs::read_dir(&runs_dir)
            .expect("read runs dir")
            .map(|entry| entry.expect("entry").path())
            .collect::<Vec<_>>();
        run_entries.sort();
        assert_eq!(run_entries.len(), 1);

        let run_dir = &run_entries[0];
        assert!(run_dir.join("manifest.json").exists());
        assert!(run_dir.join("summary.json").exists());
        assert!(run_dir
            .join("scenarios")
            .join("durable_commit_single.json")
            .exists());

        let summary: JsonValue =
            serde_json::from_slice(&fs::read(run_dir.join("summary.json")).expect("read summary"))
                .expect("parse summary");
        assert_eq!(summary["status"], "dry_run");
        assert_eq!(summary["skipped"], 1);
    }

    #[test]
    fn failed_scenario_writes_artifacts_and_returns_error() {
        let temp = TempDir::new().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let args = RunArgs {
            profile: ProfileKind::Smoke,
            scenario: vec![ScenarioId::DurableCommitSingle],
            all: false,
            dry_run: false,
            rows: None,
            point_reads: None,
            range_scan_rows: None,
            range_scans: None,
            durable_commits: None,
            batch_size: None,
            cold_batches: None,
            reader_threads: None,
            writer_ops: None,
            warmup_ops: None,
            trials: None,
            seed: None,
            scratch_root: temp.path().join("scratch"),
            artifact_root: artifact_root.clone(),
        };

        let error = run_command_with_executor(args, |_scenario_id, _profile, _scratch| {
            Err(anyhow!("synthetic harness failure"))
        })
        .expect_err("failing scenario should return error");
        assert!(error
            .to_string()
            .contains("benchmark run completed with 1 failed scenario"));

        let runs_dir = artifact_root.join("runs");
        let mut run_entries = fs::read_dir(&runs_dir)
            .expect("read runs dir")
            .map(|entry| entry.expect("entry").path())
            .collect::<Vec<_>>();
        run_entries.sort();
        assert_eq!(run_entries.len(), 1);

        let run_dir = &run_entries[0];
        let summary: JsonValue =
            serde_json::from_slice(&fs::read(run_dir.join("summary.json")).expect("read summary"))
                .expect("parse summary");
        assert_eq!(summary["status"], "incomplete");
        assert_eq!(summary["failed"], 1);

        let scenario: JsonValue = serde_json::from_slice(
            &fs::read(run_dir.join("scenarios").join("durable_commit_single.json"))
                .expect("read scenario"),
        )
        .expect("parse scenario");
        assert_eq!(scenario["status"], "failed");
    }

    #[test]
    fn scenario_artifacts_are_retained_under_run_directory() {
        let temp = TempDir::new().expect("tempdir");
        let artifact_root = temp.path().join("artifacts");
        let args = RunArgs {
            profile: ProfileKind::Smoke,
            scenario: vec![ScenarioId::DurableCommitSingle],
            all: false,
            dry_run: false,
            rows: None,
            point_reads: None,
            range_scan_rows: None,
            range_scans: None,
            durable_commits: None,
            batch_size: None,
            cold_batches: None,
            reader_threads: None,
            writer_ops: None,
            warmup_ops: None,
            trials: None,
            seed: None,
            scratch_root: temp.path().join("scratch"),
            artifact_root: artifact_root.clone(),
        };

        run_command_with_executor(args, |_scenario_id, profile, scratch| {
            fs::create_dir_all(scratch).expect("create scratch");
            let raw_artifact = scratch.join("sample-artifact.json");
            fs::write(&raw_artifact, br#"{"ok":true}"#).expect("write raw artifact");
            Ok(crate::types::ScenarioResult {
                status: crate::types::ScenarioStatus::Passed,
                error_class: None,
                scenario_id: ScenarioId::DurableCommitSingle,
                profile: profile.kind,
                workload: "test".to_string(),
                durability_mode: "full".to_string(),
                cache_mode: "real_fs".to_string(),
                trial_count: profile.trials,
                metrics: BTreeMap::new(),
                warnings: Vec::new(),
                notes: Vec::new(),
                scale: profile.scale_json(),
                histograms: None,
                vfs_stats: None,
                artifacts: vec![raw_artifact.display().to_string()],
            })
        })
        .expect("successful scenario run");

        let run_dir = fs::read_dir(artifact_root.join("runs"))
            .expect("read runs dir")
            .map(|entry| entry.expect("entry").path())
            .next()
            .expect("run dir");
        let scenario: JsonValue = serde_json::from_slice(
            &fs::read(run_dir.join("scenarios").join("durable_commit_single.json"))
                .expect("read scenario"),
        )
        .expect("parse scenario");
        let retained = scenario["artifacts"][0]
            .as_str()
            .expect("artifact path string");
        assert!(retained.contains("/artifacts/durable_commit_single/"));
        assert!(Path::new(retained).exists());
    }

    #[test]
    fn classify_error_distinguishes_harness_disk_and_engine_failures() {
        assert_eq!(
            classify_error(&anyhow!("No space left on device")),
            "disk_full"
        );
        assert_eq!(
            classify_error(&anyhow!("create trial dir /tmp/x failed")),
            "harness_error"
        );
        assert_eq!(
            classify_error(&anyhow!("expected exactly 1 row, got 0")),
            "engine_error"
        );
    }
}
