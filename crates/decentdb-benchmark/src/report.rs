use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};

use crate::artifacts::RunSummary;
use crate::cli::{ReportArgs, ReportAudience, ReportFormat};
use crate::compare::{
    read_compare_artifact, CompareArtifact, MetricComparison, MetricComparisonStatus,
};

pub(crate) fn run_report_command(args: ReportArgs) -> Result<()> {
    match (&args.input, &args.compare) {
        (Some(_), Some(_)) => {
            return Err(anyhow!(
                "provide either --input or --compare for report, not both"
            ));
        }
        (None, None) => {
            return Err(anyhow!(
                "missing report input; provide --input or --compare"
            ));
        }
        _ => {}
    }

    let rendered = if let Some(compare_path) = &args.compare {
        let compare = read_compare_artifact(compare_path)?;
        match (args.format, args.audience) {
            (ReportFormat::Markdown, ReportAudience::Human) => {
                render_compare_markdown_human(&compare)
            }
            (ReportFormat::Text, ReportAudience::Human) => render_compare_text_human(&compare),
            (ReportFormat::Markdown, ReportAudience::AgentBrief) => {
                render_compare_markdown_agent_brief(&compare)
            }
            (ReportFormat::Text, ReportAudience::AgentBrief) => {
                render_compare_agent_brief(&compare)
            }
        }
    } else {
        let input = args
            .input
            .as_ref()
            .ok_or_else(|| anyhow!("report input is missing"))?;
        let summary: RunSummary = read_json_file(input)?;
        match (args.format, args.audience) {
            (ReportFormat::Markdown, ReportAudience::Human) => render_run_markdown_human(&summary),
            (ReportFormat::Text, ReportAudience::Human) => render_run_text_human(&summary),
            (ReportFormat::Markdown, ReportAudience::AgentBrief) => {
                render_run_markdown_agent_brief(&summary)
            }
            (ReportFormat::Text, ReportAudience::AgentBrief) => render_run_agent_brief(&summary),
        }
    };

    println!("{rendered}");
    Ok(())
}

fn render_compare_markdown_human(compare: &CompareArtifact) -> String {
    let mut lines = Vec::new();
    lines.push("# DecentDB Benchmark Report".to_string());
    lines.push(String::new());
    lines.push(format!("Compare ID: `{}`", compare.compare_id));
    lines.push(format!(
        "Candidate run: `{}`",
        compare.context.candidate.run_id
    ));
    let baseline_label = compare
        .baseline_source
        .baseline_name
        .as_deref()
        .unwrap_or(compare.context.baseline.run_id.as_str());
    lines.push(format!("Baseline: `{}`", baseline_label));
    lines.push(format!(
        "Strict: `{}` | Meaningful: `{}` | Authoritative: `{}`",
        compare.strictness.strict,
        compare.strictness.meaningful,
        compare.strictness.comparison_authoritative
    ));

    lines.push(String::new());
    lines.push("## Headline KPI Snapshot".to_string());
    lines.push("| KPI | Current | Baseline | Delta | Target | Status |".to_string());
    lines.push("| --- | ---: | ---: | ---: | ---: | --- |".to_string());
    for metric_id in key_kpi_metric_ids() {
        if let Some(metric) = metric_by_id(compare, metric_id) {
            lines.push(format!(
                "| {} | {} | {} | {} | {} | {} |",
                metric.metric_id,
                fmt_opt(metric.current_value),
                fmt_opt(metric.baseline_value),
                fmt_pct_opt(metric.directional_delta_percent),
                fmt_opt(metric.target_value),
                status_label(metric.status)
            ));
        }
    }

    lines.push(String::new());
    lines.push("## Progress vs Baseline".to_string());
    lines.push(format!("- Regressions: {}", compare.totals.regressions));
    lines.push(format!("- Improvements: {}", compare.totals.improvements));
    lines.push(format!(
        "- Unchanged within noise: {}",
        compare.totals.unchanged_within_noise
    ));
    lines.push(format!(
        "- Missing metrics: {}",
        compare.totals.missing_metric
    ));

    lines.push(String::new());
    lines.push("## Top Regressions".to_string());
    if compare.top_regressions.is_empty() {
        lines.push("- none".to_string());
    } else {
        for (index, item) in compare.top_regressions.iter().enumerate() {
            lines.push(format!(
                "{}. `{}` delta={} current={} baseline={}",
                index + 1,
                item.metric_id,
                fmt_pct(item.delta_percent),
                fmt_num(item.current_value),
                fmt_num(item.baseline_value)
            ));
        }
    }

    lines.push(String::new());
    lines.push("## Top Improvements".to_string());
    if compare.top_improvements.is_empty() {
        lines.push("- none".to_string());
    } else {
        for (index, item) in compare.top_improvements.iter().enumerate() {
            lines.push(format!(
                "{}. `{}` delta={} current={} baseline={}",
                index + 1,
                item.metric_id,
                fmt_pct(item.delta_percent),
                fmt_num(item.current_value),
                fmt_num(item.baseline_value)
            ));
        }
    }

    lines.push(String::new());
    lines.push("## Optimization Opportunities".to_string());
    lines.push("| Metric | Current | Baseline | Target | Score | Likely Owners |".to_string());
    lines.push("| --- | ---: | ---: | ---: | ---: | --- |".to_string());
    for opportunity in compare.optimization_opportunities.iter().take(10) {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} |",
            opportunity.metric_id,
            fmt_opt(opportunity.current_value),
            fmt_opt(opportunity.baseline_value),
            fmt_opt(opportunity.target_value),
            fmt_num(opportunity.priority_score),
            format_owners(&opportunity.likely_owners)
        ));
    }

    lines.push(String::new());
    lines.push("## Storage".to_string());
    if let Some(storage) = &compare.storage {
        lines.push("| Metric | Candidate | Baseline | Delta |".to_string());
        lines.push("| --- | ---: | ---: | ---: |".to_string());
        lines.push(storage_row(
            "bytes_per_logical_row",
            storage.candidate.bytes_per_logical_row,
            storage.baseline.bytes_per_logical_row,
            storage.delta_percent.bytes_per_logical_row,
        ));
        lines.push(storage_row(
            "db_file_bytes",
            storage.candidate.db_file_bytes,
            storage.baseline.db_file_bytes,
            storage.delta_percent.db_file_bytes,
        ));
        lines.push(storage_row(
            "wal_file_bytes_peak",
            storage.candidate.wal_file_bytes_peak,
            storage.baseline.wal_file_bytes_peak,
            storage.delta_percent.wal_file_bytes_peak,
        ));
        lines.push(storage_row(
            "space_amplification",
            storage.candidate.space_amplification,
            storage.baseline.space_amplification,
            storage.delta_percent.space_amplification,
        ));
    } else {
        lines.push("No storage metrics available.".to_string());
    }

    lines.push(String::new());
    lines.push("## Trust and Context Warnings".to_string());
    if compare.warnings.is_empty() {
        lines.push("- none".to_string());
    } else {
        for warning in &compare.warnings {
            lines.push(format!("- {}", warning));
        }
    }

    lines.join("\n")
}

fn render_compare_text_human(compare: &CompareArtifact) -> String {
    let mut lines = Vec::new();
    let baseline_label = compare
        .baseline_source
        .baseline_name
        .as_deref()
        .unwrap_or(compare.context.baseline.run_id.as_str());

    lines.push("DecentDB Benchmark Report".to_string());
    lines.push(format!(
        "compare_id={} candidate={} baseline={}",
        compare.compare_id, compare.context.candidate.run_id, baseline_label
    ));
    lines.push(format!(
        "strict={} meaningful={} authoritative={}",
        compare.strictness.strict,
        compare.strictness.meaningful,
        compare.strictness.comparison_authoritative
    ));
    lines.push(String::new());

    lines.push("Headline KPIs".to_string());
    for metric_id in key_kpi_metric_ids() {
        if let Some(metric) = metric_by_id(compare, metric_id) {
            lines.push(format!(
                "  {} current={} baseline={} delta={} target={} status={}",
                metric.metric_id,
                fmt_opt(metric.current_value),
                fmt_opt(metric.baseline_value),
                fmt_pct_opt(metric.directional_delta_percent),
                fmt_opt(metric.target_value),
                status_label(metric.status)
            ));
        }
    }

    lines.push(String::new());
    lines.push("Top Regressions".to_string());
    for (index, item) in compare.top_regressions.iter().enumerate() {
        lines.push(format!(
            "  {}. {} delta={} owners={}",
            index + 1,
            item.metric_id,
            fmt_pct(item.delta_percent),
            format_owners(&item.likely_owners)
        ));
    }

    lines.push(String::new());
    lines.push("Top Improvements".to_string());
    for (index, item) in compare.top_improvements.iter().enumerate() {
        lines.push(format!(
            "  {}. {} delta={} owners={}",
            index + 1,
            item.metric_id,
            fmt_pct(item.delta_percent),
            format_owners(&item.likely_owners)
        ));
    }

    lines.push(String::new());
    lines.push("Optimization Opportunities".to_string());
    for (index, opportunity) in compare
        .optimization_opportunities
        .iter()
        .take(10)
        .enumerate()
    {
        lines.push(format!(
            "  {}. {} current={} baseline={} target={} score={} owners={}",
            index + 1,
            opportunity.metric_id,
            fmt_opt(opportunity.current_value),
            fmt_opt(opportunity.baseline_value),
            fmt_opt(opportunity.target_value),
            fmt_num(opportunity.priority_score),
            format_owners(&opportunity.likely_owners)
        ));
    }

    lines.push(String::new());
    lines.push("Storage Summary".to_string());
    if let Some(storage) = &compare.storage {
        lines.push(format!(
            "  bytes_per_logical_row current={} baseline={} delta={}",
            fmt_opt(storage.candidate.bytes_per_logical_row),
            fmt_opt(storage.baseline.bytes_per_logical_row),
            fmt_pct_opt(storage.delta_percent.bytes_per_logical_row)
        ));
        lines.push(format!(
            "  db_file_bytes current={} baseline={} delta={}",
            fmt_opt(storage.candidate.db_file_bytes),
            fmt_opt(storage.baseline.db_file_bytes),
            fmt_pct_opt(storage.delta_percent.db_file_bytes)
        ));
        lines.push(format!(
            "  wal_file_bytes_peak current={} baseline={} delta={}",
            fmt_opt(storage.candidate.wal_file_bytes_peak),
            fmt_opt(storage.baseline.wal_file_bytes_peak),
            fmt_pct_opt(storage.delta_percent.wal_file_bytes_peak)
        ));
        lines.push(format!(
            "  space_amplification current={} baseline={} delta={}",
            fmt_opt(storage.candidate.space_amplification),
            fmt_opt(storage.baseline.space_amplification),
            fmt_pct_opt(storage.delta_percent.space_amplification)
        ));
    } else {
        lines.push("  none".to_string());
    }

    lines.push(String::new());
    lines.push("Warnings".to_string());
    if compare.warnings.is_empty() {
        lines.push("  none".to_string());
    } else {
        for warning in &compare.warnings {
            lines.push(format!("  {}", warning));
        }
    }

    lines.join("\n")
}

fn render_compare_agent_brief(compare: &CompareArtifact) -> String {
    let mut lines = Vec::new();
    let baseline_label = compare
        .baseline_source
        .baseline_name
        .as_deref()
        .unwrap_or(compare.context.baseline.run_id.as_str());

    lines.push("DecentDB Agent Brief".to_string());
    lines.push(format!(
        "candidate={} baseline={} strict={} authoritative={}",
        compare.context.candidate.run_id,
        baseline_label,
        compare.strictness.strict,
        compare.strictness.comparison_authoritative
    ));

    for (index, opportunity) in compare
        .optimization_opportunities
        .iter()
        .take(10)
        .enumerate()
    {
        lines.push(String::new());
        lines.push(format!("{}. {}", index + 1, opportunity.metric_id));
        lines.push(format!(
            "   current={} baseline={} target={} delta_pct={} status={} score={}",
            fmt_opt(opportunity.current_value),
            fmt_opt(opportunity.baseline_value),
            fmt_opt(opportunity.target_value),
            fmt_pct_opt(opportunity.delta_percent),
            status_label(opportunity.status_relative_to_noise),
            fmt_num(opportunity.priority_score)
        ));
        lines.push(format!(
            "   likely_owners={}",
            format_owners(&opportunity.likely_owners)
        ));
    }

    if !compare.warnings.is_empty() {
        lines.push(String::new());
        lines.push("warnings:".to_string());
        for warning in &compare.warnings {
            lines.push(format!("- {}", warning));
        }
    }

    lines.join("\n")
}

fn render_compare_markdown_agent_brief(compare: &CompareArtifact) -> String {
    let mut lines = Vec::new();
    let baseline_label = compare
        .baseline_source
        .baseline_name
        .as_deref()
        .unwrap_or(compare.context.baseline.run_id.as_str());

    lines.push("# DecentDB Agent Brief".to_string());
    lines.push(String::new());
    lines.push(format!("Candidate: `{}`", compare.context.candidate.run_id));
    lines.push(format!("Baseline: `{}`", baseline_label));
    lines.push(format!(
        "Strict: `{}` | Authoritative: `{}`",
        compare.strictness.strict, compare.strictness.comparison_authoritative
    ));

    lines.push(String::new());
    lines.push("## Top Weak Metrics".to_string());
    if compare.optimization_opportunities.is_empty() {
        lines.push("- none".to_string());
    } else {
        for (index, opportunity) in compare
            .optimization_opportunities
            .iter()
            .take(10)
            .enumerate()
        {
            lines.push(format!("{}. `{}`", index + 1, opportunity.metric_id));
            lines.push(format!(
                "   current={} baseline={} target={} delta_pct={} status={} score={} owners={}",
                fmt_opt(opportunity.current_value),
                fmt_opt(opportunity.baseline_value),
                fmt_opt(opportunity.target_value),
                fmt_pct_opt(opportunity.delta_percent),
                status_label(opportunity.status_relative_to_noise),
                fmt_num(opportunity.priority_score),
                format_owners(&opportunity.likely_owners)
            ));
        }
    }

    if !compare.warnings.is_empty() {
        lines.push(String::new());
        lines.push("## Warnings".to_string());
        for warning in &compare.warnings {
            lines.push(format!("- {}", warning));
        }
    }

    lines.join("\n")
}

fn render_run_markdown_human(summary: &RunSummary) -> String {
    let mut lines = Vec::new();
    lines.push("# DecentDB Benchmark Snapshot".to_string());
    lines.push(String::new());
    lines.push(format!("Run ID: `{}`", summary.run_id));
    lines.push(format!("Profile: `{}`", summary.profile.as_str()));
    lines.push(format!("Status: `{}`", summary.status));

    lines.push(String::new());
    lines.push("## Headline KPI Snapshot".to_string());
    lines.push("| Scenario | Metric | Value |".to_string());
    lines.push("| --- | --- | ---: |".to_string());
    for scenario in &summary.scenarios {
        for (metric, value) in &scenario.headline_metrics {
            lines.push(format!(
                "| {} | {} | {} |",
                scenario.scenario_id.as_str(),
                metric,
                fmt_value(value)
            ));
        }
    }

    lines.push(String::new());
    lines.push("## Storage".to_string());
    if let Some(storage_metrics) = find_storage_headline(summary) {
        for (key, value) in storage_metrics {
            lines.push(format!("- {}: {}", key, fmt_value(value)));
        }
    } else {
        lines.push("- No storage metrics in summary headline.".to_string());
    }

    lines.push(String::new());
    lines.push("## Trust and Context Warnings".to_string());
    if summary.warnings.is_empty() {
        lines.push("- none".to_string());
    } else {
        for warning in &summary.warnings {
            lines.push(format!("- {}", warning));
        }
    }

    lines.join("\n")
}

fn render_run_text_human(summary: &RunSummary) -> String {
    let mut lines = Vec::new();
    lines.push("DecentDB Benchmark Snapshot".to_string());
    lines.push(format!(
        "run_id={} profile={} status={}",
        summary.run_id,
        summary.profile.as_str(),
        summary.status
    ));

    lines.push(String::new());
    lines.push("Headline Metrics".to_string());
    for scenario in &summary.scenarios {
        for (metric, value) in &scenario.headline_metrics {
            lines.push(format!(
                "  {}.{} = {}",
                scenario.scenario_id.as_str(),
                metric,
                fmt_value(value)
            ));
        }
    }

    lines.push(String::new());
    lines.push("Warnings".to_string());
    if summary.warnings.is_empty() {
        lines.push("  none".to_string());
    } else {
        for warning in &summary.warnings {
            lines.push(format!("  {}", warning));
        }
    }

    lines.join("\n")
}

fn render_run_agent_brief(summary: &RunSummary) -> String {
    let mut lines = Vec::new();
    lines.push("DecentDB Agent Brief".to_string());
    lines.push(format!(
        "run_id={} profile={} status={}",
        summary.run_id,
        summary.profile.as_str(),
        summary.status
    ));

    if let Some(assessment) = &summary.target_assessment {
        lines.push(String::new());
        lines.push(format!(
            "grade={} scope={} authoritative={}",
            assessment.overall_grade.as_deref().unwrap_or("partial"),
            assessment.scope,
            assessment.authoritative_context
        ));

        for (index, metric) in assessment
            .metrics
            .iter()
            .filter(|metric| metric.status == "below_floor" || metric.status == "below_target")
            .take(10)
            .enumerate()
        {
            lines.push(String::new());
            lines.push(format!("{}. {}", index + 1, metric.target_id));
            lines.push(format!(
                "   current={} target={} floor={} status={}",
                metric.current.map_or_else(|| "n/a".to_string(), fmt_num),
                metric.target.map_or_else(|| "n/a".to_string(), fmt_num),
                metric.floor.map_or_else(|| "n/a".to_string(), fmt_num),
                metric.status
            ));
            lines.push(format!(
                "   likely_owners={}",
                format_owners(&metric.likely_owners)
            ));
        }
    }

    if !summary.warnings.is_empty() {
        lines.push(String::new());
        lines.push("warnings:".to_string());
        for warning in &summary.warnings {
            lines.push(format!("- {}", warning));
        }
    }

    lines.join("\n")
}

fn render_run_markdown_agent_brief(summary: &RunSummary) -> String {
    let mut lines = Vec::new();
    lines.push("# DecentDB Agent Brief".to_string());
    lines.push(String::new());
    lines.push(format!("Run ID: `{}`", summary.run_id));
    lines.push(format!("Profile: `{}`", summary.profile.as_str()));
    lines.push(format!("Status: `{}`", summary.status));

    if let Some(assessment) = &summary.target_assessment {
        lines.push(String::new());
        lines.push("## Target Assessment".to_string());
        lines.push(format!(
            "- Grade: `{}`",
            assessment.overall_grade.as_deref().unwrap_or("partial")
        ));
        lines.push(format!("- Scope: `{}`", assessment.scope));
        lines.push(format!(
            "- Authoritative: `{}`",
            assessment.authoritative_context
        ));

        lines.push(String::new());
        lines.push("## Top Weak Metrics".to_string());
        let weak_metrics: Vec<_> = assessment
            .metrics
            .iter()
            .filter(|metric| metric.status == "below_floor" || metric.status == "below_target")
            .take(10)
            .collect();
        if weak_metrics.is_empty() {
            lines.push("- none".to_string());
        } else {
            for (index, metric) in weak_metrics.iter().enumerate() {
                lines.push(format!("{}. `{}`", index + 1, metric.target_id));
                lines.push(format!(
                    "   current={} target={} floor={} status={} owners={}",
                    metric.current.map_or_else(|| "n/a".to_string(), fmt_num),
                    metric.target.map_or_else(|| "n/a".to_string(), fmt_num),
                    metric.floor.map_or_else(|| "n/a".to_string(), fmt_num),
                    metric.status,
                    format_owners(&metric.likely_owners)
                ));
            }
        }
    }

    if !summary.warnings.is_empty() {
        lines.push(String::new());
        lines.push("## Warnings".to_string());
        for warning in &summary.warnings {
            lines.push(format!("- {}", warning));
        }
    }

    lines.join("\n")
}

fn key_kpi_metric_ids() -> &'static [&'static str] {
    &[
        "durable_commit_single.txn_p95_us",
        "point_lookup_warm.lookup_p95_us",
        "point_lookup_cold.first_read_p95_us",
        "range_scan_warm.rows_per_sec",
        "read_under_write.reader_p95_degradation_ratio",
        "checkpoint.checkpoint_ms",
        "recovery_reopen.reopen_p95_ms",
        "storage_efficiency.space_amplification",
    ]
}

fn metric_by_id<'a>(compare: &'a CompareArtifact, metric_id: &str) -> Option<&'a MetricComparison> {
    compare
        .metrics
        .iter()
        .find(|metric| metric.metric_id == metric_id)
}

fn find_storage_headline(summary: &RunSummary) -> Option<&BTreeMap<String, serde_json::Value>> {
    summary
        .scenarios
        .iter()
        .find(|scenario| scenario.scenario_id.as_str() == "storage_efficiency")
        .map(|scenario| &scenario.headline_metrics)
}

fn storage_row(
    metric: &str,
    candidate: Option<f64>,
    baseline: Option<f64>,
    delta: Option<f64>,
) -> String {
    format!(
        "| {} | {} | {} | {} |",
        metric,
        fmt_opt(candidate),
        fmt_opt(baseline),
        fmt_pct_opt(delta)
    )
}

fn fmt_value(value: &serde_json::Value) -> String {
    value.as_f64().map_or_else(|| value.to_string(), fmt_num)
}

fn fmt_num(value: f64) -> String {
    format!("{value:.3}")
}

fn fmt_opt(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), fmt_num)
}

fn fmt_pct(value: f64) -> String {
    format!("{value:+.2}%")
}

fn fmt_pct_opt(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), fmt_pct)
}

fn format_owners(owners: &[String]) -> String {
    if owners.is_empty() {
        "n/a".to_string()
    } else {
        owners.join(",")
    }
}

fn status_label(status: MetricComparisonStatus) -> &'static str {
    match status {
        MetricComparisonStatus::Improvement => "improvement",
        MetricComparisonStatus::Regression => "regression",
        MetricComparisonStatus::UnchangedWithinNoise => "within_noise",
        MetricComparisonStatus::MissingMetric => "missing_metric",
        MetricComparisonStatus::MissingTargetMetadata => "missing_metadata",
    }
}

fn read_json_file<T: for<'de> serde::Deserialize<'de>>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("read json {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse json {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{render_compare_agent_brief, render_compare_markdown_agent_brief};
    use crate::compare::{
        BaselineSource, CompareArtifact, CompareContext, CompareStrictness, CompareTotals,
        MetricComparison, MetricComparisonStatus, MetricTrend, OpportunityScoreComponents,
        OptimizationOpportunity, RunContext, StorageComparison, StorageSnapshot,
        TargetExpectations,
    };
    use crate::targets::TargetDirection;

    #[test]
    fn agent_brief_includes_top_metric() {
        let compare = CompareArtifact {
            artifact_kind: "decentdb_benchmark_compare".to_string(),
            schema_version: 1,
            compare_id: "cmp-1".to_string(),
            created_unix_ms: 1,
            output_path: "build/bench/compares/cmp-1.json".to_string(),
            targets_file: "benchmarks/targets.toml".to_string(),
            targets_format_version: 1,
            candidate_summary: "build/bench/runs/r1/summary.json".to_string(),
            baseline_source: BaselineSource {
                baseline_name: Some("local".to_string()),
                baseline_summary: Some("summary.json".to_string()),
                baseline_snapshot: Some("baseline.json".to_string()),
            },
            context: CompareContext {
                candidate: RunContext {
                    run_id: "cand".to_string(),
                    profile: "nightly".to_string(),
                    status: "passed".to_string(),
                    build_profile: Some("release".to_string()),
                    os: Some("linux".to_string()),
                    arch: Some("x86_64".to_string()),
                    git_sha: None,
                    git_branch: None,
                },
                baseline: RunContext {
                    run_id: "base".to_string(),
                    profile: "nightly".to_string(),
                    status: "passed".to_string(),
                    build_profile: Some("release".to_string()),
                    os: Some("linux".to_string()),
                    arch: Some("x86_64".to_string()),
                    git_sha: None,
                    git_branch: None,
                },
            },
            strictness: CompareStrictness {
                strict: true,
                meaningful: true,
                incompatible_context: false,
                candidate_authoritative: true,
                baseline_authoritative: true,
                comparison_authoritative: true,
                reasons: Vec::new(),
                target_expectations: TargetExpectations {
                    authoritative_build: Some("release".to_string()),
                    authoritative_benchmark_profile: Some("nightly".to_string()),
                    authoritative_host_class: None,
                },
            },
            totals: CompareTotals {
                total_metrics: 1,
                regressions: 1,
                improvements: 0,
                unchanged_within_noise: 0,
                missing_metric: 0,
                missing_target_metadata: 0,
            },
            metrics: vec![MetricComparison {
                metric_id: "durable_commit_single.txn_p95_us".to_string(),
                target_id: Some("durable_commit_single.txn_p95_us".to_string()),
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                display_name: None,
                status: MetricComparisonStatus::Regression,
                direction: Some(TargetDirection::SmallerIsBetter),
                unit: Some("microseconds".to_string()),
                current_value: Some(1200.0),
                baseline_value: Some(1000.0),
                target_value: Some(800.0),
                delta_value: Some(200.0),
                delta_percent: Some(20.0),
                directional_delta_percent: Some(-20.0),
                noise_band: Some(50.0),
                absolute_threshold: Some(50.0),
                relative_threshold: Some(0.1),
                delta_vs_target_value: Some(400.0),
                delta_vs_target_percent: Some(50.0),
                gap_to_target_ratio: Some(0.5),
                regression_beyond_noise_ratio: Some(0.15),
                floor_value: Some(2000.0),
                stretch_value: Some(600.0),
                expected_cache_mode: Some("real_fs".to_string()),
                expected_durability_mode: Some("full".to_string()),
                weight: Some(1.0),
                priority: Some("P0".to_string()),
                signature: true,
                likely_owners: vec!["wal".to_string()],
            }],
            top_regressions: vec![MetricTrend {
                metric_id: "durable_commit_single.txn_p95_us".to_string(),
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                delta_percent: -20.0,
                current_value: 1200.0,
                baseline_value: 1000.0,
                likely_owners: vec!["wal".to_string()],
            }],
            top_improvements: Vec::new(),
            optimization_opportunities: vec![OptimizationOpportunity {
                metric_id: "durable_commit_single.txn_p95_us".to_string(),
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                current_value: Some(1200.0),
                baseline_value: Some(1000.0),
                target_value: Some(800.0),
                direction: TargetDirection::SmallerIsBetter,
                delta_percent: Some(20.0),
                status_relative_to_noise: MetricComparisonStatus::Regression,
                priority_score: 67.5,
                likely_owners: vec!["wal".to_string()],
                components: OpportunityScoreComponents {
                    regression_beyond_noise_ratio: 0.15,
                    gap_to_target_ratio: 0.5,
                    weight: 1.0,
                    priority_boost: 1.35,
                },
            }],
            storage: Some(StorageComparison {
                candidate: StorageSnapshot::default(),
                baseline: StorageSnapshot::default(),
                delta_percent: StorageSnapshot::default(),
            }),
            warnings: Vec::new(),
        };

        let rendered = render_compare_agent_brief(&compare);
        assert!(rendered.contains("DecentDB Agent Brief"));
        assert!(rendered.contains("durable_commit_single.txn_p95_us"));
        assert!(rendered.contains("status=regression"));
    }

    #[test]
    fn markdown_agent_brief_renders_markdown_header() {
        let compare = CompareArtifact {
            artifact_kind: "decentdb_benchmark_compare".to_string(),
            schema_version: 1,
            compare_id: "cmp-1".to_string(),
            created_unix_ms: 1,
            output_path: "build/bench/compares/cmp-1.json".to_string(),
            targets_file: "benchmarks/targets.toml".to_string(),
            targets_format_version: 1,
            candidate_summary: "build/bench/runs/r1/summary.json".to_string(),
            baseline_source: BaselineSource {
                baseline_name: Some("local".to_string()),
                baseline_summary: Some("summary.json".to_string()),
                baseline_snapshot: Some("baseline.json".to_string()),
            },
            context: CompareContext {
                candidate: RunContext {
                    run_id: "cand".to_string(),
                    profile: "nightly".to_string(),
                    status: "passed".to_string(),
                    build_profile: Some("release".to_string()),
                    os: Some("linux".to_string()),
                    arch: Some("x86_64".to_string()),
                    git_sha: None,
                    git_branch: None,
                },
                baseline: RunContext {
                    run_id: "base".to_string(),
                    profile: "nightly".to_string(),
                    status: "passed".to_string(),
                    build_profile: Some("release".to_string()),
                    os: Some("linux".to_string()),
                    arch: Some("x86_64".to_string()),
                    git_sha: None,
                    git_branch: None,
                },
            },
            strictness: CompareStrictness {
                strict: true,
                meaningful: true,
                incompatible_context: false,
                candidate_authoritative: true,
                baseline_authoritative: true,
                comparison_authoritative: true,
                reasons: Vec::new(),
                target_expectations: TargetExpectations {
                    authoritative_build: Some("release".to_string()),
                    authoritative_benchmark_profile: Some("nightly".to_string()),
                    authoritative_host_class: None,
                },
            },
            totals: CompareTotals {
                total_metrics: 1,
                regressions: 1,
                improvements: 0,
                unchanged_within_noise: 0,
                missing_metric: 0,
                missing_target_metadata: 0,
            },
            metrics: Vec::new(),
            top_regressions: Vec::new(),
            top_improvements: Vec::new(),
            optimization_opportunities: vec![OptimizationOpportunity {
                metric_id: "durable_commit_single.txn_p95_us".to_string(),
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                current_value: Some(1200.0),
                baseline_value: Some(1000.0),
                target_value: Some(800.0),
                direction: TargetDirection::SmallerIsBetter,
                delta_percent: Some(20.0),
                status_relative_to_noise: MetricComparisonStatus::Regression,
                priority_score: 67.5,
                likely_owners: vec!["wal".to_string()],
                components: OpportunityScoreComponents {
                    regression_beyond_noise_ratio: 0.15,
                    gap_to_target_ratio: 0.5,
                    weight: 1.0,
                    priority_boost: 1.35,
                },
            }],
            storage: Some(StorageComparison {
                candidate: StorageSnapshot::default(),
                baseline: StorageSnapshot::default(),
                delta_percent: StorageSnapshot::default(),
            }),
            warnings: Vec::new(),
        };

        let rendered = render_compare_markdown_agent_brief(&compare);
        assert!(rendered.starts_with("# DecentDB Agent Brief"));
        assert!(rendered.contains("## Top Weak Metrics"));
    }
}
