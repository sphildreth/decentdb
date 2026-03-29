use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::artifacts::RunSummary;
use crate::cli::{ReportArgs, ReportAudience, ReportFormat};
use crate::compare::{
    read_compare_artifact, CompareArtifact, MetricComparison, MetricComparisonStatus,
};
use crate::types::ScenarioResult;

pub(crate) fn run_report_command(args: ReportArgs) -> Result<()> {
    let run_source_count = usize::from(args.input.is_some()) + usize::from(args.latest_run);
    let compare_source_count =
        usize::from(args.compare.is_some()) + usize::from(args.latest_compare);
    if run_source_count > 1 || compare_source_count > 1 {
        return Err(anyhow!(
            "choose one run input (--input or --latest-run) and one compare input (--compare or --latest-compare)"
        ));
    }
    if run_source_count + compare_source_count != 1 {
        return Err(anyhow!(
            "report requires exactly one source: --input/--latest-run or --compare/--latest-compare"
        ));
    }

    let compare_input = if let Some(path) = args.compare.clone() {
        Some(path)
    } else if args.latest_compare {
        Some(find_latest_compare_artifact(&args.artifact_root)?)
    } else {
        None
    };
    let run_input = if let Some(path) = args.input.clone() {
        Some(path)
    } else if args.latest_run {
        Some(find_latest_run_summary(&args.artifact_root)?)
    } else {
        None
    };

    let rendered = if let Some(compare_path) = &compare_input {
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
            (ReportFormat::Html, ReportAudience::Human) => render_compare_html_human(&compare),
            (ReportFormat::Html, ReportAudience::AgentBrief) => {
                render_compare_html_agent_brief(&compare)
            }
        }
    } else {
        let input = run_input
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
            (ReportFormat::Html, ReportAudience::Human) => {
                render_run_html_human(&summary, input.as_path())?
            }
            (ReportFormat::Html, ReportAudience::AgentBrief) => {
                render_run_html_agent_brief(&summary)
            }
        }
    };

    if let Some(output_path) = args.output {
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create report output dir {}", parent.display()))?;
        }
        fs::write(&output_path, rendered.as_bytes())
            .with_context(|| format!("write report {}", output_path.display()))?;
        println!("report={}", output_path.display());
    } else {
        println!("{rendered}");
    }
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

#[derive(Debug, Clone)]
struct RunMetricTargetMeta {
    status: String,
    target: Option<f64>,
    floor: Option<f64>,
    stretch: Option<f64>,
    likely_owners: Vec<String>,
}

#[derive(Debug, Clone)]
struct RunMetricRow {
    scenario: String,
    metric: String,
    value: serde_json::Value,
    target_meta: Option<RunMetricTargetMeta>,
}

fn render_run_html_human(summary: &RunSummary, summary_path: &Path) -> Result<String> {
    let (rows, mut load_warnings) = collect_run_metric_rows(summary, summary_path);
    let grade = summary
        .target_assessment
        .as_ref()
        .and_then(|assessment| assessment.overall_grade.as_deref())
        .unwrap_or("partial");

    let mut elite_count = 0_usize;
    let mut green_count = 0_usize;
    let mut yellow_count = 0_usize;
    let mut red_count = 0_usize;
    let mut neutral_count = 0_usize;

    for row in &rows {
        match row.target_meta.as_ref().map(|meta| meta.status.as_str()) {
            Some("stretch_met") => elite_count += 1,
            Some("target_met") => green_count += 1,
            Some("below_target") => yellow_count += 1,
            Some("below_floor") => red_count += 1,
            Some(_) | None => neutral_count += 1,
        }
    }

    let assessed_total = elite_count + green_count + yellow_count + red_count + neutral_count;
    let mut body = String::new();
    body.push_str("<h1>DecentDB Benchmark Dashboard</h1>");
    body.push_str(&format!(
        "<div class=\"meta\">run_id=<code>{}</code> profile=<code>{}</code> status=<code>{}</code></div>",
        html_escape(&summary.run_id),
        html_escape(summary.profile.as_str()),
        html_escape(&summary.status)
    ));

    body.push_str("<section class=\"cards\">");
    body.push_str(&card_html("Overall Grade", grade, grade_class(grade)));
    body.push_str(&card_html(
        "Scenario Count",
        &summary.scenario_count.to_string(),
        "neutral",
    ));
    body.push_str(&card_html(
        "Measured Metrics",
        &rows.len().to_string(),
        "neutral",
    ));
    if let Some(assessment) = &summary.target_assessment {
        body.push_str(&card_html(
            "Target-Matched Metrics",
            &format!(
                "{}/{}",
                assessment.matched_metrics, assessment.total_metrics
            ),
            "neutral",
        ));
    }
    body.push_str("</section>");

    body.push_str("<h2>Status Overview</h2>");
    body.push_str("<div class=\"legend\">");
    body.push_str("<span class=\"badge elite\">elite</span>");
    body.push_str("<span class=\"badge good\">green</span>");
    body.push_str("<span class=\"badge warn\">below_target</span>");
    body.push_str("<span class=\"badge bad\">below_floor</span>");
    body.push_str("<span class=\"badge neutral\">untargeted/missing</span>");
    body.push_str("</div>");

    body.push_str("<div class=\"status-grid\">");
    body.push_str(&status_bar_html(
        "Elite",
        elite_count,
        assessed_total,
        "elite",
    ));
    body.push_str(&status_bar_html(
        "Green",
        green_count,
        assessed_total,
        "good",
    ));
    body.push_str(&status_bar_html(
        "Below Target",
        yellow_count,
        assessed_total,
        "warn",
    ));
    body.push_str(&status_bar_html(
        "Below Floor",
        red_count,
        assessed_total,
        "bad",
    ));
    body.push_str(&status_bar_html(
        "Untargeted/Missing",
        neutral_count,
        assessed_total,
        "neutral",
    ));
    body.push_str("</div>");

    body.push_str("<h2>All Benchmarked Values</h2>");
    body.push_str("<table><thead><tr><th>Scenario</th><th>Metric</th><th>Value</th><th>Status</th><th>Target</th><th>Floor</th><th>Stretch</th><th>Likely Owners</th></tr></thead><tbody>");
    for row in &rows {
        let (status_label, status_class) =
            run_metric_status_badge(row.target_meta.as_ref().map(|meta| meta.status.as_str()));
        let target = row
            .target_meta
            .as_ref()
            .and_then(|meta| meta.target)
            .map(fmt_num)
            .unwrap_or_else(|| "n/a".to_string());
        let floor = row
            .target_meta
            .as_ref()
            .and_then(|meta| meta.floor)
            .map(fmt_num)
            .unwrap_or_else(|| "n/a".to_string());
        let stretch = row
            .target_meta
            .as_ref()
            .and_then(|meta| meta.stretch)
            .map(fmt_num)
            .unwrap_or_else(|| "n/a".to_string());
        let owners = row.target_meta.as_ref().map_or_else(
            || "n/a".to_string(),
            |meta| format_owners(&meta.likely_owners),
        );
        body.push_str(&format!(
            "<tr><td><code>{}</code></td><td><code>{}</code></td><td class=\"num\">{}</td><td><span class=\"badge {}\">{}</span></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td>{}</td></tr>",
            html_escape(&row.scenario),
            html_escape(&row.metric),
            html_escape(&fmt_value(&row.value)),
            status_class,
            status_label,
            html_escape(&target),
            html_escape(&floor),
            html_escape(&stretch),
            html_escape(&owners)
        ));
    }
    body.push_str("</tbody></table>");

    if !summary.warnings.is_empty() {
        load_warnings.extend(summary.warnings.iter().cloned());
    }
    if !load_warnings.is_empty() {
        body.push_str("<h2>Warnings</h2><ul>");
        for warning in &load_warnings {
            body.push_str(&format!("<li>{}</li>", html_escape(warning)));
        }
        body.push_str("</ul>");
    }

    Ok(html_shell("DecentDB Benchmark Dashboard", &body))
}

fn render_compare_html_human(compare: &CompareArtifact) -> String {
    let baseline_label = compare
        .baseline_source
        .baseline_name
        .as_deref()
        .unwrap_or(compare.context.baseline.run_id.as_str());

    let mut improvement_count = 0_usize;
    let mut regression_count = 0_usize;
    let mut within_noise_count = 0_usize;
    let mut missing_count = 0_usize;
    let mut missing_metadata_count = 0_usize;
    for metric in &compare.metrics {
        match metric.status {
            MetricComparisonStatus::Improvement => improvement_count += 1,
            MetricComparisonStatus::Regression => regression_count += 1,
            MetricComparisonStatus::UnchangedWithinNoise => within_noise_count += 1,
            MetricComparisonStatus::MissingMetric => missing_count += 1,
            MetricComparisonStatus::MissingTargetMetadata => missing_metadata_count += 1,
        }
    }

    let status_total = compare.metrics.len();
    let mut body = String::new();
    body.push_str("<h1>DecentDB Comparison Dashboard</h1>");
    body.push_str(&format!(
        "<div class=\"meta\">candidate=<code>{}</code> baseline=<code>{}</code> strict=<code>{}</code> authoritative=<code>{}</code></div>",
        html_escape(&compare.context.candidate.run_id),
        html_escape(baseline_label),
        compare.strictness.strict,
        compare.strictness.comparison_authoritative
    ));

    body.push_str("<section class=\"cards\">");
    body.push_str(&card_html(
        "Regressions",
        &compare.totals.regressions.to_string(),
        "bad",
    ));
    body.push_str(&card_html(
        "Improvements",
        &compare.totals.improvements.to_string(),
        "good",
    ));
    body.push_str(&card_html(
        "Within Noise",
        &compare.totals.unchanged_within_noise.to_string(),
        "neutral",
    ));
    body.push_str(&card_html(
        "Missing Metrics",
        &compare.totals.missing_metric.to_string(),
        "warn",
    ));
    body.push_str("</section>");

    body.push_str("<h2>Status Distribution</h2>");
    body.push_str("<div class=\"status-grid\">");
    body.push_str(&status_bar_html(
        "Regressions",
        regression_count,
        status_total,
        "bad",
    ));
    body.push_str(&status_bar_html(
        "Improvements",
        improvement_count,
        status_total,
        "good",
    ));
    body.push_str(&status_bar_html(
        "Within Noise",
        within_noise_count,
        status_total,
        "neutral",
    ));
    body.push_str(&status_bar_html(
        "Missing Metric",
        missing_count,
        status_total,
        "warn",
    ));
    body.push_str(&status_bar_html(
        "Missing Metadata",
        missing_metadata_count,
        status_total,
        "neutral",
    ));
    body.push_str("</div>");

    body.push_str("<h2>Top Optimization Opportunities</h2>");
    body.push_str("<table><thead><tr><th>Metric</th><th>Current</th><th>Baseline</th><th>Target</th><th>Delta %</th><th>Status</th><th>Score</th><th>Likely Owners</th></tr></thead><tbody>");
    for opportunity in compare.optimization_opportunities.iter().take(20) {
        let (status_label, status_class) =
            compare_status_badge(opportunity.status_relative_to_noise);
        body.push_str(&format!(
            "<tr><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td><span class=\"badge {}\">{}</span></td><td class=\"num\">{}</td><td>{}</td></tr>",
            html_escape(&opportunity.metric_id),
            html_escape(&fmt_opt(opportunity.current_value)),
            html_escape(&fmt_opt(opportunity.baseline_value)),
            html_escape(&fmt_opt(opportunity.target_value)),
            html_escape(&fmt_pct_opt(opportunity.delta_percent)),
            status_class,
            status_label,
            html_escape(&fmt_num(opportunity.priority_score)),
            html_escape(&format_owners(&opportunity.likely_owners))
        ));
    }
    body.push_str("</tbody></table>");

    body.push_str("<h2>All Compared Metrics</h2>");
    body.push_str("<table><thead><tr><th>Metric</th><th>Scenario</th><th>Current</th><th>Baseline</th><th>Target</th><th>Delta %</th><th>Status</th><th>Likely Owners</th></tr></thead><tbody>");
    for metric in &compare.metrics {
        let (status_label, status_class) = compare_status_badge(metric.status);
        body.push_str(&format!(
            "<tr><td><code>{}</code></td><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td><span class=\"badge {}\">{}</span></td><td>{}</td></tr>",
            html_escape(&metric.metric_id),
            html_escape(&metric.scenario),
            html_escape(&fmt_opt(metric.current_value)),
            html_escape(&fmt_opt(metric.baseline_value)),
            html_escape(&fmt_opt(metric.target_value)),
            html_escape(&fmt_pct_opt(metric.directional_delta_percent)),
            status_class,
            status_label,
            html_escape(&format_owners(&metric.likely_owners))
        ));
    }
    body.push_str("</tbody></table>");

    if !compare.warnings.is_empty() {
        body.push_str("<h2>Warnings</h2><ul>");
        for warning in &compare.warnings {
            body.push_str(&format!("<li>{}</li>", html_escape(warning)));
        }
        body.push_str("</ul>");
    }

    html_shell("DecentDB Comparison Dashboard", &body)
}

fn render_compare_html_agent_brief(compare: &CompareArtifact) -> String {
    let baseline_label = compare
        .baseline_source
        .baseline_name
        .as_deref()
        .unwrap_or(compare.context.baseline.run_id.as_str());

    let mut body = String::new();
    body.push_str("<h1>DecentDB Agent Brief</h1>");
    body.push_str(&format!(
        "<div class=\"meta\">candidate=<code>{}</code> baseline=<code>{}</code> strict=<code>{}</code> authoritative=<code>{}</code></div>",
        html_escape(&compare.context.candidate.run_id),
        html_escape(baseline_label),
        compare.strictness.strict,
        compare.strictness.comparison_authoritative
    ));
    body.push_str("<table><thead><tr><th>#</th><th>Metric</th><th>Current</th><th>Baseline</th><th>Target</th><th>Delta %</th><th>Status</th><th>Score</th><th>Likely Owners</th></tr></thead><tbody>");
    for (index, opportunity) in compare
        .optimization_opportunities
        .iter()
        .take(20)
        .enumerate()
    {
        let (status_label, status_class) =
            compare_status_badge(opportunity.status_relative_to_noise);
        body.push_str(&format!(
            "<tr><td>{}</td><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td><span class=\"badge {}\">{}</span></td><td class=\"num\">{}</td><td>{}</td></tr>",
            index + 1,
            html_escape(&opportunity.metric_id),
            html_escape(&fmt_opt(opportunity.current_value)),
            html_escape(&fmt_opt(opportunity.baseline_value)),
            html_escape(&fmt_opt(opportunity.target_value)),
            html_escape(&fmt_pct_opt(opportunity.delta_percent)),
            status_class,
            status_label,
            html_escape(&fmt_num(opportunity.priority_score)),
            html_escape(&format_owners(&opportunity.likely_owners))
        ));
    }
    body.push_str("</tbody></table>");
    html_shell("DecentDB Agent Brief", &body)
}

fn render_run_html_agent_brief(summary: &RunSummary) -> String {
    let mut body = String::new();
    body.push_str("<h1>DecentDB Agent Brief</h1>");
    body.push_str(&format!(
        "<div class=\"meta\">run_id=<code>{}</code> profile=<code>{}</code> status=<code>{}</code></div>",
        html_escape(&summary.run_id),
        html_escape(summary.profile.as_str()),
        html_escape(&summary.status)
    ));

    if let Some(assessment) = &summary.target_assessment {
        body.push_str("<table><thead><tr><th>#</th><th>Metric</th><th>Current</th><th>Target</th><th>Floor</th><th>Status</th><th>Likely Owners</th></tr></thead><tbody>");
        for (index, metric) in assessment
            .metrics
            .iter()
            .filter(|metric| metric.status == "below_floor" || metric.status == "below_target")
            .take(20)
            .enumerate()
        {
            let (status_label, status_class) =
                run_metric_status_badge(Some(metric.status.as_str()));
            body.push_str(&format!(
                "<tr><td>{}</td><td><code>{}</code></td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td><span class=\"badge {}\">{}</span></td><td>{}</td></tr>",
                index + 1,
                html_escape(&metric.target_id),
                html_escape(&metric.current.map_or_else(|| "n/a".to_string(), fmt_num)),
                html_escape(&metric.target.map_or_else(|| "n/a".to_string(), fmt_num)),
                html_escape(&metric.floor.map_or_else(|| "n/a".to_string(), fmt_num)),
                status_class,
                status_label,
                html_escape(&format_owners(&metric.likely_owners))
            ));
        }
        body.push_str("</tbody></table>");
    }

    html_shell("DecentDB Agent Brief", &body)
}

fn collect_run_metric_rows(
    summary: &RunSummary,
    summary_path: &Path,
) -> (Vec<RunMetricRow>, Vec<String>) {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();

    let mut target_lookup = BTreeMap::<String, RunMetricTargetMeta>::new();
    if let Some(assessment) = &summary.target_assessment {
        for metric in &assessment.metrics {
            let key = format!("{}.{}", metric.scenario, metric.metric);
            target_lookup.insert(
                key,
                RunMetricTargetMeta {
                    status: metric.status.clone(),
                    target: metric.target,
                    floor: metric.floor,
                    stretch: metric.stretch,
                    likely_owners: metric.likely_owners.clone(),
                },
            );
        }
    }

    for scenario in &summary.scenarios {
        let artifact_path = resolve_artifact_path(summary_path, &scenario.artifact_file);
        match read_json_file::<ScenarioResult>(&artifact_path) {
            Ok(result) => {
                for (metric, value) in result.metrics {
                    let key = format!("{}.{}", scenario.scenario_id.as_str(), metric);
                    rows.push(RunMetricRow {
                        scenario: scenario.scenario_id.as_str().to_string(),
                        metric,
                        value,
                        target_meta: target_lookup.get(&key).cloned(),
                    });
                }
            }
            Err(error) => {
                warnings.push(format!(
                    "failed to read scenario artifact {}: {}",
                    artifact_path.display(),
                    error
                ));
                for (metric, value) in &scenario.headline_metrics {
                    let key = format!("{}.{}", scenario.scenario_id.as_str(), metric);
                    rows.push(RunMetricRow {
                        scenario: scenario.scenario_id.as_str().to_string(),
                        metric: metric.clone(),
                        value: value.clone(),
                        target_meta: target_lookup.get(&key).cloned(),
                    });
                }
            }
        }
    }

    rows.sort_by(|left, right| {
        left.scenario
            .cmp(&right.scenario)
            .then_with(|| left.metric.cmp(&right.metric))
    });
    (rows, warnings)
}

fn resolve_artifact_path(summary_path: &Path, artifact_file: &str) -> std::path::PathBuf {
    let direct = Path::new(artifact_file);
    if direct.exists() {
        return direct.to_path_buf();
    }
    if let Some(parent) = summary_path.parent() {
        let joined = parent.join(artifact_file);
        if joined.exists() {
            return joined;
        }
    }
    direct.to_path_buf()
}

fn find_latest_run_summary(artifact_root: &Path) -> Result<PathBuf> {
    let runs_dir = artifact_root.join("runs");
    let mut latest: Option<(std::time::SystemTime, PathBuf)> = None;
    let entries =
        fs::read_dir(&runs_dir).with_context(|| format!("read runs dir {}", runs_dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("read entry in {}", runs_dir.display()))?;
        let summary = entry.path().join("summary.json");
        if !summary.is_file() {
            continue;
        }
        let modified = fs::metadata(&summary)
            .with_context(|| format!("stat {}", summary.display()))?
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        if latest
            .as_ref()
            .is_none_or(|(best_modified, _)| modified > *best_modified)
        {
            latest = Some((modified, summary));
        }
    }
    latest
        .map(|(_, path)| path)
        .ok_or_else(|| anyhow!("no run summary.json found under {}", runs_dir.display()))
}

fn find_latest_compare_artifact(artifact_root: &Path) -> Result<PathBuf> {
    let compares_dir = artifact_root.join("compares");
    let mut latest: Option<(std::time::SystemTime, PathBuf)> = None;
    let entries = fs::read_dir(&compares_dir)
        .with_context(|| format!("read compares dir {}", compares_dir.display()))?;
    for entry in entries {
        let entry = entry.with_context(|| format!("read entry in {}", compares_dir.display()))?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let modified = fs::metadata(&path)
            .with_context(|| format!("stat {}", path.display()))?
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        if latest
            .as_ref()
            .is_none_or(|(best_modified, _)| modified > *best_modified)
        {
            latest = Some((modified, path));
        }
    }
    latest
        .map(|(_, path)| path)
        .ok_or_else(|| anyhow!("no compare json found under {}", compares_dir.display()))
}

fn html_shell(title: &str, body: &str) -> String {
    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title><style>{}</style></head><body><main>{}</main></body></html>",
        html_escape(title),
        dashboard_css(),
        body
    )
}

fn dashboard_css() -> &'static str {
    "body { font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 0; background: #f4f6f8; color: #10202d; }
main { max-width: 1280px; margin: 0 auto; padding: 24px; }
h1 { margin: 0 0 12px 0; font-size: 28px; }
h2 { margin: 22px 0 12px 0; font-size: 20px; }
.meta { margin: 0 0 16px 0; color: #3f4d5a; }
code { background: #eaf0f5; padding: 2px 6px; border-radius: 4px; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(210px, 1fr)); gap: 10px; margin: 10px 0 16px 0; }
.card { border-radius: 10px; padding: 12px; border: 1px solid #d9e2ea; background: #fff; }
.card .label { color: #3f4d5a; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }
.card .value { font-size: 24px; font-weight: 700; margin-top: 6px; }
.card.good { border-color: #4caf50; background: #eef8ef; }
.card.warn { border-color: #f0ad4e; background: #fff7ea; }
.card.bad { border-color: #d9534f; background: #fff0ef; }
.card.elite { border-color: #198754; background: #e6f6eb; }
.card.neutral { border-color: #d9e2ea; background: #fff; }
.legend { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 10px; }
.status-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 8px; margin-bottom: 12px; }
.status-item { background: #fff; border: 1px solid #d9e2ea; border-radius: 8px; padding: 10px; }
.status-item .top { display: flex; justify-content: space-between; margin-bottom: 6px; font-size: 13px; }
.bar { width: 100%; height: 9px; border-radius: 6px; background: #e5ebf0; overflow: hidden; }
.bar > span { display: block; height: 100%; border-radius: 6px; }
.bar.good > span { background: #2e7d32; }
.bar.warn > span { background: #f39c12; }
.bar.bad > span { background: #c62828; }
.bar.elite > span { background: #146c43; }
.bar.neutral > span { background: #607d8b; }
table { width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d9e2ea; }
th, td { border-bottom: 1px solid #e6edf2; padding: 8px 10px; font-size: 13px; text-align: left; vertical-align: top; }
th { background: #f0f4f7; position: sticky; top: 0; z-index: 1; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
.badge { display: inline-block; border-radius: 999px; font-size: 11px; padding: 3px 8px; text-transform: uppercase; letter-spacing: 0.03em; font-weight: 700; }
.badge.good { background: #daf2dc; color: #245f29; }
.badge.warn { background: #fff0d7; color: #8f5a00; }
.badge.bad { background: #ffdede; color: #8e1a1a; }
.badge.elite { background: #d8f5e2; color: #11512d; }
.badge.neutral { background: #e9eff4; color: #344a5c; }
ul { background: #fff; border: 1px solid #d9e2ea; border-radius: 8px; padding: 10px 18px; }
li { margin: 6px 0; }"
}

fn card_html(label: &str, value: &str, class_name: &str) -> String {
    format!(
        "<div class=\"card {}\"><div class=\"label\">{}</div><div class=\"value\">{}</div></div>",
        class_name,
        html_escape(label),
        html_escape(value)
    )
}

fn status_bar_html(label: &str, count: usize, total: usize, class_name: &str) -> String {
    let percent = if total == 0 {
        0.0
    } else {
        (count as f64 / total as f64) * 100.0
    };
    format!(
        "<div class=\"status-item\"><div class=\"top\"><span>{}</span><span>{}</span></div><div class=\"bar {}\"><span style=\"width:{:.1}%\"></span></div></div>",
        html_escape(label),
        count,
        class_name,
        percent
    )
}

fn grade_class(grade: &str) -> &'static str {
    match grade {
        "elite" => "elite",
        "gold" | "target_met" => "good",
        "silver" | "partial" => "warn",
        _ => "neutral",
    }
}

fn run_metric_status_badge(status: Option<&str>) -> (&'static str, &'static str) {
    match status {
        Some("stretch_met") => ("elite", "elite"),
        Some("target_met") => ("green", "good"),
        Some("below_target") => ("below_target", "warn"),
        Some("below_floor") => ("below_floor", "bad"),
        Some(_) => ("missing", "neutral"),
        None => ("untargeted", "neutral"),
    }
}

fn compare_status_badge(status: MetricComparisonStatus) -> (&'static str, &'static str) {
    match status {
        MetricComparisonStatus::Improvement => ("improvement", "good"),
        MetricComparisonStatus::Regression => ("regression", "bad"),
        MetricComparisonStatus::UnchangedWithinNoise => ("within_noise", "neutral"),
        MetricComparisonStatus::MissingMetric => ("missing_metric", "warn"),
        MetricComparisonStatus::MissingTargetMetadata => ("missing_metadata", "neutral"),
    }
}

fn html_escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
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
    use super::{
        render_compare_agent_brief, render_compare_html_human, render_compare_markdown_agent_brief,
        render_run_html_agent_brief,
    };
    use crate::artifacts::RunSummary;
    use crate::compare::{
        BaselineSource, CompareArtifact, CompareContext, CompareStrictness, CompareTotals,
        MetricComparison, MetricComparisonStatus, MetricTrend, OpportunityScoreComponents,
        OptimizationOpportunity, RunContext, StorageComparison, StorageSnapshot,
        TargetExpectations,
    };
    use crate::targets::TargetDirection;
    use crate::types::ProfileKind;

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

    #[test]
    fn compare_html_renders_dashboard_sections() {
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
                regressions: 0,
                improvements: 1,
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
                status: MetricComparisonStatus::Improvement,
                direction: Some(TargetDirection::SmallerIsBetter),
                unit: Some("microseconds".to_string()),
                current_value: Some(900.0),
                baseline_value: Some(1000.0),
                target_value: Some(800.0),
                delta_value: Some(-100.0),
                delta_percent: Some(-10.0),
                directional_delta_percent: Some(10.0),
                noise_band: Some(50.0),
                absolute_threshold: Some(50.0),
                relative_threshold: Some(0.1),
                delta_vs_target_value: Some(100.0),
                delta_vs_target_percent: Some(12.5),
                gap_to_target_ratio: Some(0.125),
                regression_beyond_noise_ratio: Some(0.0),
                floor_value: Some(1200.0),
                stretch_value: Some(700.0),
                expected_cache_mode: Some("real_fs".to_string()),
                expected_durability_mode: Some("full".to_string()),
                weight: Some(1.0),
                priority: Some("P0".to_string()),
                signature: true,
                likely_owners: vec!["wal".to_string()],
            }],
            top_regressions: Vec::new(),
            top_improvements: vec![MetricTrend {
                metric_id: "durable_commit_single.txn_p95_us".to_string(),
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                delta_percent: -10.0,
                current_value: 900.0,
                baseline_value: 1000.0,
                likely_owners: vec!["wal".to_string()],
            }],
            optimization_opportunities: vec![OptimizationOpportunity {
                metric_id: "durable_commit_single.txn_p95_us".to_string(),
                scenario: "durable_commit_single".to_string(),
                metric: "txn_p95_us".to_string(),
                current_value: Some(900.0),
                baseline_value: Some(1000.0),
                target_value: Some(800.0),
                direction: TargetDirection::SmallerIsBetter,
                delta_percent: Some(-10.0),
                status_relative_to_noise: MetricComparisonStatus::Improvement,
                priority_score: 1.0,
                likely_owners: vec!["wal".to_string()],
                components: OpportunityScoreComponents {
                    regression_beyond_noise_ratio: 0.0,
                    gap_to_target_ratio: 0.125,
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

        let rendered = render_compare_html_human(&compare);
        assert!(rendered.contains("DecentDB Comparison Dashboard"));
        assert!(rendered.contains("All Compared Metrics"));
    }

    #[test]
    fn run_html_agent_brief_renders_header() {
        let summary = RunSummary {
            run_id: "run-1".to_string(),
            profile: ProfileKind::Smoke,
            dry_run: false,
            status: "passed".to_string(),
            started_unix_ms: 1,
            finished_unix_ms: 2,
            scenario_count: 0,
            passed: 0,
            failed: 0,
            skipped: 0,
            scenarios: Vec::new(),
            warnings: Vec::new(),
            target_assessment: None,
        };
        let rendered = render_run_html_agent_brief(&summary);
        assert!(rendered.contains("DecentDB Agent Brief"));
    }
}
