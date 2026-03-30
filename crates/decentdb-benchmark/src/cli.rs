use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::types::{ProfileKind, ScenarioId};

#[derive(Debug, Parser)]
#[command(
    name = "decentdb-benchmark",
    about = "Rust-native benchmark runner for DecentDB"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run one or more benchmark scenarios.
    Run(Box<RunArgs>),
    /// Compare benchmark artifacts and rank optimization opportunities.
    Compare(CompareArgs),
    /// Manage named local baseline snapshots.
    Baseline(BaselineArgs),
    /// Render run or compare artifacts for humans or coding agents.
    Report(ReportArgs),
    /// Inspect storage layout and page attribution for an existing .ddb file.
    InspectStorage(InspectStorageArgs),
    /// Internal helper commands used by benchmark scenarios.
    #[command(hide = true)]
    Internal(InternalArgs),
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// Benchmark profile preset.
    #[arg(long, value_enum, default_value_t = ProfileKind::Dev)]
    pub profile: ProfileKind,

    /// Scenario IDs to run. Defaults to all implemented scenarios.
    #[arg(long, value_enum)]
    pub scenario: Vec<ScenarioId>,

    /// Run all implemented scenarios.
    #[arg(long)]
    pub all: bool,

    /// Resolve configuration and paths only. Do not execute scenarios.
    #[arg(long)]
    pub dry_run: bool,

    /// Override total seed rows for relevant scenarios.
    #[arg(long)]
    pub rows: Option<u64>,

    /// Override measured point-lookup operations.
    #[arg(long)]
    pub point_reads: Option<u64>,

    /// Override rows targeted by each range scan.
    #[arg(long)]
    pub range_scan_rows: Option<u64>,

    /// Override measured range-scan operations.
    #[arg(long)]
    pub range_scans: Option<u64>,

    /// Override measured durable commits.
    #[arg(long)]
    pub durable_commits: Option<u64>,

    /// Override rows per transaction batch for batch-write style scenarios.
    #[arg(long)]
    pub batch_size: Option<u64>,

    /// Override cold-process batch count for cold lookup scenarios.
    #[arg(long)]
    pub cold_batches: Option<u64>,

    /// Override reader thread count for read-under-write scenario.
    #[arg(long)]
    pub reader_threads: Option<u32>,

    /// Override measured writer operations for read-under-write scenario.
    #[arg(long)]
    pub writer_ops: Option<u64>,

    /// Override complex-workload user rows for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_users: Option<u64>,

    /// Override complex-workload item rows for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_items: Option<u64>,

    /// Override complex-workload order rows for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_orders: Option<u64>,

    /// Override complex-workload history lookups for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_history_reads: Option<u64>,

    /// Override complex-workload point lookups for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_point_lookups: Option<u64>,

    /// Override complex-workload range scans for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_range_scans: Option<u64>,

    /// Override complex-workload join queries for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_joins: Option<u64>,

    /// Override complex-workload aggregate queries for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_aggregates: Option<u64>,

    /// Override complex-workload updates for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_updates: Option<u64>,

    /// Override complex-workload deletes for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_deletes: Option<u64>,

    /// Override complex-workload table scans for complex_ecommerce scenario.
    #[arg(long)]
    pub complex_table_scans: Option<u64>,

    /// Override warmup operations for latency scenarios.
    #[arg(long)]
    pub warmup_ops: Option<u64>,

    /// Override trial count.
    #[arg(long)]
    pub trials: Option<u32>,

    /// Override deterministic seed.
    #[arg(long)]
    pub seed: Option<u64>,

    /// Scratch root for temporary benchmark execution files.
    #[arg(long, default_value = ".tmp/decentdb-benchmark")]
    pub scratch_root: PathBuf,

    /// Retained artifact root.
    #[arg(long, default_value = "build/bench")]
    pub artifact_root: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub struct InspectStorageArgs {
    /// Path to an existing DecentDB file.
    #[arg(long)]
    pub db_path: PathBuf,

    /// Optional output path for JSON (prints to stdout when omitted).
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct CompareArgs {
    /// Candidate run summary path.
    #[arg(long)]
    pub candidate: PathBuf,

    /// Explicit baseline run summary path.
    #[arg(long)]
    pub baseline: Option<PathBuf>,

    /// Named baseline under build/bench/baselines/<name>.json.
    #[arg(long)]
    pub baseline_name: Option<String>,

    /// Targets metadata path.
    #[arg(long, default_value = "benchmarks/targets.toml")]
    pub targets: PathBuf,

    /// Retained artifact root.
    #[arg(long, default_value = "build/bench")]
    pub artifact_root: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub struct BaselineArgs {
    #[command(subcommand)]
    pub command: BaselineCommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum BaselineCommand {
    /// Create or replace a named baseline snapshot from a run summary.
    Set(BaselineSetArgs),
}

#[derive(Debug, Clone, Args)]
pub struct BaselineSetArgs {
    /// Baseline name.
    #[arg(long)]
    pub name: String,

    /// Input run summary path.
    #[arg(long)]
    pub input: PathBuf,

    /// Retained artifact root.
    #[arg(long, default_value = "build/bench")]
    pub artifact_root: PathBuf,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ReportFormat {
    Markdown,
    Text,
    Html,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum ReportAudience {
    Human,
    AgentBrief,
}

#[derive(Debug, Clone, Args)]
pub struct ReportArgs {
    /// Run summary input for snapshot reports.
    #[arg(long)]
    pub input: Option<PathBuf>,

    /// Use the most recent run summary under build/bench/runs/.
    #[arg(long)]
    pub latest_run: bool,

    /// Compare artifact input for progress and ranking reports.
    #[arg(long)]
    pub compare: Option<PathBuf>,

    /// Use the most recent compare artifact under build/bench/compares/.
    #[arg(long)]
    pub latest_compare: bool,

    /// Artifact root used by --latest-run / --latest-compare discovery.
    #[arg(long, default_value = "build/bench")]
    pub artifact_root: PathBuf,

    /// Report format.
    #[arg(long, value_enum, default_value_t = ReportFormat::Text)]
    pub format: ReportFormat,

    /// Report audience profile.
    #[arg(long, value_enum, default_value_t = ReportAudience::Human)]
    pub audience: ReportAudience,

    /// Optional output file path. Prints to stdout when omitted.
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct InternalArgs {
    #[command(subcommand)]
    pub command: InternalCommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum InternalCommand {
    #[command(name = "cold-point-lookup-probe")]
    ColdPointLookupProbe(ColdPointLookupProbeArgs),
    #[command(name = "recovery-reopen-probe")]
    RecoveryReopenProbe(RecoveryReopenProbeArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ColdPointLookupProbeArgs {
    #[arg(long)]
    pub db_path: PathBuf,
    #[arg(long)]
    pub rows: u64,
    #[arg(long)]
    pub seed: u64,
    #[arg(long)]
    pub trial: u32,
    #[arg(long)]
    pub start_op: u64,
    #[arg(long)]
    pub lookups: u64,
}

#[derive(Debug, Clone, Args)]
pub struct RecoveryReopenProbeArgs {
    #[arg(long)]
    pub db_path: PathBuf,
    #[arg(long)]
    pub expected_rows: u64,
}
