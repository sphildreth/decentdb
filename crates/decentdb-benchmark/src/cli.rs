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
    Run(RunArgs),
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
