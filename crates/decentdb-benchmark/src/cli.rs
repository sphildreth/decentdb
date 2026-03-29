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
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// Benchmark profile preset.
    #[arg(long, value_enum, default_value_t = ProfileKind::Dev)]
    pub profile: ProfileKind,

    /// Scenario IDs to run. Defaults to all Phase 1 scenarios.
    #[arg(long, value_enum)]
    pub scenario: Vec<ScenarioId>,

    /// Run all Phase 1 scenarios.
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
