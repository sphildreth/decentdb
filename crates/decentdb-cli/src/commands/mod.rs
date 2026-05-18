use std::convert::TryFrom;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;
use std::time::Instant;

use anyhow::{anyhow, Result};
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use decentdb::{
    evict_shared_wal, render_markdown, run_doctor, BranchDiffReport, BranchInfo, BranchLogEntry,
    BranchMergeOperation, BranchMergeReport, BranchRestoreReport, BranchTableDiffStatus,
    BulkLoadOptions, Db, DbConfig, DoctorCategory, DoctorCheckSelection, DoctorIndexVerification,
    DoctorOptions, DoctorPathMode, DoctorReport, DoctorSeverity, HeaderInfo, IndexVerification,
    NamedSnapshot, QueryResult, StorageInfo, SyncChangeBatch, SyncConflict, SyncConflictPolicy,
    SyncHandshake, SyncImportSummary, SyncPeer, SyncPeerScopeBinding, SyncRunDirection,
    SyncRunSummary, SyncScope, TableInfo, Value,
};

use crate::output::{
    render_error_json, render_exec_success_json, render_key_value_rows, render_rows,
    rows_from_query_result, stringify_value, OutputFormat,
};
use crate::repl::run_repl;

#[derive(Parser)]
#[command(name = "decentdb")]
#[command(version = decentdb::version())]
#[command(before_help = concat!(
    "DecentDB CLI v", env!("CARGO_PKG_VERSION"), "\n",
    "  ___                 _   ___  ___  \n",
    " |   \\ ___ __ ___ _ _| |_|   \\| _ ) \n",
    " | |) / -_) _/ -_) ' \\  _| |) | _ \\ \n",
    " |___/\\___\\__\\___|_||_\\__|___/|___/ \n"
))]
#[command(about = "DecentDB Command Line Interface")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Print engine version
    Version,
    /// Execute SQL statements or manage database
    Exec(ExecCommand),
    /// Interactive REPL mode
    Repl(ReplCommand),
    /// Import data from CSV or JSON
    Import(ImportCommand),
    /// Export table data to CSV or JSON
    Export(ExportCommand),
    /// Bulk load data from CSV
    BulkLoad(BulkLoadCommand),
    /// Force a WAL checkpoint
    Checkpoint(DbCommand),
    /// Export the database to a new on-disk file (snapshot backup)
    SaveAs(SaveAsCommand),
    /// Manage named time-travel snapshots
    #[command(subcommand)]
    Snapshot(SnapshotCommand),
    /// Manage database branches
    #[command(subcommand)]
    Branch(BranchCommand),
    /// Quick diagnostic view of database file headers, format version, and WAL state
    Info(InfoCommand),
    /// Describe table structure
    Describe(DescribeCommand),
    /// List all tables in the database
    ListTables(ListTablesCommand),
    /// List all indexes
    ListIndexes(ListIndexesCommand),
    /// List all views in the database
    ListViews(ListViewsCommand),
    /// Dump database as SQL
    Dump(DumpCommand),
    /// Dump raw database header fields
    DumpHeader(DumpHeaderCommand),
    /// Rebuild an index
    RebuildIndex(RebuildIndexCommand),
    /// Rebuild all indexes
    RebuildIndexes(RebuildIndexesCommand),
    /// Emit shell completion script
    Completion(CompletionCommand),
    /// Deep introspection of logical content, table metrics, and storage fragmentation
    Stats(StatsCommand),
    /// Rewrite database into a new file to reclaim space
    Vacuum(VacuumCommand),
    /// Verify database header checksum
    VerifyHeader(VerifyHeaderCommand),
    /// Verify index integrity
    VerifyIndex(VerifyIndexCommand),
    /// Run `decentdb doctor` to diagnose database health
    Doctor(DoctorCommand),
    /// Migrate a legacy database format to the current version
    Migrate(MigrateCommand),
    /// Local-first sync journal management
    #[command(subcommand)]
    Sync(SyncCommand),
}

#[derive(Clone, Debug, Parser)]
pub struct DbCommand {
    #[arg(long)]
    pub db: String,
}

#[derive(Clone, Debug, Parser)]
pub struct ExecCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub sql: Option<String>,
    #[arg(long = "params")]
    pub params: Vec<String>,
    #[arg(long = "openClose")]
    pub open_close: bool,
    #[arg(long)]
    pub timing: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
    #[arg(long = "noRows")]
    pub no_rows: bool,
    #[arg(long = "cachePages", default_value_t = 1024)]
    pub cache_pages: usize,
    #[arg(long = "cacheMb", default_value_t = 0)]
    pub cache_mb: usize,
    #[arg(long)]
    pub checkpoint: bool,
    #[arg(long = "dbInfo")]
    pub db_info: bool,
    #[arg(long = "as-of")]
    pub as_of: Option<String>,
    #[arg(long = "as-of-lsn")]
    pub as_of_lsn: Option<u64>,
    #[arg(long)]
    pub branch: Option<String>,
}

#[derive(Clone, Debug, Parser)]
pub struct ReplCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
    #[arg(long)]
    pub branch: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum DataFormat {
    Csv,
    Json,
}

#[derive(Clone, Debug, Parser)]
pub struct ImportCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long, value_enum, default_value_t = DataFormat::Csv)]
    pub format: DataFormat,
    #[arg(long = "batchSize", default_value_t = 10_000)]
    pub batch_size: usize,
}

#[derive(Clone, Debug, Parser)]
pub struct ExportCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, value_enum, default_value_t = DataFormat::Csv)]
    pub format: DataFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BulkLoadCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long)]
    pub input: PathBuf,
    #[arg(long = "batchSize", default_value_t = 10_000)]
    pub batch_size: usize,
    #[arg(long = "syncInterval", default_value_t = 10)]
    pub sync_interval: usize,
    #[arg(long = "disableIndexes", default_value_t = false)]
    pub disable_indexes: bool,
    #[arg(long = "noCheckpoint", default_value_t = false)]
    pub no_checkpoint: bool,
}

#[derive(Clone, Debug, Parser)]
pub struct SaveAsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: PathBuf,
}

#[derive(Clone, Debug, Subcommand)]
pub enum SnapshotCommand {
    /// Create a named snapshot of the current durable main state
    Create(SnapshotCreateCommand),
    /// List named snapshots
    List(SnapshotListCommand),
    /// Delete a named snapshot
    Delete(SnapshotDeleteCommand),
}

#[derive(Clone, Debug, Subcommand)]
pub enum BranchCommand {
    /// Create a branch
    Create(BranchCreateCommand),
    /// List branches
    List(BranchListCommand),
    /// Add a commit marker to a branch
    Commit(BranchCommitCommand),
    /// Show branch head history
    Log(BranchLogCommand),
    /// Compare two branches, snapshots, or heads
    Diff(BranchDiffCommand),
    /// Restore a branch head to a branch, snapshot, or head
    Restore(BranchRestoreCommand),
    /// Merge clean primary-key row changes into a branch or main
    Merge(BranchMergeCommand),
    /// Delete a branch
    Delete(BranchDeleteCommand),
    /// Rename a branch
    Rename(BranchRenameCommand),
}

#[derive(Clone, Debug, Parser)]
pub struct BranchCreateCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub from: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchListCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchCommitCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub message: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchLogCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchDiffCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub left: String,
    #[arg(long)]
    pub right: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchRestoreCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long = "to")]
    pub target: String,
    #[arg(long = "dry-run", default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub confirm: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchMergeCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub source: String,
    #[arg(long)]
    pub target: String,
    #[arg(long = "dry-run", default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub confirm: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchDeleteCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct BranchRenameCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long = "new-name")]
    pub new_name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SnapshotCreateCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SnapshotListCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SnapshotDeleteCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
/// Quick diagnostic view of database file headers, format version, and WAL state.
/// Designed to be fast and safe even if the database is corrupted or an older version.
pub struct InfoCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long = "schema-summary")]
    pub schema_summary: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct DescribeCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct ListTablesCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct ListIndexesCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct ListViewsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct DumpCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

#[derive(Clone, Debug, Parser)]
pub struct DumpHeaderCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct RebuildIndexCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub index: String,
}

#[derive(Clone, Debug, Parser)]
pub struct RebuildIndexesCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub table: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ShellKind {
    Bash,
    Zsh,
}

#[derive(Clone, Debug, Parser)]
pub struct CompletionCommand {
    #[arg(long, value_enum, default_value_t = ShellKind::Bash)]
    pub shell: ShellKind,
}

#[derive(Clone, Debug, Parser)]
/// Deep introspection of logical content, table metrics, and storage fragmentation.
/// Bootstraps the full database engine to gather exact metrics and catalog stats.
pub struct StatsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct VacuumCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long, default_value_t = false)]
    pub overwrite: bool,
}

#[derive(Clone, Debug, Parser)]
pub struct VerifyHeaderCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct VerifyIndexCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub index: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct MigrateCommand {
    /// The source legacy database file path
    pub source: String,

    /// The destination database file path for the current format
    pub dest: String,
}

#[derive(Clone, Debug, Subcommand)]
pub enum SyncCommand {
    /// Initialize a replica and enable sync
    Init(SyncInitCommand),
    /// Run sync journal integrity checks
    Doctor(SyncDoctorCommand),
    /// Enable sync on a database
    Enable(DbCommand),
    /// Disable sync on a database
    Disable(DbCommand),
    /// Show sync status
    Status(SyncStatusCommand),
    /// List pending sync changes
    Pending(SyncPendingCommand),
    /// Export sync records from the local journal
    Export(SyncExportCommand),
    /// Import sync records into the database
    Import(SyncImportCommand),
    /// Inspect unresolved sync conflicts
    Conflicts(SyncConflictsCommand),
    /// Inspect and resolve sync conflicts
    #[command(subcommand)]
    Conflict(SyncConflictCommand),
    /// Prune local sync journal records through a sequence
    Prune(SyncPruneCommand),
    /// Manage sync peers
    #[command(subcommand)]
    Peer(SyncPeerCommand),
    /// Manage sync scopes
    #[command(subcommand)]
    Scope(SyncScopeCommand),
    /// Run peer-to-peer sync over HTTP
    Run(SyncRunCommand),
    /// Serve sync protocol endpoints for tests and dev
    Serve(SyncServeCommand),
}

#[derive(Clone, Debug, Subcommand)]
pub enum SyncPeerCommand {
    /// Add or update a sync peer
    Add(SyncPeerAddCommand),
    /// Remove a sync peer
    Remove(SyncPeerRemoveCommand),
    /// List configured sync peers
    List(SyncPeerListCommand),
}

#[derive(Clone, Debug, Subcommand)]
pub enum SyncScopeCommand {
    /// Create or update a sync scope
    Create(SyncScopeCreateCommand),
    /// Drop a sync scope
    Drop(SyncScopeDropCommand),
    /// List configured sync scopes
    List(SyncScopeListCommand),
    /// Bind a peer to a sync scope
    Bind(SyncScopeBindCommand),
    /// Unbind a peer from its sync scope
    Unbind(SyncScopeUnbindCommand),
    /// List peer-to-scope bindings
    Bindings(SyncScopeBindingsCommand),
}

#[derive(Clone, Debug, Parser)]
pub struct SyncInitCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub replica_id: String,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncDoctorCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncStatusCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncExportCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub since: u64,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub limit: Option<usize>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncImportCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub input: PathBuf,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncConflictsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, default_value_t = false)]
    pub all: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Subcommand)]
pub enum SyncConflictCommand {
    /// Show a single conflict
    Show(SyncConflictShowCommand),
    /// Resolve a conflict
    Resolve(SyncConflictResolveCommand),
    /// Reopen a resolved conflict
    Reopen(SyncConflictReopenCommand),
    /// Inspect or update the conflict policy
    #[command(subcommand)]
    Policy(SyncConflictPolicyCommand),
}

#[derive(Clone, Debug, Subcommand)]
pub enum SyncConflictPolicyCommand {
    /// Show the current conflict policy
    Get(SyncConflictPolicyGetCommand),
    /// Update the current conflict policy
    Set(SyncConflictPolicySetCommand),
}

#[derive(Clone, Debug, Parser)]
pub struct SyncConflictShowCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub id: i64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncConflictResolveCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub id: i64,
    #[arg(long, value_enum)]
    pub action: SyncConflictResolveAction,
    #[arg(long)]
    pub by: Option<String>,
    #[arg(long)]
    pub note: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SyncConflictResolveAction {
    KeepLocal,
    ApplyRemote,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncConflictReopenCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub id: i64,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncConflictPolicyGetCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncConflictPolicySetCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum)]
    pub policy: SyncConflictPolicyCli,
    #[arg(long = "origin-priority")]
    pub origin_priority: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum SyncConflictPolicyCli {
    Record,
    Stop,
    LastWriterWins,
    OriginPriority,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncPruneCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub through: u64,
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
    #[arg(long, default_value_t = false)]
    pub allow_data_loss: bool,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncPeerAddCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub endpoint: String,
    #[arg(long = "token-env")]
    pub token_env: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncPeerRemoveCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncPeerListCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncScopeCreateCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long)]
    pub include: String,
    #[arg(long = "row-filter")]
    pub row_filter: Option<String>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncScopeDropCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub name: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncScopeListCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncScopeBindCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub peer: String,
    #[arg(long)]
    pub scope: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncScopeUnbindCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub peer: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncScopeBindingsCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncRunCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub peer: String,
    #[arg(long, default_value = "both")]
    pub direction: String,
    #[arg(long, default_value_t = 1000)]
    pub limit: usize,
    #[arg(long, default_value_t = 2)]
    pub retries: usize,
    #[arg(long = "conflict-policy")]
    pub conflict_policy: Option<SyncConflictPolicyCli>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Table)]
    pub format: OutputFormat,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncServeCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long)]
    pub bind: String,
    #[arg(long)]
    pub scope: Option<String>,
    #[arg(long = "token-env")]
    pub token_env: Option<String>,
    #[arg(long = "conflict-policy")]
    pub conflict_policy: Option<SyncConflictPolicyCli>,
    #[arg(long = "ready-file")]
    pub ready_file: Option<PathBuf>,
    #[arg(long = "max-requests")]
    pub max_requests: Option<usize>,
}

#[derive(Clone, Debug, Parser)]
pub struct SyncPendingCommand {
    #[arg(long)]
    pub db: String,
    #[arg(long, default_value_t = 0)]
    pub since: u64,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long, value_enum, default_value_t = OutputFormat::Json)]
    pub format: OutputFormat,
}

/// Parsed CLI values for the `--checks` comma-separated list.
fn parse_checks(raw: &str) -> Result<DoctorCheckSelection, String> {
    if raw.eq_ignore_ascii_case("all") {
        return Ok(DoctorCheckSelection::All);
    }
    let cats: Result<Vec<DoctorCategory>, _> = raw
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| match s.trim().to_lowercase().as_str() {
            "header" => Ok(DoctorCategory::Header),
            "storage" => Ok(DoctorCategory::Storage),
            "wal" => Ok(DoctorCategory::Wal),
            "fragmentation" => Ok(DoctorCategory::Fragmentation),
            "schema" => Ok(DoctorCategory::Schema),
            "statistics" => Ok(DoctorCategory::Statistics),
            "indexes" => Ok(DoctorCategory::Indexes),
            "compatibility" => Ok(DoctorCategory::Compatibility),
            other => Err(format!("invalid check category: {other}")),
        })
        .collect();
    Ok(DoctorCheckSelection::Selected(cats?))
}

fn parse_severity(raw: &str) -> Result<DoctorSeverity, String> {
    match raw.trim().to_lowercase().as_str() {
        "info" => Ok(DoctorSeverity::Info),
        "warning" => Ok(DoctorSeverity::Warning),
        "error" => Ok(DoctorSeverity::Error),
        other => Err(format!("invalid severity: {other}")),
    }
}

fn parse_path_mode(raw: &str) -> Result<DoctorPathMode, String> {
    match raw.trim().to_lowercase().as_str() {
        "absolute" => Ok(DoctorPathMode::Absolute),
        "basename" => Ok(DoctorPathMode::Basename),
        "redacted" => Ok(DoctorPathMode::Redacted),
        other => Err(format!("invalid path mode: {other}")),
    }
}

#[derive(Clone, Debug, Parser)]
pub struct DoctorCommand {
    #[arg(long)]
    pub db: String,

    #[arg(long, value_enum, default_value_t = OutputFormat::Markdown)]
    pub format: OutputFormat,

    #[arg(long, default_value = "all")]
    pub checks: String,

    #[arg(long = "verify-index")]
    pub verify_index: Vec<String>,

    #[arg(long = "verify-indexes")]
    pub verify_all: bool,

    #[arg(long = "max-index-verify", default_value = "32")]
    pub max_index_verify: usize,

    #[arg(long = "fail-on", default_value = "error")]
    pub fail_on: String,

    #[arg(
        long = "include-recommendations",
        default_value_t = true,
        default_missing_value = "true",
        num_args = 0..=1,
        require_equals = true,
        action = ArgAction::Set
    )]
    pub include_recommendations: bool,

    #[arg(long = "path-mode", default_value = "absolute")]
    pub path_mode: String,

    #[arg(long = "fix", default_value_t = false)]
    pub fix: bool,
}

impl DoctorCommand {
    fn into_options(self) -> Result<DoctorOptions, String> {
        let checks = parse_checks(&self.checks)?;
        let verify_indexes = if self.verify_all {
            DoctorIndexVerification::All {
                max_count: self.max_index_verify,
            }
        } else if !self.verify_index.is_empty() {
            DoctorIndexVerification::Named(self.verify_index.clone())
        } else {
            DoctorIndexVerification::None
        };
        let path_mode = parse_path_mode(&self.path_mode)?;
        // fail_on validated during exit-code calculation.

        Ok(DoctorOptions {
            checks,
            verify_indexes,
            include_recommendations: self.include_recommendations,
            path_mode,
            fix: self.fix,
        })
    }
}

pub fn run(cli: Cli) -> i32 {
    if let Commands::Doctor(cmd) = &cli.command {
        return run_doctor_cli(cmd);
    }
    match dispatch(cli) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("{error}");
            1
        }
    }
}

fn dispatch(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Version => {
            println!("DecentDB version: {}", decentdb::version());
        }
        Commands::Exec(command) => {
            if let Err(error) = run_exec(&command) {
                if command.format == OutputFormat::Json {
                    println!("{}", render_error_json(&error.to_string()));
                    return Ok(());
                }
                return Err(error);
            }
        }
        Commands::Repl(command) => {
            run_repl(
                open_db(&command.db, true, 0, 0)?,
                command.format,
                command.branch.as_deref(),
            )?;
        }
        Commands::Import(command) => run_import(command)?,
        Commands::Export(command) => run_export(command)?,
        Commands::BulkLoad(command) => run_bulk_load(command)?,
        Commands::Checkpoint(command) => {
            open_db(&command.db, false, 0, 0)?.checkpoint_wal()?;
            println!("checkpoint complete");
        }
        Commands::SaveAs(command) => {
            open_db(&command.db, false, 0, 0)?.save_as(&command.output)?;
            println!("{}", command.output.display());
        }
        Commands::Snapshot(command) => run_snapshot(command)?,
        Commands::Branch(command) => run_branch(command)?,
        Commands::Info(command) => run_info(command)?,
        Commands::Describe(command) => run_describe(command)?,
        Commands::ListTables(command) => run_list_tables(command)?,
        Commands::ListIndexes(command) => run_list_indexes(command)?,
        Commands::ListViews(command) => run_list_views(command)?,
        Commands::Dump(command) => run_dump(command)?,
        Commands::DumpHeader(command) => run_dump_header(command)?,
        Commands::RebuildIndex(command) => {
            open_db(&command.db, false, 0, 0)?.rebuild_index(&command.index)?;
            println!("{}", command.index);
        }
        Commands::RebuildIndexes(command) => run_rebuild_indexes(command)?,
        Commands::Completion(command) => print!("{}", completion_script(command.shell)),
        Commands::Stats(command) => run_stats(command)?,
        Commands::Vacuum(command) => run_vacuum(command)?,
        Commands::VerifyHeader(command) => run_verify_header(command)?,
        Commands::VerifyIndex(command) => run_verify_index(command)?,
        Commands::Migrate(command) => run_migrate(command)?,
        Commands::Sync(command) => run_sync(command)?,
        Commands::Doctor(_) => unreachable!("Doctor is handled in run()"),
    }
    Ok(())
}

fn run_exec(command: &ExecCommand) -> Result<()> {
    let db = open_db(&command.db, true, command.cache_pages, command.cache_mb)?;

    if command.db_info {
        print_storage_info(command.format, &db.storage_info()?);
        return Ok(());
    }

    if command.open_close {
        return Ok(());
    }

    if command.sql.is_none() && command.checkpoint {
        db.checkpoint_wal()?;
        match command.format {
            OutputFormat::Json => println!("{}", render_exec_success_json(&[], 0.0, true)),
            _ => println!("checkpoint complete"),
        }
        return Ok(());
    }

    let sql = command.sql.as_deref().ok_or_else(|| {
        anyhow!("--sql is required unless --openClose, --dbInfo, or --checkpoint is used")
    })?;
    let params = command
        .params
        .iter()
        .map(|param| parse_param(param))
        .collect::<Result<Vec<_>>>()?;
    if command.as_of.is_some() && command.as_of_lsn.is_some() {
        return Err(anyhow!("use only one of --as-of or --as-of-lsn"));
    }
    if command.branch.is_some() && (command.as_of.is_some() || command.as_of_lsn.is_some()) {
        return Err(anyhow!("use --branch or time-travel execution, not both"));
    }
    if (command.as_of.is_some() || command.as_of_lsn.is_some()) && command.checkpoint {
        return Err(anyhow!(
            "--checkpoint is not supported with time-travel execution"
        ));
    }
    if command
        .branch
        .as_deref()
        .is_some_and(|branch| branch != "main")
        && command.checkpoint
    {
        return Err(anyhow!(
            "--checkpoint is not supported with read-only branch execution"
        ));
    }
    let started = Instant::now();
    let mut results = if let Some(snapshot_name) = command.as_of.as_deref() {
        let snapshot_lsn = db
            .snapshot_lsn_for_ref(snapshot_name)?
            .ok_or_else(|| anyhow!("unknown snapshot or branch head '{snapshot_name}'"))?;
        db.execute_batch_at_snapshot_lsn_with_params(sql, snapshot_lsn, &params)?
    } else if let Some(snapshot_lsn) = command.as_of_lsn {
        db.execute_batch_at_snapshot_lsn_with_params(sql, snapshot_lsn, &params)?
    } else if let Some(branch_name) = command.branch.as_deref() {
        db.execute_batch_on_branch_with_params(sql, branch_name, &params)?
    } else {
        db.execute_batch_with_params(sql, &params)?
    };
    if command.no_rows && results.len() == 1 {
        let row_count = results[0].rows().len();
        results = vec![QueryResult::with_affected_rows(row_count as u64)];
    }
    if command.checkpoint {
        db.checkpoint_wal()?;
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    match command.format {
        OutputFormat::Json => {
            println!(
                "{}",
                render_exec_success_json(&results, elapsed_ms, command.checkpoint)
            );
        }
        OutputFormat::Csv | OutputFormat::Table | OutputFormat::Markdown => {
            for (index, result) in results.iter().enumerate() {
                if index > 0 {
                    println!();
                }
                let rows = rows_from_query_result(result);
                println!(
                    "{}",
                    render_rows(
                        command.format,
                        result.columns(),
                        &rows,
                        !result.columns().is_empty()
                    )
                );
            }
        }
    }
    Ok(())
}

fn run_snapshot(command: SnapshotCommand) -> Result<()> {
    match command {
        SnapshotCommand::Create(command) => {
            let db = open_db(&command.db, true, 0, 0)?;
            let snapshot = db.snapshot_create(&command.name)?;
            print_snapshots(command.format, &[snapshot]);
        }
        SnapshotCommand::List(command) => {
            let db = open_db(&command.db, false, 0, 0)?;
            let snapshots = db.snapshot_list()?;
            print_snapshots(command.format, &snapshots);
        }
        SnapshotCommand::Delete(command) => {
            let db = open_db(&command.db, true, 0, 0)?;
            let deleted = db.snapshot_delete(&command.name)?;
            let columns = vec!["name".to_string(), "deleted".to_string()];
            let rows = vec![vec![command.name, deleted.to_string()]];
            println!("{}", render_rows(command.format, &columns, &rows, true));
        }
    }
    Ok(())
}

fn print_snapshots(format: OutputFormat, snapshots: &[NamedSnapshot]) {
    let columns = vec![
        "name".to_string(),
        "snapshot_lsn".to_string(),
        "created_at_micros".to_string(),
        "branch_id".to_string(),
        "head_id".to_string(),
    ];
    let rows = snapshots
        .iter()
        .map(|snapshot| {
            vec![
                snapshot.name.clone(),
                snapshot.snapshot_lsn.to_string(),
                snapshot.created_at_micros.to_string(),
                snapshot.branch_id.clone(),
                snapshot.head_id.clone(),
            ]
        })
        .collect::<Vec<_>>();
    println!("{}", render_rows(format, &columns, &rows, true));
}

fn run_branch(command: BranchCommand) -> Result<()> {
    match command {
        BranchCommand::Create(command) => {
            let db = open_db(&command.db, true, 0, 0)?;
            let branch = db.branch_create(&command.name, command.from.as_deref())?;
            print_branches(command.format, &[branch]);
        }
        BranchCommand::List(command) => {
            let db = open_db(&command.db, false, 0, 0)?;
            let branches = db.branch_list()?;
            print_branches(command.format, &branches);
        }
        BranchCommand::Commit(command) => {
            let db = open_db(&command.db, true, 0, 0)?;
            let entry = db.branch_commit(&command.name, &command.message)?;
            print_branch_log(command.format, &[entry]);
        }
        BranchCommand::Log(command) => {
            let db = open_db(&command.db, false, 0, 0)?;
            let entries = db.branch_log(&command.name)?;
            print_branch_log(command.format, &entries);
        }
        BranchCommand::Diff(command) => {
            let db = open_db(&command.db, false, 0, 0)?;
            let report = db.branch_diff(&command.left, &command.right)?;
            print_branch_diff(command.format, &report)?;
        }
        BranchCommand::Restore(command) => {
            if !command.dry_run && !command.confirm {
                return Err(anyhow!("branch restore requires --dry-run or --confirm"));
            }
            let db = open_db(&command.db, true, 0, 0)?;
            let report = db.branch_restore(&command.name, &command.target, command.dry_run)?;
            print_branch_restore(command.format, &report)?;
        }
        BranchCommand::Merge(command) => {
            if !command.dry_run && !command.confirm {
                return Err(anyhow!("branch merge requires --dry-run or --confirm"));
            }
            let db = open_db(&command.db, true, 0, 0)?;
            let report = db.branch_merge(&command.source, &command.target, command.dry_run)?;
            print_branch_merge(command.format, &report)?;
        }
        BranchCommand::Delete(command) => {
            let db = open_db(&command.db, true, 0, 0)?;
            let deleted = db.branch_delete(&command.name)?;
            let columns = vec!["name".to_string(), "deleted".to_string()];
            let rows = vec![vec![command.name, deleted.to_string()]];
            println!("{}", render_rows(command.format, &columns, &rows, true));
        }
        BranchCommand::Rename(command) => {
            let db = open_db(&command.db, true, 0, 0)?;
            let renamed = db.branch_rename(&command.name, &command.new_name)?;
            let columns = vec![
                "old_name".to_string(),
                "new_name".to_string(),
                "renamed".to_string(),
            ];
            let rows = vec![vec![command.name, command.new_name, renamed.to_string()]];
            println!("{}", render_rows(command.format, &columns, &rows, true));
        }
    }
    Ok(())
}

fn print_branches(format: OutputFormat, branches: &[BranchInfo]) {
    let columns = vec![
        "name".to_string(),
        "branch_id".to_string(),
        "current_head_id".to_string(),
        "base_head_id".to_string(),
        "created_at_micros".to_string(),
        "updated_at_micros".to_string(),
    ];
    let rows = branches
        .iter()
        .map(|branch| {
            vec![
                branch.name.clone(),
                branch.branch_id.clone(),
                branch.current_head_id.clone().unwrap_or_default(),
                branch.base_head_id.clone().unwrap_or_default(),
                branch.created_at_micros.to_string(),
                branch.updated_at_micros.to_string(),
            ]
        })
        .collect::<Vec<_>>();
    println!("{}", render_rows(format, &columns, &rows, true));
}

fn print_branch_log(format: OutputFormat, entries: &[BranchLogEntry]) {
    let columns = vec![
        "head_id".to_string(),
        "branch_id".to_string(),
        "parent_head_id".to_string(),
        "message".to_string(),
        "created_at_micros".to_string(),
        "sql".to_string(),
    ];
    let rows = entries
        .iter()
        .map(|entry| {
            vec![
                entry.head_id.clone(),
                entry.branch_id.clone(),
                entry.parent_head_id.clone().unwrap_or_default(),
                entry.message.clone().unwrap_or_default(),
                entry.created_at_micros.to_string(),
                entry.sql.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    println!("{}", render_rows(format, &columns, &rows, true));
}

fn print_branch_merge(format: OutputFormat, report: &BranchMergeReport) -> Result<()> {
    if format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    let columns = vec![
        "kind".to_string(),
        "table".to_string(),
        "primary_key".to_string(),
        "operation".to_string(),
        "message".to_string(),
    ];
    let mut rows = Vec::new();
    for change in &report.applied {
        rows.push(vec![
            "applied".to_string(),
            change.table.clone(),
            change.primary_key.join(","),
            branch_merge_operation_name(&change.operation).to_string(),
            String::new(),
        ]);
    }
    for conflict in &report.conflicts {
        rows.push(vec![
            "conflict".to_string(),
            conflict.table.clone(),
            conflict.primary_key.join(","),
            conflict.conflict_type.clone(),
            conflict.message.clone(),
        ]);
    }
    if rows.is_empty() {
        rows.push(vec![
            if report.clean { "clean" } else { "conflict" }.to_string(),
            String::new(),
            String::new(),
            String::new(),
            format!(
                "changes={}, conflicts={}",
                report.applied_change_count, report.conflict_count
            ),
        ]);
    }
    println!("{}", render_rows(format, &columns, &rows, true));
    Ok(())
}

fn branch_merge_operation_name(operation: &BranchMergeOperation) -> &'static str {
    match operation {
        BranchMergeOperation::Insert => "insert",
        BranchMergeOperation::Update => "update",
        BranchMergeOperation::Delete => "delete",
    }
}

fn print_branch_restore(format: OutputFormat, report: &BranchRestoreReport) -> Result<()> {
    if format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    let rows = vec![
        ("branch".to_string(), report.branch.clone()),
        ("target_ref".to_string(), report.target_ref.clone()),
        ("dry_run".to_string(), report.dry_run.to_string()),
        (
            "previous_head_id".to_string(),
            report.previous_head_id.clone().unwrap_or_default(),
        ),
        ("target_head_id".to_string(), report.target_head_id.clone()),
        (
            "new_head_id".to_string(),
            report.new_head_id.clone().unwrap_or_default(),
        ),
        (
            "changed_table_count".to_string(),
            report.changed_table_count.to_string(),
        ),
        (
            "added_row_count".to_string(),
            report.added_row_count.to_string(),
        ),
        (
            "updated_row_count".to_string(),
            report.updated_row_count.to_string(),
        ),
        (
            "deleted_row_count".to_string(),
            report.deleted_row_count.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(format, &rows));
    Ok(())
}

fn print_branch_diff(format: OutputFormat, report: &BranchDiffReport) -> Result<()> {
    if format == OutputFormat::Json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    let columns = vec![
        "table".to_string(),
        "status".to_string(),
        "schema_changed".to_string(),
        "added".to_string(),
        "updated".to_string(),
        "deleted".to_string(),
        "message".to_string(),
    ];
    let rows = report
        .tables
        .iter()
        .map(|table| {
            vec![
                table.table.clone(),
                branch_diff_status_name(&table.status).to_string(),
                table.schema_changed.to_string(),
                table.added.len().to_string(),
                table.updated.len().to_string(),
                table.deleted.len().to_string(),
                table.message.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    println!("{}", render_rows(format, &columns, &rows, true));
    Ok(())
}

fn branch_diff_status_name(status: &BranchTableDiffStatus) -> &'static str {
    match status {
        BranchTableDiffStatus::Unchanged => "unchanged",
        BranchTableDiffStatus::Added => "added",
        BranchTableDiffStatus::Removed => "removed",
        BranchTableDiffStatus::Changed => "changed",
        BranchTableDiffStatus::Unsupported => "unsupported",
    }
}

fn run_import(command: ImportCommand) -> Result<()> {
    if command.format != DataFormat::Csv {
        return Err(anyhow!("JSON import is not supported by the Rust CLI yet"));
    }
    let db = open_db(&command.db, true, 0, 0)?;
    let (columns, rows) = parse_csv_file(&command.input)?;
    let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    db.bulk_load_rows(
        &command.table,
        &column_refs,
        &rows,
        BulkLoadOptions {
            batch_size: command.batch_size,
            ..BulkLoadOptions::default()
        },
    )?;
    println!("{}", rows.len());
    Ok(())
}

fn run_export(command: ExportCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let result = db.execute(&format!("SELECT * FROM {}", sql_identifier(&command.table)))?;
    let rows = rows_from_query_result(&result);
    let output = match command.format {
        DataFormat::Csv => render_rows(OutputFormat::Csv, result.columns(), &rows, true),
        DataFormat::Json => render_rows(OutputFormat::Json, result.columns(), &rows, false),
    };
    fs::write(&command.output, format!("{output}\n"))?;
    Ok(())
}

fn run_bulk_load(command: BulkLoadCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let (columns, rows) = parse_csv_file(&command.input)?;
    let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    let inserted = db.bulk_load_rows(
        &command.table,
        &column_refs,
        &rows,
        BulkLoadOptions {
            batch_size: command.batch_size,
            sync_interval: command.sync_interval,
            disable_indexes: command.disable_indexes,
            checkpoint_on_complete: !command.no_checkpoint,
        },
    )?;
    println!("{inserted}");
    Ok(())
}

fn run_info(command: InfoCommand) -> Result<()> {
    // Try to read header first to gracefully support unopenable old formats
    let loose_header = Db::read_header_info(&command.db).ok();

    eprintln!("Analyzing database at {}...", command.db);

    // Attempt normal full open to get live WAL/cache stats
    let db_result = open_db(&command.db, false, 0, 0);

    if let Ok(db) = db_result {
        let storage = db.storage_info()?;
        if command.schema_summary {
            let tables = db.list_tables()?;
            let indexes = db.list_indexes()?;
            let views = db.list_views()?;
            let rows = vec![
                ("path".to_string(), storage.path.display().to_string()),
                ("page_size".to_string(), storage.page_size.to_string()),
                ("page_count".to_string(), storage.page_count.to_string()),
                (
                    "schema_cookie".to_string(),
                    storage.schema_cookie.to_string(),
                ),
                ("table_count".to_string(), tables.len().to_string()),
                ("index_count".to_string(), indexes.len().to_string()),
                ("view_count".to_string(), views.len().to_string()),
            ];
            println!("{}", render_key_value_rows(command.format, &rows));
        } else {
            print_storage_info(command.format, &storage);
        }
    } else if let Some(header) = loose_header {
        // Fallback for unsupported formats
        let rows = vec![
            ("path".to_string(), command.db.clone()),
            (
                "format_version".to_string(),
                header.format_version.to_string(),
            ),
            ("page_size".to_string(), header.page_size.to_string()),
            (
                "schema_cookie".to_string(),
                header.schema_cookie.to_string(),
            ),
            (
                "last_checkpoint_lsn".to_string(),
                header.last_checkpoint_lsn.to_string(),
            ),
            (
                "status".to_string(),
                "engine failed to open (likely unsupported version)".to_string(),
            ),
        ];
        println!("{}", render_key_value_rows(command.format, &rows));
    } else {
        // Bubble up original error
        db_result.map(|_| ())?;
    }

    Ok(())
}

fn run_describe(command: DescribeCommand) -> Result<()> {
    let table = open_db(&command.db, false, 0, 0)?.describe_table(&command.table)?;
    let rows = table
        .columns
        .iter()
        .map(|column| {
            vec![
                column.name.clone(),
                column.column_type.clone(),
                column.nullable.to_string(),
                column.primary_key.to_string(),
                column.unique.to_string(),
                column.auto_increment.to_string(),
                column.default_sql.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "name".to_string(),
        "type".to_string(),
        "nullable".to_string(),
        "primary_key".to_string(),
        "unique".to_string(),
        "auto_increment".to_string(),
        "default".to_string(),
    ];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_list_tables(command: ListTablesCommand) -> Result<()> {
    let tables = open_db(&command.db, false, 0, 0)?.list_tables()?;
    let rows = tables
        .iter()
        .map(|table| vec![table.name.clone(), table.row_count.to_string()])
        .collect::<Vec<_>>();
    let columns = vec!["name".to_string(), "row_count".to_string()];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_list_indexes(command: ListIndexesCommand) -> Result<()> {
    let mut indexes = open_db(&command.db, false, 0, 0)?.list_indexes()?;
    if let Some(table) = &command.table {
        indexes.retain(|index| &index.table_name == table);
    }
    let rows = indexes
        .iter()
        .map(|index| {
            vec![
                index.name.clone(),
                index.table_name.clone(),
                index.kind.clone(),
                index.unique.to_string(),
                index.fresh.to_string(),
                index.columns.join(", "),
                index.predicate_sql.clone().unwrap_or_default(),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "name".to_string(),
        "table".to_string(),
        "kind".to_string(),
        "unique".to_string(),
        "fresh".to_string(),
        "columns".to_string(),
        "predicate".to_string(),
    ];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_list_views(command: ListViewsCommand) -> Result<()> {
    let views = open_db(&command.db, false, 0, 0)?.list_views()?;
    let rows = views
        .iter()
        .map(|view| {
            vec![
                view.name.clone(),
                view.column_names.join(", "),
                view.dependencies.join(", "),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "name".to_string(),
        "columns".to_string(),
        "dependencies".to_string(),
    ];
    println!("{}", render_rows(command.format, &columns, &rows, true));
    Ok(())
}

fn run_dump(command: DumpCommand) -> Result<()> {
    let dump = open_db(&command.db, false, 0, 0)?.dump_sql()?;
    if let Some(path) = command.output {
        fs::write(path, dump)?;
    } else {
        println!("{dump}");
    }
    Ok(())
}

fn run_dump_header(command: DumpHeaderCommand) -> Result<()> {
    let header = Db::read_header_info(&command.db)?;
    print_header_info(command.format, &header);
    Ok(())
}

fn run_rebuild_indexes(command: RebuildIndexesCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    if let Some(table) = command.table {
        for index in db
            .list_indexes()?
            .into_iter()
            .filter(|index| index.table_name == table)
        {
            db.rebuild_index(&index.name)?;
        }
    } else {
        db.rebuild_indexes()?;
    }
    println!("ok");
    Ok(())
}

fn run_stats(command: StatsCommand) -> Result<()> {
    eprintln!("Analyzing database at {}...", command.db);
    let db = open_db(&command.db, false, 0, 0)?;

    let storage = db.storage_info()?;
    let header = db.header_info()?;
    let tables = db.list_tables()?;
    let indexes = db.list_indexes()?;
    let views = db.list_views()?;
    let triggers = db.list_triggers()?;

    let total_rows: usize = tables.iter().map(|t| t.row_count).sum();
    let total_indexes = indexes.len();
    let total_tables = tables.len();
    let total_views = views.len();
    let total_triggers = triggers.len();

    let mut rows = vec![
        ("page_size".to_string(), storage.page_size.to_string()),
        ("page_count".to_string(), storage.page_count.to_string()),
        (
            "physical_bytes".to_string(),
            (u64::from(storage.page_size) * u64::from(storage.page_count)).to_string(),
        ),
        (
            "freelist_pages".to_string(),
            header.freelist_page_count.to_string(),
        ),
        (
            "freelist_bytes".to_string(),
            (u64::from(header.freelist_page_count) * u64::from(header.page_size)).to_string(),
        ),
        (
            "cache_size_mb".to_string(),
            storage.cache_size_mb.to_string(),
        ),
        ("table_count".to_string(), total_tables.to_string()),
        ("index_count".to_string(), total_indexes.to_string()),
        ("view_count".to_string(), total_views.to_string()),
        ("trigger_count".to_string(), total_triggers.to_string()),
        ("total_rows".to_string(), total_rows.to_string()),
    ];

    if storage.page_count > 0 {
        let frag_ratio = (header.freelist_page_count as f64 / storage.page_count as f64) * 100.0;
        rows.push((
            "fragmentation_percent".to_string(),
            format!("{:.2}%", frag_ratio),
        ));
    }

    if let Ok(metadata) = std::fs::metadata(&command.db) {
        let file_size = metadata.len();
        rows.push(("file_size_bytes".to_string(), file_size.to_string()));
    }

    if storage.wal_file_size > 0 {
        rows.push((
            "wal_size_bytes".to_string(),
            storage.wal_file_size.to_string(),
        ));
    }

    println!("{}", render_key_value_rows(command.format, &rows));
    Ok(())
}

fn run_vacuum(command: VacuumCommand) -> Result<()> {
    if command.overwrite && command.output.exists() {
        fs::remove_file(&command.output)?;
    }
    let db = open_db(&command.db, false, 0, 0)?;
    db.checkpoint_wal()?;
    db.save_as(&command.output)?;
    evict_shared_wal(&command.output)?;
    println!("{}", command.output.display());
    Ok(())
}

fn run_verify_header(command: VerifyHeaderCommand) -> Result<()> {
    let header = Db::read_header_info(&command.db)?;
    print_header_info(command.format, &header);
    Ok(())
}

fn run_verify_index(command: VerifyIndexCommand) -> Result<()> {
    let verification = open_db(&command.db, false, 0, 0)?.verify_index(&command.index)?;
    print_index_verification(command.format, &verification);
    Ok(())
}

fn run_migrate(command: MigrateCommand) -> Result<()> {
    let source_path = Path::new(&command.source);

    if !source_path.exists() {
        return Err(anyhow!(
            "Source database file not found: {}",
            command.source
        ));
    }

    let header = Db::read_header_info(&command.source)?;

    if header.format_version == decentdb::DB_FORMAT_VERSION {
        return Err(anyhow!(
            "Source database is already in the current format version ({})",
            decentdb::DB_FORMAT_VERSION
        ));
    }

    // Direct the user to the standalone migration tool.
    Err(anyhow!(
        "Database is in legacy format version {}. To upgrade it to the current format version {}, please use the standalone migration tool:\n\n    decentdb-migrate --source {} --dest <new_path.ddb>\n",
        header.format_version,
        decentdb::DB_FORMAT_VERSION,
        command.source
    ))
}

fn run_sync(command: SyncCommand) -> Result<()> {
    match command {
        SyncCommand::Init(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            db.sync_init_replica(&cmd.replica_id)?;
            println!("sync initialized (replica: {})", cmd.replica_id);
            Ok(())
        }
        SyncCommand::Doctor(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            let report = db.sync_operational_doctor_report()?;
            match cmd.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                }
                OutputFormat::Table => {
                    print_sync_operational_doctor_report_table(&report);
                }
                _ => {
                    return Err(anyhow!("sync doctor supports only json or table output"));
                }
            }
            Ok(())
        }
        SyncCommand::Enable(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            db.sync_set_enabled(true)?;
            println!("sync enabled");
            Ok(())
        }
        SyncCommand::Disable(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            db.sync_set_enabled(false)?;
            println!("sync disabled");
            Ok(())
        }
        SyncCommand::Status(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            let status = db.sync_status()?;
            match cmd.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&status)?);
                }
                _ => {
                    println!("enabled:        {}", status.enabled);
                    println!(
                        "replica_id:     {}",
                        status.replica_id.as_deref().unwrap_or("-")
                    );
                    println!("next_sequence:  {}", status.next_sequence);
                    println!(
                        "journal_path:   {}",
                        status.journal_path.as_deref().unwrap_or("-")
                    );
                    println!("journal_size:   {} bytes", status.journal_size_bytes);
                }
            }
            Ok(())
        }
        SyncCommand::Pending(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            let records = db.sync_pending_changes(cmd.since, cmd.limit)?;
            match cmd.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&records)?);
                }
                _ => {
                    if records.is_empty() {
                        println!("no pending changes");
                    } else {
                        println!(
                            "{:>10} | {:>20} | {:>20} | {:>8} | {:<}",
                            "sequence", "transaction_lsn", "table", "op", "primary_key"
                        );
                        println!(
                            "{:-<10}-|-{:-<20}-|-{:-<20}-|-{:-<8}-|-{:-<}",
                            "", "", "", "", ""
                        );
                        for r in &records {
                            println!(
                                "{:>10} | {:>20} | {:>20} | {:>8} | {}",
                                r.sequence,
                                r.transaction_lsn,
                                r.table,
                                r.operation,
                                serde_json::to_string(&r.primary_key)?
                            );
                        }
                    }
                    println!("{} record(s)", records.len());
                }
            }
            Ok(())
        }
        SyncCommand::Export(cmd) => run_sync_export(cmd),
        SyncCommand::Import(cmd) => {
            let db = open_db(&cmd.db, true, 0, 0)?;
            let batch = parse_sync_batch_file(&cmd.input)?;
            let summary = db.sync_import_batch(&batch)?;
            println!(
                "seen={}, applied={}, skipped={}, conflicted={}",
                summary.seen, summary.applied, summary.skipped, summary.conflicted
            );
            Ok(())
        }
        SyncCommand::Conflicts(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            let conflicts = if cmd.all {
                db.sync_conflicts_all()?
            } else {
                db.sync_conflicts()?
            };
            match cmd.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&conflicts)?);
                }
                OutputFormat::Table => {
                    print_sync_conflicts_table(&conflicts);
                }
                _ => {
                    return Err(anyhow!("sync conflicts supports only json or table output"));
                }
            }
            Ok(())
        }
        SyncCommand::Conflict(cmd) => match cmd {
            SyncConflictCommand::Show(cmd) => run_sync_conflict_show(cmd),
            SyncConflictCommand::Resolve(cmd) => run_sync_conflict_resolve(cmd),
            SyncConflictCommand::Reopen(cmd) => run_sync_conflict_reopen(cmd),
            SyncConflictCommand::Policy(cmd) => match cmd {
                SyncConflictPolicyCommand::Get(cmd) => run_sync_conflict_policy_get(cmd),
                SyncConflictPolicyCommand::Set(cmd) => run_sync_conflict_policy_set(cmd),
            },
        },
        SyncCommand::Prune(cmd) => {
            let db = open_db(&cmd.db, false, 0, 0)?;
            let summary = db.sync_prune_journal(cmd.through, cmd.dry_run, cmd.allow_data_loss)?;
            match cmd.format {
                OutputFormat::Json => {
                    println!("{}", serde_json::to_string_pretty(&summary)?);
                }
                OutputFormat::Table => {
                    print_sync_prune_summary_table(&summary);
                }
                _ => {
                    return Err(anyhow!("sync prune supports only json or table output"));
                }
            }
            Ok(())
        }
        SyncCommand::Peer(cmd) => match cmd {
            SyncPeerCommand::Add(cmd) => run_sync_peer_add(cmd),
            SyncPeerCommand::Remove(cmd) => run_sync_peer_remove(cmd),
            SyncPeerCommand::List(cmd) => run_sync_peer_list(cmd),
        },
        SyncCommand::Scope(cmd) => match cmd {
            SyncScopeCommand::Create(cmd) => run_sync_scope_create(cmd),
            SyncScopeCommand::Drop(cmd) => run_sync_scope_drop(cmd),
            SyncScopeCommand::List(cmd) => run_sync_scope_list(cmd),
            SyncScopeCommand::Bind(cmd) => run_sync_scope_bind(cmd),
            SyncScopeCommand::Unbind(cmd) => run_sync_scope_unbind(cmd),
            SyncScopeCommand::Bindings(cmd) => run_sync_scope_bindings(cmd),
        },
        SyncCommand::Run(cmd) => run_sync_run(cmd),
        SyncCommand::Serve(cmd) => run_sync_serve(cmd),
    }
}

fn run_sync_export(command: SyncExportCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let batch = db.sync_export_batch(command.since, command.limit.unwrap_or(usize::MAX))?;

    match command.format {
        OutputFormat::Json => {
            fs::write(&command.output, serde_json::to_string_pretty(&batch)?)?;
            Ok(())
        }
        OutputFormat::Table => Err(anyhow!("sync export supports only json output")),
        _ => Err(anyhow!("sync export supports only json output")),
    }
}

fn run_sync_peer_add(command: SyncPeerAddCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    db.sync_add_peer(
        &command.name,
        &command.endpoint,
        command.token_env.as_deref(),
    )?;
    let peer = db
        .sync_peer(&command.name)?
        .ok_or_else(|| anyhow!("sync peer was not persisted"))?;
    print_sync_peer_output(command.format, &peer)?;
    Ok(())
}

fn run_sync_peer_remove(command: SyncPeerRemoveCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let removed = db.sync_remove_peer(&command.name)?;
    match command.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "removed": removed }))?
            );
        }
        _ => {
            println!("removed={removed}");
        }
    }
    Ok(())
}

fn run_sync_peer_list(command: SyncPeerListCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let peers = db.sync_peers()?;
    print_sync_peers_output(command.format, &peers)?;
    Ok(())
}

fn run_sync_scope_create(command: SyncScopeCreateCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let include_tables = split_scope_tables(&command.include);
    let include_table_refs: Vec<&str> = include_tables.iter().map(|table| table.as_str()).collect();
    db.sync_create_scope(
        &command.name,
        &include_table_refs,
        command.row_filter.as_deref(),
    )?;
    let scope = db
        .sync_scope(&command.name)?
        .ok_or_else(|| anyhow!("sync scope was not persisted"))?;
    print_sync_scope_output(command.format, &scope)?;
    Ok(())
}

fn run_sync_scope_drop(command: SyncScopeDropCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let removed = db.sync_drop_scope(&command.name)?;
    match command.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "removed": removed }))?
            );
        }
        _ => {
            println!("removed={removed}");
        }
    }
    Ok(())
}

fn run_sync_scope_list(command: SyncScopeListCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let scopes = db.sync_scopes()?;
    print_sync_scopes_output(command.format, &scopes)?;
    Ok(())
}

fn run_sync_scope_bind(command: SyncScopeBindCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    db.sync_bind_peer_scope(&command.peer, &command.scope)?;
    let binding = db
        .sync_peer_scope(&command.peer)?
        .ok_or_else(|| anyhow!("sync peer scope binding was not persisted"))?;
    print_sync_peer_scope_binding_output(command.format, &binding)?;
    Ok(())
}

fn run_sync_scope_unbind(command: SyncScopeUnbindCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let removed = db.sync_unbind_peer_scope(&command.peer)?;
    match command.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "removed": removed }))?
            );
        }
        _ => {
            println!("removed={removed}");
        }
    }
    Ok(())
}

fn run_sync_scope_bindings(command: SyncScopeBindingsCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let bindings = db.sync_peer_scope_bindings()?;
    print_sync_peer_scope_bindings_output(command.format, &bindings)?;
    Ok(())
}

fn run_sync_conflict_show(command: SyncConflictShowCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let conflict = db
        .sync_conflict(command.id)?
        .ok_or_else(|| anyhow!("sync conflict '{}' not found", command.id))?;
    print_sync_conflict_output(command.format, &conflict)?;
    Ok(())
}

fn run_sync_conflict_resolve(command: SyncConflictResolveCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let resolved = match command.action {
        SyncConflictResolveAction::KeepLocal => db.sync_resolve_conflict_keep_local(
            command.id,
            command.by.as_deref(),
            command.note.as_deref(),
        )?,
        SyncConflictResolveAction::ApplyRemote => db.sync_resolve_conflict_apply_remote(
            command.id,
            command.by.as_deref(),
            command.note.as_deref(),
        )?,
    };
    if !resolved {
        return Err(anyhow!("sync conflict '{}' not found", command.id));
    }
    let conflict = db
        .sync_conflict(command.id)?
        .ok_or_else(|| anyhow!("sync conflict '{}' not found", command.id))?;
    print_sync_conflict_output(command.format, &conflict)?;
    Ok(())
}

fn run_sync_conflict_reopen(command: SyncConflictReopenCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let reopened = db.sync_reopen_conflict(command.id)?;
    if !reopened {
        return Err(anyhow!("sync conflict '{}' not found", command.id));
    }
    let conflict = db
        .sync_conflict(command.id)?
        .ok_or_else(|| anyhow!("sync conflict '{}' not found", command.id))?;
    print_sync_conflict_output(command.format, &conflict)?;
    Ok(())
}

fn run_sync_conflict_policy_get(command: SyncConflictPolicyGetCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let policy = db.sync_conflict_policy()?;
    print_sync_conflict_policy_output(command.format, &policy)?;
    Ok(())
}

fn run_sync_conflict_policy_set(command: SyncConflictPolicySetCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let policy = sync_policy_from_cli(command.policy);
    let origin_priority = command
        .origin_priority
        .as_deref()
        .map(split_scope_tables)
        .unwrap_or_default();
    let origin_priority_refs = origin_priority
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>();
    db.sync_set_conflict_policy(policy.clone(), &origin_priority_refs)?;
    let config = db.sync_conflict_policy()?;
    print_sync_conflict_policy_output(command.format, &config)?;
    Ok(())
}

fn run_sync_run(command: SyncRunCommand) -> Result<()> {
    let db = open_db(&command.db, false, 0, 0)?;
    let peer = db
        .sync_peer(&command.peer)?
        .ok_or_else(|| anyhow!("sync peer '{}' not found", command.peer))?;
    let local_scope = db.sync_peer_scope_definition(&peer.name)?;
    let direction = SyncRunDirection::from_str(&command.direction)?;
    let token = resolve_sync_peer_token(&peer)?;
    let conflict_policy = command.conflict_policy.map(sync_policy_from_cli);
    let session_id = db.sync_start_session(&peer.name, direction.clone(), None)?;

    let mut attempt = 0usize;
    loop {
        match perform_sync_run_once(
            &db,
            &peer,
            local_scope.as_ref(),
            token.as_deref(),
            direction.clone(),
            command.limit,
            conflict_policy.clone(),
        ) {
            Ok(mut summary) => {
                summary.retry_count = attempt;
                db.sync_finish_session_success(session_id, &summary)?;
                print_sync_run_summary(command.format, &summary)?;
                return Ok(());
            }
            Err(mut failure) => {
                failure.summary.retry_count = attempt;
                let redacted = redact_sync_secret(&failure.message, token.as_deref());
                if failure.retryable && attempt < command.retries {
                    attempt += 1;
                    thread::sleep(sync_retry_delay(attempt));
                    continue;
                }
                db.sync_finish_session_failed(session_id, &failure.summary, &redacted)?;
                return Err(anyhow!(redacted));
            }
        }
    }
}

fn run_sync_serve(command: SyncServeCommand) -> Result<()> {
    let db = open_db(&command.db, true, 0, 0)?;
    let scope = command.scope.clone();
    if let Some(scope_name) = scope.as_deref() {
        db.sync_scope(scope_name)?
            .ok_or_else(|| anyhow!("sync scope '{}' not found", scope_name))?;
    }
    let listener = TcpListener::bind(&command.bind)?;
    let bound_addr = listener.local_addr()?;
    if let Some(path) = &command.ready_file {
        fs::write(path, bound_addr.to_string())?;
    }

    let auth_token = match &command.token_env {
        Some(env) => Some(
            std::env::var(env)
                .map_err(|_| anyhow!("required sync server token env var '{env}' is not set"))?,
        ),
        None => None,
    };
    let conflict_policy = command.conflict_policy.map(sync_policy_from_cli);

    for (handled, incoming) in listener.incoming().enumerate() {
        let stream = incoming?;
        handle_sync_connection(
            &db,
            stream,
            auth_token.as_deref(),
            scope.as_deref(),
            conflict_policy.clone(),
        )?;
        if command.max_requests.is_some_and(|max| handled + 1 >= max) {
            break;
        }
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
fn perform_sync_run_once(
    db: &Db,
    peer: &SyncPeer,
    local_scope: Option<&SyncScope>,
    token: Option<&str>,
    direction: SyncRunDirection,
    limit: usize,
    conflict_policy: Option<SyncConflictPolicy>,
) -> std::result::Result<SyncRunSummary, SyncRunFailure> {
    let handshake = sync_http_get_handshake(&peer.endpoint, token).map_err(|error| {
        SyncRunFailure::retryable(empty_sync_summary(peer, direction.clone()), error.message)
    })?;

    if handshake.protocol_version != 1 {
        return Err(SyncRunFailure::fatal(
            empty_sync_summary(peer, direction.clone()),
            format!(
                "sync protocol version mismatch: expected 1, got {}",
                handshake.protocol_version
            ),
        ));
    }
    if !handshake
        .capabilities
        .iter()
        .any(|capability| capability == SYNC_REQUIRED_CAPABILITY)
    {
        return Err(SyncRunFailure::fatal(
            empty_sync_summary(peer, direction.clone()),
            format!(
                "peer '{}' does not advertise required capability '{}'",
                peer.name, SYNC_REQUIRED_CAPABILITY
            ),
        ));
    }

    let mut summary = SyncRunSummary {
        peer_name: peer.name.clone(),
        direction: direction.clone(),
        remote_replica_id: handshake.replica_id.clone(),
        pushed: None,
        pulled: None,
        pushed_batch_id: None,
        pulled_batch_id: None,
        retry_count: 0,
    };

    match direction {
        SyncRunDirection::Push => {
            sync_run_push_phase(db, peer, local_scope, token, limit, &mut summary).map_err(
                |error| {
                    SyncRunFailure::with_summary(summary.clone(), error.message, error.retryable)
                },
            )?;
        }
        SyncRunDirection::Pull => {
            let pull_phase = SyncRunPullPhase {
                local_scope,
                token,
                limit,
                handshake: &handshake,
                conflict_policy: conflict_policy.clone(),
            };
            sync_run_pull_phase(db, peer, pull_phase, &mut summary).map_err(|error| {
                SyncRunFailure::with_summary(summary.clone(), error.message, error.retryable)
            })?;
        }
        SyncRunDirection::Both => {
            sync_run_push_phase(db, peer, local_scope, token, limit, &mut summary).map_err(
                |error| {
                    SyncRunFailure::with_summary(summary.clone(), error.message, error.retryable)
                },
            )?;
            let pull_phase = SyncRunPullPhase {
                local_scope,
                token,
                limit,
                handshake: &handshake,
                conflict_policy: conflict_policy.clone(),
            };
            sync_run_pull_phase(db, peer, pull_phase, &mut summary).map_err(|error| {
                SyncRunFailure::with_summary(summary.clone(), error.message, error.retryable)
            })?;
        }
    }

    Ok(summary)
}

fn sync_run_push_phase(
    db: &Db,
    peer: &SyncPeer,
    local_scope: Option<&SyncScope>,
    token: Option<&str>,
    limit: usize,
    summary: &mut SyncRunSummary,
) -> std::result::Result<(), SyncRunPhaseError> {
    let since = db
        .sync_peer_out_watermark(&peer.name)
        .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
        .unwrap_or(0);
    let batch = if let Some(scope) = local_scope {
        db.sync_export_batch_for_scope(&scope.name, since, limit)
            .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
    } else {
        db.sync_export_batch(since, limit)
            .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
    };
    let remote_summary = sync_http_post_import(&peer.endpoint, token, &batch)
        .map_err(|error| SyncRunPhaseError::new(error.message, error.retryable))?;
    summary.pushed_batch_id = Some(batch.batch_id);
    summary.pushed = Some(remote_summary);
    if let Some(watermark) = batch.source_high_watermark.or(batch.last_sequence) {
        db.sync_set_peer_out_watermark(&peer.name, watermark)
            .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?;
    }
    Ok(())
}

struct SyncRunPullPhase<'a> {
    local_scope: Option<&'a SyncScope>,
    token: Option<&'a str>,
    limit: usize,
    handshake: &'a SyncHandshake,
    conflict_policy: Option<SyncConflictPolicy>,
}

fn sync_run_pull_phase(
    db: &Db,
    peer: &SyncPeer,
    phase: SyncRunPullPhase<'_>,
    summary: &mut SyncRunSummary,
) -> std::result::Result<(), SyncRunPhaseError> {
    let remote_replica_id = phase.handshake.replica_id.as_deref().ok_or_else(|| {
        SyncRunPhaseError::fatal("peer hello response is missing replica_id".to_string())
    })?;
    let since = db
        .sync_peer_watermark(remote_replica_id)
        .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
        .unwrap_or(0);
    let batch = sync_http_get_changes(&peer.endpoint, phase.token, since, phase.limit)
        .map_err(|error| SyncRunPhaseError::new(error.message, error.retryable))?;
    let local_summary = if let Some(scope) = phase.local_scope {
        if let Some(policy) = phase.conflict_policy {
            db.sync_import_batch_for_scope_with_policy(&scope.name, &batch, policy)
                .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
        } else {
            db.sync_import_batch_for_scope(&scope.name, &batch)
                .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
        }
    } else {
        if let Some(policy) = phase.conflict_policy {
            db.sync_import_batch_with_policy(&batch, policy)
                .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
        } else {
            db.sync_import_batch(&batch)
                .map_err(|error| SyncRunPhaseError::fatal(error.to_string()))?
        }
    };
    summary.pulled_batch_id = Some(batch.batch_id);
    summary.pulled = Some(local_summary);
    Ok(())
}

fn empty_sync_summary(peer: &SyncPeer, direction: SyncRunDirection) -> SyncRunSummary {
    SyncRunSummary {
        peer_name: peer.name.clone(),
        direction,
        remote_replica_id: None,
        pushed: None,
        pulled: None,
        pushed_batch_id: None,
        pulled_batch_id: None,
        retry_count: 0,
    }
}

fn print_sync_peer_output(format: OutputFormat, peer: &SyncPeer) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(peer)?);
        }
        _ => {
            let rows = vec![
                ("name".to_string(), peer.name.clone()),
                ("endpoint".to_string(), peer.endpoint.clone()),
                (
                    "token_env".to_string(),
                    peer.token_env.clone().unwrap_or_else(|| "-".to_string()),
                ),
                (
                    "created_at_micros".to_string(),
                    peer.created_at_micros.to_string(),
                ),
                (
                    "updated_at_micros".to_string(),
                    peer.updated_at_micros.to_string(),
                ),
            ];
            println!("{}", render_key_value_rows(format, &rows));
        }
    }
    Ok(())
}

fn print_sync_peers_output(format: OutputFormat, peers: &[SyncPeer]) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(peers)?);
        }
        _ => {
            let rows = peers
                .iter()
                .map(|peer| {
                    vec![
                        peer.name.clone(),
                        peer.endpoint.clone(),
                        peer.token_env.clone().unwrap_or_else(|| "-".to_string()),
                        peer.created_at_micros.to_string(),
                        peer.updated_at_micros.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            let columns = vec![
                "name".to_string(),
                "endpoint".to_string(),
                "token_env".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ];
            println!("{}", render_rows(format, &columns, &rows, true));
        }
    }
    Ok(())
}

fn print_sync_scope_output(format: OutputFormat, scope: &SyncScope) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(scope)?);
        }
        _ => {
            let rows = vec![
                ("name".to_string(), scope.name.clone()),
                (
                    "include_tables".to_string(),
                    scope.include_tables.join(", "),
                ),
                (
                    "row_filter".to_string(),
                    scope.row_filter.clone().unwrap_or_else(|| "-".to_string()),
                ),
                (
                    "filter_columns".to_string(),
                    scope.filter_columns.join(", "),
                ),
                (
                    "created_at_micros".to_string(),
                    scope.created_at_micros.to_string(),
                ),
                (
                    "updated_at_micros".to_string(),
                    scope.updated_at_micros.to_string(),
                ),
            ];
            println!("{}", render_key_value_rows(format, &rows));
        }
    }
    Ok(())
}

fn print_sync_scopes_output(format: OutputFormat, scopes: &[SyncScope]) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(scopes)?);
        }
        _ => {
            let rows = scopes
                .iter()
                .map(|scope| {
                    vec![
                        scope.name.clone(),
                        scope.include_tables.join(", "),
                        scope.row_filter.clone().unwrap_or_else(|| "-".to_string()),
                        scope.filter_columns.join(", "),
                        scope.created_at_micros.to_string(),
                        scope.updated_at_micros.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            let columns = vec![
                "name".to_string(),
                "include_tables".to_string(),
                "row_filter".to_string(),
                "filter_columns".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ];
            println!("{}", render_rows(format, &columns, &rows, true));
        }
    }
    Ok(())
}

fn print_sync_peer_scope_binding_output(
    format: OutputFormat,
    binding: &SyncPeerScopeBinding,
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(binding)?);
        }
        _ => {
            let rows = vec![
                ("peer_name".to_string(), binding.peer_name.clone()),
                ("scope_name".to_string(), binding.scope_name.clone()),
                (
                    "created_at_micros".to_string(),
                    binding.created_at_micros.to_string(),
                ),
                (
                    "updated_at_micros".to_string(),
                    binding.updated_at_micros.to_string(),
                ),
            ];
            println!("{}", render_key_value_rows(format, &rows));
        }
    }
    Ok(())
}

fn print_sync_peer_scope_bindings_output(
    format: OutputFormat,
    bindings: &[SyncPeerScopeBinding],
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(bindings)?);
        }
        _ => {
            let rows = bindings
                .iter()
                .map(|binding| {
                    vec![
                        binding.peer_name.clone(),
                        binding.scope_name.clone(),
                        binding.created_at_micros.to_string(),
                        binding.updated_at_micros.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            let columns = vec![
                "peer_name".to_string(),
                "scope_name".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ];
            println!("{}", render_rows(format, &columns, &rows, true));
        }
    }
    Ok(())
}

fn print_sync_run_summary(format: OutputFormat, summary: &SyncRunSummary) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(summary)?);
        }
        _ => {
            let rows = vec![
                ("peer_name".to_string(), summary.peer_name.clone()),
                ("direction".to_string(), summary.direction.to_string()),
                (
                    "remote_replica_id".to_string(),
                    summary
                        .remote_replica_id
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                ),
                ("retry_count".to_string(), summary.retry_count.to_string()),
                (
                    "pushed_batch_id".to_string(),
                    summary
                        .pushed_batch_id
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                ),
                (
                    "pulled_batch_id".to_string(),
                    summary
                        .pulled_batch_id
                        .clone()
                        .unwrap_or_else(|| "-".to_string()),
                ),
                (
                    "pushed".to_string(),
                    sync_import_summary_text(summary.pushed.as_ref()),
                ),
                (
                    "pulled".to_string(),
                    sync_import_summary_text(summary.pulled.as_ref()),
                ),
            ];
            println!("{}", render_key_value_rows(format, &rows));
        }
    }
    Ok(())
}

fn sync_import_summary_text(summary: Option<&SyncImportSummary>) -> String {
    summary.map_or_else(
        || "-".to_string(),
        |summary| {
            format!(
                "seen={}, applied={}, skipped={}, conflicted={}",
                summary.seen, summary.applied, summary.skipped, summary.conflicted
            )
        },
    )
}

#[derive(Debug)]
struct SyncRunPhaseError {
    message: String,
    retryable: bool,
}

impl SyncRunPhaseError {
    fn new(message: String, retryable: bool) -> Self {
        Self { message, retryable }
    }

    fn fatal(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}

#[derive(Debug)]
struct SyncRunFailure {
    summary: SyncRunSummary,
    message: String,
    retryable: bool,
}

impl SyncRunFailure {
    fn retryable(summary: SyncRunSummary, message: String) -> Self {
        Self {
            summary,
            message,
            retryable: true,
        }
    }

    fn fatal(summary: SyncRunSummary, message: String) -> Self {
        Self {
            summary,
            message,
            retryable: false,
        }
    }

    fn with_summary(summary: SyncRunSummary, message: String, retryable: bool) -> Self {
        Self {
            summary,
            message,
            retryable,
        }
    }
}

fn sync_retry_delay(attempt: usize) -> std::time::Duration {
    let shift = attempt.saturating_sub(1).min(8) as u32;
    std::time::Duration::from_millis(50u64.saturating_mul(1u64 << shift))
}

fn resolve_sync_peer_token(peer: &SyncPeer) -> Result<Option<String>> {
    match peer.token_env.as_deref() {
        Some(env_name) => {
            let token = std::env::var(env_name).map_err(|_| {
                anyhow!(
                    "sync peer '{}' requires env var '{}' to be set",
                    peer.name,
                    env_name
                )
            })?;
            Ok(Some(token))
        }
        None => Ok(None),
    }
}

fn redact_sync_secret(message: &str, secret: Option<&str>) -> String {
    match secret {
        Some(secret) if !secret.is_empty() => message.replace(secret, "[redacted]"),
        _ => message.to_string(),
    }
}

fn sync_capabilities() -> Vec<String> {
    vec![
        "batch-envelope-v1".to_string(),
        "manual-import-v1".to_string(),
        "peer-watermarks-v1".to_string(),
        "conflicts-v1".to_string(),
    ]
}

const SYNC_REQUIRED_CAPABILITY: &str = "batch-envelope-v1";

fn sync_hello_url(endpoint: &str) -> String {
    format!("{}/decentdb/sync/v1/hello", endpoint.trim_end_matches('/'))
}

fn sync_changes_url(endpoint: &str, since: u64, limit: usize) -> String {
    format!(
        "{}/decentdb/sync/v1/changes?since={since}&limit={limit}",
        endpoint.trim_end_matches('/')
    )
}

fn sync_import_url(endpoint: &str) -> String {
    format!("{}/decentdb/sync/v1/import", endpoint.trim_end_matches('/'))
}

fn sync_http_get_handshake(
    endpoint: &str,
    token: Option<&str>,
) -> Result<SyncHandshake, SyncHttpError> {
    let body = sync_http_get_text(&sync_hello_url(endpoint), token)?;
    serde_json::from_str(&body)
        .map_err(|error| SyncHttpError::fatal(format!("invalid hello response: {error}")))
}

fn sync_http_get_changes(
    endpoint: &str,
    token: Option<&str>,
    since: u64,
    limit: usize,
) -> Result<SyncChangeBatch, SyncHttpError> {
    let body = sync_http_get_text(&sync_changes_url(endpoint, since, limit), token)?;
    serde_json::from_str(&body)
        .map_err(|error| SyncHttpError::fatal(format!("invalid changes response: {error}")))
}

fn sync_http_post_import(
    endpoint: &str,
    token: Option<&str>,
    batch: &SyncChangeBatch,
) -> Result<SyncImportSummary, SyncHttpError> {
    let body = sync_http_post_json(&sync_import_url(endpoint), token, batch)?;
    serde_json::from_str(&body)
        .map_err(|error| SyncHttpError::fatal(format!("invalid import response: {error}")))
}

#[allow(clippy::result_large_err)]
fn sync_http_get_text(url: &str, token: Option<&str>) -> Result<String, SyncHttpError> {
    sync_http_send(|| {
        let mut request = ureq::get(url);
        if let Some(token) = token {
            request = request.set("Authorization", &format!("Bearer {token}"));
        }
        request.call()
    })
}

#[allow(clippy::result_large_err)]
fn sync_http_post_json<T: serde::Serialize>(
    url: &str,
    token: Option<&str>,
    payload: &T,
) -> Result<String, SyncHttpError> {
    sync_http_send(|| {
        let mut request = ureq::post(url);
        if let Some(token) = token {
            request = request.set("Authorization", &format!("Bearer {token}"));
        }
        request.send_json(payload)
    })
}

#[allow(clippy::result_large_err)]
fn sync_http_send<F>(send: F) -> Result<String, SyncHttpError>
where
    F: FnOnce() -> std::result::Result<ureq::Response, ureq::Error>,
{
    match send() {
        Ok(response) => response.into_string().map_err(|error| {
            SyncHttpError::retryable(format!("failed reading response body: {error}"))
        }),
        Err(ureq::Error::Status(code, response)) => {
            let body = response.into_string().unwrap_or_default();
            if (500..=599).contains(&code) {
                Err(SyncHttpError::retryable(format!(
                    "server returned {code}: {body}"
                )))
            } else if code == 401 || code == 403 {
                Err(SyncHttpError::fatal(format!(
                    "authentication failed with HTTP {code}"
                )))
            } else {
                Err(SyncHttpError::fatal(format!(
                    "server returned HTTP {code}: {body}"
                )))
            }
        }
        Err(ureq::Error::Transport(error)) => Err(SyncHttpError::retryable(format!(
            "transport error: {error}"
        ))),
    }
}

#[derive(Debug)]
struct SyncHttpError {
    message: String,
    retryable: bool,
}

impl SyncHttpError {
    fn retryable(message: String) -> Self {
        Self {
            message,
            retryable: true,
        }
    }

    fn fatal(message: String) -> Self {
        Self {
            message,
            retryable: false,
        }
    }
}

fn handle_sync_connection(
    db: &Db,
    stream: TcpStream,
    expected_token: Option<&str>,
    scope: Option<&str>,
    conflict_policy: Option<SyncConflictPolicy>,
) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    if request_line.trim().is_empty() {
        return Ok(());
    }

    let mut content_length = 0usize;
    let mut authorization: Option<String> = None;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            if name == "content-length" {
                content_length = value
                    .parse::<usize>()
                    .map_err(|_| anyhow!("invalid Content-Length header"))?;
            } else if name == "authorization" {
                authorization = Some(value);
            }
        }
    }

    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body)?;
    let mut stream = reader.into_inner();

    let request_line = request_line.trim_end_matches(['\r', '\n']);
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| anyhow!("malformed request line"))?;
    let target = parts
        .next()
        .ok_or_else(|| anyhow!("malformed request line"))?;
    let _version = parts
        .next()
        .ok_or_else(|| anyhow!("malformed request line"))?;

    if let Some(token) = expected_token {
        let expected = format!("Bearer {token}");
        if authorization.as_deref() != Some(expected.as_str()) {
            return write_sync_json_response(
                &mut stream,
                401,
                serde_json::json!({ "error": "unauthorized" }),
            );
        }
    }

    let (path, query) = split_request_target(target);
    let response: Result<serde_json::Value> = match (method, path) {
        ("GET", "/decentdb/sync/v1/hello") => {
            let status = db.sync_status()?;
            Ok(serde_json::json!(SyncHandshake {
                protocol_version: 1,
                engine_version: decentdb::version().to_string(),
                replica_id: status.replica_id,
                capabilities: sync_capabilities(),
            }))
        }
        ("GET", "/decentdb/sync/v1/status") => Ok(serde_json::to_value(db.sync_status()?)?),
        ("GET", "/decentdb/sync/v1/changes") => {
            let since = parse_sync_query_param_u64(query, "since")?;
            let limit = parse_sync_query_param_usize(query, "limit")?;
            let batch = if let Some(scope_name) = scope {
                db.sync_export_batch_for_scope(scope_name, since, limit)
            } else {
                db.sync_export_batch(since, limit)
            };
            batch
                .map_err(|error| anyhow!(error.to_string()))
                .and_then(|batch| serde_json::to_value(batch).map_err(Into::into))
        }
        ("POST", "/decentdb/sync/v1/import") => {
            let batch: SyncChangeBatch = serde_json::from_slice(&body)
                .map_err(|error| anyhow!("invalid sync batch payload: {error}"))?;
            let summary = if let Some(scope_name) = scope {
                if let Some(policy) = conflict_policy {
                    db.sync_import_batch_for_scope_with_policy(scope_name, &batch, policy)
                } else {
                    db.sync_import_batch_for_scope(scope_name, &batch)
                }
            } else {
                if let Some(policy) = conflict_policy {
                    db.sync_import_batch_with_policy(&batch, policy)
                } else {
                    db.sync_import_batch(&batch)
                }
            };
            summary
                .map_err(|error| anyhow!(error.to_string()))
                .and_then(|summary| serde_json::to_value(summary).map_err(Into::into))
        }
        ("GET", "/decentdb/sync/v1/conflicts") => Ok(serde_json::to_value(db.sync_conflicts()?)?),
        _ => {
            return write_sync_json_response(
                &mut stream,
                404,
                serde_json::json!({ "error": "not found" }),
            );
        }
    };

    match response {
        Ok(body) => write_sync_json_response(&mut stream, 200, body),
        Err(error) => write_sync_json_response(
            &mut stream,
            400,
            serde_json::json!({ "error": error.to_string() }),
        ),
    }
}

fn write_sync_json_response(
    stream: &mut TcpStream,
    status: u16,
    body: serde_json::Value,
) -> Result<()> {
    let body = serde_json::to_vec(&body)?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn split_request_target(target: &str) -> (&str, Option<&str>) {
    match target.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (target, None),
    }
}

fn parse_sync_query_param_u64(query: Option<&str>, key: &str) -> Result<u64> {
    let query = query.ok_or_else(|| anyhow!("missing query string"))?;
    for part in query.split('&') {
        if let Some((name, value)) = part.split_once('=') {
            if name == key {
                return value
                    .parse::<u64>()
                    .map_err(|error| anyhow!("invalid {key} query parameter: {error}"));
            }
        }
    }
    Err(anyhow!("missing {key} query parameter"))
}

fn parse_sync_query_param_usize(query: Option<&str>, key: &str) -> Result<usize> {
    let value = parse_sync_query_param_u64(query, key)?;
    usize::try_from(value).map_err(|_| anyhow!("invalid {key} query parameter: value too large"))
}

fn parse_sync_batch_file(path: &Path) -> Result<SyncChangeBatch> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str(&content).map_err(|error| anyhow!("malformed sync batch: {error}"))
}

fn print_sync_conflicts_table(conflicts: &[SyncConflict]) {
    let rows = conflicts
        .iter()
        .map(|conflict| {
            vec![
                conflict.conflict_id.to_string(),
                conflict.table_name.clone(),
                conflict.operation.clone(),
                if conflict.resolved {
                    "resolved".to_string()
                } else {
                    "open".to_string()
                },
                conflict.conflict_type.clone(),
                conflict
                    .resolution
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                conflict.message.clone(),
            ]
        })
        .collect::<Vec<_>>();
    let columns = vec![
        "conflict_id".to_string(),
        "table_name".to_string(),
        "operation".to_string(),
        "status".to_string(),
        "conflict_type".to_string(),
        "resolution".to_string(),
        "message".to_string(),
    ];
    println!(
        "{}",
        render_rows(OutputFormat::Table, &columns, &rows, true)
    );
}

fn print_sync_conflict_output(format: OutputFormat, conflict: &SyncConflict) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(conflict)?);
        }
        OutputFormat::Table => {
            print_sync_conflicts_table(std::slice::from_ref(conflict));
        }
        _ => {
            return Err(anyhow!(
                "sync conflict output supports only json or table output"
            ));
        }
    }
    Ok(())
}

fn print_sync_conflict_policy_output(
    format: OutputFormat,
    policy: &decentdb::SyncConflictPolicyConfig,
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(policy)?);
        }
        OutputFormat::Table => {
            let rows = vec![
                (
                    "default_policy".to_string(),
                    policy.default_policy.to_string(),
                ),
                (
                    "origin_priority_json".to_string(),
                    serde_json::to_string(&policy.origin_priority)?,
                ),
            ];
            println!("{}", render_key_value_rows(format, &rows));
        }
        _ => {
            return Err(anyhow!(
                "sync conflict policy output supports only json or table output"
            ));
        }
    }
    Ok(())
}

fn sync_policy_from_cli(policy: SyncConflictPolicyCli) -> SyncConflictPolicy {
    match policy {
        SyncConflictPolicyCli::Record => SyncConflictPolicy::Record,
        SyncConflictPolicyCli::Stop => SyncConflictPolicy::Stop,
        SyncConflictPolicyCli::LastWriterWins => SyncConflictPolicy::LastWriterWins,
        SyncConflictPolicyCli::OriginPriority => SyncConflictPolicy::OriginPriority,
    }
}

#[allow(dead_code)]
fn print_sync_integrity_report_table(report: &decentdb::SyncJournalIntegrityReport) {
    println!(
        "{}",
        render_key_value_rows(
            OutputFormat::Table,
            &[
                (
                    "total_records".to_string(),
                    report.total_records.to_string()
                ),
                (
                    "first_sequence".to_string(),
                    report
                        .first_sequence
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                ),
                (
                    "last_sequence".to_string(),
                    report
                        .last_sequence
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                ),
                (
                    "highest_severity".to_string(),
                    format!("{:?}", report.highest_severity),
                ),
                ("issues".to_string(), report.issues.len().to_string(),),
            ]
        )
    );

    if report.issues.is_empty() {
        println!("issues: none");
        return;
    }

    let columns = [
        "line".to_string(),
        "sequence".to_string(),
        "severity".to_string(),
        "code".to_string(),
        "message".to_string(),
    ];
    let rows = report
        .issues
        .iter()
        .map(|issue| {
            vec![
                issue.line_number.to_string(),
                issue
                    .sequence
                    .map_or_else(|| "-".to_string(), |value| value.to_string()),
                format!("{:?}", issue.severity),
                issue.code.clone(),
                issue.message.clone(),
            ]
        })
        .collect::<Vec<_>>();
    println!(
        "{}",
        render_rows(OutputFormat::Table, &columns, &rows, true)
    );
}

fn print_sync_operational_doctor_report_table(report: &decentdb::SyncOperationalDoctorReport) {
    let rows = vec![
        ("enabled".to_string(), report.status.enabled.to_string()),
        (
            "replica_id".to_string(),
            report
                .status
                .replica_id
                .as_deref()
                .unwrap_or("-")
                .to_string(),
        ),
        (
            "next_sequence".to_string(),
            report.status.next_sequence.to_string(),
        ),
        (
            "status_journal_size_bytes".to_string(),
            report.status.journal_size_bytes.to_string(),
        ),
        (
            "integrity_records".to_string(),
            report.integrity.total_records.to_string(),
        ),
        (
            "integrity_first_sequence".to_string(),
            report
                .integrity
                .first_sequence
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
        ),
        (
            "integrity_last_sequence".to_string(),
            report
                .integrity
                .last_sequence
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
        ),
        (
            "integrity_highest_severity".to_string(),
            report.integrity.highest_severity.to_string(),
        ),
        (
            "retention_safe_prune_through".to_string(),
            report
                .retention
                .safe_prune_through
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
        ),
        (
            "retention_prunable_records".to_string(),
            report.retention.prunable_records.to_string(),
        ),
        (
            "retention_blocked_by_json".to_string(),
            serde_json::to_string(&report.retention.blocked_by)
                .unwrap_or_else(|_| "[]".to_string()),
        ),
        (
            "unresolved_conflicts".to_string(),
            report.unresolved_conflicts.to_string(),
        ),
        (
            "recent_sessions".to_string(),
            report.recent_sessions.len().to_string(),
        ),
        (
            "highest_severity".to_string(),
            report.highest_severity.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(OutputFormat::Table, &rows));

    if !report.issues.is_empty() {
        let columns = [
            "line".to_string(),
            "sequence".to_string(),
            "severity".to_string(),
            "code".to_string(),
            "message".to_string(),
        ];
        let rows = report
            .issues
            .iter()
            .map(|issue| {
                vec![
                    issue.line_number.to_string(),
                    issue
                        .sequence
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    format!("{:?}", issue.severity),
                    issue.code.clone(),
                    issue.message.clone(),
                ]
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            render_rows(OutputFormat::Table, &columns, &rows, true)
        );
    }

    if !report.peer_lag.is_empty() {
        let columns = [
            "peer_name".to_string(),
            "remote_replica_id".to_string(),
            "in_watermark".to_string(),
            "out_watermark".to_string(),
            "local_high_watermark".to_string(),
            "in_lag".to_string(),
            "out_lag".to_string(),
        ];
        let rows = report
            .peer_lag
            .iter()
            .map(|lag| {
                vec![
                    lag.peer_name.clone(),
                    lag.remote_replica_id.as_deref().unwrap_or("-").to_string(),
                    lag.in_watermark
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    lag.out_watermark
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    lag.local_high_watermark
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    lag.in_lag
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                    lag.out_lag
                        .map_or_else(|| "-".to_string(), |value| value.to_string()),
                ]
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            render_rows(OutputFormat::Table, &columns, &rows, true)
        );
    }

    if report.guidance.is_empty() {
        println!("guidance: none");
    } else {
        for line in &report.guidance {
            println!("guidance: {}", line);
        }
    }
}

fn print_sync_prune_summary_table(summary: &decentdb::SyncPruneSummary) {
    let rows = vec![
        (
            "requested_through".to_string(),
            summary.requested_through.to_string(),
        ),
        (
            "effective_through".to_string(),
            summary.effective_through.to_string(),
        ),
        ("pruned".to_string(), summary.pruned.to_string()),
        ("dry_run".to_string(), summary.dry_run.to_string()),
        (
            "allow_data_loss".to_string(),
            summary.allow_data_loss.to_string(),
        ),
        (
            "blocked_by_json".to_string(),
            serde_json::to_string(&summary.blocked_by).unwrap_or_else(|_| "[]".to_string()),
        ),
    ];
    println!("{}", render_key_value_rows(OutputFormat::Table, &rows));
}

fn print_storage_info(format: OutputFormat, storage: &StorageInfo) {
    let rows = vec![
        ("path".to_string(), storage.path.display().to_string()),
        (
            "wal_path".to_string(),
            storage.wal_path.display().to_string(),
        ),
        (
            "format_version".to_string(),
            storage.format_version.to_string(),
        ),
        ("page_size".to_string(), storage.page_size.to_string()),
        (
            "cache_size_mb".to_string(),
            storage.cache_size_mb.to_string(),
        ),
        ("page_count".to_string(), storage.page_count.to_string()),
        (
            "schema_cookie".to_string(),
            storage.schema_cookie.to_string(),
        ),
        ("wal_end_lsn".to_string(), storage.wal_end_lsn.to_string()),
        (
            "wal_file_size".to_string(),
            storage.wal_file_size.to_string(),
        ),
        (
            "last_checkpoint_lsn".to_string(),
            storage.last_checkpoint_lsn.to_string(),
        ),
        (
            "active_readers".to_string(),
            storage.active_readers.to_string(),
        ),
        ("wal_versions".to_string(), storage.wal_versions.to_string()),
        (
            "warning_count".to_string(),
            storage.warning_count.to_string(),
        ),
        ("shared_wal".to_string(), storage.shared_wal.to_string()),
    ];
    println!("{}", render_key_value_rows(format, &rows));
}

fn print_header_info(format: OutputFormat, header: &HeaderInfo) {
    let rows = vec![
        ("magic_hex".to_string(), header.magic_hex.clone()),
        (
            "format_version".to_string(),
            header.format_version.to_string(),
        ),
        ("page_size".to_string(), header.page_size.to_string()),
        (
            "header_checksum".to_string(),
            header.header_checksum.to_string(),
        ),
        (
            "schema_cookie".to_string(),
            header.schema_cookie.to_string(),
        ),
        (
            "catalog_root_page_id".to_string(),
            header.catalog_root_page_id.to_string(),
        ),
        (
            "freelist_root_page_id".to_string(),
            header.freelist_root_page_id.to_string(),
        ),
        (
            "freelist_head_page_id".to_string(),
            header.freelist_head_page_id.to_string(),
        ),
        (
            "freelist_page_count".to_string(),
            header.freelist_page_count.to_string(),
        ),
        (
            "last_checkpoint_lsn".to_string(),
            header.last_checkpoint_lsn.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(format, &rows));
}

fn print_index_verification(format: OutputFormat, verification: &IndexVerification) {
    let rows = vec![
        ("name".to_string(), verification.name.clone()),
        ("valid".to_string(), verification.valid.to_string()),
        (
            "expected_entries".to_string(),
            verification.expected_entries.to_string(),
        ),
        (
            "actual_entries".to_string(),
            verification.actual_entries.to_string(),
        ),
    ];
    println!("{}", render_key_value_rows(format, &rows));
}

fn open_db(db: &str, create_if_missing: bool, cache_pages: usize, cache_mb: usize) -> Result<Db> {
    let mut config = DbConfig::default();
    if cache_mb > 0 {
        config.cache_size_mb = cache_mb;
    } else if cache_pages > 0 {
        config.cache_size_mb = ((cache_pages * 4096) / (1024 * 1024)).max(1);
    }
    if create_if_missing {
        Ok(Db::open_or_create(db, config)?)
    } else {
        Ok(Db::open(db, config)?)
    }
}

fn split_scope_tables(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_param(raw: &str) -> Result<Value> {
    if raw.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    let Some((kind, value)) = raw.split_once(':') else {
        return Err(anyhow!("invalid parameter {raw}; expected type:value"));
    };
    match kind {
        "int" | "int64" => Ok(Value::Int64(value.parse()?)),
        "float" | "float64" => Ok(Value::Float64(value.parse()?)),
        "bool" => Ok(Value::Bool(value.parse()?)),
        "text" => Ok(Value::Text(value.to_string())),
        "timestamp" => Ok(Value::TimestampMicros(value.parse()?)),
        "blob" => Ok(Value::Blob(hex_to_bytes(value)?)),
        _ => Err(anyhow!("unsupported parameter kind {kind}")),
    }
}

fn parse_csv_file(path: &Path) -> Result<(Vec<String>, Vec<Vec<Value>>)> {
    let input = fs::read_to_string(path)?;
    let mut lines = input.lines();
    let header = lines.next().ok_or_else(|| anyhow!("CSV input is empty"))?;
    let columns = split_csv_line(header);
    let rows = lines
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            split_csv_line(line)
                .into_iter()
                .map(|value| infer_value(&value))
                .collect()
        })
        .collect::<Vec<Vec<Value>>>();
    Ok((columns, rows))
}

fn split_csv_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && matches!(chars.peek(), Some('"')) {
                    current.push('"');
                    let _ = chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    values.push(current.trim().to_string());
    values
}

fn infer_value(raw: &str) -> Value {
    if raw.is_empty() {
        Value::Null
    } else if let Ok(value) = raw.parse::<i64>() {
        Value::Int64(value)
    } else if let Ok(value) = raw.parse::<f64>() {
        Value::Float64(value)
    } else if raw.eq_ignore_ascii_case("true") || raw.eq_ignore_ascii_case("false") {
        Value::Bool(raw.eq_ignore_ascii_case("true"))
    } else {
        Value::Text(raw.to_string())
    }
}

fn completion_script(shell: ShellKind) -> &'static str {
    match shell {
        ShellKind::Bash => {
            r#"_decentdb_complete() {
  local commands="version exec repl import export bulk-load checkpoint save-as info describe list-tables list-indexes list-views dump dump-header rebuild-index rebuild-indexes completion stats vacuum verify-header verify-index sync"
  COMPREPLY=( $(compgen -W "$commands" -- "${COMP_WORDS[1]}") )
}
complete -F _decentdb_complete decentdb
"#
        }
        ShellKind::Zsh => {
            r#"#compdef decentdb
_decentdb() {
  local -a commands
  commands=(
    version
    exec
    repl
    import
    export
    bulk-load
    checkpoint
    save-as
    info
    describe
    list-tables
    list-indexes
    list-views
    dump
    dump-header
    rebuild-index
    rebuild-indexes
    completion
    stats
    vacuum
    verify-header
    verify-index
    sync
  )
  _describe 'command' commands
}
_decentdb "$@"
"#
        }
    }
}

fn sql_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn hex_to_bytes(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(anyhow!("hex blob must have an even number of characters"));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    let chars = input.as_bytes().chunks_exact(2);
    for pair in chars {
        let byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
        bytes.push(byte);
    }
    Ok(bytes)
}

#[allow(dead_code)]
fn _table_summary(table: &TableInfo) -> Vec<String> {
    vec![
        table.name.clone(),
        table.row_count.to_string(),
        table
            .columns
            .iter()
            .map(|column| format!("{} {}", column.name, column.column_type))
            .collect::<Vec<_>>()
            .join(", "),
        table
            .columns
            .iter()
            .map(|column| stringify_value(&Value::Text(column.name.clone())))
            .collect::<Vec<_>>()
            .join(", "),
    ]
}

// ----------------------------------------------------------------------
// Doctor command
// ----------------------------------------------------------------------

fn run_doctor_cli(cmd: &DoctorCommand) -> i32 {
    match run_doctor_command(cmd) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{e}");
            1
        }
    }
}

fn run_doctor_command(cmd: &DoctorCommand) -> Result<i32, String> {
    let options = cmd.clone().into_options()?;
    let fail_on = parse_severity(&cmd.fail_on)?;

    let report = run_doctor(&cmd.db, options).map_err(|e| format!("Doctor engine error: {e}"))?;

    match cmd.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&report)
                    .map_err(|e| format!("JSON serialization error: {e}"))?
            );
        }
        OutputFormat::Markdown => {
            print!("{}", render_markdown(&report));
        }
        OutputFormat::Csv | OutputFormat::Table => {
            // Fall through to Markdown for non-JSON, non-Markdown
            // formats since table rendering is not implemented for
            // doctor reports in v1.
            print!("{}", render_markdown(&report));
        }
    }

    Ok(exit_code(&report, fail_on))
}

fn exit_code(report: &DoctorReport, fail_on: DoctorSeverity) -> i32 {
    if report.findings.iter().any(|f| {
        f.severity.sort_key() <= fail_on.sort_key()
            || (f.id == "fix.failed" && f.severity == DoctorSeverity::Error)
    }) {
        2
    } else {
        0
    }
}
