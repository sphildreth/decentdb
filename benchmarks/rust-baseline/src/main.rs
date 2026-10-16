// DecentDB rust-baseline benchmark.
//
// Mirrors the schema and query shapes used by the music-library comparison
// workload, with direct DecentDB and SQLite reference runners.
//
// Hot-path pattern (identical to the internal `decentdb-benchmark` scenarios):
//   1. db.transaction()          -> SqlTransaction (exclusive runtime state)
//   2. txn.prepare("INSERT ...") -> PreparedStatement (parsed once)
//   3. txn.prepared_batch(...).execute_mut(&mut [Value::..., ...]) -- per row
//   4. txn.commit()              -- single WAL commit per batch
//
// Scales mirror DecentDB.Compare.Common.Scale exactly.
//
// Output: pretty-printed JSON to
// results/<datetime>-rust-baseline-<profile>-<scale>.json.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::fs;
#[cfg(feature = "extended-suites")]
use std::hint::black_box;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[cfg(any(feature = "extended-suites", feature = "sqlite", feature = "duckdb"))]
use anyhow::bail;
#[cfg(feature = "extended-suites")]
use anyhow::Context;
#[cfg(feature = "extended-suites")]
use clap::{Parser, ValueEnum};
use decentdb::{DbConfig, PreparedStatement, Value};
#[cfg(feature = "sqlite")]
use rusqlite::{params, Connection as SqliteConnection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug)]
#[cfg_attr(feature = "extended-suites", derive(Parser))]
#[cfg_attr(
    feature = "extended-suites",
    command(
        name = "rust-baseline",
        version,
        about = "DecentDB rust-baseline benchmark"
    )
)]
struct Cli {
    /// Scale: smoke | medium | full | huge
    #[cfg_attr(feature = "extended-suites", arg(long, default_value = "smoke"))]
    scale: String,
    /// Output directory for JSON report.
    #[cfg_attr(feature = "extended-suites", arg(long, default_value = "results"))]
    out_dir: PathBuf,
    /// Database path (defaults by engine and scale).
    #[cfg_attr(feature = "extended-suites", arg(long))]
    db_path: Option<PathBuf>,
    /// Seed for the deterministic plan.
    #[cfg_attr(feature = "extended-suites", arg(long, default_value_t = 42u64))]
    seed: u64,
    /// Engine profile: default | resident-hot-read.
    #[cfg_attr(
        feature = "extended-suites",
        arg(long, value_enum, default_value_t = BenchmarkProfile::Default)
    )]
    profile: BenchmarkProfile,
    /// Engine implementation (available values depend on enabled Cargo features).
    #[cfg_attr(
        feature = "extended-suites",
        arg(long, value_enum, default_value_t = BenchmarkEngine::DecentDb)
    )]
    engine: BenchmarkEngine,
    /// Generate an HTML report from historical JSON files in the output directory.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    report: bool,
    /// Run all scales in order (smoke, medium, full, huge), then generate the HTML report.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    benchmark: bool,
    /// Run the DecentDB plan-cache guardrail benchmark and write a JSON report.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    plan_cache_benchmark: bool,
    /// HTML output path for --report or --benchmark (defaults to <out-dir>/report.html).
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    report_file: Option<PathBuf>,
    /// Run music-library latency suite after seed and checkpoint.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    latency_suite: bool,
    /// Iterations for latency-suite queries.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 10000)]
    latency_iterations: u64,
    /// Warmup iterations for latency-suite queries.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 200)]
    latency_warmup: u64,
    /// Iterations for heavy latency-suite queries (view, top10).
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 200)]
    heavy_latency_iterations: u64,
    /// Warmup iterations for heavy latency-suite queries.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 20)]
    heavy_latency_warmup: u64,
    /// Run concurrency suite after seed and checkpoint.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    concurrency_suite: bool,
    /// Comma-separated list of reader thread counts (e.g. 1,2,4,8).
    #[cfg(feature = "extended-suites")]
    #[arg(long, value_delimiter = ',', default_value = "1,2,4,8")]
    reader_thread_counts: Vec<usize>,
    /// Reads per thread in concurrency suite.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 25000)]
    concurrent_reads_per_thread: u64,
    /// Writer commits in concurrency read-under-write suite.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 1000)]
    writer_commits: u64,
    /// Run write suite after seed and checkpoint.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    write_suite: bool,
    /// Run cold/recovery suite.
    #[cfg(feature = "extended-suites")]
    #[arg(long)]
    cold_suite: bool,
    /// Iterations for write-suite operations.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 1000)]
    write_iterations: u64,
    /// Row cap for full-scan materialization latency case.
    #[cfg(feature = "extended-suites")]
    #[arg(long, default_value_t = 100000)]
    full_scan_row_limit: u64,
    /// SQLite exploratory profile (wal-normal).
    #[cfg(feature = "sqlite")]
    #[cfg_attr(feature = "extended-suites", arg(long, value_enum))]
    sqlite_profile: Option<SqliteProfile>,
    /// Hidden: helper mode for cold-process open.
    #[cfg(feature = "extended-suites")]
    #[arg(long, hide = true)]
    cold_helper: bool,
    /// Hidden: query type for cold helper (count_songs | artist_lookup).
    #[cfg(feature = "extended-suites")]
    #[arg(long, hide = true)]
    cold_helper_query: Option<String>,
    /// Hidden: output path for cold helper JSON.
    #[cfg(feature = "extended-suites")]
    #[arg(long, hide = true)]
    cold_helper_output: Option<PathBuf>,
    /// Hidden: expected query result for cold helper (count_songs only).
    #[cfg(feature = "extended-suites")]
    #[arg(long, hide = true)]
    cold_helper_expected_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "extended-suites", derive(ValueEnum))]
enum BenchmarkEngine {
    #[cfg_attr(
        feature = "extended-suites",
        value(name = "decentdb", alias = "decent-db")
    )]
    DecentDb,
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "duckdb")]
    #[cfg_attr(feature = "extended-suites", value(name = "duckdb"))]
    DuckDb,
}

#[cfg(feature = "sqlite")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "extended-suites", derive(ValueEnum))]
enum SqliteProfile {
    #[cfg_attr(feature = "extended-suites", value(name = "wal-normal"))]
    WalNormal,
}

impl BenchmarkEngine {
    fn binding_name(self) -> &'static str {
        match self {
            Self::DecentDb => "RustRaw",
            #[cfg(feature = "sqlite")]
            Self::Sqlite => "SQLiteRusqlite",
            #[cfg(feature = "duckdb")]
            Self::DuckDb => "DuckDbRs",
        }
    }

    fn default_db_path(self, scale: Scale) -> PathBuf {
        match self {
            Self::DecentDb => PathBuf::from(format!("run-rust-{}.ddb", scale.name)),
            #[cfg(feature = "sqlite")]
            Self::Sqlite => PathBuf::from(format!("run-rust-sqlite-{}.db", scale.name)),
            #[cfg(feature = "duckdb")]
            Self::DuckDb => PathBuf::from(format!("run-rust-duckdb-{}.db", scale.name)),
        }
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn validate_engine_profile(cli: &Cli) -> anyhow::Result<()> {
    #[cfg(feature = "sqlite")]
    if cli.sqlite_profile.is_some() && cli.engine != BenchmarkEngine::Sqlite {
        bail!("--sqlite-profile is only supported for --engine sqlite");
    }

    match cli.engine {
        BenchmarkEngine::DecentDb => Ok(()),
        #[cfg(feature = "sqlite")]
        BenchmarkEngine::Sqlite => validate_comparison_engine_profile(cli.profile),
        #[cfg(feature = "duckdb")]
        BenchmarkEngine::DuckDb => validate_comparison_engine_profile(cli.profile),
    }
}

#[cfg(any(feature = "sqlite", feature = "duckdb"))]
fn validate_comparison_engine_profile(profile: BenchmarkProfile) -> anyhow::Result<()> {
    if profile != BenchmarkProfile::Default {
        bail!("--profile is only supported for --engine decentdb");
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "extended-suites", derive(ValueEnum))]
enum BenchmarkProfile {
    /// Default durable engine configuration.
    Default,
    /// Durable profile for bulk-load-then-read workloads on one handle.
    ResidentHotRead,
}

impl BenchmarkProfile {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::ResidentHotRead => "resident-hot-read",
        }
    }

    fn db_config(self) -> DbConfig {
        match self {
            Self::Default => DbConfig::default(),
            Self::ResidentHotRead => DbConfig {
                retain_paged_row_sources_after_commit: true,
                ..DbConfig::default()
            },
        }
    }
}

#[cfg(not(feature = "extended-suites"))]
#[derive(Debug)]
enum CanonicalCliAction {
    Run(Cli),
    Help,
    Version,
}

#[cfg(not(feature = "extended-suites"))]
#[cold]
#[inline(never)]
fn canonical_missing_value(option: &str) -> String {
    format!("missing value for {option}")
}

#[cfg(not(feature = "extended-suites"))]
#[cold]
#[inline(never)]
fn canonical_invalid_value(option: &str, value: &str) -> String {
    format!("invalid value '{value}' for {option}")
}

#[cfg(not(feature = "extended-suites"))]
#[cold]
#[inline(never)]
fn canonical_unexpected_argument(argument: &str) -> String {
    format!("unexpected argument {argument:?}")
}

#[cfg(not(feature = "extended-suites"))]
#[cold]
#[inline(never)]
fn canonical_non_utf8_value(option: &str) -> String {
    format!("value for {option} must be valid UTF-8")
}

#[cfg(not(feature = "extended-suites"))]
fn canonical_required_os_value<I>(
    arguments: &mut I,
    inline_value: Option<&str>,
    option: &str,
) -> Result<std::ffi::OsString, String>
where
    I: Iterator<Item = std::ffi::OsString>,
{
    inline_value.map(std::ffi::OsString::from).map_or_else(
        || {
            arguments
                .next()
                .ok_or_else(|| canonical_missing_value(option))
        },
        Ok,
    )
}

#[cfg(not(feature = "extended-suites"))]
fn canonical_required_text_value<I>(
    arguments: &mut I,
    inline_value: Option<&str>,
    option: &str,
) -> Result<String, String>
where
    I: Iterator<Item = std::ffi::OsString>,
{
    canonical_required_os_value(arguments, inline_value, option)?
        .into_string()
        .map_err(|_| canonical_non_utf8_value(option))
}

#[cfg(not(feature = "extended-suites"))]
fn parse_canonical_cli_from<I, T>(arguments: I) -> Result<CanonicalCliAction, String>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString>,
{
    let mut arguments = arguments.into_iter().map(Into::into);
    let _program = arguments.next();
    let mut scale = "smoke".to_string();
    let mut out_dir = PathBuf::from("results");
    let mut db_path = None;
    let mut seed = 42_u64;
    let mut profile = BenchmarkProfile::Default;
    let mut engine = BenchmarkEngine::DecentDb;
    #[cfg(feature = "sqlite")]
    let mut sqlite_profile = None;

    while let Some(argument) = arguments.next() {
        let argument = argument
            .into_string()
            .map_err(|_| "option names must be valid UTF-8".to_string())?;
        if matches!(argument.as_str(), "-h" | "--help") {
            return Ok(CanonicalCliAction::Help);
        }
        if matches!(argument.as_str(), "-V" | "--version") {
            return Ok(CanonicalCliAction::Version);
        }
        let (option, inline_value) = argument
            .split_once('=')
            .map_or((argument.as_str(), None), |(option, value)| {
                (option, Some(value.to_string()))
            });
        match option {
            "--scale" => {
                scale =
                    canonical_required_text_value(&mut arguments, inline_value.as_deref(), option)?;
            }
            "--out-dir" => {
                out_dir = PathBuf::from(canonical_required_os_value(
                    &mut arguments,
                    inline_value.as_deref(),
                    option,
                )?);
            }
            "--db-path" => {
                db_path = Some(PathBuf::from(canonical_required_os_value(
                    &mut arguments,
                    inline_value.as_deref(),
                    option,
                )?));
            }
            "--seed" => {
                let value =
                    canonical_required_text_value(&mut arguments, inline_value.as_deref(), option)?;
                seed = value
                    .parse()
                    .map_err(|_| canonical_invalid_value("--seed", &value))?;
            }
            "--profile" => {
                let value =
                    canonical_required_text_value(&mut arguments, inline_value.as_deref(), option)?;
                profile = match value.as_str() {
                    "default" => BenchmarkProfile::Default,
                    "resident-hot-read" => BenchmarkProfile::ResidentHotRead,
                    _ => return Err(canonical_invalid_value("--profile", &value)),
                };
            }
            "--engine" => {
                let value =
                    canonical_required_text_value(&mut arguments, inline_value.as_deref(), option)?;
                engine = match value.as_str() {
                    "decentdb" | "decent-db" => BenchmarkEngine::DecentDb,
                    #[cfg(feature = "sqlite")]
                    "sqlite" => BenchmarkEngine::Sqlite,
                    #[cfg(feature = "duckdb")]
                    "duckdb" => BenchmarkEngine::DuckDb,
                    _ => return Err(canonical_invalid_value("--engine", &value)),
                };
            }
            #[cfg(feature = "sqlite")]
            "--sqlite-profile" => {
                let value =
                    canonical_required_text_value(&mut arguments, inline_value.as_deref(), option)?;
                sqlite_profile = match value.as_str() {
                    "wal-normal" => Some(SqliteProfile::WalNormal),
                    _ => {
                        return Err(format!(
                            "{}; expected 'wal-normal'",
                            canonical_invalid_value("--sqlite-profile", &value)
                        ));
                    }
                };
            }
            _ => return Err(canonical_unexpected_argument(&argument)),
        }
    }

    Ok(CanonicalCliAction::Run(Cli {
        scale,
        out_dir,
        db_path,
        seed,
        profile,
        engine,
        #[cfg(feature = "sqlite")]
        sqlite_profile,
    }))
}

#[cfg(not(feature = "extended-suites"))]
fn parse_cli() -> Cli {
    match parse_canonical_cli_from(std::env::args_os()) {
        Ok(CanonicalCliAction::Run(cli)) => cli,
        Ok(CanonicalCliAction::Help) => {
            println!(
                "DecentDB rust-baseline benchmark\n\n\
                 Usage: rust-baseline [OPTIONS]\n\n\
                 Options:\n  \
                   --scale <smoke|medium|full|huge>\n  \
                   --out-dir <PATH>\n  \
                   --db-path <PATH>\n  \
                   --seed <INTEGER>\n  \
                   --profile <default|resident-hot-read>\n  \
                   --engine <decentdb{}{}>\n  \
                   -h, --help\n  \
                   -V, --version",
                if cfg!(feature = "sqlite") {
                    "|sqlite"
                } else {
                    ""
                },
                if cfg!(feature = "duckdb") {
                    "|duckdb"
                } else {
                    ""
                },
            );
            std::process::exit(0);
        }
        Ok(CanonicalCliAction::Version) => {
            println!("rust-baseline {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(2);
        }
    }
}

#[cfg(feature = "extended-suites")]
fn parse_cli() -> Cli {
    Cli::parse()
}

fn engine_access_path(engine: BenchmarkEngine) -> &'static str {
    match engine {
        BenchmarkEngine::DecentDb => "decentdb_native_rust",
        #[cfg(feature = "sqlite")]
        BenchmarkEngine::Sqlite => "sqlite_rusqlite_c_api",
        #[cfg(feature = "duckdb")]
        BenchmarkEngine::DuckDb => "duckdb_rs_c_api",
    }
}

fn durability_profile_label(engine: BenchmarkEngine) -> &'static str {
    match engine {
        BenchmarkEngine::DecentDb => "decentdb_durable_wal_default",
        #[cfg(feature = "sqlite")]
        BenchmarkEngine::Sqlite => "sqlite_wal_full",
        #[cfg(feature = "duckdb")]
        BenchmarkEngine::DuckDb => "duckdb_engine_default",
    }
}

fn cache_profile_label(engine: BenchmarkEngine, profile: BenchmarkProfile) -> &'static str {
    match engine {
        BenchmarkEngine::DecentDb => match profile {
            BenchmarkProfile::Default => "decentdb_default_low_memory",
            BenchmarkProfile::ResidentHotRead => "decentdb_resident_hot_read",
        },
        #[cfg(feature = "sqlite")]
        BenchmarkEngine::Sqlite => "sqlite_default_cache",
        #[cfg(feature = "duckdb")]
        BenchmarkEngine::DuckDb => "duckdb_threads_1",
    }
}

const DECENTDB_CHECKPOINT_DURABILITY_CONTRACT: &str = "main-db-sync-before-wal-truncate-v1";
const RUNNER_CONTRACT_VERSION: u32 = 1;
const RESULT_SCHEMA_VERSION: u32 = 3;

fn populate_run_report_metadata(
    report: &mut RunReport,
    engine: BenchmarkEngine,
    profile: BenchmarkProfile,
) {
    report.result_schema_version = RESULT_SCHEMA_VERSION;
    report.compiled_optional_features = compiled_optional_features();
    report.checkpoint_durability_contract = if engine == BenchmarkEngine::DecentDb {
        DECENTDB_CHECKPOINT_DURABILITY_CONTRACT.to_string()
    } else {
        String::new()
    };
    report.measurement_family = "music_library_total_runtime".to_string();
    report.engine_access_path = engine_access_path(engine).to_string();
    report.durability_profile = durability_profile_label(engine).to_string();
    report.workload_class = "bulk_load_then_read_only_music_library".to_string();
    report.cache_profile = cache_profile_label(engine, profile).to_string();
    report.query_repetition_policy = "single_execution_per_query_shape".to_string();
    report.cold_state_policy = "same_process_fresh_create_then_query".to_string();
}

fn compiled_optional_features() -> Vec<String> {
    [
        (cfg!(feature = "duckdb"), "duckdb"),
        (cfg!(feature = "extended-suites"), "extended-suites"),
        (cfg!(feature = "lua-extensions"), "lua-extensions"),
        (cfg!(feature = "sqlite"), "sqlite"),
    ]
    .into_iter()
    .filter(|(enabled, _)| *enabled)
    .map(|(_, feature)| feature.to_string())
    .collect()
}

#[cfg(feature = "extended-suites")]
fn write_events_ddl() -> &'static str {
    "CREATE TABLE write_events (\
        id BIGINT PRIMARY KEY,\
        artist_id BIGINT NOT NULL,\
        payload TEXT NOT NULL\
    );\
    CREATE INDEX idx_write_events_artist ON write_events (artist_id);"
}

fn default_benchmark_profile() -> String {
    "default".to_string()
}

fn default_result_schema_version() -> u32 {
    2
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct LatencyCaseMetric {
    name: String,
    query_shape: String,
    iterations: u64,
    warmup_iterations: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    mean_ns: f64,
    stddev_ns: f64,
    operations_per_second: f64,
    rows_per_iteration: Option<u64>,
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct ConcurrencyCaseMetric {
    name: String,
    reader_threads: usize,
    reads_per_thread: u64,
    writer_commits: u64,
    reader_p50_ns: u64,
    reader_p95_ns: u64,
    reader_p99_ns: u64,
    reader_max_ns: u64,
    reader_operations_per_second: f64,
    writer_p50_ns: Option<u64>,
    writer_p95_ns: Option<u64>,
    writer_p99_ns: Option<u64>,
    writer_operations_per_second: Option<f64>,
    reader_degradation_ratio_vs_isolated: Option<f64>,
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Copy, Debug)]
struct Scale {
    name: &'static str,
    artists: u32,
    albums: u32,
    max_songs_per_album: u32,
    songs_cap: u64,
}

const SMOKE: Scale = Scale {
    name: "smoke",
    artists: 500,
    albums: 5_000,
    max_songs_per_album: 10,
    songs_cap: 50_000,
};
const MEDIUM: Scale = Scale {
    name: "medium",
    artists: 5_000,
    albums: 50_000,
    max_songs_per_album: 10,
    songs_cap: 500_000,
};
const FULL: Scale = Scale {
    name: "full",
    artists: 50_000,
    albums: 500_000,
    max_songs_per_album: 10,
    songs_cap: 5_000_000,
};
const HUGE: Scale = Scale {
    name: "huge",
    artists: 250_000,
    albums: 2_500_000,
    max_songs_per_album: 10,
    songs_cap: 25_000_000,
};
#[cfg(any(feature = "extended-suites", test))]
const BENCHMARK_SCALES: [Scale; 4] = [SMOKE, MEDIUM, FULL, HUGE];

fn parse_scale(name: &str) -> Scale {
    match name.to_ascii_lowercase().as_str() {
        "smoke" => SMOKE,
        "medium" => MEDIUM,
        "full" => FULL,
        "huge" => HUGE,
        other => panic!("Unknown scale '{other}'. Use smoke|medium|full|huge."),
    }
}

// --- deterministic RNG -----------------------------------------------------
// SplitMix64 - small, fast, deterministic. We don't need to byte-match the
// .NET System.Random output; we just need stable, reproducible seed plans.
const SPLITMIX64_INCREMENT: u64 = 0x9E3779B97F4A7C15;

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(SPLITMIX64_INCREMENT))
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(SPLITMIX64_INCREMENT);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }
    fn skip(&mut self, count: u64) {
        self.0 = self
            .0
            .wrapping_add(SPLITMIX64_INCREMENT.wrapping_mul(count));
    }
    /// Uniform in [0, n).
    fn gen_range(&mut self, n: u32) -> u32 {
        if n == 0 {
            return 0;
        }
        (self.next_u64() % n as u64) as u32
    }
}

// --- seed plan -------------------------------------------------------------
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArtistSeed {
    id: i64,
    country: &'static str,
    formed_year: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AlbumSeed {
    id: i64,
    artist_id: i64,
    release_year: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SongSeed {
    id: i64,
    album_id: i64,
    artist_id: i64,
    duration_ms: i32,
}

const COUNTRIES: &[&str] = &["US", "UK", "DE", "FR", "JP", "BR", "CA", "AU", "SE", "NL"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SeedSummary {
    total_albums: u64,
    total_songs: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SeedWalkEmit {
    artists: bool,
    albums: bool,
    songs: bool,
}

impl SeedWalkEmit {
    const NONE: Self = Self {
        artists: false,
        albums: false,
        songs: false,
    };
    const ARTISTS: Self = Self {
        artists: true,
        albums: false,
        songs: false,
    };
    const ALBUMS: Self = Self {
        artists: false,
        albums: true,
        songs: false,
    };
    const SONGS: Self = Self {
        artists: false,
        albums: false,
        songs: true,
    };
}

fn summarize_seed_plan(scale: Scale, seed: u64) -> SeedSummary {
    walk_seed_plan_select(scale, seed, SeedWalkEmit::NONE, |_| {}, |_| {}, |_| {})
}

fn walk_seed_plan_select(
    scale: Scale,
    seed: u64,
    emit: SeedWalkEmit,
    mut on_artist: impl FnMut(ArtistSeed),
    mut on_album: impl FnMut(AlbumSeed),
    mut on_song: impl FnMut(SongSeed),
) -> SeedSummary {
    let mut rng = Rng::new(seed);
    let mut album_counter: i64 = 0;
    let mut song_counter: i64 = 0;
    let mut album_quota = scale.albums as i64;
    let song_quota = scale.songs_cap as i64;

    for a in 0..scale.artists {
        let artist_id = (a + 1) as i64;
        let artists_remaining = (scale.artists - a) as i64;
        let albums_remaining = album_quota;
        let avg = (albums_remaining / artists_remaining).max(1);
        let desired = if a == scale.artists - 1 {
            albums_remaining
        } else {
            let cap = (albums_remaining - (artists_remaining - 1)).max(1);
            let r = 1 + rng.gen_range((avg as u32) * 2) as i64;
            r.min(cap).max(1)
        };
        album_quota -= desired;

        for _ in 0..desired {
            let mut songs_this_album = 1 + rng.gen_range(scale.max_songs_per_album) as i64;
            if song_counter + songs_this_album > song_quota {
                songs_this_album = (song_quota - song_counter).max(0);
            }
            album_counter += 1;
            let album_id = album_counter;
            if emit.songs {
                for _ in 0..songs_this_album {
                    song_counter += 1;
                    let duration_ms = 60_000 + rng.gen_range(360_000) as i32;
                    on_song(SongSeed {
                        id: song_counter,
                        album_id,
                        artist_id,
                        duration_ms,
                    });
                }
            } else {
                song_counter += songs_this_album;
                rng.skip(songs_this_album as u64);
            }
            let release_year = 1960 + rng.gen_range(65) as i32;
            if emit.albums {
                on_album(AlbumSeed {
                    id: album_id,
                    artist_id,
                    release_year,
                });
            }
        }

        let country = COUNTRIES[rng.gen_range(COUNTRIES.len() as u32) as usize];
        let formed_year = 1950 + rng.gen_range(75) as i32;
        if emit.artists {
            on_artist(ArtistSeed {
                id: artist_id,
                country,
                formed_year,
            });
        }
    }

    SeedSummary {
        total_albums: album_counter as u64,
        total_songs: song_counter as u64,
    }
}

// --- schema ----------------------------------------------------------------
const DDL: &[&str] = &[
    "CREATE TABLE artists (
        id          INTEGER PRIMARY KEY,
        name        TEXT NOT NULL,
        country     TEXT NOT NULL,
        formed_year INTEGER NOT NULL
    )",
    "CREATE TABLE albums (
        id          INTEGER PRIMARY KEY,
        artist_id   INTEGER NOT NULL,
        title       TEXT NOT NULL,
        release_year INTEGER NOT NULL
    )",
    "CREATE TABLE songs (
        id        INTEGER PRIMARY KEY,
        album_id  INTEGER NOT NULL,
        artist_id INTEGER NOT NULL,
        title     TEXT NOT NULL,
        duration_ms INTEGER NOT NULL
    )",
    "CREATE INDEX idx_albums_artist ON albums (artist_id)",
    "CREATE INDEX idx_songs_album   ON songs (album_id)",
    "CREATE INDEX idx_songs_artist  ON songs (artist_id)",
    "CREATE INDEX idx_artists_name  ON artists (name)",
    "CREATE INDEX idx_albums_title  ON albums (title)",
    "CREATE VIEW v_artist_songs AS
        SELECT
            a.id   AS artist_id,
            a.name AS artist_name,
            al.id  AS album_id,
            al.title AS album_title,
            s.id   AS song_id,
            s.title AS song_title,
            s.duration_ms AS duration_ms
        FROM artists a
        JOIN albums al ON al.artist_id = a.id
        JOIN songs  s  ON s.album_id   = al.id",
];

// --- metrics ---------------------------------------------------------------
#[derive(Clone, Default, Serialize, Deserialize)]
struct StepMetric {
    name: String,
    duration_seconds: f64,
    records: Option<u64>,
    records_per_second: Option<f64>,
    rss_bytes: u64,
    #[serde(default)]
    rss_anon_kb: u64,
    #[serde(default)]
    rss_file_kb: u64,
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct RunReport {
    binding: String,
    scale_name: String,
    #[serde(default = "default_benchmark_profile")]
    benchmark_profile: String,
    #[serde(default = "default_result_schema_version")]
    result_schema_version: u32,
    #[serde(default)]
    runner_contract_version: u32,
    #[serde(default)]
    seed: u64,
    #[serde(default)]
    invocation_argv: Vec<String>,
    #[serde(default)]
    executable_sha256: String,
    #[serde(default)]
    compiled_optional_features: Vec<String>,
    #[serde(default)]
    checkpoint_durability_contract: String,
    #[serde(default)]
    measurement_family: String,
    #[serde(default)]
    engine_access_path: String,
    #[serde(default)]
    durability_profile: String,
    #[serde(default)]
    workload_class: String,
    #[serde(default)]
    cache_profile: String,
    #[serde(default)]
    query_repetition_policy: String,
    #[serde(default)]
    cold_state_policy: String,
    target_artists: u32,
    target_albums: u32,
    target_songs_cap: u64,
    started_unix: u64,
    finished_unix: u64,
    engine_version: String,
    database_path: String,
    database_size_bytes: u64,
    wal_size_bytes: u64,
    peak_rss_bytes: u64,
    steps: Vec<StepMetric>,
    #[serde(default)]
    latency_cases: Vec<LatencyCaseMetric>,
    #[serde(default)]
    concurrency_cases: Vec<ConcurrencyCaseMetric>,
    #[serde(default)]
    write_cases: Vec<LatencyCaseMetric>,
    #[serde(default)]
    cold_cases: Vec<LatencyCaseMetric>,
}

#[derive(Clone, Debug, PartialEq)]
enum SemanticValue {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Text(String),
    Blob(Vec<u8>),
}

type SemanticRows = Vec<Vec<SemanticValue>>;

fn semantic_rows_from_decentdb(result: &decentdb::QueryResult) -> Result<SemanticRows, String> {
    result
        .rows()
        .iter()
        .map(|row| {
            row.values()
                .iter()
                .map(|value| match value {
                    Value::Null => Ok(SemanticValue::Null),
                    Value::Int64(value) => Ok(SemanticValue::Int64(*value)),
                    Value::Float64(value) => Ok(SemanticValue::Float64(*value)),
                    Value::Bool(value) => Ok(SemanticValue::Bool(*value)),
                    Value::Text(value) => Ok(SemanticValue::Text(value.clone())),
                    Value::Blob(value) => Ok(SemanticValue::Blob(value.clone())),
                    other => Err(format!(
                        "unsupported value in benchmark semantic evidence: {other:?}"
                    )),
                })
                .collect()
        })
        .collect()
}

fn semantic_checksum(rows: &SemanticRows) -> String {
    fn update_bytes(hasher: &mut Sha256, bytes: &[u8]) {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }

    let mut row_digests = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row_hasher = Sha256::new();
        row_hasher.update(b"decentdb-rust-baseline-query-row-v1\0");
        row_hasher.update((row.len() as u64).to_le_bytes());
        for value in row {
            match value {
                SemanticValue::Null => row_hasher.update([0]),
                SemanticValue::Int64(value) => {
                    row_hasher.update([1]);
                    row_hasher.update(value.to_le_bytes());
                }
                SemanticValue::Float64(value) => {
                    row_hasher.update([2]);
                    row_hasher.update(value.to_bits().to_le_bytes());
                }
                SemanticValue::Bool(value) => {
                    row_hasher.update([3]);
                    row_hasher.update([u8::from(*value)]);
                }
                SemanticValue::Text(value) => {
                    row_hasher.update([4]);
                    update_bytes(&mut row_hasher, value.as_bytes());
                }
                SemanticValue::Blob(value) => {
                    row_hasher.update([5]);
                    update_bytes(&mut row_hasher, value);
                }
            }
        }
        row_digests.push(row_hasher.finalize());
    }
    row_digests.sort_unstable();
    let mut hasher = Sha256::new();
    hasher.update(b"decentdb-rust-baseline-query-evidence-v2\0");
    hasher.update((row_digests.len() as u64).to_le_bytes());
    for digest in row_digests {
        hasher.update(digest);
    }
    lower_hex(&hasher.finalize())
}

fn semantic_int(row: &[SemanticValue], index: usize, label: &str) -> Result<i64, String> {
    match row.get(index) {
        Some(SemanticValue::Int64(value)) => Ok(*value),
        value => Err(format!("{label} must be Int64, got {value:?}")),
    }
}

fn semantic_number(row: &[SemanticValue], index: usize, label: &str) -> Result<f64, String> {
    match row.get(index) {
        Some(SemanticValue::Float64(value)) if value.is_finite() => Ok(*value),
        Some(SemanticValue::Int64(value)) => Ok(*value as f64),
        value => Err(format!("{label} must be a finite number, got {value:?}")),
    }
}

fn semantic_text<'a>(
    row: &'a [SemanticValue],
    index: usize,
    label: &str,
) -> Result<&'a str, String> {
    match row.get(index) {
        Some(SemanticValue::Text(value)) => Ok(value),
        value => Err(format!("{label} must be Text, got {value:?}")),
    }
}

fn require_column_count(row: &[SemanticValue], count: usize, label: &str) -> Result<(), String> {
    if row.len() != count {
        return Err(format!(
            "{label} returned {} columns; expected {count}",
            row.len()
        ));
    }
    Ok(())
}

fn validate_query_semantics(
    query_name: &str,
    rows: &SemanticRows,
    scale: Scale,
    total_songs: u64,
) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    let mut extra = serde_json::Map::new();
    extra.insert("row_count".into(), serde_json::json!(rows.len()));

    match query_name {
        "query_count_songs" => {
            if rows.len() != 1 {
                return Err(format!(
                    "count query returned {} rows; expected 1",
                    rows.len()
                ));
            }
            require_column_count(&rows[0], 1, query_name)?;
            let count = semantic_int(&rows[0], 0, "song count")?;
            if count != total_songs as i64 {
                return Err(format!(
                    "count query returned {count}; expected {total_songs}"
                ));
            }
            extra.insert("count".into(), serde_json::json!(count));
        }
        "query_aggregate_durations" => {
            if rows.len() != 1 {
                return Err(format!(
                    "aggregate query returned {} rows; expected 1",
                    rows.len()
                ));
            }
            let row = &rows[0];
            require_column_count(row, 5, query_name)?;
            let count = semantic_int(row, 0, "aggregate count")?;
            let sum = semantic_int(row, 1, "duration sum")?;
            let average = semantic_number(row, 2, "duration average")?;
            let minimum = semantic_int(row, 3, "duration minimum")?;
            let maximum = semantic_int(row, 4, "duration maximum")?;
            if count != total_songs as i64
                || sum <= 0
                || minimum <= 0
                || maximum < minimum
                || average < minimum as f64
                || average > maximum as f64
                || ((sum as f64 / count as f64) - average).abs() > 1e-9
            {
                return Err(format!(
                    "invalid aggregate tuple: count={count}, sum={sum}, avg={average}, min={minimum}, max={maximum}"
                ));
            }
            extra.insert("song_count".into(), serde_json::json!(count));
            extra.insert("duration_sum".into(), serde_json::json!(sum));
            extra.insert("duration_average".into(), serde_json::json!(average));
            extra.insert("duration_minimum".into(), serde_json::json!(minimum));
            extra.insert("duration_maximum".into(), serde_json::json!(maximum));
        }
        "query_artist_by_id" => {
            if rows.len() != 1 {
                return Err(format!(
                    "artist lookup returned {} rows; expected 1",
                    rows.len()
                ));
            }
            let row = &rows[0];
            require_column_count(row, 4, query_name)?;
            let target = i64::from(scale.artists) / 2 + 1;
            let artist_id = semantic_int(row, 0, "artist id")?;
            let artist_name = semantic_text(row, 1, "artist name")?;
            let country = semantic_text(row, 2, "artist country")?;
            let formed_year = semantic_int(row, 3, "artist formed year")?;
            if artist_id != target
                || artist_name != format!("Artist {target}")
                || country.is_empty()
                || !(1900..=2100).contains(&formed_year)
            {
                return Err(format!("invalid artist lookup row: {row:?}"));
            }
            extra.insert("target_artist_id".into(), serde_json::json!(target));
            extra.insert("artist_id".into(), serde_json::json!(artist_id));
            extra.insert("artist_name".into(), serde_json::json!(artist_name));
        }
        "query_top10_artists_by_songs" | "query_top10_albums_by_songs" => {
            if rows.len() != 10 {
                return Err(format!(
                    "{query_name} returned {} rows; expected 10",
                    rows.len()
                ));
            }
            let label = if query_name == "query_top10_artists_by_songs" {
                "Artist"
            } else {
                "Album"
            };
            let mut ids = HashSet::with_capacity(rows.len());
            let mut previous_count = i64::MAX;
            for row in rows {
                require_column_count(row, 3, query_name)?;
                let id = semantic_int(row, 0, "top10 id")?;
                let name = semantic_text(row, 1, "top10 name")?;
                let count = semantic_int(row, 2, "top10 song count")?;
                if id <= 0
                    || !ids.insert(id)
                    || name != format!("{label} {id}")
                    || count <= 0
                    || count > previous_count
                {
                    return Err(format!("invalid {query_name} row/order: {row:?}"));
                }
                previous_count = count;
            }
        }
        "query_view_first_1000" => {
            let expected_rows = usize::try_from(total_songs.min(1000)).unwrap_or(1000);
            if rows.len() != expected_rows {
                return Err(format!(
                    "view query returned {} rows; expected {expected_rows}",
                    rows.len()
                ));
            }
            for row in rows {
                require_column_count(row, 4, query_name)?;
                let artist_id = semantic_int(row, 0, "view artist id")?;
                let artist_name = semantic_text(row, 1, "view artist name")?;
                let album_title = semantic_text(row, 2, "view album title")?;
                let song_title = semantic_text(row, 3, "view song title")?;
                if artist_id <= 0
                    || artist_name != format!("Artist {artist_id}")
                    || !album_title.starts_with("Album ")
                    || !song_title.starts_with("Song ")
                {
                    return Err(format!("invalid view row: {row:?}"));
                }
            }
        }
        "query_songs_for_artist_via_view" => {
            if rows.is_empty() {
                return Err("filtered view query returned no rows".to_string());
            }
            for row in rows {
                require_column_count(row, 3, query_name)?;
                let album_title = semantic_text(row, 0, "filtered album title")?;
                let song_title = semantic_text(row, 1, "filtered song title")?;
                let duration = semantic_int(row, 2, "filtered song duration")?;
                if !album_title.starts_with("Album ")
                    || !song_title.starts_with("Song ")
                    || duration <= 0
                {
                    return Err(format!("invalid filtered view row: {row:?}"));
                }
            }
        }
        _ => return Err(format!("unknown semantic query {query_name}")),
    }
    extra.insert(
        "semantic_checksum_sha256".into(),
        serde_json::json!(semantic_checksum(rows)),
    );
    Ok(extra)
}

fn add_query_evidence(
    recorder: &mut Recorder<'_>,
    query_name: &str,
    rows: &SemanticRows,
    scale: Scale,
    total_songs: u64,
) {
    let evidence = validate_query_semantics(query_name, rows, scale, total_songs)
        .unwrap_or_else(|error| panic!("{query_name} semantic validation failed: {error}"));
    for (key, value) in evidence {
        recorder.add_extra(&key, value);
    }
}

#[cfg(feature = "extended-suites")]
#[derive(Clone, Default, Serialize)]
struct PlanCacheBenchmarkReport {
    binding: String,
    benchmark_profile: String,
    started_unix: u64,
    finished_unix: u64,
    engine_version: String,
    database_path: String,
    cases: Vec<PlanCacheCaseMetric>,
}

#[cfg(feature = "extended-suites")]
#[derive(Clone, Default, Serialize)]
struct PlanCacheCaseMetric {
    scenario: String,
    plan_cache_enabled: bool,
    iterations: u64,
    duration_seconds: f64,
    operations_per_second: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    enabled_delta_vs_disabled_percent: Option<f64>,
    p95_ns: u64,
    p99_ns: u64,
    total_hits: u64,
    total_misses: u64,
}

#[cfg(feature = "extended-suites")]
#[derive(Clone, Serialize)]
struct HistoricalRun {
    file_name: String,
    timestamp_unix: u64,
    timestamp_label: String,
    total_runtime_seconds: f64,
    /// True when the run file is not pinned by `history-manifest.json`: it is
    /// shown for local comparison but is not part of the checksum-verified
    /// canonical history.
    provisional: bool,
    report: RunReport,
}

#[cfg(feature = "extended-suites")]
#[derive(Serialize)]
struct ReportScaleSection {
    scale_name: String,
    step_names: Vec<String>,
    runs: Vec<HistoricalRun>,
}

#[cfg(feature = "extended-suites")]
#[derive(Serialize)]
struct HtmlReportData {
    generated_at_unix: u64,
    generated_at_label: String,
    source_directory: String,
    total_runs: usize,
    verified_runs: usize,
    provisional_runs: usize,
    has_manifest: bool,
    skipped_inputs: Vec<String>,
    inputs_summary: String,
    scales: Vec<ReportScaleSection>,
}

#[cfg(feature = "extended-suites")]
#[derive(Deserialize)]
struct ReportHistoryManifest {
    schema_version: u32,
    files: Vec<ReportHistoryManifestEntry>,
}

#[cfg(feature = "extended-suites")]
#[derive(Deserialize)]
struct ReportHistoryManifestEntry {
    path: String,
    sha256: String,
}

#[cfg(feature = "extended-suites")]
#[derive(Serialize, Deserialize)]
struct ColdHelperOutput {
    duration_ns: u64,
    query: String,
    engine: String,
    #[serde(default)]
    result_count: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MemorySample {
    rss_bytes: u64,
    rss_anon_kb: u64,
    rss_file_kb: u64,
}

fn parse_ascii_u64(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    bytes.iter().try_fold(0_u64, |value, byte| {
        byte.is_ascii_digit()
            .then_some(())
            .and_then(|()| value.checked_mul(10))
            .and_then(|value| value.checked_add(u64::from(*byte - b'0')))
    })
}

fn parse_statm_resident_pages(bytes: &[u8]) -> Option<u64> {
    bytes
        .split(|byte| byte.is_ascii_whitespace())
        .filter(|field| !field.is_empty())
        .nth(1)
        .and_then(parse_ascii_u64)
}

fn parse_status_kb(bytes: &[u8], field: &[u8]) -> Option<u64> {
    bytes.split(|byte| *byte == b'\n').find_map(|line| {
        line.strip_prefix(field).and_then(|suffix| {
            suffix
                .split(|byte| byte.is_ascii_whitespace())
                .find(|value| !value.is_empty())
                .and_then(parse_ascii_u64)
        })
    })
}

fn read_proc_file(path: &str, buffer: &mut [u8]) -> usize {
    let Ok(mut file) = fs::File::open(path) else {
        return 0;
    };
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(bytes_read) => filled += bytes_read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return 0,
        }
    }

    // Do not parse a truncated proc record. The benchmark report's zero
    // fallback is preferable to silently publishing a partial sample.
    if filled == buffer.len() {
        let mut extra = [0_u8; 1];
        loop {
            match file.read(&mut extra) {
                Ok(0) => break,
                Ok(_) => return 0,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => return 0,
            }
        }
    }
    filled
}

fn read_memory_sample() -> MemorySample {
    // Linux proc status records are normally well below one page. If either
    // record exceeds this bound, read_proc_file rejects it rather than parsing
    // a truncated metric.
    let mut buffer = [0_u8; 4 * 1024];
    let statm_len = read_proc_file("/proc/self/statm", &mut buffer);
    let resident_pages = parse_statm_resident_pages(&buffer[..statm_len]).unwrap_or(0);
    let rss_bytes = resident_pages.saturating_mul(unsafe { libc_sysconf_pagesize() });

    let status_len = read_proc_file("/proc/self/status", &mut buffer);
    let status = &buffer[..status_len];
    MemorySample {
        rss_bytes,
        rss_anon_kb: parse_status_kb(status, b"RssAnon:").unwrap_or(0),
        rss_file_kb: parse_status_kb(status, b"RssFile:").unwrap_or(0),
    }
}

// Tiny inline syscall to avoid pulling libc crate.
#[allow(non_snake_case)]
unsafe fn libc_sysconf_pagesize() -> u64 {
    extern "C" {
        fn sysconf(name: i32) -> i64;
    }
    const _SC_PAGESIZE: i32 = 30;
    let v = unsafe { sysconf(_SC_PAGESIZE) };
    if v <= 0 {
        4096
    } else {
        v as u64
    }
}

struct Recorder<'a> {
    report: &'a mut RunReport,
    peak_rss: u64,
}
impl<'a> Recorder<'a> {
    fn new(report: &'a mut RunReport) -> Self {
        Self {
            report,
            peak_rss: 0,
        }
    }
    #[cfg(feature = "extended-suites")]
    fn format_duration_ns(ns: u64) -> String {
        if ns < 1_000 {
            format!("{ns} ns")
        } else if ns < 1_000_000 {
            format!("{:.0} μs", ns as f64 / 1_000.0)
        } else if ns < 1_000_000_000 {
            format!("{:.0} ms", ns as f64 / 1_000_000.0)
        } else {
            format!("{:.3} s", ns as f64 / 1_000_000_000.0)
        }
    }
    fn measure<F, R>(&mut self, name: &str, records: Option<u64>, body: F) -> R
    where
        F: FnOnce() -> R,
    {
        let t0 = Instant::now();
        let out = body();
        let dur_ns = t0.elapsed().as_secs_f64() * 1_000_000_000.0;
        let dur_secs = dur_ns / 1_000_000_000.0;
        let memory = read_memory_sample();
        let rss = memory.rss_bytes;
        let rss_anon_kb = memory.rss_anon_kb;
        let rss_file_kb = memory.rss_file_kb;
        if rss > self.peak_rss {
            self.peak_rss = rss;
        }
        let rps = records.map(|r| {
            if dur_secs > 0.0 {
                r as f64 / dur_secs
            } else {
                0.0
            }
        });
        #[cfg(feature = "extended-suites")]
        println!(
            "  [Rust    ] {:<38} {:>12}  {:>14}  {:>14}  RSS={:>9}",
            name,
            Self::format_duration_ns(dur_ns as u64),
            records.map(|r| format!("{r:>12} rec")).unwrap_or_default(),
            rps.map(|r| format!("{r:>12.0} r/s")).unwrap_or_default(),
            format_bytes(rss),
        );
        self.report.steps.push(StepMetric {
            name: name.to_string(),
            duration_seconds: dur_secs,
            records,
            records_per_second: rps,
            rss_bytes: rss,
            rss_anon_kb,
            rss_file_kb,
            extra: Default::default(),
        });
        self.report.peak_rss_bytes = self.peak_rss;
        out
    }
    fn add_extra(&mut self, key: &str, v: serde_json::Value) {
        if let Some(s) = self.report.steps.last_mut() {
            s.extra.insert(key.to_string(), v);
        }
    }
}

#[cfg(feature = "extended-suites")]
fn format_bytes(b: u64) -> String {
    const U: &[&str] = &["B", "KB", "MB", "GB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < U.len() {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1}{}", U[i])
}

// --- core run --------------------------------------------------------------
fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Writes `contents` to `path` atomically: a sibling temporary file is
/// written first and then renamed over `path`, so an interrupted run cannot
/// leave a truncated JSON result that would later break report loading.
fn write_json_atomic(path: &Path, contents: &str) -> std::io::Result<()> {
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, contents)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn delete_db_files(path: &Path) {
    let _ = fs::remove_file(path);
    // Remove every companion file used by the benchmark's supported
    // DecentDB profiles so each measured create starts from clean storage.
    for suffix in [".wal", ".coord", ".wal-idx"] {
        let _ = fs::remove_file(decentdb_companion_path(path, suffix));
    }
    // Belt-and-suspenders: the .NET tests use both -wal and .wal historically.
    if let Some(stem) = path.file_name().and_then(|s| s.to_str()) {
        if let Some(parent) = path.parent() {
            let _ = fs::remove_file(parent.join(format!("{stem}-wal")));
            let _ = fs::remove_file(parent.join(format!("{stem}-shm")));
        }
    }
}

fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let file = fs::File::open(path)?;
    let mut reader = BufReader::new(file);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 8 * 1024];
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }
    Ok(lower_hex(&hasher.finalize()))
}

fn finalize_runner_provenance(report: &mut RunReport, seed: u64) -> anyhow::Result<()> {
    let invocation_argv = std::env::args_os()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| anyhow::anyhow!("benchmark argv contains non-UTF-8 bytes"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    report.runner_contract_version = RUNNER_CONTRACT_VERSION;
    report.seed = seed;
    report.invocation_argv = invocation_argv;
    let executable = std::env::current_exe()?;
    report.executable_sha256 = sha256_file(&executable)?;
    Ok(())
}

fn decentdb_wal_path(path: &Path) -> PathBuf {
    decentdb_companion_path(path, ".wal")
}

fn decentdb_companion_path(path: &Path, suffix: &str) -> PathBuf {
    let mut companion = path.as_os_str().to_owned();
    companion.push(suffix);
    PathBuf::from(companion)
}

#[cfg(feature = "extended-suites")]
fn canonical_or_original(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

#[cfg(feature = "extended-suites")]
fn helper_output_dir(preferred: &Path) -> anyhow::Result<PathBuf> {
    let fallback = PathBuf::from(".tmp");
    for candidate in [preferred, fallback.as_path()] {
        if let Ok(()) = fs::create_dir_all(candidate) {
            return Ok(candidate
                .canonicalize()
                .unwrap_or_else(|_| candidate.to_path_buf()));
        }
    }
    bail!(
        "failed to create helper output directory at {:?} or {:?}",
        preferred,
        fallback
    );
}

#[cfg(feature = "extended-suites")]
fn resolve_cold_helper_executable() -> anyhow::Result<PathBuf> {
    let exe = std::env::current_exe()?;
    Ok(exe.canonicalize().unwrap_or(exe))
}

fn run(cli: Cli) -> anyhow::Result<()> {
    #[cfg(feature = "extended-suites")]
    if cli.cold_helper {
        return run_cold_helper(cli);
    }

    #[cfg(feature = "extended-suites")]
    if cli.plan_cache_benchmark && (cli.benchmark || cli.report) {
        bail!("--plan-cache-benchmark cannot be combined with --benchmark or --report");
    }
    #[cfg(feature = "extended-suites")]
    if cli.report_file.is_some() && !cli.report && !cli.benchmark {
        bail!("--report-file requires --report or --benchmark");
    }

    #[cfg(feature = "extended-suites")]
    if cli.report && !cli.benchmark && !cli.plan_cache_benchmark {
        generate_report_from_cli(&cli)?;
        return Ok(());
    }

    validate_engine_profile(&cli)?;

    #[cfg(feature = "extended-suites")]
    if cli.plan_cache_benchmark {
        if cli.latency_suite || cli.concurrency_suite || cli.write_suite || cli.cold_suite {
            bail!("--plan-cache-benchmark cannot be combined with --latency-suite, --concurrency-suite, --write-suite, or --cold-suite");
        }
        return run_plan_cache_benchmark(&cli);
    }

    #[cfg(feature = "extended-suites")]
    if cli.benchmark {
        return run_benchmark_suite(&cli);
    }

    #[cfg(feature = "duckdb")]
    if cli.engine == BenchmarkEngine::DuckDb {
        return run_duckdb_benchmark(&cli, parse_scale(&cli.scale));
    }

    let scale = parse_scale(&cli.scale);
    run_single_benchmark(&cli, scale)
}

#[cfg(feature = "extended-suites")]
fn run_benchmark_suite(cli: &Cli) -> anyhow::Result<()> {
    if cli.db_path.is_some() {
        bail!("--db-path is not supported with --benchmark; each scale uses its own database path");
    }
    validate_engine_profile(cli)?;

    #[cfg(feature = "sqlite")]
    let suite_profile = if cli.engine == BenchmarkEngine::Sqlite {
        "sqlite-wal-full"
    } else {
        cli.profile.as_str()
    };
    #[cfg(not(feature = "sqlite"))]
    let suite_profile = cli.profile.as_str();
    println!(
        "Running rust-baseline benchmark suite: engine={:?} profile={} scales=smoke,medium,full,huge",
        cli.engine, suite_profile
    );
    for scale in BENCHMARK_SCALES {
        println!("\n=== scale: {} ===", scale.name);
        #[cfg(feature = "duckdb")]
        if cli.engine == BenchmarkEngine::DuckDb {
            run_duckdb_benchmark(cli, scale)?;
            continue;
        }
        run_single_benchmark(cli, scale)?;
    }
    generate_report_from_cli(cli)?;
    Ok(())
}

#[cfg(feature = "extended-suites")]
fn generate_report_from_cli(cli: &Cli) -> anyhow::Result<()> {
    let report_file = cli
        .report_file
        .clone()
        .unwrap_or_else(|| cli.out_dir.join("report.html"));
    generate_html_report(&cli.out_dir, &report_file)?;
    println!("Wrote {}", report_file.display());
    Ok(())
}

#[cfg(feature = "extended-suites")]
const PLAN_CACHE_BENCH_ROWS: i64 = 10_000;
#[cfg(feature = "extended-suites")]
const PLAN_CACHE_REPEATED_ITERS: u64 = 20_000;
#[cfg(feature = "extended-suites")]
const PLAN_CACHE_ONE_SHOT_ITERS: u64 = 1_000;
#[cfg(feature = "extended-suites")]
const PLAN_CACHE_CHURN_ITERS: u64 = 20_000;
#[cfg(feature = "extended-suites")]
const PLAN_CACHE_CHURN_VARIANTS: usize = 1_000;
#[cfg(feature = "extended-suites")]
const PLAN_CACHE_CHURN_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[cfg(feature = "extended-suites")]
fn run_plan_cache_benchmark(cli: &Cli) -> anyhow::Result<()> {
    if cli.engine != BenchmarkEngine::DecentDb {
        bail!("--plan-cache-benchmark is DecentDB-only");
    }
    if cli.profile != BenchmarkProfile::Default {
        bail!("--plan-cache-benchmark uses the default DecentDB profile");
    }

    let base_path = cli
        .db_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("run-rust-plan-cache.ddb"));
    let mut report = PlanCacheBenchmarkReport {
        binding: BenchmarkEngine::DecentDb.binding_name().to_string(),
        benchmark_profile: "plan-cache".to_string(),
        started_unix: now_unix(),
        engine_version: decentdb::version().to_string(),
        database_path: base_path.display().to_string(),
        ..Default::default()
    };

    let mut repeated_enabled = measure_plan_cache_repeated_prepare(&base_path, true)?;
    let repeated_disabled = measure_plan_cache_repeated_prepare(&base_path, false)?;
    record_enabled_delta(&mut repeated_enabled, &repeated_disabled);
    report.cases.push(repeated_enabled);
    report.cases.push(repeated_disabled);

    let mut one_shot_enabled = measure_plan_cache_one_shot(&base_path, true)?;
    let one_shot_disabled = measure_plan_cache_one_shot(&base_path, false)?;
    record_enabled_delta(&mut one_shot_enabled, &one_shot_disabled);
    report.cases.push(one_shot_enabled);
    report.cases.push(one_shot_disabled);

    let mut churn_enabled = measure_plan_cache_churn(&base_path, true)?;
    let churn_disabled = measure_plan_cache_churn(&base_path, false)?;
    record_enabled_delta(&mut churn_enabled, &churn_disabled);
    report.cases.push(churn_enabled);
    report.cases.push(churn_disabled);

    report.finished_unix = now_unix();
    fs::create_dir_all(&cli.out_dir)?;
    let datetime_stamp = format_unix_filename_stamp(report.finished_unix);
    let out_path = cli
        .out_dir
        .join(format!("{datetime_stamp}-rust-baseline-plan-cache.json"));
    write_json_atomic(&out_path, &serde_json::to_string_pretty(&report)?)?;
    println!("\nWrote {}", out_path.display());
    delete_db_files(&base_path);
    Ok(())
}

#[cfg(feature = "extended-suites")]
fn record_enabled_delta(enabled: &mut PlanCacheCaseMetric, disabled: &PlanCacheCaseMetric) {
    if enabled.plan_cache_enabled && disabled.duration_seconds > 0.0 {
        enabled.enabled_delta_vs_disabled_percent = Some(
            (enabled.duration_seconds - disabled.duration_seconds) * 100.0
                / disabled.duration_seconds,
        );
    }
}

#[cfg(feature = "extended-suites")]
fn create_plan_cache_fixture(
    path: &Path,
    plan_cache_enabled: bool,
    max_cache_bytes: Option<u64>,
) -> anyhow::Result<decentdb::Db> {
    delete_db_files(path);
    let mut config = DbConfig::default();
    config.with_plan_cache(|cfg| {
        cfg.enabled = plan_cache_enabled;
        if let Some(max_cache_bytes) = max_cache_bytes {
            cfg.max_size_bytes = max_cache_bytes;
        }
    });
    let db = decentdb::Db::create(path, config)?;
    db.execute("CREATE TABLE bench (id INTEGER PRIMARY KEY, val TEXT, grp INTEGER)")?;
    {
        let mut txn = db.transaction()?;
        let insert = txn.prepare("INSERT INTO bench VALUES ($1, $2, $3)")?;
        for i in 0..PLAN_CACHE_BENCH_ROWS {
            insert.execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Text(format!("v{i}")),
                    Value::Int64(i % 10),
                ],
            )?;
        }
        txn.commit()?;
    }
    Ok(db)
}

#[cfg(feature = "extended-suites")]
fn measure_plan_cache_repeated_prepare(
    base_path: &Path,
    plan_cache_enabled: bool,
) -> anyhow::Result<PlanCacheCaseMetric> {
    let db = create_plan_cache_fixture(base_path, plan_cache_enabled, None)?;
    db.flush_plan_cache()?;
    let before = db.plan_cache_summary()?;
    let mut latencies = Vec::with_capacity(PLAN_CACHE_REPEATED_ITERS as usize);
    let start = Instant::now();
    for _ in 0..PLAN_CACHE_REPEATED_ITERS {
        let op_start = Instant::now();
        let prepared = db.prepare("SELECT val FROM bench WHERE id = $1")?;
        black_box(prepared);
        latencies.push(elapsed_ns(op_start));
    }
    let elapsed = start.elapsed().as_secs_f64();
    let after = db.plan_cache_summary()?;
    let hit_delta = after.total_hits.saturating_sub(before.total_hits);
    let miss_delta = after.total_misses.saturating_sub(before.total_misses);
    if plan_cache_enabled {
        let expected_hits = PLAN_CACHE_REPEATED_ITERS.saturating_sub(1);
        if hit_delta < expected_hits {
            bail!("repeated prepare expected at least {expected_hits} hits, got {hit_delta}");
        }
    } else if hit_delta != 0 || miss_delta != 0 {
        bail!("disabled cache reported hits={hit_delta} misses={miss_delta}");
    }
    delete_db_files(base_path);
    Ok(plan_cache_case_metric(
        "repeated_prepare_point_lookup",
        plan_cache_enabled,
        PLAN_CACHE_REPEATED_ITERS,
        elapsed,
        &mut latencies,
        hit_delta,
        miss_delta,
    ))
}

#[cfg(feature = "extended-suites")]
fn measure_plan_cache_one_shot(
    base_path: &Path,
    plan_cache_enabled: bool,
) -> anyhow::Result<PlanCacheCaseMetric> {
    let db = create_plan_cache_fixture(base_path, plan_cache_enabled, None)?;
    db.flush_plan_cache()?;
    let before = db.plan_cache_summary()?;
    let mut latencies = Vec::with_capacity(PLAN_CACHE_ONE_SHOT_ITERS as usize);
    let start = Instant::now();
    for i in 0..PLAN_CACHE_ONE_SHOT_ITERS {
        let id = (i as i64) % PLAN_CACHE_BENCH_ROWS;
        let sql = format!("SELECT COUNT(*) FROM bench WHERE id = {id} AND {i} = {i}");
        let op_start = Instant::now();
        let result = db.execute(&sql)?;
        black_box(result.rows().len());
        latencies.push(elapsed_ns(op_start));
    }
    let elapsed = start.elapsed().as_secs_f64();
    let after = db.plan_cache_summary()?;
    let hit_delta = after.total_hits.saturating_sub(before.total_hits);
    let miss_delta = after.total_misses.saturating_sub(before.total_misses);
    if hit_delta != 0 {
        bail!("one-shot benchmark should not hit the cache, got {hit_delta}");
    }
    if !plan_cache_enabled && miss_delta != 0 {
        bail!("disabled cache reported one-shot misses={miss_delta}");
    }
    delete_db_files(base_path);
    Ok(plan_cache_case_metric(
        "one_shot_query",
        plan_cache_enabled,
        PLAN_CACHE_ONE_SHOT_ITERS,
        elapsed,
        &mut latencies,
        hit_delta,
        miss_delta,
    ))
}

#[cfg(feature = "extended-suites")]
fn measure_plan_cache_churn(
    base_path: &Path,
    plan_cache_enabled: bool,
) -> anyhow::Result<PlanCacheCaseMetric> {
    let db = create_plan_cache_fixture(
        base_path,
        plan_cache_enabled,
        plan_cache_enabled.then_some(PLAN_CACHE_CHURN_MAX_BYTES),
    )?;
    let statements = build_plan_cache_point_lookup_statements();
    db.flush_plan_cache()?;
    if plan_cache_enabled {
        for sql in &statements {
            black_box(db.prepare(sql)?);
        }
    }
    let before = db.plan_cache_summary()?;
    let mut latencies = Vec::with_capacity(PLAN_CACHE_CHURN_ITERS as usize);
    let start = Instant::now();
    for i in 0..PLAN_CACHE_CHURN_ITERS {
        let sql = &statements[i as usize % statements.len()];
        let op_start = Instant::now();
        let prepared = db.prepare(sql)?;
        black_box(prepared);
        latencies.push(elapsed_ns(op_start));
    }
    let elapsed = start.elapsed().as_secs_f64();
    let after = db.plan_cache_summary()?;
    let hit_delta = after.total_hits.saturating_sub(before.total_hits);
    let miss_delta = after.total_misses.saturating_sub(before.total_misses);
    if plan_cache_enabled {
        if hit_delta < PLAN_CACHE_CHURN_ITERS {
            bail!(
                "warm churn expected at least {} hits, got {hit_delta}",
                PLAN_CACHE_CHURN_ITERS
            );
        }
    } else if hit_delta != 0 || miss_delta != 0 {
        bail!("disabled cache reported hits={hit_delta} misses={miss_delta}");
    }
    delete_db_files(base_path);
    Ok(plan_cache_case_metric(
        "churn_prepare_p95_p99",
        plan_cache_enabled,
        PLAN_CACHE_CHURN_ITERS,
        elapsed,
        &mut latencies,
        hit_delta,
        miss_delta,
    ))
}

#[cfg(feature = "extended-suites")]
fn build_plan_cache_point_lookup_statements() -> Vec<String> {
    (0..PLAN_CACHE_CHURN_VARIANTS)
        .map(|idx| {
            let id = idx as i64 % PLAN_CACHE_BENCH_ROWS;
            format!("SELECT val FROM bench WHERE id = {id}")
        })
        .collect()
}

#[cfg(feature = "extended-suites")]
fn plan_cache_case_metric(
    scenario: &str,
    plan_cache_enabled: bool,
    iterations: u64,
    duration_seconds: f64,
    latencies: &mut [u64],
    total_hits: u64,
    total_misses: u64,
) -> PlanCacheCaseMetric {
    let operations_per_second = if duration_seconds > 0.0 {
        iterations as f64 / duration_seconds
    } else {
        0.0
    };
    let p95_ns = percentile_ns(latencies, 95);
    let p99_ns = percentile_ns(latencies, 99);
    println!(
        "  [PlanCache] {:<32} {:<8} {:>10.0} ops/s p95={} p99={} hits={} misses={}",
        scenario,
        if plan_cache_enabled {
            "enabled"
        } else {
            "disabled"
        },
        operations_per_second,
        Recorder::format_duration_ns(p95_ns),
        Recorder::format_duration_ns(p99_ns),
        total_hits,
        total_misses
    );
    PlanCacheCaseMetric {
        scenario: scenario.to_string(),
        plan_cache_enabled,
        iterations,
        duration_seconds,
        operations_per_second,
        enabled_delta_vs_disabled_percent: None,
        p95_ns,
        p99_ns,
        total_hits,
        total_misses,
    }
}

#[cfg(feature = "extended-suites")]
fn elapsed_ns(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(feature = "extended-suites")]
fn percentile_ns(samples: &mut [u64], percentile: u32) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let idx = (samples.len().saturating_sub(1) * percentile as usize).div_ceil(100);
    samples[idx]
}

fn run_single_benchmark(cli: &Cli, scale: Scale) -> anyhow::Result<()> {
    validate_engine_profile(cli)?;
    let db_path = cli
        .db_path
        .clone()
        .unwrap_or_else(|| cli.engine.default_db_path(scale));

    run_single_benchmark_with_path(cli, scale, db_path)
}

fn run_single_benchmark_with_path(cli: &Cli, scale: Scale, db_path: PathBuf) -> anyhow::Result<()> {
    let out_dir = cli.out_dir.clone();
    let engine = cli.engine;
    let profile = cli.profile;
    let seed = cli.seed;

    #[cfg(feature = "extended-suites")]
    println!(
        "Summarizing seed plan: engine={:?} scale={} artists={} albums(target)={} songs_cap={}",
        engine, scale.name, scale.artists, scale.albums, scale.songs_cap
    );
    let summary = summarize_seed_plan(scale, seed);
    #[cfg(feature = "extended-suites")]
    println!(
        "Plan: artists={} total_albums={} total_songs={}",
        scale.artists, summary.total_albums, summary.total_songs
    );

    #[cfg(feature = "sqlite")]
    if engine == BenchmarkEngine::Sqlite {
        return run_sqlite_benchmark(scale, seed, summary, db_path, out_dir, cli);
    }

    delete_db_files(&db_path);

    let mut report = RunReport {
        started_unix: now_unix(),
        ..Default::default()
    };
    let db;
    let peak_rss;
    #[cfg(feature = "extended-suites")]
    let total_songs;
    {
        let mut rec = Recorder::new(&mut report);

        db = rec.measure("connect_open", None, || {
            decentdb::Db::create(&db_path, profile.db_config()).expect("Db::create")
        });

        #[cfg(feature = "extended-suites")]
        let needs_write_events = cli.write_suite || cli.concurrency_suite;
        #[cfg(feature = "extended-suites")]
        let ddl_batch = if needs_write_events {
            let mut batch = build_schema_ddl_batch();
            batch.push('\n');
            batch.push_str(write_events_ddl());
            batch.push(';');
            batch
        } else {
            build_schema_ddl_batch()
        };
        #[cfg(not(feature = "extended-suites"))]
        let ddl_batch = build_schema_ddl_batch();
        rec.measure("schema_create", None, || {
            db.execute_batch(&ddl_batch).expect("ddl batch");
        });

        let insert_artist: PreparedStatement = db
            .prepare(
                "INSERT INTO artists (id, name, country, formed_year) \
             VALUES ($1, $2, $3, $4)",
            )
            .expect("prepare artists");
        // ── Seed artists ──────────────────────────────────────────────
        rec.measure("seed_artists", Some(u64::from(scale.artists)), || {
            let mut txn = db.transaction().expect("begin");
            let params: &mut [Value] = &mut [
                Value::Int64(0),
                Value::Text(String::new()),
                Value::Text(String::new()),
                Value::Int64(0),
            ];
            let mut artist_name = String::with_capacity(32);
            {
                let mut batch = txn
                    .prepared_batch(&insert_artist, params.len())
                    .expect("prepare artist batch");
                walk_seed_plan_select(
                    scale,
                    seed,
                    SeedWalkEmit::ARTISTS,
                    |a| {
                        params[0] = Value::Int64(a.id);
                        artist_name.clear();
                        artist_name.push_str("Artist ");
                        write!(&mut artist_name, "{}", a.id).expect("write artist name");
                        params[1] = Value::Text(artist_name.clone());
                        params[2] = Value::Text(a.country.to_string());
                        params[3] = Value::Int64(a.formed_year as i64);
                        batch.execute_mut(params).expect("ins artist");
                    },
                    |_| {},
                    |_| {},
                );
            }
            txn.commit().expect("commit artists");
        });

        let insert_album: PreparedStatement = db
            .prepare(
                "INSERT INTO albums (id, artist_id, title, release_year) \
             VALUES ($1, $2, $3, $4)",
            )
            .expect("prepare albums");
        // ── Seed albums ───────────────────────────────────────────────
        rec.measure("seed_albums", Some(summary.total_albums), || {
            seed_albums(&db, &insert_album, scale, seed);
        });

        let insert_song: PreparedStatement = db
            .prepare(
                "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) \
             VALUES ($1, $2, $3, $4, $5)",
            )
            .expect("prepare songs");
        // ── Seed songs ────────────────────────────────────────────────
        rec.measure("seed_songs", Some(summary.total_songs), || {
            seed_songs(&db, &insert_song, scale, seed);
        });

        let wal_bytes_before = file_size(&decentdb_wal_path(&db_path));
        let database_bytes_before = file_size(&db_path);
        rec.measure("checkpoint_after_seed", None, || {
            db.checkpoint_wal().expect("checkpoint wal after seed");
        });
        let wal_bytes_after = file_size(&decentdb_wal_path(&db_path));
        let database_bytes_after = file_size(&db_path);
        rec.add_extra("checkpoint_mode", serde_json::json!("wal"));
        rec.add_extra("wal_bytes_before", serde_json::json!(wal_bytes_before));
        rec.add_extra("wal_bytes_after", serde_json::json!(wal_bytes_after));
        rec.add_extra(
            "database_bytes_before",
            serde_json::json!(database_bytes_before),
        );
        rec.add_extra(
            "database_bytes_after",
            serde_json::json!(database_bytes_after),
        );

        // ── Queries ───────────────────────────────────────────────────
        let count_result = rec.measure("query_count_songs", None, || {
            let r = db.execute("SELECT COUNT(*) FROM songs").expect("count");
            let v = first_value(&r);
            println!("    count={v:?}");
            r
        });
        let count_rows = semantic_rows_from_decentdb(&count_result)
            .expect("convert count result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_count_songs",
            &count_rows,
            scale,
            summary.total_songs,
        );
        #[cfg(feature = "extended-suites")]
        {
            total_songs = u64::try_from(scalar_int(&count_result)).unwrap_or(0);
        }
        drop(count_rows);
        drop(count_result);

        let aggregate_result = rec.measure("query_aggregate_durations", None, || {
            let r = db
                .execute(
                    "SELECT COUNT(*), SUM(duration_ms), AVG(duration_ms), \
                       MIN(duration_ms), MAX(duration_ms) FROM songs",
                )
                .expect("agg");
            if let Some(row) = r.rows().first() {
                println!("    agg_row={row:?}");
            }
            r
        });
        let aggregate_rows = semantic_rows_from_decentdb(&aggregate_result)
            .expect("convert aggregate result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_aggregate_durations",
            &aggregate_rows,
            scale,
            summary.total_songs,
        );
        drop(aggregate_rows);
        drop(aggregate_result);

        let artist_result = rec.measure("query_artist_by_id", None, || {
            let target = i64::from(scale.artists) / 2 + 1;
            let r = db
                .execute_with_params(
                    "SELECT id, name, country, formed_year FROM artists WHERE id = $1",
                    &[Value::Int64(target)],
                )
                .expect("by id");
            if let Some(row) = r.rows().first() {
                println!("    artist={row:?}");
            }
            r
        });
        let artist_rows = semantic_rows_from_decentdb(&artist_result)
            .expect("convert artist result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_artist_by_id",
            &artist_rows,
            scale,
            summary.total_songs,
        );
        drop(artist_rows);
        drop(artist_result);

        let top_artists_result = rec.measure("query_top10_artists_by_songs", None, || {
            let r = db
                .execute(
                    "SELECT a.id, a.name, COUNT(s.id) AS song_count
                 FROM artists a
                 JOIN songs s ON s.artist_id = a.id
                 GROUP BY a.id, a.name
                 ORDER BY song_count DESC
                 LIMIT 10",
                )
                .expect("top10 artists");
            println!("    rows={}", r.rows().len());
            r
        });
        let top_artist_rows = semantic_rows_from_decentdb(&top_artists_result)
            .expect("convert top artists result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_top10_artists_by_songs",
            &top_artist_rows,
            scale,
            summary.total_songs,
        );
        drop(top_artist_rows);
        drop(top_artists_result);

        let top_albums_result = rec.measure("query_top10_albums_by_songs", None, || {
            let r = db
                .execute(
                    "SELECT al.id, al.title, COUNT(s.id) AS song_count
                 FROM albums al
                 JOIN songs s ON s.album_id = al.id
                 GROUP BY al.id, al.title
                 ORDER BY song_count DESC
                 LIMIT 10",
                )
                .expect("top10 albums");
            println!("    rows={}", r.rows().len());
            r
        });
        let top_album_rows = semantic_rows_from_decentdb(&top_albums_result)
            .expect("convert top albums result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_top10_albums_by_songs",
            &top_album_rows,
            scale,
            summary.total_songs,
        );
        drop(top_album_rows);
        drop(top_albums_result);

        let view_result = rec.measure("query_view_first_1000", None, || {
            let r = db
                .execute(
                    "SELECT artist_id, artist_name, album_title, song_title \
                 FROM v_artist_songs LIMIT 1000",
                )
                .expect("view 1000");
            println!("    rows={}", r.rows().len());
            r
        });
        let view_rows = semantic_rows_from_decentdb(&view_result)
            .expect("convert view result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_view_first_1000",
            &view_rows,
            scale,
            summary.total_songs,
        );
        drop(view_rows);
        drop(view_result);

        let filtered_view_result = rec.measure("query_songs_for_artist_via_view", None, || {
            let r = db
                .execute_with_params(
                    "SELECT album_title, song_title, duration_ms \
                 FROM v_artist_songs WHERE artist_id = $1",
                    &[Value::Int64(1)],
                )
                .expect("artist 1 view");
            println!("    rows={}", r.rows().len());
            r
        });
        let filtered_view_rows = semantic_rows_from_decentdb(&filtered_view_result)
            .expect("convert filtered view result to semantic rows");
        add_query_evidence(
            &mut rec,
            "query_songs_for_artist_via_view",
            &filtered_view_rows,
            scale,
            summary.total_songs,
        );
        drop(filtered_view_rows);
        drop(filtered_view_result);

        // Release mutable borrow of report from rec before suite calls.
        peak_rss = rec.peak_rss;
    }

    // Suite modes after queries
    #[cfg(feature = "extended-suites")]
    {
        if cli.latency_suite {
            run_latency_suite_decentdb(&db, &mut report, scale, total_songs, cli)?;
        }
        if cli.concurrency_suite {
            run_concurrency_suite_decentdb(&db, &mut report, scale, cli)?;
        }
        if cli.write_suite {
            run_write_suite_decentdb(&db, &mut report, cli)?;
        }
        if cli.cold_suite {
            let db_path = db_path.clone();
            drop(db);
            run_cold_suite_decentdb(&db_path, &mut report, scale, profile, total_songs, cli)?;
        } else {
            drop(db);
        }
    }
    #[cfg(not(feature = "extended-suites"))]
    drop(db);

    report.peak_rss_bytes = peak_rss;
    if let Ok(meta) = fs::metadata(&db_path) {
        report.database_size_bytes = meta.len();
    }
    report.wal_size_bytes = file_size(&decentdb_wal_path(&db_path));
    report.finished_unix = now_unix();
    report.binding = engine.binding_name().to_string();
    report.scale_name = scale.name.to_string();
    report.benchmark_profile = profile.as_str().to_string();
    report.target_artists = scale.artists;
    report.target_albums = scale.albums;
    report.target_songs_cap = scale.songs_cap;
    report.engine_version = decentdb::version().to_string();
    report.database_path = db_path.display().to_string();
    populate_run_report_metadata(&mut report, engine, profile);
    finalize_runner_provenance(&mut report, seed)?;

    fs::create_dir_all(&out_dir)?;
    let datetime_stamp = format_unix_filename_stamp(report.finished_unix);
    let out_path = out_dir.join(format!(
        "{datetime_stamp}-rust-baseline-{}-{}.json",
        report.benchmark_profile.as_str(),
        scale.name
    ));
    write_json_atomic(&out_path, &serde_json::to_string_pretty(&report)?)?;
    println!("\nWrote {}", out_path.display());

    delete_db_files(&db_path);
    println!("Cleaned up temp DB files: {}", db_path.display());

    Ok(())
}

#[cfg(feature = "sqlite")]
fn run_sqlite_benchmark(
    scale: Scale,
    seed: u64,
    summary: SeedSummary,
    db_path: PathBuf,
    out_dir: PathBuf,
    cli: &Cli,
) -> anyhow::Result<()> {
    delete_db_files(&db_path);

    #[cfg(feature = "extended-suites")]
    let needs_write_events = cli.write_suite || cli.concurrency_suite;
    let needs_wal_normal = matches!(cli.sqlite_profile, Some(SqliteProfile::WalNormal));

    let mut report = RunReport {
        binding: BenchmarkEngine::Sqlite.binding_name().to_string(),
        scale_name: scale.name.to_string(),
        benchmark_profile: if needs_wal_normal {
            "sqlite-wal-normal".to_string()
        } else {
            "sqlite-wal-full".to_string()
        },
        target_artists: scale.artists,
        target_albums: scale.albums,
        target_songs_cap: scale.songs_cap,
        started_unix: now_unix(),
        database_path: db_path.display().to_string(),
        ..Default::default()
    };
    let conn;
    #[cfg(feature = "extended-suites")]
    let database_path;
    let peak_rss;
    {
        let mut rec = Recorder::new(&mut report);

        conn = rec.measure("connect_open", None, || {
            if needs_wal_normal {
                open_sqlite_wal_normal(&db_path).expect("open sqlite wal-normal")
            } else {
                open_sqlite_wal_full(&db_path).expect("open sqlite wal-full")
            }
        });
        rec.report.engine_version =
            sqlite_engine_version(&conn).unwrap_or_else(|_| "unknown".into());

        #[cfg(feature = "extended-suites")]
        let mut ddl_batch = build_schema_ddl_batch();
        #[cfg(feature = "extended-suites")]
        if needs_write_events {
            ddl_batch.push('\n');
            ddl_batch.push_str(write_events_ddl());
            ddl_batch.push(';');
        }
        #[cfg(not(feature = "extended-suites"))]
        let ddl_batch = build_schema_ddl_batch();
        rec.measure("schema_create", None, || {
            conn.execute_batch(&ddl_batch).expect("sqlite ddl batch");
        });

        let mut insert_artist = conn
            .prepare(
                "INSERT INTO artists (id, name, country, formed_year) \
             VALUES (?1, ?2, ?3, ?4)",
            )
            .expect("prepare sqlite artists");
        rec.measure("seed_artists", Some(u64::from(scale.artists)), || {
            seed_sqlite_artists(&conn, &mut insert_artist, scale, seed);
        });
        drop(insert_artist);

        let mut insert_album = conn
            .prepare(
                "INSERT INTO albums (id, artist_id, title, release_year) \
             VALUES (?1, ?2, ?3, ?4)",
            )
            .expect("prepare sqlite albums");
        rec.measure("seed_albums", Some(summary.total_albums), || {
            seed_sqlite_albums(&conn, &mut insert_album, scale, seed);
        });
        drop(insert_album);

        let mut insert_song = conn
            .prepare(
                "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .expect("prepare sqlite songs");
        rec.measure("seed_songs", Some(summary.total_songs), || {
            seed_sqlite_songs(&conn, &mut insert_song, scale, seed);
        });
        drop(insert_song);

        let wal_bytes_before = file_size(&sqlite_wal_path(&db_path));
        let database_bytes_before = file_size(&db_path);
        let (busy, log_frames, checkpointed_frames) =
            rec.measure("checkpoint_after_seed", None, || {
                sqlite_checkpoint_truncate(&conn).expect("sqlite checkpoint after seed")
            });
        let wal_bytes_after = file_size(&sqlite_wal_path(&db_path));
        let database_bytes_after = file_size(&db_path);
        rec.add_extra("checkpoint_mode", serde_json::json!("truncate"));
        rec.add_extra("wal_bytes_before", serde_json::json!(wal_bytes_before));
        rec.add_extra("wal_bytes_after", serde_json::json!(wal_bytes_after));
        rec.add_extra(
            "database_bytes_before",
            serde_json::json!(database_bytes_before),
        );
        rec.add_extra(
            "database_bytes_after",
            serde_json::json!(database_bytes_after),
        );
        rec.add_extra("sqlite_busy", serde_json::json!(busy));
        rec.add_extra("sqlite_log_frames", serde_json::json!(log_frames));
        rec.add_extra(
            "sqlite_checkpointed_frames",
            serde_json::json!(checkpointed_frames),
        );

        let count = rec.measure("query_count_songs", None, || {
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM songs", [], |row| row.get(0))
                .expect("sqlite count");
            println!("    count=Some(Int64({count}))");
            count
        });
        let count_rows = vec![vec![SemanticValue::Int64(count)]];
        add_query_evidence(
            &mut rec,
            "query_count_songs",
            &count_rows,
            scale,
            summary.total_songs,
        );
        drop(count_rows);

        let aggregate = rec.measure("query_aggregate_durations", None, || {
            let row: (i64, i64, f64, i64, i64) = conn
                .query_row(
                    "SELECT COUNT(*), SUM(duration_ms), AVG(duration_ms), \
                        MIN(duration_ms), MAX(duration_ms) FROM songs",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .expect("sqlite aggregate durations");
            println!("    agg_row={row:?}");
            row
        });
        let aggregate_rows = vec![vec![
            SemanticValue::Int64(aggregate.0),
            SemanticValue::Int64(aggregate.1),
            SemanticValue::Float64(aggregate.2),
            SemanticValue::Int64(aggregate.3),
            SemanticValue::Int64(aggregate.4),
        ]];
        add_query_evidence(
            &mut rec,
            "query_aggregate_durations",
            &aggregate_rows,
            scale,
            summary.total_songs,
        );
        drop(aggregate_rows);

        let artist = rec.measure("query_artist_by_id", None, || {
            let target = i64::from(scale.artists) / 2 + 1;
            let row: (i64, String, String, i64) = conn
                .query_row(
                    "SELECT id, name, country, formed_year FROM artists WHERE id = ?1",
                    params![target],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("sqlite artist by id");
            println!("    artist={row:?}");
            row
        });
        let artist_rows = vec![vec![
            SemanticValue::Int64(artist.0),
            SemanticValue::Text(artist.1),
            SemanticValue::Text(artist.2),
            SemanticValue::Int64(artist.3),
        ]];
        add_query_evidence(
            &mut rec,
            "query_artist_by_id",
            &artist_rows,
            scale,
            summary.total_songs,
        );
        drop(artist_rows);

        let top_artists = rec.measure("query_top10_artists_by_songs", None, || {
            let mut statement = conn
                .prepare(
                    "SELECT a.id, a.name, COUNT(s.id) AS song_count
             FROM artists a
             JOIN songs s ON s.artist_id = a.id
             GROUP BY a.id, a.name
             ORDER BY song_count DESC
             LIMIT 10",
                )
                .expect("prepare sqlite top10 artists");
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .expect("query sqlite top10 artists")
                .collect::<rusqlite::Result<Vec<(i64, String, i64)>>>()
                .expect("materialize sqlite top10 artists");
            println!("    rows={}", rows.len());
            rows
        });
        let top_artist_rows = top_artists
            .into_iter()
            .map(|(id, name, count)| {
                vec![
                    SemanticValue::Int64(id),
                    SemanticValue::Text(name),
                    SemanticValue::Int64(count),
                ]
            })
            .collect();
        add_query_evidence(
            &mut rec,
            "query_top10_artists_by_songs",
            &top_artist_rows,
            scale,
            summary.total_songs,
        );
        drop(top_artist_rows);

        let top_albums = rec.measure("query_top10_albums_by_songs", None, || {
            let mut statement = conn
                .prepare(
                    "SELECT al.id, al.title, COUNT(s.id) AS song_count
             FROM albums al
             JOIN songs s ON s.album_id = al.id
             GROUP BY al.id, al.title
             ORDER BY song_count DESC
             LIMIT 10",
                )
                .expect("prepare sqlite top10 albums");
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                .expect("query sqlite top10 albums")
                .collect::<rusqlite::Result<Vec<(i64, String, i64)>>>()
                .expect("materialize sqlite top10 albums");
            println!("    rows={}", rows.len());
            rows
        });
        let top_album_rows = top_albums
            .into_iter()
            .map(|(id, name, count)| {
                vec![
                    SemanticValue::Int64(id),
                    SemanticValue::Text(name),
                    SemanticValue::Int64(count),
                ]
            })
            .collect();
        add_query_evidence(
            &mut rec,
            "query_top10_albums_by_songs",
            &top_album_rows,
            scale,
            summary.total_songs,
        );
        drop(top_album_rows);

        let view_rows = rec.measure("query_view_first_1000", None, || {
            let mut statement = conn
                .prepare(
                    "SELECT artist_id, artist_name, album_title, song_title \
             FROM v_artist_songs LIMIT 1000",
                )
                .expect("prepare sqlite view 1000");
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                })
                .expect("query sqlite view 1000")
                .collect::<rusqlite::Result<Vec<(i64, String, String, String)>>>()
                .expect("materialize sqlite view 1000");
            println!("    rows={}", rows.len());
            rows
        });
        let view_semantic_rows = view_rows
            .into_iter()
            .map(|(artist_id, artist_name, album_title, song_title)| {
                vec![
                    SemanticValue::Int64(artist_id),
                    SemanticValue::Text(artist_name),
                    SemanticValue::Text(album_title),
                    SemanticValue::Text(song_title),
                ]
            })
            .collect();
        add_query_evidence(
            &mut rec,
            "query_view_first_1000",
            &view_semantic_rows,
            scale,
            summary.total_songs,
        );
        drop(view_semantic_rows);

        let filtered_rows = rec.measure("query_songs_for_artist_via_view", None, || {
            let mut statement = conn
                .prepare(
                    "SELECT album_title, song_title, duration_ms \
             FROM v_artist_songs WHERE artist_id = ?1",
                )
                .expect("prepare sqlite artist 1 view");
            let rows = statement
                .query_map(params![1_i64], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .expect("query sqlite artist 1 view")
                .collect::<rusqlite::Result<Vec<(String, String, i64)>>>()
                .expect("materialize sqlite artist 1 view");
            println!("    rows={}", rows.len());
            rows
        });
        let filtered_semantic_rows = filtered_rows
            .into_iter()
            .map(|(album_title, song_title, duration)| {
                vec![
                    SemanticValue::Text(album_title),
                    SemanticValue::Text(song_title),
                    SemanticValue::Int64(duration),
                ]
            })
            .collect();
        add_query_evidence(
            &mut rec,
            "query_songs_for_artist_via_view",
            &filtered_semantic_rows,
            scale,
            summary.total_songs,
        );
        drop(filtered_semantic_rows);

        #[cfg(feature = "extended-suites")]
        {
            database_path = rec.report.database_path.clone();
        }
        peak_rss = rec.peak_rss;
    }

    // Suite modes after queries
    #[cfg(feature = "extended-suites")]
    {
        // The query_count_songs evidence above already asserted COUNT(*) ==
        // summary.total_songs, so reuse the planned value directly.
        let total_songs = summary.total_songs;
        if cli.latency_suite {
            run_latency_suite_sqlite(&conn, &mut report, scale, total_songs, cli)?;
        }
        if cli.concurrency_suite {
            run_concurrency_suite_sqlite(&database_path, &mut report, scale, cli)?;
        }
        if cli.write_suite {
            run_write_suite_sqlite(&conn, &mut report, cli)?;
        }
        if cli.cold_suite {
            let db_clone_path = PathBuf::from(&database_path);
            drop(conn);
            run_cold_suite_sqlite(&db_clone_path, &mut report, scale, total_songs, cli)?;
        } else {
            drop(conn);
        }
    }
    #[cfg(not(feature = "extended-suites"))]
    drop(conn);

    report.peak_rss_bytes = peak_rss;
    if let Ok(meta) = fs::metadata(&db_path) {
        report.database_size_bytes = meta.len();
    }
    report.wal_size_bytes = file_size(&sqlite_wal_path(&db_path));
    report.finished_unix = now_unix();
    populate_run_report_metadata(
        &mut report,
        BenchmarkEngine::Sqlite,
        BenchmarkProfile::Default,
    );
    if needs_wal_normal {
        report.durability_profile = "sqlite_wal_normal".to_string();
    }
    finalize_runner_provenance(&mut report, seed)?;

    fs::create_dir_all(&out_dir)?;
    let datetime_stamp = format_unix_filename_stamp(report.finished_unix);
    let out_path = out_dir.join(format!(
        "{datetime_stamp}-rust-baseline-{}-{}.json",
        report.benchmark_profile.as_str(),
        scale.name
    ));
    write_json_atomic(&out_path, &serde_json::to_string_pretty(&report)?)?;
    println!("\nWrote {}", out_path.display());

    delete_db_files(&db_path);
    println!("Cleaned up temp DB files: {}", db_path.display());

    Ok(())
}

#[cfg(feature = "sqlite")]
fn open_sqlite_wal_full(path: &Path) -> rusqlite::Result<SqliteConnection> {
    let conn = SqliteConnection::open(path)?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode=WAL;", [], |row| row.get(0))?;
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    conn.execute_batch(
        "PRAGMA synchronous=FULL;
         PRAGMA wal_autocheckpoint=0;",
    )?;
    let synchronous: i64 = conn.query_row("PRAGMA synchronous;", [], |row| row.get(0))?;
    assert_eq!(synchronous, 2, "expected SQLite synchronous=FULL");
    let wal_autocheckpoint: i64 =
        conn.query_row("PRAGMA wal_autocheckpoint;", [], |row| row.get(0))?;
    assert_eq!(
        wal_autocheckpoint, 0,
        "expected SQLite wal_autocheckpoint=0"
    );
    Ok(conn)
}

#[cfg(feature = "sqlite")]
fn sqlite_engine_version(conn: &SqliteConnection) -> rusqlite::Result<String> {
    conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))
}

#[cfg(feature = "sqlite")]
fn sqlite_checkpoint_truncate(conn: &SqliteConnection) -> rusqlite::Result<(i64, i64, i64)> {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "extended-suites")]
fn sqlite_query_row_count<P: rusqlite::Params>(
    conn: &SqliteConnection,
    sql: &str,
    params: P,
) -> rusqlite::Result<usize> {
    let mut stmt = conn.prepare(sql)?;
    let column_count = stmt.column_count();
    let mut rows = stmt.query(params)?;
    let mut count = 0usize;
    while let Some(row) = rows.next()? {
        for index in 0..column_count {
            let _: rusqlite::types::Value = row.get(index)?;
        }
        count += 1;
    }
    Ok(count)
}

#[cfg(feature = "sqlite")]
fn sqlite_wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    PathBuf::from(wal)
}

#[cfg(feature = "sqlite")]
fn seed_sqlite_artists(
    conn: &SqliteConnection,
    stmt: &mut rusqlite::Statement<'_>,
    scale: Scale,
    seed: u64,
) {
    conn.execute_batch("BEGIN IMMEDIATE;")
        .expect("begin sqlite artists");
    let mut artist_name = String::with_capacity(32);
    walk_seed_plan_select(
        scale,
        seed,
        SeedWalkEmit::ARTISTS,
        |a| {
            artist_name.clear();
            artist_name.push_str("Artist ");
            write!(&mut artist_name, "{}", a.id).expect("write artist name");
            stmt.execute(params![
                a.id,
                artist_name.as_str(),
                a.country,
                a.formed_year
            ])
            .expect("sqlite insert artist");
        },
        |_| {},
        |_| {},
    );
    conn.execute_batch("COMMIT;")
        .expect("commit sqlite artists");
}

#[cfg(feature = "sqlite")]
fn seed_sqlite_albums(
    conn: &SqliteConnection,
    stmt: &mut rusqlite::Statement<'_>,
    scale: Scale,
    seed: u64,
) {
    conn.execute_batch("BEGIN IMMEDIATE;")
        .expect("begin sqlite albums");
    let mut album_title = String::with_capacity(32);
    walk_seed_plan_select(
        scale,
        seed,
        SeedWalkEmit::ALBUMS,
        |_| {},
        |al| {
            album_title.clear();
            album_title.push_str("Album ");
            write!(&mut album_title, "{}", al.id).expect("write album title");
            stmt.execute(params![
                al.id,
                al.artist_id,
                album_title.as_str(),
                al.release_year
            ])
            .expect("sqlite insert album");
        },
        |_| {},
    );
    conn.execute_batch("COMMIT;").expect("commit sqlite albums");
}

#[cfg(feature = "sqlite")]
fn seed_sqlite_songs(
    conn: &SqliteConnection,
    stmt: &mut rusqlite::Statement<'_>,
    scale: Scale,
    seed: u64,
) {
    conn.execute_batch("BEGIN IMMEDIATE;")
        .expect("begin sqlite songs");
    let mut song_title = String::with_capacity(32);
    walk_seed_plan_select(
        scale,
        seed,
        SeedWalkEmit::SONGS,
        |_| {},
        |_| {},
        |s| {
            song_title.clear();
            song_title.push_str("Song ");
            write!(&mut song_title, "{}", s.id).expect("write song title");
            stmt.execute(params![
                s.id,
                s.album_id,
                s.artist_id,
                song_title.as_str(),
                s.duration_ms
            ])
            .expect("sqlite insert song");
        },
    );
    conn.execute_batch("COMMIT;").expect("commit sqlite songs");
}

fn seed_albums(db: &decentdb::Db, prepared: &PreparedStatement, scale: Scale, seed: u64) {
    let mut txn = db.transaction().expect("begin albums");
    let params: &mut [Value] = &mut [
        Value::Int64(0),
        Value::Int64(0),
        Value::Text(String::new()),
        Value::Int64(0),
    ];
    let mut album_title = String::with_capacity(32);
    {
        let mut batch = txn
            .prepared_batch(prepared, params.len())
            .expect("prepare album batch");
        walk_seed_plan_select(
            scale,
            seed,
            SeedWalkEmit::ALBUMS,
            |_| {},
            |al| {
                params[0] = Value::Int64(al.id);
                params[1] = Value::Int64(al.artist_id);
                album_title.clear();
                album_title.push_str("Album ");
                write!(&mut album_title, "{}", al.id).expect("write album title");
                params[2] = Value::Text(album_title.clone());
                params[3] = Value::Int64(al.release_year as i64);
                batch.execute_mut(params).expect("ins album");
            },
            |_| {},
        );
    }
    txn.commit().expect("commit albums");
}

fn seed_songs(db: &decentdb::Db, prepared: &PreparedStatement, scale: Scale, seed: u64) {
    let mut txn = db.transaction().expect("begin songs");
    let params: &mut [Value] = &mut [
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Text(String::new()),
        Value::Int64(0),
    ];
    let mut song_title = String::with_capacity(32);
    {
        let mut batch = txn
            .prepared_batch(prepared, params.len())
            .expect("prepare song batch");
        walk_seed_plan_select(
            scale,
            seed,
            SeedWalkEmit::SONGS,
            |_| {},
            |_| {},
            |s| {
                params[0] = Value::Int64(s.id);
                params[1] = Value::Int64(s.album_id);
                params[2] = Value::Int64(s.artist_id);
                song_title.clear();
                song_title.push_str("Song ");
                write!(&mut song_title, "{}", s.id).expect("write song title");
                params[3] = Value::Text(song_title.clone());
                params[4] = Value::Int64(s.duration_ms as i64);
                batch.execute_mut(params).expect("ins song");
            },
        );
    }
    txn.commit().expect("commit songs");
}

// --- helpers ---------------------------------------------------------------
fn build_schema_ddl_batch() -> String {
    let mut sql = String::from("BEGIN;\n");
    for stmt in DDL {
        sql.push_str(stmt);
        sql.push_str(";\n");
    }
    sql.push_str("COMMIT;");
    sql
}

fn first_value(r: &decentdb::QueryResult) -> Option<Value> {
    r.rows()
        .first()
        .and_then(|row| row.values().first().cloned())
}
#[cfg(feature = "extended-suites")]
fn scalar_int(r: &decentdb::QueryResult) -> i64 {
    match first_value(r) {
        Some(Value::Int64(i)) => i,
        _ => 0,
    }
}

#[cfg(feature = "extended-suites")]
fn generate_html_report(results_dir: &Path, report_file: &Path) -> anyhow::Result<()> {
    let data = load_report_data(results_dir)?;
    let html = build_report_html(&data)?;

    if let Some(parent) = report_file.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create report output directory {}",
                parent.display()
            )
        })?;
    }
    fs::write(report_file, html)
        .with_context(|| format!("failed to write {}", report_file.display()))?;
    Ok(())
}

#[cfg(feature = "extended-suites")]
struct ReportInput {
    file_name: String,
    path: PathBuf,
    json: Vec<u8>,
    /// True when the file is not pinned by `history-manifest.json`; it is
    /// loaded for local comparison but is not part of the verified history.
    provisional: bool,
}

#[cfg(feature = "extended-suites")]
fn load_report_data(results_dir: &Path) -> anyhow::Result<HtmlReportData> {
    let has_manifest = results_dir.join("history-manifest.json").exists();
    let mut runs = Vec::new();
    let mut skipped_inputs = Vec::new();
    for input in report_inputs(results_dir)? {
        let report: RunReport = match serde_json::from_slice(&input.json) {
            Ok(report) => report,
            Err(error) if input.provisional => {
                // A local result that does not match the run schema (for
                // example a truncated write or a plan-cache report) must not
                // take down the whole report; only checksum-verified history
                // is strict.
                eprintln!(
                    "skipping unreadable rust-baseline result {}: {error}",
                    input.path.display()
                );
                skipped_inputs.push(input.file_name);
                continue;
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to parse {}", input.path.display()));
            }
        };
        runs.push(HistoricalRun::new(
            input.file_name,
            input.provisional,
            report,
        ));
    }

    if runs.is_empty() {
        bail!(
            "no rust-baseline JSON files found in {}",
            results_dir.display()
        );
    }

    runs.sort_by(|left, right| {
        scale_rank(left.report.scale_name.as_str())
            .cmp(&scale_rank(right.report.scale_name.as_str()))
            .then(left.timestamp_unix.cmp(&right.timestamp_unix))
            .then(left.file_name.cmp(&right.file_name))
    });

    let verified_runs = runs.iter().filter(|run| !run.provisional).count();
    let provisional_runs = runs.len() - verified_runs;
    let mut inputs_summary = if has_manifest {
        let mut summary = format!("{verified_runs} checksum-verified historical run(s)");
        if provisional_runs > 0 {
            summary.push_str(&format!(
                " and {provisional_runs} provisional local run(s) not pinned by history-manifest.json"
            ));
        }
        summary
    } else {
        format!("{} historical run(s)", runs.len())
    };
    if !skipped_inputs.is_empty() {
        inputs_summary.push_str(&format!(
            "; skipped {} unreadable file(s): {}",
            skipped_inputs.len(),
            skipped_inputs.join(", ")
        ));
    }

    let mut sections = Vec::with_capacity(4);
    for scale_name in ["smoke", "medium", "full", "huge"] {
        let mut scale_runs = runs
            .iter()
            .filter(|run| run.report.scale_name == scale_name)
            .cloned()
            .collect::<Vec<_>>();
        scale_runs.sort_by(|left, right| {
            left.timestamp_unix
                .cmp(&right.timestamp_unix)
                .then(left.file_name.cmp(&right.file_name))
        });
        let step_names = ordered_step_names(&scale_runs);
        sections.push(ReportScaleSection {
            scale_name: scale_name.to_string(),
            step_names,
            runs: scale_runs,
        });
    }

    Ok(HtmlReportData {
        generated_at_unix: now_unix(),
        generated_at_label: format_unix_label(now_unix()),
        source_directory: results_dir.display().to_string(),
        total_runs: runs.len(),
        verified_runs,
        provisional_runs,
        has_manifest,
        skipped_inputs,
        inputs_summary,
        scales: sections,
    })
}

#[cfg(feature = "extended-suites")]
fn report_inputs(results_dir: &Path) -> anyhow::Result<Vec<ReportInput>> {
    let manifest_path = results_dir.join("history-manifest.json");
    if manifest_path.exists() {
        let manifest_json = fs::read(&manifest_path)
            .with_context(|| format!("failed to read {}", manifest_path.display()))?;
        let manifest: ReportHistoryManifest = serde_json::from_slice(&manifest_json)
            .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
        if manifest.schema_version != 1 {
            bail!(
                "unsupported history manifest schema version {} in {}",
                manifest.schema_version,
                manifest_path.display()
            );
        }
        if manifest.files.is_empty() {
            bail!("history manifest {} is empty", manifest_path.display());
        }

        let mut seen = HashSet::with_capacity(manifest.files.len());
        let mut inputs = Vec::with_capacity(manifest.files.len());
        for entry in manifest.files {
            let relative_path = Path::new(&entry.path);
            let mut components = relative_path.components();
            let is_plain_file_name =
                matches!(components.next(), Some(std::path::Component::Normal(_)))
                    && components.next().is_none();
            if !is_plain_file_name
                || relative_path
                    .extension()
                    .and_then(|extension| extension.to_str())
                    != Some("json")
                || !entry.path.contains("rust-baseline")
            {
                bail!(
                    "invalid history manifest result path {:?} in {}",
                    entry.path,
                    manifest_path.display()
                );
            }
            if !seen.insert(entry.path.clone()) {
                bail!(
                    "duplicate history manifest result path {:?} in {}",
                    entry.path,
                    manifest_path.display()
                );
            }
            if entry.sha256.len() != 64
                || !entry.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                bail!(
                    "invalid SHA-256 for {:?} in {}",
                    entry.path,
                    manifest_path.display()
                );
            }

            let path = results_dir.join(relative_path);
            let json = fs::read(&path)
                .with_context(|| format!("failed to read manifested result {}", path.display()))?;
            let actual_sha256 = lower_hex(&Sha256::digest(&json));
            if actual_sha256 != entry.sha256.to_ascii_lowercase() {
                bail!(
                    "SHA-256 mismatch for manifested result {}: expected {}, found {}",
                    path.display(),
                    entry.sha256,
                    actual_sha256
                );
            }
            inputs.push(ReportInput {
                file_name: entry.path,
                path,
                json,
                provisional: false,
            });
        }

        // The manifest pins the checksum-verified canonical history, but a
        // local `--benchmark` run writes fresh results that are not pinned
        // yet. Merge those as provisional inputs so the report reflects the
        // latest local work instead of silently dropping it.
        for input in scan_results_dir(results_dir)? {
            if seen.contains(&input.file_name) {
                continue;
            }
            inputs.push(ReportInput {
                provisional: true,
                ..input
            });
        }
        return Ok(inputs);
    }

    scan_results_dir(results_dir)
}

#[cfg(feature = "extended-suites")]
fn scan_results_dir(results_dir: &Path) -> anyhow::Result<Vec<ReportInput>> {
    let dir = fs::read_dir(results_dir)
        .with_context(|| format!("failed to read results directory {}", results_dir.display()))?;
    let mut inputs = Vec::new();
    for entry in dir {
        let entry = entry
            .with_context(|| format!("failed to read an entry in {}", results_dir.display()))?;
        let path = entry.path();
        if !path.is_file()
            || path.extension().and_then(|extension| extension.to_str()) != Some("json")
        {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if !file_name.contains("rust-baseline") {
            continue;
        }
        let json =
            fs::read(&path).with_context(|| format!("failed to read result {}", path.display()))?;
        inputs.push(ReportInput {
            file_name,
            path,
            json,
            provisional: false,
        });
    }
    Ok(inputs)
}

#[cfg(feature = "extended-suites")]
impl HistoricalRun {
    fn new(file_name: String, provisional: bool, report: RunReport) -> Self {
        let timestamp_unix = report.started_unix;
        let total_runtime_seconds = report.steps.iter().map(|step| step.duration_seconds).sum();
        Self {
            file_name,
            timestamp_unix,
            timestamp_label: format_unix_label(timestamp_unix),
            total_runtime_seconds,
            provisional,
            report,
        }
    }
}

#[cfg(feature = "extended-suites")]
fn ordered_step_names(runs: &[HistoricalRun]) -> Vec<String> {
    const KNOWN_ORDER: &[&str] = &[
        "connect_open",
        "schema_create",
        "seed_artists",
        "seed_albums",
        "seed_songs",
        "checkpoint_after_seed",
        "query_count_songs",
        "query_aggregate_durations",
        "query_artist_by_id",
        "query_top10_artists_by_songs",
        "query_top10_albums_by_songs",
        "query_view_first_1000",
        "query_songs_for_artist_via_view",
    ];

    let mut names = Vec::new();
    for known in KNOWN_ORDER {
        if runs
            .iter()
            .any(|run| run.report.steps.iter().any(|step| step.name == *known))
        {
            names.push((*known).to_string());
        }
    }

    for run in runs {
        for step in &run.report.steps {
            if !names.iter().any(|name| name == &step.name) {
                names.push(step.name.clone());
            }
        }
    }
    names
}

#[cfg(feature = "extended-suites")]
fn scale_rank(name: &str) -> usize {
    match name {
        "smoke" => 0,
        "medium" => 1,
        "full" => 2,
        "huge" => 3,
        _ => usize::MAX,
    }
}

#[cfg(feature = "extended-suites")]
fn format_unix_label(unix: u64) -> String {
    let (year, month, day, hour, minute, second) = unix_utc_parts(unix);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02} UTC")
}

fn format_unix_filename_stamp(unix: u64) -> String {
    let (year, month, day, hour, minute, _) = unix_utc_parts(unix);
    format!("{year:04}-{month:02}-{day:02}-{hour:02}{minute:02}")
}

fn unix_utc_parts(unix: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (unix / 86_400) as i64;
    let seconds_of_day = unix % 86_400;
    let (year, month, day) = civil_from_unix_days(days);
    let hour = (seconds_of_day / 3_600) as u32;
    let minute = ((seconds_of_day % 3_600) / 60) as u32;
    let second = (seconds_of_day % 60) as u32;
    (year, month, day, hour, minute, second)
}

fn civil_from_unix_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year as i32, month as u32, day as u32)
}

#[cfg(feature = "extended-suites")]
fn build_report_html(data: &HtmlReportData) -> anyhow::Result<String> {
    let report_json = safe_json_for_html(serde_json::to_string(data)?);
    let html = format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DecentDB rust-baseline report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0f172a;
      --panel: #111827;
      --panel-alt: #1f2937;
      --border: #334155;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --good: #22c55e;
      --bad: #ef4444;
      --accent: #38bdf8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.45;
    }}
    header, main {{
      width: min(1500px, calc(100vw - 32px));
      margin: 0 auto;
    }}
    header {{
      padding: 32px 0 20px;
    }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    h2 {{ margin-top: 28px; }}
    p.meta {{ color: var(--muted); margin: 0; }}
    .overview-grid, .summary-grid, .chart-grid {{
      display: grid;
      gap: 16px;
    }}
    .overview-grid {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin: 20px 0 28px;
    }}
    .summary-grid {{
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      margin-bottom: 18px;
    }}
    .chart-grid {{
      grid-template-columns: repeat(auto-fit, minmax(420px, 1fr));
      margin: 16px 0 24px;
    }}
    .card, .chart-card, .table-wrap, section {{
      background: rgba(17, 24, 39, 0.88);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: 0 8px 30px rgba(0, 0, 0, 0.25);
    }}
    .card, .chart-card {{
      padding: 18px;
    }}
    .chart-card {{
      overflow: hidden;
    }}
    .card .label {{
      color: var(--muted);
      font-size: 0.85rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .card .value {{
      font-size: 1.6rem;
      font-weight: 700;
      margin-top: 6px;
    }}
    .card .sub {{
      color: var(--muted);
      font-size: 0.9rem;
      margin-top: 6px;
    }}
    section {{
      padding: 22px;
      margin-bottom: 24px;
    }}
    .section-meta {{
      color: var(--muted);
      margin-bottom: 12px;
    }}
    .table-wrap {{
      overflow-x: auto;
      margin: 16px 0 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      min-width: 760px;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid rgba(148, 163, 184, 0.18);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      position: sticky;
      top: 0;
      background: var(--panel-alt);
      color: #f8fafc;
      font-size: 0.86rem;
    }}
    td.mono, th.mono {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.9rem;
    }}
    .muted {{ color: var(--muted); }}
    .good {{ color: var(--good); }}
    .bad {{ color: var(--bad); }}
    .chart-frame {{
      position: relative;
      height: clamp(260px, 34vh, 360px);
      max-height: 360px;
      margin-top: 12px;
    }}
    .chart-card canvas {{
      width: 100%;
      height: 100%;
    }}
    .empty {{
      color: var(--muted);
      font-style: italic;
      margin-top: 8px;
    }}
    code {{
      background: rgba(148, 163, 184, 0.12);
      border-radius: 6px;
      padding: 1px 5px;
    }}
  </style>
</head>
<body>
  <header>
    <h1>DecentDB rust-baseline analytics report</h1>
    <p class="meta">Generated {generated_at} from <code>{source_directory}</code> using {inputs_summary}.</p>
  </header>
  <main>
    <div id="overview"></div>
    <div id="scales"></div>
  </main>

  <script id="report-data" type="application/json">{report_json}</script>
  <script>
    const reportData = JSON.parse(document.getElementById('report-data').textContent);
    const chartPalette = ['#38bdf8', '#a78bfa', '#22c55e', '#f59e0b', '#ef4444', '#14b8a6', '#f472b6', '#eab308', '#60a5fa', '#c084fc', '#fb7185', '#34d399'];

    function formatBytes(bytes) {{
      if (bytes == null) return '—';
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let value = Number(bytes);
      let index = 0;
      while (value >= 1024 && index < units.length - 1) {{
        value /= 1024;
        index++;
      }}
      return `${{value.toFixed(value >= 100 || index === 0 ? 0 : 1)}}${{units[index]}}`;
    }}

    function formatSeconds(value) {{
      if (value == null || Number.isNaN(value)) return '—';
      return `${{Number(value).toFixed(3)}} s`;
    }}

    function formatRps(value) {{
      if (value == null || Number.isNaN(value)) return '—';
      return `${{Math.round(Number(value)).toLocaleString()}} r/s`;
    }}

    function getStep(run, stepName) {{
      return run.report.steps.find(step => step.name === stepName) || null;
    }}

    function formatNs(value) {{
      if (value == null || Number.isNaN(value)) return '—';
      const ns = Number(value);
      if (ns >= 1_000_000_000) return `${{(ns / 1_000_000_000).toFixed(3)}} s`;
      if (ns >= 1_000_000) return `${{(ns / 1_000_000).toFixed(3)}} ms`;
      if (ns >= 1_000) return `${{(ns / 1_000).toFixed(3)}} μs`;
      return `${{ns.toFixed(1)}} ns`;
    }}

    function ratioText(childValue, parentValue, lowerIsBetter, withDirection = false) {{
      if (!childValue || !parentValue || !Number.isFinite(childValue) || !Number.isFinite(parentValue)) {{
        return '—';
      }}
      const ratio = childValue / parentValue;
      if (!Number.isFinite(ratio)) return '—';
      const direction = lowerIsBetter ? ' (lower is better)' : ' (higher is better)';
      const badge = lowerIsBetter
        ? (ratio <= 1.0 ? 'good' : 'bad')
        : (ratio >= 1.0 ? 'good' : 'bad');
      return withDirection ? `${{ratio.toFixed(2)}}x${{badge === 'good' ? '' : ' !'}}${{direction}}` : `${{ratio.toFixed(2)}}x`;
    }}

    function toArray(value) {{
      return Array.isArray(value) ? value : [];
    }}

    function metricProfile(run) {{
      return run.report.benchmark_profile || 'default';
    }}

    function metricBinding(run) {{
      return run.report.binding || 'Unknown';
    }}

    function engineSortIndex(binding) {{
      if (binding === 'RustRaw') return 0;
      if (binding === 'SQLiteRusqlite') return 1;
      if (binding === 'DuckDbRs') return 2;
      return 99;
    }}

    function latestRunByProfileBinding(runs) {{
      const latest = new Map();
      for (const run of runs) {{
        const key = `${{metricProfile(run)}}|${{metricBinding(run)}}`;
        const existing = latest.get(key);
        if (!existing || run.timestamp_unix > existing.timestamp_unix) {{
          latest.set(key, run);
        }}
      }}
      return latest;
    }}

    function addEmptyMessage(parent, message) {{
      const empty = document.createElement('p');
      empty.className = 'empty';
      empty.textContent = message;
      parent.appendChild(empty);
    }}

    function maxStepNumeric(run, fieldName, fallback = 0) {{
      let max = fallback;
      const steps = run.report.steps || [];
      for (const step of steps) {{
        const candidate = step[fieldName];
        if (typeof candidate === 'number' && Number.isFinite(candidate)) {{
          if (candidate > max) {{
            max = candidate;
          }}
        }}
      }}
      return max;
    }}

    function rowsForCaseField(runsByProfileBinding, fieldName) {{
      const rows = [];
      for (const [key, run] of runsByProfileBinding) {{
        const [profile] = key.split('|');
        const cases = toArray(run.report[fieldName]);
        for (const entry of cases) {{
          rows.push({{
            profile,
            binding: metricBinding(run),
            file: run.file_name,
            entry,
          }});
        }}
      }}
      rows.sort((left, right) => {{
        const profileCmp = left.profile.localeCompare(right.profile);
        if (profileCmp !== 0) return profileCmp;
        const bindingCmp = left.binding.localeCompare(right.binding);
        if (bindingCmp !== 0) return bindingCmp;
        return left.entry.name.localeCompare(right.entry.name);
      }});
      return rows;
    }}

    function renderCaseRowsTable(section, title, rows, columns) {{
      const wrap = document.createElement('div');
      wrap.className = 'table-wrap';

      section.appendChild(document.createElement('h3')).textContent = title;
      if (!rows.length) {{
        addEmptyMessage(wrap, 'No rows for this run mode.');
        section.appendChild(wrap);
        return;
      }}

      const header = ['Profile', 'Binding', 'Run file']
        .concat(columns.map(column => column.label));

      const table = document.createElement('table');
      table.innerHTML = `
        <thead>
          <tr>
            ${{
              header.map(label => `<th>${{label}}</th>`).join('')
            }}
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const body = table.querySelector('tbody');

      for (const row of rows) {{
        const tr = document.createElement('tr');
        const cells = `
          <td class="mono">${{row.profile}}</td>
          <td class="mono">${{row.binding}}</td>
          <td class="mono">${{row.file}}</td>
        `;
        for (const column of columns) {{
          const value = column.format(row.entry);
          cells += `<td>${{value}}</td>`;
        }}
        tr.innerHTML = cells;
        body.appendChild(tr);
      }}

      wrap.appendChild(table);
      section.appendChild(wrap);
    }}

    function trendClass(latest, best, lowerIsBetter) {{
      if (latest == null || best == null) return 'muted';
      if (latest === best) return 'good';
      return lowerIsBetter ? (latest > best ? 'bad' : 'good') : (latest < best ? 'bad' : 'good');
    }}

    function makeCard(label, value, sub, extraClass = '') {{
      const card = document.createElement('div');
      card.className = `card ${{extraClass}}`;
      card.innerHTML = `<div class="label">${{label}}</div><div class="value">${{value}}</div><div class="sub">${{sub}}</div>`;
      return card;
    }}

    function renderOverview() {{
      const container = document.getElementById('overview');
      const title = document.createElement('h2');
      title.textContent = 'Overview';
      container.appendChild(title);

      const meta = document.createElement('p');
      meta.className = 'section-meta';
      meta.textContent = 'Historical benchmark runs grouped by scale. Charts show both improvements and regressions over time.';
      container.appendChild(meta);

      const grid = document.createElement('div');
      grid.className = 'overview-grid';

      for (const scale of reportData.scales) {{
        const latest = scale.runs.at(-1);
        if (!latest) {{
          grid.appendChild(makeCard(scale.scale_name, '0 runs', 'No data yet'));
          continue;
        }}
        const latestSeedSongs = getStep(latest, 'seed_songs');
        grid.appendChild(makeCard(
          scale.scale_name,
          `${{scale.runs.length}} run(s)`,
          `Latest seed_songs: ${{formatRps(latestSeedSongs?.records_per_second)}}`
        ));
      }}

      container.appendChild(grid);
    }}

    function createLineChart(canvas, labels, datasets, yTickFormatter) {{
      return new Chart(canvas, {{
        type: 'line',
        data: {{ labels, datasets }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          interaction: {{ mode: 'nearest', intersect: false }},
          scales: {{
            y: {{
              ticks: {{
                color: '#cbd5e1',
                callback: value => yTickFormatter(value),
              }},
              grid: {{ color: 'rgba(148, 163, 184, 0.15)' }},
            }},
            x: {{
              ticks: {{ color: '#cbd5e1' }},
              grid: {{ color: 'rgba(148, 163, 184, 0.10)' }},
            }},
          }},
          plugins: {{
            legend: {{
              labels: {{ color: '#e5e7eb', boxWidth: 14 }},
            }},
            tooltip: {{
              callbacks: {{
                label: context => `${{context.dataset.label}}: ${{yTickFormatter(context.parsed.y)}}`,
              }},
            }},
          }},
        }},
      }});
    }}

    function renderScale(scale) {{
      const host = document.getElementById('scales');
      const section = document.createElement('section');
      const title = document.createElement('h2');
      title.textContent = scale.scale_name;
      section.appendChild(title);

      if (scale.runs.length === 0) {{
        const empty = document.createElement('p');
        empty.className = 'empty';
        empty.textContent = 'No historical runs found for this scale.';
        section.appendChild(empty);
        host.appendChild(section);
        return;
      }}

      const latest = scale.runs.at(-1);
      const latestSeedSongs = getStep(latest, 'seed_songs');
      const bestSeedSongs = Math.max(...scale.runs.map(run => getStep(run, 'seed_songs')?.records_per_second || 0));
      const bestRuntime = Math.min(...scale.runs.map(run => run.total_runtime_seconds));
      const bestPeakRss = Math.min(...scale.runs.map(run => run.report.peak_rss_bytes));

      const provisionalCount = scale.runs.filter(run => run.provisional).length;
      const meta = document.createElement('p');
      meta.className = 'section-meta';
      meta.textContent = `Runs: ${{scale.runs.length}} · Latest: ${{latest.timestamp_label}}${{provisionalCount ? ` · ${{provisionalCount}} provisional` : ''}}`;
      section.appendChild(meta);

      const summary = document.createElement('div');
      summary.className = 'summary-grid';
      summary.appendChild(makeCard('Latest total runtime', formatSeconds(latest.total_runtime_seconds), `Best observed: ${{formatSeconds(bestRuntime)}}`, trendClass(latest.total_runtime_seconds, bestRuntime, true)));
      summary.appendChild(makeCard('Latest peak RSS', formatBytes(latest.report.peak_rss_bytes), `Best observed: ${{formatBytes(bestPeakRss)}}`, trendClass(latest.report.peak_rss_bytes, bestPeakRss, true)));
      summary.appendChild(makeCard('Latest seed_songs throughput', formatRps(latestSeedSongs?.records_per_second), `Best observed: ${{formatRps(bestSeedSongs)}}`, trendClass(latestSeedSongs?.records_per_second, bestSeedSongs, false)));
      summary.appendChild(makeCard('Latest DB size', formatBytes(latest.report.database_size_bytes), `WAL: ${{formatBytes(latest.report.wal_size_bytes)}}`));
      section.appendChild(summary);

      const chartGrid = document.createElement('div');
      chartGrid.className = 'chart-grid';

      const labels = scale.runs.map(run => run.timestamp_label);

      const runtimeCard = document.createElement('div');
      runtimeCard.className = 'chart-card';
      runtimeCard.innerHTML = '<h3>Total runtime over time</h3><div class="chart-frame"><canvas></canvas></div>';
      chartGrid.appendChild(runtimeCard);
      createLineChart(runtimeCard.querySelector('canvas'), labels, [{{
        label: 'Total runtime',
        data: scale.runs.map(run => run.total_runtime_seconds),
        borderColor: chartPalette[0],
        backgroundColor: chartPalette[0],
        tension: 0.2,
      }}], value => formatSeconds(value));

      const rssCard = document.createElement('div');
      rssCard.className = 'chart-card';
      rssCard.innerHTML = '<h3>Peak RSS over time</h3><div class="chart-frame"><canvas></canvas></div>';
      chartGrid.appendChild(rssCard);
      createLineChart(rssCard.querySelector('canvas'), labels, [{{
        label: 'Peak RSS',
        data: scale.runs.map(run => run.report.peak_rss_bytes),
        borderColor: chartPalette[1],
        backgroundColor: chartPalette[1],
        tension: 0.2,
      }}], value => formatBytes(value));

      const durationCard = document.createElement('div');
      durationCard.className = 'chart-card';
      durationCard.innerHTML = '<h3>Step durations over time</h3><div class="chart-frame"><canvas></canvas></div>';
      chartGrid.appendChild(durationCard);
      createLineChart(durationCard.querySelector('canvas'), labels, scale.step_names.map((stepName, index) => ({{
        label: stepName,
        data: scale.runs.map(run => getStep(run, stepName)?.duration_seconds ?? null),
        borderColor: chartPalette[index % chartPalette.length],
        backgroundColor: chartPalette[index % chartPalette.length],
        tension: 0.2,
        spanGaps: true,
      }})), value => formatSeconds(value));

      const throughputCard = document.createElement('div');
      throughputCard.className = 'chart-card';
      throughputCard.innerHTML = '<h3>Seed throughput over time</h3><div class="chart-frame"><canvas></canvas></div>';
      chartGrid.appendChild(throughputCard);
      createLineChart(throughputCard.querySelector('canvas'), labels, ['seed_artists', 'seed_albums', 'seed_songs'].map((stepName, index) => ({{
        label: stepName,
        data: scale.runs.map(run => getStep(run, stepName)?.records_per_second ?? null),
        borderColor: chartPalette[(index + 4) % chartPalette.length],
        backgroundColor: chartPalette[(index + 4) % chartPalette.length],
        tension: 0.2,
        spanGaps: true,
      }})), value => formatRps(value));

      section.appendChild(chartGrid);

      const runTableWrap = document.createElement('div');
      runTableWrap.className = 'table-wrap';
      const runTable = document.createElement('table');
      const sourceHeader = reportData.has_manifest ? '<th>Source</th>' : '';
      runTable.innerHTML = `
        <thead>
          <tr>
            <th>Run</th>
            <th>Started</th>
            <th>Engine</th>
            <th>Total runtime</th>
            <th>Peak RSS</th>
            <th>DB size</th>
            <th>WAL size</th>
            ${{sourceHeader}}
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const runBody = runTable.querySelector('tbody');
      for (const run of scale.runs) {{
        const sourceCell = reportData.has_manifest
          ? `<td>${{run.provisional ? 'provisional' : 'verified'}}</td>`
          : '';
        const row = document.createElement('tr');
        row.innerHTML = `
          <td class="mono">${{run.file_name}}</td>
          <td>${{run.timestamp_label}}</td>
          <td>${{run.report.engine_version}}</td>
          <td>${{formatSeconds(run.total_runtime_seconds)}}</td>
          <td>${{formatBytes(run.report.peak_rss_bytes)}}</td>
          <td>${{formatBytes(run.report.database_size_bytes)}}</td>
          <td>${{formatBytes(run.report.wal_size_bytes)}}</td>
          ${{sourceCell}}
        `;
        runBody.appendChild(row);
      }}
      runTableWrap.appendChild(runTable);
      section.appendChild(document.createElement('h3')).textContent = 'Run history';
      section.appendChild(runTableWrap);

      const stepSummaryWrap = document.createElement('div');
      stepSummaryWrap.className = 'table-wrap';
      const stepSummary = document.createElement('table');
      stepSummary.innerHTML = `
        <thead>
          <tr>
            <th>Step</th>
            <th>Latest duration</th>
            <th>Best duration</th>
            <th>Latest throughput</th>
            <th>Best throughput</th>
            <th>Latest RSS</th>
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const stepBody = stepSummary.querySelector('tbody');
      for (const stepName of scale.step_names) {{
        const metrics = scale.runs.map(run => getStep(run, stepName)).filter(Boolean);
        if (!metrics.length) continue;
        const latestMetric = getStep(latest, stepName);
        const bestDuration = Math.min(...metrics.map(metric => metric.duration_seconds));
        const throughputMetrics = metrics.map(metric => metric.records_per_second).filter(value => value != null);
        const bestThroughput = throughputMetrics.length ? Math.max(...throughputMetrics) : null;
        const row = document.createElement('tr');
        row.innerHTML = `
          <td class="mono">${{stepName}}</td>
          <td>${{formatSeconds(latestMetric?.duration_seconds)}}</td>
          <td>${{formatSeconds(bestDuration)}}</td>
          <td>${{formatRps(latestMetric?.records_per_second)}}</td>
          <td>${{formatRps(bestThroughput)}}</td>
          <td>${{formatBytes(latestMetric?.rss_bytes)}}</td>
        `;
        stepBody.appendChild(row);
      }}
      stepSummaryWrap.appendChild(stepSummary);
      section.appendChild(document.createElement('h3')).textContent = 'Step summary';
      section.appendChild(stepSummaryWrap);

      const profileBindingRuns = latestRunByProfileBinding(scale.runs);
      const profileMap = new Map();
      for (const [key, profileRun] of profileBindingRuns) {{
        const [profile, binding] = key.split('|');
        const profileRuns = profileMap.get(profile) || new Map();
        profileRuns.set(binding, profileRun);
        profileMap.set(profile, profileRuns);
      }}

      for (const [profile, runsByBinding] of profileMap.entries()) {{
        const bindings = [...runsByBinding.keys()].sort((a, b) => {{
          const compare = engineSortIndex(a) - engineSortIndex(b);
          if (compare !== 0) return compare;
          return a.localeCompare(b);
        }});
        if (bindings.length < 2) {{
          continue;
        }}

        const crossRatioSection = document.createElement('div');
        const crossCard = document.createElement('div');
        crossCard.className = 'table-wrap';

        const baselineBinding = bindings[0];
        const baselineRun = runsByBinding.get(baselineBinding);
        if (!baselineRun) {{
          continue;
        }}

        const metrics = [
          {{
            label: 'total_runtime',
            labelDisplay: 'Total runtime',
            valueForRun: run => run.total_runtime_seconds,
            formatter: formatSeconds,
          }},
        ];

        for (const stepName of scale.step_names) {{
          metrics.push({{
            label: stepName,
            labelDisplay: `step:${{stepName}}`,
            valueForRun: run => getStep(run, stepName)?.duration_seconds || null,
            formatter: formatSeconds,
            lowerIsBetter: true,
          }});
        }}

        const table = document.createElement('table');
        const ratioColumns = bindings
          .slice(1)
          .flatMap(binding => [
            `${{binding}}`,
            `ratio(${{binding}}/${{baselineBinding}})`
          ]);
        table.innerHTML = `
          <thead>
            <tr>
              <th>Profile</th>
              <th>Metric</th>
              <th>Baseline (${{baselineBinding}})</th>
              ${{
                ratioColumns.map(label => `<th>${{label}}</th>`).join('')
              }}
            </tr>
          </thead>
          <tbody></tbody>
        `;
        const body = table.querySelector('tbody');

        for (const metric of metrics) {{
          const baselineValue = metric.valueForRun(baselineRun);
          if (baselineValue == null && !bindings.slice(1).some(binding => metric.valueForRun(runsByBinding.get(binding)) != null)) {{
            continue;
          }}
          const row = document.createElement('tr');
          const cells = [
            `<td class="mono">${{profile}}</td>`,
            `<td class="mono">${{metric.labelDisplay}}</td>`,
            `<td>${{metric.formatter(baselineValue)}}</td>`,
          ];

          for (const binding of bindings.slice(1)) {{
            const comparisonRun = runsByBinding.get(binding);
            const comparisonValue = comparisonRun ? metric.valueForRun(comparisonRun) : null;
            const ratioValue = ratioText(comparisonValue, baselineValue, metric.lowerIsBetter !== false, true);
            const ratioClass = trendClass(comparisonValue, baselineValue, metric.lowerIsBetter !== false);
            const valueCell = comparisonValue == null ? '—' : metric.formatter(comparisonValue);
            cells.push(`<td>${{valueCell}}</td>`);
            cells.push(`<td class="${{ratioClass}}">${{ratioValue}}</td>`);
          }}
          row.innerHTML = cells.join('');
          body.appendChild(row);
        }}

        crossCard.appendChild(table);
        section.appendChild(document.createElement('h3')).textContent = `Cross-engine latest ratios (profile: ${{profile}})`;
        section.appendChild(crossCard);
      }}

      const latestRuns = [...profileBindingRuns.values()];
      const latencyRows = rowsForCaseField(profileBindingRuns, 'latency_cases');
      const concurrencyRows = rowsForCaseField(profileBindingRuns, 'concurrency_cases');
      const writeRows = rowsForCaseField(profileBindingRuns, 'write_cases');
      const coldRows = rowsForCaseField(profileBindingRuns, 'cold_cases');

      renderCaseRowsTable(section, 'Latency case results (latest profile/binding runs)', latencyRows, [
        {{ label: 'Case', format: c => c.name || '—' }},
        {{ label: 'Iterations', format: c => c.iterations?.toLocaleString() ?? '0' }},
        {{ label: 'Warmup', format: c => c.warmup_iterations?.toLocaleString() ?? '0' }},
        {{ label: 'p50', format: c => formatNs(c.p50_ns) }},
        {{ label: 'p95', format: c => formatNs(c.p95_ns) }},
        {{ label: 'p99', format: c => formatNs(c.p99_ns) }},
        {{ label: 'Max', format: c => formatNs(c.max_ns) }},
        {{ label: 'Mean', format: c => formatNs(Math.round(c.mean_ns)) }},
        {{ label: 'Stddev', format: c => formatNs(Math.round(c.stddev_ns)) }},
        {{ label: 'Ops/s', format: c => formatRps(c.operations_per_second) }},
        {{ label: 'Rows/iter', format: c => c.rows_per_iteration?.toLocaleString() ?? '—' }},
      ]);

      renderCaseRowsTable(section, 'Concurrency case results (latest profile/binding runs)', concurrencyRows, [
        {{ label: 'Case', format: c => c.name || '—' }},
        {{ label: 'Threads', format: c => c.reader_threads?.toLocaleString() ?? '0' }},
        {{ label: 'Reads/thread', format: c => c.reads_per_thread?.toLocaleString() ?? '0' }},
        {{ label: 'Writer commits', format: c => c.writer_commits?.toLocaleString() ?? '0' }},
        {{ label: 'Reader p50', format: c => formatNs(c.reader_p50_ns) }},
        {{ label: 'Reader p95', format: c => formatNs(c.reader_p95_ns) }},
        {{ label: 'Reader p99', format: c => formatNs(c.reader_p99_ns) }},
        {{ label: 'Reader max', format: c => formatNs(c.reader_max_ns) }},
        {{ label: 'Reader rps', format: c => formatRps(c.reader_operations_per_second) }},
        {{ label: 'Writer p50', format: c => formatNs(c.writer_p50_ns) }},
        {{ label: 'Writer p95', format: c => formatNs(c.writer_p95_ns) }},
        {{ label: 'Writer p99', format: c => formatNs(c.writer_p99_ns) }},
        {{ label: 'Writer rps', format: c => c.writer_operations_per_second != null ? formatRps(c.writer_operations_per_second) : '—' }},
        {{ label: 'Degrade', format: c => c.reader_degradation_ratio_vs_isolated != null ? `${{c.reader_degradation_ratio_vs_isolated.toFixed(2)}}x` : '—' }},
      ]);

      renderCaseRowsTable(section, 'Write case results (latest profile/binding runs)', writeRows, [
        {{ label: 'Case', format: c => c.name || '—' }},
        {{ label: 'Iterations', format: c => c.iterations?.toLocaleString() ?? '0' }},
        {{ label: 'Warmup', format: c => c.warmup_iterations?.toLocaleString() ?? '0' }},
        {{ label: 'p50', format: c => formatNs(c.p50_ns) }},
        {{ label: 'p95', format: c => formatNs(c.p95_ns) }},
        {{ label: 'p99', format: c => formatNs(c.p99_ns) }},
        {{ label: 'Max', format: c => formatNs(c.max_ns) }},
        {{ label: 'Mean', format: c => formatNs(Math.round(c.mean_ns)) }},
        {{ label: 'Stddev', format: c => formatNs(Math.round(c.stddev_ns)) }},
        {{ label: 'Ops/s', format: c => formatRps(c.operations_per_second) }},
        {{ label: 'Rows/iter', format: c => c.rows_per_iteration?.toLocaleString() ?? '—' }},
      ]);

      renderCaseRowsTable(section, 'Cold case results (latest profile/binding runs)', coldRows, [
        {{ label: 'Case', format: c => c.name || '—' }},
        {{ label: 'Iterations', format: c => c.iterations?.toLocaleString() ?? '0' }},
        {{ label: 'Warmup', format: c => c.warmup_iterations?.toLocaleString() ?? '0' }},
        {{ label: 'p50', format: c => formatNs(c.p50_ns) }},
        {{ label: 'p95', format: c => formatNs(c.p95_ns) }},
        {{ label: 'p99', format: c => formatNs(c.p99_ns) }},
        {{ label: 'Max', format: c => formatNs(c.max_ns) }},
        {{ label: 'Mean', format: c => formatNs(Math.round(c.mean_ns)) }},
        {{ label: 'Stddev', format: c => formatNs(Math.round(c.stddev_ns)) }},
        {{ label: 'Ops/s', format: c => formatRps(c.operations_per_second) }},
      ]);

      const memoryWrap = document.createElement('div');
      memoryWrap.className = 'table-wrap';
      const memoryTable = document.createElement('table');
      memoryTable.innerHTML = `
        <thead>
          <tr>
            <th>Profile</th>
            <th>Binding</th>
            <th>Latest total runtime</th>
            <th>Peak RSS</th>
            <th>Peak RSS anon KB</th>
            <th>Peak RSS file KB</th>
            <th>DB size</th>
            <th>WAL size</th>
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const memoryBody = memoryTable.querySelector('tbody');
      const memoryRows = [...latestRuns];
      if (memoryRows.length === 0) {{
        addEmptyMessage(memoryWrap, 'No memory metrics available yet.');
      }} else {{
        for (const run of memoryRows) {{
          const row = document.createElement('tr');
          const peakRssAnon = maxStepNumeric(run, 'rss_anon_kb', 0);
          const peakRssFile = maxStepNumeric(run, 'rss_file_kb', 0);
          row.innerHTML = `
            <td class="mono">${{metricProfile(run)}}</td>
            <td class="mono">${{metricBinding(run)}}</td>
            <td>${{formatSeconds(run.total_runtime_seconds)}}</td>
            <td>${{formatBytes(run.report.peak_rss_bytes)}}</td>
            <td>${{formatBytes(peakRssAnon * 1024)}}</td>
            <td>${{formatBytes(peakRssFile * 1024)}}</td>
            <td>${{formatBytes(run.report.database_size_bytes)}}</td>
            <td>${{formatBytes(run.report.wal_size_bytes)}}</td>
          `;
          memoryBody.appendChild(row);
        }}
        memoryWrap.appendChild(memoryTable);
      }}
      section.appendChild(document.createElement('h3')).textContent = 'Memory fields (latest profile/binding runs)';
      section.appendChild(memoryWrap);

      host.appendChild(section);
    }}

    renderOverview();
    for (const scale of reportData.scales) {{
      renderScale(scale);
    }}
  </script>
</body>
</html>
"#,
        generated_at = data.generated_at_label,
        source_directory = data.source_directory,
        inputs_summary = data.inputs_summary,
        report_json = report_json,
    );
    Ok(html)
}

#[cfg(feature = "extended-suites")]
fn safe_json_for_html(json: String) -> String {
    json.replace("</", "<\\/")
}

// --- concurrency suite implementations --------------------------------------

// Phase 2 helpers first (moved up for compilation)

#[cfg(feature = "extended-suites")]
fn compute_latency_stats(
    samples: &mut [u64],
    iterations: u64,
    _warmup: u64,
    total_duration_secs: f64,
) -> (u64, u64, u64, u64, f64, f64, f64) {
    if samples.is_empty() {
        return (0, 0, 0, 0, 0.0, 0.0, 0.0);
    }
    samples.sort_unstable();
    let len = samples.len();
    let p50 = percentile_ns_sorted(samples, 50);
    let p95 = percentile_ns_sorted(samples, 95);
    let p99 = percentile_ns_sorted(samples, 99);
    let max = samples[len - 1];
    let sum: u64 = samples.iter().sum();
    let mean = sum as f64 / len as f64;
    let variance = samples
        .iter()
        .map(|&v| {
            let diff = v as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / len as f64;
    let stddev = variance.sqrt();
    let ops_per_sec = if total_duration_secs > 0.0 {
        iterations as f64 / total_duration_secs
    } else {
        0.0
    };
    (p50, p95, p99, max, mean, stddev, ops_per_sec)
}

#[cfg(feature = "extended-suites")]
fn percentile_ns_sorted(sorted: &[u64], percentile: u32) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let len = sorted.len();
    let idx = ((percentile as f64 / 100.0 * len as f64).ceil() as usize)
        .saturating_sub(1)
        .min(len.saturating_sub(1));
    sorted[idx]
}

#[cfg(feature = "extended-suites")]
fn artist_id_for_iter(i: u64, scale: Scale) -> i64 {
    (1 + ((i * 8191) % scale.artists as u64)) as i64
}

#[cfg(feature = "extended-suites")]
fn song_id_for_iter(i: u64, total_songs: u64) -> i64 {
    (1 + ((i * 4099) % total_songs)) as i64
}

#[cfg(feature = "extended-suites")]
fn song_range_start(i: u64, total_songs: u64) -> i64 {
    (1 + ((i * 1019) % total_songs.max(1).saturating_sub(100).max(1))) as i64
}

// --- latency suite implementations -----------------------------------------

#[cfg(feature = "extended-suites")]
fn run_latency_suite_decentdb(
    db: &decentdb::Db,
    report: &mut RunReport,
    scale: Scale,
    total_songs: u64,
    cli: &Cli,
) -> anyhow::Result<()> {
    let iters = cli.latency_iterations;
    let warmup = cli.latency_warmup;
    let heavy_iters = cli.heavy_latency_iterations;
    let heavy_warmup = cli.heavy_latency_warmup;

    println!("\n--- Latency Suite (DecentDB) ---");

    let prepare = |sql: &str| db.prepare(sql).expect("prepare latency");

    // artist_pk_lookup_full_row
    {
        let stmt = prepare("SELECT id, name, country, formed_year FROM artists WHERE id = $1");
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let id = artist_id_for_iter(i, scale);
            let start = std::time::Instant::now();
            let r = stmt
                .execute(&[Value::Int64(id)])
                .expect("latency artist pk");
            assert_eq!(r.rows().len(), 1);
            all_samples.push(elapsed_ns(start));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "artist_pk_lookup_full_row".into(),
            query_shape: "SELECT id, name, country, formed_year FROM artists WHERE id = ?".into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(1),
            ..Default::default()
        });
        println!("  artist_pk_lookup_full_row: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // artist_pk_lookup_name_only
    {
        let stmt = prepare("SELECT name FROM artists WHERE id = $1");
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let id = artist_id_for_iter(i, scale);
            let start = std::time::Instant::now();
            let r = stmt.execute(&[Value::Int64(id)]).expect("latency artist n");
            assert_eq!(r.rows().len(), 1);
            all_samples.push(elapsed_ns(start));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "artist_pk_lookup_name_only".into(),
            query_shape: "SELECT name FROM artists WHERE id = ?".into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(1),
            ..Default::default()
        });
        println!("  artist_pk_lookup_name_only: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // song_pk_range_50
    {
        let stmt = prepare("SELECT id, title, duration_ms FROM songs WHERE id >= $1 AND id < $2 ORDER BY id LIMIT 50");
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let start = song_range_start(i, total_songs);
            let end = start + 100;
            let t0 = std::time::Instant::now();
            let r = stmt
                .execute(&[Value::Int64(start), Value::Int64(end)])
                .expect("latency range");
            assert!(r.rows().len() <= 50);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "song_pk_range_50".into(),
            query_shape: "SELECT id, title, duration_ms FROM songs WHERE id >= ? AND id < ? ORDER BY id LIMIT 50".into(),
            iterations: iters, warmup_iterations: warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            ..Default::default()
        });
        println!("  song_pk_range_50: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // songs_by_artist_secondary_index_50
    {
        let stmt = prepare(
            "SELECT id, title, duration_ms FROM songs WHERE artist_id = $1 ORDER BY id LIMIT 50",
        );
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let _r = stmt
                .execute(&[Value::Int64(artist_id)])
                .expect("latency songs by artist");
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "songs_by_artist_secondary_index_50".into(),
            query_shape:
                "SELECT id, title, duration_ms FROM songs WHERE artist_id = ? ORDER BY id LIMIT 50"
                    .into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            ..Default::default()
        });
        println!("  songs_by_artist_secondary_index_50: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // song_album_join_by_song_id
    {
        let stmt = prepare("SELECT s.id, s.title, al.title FROM songs s JOIN albums al ON al.id = s.album_id WHERE s.id = $1");
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let song_id = song_id_for_iter(i, total_songs);
            let t0 = std::time::Instant::now();
            let r = stmt
                .execute(&[Value::Int64(song_id)])
                .expect("latency join");
            assert_eq!(r.rows().len(), 1);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "song_album_join_by_song_id".into(),
            query_shape: "SELECT s.id, s.title, al.title FROM songs s JOIN albums al ON al.id = s.album_id WHERE s.id = ?".into(),
            iterations: iters, warmup_iterations: warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            rows_per_iteration: Some(1),
            ..Default::default()
        });
        println!("  song_album_join_by_song_id: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // artist_song_count_aggregate
    {
        let stmt = prepare("SELECT COUNT(*), SUM(duration_ms) FROM songs WHERE artist_id = $1");
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let _r = stmt
                .execute(&[Value::Int64(artist_id)])
                .expect("latency agg");
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "artist_song_count_aggregate".into(),
            query_shape: "SELECT COUNT(*), SUM(duration_ms) FROM songs WHERE artist_id = ?".into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            ..Default::default()
        });
        println!("  artist_song_count_aggregate: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // view_artist_filter (heavy)
    {
        let stmt = prepare(
            "SELECT album_title, song_title, duration_ms FROM v_artist_songs WHERE artist_id = $1",
        );
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(heavy_iters + heavy_warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let r = stmt
                .execute(&[Value::Int64(artist_id)])
                .expect("latency view");
            all_samples.push(elapsed_ns(t0));
            black_box(r.rows().len());
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "view_artist_filter".into(),
            query_shape: "SELECT album_title, song_title, duration_ms FROM v_artist_songs WHERE artist_id = ?".into(),
            iterations: heavy_iters, warmup_iterations: heavy_warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            ..Default::default()
        });
        println!("  view_artist_filter: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // top10_artists_by_songs (heavy)
    {
        let stmt = prepare(
            "SELECT a.id, a.name, COUNT(s.id) AS song_count \
             FROM artists a JOIN songs s ON s.artist_id = a.id \
             GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10",
        );
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for _ in 0..(heavy_iters + heavy_warmup) {
            let t0 = std::time::Instant::now();
            let r = stmt.execute(&[]).expect("latency top10");
            assert_eq!(r.rows().len(), 10);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "top10_artists_by_songs".into(),
            query_shape: "SELECT a.id, a.name, COUNT(s.id) ... GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10".into(),
            iterations: heavy_iters, warmup_iterations: heavy_warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            rows_per_iteration: Some(10),
            ..Default::default()
        });
        println!("  top10_artists_by_songs: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // songs_full_scan_materialized (if enabled via full_scan_row_limit)
    if let Some(row_cap) = (total_songs > 0).then(|| cli.full_scan_row_limit.min(total_songs)) {
        let stmt =
            prepare("SELECT id, album_id, artist_id, title, duration_ms FROM songs LIMIT $1");
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for _ in 0..(heavy_iters + heavy_warmup) {
            let t0 = std::time::Instant::now();
            let r = stmt
                .execute(&[Value::Int64(row_cap as i64)])
                .expect("latency full scan");
            all_samples.push(elapsed_ns(t0));
            black_box(r.rows().len());
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        let rows_per_sec = if total_secs > 0.0 {
            row_cap as f64 / (total_secs / (heavy_iters + heavy_warmup) as f64)
        } else {
            0.0
        };
        let mut extra = serde_json::Map::new();
        extra.insert("rows_per_sec".into(), serde_json::json!(rows_per_sec));
        extra.insert("rows_per_iteration".into(), serde_json::json!(row_cap));
        report.latency_cases.push(LatencyCaseMetric {
            name: "songs_full_scan_materialized".into(),
            query_shape: "SELECT id, album_id, artist_id, title, duration_ms FROM songs LIMIT ?"
                .into(),
            iterations: heavy_iters,
            warmup_iterations: heavy_warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(row_cap),
            extra,
        });
        println!("  songs_full_scan_materialized: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    Ok(())
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "extended-suites")]
fn run_latency_suite_sqlite(
    conn: &SqliteConnection,
    report: &mut RunReport,
    scale: Scale,
    total_songs: u64,
    cli: &Cli,
) -> anyhow::Result<()> {
    let iters = cli.latency_iterations;
    let warmup = cli.latency_warmup;
    let heavy_iters = cli.heavy_latency_iterations;
    let heavy_warmup = cli.heavy_latency_warmup;

    println!("\n--- Latency Suite (SQLite) ---");

    // artist_pk_lookup_full_row
    {
        let mut stmt =
            conn.prepare("SELECT id, name, country, formed_year FROM artists WHERE id = ?1")?;
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let row: (i64, String, String, i64) = stmt.query_row([id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?;
            black_box(row);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "artist_pk_lookup_full_row".into(),
            query_shape: "SELECT id, name, country, formed_year FROM artists WHERE id = ?".into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(1),
            ..Default::default()
        });
        println!("  artist_pk_lookup_full_row: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // artist_pk_lookup_name_only
    {
        let mut stmt = conn.prepare("SELECT name FROM artists WHERE id = ?1")?;
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let name: String = stmt.query_row([id], |row| row.get(0))?;
            black_box(name);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "artist_pk_lookup_name_only".into(),
            query_shape: "SELECT name FROM artists WHERE id = ?".into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(1),
            ..Default::default()
        });
        println!("  artist_pk_lookup_name_only: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // song_pk_range_50
    {
        let mut stmt = conn.prepare("SELECT id, title, duration_ms FROM songs WHERE id >= ?1 AND id < ?2 ORDER BY id LIMIT 50")?;
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let start = song_range_start(i, total_songs);
            let end = start + 100;
            let t0 = std::time::Instant::now();
            let mut rows = stmt.query(rusqlite::params![start, end])?;
            let mut row_count = 0usize;
            while let Some(_row) = rows.next()? {
                row_count += 1;
            }
            assert!(row_count <= 50);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "song_pk_range_50".into(),
            query_shape: "SELECT id, title, duration_ms FROM songs WHERE id >= ? AND id < ? ORDER BY id LIMIT 50".into(),
            iterations: iters, warmup_iterations: warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            ..Default::default()
        });
        println!("  song_pk_range_50: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // songs_by_artist_secondary_index_50
    {
        let mut stmt = conn.prepare(
            "SELECT id, title, duration_ms FROM songs WHERE artist_id = ?1 ORDER BY id LIMIT 50",
        )?;
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let mut rows = stmt.query(rusqlite::params![artist_id])?;
            let mut row_count = 0usize;
            while let Some(_row) = rows.next()? {
                row_count += 1;
            }
            black_box(row_count);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "songs_by_artist_secondary_index_50".into(),
            query_shape:
                "SELECT id, title, duration_ms FROM songs WHERE artist_id = ? ORDER BY id LIMIT 50"
                    .into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            ..Default::default()
        });
        println!("  songs_by_artist_secondary_index_50: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // song_album_join_by_song_id
    {
        let mut stmt = conn.prepare(
            "SELECT s.id, s.title, al.title FROM songs s JOIN albums al ON al.id = s.album_id WHERE s.id = ?1"
        )?;
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let song_id = song_id_for_iter(i, total_songs);
            let t0 = std::time::Instant::now();
            let row: (i64, String, String) =
                stmt.query_row([song_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            black_box(row);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "song_album_join_by_song_id".into(),
            query_shape: "SELECT s.id, s.title, al.title FROM songs s JOIN albums al ON al.id = s.album_id WHERE s.id = ?".into(),
            iterations: iters, warmup_iterations: warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            rows_per_iteration: Some(1),
            ..Default::default()
        });
        println!("  song_album_join_by_song_id: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // artist_song_count_aggregate
    {
        let mut stmt =
            conn.prepare("SELECT COUNT(*), SUM(duration_ms) FROM songs WHERE artist_id = ?1")?;
        let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(iters + warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let row: (i64, Option<i64>) =
                stmt.query_row([artist_id], |row| Ok((row.get(0)?, row.get(1)?)))?;
            black_box(row);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, iters, warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "artist_song_count_aggregate".into(),
            query_shape: "SELECT COUNT(*), SUM(duration_ms) FROM songs WHERE artist_id = ?".into(),
            iterations: iters,
            warmup_iterations: warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            ..Default::default()
        });
        println!("  artist_song_count_aggregate: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // view_artist_filter (heavy)
    {
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(heavy_iters + heavy_warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let row_count = sqlite_query_row_count(conn, "SELECT album_title, song_title, duration_ms FROM v_artist_songs WHERE artist_id = ?1", params![artist_id])?;
            black_box(row_count);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "view_artist_filter".into(),
            query_shape: "SELECT album_title, song_title, duration_ms FROM v_artist_songs WHERE artist_id = ?".into(),
            iterations: heavy_iters, warmup_iterations: heavy_warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            ..Default::default()
        });
        println!("  view_artist_filter: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // top10_artists_by_songs (heavy)
    {
        let _stmt = conn.prepare(
            "SELECT a.id, a.name, COUNT(s.id) AS song_count \
             FROM artists a JOIN songs s ON s.artist_id = a.id \
             GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10",
        )?;
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for _ in 0..(heavy_iters + heavy_warmup) {
            let t0 = std::time::Instant::now();
            let row_count = sqlite_query_row_count(
                conn,
                "SELECT a.id, a.name, COUNT(s.id) AS song_count \
                 FROM artists a JOIN songs s ON s.artist_id = a.id \
                 GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10",
                [],
            )?;
            assert_eq!(row_count, 10);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "top10_artists_by_songs".into(),
            query_shape: "SELECT a.id, a.name, COUNT(s.id) ... GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10".into(),
            iterations: heavy_iters, warmup_iterations: heavy_warmup,
            p50_ns: p50, p95_ns: p95, p99_ns: p99, max_ns: max,
            mean_ns: mean, stddev_ns: stddev, operations_per_second: ops,
            rows_per_iteration: Some(10),
            ..Default::default()
        });
        println!("  top10_artists_by_songs: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // songs_full_scan_materialized
    if total_songs > 0 {
        let row_cap = cli.full_scan_row_limit.min(total_songs);
        let mut stmt =
            conn.prepare("SELECT id, album_id, artist_id, title, duration_ms FROM songs LIMIT ?1")?;
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for _ in 0..(heavy_iters + heavy_warmup) {
            let t0 = std::time::Instant::now();
            let mut rows = stmt.query([row_cap as i64])?;
            let mut n = 0usize;
            while let Some(_row) = rows.next()? {
                n += 1;
            }
            black_box(n);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        let rows_per_sec = if total_secs > 0.0 {
            row_cap as f64 / (total_secs / (heavy_iters + heavy_warmup) as f64)
        } else {
            0.0
        };
        let mut extra = serde_json::Map::new();
        extra.insert("rows_per_sec".into(), serde_json::json!(rows_per_sec));
        extra.insert("rows_per_iteration".into(), serde_json::json!(row_cap));
        report.latency_cases.push(LatencyCaseMetric {
            name: "songs_full_scan_materialized".into(),
            query_shape: "SELECT id, album_id, artist_id, title, duration_ms FROM songs LIMIT ?"
                .into(),
            iterations: heavy_iters,
            warmup_iterations: heavy_warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(row_cap),
            extra,
        });
        println!("  songs_full_scan_materialized: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    Ok(())
}

// --- open_sqlite_wal_normal -------------------------------------------------

#[cfg(feature = "sqlite")]
fn open_sqlite_wal_normal(path: &Path) -> rusqlite::Result<SqliteConnection> {
    let conn = SqliteConnection::open(path)?;
    let journal_mode: String = conn.query_row("PRAGMA journal_mode=WAL;", [], |row| row.get(0))?;
    assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    conn.execute_batch(
        "PRAGMA synchronous=NORMAL;
         PRAGMA wal_autocheckpoint=0;",
    )?;
    Ok(conn)
}

// --- concurrency suite implementations -------------------------------------

#[cfg(feature = "extended-suites")]
fn run_concurrency_suite_decentdb(
    db: &decentdb::Db,
    report: &mut RunReport,
    scale: Scale,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Concurrency Suite (DecentDB) ---");
    let reads_per_thread = cli.concurrent_reads_per_thread;
    let writer_commits = cli.writer_commits;

    for &thread_count in &cli.reader_thread_counts {
        // Isolated concurrent reads
        let total_start = std::time::Instant::now();
        let db = std::sync::Arc::new(db.clone());
        let mut handles = Vec::with_capacity(thread_count);
        for t in 0..thread_count {
            let db = std::sync::Arc::clone(&db);
            handles.push(std::thread::spawn(move || {
                let stmt = db
                    .prepare("SELECT name FROM artists WHERE id = $1")
                    .expect("prep concurrent");
                let mut latencies = Vec::with_capacity(reads_per_thread as usize);
                for i in 0..reads_per_thread {
                    let id = 1
                        + (((t as u64 * reads_per_thread + i) * 8191) % scale.artists as u64)
                            as i64;
                    let start = std::time::Instant::now();
                    let r = stmt.execute(&[Value::Int64(id)]).expect("concurrent read");
                    assert_eq!(r.rows().len(), 1);
                    latencies.push(elapsed_ns(start));
                }
                latencies
            }));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let total_ops = all.len() as u64;
        let mut samples = all.clone();
        let (p50, p95, p99, max, _, _, ops) =
            compute_latency_stats(&mut samples, total_ops, 0, total_secs);
        let case_name = format!("concurrent_artist_pk_lookup_isolated_t{thread_count}");
        report.concurrency_cases.push(ConcurrencyCaseMetric {
            name: case_name.clone(),
            reader_threads: thread_count,
            reads_per_thread,
            writer_commits: 0,
            reader_p50_ns: p50,
            reader_p95_ns: p95,
            reader_p99_ns: p99,
            reader_max_ns: max,
            reader_operations_per_second: ops,
            ..Default::default()
        });
        println!("  {case_name}: read_p50={p50}ns read_p95={p95}ns ops={ops:.0}/s");
        let isolated_p95 = p95;

        // Read under insert writer
        let write_db = std::sync::Arc::new(db.as_ref().clone());
        let write_db_clone = std::sync::Arc::clone(&write_db);
        let result_start = std::time::Instant::now();

        let write_handle = std::thread::spawn(move || {
            let mut latencies = Vec::with_capacity(writer_commits as usize);
            let base_id = (1_000_000_000i64).saturating_add((thread_count as i64) * 10_000_000);
            let stmt = write_db_clone
                .prepare("INSERT INTO write_events (id, artist_id, payload) VALUES ($1, $2, $3)")
                .expect("prepare write");
            for i in 0..writer_commits {
                let id = base_id + i as i64;
                let artist_id = 1 + ((i * 8191) % scale.artists as u64) as i64;
                let payload = format!("event {i}");
                let start = std::time::Instant::now();
                stmt.execute(&[
                    Value::Int64(id),
                    Value::Int64(artist_id),
                    Value::Text(payload),
                ])
                .expect("write event");
                latencies.push(elapsed_ns(start));
            }
            latencies
        });

        let mut read_handles = Vec::with_capacity(thread_count);
        for t in 0..thread_count {
            let read_db = std::sync::Arc::clone(&write_db);
            read_handles.push(std::thread::spawn(move || {
                let stmt = read_db
                    .prepare("SELECT name FROM artists WHERE id = $1")
                    .expect("prep concurrent rw");
                let mut latencies = Vec::with_capacity(reads_per_thread as usize);
                for i in 0..reads_per_thread {
                    let id = 1
                        + (((t as u64 * reads_per_thread + i) * 8191) % scale.artists as u64)
                            as i64;
                    let start = std::time::Instant::now();
                    let r = stmt.execute(&[Value::Int64(id)]).expect("rw read");
                    assert_eq!(r.rows().len(), 1);
                    latencies.push(elapsed_ns(start));
                }
                latencies
            }));
        }

        let all_reads: Vec<u64> = read_handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let writer_latencies = write_handle.join().unwrap();
        let total_secs_rw = result_start.elapsed().as_secs_f64();
        let total_read_ops = all_reads.len() as u64;
        let mut read_samples = all_reads.clone();
        let (rp50, rp95, rp99, rmax, _, _, rops) =
            compute_latency_stats(&mut read_samples, total_read_ops, 0, total_secs_rw);
        let mut ws = writer_latencies.clone();
        let (wp50, wp95, wp99, _, _, _, wops) =
            compute_latency_stats(&mut ws, writer_commits, 0, total_secs_rw);
        let deg_ratio = if isolated_p95 > 0 {
            rp95 as f64 / isolated_p95 as f64
        } else {
            1.0
        };
        let rw_name = format!("artist_pk_lookup_under_insert_writer_t{thread_count}");
        report.concurrency_cases.push(ConcurrencyCaseMetric {
            name: rw_name.clone(),
            reader_threads: thread_count,
            reads_per_thread,
            writer_commits,
            reader_p50_ns: rp50,
            reader_p95_ns: rp95,
            reader_p99_ns: rp99,
            reader_max_ns: rmax,
            reader_operations_per_second: rops,
            writer_p50_ns: Some(wp50),
            writer_p95_ns: Some(wp95),
            writer_p99_ns: Some(wp99),
            writer_operations_per_second: Some(wops),
            reader_degradation_ratio_vs_isolated: Some(deg_ratio),
            ..Default::default()
        });
        println!("  {rw_name}: read_p95={rp95}ns write_p95={wp95}ns degrade={deg_ratio:.2}x");
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "extended-suites")]
fn run_concurrency_suite_sqlite(
    db_path: &str,
    report: &mut RunReport,
    scale: Scale,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Concurrency Suite (SQLite) ---");
    let reads_per_thread = cli.concurrent_reads_per_thread;
    let writer_commits = cli.writer_commits;

    for &thread_count in &cli.reader_thread_counts {
        // Isolated concurrent reads
        let path = PathBuf::from(db_path);
        let mut handles = Vec::with_capacity(thread_count);
        let total_start = std::time::Instant::now();
        for t in 0..thread_count {
            let p = path.clone();
            handles.push(std::thread::spawn(move || {
                let conn = SqliteConnection::open(&p).unwrap();
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
                    .unwrap();
                let mut stmt = conn
                    .prepare("SELECT name FROM artists WHERE id = ?1")
                    .unwrap();
                let mut latencies = Vec::with_capacity(reads_per_thread as usize);
                for i in 0..reads_per_thread {
                    let id = 1
                        + (((t as u64 * reads_per_thread + i) * 8191) % scale.artists as u64)
                            as i64;
                    let start = std::time::Instant::now();
                    let name: String = stmt.query_row([id], |row| row.get(0)).unwrap();
                    black_box(name);
                    latencies.push(elapsed_ns(start));
                }
                latencies
            }));
        }
        let all: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let total_secs = total_start.elapsed().as_secs_f64();
        let total_ops = all.len() as u64;
        let mut samples = all.clone();
        let (p50, p95, p99, max, _, _, ops) =
            compute_latency_stats(&mut samples, total_ops, 0, total_secs);
        let case_name = format!("concurrent_artist_pk_lookup_isolated_t{thread_count}");
        report.concurrency_cases.push(ConcurrencyCaseMetric {
            name: case_name.clone(),
            reader_threads: thread_count,
            reads_per_thread,
            writer_commits: 0,
            reader_p50_ns: p50,
            reader_p95_ns: p95,
            reader_p99_ns: p99,
            reader_max_ns: max,
            reader_operations_per_second: ops,
            ..Default::default()
        });
        println!("  {case_name}: read_p50={p50}ns read_p95={p95}ns ops={ops:.0}/s");
        let isolated_p95 = p95;

        // Read under insert writer
        let p = path.clone();
        let writer_p = p.clone();
        let write_handle = std::thread::spawn(move || {
            let conn = SqliteConnection::open(&writer_p).unwrap();
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
                .unwrap();
            let mut stmt = conn
                .prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?1, ?2, ?3)")
                .unwrap();
            let mut latencies = Vec::with_capacity(writer_commits as usize);
            let base_id = (1_000_000_000i64).saturating_add((thread_count as i64) * 10_000_000);
            for i in 0..writer_commits {
                let id = base_id + i as i64;
                let artist_id = 1 + ((i * 8191) % scale.artists as u64) as i64;
                let payload = format!("event {i}");
                let start = std::time::Instant::now();
                stmt.execute(rusqlite::params![id, artist_id, payload])
                    .unwrap();
                latencies.push(elapsed_ns(start));
            }
            latencies
        });

        let mut handles = Vec::with_capacity(thread_count);
        let result_start = std::time::Instant::now();
        for t in 0..thread_count {
            let p = p.clone();
            handles.push(std::thread::spawn(move || {
                let conn = SqliteConnection::open(&p).unwrap();
                conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")
                    .unwrap();
                let mut stmt = conn
                    .prepare("SELECT name FROM artists WHERE id = ?1")
                    .unwrap();
                let mut latencies = Vec::with_capacity(reads_per_thread as usize);
                for i in 0..reads_per_thread {
                    let id = 1
                        + (((t as u64 * reads_per_thread + i) * 8191) % scale.artists as u64)
                            as i64;
                    let start = std::time::Instant::now();
                    let name: String = stmt.query_row([id], |row| row.get(0)).unwrap();
                    black_box(name);
                    latencies.push(elapsed_ns(start));
                }
                latencies
            }));
        }
        let all_reads: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();
        let writer_latencies = write_handle.join().unwrap();
        let total_secs_rw = result_start.elapsed().as_secs_f64();
        let total_read_ops = all_reads.len() as u64;
        let mut rs = all_reads.clone();
        let (rp50, rp95, rp99, rmax, _, _, rops) =
            compute_latency_stats(&mut rs, total_read_ops, 0, total_secs_rw);
        let mut ws = writer_latencies.clone();
        let (wp50, wp95, wp99, _, _, _, wops) =
            compute_latency_stats(&mut ws, writer_commits, 0, total_secs_rw);
        let deg_ratio = if isolated_p95 > 0 {
            rp95 as f64 / isolated_p95 as f64
        } else {
            1.0
        };
        let rw_name = format!("artist_pk_lookup_under_insert_writer_t{thread_count}");
        report.concurrency_cases.push(ConcurrencyCaseMetric {
            name: rw_name.clone(),
            reader_threads: thread_count,
            reads_per_thread,
            writer_commits,
            reader_p50_ns: rp50,
            reader_p95_ns: rp95,
            reader_p99_ns: rp99,
            reader_max_ns: rmax,
            reader_operations_per_second: rops,
            writer_p50_ns: Some(wp50),
            writer_p95_ns: Some(wp95),
            writer_p99_ns: Some(wp99),
            writer_operations_per_second: Some(wops),
            reader_degradation_ratio_vs_isolated: Some(deg_ratio),
            ..Default::default()
        });
        println!("  {rw_name}: read_p95={rp95}ns write_p95={wp95}ns degrade={deg_ratio:.2}x");
    }
    Ok(())
}

// --- write suite implementations --------------------------------------------

#[allow(clippy::too_many_arguments)]
#[cfg(feature = "extended-suites")]
fn latency_metric(
    name: &str,
    qs: &str,
    iters: u64,
    warmup: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
    stddev: f64,
    ops: f64,
    rows_per: Option<u64>,
) -> LatencyCaseMetric {
    LatencyCaseMetric {
        name: name.to_string(),
        query_shape: qs.to_string(),
        iterations: iters,
        warmup_iterations: warmup,
        p50_ns: p50,
        p95_ns: p95,
        p99_ns: p99,
        max_ns: max,
        mean_ns: mean,
        stddev_ns: stddev,
        operations_per_second: ops,
        rows_per_iteration: rows_per,
        ..Default::default()
    }
}

#[cfg(feature = "extended-suites")]
fn run_write_suite_decentdb(
    db: &decentdb::Db,
    report: &mut RunReport,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Write Suite (DecentDB) ---");
    let n = cli.write_iterations;

    // durable_insert_autocommit
    {
        let mut samples = Vec::with_capacity(n as usize);
        let stmt =
            db.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES ($1, $2, $3)")?;
        let base = 2_000_000_000i64;
        let total_start = std::time::Instant::now();
        for i in 0..n {
            let id = base + i as i64;
            let payload = format!("autocommit {i}");
            let start = std::time::Instant::now();
            stmt.execute(&[Value::Int64(id), Value::Int64(1), Value::Text(payload)])?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, n, 0, dur);
        report.write_cases.push(latency_metric(
            "durable_insert_autocommit",
            "INSERT INTO write_events ...",
            n,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  durable_insert_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // durable_insert_batch_10
    {
        let mut samples = Vec::with_capacity((n as usize / 10) + 1);
        let base = 3_000_000_000i64;
        let total_start = std::time::Instant::now();
        for batch_start in (0..n).step_by(10) {
            let mut txn = db.transaction()?;
            let stmt = txn
                .prepare("INSERT INTO write_events (id, artist_id, payload) VALUES ($1, $2, $3)")?;
            let batch_end = (batch_start + 10).min(n);
            let start = std::time::Instant::now();
            for i in batch_start..batch_end {
                let id = base + i as i64;
                let payload = format!("batch {i}");
                stmt.execute_in(
                    &mut txn,
                    &[Value::Int64(id), Value::Int64(2), Value::Text(payload)],
                )?;
            }
            txn.commit()?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let batches = samples.len() as u64;
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, batches, 0, dur);
        let mut extra = serde_json::Map::new();
        extra.insert("rows_per_commit".into(), serde_json::json!(10));
        let mut m = latency_metric(
            "durable_insert_batch_10",
            "INSERT ... batch 10",
            batches,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        );
        m.extra = extra;
        report.write_cases.push(m);
        println!("  durable_insert_batch_10: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // update_by_pk_autocommit
    {
        let base = 4_000_000_000i64;
        {
            let mut txn = db.transaction()?;
            let stmt = txn
                .prepare("INSERT INTO write_events (id, artist_id, payload) VALUES ($1, $2, $3)")?;
            for i in 0..n {
                let id = base + i as i64;
                stmt.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(id),
                        Value::Int64(3),
                        Value::Text("pre-update".to_string()),
                    ],
                )?;
            }
            txn.commit()?;
        }
        let mut samples = Vec::with_capacity(n as usize);
        let stmt = db.prepare("UPDATE write_events SET payload = $1 WHERE id = $2")?;
        let total_start = std::time::Instant::now();
        for i in 0..n {
            let id = base + i as i64;
            let start = std::time::Instant::now();
            stmt.execute(&[Value::Text(format!("updated {i}")), Value::Int64(id)])?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, n, 0, dur);
        report.write_cases.push(latency_metric(
            "update_by_pk_autocommit",
            "UPDATE write_events SET payload = ? WHERE id = ?",
            n,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  update_by_pk_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // delete_by_pk_autocommit
    {
        let base = 5_000_000_000i64;
        {
            let mut txn = db.transaction()?;
            let stmt = txn
                .prepare("INSERT INTO write_events (id, artist_id, payload) VALUES ($1, $2, $3)")?;
            for i in 0..n {
                let id = base + i as i64;
                stmt.execute_in(
                    &mut txn,
                    &[
                        Value::Int64(id),
                        Value::Int64(4),
                        Value::Text("pre-delete".to_string()),
                    ],
                )?;
            }
            txn.commit()?;
        }
        let mut samples = Vec::with_capacity(n as usize);
        let stmt = db.prepare("DELETE FROM write_events WHERE id = $1")?;
        let total_start = std::time::Instant::now();
        for i in 0..n {
            let id = base + i as i64;
            let start = std::time::Instant::now();
            stmt.execute(&[Value::Int64(id)])?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, n, 0, dur);
        report.write_cases.push(latency_metric(
            "delete_by_pk_autocommit",
            "DELETE FROM write_events WHERE id = ?",
            n,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  delete_by_pk_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    Ok(())
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "extended-suites")]
fn run_write_suite_sqlite(
    conn: &SqliteConnection,
    report: &mut RunReport,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Write Suite (SQLite) ---");
    let n = cli.write_iterations;

    // durable_insert_autocommit
    {
        let mut stmt =
            conn.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?1, ?2, ?3)")?;
        let mut samples = Vec::with_capacity(n as usize);
        let total_start = std::time::Instant::now();
        for i in 0..n {
            let id = 2_000_000_000i64 + i as i64;
            let start = std::time::Instant::now();
            stmt.execute(rusqlite::params![id, 1i64, format!("autocommit {i}")])?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, n, 0, dur);
        report.write_cases.push(latency_metric(
            "durable_insert_autocommit",
            "INSERT INTO write_events ...",
            n,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  durable_insert_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // durable_insert_batch_10
    {
        let mut samples = Vec::with_capacity((n as usize / 10) + 1);
        let total_start = std::time::Instant::now();
        for batch_start in (0..n).step_by(10) {
            conn.execute_batch("BEGIN IMMEDIATE;")?;
            let mut stmt = conn
                .prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?1, ?2, ?3)")?;
            let batch_end = (batch_start + 10).min(n);
            let start = std::time::Instant::now();
            for i in batch_start..batch_end {
                let id = 3_000_000_000i64 + i as i64;
                stmt.execute(rusqlite::params![id, 2i64, format!("batch {i}")])?;
            }
            conn.execute_batch("COMMIT;")?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let batches = samples.len() as u64;
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, batches, 0, dur);
        let mut extra = serde_json::Map::new();
        extra.insert("rows_per_commit".into(), serde_json::json!(10));
        let mut m = latency_metric(
            "durable_insert_batch_10",
            "INSERT ... batch 10",
            batches,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        );
        m.extra = extra;
        report.write_cases.push(m);
        println!("  durable_insert_batch_10: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // update_by_pk_autocommit
    {
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        let mut insert =
            conn.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?1, ?2, ?3)")?;
        for i in 0..n {
            let id = 4_000_000_000i64 + i as i64;
            insert.execute(rusqlite::params![id, 3i64, "pre-update"])?;
        }
        conn.execute_batch("COMMIT;")?;
        let mut stmt = conn.prepare("UPDATE write_events SET payload = ?1 WHERE id = ?2")?;
        let mut samples = Vec::with_capacity(n as usize);
        let total_start = std::time::Instant::now();
        for i in 0..n {
            let id = 4_000_000_000i64 + i as i64;
            let start = std::time::Instant::now();
            stmt.execute(rusqlite::params![format!("updated {i}"), id])?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, n, 0, dur);
        report.write_cases.push(latency_metric(
            "update_by_pk_autocommit",
            "UPDATE write_events SET payload = ? WHERE id = ?",
            n,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  update_by_pk_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // delete_by_pk_autocommit
    {
        conn.execute_batch("BEGIN IMMEDIATE;")?;
        let mut insert =
            conn.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?1, ?2, ?3)")?;
        for i in 0..n {
            let id = 5_000_000_000i64 + i as i64;
            insert.execute(rusqlite::params![id, 4i64, "pre-delete"])?;
        }
        conn.execute_batch("COMMIT;")?;
        let mut stmt = conn.prepare("DELETE FROM write_events WHERE id = ?1")?;
        let mut samples = Vec::with_capacity(n as usize);
        let total_start = std::time::Instant::now();
        for i in 0..n {
            let id = 5_000_000_000i64 + i as i64;
            let start = std::time::Instant::now();
            stmt.execute(rusqlite::params![id])?;
            samples.push(elapsed_ns(start));
        }
        let dur = total_start.elapsed().as_secs_f64();
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, n, 0, dur);
        report.write_cases.push(latency_metric(
            "delete_by_pk_autocommit",
            "DELETE FROM write_events WHERE id = ?",
            n,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  delete_by_pk_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    Ok(())
}

// --- cold suite implementations ---------------------------------------------

#[cfg(feature = "extended-suites")]
fn run_cold_suite_decentdb(
    db_path: &Path,
    report: &mut RunReport,
    scale: Scale,
    profile: BenchmarkProfile,
    expected_song_count: u64,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Cold Suite (DecentDB) ---");
    let db_path = canonical_or_original(db_path);
    let helper_exe = resolve_cold_helper_executable()?;
    let helper_dir = helper_output_dir(&cli.out_dir)?;

    // same_process_reopen_first_count
    {
        let mut samples = Vec::with_capacity(30);
        for _i in 0..30u64 {
            let start = std::time::Instant::now();
            let db = decentdb::Db::open(&db_path, profile.db_config())?;
            let r = db.execute("SELECT COUNT(*) FROM songs")?;
            black_box(r.rows().first());
            let dur = start.elapsed().as_secs_f64();
            let elapsed = (dur * 1_000_000_000.0) as u64;
            samples.push(elapsed);
            drop(db);
        }
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, 30, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "same_process_reopen_first_count",
            "SELECT COUNT(*) FROM songs",
            30,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  same_process_reopen_first_count: p50={p50}ns p95={p95}ns");
    }

    // same_process_reopen_first_artist_lookup
    {
        let mut samples = Vec::with_capacity(30);
        for i in 0..30u64 {
            let start = std::time::Instant::now();
            let db = decentdb::Db::open(&db_path, profile.db_config())?;
            let id = artist_id_for_iter(i, scale);
            let r = db.execute_with_params(
                "SELECT name FROM artists WHERE id = $1",
                &[Value::Int64(id)],
            )?;
            assert_eq!(r.rows().len(), 1);
            let dur = start.elapsed().as_secs_f64();
            samples.push((dur * 1_000_000_000.0) as u64);
            drop(db);
        }
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, 30, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "same_process_reopen_first_artist_lookup",
            "SELECT name FROM artists WHERE id = ?",
            30,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  same_process_reopen_first_artist_lookup: p50={p50}ns p95={p95}ns");
    }

    // cold_process_open_first_count
    {
        let mut samples = Vec::with_capacity(10);
        for i in 0..10u64 {
            let helper_out = helper_dir.join(format!(
                "cold-helper-count-decentdb-{}-{}.json",
                std::process::id(),
                i
            ));
            let output = run_cold_helper_child(
                &helper_exe,
                &db_path,
                &helper_out,
                BenchmarkEngine::DecentDb,
                profile,
                "count_songs",
                Some(expected_song_count),
            )?;
            samples.push(output.duration_ns);
        }
        let mut samples_copy = samples.clone();
        let (p50, p95, p99, max, mean, stddev, _) =
            compute_latency_stats(&mut samples_copy, 10, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "cold_process_open_first_count",
            "SELECT COUNT(*) FROM songs",
            10,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            0.0,
            None,
        ));
        println!("  cold_process_open_first_count: p50={p50}ns p95={p95}ns");
    }

    // recovery_reopen_first_count
    {
        let recovery_expected_count = 10u64;
        let recovery_path = db_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("recovery-test.ddb");
        delete_db_files(&recovery_path);
        let db2 = decentdb::Db::create(&recovery_path, profile.db_config())?;
        db2.execute_batch(&build_schema_ddl_batch())?;
        let mut txn = db2.transaction()?;
        let ins_artist = txn.prepare(
            "INSERT INTO artists (id, name, country, formed_year) VALUES ($1, $2, $3, $4)",
        )?;
        let ins_album = txn.prepare(
            "INSERT INTO albums (id, artist_id, title, release_year) VALUES ($1, $2, $3, $4)",
        )?;
        let ins_song = txn.prepare(
            "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) VALUES ($1, $2, $3, $4, $5)",
        )?;
        ins_artist.execute_in(
            &mut txn,
            &[
                Value::Int64(1),
                Value::Text("Recovery Artist 1".to_string()),
                Value::Text("XX".to_string()),
                Value::Int64(2000),
            ],
        )?;
        ins_album.execute_in(
            &mut txn,
            &[
                Value::Int64(1),
                Value::Int64(1),
                Value::Text("Recovery Album 1".to_string()),
                Value::Int64(2000),
            ],
        )?;
        for s in 1..=recovery_expected_count {
            ins_song.execute_in(
                &mut txn,
                &[
                    Value::Int64(s as i64),
                    Value::Int64(1),
                    Value::Int64(1),
                    Value::Text(format!("Recovery Song {s}")),
                    Value::Int64(120_000),
                ],
            )?;
        }
        txn.commit()?;
        drop(db2);

        let mut samples = Vec::with_capacity(10);
        for i in 0..10u64 {
            let helper_out = helper_dir.join(format!(
                "cold-helper-recovery-decentdb-{}-{}.json",
                std::process::id(),
                i
            ));
            let output = run_cold_helper_child(
                &helper_exe,
                &recovery_path,
                &helper_out,
                BenchmarkEngine::DecentDb,
                profile,
                "count_songs",
                Some(recovery_expected_count),
            )?;
            samples.push(output.duration_ns);
        }
        let mut samples_copy = samples.clone();
        let (p50, p95, p99, max, mean, stddev, _) =
            compute_latency_stats(&mut samples_copy, 10, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "recovery_reopen_first_count",
            "SELECT COUNT(*) FROM songs",
            10,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            0.0,
            None,
        ));
        println!("  recovery_reopen_first_count: p50={p50}ns p95={p95}ns");
        delete_db_files(&recovery_path);
    }

    Ok(())
}

#[cfg(feature = "sqlite")]
#[cfg(feature = "extended-suites")]
fn run_cold_suite_sqlite(
    db_path: &Path,
    report: &mut RunReport,
    scale: Scale,
    expected_song_count: u64,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Cold Suite (SQLite) ---");
    let db_path = canonical_or_original(db_path);
    let helper_exe = resolve_cold_helper_executable()?;
    let helper_dir = helper_output_dir(&cli.out_dir)?;

    // same_process_reopen_first_count
    {
        let mut samples = Vec::with_capacity(30);
        for _ in 0..30u64 {
            let start = std::time::Instant::now();
            let conn = open_sqlite_wal_full(&db_path)?;
            let count: i64 = conn.query_row("SELECT COUNT(*) FROM songs", [], |row| row.get(0))?;
            black_box(count);
            let dur = start.elapsed().as_secs_f64();
            samples.push((dur * 1_000_000_000.0) as u64);
            drop(conn);
        }
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, 30, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "same_process_reopen_first_count",
            "SELECT COUNT(*) FROM songs",
            30,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  same_process_reopen_first_count: p50={p50}ns p95={p95}ns");
    }

    // same_process_reopen_first_artist_lookup
    {
        let mut samples = Vec::with_capacity(30);
        for i in 0..30u64 {
            let start = std::time::Instant::now();
            let conn = open_sqlite_wal_full(&db_path)?;
            let id = artist_id_for_iter(i, scale);
            let name: String =
                conn.query_row("SELECT name FROM artists WHERE id = ?1", [id], |row| {
                    row.get(0)
                })?;
            black_box(name);
            let dur = start.elapsed().as_secs_f64();
            samples.push((dur * 1_000_000_000.0) as u64);
            drop(conn);
        }
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(&mut samples, 30, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "same_process_reopen_first_artist_lookup",
            "SELECT name FROM artists WHERE id = ?",
            30,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            ops,
            None,
        ));
        println!("  same_process_reopen_first_artist_lookup: p50={p50}ns p95={p95}ns");
    }

    // cold_process_open_first_count
    {
        let mut samples = Vec::with_capacity(10);
        for i in 0..10u64 {
            let helper_out = helper_dir.join(format!(
                "cold-helper-count-sqlite-{}-{}.json",
                std::process::id(),
                i
            ));
            let output = run_cold_helper_child(
                &helper_exe,
                &db_path,
                &helper_out,
                BenchmarkEngine::Sqlite,
                BenchmarkProfile::Default,
                "count_songs",
                Some(expected_song_count),
            )?;
            samples.push(output.duration_ns);
        }
        let mut samples_copy = samples.clone();
        let (p50, p95, p99, max, mean, stddev, _) =
            compute_latency_stats(&mut samples_copy, 10, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "cold_process_open_first_count",
            "SELECT COUNT(*) FROM songs",
            10,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            0.0,
            None,
        ));
        println!("  cold_process_open_first_count: p50={p50}ns p95={p95}ns");
    }

    // recovery_reopen_first_count
    {
        let recovery_expected_count = 10u64;
        let recovery_path = db_path
            .parent()
            .unwrap_or(Path::new("."))
            .join("recovery-test-sqlite.db");
        delete_db_files(&recovery_path);
        {
            let conn = open_sqlite_wal_full(&recovery_path)?;
            conn.execute_batch(&build_schema_ddl_batch())?;
            conn.execute_batch("BEGIN IMMEDIATE;")?;
            {
                let mut artist_stmt = conn.prepare(
                    "INSERT INTO artists (id, name, country, formed_year) VALUES (?1, ?2, ?3, ?4)",
                )?;
                let mut album_stmt = conn.prepare(
                    "INSERT INTO albums (id, artist_id, title, release_year) VALUES (?1, ?2, ?3, ?4)",
                )?;
                let mut song_stmt = conn.prepare(
                    "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                )?;
                artist_stmt.execute(rusqlite::params![1_i64, "Recovery Artist 1", "XX", 2000])?;
                album_stmt.execute(rusqlite::params![1_i64, 1_i64, "Recovery Album 1", 2000])?;
                for s in 1..=i64::try_from(recovery_expected_count).unwrap_or(0) {
                    song_stmt.execute(rusqlite::params![
                        s,
                        1_i64,
                        1_i64,
                        format!("Recovery Song {s}"),
                        120_000_i64
                    ])?;
                }
            }
            conn.execute_batch("COMMIT;")?;
        }
        let mut samples = Vec::with_capacity(10);
        for i in 0..10u64 {
            let helper_out = helper_dir.join(format!(
                "cold-helper-recovery-sqlite-{}-{}.json",
                std::process::id(),
                i
            ));
            let output = run_cold_helper_child(
                &helper_exe,
                &recovery_path,
                &helper_out,
                BenchmarkEngine::Sqlite,
                BenchmarkProfile::Default,
                "count_songs",
                Some(recovery_expected_count),
            )?;
            samples.push(output.duration_ns);
        }
        let mut samples_copy = samples.clone();
        let (p50, p95, p99, max, mean, stddev, _) =
            compute_latency_stats(&mut samples_copy, 10, 0, 0.0);
        report.cold_cases.push(latency_metric(
            "recovery_reopen_first_count",
            "SELECT COUNT(*) FROM songs",
            10,
            0,
            p50,
            p95,
            p99,
            max,
            mean,
            stddev,
            0.0,
            None,
        ));
        println!("  recovery_reopen_first_count: p50={p50}ns p95={p95}ns");
        delete_db_files(&recovery_path);
    }

    Ok(())
}

// --- cold helper mode -------------------------------------------------------

#[cfg(feature = "extended-suites")]
fn run_cold_helper_child(
    helper_exe: &Path,
    db_path: &Path,
    output: &Path,
    engine: BenchmarkEngine,
    profile: BenchmarkProfile,
    query: &str,
    expected_count: Option<u64>,
) -> anyhow::Result<ColdHelperOutput> {
    let output_parent = output
        .parent()
        .context("cold helper output path has no parent")?;
    fs::create_dir_all(output_parent)?;
    let mut command = std::process::Command::new(helper_exe);
    command
        .arg("--cold-helper")
        .arg("--cold-helper-query")
        .arg(query)
        .arg("--cold-helper-output")
        .arg(canonical_or_original(output))
        .arg("--db-path")
        .arg(canonical_or_original(db_path))
        .arg("--engine")
        .arg(match engine {
            BenchmarkEngine::DecentDb => "decentdb",
            #[cfg(feature = "sqlite")]
            BenchmarkEngine::Sqlite => "sqlite",
            #[cfg(feature = "duckdb")]
            BenchmarkEngine::DuckDb => "duckdb",
        });
    if engine == BenchmarkEngine::DecentDb {
        command.arg("--profile").arg(profile.as_str());
    }
    if let Some(expected_count) = expected_count {
        command
            .arg("--cold-helper-expected-count")
            .arg(expected_count.to_string());
    }
    let output_status = command.output().context("failed to spawn cold helper")?;
    if !output_status.status.success() {
        let stderr = String::from_utf8_lossy(&output_status.stderr);
        bail!("cold helper failed: {stderr}");
    }
    let json_str = fs::read_to_string(output)?;
    let parsed: ColdHelperOutput = serde_json::from_str(&json_str)?;
    let _ = fs::remove_file(output);
    Ok(parsed)
}

#[cfg(feature = "extended-suites")]
fn run_cold_helper(cli: Cli) -> anyhow::Result<()> {
    let db_path = cli
        .db_path
        .as_ref()
        .context("--cold-helper requires --db-path")?;
    let query = cli
        .cold_helper_query
        .as_deref()
        .context("--cold-helper requires --cold-helper-query")?;
    let output = cli
        .cold_helper_output
        .as_ref()
        .context("--cold-helper requires --cold-helper-output")?;
    let expected_count = cli.cold_helper_expected_count;
    let db_path = canonical_or_original(db_path);
    let output = canonical_or_original(output);
    if expected_count.is_some() && query != "count_songs" {
        bail!("--cold-helper-expected-count is only valid for count_songs");
    }
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    } else {
        bail!("cold helper output path has no parent");
    }

    let start = std::time::Instant::now();
    let mut result_count = None;
    match cli.engine {
        BenchmarkEngine::DecentDb => {
            let db = decentdb::Db::open(db_path, cli.profile.db_config())?;
            match query {
                "count_songs" => {
                    let r = db.execute("SELECT COUNT(*) FROM songs")?;
                    let count = u64::try_from(scalar_int(&r)).unwrap_or(0);
                    result_count = Some(count);
                    if let Some(expected_count) = expected_count {
                        if count != expected_count {
                            bail!("cold helper count mismatch: expected {expected_count}, got {count}");
                        }
                    }
                    black_box(count);
                }
                "artist_lookup" => {
                    let scale = parse_scale("smoke");
                    let id = artist_id_for_iter(0, scale);
                    let r = db.execute_with_params(
                        "SELECT name FROM artists WHERE id = $1",
                        &[Value::Int64(id)],
                    )?;
                    assert_eq!(r.rows().len(), 1);
                    black_box(r.rows().first());
                }
                _ => bail!("unknown cold helper query: {query}"),
            };
            let _ = db;
        }
        #[cfg(feature = "sqlite")]
        BenchmarkEngine::Sqlite => {
            let conn = open_sqlite_wal_full(&db_path)?;
            match query {
                "count_songs" => {
                    let count: i64 =
                        conn.query_row("SELECT COUNT(*) FROM songs", [], |row| row.get(0))?;
                    let count = u64::try_from(count).unwrap_or(0);
                    result_count = Some(count);
                    if let Some(expected_count) = expected_count {
                        if count != expected_count {
                            bail!("cold helper count mismatch: expected {expected_count}, got {count}");
                        }
                    }
                    black_box(count);
                }
                "artist_lookup" => {
                    let id = artist_id_for_iter(0, parse_scale("smoke"));
                    let name: String =
                        conn.query_row("SELECT name FROM artists WHERE id = ?1", [id], |row| {
                            row.get(0)
                        })?;
                    black_box(name);
                }
                _ => bail!("unknown cold helper query: {query}"),
            };
        }
        #[cfg(feature = "duckdb")]
        BenchmarkEngine::DuckDb => bail!("cold helper not implemented for DuckDB"),
    };
    let duration_ns = elapsed_ns(start);

    let json = ColdHelperOutput {
        duration_ns,
        query: query.to_string(),
        engine: engine_access_path(cli.engine).to_string(),
        result_count,
    };
    fs::write(&output, serde_json::to_string_pretty(&json)?)?;
    Ok(())
}

// --- DuckDB benchmark implementation ---------------------------------------

#[cfg(feature = "duckdb")]
fn run_duckdb_benchmark(cli: &Cli, scale: Scale) -> anyhow::Result<()> {
    let seed = cli.seed;
    let out_dir = cli.out_dir.clone();
    let db_path = cli
        .db_path
        .clone()
        .unwrap_or_else(|| BenchmarkEngine::DuckDb.default_db_path(scale));

    println!(
        "DuckDB scale={} artists={} albums={}",
        scale.name, scale.artists, scale.albums
    );
    let summary = summarize_seed_plan(scale, seed);
    println!(
        "Plan: artists={} total_albums={} total_songs={}",
        scale.artists, summary.total_albums, summary.total_songs
    );

    delete_db_files(&db_path);
    let conn = duckdb::Connection::open(&db_path)?;
    conn.execute_batch("SET threads = 1;")?;

    let mut report = RunReport {
        binding: BenchmarkEngine::DuckDb.binding_name().to_string(),
        scale_name: scale.name.to_string(),
        benchmark_profile: "duckdb-engine-default".to_string(),
        target_artists: scale.artists,
        target_albums: scale.albums,
        target_songs_cap: scale.songs_cap,
        started_unix: now_unix(),
        database_path: db_path.display().to_string(),
        ..Default::default()
    };
    let peak_rss;
    #[cfg(feature = "extended-suites")]
    let total_songs;
    {
        let mut rec = Recorder::new(&mut report);

        rec.measure("connect_open", None, || {});
        rec.report.engine_version = conn
            .query_row("SELECT version()", [], |row| row.get::<_, String>(0))
            .unwrap_or_else(|_| "unknown".into());

        #[cfg(feature = "extended-suites")]
        let needs_write_events = cli.write_suite || cli.concurrency_suite;
        #[cfg(feature = "extended-suites")]
        let mut ddl_batch = build_schema_ddl_batch();
        #[cfg(feature = "extended-suites")]
        if needs_write_events {
            ddl_batch.push('\n');
            ddl_batch.push_str(write_events_ddl());
            ddl_batch.push(';');
        }
        #[cfg(not(feature = "extended-suites"))]
        let ddl_batch = build_schema_ddl_batch();
        rec.measure("schema_create", None, || {
            conn.execute_batch(&ddl_batch).expect("duckdb ddl");
        });

        conn.execute_batch("BEGIN TRANSACTION;")?;
        let mut ins_a = conn
            .prepare("INSERT INTO artists (id, name, country, formed_year) VALUES (?, ?, ?, ?)")?;
        rec.measure("seed_artists", Some(u64::from(scale.artists)), || {
            let mut an = String::with_capacity(32);
            walk_seed_plan_select(
                scale,
                seed,
                SeedWalkEmit::ARTISTS,
                |a| {
                    an.clear();
                    an.push_str("Artist ");
                    write!(&mut an, "{}", a.id).ok();
                    ins_a
                        .execute(duckdb::params![a.id, an.as_str(), a.country, a.formed_year])
                        .expect("duckdb ins a");
                },
                |_| {},
                |_| {},
            );
        });
        drop(ins_a);
        conn.execute_batch("COMMIT;")?;

        conn.execute_batch("BEGIN TRANSACTION;")?;
        let mut ins_al = conn.prepare(
            "INSERT INTO albums (id, artist_id, title, release_year) VALUES (?, ?, ?, ?)",
        )?;
        rec.measure("seed_albums", Some(summary.total_albums), || {
            let mut at = String::with_capacity(32);
            walk_seed_plan_select(
                scale,
                seed,
                SeedWalkEmit::ALBUMS,
                |_| {},
                |al| {
                    at.clear();
                    at.push_str("Album ");
                    write!(&mut at, "{}", al.id).ok();
                    ins_al
                        .execute(duckdb::params![
                            al.id,
                            al.artist_id,
                            at.as_str(),
                            al.release_year
                        ])
                        .expect("duckdb ins al");
                },
                |_| {},
            );
        });
        drop(ins_al);
        conn.execute_batch("COMMIT;")?;

        conn.execute_batch("BEGIN TRANSACTION;")?;
        let mut ins_s = conn.prepare(
        "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) VALUES (?, ?, ?, ?, ?)",
    )?;
        rec.measure("seed_songs", Some(summary.total_songs), || {
            let mut st = String::with_capacity(32);
            walk_seed_plan_select(
                scale,
                seed,
                SeedWalkEmit::SONGS,
                |_| {},
                |_| {},
                |s| {
                    st.clear();
                    st.push_str("Song ");
                    write!(&mut st, "{}", s.id).ok();
                    ins_s
                        .execute(duckdb::params![
                            s.id,
                            s.album_id,
                            s.artist_id,
                            st.as_str(),
                            s.duration_ms
                        ])
                        .expect("duckdb ins s");
                },
            );
        });
        drop(ins_s);
        conn.execute_batch("COMMIT;")?;

        rec.measure("checkpoint_after_seed", None, || {
            let _ = conn.execute_batch("CHECKPOINT;");
        });

        let count = rec.measure("query_count_songs", None, || {
            let count: i64 = conn
                .query_row("SELECT COUNT(*) FROM songs", [], |row| row.get(0))
                .expect("duckdb cnt");
            println!("    count={count}");
            count
        });
        let count_rows = vec![vec![SemanticValue::Int64(count)]];
        add_query_evidence(
            &mut rec,
            "query_count_songs",
            &count_rows,
            scale,
            summary.total_songs,
        );
        drop(count_rows);
        #[cfg(feature = "extended-suites")]
        {
            total_songs = u64::try_from(count).unwrap_or(0);
        }

        let aggregate = rec.measure("query_aggregate_durations", None, || {
            let row: (i64, i64, f64, i64, i64) = conn
                .query_row(
                    "SELECT COUNT(*), SUM(duration_ms), AVG(duration_ms), \
                     MIN(duration_ms), MAX(duration_ms) FROM songs",
                    [],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .expect("duckdb agg");
            println!("    agg_row={row:?}");
            row
        });
        let aggregate_rows = vec![vec![
            SemanticValue::Int64(aggregate.0),
            SemanticValue::Int64(aggregate.1),
            SemanticValue::Float64(aggregate.2),
            SemanticValue::Int64(aggregate.3),
            SemanticValue::Int64(aggregate.4),
        ]];
        add_query_evidence(
            &mut rec,
            "query_aggregate_durations",
            &aggregate_rows,
            scale,
            summary.total_songs,
        );
        drop(aggregate_rows);

        let artist = rec.measure("query_artist_by_id", None, || {
            let target = i64::from(scale.artists) / 2 + 1;
            let row: (i64, String, String, i64) = conn
                .query_row(
                    "SELECT id, name, country, formed_year FROM artists WHERE id = ?",
                    [target],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .expect("duckdb art");
            println!("    artist={row:?}");
            row
        });
        let artist_rows = vec![vec![
            SemanticValue::Int64(artist.0),
            SemanticValue::Text(artist.1),
            SemanticValue::Text(artist.2),
            SemanticValue::Int64(artist.3),
        ]];
        add_query_evidence(
            &mut rec,
            "query_artist_by_id",
            &artist_rows,
            scale,
            summary.total_songs,
        );
        drop(artist_rows);

        let top_artist_rows = rec.measure("query_top10_artists_by_songs", None, || {
            let rows = duckdb_top10_rows(
                &conn,
                "SELECT a.id, a.name, COUNT(s.id) AS song_count FROM artists a \
                 JOIN songs s ON s.artist_id = a.id GROUP BY a.id, a.name \
                 ORDER BY song_count DESC LIMIT 10",
            )
            .expect("duckdb top10 artists");
            println!("    rows={}", rows.len());
            rows
        });
        add_query_evidence(
            &mut rec,
            "query_top10_artists_by_songs",
            &top_artist_rows,
            scale,
            summary.total_songs,
        );
        drop(top_artist_rows);

        let top_album_rows = rec.measure("query_top10_albums_by_songs", None, || {
            let rows = duckdb_top10_rows(
                &conn,
                "SELECT al.id, al.title, COUNT(s.id) AS song_count FROM albums al \
                 JOIN songs s ON s.album_id = al.id GROUP BY al.id, al.title \
                 ORDER BY song_count DESC LIMIT 10",
            )
            .expect("duckdb top10 albums");
            println!("    rows={}", rows.len());
            rows
        });
        add_query_evidence(
            &mut rec,
            "query_top10_albums_by_songs",
            &top_album_rows,
            scale,
            summary.total_songs,
        );
        drop(top_album_rows);

        let view_rows = rec.measure("query_view_first_1000", None, || {
            let rows = duckdb_view_rows(&conn).expect("duckdb view 1000");
            println!("    rows={}", rows.len());
            rows
        });
        add_query_evidence(
            &mut rec,
            "query_view_first_1000",
            &view_rows,
            scale,
            summary.total_songs,
        );
        drop(view_rows);

        let filtered_rows = rec.measure("query_songs_for_artist_via_view", None, || {
            let rows = duckdb_filtered_view_rows(&conn).expect("duckdb filtered view");
            println!("    rows={}", rows.len());
            rows
        });
        add_query_evidence(
            &mut rec,
            "query_songs_for_artist_via_view",
            &filtered_rows,
            scale,
            summary.total_songs,
        );
        drop(filtered_rows);

        peak_rss = rec.peak_rss;
    }

    #[cfg(feature = "extended-suites")]
    {
        if cli.latency_suite {
            run_latency_suite_duckdb(&conn, &mut report, scale, total_songs, cli)?;
        }
        if cli.write_suite {
            run_write_suite_duckdb(&conn, &mut report, cli)?;
        }
        if cli.concurrency_suite {
            // DuckDB Connection is not Send; record fallback
            let mut extra = serde_json::Map::new();
            extra.insert(
                "concurrent_mode".into(),
                serde_json::json!("single_thread_fallback"),
            );
            report.concurrency_cases.push(ConcurrencyCaseMetric {
                name: "duckdb_concurrent_fallback".into(),
                reader_threads: 1,
                reads_per_thread: 0,
                writer_commits: 0,
                extra,
                ..Default::default()
            });
            println!("--- Concurrency Suite (DuckDB) --- note: single-thread fallback, DuckDB Connection is not Send");
        }
    }

    drop(conn);
    report.peak_rss_bytes = peak_rss;

    if let Ok(meta) = fs::metadata(&db_path) {
        report.database_size_bytes = meta.len();
    }
    let wal_path = PathBuf::from(format!("{}.wal", db_path.display()));
    report.wal_size_bytes = file_size(&wal_path);
    report.finished_unix = now_unix();
    populate_run_report_metadata(
        &mut report,
        BenchmarkEngine::DuckDb,
        BenchmarkProfile::Default,
    );
    finalize_runner_provenance(&mut report, seed)?;

    fs::create_dir_all(&out_dir)?;
    let datetime_stamp = format_unix_filename_stamp(report.finished_unix);
    let out_path = out_dir.join(format!(
        "{datetime_stamp}-rust-baseline-duckdb-engine-default-{}.json",
        scale.name
    ));
    write_json_atomic(&out_path, &serde_json::to_string_pretty(&report)?)?;
    println!("\nWrote {}", out_path.display());

    delete_db_files(&db_path);
    println!("Cleaned up temp DB files: {}", db_path.display());
    Ok(())
}

#[cfg(feature = "duckdb")]
fn duckdb_top10_rows(conn: &duckdb::Connection, sql: &str) -> anyhow::Result<SemanticRows> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(vec![
            SemanticValue::Int64(row.get(0)?),
            SemanticValue::Text(row.get(1)?),
            SemanticValue::Int64(row.get(2)?),
        ]);
    }
    Ok(result)
}

#[cfg(feature = "duckdb")]
fn duckdb_view_rows(conn: &duckdb::Connection) -> anyhow::Result<SemanticRows> {
    let mut stmt = conn.prepare(
        "SELECT artist_id, artist_name, album_title, song_title \
         FROM v_artist_songs LIMIT 1000",
    )?;
    let mut rows = stmt.query([])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(vec![
            SemanticValue::Int64(row.get(0)?),
            SemanticValue::Text(row.get(1)?),
            SemanticValue::Text(row.get(2)?),
            SemanticValue::Text(row.get(3)?),
        ]);
    }
    Ok(result)
}

#[cfg(feature = "duckdb")]
fn duckdb_filtered_view_rows(conn: &duckdb::Connection) -> anyhow::Result<SemanticRows> {
    let mut stmt = conn.prepare(
        "SELECT album_title, song_title, duration_ms \
         FROM v_artist_songs WHERE artist_id = 1",
    )?;
    let mut rows = stmt.query([])?;
    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        result.push(vec![
            SemanticValue::Text(row.get(0)?),
            SemanticValue::Text(row.get(1)?),
            SemanticValue::Int64(row.get(2)?),
        ]);
    }
    Ok(result)
}

#[cfg(feature = "duckdb")]
#[cfg(feature = "extended-suites")]
fn run_latency_suite_duckdb(
    conn: &duckdb::Connection,
    report: &mut RunReport,
    scale: Scale,
    total_songs: u64,
    cli: &Cli,
) -> anyhow::Result<()> {
    let iters = cli.latency_iterations;
    let warmup = cli.latency_warmup;
    let heavy_iters = cli.heavy_latency_iterations;
    let heavy_warmup = cli.heavy_latency_warmup;
    println!("\n--- Latency Suite (DuckDB) ---");

    let mut measure =
        |name: &str,
         qs: &str,
         sql: &str,
         run_fn: fn(&mut duckdb::Statement, u64, Scale, u64) -> anyhow::Result<u64>|
         -> anyhow::Result<()> {
            let mut stmt = conn.prepare(sql)?;
            let mut all_samples = Vec::with_capacity((iters + warmup) as usize);
            let total_start = std::time::Instant::now();
            for i in 0..(iters + warmup) {
                all_samples.push(run_fn(&mut stmt, i, scale, total_songs)?);
            }
            let total_secs = total_start.elapsed().as_secs_f64();
            let samples = &mut all_samples[warmup as usize..];
            let (p50, p95, p99, max, mean, stddev, ops) =
                compute_latency_stats(samples, iters, warmup, total_secs);
            report.latency_cases.push(LatencyCaseMetric {
                name: name.to_string(),
                query_shape: qs.to_string(),
                iterations: iters,
                warmup_iterations: warmup,
                p50_ns: p50,
                p95_ns: p95,
                p99_ns: p99,
                max_ns: max,
                mean_ns: mean,
                stddev_ns: stddev,
                operations_per_second: ops,
                rows_per_iteration: Some(1),
                ..Default::default()
            });
            println!("  {name}: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
            Ok(())
        };

    fn artist_full(
        stmt: &mut duckdb::Statement,
        i: u64,
        scale: Scale,
        _total: u64,
    ) -> anyhow::Result<u64> {
        let id = artist_id_for_iter(i, scale);
        let t0 = std::time::Instant::now();
        let _row: (i64, String, String, i64) = stmt.query_row([id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?;
        Ok(elapsed_ns(t0))
    }
    measure(
        "artist_pk_lookup_full_row",
        "SELECT id, name, country, formed_year FROM artists WHERE id = ?",
        "SELECT id, name, country, formed_year FROM artists WHERE id = ?",
        artist_full,
    )?;

    fn artist_name(
        stmt: &mut duckdb::Statement,
        i: u64,
        scale: Scale,
        _total: u64,
    ) -> anyhow::Result<u64> {
        let id = artist_id_for_iter(i, scale);
        let t0 = std::time::Instant::now();
        let _name: String = stmt.query_row([id], |row| row.get(0))?;
        Ok(elapsed_ns(t0))
    }
    measure(
        "artist_pk_lookup_name_only",
        "SELECT name FROM artists WHERE id = ?",
        "SELECT name FROM artists WHERE id = ?",
        artist_name,
    )?;

    // view_artist_filter (heavy)
    {
        let mut stmt = conn.prepare(
            "SELECT album_title, song_title, duration_ms FROM v_artist_songs WHERE artist_id = ?",
        )?;
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for i in 0..(heavy_iters + heavy_warmup) {
            let artist_id = artist_id_for_iter(i, scale);
            let t0 = std::time::Instant::now();
            let mut rows = stmt.query([artist_id])?;
            let mut n = 0usize;
            while let Some(_row) = rows.next()? {
                n += 1;
            }
            black_box(n);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "view_artist_filter".into(),
            query_shape: "SELECT ... FROM v_artist_songs WHERE artist_id = ?".into(),
            iterations: heavy_iters,
            warmup_iterations: heavy_warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            ..Default::default()
        });
        println!("  view_artist_filter: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    // top10_artists_by_songs (heavy)
    {
        let mut stmt = conn.prepare("SELECT a.id, a.name, COUNT(s.id) AS song_count FROM artists a JOIN songs s ON s.artist_id = a.id GROUP BY a.id, a.name ORDER BY song_count DESC LIMIT 10")?;
        let mut all_samples = Vec::with_capacity((heavy_iters + heavy_warmup) as usize);
        let total_start = std::time::Instant::now();
        for _ in 0..(heavy_iters + heavy_warmup) {
            let t0 = std::time::Instant::now();
            let mut rows = stmt.query([])?;
            let mut n = 0usize;
            while let Some(_row) = rows.next()? {
                n += 1;
            }
            black_box(n);
            all_samples.push(elapsed_ns(t0));
        }
        let total_secs = total_start.elapsed().as_secs_f64();
        let samples = &mut all_samples[heavy_warmup as usize..];
        let (p50, p95, p99, max, mean, stddev, ops) =
            compute_latency_stats(samples, heavy_iters, heavy_warmup, total_secs);
        report.latency_cases.push(LatencyCaseMetric {
            name: "top10_artists_by_songs".into(),
            query_shape: "SELECT a.id, a.name, COUNT(s.id) ... GROUP BY ... LIMIT 10".into(),
            iterations: heavy_iters,
            warmup_iterations: heavy_warmup,
            p50_ns: p50,
            p95_ns: p95,
            p99_ns: p99,
            max_ns: max,
            mean_ns: mean,
            stddev_ns: stddev,
            operations_per_second: ops,
            rows_per_iteration: Some(10),
            ..Default::default()
        });
        println!("  top10_artists_by_songs: p50={p50}ns p95={p95}ns ops={ops:.0}/s");
    }

    Ok(())
}

#[cfg(feature = "duckdb")]
#[cfg(feature = "extended-suites")]
fn run_write_suite_duckdb(
    conn: &duckdb::Connection,
    report: &mut RunReport,
    cli: &Cli,
) -> anyhow::Result<()> {
    println!("\n--- Write Suite (DuckDB) ---");
    let n = cli.write_iterations;

    let mut stmt =
        conn.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?, ?, ?)")?;
    let mut samples = Vec::with_capacity(n as usize);
    let total_start = std::time::Instant::now();
    for i in 0..n {
        let start = std::time::Instant::now();
        stmt.execute(duckdb::params![
            2_000_000_000i64 + i as i64,
            1i64,
            format!("autocommit {i}")
        ])?;
        samples.push(elapsed_ns(start));
    }
    let dur = total_start.elapsed().as_secs_f64();
    let (p50, p95, p99, max, mean, stddev, ops) = compute_latency_stats(&mut samples, n, 0, dur);
    report.write_cases.push(latency_metric(
        "durable_insert_autocommit",
        "INSERT INTO write_events ...",
        n,
        0,
        p50,
        p95,
        p99,
        max,
        mean,
        stddev,
        ops,
        None,
    ));
    println!("  durable_insert_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");

    // update
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let mut ins =
        conn.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?, ?, ?)")?;
    for i in 0..n {
        ins.execute(duckdb::params![
            4_000_000_000i64 + i as i64,
            3i64,
            "pre-update"
        ])?;
    }
    conn.execute_batch("COMMIT;")?;
    let mut stmt = conn.prepare("UPDATE write_events SET payload = ? WHERE id = ?")?;
    let mut samples = Vec::with_capacity(n as usize);
    let total_start = std::time::Instant::now();
    for i in 0..n {
        let start = std::time::Instant::now();
        stmt.execute(duckdb::params![
            format!("updated {i}"),
            4_000_000_000i64 + i as i64
        ])?;
        samples.push(elapsed_ns(start));
    }
    let dur = total_start.elapsed().as_secs_f64();
    let (p50, p95, p99, max, mean, stddev, ops) = compute_latency_stats(&mut samples, n, 0, dur);
    report.write_cases.push(latency_metric(
        "update_by_pk_autocommit",
        "UPDATE write_events SET payload = ? WHERE id = ?",
        n,
        0,
        p50,
        p95,
        p99,
        max,
        mean,
        stddev,
        ops,
        None,
    ));
    println!("  update_by_pk_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");

    // delete
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let mut ins =
        conn.prepare("INSERT INTO write_events (id, artist_id, payload) VALUES (?, ?, ?)")?;
    for i in 0..n {
        ins.execute(duckdb::params![
            5_000_000_000i64 + i as i64,
            4i64,
            "pre-delete"
        ])?;
    }
    conn.execute_batch("COMMIT;")?;
    let mut stmt = conn.prepare("DELETE FROM write_events WHERE id = ?")?;
    let mut samples = Vec::with_capacity(n as usize);
    let total_start = std::time::Instant::now();
    for i in 0..n {
        let start = std::time::Instant::now();
        stmt.execute(duckdb::params![5_000_000_000i64 + i as i64])?;
        samples.push(elapsed_ns(start));
    }
    let dur = total_start.elapsed().as_secs_f64();
    let (p50, p95, p99, max, mean, stddev, ops) = compute_latency_stats(&mut samples, n, 0, dur);
    report.write_cases.push(latency_metric(
        "delete_by_pk_autocommit",
        "DELETE FROM write_events WHERE id = ?",
        n,
        0,
        p50,
        p95,
        p99,
        max,
        mean,
        stddev,
        ops,
        None,
    ));
    println!("  delete_by_pk_autocommit: p50={p50}ns p95={p95}ns ops={ops:.0}/s");

    Ok(())
}

fn main() {
    let cli = parse_cli();
    if let Err(e) = run(cli) {
        eprintln!("Error: {e:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compiled_optional_features, decentdb_companion_path, delete_db_files,
        finalize_runner_provenance, format_unix_filename_stamp, parse_scale,
        parse_statm_resident_pages, parse_status_kb, populate_run_report_metadata,
        semantic_checksum, summarize_seed_plan, validate_query_semantics, walk_seed_plan_select,
        BenchmarkEngine, BenchmarkProfile, Cli, RunReport, SeedWalkEmit, SemanticValue,
        BENCHMARK_SCALES, DECENTDB_CHECKPOINT_DURABILITY_CONTRACT, HUGE, SMOKE,
    };
    #[cfg(feature = "extended-suites")]
    use super::{
        format_unix_label, load_report_data, ordered_step_names, HistoricalRun, StepMetric,
    };
    #[cfg(feature = "extended-suites")]
    use clap::{CommandFactory as _, Parser as _};
    #[cfg(feature = "extended-suites")]
    use sha2::{Digest as _, Sha256};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    #[cfg(feature = "extended-suites")]
    fn parse_cli_from<const N: usize>(arguments: [&str; N]) -> Result<Cli, String> {
        Cli::try_parse_from(arguments).map_err(|error| error.to_string())
    }

    #[cfg(not(feature = "extended-suites"))]
    fn parse_cli_from<const N: usize>(arguments: [&str; N]) -> Result<Cli, String> {
        match super::parse_canonical_cli_from(arguments)? {
            super::CanonicalCliAction::Run(cli) => Ok(cli),
            super::CanonicalCliAction::Help => Err("help requested".to_string()),
            super::CanonicalCliAction::Version => Err("version requested".to_string()),
        }
    }

    #[test]
    fn runner_provenance_is_finalized_together_after_measurement() {
        let mut report = RunReport::default();
        assert_eq!(report.seed, 0);
        assert!(report.invocation_argv.is_empty());
        assert!(report.executable_sha256.is_empty());

        finalize_runner_provenance(&mut report, 42).expect("finalize runner provenance");
        assert_eq!(report.seed, 42);
        assert!(!report.invocation_argv.is_empty());
        assert_eq!(report.executable_sha256.len(), 64);
        assert!(report
            .executable_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()));
    }

    #[test]
    fn statm_parser_accepts_linux_whitespace_and_multi_digit_values() {
        assert_eq!(
            parse_statm_resident_pages(b"123456 98765 4321 7 8 9 10\n"),
            Some(98_765)
        );
        assert_eq!(
            parse_statm_resident_pages(b"\t123456\t  98765\r\n"),
            Some(98_765)
        );
    }

    #[test]
    fn statm_parser_rejects_missing_malformed_and_overflow_values() {
        assert_eq!(parse_statm_resident_pages(b"123456\n"), None);
        assert_eq!(parse_statm_resident_pages(b"123456 resident\n"), None);
        assert_eq!(
            parse_statm_resident_pages(b"123456 18446744073709551616\n"),
            None
        );
    }

    #[test]
    fn status_parser_finds_exact_fields_independent_of_order() {
        let first = b"Name:\trust-baseline\nRssFile:\t12345 kB\nRssAnon:      6789 kB\n";
        let second = b"RssAnon:\t6789 kB\nVmRSS:\t99999 kB\nRssFile: 12345 kB\n";
        for fixture in [first.as_slice(), second.as_slice()] {
            assert_eq!(parse_status_kb(fixture, b"RssAnon:"), Some(6_789));
            assert_eq!(parse_status_kb(fixture, b"RssFile:"), Some(12_345));
        }
        assert_eq!(parse_status_kb(first, b"VmRSS:"), None);
        assert_eq!(parse_status_kb(b"RssAnon: unknown kB\n", b"RssAnon:"), None);
        assert_eq!(
            parse_status_kb(b"RssAnon: 18446744073709551616 kB\n", b"RssAnon:"),
            None
        );
    }

    #[test]
    fn semantic_checksum_is_row_order_insensitive_and_type_aware() {
        let rows = vec![
            vec![SemanticValue::Int64(1), SemanticValue::Text("one".into())],
            vec![SemanticValue::Int64(2), SemanticValue::Text("two".into())],
        ];
        let mut reversed = rows.clone();
        reversed.reverse();
        assert_eq!(semantic_checksum(&rows), semantic_checksum(&reversed));

        let mut different_type = rows;
        different_type[0][0] = SemanticValue::Float64(1.0);
        assert_ne!(
            semantic_checksum(&reversed),
            semantic_checksum(&different_type)
        );
    }

    #[test]
    fn top10_semantic_invariants_reject_duplicates_and_bad_order() {
        let mut rows = (1_i64..=10)
            .map(|id| {
                vec![
                    SemanticValue::Int64(id),
                    SemanticValue::Text(format!("Artist {id}")),
                    SemanticValue::Int64(20 - id),
                ]
            })
            .collect::<Vec<_>>();
        assert!(
            validate_query_semantics("query_top10_artists_by_songs", &rows, SMOKE, 27_783,).is_ok()
        );

        rows[1][0] = SemanticValue::Int64(1);
        rows[1][1] = SemanticValue::Text("Artist 1".into());
        assert!(
            validate_query_semantics("query_top10_artists_by_songs", &rows, SMOKE, 27_783,)
                .is_err()
        );

        rows[1][0] = SemanticValue::Int64(2);
        rows[1][1] = SemanticValue::Text("Artist 2".into());
        rows[1][2] = SemanticValue::Int64(99);
        assert!(
            validate_query_semantics("query_top10_artists_by_songs", &rows, SMOKE, 27_783,)
                .is_err()
        );
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let id = NEXT_TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "decentdb-rust-baseline-{label}-{}-{id}",
                std::process::id()
            ));
            std::fs::create_dir(&path).expect("create benchmark runner test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn delete_db_files_removes_stale_decentdb_sidecars() {
        let directory = TestDirectory::new("cleanup-sidecars");
        let db_path = directory.path().join("benchmark.ddb");
        let artifacts = [
            db_path.clone(),
            decentdb_companion_path(&db_path, ".wal"),
            decentdb_companion_path(&db_path, ".coord"),
            decentdb_companion_path(&db_path, ".wal-idx"),
            directory.path().join("benchmark.ddb-wal"),
            directory.path().join("benchmark.ddb-shm"),
        ];
        for artifact in &artifacts {
            std::fs::write(artifact, b"stale").expect("create stale database artifact");
        }

        delete_db_files(&db_path);

        for artifact in artifacts {
            assert!(
                !artifact.exists(),
                "stale artifact was not removed: {}",
                artifact.display()
            );
        }
    }

    #[cfg(feature = "extended-suites")]
    #[test]
    fn html_report_merges_verified_manifest_and_provisional_results() {
        let directory = TestDirectory::new("manifest-report-inputs");
        let accepted_name = "accepted-rust-baseline-default-smoke.json";
        let provisional_name = "provisional-rust-baseline-default-smoke.json";
        let junk_name = "junk-rust-baseline-default-smoke.json";

        let accepted = RunReport {
            scale_name: "smoke".into(),
            started_unix: 1,
            ..RunReport::default()
        };
        let accepted_json = serde_json::to_vec_pretty(&accepted).expect("serialize accepted run");
        std::fs::write(directory.path().join(accepted_name), &accepted_json)
            .expect("write accepted run");

        let mut provisional = accepted.clone();
        provisional.started_unix = 2;
        std::fs::write(
            directory.path().join(provisional_name),
            serde_json::to_vec_pretty(&provisional).expect("serialize provisional run"),
        )
        .expect("write provisional run");

        // An unparseable local file (for example a truncated run write) is
        // skipped with a warning instead of taking down the whole report.
        std::fs::write(directory.path().join(junk_name), b"not json").expect("write junk run");

        let accepted_sha256 = super::lower_hex(&Sha256::digest(&accepted_json));
        let manifest = serde_json::json!({
            "schema_version": 1,
            "files": [{"path": accepted_name, "sha256": accepted_sha256}],
        });
        std::fs::write(
            directory.path().join("history-manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("serialize history manifest"),
        )
        .expect("write history manifest");

        let report = load_report_data(directory.path()).expect("load manifested report data");
        assert!(report.has_manifest);
        assert_eq!(report.total_runs, 2);
        assert_eq!(report.verified_runs, 1);
        assert_eq!(report.provisional_runs, 1);
        assert_eq!(report.skipped_inputs, vec![junk_name.to_string()]);
        assert_eq!(report.scales[0].runs.len(), 2);
        assert_eq!(report.scales[0].runs[0].file_name, accepted_name);
        assert!(!report.scales[0].runs[0].provisional);
        assert_eq!(report.scales[0].runs[1].file_name, provisional_name);
        assert!(report.scales[0].runs[1].provisional);

        std::fs::write(directory.path().join(accepted_name), b"tampered")
            .expect("tamper accepted run");
        let error = match load_report_data(directory.path()) {
            Ok(_) => panic!("checksum mismatch must fail"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("SHA-256 mismatch"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn delete_db_files_preserves_unrelated_files() {
        let directory = TestDirectory::new("cleanup-preserves-unrelated");
        let db_path = directory.path().join("benchmark.ddb");
        let unrelated = [
            directory.path().join("benchmark.ddb.backup"),
            directory.path().join("benchmark.ddb.coord.backup"),
            directory.path().join("other.ddb.coord"),
        ];
        std::fs::write(&db_path, b"database").expect("create database artifact");
        for file in &unrelated {
            std::fs::write(file, b"keep").expect("create unrelated file");
        }

        delete_db_files(&db_path);

        assert!(!db_path.exists(), "database artifact should be removed");
        for file in unrelated {
            assert_eq!(
                std::fs::read(&file).expect("read preserved unrelated file"),
                b"keep",
                "unrelated file was modified: {}",
                file.display()
            );
        }
    }

    #[test]
    fn engine_cli_always_accepts_decentdb() {
        let cli = parse_cli_from(["rust-baseline", "--engine", "decentdb"])
            .expect("DecentDB must be available in the canonical binary");

        assert_eq!(cli.engine, BenchmarkEngine::DecentDb);
    }

    #[cfg(feature = "extended-suites")]
    #[test]
    fn extended_cli_uses_canonical_command_name() {
        assert_eq!(Cli::command().get_name(), "rust-baseline");
    }

    #[cfg(all(unix, not(feature = "extended-suites")))]
    #[test]
    fn canonical_cli_preserves_non_utf8_database_paths() {
        use std::os::unix::ffi::{OsStrExt as _, OsStringExt as _};

        let raw_path = std::ffi::OsString::from_vec(b"database-\xff.ddb".to_vec());
        let arguments = vec![
            std::ffi::OsString::from("rust-baseline"),
            std::ffi::OsString::from("--db-path"),
            raw_path.clone(),
        ];
        let super::CanonicalCliAction::Run(cli) =
            super::parse_canonical_cli_from(arguments).expect("parse non-UTF-8 path")
        else {
            panic!("expected a runnable CLI action");
        };
        assert_eq!(
            cli.db_path.expect("database path").as_os_str().as_bytes(),
            raw_path.as_os_str().as_bytes()
        );
    }

    #[cfg(not(feature = "extended-suites"))]
    #[test]
    fn extended_cli_flags_are_rejected_without_feature() {
        for flag in [
            "--report",
            "--benchmark",
            "--plan-cache-benchmark",
            "--latency-suite",
            "--concurrency-suite",
            "--write-suite",
            "--cold-suite",
        ] {
            let error = parse_cli_from(["rust-baseline", flag])
                .expect_err("extended flag must not be exposed by the canonical binary");
            assert!(
                error.to_string().contains("unexpected argument"),
                "unexpected error for {flag}: {error}"
            );
        }
    }

    #[cfg(feature = "extended-suites")]
    #[test]
    fn extended_cli_flags_are_accepted_with_feature() {
        for flag in [
            "--report",
            "--benchmark",
            "--plan-cache-benchmark",
            "--latency-suite",
            "--concurrency-suite",
            "--write-suite",
            "--cold-suite",
        ] {
            parse_cli_from(["rust-baseline", flag])
                .unwrap_or_else(|error| panic!("extended flag {flag} was rejected: {error}"));
        }
    }

    #[test]
    fn canonical_json_keeps_empty_optional_suite_arrays() {
        let json = serde_json::to_value(RunReport::default()).expect("serialize run report");

        for field in [
            "latency_cases",
            "concurrency_cases",
            "write_cases",
            "cold_cases",
        ] {
            assert_eq!(json[field], serde_json::json!([]), "field {field}");
        }
    }

    #[test]
    fn report_records_exact_sorted_compiled_optional_features() {
        let features = compiled_optional_features();
        let mut sorted = features.clone();
        sorted.sort_unstable();

        assert_eq!(features, sorted, "feature provenance must be sorted");
        assert_eq!(
            features.len(),
            cfg!(feature = "duckdb") as usize
                + cfg!(feature = "extended-suites") as usize
                + cfg!(feature = "lua-extensions") as usize
                + cfg!(feature = "sqlite") as usize,
            "only enabled optional benchmark features may be reported"
        );
        assert_eq!(
            features.iter().any(|feature| feature == "duckdb"),
            cfg!(feature = "duckdb")
        );
        assert_eq!(
            features.iter().any(|feature| feature == "extended-suites"),
            cfg!(feature = "extended-suites")
        );
        assert_eq!(
            features.iter().any(|feature| feature == "lua-extensions"),
            cfg!(feature = "lua-extensions")
        );
        assert_eq!(
            features.iter().any(|feature| feature == "sqlite"),
            cfg!(feature = "sqlite")
        );

        let mut report = RunReport::default();
        populate_run_report_metadata(
            &mut report,
            BenchmarkEngine::DecentDb,
            BenchmarkProfile::Default,
        );
        assert_eq!(report.compiled_optional_features, features);
        assert_eq!(
            report.checkpoint_durability_contract,
            DECENTDB_CHECKPOINT_DURABILITY_CONTRACT
        );
    }

    #[cfg(not(any(
        feature = "duckdb",
        feature = "extended-suites",
        feature = "lua-extensions",
        feature = "sqlite"
    )))]
    #[test]
    fn canonical_default_report_has_no_compiled_optional_features() {
        let mut report = RunReport::default();
        populate_run_report_metadata(
            &mut report,
            BenchmarkEngine::DecentDb,
            BenchmarkProfile::Default,
        );

        let json = serde_json::to_value(report).expect("serialize canonical report");
        assert_eq!(json["compiled_optional_features"], serde_json::json!([]));
        assert_eq!(
            json["checkpoint_durability_contract"],
            DECENTDB_CHECKPOINT_DURABILITY_CONTRACT
        );
    }

    #[test]
    fn reports_without_build_and_durability_provenance_remain_parseable() {
        let mut json = serde_json::to_value(RunReport::default()).expect("serialize old report");
        let object = json
            .as_object_mut()
            .expect("run report must serialize as an object");
        object.remove("compiled_optional_features");
        object.remove("checkpoint_durability_contract");

        let report: RunReport = serde_json::from_value(json).expect("parse old report JSON");
        assert!(report.compiled_optional_features.is_empty());
        assert!(report.checkpoint_durability_contract.is_empty());
    }

    #[cfg(not(feature = "sqlite"))]
    #[test]
    fn engine_cli_rejects_sqlite_without_feature() {
        let error = parse_cli_from(["rust-baseline", "--engine", "sqlite"])
            .expect_err("SQLite must not be exposed without the sqlite feature");

        assert!(error.to_string().contains("invalid value 'sqlite'"));
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn engine_cli_accepts_sqlite_with_feature() {
        let cli = parse_cli_from(["rust-baseline", "--engine", "sqlite"])
            .expect("SQLite must be exposed by the sqlite feature");

        assert_eq!(cli.engine, BenchmarkEngine::Sqlite);
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_profile_accepts_known_values_with_feature() {
        let cli = parse_cli_from([
            "rust-baseline",
            "--engine",
            "sqlite",
            "--sqlite-profile",
            "wal-normal",
        ])
        .expect("SQLite wal-normal profile must be accepted by the sqlite feature");

        assert_eq!(cli.sqlite_profile, Some(super::SqliteProfile::WalNormal));
        super::validate_engine_profile(&cli).expect("sqlite wal-normal profile should validate");
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_profile_rejects_unknown_values_with_feature() {
        let error = parse_cli_from([
            "rust-baseline",
            "--engine",
            "sqlite",
            "--sqlite-profile",
            "wal-nromal",
        ])
        .expect_err("unknown SQLite profiles must fail at parse time");

        let message = error.to_string();
        assert!(
            message.contains("invalid value"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("wal-normal"),
            "unexpected error: {message}"
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_profile_rejects_non_sqlite_engine_with_feature() {
        let cli = parse_cli_from([
            "rust-baseline",
            "--engine",
            "decentdb",
            "--sqlite-profile",
            "wal-normal",
        ])
        .expect("known SQLite profile should parse before validation");

        let error = super::validate_engine_profile(&cli)
            .expect_err("SQLite profile must be scoped to the SQLite engine");
        assert!(
            error
                .to_string()
                .contains("--sqlite-profile is only supported for --engine sqlite"),
            "unexpected error: {error}"
        );
    }

    #[cfg(not(feature = "duckdb"))]
    #[test]
    fn engine_cli_rejects_duckdb_without_feature() {
        let error = parse_cli_from(["rust-baseline", "--engine", "duckdb"])
            .expect_err("DuckDB must not be exposed without the duckdb feature");

        assert!(error.to_string().contains("invalid value 'duckdb'"));
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn engine_cli_accepts_duckdb_with_feature() {
        let cli = parse_cli_from(["rust-baseline", "--engine", "duckdb"])
            .expect("DuckDB must be exposed by the duckdb feature");

        assert_eq!(cli.engine, BenchmarkEngine::DuckDb);
    }

    #[cfg(feature = "duckdb")]
    #[test]
    fn duckdb_rejects_decentdb_profile_with_feature() {
        let cli = parse_cli_from([
            "rust-baseline",
            "--engine",
            "duckdb",
            "--profile",
            "resident-hot-read",
        ])
        .expect("DuckDB profile rejection happens after CLI parsing");

        let error = super::validate_engine_profile(&cli)
            .expect_err("DuckDB must reject DecentDB-only profiles");
        assert!(
            error
                .to_string()
                .contains("--profile is only supported for --engine decentdb"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_scale_supports_huge() {
        let scale = parse_scale("huge");

        assert_eq!(scale.name, HUGE.name);
        assert_eq!(scale.artists, HUGE.artists);
        assert_eq!(scale.albums, HUGE.albums);
        assert_eq!(scale.songs_cap, HUGE.songs_cap);
    }

    #[test]
    fn benchmark_scales_run_in_expected_order() {
        let names: Vec<_> = BENCHMARK_SCALES.iter().map(|scale| scale.name).collect();

        assert_eq!(names, vec!["smoke", "medium", "full", "huge"]);
    }

    #[test]
    fn seed_summary_matches_known_smoke_plan() {
        let summary = summarize_seed_plan(SMOKE, 42);

        assert_eq!(summary.total_albums, 5_000);
        assert_eq!(summary.total_songs, 27_783);
    }

    #[test]
    fn selective_seed_walk_preserves_emitted_rows() {
        let mut full_artists = Vec::new();
        let mut full_albums = Vec::new();
        let mut full_songs = Vec::new();
        walk_seed_plan_select(
            SMOKE,
            42,
            SeedWalkEmit {
                artists: true,
                albums: true,
                songs: true,
            },
            |row| full_artists.push(row),
            |row| full_albums.push(row),
            |row| full_songs.push(row),
        );

        let mut artists = Vec::new();
        walk_seed_plan_select(
            SMOKE,
            42,
            SeedWalkEmit::ARTISTS,
            |row| artists.push(row),
            |_| {},
            |_| {},
        );
        assert_eq!(artists, full_artists);

        let mut albums = Vec::new();
        walk_seed_plan_select(
            SMOKE,
            42,
            SeedWalkEmit::ALBUMS,
            |_| {},
            |row| albums.push(row),
            |_| {},
        );
        assert_eq!(albums, full_albums);

        let mut songs = Vec::new();
        walk_seed_plan_select(
            SMOKE,
            42,
            SeedWalkEmit::SONGS,
            |_| {},
            |_| {},
            |row| songs.push(row),
        );
        assert_eq!(songs, full_songs);
    }

    #[test]
    fn unix_timestamp_formatting_is_utc() {
        assert_eq!(format_unix_filename_stamp(1_779_193_075), "2026-05-19-1217");
    }

    #[cfg(feature = "extended-suites")]
    #[test]
    fn report_timestamp_formatting_is_utc() {
        assert_eq!(format_unix_label(1_779_193_075), "2026-05-19 12:17:55 UTC");
    }

    #[cfg(feature = "extended-suites")]
    #[test]
    fn ordered_step_names_uses_benchmark_order() {
        let run = HistoricalRun::new(
            "sample.json".to_string(),
            false,
            RunReport {
                scale_name: "full".to_string(),
                steps: vec![
                    StepMetric {
                        name: "query_view_first_1000".to_string(),
                        ..Default::default()
                    },
                    StepMetric {
                        name: "seed_songs".to_string(),
                        ..Default::default()
                    },
                    StepMetric {
                        name: "checkpoint_after_seed".to_string(),
                        ..Default::default()
                    },
                    StepMetric {
                        name: "schema_create".to_string(),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        );

        assert_eq!(
            ordered_step_names(&[run]),
            vec![
                "schema_create".to_string(),
                "seed_songs".to_string(),
                "checkpoint_after_seed".to_string(),
                "query_view_first_1000".to_string()
            ]
        );
    }
}
