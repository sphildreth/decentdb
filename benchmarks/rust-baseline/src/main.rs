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

use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context};
use clap::{Parser, ValueEnum};
use decentdb::{DbConfig, PreparedStatement, Value};
use rusqlite::{params, Connection as SqliteConnection};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(version, about = "DecentDB rust-baseline benchmark")]
struct Cli {
    /// Scale: smoke | medium | full | huge
    #[arg(long, default_value = "smoke")]
    scale: String,
    /// Output directory for JSON report.
    #[arg(long, default_value = "results")]
    out_dir: PathBuf,
    /// Database path (defaults by engine and scale).
    #[arg(long)]
    db_path: Option<PathBuf>,
    /// Seed for the deterministic plan.
    #[arg(long, default_value_t = 42u64)]
    seed: u64,
    /// Engine profile: default | resident-hot-read.
    #[arg(long, value_enum, default_value_t = BenchmarkProfile::Default)]
    profile: BenchmarkProfile,
    /// Engine implementation: decentdb | sqlite.
    #[arg(long, value_enum, default_value_t = BenchmarkEngine::DecentDb)]
    engine: BenchmarkEngine,
    /// Generate an HTML report from historical JSON files in the output directory.
    #[arg(long)]
    report: bool,
    /// Run all scales in order (smoke, medium, full, huge), then generate the HTML report.
    #[arg(long)]
    benchmark: bool,
    /// HTML output path for --report or --benchmark (defaults to <out-dir>/report.html).
    #[arg(long)]
    report_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum BenchmarkEngine {
    #[value(name = "decentdb", alias = "decent-db")]
    DecentDb,
    Sqlite,
}

impl BenchmarkEngine {
    fn binding_name(self) -> &'static str {
        match self {
            Self::DecentDb => "RustRaw",
            Self::Sqlite => "SQLiteRusqlite",
        }
    }

    fn default_db_path(self, scale: Scale) -> PathBuf {
        match self {
            Self::DecentDb => PathBuf::from(format!("run-rust-{}.ddb", scale.name)),
            Self::Sqlite => PathBuf::from(format!("run-rust-sqlite-{}.db", scale.name)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
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

fn default_benchmark_profile() -> String {
    "default".to_string()
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
}

#[derive(Clone, Serialize)]
struct HistoricalRun {
    file_name: String,
    timestamp_unix: u64,
    timestamp_label: String,
    total_runtime_seconds: f64,
    report: RunReport,
}

#[derive(Serialize)]
struct ReportScaleSection {
    scale_name: String,
    step_names: Vec<String>,
    runs: Vec<HistoricalRun>,
}

#[derive(Serialize)]
struct HtmlReportData {
    generated_at_unix: u64,
    generated_at_label: String,
    source_directory: String,
    total_runs: usize,
    scales: Vec<ReportScaleSection>,
}

fn read_rss_bytes() -> u64 {
    // /proc/self/statm: size resident shared text lib data dt
    // resident is in pages.
    if let Ok(s) = fs::read_to_string("/proc/self/statm") {
        if let Some(resident) = s.split_whitespace().nth(1) {
            if let Ok(pages) = resident.parse::<u64>() {
                let page = unsafe { libc_sysconf_pagesize() };
                return pages * page;
            }
        }
    }
    0
}

fn read_proc_status_kb(field: &str) -> Option<u64> {
    let s = fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if line.starts_with(field) {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return parts[1].parse::<u64>().ok();
            }
        }
    }
    None
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
        let rss = read_rss_bytes();
        let rss_anon_kb = read_proc_status_kb("RssAnon:").unwrap_or(0);
        let rss_file_kb = read_proc_status_kb("RssFile:").unwrap_or(0);
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

fn delete_db_files(path: &Path) {
    let _ = fs::remove_file(path);
    // DecentDB writes <db>.wal as the WAL companion (per the engine's WAL
    // suffix convention used elsewhere in the workspace).
    let _ = fs::remove_file(decentdb_wal_path(path));
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

fn decentdb_wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn run(cli: Cli) -> anyhow::Result<()> {
    if cli.report_file.is_some() && !cli.report && !cli.benchmark {
        bail!("--report-file requires --report or --benchmark");
    }

    if cli.benchmark {
        return run_benchmark_suite(&cli);
    }

    if cli.report {
        generate_report_from_cli(&cli)?;
        return Ok(());
    }

    let scale = parse_scale(&cli.scale);
    run_single_benchmark(&cli, scale)
}

fn run_benchmark_suite(cli: &Cli) -> anyhow::Result<()> {
    if cli.db_path.is_some() {
        bail!("--db-path is not supported with --benchmark; each scale uses its own database path");
    }
    if cli.engine == BenchmarkEngine::Sqlite && cli.profile != BenchmarkProfile::Default {
        bail!("--profile is only supported for --engine decentdb");
    }

    println!(
        "Running rust-baseline benchmark suite: engine={:?} profile={} scales=smoke,medium,full,huge",
        cli.engine,
        if cli.engine == BenchmarkEngine::Sqlite {
            "sqlite-wal-full"
        } else {
            cli.profile.as_str()
        }
    );
    for scale in BENCHMARK_SCALES {
        println!("\n=== scale: {} ===", scale.name);
        run_single_benchmark(cli, scale)?;
    }
    generate_report_from_cli(cli)?;
    Ok(())
}

fn generate_report_from_cli(cli: &Cli) -> anyhow::Result<()> {
    let report_file = cli
        .report_file
        .clone()
        .unwrap_or_else(|| cli.out_dir.join("report.html"));
    generate_html_report(&cli.out_dir, &report_file)?;
    println!("Wrote {}", report_file.display());
    Ok(())
}

fn run_single_benchmark(cli: &Cli, scale: Scale) -> anyhow::Result<()> {
    if cli.engine == BenchmarkEngine::Sqlite && cli.profile != BenchmarkProfile::Default {
        bail!("--profile is only supported for --engine decentdb");
    }
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

    println!(
        "Summarizing seed plan: engine={:?} scale={} artists={} albums(target)={} songs_cap={}",
        engine, scale.name, scale.artists, scale.albums, scale.songs_cap
    );
    let summary = summarize_seed_plan(scale, seed);
    println!(
        "Plan: artists={} total_albums={} total_songs={}",
        scale.artists, summary.total_albums, summary.total_songs
    );

    if engine == BenchmarkEngine::Sqlite {
        return run_sqlite_benchmark(scale, seed, summary, db_path, out_dir);
    }

    delete_db_files(&db_path);

    let mut report = RunReport {
        binding: engine.binding_name().to_string(),
        scale_name: scale.name.to_string(),
        benchmark_profile: profile.as_str().to_string(),
        target_artists: scale.artists,
        target_albums: scale.albums,
        target_songs_cap: scale.songs_cap,
        started_unix: now_unix(),
        engine_version: decentdb::version().to_string(),
        database_path: db_path.display().to_string(),
        ..Default::default()
    };
    let mut rec = Recorder::new(&mut report);

    let db = rec.measure("connect_open", None, || {
        decentdb::Db::create(&db_path, profile.db_config()).expect("Db::create")
    });

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
    rec.measure("query_count_songs", None, || {
        let r = db.execute("SELECT COUNT(*) FROM songs").expect("count");
        let v = first_value(&r);
        println!("    count={v:?}");
    });
    let v = scalar_int(&db.execute("SELECT COUNT(*) FROM songs").unwrap());
    rec.add_extra("count", serde_json::json!(v));

    rec.measure("query_aggregate_durations", None, || {
        let r = db
            .execute(
                "SELECT COUNT(*), SUM(duration_ms), AVG(duration_ms), \
                       MIN(duration_ms), MAX(duration_ms) FROM songs",
            )
            .expect("agg");
        if let Some(row) = r.rows().first() {
            println!("    agg_row={row:?}");
        }
    });

    rec.measure("query_artist_by_id", None, || {
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
    });

    rec.measure("query_top10_artists_by_songs", None, || {
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
    });

    rec.measure("query_top10_albums_by_songs", None, || {
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
    });

    rec.measure("query_view_first_1000", None, || {
        let r = db
            .execute(
                "SELECT artist_id, artist_name, album_title, song_title \
                 FROM v_artist_songs LIMIT 1000",
            )
            .expect("view 1000");
        println!("    rows={}", r.rows().len());
    });

    rec.measure("query_songs_for_artist_via_view", None, || {
        let r = db
            .execute_with_params(
                "SELECT album_title, song_title, duration_ms \
                 FROM v_artist_songs WHERE artist_id = $1",
                &[Value::Int64(1)],
            )
            .expect("artist 1 view");
        println!("    rows={}", r.rows().len());
    });

    drop(db);

    if let Ok(meta) = fs::metadata(&db_path) {
        report.database_size_bytes = meta.len();
    }
    report.wal_size_bytes = file_size(&decentdb_wal_path(&db_path));
    report.finished_unix = now_unix();

    fs::create_dir_all(&out_dir)?;
    let datetime_stamp = format_unix_filename_stamp(report.finished_unix);
    let out_path = out_dir.join(format!(
        "{datetime_stamp}-rust-baseline-{}-{}.json",
        report.benchmark_profile.as_str(),
        scale.name
    ));
    fs::write(&out_path, serde_json::to_string_pretty(&report)?)?;
    println!("\nWrote {}", out_path.display());

    delete_db_files(&db_path);
    println!("Cleaned up temp DB files: {}", db_path.display());

    Ok(())
}

fn run_sqlite_benchmark(
    scale: Scale,
    seed: u64,
    summary: SeedSummary,
    db_path: PathBuf,
    out_dir: PathBuf,
) -> anyhow::Result<()> {
    delete_db_files(&db_path);

    let mut report = RunReport {
        binding: BenchmarkEngine::Sqlite.binding_name().to_string(),
        scale_name: scale.name.to_string(),
        benchmark_profile: "sqlite-wal-full".to_string(),
        target_artists: scale.artists,
        target_albums: scale.albums,
        target_songs_cap: scale.songs_cap,
        started_unix: now_unix(),
        database_path: db_path.display().to_string(),
        ..Default::default()
    };
    let mut rec = Recorder::new(&mut report);

    let conn = rec.measure("connect_open", None, || {
        open_sqlite_wal_full(&db_path).expect("open sqlite")
    });
    rec.report.engine_version = sqlite_engine_version(&conn).unwrap_or_else(|_| "unknown".into());

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

    rec.measure("query_count_songs", None, || {
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM songs", [], |row| row.get(0))
            .expect("sqlite count");
        println!("    count=Some(Int64({count}))");
    });
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM songs", [], |row| row.get(0))
        .expect("sqlite count extra");
    rec.add_extra("count", serde_json::json!(count));

    rec.measure("query_aggregate_durations", None, || {
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
    });

    rec.measure("query_artist_by_id", None, || {
        let target = i64::from(scale.artists) / 2 + 1;
        let row: (i64, String, String, i64) = conn
            .query_row(
                "SELECT id, name, country, formed_year FROM artists WHERE id = ?1",
                params![target],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .expect("sqlite artist by id");
        println!("    artist={row:?}");
    });

    rec.measure("query_top10_artists_by_songs", None, || {
        let rows = sqlite_query_row_count(
            &conn,
            "SELECT a.id, a.name, COUNT(s.id) AS song_count
             FROM artists a
             JOIN songs s ON s.artist_id = a.id
             GROUP BY a.id, a.name
             ORDER BY song_count DESC
             LIMIT 10",
            [],
        )
        .expect("sqlite top10 artists");
        println!("    rows={rows}");
    });

    rec.measure("query_top10_albums_by_songs", None, || {
        let rows = sqlite_query_row_count(
            &conn,
            "SELECT al.id, al.title, COUNT(s.id) AS song_count
             FROM albums al
             JOIN songs s ON s.album_id = al.id
             GROUP BY al.id, al.title
             ORDER BY song_count DESC
             LIMIT 10",
            [],
        )
        .expect("sqlite top10 albums");
        println!("    rows={rows}");
    });

    rec.measure("query_view_first_1000", None, || {
        let rows = sqlite_query_row_count(
            &conn,
            "SELECT artist_id, artist_name, album_title, song_title \
             FROM v_artist_songs LIMIT 1000",
            [],
        )
        .expect("sqlite view 1000");
        println!("    rows={rows}");
    });

    rec.measure("query_songs_for_artist_via_view", None, || {
        let rows = sqlite_query_row_count(
            &conn,
            "SELECT album_title, song_title, duration_ms \
             FROM v_artist_songs WHERE artist_id = ?1",
            params![1_i64],
        )
        .expect("sqlite artist 1 view");
        println!("    rows={rows}");
    });

    if let Ok(meta) = fs::metadata(&db_path) {
        rec.report.database_size_bytes = meta.len();
    }
    rec.report.wal_size_bytes = file_size(&sqlite_wal_path(&db_path));
    rec.report.finished_unix = now_unix();

    fs::create_dir_all(&out_dir)?;
    let datetime_stamp = format_unix_filename_stamp(rec.report.finished_unix);
    let out_path = out_dir.join(format!(
        "{datetime_stamp}-rust-baseline-{}-{}.json",
        rec.report.benchmark_profile.as_str(),
        scale.name
    ));
    fs::write(&out_path, serde_json::to_string_pretty(&rec.report)?)?;
    println!("\nWrote {}", out_path.display());

    drop(conn);
    delete_db_files(&db_path);
    println!("Cleaned up temp DB files: {}", db_path.display());

    Ok(())
}

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

fn sqlite_engine_version(conn: &SqliteConnection) -> rusqlite::Result<String> {
    conn.query_row("SELECT sqlite_version()", [], |row| row.get(0))
}

fn sqlite_checkpoint_truncate(conn: &SqliteConnection) -> rusqlite::Result<(i64, i64, i64)> {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })
}

fn sqlite_wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    PathBuf::from(wal)
}

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
fn scalar_int(r: &decentdb::QueryResult) -> i64 {
    match first_value(r) {
        Some(Value::Int64(i)) => i,
        _ => 0,
    }
}

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

fn load_report_data(results_dir: &Path) -> anyhow::Result<HtmlReportData> {
    let mut runs = Vec::new();
    let dir = fs::read_dir(results_dir)
        .with_context(|| format!("failed to read results directory {}", results_dir.display()))?;

    for entry in dir {
        let entry = entry
            .with_context(|| format!("failed to read an entry in {}", results_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        if !file_name.contains("rust-baseline") {
            continue;
        }

        let json = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let report: RunReport = serde_json::from_str(&json)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        runs.push(HistoricalRun::new(file_name, report));
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
        scales: sections,
    })
}

impl HistoricalRun {
    fn new(file_name: String, report: RunReport) -> Self {
        let timestamp_unix = report.started_unix;
        let total_runtime_seconds = report.steps.iter().map(|step| step.duration_seconds).sum();
        Self {
            file_name,
            timestamp_unix,
            timestamp_label: format_unix_label(timestamp_unix),
            total_runtime_seconds,
            report,
        }
    }
}

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

fn scale_rank(name: &str) -> usize {
    match name {
        "smoke" => 0,
        "medium" => 1,
        "full" => 2,
        "huge" => 3,
        _ => usize::MAX,
    }
}

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
    <p class="meta">Generated {generated_at} from <code>{source_directory}</code> using {total_runs} historical run(s).</p>
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

    function formatDateLabel(unix) {{
      if (!unix) return '—';
      return new Date(unix * 1000).toLocaleString();
    }}

    function getStep(run, stepName) {{
      return run.report.steps.find(step => step.name === stepName) || null;
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

      const meta = document.createElement('p');
      meta.className = 'section-meta';
      meta.textContent = `Runs: ${{scale.runs.length}} · Latest: ${{latest.timestamp_label}}`;
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
          </tr>
        </thead>
        <tbody></tbody>
      `;
      const runBody = runTable.querySelector('tbody');
      for (const run of scale.runs) {{
        const row = document.createElement('tr');
        row.innerHTML = `
          <td class="mono">${{run.file_name}}</td>
          <td>${{run.timestamp_label}}</td>
          <td>${{run.report.engine_version}}</td>
          <td>${{formatSeconds(run.total_runtime_seconds)}}</td>
          <td>${{formatBytes(run.report.peak_rss_bytes)}}</td>
          <td>${{formatBytes(run.report.database_size_bytes)}}</td>
          <td>${{formatBytes(run.report.wal_size_bytes)}}</td>
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
        total_runs = data.total_runs,
        report_json = report_json,
    );
    Ok(html)
}

fn safe_json_for_html(json: String) -> String {
    json.replace("</", "<\\/")
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("Error: {e:?}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        format_unix_filename_stamp, format_unix_label, ordered_step_names, parse_scale,
        summarize_seed_plan, walk_seed_plan_select, HistoricalRun, RunReport, SeedWalkEmit,
        StepMetric, BENCHMARK_SCALES, HUGE, SMOKE,
    };

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
        assert_eq!(format_unix_label(1_779_193_075), "2026-05-19 12:17:55 UTC");
    }

    #[test]
    fn ordered_step_names_uses_benchmark_order() {
        let run = HistoricalRun::new(
            "sample.json".to_string(),
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
