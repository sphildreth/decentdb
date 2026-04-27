// DecentDB raw-engine baseline benchmark.
//
// Mirrors the schema/queries used by the .NET AdoNet/MicroOrm/EfCore benchmark
// suite at /tmp/tmp-opus47-decentdb-net-tests, but skips every layer above the
// `decentdb` crate so the numbers represent the engine's theoretical ceiling.
//
// Hot-path pattern (identical to the internal `decentdb-benchmark` scenarios):
//   1. db.transaction()          -> SqlTransaction (exclusive runtime state)
//   2. txn.prepare("INSERT ...") -> PreparedStatement (parsed once)
//   3. prepared.execute_in(&mut txn, &[Value::..., ...])   -- per row
//   4. txn.commit()              -- single WAL commit per batch
//
// Scales mirror DecentDB.Compare.Common.Scale exactly.
//
// Output: pretty-printed JSON to results/<datetime>-rust-baseline-<scale>.json with the
// same shape as RunReport so it can be diffed against the .NET reports.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{bail, Context};
use chrono::{TimeZone, Utc};
use clap::Parser;
use decentdb::{DbConfig, PreparedStatement, Value};
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(version, about = "DecentDB raw-engine baseline benchmark")]
struct Cli {
    /// Scale: smoke | medium | full | huge
    #[arg(long, default_value = "smoke")]
    scale: String,
    /// Output directory for JSON report.
    #[arg(long, default_value = "results")]
    out_dir: PathBuf,
    /// Database path (defaults to ./run-rust-<scale>.ddb).
    #[arg(long)]
    db_path: Option<PathBuf>,
    /// Seed for the deterministic plan.
    #[arg(long, default_value_t = 42u64)]
    seed: u64,
    /// Generate an HTML report from historical JSON files in the output directory.
    #[arg(long)]
    report: bool,
    /// HTML output path for --report (defaults to <out-dir>/report.html).
    #[arg(long)]
    report_file: Option<PathBuf>,
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
#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_add(0x9E3779B97F4A7C15))
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
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
struct ArtistPlan {
    id: i64,
    name: String,
    country: &'static str,
    formed_year: i32,
    albums: Vec<AlbumPlan>,
}
struct AlbumPlan {
    id: i64,
    title: String,
    release_year: i32,
    songs: Vec<SongPlan>,
}
struct SongPlan {
    id: i64,
    title: String,
    duration_ms: i32,
}

const COUNTRIES: &[&str] = &["US", "UK", "DE", "FR", "JP", "BR", "CA", "AU", "SE", "NL"];

struct SeedPlan {
    artists: Vec<ArtistPlan>,
    total_albums: u64,
    total_songs: u64,
}

fn build_seed_plan(scale: Scale, seed: u64) -> SeedPlan {
    let mut rng = Rng::new(seed);
    let mut album_counter: i64 = 0;
    let mut song_counter: i64 = 0;
    let mut album_quota = scale.albums as i64;
    let song_quota = scale.songs_cap as i64;

    let mut artists = Vec::with_capacity(scale.artists as usize);
    for a in 0..scale.artists {
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

        let mut albums = Vec::with_capacity(desired as usize);
        for _ in 0..desired {
            let mut songs_this_album = 1 + rng.gen_range(scale.max_songs_per_album) as i64;
            if song_counter + songs_this_album > song_quota {
                songs_this_album = (song_quota - song_counter).max(0);
            }
            let mut songs = Vec::with_capacity(songs_this_album as usize);
            for _ in 0..songs_this_album {
                song_counter += 1;
                songs.push(SongPlan {
                    id: song_counter,
                    title: format!("Song {song_counter}"),
                    duration_ms: 60_000 + rng.gen_range(360_000) as i32,
                });
            }
            album_counter += 1;
            albums.push(AlbumPlan {
                id: album_counter,
                title: format!("Album {album_counter}"),
                release_year: 1960 + rng.gen_range(65) as i32,
                songs,
            });
        }

        artists.push(ArtistPlan {
            id: (a + 1) as i64,
            name: format!("Artist {}", a + 1),
            country: COUNTRIES[rng.gen_range(COUNTRIES.len() as u32) as usize],
            formed_year: 1950 + rng.gen_range(75) as i32,
            albums,
        });
    }

    SeedPlan {
        artists,
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
    rss_anon_kb: u64,
    rss_file_kb: u64,
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct RunReport {
    binding: String,
    scale_name: String,
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
    let mut wal = path.as_os_str().to_owned();
    wal.push(".wal");
    let _ = fs::remove_file(PathBuf::from(&wal));
    // Belt-and-suspenders: the .NET tests use both -wal and .wal historically.
    if let Some(stem) = path.file_name().and_then(|s| s.to_str()) {
        if let Some(parent) = path.parent() {
            let _ = fs::remove_file(parent.join(format!("{stem}-wal")));
            let _ = fs::remove_file(parent.join(format!("{stem}-shm")));
        }
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    if cli.report_file.is_some() && !cli.report {
        bail!("--report-file requires --report");
    }

    if cli.report {
        let report_file = cli
            .report_file
            .clone()
            .unwrap_or_else(|| cli.out_dir.join("report.html"));
        generate_html_report(&cli.out_dir, &report_file)?;
        println!("Wrote {}", report_file.display());
        return Ok(());
    }

    let scale = parse_scale(&cli.scale);
    let db_path = cli
        .db_path
        .unwrap_or_else(|| PathBuf::from(format!("run-rust-{}.ddb", scale.name)));

    println!(
        "Building seed plan: scale={} artists={} albums(target)={} songs_cap={}",
        scale.name, scale.artists, scale.albums, scale.songs_cap
    );
    let plan = build_seed_plan(scale, cli.seed);
    println!(
        "Plan: artists={} total_albums={} total_songs={}",
        plan.artists.len(),
        plan.total_albums,
        plan.total_songs
    );

    delete_db_files(&db_path);

    let mut report = RunReport {
        binding: "RustRaw".to_string(),
        scale_name: scale.name.to_string(),
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
        decentdb::Db::create(&db_path, DbConfig::default()).expect("Db::create")
    });

    rec.measure("schema_create", None, || {
        for stmt in DDL {
            db.execute(stmt).expect("ddl");
        }
    });

    // ── Seed artists ──────────────────────────────────────────────
    rec.measure("seed_artists", Some(plan.artists.len() as u64), || {
        let mut txn = db.transaction().expect("begin");
        let prepared: PreparedStatement = txn
            .prepare(
                "INSERT INTO artists (id, name, country, formed_year) \
                 VALUES ($1, $2, $3, $4)",
            )
            .expect("prepare artists");
        let params: &mut [Value] = &mut [
            Value::Int64(0),
            Value::Text(String::new()),
            Value::Text(String::new()),
            Value::Int64(0),
        ];
        for a in &plan.artists {
            params[0] = Value::Int64(a.id);
            params[1] = Value::Text(a.name.clone());
            params[2] = Value::Text(a.country.to_string());
            params[3] = Value::Int64(a.formed_year as i64);
            prepared.execute_in(&mut txn, params).expect("ins artist");
        }
        txn.commit().expect("commit artists");
    });

    // ── Seed albums ───────────────────────────────────────────────
    rec.measure("seed_albums", Some(plan.total_albums), || {
        seed_albums(&db, &plan);
    });

    // ── Seed songs ────────────────────────────────────────────────
    rec.measure("seed_songs", Some(plan.total_songs), || {
        seed_songs(&db, &plan);
    });

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
        let target = (plan.artists.len() as i64) / 2 + 1;
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
    let mut wal = db_path.as_os_str().to_owned();
    wal.push(".wal");
    if let Ok(meta) = fs::metadata(PathBuf::from(&wal)) {
        report.wal_size_bytes = meta.len();
    }
    report.finished_unix = now_unix();

    fs::create_dir_all(&cli.out_dir)?;
    let datetime_stamp = Utc::now().format("%Y-%m-%d-%H%M").to_string();
    let out_path = cli.out_dir.join(format!(
        "{datetime_stamp}-rust-baseline-{}.json",
        scale.name
    ));
    fs::write(&out_path, serde_json::to_string_pretty(&report)?)?;
    println!("\nWrote {}", out_path.display());

    delete_db_files(&db_path);
    println!("Cleaned up temp DB files: {}", db_path.display());

    Ok(())
}

fn seed_albums(db: &decentdb::Db, plan: &SeedPlan) {
    let mut txn = db.transaction().expect("begin albums");
    let prepared = txn
        .prepare(
            "INSERT INTO albums (id, artist_id, title, release_year) \
             VALUES ($1, $2, $3, $4)",
        )
        .expect("prepare albums");
    let params: &mut [Value] = &mut [
        Value::Int64(0),
        Value::Int64(0),
        Value::Text(String::new()),
        Value::Int64(0),
    ];
    for a in &plan.artists {
        for al in &a.albums {
            params[0] = Value::Int64(al.id);
            params[1] = Value::Int64(a.id);
            params[2] = Value::Text(al.title.clone());
            params[3] = Value::Int64(al.release_year as i64);
            prepared.execute_in(&mut txn, params).expect("ins album");
        }
    }
    txn.commit().expect("commit albums");
}

fn seed_songs(db: &decentdb::Db, plan: &SeedPlan) {
    let mut txn = db.transaction().expect("begin songs");
    let prepared = txn
        .prepare(
            "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .expect("prepare songs");
    let params: &mut [Value] = &mut [
        Value::Int64(0),
        Value::Int64(0),
        Value::Int64(0),
        Value::Text(String::new()),
        Value::Int64(0),
    ];
    for a in &plan.artists {
        for al in &a.albums {
            for s in &al.songs {
                params[0] = Value::Int64(s.id);
                params[1] = Value::Int64(al.id);
                params[2] = Value::Int64(a.id);
                params[3] = Value::Text(s.title.clone());
                params[4] = Value::Int64(s.duration_ms as i64);
                prepared.execute_in(&mut txn, params).expect("ins song");
            }
        }
    }
    txn.commit().expect("commit songs");
}

// --- helpers ---------------------------------------------------------------
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
    Utc.timestamp_opt(unix as i64, 0)
        .single()
        .map(|ts| ts.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| format!("{unix}"))
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
    use super::{ordered_step_names, parse_scale, HistoricalRun, RunReport, StepMetric, HUGE};

    #[test]
    fn parse_scale_supports_huge() {
        let scale = parse_scale("huge");

        assert_eq!(scale.name, HUGE.name);
        assert_eq!(scale.artists, HUGE.artists);
        assert_eq!(scale.albums, HUGE.albums);
        assert_eq!(scale.songs_cap, HUGE.songs_cap);
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
                "query_view_first_1000".to_string()
            ]
        );
    }
}
