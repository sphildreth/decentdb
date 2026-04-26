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

use chrono::Utc;
use clap::Parser;
use decentdb::{DbConfig, PreparedStatement, Value};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(version, about = "DecentDB raw-engine baseline benchmark")]
struct Cli {
    /// Scale: smoke | medium | full
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

fn parse_scale(name: &str) -> Scale {
    match name.to_ascii_lowercase().as_str() {
        "smoke" => SMOKE,
        "medium" => MEDIUM,
        "full" => FULL,
        other => panic!("Unknown scale '{other}'. Use smoke|medium|full."),
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

const COUNTRIES: &[&str] = &[
    "US", "UK", "DE", "FR", "JP", "BR", "CA", "AU", "SE", "NL",
];

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
#[derive(Default, Serialize)]
struct StepMetric {
    name: String,
    duration_seconds: f64,
    records: Option<u64>,
    records_per_second: Option<f64>,
    rss_bytes: u64,
    extra: serde_json::Map<String, serde_json::Value>,
}

#[derive(Default, Serialize)]
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

// Tiny inline syscall to avoid pulling libc crate.
#[allow(non_snake_case)]
unsafe fn libc_sysconf_pagesize() -> u64 {
    extern "C" {
        fn sysconf(name: i32) -> i64;
    }
    const _SC_PAGESIZE: i32 = 30;
    let v = unsafe { sysconf(_SC_PAGESIZE) };
    if v <= 0 { 4096 } else { v as u64 }
}

struct Recorder<'a> {
    report: &'a mut RunReport,
    peak_rss: u64,
}
impl<'a> Recorder<'a> {
    fn new(report: &'a mut RunReport) -> Self {
        Self { report, peak_rss: 0 }
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
        let rps = records.map(|r| if dur_secs > 0.0 { r as f64 / dur_secs } else { 0.0 });
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
    let out_path = cli
        .out_dir
        .join(format!("{datetime_stamp}-rust-baseline-{}.json", scale.name));
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
    r.rows().first().and_then(|row| row.values().first().cloned())
}
fn scalar_int(r: &decentdb::QueryResult) -> i64 {
    match first_value(r) {
        Some(Value::Int64(i)) => i,
        _ => 0,
    }
}

fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli) {
        eprintln!("Error: {e:?}");
        std::process::exit(1);
    }
}
