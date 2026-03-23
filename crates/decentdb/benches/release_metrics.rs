use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use decentdb::{BulkLoadOptions, Db, DbConfig, Value};

fn main() {
    run("point_lookup", benchmark_point_lookup);
    run("fk_join_expansion", benchmark_fk_join_expansion);
    run("trigram_search", benchmark_trigram_search);
    run("bulk_load", benchmark_bulk_load);
    run("crash_recovery", benchmark_crash_recovery);
}

fn run(name: &str, benchmark: fn() -> Duration) {
    let duration = benchmark();
    println!("{name}_ms={:.3}", duration.as_secs_f64() * 1000.0);
}

fn benchmark_point_lookup() -> Duration {
    let db = Db::open(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .expect("create table");
    for id in 1..=1_000 {
        db.execute(&format!(
            "INSERT INTO users (id, name) VALUES ({id}, 'user-{id}')"
        ))
        .expect("insert row");
    }

    let started = Instant::now();
    for _ in 0..1_000 {
        black_box(
            db.execute("SELECT id, name FROM users WHERE id = 777")
                .expect("point lookup"),
        );
    }
    started.elapsed()
}

fn benchmark_fk_join_expansion() -> Duration {
    let db = Db::open(":memory:", DbConfig::default()).expect("open db");
    db.execute_batch(
        "CREATE TABLE artists (id INT64 PRIMARY KEY, name TEXT); \
         CREATE TABLE albums (id INT64 PRIMARY KEY, artist_id INT64, title TEXT); \
         CREATE TABLE tracks (id INT64 PRIMARY KEY, album_id INT64, title TEXT)",
    )
    .expect("create tables");

    for artist in 1..=50 {
        db.execute(&format!(
            "INSERT INTO artists (id, name) VALUES ({artist}, 'artist-{artist}')"
        ))
        .expect("insert artist");
        for album in 1..=5 {
            let album_id = artist * 10 + album;
            db.execute(&format!(
                "INSERT INTO albums (id, artist_id, title) VALUES ({album_id}, {artist}, 'album-{album_id}')"
            ))
            .expect("insert album");
            for track in 1..=10 {
                let track_id = album_id * 100 + track;
                db.execute(&format!(
                    "INSERT INTO tracks (id, album_id, title) VALUES ({track_id}, {album_id}, 'track-{track_id}')"
                ))
                .expect("insert track");
            }
        }
    }

    let sql = "SELECT a.name, b.title, t.title \
               FROM artists a \
               JOIN albums b ON a.id = b.artist_id \
               JOIN tracks t ON b.id = t.album_id \
               WHERE a.id = 7 \
               ORDER BY b.id, t.id";
    let started = Instant::now();
    for _ in 0..100 {
        black_box(db.execute(sql).expect("join query"));
    }
    started.elapsed()
}

fn benchmark_trigram_search() -> Duration {
    let db = Db::open(":memory:", DbConfig::default()).expect("open db");
    db.execute_batch(
        "CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT); \
         CREATE INDEX docs_body_trgm_idx ON docs USING trigram(body)",
    )
    .expect("create trigram schema");
    for id in 1..=500 {
        db.execute(&format!(
            "INSERT INTO docs (id, body) VALUES ({id}, 'alpha beta gamma document {id}')"
        ))
        .expect("insert document");
    }

    let started = Instant::now();
    for _ in 0..100 {
        black_box(
            db.execute("SELECT id FROM docs WHERE body LIKE '%gamma%' ORDER BY id")
                .expect("trigram search"),
        );
    }
    started.elapsed()
}

fn benchmark_bulk_load() -> Duration {
    let db = Db::open(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE bulk_data (id INT64 PRIMARY KEY, value TEXT)")
        .expect("create bulk table");

    let rows = (1..=10_000)
        .map(|id| vec![Value::Int64(id), Value::Text(format!("value-{id}"))])
        .collect::<Vec<_>>();
    let started = Instant::now();
    db.bulk_load_rows(
        "bulk_data",
        &["id", "value"],
        &rows,
        BulkLoadOptions::default(),
    )
    .expect("bulk load");
    started.elapsed()
}

fn benchmark_crash_recovery() -> Duration {
    let path = temp_path("crash-recovery.ddb");
    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        db.execute_batch(
            "CREATE TABLE recovered (id INT64 PRIMARY KEY, value TEXT); \
             INSERT INTO recovered (id, value) VALUES (1, 'before-reopen')",
        )
        .expect("seed data");
        db.checkpoint().expect("checkpoint");
    }

    let started = Instant::now();
    let reopened = Db::open(&path, DbConfig::default()).expect("reopen db");
    black_box(
        reopened
            .execute("SELECT id, value FROM recovered")
            .expect("recover read"),
    );
    let elapsed = started.elapsed();
    let _ = fs::remove_file(&path);
    let _ = fs::remove_file(PathBuf::from(format!("{}.wal", path.display())));
    elapsed
}

fn temp_path(name: &str) -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("decentdb-bench-{id}-{name}"))
}
