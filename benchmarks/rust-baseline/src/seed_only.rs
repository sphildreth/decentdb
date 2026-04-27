// Seed-only probe: exercise only the insert hot path so we can profile the
// writer/WAL without query-phase noise.

use std::path::PathBuf;
use std::time::Instant;

use decentdb::{DbConfig, PreparedStatement, Value};

fn main() {
    let scale = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "medium".to_string());
    let path: PathBuf = format!("seed-{}.ddb", scale).into();
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.wal", path.display()));

    let (artists, albums_per_artist, songs_per_album) = match scale.as_str() {
        "smoke" => (500u32, 10u32, 10u32),
        "medium" => (5_000u32, 10u32, 10u32),
        "full" => (50_000u32, 10u32, 10u32),
        "huge" => (250_000u32, 10u32, 10u32),
        _ => panic!("bad scale"),
    };

    let db = decentdb::Db::create(&path, DbConfig::default()).expect("create");
    db.execute(
        "CREATE TABLE songs (
            id INTEGER PRIMARY KEY,
            album_id INTEGER NOT NULL,
            artist_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            duration_ms INTEGER NOT NULL
        )",
    )
    .unwrap();
    db.execute("CREATE INDEX idx_songs_album ON songs (album_id)")
        .unwrap();
    db.execute("CREATE INDEX idx_songs_artist ON songs (artist_id)")
        .unwrap();

    let total = artists as u64 * albums_per_artist as u64 * songs_per_album as u64;
    let t0 = Instant::now();
    {
        let mut txn = db.transaction().unwrap();
        let prepared: PreparedStatement = txn
            .prepare(
                "INSERT INTO songs (id, album_id, artist_id, title, duration_ms) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .unwrap();
        let params: &mut [Value] = &mut [
            Value::Int64(0),
            Value::Int64(0),
            Value::Int64(0),
            Value::Text(String::new()),
            Value::Int64(0),
        ];
        let mut id: i64 = 0;
        for a in 1..=artists as i64 {
            for al in 1..=albums_per_artist as i64 {
                let album_id = (a - 1) * albums_per_artist as i64 + al;
                for _ in 0..songs_per_album {
                    id += 1;
                    params[0] = Value::Int64(id);
                    params[1] = Value::Int64(album_id);
                    params[2] = Value::Int64(a);
                    params[3] = Value::Text(format!("Song {id}"));
                    params[4] = Value::Int64(60_000 + (id as i32 % 360_000) as i64);
                    prepared.execute_in(&mut txn, params).unwrap();
                }
            }
        }
        txn.commit().unwrap();
    }
    let elapsed = t0.elapsed();
    println!(
        "seed_songs scale={scale}: {total} rows in {:?} = {:.0} r/s",
        elapsed,
        total as f64 / elapsed.as_secs_f64()
    );
}
