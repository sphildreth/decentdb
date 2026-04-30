// Targeted probe: repeatedly run the aggregate and view queries on an existing
// DB to measure steady-state engine cost with minimal noise.

use std::path::PathBuf;
use std::time::Instant;

use decentdb::{DbConfig, Value};

fn main() {
    let path: PathBuf = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "run-rust-full.ddb".to_string())
        .into();
    let iters: usize = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "3".to_string())
        .parse()
        .unwrap();

    let db = decentdb::Db::open(&path, DbConfig::default()).expect("open");

    println!("== warmup ==");
    let _ = db.execute("SELECT COUNT(*) FROM songs").unwrap();

    for name in [
        "agg",
        "top10_artists",
        "top10_albums",
        "view1000",
        "view_artist1",
    ] {
        for i in 0..iters {
            let t0 = Instant::now();
            match name {
                "agg" => {
                    let _ = db
                        .execute(
                            "SELECT COUNT(*), SUM(duration_ms), AVG(duration_ms), \
                                    MIN(duration_ms), MAX(duration_ms) FROM songs",
                        )
                        .unwrap();
                }
                "top10_artists" => {
                    let _ = db
                        .execute(
                            "SELECT a.id, a.name, COUNT(s.id) AS song_count
                             FROM artists a
                             JOIN songs s ON s.artist_id = a.id
                             GROUP BY a.id, a.name
                             ORDER BY song_count DESC
                             LIMIT 10",
                        )
                        .unwrap();
                }
                "top10_albums" => {
                    let _ = db
                        .execute(
                            "SELECT al.id, al.title, COUNT(s.id) AS song_count
                             FROM albums al
                             JOIN songs s ON s.album_id = al.id
                             GROUP BY al.id, al.title
                             ORDER BY song_count DESC
                             LIMIT 10",
                        )
                        .unwrap();
                }
                "view1000" => {
                    let _ = db
                        .execute(
                            "SELECT artist_id, artist_name, album_title, song_title \
                             FROM v_artist_songs LIMIT 1000",
                        )
                        .unwrap();
                }
                "view_artist1" => {
                    let _ = db
                        .execute_with_params(
                            "SELECT album_title, song_title, duration_ms \
                             FROM v_artist_songs WHERE artist_id = $1",
                            &[Value::Int64(1)],
                        )
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let dur = t0.elapsed();
            println!("{name:<15} iter {i}: {:?}", dur);
        }
    }
}
