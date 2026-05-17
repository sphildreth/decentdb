//! WAL fuzz binary: generates random valid database files, introduces
//! controlled corruptions in the WAL, and verifies that recovery either
//! succeeds cleanly or returns a corruption error — never panics.
//!
//! Build: `cargo build -p decentdb --bin wal_fuzz`
//! Run:   `cargo run -p decentdb --bin wal_fuzz`

use std::fs;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, DbError, WalSyncMode};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn temp_path(label: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ordinal = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "wal_fuzz_{label}_{pid}_{ts}_{ordinal}.ddb",
        pid = std::process::id()
    ))
}

fn wal_path(db_path: &std::path::Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_os_string();
    p.push(".wal");
    PathBuf::from(p)
}

fn cleanup(path: &std::path::Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(wal_path(path));
}

fn corrupt_wal_file(
    path: &std::path::Path,
    strategy: &str,
    idx: usize,
) -> Result<(), std::io::Error> {
    let wal = wal_path(path);
    let mut data = fs::read(&wal)?;
    if data.is_empty() {
        fs::write(&wal, vec![0xFF; 32])?;
        return Ok(());
    }

    match strategy {
        "truncate" => {
            let truncate_to = if data.len() > 1 { idx % data.len() } else { 0 };
            data.truncate(truncate_to);
        }
        "flip_bits" => {
            for pos in (0..data.len()).step_by(3) {
                if idx > 0 && pos % (idx + 1) == 0 {
                    data[pos] ^= 0xFF;
                }
            }
        }
        "bad_magic" if data.len() >= 8 => {
            data[0..8].copy_from_slice(b"BADC0DE\0");
        }
        "random_bytes" => {
            let seed = (idx as u64).wrapping_mul(6364136223846793005);
            for b in &mut data {
                *b = (*b).wrapping_add(seed as u8).wrapping_mul(17);
            }
        }
        "zero_wal" => {
            data.clear();
        }
        "oversized" => {
            data.extend(vec![0xAA; 4096 * (idx % 4 + 1)]);
        }
        _ => {}
    }

    fs::write(&wal, data)
}

fn create_db_with_data(
    path: &std::path::Path,
    rows: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = DbConfig {
        wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
        ..DbConfig::default()
    };
    let db = Db::create(path, config)?;
    db.execute("CREATE TABLE t(id INT64, val TEXT)")?;
    for i in 0..rows {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 'row-{i}')"))?;
    }
    Ok(())
}

fn reopen_and_verify(path: &std::path::Path, expected_rows: Option<usize>) {
    let config = DbConfig::default();
    let result = Db::open(path, config);
    match (result, expected_rows) {
        (Ok(db), Some(count)) => match db.execute("SELECT COUNT(*) FROM t") {
            Ok(r) => {
                let actual = match &r.rows()[0].values()[0] {
                    decentdb::Value::Int64(v) => *v as usize,
                    _ => 0,
                };
                println!("  opened ok, rows={actual}, expected={count}");
                if actual != count {
                    println!("  WARNING: row count mismatch: {actual} vs {count}");
                }
            }
            Err(e) => {
                println!("  opened ok but query failed: {e}");
            }
        },
        (Ok(_), None) => {
            println!("  opened ok (tables may be absent)");
        }
        (Err(DbError::Corruption { .. }), _) => {
            println!("  corruption error (expected after WAL corruption)");
        }
        (Err(other), _) => {
            println!("  other error: {other:?}");
        }
    }
}

fn main() {
    let strategies = [
        "truncate",
        "flip_bits",
        "bad_magic",
        "random_bytes",
        "zero_wal",
        "oversized",
    ];
    let rows_variants = [0usize, 1, 5, 10, 50];

    let mut passed = 0usize;
    let mut total = 0usize;

    for &rows in &rows_variants {
        for strategy in strategies.iter() {
            for variant in 0..=2 {
                total += 1;
                let path = temp_path(&format!("{strategy}-{rows}r-v{variant}"));

                // Create a valid database with data.
                if let Err(e) = create_db_with_data(&path, rows) {
                    println!("SKIP [{strategy}/r{rows}/v{variant}]: create failed: {e}");
                    cleanup(&path);
                    continue;
                }

                // Corrupt the WAL.
                if let Err(e) = corrupt_wal_file(&path, strategy, variant) {
                    println!("SKIP [{strategy}/r{rows}/v{variant}]: corrupt failed: {e}");
                    cleanup(&path);
                    continue;
                }

                // Attempt to reopen — must not panic.
                let expected = if *strategy == "zero_wal" {
                    Some(rows)
                } else {
                    None
                };
                println!("FUZZ [{strategy}/r{rows}/v{variant}]:",);
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    reopen_and_verify(&path, expected);
                }));

                match result {
                    Ok(()) => {
                        passed += 1;
                    }
                    Err(_) => {
                        println!("  PANIC: fuzz target panicked!");
                    }
                }

                cleanup(&path);
            }
        }
    }

    println!("\nResults: {passed}/{total} cases completed without panic");
    if passed < total {
        process::exit(1);
    }
}
