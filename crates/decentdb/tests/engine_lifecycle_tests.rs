use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, DbError, Result};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn stable_public_exports_are_available() {
    let _: fn(&std::path::Path, DbConfig) -> Result<Db> = |path, config| Db::create(path, config);
    let _: fn(&std::path::Path, DbConfig) -> Result<Db> = |path, config| Db::open(path, config);
    let _error = DbError::internal("placeholder");
}

#[test]
fn create_and_open_round_trip_bootstrap_state() {
    let path = unique_db_path("roundtrip");
    let config = DbConfig {
        page_size: 8192,
        ..DbConfig::default()
    };

    let created = Db::create(&path, config.clone()).expect("create should succeed");
    assert_eq!(created.config().page_size, 8192);
    assert_eq!(created.path(), path.as_path());

    let metadata = fs::metadata(&path).expect("database file should exist");
    assert_eq!(metadata.len(), 16_384);

    let reopened = Db::open(&path, DbConfig::default()).expect("open should succeed");
    assert_eq!(reopened.config().page_size, 8192);
    assert_eq!(reopened.path(), path.as_path());

    cleanup_db_file(&path);
}

#[test]
fn create_again_returns_already_exists_error() {
    let path = unique_db_path("create-again");
    let config = DbConfig::default();

    Db::create(&path, config.clone()).expect("initial create should succeed");
    let error = Db::create(&path, config).expect_err("second create should fail");

    match error {
        DbError::Io { source, .. } => assert_eq!(source.kind(), ErrorKind::AlreadyExists),
        other => panic!("expected already-exists I/O error, got {other:?}"),
    }

    cleanup_db_file(&path);
}

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic enough for tests")
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "decentdb-phase0-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn cleanup_db_file(path: &PathBuf) {
    let _ = fs::remove_file(path);
}

/// Regression test for TOCTOU race between reader registration and the
/// writer's retain_history check.  Before the fix, the writer could clear
/// old WAL index versions while a reader still needed them, causing
/// "overflow payload length mismatch" errors during concurrent access.
#[test]
fn concurrent_writer_reader_overflow_consistency() {
    run_concurrent_writer_reader_overflow_consistency(DbConfig::default());
}

#[test]
fn concurrent_writer_reader_overflow_consistency_with_deferred_materialization() {
    run_concurrent_writer_reader_overflow_consistency(DbConfig {
        defer_table_materialization: true,
        ..DbConfig::default()
    });
}

fn run_concurrent_writer_reader_overflow_consistency(config: DbConfig) {
    let path = unique_db_path("concurrent-overflow");
    let db = Db::open_or_create(&path, config.clone()).expect("create db");

    // Use a value large enough to spill into overflow pages.
    let big_value: String = "X".repeat(8000);
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, data TEXT)")
        .expect("create table");

    let stop = Arc::new(AtomicBool::new(false));
    let errors: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    // Writer thread: continuous INSERT + COMMIT.
    let writer_stop = Arc::clone(&stop);
    let writer_path = path.clone();
    let writer_big = big_value.clone();
    let writer_config = config.clone();
    let writer_handle = std::thread::spawn(move || {
        let db = Db::open_or_create(&writer_path, writer_config).expect("open writer");
        let mut i: i64 = 0;
        while !writer_stop.load(Ordering::Relaxed) {
            let sql = format!("INSERT INTO t VALUES ({i}, '{}')", writer_big);
            if let Err(e) = db.execute(&sql) {
                // Primary-key conflicts are acceptable when readers overlap.
                let msg = e.to_string();
                if !msg.contains("UNIQUE constraint") {
                    eprintln!("writer error at row {i}: {e}");
                }
            }
            if i % 8 == 0 {
                let _ = db.checkpoint();
            }
            i += 1;
        }
    });

    // Reader threads: continuous SELECT that traverses overflow chains.
    let mut reader_handles = Vec::new();
    for reader_id in 0..3u32 {
        let reader_stop = Arc::clone(&stop);
        let reader_errors = Arc::clone(&errors);
        let reader_path = path.clone();
        let reader_config = config.clone();
        reader_handles.push(std::thread::spawn(move || {
            let db = Db::open_or_create(&reader_path, reader_config).expect("open reader");
            while !reader_stop.load(Ordering::Relaxed) {
                match db.execute("SELECT COUNT(*), LENGTH(data) FROM t") {
                    Ok(_) => {}
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("overflow payload length mismatch")
                            || msg.contains("corruption")
                        {
                            reader_errors
                                .lock()
                                .expect("error lock")
                                .push(format!("reader {reader_id}: {msg}"));
                            return;
                        }
                    }
                }
            }
        }));
    }

    // Run for 2 seconds.
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(50));
    }
    stop.store(true, Ordering::Relaxed);

    writer_handle.join().expect("writer thread panicked");
    for h in reader_handles {
        h.join().expect("reader thread panicked");
    }

    let errs = errors.lock().expect("error lock");
    assert!(
        errs.is_empty(),
        "concurrent readers saw corruption: {errs:?}"
    );

    cleanup_db_file(&path);
    let wal_path = PathBuf::from(format!("{}.wal", path.display()));
    let _ = fs::remove_file(&wal_path);
}
