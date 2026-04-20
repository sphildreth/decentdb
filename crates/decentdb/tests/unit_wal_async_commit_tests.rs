//! Integration tests for `WalSyncMode::AsyncCommit` (ADR 0135).
//!
//! Validates the durability barrier (`Db::sync`), the background flusher's
//! catch-up behavior, and round-trip data correctness under group commit.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, QueryResult, Value, WalSyncMode};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

fn unique_db_path(label: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "decentdb-async-commit-{label}-{}-{ts}-{ordinal}.ddb",
        std::process::id()
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

fn async_commit_config(interval_ms: u32) -> DbConfig {
    DbConfig {
        wal_sync_mode: WalSyncMode::AsyncCommit { interval_ms },
        ..DbConfig::default()
    }
}

fn row_values(result: &QueryResult) -> Vec<Vec<Value>> {
    result
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

/// AsyncCommit round-trips data correctly: writes are immediately visible to
/// readers in the same process even before the background flusher runs.
#[test]
fn async_commit_reads_see_latest_writes_immediately() {
    let path = unique_db_path("read-latest");
    let db = Db::create(&path, async_commit_config(50)).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (id, v) VALUES (1, 'hello')")
        .unwrap();

    let result = db.execute("SELECT v FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("hello".into()));

    drop(db);
    cleanup(&path);
}

/// `Db::sync()` is a no-op for non-AsyncCommit modes (does not error).
#[test]
fn sync_is_noop_for_full_mode() {
    let path = unique_db_path("sync-noop");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.sync().expect("sync should succeed in Full mode");
    drop(db);
    cleanup(&path);
}

/// Under AsyncCommit, `Db::sync` blocks until the background flusher reports
/// the WAL caught up to the latest commit.
#[test]
fn async_commit_sync_blocks_until_durable() {
    let path = unique_db_path("sync-barrier");
    let db = Db::create(&path, async_commit_config(50)).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 0..200 {
        db.execute(&format!("INSERT INTO t (id, v) VALUES ({i}, {i})"))
            .unwrap();
    }

    let start = Instant::now();
    db.sync().expect("sync");
    let elapsed = start.elapsed();
    // Sync must complete in bounded time (well under any test timeout) and
    // not return errors. The exact value isn't asserted—just liveness.
    assert!(
        elapsed < Duration::from_secs(10),
        "sync took too long: {elapsed:?}"
    );

    drop(db);
    cleanup(&path);
}

/// AsyncCommit mode survives recovery: data committed and then `sync()`-ed is
/// present after reopening (in `Full` mode to avoid relying on async flush).
#[test]
fn async_commit_survives_reopen_after_explicit_sync() {
    let path = unique_db_path("reopen-after-sync");
    {
        let db = Db::create(&path, async_commit_config(100)).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 'durable')")
            .unwrap();
        db.sync().expect("sync");
        drop(db);
    }

    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    let result = db.execute("SELECT v FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("durable".into()));
    drop(db);
    cleanup(&path);
}

/// Dropping the database without an explicit `sync()` still flushes pending
/// writes (the AsyncCommitState's Drop performs a final synchronous flush),
/// so data committed just before drop is visible after reopen.
#[test]
fn async_commit_drop_performs_final_flush() {
    let path = unique_db_path("drop-flushes");
    {
        let db = Db::create(&path, async_commit_config(60_000)).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 'final')")
            .unwrap();
        // Intentionally no sync(); rely on Drop's final flush.
        drop(db);
    }

    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    let result = db.execute("SELECT v FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("final".into()));
    drop(db);
    cleanup(&path);
}

/// The background flusher actually runs: with a short interval, after
/// sleeping we observe the WAL caught up via a successful `sync()` that
/// returns essentially instantly.
#[test]
fn async_commit_background_flusher_catches_up() {
    let path = unique_db_path("flusher-runs");
    let db = Db::create(&path, async_commit_config(20)).unwrap();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    // Wait long enough for several flusher ticks.
    thread::sleep(Duration::from_millis(150));

    // Now sync() should be cheap because durable_lsn already caught up.
    let start = Instant::now();
    db.sync().expect("sync");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(100),
        "sync should be fast after background catch-up: {elapsed:?}"
    );
    drop(db);
    cleanup(&path);
}
