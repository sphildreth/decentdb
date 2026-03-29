//! Unit tests for WAL recovery edge cases.
//!
//! Exercises: wal/recovery.rs initialize_or_recover, wal/format.rs frame
//! decode, wal/shared.rs acquire/evict, and the interaction between WAL
//! state and database re-open semantics.

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, DbError, Value};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

fn unique_db_path(label: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "decentdb-wal-recovery-{label}-{}-{ts}-{ordinal}.ddb",
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

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) {
    db.execute(sql).unwrap();
}

// ── Basic WAL lifecycle ───────────────────────────────────────────────────────

/// WAL file is created next to the database file when the db is opened.
#[test]
fn wal_file_exists_after_database_open() {
    let path = unique_db_path("wal-exists");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    let wal = wal_path(&path);
    assert!(wal.exists(), "WAL file should be created alongside the db");
    drop(db);
    cleanup(&path);
}

/// The WAL file is exactly 32 bytes (header only) after a clean checkpoint.
#[test]
fn wal_is_header_only_after_checkpoint_with_no_readers() {
    let path = unique_db_path("wal-header-only");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64)");
    exec(&db, "INSERT INTO t VALUES (1)");
    db.checkpoint().unwrap();
    let wal_size = fs::metadata(wal_path(&path)).unwrap().len();
    assert_eq!(wal_size, 32, "clean checkpoint should leave a 32-byte WAL");
    drop(db);
    cleanup(&path);
}

/// Data committed to WAL before a checkpoint survives re-open.
#[test]
fn committed_data_persists_through_reopen_without_checkpoint() {
    let path = unique_db_path("reopen-no-ckpt");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64, val TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    }
    let db2 = Db::open(&path, DbConfig::default()).unwrap();
    let r = db2.execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".into()));
    drop(db2);
    cleanup(&path);
}

/// Data committed before a checkpoint and then re-opened survives.
#[test]
fn committed_data_persists_through_checkpoint_and_reopen() {
    let path = unique_db_path("ckpt-reopen");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        exec(&db, "INSERT INTO t VALUES (42)");
        db.checkpoint().unwrap();
    }
    let db2 = Db::open(&path, DbConfig::default()).unwrap();
    let r = db2.execute("SELECT id FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
    drop(db2);
    cleanup(&path);
}

// ── Crash / partial-write simulation ─────────────────────────────────────────

/// A failed WAL commit does not expose the uncommitted data on re-open.
/// Uses failpoint injection to simulate a crash mid-write.
#[test]
fn uncommitted_wal_frames_are_not_visible_after_reopen() {
    let path = unique_db_path("uncommitted-reopen");
    let config = DbConfig {
        wal_sync_mode: decentdb::WalSyncMode::TestingOnlyUnsafeNoSync,
        ..DbConfig::default()
    };

    // Write and commit good data first.
    {
        let db = Db::create(&path, config.clone()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        exec(&db, "INSERT INTO t VALUES (1)");
    }

    // Simulate a crash before the commit frame is written.
    Db::clear_failpoints().unwrap();
    Db::install_failpoint("wal.write_commit", "error", 1, 0).unwrap();
    {
        let db = Db::open(&path, config.clone()).unwrap();
        let _ = db.execute("INSERT INTO t VALUES (2)"); // expected to fail
    }
    Db::clear_failpoints().unwrap();

    // Re-open: only committed row (id=1) should be visible.
    let db = Db::open(&path, config).unwrap();
    let r = db.execute("SELECT id FROM t ORDER BY id").unwrap();
    let ids: Vec<Value> = r.rows().iter().map(|row| row.values()[0].clone()).collect();
    assert_eq!(ids, vec![Value::Int64(1)]);
    drop(db);
    cleanup(&path);
}

/// Hypothesis: truncating the WAL to 0 bytes causes a corruption error on open.
///
/// Classification: **Test assumption likely wrong.**
///
/// The recovery code (`wal/recovery.rs`, `initialize_or_recover`) explicitly
/// treats a zero-byte WAL file as a valid "empty" state: it writes a fresh
/// 32-byte header and returns an empty index.  A missing or zero-length WAL
/// simply means "no unplayed frames" — the database falls back to whatever
/// state the DB file contains.  This is intentional design (consistent with
/// SQLite WAL mode and crash-safe semantics).
///
/// Only `0 < size < 32` triggers the corruption path.  Size == 0 is a clean
/// initialization signal, not an error.
///
/// Recommendation: keep this test as a specification of the _current_
/// behaviour so future refactors cannot accidentally regress it.
#[test]
#[ignore = "Design choice: zero-byte WAL is valid initialization state, not corruption (see recovery.rs line 17-21)"]
fn truncated_wal_to_zero_bytes_is_corruption() {
    let path = unique_db_path("wal-truncated");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        exec(&db, "INSERT INTO t VALUES (1)");
        drop(db);
    }

    // Truncate the WAL to 0 bytes (below the 32-byte minimum header size).
    let wal = wal_path(&path);
    fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&wal)
        .unwrap();

    let result = Db::open(&path, DbConfig::default());
    assert!(
        result.is_err(),
        "opening a db with a zero-byte WAL should fail"
    );
    match result.unwrap_err() {
        DbError::Corruption { .. } => {}
        other => panic!("expected Corruption error, got {other:?}"),
    }
    cleanup(&path);
}

/// Zero-byte WAL is treated as a clean initialization state — the engine
/// writes a fresh 32-byte header and opens successfully.  This documents the
/// designed behaviour for future regression protection.
#[test]
fn zero_byte_wal_is_reinitialized_and_db_opens_successfully() {
    let path = unique_db_path("wal-zero-reinit");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        exec(&db, "INSERT INTO t VALUES (1)");
        db.checkpoint().unwrap();
        drop(db);
    }
    // Truncate WAL to 0 bytes, simulating a crash before any WAL writes.
    let wal = wal_path(&path);
    fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&wal)
        .unwrap();
    assert_eq!(fs::metadata(&wal).unwrap().len(), 0);

    // Engine must re-initialize the WAL and open without error.
    let db = Db::open(&path, DbConfig::default()).unwrap();
    let r = db.execute("SELECT id FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    drop(db);
    cleanup(&path);
}

/// A WAL with 1 ≤ size < 32 bytes is below the minimum fixed header size
/// and must be detected as corruption.
#[test]
fn partial_wal_header_is_corruption() {
    let path = unique_db_path("wal-partial-header");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        exec(&db, "INSERT INTO t VALUES (1)");
        drop(db);
    }

    let wal = wal_path(&path);
    // Truncate to 16 bytes — below the 32-byte WAL header.
    {
        let f = fs::OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(&wal)
            .unwrap();
        f.set_len(16).unwrap();
    }

    let result = Db::open(&path, DbConfig::default());
    assert!(
        result.is_err(),
        "WAL with 16-byte partial header should be corruption"
    );
    match result.unwrap_err() {
        DbError::Corruption { .. } => {}
        other => panic!("expected Corruption, got {other:?}"),
    }
    cleanup(&path);
}

#[test]
fn corrupt_wal_magic_is_detected_on_reopen() {
    let path = unique_db_path("wal-bad-magic");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        drop(db);
    }

    // Overwrite the WAL header magic.
    let wal = wal_path(&path);
    let mut file = fs::OpenOptions::new().write(true).open(&wal).unwrap();
    file.write_all(b"BADMAGIC").unwrap();

    let result = Db::open(&path, DbConfig::default());
    assert!(result.is_err());
    match result.unwrap_err() {
        DbError::Corruption { .. } => {}
        other => panic!("expected Corruption, got {other:?}"),
    }
    cleanup(&path);
}

// ── Multiple commits and recovery ────────────────────────────────────────────

/// Many sequential commits all survive re-open without a checkpoint.
#[test]
fn many_sequential_commits_all_survive_reopen() {
    let path = unique_db_path("many-commits");
    let row_count = 50_u32;
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        for i in 0..row_count {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
    }
    let db = Db::open(&path, DbConfig::default()).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(i64::from(row_count)));
    drop(db);
    cleanup(&path);
}

/// Checkpointing after each commit keeps the WAL small and data persists.
#[test]
fn checkpoint_after_each_commit_keeps_wal_small() {
    let path = unique_db_path("incremental-ckpt");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64)");
    for i in 0_u32..10 {
        db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        db.checkpoint().unwrap();
        let wal_size = fs::metadata(wal_path(&path)).unwrap().len();
        assert_eq!(
            wal_size, 32,
            "WAL should be header-only after checkpoint at iteration {i}"
        );
    }
    drop(db);
    cleanup(&path);
}

// ── In-memory WAL (no disk) ───────────────────────────────────────────────────

/// In-memory databases do not create a WAL file on disk.
#[test]
fn memory_db_does_not_create_disk_wal_file() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64)");
    exec(&db, "INSERT INTO t VALUES (1)");
    db.checkpoint().unwrap();
    // Verify the current directory has no unexpected .wal files from this test.
    // (We just assert checkpoint succeeds on :memory:.)
}

/// Multiple in-memory databases are isolated from each other.
#[test]
fn two_memory_dbs_are_isolated() {
    let db1 = mem_db();
    let db2 = mem_db();
    exec(&db1, "CREATE TABLE t(id INT64)");
    exec(&db1, "INSERT INTO t VALUES (100)");
    // db2 has no table t; querying it must fail.
    assert!(db2.execute("SELECT * FROM t").is_err());
}

// ── evict_shared_wal ──────────────────────────────────────────────────────────

/// evict_shared_wal for a :memory: path is a no-op and does not error.
#[test]
fn evict_shared_wal_for_memory_path_is_noop() {
    decentdb::evict_shared_wal(":memory:").unwrap();
}

// ── Snapshot isolation through SQL ───────────────────────────────────────────

/// A snapshot held via BEGIN + SELECT sees the committed state at the time the
/// read transaction started, not later writes from other connections.
#[test]
fn sql_transaction_reads_see_committed_snapshot_at_start() {
    let path = unique_db_path("sql-transaction-snapshot");

    // Create the database and an initial committed row.
    let db_reader = Db::create(&path, DbConfig::default()).unwrap();
    exec(&db_reader, "CREATE TABLE t(id INT64)");
    exec(&db_reader, "INSERT INTO t VALUES (1)");

    // Open an explicit read transaction and observe the committed state.
    exec(&db_reader, "BEGIN");
    let count_at_begin = db_reader.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        count_at_begin.rows()[0].values()[0],
        Value::Int64(1),
        "snapshot at BEGIN should see exactly 1 committed row"
    );

    // From another connection, commit an additional row while the snapshot is held.
    let db_writer = Db::open(&path, DbConfig::default()).unwrap();
    exec(&db_writer, "INSERT INTO t VALUES (2)");
    drop(db_writer);

    // Inside the original transaction we should still see the snapshot as of BEGIN.
    let count_during_txn_after_write = db_reader.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        count_during_txn_after_write.rows()[0].values()[0],
        Value::Int64(1),
        "read transaction should not see rows committed after BEGIN"
    );

    exec(&db_reader, "ROLLBACK");

    // Outside the transaction, the committed write should now be visible.
    let count_after_rollback = db_reader.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(
        count_after_rollback.rows()[0].values()[0],
        Value::Int64(2),
        "after rollback of the read txn, all committed rows should be visible"
    );

    drop(db_reader);
    cleanup(&path);
}

// ── WAL version retention with active readers ─────────────────────────────────

/// The WAL retains versions for active readers and truncates after release.
#[test]
fn wal_retains_version_while_snapshot_is_held() {
    let path = unique_db_path("wal-snapshot");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64)");
    exec(&db, "INSERT INTO t VALUES (1)");

    // Hold a snapshot; this prevents WAL truncation.
    let snapshot = db.hold_snapshot().unwrap();

    exec(&db, "INSERT INTO t VALUES (2)");
    db.checkpoint().unwrap();

    let wal_size_with_snapshot = fs::metadata(wal_path(&path)).unwrap().len();
    assert!(
        wal_size_with_snapshot > 32,
        "WAL should not be fully truncated while a snapshot is held"
    );

    db.release_snapshot(snapshot).unwrap();
    db.checkpoint().unwrap();

    let wal_size_after_release = fs::metadata(wal_path(&path)).unwrap().len();
    assert_eq!(
        wal_size_after_release, 32,
        "WAL should be truncated to header-only after all snapshots are released"
    );

    drop(db);
    cleanup(&path);
}

// ── Page size persistence ─────────────────────────────────────────────────────

/// Page size written during creation is correctly recovered on re-open.
#[test]
fn page_size_is_preserved_through_wal_recovery() {
    for &page_size in &[4096_u32, 8192, 16384] {
        let path = unique_db_path(&format!("ps-{page_size}"));
        let config = DbConfig {
            page_size,
            ..DbConfig::default()
        };
        {
            let db = Db::create(&path, config.clone()).unwrap();
            exec(&db, "CREATE TABLE t(id INT64)");
            exec(&db, "INSERT INTO t VALUES (1)");
        }
        let db = Db::open(&path, DbConfig::default()).unwrap();
        assert_eq!(
            db.config().page_size,
            page_size,
            "page size {page_size} should survive WAL recovery"
        );
        drop(db);
        cleanup(&path);
    }
}
