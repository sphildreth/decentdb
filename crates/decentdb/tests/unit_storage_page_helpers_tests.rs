//! Integration tests for storage / page-level behaviors exercised through
//! the public `Db` API.
//!
//! Covers: page-size persistence, DB file sizing, schema cookie increments,
//! unsupported page sizes, and basic DDL→data recovery across all supported
//! page sizes.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, Value};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

fn unique_path(label: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ord = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "decentdb-page-{label}-{}-{ts}-{ord}.ddb",
        std::process::id()
    ))
}

fn cleanup(p: &std::path::Path) {
    let _ = fs::remove_file(p);
    let mut wal = p.as_os_str().to_os_string();
    wal.push(".wal");
    let _ = fs::remove_file(PathBuf::from(wal));
}

fn exec(db: &Db, sql: &str) {
    db.execute(sql).unwrap();
}

// ── Page-size persistence ─────────────────────────────────────────────────────

/// The three supported page sizes all round-trip through create→reopen.
#[test]
fn all_supported_page_sizes_persist_through_reopen() {
    for &page_size in &[4096_u32, 8192, 16384] {
        let path = unique_path(&format!("ps-{page_size}"));
        let config = DbConfig {
            page_size,
            ..DbConfig::default()
        };
        {
            let db = Db::create(&path, config.clone()).unwrap();
            exec(&db, "CREATE TABLE t(id INT64)");
            exec(&db, "INSERT INTO t VALUES (1)");
            db.checkpoint().unwrap();
        }
        let db = Db::open(&path, DbConfig::default()).unwrap();
        assert_eq!(
            db.config().page_size,
            page_size,
            "page_size {page_size} should survive reopen"
        );
        let r = db.execute("SELECT id FROM t").unwrap();
        assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
        drop(db);
        cleanup(&path);
    }
}

/// The DB file size after a checkpoint is an exact multiple of the page size.
#[test]
fn db_file_size_is_multiple_of_page_size_after_checkpoint() {
    for &page_size in &[4096_u32, 8192, 16384] {
        let path = unique_path(&format!("filesize-{page_size}"));
        let config = DbConfig {
            page_size,
            ..DbConfig::default()
        };
        let db = Db::create(&path, config).unwrap();
        exec(&db, "CREATE TABLE t(id INT64)");
        exec(&db, "INSERT INTO t VALUES (1)");
        db.checkpoint().unwrap();

        let size = fs::metadata(&path).unwrap().len();
        assert_eq!(
            size % u64::from(page_size),
            0,
            "db file size {size} is not a multiple of page_size {page_size}"
        );
        assert!(size > 0, "db file must be non-empty after checkpoint");
        drop(db);
        cleanup(&path);
    }
}

// ── Unsupported page sizes ────────────────────────────────────────────────────

/// Page size 2048 is smaller than the minimum and must be rejected.
#[test]
fn page_size_2048_is_rejected() {
    let path = unique_path("ps-2048");
    let config = DbConfig {
        page_size: 2048,
        ..DbConfig::default()
    };
    let result = Db::create(&path, config);
    assert!(result.is_err(), "page size 2048 should be rejected");
    cleanup(&path);
}

/// Page size 65536 is above the supported maximum and must be rejected.
#[test]
fn page_size_65536_is_rejected() {
    let path = unique_path("ps-65536");
    let config = DbConfig {
        page_size: 65536,
        ..DbConfig::default()
    };
    let result = Db::create(&path, config);
    assert!(result.is_err(), "page size 65536 should be rejected");
    cleanup(&path);
}

// ── Schema cookie ─────────────────────────────────────────────────────────────

/// The schema cookie is non-zero after the first DDL statement.
#[test]
fn schema_cookie_is_nonzero_after_ddl() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64)");
    let cookie = db.schema_cookie().unwrap();
    assert!(
        cookie > 0,
        "schema cookie should be positive after CREATE TABLE"
    );
}

/// Each DDL statement increments the schema cookie by a positive amount.
#[test]
fn schema_cookie_increments_on_each_ddl() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t1(id INT64)");
    let c1 = db.schema_cookie().unwrap();
    exec(&db, "CREATE TABLE t2(id INT64)");
    let c2 = db.schema_cookie().unwrap();
    exec(&db, "DROP TABLE t1");
    let c3 = db.schema_cookie().unwrap();
    assert!(c2 > c1, "schema cookie should increase after second CREATE");
    assert!(c3 > c2, "schema cookie should increase after DROP TABLE");
}

/// DML does not change the schema cookie.
#[test]
fn schema_cookie_unchanged_by_dml() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64)");
    let before = db.schema_cookie().unwrap();
    exec(&db, "INSERT INTO t VALUES (1)");
    exec(&db, "INSERT INTO t VALUES (2)");
    exec(&db, "DELETE FROM t WHERE id = 1");
    let after = db.schema_cookie().unwrap();
    assert_eq!(
        before, after,
        "schema cookie must not change due to DML-only transactions"
    );
}

// ── Page count grows proportionally ──────────────────────────────────────────

/// A single checkpoint flushes at least one page to disk, meaning the
/// file is at least one page long.
#[test]
fn db_file_has_at_least_one_page_after_create() {
    let page_size = 4096_u32;
    let path = unique_path("one-page");
    let db = Db::create(
        &path,
        DbConfig {
            page_size,
            ..DbConfig::default()
        },
    )
    .unwrap();
    db.checkpoint().unwrap();
    let size = fs::metadata(&path).unwrap().len();
    assert!(
        size >= u64::from(page_size),
        "db must be at least one page ({page_size} bytes) after initial checkpoint; got {size}"
    );
    drop(db);
    cleanup(&path);
}

/// Inserting many rows causes the db file to grow beyond the initial size.
#[test]
fn bulk_insert_causes_db_file_to_grow() {
    let path = unique_path("bulk-grow");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64, val TEXT)");
    db.checkpoint().unwrap();
    let size_before = fs::metadata(&path).unwrap().len();

    for i in 0..200 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({i}, 'padding-abcdefghijklmnopqrstuvwxyz-{i}')"
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
    let size_after = fs::metadata(&path).unwrap().len();
    assert!(
        size_after > size_before,
        "db file should grow after bulk inserts; before={size_before} after={size_after}"
    );
    drop(db);
    cleanup(&path);
}

// ── Opening a nonexistent file ────────────────────────────────────────────────

/// Opening a path that does not exist returns an error.
#[test]
fn open_nonexistent_path_returns_error() {
    let path = unique_path("nonexistent");
    // Path never created.
    let result = Db::open(&path, DbConfig::default());
    assert!(
        result.is_err(),
        "Db::open on a nonexistent path should return an error"
    );
}

// ── Read-after-checkpoint with large page size ────────────────────────────────

/// DDL + DML written with 16 KiB pages is fully readable after reopen.
#[test]
fn large_page_size_data_readable_after_reopen() {
    let path = unique_path("large-page-reopen");
    let config = DbConfig {
        page_size: 16384,
        ..DbConfig::default()
    };
    {
        let db = Db::create(&path, config.clone()).unwrap();
        exec(&db, "CREATE TABLE kv(k TEXT, v INT64)");
        for i in 0_i64..25 {
            db.execute(&format!("INSERT INTO kv VALUES ('key-{i}', {i})"))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    let db = Db::open(&path, DbConfig::default()).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM kv").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(25));
    drop(db);
    cleanup(&path);
}

// ── Multiple tables on disk ───────────────────────────────────────────────────

/// Multiple tables survive checkpoint and reopen without cross-contamination.
#[test]
fn multiple_tables_are_independent_after_reopen() {
    let path = unique_path("multi-table");
    {
        let db = Db::create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE a(id INT64)");
        exec(&db, "CREATE TABLE b(id INT64)");
        exec(&db, "INSERT INTO a VALUES (1)");
        exec(&db, "INSERT INTO a VALUES (2)");
        exec(&db, "INSERT INTO b VALUES (99)");
        db.checkpoint().unwrap();
    }
    let db = Db::open(&path, DbConfig::default()).unwrap();
    let ra = db.execute("SELECT COUNT(*) FROM a").unwrap();
    let rb = db.execute("SELECT COUNT(*) FROM b").unwrap();
    assert_eq!(ra.rows()[0].values()[0], Value::Int64(2));
    assert_eq!(rb.rows()[0].values()[0], Value::Int64(1));
    drop(db);
    cleanup(&path);
}

// ── Schema errors ─────────────────────────────────────────────────────────────

/// Creating a table that already exists returns an error.
#[test]
fn create_duplicate_table_returns_error() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    exec(&db, "CREATE TABLE t(id INT64)");
    let result = db.execute("CREATE TABLE t(id INT64)");
    assert!(
        result.is_err(),
        "creating a duplicate table should return an error"
    );
}

/// Querying a table that does not exist returns an error.
#[test]
fn query_nonexistent_table_returns_error() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT * FROM does_not_exist");
    assert!(
        result.is_err(),
        "querying a non-existent table should return an error"
    );
}

/// DROP TABLE on a non-existent table returns an error.
#[test]
fn drop_nonexistent_table_returns_error() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("DROP TABLE phantom");
    assert!(result.is_err(), "DROP TABLE on phantom table should error");
}

// ── Db::config() reflects creation settings ───────────────────────────────────

/// `Db::config()` reflects the page size used at creation time.
#[test]
fn config_reflects_page_size_from_creation() {
    for &ps in &[4096_u32, 8192, 16384] {
        let db = Db::open_or_create(
            ":memory:",
            DbConfig {
                page_size: ps,
                ..DbConfig::default()
            },
        )
        .unwrap();
        assert_eq!(db.config().page_size, ps);
    }
}
