//! SQL persistence, WAL recovery, dump, and bulk load tests.
//!
//! Covers: File persistence roundtrips, WAL recovery, checkpoints,
//! reopen after close, dump_sql, bulk load operations, and schema
//! persistence across reopens.

use decentdb::{BulkLoadOptions, Db, DbConfig, QueryResult, Value};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

static NEXT_PERSIST_ID: AtomicU64 = AtomicU64::new(0);

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let ordinal = NEXT_PERSIST_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "decentdb-persist-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn cleanup_db(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(wal_path(path));
}

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn rows(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

#[test]
fn bulk_load_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT, score INT64)")
        .unwrap();
    let cols = &["id", "name", "score"];
    let data: Vec<Vec<Value>> = (0..100)
        .map(|i| {
            vec![
                Value::Int64(i),
                Value::Text(format!("item_{}", i)),
                Value::Int64(i * 10),
            ]
        })
        .collect();
    db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(100));
}

#[test]
fn bulk_load_large() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b INT64, c FLOAT64)")
        .unwrap();
    let cols = &["id", "a", "b", "c"];
    let data: Vec<Vec<Value>> = (0..5000)
        .map(|i| {
            vec![
                Value::Int64(i),
                Value::Text(format!("row_{:05}", i)),
                Value::Int64(i * 7),
                Value::Float64(i as f64 * 0.1),
            ]
        })
        .collect();
    let opts = BulkLoadOptions {
        checkpoint_on_complete: true,
        ..Default::default()
    };
    db.bulk_load_rows("t", cols, &data, opts).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(5000));
    let r2 = db.execute("SELECT SUM(b) FROM t").unwrap();
    let expected_sum: i64 = (0..5000).map(|i: i64| i * 7).sum();
    assert_eq!(rows(&r2)[0][0], Value::Int64(expected_sum));
}

#[test]
fn bulk_load_no_checkpoint() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let cols = &["id"];
    let data: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::Int64(i)]).collect();
    let opts = BulkLoadOptions {
        checkpoint_on_complete: false,
        ..Default::default()
    };
    db.bulk_load_rows("t", cols, &data, opts).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(10));
}

#[test]
fn bulk_load_with_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    let cols = &["id", "val"];
    let data: Vec<Vec<Value>> = (0..100)
        .map(|i| vec![Value::Int64(i), Value::Text(format!("v_{}", i))])
        .collect();
    db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
        .unwrap();
    let r = db.execute("SELECT id FROM t WHERE val = 'v_42'").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn bulk_load_with_nulls() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let cols = &["id", "val"];
    let data = vec![
        vec![Value::Int64(1), Value::Text("a".into())],
        vec![Value::Int64(2), Value::Null],
        vec![Value::Int64(3), Value::Text("c".into())],
    ];
    db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
        .unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE val IS NULL")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn checkpoint_on_memory_db() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // checkpoint on memory db should be fine (no-op or succeed)
    let _ = db.checkpoint();
}

#[test]
fn decimal_in_dump_sql() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, price DECIMAL(8, 2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 19.99)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
}

#[test]
fn dump_sql_complex_schema() {
    let db = mem_db();
    db.execute("CREATE TABLE users(id INT64 PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")
        .unwrap();
    db.execute("CREATE TABLE posts(id INT64 PRIMARY KEY, user_id INT64 REFERENCES users(id) ON DELETE CASCADE, title TEXT NOT NULL, body TEXT, score INT64 DEFAULT 0 CHECK (score >= 0))")
        .unwrap();
    db.execute("CREATE INDEX idx_posts_user ON posts(user_id)")
        .unwrap();
    db.execute("CREATE VIEW user_posts AS SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id")
        .unwrap();
    db.execute("CREATE TABLE audit(msg TEXT)").unwrap();
    db.execute("CREATE TRIGGER trg_post AFTER INSERT ON posts FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit VALUES (''new_post'')')").unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')")
        .unwrap();
    db.execute("INSERT INTO posts VALUES (1, 1, 'Hello', 'World', 5)")
        .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
    assert!(sql.len() > 100);
}

#[test]
fn dump_sql_includes_tables_and_indexes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ds (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE INDEX ds_name ON ds (name)");
    exec(&db, "INSERT INTO ds VALUES (1, 'hello')");
    let dump = db.dump_sql().unwrap();
    assert!(
        dump.contains("CREATE TABLE"),
        "dump should contain CREATE TABLE"
    );
    assert!(dump.contains("INSERT"), "dump should contain INSERT");
}

#[test]
fn dump_sql_roundtrip() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE dsr (id INT PRIMARY KEY, val TEXT NOT NULL)",
    );
    exec(&db, "INSERT INTO dsr VALUES (1, 'hello'), (2, 'world')");
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CREATE TABLE"));
    assert!(dump.contains("INSERT INTO"));
}

#[test]
fn dump_sql_with_bool_default() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, active BOOL DEFAULT TRUE)")
        .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
}

#[test]
fn dump_sql_with_complex_views() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT, val INT64)")
        .unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, label TEXT)")
        .unwrap();
    db.execute(
        "CREATE VIEW joined_v AS
         SELECT t1.name, t2.label, t1.val
         FROM t1 JOIN t2 ON t1.id = t2.t1_id
         WHERE t1.val > 0",
    )
    .unwrap();
    db.execute(
        "CREATE VIEW agg_v AS
         SELECT name, SUM(val) AS total FROM t1 GROUP BY name",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE VIEW"));
}

#[test]
fn dump_sql_with_default_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dump_defaults (id INT PRIMARY KEY, val TEXT DEFAULT 'hello', num INT DEFAULT 42)");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("DEFAULT"));
}

#[test]
fn dump_sql_with_indexes_and_views() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE dump_base (id INT PRIMARY KEY, name TEXT, val INT)",
    );
    exec(&db, "CREATE INDEX dump_idx ON dump_base (name)");
    exec(
        &db,
        "CREATE VIEW dump_view AS SELECT id, name FROM dump_base WHERE val > 10",
    );
    exec(&db, "INSERT INTO dump_base VALUES (1, 'test', 20)");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
    assert!(sql.contains("CREATE INDEX"));
    assert!(sql.contains("CREATE VIEW"));
}

#[test]
fn dump_sql_with_covering_index_include_clause() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE dump_cover (id INT PRIMARY KEY, k TEXT, payload TEXT, flag BOOL)",
    );
    exec(
        &db,
        "CREATE INDEX dump_cover_idx ON dump_cover (k) INCLUDE (payload, flag)",
    );
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains(
        "CREATE INDEX \"dump_cover_idx\" ON \"dump_cover\" (k) INCLUDE (\"payload\", \"flag\")"
    ));
}

#[test]
fn covering_index_include_columns_persist_after_reopen() {
    let path = unique_db_path("covering-index-persist");
    cleanup_db(&path);
    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        exec(
            &db,
            "CREATE TABLE cip (id INT64 PRIMARY KEY, k TEXT, payload TEXT, flag BOOL)",
        );
        exec(
            &db,
            "CREATE INDEX cip_idx ON cip (k) INCLUDE (payload, flag)",
        );
        db.checkpoint().unwrap();
    }

    let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
    let indexes = reopened.list_indexes().unwrap();
    let idx = indexes
        .iter()
        .find(|index| index.name == "cip_idx")
        .expect("covering index metadata");
    assert_eq!(
        idx.include_columns,
        vec!["payload".to_string(), "flag".to_string()]
    );
    let sql = reopened.dump_sql().unwrap();
    assert!(sql.contains("CREATE INDEX \"cip_idx\" ON \"cip\" (k) INCLUDE (\"payload\", \"flag\")"));
    cleanup_db(&path);
}

#[test]
fn create_schema_persists_after_reopen() {
    let path = unique_db_path("create-schema-persist");
    cleanup_db(&path);
    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        exec(&db, "CREATE SCHEMA app");
        db.checkpoint().unwrap();
    }
    let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
    reopened.execute("CREATE SCHEMA IF NOT EXISTS app").unwrap();
    let err = reopened.execute("CREATE SCHEMA app").unwrap_err();
    assert!(err.to_string().contains("schema app already exists"));
    cleanup_db(&path);
}

#[test]
fn dump_sql_with_not_null_and_defaults() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE d_full (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        val INT DEFAULT 0,
        flag BOOLEAN DEFAULT true
    )",
    );
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("NOT NULL"));
    assert!(sql.contains("DEFAULT"));
}

#[test]
fn dump_sql_with_null_defaults() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT DEFAULT NULL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
}

#[test]
fn file_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.checkpoint().unwrap();
    // After checkpoint, WAL should be folded into main file
    let r = db.execute("SELECT x FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn file_persistence_and_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'persist')").unwrap();
    }
    {
        let db = Db::open(&path, DbConfig::default()).unwrap();
        let r = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Text("persist".into()));
    }
}

#[test]
fn file_persistence_basic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'world')").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(2));
    }
}

#[test]
fn file_persistence_large_dataset() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("large.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
        for i in 0..500 {
            stmt.execute_in(
                &mut txn,
                &[Value::Int64(i), Value::Text(format!("data_{:04}", i))],
            )
            .unwrap();
        }
        txn.commit().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(500));
    }
}

#[test]
fn file_persistence_multiple_checkpoints() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("multi_ckpt.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        db.checkpoint().unwrap();
        db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(3));
    }
}

#[test]
fn file_persistence_wal_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
        // Don't checkpoint – data is only in WAL
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(1));
    }
}

#[test]
fn file_persistence_with_fk() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("fk.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE parent(id INT64 PRIMARY KEY)")
            .unwrap();
        db.execute("CREATE TABLE child(id INT64, pid INT64 REFERENCES parent(id))")
            .unwrap();
        db.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
        db.execute("INSERT INTO child VALUES (10, 1), (20, 2)")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM child").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(2));
    }
}

#[test]
fn file_persistence_with_index() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("idx.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alpha'), (2, 'beta')")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT id FROM t WHERE val = 'alpha'").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(1));
    }
}

#[test]
fn file_persistence_with_views_and_triggers() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("views.ddb");
    let path_str = path.to_str().unwrap();
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        db.execute("CREATE VIEW v AS SELECT id, val FROM t WHERE id > 0")
            .unwrap();
        db.execute("CREATE TABLE log(msg TEXT)").unwrap();
        db.execute("CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''row_added'')')").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path_str, DbConfig::default()).unwrap();
        let r = db.execute("SELECT * FROM v").unwrap();
        assert_eq!(rows(&r).len(), 1);
        let r2 = db.execute("SELECT * FROM log").unwrap();
        assert_eq!(rows(&r2).len(), 1);
    }
}

#[test]
fn metadata_dump_sql() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CREATE TABLE"));
    assert!(dump.contains("INSERT"));
}

#[test]
fn persist_bulk_load() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bulk.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        let cols = &["id", "val"];
        let data: Vec<Vec<Value>> = (0..1000)
            .map(|i| vec![Value::Int64(i), Value::Text(format!("row_{}", i))])
            .collect();
        db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
            .unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(1000));
    }
}

#[test]
fn persist_delete_and_reinsert() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("del_reins.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'v_{}')", i, i))
                .unwrap();
        }
        // Delete half
        db.execute("DELETE FROM t WHERE id >= 25").unwrap();
        // Reinsert with different values
        for i in 25..50 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'new_{}')", i, i))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT val FROM t WHERE id = 30").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Text("new_30".into()));
    }
}

#[test]
fn persist_heavy_writes_with_multiple_checkpoints() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("heavy.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
        for batch in 0..5 {
            for i in 0..100 {
                let id = batch * 100 + i;
                db.execute(&format!("INSERT INTO t VALUES ({}, 'data_{}')", id, id))
                    .unwrap();
            }
            db.checkpoint().unwrap();
        }
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(500));
    }
}

#[test]
fn persist_large_text() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("large_text.ddb");
    let ps = path.to_str().unwrap();
    let big = "x".repeat(50000);
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(1), Value::Text(big.clone())],
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
        if let Value::Text(s) = &rows(&r)[0][0] {
            assert_eq!(s.len(), 50000);
        }
    }
}

#[test]
fn persist_many_tables() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("many.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        for i in 0..20 {
            db.execute(&format!("CREATE TABLE t{}(id INT64, val TEXT)", i))
                .unwrap();
            db.execute(&format!("INSERT INTO t{} VALUES ({}, 'data')", i, i))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        for i in 0..20 {
            let r = db.execute(&format!("SELECT COUNT(*) FROM t{}", i)).unwrap();
            assert_eq!(rows(&r)[0][0], Value::Int64(1));
        }
    }
}

#[test]
fn persist_schema_changes_across_opens() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("schema.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t1(id INT64 PRIMARY KEY)").unwrap();
        db.execute("INSERT INTO t1 VALUES (1)").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t2(id INT64, ref_id INT64 REFERENCES t1(id))")
            .unwrap();
        db.execute("INSERT INTO t2 VALUES (10, 1)").unwrap();
        db.execute("CREATE INDEX idx ON t2(ref_id)").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("ALTER TABLE t1 ADD COLUMN name TEXT").unwrap();
        db.execute("UPDATE t1 SET name = 'hello' WHERE id = 1")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT id, name FROM t1").unwrap();
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Int64(1));
        assert_eq!(v[0][1], Value::Text("hello".into()));
        let r2 = db.execute("SELECT COUNT(*) FROM t2").unwrap();
        assert_eq!(rows(&r2)[0][0], Value::Int64(1));
    }
}

#[test]
fn persist_update_and_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("upd_del.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
            .unwrap();
        db.execute("UPDATE t SET val = 'updated' WHERE id = 2")
            .unwrap();
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT id, val FROM t ORDER BY id").unwrap();
        let v = rows(&r);
        assert_eq!(v.len(), 2);
        assert_eq!(v[1][1], Value::Text("updated".into()));
    }
}

#[test]
fn persist_with_txn() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("txn.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64)").unwrap();
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO t VALUES ($1)").unwrap();
        for i in 0..100 {
            stmt.execute_in(&mut txn, &[Value::Int64(i)]).unwrap();
        }
        txn.commit().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(100));
    }
}

#[test]
fn persistence_create_close_reopen() {
    let path = "/tmp/decentdb_test_persist_batch10.ddb";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE persist (id INT PRIMARY KEY, val TEXT)");
        exec(&db, "INSERT INTO persist VALUES (1, 'saved')");
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        let r = exec(&db, "SELECT val FROM persist WHERE id = 1");
        assert_eq!(r.rows()[0].values()[0], Value::Text("saved".to_string()));
    }
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
}

#[test]
fn persistence_with_indexes() {
    let path = "/tmp/decentdb_test_persist_idx_batch10.ddb";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE pidx (id INT PRIMARY KEY, val TEXT)");
        exec(&db, "CREATE INDEX pidx_val ON pidx (val)");
        for i in 0..20 {
            exec(&db, &format!("INSERT INTO pidx VALUES ({i}, 'v{i}')"));
        }
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        let r = exec(&db, "SELECT id FROM pidx WHERE val = 'v10'");
        assert_eq!(r.rows().len(), 1);
        let verification = db.verify_index("pidx_val").unwrap();
        assert!(verification.valid);
    }
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
}

#[test]
fn wal_checkpoint_and_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        for i in 0..50 {
            db.execute_with_params(
                "INSERT INTO t VALUES ($1, $2)",
                &[Value::Int64(i), Value::Text(format!("val_{}", i))],
            )
            .unwrap();
        }
        db.checkpoint().unwrap();
    }
    // Reopen and verify
    {
        let db = Db::open(&path, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(50));
    }
}

#[test]
fn wal_recovery_after_crash() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    {
        let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64)").unwrap();
        for i in 0..100 {
            db.execute_with_params("INSERT INTO t VALUES ($1)", &[Value::Int64(i)])
                .unwrap();
        }
        // Don't checkpoint — leave data in WAL
    }
    // Reopen should recover from WAL
    {
        let db = Db::open(&path, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(100));
    }
}

#[test]
fn wal_recovery_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'world')").unwrap();
    }

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
        let rows = rows(&result);
        assert_eq!(rows[0][0], Value::Int64(2));
    }
}

#[test]
fn wal_recovery_no_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("nocp.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        for i in 0..50 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'data_{}')", i, i))
                .unwrap();
        }
        // No checkpoint – WAL should have all the data
        drop(db);
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(50));
    }
}

#[test]
fn wal_recovery_partial_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("partial.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64)").unwrap();
        for i in 0..20 {
            db.execute(&format!("INSERT INTO t VALUES ({})", i))
                .unwrap();
        }
        db.checkpoint().unwrap();
        // More writes after checkpoint
        for i in 20..40 {
            db.execute(&format!("INSERT INTO t VALUES ({})", i))
                .unwrap();
        }
        drop(db);
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(40));
    }
}

#[test]
fn wal_recovery_with_checkpoint_and_more_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
        for i in 1..=100 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        db.checkpoint().unwrap();
        for i in 101..=150 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
    }

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
        let rows = rows(&result);
        assert_eq!(rows[0][0], Value::Int64(150));
    }
}

// ── Tests merged from row_id_persistence_tests.rs ──

#[test]
fn reopen_preserves_internal_next_row_id_for_rowid_tables() {
    let path = unique_db_path("row-id-persistence");
    let config = DbConfig::default();

    let setup = Db::open_or_create(&path, config.clone()).expect("open setup database");
    setup
        .execute("CREATE TABLE t (id INT64, payload TEXT)")
        .expect("create table");
    for id in 0_i64..30 {
        setup
            .execute_with_params(
                "INSERT INTO t VALUES ($1, $2)",
                &[Value::Int64(id), Value::Text("seed".repeat(8))],
            )
            .expect("insert setup row");
    }
    drop(setup);

    let reopened = Db::open(&path, config).expect("reopen database");
    reopened
        .execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(30), Value::Text("writer".repeat(8))],
        )
        .expect("insert reopened row");
    reopened
        .execute("DELETE FROM t WHERE id >= 30")
        .expect("delete reopened row");

    let count = reopened
        .execute("SELECT COUNT(*) FROM t")
        .expect("count rows");
    assert_eq!(count.rows()[0].values(), &[Value::Int64(30)]);

    drop(reopened);
    cleanup_db(&path);
}
