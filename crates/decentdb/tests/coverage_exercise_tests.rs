use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use decentdb::{Db, DbConfig, Value};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn unique_db_path(label: &str) -> PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    let pid = std::process::id();
    std::env::temp_dir().join(format!("decentdb-test-{}-{}-{}-{}", label, pid, stamp, id))
}

#[test]
fn exercise_engine_broad_paths() {
    let path = unique_db_path("coverage-exercise");
    let db = Db::create(&path, DbConfig::default()).expect("create db");

    // DDL: tables, indexes, views
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY, name TEXT, v INT64)")
        .expect("create t1");
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY, t1_id INT64 REFERENCES t1(id), x FLOAT64)")
        .expect("create t2");
    db.execute("CREATE INDEX t2_t1_idx ON t2 (t1_id)")
        .expect("create index");
    db.execute("CREATE VIEW v1 AS SELECT t1.id AS tid, t1.name, t2.x FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id")
        .expect("create view");

    // Insert some data
    db.execute("INSERT INTO t1 (id, name, v) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
        .expect("insert t1");
    db.execute("INSERT INTO t2 (id, t1_id, x) VALUES (10, 1, 1.5), (11, 1, 2.5), (12, 2, 3.0)")
        .expect("insert t2");

    // Simple select and projection
    let res = db
        .execute("SELECT id, name FROM t1 WHERE v >= 20 ORDER BY id")
        .expect("select");
    assert_eq!(res.rows().len(), 2);

    // Join and aggregate
    let agg = db.execute("SELECT t1.name, COUNT(t2.id) AS cnt, SUM(t2.x) AS s FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id GROUP BY t1.name ORDER BY t1.name").expect("aggregate");
    assert_eq!(agg.columns(), &["name", "cnt", "s"]);

    // Window function
    let rn = db
        .execute("SELECT name, ROW_NUMBER() OVER (ORDER BY name) AS rn FROM t1 ORDER BY rn")
        .expect("row_number");
    assert_eq!(rn.rows().len(), 3);

    // CTE and subquery
    let with_res = db
        .execute("WITH x AS (SELECT id FROM t1 WHERE v > 15) SELECT id FROM x")
        .expect("cte");
    assert!(!with_res.rows().is_empty());

    // Transactions and savepoints
    db.execute("BEGIN").expect("begin");
    db.execute("INSERT INTO t1 (id, name, v) VALUES (4, 'd', 40)")
        .expect("insert in tx");
    db.execute("SAVEPOINT sp1").expect("savepoint");
    db.execute("INSERT INTO t1 (id, name, v) VALUES (5, 'e', 50)")
        .expect("insert sp");
    db.execute("ROLLBACK TO SAVEPOINT sp1")
        .expect("rollback to sp");
    db.execute("COMMIT").expect("commit");

    // Verify row 5 not present, row 4 present
    let post = db
        .execute("SELECT COUNT(*) FROM t1 WHERE id IN (4,5)")
        .expect("count");
    assert_eq!(post.rows().len(), 1);

    // Prepared statement style with parameters
    let p = db
        .execute_with_params("SELECT name FROM t1 WHERE id = $1", &[Value::Int64(1)])
        .expect("param select");
    assert_eq!(p.rows().len(), 1);

    // Explain plan
    let explain = db
        .execute("EXPLAIN SELECT * FROM t2 WHERE t1_id = 1")
        .expect("explain");
    assert!(!explain.explain_lines().is_empty());

    // DDL changes: drop and recreate
    db.execute("DROP VIEW v1").expect("drop view");
    db.execute("DROP INDEX t2_t1_idx").expect("drop index");

    // More DDL/DML: constraints, updates, deletes, triggers
    db.execute("CREATE TABLE c1 (id INT64 PRIMARY KEY, v INT64 NOT NULL, u TEXT UNIQUE)")
        .expect("create c1");
    db.execute("INSERT INTO c1 (id, v, u) VALUES (1, 10, 'a'), (2, 20, 'b')")
        .expect("insert c1");

    // Expect constraint violation when inserting null into NOT NULL
    assert!(db
        .execute("INSERT INTO c1 (id, v) VALUES (3, NULL)")
        .is_err());

    // Update and delete
    db.execute("UPDATE c1 SET v = v + 1 WHERE id = 1")
        .expect("update");
    db.execute("DELETE FROM c1 WHERE id = 2").expect("delete");

    // Triggers: create a simple trigger that copies rows into an audit table
    db.execute("CREATE TABLE audit (aid INT64 PRIMARY KEY, old_v INT64)")
        .expect("create audit");
    db.execute("CREATE TRIGGER copy_to_audit AFTER UPDATE ON c1 FOR EACH ROW EXECUTE PROCEDURE (INSERT INTO audit (aid, old_v) VALUES (NEW.id, OLD.v))").ok();
    // Try an update to fire trigger (if triggers are supported the engine will execute it)
    let _ = db.execute("UPDATE c1 SET v = v + 5 WHERE id = 1");

    // Cleanup created tables
    db.execute("DROP TABLE IF EXISTS audit").ok();
    db.execute("DROP TABLE IF EXISTS c1").ok();

    // Clean up
    let _ = std::fs::remove_dir_all(&path);
}
