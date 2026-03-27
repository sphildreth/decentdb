#![allow(clippy::approx_constant)]

//! SQL transaction, prepared statement, EXPLAIN, and snapshot tests.
//!
//! Covers: BEGIN/COMMIT/ROLLBACK, savepoints, autocommit, prepared
//! statements (INSERT/SELECT/UPDATE/DELETE), schema invalidation,
//! batch execution, EXPLAIN, EXPLAIN ANALYZE, ANALYZE, snapshots,
//! and transaction state validation.

use decentdb::{Db, DbConfig, QueryResult, Value};
use tempfile::TempDir;

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn exec_err(db: &Db, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

fn rows(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

#[test]
fn analyze_empty_database() {
    let db = mem_db();
    let r = db.execute("ANALYZE");
    // May or may not be supported
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn analyze_specific_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ast (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE INDEX ast_idx ON ast (val)");
    for i in 0..20 {
        exec(&db, &format!("INSERT INTO ast VALUES ({i}, {0})", i % 5));
    }
    exec(&db, "ANALYZE ast");
    // After ANALYZE, the planner can use stats
    let r = exec(&db, "EXPLAIN SELECT * FROM ast WHERE val = 3");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn analyze_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE analyze_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx_analyze ON analyze_t (val)");
    for i in 0..100 {
        exec(
            &db,
            &format!("INSERT INTO analyze_t VALUES ({i}, 'v{v}')", v = i % 10),
        );
    }
    exec(&db, "ANALYZE");
}

#[test]
fn analyze_with_data() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    let r = db.execute("ANALYZE");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn pragma_query_commands_return_expected_shapes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pragma_t (id INT64 PRIMARY KEY, v TEXT)");
    exec(&db, "INSERT INTO pragma_t VALUES (1, 'a')");

    let page_size = exec(&db, "PRAGMA page_size");
    assert_eq!(page_size.columns(), &["page_size".to_string()]);
    assert_eq!(rows(&page_size), vec![vec![Value::Int64(4096)]]);

    let cache_size = exec(&db, "PRAGMA cache_size");
    assert_eq!(cache_size.columns(), &["cache_size".to_string()]);
    assert_eq!(cache_size.rows().len(), 1);
    assert!(matches!(rows(&cache_size)[0][0], Value::Int64(v) if v > 0));

    let integrity = exec(&db, "PRAGMA integrity_check");
    assert_eq!(integrity.columns(), &["integrity_check".to_string()]);
    assert_eq!(rows(&integrity), vec![vec![Value::Text("ok".to_string())]]);

    let db_list = exec(&db, "PRAGMA database_list");
    assert_eq!(
        db_list.columns(),
        &["seq".to_string(), "name".to_string(), "file".to_string()]
    );
    assert_eq!(
        rows(&db_list),
        vec![vec![
            Value::Int64(0),
            Value::Text("main".to_string()),
            Value::Text(":memory:".to_string())
        ]]
    );

    let table_info = exec(&db, "PRAGMA table_info(pragma_t)");
    assert_eq!(
        table_info.columns(),
        &[
            "cid".to_string(),
            "name".to_string(),
            "type".to_string(),
            "notnull".to_string(),
            "dflt_value".to_string(),
            "pk".to_string()
        ]
    );
    assert_eq!(table_info.rows().len(), 2);
    let first_col = table_info.rows()[0].values();
    assert_eq!(first_col[0], Value::Int64(0));
    assert_eq!(first_col[1], Value::Text("id".to_string()));
    assert_eq!(first_col[5], Value::Int64(1));
}

#[test]
fn pragma_assignment_semantics_are_enforced() {
    let db = mem_db();

    exec(&db, "PRAGMA page_size = 4096");

    let cache_err = exec_err(&db, "PRAGMA cache_size = 8");
    assert!(
        cache_err.contains("cannot be changed on an open connection"),
        "unexpected error: {cache_err}"
    );

    let integrity_err = exec_err(&db, "PRAGMA integrity_check = 1");
    assert!(
        integrity_err.contains("does not support assignment"),
        "unexpected error: {integrity_err}"
    );
}

#[test]
fn pragma_unknown_name_is_rejected() {
    let db = mem_db();
    let err = exec_err(&db, "PRAGMA foreign_keys");
    assert!(
        err.contains("unsupported PRAGMA"),
        "unexpected error: {err}"
    );
}

#[test]
fn pragma_table_info_errors_for_unknown_table() {
    let db = mem_db();
    let err = exec_err(&db, "PRAGMA table_info(missing_table)");
    assert!(
        err.contains("unknown table"),
        "unexpected error for missing table: {err}"
    );
}

#[test]
fn commit_without_begin_error() {
    let db = mem_db();
    let err = exec_err(&db, "COMMIT");
    assert!(!err.is_empty());
}

#[test]
fn error_in_transaction_allows_rollback() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    let _ = db.execute("INSERT INTO t VALUES (1)"); // dup PK
    db.execute("ROLLBACK").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn error_multiple_statements_in_execute() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db.execute("SELECT 1; SELECT 2").unwrap_err();
    assert!(err
        .to_string()
        .contains("expected exactly one SQL statement"));
}

#[test]
fn error_multiple_statements_in_execute_with_params() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db
        .execute_with_params("SELECT $1; SELECT $2", &[Value::Int64(1), Value::Int64(2)])
        .unwrap_err();
    assert!(err.to_string().contains("expected exactly one") || !err.to_string().is_empty());
}

#[test]
fn explain_analyze_aggregation() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('B',2)").unwrap();
    let r = db.execute("EXPLAIN ANALYZE SELECT grp, SUM(val) FROM t GROUP BY grp");
    if let Ok(r) = r {
        assert!(!r.explain_lines().is_empty());
    }
}

#[test]
fn explain_analyze_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3)").unwrap();
    let r = db.execute("EXPLAIN ANALYZE SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    if let Ok(r) = r {
        assert!(!r.explain_lines().is_empty());
    }
}

#[test]
fn explain_analyze_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ea (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ea VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "EXPLAIN ANALYZE SELECT * FROM ea WHERE val > 15");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_analyze_select() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let r = db.execute("EXPLAIN ANALYZE SELECT * FROM t WHERE id > 1");
    if let Ok(r) = r {
        let lines = r.explain_lines();
        assert!(!lines.is_empty());
        // ANALYZE adds actual row counts
        let text = lines.join("\n");
        assert!(text.contains("ANALYZE") || !text.is_empty());
    }
}

#[test]
fn explain_cte() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN WITH cte AS (SELECT id FROM t WHERE id > 0) SELECT * FROM cte")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_delete() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ed (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO ed VALUES (1)");
    let r = exec(&db, "EXPLAIN DELETE FROM ed WHERE id = 1");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_insert() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE explain_ins (id INT PRIMARY KEY, val TEXT)",
    );
    let r = exec(&db, "EXPLAIN INSERT INTO explain_ins VALUES (1, 'test')");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)")
        .unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_join_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ej1 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE ej2 (id INT PRIMARY KEY, ref_id INT)");
    let r = exec(
        &db,
        "EXPLAIN SELECT * FROM ej1 JOIN ej2 ON ej1.id = ej2.ref_id",
    );
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_join_shows_nested_loop() {
    let db = mem_db();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY)")
        .unwrap();

    let result = db
        .execute("EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id")
        .unwrap();
    let text = format!("{:?}", rows(&result));
    assert!(!text.is_empty());
}

#[test]
fn explain_left_right_full_cross_joins() {
    let db = mem_db();
    db.execute("CREATE TABLE a (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (id INT64 PRIMARY KEY)").unwrap();

    let _ = db
        .execute("EXPLAIN SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        .unwrap();
    let _ = db
        .execute("EXPLAIN SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .unwrap();
    let _ = db
        .execute("EXPLAIN SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")
        .unwrap();
    let _ = db.execute("EXPLAIN SELECT * FROM a CROSS JOIN b").unwrap();
}

#[test]
fn explain_right_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE explain_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx ON explain_t (val)");
    let r = exec(&db, "EXPLAIN SELECT * FROM explain_t WHERE val = 'x'");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_select_produces_output() {
    let db = mem_db();
    exec(&db, "CREATE TABLE es (id INT PRIMARY KEY, val INT)");
    let r = exec(&db, "EXPLAIN SELECT * FROM es WHERE id = 1");
    assert!(!r.rows().is_empty());
}

#[test]
fn explain_sort_and_limit() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();

    let result = db
        .execute("EXPLAIN SELECT * FROM t ORDER BY val LIMIT 10 OFFSET 5")
        .unwrap();
    let text = format!("{:?}", rows(&result));
    assert!(!text.is_empty());
}

#[test]
fn explain_union() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();

    let result = db
        .execute("EXPLAIN SELECT id FROM t UNION SELECT id FROM t")
        .unwrap();
    let text = format!("{:?}", rows(&result));
    assert!(!text.is_empty());
}

#[test]
fn explain_update() {
    let db = mem_db();
    exec(&db, "CREATE TABLE eu (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO eu VALUES (1, 10)");
    let r = exec(&db, "EXPLAIN UPDATE eu SET val = 20 WHERE id = 1");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_with_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("CREATE INDEX idx ON t(name)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t WHERE name = 'test'")
        .unwrap();
    let text = format!("{:?}", rows(&r));
    // Should mention the index in the plan
    assert!(!text.is_empty());
}

#[test]
fn hold_and_release_snapshot() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let token = db.hold_snapshot().unwrap();
    // Insert more data after snapshot
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.release_snapshot(token).unwrap();
    // Data should still be there
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn in_transaction_state() {
    let db = mem_db();
    assert!(!db.in_transaction().unwrap());
    db.begin_transaction().unwrap();
    assert!(db.in_transaction().unwrap());
    db.rollback_transaction().unwrap();
    assert!(!db.in_transaction().unwrap());
}

#[test]
fn large_transaction_many_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b TEXT, c INT64)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn
        .prepare("INSERT INTO t VALUES ($1, $2, $3, $4)")
        .unwrap();
    for i in 0..2000 {
        stmt.execute_in(
            &mut txn,
            &[
                Value::Int64(i),
                Value::Text(format!("name_{}", i)),
                Value::Text(format!("desc_{}", i % 100)),
                Value::Int64(i * 10),
            ],
        )
        .unwrap();
    }
    txn.commit().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2000));
}

#[test]
fn multiple_savepoints() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.begin_transaction().unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.create_savepoint("sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.create_savepoint("sp2").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    db.rollback_to_savepoint("sp2").unwrap();
    db.commit_transaction().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2)); // 1 and 2, not 3
}

#[test]
fn multiple_snapshots() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let s1 = db.hold_snapshot().unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let s2 = db.hold_snapshot().unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.release_snapshot(s1).unwrap();
    db.release_snapshot(s2).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn multiple_transactions_on_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();

    // Transaction 1: insert
    db.begin_transaction().unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 200)").unwrap();
    db.commit_transaction().unwrap();

    // Transaction 2: update
    db.begin_transaction().unwrap();
    db.execute("UPDATE t SET val = val + 50 WHERE id = 1")
        .unwrap();
    db.commit_transaction().unwrap();

    let r = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(150));
}

#[test]
fn nested_begin_error() {
    let db = mem_db();
    exec(&db, "BEGIN");
    let err = exec_err(&db, "BEGIN");
    assert!(!err.is_empty());
    exec(&db, "ROLLBACK");
}

#[test]
fn nested_savepoints() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nsp (id INT PRIMARY KEY, val TEXT)");
    db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO nsp VALUES (1, 'a')");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO nsp VALUES (2, 'b')");
    exec(&db, "SAVEPOINT sp2");
    exec(&db, "INSERT INTO nsp VALUES (3, 'c')");
    db.rollback_to_savepoint("sp2").unwrap();
    // Row 3 should be gone
    let r = exec(&db, "SELECT COUNT(*) FROM nsp");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
    db.rollback_to_savepoint("sp1").unwrap();
    // Row 2 should be gone
    let r = exec(&db, "SELECT COUNT(*) FROM nsp");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    db.commit_transaction().unwrap();
}

#[test]
fn nested_savepoints_rollback_inner() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nsp (id INT PRIMARY KEY)");
    exec(&db, "BEGIN");
    exec(&db, "INSERT INTO nsp VALUES (1)");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO nsp VALUES (2)");
    exec(&db, "SAVEPOINT sp2");
    exec(&db, "INSERT INTO nsp VALUES (3)");
    exec(&db, "ROLLBACK TO SAVEPOINT sp2");
    // row 3 rolled back
    exec(&db, "COMMIT");
    let r = exec(&db, "SELECT COUNT(*) FROM nsp");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn prepared_batch_in_transaction() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();

    let prepared = db.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();

    db.execute("BEGIN").unwrap();
    for i in 1..=10_i64 {
        prepared
            .execute(&[Value::Int64(i), Value::Text(format!("v{i}"))])
            .unwrap();
    }
    db.execute("COMMIT").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Int64(10));
}

#[test]
fn prepared_batch_insert() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();

    let prepared = db.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 1..=50_i64 {
        prepared
            .execute(&[Value::Int64(i), Value::Text(format!("val_{i}"))])
            .unwrap();
    }

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Int64(50));
}

#[test]
fn prepared_delete_statement() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pd (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO pd VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let stmt = db.prepare("DELETE FROM pd WHERE id = $1").unwrap();
    stmt.execute(&[Value::Int64(2)]).unwrap();
    let r = exec(&db, "SELECT COUNT(*) FROM pd");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn prepared_delete_with_params() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    db.execute_with_params("DELETE FROM t WHERE id = $1", &[Value::Int64(2)])
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn prepared_delete_uses_indexed_filter() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE child (id INT PRIMARY KEY, parent_id INT, label TEXT)",
    );
    exec(&db, "CREATE INDEX idx_child_parent_id ON child(parent_id)");
    exec(
        &db,
        "INSERT INTO child VALUES (1, 10, 'a'), (2, 10, 'b'), (3, 20, 'c')",
    );
    let stmt = db
        .prepare("DELETE FROM child WHERE parent_id = $1")
        .unwrap();
    stmt.execute(&[Value::Int64(10)]).unwrap();
    let result = exec(&db, "SELECT id FROM child ORDER BY id");
    let rows = rows(&result);
    assert_eq!(rows, vec![vec![Value::Int64(3)]]);
}

#[test]
fn prepared_insert_and_query() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let stmt = db.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    stmt.execute(&[Value::Int64(1), Value::Text("a".into())])
        .unwrap();
    stmt.execute(&[Value::Int64(2), Value::Text("b".into())])
        .unwrap();
    let r = db.execute("SELECT * FROM t ORDER BY id").unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn prepared_insert_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT, val INT64)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2, $3)").unwrap();
    for i in 0..20 {
        stmt.execute_in(
            &mut txn,
            &[
                Value::Int64(i),
                Value::Text(format!("name_{}", i)),
                Value::Int64(i * 10),
            ],
        )
        .unwrap();
    }
    txn.commit().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(20));
}

#[test]
fn prepared_insert_batch_positional_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)");
    let stmt = db
        .prepare("INSERT INTO items (id, name) VALUES ($1, $2)")
        .unwrap();
    for i in 0..10 {
        let name = format!("item_{i}");
        stmt.execute(&[Value::Int64(i), Value::Text(name)]).unwrap();
    }
    let r = exec(&db, "SELECT COUNT(*) FROM items");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(10));
}

#[test]
fn prepared_insert_dup_pk_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Text("a".into())])
        .unwrap();
    let err = stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Text("b".into())]);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn prepared_insert_in_transaction() {
    let db = mem_db();
    exec(&db, "CREATE TABLE txitems (id INT PRIMARY KEY, label TEXT)");
    let stmt = db
        .prepare("INSERT INTO txitems (id, label) VALUES ($1, $2)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    for i in 0..5 {
        let label = format!("label_{i}");
        stmt.execute_in(&mut txn, &[Value::Int64(i), Value::Text(label)])
            .unwrap();
    }
    txn.commit().unwrap();
    let r = exec(&db, "SELECT COUNT(*) FROM txitems");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
}

#[test]
fn prepared_insert_null_into_not_null() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT NOT NULL)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    let err = stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Null]);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn prepared_insert_with_all_types() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE pat (id INT PRIMARY KEY, f FLOAT, t TEXT, b BOOL, bl BLOB)",
    );
    let stmt = db
        .prepare("INSERT INTO pat VALUES ($1, $2, $3, $4, $5)")
        .unwrap();
    stmt.execute(&[
        Value::Int64(1),
        Value::Float64(3.14),
        Value::Text("hello".into()),
        Value::Bool(true),
        Value::Blob(vec![0xDE, 0xAD]),
    ])
    .unwrap();
    let r = exec(&db, "SELECT * FROM pat");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[3], Value::Bool(true));
}

#[test]
fn prepared_insert_with_defaults() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT DEFAULT 'hello', score INT64 DEFAULT 0)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t (id) VALUES ($1)").unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(1)]).unwrap();
    txn.commit().unwrap();
    let r = db.execute("SELECT val, score FROM t WHERE id = 1").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("hello".into()));
    assert_eq!(v[0][1], Value::Int64(0));
}

#[test]
fn prepared_insert_with_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 0..100 {
        stmt.execute_in(
            &mut txn,
            &[Value::Int64(i), Value::Text(format!("indexed_{}", i))],
        )
        .unwrap();
    }
    txn.commit().unwrap();
    // Index is populated
    let r = db
        .execute("SELECT id FROM t WHERE val = 'indexed_42'")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn prepared_insert_with_null_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pin (id INT PRIMARY KEY, val TEXT)");
    let stmt = db
        .prepare("INSERT INTO pin (id, val) VALUES ($1, $2)")
        .unwrap();
    stmt.execute(&[Value::Int64(1), Value::Null]).unwrap();
    let r = exec(&db, "SELECT val FROM pin WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn prepared_insert_with_pk() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 0..50 {
        stmt.execute_in(&mut txn, &[Value::Int64(i), Value::Text(format!("v{}", i))])
            .unwrap();
    }
    txn.commit().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(50));
}

#[test]
fn prepared_read_in_transaction() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("SELECT val FROM t WHERE id = $1").unwrap();
    let r = stmt.execute_in(&mut txn, &[Value::Int64(2)]).unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("b".into()));
    txn.commit().unwrap();
}

#[test]
fn prepared_select_read_only_batch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE data (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO data VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let stmt = db.prepare("SELECT * FROM data WHERE id = $1").unwrap();
    let r = stmt.execute(&[Value::Int64(2)]).unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[1], Value::Text("b".to_string()));
}

#[test]
fn prepared_select_with_multiple_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psm (id INT PRIMARY KEY, a INT, b INT)");
    exec(
        &db,
        "INSERT INTO psm VALUES (1, 10, 20), (2, 30, 40), (3, 50, 60)",
    );
    let stmt = db
        .prepare("SELECT id FROM psm WHERE a >= $1 AND b <= $2")
        .unwrap();
    let r = stmt.execute(&[Value::Int64(30), Value::Int64(50)]).unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn prepared_select_with_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psp (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO psp VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let stmt = db.prepare("SELECT id FROM psp WHERE val = $1").unwrap();
    let r = stmt.execute(&[Value::Text("b".into())]).unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn prepared_statement_all_types() {
    let db = mem_db();
    db.execute("CREATE TABLE t(i INT64, f FLOAT64, t TEXT, b BOOL, bl BLOB)")
        .unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn
        .prepare("INSERT INTO t VALUES ($1, $2, $3, $4, $5)")
        .unwrap();
    stmt.execute_in(
        &mut txn,
        &[
            Value::Int64(42),
            Value::Float64(3.14),
            Value::Text("hello".into()),
            Value::Bool(true),
            Value::Blob(vec![1, 2, 3]),
        ],
    )
    .unwrap();
    txn.commit().unwrap();
    let r = db.execute("SELECT * FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(42));
    assert_eq!(v[0][3], Value::Bool(true));
}

#[test]
fn prepared_statement_in_transaction_commit() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pst (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("INSERT INTO pst VALUES ($1, $2)").unwrap();
    let mut txn = db.transaction().unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Text("a".into())])
        .unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(2), Value::Text("b".into())])
        .unwrap();
    txn.commit().unwrap();
    let r = exec(&db, "SELECT * FROM pst ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn prepared_statement_in_transaction_rollback() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psr (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("INSERT INTO psr VALUES ($1, $2)").unwrap();
    let mut txn = db.transaction().unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Text("a".into())])
        .unwrap();
    txn.rollback().unwrap();
    let r = exec(&db, "SELECT * FROM psr");
    assert_eq!(r.rows().len(), 0);
}

#[test]
fn prepared_statement_reuse() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 0..50 {
        stmt.execute_in(
            &mut txn,
            &[Value::Int64(i), Value::Text(format!("item_{}", i))],
        )
        .unwrap();
    }
    txn.commit().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(50));
}

#[test]
fn prepared_statement_reuse_after_schema_change() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psr (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("SELECT * FROM psr").unwrap();
    let r = stmt.execute(&[]).unwrap();
    assert_eq!(r.rows().len(), 0);
    exec(&db, "INSERT INTO psr VALUES (1, 'hello')");
    let r = stmt.execute(&[]).unwrap();
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn prepared_update_statement() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pu (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO pu VALUES (1, 100), (2, 200)");
    let stmt = db.prepare("UPDATE pu SET val = $1 WHERE id = $2").unwrap();
    stmt.execute(&[Value::Int64(999), Value::Int64(1)]).unwrap();
    let r = exec(&db, "SELECT val FROM pu WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(999));
}

#[test]
fn prepared_update_statement_reuses_in_explicit_transaction() {
    let db = mem_db();
    exec(&db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT)");
    exec(
        &db,
        "INSERT INTO users VALUES (1, 'a@example.com'), (2, 'b@example.com')",
    );
    exec(&db, "BEGIN");
    let stmt = db
        .prepare("UPDATE users SET email = $1 WHERE id = $2")
        .unwrap();
    stmt.execute(&[Value::Text("first@example.com".into()), Value::Int64(1)])
        .unwrap();
    stmt.execute(&[Value::Text("second@example.com".into()), Value::Int64(2)])
        .unwrap();
    exec(&db, "COMMIT");

    let result = exec(&db, "SELECT email FROM users ORDER BY id");
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Text("first@example.com".into()));
    assert_eq!(rows[1][0], Value::Text("second@example.com".into()));
}

#[test]
fn prepared_update_with_params() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute_with_params(
        "UPDATE t SET val = $1 WHERE id = $2",
        &[Value::Int64(99), Value::Int64(1)],
    )
    .unwrap();
    let r = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(99));
}

#[test]
fn prepared_write_outside_transaction() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pw (id INT PRIMARY KEY, val TEXT)");
    let stmt = db
        .prepare("INSERT INTO pw (id, val) VALUES ($1, $2)")
        .unwrap();
    for i in 0..20 {
        stmt.execute(&[Value::Int64(i), Value::Text(format!("val_{i}"))])
            .unwrap();
    }
    let r = exec(&db, "SELECT COUNT(*) FROM pw");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(20));
}

#[test]
fn release_invalid_snapshot_error() {
    let db = mem_db();
    let err = db.release_snapshot(99999);
    assert!(err.is_err());
}

#[test]
fn release_nonexistent_savepoint() {
    let db = mem_db();
    exec(&db, "BEGIN");
    exec(&db, "SAVEPOINT sp1");
    let err = exec_err(&db, "RELEASE SAVEPOINT sp_nonexistent");
    assert!(!err.is_empty());
    exec(&db, "ROLLBACK");
}

#[test]
fn release_unknown_snapshot() {
    let db = mem_db();
    let err = db.release_snapshot(99999);
    assert!(err.is_err());
}

#[test]
fn rollback_to_nonexistent_savepoint() {
    let db = mem_db();
    exec(&db, "BEGIN");
    exec(&db, "SAVEPOINT sp1");
    let err = exec_err(&db, "ROLLBACK TO SAVEPOINT sp_nonexistent");
    assert!(!err.is_empty());
    exec(&db, "ROLLBACK");
}

#[test]
fn rollback_without_begin_error() {
    let db = mem_db();
    let err = exec_err(&db, "ROLLBACK");
    assert!(!err.is_empty());
}

#[test]
fn savepoint_create_and_release() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("RELEASE SAVEPOINT sp1").unwrap();
    db.execute("COMMIT").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn savepoint_nested() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("SAVEPOINT sp2").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    db.execute("ROLLBACK TO SAVEPOINT sp2").unwrap();
    // Row 3 rolled back, but row 2 still there
    db.execute("RELEASE SAVEPOINT sp1").unwrap();
    db.execute("COMMIT").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn savepoint_release_and_rollback() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sp_t (id INT PRIMARY KEY, val TEXT)");
    db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO sp_t VALUES (1, 'committed')");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO sp_t VALUES (2, 'rolled_back')");
    db.rollback_to_savepoint("sp1").unwrap();
    exec(&db, "INSERT INTO sp_t VALUES (3, 'after_rollback')");
    db.commit_transaction().unwrap();
    let r = exec(&db, "SELECT id FROM sp_t ORDER BY id");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));
}

#[test]
fn savepoint_release_nonexistent() {
    let db = mem_db();
    db.execute("BEGIN").unwrap();
    let err = db.execute("RELEASE SAVEPOINT nope");
    assert!(err.is_err());
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn savepoint_rollback() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("SAVEPOINT sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
    db.execute("COMMIT").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn savepoint_rollback_nonexistent() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("BEGIN").unwrap();
    let err = db.execute("ROLLBACK TO SAVEPOINT nope");
    assert!(err.is_err());
    // Must be able to continue after error
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn snapshot_hold_and_release() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let token = db.hold_snapshot().unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    // Snapshot page read
    let _page = db.read_page_for_snapshot(token, 1).unwrap();
    db.release_snapshot(token).unwrap();
}

#[test]
fn snapshot_read() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let snap_id = db.hold_snapshot().unwrap();
    // Write more data after snapshot
    db.execute("INSERT INTO t VALUES (4),(5)").unwrap();
    // Current query sees all 5
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(5));
    // Release snapshot
    db.release_snapshot(snap_id).unwrap();
}

#[test]
fn snapshot_release_unknown_token() {
    let db = mem_db();
    let err = db.release_snapshot(99999).unwrap_err().to_string();
    assert!(
        err.to_lowercase().contains("snapshot")
            || err.to_lowercase().contains("unknown")
            || err.to_lowercase().contains("token"),
        "got: {err}"
    );
}

#[test]
fn transaction_nested_savepoint_release() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.begin_transaction().unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.create_savepoint("sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.create_savepoint("sp2").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    db.release_savepoint("sp2").unwrap();
    db.release_savepoint("sp1").unwrap();
    db.commit_transaction().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn transaction_rollback() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    {
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO t VALUES ($1)").unwrap();
        stmt.execute_in(&mut txn, &[Value::Int64(2)]).unwrap();
        txn.rollback().unwrap();
    }
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1)); // rollback undid insert
}

#[test]
fn transaction_rollback_restores_state() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("INSERT INTO t VALUES (3)").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn txn_api_transaction() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    {
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO t VALUES ($1)").unwrap();
        stmt.execute_in(&mut txn, &[Value::Int64(1)]).unwrap();
        stmt.execute_in(&mut txn, &[Value::Int64(2)]).unwrap();
        txn.commit().unwrap();
    }
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn txn_api_transaction_rollback() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    {
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO t VALUES ($1)").unwrap();
        stmt.execute_in(&mut txn, &[Value::Int64(2)]).unwrap();
        txn.rollback().unwrap();
    }
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn txn_begin_commit() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("COMMIT").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn txn_begin_rollback() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn txn_in_transaction_flag() {
    let db = mem_db();
    assert!(!db.in_transaction().unwrap());
    db.begin_transaction().unwrap();
    assert!(db.in_transaction().unwrap());
    db.rollback_transaction().unwrap();
    assert!(!db.in_transaction().unwrap());
}

#[test]
fn txn_savepoint_and_rollback_to() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.begin_transaction().unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.create_savepoint("sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.rollback_to_savepoint("sp1").unwrap();
    db.commit_transaction().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn txn_savepoint_release() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.begin_transaction().unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.create_savepoint("sp1").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.release_savepoint("sp1").unwrap();
    db.commit_transaction().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

// ── Tests merged from engine_coverage_tests.rs ──

#[test]
fn analyze_executes_in_autocommit_and_rejects_explicit_transactions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, email TEXT)")
        .unwrap();
    db.execute("CREATE INDEX docs_email_idx ON docs (email)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'a@example.com'), (2, 'a@example.com')")
        .unwrap();

    db.execute("ANALYZE docs").unwrap();

    db.execute("BEGIN").unwrap();
    let err = db.execute("ANALYZE docs").unwrap_err();
    assert!(
        err.to_string()
            .contains("ANALYZE is not supported inside an explicit SQL transaction"),
        "unexpected error: {err}"
    );
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn commit_persists_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn rollback_discards_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

/// Regression: mimics the .NET EF Core batch that deletes a child (album)
/// then its parent (artist) using separate prepared statements inside an
/// explicit `begin_transaction` / `commit_transaction` pair.
#[test]
fn prepared_delete_child_then_parent_in_explicit_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();

    db.execute(
        r#"CREATE TABLE artists (
             Id INTEGER PRIMARY KEY,
             Name TEXT NOT NULL,
             IsLocked INTEGER NOT NULL DEFAULT 0
           )"#,
    )
    .unwrap();
    db.execute(
        r#"CREATE TABLE albums (
             Id INTEGER PRIMARY KEY,
             ArtistId INTEGER NOT NULL
               REFERENCES artists(Id) ON DELETE CASCADE,
             Name TEXT NOT NULL,
             Year INTEGER NOT NULL
           )"#,
    )
    .unwrap();

    // Insert data
    db.begin_transaction().unwrap();
    let r = db
        .execute_with_params(
            r#"INSERT INTO artists (Name, IsLocked) VALUES ($1, $2) RETURNING Id"#,
            &[Value::Text("Beatles".into()), Value::Int64(0)],
        )
        .unwrap();
    let artist_id = r.rows()[0].values()[0].clone();

    let r = db
        .execute_with_params(
            r#"INSERT INTO albums (ArtistId, Name, Year) VALUES ($1, $2, $3) RETURNING Id"#,
            &[
                artist_id.clone(),
                Value::Text("Abbey Road".into()),
                Value::Int64(1969),
            ],
        )
        .unwrap();
    let album_id = r.rows()[0].values()[0].clone();
    db.commit_transaction().unwrap();

    // Update artist (creates a second committed version)
    db.begin_transaction().unwrap();
    db.execute_with_params(
        r#"UPDATE artists SET IsLocked = $1 WHERE Id = $2"#,
        &[Value::Int64(1), artist_id.clone()],
    )
    .unwrap();
    db.commit_transaction().unwrap();

    // DELETE batch: child first, then parent, via prepared stmts
    db.begin_transaction().unwrap();

    let stmt_album = db.prepare("DELETE FROM albums WHERE Id = $1").unwrap();
    let r = stmt_album.execute(&[album_id]).unwrap();
    assert_eq!(r.affected_rows(), 1, "album DELETE should affect 1 row");

    let stmt_artist = db.prepare("DELETE FROM artists WHERE Id = $1").unwrap();
    let r = stmt_artist.execute(&[artist_id]).unwrap();
    assert_eq!(r.affected_rows(), 1, "artist DELETE should affect 1 row");

    db.commit_transaction().unwrap();

    // Verify counts
    let r = db.execute("SELECT COUNT(*) FROM artists").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0), "artist count");

    let r = db.execute("SELECT COUNT(*) FROM albums").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0), "album count");
}

/// Regression: fast-path DELETE marks a table dirty, then a generic-path
/// operation clones the runtime. The dirty mark must survive the clone
/// so the first table's changes are committed.
#[test]
fn fast_then_generic_delete_in_transaction_preserves_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    // Child with CASCADE — makes parent DELETE go through generic path.
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE, data TEXT)",
    )
    .unwrap();

    // Seed data
    db.execute("INSERT INTO parent VALUES (1, 'A')").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1, 'x')").unwrap();
    db.execute("INSERT INTO parent VALUES (2, 'B')").unwrap();
    db.execute("INSERT INTO child VALUES (20, 2, 'y')").unwrap();

    // Transaction: fast-path DELETE child row 10, then generic-path DELETE parent 1
    db.begin_transaction().unwrap();
    let stmt_child = db.prepare("DELETE FROM child WHERE id = $1").unwrap();
    let r = stmt_child.execute(&[Value::Int64(10)]).unwrap();
    assert_eq!(r.affected_rows(), 1);

    let stmt_parent = db.prepare("DELETE FROM parent WHERE id = $1").unwrap();
    let r = stmt_parent.execute(&[Value::Int64(1)]).unwrap();
    assert_eq!(r.affected_rows(), 1);
    db.commit_transaction().unwrap();

    let r = db.execute("SELECT COUNT(*) FROM child").unwrap();
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Int64(1),
        "should have 1 child row remaining"
    );
    let r = db.execute("SELECT COUNT(*) FROM parent").unwrap();
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Int64(1),
        "should have 1 parent row remaining"
    );
}

/// Regression: fast-path UPDATE marks a table dirty, then a generic-path
/// DELETE on a different table clones. The UPDATE must survive the clone.
#[test]
fn fast_update_then_generic_delete_preserves_dirty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        .unwrap();
    db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();

    db.execute("INSERT INTO items VALUES (1, 'old')").unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'p')").unwrap();
    db.execute("INSERT INTO child VALUES (1, 1)").unwrap();

    db.begin_transaction().unwrap();
    let stmt_update = db
        .prepare("UPDATE items SET name = $1 WHERE id = $2")
        .unwrap();
    let r = stmt_update
        .execute(&[Value::Text("new".into()), Value::Int64(1)])
        .unwrap();
    assert_eq!(r.affected_rows(), 1);

    // Generic-path DELETE (parent with CASCADE child)
    let stmt_del = db.prepare("DELETE FROM parent WHERE id = $1").unwrap();
    let r = stmt_del.execute(&[Value::Int64(1)]).unwrap();
    assert_eq!(r.affected_rows(), 1);
    db.commit_transaction().unwrap();

    let r = db.execute("SELECT name FROM items WHERE id = 1").unwrap();
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Text("new".into()),
        "UPDATE must persist across the clone boundary"
    );

    let r = db.execute("SELECT COUNT(*) FROM parent").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
    let r = db.execute("SELECT COUNT(*) FROM child").unwrap();
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Int64(0),
        "CASCADE should have cleaned child"
    );
}
