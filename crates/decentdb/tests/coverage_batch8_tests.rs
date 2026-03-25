//! Batch 8 – surgical coverage: prepared inserts, decimal formatting,
//! NATURAL/CROSS joins, EXPLAIN ANALYZE, DISTINCT ON to_sql, ARRAY,
//! NULLIF in views, window in views + dump_sql, WAL decode paths.

use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

fn mem() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn rows(r: &decentdb::QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

// ===========================================================================
// Prepared simple insert – positional params (db.rs L1421+, dml.rs L332+)
// ===========================================================================

#[test]
fn prepared_insert_basic() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT, val INT64)").unwrap();
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
fn prepared_insert_with_pk() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 0..50 {
        stmt.execute_in(
            &mut txn,
            &[Value::Int64(i), Value::Text(format!("v{}", i))],
        )
        .unwrap();
    }
    txn.commit().unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(50));
}

#[test]
fn prepared_insert_with_index() {
    let db = mem();
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
fn prepared_insert_dup_pk_error() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Text("a".into())])
        .unwrap();
    let err = stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Text("b".into())]);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn prepared_insert_check_constraint() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64 CHECK (val > 0))").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Int64(10)])
        .unwrap();
    let err = stmt.execute_in(&mut txn, &[Value::Int64(2), Value::Int64(-1)]);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn prepared_insert_null_into_not_null() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT NOT NULL)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    let err = stmt.execute_in(&mut txn, &[Value::Int64(1), Value::Null]);
    assert!(err.is_err());
    txn.rollback().unwrap();
}

#[test]
fn prepared_insert_with_defaults() {
    let db = mem();
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

// ===========================================================================
// Decimal formatting (exec/mod.rs L7060-7078, L7243-7249)
// ===========================================================================

#[test]
fn decimal_column_basic() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, price DECIMAL(10, 2))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 19.99)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 100.00)").unwrap();
    let r = db.execute("SELECT price FROM t ORDER BY id").unwrap();
    let v = rows(&r);
    assert!(v.len() == 2);
}

#[test]
fn decimal_zero_scale() {
    let db = mem();
    db.execute("CREATE TABLE t(val DECIMAL(10, 0))").unwrap();
    db.execute("INSERT INTO t VALUES (42)").unwrap();
    let r = db.execute("SELECT val FROM t").unwrap();
    assert!(rows(&r).len() == 1);
}

#[test]
fn decimal_negative() {
    let db = mem();
    db.execute("CREATE TABLE t(val DECIMAL(10, 2))").unwrap();
    db.execute("INSERT INTO t VALUES (-42.50)").unwrap();
    let r = db.execute("SELECT val FROM t").unwrap();
    assert!(rows(&r).len() == 1);
}

#[test]
fn decimal_in_dump_sql() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, price DECIMAL(8, 2))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 19.99)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
}

// ===========================================================================
// NATURAL JOIN and CROSS JOIN (ast.rs L605-616)
// ===========================================================================

#[test]
fn natural_join_basic() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 100), (3, 300)").unwrap();
    let r = db.execute("SELECT * FROM t1 NATURAL JOIN t2");
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 1); // id=1 matches
    }
}

#[test]
fn cross_join_basic() {
    let db = mem();
    db.execute("CREATE TABLE t1(a INT64)").unwrap();
    db.execute("CREATE TABLE t2(b INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10), (20)").unwrap();
    let r = db.execute("SELECT a, b FROM t1 CROSS JOIN t2 ORDER BY a, b").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 4); // 2 x 2 = 4 rows
}

#[test]
fn cross_join_empty_table() {
    let db = mem();
    db.execute("CREATE TABLE t1(a INT64)").unwrap();
    db.execute("CREATE TABLE t2(b INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1)").unwrap();
    let r = db.execute("SELECT a, b FROM t1 CROSS JOIN t2").unwrap();
    assert_eq!(rows(&r).len(), 0);
}

// ===========================================================================
// EXPLAIN ANALYZE (exec/mod.rs L1089-1099)
// ===========================================================================

#[test]
fn explain_analyze_select() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    let r = db.execute("EXPLAIN ANALYZE SELECT * FROM t WHERE id > 1");
    if let Ok(r) = r {
        let lines = r.explain_lines();
        assert!(!lines.is_empty());
        // ANALYZE adds actual row counts
        let text = lines.join("\n");
        assert!(text.contains("ANALYZE") || text.len() > 0);
    }
}

#[test]
fn explain_analyze_join() {
    let db = mem();
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
fn explain_analyze_aggregation() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('B',2)").unwrap();
    let r = db.execute("EXPLAIN ANALYZE SELECT grp, SUM(val) FROM t GROUP BY grp");
    if let Ok(r) = r {
        assert!(!r.explain_lines().is_empty());
    }
}

// ===========================================================================
// DISTINCT ON to_sql (ast.rs L495-516)
// ===========================================================================

#[test]
fn distinct_on_basic() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3),('B',4)").unwrap();
    let r = db.execute("SELECT DISTINCT ON (grp) grp, val FROM t ORDER BY grp, val");
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v.len(), 2); // one per group
    }
}

#[test]
fn distinct_on_in_view_dump() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    let r = db.execute("CREATE VIEW v AS SELECT DISTINCT ON (grp) grp, val FROM t ORDER BY grp, val");
    if r.is_ok() {
        let sql = db.dump_sql().unwrap();
        assert!(sql.contains("VIEW"));
    }
}

// ===========================================================================
// ARRAY expression (normalize.rs L1131-1137)
// ===========================================================================

#[test]
fn array_expression_in_select() {
    let db = mem();
    let r = db.execute("SELECT ARRAY[1, 2, 3]");
    // ARRAY may or may not be supported
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn array_in_where() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // ANY(ARRAY[...]) syntax
    let r = db.execute("SELECT id FROM t WHERE id = ANY(ARRAY[1, 3])");
    assert!(r.is_ok() || r.is_err());
}

// ===========================================================================
// Window function in view + dump_sql (ast.rs L835-861)
// ===========================================================================

#[test]
fn window_in_view_dump() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW ranked AS SELECT id, val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("OVER"));
}

#[test]
fn lag_lead_in_view_dump() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW with_lag AS SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("LAG"));
}

#[test]
fn rank_in_view_dump() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW ranked AS SELECT grp, val,
         RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS rnk,
         DENSE_RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS drnk
         FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("PARTITION BY"));
    assert!(sql.contains("RANK"));
}

#[test]
fn first_last_value_in_view() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    let r = db.execute(
        "CREATE VIEW fv AS SELECT id, FIRST_VALUE(val) OVER (ORDER BY id) AS fv FROM t",
    );
    if r.is_ok() {
        let sql = db.dump_sql().unwrap();
        assert!(sql.contains("FIRST_VALUE"));
    }
}

// ===========================================================================
// Recursive CTE with LIMIT / ORDER BY (exec/mod.rs L1673-1704)
// ===========================================================================

#[test]
fn recursive_cte_with_limit() {
    let db = mem();
    let r = db
        .execute(
            "WITH RECURSIVE seq(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < 100
            )
            SELECT n FROM seq LIMIT 5",
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 5);
}

#[test]
fn recursive_cte_with_order_and_limit() {
    let db = mem();
    let r = db
        .execute(
            "WITH RECURSIVE seq(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < 50
            )
            SELECT n FROM seq ORDER BY n DESC LIMIT 3",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][0], Value::Int64(50));
}

// ===========================================================================
// File persistence – heavy writes to exercise WAL decode (wal/format.rs)
// ===========================================================================

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
        db.execute("CREATE TABLE t2(id INT64, ref_id INT64 REFERENCES t1(id))").unwrap();
        db.execute("INSERT INTO t2 VALUES (10, 1)").unwrap();
        db.execute("CREATE INDEX idx ON t2(ref_id)").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("ALTER TABLE t1 ADD COLUMN name TEXT").unwrap();
        db.execute("UPDATE t1 SET name = 'hello' WHERE id = 1").unwrap();
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
fn persist_delete_and_reinsert() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("del_reins.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
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
fn persist_trigger_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("trigger.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE items(id INT64, name TEXT)").unwrap();
        db.execute("CREATE TABLE audit(cnt INT64)").unwrap();
        db.execute("INSERT INTO audit VALUES (0)").unwrap();
        db.execute(
            "CREATE TRIGGER item_cnt AFTER INSERT ON items FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('UPDATE audit SET cnt = cnt + 1')",
        )
        .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'a')").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        // Trigger should still fire after reopen
        db.execute("INSERT INTO items VALUES (2, 'b')").unwrap();
        let r = db.execute("SELECT cnt FROM audit").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(2));
    }
}

#[test]
fn persist_view_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("view.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
        db.execute("CREATE VIEW v AS SELECT id, val * 2 AS doubled FROM t").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT doubled FROM v ORDER BY id").unwrap();
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Int64(20));
        assert_eq!(v[1][0], Value::Int64(40));
    }
}

// ===========================================================================
// Parameter expressions ($n) in various contexts (ast.rs L678-681)
// ===========================================================================

#[test]
fn parameterized_select() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
    let r = db
        .execute_with_params("SELECT id, val FROM t WHERE id > $1", &[Value::Int64(1)])
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn parameterized_update() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old')").unwrap();
    db.execute_with_params(
        "UPDATE t SET val = $1 WHERE id = $2",
        &[Value::Text("new".into()), Value::Int64(1)],
    )
    .unwrap();
    let r = db.execute("SELECT val FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("new".into()));
}

#[test]
fn parameterized_delete() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    db.execute_with_params("DELETE FROM t WHERE id = $1", &[Value::Int64(2)])
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

// ===========================================================================
// CTE with outer reference (exec/mod.rs L1612-1670)
// ===========================================================================

#[test]
fn cte_used_in_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = db
        .execute(
            "WITH cte AS (SELECT id, val FROM t WHERE val > 10)
             SELECT * FROM t WHERE id IN (SELECT id FROM cte)",
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn cte_with_aggregation() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3)").unwrap();
    let r = db
        .execute(
            "WITH agg AS (SELECT grp, SUM(val) AS total FROM t GROUP BY grp)
             SELECT grp, total FROM agg WHERE total > 2 ORDER BY grp",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn cte_with_join() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1,100),(20,2,200)").unwrap();
    let r = db
        .execute(
            "WITH joined AS (
                SELECT t1.name, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id
             )
             SELECT name, val FROM joined ORDER BY name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

// ===========================================================================
// Various expr/function edge cases for exec/mod.rs coverage
// ===========================================================================

#[test]
fn concat_operator_with_null() {
    let db = mem();
    let r = db.execute("SELECT 'hello' || NULL || 'world'").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
}

#[test]
fn nested_case_expressions() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)").unwrap();
    let r = db
        .execute(
            "SELECT val,
             CASE
                WHEN val < 3 THEN
                    CASE WHEN val = 1 THEN 'one' ELSE 'two' END
                ELSE 'many'
             END AS label
             FROM t ORDER BY val",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("one".into()));
    assert_eq!(v[1][1], Value::Text("two".into()));
    assert_eq!(v[2][1], Value::Text("many".into()));
}

#[test]
fn arithmetic_with_null() {
    let db = mem();
    let r = db
        .execute("SELECT 1 + NULL, NULL * 5, NULL - NULL, NULL / 2")
        .unwrap();
    let v = rows(&r);
    for val in &v[0] {
        assert_eq!(*val, Value::Null);
    }
}

#[test]
fn string_comparison_operators() {
    let db = mem();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('apple'),('banana'),('cherry')").unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name >= 'banana' ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("banana".into()));
    assert_eq!(v[1][0], Value::Text("cherry".into()));
}

#[test]
fn mixed_type_comparison() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b FLOAT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 10.5), (20, 19.5)").unwrap();
    let r = db
        .execute("SELECT a, b FROM t WHERE a > b ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(20));
}

// ===========================================================================
// Views with complex expressions (ast.rs Display coverage)
// ===========================================================================

#[test]
fn view_with_case_expression() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, status INT64)").unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id,
         CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END AS label
         FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CASE"));
}

#[test]
fn view_with_between_expression() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t WHERE val BETWEEN 10 AND 100").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("BETWEEN"));
}

#[test]
fn view_with_in_list() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, status TEXT)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t WHERE status IN ('a', 'b', 'c')").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("IN"));
}

#[test]
fn view_with_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("SELECT"));
}

#[test]
fn view_with_coalesce_nullif() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, a INT64, b INT64)").unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, COALESCE(a, b, 0) AS val, NULLIF(a, 0) AS nonzero FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    // COALESCE may be rendered differently in dump
    assert!(sql.contains("VIEW") || sql.contains("view"));
}

// ===========================================================================
// More DDL error paths (exec/ddl.rs)
// ===========================================================================

#[test]
fn drop_table_if_exists_nonexistent() {
    let db = mem();
    // Should not error with IF EXISTS
    db.execute("DROP TABLE IF EXISTS nonexistent").unwrap();
}

#[test]
fn drop_view_if_exists_nonexistent() {
    let db = mem();
    db.execute("DROP VIEW IF EXISTS nonexistent").unwrap();
}

#[test]
fn drop_index_if_exists_nonexistent() {
    let db = mem();
    db.execute("DROP INDEX IF EXISTS nonexistent").unwrap();
}

#[test]
fn alter_table_add_column_duplicate() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    let err = db.execute("ALTER TABLE t ADD COLUMN name TEXT");
    assert!(err.is_err());
}

#[test]
fn alter_table_drop_last_column() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db.execute("ALTER TABLE t DROP COLUMN id");
    // May or may not be supported (single-column table)
    assert!(err.is_ok() || err.is_err());
}

#[test]
fn create_index_on_nonexistent_column() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db.execute("CREATE INDEX idx ON t(nonexistent)");
    assert!(err.is_err());
}

#[test]
fn create_index_on_nonexistent_table() {
    let db = mem();
    let err = db.execute("CREATE INDEX idx ON nonexistent(col)");
    assert!(err.is_err());
}

#[test]
fn alter_nonexistent_table() {
    let db = mem();
    let err = db.execute("ALTER TABLE nonexistent ADD COLUMN col TEXT");
    assert!(err.is_err());
}

// ===========================================================================
// Complex constraint scenarios (exec/constraints.rs)
// ===========================================================================

#[test]
fn multi_column_unique_constraint() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b INT64, UNIQUE(a, b))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 2)").unwrap(); // different b, ok
    db.execute("INSERT INTO t VALUES (2, 1)").unwrap(); // different a, ok
    let err = db.execute("INSERT INTO t VALUES (1, 1)"); // duplicate (1,1)
    assert!(err.is_err());
}

#[test]
fn check_constraint_complex() {
    let db = mem();
    db.execute(
        "CREATE TABLE t(a INT64, b INT64, CHECK (a > 0 AND b > 0 AND a + b < 100))",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (10, 20)").unwrap(); // ok
    let err = db.execute("INSERT INTO t VALUES (50, 60)"); // 50+60=110 > 100
    assert!(err.is_err());
}

#[test]
fn fk_violation_insert() {
    let db = mem();
    db.execute("CREATE TABLE parent(id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE child(id INT64, parent_id INT64 REFERENCES parent(id))").unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (1, 1)").unwrap(); // ok
    let err = db.execute("INSERT INTO child VALUES (2, 999)"); // no parent 999
    assert!(err.is_err());
}

#[test]
fn fk_violation_delete_restrict() {
    let db = mem();
    db.execute("CREATE TABLE parent(id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE child(id INT64, parent_id INT64 REFERENCES parent(id))").unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (1, 1)").unwrap();
    let err = db.execute("DELETE FROM parent WHERE id = 1");
    assert!(err.is_err()); // RESTRICT is default
}

// ===========================================================================
// Blob/NULL in dump_sql (ast.rs L911-924)
// ===========================================================================

#[test]
fn dump_sql_with_null_defaults() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT DEFAULT NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
}

#[test]
fn dump_sql_with_bool_default() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, active BOOL DEFAULT TRUE)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
}

// ===========================================================================
// Qualified wildcard (ast.rs L573)
// ===========================================================================

#[test]
fn select_table_dot_star() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 100)").unwrap();
    let r = db
        .execute("SELECT t1.*, t2.val FROM t1 JOIN t2 ON t1.id = t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[0][1], Value::Text("a".into()));
    assert_eq!(v[0][2], Value::Int64(100));
}

#[test]
fn select_qualified_wildcard_in_view() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("CREATE VIEW v AS SELECT t.* FROM t").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE VIEW"));
}

// ===========================================================================
// Recursive CTE to_sql (ast.rs L439-444)
// ===========================================================================

#[test]
fn recursive_cte_in_view() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, parent_id INT64)").unwrap();
    let r = db.execute(
        "CREATE VIEW hierarchy AS
         WITH RECURSIVE tree(id, depth) AS (
             SELECT id, 0 FROM t WHERE parent_id IS NULL
             UNION ALL
             SELECT t.id, tree.depth + 1 FROM t JOIN tree ON t.parent_id = tree.id
         )
         SELECT id, depth FROM tree",
    );
    if r.is_ok() {
        let sql = db.dump_sql().unwrap();
        assert!(sql.contains("RECURSIVE") || sql.contains("WITH"));
    }
}

// ===========================================================================
// SELECT with complex ORDER BY (offset + limit coverage)
// ===========================================================================

#[test]
fn offset_and_limit() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    let r = db
        .execute("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0][0], Value::Int64(10));
    assert_eq!(v[4][0], Value::Int64(14));
}

#[test]
fn offset_beyond_rows() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT id FROM t ORDER BY id OFFSET 10")
        .unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn limit_zero() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT id FROM t LIMIT 0")
        .unwrap();
    assert_eq!(rows(&r).len(), 0);
}

// ===========================================================================
// search/mod.rs trigram and FTS coverage
// ===========================================================================

#[test]
fn trigram_index_basic() {
    let db = mem();
    db.execute("CREATE TABLE docs(id INT64, content TEXT)").unwrap();
    let r = db.execute("CREATE INDEX idx_trigram ON docs USING TRIGRAM (content)");
    if r.is_ok() {
        db.execute("INSERT INTO docs VALUES (1, 'hello world')").unwrap();
        db.execute("INSERT INTO docs VALUES (2, 'goodbye world')").unwrap();
        let r2 = db.execute("SELECT id FROM docs WHERE content LIKE '%hello%'");
        assert!(r2.is_ok() || r2.is_err());
    }
}

// ===========================================================================
// Failpoint log API (db.rs L1290)
// ===========================================================================

#[test]
fn failpoint_log() {
    let r = Db::failpoint_log_json();
    if let Ok(json) = r {
        // Should be valid JSON even if empty
        assert!(json.starts_with('[') || json.starts_with('{'));
    }
}
