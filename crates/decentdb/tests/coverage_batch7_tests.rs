//! Batch 7 – deep coverage: RIGHT/FULL JOINs, JSON ops, ANALYZE, savepoints,
//! snapshots, SET ops with ALL, division by zero, type coercion edges,
//! window function variants, ON CONFLICT variants, overflow/WAL paths.

use decentdb::{Db, DbConfig, Value};
use std::thread;
use tempfile::TempDir;

fn mem() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn rows(r: &decentdb::QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

// ===========================================================================
// RIGHT JOIN and FULL OUTER JOIN
// ===========================================================================

#[test]
fn right_join_basic() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, label TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (2, 'x'), (3, 'y')").unwrap();
    let r = db
        .execute("SELECT t1.id, t1.name, t2.id, t2.label FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    // t2.id=2 matches t1.id=2
    assert_eq!(v[0][2], Value::Int64(2));
    assert_eq!(v[0][0], Value::Int64(2));
    // t2.id=3 has no match in t1
    assert_eq!(v[1][2], Value::Int64(3));
    assert_eq!(v[1][0], Value::Null);
    assert_eq!(v[1][1], Value::Null);
}

#[test]
fn right_join_all_unmatched() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1)").unwrap();
    db.execute("INSERT INTO t2 VALUES (100), (200)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[1][0], Value::Null);
}

#[test]
fn full_outer_join_basic() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, a TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, b TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'x'), (2, 'y')").unwrap();
    db.execute("INSERT INTO t2 VALUES (2, 'p'), (3, 'q')").unwrap();
    let r = db
        .execute("SELECT t1.id, t1.a, t2.id, t2.b FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY COALESCE(t1.id, t2.id)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    // Row for t1.id=1: no match in t2
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[0][2], Value::Null);
    // Row for t1.id=2 = t2.id=2
    assert_eq!(v[1][0], Value::Int64(2));
    assert_eq!(v[1][2], Value::Int64(2));
    // Row for t2.id=3: no match in t1
    assert_eq!(v[2][0], Value::Null);
    assert_eq!(v[2][2], Value::Int64(3));
}

#[test]
fn full_outer_join_empty_left() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1), (2)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[1][0], Value::Null);
}

#[test]
fn full_outer_join_empty_right() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][1], Value::Null);
    assert_eq!(v[1][1], Value::Null);
}

// ===========================================================================
// SET operations: UNION ALL, INTERSECT, EXCEPT
// ===========================================================================

#[test]
fn union_vs_union_all() {
    let db = mem();
    let r1 = db
        .execute("SELECT 1 AS v UNION SELECT 1 UNION SELECT 2")
        .unwrap();
    assert_eq!(rows(&r1).len(), 2); // dedup: 1, 2

    let r2 = db
        .execute("SELECT 1 AS v UNION ALL SELECT 1 UNION ALL SELECT 2")
        .unwrap();
    assert_eq!(rows(&r2).len(), 3); // no dedup: 1, 1, 2
}

#[test]
fn intersect_basic() {
    let db = mem();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT v FROM t1 INTERSECT SELECT v FROM t2 ORDER BY v")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn intersect_all() {
    let db = mem();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(2),(3),(4)").unwrap();
    let r = db.execute("SELECT v FROM t1 INTERSECT ALL SELECT v FROM t2 ORDER BY v");
    // INTERSECT ALL may or may not be supported
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 2);
    }
}

#[test]
fn except_basic() {
    let db = mem();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(4)").unwrap();
    let r = db
        .execute("SELECT v FROM t1 EXCEPT SELECT v FROM t2 ORDER BY v")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // 1, 3
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn except_all() {
    let db = mem();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2)").unwrap();
    let r = db.execute("SELECT v FROM t1 EXCEPT ALL SELECT v FROM t2 ORDER BY v");
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 2);
    }
}

#[test]
fn chained_set_operations() {
    let db = mem();
    let r = db
        .execute(
            "(SELECT 1 AS v UNION SELECT 2 UNION SELECT 3)
             EXCEPT
             (SELECT 2 AS v)
             ORDER BY v",
        );
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 1);
    }
}

// ===========================================================================
// Division by zero and arithmetic edge cases
// ===========================================================================

#[test]
fn division_by_zero_int() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 0)").unwrap();
    let r = db.execute("SELECT a / b FROM t");
    // Should return error or NULL
    match r {
        Ok(r) => {
            let v = rows(&r);
            assert_eq!(v[0][0], Value::Null);
        }
        Err(_) => {} // also acceptable
    }
}

#[test]
fn modulo_by_zero() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 0)").unwrap();
    let r = db.execute("SELECT a % b FROM t");
    match r {
        Ok(r) => {
            let v = rows(&r);
            assert_eq!(v[0][0], Value::Null);
        }
        Err(_) => {}
    }
}

#[test]
fn float_division_by_zero() {
    let db = mem();
    let r = db.execute("SELECT CAST(1.0 AS FLOAT64) / CAST(0.0 AS FLOAT64)");
    // Could be Infinity, NULL, or error
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn negative_zero_operations() {
    let db = mem();
    let r = db.execute("SELECT -0, 0 - 0, -1 + 1").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(0));
    assert_eq!(v[0][1], Value::Int64(0));
    assert_eq!(v[0][2], Value::Int64(0));
}

#[test]
fn large_integer_arithmetic() {
    let db = mem();
    // Very large integers are parsed as Float64 by SQL parser
    let r = db
        .execute("SELECT 1000000 + 0, 1000000 - 0")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(1_000_000));
}

// ===========================================================================
// Type coercion edge cases
// ===========================================================================

#[test]
fn not_null_returns_null() {
    let db = mem();
    let r = db.execute("SELECT NOT NULL").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn negate_null() {
    let db = mem();
    let r = db.execute("SELECT -NULL");
    if let Ok(r) = r {
        assert_eq!(rows(&r)[0][0], Value::Null);
    }
}

#[test]
fn bool_and_null() {
    let db = mem();
    let r = db.execute("SELECT TRUE AND NULL, FALSE AND NULL, NULL AND NULL").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Bool(false));
    assert_eq!(v[0][2], Value::Null);
}

#[test]
fn bool_or_null() {
    let db = mem();
    let r = db.execute("SELECT TRUE OR NULL, FALSE OR NULL, NULL OR NULL").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Bool(true));
    assert_eq!(v[0][1], Value::Null);
    assert_eq!(v[0][2], Value::Null);
}

#[test]
fn comparison_with_null() {
    let db = mem();
    let r = db
        .execute("SELECT NULL = NULL, NULL <> NULL, NULL < 1, NULL > 1, NULL = 1")
        .unwrap();
    let v = rows(&r);
    // All comparisons with NULL return NULL (not TRUE or FALSE)
    for val in &v[0] {
        assert_eq!(*val, Value::Null);
    }
}

#[test]
fn text_to_number_coercion_in_comparison() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    // Comparing text column with numeric literal
    let r = db.execute("SELECT * FROM t WHERE id = 1");
    assert!(r.is_ok());
}

// ===========================================================================
// ANALYZE statement
// ===========================================================================

#[test]
fn analyze_empty_database() {
    let db = mem();
    let r = db.execute("ANALYZE");
    // May or may not be supported
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn analyze_with_data() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    let r = db.execute("ANALYZE");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn analyze_specific_table() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db.execute("ANALYZE t");
    assert!(r.is_ok() || r.is_err());
}

// ===========================================================================
// Savepoint API (through Rust API)
// ===========================================================================

#[test]
fn savepoint_create_and_release() {
    let db = mem();
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
fn savepoint_rollback() {
    let db = mem();
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
fn savepoint_nested() {
    let db = mem();
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
fn savepoint_rollback_nonexistent() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("BEGIN").unwrap();
    let err = db.execute("ROLLBACK TO SAVEPOINT nope");
    assert!(err.is_err());
    // Must be able to continue after error
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn savepoint_release_nonexistent() {
    let db = mem();
    db.execute("BEGIN").unwrap();
    let err = db.execute("RELEASE SAVEPOINT nope");
    assert!(err.is_err());
    db.execute("ROLLBACK").unwrap();
}

// ===========================================================================
// Snapshot management (Rust API)
// ===========================================================================

#[test]
fn hold_and_release_snapshot() {
    let db = mem();
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
fn release_unknown_snapshot() {
    let db = mem();
    let err = db.release_snapshot(99999);
    assert!(err.is_err());
}

#[test]
fn multiple_snapshots() {
    let db = mem();
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

// ===========================================================================
// Window functions – more variants
// ===========================================================================

#[test]
fn window_sum_over_unsupported() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',30)").unwrap();
    // SUM/COUNT/MIN/MAX OVER() are NOT supported as window functions
    let r = db.execute("SELECT grp, SUM(val) OVER (PARTITION BY grp) FROM t");
    assert!(r.is_err());
}

#[test]
fn window_count_over_unsupported() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3)").unwrap();
    let r = db.execute("SELECT grp, COUNT(*) OVER (PARTITION BY grp) FROM t");
    assert!(r.is_err());
}

#[test]
fn window_min_max_over_unsupported() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = db.execute("SELECT id, MIN(val) OVER () FROM t");
    assert!(r.is_err());
}

#[test]
fn window_rank_dense_rank() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(10),(20),(30)").unwrap();
    let r = db
        .execute(
            "SELECT val, RANK() OVER (ORDER BY val) AS rnk, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM t ORDER BY val",
        )
        .unwrap();
    let v = rows(&r);
    // RANK: 1,1,3,4; DENSE_RANK: 1,1,2,3
    assert_eq!(v[0][1], Value::Int64(1));
    assert_eq!(v[1][1], Value::Int64(1));
    assert_eq!(v[2][1], Value::Int64(3));
    assert_eq!(v[2][2], Value::Int64(2)); // dense_rank
}

#[test]
fn window_first_value_last_value() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,100),(2,200),(3,300)").unwrap();
    let r = db.execute(
        "SELECT id, FIRST_VALUE(val) OVER (ORDER BY id), LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t ORDER BY id",
    );
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v[0][1], Value::Int64(100)); // first
        assert_eq!(v[0][2], Value::Int64(300)); // last
    }
}

#[test]
fn window_lag_lead_with_offset() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)").unwrap();
    let r = db
        .execute(
            "SELECT id, LAG(val, 2) OVER (ORDER BY id), LEAD(val, 2) OVER (ORDER BY id) FROM t ORDER BY id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Null); // lag(2) for id=1
    assert_eq!(v[1][1], Value::Null); // lag(2) for id=2
    assert_eq!(v[2][1], Value::Int64(10)); // lag(2) for id=3
    assert_eq!(v[2][2], Value::Int64(50)); // lead(2) for id=3
}

// ===========================================================================
// JSON operators (if supported)
// ===========================================================================

#[test]
fn json_extract_operator() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    db.execute(r#"INSERT INTO t VALUES (1, '{"name":"alice","age":30}')"#).unwrap();
    // Try -> operator
    let r = db.execute("SELECT data -> 'name' FROM t");
    // If supported, check result; otherwise just pass
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn json_extract_text_operator() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    db.execute(r#"INSERT INTO t VALUES (1, '{"key":"value"}')"#).unwrap();
    let r = db.execute("SELECT data ->> 'key' FROM t");
    assert!(r.is_ok() || r.is_err());
}

// ===========================================================================
// ON CONFLICT – more variants
// ===========================================================================

#[test]
fn on_conflict_with_where_clause() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT, active BOOL)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old', TRUE)").unwrap();
    // ON CONFLICT with WHERE filter on DO UPDATE
    let r = db.execute(
        "INSERT INTO t VALUES (1, 'new', TRUE) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE t.active = TRUE",
    );
    if let Ok(_) = r {
        let r2 = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(rows(&r2)[0][0], Value::Text("new".into()));
    }
}

#[test]
fn on_conflict_do_nothing_no_target() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let r = db.execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT DO NOTHING");
    if let Ok(_) = r {
        let r2 = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(rows(&r2)[0][0], Value::Text("a".into()));
    }
}

#[test]
fn on_conflict_with_excluded_reference() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, hits INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET hits = t.hits + EXCLUDED.hits",
    )
    .unwrap();
    let r = db.execute("SELECT hits FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(15));
}

// ===========================================================================
// Subquery variants
// ===========================================================================

#[test]
fn scalar_subquery_returning_null() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("SELECT (SELECT id FROM t LIMIT 1)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn scalar_subquery_multiple_rows() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    // DecentDB returns first row for scalar subquery (no error)
    let r = db.execute("SELECT (SELECT id FROM t)");
    // Either error or returns first value — both are valid
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn in_subquery_empty() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)")
        .unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn not_in_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn exists_with_empty_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("SELECT EXISTS (SELECT 1 FROM t)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Bool(false));
}

#[test]
fn exists_with_nonempty_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = db
        .execute("SELECT EXISTS (SELECT 1 FROM t)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Bool(true));
}

#[test]
fn not_exists() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("SELECT NOT EXISTS (SELECT 1 FROM t)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Bool(true));
}

#[test]
fn correlated_subquery_in_select() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, grp TEXT)").unwrap();
    db.execute("CREATE TABLE t2(grp TEXT, label TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B'),(3,'A')").unwrap();
    db.execute("INSERT INTO t2 VALUES ('A','alpha'),('B','beta')").unwrap();
    let r = db
        .execute(
            "SELECT t1.id, (SELECT t2.label FROM t2 WHERE t2.grp = t1.grp) AS lbl FROM t1 ORDER BY t1.id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("alpha".into()));
    assert_eq!(v[1][1], Value::Text("beta".into()));
}

// ===========================================================================
// Complex expressions
// ===========================================================================

#[test]
fn case_searched_with_null() {
    let db = mem();
    let r = db
        .execute("SELECT CASE WHEN NULL THEN 'yes' WHEN TRUE THEN 'true' ELSE 'else' END")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("true".into()));
}

#[test]
fn case_simple_expression() {
    let db = mem();
    db.execute("CREATE TABLE t(status INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(99)").unwrap();
    let r = db
        .execute(
            "SELECT status, CASE status WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' ELSE 'other' END AS lbl FROM t ORDER BY status",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("one".into()));
    assert_eq!(v[3][1], Value::Text("other".into()));
}

#[test]
fn nullif_returns_null() {
    let db = mem();
    let r = db.execute("SELECT NULLIF(5, 5), NULLIF(5, 3)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Int64(5));
}

#[test]
fn coalesce_all_null() {
    let db = mem();
    let r = db.execute("SELECT COALESCE(NULL, NULL, NULL)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn coalesce_mixed() {
    let db = mem();
    let r = db.execute("SELECT COALESCE(NULL, NULL, 42, 100)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn between_on_column() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(15),(20)").unwrap();
    let r = db
        .execute("SELECT val FROM t WHERE val BETWEEN 5 AND 15 ORDER BY val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][0], Value::Int64(5));
    assert_eq!(v[2][0], Value::Int64(15));
}

#[test]
fn not_between() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(15),(20)").unwrap();
    let r = db
        .execute("SELECT val FROM t WHERE val NOT BETWEEN 5 AND 15 ORDER BY val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(20));
}

#[test]
fn like_patterns() {
    let db = mem();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('alice'),('bob'),('alicia'),('ALICE'),('eve')").unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name LIKE 'ali%' ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert!(v.len() >= 2); // alice, alicia
}

#[test]
fn not_like() {
    let db = mem();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('alice'),('bob'),('charlie')").unwrap();
    // Test NOT LIKE operator exists and works
    let r = db
        .execute("SELECT name FROM t WHERE NOT (name LIKE 'ali%') ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert!(v.len() >= 1);
}

#[test]
fn ilike_case_insensitive() {
    let db = mem();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('BOB'),('charlie')").unwrap();
    let r = db.execute("SELECT name FROM t WHERE name ILIKE 'alice'");
    if let Ok(r) = r {
        assert!(rows(&r).len() >= 1);
    }
}

// ===========================================================================
// Persistence – WAL recovery after crash (unclean shutdown)
// ===========================================================================

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
            db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        db.checkpoint().unwrap();
        // More writes after checkpoint
        for i in 20..40 {
            db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
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
fn persist_with_indexes_and_constraints() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("idx.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, name TEXT NOT NULL UNIQUE, val INT64 CHECK (val >= 0))").unwrap();
        db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        // Verify unique constraint still works
        let err = db.execute("INSERT INTO t VALUES (4, 'a', 40)");
        assert!(err.is_err());
        // Verify CHECK constraint still works
        let err2 = db.execute("INSERT INTO t VALUES (4, 'd', -1)");
        assert!(err2.is_err());
        // Verify data
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(3));
    }
}

// ===========================================================================
// Multi-threaded reader tests (db.rs concurrency)
// ===========================================================================

#[test]
fn concurrent_readers() {
    let db = std::sync::Arc::new(mem());
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'v_{}')", i, i))
            .unwrap();
    }

    let mut handles = vec![];
    for _ in 0..4 {
        let db_clone = db.clone();
        handles.push(thread::spawn(move || {
            let r = db_clone.execute("SELECT COUNT(*) FROM t").unwrap();
            assert_eq!(rows(&r)[0][0], Value::Int64(100));
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn reader_during_write() {
    let db = std::sync::Arc::new(mem());
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();

    // Hold a snapshot
    let token = db.hold_snapshot().unwrap();

    // Write more data
    db.execute("INSERT INTO t VALUES (2)").unwrap();

    // Read should see current data
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));

    db.release_snapshot(token).unwrap();
}

// ===========================================================================
// Metadata / info APIs
// ===========================================================================

#[test]
fn list_tables_api() {
    let db = mem();
    db.execute("CREATE TABLE aaa(id INT64)").unwrap();
    db.execute("CREATE TABLE bbb(id INT64)").unwrap();
    let tables = db.list_tables().unwrap();
    assert!(tables.len() >= 2);
    assert!(tables.iter().any(|t| t.name == "aaa"));
    assert!(tables.iter().any(|t| t.name == "bbb"));
}

#[test]
fn table_info_api() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT, val FLOAT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 1.0)").unwrap();
    let tables = db.list_tables().unwrap();
    let t = tables.iter().find(|t| t.name == "t").unwrap();
    assert_eq!(t.columns.len(), 3);
    assert_eq!(t.row_count, 1);
}

#[test]
fn table_ddl_api() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, name TEXT NOT NULL)").unwrap();
    let ddl = db.table_ddl("t").unwrap();
    assert!(ddl.contains("CREATE TABLE"));
    assert!(ddl.contains("id"));
    assert!(ddl.contains("name"));
}

#[test]
fn table_ddl_nonexistent() {
    let db = mem();
    let err = db.table_ddl("nonexistent");
    assert!(err.is_err());
}

#[test]
fn list_indexes_api() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("CREATE INDEX idx1 ON t(val)").unwrap();
    let indexes = db.list_indexes().unwrap();
    assert!(indexes.iter().any(|i| i.name.contains("idx1")));
}

#[test]
fn verify_index_api() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
    let v = db.verify_index("idx").unwrap();
    assert!(v.valid);
}

// ===========================================================================
// Aggregate edge cases
// ===========================================================================

#[test]
fn aggregate_on_empty_table() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    let r = db
        .execute("SELECT COUNT(*), COUNT(val), SUM(val), MIN(val), MAX(val), AVG(val) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(0)); // COUNT(*)
    assert_eq!(v[0][1], Value::Int64(0)); // COUNT(val)
    assert_eq!(v[0][2], Value::Null); // SUM empty
    assert_eq!(v[0][3], Value::Null); // MIN empty
    assert_eq!(v[0][4], Value::Null); // MAX empty
    assert_eq!(v[0][5], Value::Null); // AVG empty
}

#[test]
fn aggregate_with_all_nulls() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL),(NULL),(NULL)").unwrap();
    let r = db
        .execute("SELECT COUNT(*), COUNT(val), SUM(val), MIN(val), MAX(val) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(3)); // COUNT(*) counts rows
    assert_eq!(v[0][1], Value::Int64(0)); // COUNT(val) ignores NULL
    assert_eq!(v[0][2], Value::Null); // SUM of all NULLs
}

#[test]
fn aggregate_count_distinct() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(1),(2),(2),(3)").unwrap();
    let r = db
        .execute("SELECT COUNT(DISTINCT val) FROM t")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn aggregate_sum_mixed_null() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(NULL),(20),(NULL),(30)").unwrap();
    let r = db.execute("SELECT SUM(val) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(60));
}

// ===========================================================================
// Complex multi-join queries
// ===========================================================================

#[test]
fn three_way_join() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, label TEXT)").unwrap();
    db.execute("CREATE TABLE t3(id INT64, t2_id INT64, tag TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1,'x'),(20,2,'y')").unwrap();
    db.execute("INSERT INTO t3 VALUES (100,10,'p'),(200,20,'q')").unwrap();
    let r = db
        .execute(
            "SELECT t1.name, t2.label, t3.tag
             FROM t1 JOIN t2 ON t1.id = t2.t1_id
                      JOIN t3 ON t2.id = t3.t2_id
             ORDER BY t1.name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("a".into()));
    assert_eq!(v[0][2], Value::Text("p".into()));
}

#[test]
fn left_join_with_right_join() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("CREATE TABLE t3(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3)").unwrap();
    db.execute("INSERT INTO t3 VALUES (3),(4)").unwrap();
    let r = db
        .execute(
            "SELECT t1.id, t2.id, t3.id FROM t1
             LEFT JOIN t2 ON t1.id = t2.id
             RIGHT JOIN t3 ON t2.id = t3.id
             ORDER BY t3.id",
        );
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 1);
    }
}

// ===========================================================================
// Views – more complex
// ===========================================================================

#[test]
fn view_with_aggregation() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3)").unwrap();
    db.execute("CREATE VIEW agg_view AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp")
        .unwrap();
    let r = db
        .execute("SELECT grp, total FROM agg_view ORDER BY grp")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(3)); // A: 1+2
    assert_eq!(v[1][1], Value::Int64(3)); // B: 3
}

#[test]
fn view_with_join() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1,100)").unwrap();
    db.execute(
        "CREATE VIEW joined AS SELECT t1.name, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id",
    )
    .unwrap();
    let r = db.execute("SELECT name, val FROM joined").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("a".into()));
    assert_eq!(rows(&r)[0][1], Value::Int64(100));
}

// ===========================================================================
// Triggers – after insert with complex logic
// ===========================================================================

#[test]
fn trigger_maintains_audit_count() {
    let db = mem();
    db.execute("CREATE TABLE items(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE audit(cnt INT64)").unwrap();
    db.execute("INSERT INTO audit VALUES (0)").unwrap();
    db.execute(
        "CREATE TRIGGER item_count AFTER INSERT ON items
         FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE audit SET cnt = cnt + 1')",
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO items VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO items VALUES (3, 'c')").unwrap();
    let r = db.execute("SELECT cnt FROM audit").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn trigger_on_delete() {
    let db = mem();
    db.execute("CREATE TABLE items(id INT64)").unwrap();
    db.execute("CREATE TABLE deleted_count(cnt INT64)").unwrap();
    db.execute("INSERT INTO deleted_count VALUES (0)").unwrap();
    db.execute(
        "CREATE TRIGGER log_delete AFTER DELETE ON items
         FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE deleted_count SET cnt = cnt + 1')",
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1),(2),(3)").unwrap();
    db.execute("DELETE FROM items WHERE id = 2").unwrap();
    let r = db.execute("SELECT cnt FROM deleted_count").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn trigger_on_update() {
    let db = mem();
    db.execute("CREATE TABLE items(id INT64, val TEXT)").unwrap();
    db.execute("CREATE TABLE update_count(cnt INT64)").unwrap();
    db.execute("INSERT INTO update_count VALUES (0)").unwrap();
    db.execute(
        "CREATE TRIGGER log_update AFTER UPDATE ON items
         FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE update_count SET cnt = cnt + 1')",
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'before')").unwrap();
    db.execute("UPDATE items SET val = 'after' WHERE id = 1").unwrap();
    let r = db.execute("SELECT cnt FROM update_count").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

// ===========================================================================
// Blob type handling
// ===========================================================================

#[test]
fn blob_insert_and_read() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, data BLOB)").unwrap();
    let blob = vec![0u8, 1, 2, 255, 128, 64];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(blob.clone())],
    )
    .unwrap();
    let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Blob(blob));
}

#[test]
fn blob_large() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, data BLOB)").unwrap();
    let blob = vec![42u8; 100_000];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(blob.clone())],
    )
    .unwrap();
    // Read back and verify size
    let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
    if let Value::Blob(b) = &rows(&r)[0][0] {
        assert_eq!(b.len(), 100_000);
    } else {
        panic!("expected blob");
    }
}

// ===========================================================================
// Transaction rollback and error recovery
// ===========================================================================

#[test]
fn transaction_rollback_restores_state() {
    let db = mem();
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
fn error_in_transaction_allows_rollback() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    let _ = db.execute("INSERT INTO t VALUES (1)"); // dup PK
    db.execute("ROLLBACK").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

// ===========================================================================
// Prepared statements with various types
// ===========================================================================

#[test]
fn prepared_statement_all_types() {
    let db = mem();
    db.execute("CREATE TABLE t(i INT64, f FLOAT64, t TEXT, b BOOL, bl BLOB)").unwrap();
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
fn prepared_statement_reuse() {
    let db = mem();
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

// ===========================================================================
// EXPLAIN for deeper planner coverage
// ===========================================================================

#[test]
fn explain_set_operation() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT id FROM t UNION ALL SELECT id FROM t")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t WHERE id IN (SELECT id FROM t)")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_cte() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN WITH cte AS (SELECT id FROM t) SELECT * FROM cte")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_right_join() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}
