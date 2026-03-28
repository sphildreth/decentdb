//! Unit tests for index key sorting, uniqueness enforcement, and ORDER BY
//! with various types.
//!
//! Exercises: record/key.rs encode_index_key / compare_index_values,
//! record/value.rs compare_decimal, btree/page.rs leaf/internal page
//! encoding/decoding.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn exec_err(db: &Db, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

#[test]
fn order_by_int64_descending() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64)");
    for i in [-100, 0, 100, i64::MIN, i64::MAX] {
        exec(&db, &format!("INSERT INTO t VALUES ({i})"));
    }
    let r = exec(&db, "SELECT id FROM t ORDER BY id DESC");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(i64::MAX));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(100));
    assert_eq!(r.rows()[4].values()[0], Value::Int64(i64::MIN));
}

#[test]
fn order_by_text_ascending() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'zebra')");
    exec(&db, "INSERT INTO t VALUES (2, 'Alpha')");
    exec(&db, "INSERT INTO t VALUES (3, 'beta')");
    exec(&db, "INSERT INTO t VALUES (4, 'ZEBRA')");
    let r = exec(&db, "SELECT name FROM t ORDER BY name");
    let names: Vec<String> = r
        .rows()
        .iter()
        .map(|r| match &r.values()[0] {
            Value::Text(s) => s.clone(),
            other => panic!("expected text, got {other:?}"),
        })
        .collect();
    assert_eq!(names.len(), 4);
    for i in 0..names.len() - 1 {
        assert!(
            names[i].as_bytes() <= names[i + 1].as_bytes(),
            "ORDER BY TEXT violated: {} > {}",
            names[i],
            names[i + 1]
        );
    }
}

#[test]
fn order_by_bool_sorts_false_before_true() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, flag BOOL)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE)");
    exec(&db, "INSERT INTO t VALUES (2, FALSE)");
    exec(&db, "INSERT INTO t VALUES (3, TRUE)");
    exec(&db, "INSERT INTO t VALUES (4, FALSE)");
    let r = exec(&db, "SELECT flag FROM t ORDER BY flag");
    assert_eq!(r.rows()[0].values()[0], Value::Bool(false));
    assert_eq!(r.rows()[3].values()[0], Value::Bool(true));
}

#[test]
fn unique_index_prevents_duplicate_insert() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, name TEXT)");
    exec(&db, "CREATE UNIQUE INDEX idx_name ON t(name)");
    exec(&db, "INSERT INTO t VALUES (1, 'alice')");
    let err = exec_err(&db, "INSERT INTO t VALUES (2, 'alice')");
    assert!(err.to_lowercase().contains("unique") || err.to_lowercase().contains("constraint"));
}

#[test]
fn unique_index_allows_different_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, name TEXT)");
    exec(&db, "CREATE UNIQUE INDEX idx_name ON t(name)");
    exec(&db, "INSERT INTO t VALUES (1, 'alice')");
    exec(&db, "INSERT INTO t VALUES (2, 'bob')");
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn multi_column_ordering() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, a INT64, b INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 2)");
    exec(&db, "INSERT INTO t VALUES (2, 1, 1)");
    exec(&db, "INSERT INTO t VALUES (3, 2, 1)");
    exec(&db, "INSERT INTO t VALUES (4, 2, 2)");
    let r = exec(&db, "SELECT id FROM t ORDER BY a, b");
    let ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(
        ids,
        vec![
            Value::Int64(2),
            Value::Int64(1),
            Value::Int64(3),
            Value::Int64(4)
        ]
    );
}

#[test]
fn order_by_nulls_sort_first_in_ascending() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 5)");
    exec(&db, "INSERT INTO t VALUES (4, NULL)");
    let r = exec(&db, "SELECT val FROM t ORDER BY val");
    assert_eq!(r.rows().len(), 4);
    assert!(matches!(r.rows()[0].values()[0], Value::Null));
    assert!(matches!(r.rows()[1].values()[0], Value::Null));
    assert_eq!(r.rows()[2].values()[0], Value::Int64(5));
    assert_eq!(r.rows()[3].values()[0], Value::Int64(10));
}

#[test]
fn order_by_float64_boundary_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val FLOAT64)");
    exec(&db, "INSERT INTO t VALUES (1, 0.0)");
    exec(&db, "INSERT INTO t VALUES (2, -1.0)");
    exec(&db, "INSERT INTO t VALUES (3, 1.0)");
    exec(&db, "INSERT INTO t VALUES (4, 1.0e308)");
    exec(&db, "INSERT INTO t VALUES (5, -1.0e308)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
    assert_eq!(r.rows()[4].values()[0], Value::Int64(4));
}

#[test]
fn index_used_for_where_equality() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, val INT64)");
    exec(&db, "CREATE INDEX idx_val ON t(val)");
    for i in 0..100 {
        exec(&db, &format!("INSERT INTO t VALUES ({i}, {i})"));
    }
    let r = exec(&db, "SELECT id FROM t WHERE val = 50");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(50));
}

#[test]
fn index_used_for_range_scan() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, val INT64)");
    exec(&db, "CREATE INDEX idx_val ON t(val)");
    for i in 0..100 {
        exec(&db, &format!("INSERT INTO t VALUES ({i}, {i})"));
    }
    let r = exec(&db, "SELECT COUNT(*) FROM t WHERE val >= 90");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(10));
}

#[test]
fn order_by_decimal_precision() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 4))");
    exec(&db, "INSERT INTO t VALUES (1, 1.0001)");
    exec(&db, "INSERT INTO t VALUES (2, 1.0010)");
    exec(&db, "INSERT INTO t VALUES (3, 1.0100)");
    exec(&db, "INSERT INTO t VALUES (4, 1.1000)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[3].values()[0], Value::Int64(4));
}

#[test]
fn primary_key_enforces_uniqueness() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a')");
    let err = exec_err(&db, "INSERT INTO t VALUES (1, 'b')");
    assert!(err.to_lowercase().contains("unique") || err.to_lowercase().contains("constraint"));
}

#[test]
fn delete_from_unique_index_frees_slot() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, name TEXT)");
    exec(&db, "CREATE UNIQUE INDEX idx_name ON t(name)");
    exec(&db, "INSERT INTO t VALUES (1, 'alice')");
    exec(&db, "DELETE FROM t WHERE name = 'alice'");
    exec(&db, "INSERT INTO t VALUES (2, 'alice')");
    let r = exec(&db, "SELECT id FROM t WHERE name = 'alice'");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}
