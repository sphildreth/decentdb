//! Unit tests for DECIMAL comparison, ordering, and precision edge cases.
//!
//! Exercises: record/value.rs normalize_decimal, compare_decimal,
//! parse_decimal_text, and the sortable_decimal_bytes path in
//! record/key.rs.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

#[test]
fn decimal_ordering_negative_to_positive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, -100.50)");
    exec(&db, "INSERT INTO t VALUES (2, -1.00)");
    exec(&db, "INSERT INTO t VALUES (3, 0.00)");
    exec(&db, "INSERT INTO t VALUES (4, 0.01)");
    exec(&db, "INSERT INTO t VALUES (5, 99.99)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    let ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(
        ids,
        vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3),
            Value::Int64(4),
            Value::Int64(5)
        ]
    );
}

#[test]
fn decimal_order_by_many_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    for i in 0..50 {
        let v = (i as f64) * 0.50 - 12.25;
        exec(&db, &format!("INSERT INTO t VALUES ({i}, {v:.2})"));
    }
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    let ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(ids.len(), 50);
    assert_eq!(ids[0], Value::Int64(0));
    assert_eq!(ids[49], Value::Int64(49));
}

#[test]
fn decimal_large_scale_values_ordering() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(15, 4))");
    exec(&db, "INSERT INTO t VALUES (1, 0.0001)");
    exec(&db, "INSERT INTO t VALUES (2, 0.0010)");
    exec(&db, "INSERT INTO t VALUES (3, 0.0100)");
    exec(&db, "INSERT INTO t VALUES (4, 0.1000)");
    exec(&db, "INSERT INTO t VALUES (5, 1.0000)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    let ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(
        ids,
        vec![
            Value::Int64(1),
            Value::Int64(2),
            Value::Int64(3),
            Value::Int64(4),
            Value::Int64(5)
        ]
    );
}

#[test]
fn decimal_cast_from_text() {
    let db = mem_db();
    let r = exec(&db, "SELECT CAST('19.990' AS DECIMAL(10, 2))");
    match &r.rows()[0].values()[0] {
        Value::Decimal { scaled, scale } => {
            assert_eq!(*scaled, 1999);
            assert_eq!(*scale, 2);
        }
        other => panic!("expected Decimal, got {other:?}"),
    }
}

#[test]
fn decimal_ordering_descending() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, 1.50)");
    exec(&db, "INSERT INTO t VALUES (2, 3.00)");
    exec(&db, "INSERT INTO t VALUES (3, 2.25)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val DESC");
    let ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(ids, vec![Value::Int64(2), Value::Int64(3), Value::Int64(1)]);
}

#[test]
fn decimal_insert_select_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, 19.99)");
    exec(&db, "INSERT INTO t VALUES (2, -0.50)");
    exec(&db, "INSERT INTO t VALUES (3, 0.00)");
    exec(&db, "INSERT INTO t VALUES (4, 99999999.99)");
    let r = exec(&db, "SELECT val FROM t ORDER BY id");
    match &r.rows()[0].values()[0] {
        Value::Decimal { scaled, scale } => {
            assert_eq!(*scaled, 1999);
            assert_eq!(*scale, 2);
        }
        other => panic!("expected Decimal, got {other:?}"),
    }
    match &r.rows()[3].values()[0] {
        Value::Decimal { scaled, scale } => {
            assert_eq!(*scaled, 9999999999);
            assert_eq!(*scale, 2);
        }
        other => panic!("expected Decimal, got {other:?}"),
    }
}

#[test]
fn decimal_normalized_on_storage() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, -0.50)");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    match &r.rows()[0].values()[0] {
        Value::Decimal { scaled, scale } => {
            assert_eq!(*scaled, -5);
            assert_eq!(*scale, 1);
        }
        other => panic!("expected Decimal, got {other:?}"),
    }
}

#[test]
fn decimal_zero_normalized_to_zero_scale() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, 0.00)");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    match &r.rows()[0].values()[0] {
        Value::Decimal { scaled, scale } => {
            assert_eq!(*scaled, 0);
            assert_eq!(*scale, 0);
        }
        other => panic!("expected Decimal, got {other:?}"),
    }
}

/// Large negative decimals (-99999999.99) sort before small negative
/// decimals (-0.01) via sortable_decimal_bytes. Validates that exponent
/// inversion with digit complement produces correct lexicographic order.
#[test]
fn decimal_negative_precision_ordering() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, -0.01)");
    exec(&db, "INSERT INTO t VALUES (2, -99999999.99)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    let ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(ids, vec![Value::Int64(2), Value::Int64(1)]);
}

#[test]
fn decimal_where_with_float_literal() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, 1.50)");
    exec(&db, "INSERT INTO t VALUES (2, 1.50)");
    exec(&db, "INSERT INTO t VALUES (3, 1.50)");
    let r = exec(&db, "SELECT COUNT(*) FROM t WHERE val = 1.50");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

#[test]
fn decimal_where_with_float_literal_on_left_hand_side() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, 1.50)");
    exec(&db, "INSERT INTO t VALUES (2, 2.25)");
    exec(&db, "INSERT INTO t VALUES (3, 3.75)");

    let r = exec(&db, "SELECT COUNT(*) FROM t WHERE 2.25 <= val");

    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}
