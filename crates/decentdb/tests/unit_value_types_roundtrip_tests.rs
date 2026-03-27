//! Unit tests for all Value type roundtrips through SQL INSERT/SELECT.
//!
//! Exercises: record/mod.rs varint/zigzag encoding, record/value.rs Value
//! variants, record/row.rs row serialization, record/key.rs index key encoding,
//! storage/page.rs InMemoryPageStore, and overflow pages.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

#[test]
fn null_values_insert_select_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, a INT64, b TEXT, c BOOL)");
    exec(&db, "INSERT INTO t VALUES (1, NULL, NULL, NULL)");
    let r = exec(&db, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
    assert_eq!(r.rows()[0].values()[1], Value::Null);
    assert_eq!(r.rows()[0].values()[2], Value::Null);
}

#[test]
fn int64_boundary_values_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, -9223372036854775808)");
    exec(&db, "INSERT INTO t VALUES (2, 9223372036854775807)");
    exec(&db, "INSERT INTO t VALUES (3, 0)");
    exec(&db, "INSERT INTO t VALUES (4, -1)");
    exec(&db, "INSERT INTO t VALUES (5, 1)");
    let r = exec(&db, "SELECT val FROM t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(i64::MIN));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(i64::MAX));
    assert_eq!(r.rows()[2].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[3].values()[0], Value::Int64(-1));
    assert_eq!(r.rows()[4].values()[0], Value::Int64(1));
}

#[test]
fn int64_ordering_is_correct() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(val INT64)");
    exec(&db, "INSERT INTO t VALUES (0)");
    exec(&db, "INSERT INTO t VALUES (-1)");
    exec(&db, "INSERT INTO t VALUES (1)");
    exec(&db, "INSERT INTO t VALUES (-9223372036854775808)");
    exec(&db, "INSERT INTO t VALUES (9223372036854775807)");
    let r = exec(&db, "SELECT val FROM t ORDER BY val");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(i64::MIN));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(-1));
    assert_eq!(r.rows()[2].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[3].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[4].values()[0], Value::Int64(i64::MAX));
}

#[test]
fn float64_special_values_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val FLOAT64)");
    exec(&db, "INSERT INTO t VALUES (1, 0.0)");
    exec(&db, "INSERT INTO t VALUES (2, -0.0)");
    exec(&db, "INSERT INTO t VALUES (3, 1.0e308)");
    exec(&db, "INSERT INTO t VALUES (4, -1.0e308)");
    exec(&db, "INSERT INTO t VALUES (5, 1.0e-308)");
    let r = exec(&db, "SELECT val FROM t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Float64(0.0));
    assert_eq!(r.rows()[2].values()[0], Value::Float64(1.0e308));
    assert_eq!(r.rows()[3].values()[0], Value::Float64(-1.0e308));
    assert_eq!(r.rows()[4].values()[0], Value::Float64(1.0e-308));
}

#[test]
fn bool_values_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val BOOL)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE)");
    exec(&db, "INSERT INTO t VALUES (2, FALSE)");
    let r = exec(&db, "SELECT val FROM t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Bool(true));
    assert_eq!(r.rows()[1].values()[0], Value::Bool(false));
}

#[test]
fn text_unicode_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val TEXT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 'Grüße, 世界! 🦀 café résumé naïve')",
    );
    exec(&db, "INSERT INTO t VALUES (2, '')");
    let r = exec(&db, "SELECT val FROM t ORDER BY id");
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Text("Grüße, 世界! 🦀 café résumé naïve".to_string())
    );
    assert_eq!(r.rows()[1].values()[0], Value::Text(String::new()));
}

#[test]
fn blob_roundtrip_via_parameterized_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data BLOB)");
    let blob = vec![0x00, 0x01, 0x7F, 0x80, 0xFF, 0xDE, 0xAD];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(blob.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Blob(blob));
}

#[test]
fn null_in_mixed_columns() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE t(id INT64, a INT64, b TEXT, c FLOAT64, d BOOL)",
    );
    exec(&db, "INSERT INTO t VALUES (1, 42, NULL, 3.14, NULL)");
    exec(&db, "INSERT INTO t VALUES (2, NULL, 'hello', NULL, TRUE)");
    let r = exec(&db, "SELECT a, b, c, d FROM t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
    assert_eq!(r.rows()[0].values()[1], Value::Null);
    assert_eq!(r.rows()[0].values()[3], Value::Null);
    assert_eq!(r.rows()[1].values()[0], Value::Null);
    assert_eq!(r.rows()[1].values()[1], Value::Text("hello".to_string()));
    assert_eq!(r.rows()[1].values()[3], Value::Bool(true));
}

#[test]
fn multi_column_all_types_insert_and_select() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE t(id INT64, i INT64, f FLOAT64, b BOOL, t TEXT, bl BLOB)",
    );
    let blob = vec![0xAB, 0xCD];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6)",
        &[
            Value::Int64(1),
            Value::Int64(-99),
            Value::Float64(2.5),
            Value::Bool(true),
            Value::Text("test".to_string()),
            Value::Blob(blob.clone()),
        ],
    )
    .unwrap();
    let r = exec(&db, "SELECT i, f, b, t, bl FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(-99));
    assert_eq!(r.rows()[0].values()[1], Value::Float64(2.5));
    assert_eq!(r.rows()[0].values()[2], Value::Bool(true));
    assert_eq!(r.rows()[0].values()[3], Value::Text("test".to_string()));
    assert_eq!(r.rows()[0].values()[4], Value::Blob(blob));
}

#[test]
fn many_rows_stress_encoding() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val TEXT)");
    for batch_start in (0..500).step_by(50) {
        let values: Vec<String> = (0..50)
            .map(|i| format!("({}, 'row_{}')", batch_start + i, batch_start + i))
            .collect();
        exec(&db, &format!("INSERT INTO t VALUES {}", values.join(", ")));
    }
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(500));

    let r = exec(&db, "SELECT val FROM t WHERE id = 0");
    assert_eq!(r.rows()[0].values()[0], Value::Text("row_0".to_string()));

    let r = exec(&db, "SELECT val FROM t WHERE id = 499");
    assert_eq!(r.rows()[0].values()[0], Value::Text("row_499".to_string()));
}

#[test]
fn where_clause_equality_with_various_types() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");

    let r = exec(&db, "SELECT id FROM t WHERE val = 20");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));

    let r = exec(&db, "SELECT id FROM t WHERE val = 99");
    assert_eq!(r.rows().len(), 0);
}

#[test]
fn update_modifies_values_in_place() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'alpha')");
    exec(&db, "UPDATE t SET val = 'beta' WHERE id = 1");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("beta".to_string()));
}

#[test]
fn delete_and_reinsert_preserves_encoding() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 100)");
    exec(&db, "DELETE FROM t WHERE id = 1");
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));

    exec(&db, "INSERT INTO t VALUES (1, 200)");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(200));
}

#[test]
fn null_comparison_semantics() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, 10)");

    let r = exec(&db, "SELECT id FROM t WHERE val = NULL");
    assert_eq!(r.rows().len(), 0);

    let r = exec(&db, "SELECT id FROM t WHERE val IS NULL");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));

    let r = exec(&db, "SELECT id FROM t WHERE val IS NOT NULL");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn order_by_with_nulls() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 30)");
    exec(&db, "INSERT INTO t VALUES (2, NULL)");
    exec(&db, "INSERT INTO t VALUES (3, 10)");
    exec(&db, "INSERT INTO t VALUES (4, NULL)");
    exec(&db, "INSERT INTO t VALUES (5, 20)");
    let r = exec(&db, "SELECT val FROM t ORDER BY val");
    let vals: Vec<&Value> = r.rows().iter().map(|r| &r.values()[0]).collect();
    let non_null: Vec<i64> = vals
        .iter()
        .filter_map(|v| match v {
            Value::Int64(n) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(non_null, vec![10, 20, 30]);
}

#[test]
fn decimal_values_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val DECIMAL(10, 2))");
    exec(&db, "INSERT INTO t VALUES (1, 19.99)");
    exec(&db, "INSERT INTO t VALUES (2, -0.50)");
    exec(&db, "INSERT INTO t VALUES (3, 0.00)");
    exec(&db, "INSERT INTO t VALUES (4, 99999999.99)");
    let r = exec(&db, "SELECT val FROM t ORDER BY id");
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Decimal {
            scaled: 1999,
            scale: 2
        }
    );
    assert_eq!(
        r.rows()[1].values()[0],
        Value::Decimal {
            scaled: -5,
            scale: 1
        }
    );
    assert_eq!(
        r.rows()[2].values()[0],
        Value::Decimal {
            scaled: 0,
            scale: 0
        }
    );
    assert_eq!(
        r.rows()[3].values()[0],
        Value::Decimal {
            scaled: 9999999999,
            scale: 2
        }
    );
}
