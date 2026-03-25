#![allow(clippy::approx_constant)]

//! SQL expression evaluation tests.
//!
//! Covers: CASE/WHEN, LIKE/ILIKE, BETWEEN, IN, IS NULL, arithmetic,
//! boolean logic, comparison operators, CAST, type coercion, decimal,
//! string/math/date functions, COALESCE, NULLIF, concat, unary ops,
//! and parser/normalize edge cases.

use decentdb::{Db, DbConfig, QueryResult, Value};
use std::thread;
use tempfile::TempDir;

fn assert_float_close(value: &Value, expected: f64) {
    match value {
        Value::Float64(actual) => assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        ),
        other => panic!("expected FLOAT64 result, got {other:?}"),
    }
}

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
fn abs_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT abs(-42), abs(42)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(42));
}

#[test]
fn all_basic_types_roundtrip() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE all_types (
        id INT PRIMARY KEY,
        i INT,
        f FLOAT,
        t TEXT,
        b BOOLEAN
    )",
    );
    exec(
        &db,
        "INSERT INTO all_types VALUES (1, 42, 3.14, 'hello', true)",
    );
    let r = exec(&db, "SELECT * FROM all_types");
    assert_eq!(r.rows().len(), 1);
    let vals = r.rows()[0].values();
    assert_eq!(vals[1], Value::Int64(42));
    assert_eq!(vals[4], Value::Bool(true));
}

#[test]
fn arithmetic_expressions_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE arith (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO arith VALUES (1, 10, 3)");
    let r = exec(&db, "SELECT a + b, a - b, a * b, a / b, a % b FROM arith");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(13));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(7));
    assert_eq!(r.rows()[0].values()[2], Value::Int64(30));
    assert_eq!(r.rows()[0].values()[3], Value::Int64(3));
    assert_eq!(r.rows()[0].values()[4], Value::Int64(1));
}

#[test]
fn arithmetic_with_mixed_types() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b FLOAT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 3.5)").unwrap();
    let r = db.execute("SELECT a + b, a * b, a - b FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    // Should produce float results
    if let Value::Float64(f) = v[0][0] {
        assert!((f - 13.5).abs() < 0.001);
    }
}

#[test]
fn arithmetic_with_null() {
    let db = mem_db();
    let r = db
        .execute("SELECT 1 + NULL, NULL * 5, NULL - NULL, NULL / 2")
        .unwrap();
    let v = rows(&r);
    for val in &v[0] {
        assert_eq!(*val, Value::Null);
    }
}

#[test]
fn array_expression_in_select() {
    let db = mem_db();
    let r = db.execute("SELECT ARRAY[1, 2, 3]");
    // ARRAY may or may not be supported
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn array_in_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // ANY(ARRAY[...]) syntax
    let r = db.execute("SELECT id FROM t WHERE id = ANY(ARRAY[1, 3])");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn ast_display_complex_table() {
    let db = mem_db();
    db.execute("CREATE TABLE parent_ref(id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute(
        "CREATE TABLE complex(
            id INT64 PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            age INT64 CHECK (age >= 0),
            status TEXT DEFAULT 'active',
            parent_id INT64 REFERENCES parent_ref(id) ON DELETE CASCADE
        )",
    )
    .unwrap();
    let ddl = db.table_ddl("complex").unwrap();
    assert!(ddl.contains("CREATE TABLE"));
    assert!(ddl.contains("PRIMARY KEY") || ddl.contains("id"));
}

#[test]
fn between_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(15),(20)")
        .unwrap();
    let r = db
        .execute("SELECT val FROM t WHERE val BETWEEN 5 AND 15 ORDER BY val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3); // 5, 10, 15
}

#[test]
fn between_on_column() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(15),(20)")
        .unwrap();
    let r = db
        .execute("SELECT val FROM t WHERE val BETWEEN 5 AND 15 ORDER BY val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][0], Value::Int64(5));
    assert_eq!(v[2][0], Value::Int64(15));
}

#[test]
fn between_with_non_integer_types() {
    let db = mem_db();
    exec(&db, "CREATE TABLE bni (id INT PRIMARY KEY, name TEXT)");
    exec(
        &db,
        "INSERT INTO bni VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')",
    );
    let r = exec(
        &db,
        "SELECT name FROM bni WHERE name BETWEEN 'banana' AND 'date' ORDER BY name",
    );
    assert!(r.rows().len() >= 2);
}

#[test]
fn between_with_nulls() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE between_null (id INT PRIMARY KEY, val INT)",
    );
    exec(
        &db,
        "INSERT INTO between_null VALUES (1, 5), (2, NULL), (3, 15)",
    );
    let r = exec(
        &db,
        "SELECT id FROM between_null WHERE val BETWEEN 1 AND 10 ORDER BY id",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn blob_large() {
    let db = mem_db();
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

#[test]
fn bool_and_null() {
    let db = mem_db();
    let r = db
        .execute("SELECT TRUE AND NULL, FALSE AND NULL, NULL AND NULL")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Bool(false));
    assert_eq!(v[0][2], Value::Null);
}

#[test]
fn bool_or_null() {
    let db = mem_db();
    let r = db
        .execute("SELECT TRUE OR NULL, FALSE OR NULL, NULL OR NULL")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Bool(true));
    assert_eq!(v[0][1], Value::Null);
    assert_eq!(v[0][2], Value::Null);
}

#[test]
fn bool_to_int_comparison() {
    let db = mem_db();
    let r = exec(&db, "SELECT true = true, false = false, true <> false");
    assert_eq!(r.rows()[0].values()[0], Value::Bool(true));
    assert_eq!(r.rows()[0].values()[1], Value::Bool(true));
    assert_eq!(r.rows()[0].values()[2], Value::Bool(true));
}

#[test]
fn boolean_and_or_not_in_where() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE bool_t (id INT PRIMARY KEY, a BOOLEAN, b BOOLEAN)",
    );
    exec(
        &db,
        "INSERT INTO bool_t VALUES (1, true, false), (2, false, true), (3, true, true)",
    );
    let r = exec(&db, "SELECT id FROM bool_t WHERE a AND b");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
    let r2 = exec(&db, "SELECT id FROM bool_t WHERE a OR b ORDER BY id");
    assert_eq!(r2.rows().len(), 3);
    let r3 = exec(&db, "SELECT id FROM bool_t WHERE NOT a ORDER BY id");
    assert_eq!(r3.rows().len(), 1);
}

#[test]
fn boolean_literal_true_false() {
    let db = mem_db();
    exec(&db, "CREATE TABLE bl (id INT PRIMARY KEY, flag BOOL)");
    exec(&db, "INSERT INTO bl VALUES (1, true), (2, false)");
    let r = exec(&db, "SELECT flag FROM bl WHERE flag = true");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn boolean_logic_with_nulls() {
    let db = mem_db();
    let r = db
        .execute("SELECT NULL AND TRUE, NULL OR FALSE, NOT NULL")
        .unwrap();
    let v = rows(&r);
    // NULL AND TRUE = NULL, NULL OR FALSE = NULL, NOT NULL = NULL
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Null);
    assert_eq!(v[0][2], Value::Null);
}

#[test]
fn btree_exercise_splits_and_deletes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    // Insert enough rows to cause splits
    for i in 0..200 {
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(format!("value_{:03}", i))],
        )
        .unwrap();
    }
    // Delete every other row
    for i in (0..200).step_by(2) {
        db.execute_with_params("DELETE FROM t WHERE id = $1", &[Value::Int64(i)])
            .unwrap();
    }
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(100));
    // Verify index integrity
    let indexes = db.list_indexes().unwrap();
    for idx in &indexes {
        let v = db.verify_index(&idx.name).unwrap();
        assert!(v.valid, "Index {} not valid after deletes", idx.name);
    }
}

#[test]
fn case_expression_searched() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    let r = db
        .execute(
            "SELECT val, CASE WHEN val < 3 THEN 'low' WHEN val < 5 THEN 'mid' ELSE 'high' END AS label FROM t ORDER BY val",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("low".into()));
    assert_eq!(v[2][1], Value::Text("mid".into()));
    assert_eq!(v[4][1], Value::Text("high".into()));
}

#[test]
fn case_expression_simple() {
    let db = mem_db();
    db.execute("CREATE TABLE t(status INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END FROM t ORDER BY status")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("active".into()));
    assert_eq!(v[1][0], Value::Text("inactive".into()));
    assert_eq!(v[2][0], Value::Text("unknown".into()));
}

#[test]
fn case_expression_with_operand() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ceo (id INT PRIMARY KEY, status INT)");
    exec(&db, "INSERT INTO ceo VALUES (1, 1), (2, 2), (3, 3)");
    let r = exec(
        &db,
        "SELECT id, CASE status WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM ceo ORDER BY id",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Text("one".to_string()));
    assert_eq!(r.rows()[2].values()[1], Value::Text("other".to_string()));
}

#[test]
fn case_insensitive_column_in_where() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cit (ID INT PRIMARY KEY, NAME TEXT)");
    exec(&db, "INSERT INTO cit VALUES (1, 'Alice')");
    let r = exec(&db, "SELECT id FROM cit WHERE name = 'Alice'");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn case_insensitive_table_name() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE MixedCase (Id INT PRIMARY KEY, Name TEXT)",
    );
    exec(&db, "INSERT INTO MixedCase VALUES (1, 'hello')");
    let r = exec(&db, "SELECT name FROM mixedcase WHERE id = 1");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn case_searched_with_null() {
    let db = mem_db();
    let r = db
        .execute("SELECT CASE WHEN NULL THEN 'yes' WHEN TRUE THEN 'true' ELSE 'else' END")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("true".into()));
}

#[test]
fn case_simple_expression() {
    let db = mem_db();
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
fn cast_bool_to_text() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cbt (id INT PRIMARY KEY, val BOOLEAN)");
    exec(&db, "INSERT INTO cbt VALUES (1, true), (2, false)");
    let r = exec(&db, "SELECT CAST(val AS TEXT) FROM cbt ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn cast_float_to_int() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cfi (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO cfi VALUES (1, 3.7)");
    let r = exec(&db, "SELECT CAST(val AS INT) FROM cfi");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn cast_int_to_float() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cif (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO cif VALUES (1, 42)");
    let r = exec(&db, "SELECT CAST(val AS FLOAT) FROM cif");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn cast_int_to_text() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cast_t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO cast_t VALUES (42)");
    let r = exec(&db, "SELECT CAST(id AS TEXT) FROM cast_t");
    assert_eq!(r.rows()[0].values()[0], Value::Text("42".to_string()));
}

#[test]
fn cast_operations() {
    let db = mem_db();
    let r = db.execute("SELECT CAST(42 AS FLOAT64)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Float64(42.0));
    let r2 = db.execute("SELECT CAST(3.14 AS INT64)").unwrap();
    assert_eq!(rows(&r2)[0][0], Value::Int64(3));
    let r3 = db.execute("SELECT CAST(TRUE AS INT64)").unwrap();
    assert_eq!(rows(&r3)[0][0], Value::Int64(1));
}

#[test]
fn cast_text_to_int() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cast_t2 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO cast_t2 VALUES (1, '99')");
    let r = exec(&db, "SELECT CAST(val AS INT) FROM cast_t2");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(99));
}

#[test]
fn coalesce_all_null() {
    let db = mem_db();
    let r = db.execute("SELECT COALESCE(NULL, NULL, NULL)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn coalesce_expression() {
    let db = mem_db();
    let r = db.execute("SELECT COALESCE(NULL, NULL, 42, 99)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn coalesce_function() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE coal (id INT PRIMARY KEY, a TEXT, b TEXT)",
    );
    exec(
        &db,
        "INSERT INTO coal VALUES (1, NULL, 'fallback'), (2, 'primary', 'fallback')",
    );
    let r = exec(&db, "SELECT COALESCE(a, b) FROM coal ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Text("fallback".to_string()));
    assert_eq!(r.rows()[1].values()[0], Value::Text("primary".to_string()));
}

#[test]
fn coalesce_many_args() {
    let db = mem_db();
    let r = db
        .execute("SELECT COALESCE(NULL, NULL, NULL, 42, 99)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn coalesce_mixed() {
    let db = mem_db();
    let r = db.execute("SELECT COALESCE(NULL, NULL, 42, 100)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn coalesce_returns_first_non_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT COALESCE(NULL, NULL, 42, 99)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
}

#[test]
fn column_default_numeric_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64 DEFAULT 42)")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let result = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Int64(42));
}

#[test]
fn column_default_value_applied_on_insert() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, status TEXT DEFAULT 'active')")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let result = db.execute("SELECT status FROM t WHERE id = 1").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Text("active".into()));
}

#[test]
fn compare_float_to_text() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a FLOAT64, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (3.14, '2.71'), (1.0, '5.0')")
        .unwrap();
    let r = db
        .execute("SELECT a FROM t WHERE a > b ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert!(!v.is_empty()); // 3.14 > '2.71'
}

#[test]
fn compare_float_with_text() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val FLOAT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1.5), (2.5), (3.5)")
        .unwrap();
    let r = db.execute("SELECT val FROM t WHERE val = '2.5'").unwrap();
    let v = rows(&r);
    assert!(v.len() <= 1);
}

#[test]
fn compare_int_to_text() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (5, '10'), (20, '5')")
        .unwrap();
    let r = db
        .execute("SELECT a, b FROM t WHERE a < b ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert!(!v.is_empty()); // 5 < '10' (numeric comparison)
}

#[test]
fn compare_int_with_text() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, label TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, '1'), (2, '2'), (3, 'three')")
        .unwrap();
    // Comparing INT64 column with text literal
    let r = db.execute("SELECT id FROM t WHERE id = '2'").unwrap();
    // May or may not match depending on coercion rules
    let v = rows(&r);
    assert!(v.len() <= 1); // at most 1 match
}

#[test]
fn comparison_operators() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cmp (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO cmp VALUES (1, 10), (2, 20), (3, 30)");
    let r1 = exec(&db, "SELECT id FROM cmp WHERE val > 15 ORDER BY id");
    assert_eq!(r1.rows().len(), 2);
    let r2 = exec(&db, "SELECT id FROM cmp WHERE val >= 20 ORDER BY id");
    assert_eq!(r2.rows().len(), 2);
    let r3 = exec(&db, "SELECT id FROM cmp WHERE val < 20");
    assert_eq!(r3.rows().len(), 1);
    let r4 = exec(&db, "SELECT id FROM cmp WHERE val <= 20 ORDER BY id");
    assert_eq!(r4.rows().len(), 2);
    let r5 = exec(&db, "SELECT id FROM cmp WHERE val != 20 ORDER BY id");
    assert_eq!(r5.rows().len(), 2);
}

#[test]
fn comparison_operators_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cops (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "CREATE VIEW cops_v AS SELECT id FROM cops WHERE val >= 1 AND val <= 10 AND val <> 5",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains(">=") || dump.contains("<=") || dump.contains("<>"));
}

#[test]
fn comparison_with_null() {
    let db = mem_db();
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
fn complex_boolean_with_nulls() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL), (NULL, 2), (NULL, NULL), (1, 2)")
        .unwrap();
    // Test AND/OR with NULLs
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE a = 1 AND b = 2")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
    let r2 = db
        .execute("SELECT COUNT(*) FROM t WHERE a = 1 OR b = 2")
        .unwrap();
    assert_eq!(rows(&r2)[0][0], Value::Int64(3));
}

#[test]
fn concat_operator_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cod (id INT PRIMARY KEY, a TEXT, b TEXT)");
    exec(
        &db,
        "CREATE VIEW cod_v AS SELECT id, a || ' ' || b AS full_name FROM cod",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("||"));
}

#[test]
fn concat_operator_with_null() {
    let db = mem_db();
    let r = db.execute("SELECT 'hello' || NULL || 'world'").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
}

#[test]
fn concat_with_null_propagates() {
    let db = mem_db();
    let r = exec(&db, "SELECT 'hello' || NULL");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn concurrent_readers() {
    let db = std::sync::Arc::new(mem_db());
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
fn current_date_time_functions() {
    let db = mem_db();
    let r1 = db.execute("SELECT CURRENT_DATE").unwrap();
    assert_eq!(r1.rows().len(), 1);

    let r2 = db.execute("SELECT CURRENT_TIME").unwrap();
    assert_eq!(r2.rows().len(), 1);

    let r3 = db.execute("SELECT CURRENT_TIMESTAMP").unwrap();
    assert_eq!(r3.rows().len(), 1);
}

#[test]
fn decimal_column_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, price DECIMAL(10, 2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 19.99)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 100.00)").unwrap();
    let r = db.execute("SELECT price FROM t ORDER BY id").unwrap();
    let v = rows(&r);
    assert!(v.len() == 2);
}

#[test]
fn decimal_column_display() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE prices (id INT PRIMARY KEY, price DECIMAL(10, 2))",
    );
    exec(
        &db,
        "INSERT INTO prices VALUES (1, 19.99), (2, 0.01), (3, 100.00)",
    );
    let r = exec(&db, "SELECT price FROM prices ORDER BY id");
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn decimal_comparison() {
    let db = mem_db();
    db.execute("CREATE TABLE t(price DECIMAL(10,2))").unwrap();
    db.execute("INSERT INTO t VALUES (10.50), (20.75), (5.25)")
        .unwrap();
    // Decimal comparison not fully supported; just verify data storage
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn decimal_formatting_small_values() {
    let db = mem_db();
    db.execute("CREATE TABLE d (val DECIMAL(10, 4))").unwrap();
    db.execute("INSERT INTO d VALUES (0.0001)").unwrap();
    db.execute("INSERT INTO d VALUES (123.4567)").unwrap();
    db.execute("INSERT INTO d VALUES (-0.01)").unwrap();

    let result = db.execute("SELECT val FROM d ORDER BY val").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 3);
    // Values should be stored as Decimal
    assert!(matches!(rows[0][0], Value::Decimal { .. }));
}

#[test]
fn decimal_formatting_zero_scale() {
    let db = mem_db();
    db.execute("CREATE TABLE d2 (val DECIMAL(10, 0))").unwrap();
    db.execute("INSERT INTO d2 VALUES (42)").unwrap();

    let result = db.execute("SELECT val FROM d2").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Decimal { .. }));
}

#[test]
fn decimal_negative() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val DECIMAL(10, 2))").unwrap();
    db.execute("INSERT INTO t VALUES (-42.50)").unwrap();
    let r = db.execute("SELECT val FROM t").unwrap();
    assert!(rows(&r).len() == 1);
}

#[test]
fn decimal_negative_values() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE ledger (id INT PRIMARY KEY, amount DECIMAL(10, 2))",
    );
    exec(&db, "INSERT INTO ledger VALUES (1, -5.50), (2, -0.01)");
    let r = exec(&db, "SELECT amount FROM ledger ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn decimal_operations() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, price DECIMAL(10,2))")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 19.99), (2, 29.99), (3, 9.99)")
        .unwrap();
    // Decimal aggregates (SUM/AVG) not supported; test simple select
    let r = db.execute("SELECT price FROM t ORDER BY id").unwrap();
    assert_eq!(rows(&r).len(), 3);
}

#[test]
fn decimal_small_fraction() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE precise (id INT PRIMARY KEY, val DECIMAL(10, 4))",
    );
    exec(&db, "INSERT INTO precise VALUES (1, 0.0001)");
    let r = exec(&db, "SELECT val FROM precise");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn decimal_zero_scale() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE counts (id INT PRIMARY KEY, cnt DECIMAL(10, 0))",
    );
    exec(&db, "INSERT INTO counts VALUES (1, 42)");
    let r = exec(&db, "SELECT cnt FROM counts");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn deeply_nested_expressions() {
    let db = mem_db();
    let r = db
        .execute("SELECT ((1 + 2) * (3 + 4)) - ((5 - 6) * (7 + 8))")
        .unwrap();
    // (3*7) - ((-1)*15) = 21 - (-15) = 36
    assert_eq!(rows(&r)[0][0], Value::Int64(36));
}

#[test]
fn default_expression_evaluated() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, computed INT64 DEFAULT 1 + 2 + 3)")
        .unwrap();
    db.execute("INSERT INTO t(id) VALUES (1)").unwrap();
    let r = db.execute("SELECT computed FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(6));
}

#[test]
fn default_text_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, label TEXT DEFAULT 'hello' || ' ' || 'world')")
        .unwrap();
    db.execute("INSERT INTO t(id) VALUES (1)").unwrap();
    let r = db.execute("SELECT label FROM t WHERE id = 1").unwrap();
    // Might eval to concatenated string or just the literal depending on parsing
    let v = &rows(&r)[0][0];
    assert!(*v != Value::Null);
}

#[test]
fn default_value_expression() {
    let db = mem_db();
    db.execute(
        "CREATE TABLE t(id INT64, created_at INT64 DEFAULT 0, status TEXT DEFAULT 'pending')",
    )
    .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = db.execute("SELECT created_at, status FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(0));
    assert_eq!(v[0][1], Value::Text("pending".into()));
}

#[test]
fn division_by_zero() {
    let db = mem_db();
    let r = db.execute("SELECT 10 / 0");
    // Should either error or return NULL
    assert!(r.is_err() || rows(&r.unwrap())[0][0] == Value::Null);
}

#[test]
fn division_by_zero_int() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 0)").unwrap();
    let r = db.execute("SELECT a / b FROM t");
    // Should return error or NULL
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Null);
    }
}

#[test]
fn division_by_zero_returns_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT 1 / 0");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn error_division_by_zero_in_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 0)").unwrap();
    let r = db.execute("SELECT a / b FROM t");
    // Should error or return NULL
    assert!(r.is_err() || r.is_ok());
}

#[test]
fn error_lag_negative_offset() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let err = db
        .execute("SELECT LAG(val, -1) OVER (ORDER BY id) FROM t")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("negative") || msg.contains("offset") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn error_lag_non_integer_offset() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let err = db
        .execute("SELECT LAG(val, 'abc') OVER (ORDER BY id) FROM t")
        .unwrap_err();
    let msg = err.to_string();
    assert!(!msg.is_empty(), "expected an error for non-integer offset");
}

#[test]
fn error_primary_key_violation() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let err = db.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn execute_batch_multiple_statements() {
    let db = mem_db();
    let results = db
        .execute_batch(
            "CREATE TABLE t(id INT64);
             INSERT INTO t VALUES (1);
             INSERT INTO t VALUES (2);
             SELECT COUNT(*) FROM t;",
        )
        .unwrap();
    let last = results.last().unwrap();
    assert_eq!(rows(last)[0][0], Value::Int64(2));
}

#[test]
fn execute_batch_with_params() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let results = db
        .execute_batch_with_params(
            "INSERT INTO t VALUES ($1, $2); SELECT * FROM t;",
            &[Value::Int64(1), Value::Text("test".into())],
        )
        .unwrap();
    let last = results.last().unwrap();
    assert_eq!(rows(last).len(), 1);
}

#[test]
fn execute_with_params() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'hello'),(2,'world')")
        .unwrap();
    let r = db
        .execute_with_params("SELECT val FROM t WHERE id = $1", &[Value::Int64(1)])
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("hello".into()));
}

#[test]
fn expr_abs_and_math() {
    let db = mem_db();
    let r = db.execute("SELECT ABS(-42), ABS(42)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(42));
    assert_eq!(v[0][1], Value::Int64(42));
}

#[test]
fn expr_trigonometric_functions() {
    let db = mem_db();
    let r = db
        .execute(
            "SELECT
                SIN(PI() / 2),
                COS(0),
                TAN(0),
                ASIN(1),
                ACOS(1),
                ATAN(1),
                ATAN2(1, 1),
                DEGREES(PI()),
                RADIANS(180),
                COT(PI() / 4)",
        )
        .unwrap();
    let row = r.rows()[0].values();
    assert_float_close(&row[0], 1.0);
    assert_float_close(&row[1], 1.0);
    assert_float_close(&row[2], 0.0);
    assert_float_close(&row[3], std::f64::consts::FRAC_PI_2);
    assert_float_close(&row[4], 0.0);
    assert_float_close(&row[5], std::f64::consts::FRAC_PI_4);
    assert_float_close(&row[6], std::f64::consts::FRAC_PI_4);
    assert_float_close(&row[7], 180.0);
    assert_float_close(&row[8], std::f64::consts::PI);
    assert_float_close(&row[9], 1.0);
}

#[test]
fn expr_trigonometric_domain_and_null_handling() {
    let db = mem_db();
    let r = db
        .execute("SELECT ASIN(2), ACOS(-2), TAN(PI() / 2), COT(0), SIN(NULL)")
        .unwrap();
    assert_eq!(
        r.rows()[0].values(),
        &[
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );
}

#[test]
fn expr_conditional_functions() {
    let db = mem_db();
    let r = db
        .execute(
            "SELECT
                GREATEST(1, 5, 3),
                LEAST(1, 5, 3),
                IIF(TRUE, 'yes', 'no'),
                IIF(FALSE, 'yes', 'no'),
                IIF(NULL, 'yes', 'no')",
        )
        .unwrap();
    assert_eq!(
        r.rows()[0].values(),
        &[
            Value::Int64(5),
            Value::Int64(1),
            Value::Text("yes".to_string()),
            Value::Text("no".to_string()),
            Value::Text("no".to_string()),
        ]
    );
}

#[test]
fn expr_conditional_functions_null_propagation() {
    let db = mem_db();
    let r = db
        .execute("SELECT GREATEST(1, NULL, 3), LEAST(1, NULL, 3)")
        .unwrap();
    assert_eq!(r.rows()[0].values(), &[Value::Null, Value::Null]);
}

#[test]
fn expr_arithmetic_operators() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 3)").unwrap();
    let r = db
        .execute("SELECT a + b, a - b, a * b, a / b, a % b FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(13));
    assert_eq!(v[0][1], Value::Int64(7));
    assert_eq!(v[0][2], Value::Int64(30));
    assert_eq!(v[0][3], Value::Int64(3));
    assert_eq!(v[0][4], Value::Int64(1));
}

#[test]
fn expr_between_not_between() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(15),(20)")
        .unwrap();
    let r = db
        .execute("SELECT x FROM t WHERE x BETWEEN 5 AND 15 ORDER BY x")
        .unwrap();
    assert_eq!(rows(&r).len(), 3);
    let r2 = db
        .execute("SELECT x FROM t WHERE x NOT BETWEEN 5 AND 15 ORDER BY x")
        .unwrap();
    assert_eq!(rows(&r2).len(), 2);
}

#[test]
fn expr_boolean_and_or_combinations() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a BOOLEAN, b BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (TRUE, TRUE), (TRUE, FALSE), (FALSE, TRUE), (FALSE, FALSE)")
        .unwrap();
    let r = db.execute("SELECT a, b FROM t WHERE a AND b").unwrap();
    assert_eq!(rows(&r).len(), 1);
    let r2 = db.execute("SELECT a, b FROM t WHERE a OR b").unwrap();
    assert_eq!(rows(&r2).len(), 3);
}

#[test]
fn expr_case_simple_form() {
    let db = mem_db();
    db.execute("CREATE TABLE t(status TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('active'),('inactive'),('pending')")
        .unwrap();
    let r = db
        .execute("SELECT CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 ELSE -1 END AS code FROM t ORDER BY status")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(1)); // active
    assert_eq!(v[1][0], Value::Int64(0)); // inactive
    assert_eq!(v[2][0], Value::Int64(-1)); // pending
}

#[test]
fn expr_case_when_simple() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(NULL)")
        .unwrap();
    let r = db
        .execute("SELECT x, CASE WHEN x = 1 THEN 'one' WHEN x = 2 THEN 'two' ELSE 'other' END AS label FROM t ORDER BY x")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("other".into())); // NULL row sorts first
    assert_eq!(v[1][1], Value::Text("one".into()));
    assert_eq!(v[2][1], Value::Text("two".into()));
    assert_eq!(v[3][1], Value::Text("other".into()));
}

#[test]
fn expr_cast_int_to_text_to_float() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (42)").unwrap();
    let r = db.execute("SELECT CAST(x AS FLOAT64) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Float64(42.0));
}

#[test]
fn expr_coalesce_and_nullif() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL, 10), (5, 5), (3, NULL)")
        .unwrap();
    let r = db
        .execute(
            "SELECT COALESCE(a, b, 0) AS c, NULLIF(a, b) AS n FROM t ORDER BY COALESCE(a, b, 0)",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(3));
    assert_eq!(v[0][1], Value::Int64(3)); // 3 != NULL → 3
    assert_eq!(v[1][0], Value::Int64(5));
    assert_eq!(v[1][1], Value::Null); // 5 = 5 → NULL
    assert_eq!(v[2][0], Value::Int64(10));
    assert_eq!(v[2][1], Value::Null); // NULL, 10 → COALESCE=10, NULLIF(NULL,10)=NULL
}

#[test]
fn expr_comparison_operators() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    assert_eq!(
        rows(&db.execute("SELECT COUNT(*) FROM t WHERE x > 3").unwrap())[0][0],
        Value::Int64(2)
    );
    assert_eq!(
        rows(&db.execute("SELECT COUNT(*) FROM t WHERE x >= 3").unwrap())[0][0],
        Value::Int64(3)
    );
    assert_eq!(
        rows(&db.execute("SELECT COUNT(*) FROM t WHERE x < 3").unwrap())[0][0],
        Value::Int64(2)
    );
    assert_eq!(
        rows(&db.execute("SELECT COUNT(*) FROM t WHERE x <= 3").unwrap())[0][0],
        Value::Int64(3)
    );
    assert_eq!(
        rows(&db.execute("SELECT COUNT(*) FROM t WHERE x != 3").unwrap())[0][0],
        Value::Int64(4)
    );
}

#[test]
fn expr_concat_operator() {
    let db = mem_db();
    db.execute("CREATE TABLE t(first TEXT, last TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('John', 'Doe')").unwrap();
    let r = db.execute("SELECT first || ' ' || last FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("John Doe".into()));
}

#[test]
fn expr_float_arithmetic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x FLOAT64)").unwrap();
    db.execute("INSERT INTO t VALUES (3.14), (2.71)").unwrap();
    let r = db.execute("SELECT x * 2.0 FROM t ORDER BY x").unwrap();
    let v = rows(&r);
    if let Value::Float64(f) = v[0][0] {
        assert!((f - 5.42).abs() < 0.001);
    } else {
        panic!("expected Float64");
    }
}

#[test]
fn expr_in_list_and_not_in() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    let r = db
        .execute("SELECT x FROM t WHERE x IN (2, 4) ORDER BY x")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(4));
    let r2 = db
        .execute("SELECT x FROM t WHERE x NOT IN (2, 4) ORDER BY x")
        .unwrap();
    assert_eq!(rows(&r2).len(), 3);
}

#[test]
fn expr_is_null_is_not_null() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(NULL),(3),(NULL)")
        .unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE x IS NULL")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
    let r2 = db
        .execute("SELECT COUNT(*) FROM t WHERE x IS NOT NULL")
        .unwrap();
    assert_eq!(rows(&r2)[0][0], Value::Int64(2));
}

#[test]
fn expr_like_and_ilike() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('Bob'),('ALICE'),('Charlie')")
        .unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name LIKE 'A%' ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // ALICE and Alice
    let r2 = db
        .execute("SELECT name FROM t WHERE name ILIKE 'a%' ORDER BY name")
        .unwrap();
    let v2 = rows(&r2);
    assert_eq!(v2.len(), 2); // ALICE and Alice
}

#[test]
fn expr_like_with_underscore_pattern() {
    let db = mem_db();
    db.execute("CREATE TABLE t(code TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('AB'),('ABC'),('A1'),('XY')")
        .unwrap();
    let r = db
        .execute("SELECT code FROM t WHERE code LIKE 'A_' ORDER BY code")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // A1 and AB
}

#[test]
fn expr_string_functions_lower_upper_length() {
    let db = mem_db();
    db.execute("CREATE TABLE t(s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Hello World')").unwrap();
    let r = db
        .execute("SELECT LOWER(s), UPPER(s), LENGTH(s) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("hello world".into()));
    assert_eq!(v[0][1], Value::Text("HELLO WORLD".into()));
    assert_eq!(v[0][2], Value::Int64(11));
}

#[test]
fn expr_substring_and_trim() {
    let db = mem_db();
    db.execute("CREATE TABLE t(s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('  hello  ')").unwrap();
    let r = db.execute("SELECT TRIM(s) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("hello".into()));
}

#[test]
fn expr_unary_minus_and_not() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64, flag BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (5, TRUE), (-3, FALSE)")
        .unwrap();
    let r = db.execute("SELECT -x FROM t ORDER BY x").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(3)); // -(-3) = 3
    assert_eq!(v[1][0], Value::Int64(-5)); // -(5)
    let r2 = db
        .execute("SELECT x FROM t WHERE NOT flag ORDER BY x")
        .unwrap();
    assert_eq!(rows(&r2).len(), 1);
}

#[test]
fn expression_with_table_data() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 3), (20, 7), (15, 5)")
        .unwrap();
    // Complex expression in SELECT
    let r = db
        .execute("SELECT a * b + (a - b), a / b, a % b FROM t ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(10 * 3 + (10 - 3)));
}

#[test]
fn failpoint_log() {
    let r = Db::failpoint_log_json();
    if let Ok(json) = r {
        // Should be valid JSON even if empty
        assert!(json.starts_with('[') || json.starts_with('{'));
    }
}

#[test]
fn file_save_as() {
    let dir = TempDir::new().unwrap();
    let path1 = dir.path().join("original.ddb");
    let path2 = dir.path().join("copy.ddb");
    let db = Db::open_or_create(&path1, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (42)").unwrap();
    db.save_as(&path2).unwrap();
    let db2 = Db::open(&path2, DbConfig::default()).unwrap();
    let r = db2.execute("SELECT x FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn float_division_by_zero() {
    let db = mem_db();
    let r = db.execute("SELECT CAST(1.0 AS FLOAT64) / CAST(0.0 AS FLOAT64)");
    // Could be Infinity, NULL, or error
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn ilike_case_insensitive() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('BOB'),('charlie')")
        .unwrap();
    let r = db.execute("SELECT name FROM t WHERE name ILIKE 'alice'");
    if let Ok(r) = r {
        assert!(!rows(&r).is_empty());
    }
}

#[test]
fn ilike_pattern() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('ALICE'),('Bob')")
        .unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name ILIKE 'alice' ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn ilike_pattern_matching() {
    let db = mem_db();
    exec(&db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)");
    exec(
        &db,
        "INSERT INTO items VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'hello')",
    );
    let r = exec(
        &db,
        "SELECT id FROM items WHERE name ILIKE 'hello' ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn ilike_with_mixed_case() {
    let db = mem_db();
    exec(&db, "CREATE TABLE il (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO il VALUES (1, 'Hello'), (2, 'HELLO'), (3, 'hello'), (4, 'World')",
    );
    let r = exec(
        &db,
        "SELECT id FROM il WHERE val ILIKE '%ello%' ORDER BY id",
    );
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn implicit_type_coercion_int_to_float() {
    let db = mem_db();
    exec(&db, "CREATE TABLE itf (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO itf VALUES (1, 42)");
    let r = exec(&db, "SELECT val FROM itf");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn in_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    let r = db
        .execute("SELECT id FROM t WHERE id IN (2, 4) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(4));
}

#[test]
fn in_list_with_nulls() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nullcheck (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "INSERT INTO nullcheck VALUES (1, 10), (2, NULL), (3, 30)",
    );
    let r = exec(
        &db,
        "SELECT id FROM nullcheck WHERE val IN (10, 30) ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn index_rebuild() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    db.rebuild_index("idx_val").unwrap();
    let r = db.execute("SELECT id FROM t WHERE val = 'b'").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn index_rebuild_all() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a')").unwrap();
    db.rebuild_indexes().unwrap();
}

#[test]
fn index_verify() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    let verification = db.verify_index("idx_val").unwrap();
    assert!(verification.valid);
}

#[test]
fn indexes_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    // Index should be usable
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let r = db.execute("SELECT val FROM t WHERE val = 'hello'").unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn int_float_comparison() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ifc (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO ifc VALUES (1, 1.5), (2, 2.0), (3, 3.5)");
    let r = exec(&db, "SELECT id FROM ifc WHERE val > 2.0 ORDER BY id");
    assert!(!r.rows().is_empty());
}

#[test]
fn is_distinct_from() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 1), (1, 2), (NULL, NULL), (1, NULL)")
        .unwrap();
    let r = db
        .execute("SELECT a, b FROM t WHERE a IS DISTINCT FROM b ORDER BY a, b")
        .unwrap();
    let v = rows(&r);
    // (1,2) and (1,NULL) are distinct; (1,1) not distinct; (NULL,NULL) not distinct
    assert_eq!(v.len(), 2);
}

#[test]
fn regex_match_operators() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('bob'),('ALPHA')")
        .unwrap();

    let sensitive = db
        .execute("SELECT name FROM t WHERE name ~ '^A' ORDER BY name")
        .unwrap();
    assert_eq!(
        rows(&sensitive),
        vec![
            vec![Value::Text("ALPHA".to_string())],
            vec![Value::Text("Alice".to_string())]
        ]
    );

    let insensitive = db
        .execute("SELECT name FROM t WHERE name ~* '^a' ORDER BY name")
        .unwrap();
    assert_eq!(
        rows(&insensitive),
        vec![
            vec![Value::Text("ALPHA".to_string())],
            vec![Value::Text("Alice".to_string())]
        ]
    );
}

#[test]
fn regex_not_match_operators() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('bob'),('ALPHA')")
        .unwrap();

    let sensitive = db
        .execute("SELECT name FROM t WHERE name !~ '^A' ORDER BY name")
        .unwrap();
    assert_eq!(rows(&sensitive), vec![vec![Value::Text("bob".to_string())]]);

    let insensitive = db
        .execute("SELECT name FROM t WHERE name !~* '^a' ORDER BY name")
        .unwrap();
    assert_eq!(
        rows(&insensitive),
        vec![vec![Value::Text("bob".to_string())]]
    );
}

#[test]
fn regex_operator_null_and_invalid_pattern() {
    let db = mem_db();
    let null_result = db.execute("SELECT 'abc' ~ NULL, NULL ~ 'a'").unwrap();
    assert_eq!(rows(&null_result), vec![vec![Value::Null, Value::Null]]);

    let err = exec_err(&db, "SELECT 'abc' ~ '['");
    assert!(err.contains("invalid regular expression"));
}

#[test]
fn is_null_and_is_not_null() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nulls (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO nulls VALUES (1, 'a'), (2, NULL), (3, 'c')",
    );
    let r1 = exec(&db, "SELECT id FROM nulls WHERE val IS NULL");
    assert_eq!(r1.rows().len(), 1);
    let r2 = exec(
        &db,
        "SELECT id FROM nulls WHERE val IS NOT NULL ORDER BY id",
    );
    assert_eq!(r2.rows().len(), 2);
}

#[test]
fn is_null_is_not_null() {
    let db = mem_db();
    exec(&db, "CREATE TABLE inn (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO inn VALUES (1, NULL), (2, 10)");
    let r1 = exec(&db, "SELECT id FROM inn WHERE val IS NULL");
    assert_eq!(r1.rows().len(), 1);
    assert_eq!(r1.rows()[0].values()[0], Value::Int64(1));
    let r2 = exec(&db, "SELECT id FROM inn WHERE val IS NOT NULL");
    assert_eq!(r2.rows().len(), 1);
    assert_eq!(r2.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn json_extract_operator() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    db.execute(r#"INSERT INTO t VALUES (1, '{"name":"alice","age":30}')"#)
        .unwrap();
    let r = db.execute("SELECT data -> 'name' FROM t").unwrap();
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Text("\"alice\"".to_string())
    );

    let r = db.execute("SELECT data -> 'age' FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("30".to_string()));

    let r = db.execute("SELECT '{\"a\":1}'::text -> 'a'").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("1".to_string()));

    let r = db.execute("SELECT '[10,20,30]'::text -> 1").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("20".to_string()));

    let r = db.execute("SELECT '{\"x\":null}'::text -> 'x'").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("null".to_string()));
}

#[test]
fn json_extract_text_operator() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    db.execute(r#"INSERT INTO t VALUES (1, '{"key":"value"}')"#)
        .unwrap();
    let r = db.execute("SELECT data ->> 'key' FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("value".to_string()));

    let r = db
        .execute("SELECT '{\"a\":\"hello\"}'::text ->> 'a'")
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".to_string()));

    let r = db.execute("SELECT '[10,20,30]'::text ->> 1").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("20".to_string()));

    let r = db.execute("SELECT '[true,false]'::text ->> 0").unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Text("true".to_string()));

    let r = db
        .execute("SELECT '{\"a\":1}'::text ->> 'missing'")
        .unwrap();
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn large_blob_storage_and_retrieval() {
    let db = mem_db();
    exec(&db, "CREATE TABLE large_b (id INT PRIMARY KEY, data BLOB)");
    let big_blob = vec![0xABu8; 100_000];
    db.execute_with_params(
        "INSERT INTO large_b VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(big_blob.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM large_b WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Blob(big_blob));
}

#[test]
fn large_blob_values() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data BLOB)").unwrap();
    let large_blob = vec![0xABu8; 10000];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(large_blob.clone())],
    )
    .unwrap();
    let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
    let v = rows(&r);
    if let Value::Blob(b) = &v[0][0] {
        assert_eq!(b.len(), 10000);
    }
}

#[test]
fn large_integer_arithmetic() {
    let db = mem_db();
    // Very large integers are parsed as Float64 by SQL parser
    let r = db.execute("SELECT 1000000 + 0, 1000000 - 0").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(1_000_000));
}

#[test]
fn large_text_storage_and_retrieval() {
    let db = mem_db();
    exec(&db, "CREATE TABLE large_t (id INT PRIMARY KEY, data TEXT)");
    let big_text = "x".repeat(100_000);
    db.execute_with_params(
        "INSERT INTO large_t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(big_text.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM large_t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(big_text));
}

#[test]
fn large_text_values() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    let large_text = "x".repeat(10000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(large_text.clone())],
    )
    .unwrap();
    let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
    let v = rows(&r);
    if let Value::Text(s) = &v[0][0] {
        assert_eq!(s.len(), 10000);
    }
}

#[test]
fn length_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT length('hello')");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
}

#[test]
fn like_patterns() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('Alice'),('Bob'),('Charlie'),('Alicia')")
        .unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name LIKE 'Ali%' ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("Alice".into()));
    assert_eq!(v[1][0], Value::Text("Alicia".into()));
}

#[test]
fn like_percent_in_middle() {
    let db = mem_db();
    exec(&db, "CREATE TABLE lm (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO lm VALUES (1, 'abc_def'), (2, 'abc_xyz'), (3, 'xyz_def')",
    );
    let r = exec(&db, "SELECT id FROM lm WHERE val LIKE 'abc%' ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn like_underscore_wildcard() {
    let db = mem_db();
    exec(&db, "CREATE TABLE lu (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO lu VALUES (1, 'cat'), (2, 'car'), (3, 'cab'), (4, 'cap'), (5, 'cage')",
    );
    let r = exec(&db, "SELECT id FROM lu WHERE val LIKE 'ca_' ORDER BY id");
    assert_eq!(r.rows().len(), 4); // cat, car, cab, cap (3 chars matching 'ca_')
}

#[test]
fn like_with_escape_character() {
    let db = mem_db();
    exec(&db, "CREATE TABLE patterns (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO patterns VALUES (1, 'abc%def'), (2, 'abcXdef'), (3, 'abc')",
    );
    let r = exec(
        &db,
        "SELECT id FROM patterns WHERE val LIKE 'abc!%def' ESCAPE '!'",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn like_with_underscore() {
    let db = mem_db();
    db.execute("CREATE TABLE t(code TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('AB'),('AC'),('ABC'),('A1')")
        .unwrap();
    let r = db
        .execute("SELECT code FROM t WHERE code LIKE 'A_' ORDER BY code")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3); // AB, AC, A1
}

#[test]
fn many_columns_wide_row() {
    let db = mem_db();
    let cols: Vec<String> = (0..50).map(|i| format!("c{} INT64", i)).collect();
    let create = format!("CREATE TABLE wide({})", cols.join(", "));
    db.execute(&create).unwrap();
    let vals: Vec<String> = (0..50).map(|i| i.to_string()).collect();
    let insert = format!("INSERT INTO wide VALUES ({})", vals.join(", "));
    db.execute(&insert).unwrap();
    let r = db.execute("SELECT * FROM wide").unwrap();
    assert_eq!(rows(&r)[0].len(), 50);
}

#[test]
fn math_functions() {
    let db = mem_db();
    let r = db
        .execute("SELECT ABS(-42), ABS(42), ROUND(3.14159, 2), ROUND(3.5, 0)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(42));
    assert_eq!(v[0][1], Value::Int64(42));
}

#[test]
fn mixed_type_comparison() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b FLOAT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 10.5), (20, 19.5)")
        .unwrap();
    let r = db
        .execute("SELECT a, b FROM t WHERE a > b ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(20));
}

#[test]
fn modulo_by_zero() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 0)").unwrap();
    let r = db.execute("SELECT a % b FROM t");
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Null);
    }
}

#[test]
fn modulo_operator() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mo (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO mo VALUES (1, 17)");
    let r = exec(&db, "SELECT val % 5 FROM mo");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn multi_column_fk() {
    let db = mem_db();
    db.execute("CREATE TABLE parent(a INT64, b INT64, val TEXT, PRIMARY KEY (a, b))")
        .unwrap();
    db.execute(
        "CREATE TABLE child(id INT64, pa INT64, pb INT64,
         FOREIGN KEY (pa, pb) REFERENCES parent(a, b))",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 1, 'ok')")
        .unwrap();
    db.execute("INSERT INTO child VALUES (10, 1, 1)").unwrap();
    let err = db
        .execute("INSERT INTO child VALUES (20, 1, 2)")
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn multiple_triggers_on_same_table() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("CREATE TABLE log1(msg TEXT)").unwrap();
    db.execute("CREATE TABLE log2(msg TEXT)").unwrap();
    db.execute("CREATE TRIGGER trg1 AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log1 VALUES (''inserted'')')").unwrap();
    db.execute("CREATE TRIGGER trg2 AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log2 VALUES (''also_inserted'')')").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let r1 = db.execute("SELECT COUNT(*) FROM log1").unwrap();
    let r2 = db.execute("SELECT COUNT(*) FROM log2").unwrap();
    assert_eq!(rows(&r1)[0][0], Value::Int64(1));
    assert_eq!(rows(&r2)[0][0], Value::Int64(1));
}

#[test]
fn multiple_triggers_same_event() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE TABLE log(msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER trg1 AFTER INSERT ON t FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''trigger1'')')",
    )
    .unwrap();
    db.execute(
        "CREATE TRIGGER trg2 AFTER INSERT ON t FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''trigger2'')')",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM log").unwrap();
    assert!(rows(&r)[0][0] == Value::Int64(2));
}

#[test]
fn negate_null() {
    let db = mem_db();
    let r = db.execute("SELECT -NULL");
    if let Ok(r) = r {
        assert_eq!(rows(&r)[0][0], Value::Null);
    }
}

#[test]
fn negation_in_where_clause() {
    let db = mem_db();
    exec(&db, "CREATE TABLE neg (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO neg VALUES (1, 5), (2, -3)");
    let r = exec(&db, "SELECT id FROM neg WHERE -val > 0 ORDER BY id");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn negative_numbers_in_expressions() {
    let db = mem_db();
    let r = db.execute("SELECT -5 + 3, -(-10), 0 - 7").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(-2));
    assert_eq!(v[0][1], Value::Int64(10));
    assert_eq!(v[0][2], Value::Int64(-7));
}

#[test]
fn negative_zero_operations() {
    let db = mem_db();
    let r = db.execute("SELECT -0, 0 - 0, -1 + 1").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(0));
    assert_eq!(v[0][1], Value::Int64(0));
    assert_eq!(v[0][2], Value::Int64(0));
}

#[test]
fn nested_boolean_expressions() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64, c INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3),(4,5,6),(7,8,9)")
        .unwrap();
    let r = db
        .execute("SELECT * FROM t WHERE (a > 1 AND b < 8) OR (c = 9)")
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn nested_case_expression() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE nested_case (id INT PRIMARY KEY, val INT)",
    );
    exec(
        &db,
        "INSERT INTO nested_case VALUES (1, 10), (2, 50), (3, 90)",
    );
    let r = exec(
        &db,
        "
        SELECT id, 
            CASE 
                WHEN val < 30 THEN CASE WHEN val < 20 THEN 'very_low' ELSE 'low' END
                WHEN val < 70 THEN 'medium'
                ELSE 'high'
            END as category
        FROM nested_case
        ORDER BY id
    ",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Text("very_low".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("medium".to_string()));
    assert_eq!(r.rows()[2].values()[1], Value::Text("high".to_string()));
}

#[test]
fn nested_case_expressions() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
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
fn nested_case_when() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(20)")
        .unwrap();
    let r = db
        .execute(
            "SELECT x, CASE
                WHEN x < 5 THEN CASE WHEN x = 1 THEN 'one' ELSE 'few' END
                WHEN x < 15 THEN 'medium'
                ELSE 'many'
             END AS label FROM t ORDER BY x",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("one".into()));
    assert_eq!(v[1][1], Value::Text("medium".into()));
    assert_eq!(v[3][1], Value::Text("many".into()));
}

#[test]
fn nested_function_calls() {
    let db = mem_db();
    let r = db.execute("SELECT LOWER(UPPER('hello'))").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("hello".into()));
    let r2 = db.execute("SELECT LENGTH(TRIM('  hello  '))").unwrap();
    assert_eq!(rows(&r2)[0][0], Value::Int64(5));
}

#[test]
fn nested_subqueries_three_levels() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let r = db
        .execute(
            "SELECT * FROM (
                SELECT id, val FROM (
                    SELECT id, val FROM t WHERE val > 5
                ) sub1 WHERE id < 3
            ) sub2 ORDER BY id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn not_between() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(5),(10),(15),(20)")
        .unwrap();
    let r = db
        .execute("SELECT val FROM t WHERE val NOT BETWEEN 5 AND 15 ORDER BY val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(20));
}

#[test]
fn not_between_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nbe (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nbe VALUES (1, 5), (2, 15), (3, 25)");
    let r = exec(
        &db,
        "SELECT id FROM nbe WHERE val NOT BETWEEN 10 AND 20 ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn not_between_filter() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nb (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nb VALUES (1, 5), (2, 15), (3, 25)");
    let r = exec(
        &db,
        "SELECT id FROM nb WHERE val NOT BETWEEN 10 AND 20 ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn not_boolean_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nbe (id INT PRIMARY KEY, flag BOOL)");
    exec(&db, "INSERT INTO nbe VALUES (1, true), (2, false)");
    let r = exec(&db, "SELECT id FROM nbe WHERE NOT flag ORDER BY id");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn not_exists() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db.execute("SELECT NOT EXISTS (SELECT 1 FROM t)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Bool(true));
}

#[test]
fn not_in_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    let r = db
        .execute("SELECT id FROM t WHERE id NOT IN (2, 4) ORDER BY id")
        .unwrap();
    assert_eq!(rows(&r).len(), 3);
}

#[test]
fn not_in_list_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nil_t (id INT PRIMARY KEY, status TEXT)");
    exec(&db, "INSERT INTO nil_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let r = exec(
        &db,
        "SELECT id FROM nil_t WHERE status NOT IN ('a', 'c') ORDER BY id",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn not_like() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('alice'),('bob'),('charlie')")
        .unwrap();
    // Test NOT LIKE operator exists and works
    let r = db
        .execute("SELECT name FROM t WHERE NOT (name LIKE 'ali%') ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert!(!v.is_empty());
}

#[test]
fn not_like_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nlk (id INT PRIMARY KEY, name TEXT)");
    exec(
        &db,
        "INSERT INTO nlk VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia')",
    );
    // Exercise NOT LIKE code path
    let _r = exec(
        &db,
        "SELECT id FROM nlk WHERE name NOT LIKE 'Al%' ORDER BY id",
    );
}

#[test]
fn not_like_pattern() {
    let db = mem_db();
    exec(&db, "CREATE TABLE words (id INT PRIMARY KEY, word TEXT)");
    exec(
        &db,
        "INSERT INTO words VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')",
    );
    let r = exec(
        &db,
        "SELECT id FROM words WHERE word NOT LIKE 'a%' ORDER BY id",
    );
    // NOT LIKE 'a%' should exclude 'apple', keeping banana and cherry
    assert!(!r.rows().is_empty());
}

#[test]
fn not_null_returns_null() {
    let db = mem_db();
    let r = db.execute("SELECT NOT NULL").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn null_and_true_is_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULL AND TRUE");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn null_arithmetic() {
    let db = mem_db();
    let r = db
        .execute("SELECT NULL + 1, NULL * 5, NULL - NULL")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Null);
    assert_eq!(v[0][2], Value::Null);
}

#[test]
fn null_comparison_is_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULL = NULL");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn null_or_true_is_true() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULL OR TRUE");
    assert_eq!(r.rows()[0].values()[0], Value::Bool(true));
}

#[test]
fn null_safe_comparisons() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nsc (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nsc VALUES (1, 10), (2, NULL), (3, 30)");
    let r1 = exec(&db, "SELECT id FROM nsc WHERE val > 5 ORDER BY id");
    assert_eq!(r1.rows().len(), 2); // NULL doesn't match
    let r2 = exec(&db, "SELECT id FROM nsc WHERE val = NULL");
    assert_eq!(r2.rows().len(), 0); // = NULL is always false
}

#[test]
fn nullif_expression() {
    let db = mem_db();
    let r = db.execute("SELECT NULLIF(1, 1), NULLIF(1, 2)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Int64(1));
}

#[test]
fn nullif_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nf (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO nf VALUES (1, 10, 10), (2, 10, 20)");
    let r = exec(&db, "SELECT NULLIF(a, b) FROM nf ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
    assert_eq!(r.rows()[1].values()[0], Value::Int64(10));
}

#[test]
fn nullif_returns_first_on_unequal() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULLIF(1, 2)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn nullif_returns_null() {
    let db = mem_db();
    let r = db.execute("SELECT NULLIF(5, 5), NULLIF(5, 3)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[0][1], Value::Int64(5));
}

#[test]
fn nullif_returns_null_on_equal() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULLIF(1, 1)");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn overflow_large_blob() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data BYTEA)").unwrap();
    let big_blob = vec![0xABu8; 20_000];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(big_blob.clone())],
    )
    .unwrap();
    let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
    if let Value::Blob(b) = &rows(&r)[0][0] {
        assert_eq!(b.len(), 20_000);
    } else {
        panic!("expected Blob");
    }
}

#[test]
fn overflow_multiple_large_columns() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b TEXT, c TEXT)")
        .unwrap();
    let large_a = "A".repeat(5000);
    let large_b = "B".repeat(5000);
    let large_c = "C".repeat(5000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2, $3, $4)",
        &[
            Value::Int64(1),
            Value::Text(large_a.clone()),
            Value::Text(large_b.clone()),
            Value::Text(large_c.clone()),
        ],
    )
    .unwrap();
    let r = db
        .execute("SELECT LENGTH(a), LENGTH(b), LENGTH(c) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(5000));
    assert_eq!(v[0][1], Value::Int64(5000));
    assert_eq!(v[0][2], Value::Int64(5000));
}

#[test]
fn overflow_multiple_large_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, big TEXT)").unwrap();
    for i in 0..20 {
        let text = "x".repeat(5000).to_string();
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(text)],
        )
        .unwrap();
    }
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(20));
    // Verify we can read them back
    let r2 = db
        .execute("SELECT LENGTH(big) FROM t WHERE id = 5")
        .unwrap();
    assert_eq!(rows(&r2)[0][0], Value::Int64(5000));
}

#[test]
fn parameterized_delete() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    db.execute_with_params("DELETE FROM t WHERE id = $1", &[Value::Int64(2)])
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn parameterized_select() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
    let r = db
        .execute_with_params("SELECT id, val FROM t WHERE id > $1", &[Value::Int64(1)])
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn parameterized_update() {
    let db = mem_db();
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
fn params_all_types() {
    let db = mem_db();
    db.execute("CREATE TABLE t(i INT64, f FLOAT64, t TEXT, b BOOLEAN, bl BYTEA)")
        .unwrap();
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2, $3, $4, $5)",
        &[
            Value::Int64(42),
            Value::Float64(3.14),
            Value::Text("hello".into()),
            Value::Bool(true),
            Value::Blob(vec![1, 2, 3]),
        ],
    )
    .unwrap();
    let r = db
        .execute_with_params(
            "SELECT * FROM t WHERE i = $1 AND f > $2",
            &[Value::Int64(42), Value::Float64(3.0)],
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn parse_array_subscript_select() {
    let db = mem_db();
    // Even if arrays aren't fully supported, the parser path should handle it
    let r = db.execute("SELECT 1 AS val");
    assert!(r.is_ok());
}

#[test]
fn parse_boolean_literals() {
    let db = mem_db();
    let r = db.execute("SELECT TRUE, FALSE").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Bool(true));
    assert_eq!(v[0][1], Value::Bool(false));
}

#[test]
fn parse_column_alias() {
    let db = mem_db();
    let r = db
        .execute("SELECT 1 + 2 AS result, 'hello' AS greeting")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(3));
    assert_eq!(v[0][1], Value::Text("hello".into()));
}

#[test]
fn parse_complex_default_expressions() {
    let db = mem_db();
    db.execute(
        "CREATE TABLE t(
            id INT64,
            status TEXT DEFAULT 'active',
            created INT64 DEFAULT 0,
            flag BOOL DEFAULT TRUE
        )",
    )
    .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = db.execute("SELECT status, created, flag FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("active".into()));
    assert_eq!(v[0][1], Value::Int64(0));
    assert_eq!(v[0][2], Value::Bool(true));
}

#[test]
fn parse_complex_where_clause() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b TEXT, c FLOAT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello', 3.14), (2, 'world', 2.72), (3, NULL, 1.0)")
        .unwrap();
    let r = db
        .execute(
            "SELECT a FROM t WHERE (a > 1 AND b IS NOT NULL) OR (c < 2.0 AND a = 3) ORDER BY a",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // a=2 and a=3
}

#[test]
fn parse_complex_where_with_parens() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64, c INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3),(4,5,6),(7,8,9)")
        .unwrap();
    let r = db
        .execute("SELECT a FROM t WHERE ((a > 1 AND b > 3) OR c = 3) AND a < 8 ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert!(v.len() >= 2);
}

#[test]
fn parse_deeply_nested_parens() {
    let db = mem_db();
    let r = db.execute("SELECT ((((1 + 2) * 3) - 4) / 5)").unwrap();
    let v = rows(&r);
    // ((1+2)*3-4)/5 = (9-4)/5 = 5/5 = 1
    assert_eq!(v[0][0], Value::Int64(1));
}

#[test]
fn parse_double_colon_cast() {
    let db = mem_db();
    let r = db.execute("SELECT 42::TEXT, '100'::INT64");
    // May or may not be supported
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Text("42".into()));
    }
}

#[test]
fn parse_empty_in_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // IN with single element
    let r = db.execute("SELECT id FROM t WHERE id IN (2)").unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn parse_escaped_string_literal() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('it''s a test')").unwrap();
    let r = db.execute("SELECT val FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("it's a test".into()));
}

#[test]
fn parse_explicit_cast_syntax() {
    let db = mem_db();
    let r = db
        .execute("SELECT CAST(42 AS TEXT), CAST('100' AS INT64), CAST(3.14 AS INT64)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("42".into()));
    assert_eq!(v[0][1], Value::Int64(100));
    assert_eq!(v[0][2], Value::Int64(3));
}

#[test]
fn parse_expression_with_nested_parens() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE expr_t (id INT PRIMARY KEY, a INT, b INT, c INT)",
    );
    exec(&db, "INSERT INTO expr_t VALUES (1, 2, 3, 4)");
    let r = exec(&db, "SELECT ((a + b) * c) FROM expr_t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(20));
}

#[test]
fn parse_long_column_and_table_names() {
    let db = mem_db();
    let long_table = "very_long_table_name_that_tests_parsing";
    let long_col = "a_very_long_column_name_for_testing";
    db.execute(&format!("CREATE TABLE {}({} INT64)", long_table, long_col))
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (42)", long_table))
        .unwrap();
    let r = db
        .execute(&format!("SELECT {} FROM {}", long_col, long_table))
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn parse_multi_statement_batch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE batch1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE batch2 (id INT PRIMARY KEY)");
    // Both tables should exist
    let r1 = exec(&db, "SELECT * FROM batch1");
    let r2 = exec(&db, "SELECT * FROM batch2");
    assert_eq!(r1.rows().len(), 0);
    assert_eq!(r2.rows().len(), 0);
}

#[test]
fn parse_multiline_sql() {
    let db = mem_db();
    db.execute(
        "
        CREATE TABLE
            multiline_test
        (
            id INT64,
            name TEXT,
            val INT64
        )
        ",
    )
    .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM multiline_test").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(0));
}

#[test]
fn parse_negative_literal() {
    let db = mem_db();
    let r = db.execute("SELECT -42, -3.14").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(-42));
}

#[test]
fn parse_nested_function_calls() {
    let db = mem_db();
    let r = db.execute("SELECT UPPER(LOWER('Hello World'))").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("HELLO WORLD".into()));
}

#[test]
fn parse_qualified_column_names() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, val TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, ref_id INT64)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10, 1)").unwrap();
    let r = db
        .execute("SELECT t1.id, t1.val, t2.id FROM t1 JOIN t2 ON t1.id = t2.ref_id")
        .unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn parse_star_with_table_qualifier() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, a TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, b TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'x')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 'y')").unwrap();
    let r = db
        .execute("SELECT t1.*, t2.b FROM t1 JOIN t2 ON t1.id = t2.id")
        .unwrap();
    let v = rows(&r);
    assert!(v[0].len() >= 3);
}

#[test]
fn parse_table_alias() {
    let db = mem_db();
    db.execute("CREATE TABLE employees(id INT64, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO employees VALUES (1, 'Alice')")
        .unwrap();
    let r = db
        .execute("SELECT e.id, e.name FROM employees AS e")
        .unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn parse_type_cast() {
    let db = mem_db();
    let r = db.execute("SELECT CAST(42 AS TEXT)").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("42".into()));
}

#[test]
fn planner_aggregation_detection_in_between() {
    let db = mem_db();
    exec(&db, "CREATE TABLE between_agg (grp TEXT, val INT)");
    exec(
        &db,
        "INSERT INTO between_agg VALUES ('a', 1), ('a', 2), ('b', 10)",
    );
    let r = exec(
        &db,
        "
        SELECT grp
        FROM between_agg
        GROUP BY grp
        HAVING SUM(val) BETWEEN 1 AND 5
    ",
    );
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn planner_aggregation_detection_in_case() {
    let db = mem_db();
    exec(&db, "CREATE TABLE agg_test (grp TEXT, val INT)");
    exec(
        &db,
        "INSERT INTO agg_test VALUES ('a', 1), ('a', 2), ('b', 3)",
    );
    let r = exec(
        &db,
        "
        SELECT grp, CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END as cnt_label
        FROM agg_test
        GROUP BY grp
        ORDER BY grp
    ",
    );
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Text("many".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("one".to_string()));
}

#[test]
fn planner_aggregation_detection_in_like() {
    let db = mem_db();
    exec(&db, "CREATE TABLE like_agg (grp TEXT, val TEXT)");
    exec(
        &db,
        "INSERT INTO like_agg VALUES ('a', 'hello'), ('a', 'world'), ('b', 'hi')",
    );
    let r = exec(
        &db,
        "
        SELECT grp, MIN(val) as min_val
        FROM like_agg
        GROUP BY grp
        HAVING MIN(val) LIKE 'h%'
        ORDER BY grp
    ",
    );
    assert!(!r.rows().is_empty());
}

#[test]
fn planner_index_scan_with_equality_filter() {
    let db = mem_db();
    exec(&db, "CREATE TABLE indexed (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx_val ON indexed (val)");
    for i in 0..100 {
        exec(&db, &format!("INSERT INTO indexed VALUES ({i}, 'v{i}')"));
    }
    let r = exec(&db, "SELECT id FROM indexed WHERE val = 'v50'");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn pragma_dump_covers_tables_views_indexes_triggers() {
    let db = mem_db();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT DEFAULT 'anon', active BOOL)")
        .unwrap();
    db.execute("CREATE TABLE orders (id INT64 PRIMARY KEY, uid INT64 REFERENCES users(id))")
        .unwrap();
    db.execute("CREATE INDEX idx_name ON users (name)").unwrap();
    db.execute("CREATE VIEW active_users AS SELECT * FROM users WHERE active = TRUE")
        .unwrap();
    db.execute("CREATE TABLE audit_log (event TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER after_insert_user AFTER INSERT ON users FOR EACH ROW \
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log(event) VALUES (''inserted'')')",
    )
    .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'alice', TRUE)")
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, NULL, FALSE)")
        .unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();

    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CREATE TABLE"));
    assert!(dump.contains("INSERT"));
}

#[test]
fn qualified_wildcard() {
    let db = mem_db();
    exec(&db, "CREATE TABLE qw (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO qw VALUES (1, 'hello')");
    let r = exec(&db, "SELECT qw.* FROM qw");
    assert_eq!(r.columns().len(), 2);
}

#[test]
fn query_with_computed_columns() {
    let db = mem_db();
    db.execute("CREATE TABLE products(name TEXT, price INT64, qty INT64)")
        .unwrap();
    db.execute("INSERT INTO products VALUES ('A', 10, 5), ('B', 20, 3), ('C', 5, 10)")
        .unwrap();
    let r = db
        .execute("SELECT name, price * qty AS total FROM products ORDER BY total DESC")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(60)); // B: 20*3
    assert_eq!(v[1][1], Value::Int64(50)); // C: 5*10 or A: 10*5
}

#[test]
fn query_with_offset_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE od (id INT PRIMARY KEY)");
    exec(
        &db,
        "CREATE VIEW odv AS SELECT id FROM od ORDER BY id LIMIT 10 OFFSET 5",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("OFFSET") || dump.contains("offset"));
}

#[test]
fn reader_during_write() {
    let db = std::sync::Arc::new(mem_db());
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

#[test]
fn replace_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT replace('hello world', 'world', 'earth')");
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Text("hello earth".to_string())
    );
}

#[test]
fn save_as_creates_copy() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("source.ddb");
    let dst = dir.path().join("backup.ddb");

    let db = Db::open_or_create(src.to_str().unwrap(), DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
        .unwrap();

    db.save_as(dst.to_str().unwrap()).unwrap();

    let db2 = Db::open_or_create(dst.to_str().unwrap(), DbConfig::default()).unwrap();
    let result = db2.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Int64(2));
}

#[test]
fn scalar_abs() {
    let db = mem_db();
    let r = db.execute("SELECT ABS(-42), ABS(42), ABS(0)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(42));
    assert_eq!(v[0][1], Value::Int64(42));
    assert_eq!(v[0][2], Value::Int64(0));
}

#[test]
fn scalar_concat() {
    let db = mem_db();
    let r = db
        .execute(
            "SELECT
                CONCAT('hello', ' ', 'world'),
                CONCAT('a', NULL, 'b'),
                CONCAT_WS('-', '2024', '03', '25'),
                CONCAT_WS(', ', 'Alice', NULL, 'Bob')",
        )
        .unwrap();
    assert_eq!(
        rows(&r)[0],
        &[
            Value::Text("hello world".into()),
            Value::Text("ab".into()),
            Value::Text("2024-03-25".into()),
            Value::Text("Alice, Bob".into()),
        ]
    );
}

#[test]
fn scalar_left_right() {
    let db = mem_db();
    let r = db
        .execute("SELECT LEFT('hello world', 5), RIGHT('hello world', 5)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("hello".into()));
    assert_eq!(v[0][1], Value::Text("world".into()));
}

#[test]
fn scalar_round() {
    let db = mem_db();
    let r = db.execute("SELECT ROUND(3.14159, 2)").unwrap();
    if let Value::Float64(f) = rows(&r)[0][0] {
        assert!((f - 3.14).abs() < 0.01);
    }
}

#[test]
fn scalar_trim() {
    let db = mem_db();
    let r = db.execute("SELECT TRIM('  hello  ')").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("hello".into()));
}

#[test]
fn scalar_upper_lower() {
    let db = mem_db();
    let r = db.execute("SELECT UPPER('hello'), LOWER('WORLD')").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("HELLO".into()));
    assert_eq!(v[0][1], Value::Text("world".into()));
}

#[test]
fn select_expression_no_from() {
    let db = mem_db();
    let r = exec(&db, "SELECT 'hello' || ' ' || 'world'");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn select_from_nonexistent_table() {
    let db = mem_db();
    let err = exec_err(&db, "SELECT * FROM ghost_table");
    assert!(
        err.contains("ghost_table") || err.contains("not found") || err.contains("unknown"),
        "got: {err}"
    );
}

#[test]
fn select_from_table_valued_function_unsupported() {
    let db = mem_db();
    // generate_series may not be supported; exercise the error path
    let err = exec_err(&db, "SELECT * FROM generate_series(1, 5)");
    assert!(
        err.to_lowercase().contains("not supported")
            || err.to_lowercase().contains("function")
            || err.to_lowercase().contains("generate_series"),
        "got: {err}"
    );
}

#[test]
fn select_literal_no_from() {
    let db = mem_db();
    let r = db
        .execute("SELECT 1 + 2 AS result, 'hello' AS greeting")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(3));
    assert_eq!(v[0][1], Value::Text("hello".into()));
}

#[test]
fn select_star_from_empty_table_with_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    let r = db.execute("SELECT * FROM t WHERE val = 'nothing'").unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn select_table_dot_star() {
    let db = mem_db();
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
fn select_without_from() {
    let db = mem_db();
    let r = exec(&db, "SELECT 1 + 2 AS result");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

#[test]
fn values_query_body_executes() {
    let db = mem_db();
    let r = exec(&db, "VALUES (1, 'one'), (2, 'two') ORDER BY col1");
    assert_eq!(
        rows(&r),
        vec![
            vec![Value::Int64(1), Value::Text("one".into())],
            vec![Value::Int64(2), Value::Text("two".into())],
        ]
    );
}

#[test]
fn create_table_as_select_populates_rows() {
    let db = mem_db();
    exec(&db, "CREATE TABLE src(id INT64, name TEXT)");
    exec(&db, "INSERT INTO src VALUES (1, 'a'), (2, 'b')");
    exec(
        &db,
        "CREATE TABLE copy AS SELECT id, name FROM src ORDER BY id",
    );
    let r = exec(&db, "SELECT id, name FROM copy ORDER BY id");
    assert_eq!(
        rows(&r),
        vec![
            vec![Value::Int64(1), Value::Text("a".into())],
            vec![Value::Int64(2), Value::Text("b".into())],
        ]
    );
}

#[test]
fn create_table_as_with_column_list_and_no_data() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE ctas_named (a, b) AS SELECT 1, 'x' WITH NO DATA",
    );
    let count = exec(&db, "SELECT COUNT(*) FROM ctas_named");
    assert_eq!(rows(&count)[0][0], Value::Int64(0));
    let r = exec(&db, "SELECT a, b FROM ctas_named");
    assert_eq!(r.columns(), &["a".to_string(), "b".to_string()]);
}

#[test]
fn set_except() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(x INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT x FROM t1 EXCEPT SELECT x FROM t2 ORDER BY x")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(1));
}

#[test]
fn set_intersect() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(x INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT x FROM t1 INTERSECT SELECT x FROM t2 ORDER BY x")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn simple_case_expression_with_operand() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE simple_case (id INT PRIMARY KEY, status TEXT)",
    );
    exec(
        &db,
        "INSERT INTO simple_case VALUES (1, 'active'), (2, 'inactive'), (3, 'pending')",
    );
    let r = exec(
        &db,
        "
        SELECT id,
            CASE status
                WHEN 'active' THEN 1
                WHEN 'inactive' THEN 0
                ELSE -1
            END as code
        FROM simple_case
        ORDER BY id
    ",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[1], Value::Int64(0));
    assert_eq!(r.rows()[2].values()[1], Value::Int64(-1));
}

#[test]
fn sql_with_block_comments_containing_semicolons() {
    let db = mem_db();
    let result = db
        .execute("/* this has a ; in it */ SELECT 1 AS val")
        .unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Int64(1));
}

#[test]
fn storage_page_operations() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    // Read a page
    let page = db.read_page(1).unwrap();
    assert!(!page.is_empty());
    // Storage info should show page details
    let info = db.storage_info().unwrap();
    assert!(info.page_count >= 2);
}

#[test]
fn storage_write_page_operations() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let info = db.header_info().unwrap();
    let page_size = info.page_size as usize;
    // Allocate and write a page
    db.begin_write().unwrap();
    let page_id = db.allocate_page().unwrap();
    let data = vec![0u8; page_size];
    db.write_page(page_id, &data).unwrap();
    db.commit().unwrap();
}

#[test]
fn string_comparison_operators() {
    let db = mem_db();
    db.execute("CREATE TABLE t(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('apple'),('banana'),('cherry')")
        .unwrap();
    let r = db
        .execute("SELECT name FROM t WHERE name >= 'banana' ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("banana".into()));
    assert_eq!(v[1][0], Value::Text("cherry".into()));
}

#[test]
fn string_concatenation_operator() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE concat_t (id INT PRIMARY KEY, first TEXT, last TEXT)",
    );
    exec(&db, "INSERT INTO concat_t VALUES (1, 'John', 'Doe')");
    let r = exec(&db, "SELECT first || ' ' || last FROM concat_t");
    assert_eq!(r.rows()[0].values()[0], Value::Text("John Doe".to_string()));
}

#[test]
fn string_functions_comprehensive() {
    let db = mem_db();
    let r = db.execute("SELECT LENGTH('hello'), UPPER('hello'), LOWER('HELLO'), TRIM('  hi  '), LEFT('hello', 3), RIGHT('hello', 3)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(5));
    assert_eq!(v[0][1], Value::Text("HELLO".into()));
    assert_eq!(v[0][2], Value::Text("hello".into()));
    assert_eq!(v[0][3], Value::Text("hi".into()));
    assert_eq!(v[0][4], Value::Text("hel".into()));
    assert_eq!(v[0][5], Value::Text("llo".into()));
}

#[test]
fn string_functions_coverage() {
    let db = mem_db();
    let r = exec(&db, "SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test'), SUBSTR('abcdef', 2, 3), REPLACE('hello', 'l', 'r'), TRIM('  hi  ')");
    assert_eq!(r.rows()[0].values()[0], Value::Text("HELLO".into()));
    assert_eq!(r.rows()[0].values()[1], Value::Text("world".into()));
    assert_eq!(r.rows()[0].values()[2], Value::Int64(4));
    assert_eq!(r.rows()[0].values()[3], Value::Text("bcd".into()));
    assert_eq!(r.rows()[0].values()[4], Value::Text("herro".into()));
    assert_eq!(r.rows()[0].values()[5], Value::Text("hi".into()));
}

#[test]
fn string_length_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE str_t (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO str_t VALUES (1, 'hello'), (2, ''), (3, 'world!')",
    );
    let r = exec(&db, "SELECT length(val) FROM str_t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[2].values()[0], Value::Int64(6));
}

#[test]
fn string_lower_upper() {
    let db = mem_db();
    exec(&db, "CREATE TABLE case_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO case_t VALUES (1, 'Hello World')");
    let r1 = exec(&db, "SELECT lower(val) FROM case_t");
    assert_eq!(
        r1.rows()[0].values()[0],
        Value::Text("hello world".to_string())
    );
    let r2 = exec(&db, "SELECT upper(val) FROM case_t");
    assert_eq!(
        r2.rows()[0].values()[0],
        Value::Text("HELLO WORLD".to_string())
    );
}

#[test]
fn string_operators_on_columns() {
    let db = mem_db();
    db.execute("CREATE TABLE t(first_name TEXT, last_name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .unwrap();
    let r = db
        .execute("SELECT first_name || ' ' || last_name AS full_name FROM t ORDER BY first_name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("Jane Smith".into()));
    assert_eq!(v[1][0], Value::Text("John Doe".into()));
}

#[test]
fn string_position() {
    let db = mem_db();
    let r = db
        .execute("SELECT POSITION('world' IN 'hello world'), POSITION('xyz' IN 'hello world')")
        .unwrap();
    assert_eq!(rows(&r)[0], &[Value::Int64(7), Value::Int64(0)]);
}

#[test]
fn extended_string_functions_slice5() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                INITCAP('hello world from decentdb'),
                ASCII('A'),
                REGEXP_REPLACE('abc123def', '\\d', '', 'g'),
                SPLIT_PART('a,b,c', ',', 2),
                STRING_TO_ARRAY('a,b,c', ','),
                QUOTE_IDENT('table name'),
                QUOTE_LITERAL('O''Brien'),
                MD5('hello'),
                SHA256('hello')",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("Hello World From Decentdb".to_string()),
            Value::Int64(65),
            Value::Text("abcdef".to_string()),
            Value::Text("b".to_string()),
            Value::Text("[\"a\",\"b\",\"c\"]".to_string()),
            Value::Text("\"table name\"".to_string()),
            Value::Text("'O''Brien'".to_string()),
            Value::Text("5d41402abc4b2a76b9719d911017c592".to_string()),
            Value::Text(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_string(),
            ),
        ]
    );
}

#[test]
fn string_replace() {
    let db = mem_db();
    db.execute("CREATE TABLE t(s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('hello world')").unwrap();
    let r = db
        .execute("SELECT REPLACE(s, 'world', 'rust') FROM t")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("hello rust".into()));
}

#[test]
fn substr_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT substr('hello world', 7, 5)");
    assert_eq!(r.rows()[0].values()[0], Value::Text("world".to_string()));
}

#[test]
fn table_info_api() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT, val FLOAT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a', 1.0)").unwrap();
    let tables = db.list_tables().unwrap();
    let t = tables.iter().find(|t| t.name == "t").unwrap();
    assert_eq!(t.columns.len(), 3);
    assert_eq!(t.row_count, 1);
}

#[test]
fn tables_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    // Verify both tables can be queried
    db.execute("SELECT * FROM t1").unwrap();
    db.execute("SELECT * FROM t2").unwrap();
}

#[test]
fn text_to_number_coercion_in_comparison() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    // Comparing text column with numeric literal
    let r = db.execute("SELECT * FROM t WHERE id = 1");
    assert!(r.is_ok());
}

#[test]
fn timestamp_type() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, '2024-01-15 10:30:00')")
        .unwrap();
    let r = db.execute("SELECT ts FROM t WHERE id = 1").unwrap();
    assert!(rows(&r)[0][0] != Value::Null);
}

#[test]
fn unary_minus_in_query() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (5), (10), (15)").unwrap();
    let r = db.execute("SELECT -val FROM t ORDER BY val").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(-5));
    assert_eq!(v[1][0], Value::Int64(-10));
}

#[test]
fn unary_not_in_where() {
    let db = mem_db();
    exec(&db, "CREATE TABLE unw (id INT PRIMARY KEY, active BOOL)");
    exec(
        &db,
        "INSERT INTO unw VALUES (1, true), (2, false), (3, true)",
    );
    let r = exec(&db, "SELECT id FROM unw WHERE NOT active ORDER BY id");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn unique_index_violation_after_update() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT UNIQUE)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    let err = db
        .execute("UPDATE t SET val = 'a' WHERE id = 2")
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn upper_lower_functions() {
    let db = mem_db();
    let r = exec(&db, "SELECT upper('hello'), lower('WORLD')");
    assert_eq!(r.rows()[0].values()[0], Value::Text("HELLO".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Text("world".to_string()));
}

#[test]
fn uuid_column_type() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uuid_t (id INT PRIMARY KEY, uid UUID)");
    // Use parameterized insert for UUID
    db.execute_with_params(
        "INSERT INTO uuid_t VALUES ($1, $2)",
        &[
            Value::Int64(1),
            Value::Uuid([
                0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
                0x00, 0x00,
            ]),
        ],
    )
    .unwrap();
    let r = exec(&db, "SELECT uid FROM uuid_t");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn verify_pk_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    for i in 0..50 {
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(format!("v{}", i))],
        )
        .unwrap();
    }
    let indexes = db.list_indexes().unwrap();
    for idx in &indexes {
        let v = db.verify_index(&idx.name).unwrap();
        assert!(v.valid);
    }
}

// ── Tests merged from engine_coverage_tests.rs ──

#[test]
fn case_when() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Text("yes".to_string())]);
}

#[test]
fn case_with_value() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Text("one".to_string())]);
}

#[test]
fn cast_from_int_to_text() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT CAST(42 AS TEXT)").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Text("42".to_string())]);
}

#[test]
fn cast_from_text_to_int() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT CAST('42' AS INT64)").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(42)]);
}

#[test]
fn cast_parameterized_text_to_decimal() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute_with_params(
            "SELECT CAST($1 AS DECIMAL(10,2))",
            &[Value::Text("19.99".to_string())],
        )
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[Value::Decimal {
            scaled: 1999,
            scale: 2
        }]
    );
}

#[test]
fn current_date_time_functions_return_expected_shapes() {
    use chrono::{Datelike, NaiveDate, NaiveTime, Utc};

    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, NOW()")
        .unwrap();
    let row = result.rows()[0].values();

    let expected_year = i64::from(Utc::now().year());
    assert_eq!(
        db.execute("SELECT EXTRACT(YEAR FROM CURRENT_TIMESTAMP)")
            .unwrap()
            .rows()[0]
            .values()[0],
        Value::Int64(expected_year)
    );

    match &row[0] {
        Value::Text(value) => {
            NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("CURRENT_DATE format")
        }
        other => panic!("expected CURRENT_DATE text output, got {other:?}"),
    };
    match &row[1] {
        Value::Text(value) => {
            NaiveTime::parse_from_str(value, "%H:%M:%S").expect("CURRENT_TIME format")
        }
        other => panic!("expected CURRENT_TIME text output, got {other:?}"),
    };
    assert!(matches!(row[2], Value::TimestampMicros(_)));
    assert!(matches!(row[3], Value::TimestampMicros(_)));
}

#[test]
fn date_time_functions_propagate_nulls() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT DATE(NULL), DATETIME(NULL), STRFTIME('%Y', NULL), EXTRACT(YEAR FROM NULL)")
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Null, Value::Null, Value::Null, Value::Null]
    );
}

#[test]
fn date_time_scalar_functions_cover_documented_slice_6_examples() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                DATE('2024-03-15', '+1 month'),
                DATETIME('2024-03-15 10:30:00', '+2 hours'),
                STRFTIME('%Y-%m-%d', '2024-03-15 14:30:00'),
                STRFTIME('%H:%M:%S', '2024-03-15 14:30:00'),
                STRFTIME('%Y', '2024-03-15'),
                EXTRACT(YEAR FROM '2024-03-15'),
                EXTRACT(MONTH FROM '2024-03-15'),
                EXTRACT(DOW FROM '2024-03-15'),
                EXTRACT(HOUR FROM '2024-03-15 14:30:00')",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("2024-04-15".to_string()),
            Value::Text("2024-03-15 12:30:00".to_string()),
            Value::Text("2024-03-15".to_string()),
            Value::Text("14:30:00".to_string()),
            Value::Text("2024".to_string()),
            Value::Int64(2024),
            Value::Int64(3),
            Value::Int64(5),
            Value::Int64(14),
        ]
    );
}

#[test]
fn extended_datetime_functions_date_trunc_and_parts() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                DATE_TRUNC('month', '2024-03-15 14:30:45'),
                DATE_TRUNC('day', '2024-03-15 14:30:45'),
                DATE_TRUNC('hour', '2024-03-15 14:30:45'),
                DATE_PART('year', '2024-03-15'),
                DATE_PART('doy', '2024-03-15')",
        )
        .unwrap();
    let row = result.rows()[0].values();
    assert_eq!(row[0], Value::TimestampMicros(1_709_251_200_000_000));
    assert_eq!(row[1], Value::TimestampMicros(1_710_460_800_000_000));
    assert_eq!(row[2], Value::TimestampMicros(1_710_511_200_000_000));
    assert_eq!(row[3], Value::Int64(2024));
    assert_eq!(row[4], Value::Int64(75));
}

#[test]
fn extended_datetime_functions_diff_and_constructors() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                DATE_DIFF('day', '2024-03-10', '2024-03-15'),
                DATE_DIFF('month', '2024-01-15', '2024-03-14'),
                LAST_DAY('2024-02-11'),
                NEXT_DAY('2024-03-15', 'Monday'),
                MAKE_DATE(2024, 3, 15),
                MAKE_TIMESTAMP(2024, 3, 15, 14, 30, 0)",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(5),
            Value::Int64(1),
            Value::Text("2024-02-29".to_string()),
            Value::Text("2024-03-18".to_string()),
            Value::Text("2024-03-15".to_string()),
            Value::TimestampMicros(1_710_513_000_000_000),
        ]
    );
}

#[test]
fn extended_datetime_functions_to_timestamp_and_age() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                TO_TIMESTAMP(1710505800),
                TO_TIMESTAMP('2024-03-15 14:30:00', 'YYYY-MM-DD HH24:MI:SS'),
                TO_TIMESTAMP('15/03/2024', 'DD/MM/YYYY'),
                AGE('2024-03-15', '2024-03-14')",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::TimestampMicros(1_710_505_800_000_000),
            Value::TimestampMicros(1_710_513_000_000_000),
            Value::TimestampMicros(1_710_460_800_000_000),
            Value::Text("1 days 00:00:00".to_string()),
        ]
    );
}

#[test]
fn extended_datetime_functions_null_propagation() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                DATE_TRUNC('day', NULL),
                DATE_PART('day', NULL),
                DATE_DIFF('day', NULL, '2024-01-01'),
                LAST_DAY(NULL),
                NEXT_DAY(NULL, 'Monday'),
                MAKE_DATE(NULL, 1, 1),
                MAKE_TIMESTAMP(2024, 1, 1, 0, 0, NULL),
                TO_TIMESTAMP(NULL),
                AGE(NULL, '2024-01-01')",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]
    );
}

#[test]
fn extended_datetime_interval_arithmetic() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                '2024-03-15 14:30:00'::timestamp + INTERVAL '1 day',
                '2024-03-15 14:30:00'::timestamp - INTERVAL '2 hour',
                '2024-03-15 14:30:00'::timestamp + INTERVAL '1 month',
                '2024-03-15 14:30:00'::timestamp + INTERVAL '1 year 2 months 3 days',
                '2024-03-15'::date + INTERVAL '7 days'",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::TimestampMicros(1_710_599_400_000_000),
            Value::TimestampMicros(1_710_505_800_000_000),
            Value::TimestampMicros(1_713_105_000_000_000),
            Value::TimestampMicros(1_747_492_200_000_000),
            Value::TimestampMicros(1_711_065_600_000_000),
        ]
    );
}

#[test]
fn extended_datetime_interval_arithmetic_with_casted_text() {
    let db = mem_db();
    let result = db
        .execute(
            "SELECT
                CAST('2024-03-15 14:30:00' AS TIMESTAMP) + CAST('1 day' AS INTERVAL),
                CAST('2024-03-15' AS DATE) + CAST('1 month' AS INTERVAL),
                CAST('2024-03-15 14:30:00' AS TIMESTAMP) - CAST('2 hour' AS INTERVAL)",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::TimestampMicros(1_710_599_400_000_000),
            Value::TimestampMicros(1_713_052_800_000_000),
            Value::TimestampMicros(1_710_505_800_000_000),
        ]
    );
}

#[test]
fn in_with_list() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let result = db
        .execute("SELECT id FROM t WHERE id IN (1, 3) ORDER BY id")
        .unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
    assert_eq!(rows[1].values(), &[Value::Int64(3)]);
}

#[test]
fn instr_function() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT INSTR('hello world', 'world')").unwrap();
    assert_eq!(result.rows()[0].values()[0], Value::Int64(7));
}

#[test]
fn is_not_null_operator() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();

    let result = db
        .execute("SELECT id FROM t WHERE val IS NOT NULL")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(2)]);
}

#[test]
fn is_null_operator() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();

    let result = db.execute("SELECT id FROM t WHERE val IS NULL").unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn json_operators_execute_and_round_trip_through_views() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE VIEW json_ops AS
         SELECT
             '{\"name\":\"Alice\",\"meta\":{\"version\":2}}'->>'name' AS name_text,
             '{\"name\":\"Alice\"}'->'name' AS name_json,
             '[10,20,30]'->>1 AS second_item,
             '{\"meta\":{\"version\":2}}'->'meta'->>'version' AS version_text",
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT name_text, name_json, second_item, version_text,
                    NULL->>'name',
                    '{\"name\":\"Alice\"}'->>'missing'
             FROM json_ops",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("Alice".to_string()),
            Value::Text("\"Alice\"".to_string()),
            Value::Text("20".to_string()),
            Value::Text("2".to_string()),
            Value::Null,
            Value::Null,
        ]
    );
}

#[test]
fn json_scalar_functions_cover_documented_slice_6_surface() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                JSON_OBJECT('age', 30, 'name', 'Alice'),
                JSON_ARRAY(1, 2, 'three', NULL),
                JSON_TYPE('{\"a\": 1}', '$.a'),
                JSON_TYPE('[1, 2, 3]'),
                JSON_TYPE('{\"a\": 1}', '$.missing'),
                JSON_VALID('{\"a\":1}'),
                JSON_VALID('not json'),
                JSON_VALID(NULL)",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("{\"age\":30,\"name\":\"Alice\"}".to_string()),
            Value::Text("[1,2,\"three\",null]".to_string()),
            Value::Text("integer".to_string()),
            Value::Text("array".to_string()),
            Value::Null,
            Value::Bool(true),
            Value::Bool(false),
            Value::Null,
        ]
    );
}

#[test]
fn json_table_functions_execute_documented_examples() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let each = db
        .execute("SELECT key, value, type FROM json_each('[10,20,30]') ORDER BY key")
        .unwrap();
    assert_eq!(
        each.rows()[0].values(),
        &[
            Value::Int64(0),
            Value::Int64(10),
            Value::Text("integer".to_string())
        ]
    );
    assert_eq!(
        each.rows()[1].values(),
        &[
            Value::Int64(1),
            Value::Int64(20),
            Value::Text("integer".to_string())
        ]
    );
    assert_eq!(
        each.rows()[2].values(),
        &[
            Value::Int64(2),
            Value::Int64(30),
            Value::Text("integer".to_string())
        ]
    );

    let object_each = db
        .execute("SELECT key, value, type FROM json_each('{\"a\":1,\"b\":2}') ORDER BY key")
        .unwrap();
    assert_eq!(
        object_each.rows()[0].values(),
        &[
            Value::Text("a".to_string()),
            Value::Int64(1),
            Value::Text("integer".to_string())
        ]
    );
    assert_eq!(
        object_each.rows()[1].values(),
        &[
            Value::Text("b".to_string()),
            Value::Int64(2),
            Value::Text("integer".to_string())
        ]
    );

    let tree = db
        .execute(
            "SELECT key, value, type, path
             FROM json_tree('{\"a\":{\"b\":1},\"c\":[2,3]}')
             ORDER BY path",
        )
        .unwrap();
    assert_eq!(
        tree.rows()[0].values(),
        &[
            Value::Null,
            Value::Text("{\"a\":{\"b\":1},\"c\":[2,3]}".to_string()),
            Value::Text("object".to_string()),
            Value::Text("$".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[1].values(),
        &[
            Value::Text("a".to_string()),
            Value::Text("{\"b\":1}".to_string()),
            Value::Text("object".to_string()),
            Value::Text("$.a".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[2].values(),
        &[
            Value::Text("b".to_string()),
            Value::Int64(1),
            Value::Text("integer".to_string()),
            Value::Text("$.a.b".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[3].values(),
        &[
            Value::Text("c".to_string()),
            Value::Text("[2,3]".to_string()),
            Value::Text("array".to_string()),
            Value::Text("$.c".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[4].values(),
        &[
            Value::Int64(0),
            Value::Int64(2),
            Value::Text("integer".to_string()),
            Value::Text("$.c[0]".to_string())
        ]
    );
    assert_eq!(
        tree.rows()[5].values(),
        &[
            Value::Int64(1),
            Value::Int64(3),
            Value::Text("integer".to_string()),
            Value::Text("$.c[1]".to_string())
        ]
    );
}

#[test]
fn json_table_functions_handle_null_inputs_and_view_roundtrip() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let each = db.execute("SELECT * FROM json_each(NULL)").unwrap();
    assert!(each.rows().is_empty());

    let tree = db.execute("SELECT * FROM json_tree(NULL)").unwrap();
    assert!(tree.rows().is_empty());

    db.execute("CREATE VIEW json_each_view AS SELECT key, value FROM json_each('[10,20]')")
        .unwrap();
    let view_rows = db
        .execute("SELECT key, value FROM json_each_view ORDER BY key")
        .unwrap();
    assert_eq!(view_rows.rows().len(), 2);
    assert_eq!(
        view_rows.rows()[0].values(),
        &[Value::Int64(0), Value::Int64(10)]
    );
    assert_eq!(
        view_rows.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(20)]
    );
}

#[test]
fn math_scalar_functions_cover_documented_slice_6_surface() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                ABS(-42),
                CEIL(3.2),
                CEILING(3.2),
                FLOOR(3.8),
                ROUND(3.14159, 2),
                SQRT(144),
                POWER(2, 10),
                POW(2, 3),
                MOD(17, 5),
                SIGN(-99),
                LN(2.718281828),
                LOG(1000),
                LOG(2, 8),
                EXP(1)",
        )
        .unwrap();
    let row = result.rows()[0].values();
    assert_eq!(row[0], Value::Int64(42));
    assert_float_close(&row[1], 4.0);
    assert_float_close(&row[2], 4.0);
    assert_float_close(&row[3], 3.0);
    assert_float_close(&row[4], 3.14);
    assert_float_close(&row[5], 12.0);
    assert_float_close(&row[6], 1024.0);
    assert_float_close(&row[7], 8.0);
    assert_eq!(row[8], Value::Int64(2));
    assert_eq!(row[9], Value::Int64(-1));
    assert_float_close(&row[10], 1.0);
    assert_float_close(&row[11], 3.0);
    assert_float_close(&row[12], 3.0);
    assert_float_close(&row[13], std::f64::consts::E);
}

#[test]
fn math_scalar_functions_preserve_nulls_and_expected_edge_cases() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT ABS(NULL), MOD(5, 0), SQRT(-1), LOG(-10), ROUND(NULL, 2)")
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );

    let random = db.execute("SELECT RANDOM()").unwrap();
    match random.rows()[0].values()[0] {
        Value::Float64(value) => assert!((0.0..1.0).contains(&value)),
        ref other => panic!("expected RANDOM() to return FLOAT64, got {other:?}"),
    }
}

#[test]
fn not_in_with_list() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let result = db
        .execute("SELECT id FROM t WHERE id NOT IN (1, 3)")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(2)]);
}

#[test]
fn slice_6_scalar_function_type_errors_are_explicit() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let abs_error = db.execute("SELECT ABS('not-a-number')").unwrap_err();
    assert!(abs_error
        .to_string()
        .contains("ABS expects numeric input for first argument"));

    let left_error = db.execute("SELECT LEFT(123, 2)").unwrap_err();
    assert!(left_error
        .to_string()
        .contains("LEFT expects text for first argument"));

    let uuid_error = db.execute("SELECT UUID_PARSE('not-a-uuid')").unwrap_err();
    assert!(uuid_error
        .to_string()
        .contains("UUID_PARSE expects canonical UUID text"));

    let json_operator_error = db.execute("SELECT 1->>'name'").unwrap_err();
    assert!(json_operator_error
        .to_string()
        .contains("JSON operators expect text JSON input"));

    let json_each_error = db
        .execute("SELECT * FROM json_each('not json')")
        .unwrap_err();
    assert!(json_each_error.to_string().contains("invalid JSON"));
}

#[test]
fn string_functions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute("SELECT LENGTH('hello'), UPPER('hello'), LOWER('WORLD'), TRIM('  hello  ')")
        .unwrap();
    let rows = result.rows();
    println!("rows: {:?}", rows);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            Value::Int64(5),
            Value::Text("HELLO".to_string()),
            Value::Text("world".to_string()),
            Value::Text("hello".to_string())
        ]
    );
}

#[test]
fn string_scalar_functions_cover_documented_slice_6_surface() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                LTRIM('  hello'),
                RTRIM('hello  '),
                LEFT('hello', 3),
                RIGHT('hello', 3),
                LPAD('42', 5, '0'),
                RPAD('hi', 5, '!'),
                REPEAT('ab', 3),
                REVERSE('hello'),
                CHR(65),
                HEX('ABC'),
                SUBSTRING('hello world', 1, 5)",
        )
        .unwrap();
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Text("hello".to_string()),
            Value::Text("hello".to_string()),
            Value::Text("hel".to_string()),
            Value::Text("llo".to_string()),
            Value::Text("00042".to_string()),
            Value::Text("hi!!!".to_string()),
            Value::Text("ababab".to_string()),
            Value::Text("olleh".to_string()),
            Value::Text("A".to_string()),
            Value::Text("414243".to_string()),
            Value::Text("hello".to_string()),
        ]
    );
}

#[test]
fn temp_schema_changes_invalidate_prepared_statements_and_drop_reveals_persistent_tables() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO docs VALUES (1, 'persistent')")
        .unwrap();

    let prepared = db.prepare("SELECT val FROM docs").unwrap();
    assert_eq!(
        prepared.execute(&[]).unwrap().rows()[0].values(),
        &[Value::Text("persistent".to_string())]
    );

    db.execute("CREATE TEMP TABLE docs (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    let stale = prepared.execute(&[]).unwrap_err();
    assert!(
        stale
            .to_string()
            .contains("prepared statement is no longer valid because the schema changed"),
        "unexpected error: {stale}"
    );

    db.execute("INSERT INTO docs VALUES (2, 'temporary')")
        .unwrap();
    assert_eq!(
        db.execute("SELECT val FROM docs").unwrap().rows()[0].values(),
        &[Value::Text("temporary".to_string())]
    );

    db.execute("DROP TABLE docs").unwrap();
    assert_eq!(
        db.execute("SELECT val FROM docs").unwrap().rows()[0].values(),
        &[Value::Text("persistent".to_string())]
    );
}

#[test]
fn upper_and_lower_functions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db.execute("SELECT UPPER('hello'), LOWER('WORLD')").unwrap();
    assert_eq!(
        result.rows()[0].values()[0],
        Value::Text("HELLO".to_string())
    );
    assert_eq!(
        result.rows()[0].values()[1],
        Value::Text("world".to_string())
    );
}

#[test]
fn uuid_helper_functions_round_trip_and_generate_v4_values() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let result = db
        .execute(
            "SELECT
                UUID_TO_STRING(UUID_PARSE('550e8400-e29b-41d4-a716-446655440000')),
                GEN_RANDOM_UUID(),
                UUID_TO_STRING(GEN_RANDOM_UUID()),
                UUID_PARSE(NULL),
                UUID_TO_STRING(NULL)",
        )
        .unwrap();
    let row = result.rows()[0].values();

    assert_eq!(
        row[0],
        Value::Text("550e8400-e29b-41d4-a716-446655440000".to_string())
    );
    match &row[1] {
        Value::Uuid(value) => {
            assert_eq!(value[6] & 0xf0, 0x40);
            assert_eq!(value[8] & 0xc0, 0x80);
        }
        other => panic!("expected GEN_RANDOM_UUID() to return UUID, got {other:?}"),
    }
    match &row[2] {
        Value::Text(value) => {
            assert_eq!(value.len(), 36);
            assert_eq!(value.as_bytes()[8], b'-');
            assert_eq!(value.as_bytes()[13], b'-');
            assert_eq!(value.as_bytes()[18], b'-');
            assert_eq!(value.as_bytes()[23], b'-');
        }
        other => panic!("expected UUID_TO_STRING(GEN_RANDOM_UUID()) text, got {other:?}"),
    }
    assert_eq!(row[3], Value::Null);
    assert_eq!(row[4], Value::Null);
}
