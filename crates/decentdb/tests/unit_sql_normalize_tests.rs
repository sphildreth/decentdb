//! Unit tests for SQL normalization edge cases.
//!
//! Exercises: sql/normalize.rs (normalize_statement_text), which transforms
//! libpg_query parse trees into DecentDB AST nodes, covering various statement types.

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
fn normalize_select_with_whitespace() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64)");
    exec(&db, "  SELECT\n  id\n  FROM\n  t");
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
}

#[test]
fn normalize_mixed_case_keywords() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(Id INT64, Val TEXT)");
    exec(&db, "insert into T (ID, VAL) VALUES (1, 'hello')");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".to_string()));
}

#[test]
fn normalize_insert_with_expressions() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 2 + 3 * 4)");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(14));
}

#[test]
fn normalize_update_with_arithmetic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "UPDATE t SET val = val * 2 + 5 WHERE id = 1");
    let r = exec(&db, "SELECT val FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(25));
}

#[test]
fn normalize_delete_with_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    exec(&db, "DELETE FROM t WHERE val > (SELECT AVG(val) FROM t)");
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}
#[test]
fn normalize_create_with_constraints() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE t(id INT64 PRIMARY KEY, val INT64 NOT NULL CHECK(val > 0))",
    );
    exec(&db, "INSERT INTO t VALUES (1, 42)");
    let err = exec_err(&db, "INSERT INTO t VALUES (2, -1)");
    assert!(err.to_lowercase().contains("constraint") || err.to_lowercase().contains("check"));
}

#[test]
fn normalize_create_index_with_include() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, name TEXT, age INT64)");
    exec(&db, "CREATE INDEX idx ON t(name) INCLUDE (age)");
    exec(&db, "INSERT INTO t VALUES (1, 'alice', 30)");
    let r = exec(&db, "SELECT age FROM t WHERE name = 'alice'");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(30));
}

#[test]
fn normalize_cte_with_recursive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64)");
    exec(&db, "INSERT INTO t VALUES (1), (2), (3)");
    let r = exec(
        &db,
        "WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x < 5) SELECT * FROM cte ORDER BY x",
    );
    assert_eq!(r.rows().len(), 5);
}

#[test]
fn normalize_nested_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "SELECT * FROM t WHERE val = (SELECT MAX(val) FROM t)");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

#[test]
fn normalize_join_with_on_clause() {
    let db = mem_db();
    exec(&db, "CREATE TABLE a(id INT64, name TEXT)");
    exec(&db, "CREATE TABLE b(aid INT64, val INT64)");
    exec(&db, "INSERT INTO a VALUES (1, 'x')");
    exec(&db, "INSERT INTO b VALUES (1, 10)");
    let r = exec(
        &db,
        "SELECT a.name, b.val FROM a JOIN b ON a.id = b.aid WHERE b.val > 5",
    );
    assert_eq!(r.rows()[0].values()[0], Value::Text("x".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(10));
}

#[test]
fn normalize_aggregate_with_having() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(cat TEXT, val INT64)");
    exec(
        &db,
        "INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 10), ('b', 20)",
    );
    let r = exec(
        &db,
        "SELECT cat, SUM(val) FROM t GROUP BY cat HAVING SUM(val) > 5 ORDER BY cat",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Text("b".to_string()));
}

#[test]
fn normalize_case_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "SELECT CASE WHEN val < 15 THEN 'small' WHEN val < 25 THEN 'medium' ELSE 'large' END FROM t ORDER BY id",
    );
    assert_eq!(r.rows()[0].values()[0], Value::Text("small".to_string()));
    assert_eq!(r.rows()[1].values()[0], Value::Text("medium".to_string()));
    assert_eq!(r.rows()[2].values()[0], Value::Text("large".to_string()));
}

#[test]
fn normalize_in_list_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "SELECT id FROM t WHERE val IN (10, 30) ORDER BY id");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));
}

#[test]
fn normalize_between_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)");
    let r = exec(&db, "SELECT id FROM t WHERE val BETWEEN 10 AND 20");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn normalize_like_pattern() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, name TEXT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'alexander')",
    );
    let r = exec(&db, "SELECT id FROM t WHERE name LIKE 'al%' ORDER BY id");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));
}

#[test]
fn normalize_is_null_is_not_null() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, 10)");
    let r_null = exec(&db, "SELECT id FROM t WHERE val IS NULL");
    assert_eq!(r_null.rows()[0].values()[0], Value::Int64(1));
    let r_not_null = exec(&db, "SELECT id FROM t WHERE val IS NOT NULL");
    assert_eq!(r_not_null.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn normalize_coalesce_nullif() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, 10)");
    let r = exec(&db, "SELECT COALESCE(val, 0) FROM t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(10));
    let r2 = exec(&db, "SELECT NULLIF(val, 10) FROM t ORDER BY id");
    assert_eq!(r2.rows()[0].values()[0], Value::Null);
    assert_eq!(r2.rows()[1].values()[0], Value::Null);
}

#[test]
fn normalize_cast_expressions() {
    let db = mem_db();
    let r = exec(
        &db,
        "SELECT CAST('42' AS INT64), CAST(42 AS FLOAT64), CAST(42 AS TEXT)",
    );
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
    assert!(matches!(r.rows()[0].values()[1], Value::Float64(_)));
    assert_eq!(r.rows()[0].values()[2], Value::Text("42".to_string()));
}

#[test]
fn normalize_boolean_expression_in_where() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = exec(
        &db,
        "SELECT id FROM t WHERE (val > 5 AND val < 25) OR val = 100 ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn normalize_unary_minus() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = exec(&db, "SELECT -val FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(-10));
}

#[test]
fn normalize_modulo() {
    let db = mem_db();
    let r = exec(&db, "SELECT 10 % 3, 10 % 5");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(0));
}

/// Missing feature: INSERT INTO t DEFAULT VALUES is not supported.
/// The engine returns "INSERT is missing its source rows" instead of
/// inserting a row with default values for all columns.
#[test]
#[ignore = "missing feature: INSERT DEFAULT VALUES not supported"]
fn normalize_insert_default_values() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE t(id INT64 DEFAULT 42, val TEXT DEFAULT 'hello')",
    );
    exec(&db, "INSERT INTO t DEFAULT VALUES");
    let r = exec(&db, "SELECT id, val FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
    assert_eq!(r.rows()[0].values()[1], Value::Text("hello".to_string()));
}

#[test]
fn normalize_create_table_as_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE src(id INT64, val TEXT)");
    exec(&db, "INSERT INTO src VALUES (1, 'a'), (2, 'b')");
    exec(&db, "CREATE TABLE dst AS SELECT * FROM src");
    let r = exec(&db, "SELECT COUNT(*) FROM dst");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn normalize_insert_from_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE src(id INT64, val INT64)");
    exec(&db, "CREATE TABLE dst(id INT64, val INT64)");
    exec(&db, "INSERT INTO src VALUES (1, 10), (2, 20)");
    exec(&db, "INSERT INTO dst SELECT * FROM src WHERE val > 15");
    let r = exec(&db, "SELECT COUNT(*) FROM dst");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn normalize_explain_statement() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64)");
    exec(&db, "INSERT INTO t VALUES (1)");
    let r = exec(&db, "EXPLAIN SELECT * FROM t");
    assert!(!r.rows().is_empty());
}
