//! Unit tests for case-insensitive SQL identifier resolution.
//!
//! Exercises: catalog/schema.rs identifiers_equal, CatalogState lookups
//! (table, index, view, trigger with case-insensitive matching).

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
fn table_name_case_insensitive_create_and_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE MyTable (id INT64, val TEXT)");
    exec(&db, "INSERT INTO MYTABLE VALUES (1, 'hello')");
    let r = exec(&db, "SELECT val FROM mytable WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".to_string()));
}

#[test]
fn column_name_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (Id INT64, ValName TEXT)");
    exec(&db, "INSERT INTO t (ID, VALNAME) VALUES (1, 'test')");
    let r = exec(&db, "SELECT valname FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("test".to_string()));
}

#[test]
fn index_name_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64, name TEXT)");
    exec(&db, "CREATE INDEX MyIndex ON t(name)");
    exec(&db, "INSERT INTO t VALUES (1, 'alice')");
    let r = exec(&db, "SELECT name FROM t WHERE name = 'alice'");
    assert_eq!(r.rows()[0].values()[0], Value::Text("alice".to_string()));
}

#[test]
fn duplicate_table_name_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE users (id INT64)");
    let err = exec_err(&db, "CREATE TABLE USERS (id INT64)");
    assert!(err.to_lowercase().contains("already exists"));
}

#[test]
fn duplicate_index_name_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64, name TEXT)");
    exec(&db, "CREATE INDEX idx_name ON t(name)");
    let err = exec_err(&db, "CREATE INDEX IDX_NAME ON t(id)");
    assert!(err.to_lowercase().contains("already exists"));
}

#[test]
fn drop_table_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE Foo (id INT64)");
    exec(&db, "DROP TABLE foo");
    let err = exec_err(&db, "SELECT * FROM FOO");
    assert!(
        !err.is_empty(),
        "expected an error querying a dropped table"
    );
}

#[test]
fn view_name_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64, val TEXT)");
    exec(&db, "CREATE VIEW MyView AS SELECT * FROM t");
    let r = exec(&db, "SELECT * FROM myview");
    assert_eq!(r.rows().len(), 0);
}

#[test]
fn duplicate_view_name_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64)");
    exec(&db, "CREATE VIEW v1 AS SELECT * FROM t");
    let err = exec_err(&db, "CREATE VIEW V1 AS SELECT * FROM t");
    assert!(err.to_lowercase().contains("already exists"));
}

#[test]
fn alter_table_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE MyTable (id INT64)");
    exec(&db, "ALTER TABLE MYTABLE ADD COLUMN name TEXT");
    exec(&db, "INSERT INTO mytable VALUES (1, 'alice')");
    let r = exec(&db, "SELECT name FROM MyTable WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("alice".to_string()));
}

#[test]
fn where_clause_column_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (Id INT64, Value TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello')");
    let r = exec(&db, "SELECT value FROM t WHERE VALUE = 'hello'");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn order_by_column_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (Id INT64, Val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 30)");
    exec(&db, "INSERT INTO t VALUES (2, 10)");
    exec(&db, "INSERT INTO t VALUES (3, 20)");
    let r = exec(&db, "SELECT id FROM t ORDER BY val");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));
    assert_eq!(r.rows()[2].values()[0], Value::Int64(1));
}

#[test]
fn group_by_column_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64, Category TEXT, val INT64)");
    exec(&db, "INSERT INTO t VALUES (1, 'a', 10)");
    exec(&db, "INSERT INTO t VALUES (2, 'a', 20)");
    exec(&db, "INSERT INTO t VALUES (3, 'b', 30)");
    let r = exec(
        &db,
        "SELECT category, SUM(val) FROM t GROUP BY CATEGORY ORDER BY category",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn join_column_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE a (Id INT64, Name TEXT)");
    exec(&db, "CREATE TABLE b (Id INT64, Aid INT64, Value TEXT)");
    exec(&db, "INSERT INTO a VALUES (1, 'alice')");
    exec(&db, "INSERT INTO b VALUES (10, 1, 'hello')");
    let r = exec(&db, "SELECT a.name, b.value FROM a JOIN b ON a.id = b.aid");
    assert_eq!(r.rows()[0].values()[0], Value::Text("alice".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Text("hello".to_string()));
}

#[test]
fn drop_table_if_exists_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE Foo (id INT64)");
    exec(&db, "DROP TABLE IF EXISTS foo");
    let err = exec_err(&db, "SELECT * FROM FOO");
    assert!(
        !err.is_empty(),
        "expected an error querying a dropped table"
    );
}

#[test]
fn drop_index_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64, name TEXT)");
    exec(&db, "CREATE INDEX MyIdx ON t(name)");
    exec(&db, "DROP INDEX myidx");
}

#[test]
fn drop_view_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT64)");
    exec(&db, "CREATE VIEW MyView AS SELECT * FROM t");
    exec(&db, "DROP VIEW myview");
}

#[test]
fn metadata_list_tables_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE MyTable (id INT64)");
    let tables = db.list_tables().unwrap();
    assert_eq!(tables.len(), 1);
    assert!(tables[0].name.eq_ignore_ascii_case("MyTable"));
}

#[test]
fn describe_table_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE MyTable (id INT64, name TEXT)");
    let info = db.describe_table("mytable").unwrap();
    assert_eq!(info.columns.len(), 2);
}

#[test]
fn drop_view_if_exists_no_error_when_missing() {
    let db = mem_db();
    exec(&db, "DROP VIEW IF EXISTS nonexistent_view");
}

#[test]
fn drop_index_if_exists_no_error_when_missing() {
    let db = mem_db();
    exec(&db, "DROP INDEX IF EXISTS nonexistent_index");
}
