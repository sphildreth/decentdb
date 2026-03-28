//! Unit tests for error code taxonomy and error classification.
//!
//! Exercises: error.rs DbError variants and DbErrorCode classification.

use decentdb::{Db, DbConfig, DbError, DbErrorCode};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn err_code(db: &Db, sql: &str) -> DbErrorCode {
    match db.execute(sql) {
        Ok(_) => panic!("expected error for: {sql}"),
        Err(e) => e.code(),
    }
}

#[test]
fn syntax_error_is_sql_error() {
    let db = mem_db();
    assert_eq!(err_code(&db, "INVALID SQL !!@#"), DbErrorCode::Sql);
}

#[test]
fn select_from_nonexistent_table_is_error() {
    let db = mem_db();
    let code = err_code(&db, "SELECT * FROM does_not_exist");
    assert!(
        code == DbErrorCode::Sql || code == DbErrorCode::Internal,
        "expected Sql or Internal, got {code:?}"
    );
}

#[test]
fn duplicate_table_creation_is_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db.execute("CREATE TABLE t(id INT64)").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("already exists"));
}

#[test]
fn unique_constraint_violation_is_constraint_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let err = db.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert_eq!(err.code(), DbErrorCode::Constraint);
}

#[test]
fn not_null_violation_is_constraint_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64 NOT NULL)")
        .unwrap();
    let err = db.execute("INSERT INTO t(id) VALUES (1)").unwrap_err();
    assert_eq!(err.code(), DbErrorCode::Constraint);
}

#[test]
fn check_constraint_violation_is_constraint_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64 CHECK(val > 0))")
        .unwrap();
    let err = db.execute("INSERT INTO t VALUES (1, -5)").unwrap_err();
    assert_eq!(err.code(), DbErrorCode::Constraint);
}

#[test]
fn foreign_key_violation_is_constraint_error() {
    let db = mem_db();
    db.execute("CREATE TABLE parent(id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE child(id INT64, parent_id INT64 REFERENCES parent(id))")
        .unwrap();
    let err = db.execute("INSERT INTO child VALUES (1, 999)").unwrap_err();
    assert_eq!(err.code(), DbErrorCode::Constraint);
}

#[test]
fn duplicate_key_in_unique_index_is_constraint_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("CREATE UNIQUE INDEX idx ON t(name)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
    let err = db.execute("INSERT INTO t VALUES (2, 'alice')").unwrap_err();
    assert_eq!(err.code(), DbErrorCode::Constraint);
}

#[test]
fn type_mismatch_in_insert_is_sql_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val BOOL)").unwrap();
    let err = db
        .execute("INSERT INTO t VALUES (1, 'not_a_bool')")
        .unwrap_err();
    assert!(matches!(
        err.code(),
        DbErrorCode::Sql | DbErrorCode::Constraint
    ));
}

#[test]
fn error_codes_are_stable() {
    assert_eq!(DbErrorCode::Io.as_u32(), 1);
    assert_eq!(DbErrorCode::Corruption.as_u32(), 2);
    assert_eq!(DbErrorCode::Constraint.as_u32(), 3);
    assert_eq!(DbErrorCode::Transaction.as_u32(), 4);
    assert_eq!(DbErrorCode::Sql.as_u32(), 5);
    assert_eq!(DbErrorCode::Internal.as_u32(), 6);
    assert_eq!(DbErrorCode::Panic.as_u32(), 7);
}

#[test]
fn numeric_code_matches_code() {
    let errors = vec![
        DbError::io("ctx", std::io::Error::other("x")),
        DbError::corruption("msg"),
        DbError::constraint("msg"),
        DbError::transaction("msg"),
        DbError::sql("msg"),
        DbError::internal("msg"),
        DbError::panic("msg"),
    ];
    for error in errors {
        assert_eq!(error.numeric_code(), error.code().as_u32());
    }
}

#[test]
fn error_display_includes_message() {
    let err = DbError::corruption("test corruption detail");
    let msg = err.to_string();
    assert!(msg.contains("corruption"));
    assert!(msg.contains("test corruption detail"));
}

#[test]
fn error_display_sql_includes_context() {
    let err = DbError::sql("unexpected token near 'WHERE'");
    assert!(err.to_string().contains("WHERE"));
}

#[test]
fn error_display_constraint_includes_detail() {
    let err = DbError::constraint("NOT NULL violation on column 'name'");
    assert!(err.to_string().contains("NOT NULL"));
    assert!(err.to_string().contains("name"));
}

#[test]
fn drop_nonexistent_table_is_error() {
    let db = mem_db();
    let err = db.execute("DROP TABLE nonexistent").unwrap_err();
    assert!(matches!(
        err.code(),
        DbErrorCode::Sql | DbErrorCode::Internal
    ));
}

#[test]
fn drop_nonexistent_table_if_exists_no_error() {
    let db = mem_db();
    db.execute("DROP TABLE IF EXISTS nonexistent").unwrap();
}

#[test]
fn insert_wrong_column_count_is_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64, c INT64)")
        .unwrap();
    let err = db.execute("INSERT INTO t VALUES (1, 2)").unwrap_err();
    assert!(matches!(
        err.code(),
        DbErrorCode::Sql | DbErrorCode::Constraint
    ));
}
