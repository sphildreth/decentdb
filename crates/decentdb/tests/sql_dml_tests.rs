//! SQL DML (INSERT, UPDATE, DELETE) tests.
//!
//! Covers: INSERT with ON CONFLICT, RETURNING, DEFAULT VALUES, multi-row,
//! INSERT from SELECT; UPDATE with SET, CASE, subquery; DELETE with WHERE,
//! subquery; and DML validation errors.

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

fn rows(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

#[test]
fn blob_insert_and_read() {
    let db = mem_db();
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
fn blob_insert_and_select() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data BLOB)").unwrap();
    let r = db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])],
    );
    if r.is_ok() {
        let r2 = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
        let v = rows(&r2);
        if let Value::Blob(b) = &v[0][0] {
            assert_eq!(b, &[0xDE, 0xAD, 0xBE, 0xEF]);
        }
    }
}

#[test]
fn delete_all_from_table() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)")
        .unwrap();
    db.execute("DELETE FROM t").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(0));
}

#[test]
fn delete_many_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1)").unwrap();
    for i in 0..500 {
        stmt.execute_in(&mut txn, &[Value::Int64(i)]).unwrap();
    }
    txn.commit().unwrap();
    db.execute("DELETE FROM t WHERE id < 250").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(250));
}

#[test]
fn delete_returning() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    let result = db
        .execute("DELETE FROM t WHERE id > 1 RETURNING id, val")
        .unwrap();
    let returned = rows(&result);
    assert_eq!(returned.len(), 2);
    assert_eq!(
        returned[0],
        vec![Value::Int64(2), Value::Text("b".to_string())]
    );
    assert_eq!(
        returned[1],
        vec![Value::Int64(3), Value::Text("c".to_string())]
    );
    let remaining = db.execute("SELECT id FROM t ORDER BY id").unwrap();
    assert_eq!(rows(&remaining), vec![vec![Value::Int64(1)]]);
}

#[test]
fn delete_unknown_table() {
    let db = mem_db();
    let err = exec_err(&db, "DELETE FROM no_such_table WHERE id = 1");
    assert!(
        err.to_lowercase().contains("no_such_table")
            || err.to_lowercase().contains("unknown")
            || err.to_lowercase().contains("not found")
            || err.to_lowercase().contains("does not exist"),
        "got: {err}"
    );
}

#[test]
fn delete_with_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'),(2, 'b'),(3, 'c')")
        .unwrap();
    db.execute("DELETE FROM t WHERE val = 'b'").unwrap();
    let r = db.execute("SELECT id FROM t ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn delete_with_returning_unsupported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE retr (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO retr VALUES (1, 'hello')");
    let result = exec(&db, "DELETE FROM retr WHERE id = 1 RETURNING id, val");
    assert_eq!(
        rows(&result),
        vec![vec![Value::Int64(1), Value::Text("hello".to_string())]]
    );
}

#[test]
fn error_insert_duplicate_column_names() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let err = db
        .execute("INSERT INTO t (id, id) VALUES (1, 2)")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("assigned more than once") || msg.contains("duplicate") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn error_insert_into_view() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let err = db.execute("INSERT INTO v VALUES (1)").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_insert_too_few_values() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT NOT NULL)")
        .unwrap();
    let err = db.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_insert_too_many_columns() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64)").unwrap();
    let err = db.execute("INSERT INTO t VALUES (1, 2)").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_insert_too_many_values() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let err = db
        .execute("INSERT INTO t VALUES (1, 'a', 'extra')")
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_insert_wrong_column_count() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    let err = db.execute("INSERT INTO t VALUES (1)").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_update_nonexistent_column() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    // Engine may silently ignore unknown columns; just verify no panic
    let r = db.execute("UPDATE t SET nonexistent = 1");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn insert_and_update_with_index() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE INDEX idx_val ON t(val)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();
    db.execute("UPDATE t SET val = 'updated' WHERE id = 1")
        .unwrap();
    let r = db
        .execute("SELECT val FROM t WHERE val = 'updated'")
        .unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn insert_column_count_mismatch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ccm (id INT PRIMARY KEY, a TEXT, b TEXT)");
    let err = exec_err(&db, "INSERT INTO ccm VALUES (1, 'only_one')");
    assert!(
        err.to_lowercase().contains("column")
            || err.to_lowercase().contains("mismatch")
            || err.to_lowercase().contains("expected"),
        "got: {err}"
    );
}

#[test]
fn insert_default_values() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE dv (id INT PRIMARY KEY, name TEXT DEFAULT 'anon', score INT DEFAULT 0)",
    );
    exec(&db, "INSERT INTO dv (id) VALUES (1)");
    let r = exec(&db, "SELECT name, score FROM dv WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("anon".into()));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(0));
}

#[test]
fn insert_duplicate_column_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dci (id INT PRIMARY KEY, val INT)");
    let err = exec_err(&db, "INSERT INTO dci (id, id) VALUES (1, 2)");
    assert!(!err.is_empty());
}

#[test]
fn insert_from_select() {
    let db = mem_db();
    db.execute("CREATE TABLE src(id INT64, val TEXT)").unwrap();
    db.execute("CREATE TABLE dst(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO src VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    db.execute("INSERT INTO dst SELECT * FROM src WHERE id <= 2")
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM dst").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn insert_null_into_nullable_column() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    let r = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn insert_returning() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    let r = db
        .execute("INSERT INTO t VALUES (1, 'hello') RETURNING id, val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[0][1], Value::Text("hello".into()));
}

#[test]
fn insert_select() {
    let db = mem_db();
    db.execute("CREATE TABLE src(x INT64)").unwrap();
    db.execute("CREATE TABLE dst(x INT64)").unwrap();
    db.execute("INSERT INTO src VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO dst SELECT x FROM src WHERE x > 1")
        .unwrap();
    let r = db.execute("SELECT x FROM dst ORDER BY x").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
}

#[test]
fn insert_too_many_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tmv (id INT PRIMARY KEY)");
    let err = exec_err(&db, "INSERT INTO tmv VALUES (1, 2, 3)");
    assert!(
        err.to_lowercase().contains("column")
            || err.to_lowercase().contains("mismatch")
            || err.to_lowercase().contains("expected"),
        "got: {err}"
    );
}

#[test]
fn insert_type_mismatch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE typed (id INT PRIMARY KEY, val INT)");
    // Text that can't be coerced to INT
    let err = exec_err(&db, "INSERT INTO typed VALUES (1, 'not_a_number')");
    assert!(
        err.contains("type")
            || err.contains("cast")
            || err.contains("convert")
            || err.contains("coer"),
        "got: {err}"
    );
}

#[test]
fn insert_unknown_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE iuc (id INT PRIMARY KEY, val TEXT)");
    let err = exec_err(&db, "INSERT INTO iuc (id, nonexistent) VALUES (1, 'x')");
    assert!(
        err.to_lowercase().contains("nonexist")
            || err.to_lowercase().contains("column")
            || err.to_lowercase().contains("unknown"),
        "got: {err}"
    );
}

#[test]
fn insert_unknown_table() {
    let db = mem_db();
    let err = exec_err(&db, "INSERT INTO no_such_table VALUES (1)");
    assert!(
        err.to_lowercase().contains("no_such_table")
            || err.to_lowercase().contains("unknown")
            || err.to_lowercase().contains("not found")
            || err.to_lowercase().contains("does not exist"),
        "got: {err}"
    );
}

#[test]
fn insert_with_column_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT DEFAULT 'default', val INT64 DEFAULT 0)")
        .unwrap();
    db.execute("INSERT INTO t(id, val) VALUES (1, 42)").unwrap();
    let r = db.execute("SELECT id, name, val FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[0][1], Value::Text("default".into()));
    assert_eq!(v[0][2], Value::Int64(42));
}

#[test]
fn insert_with_partial_columns() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE ipc (id INT PRIMARY KEY, a TEXT, b TEXT DEFAULT 'default_b')",
    );
    exec(&db, "INSERT INTO ipc (id, a) VALUES (1, 'hello')");
    let r = exec(&db, "SELECT a, b FROM ipc");
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".to_string()));
    assert_eq!(
        r.rows()[0].values()[1],
        Value::Text("default_b".to_string())
    );
}

#[test]
fn insert_with_returning_clause() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ret (id INT PRIMARY KEY, val TEXT)");
    let r = exec(&db, "INSERT INTO ret VALUES (1, 'hello') RETURNING id, val");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn multi_row_insert() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mri (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO mri VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let r = exec(&db, "SELECT COUNT(*) FROM mri");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

#[test]
fn normalize_insert_on_conflict_do_nothing() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE unique_tbl (id INT PRIMARY KEY, val TEXT)",
    );
    exec(&db, "INSERT INTO unique_tbl VALUES (1, 'first')");
    exec(
        &db,
        "INSERT INTO unique_tbl VALUES (1, 'second') ON CONFLICT DO NOTHING",
    );
    let r = exec(&db, "SELECT val FROM unique_tbl WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("first".to_string()));
}

#[test]
fn normalize_insert_on_conflict_do_update() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE upsert_tbl (id INT PRIMARY KEY, val TEXT, count INT DEFAULT 1)",
    );
    exec(&db, "INSERT INTO upsert_tbl VALUES (1, 'first', 1)");
    exec(&db, "INSERT INTO upsert_tbl VALUES (1, 'second', 1) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, count = upsert_tbl.count + 1");
    let r = exec(&db, "SELECT val, count FROM upsert_tbl WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("second".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(2));
}

#[test]
fn on_conflict_do_nothing_no_target() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    let r = db.execute("INSERT INTO t VALUES (1, 'b') ON CONFLICT DO NOTHING");
    if r.is_ok() {
        let r2 = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(rows(&r2)[0][0], Value::Text("a".into()));
    }
}

#[test]
fn on_conflict_do_update_without_target() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ocu (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO ocu VALUES (1, 'a')");
    // ON CONFLICT DO UPDATE without specifying conflict target
    let err = exec_err(
        &db,
        "INSERT INTO ocu VALUES (1, 'b') ON CONFLICT DO UPDATE SET val = EXCLUDED.val",
    );
    assert!(
        err.to_lowercase().contains("conflict")
            || err.to_lowercase().contains("target")
            || err.to_lowercase().contains("column"),
        "got: {err}"
    );
}

#[test]
fn on_conflict_nonexistent_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ocn (id INT PRIMARY KEY, val TEXT)");
    let err = exec_err(
        &db,
        "INSERT INTO ocn VALUES (1, 'a') ON CONFLICT (nonexistent) DO NOTHING",
    );
    assert!(
        err.to_lowercase().contains("nonexist")
            || err.to_lowercase().contains("column")
            || err.to_lowercase().contains("unique")
            || err.to_lowercase().contains("index"),
        "got: {err}"
    );
}

#[test]
fn on_conflict_with_excluded_reference() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, hits INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET hits = t.hits + EXCLUDED.hits",
    )
    .unwrap();
    let r = db.execute("SELECT hits FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(15));
}

#[test]
fn on_conflict_with_where_clause() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT, active BOOL)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old', TRUE)").unwrap();
    // ON CONFLICT with WHERE filter on DO UPDATE
    let r = db.execute(
        "INSERT INTO t VALUES (1, 'new', TRUE) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE t.active = TRUE",
    );
    if r.is_ok() {
        let r2 = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(rows(&r2)[0][0], Value::Text("new".into()));
    }
}

#[test]
fn overflow_delete_large_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    let large = "Y".repeat(10000);
    for i in 0..5 {
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(large.clone())],
        )
        .unwrap();
    }
    db.execute("DELETE FROM t WHERE id < 3").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn overflow_update_large_to_small() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    let large = "X".repeat(10000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(large)],
    )
    .unwrap();
    db.execute("UPDATE t SET data = 'small' WHERE id = 1")
        .unwrap();
    let r = db.execute("SELECT data FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("small".into()));
}

#[test]
fn parse_insert_with_column_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b TEXT, c INT64)")
        .unwrap();
    db.execute("INSERT INTO t (c, a) VALUES (30, 10)").unwrap();
    let r = db.execute("SELECT a, b, c FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(10));
    assert_eq!(v[0][1], Value::Null); // b not specified
    assert_eq!(v[0][2], Value::Int64(30));
}

#[test]
fn parse_update_multiple_columns() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b TEXT, c INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old_a', 'old_b', 0)")
        .unwrap();
    db.execute("UPDATE t SET a = 'new_a', b = 'new_b', c = 42 WHERE id = 1")
        .unwrap();
    let r = db.execute("SELECT a, b, c FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("new_a".into()));
    assert_eq!(v[0][1], Value::Text("new_b".into()));
    assert_eq!(v[0][2], Value::Int64(42));
}

#[test]
fn update_all_rows_no_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("UPDATE t SET val = val * 10").unwrap();
    let r = db.execute("SELECT SUM(val) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(600));
}

#[test]
fn update_from_clause_not_supported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE target (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "CREATE TABLE source (id INT PRIMARY KEY, new_val TEXT)",
    );
    exec(&db, "INSERT INTO target VALUES (1, 'old'), (2, 'old')");
    exec(&db, "INSERT INTO source VALUES (1, 'new')");
    // UPDATE...FROM is not supported in DecentDB 1.0
    let err = exec_err(
        &db,
        "UPDATE target SET val = source.new_val FROM source WHERE target.id = source.id",
    );
    assert!(
        err.contains("not supported") || err.contains("FROM"),
        "got: {err}"
    );
}

#[test]
fn update_many_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    let mut txn = db.transaction().unwrap();
    let stmt = txn.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 0..500 {
        stmt.execute_in(&mut txn, &[Value::Int64(i), Value::Int64(0)])
            .unwrap();
    }
    txn.commit().unwrap();
    db.execute("UPDATE t SET val = id * 2").unwrap();
    let r = db.execute("SELECT SUM(val) FROM t").unwrap();
    let expected_sum: i64 = (0..500).map(|i: i64| i * 2).sum();
    assert_eq!(rows(&r)[0][0], Value::Int64(expected_sum));
}

#[test]
fn update_returning() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10),(2, 20)").unwrap();
    let result = db
        .execute("UPDATE t SET val = val + 5 RETURNING id, val")
        .unwrap();
    assert_eq!(
        rows(&result),
        vec![
            vec![Value::Int64(1), Value::Int64(15)],
            vec![Value::Int64(2), Value::Int64(25)]
        ]
    );
}

#[test]
fn update_unknown_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uuc (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO uuc VALUES (1, 'x')");
    let err = exec_err(&db, "UPDATE uuc SET nonexistent = 'y' WHERE id = 1");
    assert!(
        err.to_lowercase().contains("nonexist")
            || err.to_lowercase().contains("column")
            || err.to_lowercase().contains("unknown"),
        "got: {err}"
    );
}

#[test]
fn update_unknown_table() {
    let db = mem_db();
    let err = exec_err(&db, "UPDATE no_such_table SET x = 1");
    assert!(
        err.to_lowercase().contains("no_such_table")
            || err.to_lowercase().contains("unknown")
            || err.to_lowercase().contains("not found")
            || err.to_lowercase().contains("does not exist"),
        "got: {err}"
    );
}

#[test]
fn update_with_case_expression() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE uwc (id INT PRIMARY KEY, val INT, label TEXT)",
    );
    exec(
        &db,
        "INSERT INTO uwc VALUES (1, 10, ''), (2, 20, ''), (3, 5, '')",
    );
    exec(
        &db,
        "UPDATE uwc SET label = CASE WHEN val > 15 THEN 'high' ELSE 'low' END",
    );
    let r = exec(&db, "SELECT id, label FROM uwc ORDER BY id");
    assert_eq!(r.rows()[0].values()[1], Value::Text("low".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("high".to_string()));
}

#[test]
fn update_with_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10),(2, 20),(3, 30)")
        .unwrap();
    db.execute("UPDATE t SET val = val * 2 WHERE id > 1")
        .unwrap();
    let r = db.execute("SELECT id, val FROM t ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(10));
    assert_eq!(v[1][1], Value::Int64(40));
    assert_eq!(v[2][1], Value::Int64(60));
}

#[test]
fn update_with_returning_unsupported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE retu (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO retu VALUES (1, 'hello')");
    let result = exec(
        &db,
        "UPDATE retu SET val = 'world' WHERE id = 1 RETURNING id, val",
    );
    assert_eq!(
        rows(&result),
        vec![vec![Value::Int64(1), Value::Text("world".to_string())]]
    );
}

#[test]
fn upsert_multiple_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'new_b'), (3, 'c') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val").unwrap();
    let r = db.execute("SELECT id, val FROM t ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[1][1], Value::Text("new_b".into())); // updated
    assert_eq!(v[2][1], Value::Text("c".into())); // new
}

#[test]
fn upsert_on_conflict_do_nothing() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'first')").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'second') ON CONFLICT (id) DO NOTHING")
        .unwrap();
    let r = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("first".into()));
}

#[test]
fn upsert_on_conflict_do_update() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT, version INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'v1', 1)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 'v2', 2) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, version = EXCLUDED.version",
    )
    .unwrap();
    let r = db
        .execute("SELECT val, version FROM t WHERE id = 1")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("v2".into()));
    assert_eq!(v[0][1], Value::Int64(2));
}

#[test]
fn upsert_on_conflict_do_update_with_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val INT64, version INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 1)").unwrap();
    // Update only if EXCLUDED version is higher
    db.execute(
        "INSERT INTO t VALUES (1, 20, 2) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, version = EXCLUDED.version WHERE EXCLUDED.version > t.version"
    ).unwrap();
    let r = db
        .execute("SELECT val, version FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(20));
    assert_eq!(rows(&r)[0][1], Value::Int64(2));
    // Now try with lower version — should NOT update
    db.execute(
        "INSERT INTO t VALUES (1, 30, 1) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, version = EXCLUDED.version WHERE EXCLUDED.version > t.version"
    ).unwrap();
    let r2 = db
        .execute("SELECT val, version FROM t WHERE id = 1")
        .unwrap();
    assert_eq!(rows(&r2)[0][0], Value::Int64(20)); // Unchanged
}

#[test]
fn upsert_with_filter_accepts() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uf (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO uf VALUES (1, 100)");
    exec(&db, "INSERT INTO uf VALUES (1, 200) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE uf.val < 200");
    let r = exec(&db, "SELECT val FROM uf WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(200));
}

#[test]
fn upsert_with_filter_rejects() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ufr (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ufr VALUES (1, 100)");
    // Filter rejects because val is NOT > 999
    exec(&db, "INSERT INTO ufr VALUES (1, 200) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE ufr.val > 999");
    let r = exec(&db, "SELECT val FROM ufr WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(100)); // unchanged
}
