use decentdb::{Db, DbConfig, Value};

#[test]
fn commit_persists_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("COMMIT").unwrap();
    
    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn rollback_discards_changes() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("ROLLBACK").unwrap();
    
    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn is_null_operator() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
    
    let result = db.execute("SELECT id FROM t WHERE val IS NULL").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(1)]);
}

#[test]
fn is_not_null_operator() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'x')").unwrap();
    
    let result = db.execute("SELECT id FROM t WHERE val IS NOT NULL").unwrap();
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[Value::Int64(2)]);
}
