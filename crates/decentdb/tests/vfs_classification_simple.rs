use decentdb::{Db, DbConfig};

#[test]
fn test_basic_db_operations() {
    // Basic test to ensure the testing framework works
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .expect("create table");
    db.execute("INSERT INTO test VALUES (1, 'hello')")
        .expect("insert");

    let result = db.execute("SELECT * FROM test").expect("select");
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values(),
        &[
            decentdb::Value::Int64(1),
            decentdb::Value::Text("hello".to_string())
        ]
    );
}

#[test]
fn test_transaction_commit_rollback() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        .expect("create table");

    // Test commit
    db.execute("BEGIN").expect("begin");
    db.execute("INSERT INTO test VALUES (1, 'committed')")
        .expect("insert");
    db.execute("COMMIT").expect("commit");

    let result = db.execute("SELECT COUNT(*) FROM test").expect("count");
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[decentdb::Value::Int64(1)]);

    // Test rollback
    db.execute("BEGIN").expect("begin");
    db.execute("INSERT INTO test VALUES (2, 'rolled_back')")
        .expect("insert");
    db.execute("ROLLBACK").expect("rollback");

    let result = db.execute("SELECT COUNT(*) FROM test").expect("count");
    let rows = result.rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), &[decentdb::Value::Int64(1)]); // Still only 1 row
}
