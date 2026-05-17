use decentdb::{Db, DbConfig, Value};

#[test]
fn sync_sql_inspection_views_expose_status_and_journal() {
    let dir = tempfile::TempDir::with_prefix("decentdb-sync-sql-inspection").unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::create(&path, DbConfig::default()).unwrap();

    let disabled = db.execute("SELECT * FROM sys_sync_status").unwrap();
    assert_eq!(
        disabled.columns(),
        &[
            "enabled",
            "replica_id",
            "next_sequence",
            "journal_path",
            "journal_size_bytes"
        ]
    );
    assert_eq!(disabled.rows().len(), 1);
    assert_eq!(disabled.rows()[0].values()[0], Value::Bool(false));

    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.sync_init_replica("node-a").unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .unwrap();
    db.execute("UPDATE users SET name = 'Ada Lovelace' WHERE id = 1")
        .unwrap();

    let status = db.execute("SELECT * FROM sys_sync_status").unwrap();
    assert_eq!(status.rows()[0].values()[0], Value::Bool(true));
    assert_eq!(
        status.rows()[0].values()[1],
        Value::Text("node-a".to_string())
    );
    assert_eq!(status.rows()[0].values()[2], Value::Int64(3));

    let journal = db
        .execute("SELECT * FROM sys_sync_journal ORDER BY sequence")
        .unwrap();
    assert_eq!(
        journal.columns(),
        &[
            "sequence",
            "replica_id",
            "transaction_lsn",
            "table_name",
            "operation",
            "primary_key_json",
            "after_json",
            "schema_cookie",
            "committed_at_micros"
        ]
    );
    assert_eq!(journal.rows().len(), 2);
    assert_eq!(journal.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(
        journal.rows()[0].values()[3],
        Value::Text("users".to_string())
    );
    assert_eq!(
        journal.rows()[0].values()[4],
        Value::Text("insert".to_string())
    );
    assert_eq!(journal.rows()[1].values()[0], Value::Int64(2));
    assert_eq!(
        journal.rows()[1].values()[4],
        Value::Text("update".to_string())
    );

    let from_sequence_1 = db
        .execute("SELECT * FROM sys_sync_journal WHERE sequence > 1 ORDER BY sequence ASC")
        .unwrap();
    assert_eq!(from_sequence_1.rows().len(), 1);
    assert_eq!(from_sequence_1.rows()[0].values()[0], Value::Int64(2));
}
