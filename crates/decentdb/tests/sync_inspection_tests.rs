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

#[test]
fn sync_sql_inspection_views_expose_scopes_and_bindings() {
    let dir = tempfile::TempDir::with_prefix("decentdb-sync-sql-scopes").unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE tenant_items (tenant_id INT64, id INT64, value TEXT, PRIMARY KEY (tenant_id, id))")
        .unwrap();
    db.sync_init_replica("node-a").unwrap();
    db.sync_create_scope("tenant_1", &["tenant_items"], Some("tenant_id = 1"))
        .unwrap();
    db.sync_add_peer("relay", "https://relay.example.com", None)
        .unwrap();
    db.sync_bind_peer_scope("relay", "tenant_1").unwrap();

    let scopes = db
        .execute("SELECT * FROM sys_sync_scopes ORDER BY name")
        .unwrap();
    assert_eq!(
        scopes.columns(),
        &[
            "name",
            "include_tables_json",
            "row_filter",
            "filter_columns_json",
            "created_at_micros",
            "updated_at_micros",
        ]
    );
    assert_eq!(scopes.rows().len(), 1);
    assert_eq!(
        scopes.rows()[0].values()[0],
        Value::Text("tenant_1".to_string())
    );

    let scope_tables = db
        .execute("SELECT * FROM sys_sync_scope_tables ORDER BY scope_name, table_name")
        .unwrap();
    assert_eq!(scope_tables.columns(), &["scope_name", "table_name"]);
    assert_eq!(scope_tables.rows().len(), 1);
    assert_eq!(
        scope_tables.rows()[0].values(),
        &[
            Value::Text("tenant_1".to_string()),
            Value::Text("tenant_items".to_string()),
        ]
    );

    let peer_scopes = db
        .execute("SELECT * FROM sys_sync_peer_scopes ORDER BY peer_name")
        .unwrap();
    assert_eq!(
        peer_scopes.columns(),
        &[
            "peer_name",
            "scope_name",
            "created_at_micros",
            "updated_at_micros",
        ]
    );
    assert_eq!(peer_scopes.rows().len(), 1);
    assert_eq!(
        peer_scopes.rows()[0].values()[0],
        Value::Text("relay".to_string())
    );
    assert_eq!(
        peer_scopes.rows()[0].values()[1],
        Value::Text("tenant_1".to_string())
    );
}

#[test]
fn sys_views_expose_operational_metrics() {
    let dir = tempfile::TempDir::with_prefix("decentdb-sys-metrics").unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();

    let legacy_status = db.execute("SELECT * FROM sys_sync_status").unwrap();
    assert_eq!(
        legacy_status.columns(),
        &[
            "enabled",
            "replica_id",
            "next_sequence",
            "journal_path",
            "journal_size_bytes"
        ]
    );
    assert_eq!(legacy_status.rows().len(), 1);
    assert_eq!(legacy_status.rows()[0].values()[0], Value::Bool(false));

    let canonical_status = db.execute("SELECT * FROM sys.sync_status").unwrap();
    assert_eq!(canonical_status.columns(), legacy_status.columns());
    assert_eq!(canonical_status.rows(), legacy_status.rows());

    db.sync_init_replica("node-a").unwrap();
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")
        .unwrap();

    let status = db.execute("SELECT * FROM sys.sync_status").unwrap();
    assert_eq!(status.rows().len(), 1);
    assert_eq!(status.rows()[0].values()[0], Value::Bool(true));
    assert_eq!(
        status.rows()[0].values()[1],
        Value::Text("node-a".to_string())
    );

    let queue = db.write_queue_metrics();
    let queue_view = db.execute("SELECT * FROM sys.write_queue_metrics").unwrap();
    assert_eq!(
        queue_view.columns(),
        &[
            "capacity",
            "current_depth",
            "admitted",
            "rejected",
            "timed_out",
            "canceled",
            "executed",
            "committed",
            "failed",
            "group_commit_batches",
            "group_commit_syncs",
            "group_commit_max_batch",
            "group_commit_commits_covered",
            "physical_syncs_saved",
            "total_queue_wait_ns",
        ]
    );
    assert_eq!(queue_view.rows().len(), 1);
    assert_eq!(
        queue_view.rows()[0].values()[0],
        Value::Int64(queue.capacity as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[1],
        Value::Int64(queue.current_depth as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[2],
        Value::Int64(queue.admitted as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[3],
        Value::Int64(queue.rejected as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[4],
        Value::Int64(queue.timed_out as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[5],
        Value::Int64(queue.canceled as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[6],
        Value::Int64(queue.executed as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[7],
        Value::Int64(queue.committed as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[8],
        Value::Int64(queue.failed as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[9],
        Value::Int64(queue.group_commit_batches as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[10],
        Value::Int64(queue.group_commit_syncs as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[11],
        Value::Int64(queue.group_commit_max_batch as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[12],
        Value::Int64(queue.group_commit_commits_covered as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[13],
        Value::Int64(queue.physical_syncs_saved as i64)
    );
    assert_eq!(
        queue_view.rows()[0].values()[14],
        Value::Int64(queue.total_queue_wait_ns as i64)
    );

    let storage = db.storage_info().unwrap();
    let wal_view = db.execute("SELECT * FROM sys.wal_metrics").unwrap();
    assert_eq!(
        wal_view.columns(),
        &[
            "latest_lsn",
            "file_size_bytes",
            "active_readers",
            "max_page_count",
            "checkpoint_epoch",
            "warning_count",
            "version_count",
            "resident_versions",
            "on_disk_versions",
            "shared_wal",
        ]
    );
    assert_eq!(wal_view.rows().len(), 1);
    assert_eq!(
        wal_view.rows()[0].values()[0],
        Value::Int64(storage.wal_end_lsn as i64)
    );
    assert_eq!(
        wal_view.rows()[0].values()[1],
        Value::Int64(storage.wal_file_size as i64)
    );
    assert_eq!(
        wal_view.rows()[0].values()[2],
        Value::Int64(storage.active_readers as i64)
    );
    assert_eq!(
        wal_view.rows()[0].values()[5],
        Value::Int64(storage.warning_count as i64)
    );
    assert_eq!(
        wal_view.rows()[0].values()[6],
        Value::Int64(storage.wal_versions as i64)
    );
    assert_eq!(
        wal_view.rows()[0].values()[9],
        Value::Bool(storage.shared_wal)
    );

    let storage_view = db.execute("SELECT * FROM sys.storage_metrics").unwrap();
    assert_eq!(
        storage_view.columns(),
        &[
            "path",
            "wal_path",
            "format_version",
            "page_size",
            "cache_size_mb",
            "page_count",
            "schema_cookie",
            "wal_end_lsn",
            "wal_file_size",
            "last_checkpoint_lsn",
            "active_readers",
            "wal_versions",
            "warning_count",
            "shared_wal",
        ]
    );
    assert_eq!(storage_view.rows().len(), 1);
    assert_eq!(
        storage_view.rows()[0].values()[0],
        Value::Text(db.path().to_string_lossy().to_string())
    );
    assert_eq!(
        storage_view.rows()[0].values()[2],
        Value::Int64(storage.format_version as i64)
    );
    assert_eq!(
        storage_view.rows()[0].values()[3],
        Value::Int64(storage.page_size as i64)
    );
    assert_eq!(
        storage_view.rows()[0].values()[4],
        Value::Int64(storage.cache_size_mb as i64)
    );
    assert_eq!(
        storage_view.rows()[0].values()[5],
        Value::Int64(storage.page_count as i64)
    );
    assert_eq!(
        storage_view.rows()[0].values()[12],
        Value::Int64(storage.warning_count as i64)
    );
    assert_eq!(
        storage_view.rows()[0].values()[13],
        Value::Bool(storage.shared_wal)
    );
}
