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

const PREPARED_SYS_VIEW_ASSERTIONS: [(&str, &[&str]); 14] = [
    (
        "SELECT * FROM sys.wal_metrics",
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
        ],
    ),
    (
        "SELECT * FROM sys.storage_metrics",
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
        ],
    ),
    (
        "SELECT * FROM sys.write_queue_metrics",
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
        ],
    ),
    (
        "SELECT * FROM sys.sync_status",
        &[
            "enabled",
            "replica_id",
            "next_sequence",
            "journal_path",
            "journal_size_bytes",
        ],
    ),
    (
        "SELECT * FROM sys.sync_retention",
        &[
            "journal_records",
            "first_sequence",
            "last_sequence",
            "safe_prune_through",
            "prunable_records",
            "blocked_by_json",
            "journal_size_bytes",
        ],
    ),
    (
        "SELECT * FROM sys.sync_peer_lag",
        &[
            "peer_name",
            "remote_replica_id",
            "in_watermark",
            "out_watermark",
            "local_high_watermark",
            "in_lag",
            "out_lag",
        ],
    ),
    (
        "SELECT * FROM sys.sync_relay_status",
        &[
            "relay_id",
            "protocol_version",
            "database_replica_id",
            "production_mode",
            "secure_transport_required",
            "insecure_override_enabled",
            "active_sessions",
            "active_streams",
            "started_at_micros",
        ],
    ),
    (
        "SELECT * FROM sys.reactive_metrics",
        &[
            "active_watch_count",
            "table_watch_count",
            "range_watch_count",
            "query_watch_count",
            "change_stream_count",
            "events_published",
            "events_delivered",
            "events_dropped",
            "lagged_watch_count",
            "row_change_events_truncated",
        ],
    ),
    (
        "SELECT * FROM sys.reactive_subscriptions",
        &[
            "watch_id",
            "kind",
            "created_at_micros",
            "queue_capacity",
            "queue_depth",
            "last_delivered_event_id",
            "dropped_events",
            "lagged",
            "dependencies_json",
        ],
    ),
    (
        "SELECT * FROM sys.extensions",
        &[
            "name",
            "version",
            "content_hash",
            "enabled",
            "installed_at_micros",
        ],
    ),
    (
        "SELECT * FROM sys.extension_functions",
        &[
            "extension_name",
            "content_hash",
            "function_name",
            "export",
            "kind",
            "args",
            "returns",
            "deterministic",
            "null_handling",
        ],
    ),
    (
        "SELECT * FROM sys.extension_collations",
        &[
            "extension_name",
            "content_hash",
            "collation_name",
            "export",
            "deterministic",
        ],
    ),
    (
        "SELECT * FROM sys.extension_dependencies",
        &[
            "object_kind",
            "object_name",
            "extension_name",
            "dependency_name",
            "dependency_kind",
            "content_hash",
            "recorded_at_micros",
        ],
    ),
    (
        "SELECT * FROM sys.extension_validation",
        &["name", "valid", "error"],
    ),
];

fn assert_prepared_sys_view_columns(result: &decentdb::QueryResult, expected: &[&str]) {
    assert_eq!(result.columns().len(), expected.len());
    for (actual, expected) in result.columns().iter().zip(expected.iter()) {
        assert_eq!(actual, expected);
    }
}

#[test]
fn prepared_sys_views_execute_without_schema_support_errors() {
    let dir = tempfile::TempDir::with_prefix("decentdb-sys-prepared-views").unwrap();
    let path = dir.path().join("test.ddb");
    let db = Db::create(&path, DbConfig::default()).unwrap();

    for (sql, expected_columns) in PREPARED_SYS_VIEW_ASSERTIONS {
        let statement = db.prepare(sql).unwrap();
        let result = statement.execute(&[]).unwrap();
        assert_prepared_sys_view_columns(&result, expected_columns);
    }
}
