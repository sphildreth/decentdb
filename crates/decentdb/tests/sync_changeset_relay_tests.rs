use decentdb::{
    ApplyChangesetOptions, CreateChangesetOptions, CreateShapeOptions, Db, DbConfig,
    InspectChangesetOptions, ShapeAckOptions, SyncChangesetSource, Value,
};

fn create_sync_db(path: &std::path::Path, replica_id: &str) -> Db {
    let db = Db::create(path, DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE tasks (tenant_id INT64, id INT64, title TEXT, PRIMARY KEY (tenant_id, id))",
    )
    .unwrap();
    db.sync_init_replica(replica_id).unwrap();
    db
}

#[test]
fn public_checkpoint_changeset_applies_and_replays_idempotently() {
    let dir = tempfile::TempDir::with_prefix("decentdb-changeset-roundtrip").unwrap();
    let source = create_sync_db(&dir.path().join("source.ddb"), "node-a");
    let target = create_sync_db(&dir.path().join("target.ddb"), "node-b");

    source
        .execute("INSERT INTO tasks (tenant_id, id, title) VALUES (42, 1, 'draft')")
        .unwrap();
    source
        .execute("UPDATE tasks SET title = 'ready' WHERE tenant_id = 42 AND id = 1")
        .unwrap();

    let changeset = source
        .sync_create_changeset(CreateChangesetOptions {
            source: SyncChangesetSource::Checkpoint {
                peer: "node-b".to_string(),
                since_sequence: 0,
            },
            scope_name: None,
            shape_id: None,
            max_records: None,
            max_bytes: None,
            principal: None,
        })
        .unwrap();
    assert_eq!(changeset.changeset_version, 1);
    assert_eq!(changeset.records.len(), 2);
    assert!(changeset
        .integrity_hash
        .as_deref()
        .unwrap()
        .starts_with("sha256:"));

    let inspection = target
        .sync_inspect_changeset(
            &changeset,
            InspectChangesetOptions {
                check_local_compatibility: true,
            },
        )
        .unwrap();
    assert_eq!(inspection.compatibility.status, "compatible");
    assert_eq!(inspection.record_count, 2);

    let applied = target
        .sync_apply_changeset(&changeset, ApplyChangesetOptions::default())
        .unwrap();
    assert_eq!(applied.outcome, "applied");
    assert_eq!(applied.rows_applied, 2);

    let rows = target
        .execute("SELECT title FROM tasks WHERE tenant_id = 42 AND id = 1")
        .unwrap();
    assert_eq!(rows.rows()[0].values()[0], Value::Text("ready".to_string()));

    let replay = target
        .sync_apply_changeset(&changeset, ApplyChangesetOptions::default())
        .unwrap();
    assert_eq!(replay.outcome, "already_applied");
    assert_eq!(replay.rows_applied, 0);

    let history = target
        .execute("SELECT * FROM sys.sync_changeset_history")
        .unwrap();
    assert_eq!(history.rows().len(), 1);
}

#[test]
fn public_changeset_rejects_tampering_before_mutation() {
    let dir = tempfile::TempDir::with_prefix("decentdb-changeset-tamper").unwrap();
    let source = create_sync_db(&dir.path().join("source.ddb"), "node-a");
    let target = create_sync_db(&dir.path().join("target.ddb"), "node-b");

    source
        .execute("INSERT INTO tasks (tenant_id, id, title) VALUES (42, 1, 'draft')")
        .unwrap();
    let mut changeset = source
        .sync_create_changeset(CreateChangesetOptions {
            source: SyncChangesetSource::Checkpoint {
                peer: "node-b".to_string(),
                since_sequence: 0,
            },
            scope_name: None,
            shape_id: None,
            max_records: None,
            max_bytes: None,
            principal: None,
        })
        .unwrap();
    changeset.records[0].after = Some(serde_json::json!({
        "tenant_id": 42,
        "id": 1,
        "title": "tampered"
    }));

    let error = target
        .sync_apply_changeset(&changeset, ApplyChangesetOptions::default())
        .unwrap_err()
        .to_string();
    assert!(error.contains("integrity_hash"));
    let rows = target.execute("SELECT * FROM tasks").unwrap();
    assert!(rows.rows().is_empty());
}

#[test]
fn relay_shapes_snapshot_ack_and_retention_diagnostics_are_durable() {
    let dir = tempfile::TempDir::with_prefix("decentdb-shape-diagnostics").unwrap();
    let db = create_sync_db(&dir.path().join("shape.ddb"), "node-a");
    db.execute("INSERT INTO tasks (tenant_id, id, title) VALUES (42, 1, 'draft')")
        .unwrap();
    db.sync_create_scope("tenant_42_tasks", &["tasks"], Some("tenant_id = 42"))
        .unwrap();
    let shape = db
        .sync_create_shape(CreateShapeOptions {
            shape_id: "tenant_42_tasks_v1".to_string(),
            name: None,
            scope_name: "tenant_42_tasks".to_string(),
            tenant_id: "tenant_42".to_string(),
            allowed_roles: vec!["user".to_string()],
            allowed_subjects: Vec::new(),
            retention_ttl_micros: None,
            max_records: None,
            ack_deadline_micros: None,
            heartbeat_micros: None,
        })
        .unwrap();
    assert_eq!(shape.scope_name, "tenant_42_tasks");

    let delivery = db
        .sync_shape_snapshot("tenant_42_tasks_v1", "web-1", None)
        .unwrap();
    assert_eq!(delivery.message_type, "snapshot");
    assert_eq!(delivery.changeset.records.len(), 1);

    let client = db
        .sync_ack_shape(ShapeAckOptions {
            shape_id: "tenant_42_tasks_v1".to_string(),
            tenant_id: "tenant_42".to_string(),
            client_replica_id: "web-1".to_string(),
            subject_id: "user-1".to_string(),
            session_id: Some("sess-1".to_string()),
            shape_sequence: delivery.checkpoint.shape_sequence,
            source_high_watermark: delivery.checkpoint.source_high_watermark,
            changeset_id: Some(delivery.changeset.changeset_id.clone()),
        })
        .unwrap();
    assert!(client.retention_blocking);

    let shape_clients = db.execute("SELECT * FROM sys.sync_shape_clients").unwrap();
    assert_eq!(shape_clients.rows().len(), 1);
    let retention = db.sync_retention_report().unwrap();
    assert!(retention
        .blocked_by
        .iter()
        .any(|entry| entry.contains("shape:tenant_42_tasks_v1:client:web-1")));
}
