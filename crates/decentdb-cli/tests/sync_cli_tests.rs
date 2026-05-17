use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, SyncImportSummary, SyncRunDirection, SyncRunSummary};

fn temp_dir(prefix: &str) -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}-{id}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_decentdb")
}

fn run(args: &[&str]) -> String {
    let output = Command::new(bin())
        .args(args)
        .output()
        .expect("run command");
    assert!(
        output.status.success(),
        "command failed: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

fn run_result(args: &[&str], envs: &[(&str, &str)]) -> (i32, String, String) {
    let mut command = Command::new(bin());
    command.args(args);
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command.output().expect("run command");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8(output.stdout).expect("utf8 stdout"),
        String::from_utf8(output.stderr).expect("utf8 stderr"),
    )
}

fn setup_sync_db(path: &Path, replica_id: &str, rows: &[(i64, &str)]) {
    let db_str = path.display().to_string();
    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT);",
        "--format",
        "json",
    ]);
    run(&["sync", "init", "--db", &db_str, "--replica-id", replica_id]);
    for (id, name) in rows {
        run(&[
            "exec",
            "--db",
            &db_str,
            "--sql",
            &format!("INSERT INTO users VALUES ({id}, '{name}')"),
            "--format",
            "json",
        ]);
    }
}

fn setup_tenant_sync_db(
    path: &Path,
    replica_id: &str,
    tenant_one_id: i64,
    tenant_one_value: &str,
    tenant_two_id: i64,
    tenant_two_value: &str,
) {
    let db_str = path.display().to_string();
    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE tenant_items (tenant_id INT64, id INT64, value TEXT, PRIMARY KEY (tenant_id, id));",
        "--format",
        "json",
    ]);
    run(&["sync", "init", "--db", &db_str, "--replica-id", replica_id]);
    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        &format!("INSERT INTO tenant_items VALUES (1, {tenant_one_id}, '{tenant_one_value}')"),
        "--format",
        "json",
    ]);
    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        &format!("INSERT INTO tenant_items VALUES (2, {tenant_two_id}, '{tenant_two_value}')"),
        "--format",
        "json",
    ]);
}

fn open_db(path: &Path) -> Db {
    Db::open(path, DbConfig::default()).expect("open db")
}

fn query_tenant_items(path: &Path) -> Vec<(i64, i64, String)> {
    let db = open_db(path);
    let result = db
        .execute("SELECT tenant_id, id, value FROM tenant_items ORDER BY tenant_id, id")
        .expect("query tenant items");
    result
        .rows()
        .iter()
        .map(|row| match row.values() {
            [
                decentdb::Value::Int64(tenant_id),
                decentdb::Value::Int64(id),
                decentdb::Value::Text(value),
            ] => (*tenant_id, *id, value.clone()),
            other => panic!("unexpected row values: {:?}", other),
        })
        .collect()
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_sync_serve(db: &Path, max_requests: usize) -> (ChildGuard, String) {
    spawn_sync_serve_with_token(db, max_requests, None)
}

fn spawn_sync_serve_with_token(
    db: &Path,
    max_requests: usize,
    token_env: Option<(&str, &str)>,
) -> (ChildGuard, String) {
    spawn_sync_serve_scoped(db, max_requests, None, token_env)
}

fn spawn_sync_serve_scoped(
    db: &Path,
    max_requests: usize,
    scope: Option<&str>,
    token_env: Option<(&str, &str)>,
) -> (ChildGuard, String) {
    spawn_sync_serve_scoped_with_policy(db, max_requests, scope, token_env, None)
}

fn spawn_sync_serve_scoped_with_policy(
    db: &Path,
    max_requests: usize,
    scope: Option<&str>,
    token_env: Option<(&str, &str)>,
    conflict_policy: Option<&str>,
) -> (ChildGuard, String) {
    let ready_file = db.with_extension("ready");
    let mut command = Command::new(bin());
    command.args([
        "sync",
        "serve",
        "--db",
        &db.display().to_string(),
        "--bind",
        "127.0.0.1:0",
        "--ready-file",
        &ready_file.display().to_string(),
        "--max-requests",
        &max_requests.to_string(),
    ]);
    if let Some(scope_name) = scope {
        command.args(["--scope", scope_name]);
    }
    if let Some((env_name, env_value)) = token_env {
        command.env(env_name, env_value);
        command.args(["--token-env", env_name]);
    }
    if let Some(policy) = conflict_policy {
        command.args(["--conflict-policy", policy]);
    }
    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sync serve");

    let addr = wait_for_ready_file(&ready_file);
    (ChildGuard(child), addr)
}

fn journal_line_count(path: &Path) -> usize {
    match fs::read_to_string(path) {
        Ok(content) => content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
        Err(_) => 0,
    }
}

fn wait_for_ready_file(path: &Path) -> String {
    for _ in 0..200 {
        if let Ok(value) = fs::read_to_string(path) {
            let trimmed = value.trim().to_string();
            if !trimmed.is_empty() {
                return trimmed;
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("ready file was not populated");
}

fn query_users(path: &Path) -> Vec<(i64, String)> {
    let db = open_db(path);
    let result = db
        .execute("SELECT id, name FROM users ORDER BY id")
        .expect("query users");
    result
        .rows()
        .iter()
        .map(|row| match row.values() {
            [decentdb::Value::Int64(id), decentdb::Value::Text(name)] => (*id, name.clone()),
            other => panic!("unexpected row values: {:?}", other),
        })
        .collect()
}

fn setup_operational_sync_db(path: &Path) {
    let db = Db::create(path, DbConfig::default()).expect("create db");
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .expect("create table");
    db.sync_init_replica("node-b").expect("init replica");
    db.sync_add_peer("peer-a", "https://peer.example.com", None)
        .expect("add peer");
    db.execute("INSERT INTO users VALUES (10, 'dst10')")
        .expect("insert local row");
    db.execute("INSERT INTO users VALUES (11, 'dst11')")
        .expect("insert local row");

    let src_dir = temp_dir("decentdb-sync-cli-src");
    let src_path = src_dir.join("src.ddb");
    let src = Db::create(&src_path, DbConfig::default()).expect("create source db");
    src.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)")
        .expect("create source table");
    src.sync_init_replica("node-a").expect("init source");
    src.execute("INSERT INTO users VALUES (1, 'src1')")
        .expect("insert source row");
    src.execute("INSERT INTO users VALUES (2, 'src2')")
        .expect("insert source row");

    let batch = src.sync_export_batch(0, 100).expect("export batch");
    db.sync_import_batch(&batch).expect("import batch");
    db.sync_set_peer_out_watermark("peer-a", 10)
        .expect("set outbound watermark");

    let session_id = db
        .sync_start_session("peer-a", SyncRunDirection::Pull, Some("node-a"))
        .expect("start session");
    let summary = SyncRunSummary {
        peer_name: "peer-a".to_string(),
        direction: SyncRunDirection::Pull,
        remote_replica_id: Some("node-a".to_string()),
        pushed: None,
        pulled: Some(SyncImportSummary {
            seen: 2,
            applied: 2,
            skipped: 0,
            conflicted: 0,
        }),
        pushed_batch_id: None,
        pulled_batch_id: Some(batch.batch_id.clone()),
        retry_count: 0,
    };
    db.sync_finish_session_success(session_id, &summary)
        .expect("finish session");
}

#[test]
fn sync_peer_add_list_remove_supports_json_and_table_outputs() {
    let dir = temp_dir("decentdb-sync-peer-cli");
    let db = dir.join("peers.ddb");
    let db_str = db.display().to_string();

    let add = run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &db_str,
        "--name",
        "central",
        "--endpoint",
        "https://sync.example.com",
        "--token-env",
        "DECENTDB_SYNC_TOKEN",
        "--format",
        "json",
    ]);
    let parsed: serde_json::Value = serde_json::from_str(&add).expect("json");
    assert_eq!(parsed["name"], "central");
    assert_eq!(parsed["endpoint"], "https://sync.example.com");
    assert_eq!(parsed["token_env"], "DECENTDB_SYNC_TOKEN");

    let list = run(&["sync", "peer", "list", "--db", &db_str, "--format", "table"]);
    assert!(list.contains("central"));
    assert!(list.contains("https://sync.example.com"));

    let remove = run(&[
        "sync", "peer", "remove", "--db", &db_str, "--name", "central", "--format", "json",
    ]);
    let removed: serde_json::Value = serde_json::from_str(&remove).expect("json");
    assert_eq!(removed["removed"], true);

    let list_after = run(&["sync", "peer", "list", "--db", &db_str, "--format", "json"]);
    let parsed_after: serde_json::Value = serde_json::from_str(&list_after).expect("json");
    assert!(parsed_after.as_array().expect("array").is_empty());
}

#[test]
fn sync_doctor_json_reports_operational_state_and_guidance() {
    let dir = temp_dir("decentdb-sync-doctor-cli");
    let db = dir.join("doctor.ddb");
    setup_operational_sync_db(&db);

    let db_str = db.display().to_string();
    let json = run(&["sync", "doctor", "--db", &db_str, "--format", "json"]);
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("json");
    assert!(parsed["status"]["enabled"].as_bool().unwrap());
    assert_eq!(parsed["status"]["replica_id"], "node-b");
    assert_eq!(parsed["integrity"]["total_records"], 2);
    assert_eq!(parsed["retention"]["safe_prune_through"], 1);
    assert_eq!(parsed["peer_lag"].as_array().unwrap().len(), 1);
    assert_eq!(parsed["recent_sessions"].as_array().unwrap().len(), 1);
    assert!(parsed["guidance"].as_array().unwrap().iter().any(|line| {
        line.as_str()
            .expect("guidance string")
            .contains("safe prune is available")
    }));

    let table = run(&["sync", "doctor", "--db", &db_str, "--format", "table"]);
    assert!(table.contains("integrity_records"));
    assert!(table.contains("retention_safe_prune_through"));
    assert!(table.contains("peer-a"));
    assert!(table.contains("guidance:"));
}

#[test]
fn sync_prune_supports_dry_run_json_and_allow_data_loss_table_outputs() {
    let dir = temp_dir("decentdb-sync-prune-cli");
    let db = dir.join("prune.ddb");
    setup_operational_sync_db(&db);

    let db_str = db.display().to_string();
    let journal_path = db.with_extension("ddb.sync-journal");
    let dry_run = run(&[
        "sync",
        "prune",
        "--db",
        &db_str,
        "--through",
        "1",
        "--dry-run",
        "--format",
        "json",
    ]);
    let parsed: serde_json::Value = serde_json::from_str(&dry_run).expect("json");
    assert_eq!(parsed["requested_through"], 1);
    assert_eq!(parsed["effective_through"], 1);
    assert_eq!(parsed["pruned"], 1);
    assert!(parsed["dry_run"].as_bool().unwrap());
    assert!(!parsed["allow_data_loss"].as_bool().unwrap());
    assert_eq!(journal_line_count(&journal_path), 2);

    let table = run(&[
        "sync",
        "prune",
        "--db",
        &db_str,
        "--through",
        "2",
        "--allow-data-loss",
        "--format",
        "table",
    ]);
    assert!(table.contains("requested_through"));
    assert!(table.contains("allow_data_loss"));
    assert!(table.contains("blocked_by_json"));
    assert!(table.contains("remote:node-a"));
    assert_eq!(journal_line_count(&journal_path), 0);
}

#[test]
fn sync_scope_create_list_bind_unbind_supports_json_and_table_outputs() {
    let dir = temp_dir("decentdb-sync-scope-cli");
    let db = dir.join("scopes.ddb");
    let db_str = db.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, tenant_id INT64, name TEXT);",
        "--format",
        "json",
    ]);
    run(&["sync", "init", "--db", &db_str, "--replica-id", "local-a"]);
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &db_str,
        "--name",
        "relay",
        "--endpoint",
        "https://relay.example.com",
        "--format",
        "json",
    ]);

    let create = run(&[
        "sync",
        "scope",
        "create",
        "--db",
        &db_str,
        "--name",
        "tenant_1",
        "--include",
        "users",
        "--row-filter",
        "id = 1",
        "--format",
        "json",
    ]);
    let created: serde_json::Value = serde_json::from_str(&create).expect("json");
    assert_eq!(created["name"], "tenant_1");
    assert_eq!(created["include_tables"], serde_json::json!(["users"]));
    assert_eq!(created["filter_columns"], serde_json::json!(["id"]));

    let list = run(&[
        "sync", "scope", "list", "--db", &db_str, "--format", "table",
    ]);
    assert!(list.contains("tenant_1"));
    assert!(list.contains("users"));
    assert!(list.contains("id"));

    let bind = run(&[
        "sync", "scope", "bind", "--db", &db_str, "--peer", "relay", "--scope", "tenant_1",
        "--format", "json",
    ]);
    let binding: serde_json::Value = serde_json::from_str(&bind).expect("json");
    assert_eq!(binding["peer_name"], "relay");
    assert_eq!(binding["scope_name"], "tenant_1");

    let bindings = run(&[
        "sync", "scope", "bindings", "--db", &db_str, "--format", "table",
    ]);
    assert!(bindings.contains("relay"));
    assert!(bindings.contains("tenant_1"));

    let unbind = run(&[
        "sync", "scope", "unbind", "--db", &db_str, "--peer", "relay", "--format", "json",
    ]);
    let unbound: serde_json::Value = serde_json::from_str(&unbind).expect("json");
    assert_eq!(unbound["removed"], true);

    let bindings_after = run(&[
        "sync", "scope", "bindings", "--db", &db_str, "--format", "json",
    ]);
    let parsed_after: serde_json::Value = serde_json::from_str(&bindings_after).expect("json");
    assert!(parsed_after.as_array().expect("array").is_empty());
}

#[test]
fn sync_run_both_round_trips_changes_and_remains_incremental() {
    let dir = temp_dir("decentdb-sync-run-both");
    let local = dir.join("local.ddb");
    let remote = dir.join("remote.ddb");

    setup_sync_db(&local, "local-a", &[(1, "alice")]);
    setup_sync_db(&remote, "remote-b", &[(2, "bob")]);

    let (_server, addr) = spawn_sync_serve(&remote, 10);
    let local_str = local.display().to_string();
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &local_str,
        "--name",
        "remote",
        "--endpoint",
        &format!("http://{addr}"),
    ]);

    let first = run(&[
        "sync", "run", "--db", &local_str, "--peer", "remote", "--format", "json",
    ]);
    let first_json: serde_json::Value = serde_json::from_str(&first).expect("json");
    assert_eq!(first_json["direction"], "both");
    assert_eq!(first_json["pushed"]["applied"], 1);
    assert_eq!(first_json["pulled"]["applied"], 1);

    assert_eq!(
        query_users(&local),
        vec![(1, "alice".to_string()), (2, "bob".to_string())]
    );
    assert_eq!(
        query_users(&remote),
        vec![(1, "alice".to_string()), (2, "bob".to_string())]
    );

    let second = run(&[
        "sync", "run", "--db", &local_str, "--peer", "remote", "--format", "json",
    ]);
    let second_json: serde_json::Value = serde_json::from_str(&second).expect("json");
    assert_eq!(second_json["pushed"]["seen"], 0);
    assert_eq!(second_json["pulled"]["seen"], 0);

    let local_db = open_db(&local);
    let sessions = local_db
        .execute("SELECT * FROM sys_sync_sessions ORDER BY session_id")
        .unwrap();
    assert_eq!(sessions.rows().len(), 2);
    assert_eq!(
        sessions.rows()[0].values()[6],
        decentdb::Value::Text("success".to_string())
    );
    assert_eq!(
        sessions.rows()[1].values()[6],
        decentdb::Value::Text("success".to_string())
    );
}

#[test]
fn sync_conflict_cli_commands_support_show_resolve_reopen_and_all() {
    let dir = temp_dir("decentdb-sync-conflict-cli");
    let local = dir.join("local.ddb");
    let remote = dir.join("remote.ddb");
    setup_sync_db(&local, "local-a", &[(1, "alice"), (2, "bravo")]);
    setup_sync_db(
        &remote,
        "remote-b",
        &[(1, "remote-alice"), (2, "remote-bravo")],
    );

    let (_server, addr) = spawn_sync_serve(&remote, 10);
    let local_str = local.display().to_string();
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &local_str,
        "--name",
        "remote",
        "--endpoint",
        &format!("http://{addr}"),
        "--format",
        "json",
    ]);

    let sync = run(&[
        "sync",
        "run",
        "--db",
        &local_str,
        "--peer",
        "remote",
        "--direction",
        "pull",
        "--format",
        "json",
    ]);
    let sync_json: serde_json::Value = serde_json::from_str(&sync).expect("json");
    assert_eq!(sync_json["direction"], "pull");
    assert_eq!(sync_json["pulled"]["seen"], 2);
    assert_eq!(sync_json["pulled"]["conflicted"], 2);

    let all_before = run(&[
        "sync",
        "conflicts",
        "--db",
        &local_str,
        "--all",
        "--format",
        "json",
    ]);
    let all_before_json: serde_json::Value = serde_json::from_str(&all_before).expect("json");
    let all_before_rows = all_before_json.as_array().expect("array");
    assert_eq!(all_before_rows.len(), 2);
    assert!(all_before_rows.iter().all(|row| row["resolved"] == false));

    let show = run(&[
        "sync", "conflict", "show", "--db", &local_str, "--id", "1", "--format", "json",
    ]);
    let show_json: serde_json::Value = serde_json::from_str(&show).expect("json");
    assert!(show_json["remote_record_json"].is_object());
    assert_eq!(show_json["local_record_json"], show_json["local_row_json"]);
    assert_eq!(show_json["resolved"], false);

    let keep_local = run(&[
        "sync",
        "conflict",
        "resolve",
        "--db",
        &local_str,
        "--id",
        "1",
        "--action",
        "keep-local",
        "--by",
        "cli",
        "--note",
        "keep local",
        "--format",
        "json",
    ]);
    let keep_local_json: serde_json::Value = serde_json::from_str(&keep_local).expect("json");
    assert_eq!(keep_local_json["resolution"], "keep_local");
    assert_eq!(keep_local_json["resolved_by"], "cli");
    assert_eq!(
        query_users(&local),
        vec![(1, "alice".to_string()), (2, "bravo".to_string())]
    );

    let unresolved = run(&["sync", "conflicts", "--db", &local_str, "--format", "json"]);
    let unresolved_json: serde_json::Value = serde_json::from_str(&unresolved).expect("json");
    let unresolved_rows = unresolved_json.as_array().expect("array");
    assert_eq!(unresolved_rows.len(), 1);
    assert_eq!(unresolved_rows[0]["conflict_id"], 2);

    let reopened = run(&[
        "sync", "conflict", "reopen", "--db", &local_str, "--id", "1", "--format", "json",
    ]);
    let reopened_json: serde_json::Value = serde_json::from_str(&reopened).expect("json");
    assert_eq!(reopened_json["resolved"], false);

    let apply_remote = run(&[
        "sync",
        "conflict",
        "resolve",
        "--db",
        &local_str,
        "--id",
        "1",
        "--action",
        "apply-remote",
        "--by",
        "cli",
        "--note",
        "apply remote",
        "--format",
        "json",
    ]);
    let apply_remote_json: serde_json::Value = serde_json::from_str(&apply_remote).expect("json");
    assert_eq!(apply_remote_json["resolution"], "apply_remote");
    assert_eq!(
        query_users(&local),
        vec![(1, "remote-alice".to_string()), (2, "bravo".to_string())]
    );
    assert_eq!(
        journal_line_count(&local.with_file_name("local.ddb.sync-journal")),
        2
    );

    let all_after = run(&[
        "sync",
        "conflicts",
        "--db",
        &local_str,
        "--all",
        "--format",
        "table",
    ]);
    assert!(all_after.contains("resolved"));
    assert!(all_after.contains("open"));
    assert!(all_after.contains("apply_remote"));
}

#[test]
fn sync_conflict_policy_cli_round_trips_get_and_set() {
    let dir = temp_dir("decentdb-sync-conflict-policy-cli");
    let db = dir.join("policy.ddb");
    let db_str = db.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT);",
        "--format",
        "json",
    ]);
    run(&["sync", "init", "--db", &db_str, "--replica-id", "local-a"]);

    let set = run(&[
        "sync",
        "conflict",
        "policy",
        "set",
        "--db",
        &db_str,
        "--policy",
        "origin-priority",
        "--origin-priority",
        "remote-a,local-a",
        "--format",
        "json",
    ]);
    let set_json: serde_json::Value = serde_json::from_str(&set).expect("json");
    assert_eq!(set_json["default_policy"], "origin_priority");
    assert_eq!(
        set_json["origin_priority"],
        serde_json::json!(["remote-a", "local-a"])
    );

    let get = run(&[
        "sync", "conflict", "policy", "get", "--db", &db_str, "--format", "table",
    ]);
    assert!(get.contains("origin_priority"));
    assert!(get.contains("remote-a"));
    assert!(get.contains("local-a"));
}

#[test]
fn sync_run_respects_server_side_conflict_policy_stop() {
    let dir = temp_dir("decentdb-sync-conflict-stop-server");
    let local = dir.join("local.ddb");
    let remote = dir.join("remote.ddb");
    setup_sync_db(&local, "local-a", &[(1, "alice")]);
    setup_sync_db(
        &remote,
        "remote-b",
        &[(1, "remote-alice"), (2, "remote-bravo")],
    );

    let (_server, addr) =
        spawn_sync_serve_scoped_with_policy(&remote, 10, None, None, Some("stop"));
    let local_str = local.display().to_string();
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &local_str,
        "--name",
        "remote",
        "--endpoint",
        &format!("http://{addr}"),
        "--format",
        "json",
    ]);

    let (code, _stdout, stderr) = run_result(
        &[
            "sync",
            "run",
            "--db",
            &local_str,
            "--peer",
            "remote",
            "--direction",
            "push",
            "--format",
            "json",
        ],
        &[],
    );
    assert_ne!(code, 0);
    assert!(stderr.contains("stopped on conflict") || stderr.contains("conflict"));
    assert_eq!(
        query_users(&remote),
        vec![
            (1, "remote-alice".to_string()),
            (2, "remote-bravo".to_string())
        ]
    );
    let remote_db = open_db(&remote);
    let conflicts = remote_db.sync_conflicts().expect("remote conflicts");
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].conflict_type, "insert_insert");
}

#[test]
fn sync_scoped_http_sync_prevents_tenant_leakage() {
    let dir = temp_dir("decentdb-sync-scope-http");
    let local = dir.join("local.ddb");
    let remote = dir.join("remote.ddb");

    setup_tenant_sync_db(&local, "local-a", 1, "local-t1", 2, "local-t2");
    setup_tenant_sync_db(&remote, "remote-b", 11, "remote-t1", 22, "remote-t2");

    let local_str = local.display().to_string();
    let remote_str = remote.display().to_string();

    run(&[
        "sync",
        "scope",
        "create",
        "--db",
        &local_str,
        "--name",
        "tenant_1",
        "--include",
        "tenant_items",
        "--row-filter",
        "tenant_id = 1",
        "--format",
        "json",
    ]);
    run(&[
        "sync",
        "scope",
        "create",
        "--db",
        &remote_str,
        "--name",
        "tenant_1",
        "--include",
        "tenant_items",
        "--row-filter",
        "tenant_id = 1",
        "--format",
        "json",
    ]);

    let (_server, addr) = spawn_sync_serve_scoped(&remote, 10, Some("tenant_1"), None);
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &local_str,
        "--name",
        "remote",
        "--endpoint",
        &format!("http://{addr}"),
        "--format",
        "json",
    ]);
    run(&[
        "sync", "scope", "bind", "--db", &local_str, "--peer", "remote", "--scope", "tenant_1",
        "--format", "json",
    ]);

    let sync = run(&[
        "sync", "run", "--db", &local_str, "--peer", "remote", "--format", "json",
    ]);
    let parsed: serde_json::Value = serde_json::from_str(&sync).expect("json");
    assert_eq!(parsed["direction"], "both");
    assert_eq!(parsed["pushed"]["applied"], 1);
    assert_eq!(parsed["pulled"]["applied"], 1);

    let local_rows = query_tenant_items(&local);
    let remote_rows = query_tenant_items(&remote);
    assert!(local_rows.contains(&(1, 1, "local-t1".to_string())));
    assert!(local_rows.contains(&(1, 11, "remote-t1".to_string())));
    assert!(remote_rows.contains(&(1, 1, "local-t1".to_string())));
    assert!(remote_rows.contains(&(1, 11, "remote-t1".to_string())));
    assert!(!local_rows
        .iter()
        .any(|row| row.0 == 2 && row.2 == "remote-t2"));
    assert!(!remote_rows
        .iter()
        .any(|row| row.0 == 2 && row.2 == "local-t2"));
}

#[test]
fn sync_run_redacts_token_errors_and_retries_transient_failures() {
    let dir = temp_dir("decentdb-sync-redaction");
    let local = dir.join("local.ddb");
    let remote = dir.join("remote.ddb");
    let token_env = "DECENTDB_SYNC_TOKEN";
    let token_value = "supersecret";

    setup_sync_db(&local, "local-a", &[(1, "alice")]);
    setup_sync_db(&remote, "remote-b", &[]);

    let (_server, addr) = spawn_sync_serve_with_token(&remote, 10, Some((token_env, token_value)));
    let local_str = local.display().to_string();
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &local_str,
        "--name",
        "remote",
        "--endpoint",
        &format!("http://{addr}"),
        "--token-env",
        token_env,
    ]);

    let ok_run = run_result(
        &[
            "sync",
            "run",
            "--db",
            &local_str,
            "--peer",
            "remote",
            "--direction",
            "push",
            "--format",
            "json",
        ],
        &[(token_env, token_value)],
    );
    assert_eq!(ok_run.0, 0);

    let fail_run = run_result(
        &[
            "sync",
            "run",
            "--db",
            &local_str,
            "--peer",
            "remote",
            "--direction",
            "push",
            "--format",
            "json",
        ],
        &[(token_env, "wrongsecret")],
    );
    assert_ne!(fail_run.0, 0);
    assert!(!fail_run.1.contains("wrongsecret"));
    assert!(!fail_run.2.contains("wrongsecret"));
}

#[test]
fn sync_run_retries_transient_hello_failure() {
    let dir = temp_dir("decentdb-sync-retry");
    let local = dir.join("local.ddb");
    setup_sync_db(&local, "local-a", &[(1, "alice")]);

    let endpoint = spawn_retry_server();
    let local_str = local.display().to_string();
    run(&[
        "sync",
        "peer",
        "add",
        "--db",
        &local_str,
        "--name",
        "retry-peer",
        "--endpoint",
        &endpoint,
    ]);

    let run = run_result(
        &[
            "sync",
            "run",
            "--db",
            &local_str,
            "--peer",
            "retry-peer",
            "--direction",
            "push",
            "--retries",
            "2",
            "--format",
            "json",
        ],
        &[],
    );
    assert_eq!(run.0, 0);
    let parsed: serde_json::Value = serde_json::from_str(&run.1).expect("json");
    assert_eq!(parsed["retry_count"], 1);
    assert_eq!(query_users(&local), vec![(1, "alice".to_string())]);
}

fn spawn_retry_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind retry server");
    let addr = listener.local_addr().expect("addr");
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_clone = Arc::clone(&attempts);

    thread::spawn(move || {
        for stream in listener.incoming().take(3) {
            let mut stream = stream.expect("retry server stream");
            let attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
            let (method, path, body) = read_http_request(&mut stream);
            if attempt == 0 && path == "/decentdb/sync/v1/hello" {
                write_http_response(&mut stream, 500, serde_json::json!({"error":"temporary"}));
                continue;
            }
            match (method.as_str(), path.as_str()) {
                ("GET", "/decentdb/sync/v1/hello") => {
                    let body = serde_json::json!({
                        "protocol_version": 1,
                        "engine_version": "test",
                        "replica_id": "retry-remote",
                        "capabilities": [
                            "batch-envelope-v1",
                            "manual-import-v1",
                            "peer-watermarks-v1",
                            "conflicts-v1"
                        ]
                    });
                    write_http_response(&mut stream, 200, body);
                }
                ("POST", "/decentdb/sync/v1/import") => {
                    let _ = body;
                    write_http_response(
                        &mut stream,
                        200,
                        serde_json::json!({
                            "seen": 1,
                            "applied": 1,
                            "skipped": 0,
                            "conflicted": 0
                        }),
                    );
                }
                _ => {
                    write_http_response(&mut stream, 404, serde_json::json!({"error":"not found"}))
                }
            }
        }
    });

    format!("http://{addr}")
}

fn read_http_request(stream: &mut std::net::TcpStream) -> (String, String, Vec<u8>) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut request_line = String::new();
    reader.read_line(&mut request_line).expect("request line");
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("header");
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().expect("content length");
            }
        }
    }
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).expect("body");

    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let target = parts.next().unwrap_or("").to_string();
    (method, target, body)
}

fn write_http_response(stream: &mut std::net::TcpStream, status: u16, body: serde_json::Value) {
    let body = serde_json::to_vec(&body).expect("json");
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .expect("response header");
    stream.write_all(&body).expect("response body");
    stream.flush().expect("flush");
}
