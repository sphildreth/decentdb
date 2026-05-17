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

use decentdb::{Db, DbConfig};

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

fn open_db(path: &Path) -> Db {
    Db::open(path, DbConfig::default()).expect("open db")
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
    if let Some((env_name, env_value)) = token_env {
        command.env(env_name, env_value);
        command.args(["--token-env", env_name]);
    }
    let child = command
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn sync serve");

    let addr = wait_for_ready_file(&ready_file);
    (ChildGuard(child), addr)
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
