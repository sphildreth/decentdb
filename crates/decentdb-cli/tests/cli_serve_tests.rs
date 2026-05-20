use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn temp_dir(prefix: &str) -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("decentdb-cli-serve-{prefix}-{id}"));
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
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

fn next_free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("allocate port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

fn port_allocation_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_serve(
    db: &Path,
    read_only: bool,
    token: Option<(&str, &str)>,
    max_result_rows: usize,
    max_body_size: &str,
) -> (ChildGuard, u16) {
    let _port_guard = port_allocation_lock().lock().expect("port lock");
    let port = next_free_port();
    let mut command = Command::new(bin());
    command
        .args([
            "serve",
            "--db",
            &db.display().to_string(),
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--max-result-rows",
            &max_result_rows.to_string(),
            "--max-body-size",
            max_body_size,
            "--query-timeout",
            "500ms",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    if read_only {
        command.arg("--read-only");
    }
    if let Some((name, value)) = token {
        command.env(name, value);
        command.args(["--token-env", name]);
    }

    let child = command.spawn().expect("spawn serve");
    wait_for_connect(port);
    (ChildGuard(child), port)
}

fn spawn_serve_no_auth(db: &Path) -> (ChildGuard, u16) {
    let _port_guard = port_allocation_lock().lock().expect("port lock");
    let port = next_free_port();
    let mut command = Command::new(bin());
    command
        .args([
            "serve",
            &db.display().to_string(),
            "--host",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--no-auth",
            "--query-timeout",
            "500ms",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = command.spawn().expect("spawn serve");
    wait_for_connect(port);
    (ChildGuard(child), port)
}

fn spawn_serve_remote_with_token(db: &Path, token_name: &str, token: &str) -> (ChildGuard, u16) {
    let _port_guard = port_allocation_lock().lock().expect("port lock");
    let port = next_free_port();
    let mut command = Command::new(bin());
    command
        .args([
            "serve",
            "--db",
            &db.display().to_string(),
            "--host",
            "0.0.0.0",
            "--port",
            &port.to_string(),
            "--token-env",
            token_name,
            "--query-timeout",
            "500ms",
        ])
        .env(token_name, token)
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let child = command.spawn().expect("spawn serve");
    wait_for_connect(port);
    (ChildGuard(child), port)
}

fn wait_for_connect(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(3) {
        if TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("server did not start on {addr}");
}

fn http_request(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
    token: Option<&str>,
) -> (u16, String, Vec<u8>) {
    let addr = format!("127.0.0.1:{port}");
    let mut stream = TcpStream::connect(&addr).expect("connect");

    let body = body.unwrap_or("");
    let mut request = String::new();
    request.push_str(&format!("{method} {path} HTTP/1.1\r\n"));
    request.push_str(&format!("Host: {addr}\r\n"));
    request.push_str("Connection: close\r\n");
    if !body.is_empty() {
        request.push_str("Content-Type: application/json\r\n");
    }
    if let Some(token_value) = token {
        request.push_str(&format!("Authorization: Bearer {token_value}\r\n"));
    }
    request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).expect("write request");
    stream.flush().expect("flush request");

    let mut reader = BufReader::new(stream);
    let mut status_line = String::new();
    reader.read_line(&mut status_line).expect("status line");
    let mut parts = status_line.split_whitespace();
    let _ = parts.next().unwrap_or("");
    let status: u16 = parts.next().unwrap_or("0").parse().unwrap_or(0);

    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).expect("header line");
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some((name, value)) = trimmed.split_once(':') {
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }

    let mut body_bytes = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body_bytes).expect("read body");
    }
    let text = String::from_utf8_lossy(&body_bytes).to_string();
    (status, text, body_bytes)
}

fn setup_db(path: &Path) {
    run(&[
        "exec",
        "--db",
        &path.display().to_string(),
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT); INSERT INTO users VALUES (1, 'Ada'); INSERT INTO users VALUES (2, 'Ben'); INSERT INTO users VALUES (3, 'Cid');",
        "--format",
        "json",
    ]);
}

#[test]
fn serve_health_info_schema_sql_and_auth_workflow() {
    let dir = temp_dir("serve-endpoints");
    let db = dir.join("app.ddb");
    setup_db(&db);

    let token_name = "DECENTDB_SERVE_TOKEN";
    let token = "test-token-123";
    let (_child, port) = spawn_serve(&db, false, Some((token_name, token)), 2, "16kb");

    let health = http_request(port, "GET", "/healthz", None, None);
    assert_eq!(health.0, 200);
    let health_json: serde_json::Value = serde_json::from_str(&health.1).expect("json");
    assert_eq!(health_json["ok"], true);

    let ready = http_request(port, "GET", "/readyz", None, None);
    assert_eq!(ready.0, 200);

    let _ui = http_request(port, "GET", "/", None, None);
    assert_eq!(http_request(port, "GET", "/", None, None).0, 200);

    let info_unauth = http_request(port, "GET", "/api/v1/info", None, None);
    assert_eq!(info_unauth.0, 401);

    let info = http_request(port, "GET", "/api/v1/info", None, Some(token));
    assert_eq!(info.0, 200);
    let info_json: serde_json::Value = serde_json::from_str(&info.1).expect("json");
    assert_eq!(info_json["database"]["fileName"], "app.ddb");

    let schema = http_request(port, "GET", "/api/v1/schema", None, Some(token));
    assert_eq!(schema.0, 200);
    let schema_json: serde_json::Value = serde_json::from_str(&schema.1).expect("json");
    assert!(schema_json["tables"].is_array());

    let select = serde_json::json!({"sql": "SELECT name FROM users ORDER BY id"});
    let select = http_request(
        port,
        "POST",
        "/api/v1/sql",
        Some(&select.to_string()),
        Some(token),
    );
    assert_eq!(select.0, 200);
    let select_json: serde_json::Value = serde_json::from_str(&select.1).expect("json");
    assert_eq!(select_json["ok"], true);
    assert_eq!(
        select_json["results"][0]["rows"]
            .as_array()
            .expect("rows")
            .len(),
        2
    );

    let sql_no_auth = http_request(port, "POST", "/api/v1/sql", Some("{}"), None);
    assert_eq!(sql_no_auth.0, 401);

    let bad_sql_token = http_request(
        port,
        "POST",
        "/api/v1/sql",
        Some(&serde_json::json!({"sql": "SELECT 1"}).to_string()),
        Some("wrong"),
    );
    assert_eq!(bad_sql_token.0, 401);
}

#[test]
fn serve_read_only_blocks_mutations_and_truncates_rows() {
    let dir = temp_dir("serve-readonly");
    let db = dir.join("ro.ddb");
    setup_db(&db);

    let token = "read-only-token";
    let token_name = "DECENTDB_SERVE_TOKEN";
    let (_child, port) = spawn_serve(&db, true, Some((token_name, token)), 1, "16kb");

    let update = serde_json::json!({"sql": "INSERT INTO users VALUES (4, 'Dave');"});
    let update_resp = http_request(
        port,
        "POST",
        "/api/v1/sql",
        Some(&update.to_string()),
        Some(token),
    );
    assert_eq!(update_resp.0, 400);
    let update_json: serde_json::Value = serde_json::from_str(&update_resp.1).expect("json");
    assert_eq!(update_json["error"]["code"], "READ_ONLY");

    let select = serde_json::json!({"sql": "SELECT id, name FROM users ORDER BY id"});
    let select_resp = http_request(
        port,
        "POST",
        "/api/v1/sql",
        Some(&select.to_string()),
        Some(token),
    );
    assert_eq!(select_resp.0, 200);
    let rows = serde_json::from_str::<serde_json::Value>(&select_resp.1).expect("json")["results"]
        [0]["rows"]
        .as_array()
        .expect("rows array")
        .len();
    assert_eq!(rows, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&select_resp.1).expect("json")["results"][0]
            ["truncated"],
        true
    );
}

#[test]
fn serve_enforces_body_limit() {
    let dir = temp_dir("serve-body-limit");
    let db = dir.join("limit.ddb");
    setup_db(&db);

    let token = "body-limit-token";
    let token_name = "DECENTDB_SERVE_TOKEN";
    let (_child, port) = spawn_serve(&db, false, Some((token_name, token)), 10, "1kb");

    let mut huge_sql = String::from("INSERT INTO users VALUES (7, '");
    huge_sql.push_str(&"x".repeat(2048));
    huge_sql.push_str("')");
    let payload = serde_json::json!({"sql": huge_sql});
    let response = http_request(
        port,
        "POST",
        "/api/v1/sql",
        Some(&payload.to_string()),
        Some(token),
    );
    assert_eq!(response.0, 413);
}

#[test]
fn serve_no_auth_localhost_table_detail_and_explain_work() {
    let dir = temp_dir("serve-no-auth");
    let db = dir.join("detail.ddb");
    setup_db(&db);

    let (_child, port) = spawn_serve_no_auth(&db);

    let docs = http_request(port, "GET", "/api/v1", None, None);
    assert_eq!(docs.0, 200);
    let docs_json: serde_json::Value = serde_json::from_str(&docs.1).expect("json");
    assert_eq!(docs_json["version"], "v1");

    let detail = http_request(port, "GET", "/api/v1/tables/users", None, None);
    assert_eq!(detail.0, 200);
    let detail_json: serde_json::Value = serde_json::from_str(&detail.1).expect("json");
    assert_eq!(detail_json["table"]["name"], "users");
    assert!(detail_json["ddl"]
        .as_str()
        .expect("ddl")
        .contains("CREATE TABLE"));

    let explain = serde_json::json!({"sql": "SELECT name FROM users WHERE id = $1", "params": [1]});
    let explain_resp = http_request(
        port,
        "POST",
        "/api/v1/explain",
        Some(&explain.to_string()),
        None,
    );
    assert_eq!(explain_resp.0, 200);
    let explain_json: serde_json::Value = serde_json::from_str(&explain_resp.1).expect("json");
    assert_eq!(explain_json["ok"], true);
    assert!(!explain_json["results"][0]["rows"]
        .as_array()
        .expect("rows")
        .is_empty());
}

#[test]
fn serve_rejects_remote_bind_without_token_env() {
    let dir = temp_dir("serve-remote-safety");
    let db = dir.join("remote.ddb");
    setup_db(&db);

    let output = Command::new(bin())
        .args([
            "serve",
            "--db",
            &db.display().to_string(),
            "--host",
            "0.0.0.0",
            "--port",
            "0",
        ])
        .output()
        .expect("run serve");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--token-env"));
}

#[test]
fn serve_remote_bind_does_not_bootstrap_token_into_root_page() {
    let dir = temp_dir("serve-remote-token");
    let db = dir.join("remote-token.ddb");
    setup_db(&db);

    let token_name = "DECENTDB_REMOTE_SERVE_TOKEN";
    let token = "remote-secret-token";
    let (_child, port) = spawn_serve_remote_with_token(&db, token_name, token);

    let root = http_request(port, "GET", "/", None, None);
    assert_eq!(root.0, 200);
    assert!(!root.1.contains(token));

    let info_unauth = http_request(port, "GET", "/api/v1/info", None, None);
    assert_eq!(info_unauth.0, 401);

    let info = http_request(port, "GET", "/api/v1/info", None, Some(token));
    assert_eq!(info.0, 200);
}
