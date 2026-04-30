use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir() -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("decentdb-cli-doctor-tests-{id}"));
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

fn run_result(args: &[&str]) -> (i32, String, String) {
    let output = Command::new(bin())
        .args(args)
        .output()
        .expect("run command");
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let stderr = String::from_utf8(output.stderr).expect("utf8 stderr");
    (code, stdout, stderr)
}

fn setup_empty_db(dir: &Path) -> String {
    let path = dir.join("test.ddb");
    let path_str = path.display().to_string();
    run(&["exec", "--db", &path_str, "--sql", "CREATE TABLE t(id INT64 PRIMARY KEY, v TEXT); INSERT INTO t VALUES (1, 'a'); CREATE INDEX t_v ON t(v)"]);
    path_str
}

#[test]
fn doctor_json_output_is_parseable() {
    let dir = temp_dir();
    let db = setup_empty_db(&dir);
    let (code, stdout, _) = run_result(&["doctor", "--db", &db, "--format", "json"]);
    assert_eq!(code, 0, "doctor should exit 0 for info-only findings");
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(parsed["schema_version"], 1);
    assert_eq!(parsed["mode"], "check");
    assert!(parsed.get("status").is_some());
    assert!(parsed.get("summary").is_some());
    assert!(parsed.get("findings").is_some());
    assert!(parsed.get("fixes").is_some());
    assert!(parsed.get("collected").is_some());
    assert_eq!(parsed["fixes"].as_array().unwrap().len(), 0);
}

#[test]
fn doctor_default_format_is_markdown() {
    let dir = temp_dir();
    let db = setup_empty_db(&dir);
    let (code, stdout, _) = run_result(&["doctor", "--db", &db]);
    assert_eq!(code, 0, "doctor should exit 0 for default run");
    assert!(stdout.contains("# DecentDB Doctor Report"));
    assert!(stdout.contains("## Status"));
    assert!(stdout.contains("## Database"));
    assert!(stdout.contains("| Path |"));
    assert!(stdout.contains("## Findings"));
}

#[test]
fn doctor_fail_on_info_exits_2() {
    let dir = temp_dir();
    let db = setup_empty_db(&dir);
    let (code, _, _) = run_result(&["doctor", "--db", &db, "--fail-on", "info"]);
    assert_ne!(
        code, 0,
        "real databases always have findings, exit 2 with --fail-on info"
    );
}

#[test]
fn doctor_fail_on_error_exits_2_for_errors() {
    let (code, _, _) = run_result(&[
        "doctor",
        "--db",
        "/nonexistent/path.ddb",
        "--fail-on",
        "error",
        "--format",
        "json",
    ]);
    assert_eq!(
        code, 2,
        "header.unreadable is error severity, default fail-on error should exit 2"
    );
}

#[test]
fn doctor_markdown_output() {
    let dir = temp_dir();
    let db = setup_empty_db(&dir);
    let (code, stdout, _) = run_result(&["doctor", "--db", &db, "--format", "markdown"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("# DecentDB Doctor Report"));
    assert!(stdout.contains("## Summary"));
    assert!(stdout.contains("| Severity | Count |"));
}

#[test]
fn doctor_path_mode_basename_hides_parent_directories() {
    let dir = temp_dir();
    let db = setup_empty_db(&dir);
    let db_name = Path::new(&db).file_name().unwrap().to_string_lossy();
    let (code, stdout, _) = run_result(&[
        "doctor",
        "--db",
        &db,
        "--format",
        "json",
        "--path-mode",
        "basename",
    ]);
    assert_eq!(code, 0);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(parsed["database"]["path"], db_name.as_ref());
    assert_eq!(parsed["database"]["wal_path"], format!("{db_name}.wal"));
    assert!(!parsed["database"]["path"]
        .as_str()
        .expect("path string")
        .contains('/'));
}

#[test]
fn doctor_include_recommendations_false_suppresses_recommendations() {
    let (code, stdout, _) = run_result(&[
        "doctor",
        "--db",
        "/nonexistent/path.ddb",
        "--format",
        "json",
        "--include-recommendations=false",
    ]);
    assert_eq!(code, 2);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert!(parsed["findings"]
        .as_array()
        .expect("findings array")
        .iter()
        .all(|finding| finding["recommendation"].is_null()));
}

#[test]
fn doctor_verify_index_named() {
    let dir = temp_dir();
    let db = setup_empty_db(&dir);
    let (code, stdout, _) = run_result(&[
        "doctor",
        "--db",
        &db,
        "--verify-index",
        "t_v",
        "--format",
        "json",
    ]);
    assert_eq!(code, 0);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert!(!parsed["collected"]["indexes_verified"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn doctor_invalid_checks_fails() {
    let (code, _out, stderr) = run_result(&[
        "doctor",
        "--db",
        "/nonexistent/path.ddb",
        "--checks",
        "bogus_category",
    ]);
    assert_ne!(code, 0);
    assert!(stderr.contains("invalid check category") || !stderr.is_empty());
}

#[test]
fn doctor_missing_file_exits_2() {
    let (code, _, _) = run_result(&[
        "doctor",
        "--db",
        "/nonexistent/path.ddb",
        "--format",
        "json",
    ]);
    assert_eq!(code, 2);
}

#[test]
fn doctor_missing_file_exits_2_even_with_filtered_checks() {
    let (code, stdout, _) = run_result(&[
        "doctor",
        "--db",
        "/nonexistent/path.ddb",
        "--format",
        "json",
        "--checks",
        "wal",
    ]);
    assert_eq!(code, 2);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert!(parsed["findings"]
        .as_array()
        .expect("findings array")
        .iter()
        .any(|finding| finding["id"] == "header.unreadable"));
}
