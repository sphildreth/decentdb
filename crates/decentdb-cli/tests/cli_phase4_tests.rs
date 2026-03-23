use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir() -> PathBuf {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("decentdb-cli-tests-{id}"));
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

#[test]
fn exec_and_schema_introspection_commands_work() {
    let dir = temp_dir();
    let db = dir.join("app.ddb");

    let db_str = db.display().to_string();
    let json = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT); \
         CREATE VIEW user_names AS SELECT name FROM users; \
         CREATE INDEX users_name_idx ON users(name); \
         INSERT INTO users (id, name) VALUES (1, 'Ada'); \
         SELECT id, name FROM users ORDER BY id",
        "--format",
        "json",
    ]);
    assert!(json.contains("\"ok\":true"));
    assert!(json.contains("\"checkpointed\":false"));
    assert!(json.contains("\"columns\":[\"id\",\"name\"]"));
    assert!(json.contains("\"rows\":[[\"1\",\"Ada\"]]"));

    let tables = run(&["list-tables", "--db", &db_str, "--format", "table"]);
    assert_eq!(
        tables.trim(),
        "name  | row_count\n------+----------\nusers | 1"
    );

    let describe = run(&[
        "describe", "--db", &db_str, "--table", "users", "--format", "table",
    ]);
    assert!(describe.contains("name | type"));
    assert!(describe.contains("id   | INT64"));

    let indexes = run(&["list-indexes", "--db", &db_str, "--format", "table"]);
    assert!(indexes.contains("users_name_idx"));

    let views = run(&["list-views", "--db", &db_str, "--format", "table"]);
    assert!(views.contains("user_names"));

    let info = run(&[
        "info",
        "--db",
        &db_str,
        "--schema-summary",
        "--format",
        "table",
    ]);
    assert!(info.contains("table_count"));
    assert!(info.contains("index_count"));

    let stats = run(&["stats", "--db", &db_str, "--format", "table"]);
    assert!(stats.contains("physical_bytes"));

    let dump = run(&["dump", "--db", &db_str]);
    assert!(dump.contains("CREATE TABLE \"users\""));
    assert!(dump.contains("CREATE VIEW \"user_names\""));

    let header = run(&["dump-header", "--db", &db_str, "--format", "table"]);
    assert!(header.contains("format_version"));

    let verify_header = run(&["verify-header", "--db", &db_str, "--format", "table"]);
    assert!(verify_header.contains("magic_hex"));

    let verify_index = run(&[
        "verify-index",
        "--db",
        &db_str,
        "--index",
        "users_name_idx",
        "--format",
        "table",
    ]);
    assert!(verify_index.contains("valid"));

    let checkpoint = run(&["checkpoint", "--db", &db_str]);
    assert!(checkpoint.contains("checkpoint complete"));
}

#[test]
fn import_export_bulk_load_and_maintenance_commands_work() {
    let dir = temp_dir();
    let db = dir.join("ops.ddb");
    let csv = dir.join("import.csv");
    fs::write(&csv, "id,name\n1,Ada\n2,Grace\n").expect("write csv");

    let db_str = db.display().to_string();
    let csv_str = csv.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT); \
         CREATE INDEX users_name_idx ON users(name); \
         CREATE TABLE bulk_users (id INT64 PRIMARY KEY, name TEXT); \
         CREATE INDEX bulk_users_name_idx ON bulk_users(name);",
        "--format",
        "json",
    ]);

    let imported = run(&[
        "import", "--db", &db_str, "--table", "users", "--input", &csv_str,
    ]);
    assert_eq!(imported.trim(), "2");

    let exported = dir.join("export.csv");
    run(&[
        "export",
        "--db",
        &db_str,
        "--table",
        "users",
        "--output",
        &exported.display().to_string(),
        "--format",
        "csv",
    ]);
    let export_body = fs::read_to_string(&exported).expect("read export");
    assert!(export_body.contains("id,name"));
    assert!(export_body.contains("Ada"));

    let bulked = run(&[
        "bulk-load",
        "--db",
        &db_str,
        "--table",
        "bulk_users",
        "--input",
        &csv_str,
        "--disableIndexes",
    ]);
    assert_eq!(bulked.trim(), "2");

    let save_as = dir.join("snapshot.ddb");
    let save_output = run(&[
        "save-as",
        "--db",
        &db_str,
        "--output",
        &save_as.display().to_string(),
    ]);
    assert!(save_output.contains("snapshot.ddb"));
    assert!(save_as.exists());

    let vacuumed = dir.join("vacuumed.ddb");
    let vacuum_output = run(&[
        "vacuum",
        "--db",
        &db_str,
        "--output",
        &vacuumed.display().to_string(),
    ]);
    assert!(vacuum_output.contains("vacuumed.ddb"));
    assert!(vacuumed.exists());

    let rebuilt = run(&[
        "rebuild-index",
        "--db",
        &db_str,
        "--index",
        "users_name_idx",
    ]);
    assert!(rebuilt.contains("users_name_idx"));

    let rebuilt_all = run(&["rebuild-indexes", "--db", &db_str, "--table", "bulk_users"]);
    assert!(rebuilt_all.contains("ok"));
}

#[test]
fn completion_and_repl_smoke_work() {
    let bash_completion = run(&["completion", "--shell", "bash"]);
    assert!(bash_completion.contains("exec repl import export"));

    let zsh_completion = run(&["completion", "--shell", "zsh"]);
    assert!(zsh_completion.contains("#compdef decentdb"));

    let dir = temp_dir();
    let db = dir.join("repl.ddb");
    let db_str = db.display().to_string();

    let mut child = Command::new(bin())
        .args(["repl", "--db", &db_str, "--format", "table"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn repl");
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        writeln!(
            stdin,
            "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT);\n\
             INSERT INTO items (id, name) VALUES (1, 'Ada');\n\
             SELECT id, name FROM items;\n\
             .exit"
        )
        .expect("write repl input");
    }
    let output = child.wait_with_output().expect("wait repl");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("stdout");
    assert!(stdout.contains("id | name"));
    assert!(stdout.contains("1  | Ada"));
}

#[test]
fn version_command_reports_engine_version() {
    let version = run(&["version"]);
    assert!(version.contains("DecentDB version:"));
}
