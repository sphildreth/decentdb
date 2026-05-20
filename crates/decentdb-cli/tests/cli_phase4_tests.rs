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

fn wal_path(db: &std::path::Path) -> PathBuf {
    let mut wal = db.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
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
fn describe_command_shows_foreign_keys() {
    let dir = temp_dir();
    let db = dir.join("foreign-keys.ddb");
    let db_str = db.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE artists (id INT64 PRIMARY KEY); \
         CREATE TABLE albums (id INT64 PRIMARY KEY, artist_id INT64, \
           FOREIGN KEY (artist_id) REFERENCES artists(id) \
           ON DELETE CASCADE ON UPDATE SET NULL);",
        "--format",
        "json",
    ]);

    let describe = run(&[
        "describe", "--db", &db_str, "--table", "albums", "--format", "table",
    ]);
    assert!(describe.contains("foreign_key"));
    assert!(describe.contains("artist_id"));
    assert!(describe.contains("REFERENCES artists(id) ON DELETE CASCADE ON UPDATE SET NULL"));

    let json = run(&[
        "describe", "--db", &db_str, "--table", "albums", "--format", "json",
    ]);
    assert!(json.contains("\"foreign_key\""));
    assert!(json.contains("REFERENCES artists(id)"));
}

#[test]
fn checkpoint_command_flushes_wal_and_preserves_data_without_wal_file() {
    let dir = temp_dir();
    let db = dir.join("checkpoint.ddb");
    let db_str = db.display().to_string();

    let result = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE t (id INT64 PRIMARY KEY, value TEXT); \
         INSERT INTO t (id, value) VALUES (1, 'before'); \
         UPDATE t SET value = 'after' WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(result.contains("\"ok\":true"));

    let wal = wal_path(&db);
    let wal_size_before = fs::metadata(&wal)
        .expect("stat WAL before checkpoint")
        .len();
    assert!(
        wal_size_before > 32,
        "test setup should leave committed frames in the WAL"
    );

    let checkpoint = run(&["checkpoint", "--db", &db_str]);
    assert!(checkpoint.contains("checkpoint complete"));
    assert_eq!(
        fs::metadata(&wal).expect("stat WAL after checkpoint").len(),
        32,
        "checkpoint should truncate WAL to its header when no readers are active"
    );

    fs::remove_file(&wal).expect("remove checkpointed WAL");
    let selected = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "SELECT value FROM t WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(selected.contains("\"rows\":[[\"after\"]]"));
}

#[test]
fn snapshot_commands_and_exec_as_of_work() {
    let dir = temp_dir();
    let db = dir.join("snapshots.ddb");
    let db_str = db.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT); \
         INSERT INTO items (id, name) VALUES (1, 'before');",
        "--format",
        "json",
    ]);

    let created = run(&[
        "snapshot",
        "create",
        "--db",
        &db_str,
        "--name",
        "before-update",
        "--format",
        "json",
    ]);
    assert!(created.contains("before-update"));
    assert!(created.contains("snapshot_lsn"));

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "UPDATE items SET name = 'after' WHERE id = 1;",
        "--format",
        "json",
    ]);

    let historical = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--as-of",
        "before-update",
        "--format",
        "json",
    ]);
    assert!(
        historical.contains("\"rows\":[[\"before\"]]"),
        "historical output: {historical}"
    );

    let latest = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(latest.contains("\"rows\":[[\"after\"]]"));

    let listed = run(&["snapshot", "list", "--db", &db_str, "--format", "table"]);
    assert!(listed.contains("before-update"));

    let deleted = run(&[
        "snapshot",
        "delete",
        "--db",
        &db_str,
        "--name",
        "before-update",
        "--format",
        "json",
    ]);
    assert!(deleted.contains("before-update"));
    assert!(deleted.contains("true"));
}

#[test]
fn branch_commands_and_exec_branch_work() {
    let dir = temp_dir();
    let db = dir.join("branches.ddb");
    let db_str = db.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT); \
         INSERT INTO items (id, name) VALUES (1, 'before');",
        "--format",
        "json",
    ]);

    let created = run(&[
        "branch", "create", "--db", &db_str, "--name", "work", "--format", "json",
    ]);
    assert!(created.contains("work"));
    assert!(created.contains("current_head_id"));

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "UPDATE items SET name = 'after' WHERE id = 1;",
        "--format",
        "json",
    ]);

    let branch_read = run(&[
        "exec",
        "--db",
        &db_str,
        "--branch",
        "work",
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(branch_read.contains("\"rows\":[[\"before\"]]"));

    run(&[
        "exec",
        "--db",
        &db_str,
        "--branch",
        "work",
        "--sql",
        "UPDATE items SET name = 'branch' WHERE id = 1;",
        "--format",
        "json",
    ]);
    let branch_after_write = run(&[
        "exec",
        "--db",
        &db_str,
        "--branch",
        "work",
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(branch_after_write.contains("\"rows\":[[\"branch\"]]"));
    let main_after_branch_write = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(main_after_branch_write.contains("\"rows\":[[\"after\"]]"));

    let committed = run(&[
        "branch",
        "commit",
        "--db",
        &db_str,
        "--name",
        "work",
        "--message",
        "reviewed branch change",
        "--format",
        "json",
    ]);
    assert!(committed.contains("reviewed branch change"));
    let log = run(&[
        "branch", "log", "--db", &db_str, "--name", "work", "--format", "json",
    ]);
    assert!(log.contains("reviewed branch change"));
    assert!(log.contains("UPDATE items SET name = 'branch' WHERE id = 1"));

    let diff = run(&[
        "branch", "diff", "--db", &db_str, "--left", "main", "--right", "work", "--format", "json",
    ]);
    assert!(diff.contains("\"changed_table_count\": 1"));
    assert!(diff.contains("\"updated_row_count\": 1"));
    assert!(diff.contains("\"table\": \"items\""));

    run(&[
        "snapshot",
        "create",
        "--db",
        &db_str,
        "--name",
        "main-after-update",
        "--format",
        "json",
    ]);
    let restore_dry_run = run(&[
        "branch",
        "restore",
        "--db",
        &db_str,
        "--name",
        "work",
        "--to",
        "main-after-update",
        "--dry-run",
        "--format",
        "json",
    ]);
    assert!(restore_dry_run.contains("\"dry_run\": true"));
    assert!(restore_dry_run.contains("\"updated_row_count\": 1"));
    let restored = run(&[
        "branch",
        "restore",
        "--db",
        &db_str,
        "--name",
        "work",
        "--to",
        "main-after-update",
        "--confirm",
        "--format",
        "json",
    ]);
    assert!(restored.contains("\"dry_run\": false"));
    let branch_after_restore = run(&[
        "exec",
        "--db",
        &db_str,
        "--branch",
        "work",
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(branch_after_restore.contains("\"rows\":[[\"after\"]]"));

    run(&[
        "branch",
        "create",
        "--db",
        &db_str,
        "--name",
        "mergework",
        "--from",
        "main-after-update",
        "--format",
        "json",
    ]);
    run(&[
        "exec",
        "--db",
        &db_str,
        "--branch",
        "mergework",
        "--sql",
        "UPDATE items SET name = 'merged' WHERE id = 1;",
        "--format",
        "json",
    ]);
    let merge_dry_run = run(&[
        "branch",
        "merge",
        "--db",
        &db_str,
        "--source",
        "mergework",
        "--target",
        "main",
        "--dry-run",
        "--format",
        "json",
    ]);
    assert!(merge_dry_run.contains("\"clean\": true"));
    assert!(merge_dry_run.contains("\"conflict_count\": 0"));
    let merged = run(&[
        "branch",
        "merge",
        "--db",
        &db_str,
        "--source",
        "mergework",
        "--target",
        "main",
        "--confirm",
        "--format",
        "json",
    ]);
    assert!(merged.contains("\"applied_change_count\": 1"));
    let main_after_merge = run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "SELECT name FROM items WHERE id = 1;",
        "--format",
        "json",
    ]);
    assert!(main_after_merge.contains("\"rows\":[[\"merged\"]]"));

    let listed = run(&["branch", "list", "--db", &db_str, "--format", "table"]);
    assert!(listed.contains("main"));
    assert!(listed.contains("work"));

    let renamed = run(&[
        "branch",
        "rename",
        "--db",
        &db_str,
        "--name",
        "work",
        "--new-name",
        "review",
        "--format",
        "json",
    ]);
    assert!(renamed.contains("review"));
    assert!(renamed.contains("true"));

    let deleted = run(&[
        "branch", "delete", "--db", &db_str, "--name", "review", "--format", "json",
    ]);
    assert!(deleted.contains("review"));
    assert!(deleted.contains("true"));
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
fn header_only_commands_ignore_sparse_huge_wal_files() {
    let dir = temp_dir();
    let db = dir.join("huge-wal-header-only.ddb");
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

    let wal = wal_path(&db);
    let wal_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&wal)
        .expect("open sparse WAL");
    if let Err(err) = wal_file.set_len(8 * 1024 * 1024) {
        eprintln!("skipping test: unable to create sparse WAL fixture: {err}");
        return;
    }

    let header = run(&["dump-header", "--db", &db_str, "--format", "table"]);
    assert!(header.contains("format_version"));

    let verify_header = run(&["verify-header", "--db", &db_str, "--format", "table"]);
    assert!(verify_header.contains("magic_hex"));
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
    let script = dir.join("script.sql");
    let import_csv = dir.join("import.csv");
    let export_csv = dir.join("export.csv");
    let once_output = dir.join("once.txt");
    let redirected_output = dir.join("redirected.txt");
    fs::write(
        &script,
        "CREATE VIEW item_names AS SELECT name FROM items;\n\
         CREATE INDEX items_name_idx ON items(name);\n",
    )
    .expect("write script");
    fs::write(&import_csv, "id,name\n2,Bob\n3,\n").expect("write import csv");
    let script_str = script.display().to_string();
    let import_csv_str = import_csv.display().to_string();
    let export_csv_str = export_csv.display().to_string();
    let once_output_str = once_output.display().to_string();
    let redirected_output_str = redirected_output.display().to_string();

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
            "help schema\n\
             CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT);\n\
             INSERT INTO items (id, name) VALUES (1, 'Ada');\n\
             .read {script_str}\n\
             SELECT id, name FROM items;\n\
             .tables\n\
             .dt\n\
             .d items\n\
             .schema items\n\
             .indexes items\n\
             .views\n\
             .df\n\
             .mode csv\n\
             .headers off\n\
             .nullvalue (null)\n\
             SELECT NULL AS missing;\n\
             .mode table\n\
             .headers on\n\
             .timer on\n\
             .explain SELECT id FROM items;\n\
             .param set 1 int:1\n\
             SELECT name FROM items WHERE id = $1;\n\
             .param list\n\
             .import {import_csv_str} items\n\
             .export items {export_csv_str} csv\n\
             .once {once_output_str}\n\
             SELECT name FROM items WHERE id = 2;\n\
             .output {redirected_output_str}\n\
             SELECT name FROM items WHERE id = 1;\n\
             .output stdout\n\
             .s\n\
             .g\n\
             \\q"
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
    assert!(stdout.contains("DecentDB CLI"));
    assert!(stdout.contains("Type \"help\" for help."));
    assert!(stdout.contains(".schema <object>"));
    assert!(stdout.contains(".dt"));
    assert!(stdout.contains("id | name"));
    assert!(stdout.contains("1  | Ada"));
    assert!(stdout.contains("items"));
    assert!(stdout.contains("row_count"));
    assert!(stdout.contains("constraints"));
    assert!(stdout.contains("PRIMARY KEY"));
    assert!(stdout.contains("CREATE TABLE"));
    assert!(stdout.contains("items_name_idx"));
    assert!(stdout.contains("item_names"));
    assert!(stdout.contains("length"));
    assert!(stdout.contains("(null)"));
    assert!(stdout.contains("plan"));
    assert!(stdout.contains("Time:"));
    assert!(stdout.contains("1     | 1"));
    assert!(stdout.contains("3"));
    assert!(stdout.contains("SELECT id, name FROM items;"));
    assert!(fs::read_to_string(&export_csv)
        .expect("read export csv")
        .contains("2,Bob"));
    assert!(fs::read_to_string(&once_output)
        .expect("read once output")
        .contains("Bob"));
    assert!(fs::read_to_string(&redirected_output)
        .expect("read redirected output")
        .contains("Ada"));
}

#[test]
fn sync_export_import_and_doctor_commands_work() {
    let dir = temp_dir();
    let source = dir.join("sync_source.ddb");
    let target = dir.join("sync_target.ddb");
    let export = dir.join("sync_export.jsonl");
    let source_str = source.display().to_string();
    let target_str = target.display().to_string();
    let export_str = export.display().to_string();

    run(&[
        "exec",
        "--db",
        &source_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)",
    ]);
    run(&[
        "sync",
        "init",
        "--db",
        &source_str,
        "--replica-id",
        "node-a",
    ]);
    run(&[
        "exec",
        "--db",
        &source_str,
        "--sql",
        "INSERT INTO users (id, name) VALUES (1, 'Ada'); \
         UPDATE users SET name = 'Ada Lovelace' WHERE id = 1; \
         INSERT INTO users (id, name) VALUES (2, 'Grace');",
    ]);

    let doctor = run(&["sync", "doctor", "--db", &source_str, "--format", "json"]);
    assert!(doctor.contains("\"total_records\": 3"));
    assert!(doctor.contains("\"issues\": []"));

    run(&[
        "sync",
        "export",
        "--db",
        &source_str,
        "--since",
        "0",
        "--output",
        &export_str,
    ]);
    let exported = fs::read_to_string(&export).expect("read sync export");
    let exported_json: serde_json::Value =
        serde_json::from_str(&exported).expect("parse sync export batch");
    assert_eq!(exported_json["protocol_version"], serde_json::json!(1));
    assert_eq!(
        exported_json["batch_id"],
        serde_json::json!("sync-batch:v1:node-a:1:3:3")
    );
    assert_eq!(exported_json["record_count"], serde_json::json!(3));
    assert_eq!(
        exported_json["source_replica_id"],
        serde_json::json!("node-a")
    );
    assert_eq!(exported_json["records"].as_array().unwrap().len(), 3);

    run(&[
        "exec",
        "--db",
        &target_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)",
    ]);
    run(&[
        "sync",
        "init",
        "--db",
        &target_str,
        "--replica-id",
        "node-b",
    ]);

    let imported = run(&[
        "sync",
        "import",
        "--db",
        &target_str,
        "--input",
        &export_str,
    ]);
    assert_eq!(
        imported.trim(),
        "seen=3, applied=3, skipped=0, conflicted=0"
    );

    let selected = run(&[
        "exec",
        "--db",
        &target_str,
        "--sql",
        "SELECT id, name FROM users ORDER BY id",
        "--format",
        "json",
    ]);
    assert!(selected.contains("\"rows\":[[\"1\",\"Ada Lovelace\"],[\"2\",\"Grace\"]]"));

    let reimported = run(&[
        "sync",
        "import",
        "--db",
        &target_str,
        "--input",
        &export_str,
    ]);
    assert_eq!(
        reimported.trim(),
        "seen=3, applied=0, skipped=3, conflicted=0"
    );

    let pending = run(&["sync", "pending", "--db", &target_str, "--format", "json"]);
    assert_eq!(pending.trim(), "[]");
}

#[test]
fn sync_import_rejects_malformed_jsonl() {
    let dir = temp_dir();
    let db = dir.join("sync_import.ddb");
    let db_str = db.display().to_string();

    run(&[
        "exec",
        "--db",
        &db_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)",
    ]);
    run(&["sync", "init", "--db", &db_str, "--replica-id", "node-a"]);

    let bad = dir.join("malformed.jsonl");
    fs::write(&bad, "{ \"schema_version\": 1").expect("write malformed payload");

    let (code, _stdout, stderr) = run_result(&[
        "sync",
        "import",
        "--db",
        &db_str,
        "--input",
        &bad.display().to_string(),
    ]);
    assert_ne!(code, 0);
    assert!(stderr.contains("malformed sync batch"));
}

#[test]
fn sync_conflicts_command_displays_json_and_table() {
    let dir = temp_dir();
    let source = dir.join("sync_conflict_source.ddb");
    let target = dir.join("sync_conflict_target.ddb");
    let source_str = source.display().to_string();
    let target_str = target.display().to_string();

    run(&[
        "exec",
        "--db",
        &source_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT)",
        "--format",
        "json",
    ]);
    run(&[
        "sync",
        "init",
        "--db",
        &source_str,
        "--replica-id",
        "node-a",
    ]);
    run(&[
        "exec",
        "--db",
        &source_str,
        "--sql",
        "INSERT INTO users VALUES (1, 'Ada')",
        "--format",
        "json",
    ]);
    run(&[
        "exec",
        "--db",
        &target_str,
        "--sql",
        "CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT); \
         INSERT INTO users VALUES (1, 'Existing')",
        "--format",
        "json",
    ]);
    run(&[
        "sync",
        "init",
        "--db",
        &target_str,
        "--replica-id",
        "node-b",
    ]);

    let export = dir.join("conflict_export.json");
    run(&[
        "sync",
        "export",
        "--db",
        &source_str,
        "--since",
        "0",
        "--output",
        &export.display().to_string(),
    ]);
    run(&[
        "sync",
        "import",
        "--db",
        &target_str,
        "--input",
        &export.display().to_string(),
    ]);

    let conflicts_json = run(&["sync", "conflicts", "--db", &target_str, "--format", "json"]);
    assert!(conflicts_json.contains("\"conflict_type\": \"insert_insert\""));
    assert!(conflicts_json.contains("\"batch_id\": \"sync-batch:v1:node-a:1:1:1\""));

    let conflicts_table = run(&[
        "sync",
        "conflicts",
        "--db",
        &target_str,
        "--format",
        "table",
    ]);
    assert!(conflicts_table.contains("insert_insert"));
}

#[test]
fn version_command_reports_engine_version() {
    let version = run(&["version"]);
    assert!(version.contains("DecentDB version:"));
}
