use decentdb::{Db, DbConfig};

fn setup_db_with_tracing(threshold_us: u64) -> Db {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.ddb");
    let mut config = DbConfig::default();
    config.tracing.enabled = true;
    config.tracing.slow_query.enabled = true;
    config.tracing.slow_query.threshold_us = threshold_us;
    Db::create(&path, config).unwrap()
}

fn setup_db_with_lock_wait_tracing() -> Db {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.ddb");
    let mut config = DbConfig::default();
    config.tracing.enabled = true;
    config.tracing.lock_wait.enabled = true;
    config.tracing.lock_wait.threshold_us = 0; // record every acquisition
    config.process_coordination = decentdb::ProcessCoordinationMode::SingleProcessUnsafe;
    Db::create(&path, config).unwrap()
}

#[test]
fn test_disabled_by_default() {
    let db = setup_db_with_tracing(0);
    let result = db.execute("SELECT * FROM sys.slow_queries").unwrap();
    let rows = result.rows();
    assert!(rows.is_empty());
}

#[test]
fn test_enable_slow_query_tracing() {
    let db = setup_db_with_tracing(1);
    db.execute("SELECT 1").unwrap();
    let result = db.execute("SELECT * FROM sys.slow_queries").unwrap();
    assert_eq!(result.rows().len(), 1);
}

#[test]
fn test_redaction_default_mode_hides_template() {
    let db = setup_db_with_tracing(1);
    db.execute("SELECT * FROM sqlite_schema").unwrap();
    let result = db.execute("SELECT * FROM sys.slow_queries").unwrap();
    let rows = result.rows();
    let row = &rows[0];
    let template = row.values()[9].as_text().unwrap_or_default();
    let mode = row.values()[10].as_text().unwrap_or_default();
    assert!(
        template.is_empty(),
        "expected empty template in default None mode, got: {template}"
    );
    assert_eq!(mode, "none");
}

#[test]
fn test_full_mode_shows_sql() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("test.ddb");
    let mut config = DbConfig::default();
    config.tracing.enabled = true;
    config.tracing.slow_query.enabled = true;
    config.tracing.slow_query.threshold_us = 1;
    config.tracing.slow_query.sql_text_mode = decentdb::SqlTextMode::Full;
    let db = Db::create(&path, config).unwrap();
    db.execute("SELECT 1").unwrap();
    let result = db.execute("SELECT * FROM sys.slow_queries").unwrap();
    let rows = result.rows();
    let row = &rows[0];
    let template = row.values()[9].as_text().unwrap_or_default();
    let mode = row.values()[10].as_text().unwrap_or_default();
    assert!(template.contains("SELECT 1"));
    assert_eq!(mode, "full");
}

#[test]
fn test_sessions_view() {
    let db = setup_db_with_tracing(0);
    let result = db.execute("SELECT * FROM sys.sessions").unwrap();
    assert_eq!(result.rows().len(), 1);
}

#[test]
fn test_lock_waits_view_disabled_by_default() {
    let db = setup_db_with_tracing(0);
    let result = db.execute("SELECT * FROM sys.lock_waits").unwrap();
    let rows = result.rows();
    assert!(rows.is_empty());
}

#[test]
fn test_lock_wait_tracing_sql_write_lock() {
    let db = setup_db_with_lock_wait_tracing();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let result = db.execute("SELECT * FROM sys.lock_waits").unwrap();
    let rows = result.rows();
    assert!(
        !rows.is_empty(),
        "expected at least one lock wait event for sql_write lock"
    );
    let source = rows[0].values()[5].as_text().unwrap_or_default();
    assert_eq!(source, "sql_write", "unexpected lock wait source: {source}");
}
