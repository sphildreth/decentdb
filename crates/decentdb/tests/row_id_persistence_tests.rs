use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, Value};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn reopen_preserves_internal_next_row_id_for_rowid_tables() {
    let path = unique_db_path("row-id-persistence");
    let config = DbConfig::default();

    let setup = Db::open_or_create(&path, config.clone()).expect("open setup database");
    setup
        .execute("CREATE TABLE t (id INT64, payload TEXT)")
        .expect("create table");
    for id in 0_i64..30 {
        setup
            .execute_with_params(
                "INSERT INTO t VALUES ($1, $2)",
                &[Value::Int64(id), Value::Text("seed".repeat(8))],
            )
            .expect("insert setup row");
    }
    drop(setup);

    let reopened = Db::open(&path, config).expect("reopen database");
    reopened
        .execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(30), Value::Text("writer".repeat(8))],
        )
        .expect("insert reopened row");
    reopened
        .execute("DELETE FROM t WHERE id >= 30")
        .expect("delete reopened row");

    let count = reopened
        .execute("SELECT COUNT(*) FROM t")
        .expect("count rows");
    assert_eq!(count.rows()[0].values(), &[Value::Int64(30)]);

    drop(reopened);
    cleanup_db(&path);
}

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic enough for tests")
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "decentdb-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn cleanup_db(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(wal_path(path));
}
