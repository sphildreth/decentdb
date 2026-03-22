use std::fs;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, DbError, Result};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn stable_public_exports_are_available() {
    let _: fn(&std::path::Path, DbConfig) -> Result<Db> = |path, config| Db::create(path, config);
    let _: fn(&std::path::Path, DbConfig) -> Result<Db> = |path, config| Db::open(path, config);
    let _error = DbError::internal("placeholder");
}

#[test]
fn create_and_open_round_trip_bootstrap_state() {
    let path = unique_db_path("roundtrip");
    let config = DbConfig {
        page_size: 8192,
        ..DbConfig::default()
    };

    let created = Db::create(&path, config.clone()).expect("create should succeed");
    assert_eq!(created.config().page_size, 8192);
    assert_eq!(created.path(), path.as_path());

    let metadata = fs::metadata(&path).expect("database file should exist");
    assert_eq!(metadata.len(), 16_384);

    let reopened = Db::open(&path, DbConfig::default()).expect("open should succeed");
    assert_eq!(reopened.config().page_size, 8192);
    assert_eq!(reopened.path(), path.as_path());

    cleanup_db_file(&path);
}

#[test]
fn create_again_returns_already_exists_error() {
    let path = unique_db_path("create-again");
    let config = DbConfig::default();

    Db::create(&path, config.clone()).expect("initial create should succeed");
    let error = Db::create(&path, config).expect_err("second create should fail");

    match error {
        DbError::Io { source, .. } => assert_eq!(source.kind(), ErrorKind::AlreadyExists),
        other => panic!("expected already-exists I/O error, got {other:?}"),
    }

    cleanup_db_file(&path);
}

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic enough for tests")
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "decentdb-phase0-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn cleanup_db_file(path: &PathBuf) {
    let _ = fs::remove_file(path);
}
