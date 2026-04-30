//! Unit tests for DbConfig validation and page size variations.
//!
//! Exercises: config.rs validate_for_create, storage/page.rs page helpers,
//! storage/header.rs DatabaseHeader encoding/decoding across page sizes.

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig, WalSyncMode, DB_FORMAT_VERSION};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "decentdb-unit-config-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn cleanup(path: &PathBuf) {
    let _ = fs::remove_file(path);
    let wal_path = path.with_extension("ddb-wal");
    let _ = fs::remove_file(&wal_path);
}

#[test]
fn default_config_has_expected_values() {
    let config = DbConfig::default();
    assert_eq!(config.page_size, 4096);
    assert_eq!(config.cache_size_mb, 4);
    assert_eq!(config.wal_sync_mode, WalSyncMode::Full);
    assert_eq!(config.checkpoint_timeout_sec, 30);
    assert_eq!(config.trigram_postings_threshold, 100_000);
}

#[test]
fn create_with_4096_page_size() {
    let path = unique_db_path("4096");
    let config = DbConfig {
        page_size: 4096,
        ..DbConfig::default()
    };
    let db = Db::create(&path, config.clone()).unwrap();
    assert_eq!(db.config().page_size, 4096);
    let info = db.header_info().unwrap();
    assert_eq!(info.page_size, 4096);
    drop(db);
    cleanup(&path);
}

#[test]
fn create_with_8192_page_size() {
    let path = unique_db_path("8192");
    let config = DbConfig {
        page_size: 8192,
        ..DbConfig::default()
    };
    let db = Db::create(&path, config.clone()).unwrap();
    assert_eq!(db.config().page_size, 8192);
    let info = db.header_info().unwrap();
    assert_eq!(info.page_size, 8192);
    let metadata = fs::metadata(&path).unwrap();
    assert_eq!(metadata.len(), 8192 * 2);
    drop(db);
    cleanup(&path);
}

#[test]
fn create_with_16384_page_size() {
    let path = unique_db_path("16384");
    let config = DbConfig {
        page_size: 16384,
        ..DbConfig::default()
    };
    let db = Db::create(&path, config.clone()).unwrap();
    assert_eq!(db.config().page_size, 16384);
    let info = db.header_info().unwrap();
    assert_eq!(info.page_size, 16384);
    drop(db);
    cleanup(&path);
}

#[test]
fn create_with_unsupported_page_size_fails() {
    let path = unique_db_path("bad-page");
    let config = DbConfig {
        page_size: 2048,
        ..DbConfig::default()
    };
    assert!(Db::create(&path, config).is_err());
    cleanup(&path);
}

#[test]
fn create_with_unsupported_65536_page_size_fails() {
    let path = unique_db_path("bad-65536");
    let config = DbConfig {
        page_size: 65536,
        ..DbConfig::default()
    };
    assert!(Db::create(&path, config).is_err());
    cleanup(&path);
}

#[test]
fn open_preserves_page_size_across_reopen() {
    let path = unique_db_path("persist-ps");
    let config = DbConfig {
        page_size: 8192,
        ..DbConfig::default()
    };
    {
        let db = Db::create(&path, config).unwrap();
        db.execute("CREATE TABLE t(id INT64)").unwrap();
        db.execute("INSERT INTO t VALUES (1)").unwrap();
    }
    let reopened = Db::open(&path, DbConfig::default()).unwrap();
    assert_eq!(reopened.config().page_size, 8192);
    let r = reopened.execute("SELECT id FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], decentdb::Value::Int64(1));
    drop(reopened);
    cleanup(&path);
}

#[test]
fn wal_sync_mode_full_by_default() {
    let config = DbConfig::default();
    assert_eq!(config.wal_sync_mode, WalSyncMode::Full);
}

#[test]
fn wal_sync_mode_testing_unsafe_no_sync() {
    let path = unique_db_path("sync-test");
    let config = DbConfig {
        wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).unwrap();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (42)").unwrap();
    let r = db.execute("SELECT id FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], decentdb::Value::Int64(42));
    drop(db);
    cleanup(&path);
}

#[test]
fn memory_db_works_with_default_config() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    assert_eq!(db.config().page_size, 4096);
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(r.rows()[0].values()[0], decentdb::Value::Int64(1));
}

#[test]
fn header_info_returns_consistent_values() {
    let path = unique_db_path("hdr-info");
    let config = DbConfig {
        page_size: 4096,
        ..DbConfig::default()
    };
    let db = Db::create(&path, config).unwrap();
    let hdr = db.header_info().unwrap();
    assert_eq!(hdr.page_size, 4096);
    assert_eq!(hdr.format_version, DB_FORMAT_VERSION);
    assert_eq!(hdr.catalog_root_page_id, 2);
    assert_eq!(hdr.freelist_root_page_id, 0);
    assert_eq!(hdr.freelist_head_page_id, 0);
    assert_eq!(hdr.freelist_page_count, 0);
    assert!(hdr.header_checksum != 0);
    drop(db);
    cleanup(&path);
}

#[test]
fn storage_info_returns_basic_fields() {
    let path = unique_db_path("sto-info");
    let config = DbConfig {
        page_size: 4096,
        cache_size_mb: 8,
        ..DbConfig::default()
    };
    let db = Db::open_or_create(&path, config).unwrap();
    let info = db.storage_info().unwrap();
    assert_eq!(info.page_size, 4096);
    assert_eq!(info.cache_size_mb, 8);
    assert_eq!(info.page_count, 2);
    drop(db);
    cleanup(&path);
}

#[test]
fn schema_cookie_starts_at_zero() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    let cookie = db.schema_cookie().unwrap();
    assert_eq!(cookie, 0);
}

#[test]
fn checkpoint_timeout_sec_customizable() {
    let config = DbConfig {
        checkpoint_timeout_sec: 60,
        ..DbConfig::default()
    };
    assert_eq!(config.checkpoint_timeout_sec, 60);
}

#[test]
fn trigram_postings_threshold_customizable() {
    let config = DbConfig {
        trigram_postings_threshold: 50_000,
        ..DbConfig::default()
    };
    assert_eq!(config.trigram_postings_threshold, 50_000);
}
