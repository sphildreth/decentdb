use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use decentdb::{Db, DbConfig};

static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn shared_wal_cross_connection_visibility_is_immediate() {
    let _guard = test_lock().lock().expect("test lock");
    let path = unique_db_path("shared-wal");
    let config = DbConfig::default();

    let writer = Db::create(&path, config.clone()).expect("create database");
    let reader = Db::open(&path, config).expect("open second connection");

    writer.begin_write().expect("begin write");
    writer
        .write_page(3, &filled_page(writer.config().page_size, 0x11))
        .expect("stage page");
    writer.commit().expect("commit pages");

    let visible = reader.read_page(3).expect("read committed page");
    assert_eq!(visible, filled_page(reader.config().page_size, 0x11));

    cleanup_db(&path);
}

#[test]
fn failed_commit_does_not_publish_uncommitted_pages_after_reopen() {
    let _guard = test_lock().lock().expect("test lock");
    let path = unique_db_path("failed-commit");
    let config = DbConfig::default();
    let db = Db::create(&path, config.clone()).expect("create database");

    db.begin_write().expect("begin write");
    db.write_page(3, &filled_page(db.config().page_size, 0x22))
        .expect("write first page image");
    db.commit().expect("commit first image");

    Db::clear_failpoints().expect("clear failpoint state");
    Db::install_failpoint("wal.write_commit", "error", 1, 0).expect("install failpoint");
    db.begin_write().expect("begin second write");
    db.write_page(3, &filled_page(db.config().page_size, 0x44))
        .expect("stage second page image");
    let error = db.commit().expect_err("commit should fail");
    assert!(matches!(error, decentdb::DbError::Io { .. }));
    Db::clear_failpoints().expect("clear failpoints");

    let reopened = Db::open(&path, config).expect("reopen database");
    let visible = reopened.read_page(3).expect("read visible page");
    assert_eq!(visible, filled_page(reopened.config().page_size, 0x22));

    cleanup_db(&path);
}

#[test]
fn checkpoint_truncates_wal_without_readers_and_preserves_data() {
    let _guard = test_lock().lock().expect("test lock");
    let path = unique_db_path("checkpoint-truncate");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.begin_write().expect("begin write");
    db.write_page(3, &filled_page(db.config().page_size, 0x33))
        .expect("write page");
    db.commit().expect("commit write");
    db.checkpoint().expect("checkpoint");

    let wal_path = wal_path(&path);
    let wal_size = fs::metadata(&wal_path).expect("stat wal").len();
    assert_eq!(wal_size, 32);

    let reopened = Db::open(&path, DbConfig::default()).expect("reopen database");
    assert_eq!(
        reopened.read_page(3).expect("read page after checkpoint"),
        filled_page(reopened.config().page_size, 0x33)
    );

    cleanup_db(&path);
}

#[test]
fn checkpoint_defers_truncation_when_snapshot_is_held_and_prunes_index() {
    let _guard = test_lock().lock().expect("test lock");
    let path = unique_db_path("checkpoint-reader");
    let db = Db::create(&path, DbConfig::default()).expect("create database");

    db.begin_write().expect("begin write");
    db.write_page(3, &filled_page(db.config().page_size, 0x55))
        .expect("write first image");
    db.commit().expect("commit first image");

    let snapshot = db.hold_snapshot().expect("hold snapshot");

    db.begin_write().expect("begin second write");
    db.write_page(3, &filled_page(db.config().page_size, 0x77))
        .expect("write second image");
    db.commit().expect("commit second image");
    db.checkpoint().expect("checkpoint with active reader");

    let wal_size = fs::metadata(wal_path(&path)).expect("stat wal").len();
    assert!(wal_size > 32, "active reader should block truncation");

    let snapshot_page = db
        .read_page_for_snapshot(snapshot, 3)
        .expect("read page through held snapshot");
    assert_eq!(snapshot_page, filled_page(db.config().page_size, 0x55));

    let inspect = db
        .inspect_storage_state_json()
        .expect("inspect storage state");
    assert!(inspect.contains("\"active_readers\":1"));
    assert!(
        inspect.contains("\"wal_versions\":1"),
        "checkpoint should prune old versions already copied back"
    );

    db.release_snapshot(snapshot).expect("release snapshot");
    db.checkpoint()
        .expect("checkpoint after releasing snapshot");
    let wal_size = fs::metadata(wal_path(&path)).expect("stat wal").len();
    assert_eq!(wal_size, 32);

    cleanup_db(&path);
}

fn filled_page(page_size: u32, byte: u8) -> Vec<u8> {
    vec![byte; page_size as usize]
}

fn unique_db_path(label: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should be monotonic enough for tests")
        .as_nanos();
    let ordinal = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "decentdb-phase1-{label}-{}-{timestamp}-{ordinal}.ddb",
        std::process::id()
    ))
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn cleanup_db(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(wal_path(path));
}
