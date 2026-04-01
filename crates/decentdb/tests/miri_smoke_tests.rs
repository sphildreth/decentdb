//! Small representative engine tests kept fast and Miri-compatible for nightly CI.
//!
//! These tests intentionally stay on the low-level `Db`/WAL/page APIs and avoid
//! SQL parsing paths, because the SQL parser currently depends on foreign
//! function calls that Miri cannot execute. The full native test suite continues
//! to exercise the broader SQL behavior matrix outside Miri.

use decentdb::{Db, DbConfig};
use std::sync::{Mutex, OnceLock};

fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).expect("open memory db")
}

fn filled_page(page_size: u32, byte: u8) -> Vec<u8> {
    vec![byte; page_size as usize]
}

#[test]
fn held_snapshot_keeps_prior_page_image_visible() {
    let _guard = test_lock().lock().expect("test lock");
    let db = mem_db();
    let first = filled_page(db.config().page_size, 0x11);
    let second = filled_page(db.config().page_size, 0x22);

    db.begin_write().expect("begin first write");
    db.write_page(3, &first).expect("write first page image");
    db.commit().expect("commit first image");

    let snapshot = db.hold_snapshot().expect("hold snapshot");
    assert_eq!(db.storage_info().expect("storage info").active_readers, 1);

    db.begin_write().expect("begin second write");
    db.write_page(3, &second).expect("write second page image");
    db.commit().expect("commit second image");

    assert_eq!(db.read_page(3).expect("read latest page").to_vec(), second);
    assert_eq!(
        db.read_page_for_snapshot(snapshot, 3)
            .expect("read snapshot page")
            .to_vec(),
        first
    );

    db.release_snapshot(snapshot).expect("release snapshot");
    assert_eq!(db.storage_info().expect("storage info").active_readers, 0);
}

#[test]
fn rollback_discards_staged_page_images() {
    let _guard = test_lock().lock().expect("test lock");
    let db = mem_db();
    let original = filled_page(db.config().page_size, 0x33);
    let replacement = filled_page(db.config().page_size, 0x44);

    db.begin_write().expect("begin initial write");
    db.write_page(3, &original)
        .expect("write original page image");
    db.commit().expect("commit original image");

    db.begin_write().expect("begin rollback write");
    db.write_page(3, &replacement)
        .expect("write replacement page image");
    assert_eq!(
        db.read_page(3)
            .expect("read staged page before rollback")
            .to_vec(),
        replacement
    );
    db.rollback().expect("rollback write");

    assert_eq!(
        db.read_page(3).expect("read page after rollback").to_vec(),
        original
    );
}

#[test]
fn freed_pages_are_reused_after_commit() {
    let _guard = test_lock().lock().expect("test lock");
    let db = mem_db();
    let first = filled_page(db.config().page_size, 0x55);
    let second = filled_page(db.config().page_size, 0x66);

    db.begin_write().expect("begin first allocation");
    let page_id = db.allocate_page().expect("allocate first page");
    db.write_page(page_id, &first)
        .expect("write allocated page image");
    db.commit().expect("commit first allocation");

    assert_eq!(
        db.read_page(page_id)
            .expect("read allocated page after commit")
            .to_vec(),
        first
    );

    db.begin_write().expect("begin free");
    db.free_page(page_id).expect("free allocated page");
    db.commit().expect("commit free");

    db.begin_write().expect("begin second allocation");
    let reused_page_id = db.allocate_page().expect("allocate recycled page");
    assert_eq!(reused_page_id, page_id);
    db.write_page(reused_page_id, &second)
        .expect("write recycled page image");
    db.commit().expect("commit recycled allocation");

    assert_eq!(
        db.read_page(reused_page_id)
            .expect("read recycled page after commit")
            .to_vec(),
        second
    );
}
