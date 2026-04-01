use decentdb::{Db, DbConfig, Result};

fn miri_db() -> Result<Db> {
    // Miri interprets every operation, so keep the cache at the minimum
    // effective size while still exercising the real pager/WAL code paths.
    let config = DbConfig {
        cache_size_mb: 0,
        ..DbConfig::default()
    };
    Db::open_or_create(":memory:", config)
}

fn page_image(page_size: u32, marker: u8) -> Vec<u8> {
    let mut page = vec![0_u8; page_size as usize];
    page[0] = marker;
    page
}

fn main() -> Result<()> {
    let db = miri_db()?;
    let original = page_image(db.config().page_size, 0x11);
    let replacement = page_image(db.config().page_size, 0x22);

    db.begin_write()?;
    db.write_page(3, &original)?;
    db.commit()?;

    let snapshot = db.hold_snapshot()?;
    assert_eq!(db.storage_info()?.active_readers, 1);

    db.begin_write()?;
    db.write_page(3, &replacement)?;
    db.rollback()?;

    assert_eq!(db.read_page(3)?.to_vec(), original);
    assert_eq!(db.read_page_for_snapshot(snapshot, 3)?.to_vec(), original);

    db.release_snapshot(snapshot)?;
    assert_eq!(db.storage_info()?.active_readers, 0);

    Ok(())
}
