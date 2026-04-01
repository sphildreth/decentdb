use decentdb::{Db, DbConfig, Result, WalSyncMode};

fn miri_db() -> Result<Db> {
    // Miri interprets every operation, so keep the cache at the minimum
    // effective size and disable sync-heavy durability work while still
    // exercising the real engine transaction and snapshot paths.
    let config = DbConfig {
        cache_size_mb: 0,
        wal_sync_mode: WalSyncMode::TestingOnlyUnsafeNoSync,
        ..DbConfig::default()
    };
    Db::open_or_create(":memory:", config)
}

fn main() -> Result<()> {
    eprintln!("miri_smoke: open db");
    let db = miri_db()?;

    eprintln!("miri_smoke: read header page");
    let header = db.read_page(1)?;
    assert_eq!(header.len(), db.config().page_size as usize);

    eprintln!("miri_smoke: commit empty write txn");
    db.begin_write()?;
    db.commit()?;

    eprintln!("miri_smoke: hold and release snapshot");
    let snapshot = db.hold_snapshot()?;
    assert_eq!(db.storage_info()?.active_readers, 1);
    db.release_snapshot(snapshot)?;
    assert_eq!(db.storage_info()?.active_readers, 0);

    eprintln!("miri_smoke: rollback empty write txn");
    db.begin_write()?;
    db.rollback()?;

    eprintln!("miri_smoke: done");
    Ok(())
}
