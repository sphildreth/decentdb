use decentdb::{Db, DbConfig, DbError};
use std::fs::{self, OpenOptions};
use std::io::Write;

#[test]
fn open_with_corrupt_magic_returns_corruption() {
    let path = std::env::temp_dir().join("decentdb-phase0-corrupt-magic.ddb");

    // First create a valid db
    let config = DbConfig::default();
    let db = Db::create(&path, config).expect("create should succeed");
    drop(db);

    // Corrupt the magic bytes (first 16 bytes)
    let mut file = OpenOptions::new().write(true).open(&path).unwrap();
    file.write_all(b"CORRUPT MAGIC!!!").unwrap();
    drop(file);

    // Try to open it
    let result = Db::open(&path, DbConfig::default());
    assert!(result.is_err());

    match result.unwrap_err() {
        DbError::Corruption { message } => {
            assert!(message.contains("invalid database header magic"))
        }
        other => panic!("expected corruption error, got {:?}", other),
    }

    let _ = fs::remove_file(&path);
}
