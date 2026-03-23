use decentdb::{Db, DbConfig, DbError};
use std::fs;

#[test]
fn create_with_invalid_page_size_fails() {
    let path = std::env::temp_dir().join("decentdb-phase0-invalid-page-size.ddb");
    let config = DbConfig {
        page_size: 2048,
        ..DbConfig::default()
    };
    let result = Db::create(&path, config);
    assert!(result.is_err());
    if let Err(DbError::Internal { message }) = result {
        assert!(message.contains("unsupported page size"));
    } else {
        panic!("expected internal error about unsupported page size");
    }
    let _ = fs::remove_file(&path);
}
