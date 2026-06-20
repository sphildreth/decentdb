use decentdb::{Db, DbConfig, DbEncryptionConfig, Value};

fn tde_config() -> DbConfig {
    DbConfig {
        encryption: Some(
            DbEncryptionConfig::from_key_bytes(b"test-only-32-byte-local-tde-key-material")
                .expect("valid key"),
        ),
        ..DbConfig::default()
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn encrypted_database_hides_header_and_payload_and_reopens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secure.ddb");
    let config = tde_config();

    {
        let db = Db::create(&path, config.clone()).expect("create encrypted db");
        db.execute("CREATE TABLE secrets (id INT PRIMARY KEY, tenant TEXT, secret TEXT)")
            .expect("create table");
        db.execute(
            "INSERT INTO secrets (id, tenant, secret) VALUES (1, 'tenant-a', 'ssn-111-22-3333')",
        )
        .expect("insert row");
        let result = db
            .execute("SELECT secret FROM secrets WHERE tenant = 'tenant-a'")
            .expect("select row");
        assert_eq!(
            result.rows()[0].values(),
            &[Value::Text("ssn-111-22-3333".to_string())]
        );
    }

    let db_bytes = std::fs::read(&path).expect("read encrypted db");
    assert!(db_bytes.starts_with(b"DDBTDE1\0"));
    assert!(!contains_bytes(&db_bytes, b"DECENTDB"));
    assert!(!contains_bytes(&db_bytes, b"ssn-111-22-3333"));

    let header = Db::read_header_info_with_config(&path, &config).expect("encrypted header");
    assert_eq!(header.magic_hex, "444543454e5444420000000000000000");

    let db = Db::open(&path, config).expect("reopen encrypted db");
    let result = db
        .execute("SELECT id, tenant, secret FROM secrets")
        .expect("read reopened row");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("tenant-a".to_string()),
            Value::Text("ssn-111-22-3333".to_string()),
        ]
    );
}

#[test]
fn encrypted_database_requires_the_correct_key() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secure.ddb");
    let config = tde_config();

    Db::create(&path, config).expect("create encrypted db");

    let missing_key = Db::open(&path, DbConfig::default()).expect_err("missing key should fail");
    assert!(
        missing_key.to_string().contains("database is encrypted"),
        "{missing_key}"
    );

    let wrong_config = DbConfig {
        encryption: Some(DbEncryptionConfig::from_key_bytes(b"wrong-key").expect("valid key")),
        ..DbConfig::default()
    };
    let wrong_key = Db::open(&path, wrong_config).expect_err("wrong key should fail");
    assert!(
        wrong_key.to_string().contains("supplied encryption key"),
        "{wrong_key}"
    );
}

#[test]
fn encrypted_save_as_preserves_tde() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source = dir.path().join("source.ddb");
    let dest = dir.path().join("backup.ddb");
    let config = tde_config();

    let db = Db::create(&source, config.clone()).expect("create encrypted db");
    db.execute("CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
        .expect("create table");
    db.execute("INSERT INTO docs (id, body) VALUES (1, 'backup-secret')")
        .expect("insert row");
    db.save_as(&dest).expect("encrypted save_as");
    drop(db);

    let backup_bytes = std::fs::read(&dest).expect("read backup");
    assert!(backup_bytes.starts_with(b"DDBTDE1\0"));
    assert!(!contains_bytes(&backup_bytes, b"backup-secret"));

    let reopened = Db::open(&dest, config).expect("open encrypted backup");
    let result = reopened
        .execute("SELECT body FROM docs WHERE id = 1")
        .expect("read backup");
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("backup-secret".to_string())]
    );
}

#[test]
fn encrypted_database_wrong_key_open_does_not_corrupt_file_for_future_recovery() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("secure.ddb");
    let config = tde_config();

    {
        let db = Db::create(&path, config.clone()).expect("create encrypted db");
        db.execute("CREATE TABLE secrets (id INT64 PRIMARY KEY, secret TEXT)")
            .expect("create table");
        db.execute("INSERT INTO secrets (id, secret) VALUES (1, 'top-secret')")
            .expect("insert encrypted secret");
    }

    let wrong_key_config = DbConfig {
        encryption: Some(
            DbEncryptionConfig::from_key_bytes(b"wrong-key-material-32-bytes-long")
                .expect("valid wrong key"),
        ),
        ..DbConfig::default()
    };
    let wrong_open = Db::open(&path, wrong_key_config).expect_err("wrong key should fail");
    assert!(
        wrong_open.to_string().contains("supplied encryption key"),
        "expected wrong-key rejection details, got {wrong_open}"
    );

    let db = Db::open(&path, config).expect("reopen with correct key");
    let recovered = db
        .execute("SELECT id, secret FROM secrets WHERE id = 1")
        .expect("recover after wrong-key attempt");
    assert_eq!(
        recovered.rows()[0].values(),
        &[Value::Int64(1), Value::Text("top-secret".to_string())]
    );
}
