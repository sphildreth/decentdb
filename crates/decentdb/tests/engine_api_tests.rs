use decentdb::version;

#[test]
fn test_public_engine_version() {
    // This test lives in the `tests/` folder and can only test public APIs,
    // exactly like a user of the library would!
    let v = version();
    assert!(!v.is_empty());
}
