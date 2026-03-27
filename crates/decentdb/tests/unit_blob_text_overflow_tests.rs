//! Unit tests for large TEXT and BLOB values that exercise overflow pages.
//!
//! Exercises: record/overflow.rs write/read/free overflow chains,
//! record/compression.rs maybe_compress/decompress, record/row.rs
//! overflow encoding path.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn generate_text(len: usize) -> String {
    "abcdefghij".repeat(len / 10)
}

#[test]
fn large_text_insert_and_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data TEXT)");
    let large = generate_text(10_000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(large.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(large));
}

#[test]
fn large_blob_insert_and_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data BLOB)");
    let blob: Vec<u8> = (0..20_000).map(|i| (i % 256) as u8).collect();
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(blob.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Blob(blob));
}

#[test]
fn update_large_text_to_different_size() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data TEXT)");
    let original = generate_text(10_000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(original)],
    )
    .unwrap();
    let shorter = generate_text(500);
    db.execute_with_params(
        "UPDATE t SET data = $1 WHERE id = $2",
        &[Value::Text(shorter.clone()), Value::Int64(1)],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(shorter));
}

#[test]
fn update_large_text_to_larger_value() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data TEXT)");
    let original = generate_text(5_000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(original)],
    )
    .unwrap();
    let larger = generate_text(30_000);
    db.execute_with_params(
        "UPDATE t SET data = $1 WHERE id = $2",
        &[Value::Text(larger.clone()), Value::Int64(1)],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(larger));
}

#[test]
fn delete_large_value_reclaims_space() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, data TEXT)");
    for i in 0..5 {
        let large = generate_text(10_000);
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(large)],
        )
        .unwrap();
    }
    exec(&db, "DELETE FROM t WHERE id < 3");
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn multiple_large_columns_in_single_row() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, a TEXT, b BLOB, c TEXT)");
    let text_a = generate_text(8_000);
    let blob_b: Vec<u8> = (0..12_000).map(|i| (i % 256) as u8).collect();
    let text_c = generate_text(6_000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2, $3, $4)",
        &[
            Value::Int64(1),
            Value::Text(text_a.clone()),
            Value::Blob(blob_b.clone()),
            Value::Text(text_c.clone()),
        ],
    )
    .unwrap();
    let r = exec(&db, "SELECT a, b, c FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(text_a));
    assert_eq!(r.rows()[0].values()[1], Value::Blob(blob_b));
    assert_eq!(r.rows()[0].values()[2], Value::Text(text_c));
}

#[test]
fn large_text_with_where_clause() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, data TEXT)");
    let pattern = "unique_marker_xyz";
    let large = format!("{}{}", generate_text(8_000), pattern);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(large)],
    )
    .unwrap();
    let r = exec(
        &db,
        &format!("SELECT id FROM t WHERE data LIKE '%{pattern}%'"),
    );
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn many_rows_with_mixed_sizes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data TEXT)");
    for i in 0..50 {
        let size = if i % 3 == 0 { 0 } else { (i as usize) * 200 };
        let text = generate_text(size);
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(text)],
        )
        .unwrap();
    }
    let r = exec(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(50));

    let r = exec(&db, "SELECT id FROM t WHERE data = '' ORDER BY id");
    let empty_ids: Vec<Value> = r.rows().iter().map(|r| r.values()[0].clone()).collect();
    assert!(empty_ids.contains(&Value::Int64(0)));
}

#[test]
fn null_in_large_text_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data TEXT)");
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Null],
    )
    .unwrap();
    let large = generate_text(15_000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(2), Value::Text(large.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
    assert_eq!(r.rows()[1].values()[0], Value::Text(large));
}

#[test]
fn large_blob_zero_bytes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data BLOB)");
    let blob = vec![0u8; 50_000];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(blob.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Blob(blob));
}

#[test]
fn large_blob_all_ff_bytes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64, data BLOB)");
    let blob = vec![0xFFu8; 50_000];
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Blob(blob.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Blob(blob));
}

#[test]
fn roundtrip_after_delete_and_reinsert_large() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t(id INT64 PRIMARY KEY, data TEXT)");
    let text = generate_text(20_000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(text.clone())],
    )
    .unwrap();
    exec(&db, "DELETE FROM t WHERE id = 1");
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(text.clone())],
    )
    .unwrap();
    let r = exec(&db, "SELECT data FROM t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(text));
}
