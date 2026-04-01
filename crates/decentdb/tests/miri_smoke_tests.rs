//! Small representative engine tests kept fast enough for the nightly Miri job.
//!
//! These tests intentionally exercise a few broad public API paths without
//! trying to mirror the full `cargo test -p decentdb` corpus. The nightly
//! workflow runs this target under Miri, while the regular Rust test suite
//! still covers the full engine behavior matrix.

use decentdb::{BulkLoadOptions, Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).expect("open memory db")
}

fn rows(result: &QueryResult) -> Vec<Vec<Value>> {
    result
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn count_rows(db: &Db, sql: &str) -> i64 {
    match &rows(&db.execute(sql).expect("count query"))[0][0] {
        Value::Int64(value) => *value,
        other => panic!("expected INT64 count, got {other:?}"),
    }
}

#[test]
fn prepared_crud_roundtrip_stays_consistent() {
    let db = mem_db();
    db.execute(
        "CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT NOT NULL, qty INTEGER NOT NULL)",
    )
    .expect("create table");
    db.execute("CREATE INDEX idx_items_name ON items(name)")
        .expect("create index");

    let insert = db
        .prepare("INSERT INTO items VALUES ($1, $2, $3)")
        .expect("prepare insert");
    insert
        .execute(&[
            Value::Int64(1),
            Value::Text("alpha".to_string()),
            Value::Int64(3),
        ])
        .expect("insert first row");
    insert
        .execute(&[
            Value::Int64(2),
            Value::Text("beta".to_string()),
            Value::Int64(7),
        ])
        .expect("insert second row");

    db.execute_with_params(
        "UPDATE items SET qty = qty + $1 WHERE id = $2",
        &[Value::Int64(2), Value::Int64(1)],
    )
    .expect("update row");

    let select = db
        .prepare("SELECT id, qty FROM items WHERE name = $1")
        .expect("prepare select");
    assert_eq!(
        rows(
            &select
                .execute(&[Value::Text("alpha".to_string())])
                .expect("select by indexed column"),
        ),
        vec![vec![Value::Int64(1), Value::Int64(5)]],
    );

    db.execute_with_params("DELETE FROM items WHERE id = $1", &[Value::Int64(2)])
        .expect("delete row");
    assert_eq!(count_rows(&db, "SELECT COUNT(*) FROM items"), 1);
}

#[test]
fn exclusive_transaction_commit_and_rollback_roundtrip() {
    let db = mem_db();
    db.execute("CREATE TABLE ledger(id INTEGER PRIMARY KEY, note TEXT NOT NULL)")
        .expect("create table");

    {
        let mut txn = db.transaction().expect("begin exclusive transaction");
        let insert = txn
            .prepare("INSERT INTO ledger VALUES ($1, $2)")
            .expect("prepare insert");
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(1), Value::Text("draft".to_string())],
            )
            .expect("insert row in rollback txn");
        txn.rollback().expect("rollback transaction");
    }
    assert_eq!(count_rows(&db, "SELECT COUNT(*) FROM ledger"), 0);

    {
        let mut txn = db.transaction().expect("begin second transaction");
        let insert = txn
            .prepare("INSERT INTO ledger VALUES ($1, $2)")
            .expect("prepare insert");
        insert
            .execute_in(
                &mut txn,
                &[Value::Int64(1), Value::Text("posted".to_string())],
            )
            .expect("insert row in commit txn");
        txn.commit().expect("commit transaction");
    }

    assert_eq!(
        rows(
            &db.execute("SELECT note FROM ledger WHERE id = 1")
                .expect("select committed row"),
        ),
        vec![vec![Value::Text("posted".to_string())]],
    );
}

#[test]
fn bulk_load_roundtrip_with_large_values() {
    let db = mem_db();
    db.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT, payload BLOB)")
        .expect("create table");

    let large_body_a = "alpha".repeat(400);
    let large_body_b = "beta".repeat(500);
    let large_blob_a = vec![0xAB; 6_000];
    let large_blob_b = vec![0xCD; 8_000];
    let rows_to_load = vec![
        vec![
            Value::Int64(1),
            Value::Text("doc-a".to_string()),
            Value::Text(large_body_a.clone()),
            Value::Blob(large_blob_a.clone()),
        ],
        vec![
            Value::Int64(2),
            Value::Text("doc-b".to_string()),
            Value::Text(large_body_b.clone()),
            Value::Blob(large_blob_b.clone()),
        ],
    ];

    db.bulk_load_rows(
        "docs",
        &["id", "title", "body", "payload"],
        &rows_to_load,
        BulkLoadOptions::default(),
    )
    .expect("bulk load rows");

    assert_eq!(count_rows(&db, "SELECT COUNT(*) FROM docs"), 2);
    assert_eq!(
        rows(
            &db.execute("SELECT title, body, payload FROM docs WHERE id = 2")
                .expect("select loaded row"),
        ),
        vec![vec![
            Value::Text("doc-b".to_string()),
            Value::Text(large_body_b),
            Value::Blob(large_blob_b),
        ]],
    );
}
