use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

#[test]
fn fulltext_match_and_bm25_rank_results() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    create_docs(&db);

    let result = db
        .execute(
            "SELECT id, bm25('idx_docs_search') AS rank \
             FROM docs \
             WHERE fulltext_match('idx_docs_search', 'rust OR database') \
             ORDER BY rank DESC, id",
        )
        .expect("query fulltext");

    assert_eq!(result.columns(), &["id", "rank"]);
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(result.rows()[1].values()[0], Value::Int64(2));
    let Value::Float64(first_rank) = result.rows()[0].values()[1] else {
        panic!("expected first rank");
    };
    let Value::Float64(second_rank) = result.rows()[1].values()[1] else {
        panic!("expected second rank");
    };
    assert!(first_rank > second_rank);
}

#[test]
fn fulltext_match_bm25_limit_uses_same_top_row_as_full_query() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    create_docs(&db);

    let limited = db
        .execute(
            "SELECT id, title, bm25('idx_docs_search') AS rank \
             FROM docs \
             WHERE fulltext_match('idx_docs_search', 'rust OR database') \
             ORDER BY rank DESC \
             LIMIT 1",
        )
        .expect("limited fulltext query");
    let full = db
        .execute(
            "SELECT id, title, bm25('idx_docs_search') AS rank \
             FROM docs \
             WHERE fulltext_match('idx_docs_search', 'rust OR database') \
             ORDER BY rank DESC",
        )
        .expect("full fulltext query");

    assert_eq!(limited.columns(), &["id", "title", "rank"]);
    assert_eq!(limited.rows().len(), 1);
    assert_eq!(limited.rows()[0], full.rows()[0]);
}

#[test]
fn fulltext_prefix_phrase_update_delete_and_verify_work() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    create_docs(&db);

    let prefix = db
        .execute(
            "SELECT id FROM docs \
             WHERE fulltext_match('idx_docs_search', 'dec*') \
             ORDER BY id",
        )
        .expect("prefix query");
    assert_eq!(ids(&prefix), vec![1]);

    let phrase = db
        .execute(
            "SELECT id FROM docs \
             WHERE fulltext_match('idx_docs_search', '\"database search\"') \
             ORDER BY id",
        )
        .expect("phrase query");
    assert_eq!(ids(&phrase), vec![1]);

    db.execute("UPDATE docs SET body = 'Rust extension hooks' WHERE id = 2")
        .expect("update doc");
    let updated = db
        .execute(
            "SELECT id FROM docs \
             WHERE fulltext_match('idx_docs_search', 'rust') \
             ORDER BY id",
        )
        .expect("updated query");
    assert_eq!(ids(&updated), vec![1, 2]);

    db.execute("DELETE FROM docs WHERE id = 1")
        .expect("delete doc");
    let deleted = db
        .execute(
            "SELECT id FROM docs \
             WHERE fulltext_match('idx_docs_search', 'rust') \
             ORDER BY id",
        )
        .expect("deleted query");
    assert_eq!(ids(&deleted), vec![2]);

    db.execute("ALTER INDEX idx_docs_search VERIFY")
        .expect("verify index");
    db.execute("ALTER INDEX idx_docs_search REBUILD")
        .expect("rebuild index");
}

#[test]
fn fulltext_range_delete_removes_docs_from_match_search_and_bm25() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    create_docs(&db);
    db.execute(
        "INSERT INTO docs (id, title, body) VALUES \
         (4, 'Delete Range', 'bulk delete coverage'), \
         (5, 'Remaining Match', 'database search benchmark')",
    )
    .expect("insert extra docs");

    db.execute("DELETE FROM docs WHERE id BETWEEN 2 AND 4")
        .expect("delete range");

    let match_after_delete = db
        .execute(
            "SELECT id \
             FROM docs \
             WHERE fulltext_match('idx_docs_search', 'database') \
             ORDER BY id",
        )
        .expect("database match query");
    assert_eq!(ids(&match_after_delete), vec![1, 5]);

    let bm25_after_delete = db
        .execute(
            "SELECT id, bm25('idx_docs_search') AS rank \
             FROM docs \
             WHERE fulltext_match('idx_docs_search', 'database') \
             ORDER BY id",
        )
        .expect("bm25 database query");
    assert_eq!(ids(&bm25_after_delete), vec![1, 5]);
    for row in bm25_after_delete.rows() {
        match row.values().first() {
            Some(Value::Int64(id)) if *id == 1 || *id == 5 => {}
            other => panic!("expected remaining ids only, got {:?}", other),
        }
        let Value::Float64(rank) = row.values().get(1).expect("rank") else {
            panic!("expected float rank");
        };
        assert!(*rank > 0.0);
    }

    let deleted_matches = db
        .execute(
            "SELECT id FROM docs \
             WHERE fulltext_match('idx_docs_search', 'extensions') \
             ORDER BY id",
        )
        .expect("deleted row query");
    assert!(deleted_matches.rows().is_empty());

    db.execute("ALTER INDEX idx_docs_search VERIFY")
        .expect("verify index");
}

#[test]
fn fulltext_catalog_metadata_survives_reopen() {
    let tempdir = TempDir::new().expect("tempdir");
    let path = tempdir.path().join("fts.ddb");

    {
        let db = Db::open_or_create(&path, DbConfig::default()).expect("open db");
        create_docs(&db);
        db.checkpoint().expect("checkpoint");
    }

    let reopened = Db::open_or_create(&path, DbConfig::default()).expect("reopen db");
    let result = reopened
        .execute(
            "SELECT id FROM docs \
             WHERE fulltext_match('idx_docs_search', 'dec*') \
             ORDER BY id",
        )
        .expect("query reopened fulltext");
    assert_eq!(ids(&result), vec![1]);

    let dump = reopened.dump_sql().expect("dump sql");
    assert!(dump.contains("USING fulltext"));
    assert!(dump.contains("prefix = '2,3'"));
}

#[test]
fn fulltext_errors_are_specific() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).expect("open db");
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, body TEXT, n INT64)")
        .expect("create table");
    db.execute("CREATE INDEX idx_docs_body ON docs USING fulltext (body)")
        .expect("create fulltext index");
    db.execute("INSERT INTO docs (id, body, n) VALUES (1, 'alpha beta', 10)")
        .expect("insert row");

    let bm25_error = db
        .execute("SELECT bm25('idx_docs_body') FROM docs")
        .expect_err("bm25 should require fulltext_match");
    assert!(bm25_error
        .to_string()
        .contains("FTS semantic error: bm25 requires fulltext_match"));

    let prefix_error = db
        .execute("SELECT id FROM docs WHERE fulltext_match('idx_docs_body', 'al*')")
        .expect_err("prefix query should require prefix option");
    assert!(prefix_error.to_string().contains("FTS query error:"));

    let ddl_error = db
        .execute("CREATE INDEX idx_docs_n ON docs USING fulltext (n)")
        .expect_err("non-text fulltext index should fail");
    assert!(ddl_error.to_string().contains("FTS DDL error:"));
}

fn create_docs(db: &Db) {
    db.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, title TEXT, body TEXT)")
        .expect("create docs");
    db.execute(
        "CREATE INDEX idx_docs_search ON docs USING fulltext (title, body) \
         WITH (prefix = '2,3', diacritics = 'remove')",
    )
    .expect("create fulltext index");
    db.execute(
        "INSERT INTO docs (id, title, body) VALUES \
         (1, 'DecentDB Rust', 'embedded database search engine'), \
         (2, 'SQLite notes', 'database extensions'), \
         (3, 'Other', 'queue processing')",
    )
    .expect("insert docs");
}

fn ids(result: &decentdb::QueryResult) -> Vec<i64> {
    result
        .rows()
        .iter()
        .map(|row| match row.values().first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("expected int id, got {other:?}"),
        })
        .collect()
}
