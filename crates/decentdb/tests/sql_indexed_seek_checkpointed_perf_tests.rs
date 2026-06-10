//! Regression tests for indexed seek performance on checkpointed/paged tables.
//!
//! These tests guard against the O(manifest_size) per-row lookup regression
//! that occurs when the paged row locator cache is not built for large tables.
//! The Melodee MusicBrainz validation observed ~3.5s for indexed equality on
//! a 2.9M row checkpointed Artist table, while the same query shape on a 493K
//! row ArtistAlias table took ~260ms.
//!
//! Root cause: the `DEFERRED_PAGED_ROW_LOCATOR_CACHE_MAX_ROWS` constant
//! (250,000) prevented the paged row locator cache from being built for tables
//! exceeding this limit. Without the cache, each row-by-id lookup from a
//! secondary index reads the entire paged table manifest from disk, making
//! indexed equality O(manifest_size) per row instead of O(1).
//!
//! Fix: remove the row count limit on the paged row locator cache. The cache
//! is now built for all tables that have btree indexes or row_id alias columns,
//! regardless of row count. The memory cost is ~40 bytes per row, which is
//! acceptable for tables that are actively queried through indexes.

use decentdb::{Db, DbConfig, Value};
use std::time::Instant;
use tempfile::TempDir;

fn large_table_config() -> DbConfig {
    DbConfig {
        paged_row_storage: true,
        defer_table_materialization: true,
        ..DbConfig::default()
    }
}

/// Regression test for DDB-002/DDB-003: indexed equality on a large
/// checkpointed table must use the paged row locator cache for O(1)
/// row-by-id lookups, not fall through to O(manifest_size) linear scan.
///
/// Creates a synthetic table with enough rows to exceed the old 250,000
/// paged row locator cache limit, checkpoints and reopens the database,
/// then verifies that indexed equality queries return correct results
/// within a bounded time.
#[test]
fn indexed_equality_on_large_checkpointed_table_is_bounded() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("large.ddb");
    let path_str = db_path.to_str().unwrap();

    let target_row_id: i64 = 280_001;
    let target_name = format!("NameNormalized_{target_row_id}");
    let target_mbid = format!("mbid-{target_row_id:08x}-fade-4beef-beef-{target_row_id:012x}");

    // Phase 1: Create table, insert data, checkpoint, and close
    {
        let db = Db::open_or_create(path_str, large_table_config()).unwrap();
        db.execute(
            "CREATE TABLE Artist (
                Id INT64 PRIMARY KEY,
                MusicBrainzArtistId INT64,
                MusicBrainzIdRaw TEXT,
                NameNormalized TEXT,
                SortName TEXT,
                AlternateNames TEXT
            )",
        )
        .unwrap();
        db.execute("CREATE INDEX IX_Artist_MusicBrainzIdRaw ON Artist(MusicBrainzIdRaw)")
            .unwrap();
        db.execute("CREATE INDEX IX_Artist_NameNormalized ON Artist(NameNormalized)")
            .unwrap();

        // Insert 300,000 rows using transactions (exceeds old 250,000 limit)
        let row_count: i64 = 300_000;
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare(
            "INSERT INTO Artist VALUES ($1, $2, $3, $4, $5, $6)"
        ).unwrap();
        for i in 1..=row_count {
            stmt.execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Int64(i),
                    Value::Text(format!("mbid-{i:08x}-fade-4beef-beef-{i:012x}")),
                    Value::Text(format!("NameNormalized_{i}")),
                    Value::Text(format!("SortName_{i}")),
                    Value::Text(format!("Alt1_{i};Alt2_{i};Alt3_{i}")),
                ],
            )
            .unwrap();
        }
        txn.commit().unwrap();

        db.checkpoint().expect("checkpoint");
        // db is dropped here when exiting the block scope
    }

    // Phase 2: Reopen and test indexed seek performance
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();

    let explain = db
        .execute(&format!(
            "EXPLAIN SELECT * FROM Artist WHERE NameNormalized = '{target_name}' ORDER BY SortName LIMIT 10"
        ))
        .unwrap();
    let explain_lines: Vec<String> = explain
        .explain_lines()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(
        explain_lines
            .iter()
            .any(|line| line.contains("IndexSeek") && line.contains("ix_artist_namenormalized")),
        "expected IndexSeek on ix_artist_namenormalized, got: {explain_lines:?}"
    );

    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE NameNormalized = '{target_name}' ORDER BY SortName LIMIT 10"
        ))
        .unwrap();
    let elapsed = started.elapsed();

    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");
    let row = &result.rows()[0];
    assert_eq!(row.values()[3], Value::Text(target_name.clone()));

    let started2 = Instant::now();
    let result2 = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE MusicBrainzIdRaw = '{target_mbid}' ORDER BY Id LIMIT 1"
        ))
        .unwrap();
    let elapsed2 = started2.elapsed();

    assert_eq!(result2.rows().len(), 1, "expected exactly 1 matching row");
    let row2 = &result2.rows()[0];
    assert_eq!(row2.values()[2], Value::Text(target_mbid.clone()));

    assert!(
        elapsed.as_secs_f64() < 5.0,
        "indexed NameNormalized equality on 300K-row checkpointed table took {elapsed:?}; \
         expected bounded paged-row-locator lookup, not O(manifest_size) scan"
    );
    assert!(
        elapsed2.as_secs_f64() < 5.0,
        "indexed MusicBrainzIdRaw equality on 300K-row checkpointed table took {elapsed2:?}; \
         expected bounded paged-row-locator lookup, not O(manifest_size) scan"
    );
    // TempDir is automatically cleaned up
}

/// Comparator: indexed equality on a small checkpointed table should also
/// be fast. This proves the indexed path works correctly for both small
/// and large tables after checkpoint/reopen.
#[test]
fn indexed_equality_on_small_checkpointed_table_is_correct() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("small.ddb");
    let path_str = db_path.to_str().unwrap();

    // Phase 1: Create, insert, checkpoint, close
    {
        let db = Db::open_or_create(path_str, large_table_config()).unwrap();
        db.execute(
            "CREATE TABLE Item (
                Id INT64 PRIMARY KEY,
                LookupKey TEXT,
                Payload TEXT
            )",
        )
        .unwrap();
        db.execute("CREATE INDEX IX_Item_LookupKey ON Item(LookupKey)")
            .unwrap();

        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO Item VALUES ($1, $2, $3)").unwrap();
        for i in 1..=10_000 {
            stmt.execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Text(format!("key_{i:06}")),
                    Value::Text(format!("payload_data_{i}")),
                ],
            )
            .unwrap();
        }
        txn.commit().unwrap();
        db.checkpoint().expect("checkpoint");
    }

    // Phase 2: Reopen and test
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();

    let result = db
        .execute("SELECT * FROM Item WHERE LookupKey = 'key_009999' ORDER BY Id LIMIT 1")
        .unwrap();
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values()[1],
        Value::Text("key_009999".to_string())
    );
    assert_eq!(
        result.rows()[0].values()[2],
        Value::Text("payload_data_9999".to_string())
    );

    let result_miss = db
        .execute("SELECT * FROM Item WHERE LookupKey = 'key_999999' ORDER BY Id LIMIT 1")
        .unwrap();
    assert_eq!(result_miss.rows().len(), 0);
    // TempDir is automatically cleaned up
}

/// Verifies that EXPLAIN reports IndexSeek for the indexed equality shape
/// on a checkpointed table, proving the planner correctly selects the
/// index path even when the table is deferred/paged.
#[test]
fn explain_reports_index_seek_for_checkpointed_table() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("explain.ddb");
    let path_str = db_path.to_str().unwrap();

    // Phase 1: Create, insert, checkpoint, close
    {
        let db = Db::open_or_create(path_str, large_table_config()).unwrap();
        db.execute(
            "CREATE TABLE Probe (
                Id INT64 PRIMARY KEY,
                Code TEXT,
                Label TEXT
            )",
        )
        .unwrap();
        db.execute("CREATE INDEX IX_Probe_Code ON Probe(Code)")
            .unwrap();

        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO Probe VALUES ($1, $2, $3)").unwrap();
        for i in 1..=5_000 {
            stmt.execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Text(format!("code_{i:06}")),
                    Value::Text(format!("label_{i}")),
                ],
            )
            .unwrap();
        }
        txn.commit().unwrap();
        db.checkpoint().expect("checkpoint");
    }

    // Phase 2: Reopen and test
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();

    let explain = db
        .execute("EXPLAIN SELECT * FROM Probe WHERE Code = 'code_002500' ORDER BY Id LIMIT 1")
        .unwrap();
    let lines: Vec<String> = explain
        .explain_lines()
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert!(
        lines
            .iter()
            .any(|line| line.contains("IndexSeek") && line.contains("ix_probe_code")),
        "expected IndexSeek on ix_probe_code, got: {lines:?}"
    );

    let result = db
        .execute("SELECT * FROM Probe WHERE Code = 'code_002500' ORDER BY Id LIMIT 1")
        .unwrap();
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].values()[1],
        Value::Text("code_002500".to_string())
    );
    // TempDir is automatically cleaned up
}
