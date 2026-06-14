//! Performance regression tests for indexed equality on large checkpointed tables.
//!
//! These tests reproduce the Melodee DDB-002/DDB-003 scenario with a 1M+ row table
//! to verify that indexed equality queries are fast (< 1 second) even on the first
//! execution after database open.
//!
//! Root cause (fixed): When `defer_table_materialization` is true, secondary indexes
//! were not rebuilt on database open. Each index was lazily hydrated on first use,
//! which required reading the full table manifest from disk and rebuilding the BTreeMap.
//! For a 1M row table, this took ~12 seconds per index.
//!
//! Fix: Eagerly hydrate all indexes for deferred tables on database open. This makes
//! database open slower but ensures that all queries (including the first one) are
//! fast (sub-millisecond).

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

/// Regression test for DDB-002/DDB-003: indexed equality on a 1M row checkpointed
/// table must be fast (< 1 second) even on the first query execution.
///
/// This test verifies that:
/// 1. First query execution is fast (indexes are eagerly hydrated on open)
/// 2. Multiple indexes are all hydrated (not just the first one queried)
/// 3. Subsequent queries remain fast
#[test]
fn indexed_equality_on_1m_row_checkpointed_table_performance() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("large_1m.ddb");
    let path_str = db_path.to_str().unwrap();

    let row_count: i64 = 1_000_000;
    let target_row_id: i64 = 999_999;
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

        let mut txn = db.transaction().unwrap();
        let stmt = txn
            .prepare("INSERT INTO Artist VALUES ($1, $2, $3, $4, $5, $6)")
            .unwrap();
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
    }

    // Phase 2: Reopen and test indexed seek performance
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();

    // Test 1: First MusicBrainzIdRaw query (should be fast due to eager index hydration)
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE MusicBrainzIdRaw = '{target_mbid}' ORDER BY Id LIMIT 1"
        ))
        .unwrap();
    let elapsed_first = started.elapsed();

    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");
    let row = &result.rows()[0];
    assert_eq!(row.values()[2], Value::Text(target_mbid.clone()));

    eprintln!("First MusicBrainzIdRaw query: {:?}", elapsed_first);

    // Test 2: Second query execution (should also be fast)
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE MusicBrainzIdRaw = '{target_mbid}' ORDER BY Id LIMIT 1"
        ))
        .unwrap();
    let elapsed_second = started.elapsed();

    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");

    eprintln!("Second MusicBrainzIdRaw query: {:?}", elapsed_second);

    // Test 3: Multiple subsequent executions
    let mut elapsed_subsequent = Vec::new();
    for _ in 0..5 {
        let started = Instant::now();
        let result = db
            .execute(&format!(
                "SELECT * FROM Artist WHERE MusicBrainzIdRaw = '{target_mbid}' ORDER BY Id LIMIT 1"
            ))
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");
        elapsed_subsequent.push(elapsed);
    }

    eprintln!(
        "Subsequent MusicBrainzIdRaw queries: {:?}",
        elapsed_subsequent
    );

    // Test 4: NameNormalized query (different index, should also be fast)
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE NameNormalized = '{target_name}' ORDER BY SortName LIMIT 10"
        ))
        .unwrap();
    let elapsed_name = started.elapsed();

    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");

    eprintln!("NameNormalized query: {:?}", elapsed_name);

    // Performance assertions
    // With eager index hydration, ALL queries should be fast (< 1 second),
    // including the first query for each index.
    assert!(
        elapsed_first.as_secs_f64() < 1.0,
        "First MusicBrainzIdRaw query took {:?}; expected < 1 second with eager index hydration",
        elapsed_first
    );

    assert!(
        elapsed_second.as_secs_f64() < 1.0,
        "Second MusicBrainzIdRaw query took {:?}; expected < 1 second",
        elapsed_second
    );

    for (i, elapsed) in elapsed_subsequent.iter().enumerate() {
        assert!(
            elapsed.as_secs_f64() < 1.0,
            "Subsequent query {} took {:?}; expected < 1 second",
            i + 3,
            elapsed
        );
    }

    assert!(
        elapsed_name.as_secs_f64() < 1.0,
        "NameNormalized query (different index) took {:?}; expected < 1 second with eager index hydration",
        elapsed_name
    );
}

/// Test indexed equality without ORDER BY on a 500K row checkpointed table.
/// Verifies that the first query is fast due to eager index hydration on open.
#[test]
fn indexed_equality_without_order_by_is_fast() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("no_order_by.ddb");
    let path_str = db_path.to_str().unwrap();

    let row_count: i64 = 500_000;
    let target_row_id: i64 = 499_999;

    // Phase 1: Create, insert, checkpoint
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
        for i in 1..=row_count {
            stmt.execute_in(
                &mut txn,
                &[
                    Value::Int64(i),
                    Value::Text(format!("key_{i:08}")),
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

    // First query (should be fast due to eager index hydration)
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Item WHERE LookupKey = 'key_{target_row_id:08}'"
        ))
        .unwrap();
    let elapsed_first = started.elapsed();

    assert_eq!(result.rows().len(), 1);
    eprintln!("First query without ORDER BY: {:?}", elapsed_first);

    // Second query (should also be fast)
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Item WHERE LookupKey = 'key_{target_row_id:08}'"
        ))
        .unwrap();
    let elapsed_second = started.elapsed();

    assert_eq!(result.rows().len(), 1);
    eprintln!("Second query without ORDER BY: {:?}", elapsed_second);

    // Both queries should be fast (< 1 second) with eager index hydration
    assert!(
        elapsed_first.as_secs_f64() < 1.0,
        "First query without ORDER BY took {:?}; expected < 1 second with eager index hydration",
        elapsed_first
    );

    assert!(
        elapsed_second.as_secs_f64() < 1.0,
        "Second query without ORDER BY took {:?}; expected < 1 second",
        elapsed_second
    );
}
