//! Performance regression tests for indexed equality on large checkpointed tables.
//!
//! These tests reproduce the Melodee DDB-002/DDB-003 scenario with a 1M+ row table
//! to verify that database open stays fast and indexed equality queries are reused
//! across short-lived connections in the same process.
//!
//! Root cause (fixed): An earlier DDB-002/DDB-003 fix eagerly hydrated every
//! deferred table index during database open. That made the first indexed query
//! fast, but moved multi-million-row index rebuild work into every connection
//! open and regressed general DecentDB benchmarks.
//!
//! Fix: database open remains lazy. The first query for a specific deferred
//! secondary index may populate that one runtime index, then a bounded process
//! cache reuses the index and paged-row locator cache for subsequent connections.

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

fn strict_wall_clock_regression_assertions_enabled() -> bool {
    std::env::var_os("DECENTDB_SANITIZER").is_none()
}

/// Regression test for DDB-002/DDB-003: indexed equality on a 1M row checkpointed
/// table must not make database open rebuild every deferred secondary index.
///
/// This test verifies that:
/// 1. Reopen is fast because indexes are not eagerly hydrated.
/// 2. First use of one secondary index is bounded.
/// 3. The same indexed query is fast after another reopen in the same process.
/// 4. A different index is hydrated independently instead of eagerly at open.
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

    // Phase 2: Reopen and test indexed seek performance. Opening must not
    // rebuild every deferred secondary index.
    let started_open = Instant::now();
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();
    let elapsed_open = started_open.elapsed();
    eprintln!("Reopen without eager index hydration: {:?}", elapsed_open);

    // First use of an index may populate that one runtime index.
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

    // A second execution on the same connection must be fast.
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE MusicBrainzIdRaw = '{target_mbid}' ORDER BY Id LIMIT 1"
        ))
        .unwrap();
    let elapsed_second = started.elapsed();

    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");

    eprintln!("Second MusicBrainzIdRaw query: {:?}", elapsed_second);

    drop(db);

    // A new connection in the same process should reuse the cached runtime
    // index and paged-row locator cache instead of rebuilding them.
    let started_reopen = Instant::now();
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();
    let elapsed_reopen = started_reopen.elapsed();
    eprintln!(
        "Second reopen with cached runtime index: {:?}",
        elapsed_reopen
    );

    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE MusicBrainzIdRaw = '{target_mbid}' ORDER BY Id LIMIT 1"
        ))
        .unwrap();
    let elapsed_cached_reopen = started.elapsed();
    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");
    eprintln!(
        "MusicBrainzIdRaw query after reopen/cache hit: {:?}",
        elapsed_cached_reopen
    );

    // A different index is hydrated independently, then immediately reused.
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE NameNormalized = '{target_name}' ORDER BY SortName LIMIT 10"
        ))
        .unwrap();
    let elapsed_name = started.elapsed();

    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");

    eprintln!("NameNormalized query: {:?}", elapsed_name);

    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Artist WHERE NameNormalized = '{target_name}' ORDER BY SortName LIMIT 10"
        ))
        .unwrap();
    let elapsed_name_second = started.elapsed();
    assert_eq!(result.rows().len(), 1, "expected exactly 1 matching row");
    eprintln!("Second NameNormalized query: {:?}", elapsed_name_second);

    if strict_wall_clock_regression_assertions_enabled() {
        assert!(
            elapsed_open.as_secs_f64() < 1.0,
            "reopen took {:?}; expected no eager all-index hydration during open",
            elapsed_open
        );
        assert!(
            elapsed_first.as_secs_f64() < 30.0,
            "first MusicBrainzIdRaw hydration took {:?}; expected bounded single-index hydration",
            elapsed_first
        );
        assert!(
            elapsed_second.as_secs_f64() < 1.0,
            "second MusicBrainzIdRaw query took {:?}; expected runtime index reuse",
            elapsed_second
        );
        assert!(
            elapsed_reopen.as_secs_f64() < 1.0,
            "second reopen took {:?}; expected no eager all-index hydration",
            elapsed_reopen
        );
        assert!(
            elapsed_cached_reopen.as_secs_f64() < 1.0,
            "MusicBrainzIdRaw query after reopen took {:?}; expected process-cache reuse",
            elapsed_cached_reopen
        );
        assert!(
            elapsed_name.as_secs_f64() < 30.0,
            "NameNormalized first hydration took {:?}; expected bounded single-index hydration",
            elapsed_name
        );
        assert!(
            elapsed_name_second.as_secs_f64() < 1.0,
            "second NameNormalized query took {:?}; expected runtime index reuse",
            elapsed_name_second
        );
    }
}

/// Test indexed equality without ORDER BY on a 500K row checkpointed table.
/// Verifies that the first query hydrates the requested index and the second
/// query reuses it without ORDER BY.
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
    let started_open = Instant::now();
    let db = Db::open_or_create(path_str, large_table_config()).unwrap();
    let elapsed_open = started_open.elapsed();
    eprintln!("Reopen without ORDER BY test: {:?}", elapsed_open);

    // First query hydrates only the requested index.
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Item WHERE LookupKey = 'key_{target_row_id:08}'"
        ))
        .unwrap();
    let elapsed_first = started.elapsed();

    assert_eq!(result.rows().len(), 1);
    eprintln!("First query without ORDER BY: {:?}", elapsed_first);

    // Second query reuses the runtime index.
    let started = Instant::now();
    let result = db
        .execute(&format!(
            "SELECT * FROM Item WHERE LookupKey = 'key_{target_row_id:08}'"
        ))
        .unwrap();
    let elapsed_second = started.elapsed();

    assert_eq!(result.rows().len(), 1);
    eprintln!("Second query without ORDER BY: {:?}", elapsed_second);

    if strict_wall_clock_regression_assertions_enabled() {
        assert!(
            elapsed_open.as_secs_f64() < 1.0,
            "reopen took {:?}; expected no eager all-index hydration",
            elapsed_open
        );
        assert!(
            elapsed_first.as_secs_f64() < 30.0,
            "First query without ORDER BY took {:?}; expected bounded single-index hydration",
            elapsed_first
        );
        assert!(
            elapsed_second.as_secs_f64() < 1.0,
            "Second query without ORDER BY took {:?}; expected runtime index reuse",
            elapsed_second
        );
    }
}
