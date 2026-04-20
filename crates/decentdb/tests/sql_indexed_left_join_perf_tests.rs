//! Performance regression tests for indexed equi-joins.
//!
//! These tests guard against the O(n*m) nested-loop fallback that occurs
//! when a LEFT JOIN on an indexed equi-key cannot use the indexed-probe
//! path that INNER JOIN uses. The Melodee MusicBrainz importer triggers
//! this shape (5-way join over staging tables, with two LEFT JOINs onto
//! indexed keys) and observed ~26s on 7447 driving rows before the fix.
//!
//! The thresholds here are deliberately loose (5s wall clock for ~40M
//! cross-pairs worth of work) so they only fire when the indexed path
//! has genuinely regressed to a full Cartesian scan. On the indexed
//! path the same query should complete well under a second.

use decentdb::{Db, DbConfig, Value};
use std::time::Instant;

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn scalar_i64(db: &Db, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap();
    match &r.rows()[0].values()[0] {
        Value::Int64(v) => *v,
        other => panic!("expected Int64 scalar, got {other:?}"),
    }
}

/// Populate the staging-shaped schema used by the Melodee importer.
///
/// * `r`   has `driver_count` rows with FK fan-out to the three inner
///   tables (rg, rgm, acp) and a 1:0-or-1 relation to `rc`.
/// * `rg`, `rgm`, `acp` each have `lookup_count` rows.
/// * `rc`  has approximately `driver_count * 2 / 3` rows (so the LEFT
///   JOIN produces both matched and unmatched outputs).
fn populate_staging(db: &Db, driver_count: i64, lookup_count: i64) {
    db.execute("CREATE TABLE r (id INT64 PRIMARY KEY, rg_id INT64 NOT NULL, ac_id INT64 NOT NULL, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE rg (rg_id INT64 PRIMARY KEY, rtype INT64)")
        .unwrap();
    db.execute("CREATE TABLE rc (id INT64 PRIMARY KEY, y INT64)")
        .unwrap();
    db.execute("CREATE TABLE rgm (rg_id INT64 PRIMARY KEY, y INT64)")
        .unwrap();
    db.execute("CREATE TABLE acp (ac_id INT64 PRIMARY KEY, artist_id INT64 NOT NULL)")
        .unwrap();
    db.execute("CREATE INDEX ix_r_rg ON r(rg_id)").unwrap();
    db.execute("CREATE INDEX ix_r_ac ON r(ac_id)").unwrap();

    // Fill lookup tables first so FKs resolve.
    for id in 1..=lookup_count {
        db.execute(&format!(
            "INSERT INTO rg (rg_id, rtype) VALUES ({id}, {})",
            id % 5
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO rgm (rg_id, y) VALUES ({id}, {})",
            1970 + (id % 50)
        ))
        .unwrap();
        db.execute(&format!(
            "INSERT INTO acp (ac_id, artist_id) VALUES ({id}, {})",
            1000 + id
        ))
        .unwrap();
    }

    for id in 1..=driver_count {
        let rg_id = ((id - 1) % lookup_count) + 1;
        let ac_id = ((id * 7 - 1) % lookup_count) + 1;
        db.execute(&format!(
            "INSERT INTO r (id, rg_id, ac_id, name) VALUES ({id}, {rg_id}, {ac_id}, 'row{id}')"
        ))
        .unwrap();
        if id % 3 != 0 {
            db.execute(&format!(
                "INSERT INTO rc (id, y) VALUES ({id}, {})",
                2000 + (id % 25)
            ))
            .unwrap();
        }
    }
}

/// Failing pre-fix: the Melodee 5-way shape with two LEFT JOINs on
/// indexed keys. Before the fix this took many seconds on even tiny
/// inputs because the LEFT JOINs fell back to full Cartesian nested
/// loops. After the fix the indexed probe path is used for LEFT JOINs
/// too.
#[test]
fn indexed_left_join_scales_linearly_not_quadratically() {
    let db = mem_db();
    // 2500 drivers * 800 lookups: an unindexed LEFT JOIN is already
    // 2M comparisons per level (4M across both LEFT JOINs). Before the
    // indexed LEFT JOIN fix, this took >8 seconds even in debug mode.
    // After the fix the probe path is used and the query completes in
    // well under a second.
    let driver_count: i64 = 2_500;
    let lookup_count: i64 = 800;
    populate_staging(&db, driver_count, lookup_count);

    let sql = r#"
        SELECT COUNT(*)
        FROM r
        INNER JOIN rg  ON rg.rg_id      = r.rg_id
        LEFT  JOIN rc  ON rc.id         = r.id
        LEFT  JOIN rgm ON rgm.rg_id     = r.rg_id
        INNER JOIN acp ON acp.ac_id     = r.ac_id
    "#;

    let started = Instant::now();
    let count = scalar_i64(&db, sql);
    let elapsed = started.elapsed();

    assert_eq!(
        count, driver_count,
        "5-way join must produce one row per driver row"
    );
    assert!(
        elapsed.as_secs_f64() < 5.0,
        "indexed LEFT JOIN regressed to O(n*m) nested loop; took {:?} for {} drivers × {} lookups",
        elapsed,
        driver_count,
        lookup_count
    );
}

/// Correctness: a LEFT JOIN on an indexed key must still emit NULL-
/// extended rows for unmatched left rows, regardless of which path the
/// planner picks.
#[test]
fn indexed_left_join_preserves_null_extended_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE driver (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE lookup (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO driver VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')")
        .unwrap();
    // Only 1 and 3 have lookup matches.
    db.execute("INSERT INTO lookup VALUES (1,'x'),(3,'z')")
        .unwrap();

    let r = db
        .execute(
            "SELECT d.id, l.label FROM driver d \
             LEFT JOIN lookup l ON l.id = d.id \
             ORDER BY d.id",
        )
        .unwrap();

    let got: Vec<(i64, Option<String>)> = r
        .rows()
        .iter()
        .map(|row| {
            let id = match &row.values()[0] {
                Value::Int64(v) => *v,
                other => panic!("expected Int64, got {other:?}"),
            };
            let label = match &row.values()[1] {
                Value::Null => None,
                Value::Text(t) => Some(t.clone()),
                other => panic!("expected Text or Null, got {other:?}"),
            };
            (id, label)
        })
        .collect();

    assert_eq!(
        got,
        vec![
            (1, Some("x".to_string())),
            (2, None),
            (3, Some("z".to_string())),
            (4, None),
        ]
    );
}

/// Correctness: a LEFT JOIN on an indexed key must produce multiple
/// output rows when the right side has multiple matches per key.
#[test]
fn indexed_left_join_handles_multi_match_right_side() {
    let db = mem_db();
    db.execute("CREATE TABLE driver (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("CREATE TABLE child (id INT64 PRIMARY KEY, driver_id INT64, tag TEXT)")
        .unwrap();
    db.execute("CREATE INDEX ix_child_driver_id ON child(driver_id)")
        .unwrap();
    db.execute("INSERT INTO driver VALUES (1,'a'),(2,'b')")
        .unwrap();
    db.execute("INSERT INTO child VALUES (10,1,'x'),(11,1,'y'),(12,1,'z')")
        .unwrap();
    // Driver 2 has no children.

    let r = db
        .execute(
            "SELECT d.id, c.tag FROM driver d \
             LEFT JOIN child c ON c.driver_id = d.id \
             ORDER BY d.id, c.tag",
        )
        .unwrap();

    let got: Vec<(i64, Option<String>)> = r
        .rows()
        .iter()
        .map(|row| {
            let id = match &row.values()[0] {
                Value::Int64(v) => *v,
                other => panic!("expected Int64, got {other:?}"),
            };
            let tag = match &row.values()[1] {
                Value::Null => None,
                Value::Text(t) => Some(t.clone()),
                other => panic!("expected Text or Null, got {other:?}"),
            };
            (id, tag)
        })
        .collect();

    assert_eq!(
        got,
        vec![
            (1, Some("x".to_string())),
            (1, Some("y".to_string())),
            (1, Some("z".to_string())),
            (2, None),
        ]
    );
}
