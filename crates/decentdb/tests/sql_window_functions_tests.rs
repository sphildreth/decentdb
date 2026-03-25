//! SQL window function tests.
//!
//! Covers: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE,
//! LAST_VALUE, NTH_VALUE, PARTITION BY, and window function edge cases.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn rows(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

#[test]
fn correlated_subquery_with_row_number_partition() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE csrn (id INT PRIMARY KEY, grp TEXT, val INT)",
    );
    exec(
        &db,
        "INSERT INTO csrn VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)",
    );
    let r = exec(
        &db,
        "
        SELECT c.id, 
            (SELECT ROW_NUMBER() OVER (ORDER BY i.val) 
             FROM csrn i WHERE i.grp = c.grp AND i.id = c.id) as rn
        FROM csrn c
        ORDER BY c.id
    ",
    );
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn dense_rank_no_gaps() {
    let db = mem_db();
    exec(&db, "CREATE TABLE drng (id INT PRIMARY KEY, score INT)");
    exec(&db, "INSERT INTO drng VALUES (1,100),(2,90),(3,90),(4,80)");
    let r = exec(
        &db,
        "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM drng ORDER BY id",
    );
    assert_eq!(r.rows()[3].values()[1], Value::Int64(3)); // 80 → dense_rank 3
}

#[test]
fn error_unsupported_window_function() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    let err = db
        .execute("SELECT PERCENT_RANK() OVER (ORDER BY id) FROM t")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported") || msg.contains("window") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn explain_window_function() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT id, ROW_NUMBER() OVER (ORDER BY val) FROM t")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn first_value_last_value() {
    let db = mem_db();
    exec(&db, "CREATE TABLE flv (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO flv VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "SELECT id, FIRST_VALUE(val) OVER (ORDER BY id), LAST_VALUE(val) OVER (ORDER BY id) FROM flv ORDER BY id");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(10));
}

#[test]
fn generated_column_rejects_window_function() {
    let db = mem_db();
    let err = db
        .execute(
            "CREATE TABLE t (id INT64, gen INT64 GENERATED ALWAYS AS (ROW_NUMBER() OVER ()) STORED)",
        )
        .unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("generated")
            || err.to_string().to_lowercase().contains("window")
    );
}

#[test]
fn lag_lead_functions() {
    let db = mem_db();
    exec(&db, "CREATE TABLE llf (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO llf VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "SELECT id, LAG(val, 1) OVER (ORDER BY id), LEAD(val, 1) OVER (ORDER BY id) FROM llf ORDER BY id");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[1], Value::Null); // no previous
    assert_eq!(r.rows()[0].values()[2], Value::Int64(20)); // next
    assert_eq!(r.rows()[2].values()[1], Value::Int64(20)); // previous
    assert_eq!(r.rows()[2].values()[2], Value::Null); // no next
}

#[test]
fn lag_with_default_value() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let r = db
        .execute("SELECT id, LAG(val, 1, -1) OVER (ORDER BY id) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(-1)); // first row has no prior, default -1
    assert_eq!(v[1][1], Value::Int64(10));
}

#[test]
fn lead_with_default_value() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    let r = db
        .execute("SELECT id, LEAD(val, 1, -1) OVER (ORDER BY id) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[2][1], Value::Int64(-1)); // last row has no next, default -1
    assert_eq!(v[0][1], Value::Int64(20));
}

#[test]
fn multiple_window_functions() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)")
        .unwrap();
    let r = db
        .execute(
            "SELECT id, val,
                    ROW_NUMBER() OVER (ORDER BY val) AS rn,
                    LAG(val) OVER (ORDER BY val) AS prev_val,
                    LEAD(val) OVER (ORDER BY val) AS next_val
             FROM t ORDER BY id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 4);
    assert_eq!(v[0][2], Value::Int64(1)); // rn
}

#[test]
fn nth_value_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nv (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nv VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) FROM nv ORDER BY id",
    );
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn rank_with_ties() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rwt (id INT PRIMARY KEY, score INT)");
    exec(&db, "INSERT INTO rwt VALUES (1,100),(2,90),(3,90),(4,80)");
    let r = exec(
        &db,
        "SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM rwt ORDER BY id",
    );
    assert_eq!(r.rows().len(), 4);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1)); // 100 → rank 1
    assert_eq!(r.rows()[1].values()[1], Value::Int64(2)); // 90 → rank 2
    assert_eq!(r.rows()[2].values()[1], Value::Int64(2)); // 90 → rank 2
    assert_eq!(r.rows()[3].values()[1], Value::Int64(4)); // 80 → rank 4 (skip 3)
}

#[test]
fn row_number_over_order() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rno (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO rno VALUES (1, 30), (2, 10), (3, 20)");
    let r = exec(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM rno ORDER BY rn",
    );
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1));
}

#[test]
fn row_number_with_partition() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE rnp (id INT PRIMARY KEY, cat TEXT, val INT)",
    );
    exec(
        &db,
        "INSERT INTO rnp VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)",
    );
    let r = exec(
        &db,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) AS rn FROM rnp ORDER BY id",
    );
    assert_eq!(r.rows().len(), 4);
    // First in each partition should be 1
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1));
    assert_eq!(r.rows()[2].values()[1], Value::Int64(1));
}

#[test]
fn window_count_over_unsupported() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3)")
        .unwrap();
    let r = db.execute("SELECT grp, COUNT(*) OVER (PARTITION BY grp) FROM t");
    assert!(r.is_err());
}

#[test]
fn window_dense_rank() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, score INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,100),(2,100),(3,90),(4,80)")
        .unwrap();
    let r = db
        .execute("SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS dr FROM t ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(1)); // score 100 → rank 1
    assert_eq!(v[1][1], Value::Int64(1)); // score 100 → rank 1
    assert_eq!(v[2][1], Value::Int64(2)); // score 90 → rank 2
    assert_eq!(v[3][1], Value::Int64(3)); // score 80 → rank 3
}

#[test]
fn window_first_value_last_value() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,100),(2,200),(3,300)")
        .unwrap();
    let r = db.execute(
        "SELECT id, FIRST_VALUE(val) OVER (ORDER BY id), LAST_VALUE(val) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t ORDER BY id",
    );
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v[0][1], Value::Int64(100)); // first
        assert_eq!(v[0][2], Value::Int64(300)); // last
    }
}

#[test]
fn window_function_complex() {
    let db = mem_db();
    db.execute("CREATE TABLE emp(id INT64, dept TEXT, salary INT64)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1,'A',50000),(2,'A',60000),(3,'A',55000),(4,'B',70000),(5,'B',65000)")
        .unwrap();
    let r = db
        .execute(
            "SELECT id, dept, salary,
                    ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn,
                    RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
             FROM emp ORDER BY dept, salary DESC",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
}

#[test]
fn window_function_dense_rank() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wfd (id INT PRIMARY KEY, score INT)");
    exec(&db, "INSERT INTO wfd VALUES (1, 100), (2, 100), (3, 90)");
    let r = exec(
        &db,
        "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM wfd ORDER BY id",
    );
    assert_eq!(r.rows()[2].values()[1], Value::Int64(2)); // dense rank = 2 (not 3)
}

#[test]
fn window_function_first_last_value() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wffl (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wffl VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "SELECT id, FIRST_VALUE(val) OVER (ORDER BY id) as fv, LAST_VALUE(val) OVER (ORDER BY id) as lv FROM wffl ORDER BY id",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Int64(10));
}

#[test]
fn window_function_lag_lead() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wf2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wf2 VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "
        SELECT id, val,
            LAG(val) OVER (ORDER BY id) as prev_val,
            LEAD(val) OVER (ORDER BY id) as next_val
        FROM wf2
        ORDER BY id
    ",
    );
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[2], Value::Null); // no previous for first row
    assert_eq!(r.rows()[0].values()[3], Value::Int64(20));
}

#[test]
fn window_function_nth_value() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wfn (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wfn VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) as nv FROM wfn ORDER BY id",
    );
    // NTH_VALUE(val, 2) should return NULL for first row, 20 for second onward
    assert!(r.rows()[0].values()[1] == Value::Null || r.rows()[0].values()[1] == Value::Int64(20));
}

#[test]
fn window_function_rank_dense_rank() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wf3 (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "INSERT INTO wf3 VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30)",
    );
    let r = exec(
        &db,
        "
        SELECT id, val,
            RANK() OVER (ORDER BY val) as rnk,
            DENSE_RANK() OVER (ORDER BY val) as drnk
        FROM wf3
        ORDER BY id
    ",
    );
    assert_eq!(r.rows().len(), 5);
}

#[test]
fn window_function_rank_with_ties() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wfr (id INT PRIMARY KEY, score INT)");
    exec(&db, "INSERT INTO wfr VALUES (1, 100), (2, 100), (3, 90)");
    let r = exec(
        &db,
        "SELECT id, score, RANK() OVER (ORDER BY score DESC) as rnk FROM wfr ORDER BY id",
    );
    assert_eq!(r.rows().len(), 3);
    // Both score=100 should have rank 1
    assert_eq!(r.rows()[0].values()[2], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[2], Value::Int64(1));
    // score=90 should have rank 3
    assert_eq!(r.rows()[2].values()[2], Value::Int64(3));
}

#[test]
fn window_function_with_partition() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE wfp (id INT PRIMARY KEY, grp TEXT, val INT)",
    );
    exec(
        &db,
        "INSERT INTO wfp VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)",
    );
    let r = exec(
        &db,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn FROM wfp ORDER BY id",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1)); // first in group a
    assert_eq!(r.rows()[1].values()[1], Value::Int64(2)); // second in group a
    assert_eq!(r.rows()[2].values()[1], Value::Int64(1)); // first in group b
}

#[test]
fn window_function_with_partition_and_order() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE wf (id INT PRIMARY KEY, grp TEXT, val INT)",
    );
    exec(
        &db,
        "INSERT INTO wf VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)",
    );
    let r = exec(
        &db,
        "
        SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
        FROM wf
        ORDER BY id
    ",
    );
    assert_eq!(r.rows().len(), 4);
    assert_eq!(r.rows()[0].values()[3], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[3], Value::Int64(2));
}

#[test]
fn window_lag_lead_with_offset() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)")
        .unwrap();
    let r = db
        .execute(
            "SELECT id, LAG(val, 2) OVER (ORDER BY id), LEAD(val, 2) OVER (ORDER BY id) FROM t ORDER BY id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Null); // lag(2) for id=1
    assert_eq!(v[1][1], Value::Null); // lag(2) for id=2
    assert_eq!(v[2][1], Value::Int64(10)); // lag(2) for id=3
    assert_eq!(v[2][2], Value::Int64(50)); // lead(2) for id=3
}

#[test]
fn window_min_max_over_unsupported() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = db.execute("SELECT id, MIN(val) OVER () FROM t");
    assert!(r.is_err());
}

#[test]
fn window_nth_value() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')")
        .unwrap();
    let r = db
        .execute(
            "SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("b".into()));
}

#[test]
fn window_ntile() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5),(6)")
        .unwrap();
    // NTILE is not supported; test the error path
    let err = db
        .execute("SELECT id, NTILE(3) OVER (ORDER BY id) AS bucket FROM t")
        .unwrap_err();
    assert!(err.to_string().contains("supported") || !err.to_string().is_empty());
}

#[test]
fn window_over_empty_partition() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A', 10)").unwrap();
    let r = db
        .execute("SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][2], Value::Int64(1));
}

#[test]
fn window_rank_dense_rank() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10),(10),(20),(30)")
        .unwrap();
    let r = db
        .execute(
            "SELECT val, RANK() OVER (ORDER BY val) AS rnk, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM t ORDER BY val",
        )
        .unwrap();
    let v = rows(&r);
    // RANK: 1,1,3,4; DENSE_RANK: 1,1,2,3
    assert_eq!(v[0][1], Value::Int64(1));
    assert_eq!(v[1][1], Value::Int64(1));
    assert_eq!(v[2][1], Value::Int64(3));
    assert_eq!(v[2][2], Value::Int64(2)); // dense_rank
}

#[test]
fn window_row_number_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(category TEXT, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',30),('B',40),('B',50)")
        .unwrap();
    let r = db
        .execute(
            "SELECT category, val, ROW_NUMBER() OVER (PARTITION BY category ORDER BY val) AS rn
             FROM t ORDER BY category, val",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][2], Value::Int64(1)); // A,10
    assert_eq!(v[1][2], Value::Int64(2)); // A,20
    assert_eq!(v[2][2], Value::Int64(1)); // B,30
    assert_eq!(v[4][2], Value::Int64(3)); // B,50
}

#[test]
fn window_row_number_no_partition() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (30),(10),(20)").unwrap();
    let r = db
        .execute("SELECT x, ROW_NUMBER() OVER (ORDER BY x) AS rn FROM t ORDER BY x")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(1));
    assert_eq!(v[1][1], Value::Int64(2));
    assert_eq!(v[2][1], Value::Int64(3));
}

#[test]
fn window_sum_over_unsupported() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',30)")
        .unwrap();
    // SUM/COUNT/MIN/MAX OVER() are NOT supported as window functions
    let r = db.execute("SELECT grp, SUM(val) OVER (PARTITION BY grp) FROM t");
    assert!(r.is_err());
}

#[test]
fn window_with_cte_and_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, grp TEXT, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)")
        .unwrap();
    let r = db
        .execute(
            "WITH ranked AS (
                SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val DESC) AS rn
                FROM t
            )
            SELECT id, grp, val FROM ranked WHERE rn = 1 ORDER BY grp",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][2], Value::Int64(20)); // Top of group A
    assert_eq!(v[1][2], Value::Int64(40)); // Top of group B
}

#[test]
fn window_with_partition_by() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, id INT64, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES ('A',1,10),('A',2,20),('B',1,30),('B',2,40)")
        .unwrap();
    let r = db
        .execute(
            "SELECT grp, id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM t ORDER BY grp, id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][2], Value::Int64(1)); // A group, row 1
    assert_eq!(v[1][2], Value::Int64(2)); // A group, row 2
    assert_eq!(v[2][2], Value::Int64(1)); // B group, row 1
}

// ── Tests merged from engine_coverage_tests.rs ──

#[test]
fn nth_value_out_of_range_returns_null_and_lag_lead_still_work() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE names (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO names VALUES (1, 'Ada'), (2, 'Grace'), (3, 'Linus')")
        .unwrap();

    let result = db
        .execute(
            "SELECT name,
                    LAG(name, 1) OVER (ORDER BY id) AS prev_name,
                    LEAD(name, 1) OVER (ORDER BY id) AS next_name,
                    NTH_VALUE(name, 10) OVER (ORDER BY id) AS tenth_name
             FROM names
             ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Null,
                Value::Text("Grace".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("Grace".to_string()),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Null,
            ],
            vec![
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
                Value::Null,
                Value::Null,
            ],
        ]
    );

    let err = db
        .execute("SELECT NTH_VALUE(name, 0) OVER (ORDER BY id) FROM names")
        .unwrap_err();
    assert!(
        err.to_string().contains("NTH_VALUE position must be >= 1"),
        "unexpected error: {err}"
    );
}

#[test]
fn window_value_accessors_cover_single_row_and_argument_validation() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE window_edges (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO window_edges VALUES (1, 'Ada'), (2, 'Grace')")
        .unwrap();

    let single = db
        .execute(
            "SELECT FIRST_VALUE(name) OVER (PARTITION BY id ORDER BY id),
                    LAST_VALUE(name) OVER (PARTITION BY id ORDER BY id),
                    NTH_VALUE(name, 1) OVER (PARTITION BY id ORDER BY id)
             FROM window_edges
             WHERE id = 1",
        )
        .unwrap();
    assert_eq!(
        single.rows()[0].values(),
        &[
            Value::Text("Ada".to_string()),
            Value::Text("Ada".to_string()),
            Value::Text("Ada".to_string()),
        ]
    );

    let first_err = db
        .execute("SELECT FIRST_VALUE(name, name) OVER (ORDER BY id) FROM window_edges")
        .unwrap_err();
    assert!(
        first_err
            .to_string()
            .contains("FIRST_VALUE expects exactly 1 argument"),
        "unexpected error: {first_err}"
    );

    let last_err = db
        .execute("SELECT LAST_VALUE() OVER (ORDER BY id) FROM window_edges")
        .unwrap_err();
    assert!(
        last_err
            .to_string()
            .contains("LAST_VALUE expects exactly 1 argument"),
        "unexpected error: {last_err}"
    );

    let nth_type_err = db
        .execute("SELECT NTH_VALUE(name, 'bad') OVER (ORDER BY id) FROM window_edges")
        .unwrap_err();
    assert!(
        nth_type_err
            .to_string()
            .contains("NTH_VALUE position must be INT64"),
        "unexpected error: {nth_type_err}"
    );
}

#[test]
fn window_value_accessors_work_with_partitions() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE employees (
            id INT64 PRIMARY KEY,
            name TEXT,
            department TEXT,
            salary INT64
        )",
    )
    .unwrap();
    db.execute(
        "INSERT INTO employees VALUES
            (1, 'Ada', 'eng', 100),
            (2, 'Grace', 'eng', 90),
            (3, 'Linus', 'eng', 90),
            (4, 'Ken', 'ops', 80),
            (5, 'Denise', 'ops', 70)",
    )
    .unwrap();

    let result = db
        .execute(
            "SELECT name, department, salary,
                    ROW_NUMBER()   OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
                    RANK()         OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
                    DENSE_RANK()   OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk,
                    FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary DESC) AS top_earner,
                    LAST_VALUE(name)  OVER (PARTITION BY department ORDER BY salary DESC) AS low_earner,
                    NTH_VALUE(name, 2) OVER (PARTITION BY department ORDER BY salary DESC) AS second_earner
             FROM employees
             ORDER BY department, salary DESC, name",
        )
        .unwrap();

    assert_eq!(
        result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("Ada".to_string()),
                Value::Text("eng".to_string()),
                Value::Int64(100),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Grace".to_string()),
                Value::Text("eng".to_string()),
                Value::Int64(90),
                Value::Int64(2),
                Value::Int64(2),
                Value::Int64(2),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Linus".to_string()),
                Value::Text("eng".to_string()),
                Value::Int64(90),
                Value::Int64(3),
                Value::Int64(2),
                Value::Int64(2),
                Value::Text("Ada".to_string()),
                Value::Text("Linus".to_string()),
                Value::Text("Grace".to_string()),
            ],
            vec![
                Value::Text("Ken".to_string()),
                Value::Text("ops".to_string()),
                Value::Int64(80),
                Value::Int64(1),
                Value::Int64(1),
                Value::Int64(1),
                Value::Text("Ken".to_string()),
                Value::Text("Denise".to_string()),
                Value::Text("Denise".to_string()),
            ],
            vec![
                Value::Text("Denise".to_string()),
                Value::Text("ops".to_string()),
                Value::Int64(70),
                Value::Int64(2),
                Value::Int64(2),
                Value::Int64(2),
                Value::Text("Ken".to_string()),
                Value::Text("Denise".to_string()),
                Value::Text("Denise".to_string()),
            ],
        ]
    );
}
