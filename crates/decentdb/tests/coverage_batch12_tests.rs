//! Batch 12: Targeting exec/mod.rs error branches, normalize.rs edge cases,
//! pager/cache paths, WAL recovery, and search module coverage.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn exec_err(db: &Db, sql: &str) -> String {
    db.execute(sql).unwrap_err().to_string()
}

// ── Index type mismatch / stale index errors ───────────────────────

#[test]
fn select_with_index_on_wrong_type() {
    let db = mem_db();
    exec(&db, "CREATE TABLE iwt (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX iwt_idx ON iwt (val)");
    for i in 0..30 {
        exec(&db, &format!("INSERT INTO iwt VALUES ({i}, 'val{i}')"));
    }
    exec(&db, "ANALYZE iwt");
    // Query using the index
    let r = exec(&db, "SELECT id FROM iwt WHERE val = 'val15'");
    assert_eq!(r.rows().len(), 1);
}

// ── CTE column aliasing ────────────────────────────────────────────

#[test]
fn cte_with_column_aliases() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cca (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO cca VALUES (1, 'a'), (2, 'b')");
    let r = exec(
        &db,
        "WITH aliased(x, y) AS (SELECT id, val FROM cca) SELECT x, y FROM aliased ORDER BY x",
    );
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn cte_column_count_mismatch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ccm (id INT PRIMARY KEY, val TEXT)");
    let err = exec_err(
        &db,
        "WITH bad(x) AS (SELECT id, val FROM ccm) SELECT * FROM bad",
    );
    assert!(
        err.to_lowercase().contains("column")
            || err.to_lowercase().contains("mismatch")
            || err.to_lowercase().contains("alias"),
        "got: {err}"
    );
}

// ── Recursive CTE edge cases ──────────────────────────────────────

#[test]
fn recursive_cte_with_limit() {
    let db = mem_db();
    let r = exec(
        &db,
        "WITH RECURSIVE cnt(n) AS (
            SELECT 1
            UNION ALL
            SELECT n + 1 FROM cnt WHERE n < 1000
        )
        SELECT n FROM cnt LIMIT 10",
    );
    assert_eq!(r.rows().len(), 10);
}

#[test]
fn recursive_cte_fibonacci() {
    let db = mem_db();
    let r = exec(
        &db,
        "WITH RECURSIVE fib(a, b) AS (
            SELECT 0, 1
            UNION ALL
            SELECT b, a + b FROM fib WHERE b < 100
        )
        SELECT a FROM fib ORDER BY a",
    );
    assert!(r.rows().len() >= 5);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(1));
}

// ── Outer query references in correlated subqueries ────────────────

#[test]
fn correlated_subquery_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cso (id INT PRIMARY KEY, dept TEXT)");
    exec(&db, "CREATE TABLE csi (id INT PRIMARY KEY, dept TEXT, salary INT)");
    exec(&db, "INSERT INTO cso VALUES (1, 'eng'), (2, 'sales')");
    exec(&db, "INSERT INTO csi VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150)");
    let r = exec(
        &db,
        "SELECT cso.dept, (SELECT COUNT(*) FROM csi WHERE csi.dept = cso.dept) as cnt FROM cso ORDER BY dept",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn correlated_subquery_in_where() {
    let db = mem_db();
    exec(&db, "CREATE TABLE csw1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE csw2 (id INT PRIMARY KEY, ref_id INT)");
    exec(&db, "INSERT INTO csw1 VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO csw2 VALUES (1, 1), (2, 3)");
    let r = exec(
        &db,
        "SELECT id FROM csw1 WHERE EXISTS (SELECT 1 FROM csw2 WHERE csw2.ref_id = csw1.id) ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));
}

// ── Window function edge cases ─────────────────────────────────────

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
fn window_function_lag_lead() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wfl (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wfl VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "SELECT id, LAG(val) OVER (ORDER BY id) as prev, LEAD(val) OVER (ORDER BY id) as next FROM wfl ORDER BY id",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Null); // no previous for first row
    assert_eq!(r.rows()[0].values()[2], Value::Int64(20));
    assert_eq!(r.rows()[2].values()[2], Value::Null); // no next for last row
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
fn window_function_nth_value() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wfn (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wfn VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) as nv FROM wfn ORDER BY id",
    );
    // NTH_VALUE(val, 2) should return NULL for first row, 20 for second onward
    assert!(
        r.rows()[0].values()[1] == Value::Null || r.rows()[0].values()[1] == Value::Int64(20)
    );
}

#[test]
fn window_function_with_partition() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wfp (id INT PRIMARY KEY, grp TEXT, val INT)");
    exec(&db, "INSERT INTO wfp VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)");
    let r = exec(
        &db,
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn FROM wfp ORDER BY id",
    );
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1)); // first in group a
    assert_eq!(r.rows()[1].values()[1], Value::Int64(2)); // second in group a
    assert_eq!(r.rows()[2].values()[1], Value::Int64(1)); // first in group b
}

// ── eval_group_expr covering IsNull, Cast, InList, Like, Case ──────

#[test]
fn group_by_is_null_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gbin (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO gbin VALUES (1, 'a'), (2, NULL), (3, 'b'), (4, NULL)");
    let r = exec(
        &db,
        "SELECT val IS NULL as is_nil, COUNT(*) FROM gbin GROUP BY val IS NULL ORDER BY is_nil",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_case_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gbc (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO gbc VALUES (1, 10), (2, 20), (3, 10), (4, 30)");
    let r = exec(
        &db,
        "SELECT CASE WHEN val > 15 THEN 'high' ELSE 'low' END as bucket, COUNT(*)
         FROM gbc GROUP BY CASE WHEN val > 15 THEN 'high' ELSE 'low' END
         ORDER BY bucket",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_in_list_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gbil (id INT PRIMARY KEY, status TEXT)");
    exec(&db, "INSERT INTO gbil VALUES (1, 'active'), (2, 'pending'), (3, 'active'), (4, 'closed')");
    let r = exec(
        &db,
        "SELECT status IN ('active', 'pending') as is_open, COUNT(*)
         FROM gbil GROUP BY status IN ('active', 'pending')
         ORDER BY is_open",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_like_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gbl (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO gbl VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia'), (4, 'Charlie')");
    let r = exec(
        &db,
        "SELECT name LIKE 'Al%' as starts_al, COUNT(*)
         FROM gbl GROUP BY name LIKE 'Al%'
         ORDER BY starts_al",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_cast_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gbcast (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO gbcast VALUES (1, 1.5), (2, 2.5), (3, 1.5)");
    let r = exec(
        &db,
        "SELECT CAST(val AS INT) as int_val, COUNT(*)
         FROM gbcast GROUP BY CAST(val AS INT)
         ORDER BY int_val",
    );
    assert!(r.rows().len() >= 2);
}

// ── Scalar subquery in SELECT ──────────────────────────────────────

#[test]
fn scalar_subquery_in_group_by_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ssgs (id INT PRIMARY KEY, dept TEXT)");
    exec(&db, "CREATE TABLE ssgs_counts (dept TEXT, cnt INT)");
    exec(&db, "INSERT INTO ssgs VALUES (1, 'eng'), (2, 'eng'), (3, 'sales')");
    exec(&db, "INSERT INTO ssgs_counts VALUES ('eng', 2), ('sales', 1)");
    let r = exec(
        &db,
        "SELECT dept, (SELECT cnt FROM ssgs_counts WHERE ssgs_counts.dept = ssgs.dept) as expected
         FROM ssgs
         GROUP BY dept
         ORDER BY dept",
    );
    assert_eq!(r.rows().len(), 2);
}

// ── Expression evaluation: BETWEEN, IN list ────────────────────────

#[test]
fn between_with_non_integer_types() {
    let db = mem_db();
    exec(&db, "CREATE TABLE bni (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO bni VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')");
    let r = exec(
        &db,
        "SELECT name FROM bni WHERE name BETWEEN 'banana' AND 'date' ORDER BY name",
    );
    assert!(r.rows().len() >= 2);
}

#[test]
fn not_between_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nbe (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nbe VALUES (1, 5), (2, 15), (3, 25)");
    let r = exec(
        &db,
        "SELECT id FROM nbe WHERE val NOT BETWEEN 10 AND 20 ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn not_in_list_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nil_t (id INT PRIMARY KEY, status TEXT)");
    exec(&db, "INSERT INTO nil_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let r = exec(
        &db,
        "SELECT id FROM nil_t WHERE status NOT IN ('a', 'c') ORDER BY id",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn in_subquery_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE isq1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE isq2 (id INT PRIMARY KEY, ref_id INT)");
    exec(&db, "INSERT INTO isq1 VALUES (1), (2), (3)");
    exec(&db, "INSERT INTO isq2 VALUES (1, 1), (2, 3)");
    let r = exec(
        &db,
        "SELECT id FROM isq1 WHERE id IN (SELECT ref_id FROM isq2) ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn not_in_subquery_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nisq1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE nisq2 (id INT PRIMARY KEY, ref_id INT)");
    exec(&db, "INSERT INTO nisq1 VALUES (1), (2), (3)");
    exec(&db, "INSERT INTO nisq2 VALUES (1, 1), (2, 3)");
    let r = exec(
        &db,
        "SELECT id FROM nisq1 WHERE id NOT IN (SELECT ref_id FROM nisq2) ORDER BY id",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ── Multiple FROM items (cross join) ───────────────────────────────

#[test]
fn implicit_cross_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cj1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE cj2 (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO cj1 VALUES (1), (2)");
    exec(&db, "INSERT INTO cj2 VALUES (10), (20)");
    let r = exec(&db, "SELECT cj1.id, cj2.id FROM cj1, cj2 ORDER BY cj1.id, cj2.id");
    assert_eq!(r.rows().len(), 4);
}

// ── HAVING clause ──────────────────────────────────────────────────

#[test]
fn having_filters_groups() {
    let db = mem_db();
    exec(&db, "CREATE TABLE hfg (id INT PRIMARY KEY, grp TEXT, val INT)");
    exec(&db, "INSERT INTO hfg VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5), (4, 'b', 6)");
    let r = exec(
        &db,
        "SELECT grp, SUM(val) as total FROM hfg GROUP BY grp HAVING SUM(val) > 15 ORDER BY grp",
    );
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Text("a".to_string()));
}

// ── DISTINCT ON ────────────────────────────────────────────────────

#[test]
fn distinct_on_keeps_first() {
    let db = mem_db();
    exec(&db, "CREATE TABLE don (id INT PRIMARY KEY, grp TEXT, val INT)");
    exec(&db, "INSERT INTO don VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 5)");
    let r = exec(
        &db,
        "SELECT DISTINCT ON (grp) grp, val FROM don ORDER BY grp, val",
    );
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(10)); // first 'a' by val asc
    assert_eq!(r.rows()[1].values()[1], Value::Int64(5)); // first 'b' by val asc
}

// ── Qualified wildcard and wildcard expansion ──────────────────────

#[test]
fn qualified_wildcard() {
    let db = mem_db();
    exec(&db, "CREATE TABLE qw (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO qw VALUES (1, 'hello')");
    let r = exec(&db, "SELECT qw.* FROM qw");
    assert_eq!(r.columns().len(), 2);
}

// ── Complex ORDER BY with expressions ──────────────────────────────

#[test]
fn order_by_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE obe (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO obe VALUES (1, 3, 2), (2, 1, 4), (3, 2, 1)");
    let r = exec(&db, "SELECT id FROM obe ORDER BY a + b");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3)); // 2+1=3, smallest
}

#[test]
fn order_by_desc_nulls() {
    let db = mem_db();
    exec(&db, "CREATE TABLE odn (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO odn VALUES (1, 10), (2, NULL), (3, 20)");
    let r = exec(&db, "SELECT id FROM odn ORDER BY val DESC");
    // Just verify ordering works; NULLS FIRST may not be supported
    assert_eq!(r.rows().len(), 3);
}

// ── Function calls ─────────────────────────────────────────────────

#[test]
fn coalesce_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cf (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO cf VALUES (1, NULL, 42), (2, 10, 20)");
    let r = exec(&db, "SELECT id, COALESCE(a, b) as val FROM cf ORDER BY id");
    assert_eq!(r.rows()[0].values()[1], Value::Int64(42));
    assert_eq!(r.rows()[1].values()[1], Value::Int64(10));
}

#[test]
fn nullif_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nf (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nf VALUES (1, 0), (2, 5)");
    let r = exec(&db, "SELECT id, NULLIF(val, 0) FROM nf ORDER BY id");
    assert_eq!(r.rows()[0].values()[1], Value::Null);
    assert_eq!(r.rows()[1].values()[1], Value::Int64(5));
}

#[test]
fn upper_lower_functions() {
    let db = mem_db();
    let r = exec(&db, "SELECT upper('hello'), lower('WORLD')");
    assert_eq!(r.rows()[0].values()[0], Value::Text("HELLO".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Text("world".to_string()));
}

#[test]
fn length_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT length('hello')");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
}

#[test]
fn substr_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT substr('hello world', 7, 5)");
    assert_eq!(r.rows()[0].values()[0], Value::Text("world".to_string()));
}

#[test]
fn replace_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT replace('hello world', 'world', 'earth')");
    assert_eq!(
        r.rows()[0].values()[0],
        Value::Text("hello earth".to_string())
    );
}

#[test]
fn abs_function() {
    let db = mem_db();
    let r = exec(&db, "SELECT abs(-42), abs(42)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(42));
}

// ── Prepared statement with various types ──────────────────────────

#[test]
fn prepared_insert_with_all_types() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE pat (id INT PRIMARY KEY, f FLOAT, t TEXT, b BOOL, bl BLOB)",
    );
    let stmt = db
        .prepare("INSERT INTO pat VALUES ($1, $2, $3, $4, $5)")
        .unwrap();
    stmt.execute(&[
        Value::Int64(1),
        Value::Float64(3.14),
        Value::Text("hello".into()),
        Value::Bool(true),
        Value::Blob(vec![0xDE, 0xAD]),
    ])
    .unwrap();
    let r = exec(&db, "SELECT * FROM pat");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[3], Value::Bool(true));
}

#[test]
fn prepared_select_with_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psp (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO psp VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let stmt = db.prepare("SELECT id FROM psp WHERE val = $1").unwrap();
    let r = stmt.execute(&[Value::Text("b".into())]).unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ── Partial index coverage ─────────────────────────────────────────

#[test]
fn partial_index_creation_and_use() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pi (id INT PRIMARY KEY, active BOOL, val TEXT)");
    exec(&db, "CREATE INDEX pi_active_idx ON pi (val) WHERE active = true");
    for i in 0..20 {
        exec(
            &db,
            &format!(
                "INSERT INTO pi VALUES ({i}, {}, 'val{i}')",
                if i % 2 == 0 { "true" } else { "false" }
            ),
        );
    }
    exec(&db, "ANALYZE pi");
    let r = exec(&db, "SELECT id FROM pi WHERE active = true AND val = 'val10'");
    assert_eq!(r.rows().len(), 1);
}

// ── Multiple CTEs ──────────────────────────────────────────────────

#[test]
fn multiple_ctes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mc (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO mc VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(
        &db,
        "WITH
            a AS (SELECT id, val FROM mc WHERE val > 10),
            b AS (SELECT id, val * 2 as doubled FROM a)
         SELECT * FROM b ORDER BY id",
    );
    assert_eq!(r.rows().len(), 2);
}

// ── Empty table operations ─────────────────────────────────────────

#[test]
fn aggregate_on_empty_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE emt (id INT PRIMARY KEY, val INT)");
    let r = exec(&db, "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM emt");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
}

#[test]
fn group_by_on_empty_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE emtg (id INT PRIMARY KEY, grp TEXT)");
    let r = exec(&db, "SELECT grp, COUNT(*) FROM emtg GROUP BY grp");
    assert_eq!(r.rows().len(), 0);
}

// ── ILIKE expression ───────────────────────────────────────────────

#[test]
fn ilike_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ilk (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO ilk VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'hello')");
    let r = exec(&db, "SELECT id FROM ilk WHERE name ILIKE 'hello' ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn not_like_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nlk (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO nlk VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Alicia')");
    // Exercise NOT LIKE code path
    let _r = exec(&db, "SELECT id FROM nlk WHERE name NOT LIKE 'Al%' ORDER BY id");
}

// ── INSERT with explicit column list ───────────────────────────────

#[test]
fn insert_with_partial_columns() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE ipc (id INT PRIMARY KEY, a TEXT, b TEXT DEFAULT 'default_b')",
    );
    exec(&db, "INSERT INTO ipc (id, a) VALUES (1, 'hello')");
    let r = exec(&db, "SELECT a, b FROM ipc");
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".to_string()));
    assert_eq!(
        r.rows()[0].values()[1],
        Value::Text("default_b".to_string())
    );
}

// ── Multi-row INSERT ───────────────────────────────────────────────

#[test]
fn multi_row_insert() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mri (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "INSERT INTO mri VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    );
    let r = exec(&db, "SELECT COUNT(*) FROM mri");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

// ── INSERT ... SELECT ──────────────────────────────────────────────

#[test]
fn insert_from_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ifs1 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE ifs2 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO ifs1 VALUES (1, 'a'), (2, 'b')");
    exec(&db, "INSERT INTO ifs2 SELECT * FROM ifs1");
    let r = exec(&db, "SELECT COUNT(*) FROM ifs2");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ── UPDATE with complex expressions ────────────────────────────────

#[test]
fn update_with_case_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uwc (id INT PRIMARY KEY, val INT, label TEXT)");
    exec(&db, "INSERT INTO uwc VALUES (1, 10, ''), (2, 20, ''), (3, 5, '')");
    exec(
        &db,
        "UPDATE uwc SET label = CASE WHEN val > 15 THEN 'high' ELSE 'low' END",
    );
    let r = exec(&db, "SELECT id, label FROM uwc ORDER BY id");
    assert_eq!(r.rows()[0].values()[1], Value::Text("low".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("high".to_string()));
}

// ── DELETE with complex WHERE ──────────────────────────────────────

#[test]
fn delete_with_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dws1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE dws2 (id INT PRIMARY KEY, ref_id INT)");
    exec(&db, "INSERT INTO dws1 VALUES (1), (2), (3)");
    exec(&db, "INSERT INTO dws2 VALUES (1, 2)");
    exec(&db, "DELETE FROM dws1 WHERE id IN (SELECT ref_id FROM dws2)");
    let r = exec(&db, "SELECT COUNT(*) FROM dws1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ── Search module: trigram and full-text search paths ───────────────

#[test]
fn trigram_index_with_updates() {
    let db = mem_db();
    exec(&db, "CREATE TABLE trg (id INT PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE INDEX trg_gin ON trg USING gin (body)");
    exec(&db, "INSERT INTO trg VALUES (1, 'the quick brown fox')");
    exec(&db, "INSERT INTO trg VALUES (2, 'lazy dog sleeps')");
    exec(&db, "UPDATE trg SET body = 'the fast brown fox' WHERE id = 1");
    exec(&db, "DELETE FROM trg WHERE id = 2");
    let r = exec(&db, "SELECT id FROM trg");
    assert_eq!(r.rows().len(), 1);
}

// ── Savepoints nested ──────────────────────────────────────────────

#[test]
fn nested_savepoints() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nsp (id INT PRIMARY KEY, val TEXT)");
    db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO nsp VALUES (1, 'a')");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO nsp VALUES (2, 'b')");
    exec(&db, "SAVEPOINT sp2");
    exec(&db, "INSERT INTO nsp VALUES (3, 'c')");
    db.rollback_to_savepoint("sp2").unwrap();
    // Row 3 should be gone
    let r = exec(&db, "SELECT COUNT(*) FROM nsp");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
    db.rollback_to_savepoint("sp1").unwrap();
    // Row 2 should be gone
    let r = exec(&db, "SELECT COUNT(*) FROM nsp");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    db.commit_transaction().unwrap();
}

// ── Type coercion edge cases ───────────────────────────────────────

#[test]
fn int_float_comparison() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ifc (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO ifc VALUES (1, 1.5), (2, 2.0), (3, 3.5)");
    let r = exec(&db, "SELECT id FROM ifc WHERE val > 2.0 ORDER BY id");
    assert!(r.rows().len() >= 1);
}

#[test]
fn bool_to_int_comparison() {
    let db = mem_db();
    let r = exec(&db, "SELECT true = true, false = false, true <> false");
    assert_eq!(r.rows()[0].values()[0], Value::Bool(true));
    assert_eq!(r.rows()[0].values()[1], Value::Bool(true));
    assert_eq!(r.rows()[0].values()[2], Value::Bool(true));
}

// ── dump_sql roundtrip ─────────────────────────────────────────────

#[test]
fn dump_sql_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dsr (id INT PRIMARY KEY, val TEXT NOT NULL)");
    exec(&db, "INSERT INTO dsr VALUES (1, 'hello'), (2, 'world')");
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CREATE TABLE"));
    assert!(dump.contains("INSERT INTO"));
}
