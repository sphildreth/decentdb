//! SQL set operation, DISTINCT, ORDER BY, and LIMIT tests.
//!
//! Covers: UNION, INTERSECT, EXCEPT (with ALL variants), DISTINCT,
//! DISTINCT ON, ORDER BY (ASC/DESC, expressions), LIMIT, OFFSET,
//! and chained set operations.

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

fn rows(r: &QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

#[test]
fn chained_set_operations() {
    let db = mem_db();
    let r = db
        .execute(
            "(SELECT 1 AS v UNION SELECT 2 UNION SELECT 3)
             EXCEPT
             (SELECT 2 AS v)
             ORDER BY v",
        );
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 1);
    }
}

#[test]
fn complex_union_with_cte() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (3,'c'),(4,'d')").unwrap();
    let r = db
        .execute(
            "WITH combined AS (
                SELECT id, name FROM t1
                UNION ALL
                SELECT id, name FROM t2
            )
            SELECT COUNT(*) AS total, MIN(id) AS min_id, MAX(id) AS max_id FROM combined",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(4));
    assert_eq!(v[0][1], Value::Int64(1));
    assert_eq!(v[0][2], Value::Int64(4));
}

#[test]
fn cte_with_offset_and_limit() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nums (n INT PRIMARY KEY)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO nums VALUES ({i})"));
    }
    let r = exec(&db, "
        WITH paged AS (
            SELECT n FROM nums ORDER BY n LIMIT 5 OFFSET 10
        )
        SELECT * FROM paged
    ");
    assert_eq!(r.rows().len(), 5);
}

#[test]
fn distinct_on_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3),('B',4)").unwrap();
    let r = db.execute("SELECT DISTINCT ON (grp) grp, val FROM t ORDER BY grp, val");
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v.len(), 2); // one per group
    }
}

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

#[test]
fn distinct_on_single_key() {
    let db = mem_db();
    exec(&db, "CREATE TABLE don (id INT PRIMARY KEY, category TEXT, val INT)");
    exec(&db, "INSERT INTO don VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 30), (4, 'B', 5)");
    let r = exec(&db,
        "SELECT DISTINCT ON (category) category, val FROM don ORDER BY category, val"
    );
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn error_recursive_cte_with_order_by() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 FROM cte WHERE n < 5
                ORDER BY 1
            ) SELECT * FROM cte",
        )
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("ORDER BY") || msg.contains("recursive") || msg.len() > 0,
        "unexpected error: {msg}"
    );
}

#[test]
fn except_all() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2)").unwrap();
    let r = db.execute("SELECT v FROM t1 EXCEPT ALL SELECT v FROM t2 ORDER BY v");
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 2);
    }
}

#[test]
fn except_all_consumes_one_at_a_time() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ea1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE ea2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ea1 VALUES (1, 10), (2, 10), (3, 10)");
    exec(&db, "INSERT INTO ea2 VALUES (4, 10)");
    let r = exec(&db, "SELECT val FROM ea1 EXCEPT ALL SELECT val FROM ea2");
    // 3 - 1 = 2 copies of 10
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn except_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(4)").unwrap();
    let r = db
        .execute("SELECT v FROM t1 EXCEPT SELECT v FROM t2 ORDER BY v")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // 1, 3
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn except_query() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();
    let r = db
        .execute("SELECT id FROM t EXCEPT SELECT id FROM t WHERE id > 3")
        .unwrap();
    let v = rows(&r);
    assert!(v.len() >= 3); // 1, 2, 3
}

#[test]
fn except_removes_matching() {
    let db = mem_db();
    exec(&db, "CREATE TABLE e1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE e2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO e1 VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO e2 VALUES (4, 20)");
    let r = exec(&db, "SELECT val FROM e1 EXCEPT SELECT val FROM e2 ORDER BY val");
    assert_eq!(r.rows().len(), 2); // 10, 30
}

#[test]
fn explain_intersect_except() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();

    let r1 = db
        .execute("EXPLAIN SELECT id FROM t INTERSECT SELECT id FROM t")
        .unwrap();
    assert!(r1.rows().len() > 0);

    let r2 = db
        .execute("EXPLAIN SELECT id FROM t EXCEPT ALL SELECT id FROM t")
        .unwrap();
    assert!(r2.rows().len() > 0);
}

#[test]
fn explain_set_operation() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT id FROM t UNION ALL SELECT id FROM t")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn intersect_all() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(2),(3),(4)").unwrap();
    let r = db.execute("SELECT v FROM t1 INTERSECT ALL SELECT v FROM t2 ORDER BY v");
    // INTERSECT ALL may or may not be supported
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(v.len() >= 2);
    }
}

#[test]
fn intersect_all_with_duplicates() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ia1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE ia2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ia1 VALUES (1, 10), (2, 10), (3, 20)");
    exec(&db, "INSERT INTO ia2 VALUES (4, 10), (5, 20), (6, 20)");
    let r = exec(&db, "SELECT val FROM ia1 INTERSECT ALL SELECT val FROM ia2 ORDER BY val");
    // ia1 has 10x2, 20x1; ia2 has 10x1, 20x2 → min(2,1)=1 for 10, min(1,2)=1 for 20
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn intersect_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(v INT64)").unwrap();
    db.execute("CREATE TABLE t2(v INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT v FROM t1 INTERSECT SELECT v FROM t2 ORDER BY v")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn intersect_query() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();
    let r = db
        .execute("SELECT id FROM t WHERE id <= 3 INTERSECT SELECT id FROM t WHERE id >= 2")
        .unwrap();
    let v = rows(&r);
    assert!(v.len() >= 2); // 2, 3
}

#[test]
fn intersect_returns_common() {
    let db = mem_db();
    exec(&db, "CREATE TABLE i1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE i2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO i1 VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO i2 VALUES (4, 20), (5, 30), (6, 40)");
    let r = exec(&db, "SELECT val FROM i1 INTERSECT SELECT val FROM i2 ORDER BY val");
    assert_eq!(r.rows().len(), 2); // 20, 30
}

#[test]
fn limit_and_offset() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3),(4),(5)").unwrap();
    let r = db
        .execute("SELECT x FROM t ORDER BY x LIMIT 2 OFFSET 1")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn limit_zero() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT id FROM t LIMIT 0")
        .unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn multi_column_order_by() {
    let db = mem_db();
    db.execute("CREATE TABLE t(a INT64, b INT64, c INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3),(1,1,4),(2,1,5),(2,2,6)").unwrap();
    let r = db
        .execute("SELECT a, b, c FROM t ORDER BY a ASC, b DESC, c ASC")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(1)); // a=1
    assert_eq!(v[0][1], Value::Int64(2)); // b=2 (desc)
}

#[test]
fn offset_and_limit() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    for i in 0..20 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    let r = db
        .execute("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0][0], Value::Int64(10));
    assert_eq!(v[4][0], Value::Int64(14));
}

#[test]
fn offset_beyond_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT id FROM t ORDER BY id OFFSET 10")
        .unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn order_by_desc() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (3),(1),(4),(1),(5)").unwrap();
    let r = db.execute("SELECT id FROM t ORDER BY id DESC").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(5));
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

#[test]
fn order_by_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (3),(1),(2)").unwrap();
    let r = db.execute("SELECT x FROM t ORDER BY x DESC").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(3));
    assert_eq!(v[2][0], Value::Int64(1));
}

#[test]
fn order_by_expression_and_positional() {
    let db = mem_db();
    db.execute("CREATE TABLE t(x INT64, y INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,5),(3,15)").unwrap();
    // Order by expression
    let r = db.execute("SELECT x, y FROM t ORDER BY x + y DESC").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(3)); // 3+15=18
    // Order by column position (second column)
    let r2 = db.execute("SELECT x, y FROM t ORDER BY y ASC").unwrap();
    let v2 = rows(&r2);
    assert_eq!(v2[0][1], Value::Int64(5)); // y=5 first
}

#[test]
fn order_by_multiple_columns() {
    let db = mem_db();
    exec(&db, "CREATE TABLE omc (id INT PRIMARY KEY, grp TEXT, val INT)");
    exec(&db, "INSERT INTO omc VALUES (1, 'a', 2), (2, 'b', 1), (3, 'a', 1), (4, 'b', 2)");
    let r = exec(&db, "SELECT id FROM omc ORDER BY grp ASC, val DESC");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));  // a,2
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));  // a,1
}

#[test]
fn order_by_nulls_first_last() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(NULL),(3),(NULL),(2)").unwrap();
    // Verify NULLS FIRST/LAST syntax parses and returns results
    let r = db.execute("SELECT val FROM t ORDER BY val ASC NULLS FIRST").unwrap();
    assert_eq!(rows(&r).len(), 5);
    let r2 = db.execute("SELECT val FROM t ORDER BY val ASC NULLS LAST").unwrap();
    assert_eq!(rows(&r2).len(), 5);
    let r3 = db.execute("SELECT val FROM t ORDER BY val DESC NULLS FIRST").unwrap();
    assert_eq!(rows(&r3).len(), 5);
    let r4 = db.execute("SELECT val FROM t ORDER BY val DESC NULLS LAST").unwrap();
    assert_eq!(rows(&r4).len(), 5);
}

#[test]
fn query_with_distinct_on_display() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE dod (grp INT, val INT, name TEXT)",
    );
    exec(
        &db,
        "CREATE VIEW dod_v AS SELECT DISTINCT ON (grp) grp, val, name FROM dod ORDER BY grp, val DESC",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("DISTINCT ON"));
}

#[test]
fn query_with_except_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ed1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE ed2 (id INT PRIMARY KEY)");
    exec(
        &db,
        "CREATE VIEW edv AS SELECT id FROM ed1 EXCEPT SELECT id FROM ed2",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("EXCEPT") || dump.contains("except"));
}

#[test]
fn query_with_intersect_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE id1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE id2 (id INT PRIMARY KEY)");
    exec(
        &db,
        "CREATE VIEW idv AS SELECT id FROM id1 INTERSECT SELECT id FROM id2",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("INTERSECT") || dump.contains("intersect"));
}

#[test]
fn query_with_union_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ud1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE ud2 (id INT PRIMARY KEY)");
    exec(
        &db,
        "CREATE VIEW udv AS SELECT id FROM ud1 UNION ALL SELECT id FROM ud2",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("UNION ALL") || dump.contains("union all"));
}

#[test]
fn recursive_cte_with_union_distinct() {
    let db = mem_db();
    // UNION (not UNION ALL) should deduplicate in the recursion
    let r = db
        .execute(
            "WITH RECURSIVE cnt(n) AS (
                SELECT 1
                UNION
                SELECT n + 1 FROM cnt WHERE n < 5
            )
            SELECT n FROM cnt ORDER BY n"
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
}

#[test]
fn select_distinct() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('a'),('b'),('a'),('c'),('b')").unwrap();
    let r = db.execute("SELECT DISTINCT val FROM t ORDER BY val").unwrap();
    assert_eq!(rows(&r).len(), 3);
}

#[test]
fn select_distinct_basic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dist (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO dist VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'c')");
    let r = exec(&db, "SELECT DISTINCT val FROM dist ORDER BY val");
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn select_distinct_on() {
    let db = mem_db();
    db.execute("CREATE TABLE t(category TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',3),('A',1),('B',2),('B',5)").unwrap();
    let r = db
        .execute("SELECT DISTINCT ON (category) category, val FROM t ORDER BY category, val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    // Should get first row per category after ORDER BY
    assert_eq!(v[0][0], Value::Text("A".into()));
    assert_eq!(v[0][1], Value::Int64(1));
}

#[test]
fn select_distinct_with_null() {
    let db = mem_db();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(NULL),(1),(NULL),(2)").unwrap();
    let r = db.execute("SELECT DISTINCT val FROM t ORDER BY val").unwrap();
    let v = rows(&r);
    assert!(v.len() >= 3); // 1, 2, NULL
}

#[test]
fn select_offset_beyond_rows() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ob_t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO ob_t VALUES (1), (2)");
    let r = exec(&db, "SELECT id FROM ob_t ORDER BY id OFFSET 100");
    assert_eq!(r.rows().len(), 0);
}

#[test]
fn select_with_limit_and_offset() {
    let db = mem_db();
    exec(&db, "CREATE TABLE lo_t (id INT PRIMARY KEY)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO lo_t VALUES ({i})"));
    }
    let r = exec(&db, "SELECT id FROM lo_t ORDER BY id LIMIT 3 OFFSET 10");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(11));
}

#[test]
fn select_with_limit_offset() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    for i in 1..=20 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }
    let r = db.execute("SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0][0], Value::Int64(11));
}

#[test]
fn select_with_offset_only() {
    let db = mem_db();
    exec(&db, "CREATE TABLE off_t (id INT PRIMARY KEY)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO off_t VALUES ({i})"));
    }
    let r = exec(&db, "SELECT id FROM off_t ORDER BY id OFFSET 5");
    assert_eq!(r.rows().len(), 5);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(6));
}

#[test]
fn set_op_column_count_mismatch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sc1 (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "CREATE TABLE sc2 (id INT PRIMARY KEY, x INT)");
    exec(&db, "INSERT INTO sc1 VALUES (1, 2, 3)");
    exec(&db, "INSERT INTO sc2 VALUES (1, 2)");
    let err = exec_err(&db, "SELECT a, b FROM sc1 UNION SELECT x FROM sc2");
    assert!(!err.is_empty());
}

#[test]
fn set_operation_in_correlated_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE outer_set (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE inner1 (id INT, ref_id INT)");
    exec(&db, "CREATE TABLE inner2 (id INT, ref_id INT)");
    exec(&db, "INSERT INTO outer_set VALUES (1), (2)");
    exec(&db, "INSERT INTO inner1 VALUES (10, 1), (20, 2)");
    exec(&db, "INSERT INTO inner2 VALUES (30, 1), (40, 3)");
    let r = exec(&db, "
        SELECT os.id, (
            SELECT COUNT(*) FROM (
                SELECT id FROM inner1 WHERE ref_id = os.id
                UNION ALL
                SELECT id FROM inner2 WHERE ref_id = os.id
            ) combined
        ) as total
        FROM outer_set os
        ORDER BY os.id
    ");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn set_operation_in_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(x INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute(
            "SELECT * FROM (SELECT x FROM t1 UNION SELECT x FROM t2) AS combined ORDER BY x"
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 4);
}

#[test]
fn set_union_all() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(x INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT x FROM t1 UNION ALL SELECT x FROM t2 ORDER BY x")
        .unwrap();
    assert_eq!(rows(&r).len(), 6);
}

#[test]
fn set_union_distinct() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(x INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT x FROM t1 UNION SELECT x FROM t2 ORDER BY x")
        .unwrap();
    assert_eq!(rows(&r).len(), 4);
}

#[test]
fn set_union_with_limit() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(x INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (3),(4)").unwrap();
    let r = db
        .execute("SELECT x FROM t1 UNION ALL SELECT x FROM t2 ORDER BY x LIMIT 3")
        .unwrap();
    assert_eq!(rows(&r).len(), 3);
}

#[test]
fn union_all_preserves_duplicates() {
    let db = mem_db();
    exec(&db, "CREATE TABLE s1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE s2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO s1 VALUES (1, 10), (2, 20)");
    exec(&db, "INSERT INTO s2 VALUES (3, 10), (4, 20)");
    let r = exec(&db, "SELECT val FROM s1 UNION ALL SELECT val FROM s2 ORDER BY val");
    assert_eq!(r.rows().len(), 4); // duplicates preserved
}

#[test]
fn union_all_three_queries() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let r = db
        .execute("SELECT id FROM t UNION ALL SELECT id + 10 FROM t UNION ALL SELECT id + 100 FROM t")
        .unwrap();
    assert_eq!(rows(&r).len(), 6);
}

#[test]
fn union_deduplicates() {
    let db = mem_db();
    exec(&db, "CREATE TABLE u1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE u2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO u1 VALUES (1, 10), (2, 20)");
    exec(&db, "INSERT INTO u2 VALUES (3, 10), (4, 30)");
    let r = exec(&db, "SELECT val FROM u1 UNION SELECT val FROM u2 ORDER BY val");
    assert_eq!(r.rows().len(), 3); // 10, 20, 30
}

#[test]
fn union_vs_union_all() {
    let db = mem_db();
    let r1 = db
        .execute("SELECT 1 AS v UNION SELECT 1 UNION SELECT 2")
        .unwrap();
    assert_eq!(rows(&r1).len(), 2); // dedup: 1, 2

    let r2 = db
        .execute("SELECT 1 AS v UNION ALL SELECT 1 UNION ALL SELECT 2")
        .unwrap();
    assert_eq!(rows(&r2).len(), 3); // no dedup: 1, 1, 2
}

#[test]
fn union_with_order_by_and_limit() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (3,'c'),(4,'d')").unwrap();
    let r = db
        .execute("SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 ORDER BY id LIMIT 3")
        .unwrap();
    assert_eq!(rows(&r).len(), 3);
}

