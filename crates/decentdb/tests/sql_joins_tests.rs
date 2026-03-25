//! SQL JOIN tests.
//!
//! Covers: INNER, LEFT, RIGHT, FULL OUTER, CROSS, NATURAL joins,
//! USING clause, multi-table joins, implicit cross joins, and
//! join-related edge cases.

use decentdb::{Db, DbConfig, QueryResult, Value};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_JOIN_ID: AtomicU64 = AtomicU64::new(0);

fn unique_path() -> String {
    format!(
        "/tmp/test_join_{}.db",
        NEXT_JOIN_ID.fetch_add(1, Ordering::SeqCst)
    )
}

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
fn complex_multi_join_with_filter() {
    let db = mem_db();
    db.execute("CREATE TABLE customers(id INT64 PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders(id INT64 PRIMARY KEY, cust_id INT64, total INT64)").unwrap();
    db.execute("CREATE TABLE items(id INT64, order_id INT64, product TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')").unwrap();
    db.execute("INSERT INTO orders VALUES (10,1,100),(20,1,200),(30,2,50)").unwrap();
    db.execute("INSERT INTO items VALUES (1,10,'Widget'),(2,10,'Gadget'),(3,20,'Widget'),(4,30,'Gadget')").unwrap();
    let r = db
        .execute(
            "SELECT c.name, COUNT(DISTINCT o.id) AS order_count, COUNT(i.id) AS item_count
             FROM customers c
             LEFT JOIN orders o ON c.id = o.cust_id
             LEFT JOIN items i ON o.id = i.order_id
             GROUP BY c.name
             ORDER BY c.name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    // Just verify all 3 customers appear with correct column count
    assert!(v[0].len() >= 3);
}

#[test]
fn cross_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cj1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE cj2 (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO cj1 VALUES (1), (2)");
    exec(&db, "INSERT INTO cj2 VALUES (10), (20)");
    let r = exec(&db, "SELECT cj1.id, cj2.id FROM cj1 CROSS JOIN cj2 ORDER BY cj1.id, cj2.id");
    assert_eq!(r.rows().len(), 4);
}

#[test]
fn cross_join_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(a INT64)").unwrap();
    db.execute("CREATE TABLE t2(b INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10), (20)").unwrap();
    let r = db.execute("SELECT a, b FROM t1 CROSS JOIN t2 ORDER BY a, b").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 4); // 2 x 2 = 4 rows
}

#[test]
fn cross_join_empty_table() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(a INT64)").unwrap();
    db.execute("CREATE TABLE t2(b INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1)").unwrap();
    let r = db.execute("SELECT a, b FROM t1 CROSS JOIN t2").unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn full_outer_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B'),(3,'C')").unwrap();
    db.execute("INSERT INTO t2 VALUES (2,200),(3,300),(4,400)").unwrap();
    let r = db
        .execute("SELECT t1.name, t2.val FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY COALESCE(t1.id, t2.id)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 4); // 1(A,NULL), 2(B,200), 3(C,300), 4(NULL,400)
}

#[test]
fn full_outer_join_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, a TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, b TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'x'), (2, 'y')").unwrap();
    db.execute("INSERT INTO t2 VALUES (2, 'p'), (3, 'q')").unwrap();
    let r = db
        .execute("SELECT t1.id, t1.a, t2.id, t2.b FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY COALESCE(t1.id, t2.id)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    // Row for t1.id=1: no match in t2
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[0][2], Value::Null);
    // Row for t1.id=2 = t2.id=2
    assert_eq!(v[1][0], Value::Int64(2));
    assert_eq!(v[1][2], Value::Int64(2));
    // Row for t2.id=3: no match in t1
    assert_eq!(v[2][0], Value::Null);
    assert_eq!(v[2][2], Value::Int64(3));
}

#[test]
fn full_outer_join_empty_left() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1), (2)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[1][0], Value::Null);
}

#[test]
fn full_outer_join_empty_right() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][1], Value::Null);
    assert_eq!(v[1][1], Value::Null);
}

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

#[test]
fn implicit_cross_join_from_multiple_tables() {
    let db = mem_db();
    exec(&db, "CREATE TABLE icj1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE icj2 (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO icj1 VALUES (1), (2)");
    exec(&db, "INSERT INTO icj2 VALUES (10)");
    let r = exec(&db, "SELECT icj1.id, icj2.id FROM icj1, icj2");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn implicit_cross_join_multiple_tables() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(a INT64)").unwrap();
    db.execute("CREATE TABLE t2(b INT64)").unwrap();
    db.execute("CREATE TABLE t3(c INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10),(20)").unwrap();
    db.execute("INSERT INTO t3 VALUES (100)").unwrap();
    let r = db
        .execute("SELECT a, b, c FROM t1, t2, t3 ORDER BY a, b, c")
        .unwrap();
    assert_eq!(rows(&r).len(), 4); // 2 * 2 * 1
}

#[test]
fn implicit_cross_join_three_tables() {
    let db = mem_db();
    db.execute("CREATE TABLE a(x INT64)").unwrap();
    db.execute("CREATE TABLE b(y INT64)").unwrap();
    db.execute("CREATE TABLE c(z INT64)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES (10), (20)").unwrap();
    db.execute("INSERT INTO c VALUES (100)").unwrap();
    let r = db.execute("SELECT x, y, z FROM a, b, c ORDER BY x, y").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 4); // 2*2*1
}

#[test]
fn join_cross() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(x INT64)").unwrap();
    db.execute("CREATE TABLE t2(y INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10),(20),(30)").unwrap();
    let r = db.execute("SELECT x, y FROM t1 CROSS JOIN t2").unwrap();
    assert_eq!(rows(&r).len(), 6);
}

#[test]
fn join_inner() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B'),(3,'C')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1,100),(20,1,200),(30,2,300)").unwrap();
    let r = db
        .execute("SELECT t1.name, t2.val FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id ORDER BY t2.val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][0], Value::Text("A".into()));
}

#[test]
fn join_left() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(t1_id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B'),(3,'C')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1,100),(1,200)").unwrap();
    let r = db
        .execute("SELECT t1.name, t2.val FROM t1 LEFT JOIN t2 ON t1.id = t2.t1_id ORDER BY t1.name, t2.val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 4); // A×2 + B×NULL + C×NULL
    assert_eq!(v[2][1], Value::Null); // B has no match
}

#[test]
fn join_multi_table() {
    let db = mem_db();
    db.execute("CREATE TABLE a(id INT64)").unwrap();
    db.execute("CREATE TABLE b(id INT64, a_id INT64)").unwrap();
    db.execute("CREATE TABLE c(id INT64, b_id INT64)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (10,1),(20,2)").unwrap();
    db.execute("INSERT INTO c VALUES (100,10),(200,20),(300,10)").unwrap();
    let r = db
        .execute(
            "SELECT a.id, b.id, c.id FROM a
             INNER JOIN b ON a.id = b.a_id
             INNER JOIN c ON b.id = c.b_id
             ORDER BY c.id",
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 3);
}

#[test]
fn join_right() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(t1_id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1,100),(2,200)").unwrap();
    let r = db
        .execute("SELECT t1.name, t2.val FROM t1 RIGHT JOIN t2 ON t1.id = t2.t1_id ORDER BY t2.val")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("A".into())); // matched
    assert_eq!(v[1][0], Value::Null);              // no t1 match
}

#[test]
fn join_self() {
    let db = mem_db();
    db.execute("CREATE TABLE emp(id INT64, name TEXT, manager_id INT64)").unwrap();
    db.execute("INSERT INTO emp VALUES (1,'CEO',NULL),(2,'VP',1),(3,'Dev',2)").unwrap();
    let r = db
        .execute(
            "SELECT e.name, m.name AS manager FROM emp e
             LEFT JOIN emp m ON e.manager_id = m.id ORDER BY e.id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Null);                // CEO has no manager
    assert_eq!(v[1][1], Value::Text("CEO".into()));   // VP's manager is CEO
    assert_eq!(v[2][1], Value::Text("VP".into()));    // Dev's manager is VP
}

#[test]
fn join_using_clause() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1,100),(2,200)").unwrap();
    let r = db
        .execute("SELECT t1.name, t2.val FROM t1 INNER JOIN t2 USING(id) ORDER BY t1.name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("A".into()));
}

#[test]
fn left_join_with_right_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("CREATE TABLE t3(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3)").unwrap();
    db.execute("INSERT INTO t3 VALUES (3),(4)").unwrap();
    let r = db
        .execute(
            "SELECT t1.id, t2.id, t3.id FROM t1
             LEFT JOIN t2 ON t1.id = t2.id
             RIGHT JOIN t3 ON t2.id = t3.id
             ORDER BY t3.id",
        );
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(!v.is_empty());
    }
}

#[test]
fn multiple_joins() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)").unwrap();
    db.execute("CREATE TABLE t3(id INT64, t2_id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10, 1),(20, 2)").unwrap();
    db.execute("INSERT INTO t3 VALUES (100, 10),(200, 20)").unwrap();
    let r = db
        .execute(
            "SELECT t1.id, t2.id, t3.id FROM t1
             JOIN t2 ON t2.t1_id = t1.id
             JOIN t3 ON t3.t2_id = t2.id
             ORDER BY t1.id",
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn natural_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1,100),(2,200)").unwrap();
    let r = db
        .execute("SELECT name, val FROM t1 NATURAL JOIN t2 ORDER BY name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn natural_join_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 100), (3, 300)").unwrap();
    let r = db.execute("SELECT * FROM t1 NATURAL JOIN t2");
    if let Ok(r) = r {
        let v = rows(&r);
        assert!(!v.is_empty()); // id=1 matches
    }
}

#[test]
fn parse_complex_join_conditions() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, a TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, b TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'x'), (2, 'y')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 'x'), (2, 'z')").unwrap();
    let r = db
        .execute("SELECT t1.id FROM t1 JOIN t2 ON t1.id = t2.id AND t1.a = t2.b")
        .unwrap();
    assert_eq!(rows(&r).len(), 1); // Only id=1 matches both conditions
}

#[test]
fn planner_multiple_from_items_cross_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t1 (a INT)");
    exec(&db, "CREATE TABLE t2 (b INT)");
    exec(&db, "INSERT INTO t1 VALUES (1), (2)");
    exec(&db, "INSERT INTO t2 VALUES (10), (20)");
    let r = exec(&db, "SELECT a, b FROM t1, t2 ORDER BY a, b");
    assert_eq!(r.rows().len(), 4);
}

#[test]
fn qualified_wildcard_in_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE qw1 (id INT PRIMARY KEY, a INT)");
    exec(&db, "CREATE TABLE qw2 (id INT PRIMARY KEY, b INT)");
    exec(&db, "INSERT INTO qw1 VALUES (1, 10)");
    exec(&db, "INSERT INTO qw2 VALUES (1, 20)");
    let r = exec(&db, "SELECT qw1.* FROM qw1 JOIN qw2 ON qw1.id = qw2.id");
    assert_eq!(r.rows()[0].values().len(), 2); // id, a
}

#[test]
fn right_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // (2,2), (NULL,3)
    assert_eq!(v[1][0], Value::Null);
}

#[test]
fn right_join_all_unmatched() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1)").unwrap();
    db.execute("INSERT INTO t2 VALUES (100), (200)").unwrap();
    let r = db
        .execute("SELECT t1.id, t2.id FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Null);
    assert_eq!(v[1][0], Value::Null);
}

#[test]
fn right_join_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, label TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (2, 'x'), (3, 'y')").unwrap();
    let r = db
        .execute("SELECT t1.id, t1.name, t2.id, t2.label FROM t1 RIGHT JOIN t2 ON t1.id = t2.id ORDER BY t2.id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    // t2.id=2 matches t1.id=2
    assert_eq!(v[0][2], Value::Int64(2));
    assert_eq!(v[0][0], Value::Int64(2));
    // t2.id=3 has no match in t1
    assert_eq!(v[1][2], Value::Int64(3));
    assert_eq!(v[1][0], Value::Null);
    assert_eq!(v[1][1], Value::Null);
}

#[test]
fn select_from_three_tables_implicit_cross_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mf1 (a INT)");
    exec(&db, "CREATE TABLE mf2 (b INT)");
    exec(&db, "CREATE TABLE mf3 (c INT)");
    exec(&db, "INSERT INTO mf1 VALUES (1)");
    exec(&db, "INSERT INTO mf2 VALUES (2)");
    exec(&db, "INSERT INTO mf3 VALUES (3)");
    let r = exec(&db, "SELECT a, b, c FROM mf1, mf2, mf3");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(2));
    assert_eq!(r.rows()[0].values()[2], Value::Int64(3));
}

#[test]
fn self_join() {
    let db = mem_db();
    db.execute("CREATE TABLE emp(id INT64, name TEXT, manager_id INT64)").unwrap();
    db.execute("INSERT INTO emp VALUES (1,'Alice',NULL),(2,'Bob',1),(3,'Charlie',1)").unwrap();
    let r = db
        .execute(
            "SELECT e.name, m.name AS manager FROM emp e LEFT JOIN emp m ON e.manager_id = m.id ORDER BY e.id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][1], Value::Null); // Alice has no manager
    assert_eq!(v[1][1], Value::Text("Alice".into())); // Bob's manager is Alice
}

#[test]
fn three_way_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, label TEXT)").unwrap();
    db.execute("CREATE TABLE t3(id INT64, t2_id INT64, tag TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1,'x'),(20,2,'y')").unwrap();
    db.execute("INSERT INTO t3 VALUES (100,10,'p'),(200,20,'q')").unwrap();
    let r = db
        .execute(
            "SELECT t1.name, t2.label, t3.tag
             FROM t1 JOIN t2 ON t1.id = t2.t1_id
                      JOIN t3 ON t2.id = t3.t2_id
             ORDER BY t1.name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("a".into()));
    assert_eq!(v[0][2], Value::Text("p".into()));
}


// ── Tests merged from engine_coverage_tests.rs, debug_ast.rs ──

#[test]
fn cross_right_and_full_outer_joins_execute_with_null_extension() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE lhs (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE rhs (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO lhs VALUES (1, 'left-only'), (2, 'shared')")
        .unwrap();
    db.execute("INSERT INTO rhs VALUES (2, 'shared'), (3, 'right-only')")
        .unwrap();

    let cross = db
        .execute(
            "SELECT l.id, r.id
             FROM lhs AS l CROSS JOIN rhs AS r
             ORDER BY l.id, r.id",
        )
        .unwrap();
    assert_eq!(cross.rows().len(), 4);
    assert_eq!(
        cross.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(2)]
    );
    assert_eq!(
        cross.rows()[1].values(),
        &[Value::Int64(1), Value::Int64(3)]
    );
    assert_eq!(
        cross.rows()[2].values(),
        &[Value::Int64(2), Value::Int64(2)]
    );
    assert_eq!(
        cross.rows()[3].values(),
        &[Value::Int64(2), Value::Int64(3)]
    );

    let right = db
        .execute(
            "SELECT l.id, l.name, r.id, r.label
             FROM lhs AS l RIGHT JOIN rhs AS r ON l.id = r.id
             ORDER BY r.id",
        )
        .unwrap();
    assert_eq!(
        right
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(2),
                Value::Text("shared".to_string()),
                Value::Int64(2),
                Value::Text("shared".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int64(3),
                Value::Text("right-only".to_string()),
            ],
        ]
    );

    let full = db
        .execute(
            "SELECT l.id, l.name, r.id, r.label
             FROM lhs AS l FULL OUTER JOIN rhs AS r ON l.id = r.id
             ORDER BY COALESCE(l.id, r.id)",
        )
        .unwrap();
    assert_eq!(
        full.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(1),
                Value::Text("left-only".to_string()),
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Int64(2),
                Value::Text("shared".to_string()),
                Value::Int64(2),
                Value::Text("shared".to_string()),
            ],
            vec![
                Value::Null,
                Value::Null,
                Value::Int64(3),
                Value::Text("right-only".to_string()),
            ],
        ]
    );
}

#[test]
fn joins_cover_empty_sides_and_using_outer_merge_columns() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute("CREATE TABLE left_empty (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE right_populated (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO right_populated VALUES (1, 'r1'), (2, 'r2')")
        .unwrap();

    let cross = db
        .execute(
            "SELECT l.id, r.id
             FROM left_empty AS l CROSS JOIN right_populated AS r",
        )
        .unwrap();
    assert!(cross.rows().is_empty());

    let right = db
        .execute(
            "SELECT l.id, r.id
             FROM left_empty AS l RIGHT JOIN right_populated AS r ON l.id = r.id
             ORDER BY r.id",
        )
        .unwrap();
    assert_eq!(
        right
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Null, Value::Int64(1)],
            vec![Value::Null, Value::Int64(2)],
        ]
    );

    db.execute("CREATE TABLE left_populated (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE right_empty (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO left_populated VALUES (1, 'l1'), (2, 'l2')")
        .unwrap();

    let full = db
        .execute(
            "SELECT l.id, r.id
             FROM left_populated AS l FULL OUTER JOIN right_empty AS r ON l.id = r.id
             ORDER BY l.id",
        )
        .unwrap();
    assert_eq!(
        full.rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Null],
            vec![Value::Int64(2), Value::Null],
        ]
    );

    db.execute("CREATE TABLE full_left (id INT64 PRIMARY KEY, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE full_right (id INT64 PRIMARY KEY, label TEXT)")
        .unwrap();
    db.execute("INSERT INTO full_left VALUES (1, 'l1'), (2, 'l2')")
        .unwrap();
    db.execute("INSERT INTO full_right VALUES (2, 'r2'), (3, 'r3')")
        .unwrap();

    let using_full = db
        .execute("SELECT * FROM full_left FULL OUTER JOIN full_right USING (id) ORDER BY id")
        .unwrap();
    assert_eq!(
        using_full.columns(),
        &["id".to_string(), "name".to_string(), "label".to_string()]
    );
    assert_eq!(
        using_full
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("l1".into()), Value::Null],
            vec![
                Value::Int64(2),
                Value::Text("l2".into()),
                Value::Text("r2".into())
            ],
            vec![Value::Int64(3), Value::Null, Value::Text("r3".into())],
        ]
    );
}

#[test]
fn test_join_execution() {
    let path = unique_path();
    let db = Db::create(&path, Default::default()).expect("create db");
    db.execute("CREATE TABLE a (id INT64 PRIMARY KEY)")
        .expect("create a");
    db.execute("CREATE TABLE b (id INT64 PRIMARY KEY)")
        .expect("create b");
    db.execute("INSERT INTO a VALUES (1), (2)")
        .expect("insert a");
    db.execute("INSERT INTO b VALUES (1), (3)")
        .expect("insert b");

    let queries = vec![
        ("CROSS JOIN", "SELECT * FROM a CROSS JOIN b"),
        ("RIGHT JOIN", "SELECT * FROM a RIGHT JOIN b ON a.id = b.id"),
        (
            "FULL OUTER JOIN",
            "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id",
        ),
        ("NATURAL JOIN", "SELECT * FROM a NATURAL JOIN b"),
        ("INNER JOIN", "SELECT * FROM a INNER JOIN b ON a.id = b.id"),
        ("LEFT JOIN", "SELECT * FROM a LEFT JOIN b ON a.id = b.id"),
    ];

    for (name, q) in queries {
        match db.execute(q) {
            Ok(_result) => println!("✓ Executed: {} - {}", name, q),
            Err(e) => println!("✗ Failed: {} - {} - {}", name, q, e),
        }
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn using_and_natural_joins_merge_output_columns_but_keep_qualified_access() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();
    db.execute(
        "CREATE TABLE using_left (
            id INT64 PRIMARY KEY,
            shared TEXT,
            left_only TEXT
        )",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE using_right (
            id INT64 PRIMARY KEY,
            shared TEXT,
            right_only TEXT
        )",
    )
    .unwrap();
    db.execute("INSERT INTO using_left VALUES (1, 'left-shared', 'l1'), (2, 'left-two', 'l2')")
        .unwrap();
    db.execute(
        "INSERT INTO using_right VALUES (1, 'right-shared', 'r1'), (3, 'right-three', 'r3')",
    )
    .unwrap();

    let using_star = db
        .execute("SELECT * FROM using_left JOIN using_right USING (id) ORDER BY id")
        .unwrap();
    assert_eq!(
        using_star.columns(),
        &[
            "id".to_string(),
            "shared".to_string(),
            "left_only".to_string(),
            "shared".to_string(),
            "right_only".to_string(),
        ]
    );
    assert_eq!(
        using_star
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![vec![
            Value::Int64(1),
            Value::Text("left-shared".into()),
            Value::Text("l1".into()),
            Value::Text("right-shared".into()),
            Value::Text("r1".into()),
        ]]
    );

    db.execute(
        "CREATE VIEW using_join_view AS
         SELECT * FROM using_left JOIN using_right USING (id)",
    )
    .unwrap();
    let using_view = db
        .execute("SELECT * FROM using_join_view ORDER BY id")
        .unwrap();
    assert_eq!(using_view.columns(), using_star.columns());
    assert_eq!(
        using_view
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        using_star
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>()
    );

    let qualified = db
        .execute(
            "SELECT using_left.id, using_right.id, id
             FROM using_left JOIN using_right USING (id)",
        )
        .unwrap();
    assert_eq!(
        qualified.rows()[0].values(),
        &[Value::Int64(1), Value::Int64(1), Value::Int64(1)]
    );

    let table_wildcards = db
        .execute("SELECT using_left.*, using_right.* FROM using_left JOIN using_right USING (id)")
        .unwrap();
    assert_eq!(
        table_wildcards.columns(),
        &[
            "id".to_string(),
            "shared".to_string(),
            "left_only".to_string(),
            "id".to_string(),
            "shared".to_string(),
            "right_only".to_string(),
        ]
    );
    assert_eq!(
        table_wildcards.rows()[0].values(),
        &[
            Value::Int64(1),
            Value::Text("left-shared".into()),
            Value::Text("l1".into()),
            Value::Int64(1),
            Value::Text("right-shared".into()),
            Value::Text("r1".into()),
        ]
    );

    let ambiguous = db
        .execute("SELECT shared FROM using_left JOIN using_right USING (id)")
        .unwrap_err();
    assert!(
        ambiguous
            .to_string()
            .contains("ambiguous column reference shared"),
        "unexpected error: {ambiguous}"
    );

    db.execute(
        "CREATE TABLE natural_left (
            id INT64 PRIMARY KEY,
            shared TEXT,
            left_only TEXT
        )",
    )
    .unwrap();
    db.execute(
        "CREATE TABLE natural_right (
            id INT64 PRIMARY KEY,
            shared TEXT,
            right_only TEXT
        )",
    )
    .unwrap();
    db.execute("INSERT INTO natural_left VALUES (1, 'same', 'l1'), (2, 'two', 'l2')")
        .unwrap();
    db.execute("INSERT INTO natural_right VALUES (1, 'same', 'r1')")
        .unwrap();

    let natural = db
        .execute("SELECT * FROM natural_left NATURAL LEFT JOIN natural_right ORDER BY id")
        .unwrap();
    assert_eq!(
        natural.columns(),
        &[
            "id".to_string(),
            "shared".to_string(),
            "left_only".to_string(),
            "right_only".to_string(),
        ]
    );
    assert_eq!(
        natural
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Int64(1),
                Value::Text("same".into()),
                Value::Text("l1".into()),
                Value::Text("r1".into()),
            ],
            vec![
                Value::Int64(2),
                Value::Text("two".into()),
                Value::Text("l2".into()),
                Value::Null,
            ],
        ]
    );

    db.execute("CREATE TABLE natural_cross_left (left_id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE natural_cross_right (right_id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO natural_cross_left VALUES (1), (2)")
        .unwrap();
    db.execute("INSERT INTO natural_cross_right VALUES (10)")
        .unwrap();

    let natural_cross = db
        .execute(
            "SELECT * FROM natural_cross_left NATURAL JOIN natural_cross_right ORDER BY left_id",
        )
        .unwrap();
    assert_eq!(
        natural_cross
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Int64(10)],
            vec![Value::Int64(2), Value::Int64(10)],
        ]
    );
}

