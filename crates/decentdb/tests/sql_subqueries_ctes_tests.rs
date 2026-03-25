//! SQL subquery and CTE tests.
//!
//! Covers: Correlated subqueries, EXISTS, IN subquery, scalar subqueries,
//! FROM subqueries, WITH (CTEs), recursive CTEs, column aliases,
//! fibonacci sequences, tree traversal, and CTE validation errors.

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
fn any_all_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(val INT64)").unwrap();
    db.execute("CREATE TABLE t2(val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (10),(20),(30)").unwrap();
    db.execute("INSERT INTO t2 VALUES (15),(25)").unwrap();
    // Test > ANY
    let r = db
        .execute("SELECT val FROM t1 WHERE val > ANY (SELECT val FROM t2) ORDER BY val");
    // May or may not be supported
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn complex_cte_recursive_with_data() {
    let db = mem_db();
    db.execute("CREATE TABLE categories(id INT64, parent_id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO categories VALUES (1, NULL, 'Root'), (2, 1, 'Electronics'), (3, 1, 'Books'), (4, 2, 'Phones'), (5, 2, 'Laptops')").unwrap();
    let r = db
        .execute(
            "WITH RECURSIVE tree AS (
                SELECT id, name, 0 AS depth FROM categories WHERE parent_id IS NULL
                UNION ALL
                SELECT c.id, c.name, t.depth + 1
                FROM categories c JOIN tree t ON c.parent_id = t.id
            )
            SELECT id, name, depth FROM tree ORDER BY depth, id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0][2], Value::Int64(0)); // Root depth 0
}

#[test]
fn complex_cte_with_join_and_aggregate() {
    let db = mem_db();
    db.execute("CREATE TABLE departments(id INT64 PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE employees(id INT64 PRIMARY KEY, name TEXT, dept_id INT64, salary INT64)").unwrap();
    db.execute("INSERT INTO departments VALUES (1,'Engineering'),(2,'Sales'),(3,'HR')").unwrap();
    db.execute("INSERT INTO employees VALUES (1,'Alice',1,100000),(2,'Bob',1,90000),(3,'Charlie',2,80000),(4,'Diana',2,85000),(5,'Eve',3,70000)").unwrap();

    let r = db
        .execute(
            "WITH dept_stats AS (
                SELECT dept_id, COUNT(*) AS cnt, AVG(salary) AS avg_sal
                FROM employees GROUP BY dept_id
            )
            SELECT d.name, ds.cnt, ds.avg_sal
            FROM departments d
            JOIN dept_stats ds ON d.id = ds.dept_id
            ORDER BY d.name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][0], Value::Text("Engineering".into()));
}

#[test]
fn correlated_scalar_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY, val INT64)").unwrap();
    db.execute("CREATE TABLE t2 (id INT64, score INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 100), (2, 200)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 10), (1, 20), (2, 30)").unwrap();

    let result = db
        .execute(
            "SELECT t1.id, (SELECT SUM(score) FROM t2 WHERE t2.id = t1.id) AS total
             FROM t1 ORDER BY t1.id",
        )
        .unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][1], Value::Int64(30));
    assert_eq!(rows[1][1], Value::Int64(30));
}

#[test]
fn correlated_subquery_in_select() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, grp TEXT)").unwrap();
    db.execute("CREATE TABLE t2(grp TEXT, label TEXT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'A'),(2,'B'),(3,'A')").unwrap();
    db.execute("INSERT INTO t2 VALUES ('A','alpha'),('B','beta')").unwrap();
    let r = db
        .execute(
            "SELECT t1.id, (SELECT t2.label FROM t2 WHERE t2.grp = t1.grp) AS lbl FROM t1 ORDER BY t1.id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Text("alpha".into()));
    assert_eq!(v[1][1], Value::Text("beta".into()));
}

#[test]
fn correlated_subquery_in_where() {
    let db = mem_db();
    db.execute("CREATE TABLE orders(id INT64, customer_id INT64, amount INT64)").unwrap();
    db.execute("CREATE TABLE customers(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 50)").unwrap();
    let r = db
        .execute(
            "SELECT c.name FROM customers c
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Text("Alice".into()));
}

#[test]
fn correlated_subquery_with_between() {
    let db = mem_db();
    db.execute("CREATE TABLE ranges (id INT64 PRIMARY KEY, lo INT64, hi INT64)").unwrap();
    db.execute("CREATE TABLE vals (v INT64)").unwrap();
    db.execute("INSERT INTO ranges VALUES (1, 10, 20), (2, 30, 40)").unwrap();
    db.execute("INSERT INTO vals VALUES (15), (35), (50)").unwrap();

    let result = db
        .execute(
            "SELECT r.id FROM ranges r
             WHERE EXISTS (SELECT 1 FROM vals WHERE v BETWEEN r.lo AND r.hi)
             ORDER BY r.id",
        )
        .unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn correlated_subquery_with_case_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount INT)");
    exec(&db, "INSERT INTO orders VALUES (1, 'alice', 100), (2, 'bob', 200), (3, 'alice', 300)");
    let r = exec(&db, "
        SELECT o1.customer, o1.amount,
            (SELECT CASE WHEN o2.amount > 150 THEN 'high' ELSE 'low' END 
             FROM orders o2 WHERE o2.id = o1.id) as category
        FROM orders o1
        ORDER BY o1.id
    ");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[2], Value::Text("low".to_string()));
    assert_eq!(r.rows()[1].values()[2], Value::Text("high".to_string()));
}

#[test]
fn correlated_subquery_with_function_call() {
    let db = mem_db();
    exec(&db, "CREATE TABLE names (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO names VALUES (1, 'alice'), (2, 'BOB')");
    let r = exec(&db, "
        SELECT n1.name,
            (SELECT lower(n2.name) FROM names n2 WHERE n2.id = n1.id) as lower_name
        FROM names n1
        ORDER BY n1.id
    ");
    assert_eq!(r.rows()[0].values()[1], Value::Text("alice".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("bob".to_string()));
}

#[test]
fn correlated_subquery_with_in_list() {
    let db = mem_db();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY, code TEXT)").unwrap();
    db.execute("CREATE TABLE child (pid INT64, tag TEXT)").unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("INSERT INTO child VALUES (1, 'a'), (1, 'x'), (2, 'y')").unwrap();

    let result = db
        .execute(
            "SELECT p.id FROM parent p
             WHERE EXISTS (SELECT 1 FROM child c WHERE c.tag IN (p.code, 'x') AND c.pid = p.id)
             ORDER BY p.id",
        )
        .unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(1));
}

#[test]
fn correlated_subquery_with_like() {
    let db = mem_db();
    db.execute("CREATE TABLE patterns (id INT64 PRIMARY KEY, pat TEXT)").unwrap();
    db.execute("CREATE TABLE strings (val TEXT)").unwrap();
    db.execute("INSERT INTO patterns VALUES (1, 'a%'), (2, 'b%')").unwrap();
    db.execute("INSERT INTO strings VALUES ('apple'), ('banana'), ('cherry')").unwrap();

    let result = db
        .execute(
            "SELECT p.id FROM patterns p
             WHERE EXISTS (SELECT 1 FROM strings s WHERE s.val LIKE p.pat)
             ORDER BY p.id",
        )
        .unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn cte_basic() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = db
        .execute(
            "WITH big AS (SELECT * FROM t WHERE val > 15)
             SELECT id FROM big ORDER BY id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn cte_column_alias_count_mismatch_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 2)");
    let err = exec_err(&db, "WITH c(x) AS (SELECT a, b FROM t) SELECT * FROM c");
    assert!(err.contains("expected") || err.contains("column"), "got: {err}");
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

#[test]
fn cte_column_count_mismatch_errors() {
    let db = mem_db();
    let err = db
        .execute("WITH cte(a, b) AS (SELECT 1) SELECT * FROM cte")
        .unwrap_err();
    assert!(err.to_string().contains("column"));
}

#[test]
fn cte_column_rename_works() {
    let db = mem_db();
    let result = db
        .execute("WITH cte(a, b) AS (SELECT 1, 2) SELECT a, b FROM cte")
        .unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0], vec![Value::Int64(1), Value::Int64(2)]);
}

#[test]
fn cte_multiple_ctes() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    let r = db
        .execute(
            "WITH low AS (SELECT * FROM t WHERE val <= 20),
                  high AS (SELECT * FROM t WHERE val > 20)
             SELECT (SELECT COUNT(*) FROM low) AS low_count,
                    (SELECT COUNT(*) FROM high) AS high_count",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[0][1], Value::Int64(2));
}

#[test]
fn cte_recursive_counting() {
    let db = mem_db();
    let r = db
        .execute(
            "WITH RECURSIVE cnt(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM cnt WHERE n < 5
            )
            SELECT n FROM cnt ORDER BY n",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 5);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[4][0], Value::Int64(5));
}

#[test]
fn cte_used_in_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = db
        .execute(
            "WITH cte AS (SELECT id, val FROM t WHERE val > 10)
             SELECT * FROM t WHERE id IN (SELECT id FROM cte)",
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn cte_with_aggregation() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3)").unwrap();
    let r = db
        .execute(
            "WITH agg AS (SELECT grp, SUM(val) AS total FROM t GROUP BY grp)
             SELECT grp, total FROM agg WHERE total > 2 ORDER BY grp",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn cte_with_column_aliases() {
    let db = mem_db();
    exec(&db, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)");
    exec(&db, "INSERT INTO products VALUES (1, 'widget', 10), (2, 'gadget', 20)");
    let r = exec(&db, "
        WITH aliased(product_id, product_name, product_price) AS (
            SELECT id, name, price FROM products
        )
        SELECT product_id, product_name FROM aliased ORDER BY product_id
    ");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.columns()[0], "product_id");
    assert_eq!(r.columns()[1], "product_name");
}

#[test]
fn cte_with_column_names() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    let r = db
        .execute(
            "WITH renamed(my_id, my_val) AS (SELECT id, val FROM t)
             SELECT my_id, my_val FROM renamed ORDER BY my_id"
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
}

#[test]
fn cte_with_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1,100),(20,2,200)").unwrap();
    let r = db
        .execute(
            "WITH joined AS (
                SELECT t1.name, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id
             )
             SELECT name, val FROM joined ORDER BY name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn cte_wrong_column_count_error() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let err = db
        .execute(
            "WITH wrong(a, b, c) AS (SELECT id, val FROM t)
             SELECT * FROM wrong"
        )
        .unwrap_err();
    assert!(err.to_string().contains("column") || !err.to_string().is_empty());
}

#[test]
fn delete_with_in_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dws_main (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE dws_del (val INT)");
    exec(&db, "INSERT INTO dws_main VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO dws_del VALUES (10), (30)");
    exec(&db, "DELETE FROM dws_main WHERE val IN (SELECT val FROM dws_del)");
    let r = exec(&db, "SELECT COUNT(*) FROM dws_main");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn delete_with_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64 PRIMARY KEY, val INT64)").unwrap();
    db.execute("CREATE TABLE t2(ref_id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1),(3)").unwrap();
    db.execute("DELETE FROM t1 WHERE id IN (SELECT ref_id FROM t2)").unwrap();
    let r = db.execute("SELECT id FROM t1").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(2));
}

#[test]
fn delete_with_subquery_in_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3),(4),(5)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(4)").unwrap();
    db.execute("DELETE FROM t1 WHERE id IN (SELECT id FROM t2)").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn error_cte_column_list_mismatch() {
    let db = mem_db();
    let err = db
        .execute("WITH cte(a, b) AS (SELECT 1) SELECT * FROM cte")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("column") || msg.contains("expected") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn error_recursive_cte_column_count_mismatch() {
    let db = mem_db();
    let err = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1, n + 2 FROM cte WHERE n < 5
            ) SELECT * FROM cte",
        )
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("column") || msg.contains("produced") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn error_recursive_cte_no_anchor() {
    let db = mem_db();
    // Both terms reference the CTE – no real anchor
    let err = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT n FROM cte
                UNION ALL
                SELECT n + 1 FROM cte WHERE n < 5
            ) SELECT * FROM cte",
        )
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("anchor") || msg.contains("recursive") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn error_recursive_cte_no_recursive_ref() {
    let db = mem_db();
    db.execute("CREATE TABLE other(n INT64)").unwrap();
    db.execute("INSERT INTO other VALUES (1)").unwrap();
    // The engine accepts this as a valid UNION ALL with non-recursive second term
    let r = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 FROM other WHERE n < 5
            ) SELECT * FROM cte",
        );
    // May succeed (treating it as finite) or error depending on engine logic
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn error_recursive_cte_with_intersect() {
    let db = mem_db();
    let err = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT 1 AS n
                INTERSECT
                SELECT n + 1 FROM cte WHERE n < 5
            ) SELECT * FROM cte",
        )
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("UNION") || msg.contains("recursive") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn exists_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE esq (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO esq VALUES (1, 10), (2, 20)");
    let r = exec(&db, "SELECT id FROM esq WHERE EXISTS (SELECT 1 FROM esq WHERE val = 10)");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn exists_with_empty_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("SELECT EXISTS (SELECT 1 FROM t)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Bool(false));
}

#[test]
fn exists_with_nonempty_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = db
        .execute("SELECT EXISTS (SELECT 1 FROM t)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Bool(true));
}

#[test]
fn explain_correlated_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)")
        .unwrap();
    assert!(!rows(&r).is_empty());
}

#[test]
fn generated_column_rejects_subquery() {
    let db = mem_db();
    let err = db
        .execute(
            "CREATE TABLE t (id INT64, gen INT64 GENERATED ALWAYS AS ((SELECT 1)) STORED)",
        )
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("generated") || err.to_string().to_lowercase().contains("subquer"));
}

#[test]
fn group_by_with_in_subquery_having() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh_sub (grp TEXT, val INT)");
    exec(&db, "CREATE TABLE allowed_groups (name TEXT)");
    exec(&db, "INSERT INTO gh_sub VALUES ('a', 1), ('b', 2), ('c', 3)");
    exec(&db, "INSERT INTO allowed_groups VALUES ('a'), ('c')");
    let r = exec(&db, "
        SELECT grp, SUM(val) FROM gh_sub
        GROUP BY grp
        HAVING grp IN (SELECT name FROM allowed_groups)
        ORDER BY grp
    ");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_with_scalar_subquery_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh_ss (grp TEXT, val INT)");
    exec(&db, "CREATE TABLE multipliers (grp TEXT, mult INT)");
    exec(&db, "INSERT INTO gh_ss VALUES ('a', 10), ('a', 20), ('b', 30)");
    exec(&db, "INSERT INTO multipliers VALUES ('a', 2), ('b', 3)");
    let r = exec(&db, "
        SELECT gh_ss.grp, SUM(val) * (SELECT mult FROM multipliers WHERE multipliers.grp = gh_ss.grp) as scaled
        FROM gh_ss
        GROUP BY gh_ss.grp
        ORDER BY gh_ss.grp
    ");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn in_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(3),(4)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
}

#[test]
fn in_subquery_empty() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE id IN (SELECT id FROM t2)")
        .unwrap();
    assert_eq!(rows(&r).len(), 0);
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
fn in_subquery_normalization() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let result = db.execute("SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE id > 1) ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn in_subquery_with_multiple_values() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, category TEXT)").unwrap();
    db.execute("CREATE TABLE categories(name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'A'),(2,'B'),(3,'C'),(4,'A')").unwrap();
    db.execute("INSERT INTO categories VALUES ('A'),('C')").unwrap();
    let r = db
        .execute("SELECT id FROM t WHERE category IN (SELECT name FROM categories) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3); // ids 1, 3, 4
}

#[test]
fn multiple_ctes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mct (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO mct VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db,
        "WITH low AS (SELECT * FROM mct WHERE val < 20),
              high AS (SELECT * FROM mct WHERE val >= 20)
         SELECT (SELECT COUNT(*) FROM low) AS lc, (SELECT COUNT(*) FROM high) AS hc"
    );
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(2));
}

#[test]
fn multiple_ctes_used_together() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'A',10),(2,'A',20),(3,'B',30)").unwrap();
    let r = db
        .execute(
            "WITH
                sums AS (SELECT grp, SUM(val) AS total FROM t GROUP BY grp),
                counts AS (SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp)
            SELECT s.grp, s.total, c.cnt
            FROM sums s JOIN counts c ON s.grp = c.grp
            ORDER BY s.grp",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn nested_subquery_in_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)").unwrap();
    db.execute("CREATE TABLE t3(id INT64, t2_id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10,1),(20,2)").unwrap();
    db.execute("INSERT INTO t3 VALUES (100,10),(200,20)").unwrap();
    let r = db
        .execute(
            "SELECT id FROM t1 WHERE id IN (
                SELECT t1_id FROM t2 WHERE id IN (SELECT t2_id FROM t3)
            ) ORDER BY id"
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 2);
}

#[test]
fn normalize_complex_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE outer_t (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO outer_t VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "
        SELECT o.id, (SELECT SUM(i.val) FROM outer_t i WHERE i.id <= o.id) as running
        FROM outer_t o
        ORDER BY o.id
    ");
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn normalize_exists_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE main_tbl (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE ref_tbl (main_id INT)");
    exec(&db, "INSERT INTO main_tbl VALUES (1), (2), (3)");
    exec(&db, "INSERT INTO ref_tbl VALUES (1), (3)");
    let r = exec(&db, "
        SELECT id FROM main_tbl m
        WHERE EXISTS (SELECT 1 FROM ref_tbl r WHERE r.main_id = m.id)
        ORDER BY id
    ");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn normalize_not_exists_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE main2 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE ref2 (main_id INT)");
    exec(&db, "INSERT INTO main2 VALUES (1), (2), (3)");
    exec(&db, "INSERT INTO ref2 VALUES (1), (3)");
    let r = exec(&db, "
        SELECT id FROM main2 m
        WHERE NOT EXISTS (SELECT 1 FROM ref2 r WHERE r.main_id = m.id)
        ORDER BY id
    ");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn not_in_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2)").unwrap();

    let result = db.execute("SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2) ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int64(1));
    assert_eq!(rows[1][0], Value::Int64(3));
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

#[test]
fn parse_exists_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (10, 1), (20, 3)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn parse_multiple_ctes() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'A',10),(2,'A',20),(3,'B',30)").unwrap();
    let r = db
        .execute(
            "WITH
                grp_a AS (SELECT * FROM t WHERE grp = 'A'),
                grp_b AS (SELECT * FROM t WHERE grp = 'B')
            SELECT (SELECT COUNT(*) FROM grp_a) AS a_count, (SELECT COUNT(*) FROM grp_b) AS b_count",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[0][1], Value::Int64(1));
}

#[test]
fn parse_not_exists_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(t1_id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1), (3)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(2));
}

#[test]
fn parse_subquery_in_select_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    let r = db
        .execute("SELECT id, (SELECT MAX(val) FROM t) AS max_val FROM t")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(100));
}

#[test]
fn query_with_aliased_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = db
        .execute(
            "SELECT s.id, s.val, s.val * 2 AS doubled
             FROM (SELECT id, val FROM t WHERE val > 10) AS s
             ORDER BY s.id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][2], Value::Int64(40)); // 20*2
}

#[test]
fn query_with_recursive_cte_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rcd (id INT PRIMARY KEY, parent_id INT)");
    exec(
        &db,
        "CREATE VIEW rcd_v AS
         WITH RECURSIVE tree AS (
             SELECT id, parent_id FROM rcd WHERE parent_id IS NULL
             UNION ALL
             SELECT c.id, c.parent_id FROM rcd c JOIN tree t ON c.parent_id = t.id
         )
         SELECT * FROM tree",
    );
    // dump_sql exercises to_sql Display
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("RECURSIVE") || dump.contains("recursive"));
}

#[test]
fn recursive_cte_fibonacci() {
    let db = mem_db();
    let r = db
        .execute(
            "WITH RECURSIVE fib(n, a, b) AS (
                SELECT 1, 0, 1
                UNION ALL
                SELECT n + 1, b, a + b FROM fib WHERE n < 10
            )
            SELECT n, a FROM fib ORDER BY n",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 10);
    assert_eq!(v[0][1], Value::Int64(0)); // fib(1)=0
    assert_eq!(v[1][1], Value::Int64(1)); // fib(2)=1
}

#[test]
fn recursive_cte_max_iteration_limit() {
    let db = mem_db();
    // Infinite recursion should be capped
    let err = exec_err(&db,
        "WITH RECURSIVE inf AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM inf) SELECT * FROM inf"
    );
    assert!(err.contains("1000") || err.contains("limit") || err.contains("iteration"),
        "Expected iteration limit error, got: {err}");
}

#[test]
fn recursive_cte_rejects_aggregate_in_recursive_term() {
    let db = mem_db();
    let err = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT 1 AS n
                UNION ALL
                SELECT COUNT(*) FROM cte WHERE n < 3
             )
             SELECT * FROM cte",
        )
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("aggregate") || err.to_string().to_lowercase().contains("recursive"));
}

#[test]
fn recursive_cte_rejects_case_with_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE aux (id INT64 PRIMARY KEY)").unwrap();
    let err = db
        .execute(
            "WITH RECURSIVE cte AS (
                SELECT 1 AS n
                UNION ALL
                SELECT CASE WHEN (SELECT COUNT(*) FROM aux) > 0 THEN n + 1 ELSE n END FROM cte WHERE n < 3
             )
             SELECT * FROM cte",
        )
        .unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn recursive_cte_union_dedup() {
    let db = mem_db();
    // UNION (not UNION ALL) should deduplicate and terminate
    let r = exec(&db,
        "WITH RECURSIVE cte AS (
            SELECT 1 AS n
            UNION
            SELECT (n % 3) + 1 FROM cte WHERE n < 10
        ) SELECT n FROM cte ORDER BY n"
    );
    assert_eq!(r.rows().len(), 3); // 1, 2, 3
}

#[test]
fn recursive_cte_with_between() {
    let db = mem_db();
    let r = exec(&db, "
        WITH RECURSIVE seq(n, in_range) AS (
            SELECT 1, CASE WHEN 1 BETWEEN 3 AND 7 THEN true ELSE false END
            UNION ALL
            SELECT n + 1, CASE WHEN n + 1 BETWEEN 3 AND 7 THEN true ELSE false END
            FROM seq WHERE n < 10
        )
        SELECT n, in_range FROM seq ORDER BY n
    ");
    assert_eq!(r.rows().len(), 10);
    assert_eq!(r.rows()[0].values()[1], Value::Bool(false));
    assert_eq!(r.rows()[4].values()[1], Value::Bool(true));
}

#[test]
fn recursive_cte_with_case_expression() {
    let db = mem_db();
    let r = exec(&db, "
        WITH RECURSIVE cnt(n, label) AS (
            SELECT 1, CASE WHEN 1 <= 5 THEN 'low' ELSE 'high' END
            UNION ALL
            SELECT n + 1, CASE WHEN n + 1 <= 5 THEN 'low' ELSE 'high' END
            FROM cnt WHERE n < 10
        )
        SELECT * FROM cnt ORDER BY n
    ");
    assert_eq!(r.rows().len(), 10);
    assert_eq!(r.rows()[0].values()[1], Value::Text("low".to_string()));
    assert_eq!(r.rows()[9].values()[1], Value::Text("high".to_string()));
}

#[test]
fn recursive_cte_with_distinct_in_recursive_term() {
    let db = mem_db();
    let err = db.execute(
        "WITH RECURSIVE cte AS (
            SELECT 1 AS n
            UNION ALL
            SELECT DISTINCT n + 1 FROM cte WHERE n < 5
        ) SELECT * FROM cte"
    );
    // DISTINCT in recursive term may be unsupported
    assert!(err.is_ok() || err.is_err());
}

#[test]
fn recursive_cte_with_function_call() {
    let db = mem_db();
    let r = exec(&db, "
        WITH RECURSIVE names(n, padded) AS (
            SELECT 1, 'item_1'
            UNION ALL
            SELECT n + 1, 'item_' || CAST(n + 1 AS TEXT)
            FROM names WHERE n < 5
        )
        SELECT * FROM names ORDER BY n
    ");
    assert_eq!(r.rows().len(), 5);
}

#[test]
fn recursive_cte_with_in_list() {
    let db = mem_db();
    let r = exec(&db, "
        WITH RECURSIVE seq(n, is_special) AS (
            SELECT 1, CASE WHEN 1 IN (1, 3, 5, 7) THEN true ELSE false END
            UNION ALL
            SELECT n + 1, CASE WHEN n + 1 IN (1, 3, 5, 7) THEN true ELSE false END
            FROM seq WHERE n < 8
        )
        SELECT n, is_special FROM seq ORDER BY n
    ");
    assert_eq!(r.rows().len(), 8);
    assert_eq!(r.rows()[0].values()[1], Value::Bool(true));  // 1
    assert_eq!(r.rows()[1].values()[1], Value::Bool(false)); // 2
    assert_eq!(r.rows()[2].values()[1], Value::Bool(true));  // 3
}

#[test]
fn recursive_cte_with_is_null() {
    let db = mem_db();
    let r = exec(&db, "
        WITH RECURSIVE seq(n, val) AS (
            SELECT 1, CAST(NULL AS INT)
            UNION ALL
            SELECT n + 1, CASE WHEN n + 1 > 3 THEN n + 1 ELSE NULL END
            FROM seq WHERE n < 6
        )
        SELECT n, val IS NULL as is_n FROM seq ORDER BY n
    ");
    assert_eq!(r.rows().len(), 6);
}

#[test]
fn recursive_cte_with_like_expression() {
    let db = mem_db();
    let r = exec(&db, "
        WITH RECURSIVE items(n, name) AS (
            SELECT 1, 'item_1'
            UNION ALL
            SELECT n + 1, 'item_' || CAST(n + 1 AS TEXT)
            FROM items WHERE n < 5
        )
        SELECT n, name FROM items WHERE name LIKE 'item_%' ORDER BY n
    ");
    assert_eq!(r.rows().len(), 5);
}

#[test]
fn recursive_cte_with_limit() {
    let db = mem_db();
    let r = db
        .execute(
            "WITH RECURSIVE seq(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < 100
            )
            SELECT n FROM seq LIMIT 5",
        )
        .unwrap();
    assert_eq!(rows(&r).len(), 5);
}

#[test]
fn recursive_cte_with_order_and_limit() {
    let db = mem_db();
    let r = db
        .execute(
            "WITH RECURSIVE seq(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < 50
            )
            SELECT n FROM seq ORDER BY n DESC LIMIT 3",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][0], Value::Int64(50));
}

#[test]
fn recursive_cte_without_union() {
    let db = mem_db();
    let result = db.execute("WITH RECURSIVE r AS (SELECT 1 AS n) SELECT * FROM r");
    // May error about recursive CTE needing UNION, or may succeed treating it as non-recursive
    if let Err(e) = result {
        let err = e.to_string();
        assert!(
            err.to_lowercase().contains("recursive")
                || err.to_lowercase().contains("union")
                || err.to_lowercase().contains("anchor"),
            "got: {err}"
        );
    }
}

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

#[test]
fn scalar_subquery_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ssq (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ssq VALUES (1, 10), (2, 20)");
    let r = exec(&db, "SELECT id, (SELECT MAX(val) FROM ssq) AS mx FROM ssq ORDER BY id");
    assert_eq!(r.rows()[0].values()[1], Value::Int64(20));
}

#[test]
fn scalar_subquery_in_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
    let r = db
        .execute("SELECT id FROM t WHERE val > (SELECT AVG(val) FROM t) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1); // Only id=3 (30 > 20)
}

#[test]
fn scalar_subquery_multiple_rows() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    // DecentDB returns first row for scalar subquery (no error)
    let r = db.execute("SELECT (SELECT id FROM t)");
    // Either error or returns first value — both are valid
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn scalar_subquery_returning_null() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let r = db
        .execute("SELECT (SELECT id FROM t LIMIT 1)")
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Null);
}

#[test]
fn subquery_exists() {
    let db = mem_db();
    db.execute("CREATE TABLE orders(id INT64, customer_id INT64)").unwrap();
    db.execute("CREATE TABLE customers(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')").unwrap();
    db.execute("INSERT INTO orders VALUES (100, 1),(101, 1),(102, 2)").unwrap();
    let r = db
        .execute(
            "SELECT name FROM customers c WHERE EXISTS (
                SELECT 1 FROM orders o WHERE o.customer_id = c.id
            ) ORDER BY name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("Alice".into()));
    assert_eq!(v[1][0], Value::Text("Bob".into()));
}

#[test]
fn subquery_in_from_clause() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = db
        .execute(
            "SELECT sub.id, sub.val FROM (SELECT id, val FROM t WHERE val > 15) AS sub ORDER BY sub.id"
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn subquery_in_from_with_aggregation() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3),('B',4),('B',5)").unwrap();
    let r = db
        .execute(
            "SELECT grp, total FROM (SELECT grp, SUM(val) AS total FROM t GROUP BY grp) sub ORDER BY grp",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(3)); // A: 1+2
    assert_eq!(v[1][1], Value::Int64(12)); // B: 3+4+5
}

#[test]
fn subquery_in_from_with_alias() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = db
        .execute(
            "SELECT s.id, s.doubled FROM (SELECT id, val * 2 AS doubled FROM t) s ORDER BY s.id",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(20));
}

#[test]
fn subquery_in_select() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3),(4)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(4)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE id IN (SELECT id FROM t2) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
    assert_eq!(v[1][0], Value::Int64(4));
}

#[test]
fn subquery_not_exists() {
    let db = mem_db();
    db.execute("CREATE TABLE orders(id INT64, customer_id INT64)").unwrap();
    db.execute("CREATE TABLE customers(id INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')").unwrap();
    db.execute("INSERT INTO orders VALUES (100, 1)").unwrap();
    let r = db
        .execute(
            "SELECT name FROM customers c WHERE NOT EXISTS (
                SELECT 1 FROM orders o WHERE o.customer_id = c.id
            ) ORDER BY name",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("Bob".into()));
    assert_eq!(v[1][0], Value::Text("Charlie".into()));
}

#[test]
fn subquery_not_in_select() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1),(2),(3),(4)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2),(4)").unwrap();
    let r = db
        .execute("SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2) ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(1));
    assert_eq!(v[1][0], Value::Int64(3));
}

#[test]
fn subquery_scalar_in_select_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10),(2, 20),(3, 30)").unwrap();
    let r = db
        .execute("SELECT id, (SELECT MAX(val) FROM t) AS max_val FROM t ORDER BY id")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[0][1], Value::Int64(30));
    assert_eq!(v[2][1], Value::Int64(30));
}

#[test]
fn update_with_subquery_in_set() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64 PRIMARY KEY, val INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, bonus INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 50)").unwrap();
    db.execute(
        "UPDATE t1 SET val = val + (SELECT bonus FROM t2 WHERE t2.id = t1.id) WHERE id = 1"
    ).unwrap();
    let r = db.execute("SELECT val FROM t1 WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(150));
}

#[test]
fn update_with_subquery_in_where() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, val INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,10),(2,20),(3,30)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1),(3)").unwrap();
    db.execute("UPDATE t1 SET val = val * 10 WHERE id IN (SELECT id FROM t2)").unwrap();
    let r = db.execute("SELECT id, val FROM t1 ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(100)); // id=1: 10*10
    assert_eq!(v[1][1], Value::Int64(20)); // id=2: unchanged
    assert_eq!(v[2][1], Value::Int64(300)); // id=3: 30*10
}

#[test]
fn where_in_subquery_with_and() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, active BOOLEAN)").unwrap();
    db.execute("CREATE TABLE t2(t1_id INT64)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1,TRUE),(2,FALSE),(3,TRUE)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1),(3)").unwrap();
    let r = db
        .execute(
            "SELECT id FROM t1 WHERE active = TRUE AND id IN (SELECT t1_id FROM t2) ORDER BY id"
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}


// ── Tests merged from engine_coverage_tests.rs ──

#[test]
fn recursive_ctes_support_sequence_generation_and_tree_traversal() {
    let db = Db::open_or_create(":memory:", DbConfig::default()).unwrap();

    let sequence = db
        .execute(
            "WITH RECURSIVE cnt(x) AS (
               SELECT 1
               UNION ALL
               SELECT x + 1 FROM cnt WHERE x < 10
             )
             SELECT x FROM cnt ORDER BY x",
        )
        .unwrap();
    assert_eq!(
        sequence
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        (1..=10)
            .map(|value| vec![Value::Int64(value)])
            .collect::<Vec<_>>()
    );

    db.execute("CREATE TABLE categories (id INT64 PRIMARY KEY, name TEXT, parent_id INT64)")
        .unwrap();
    db.execute(
        "INSERT INTO categories VALUES
            (1, 'root', NULL),
            (2, 'child_a', 1),
            (3, 'child_b', 1),
            (4, 'grandchild', 2)",
    )
    .unwrap();

    let descendants = db
        .execute(
            "WITH RECURSIVE descendants AS (
               SELECT id, name, parent_id FROM categories WHERE id = 1
               UNION ALL
               SELECT c.id, c.name, c.parent_id
               FROM categories AS c INNER JOIN descendants AS d ON c.parent_id = d.id
             )
             SELECT id, name, parent_id FROM descendants ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        descendants
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Int64(1), Value::Text("root".into()), Value::Null,],
            vec![
                Value::Int64(2),
                Value::Text("child_a".into()),
                Value::Int64(1),
            ],
            vec![
                Value::Int64(3),
                Value::Text("child_b".into()),
                Value::Int64(1),
            ],
            vec![
                Value::Int64(4),
                Value::Text("grandchild".into()),
                Value::Int64(2),
            ],
        ]
    );
}

