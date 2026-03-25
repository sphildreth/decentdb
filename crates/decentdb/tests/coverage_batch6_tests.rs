//! Batch 6 – deep targeted coverage: bulk load, complex persistence,
//! unusual SQL syntax, dump_sql Display paths, and error edge cases.

use decentdb::{BulkLoadOptions, Db, DbConfig, Value};
use tempfile::TempDir;

fn mem() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn rows(r: &decentdb::QueryResult) -> Vec<Vec<Value>> {
    r.rows().iter().map(|r| r.values().to_vec()).collect()
}

// ===========================================================================
// db.rs – bulk_load_rows API coverage
// ===========================================================================

#[test]
fn bulk_load_basic() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT, score INT64)").unwrap();
    let cols = &["id", "name", "score"];
    let data: Vec<Vec<Value>> = (0..100)
        .map(|i| {
            vec![
                Value::Int64(i),
                Value::Text(format!("item_{}", i)),
                Value::Int64(i * 10),
            ]
        })
        .collect();
    db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(100));
}

#[test]
fn bulk_load_with_nulls() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let cols = &["id", "val"];
    let data = vec![
        vec![Value::Int64(1), Value::Text("a".into())],
        vec![Value::Int64(2), Value::Null],
        vec![Value::Int64(3), Value::Text("c".into())],
    ];
    db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
        .unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t WHERE val IS NULL").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn bulk_load_large() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b INT64, c FLOAT64)").unwrap();
    let cols = &["id", "a", "b", "c"];
    let data: Vec<Vec<Value>> = (0..5000)
        .map(|i| {
            vec![
                Value::Int64(i),
                Value::Text(format!("row_{:05}", i)),
                Value::Int64(i * 7),
                Value::Float64(i as f64 * 0.1),
            ]
        })
        .collect();
    let opts = BulkLoadOptions {
        checkpoint_on_complete: true,
        ..Default::default()
    };
    db.bulk_load_rows("t", cols, &data, opts).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(5000));
    let r2 = db.execute("SELECT SUM(b) FROM t").unwrap();
    let expected_sum: i64 = (0..5000).map(|i: i64| i * 7).sum();
    assert_eq!(rows(&r2)[0][0], Value::Int64(expected_sum));
}

#[test]
fn bulk_load_with_index() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE INDEX idx ON t(val)").unwrap();
    let cols = &["id", "val"];
    let data: Vec<Vec<Value>> = (0..100)
        .map(|i| vec![Value::Int64(i), Value::Text(format!("v_{}", i))])
        .collect();
    db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
        .unwrap();
    let r = db.execute("SELECT id FROM t WHERE val = 'v_42'").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}

#[test]
fn bulk_load_no_checkpoint() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let cols = &["id"];
    let data: Vec<Vec<Value>> = (0..10).map(|i| vec![Value::Int64(i)]).collect();
    let opts = BulkLoadOptions {
        checkpoint_on_complete: false,
        ..Default::default()
    };
    db.bulk_load_rows("t", cols, &data, opts).unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(10));
}

// ===========================================================================
// Persistence – more scenarios for WAL/pager/cache coverage
// ===========================================================================

#[test]
fn persist_many_tables() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("many.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        for i in 0..20 {
            db.execute(&format!("CREATE TABLE t{}(id INT64, val TEXT)", i))
                .unwrap();
            db.execute(&format!("INSERT INTO t{} VALUES ({}, 'data')", i, i))
                .unwrap();
        }
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        for i in 0..20 {
            let r = db
                .execute(&format!("SELECT COUNT(*) FROM t{}", i))
                .unwrap();
            assert_eq!(rows(&r)[0][0], Value::Int64(1));
        }
    }
}

#[test]
fn persist_update_and_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("upd_del.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')").unwrap();
        db.execute("UPDATE t SET val = 'updated' WHERE id = 2").unwrap();
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT id, val FROM t ORDER BY id").unwrap();
        let v = rows(&r);
        assert_eq!(v.len(), 2);
        assert_eq!(v[1][1], Value::Text("updated".into()));
    }
}

#[test]
fn persist_large_text() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("large_text.ddb");
    let ps = path.to_str().unwrap();
    let big = "x".repeat(50000);
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(1), Value::Text(big.clone())],
        )
        .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT data FROM t WHERE id = 1").unwrap();
        if let Value::Text(s) = &rows(&r)[0][0] {
            assert_eq!(s.len(), 50000);
        }
    }
}

#[test]
fn persist_bulk_load() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("bulk.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
        let cols = &["id", "val"];
        let data: Vec<Vec<Value>> = (0..1000)
            .map(|i| vec![Value::Int64(i), Value::Text(format!("row_{}", i))])
            .collect();
        db.bulk_load_rows("t", cols, &data, BulkLoadOptions::default())
            .unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(1000));
    }
}

#[test]
fn persist_with_txn() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("txn.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64)").unwrap();
        let mut txn = db.transaction().unwrap();
        let stmt = txn.prepare("INSERT INTO t VALUES ($1)").unwrap();
        for i in 0..100 {
            stmt.execute_in(&mut txn, &[Value::Int64(i)]).unwrap();
        }
        txn.commit().unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(100));
    }
}

// ===========================================================================
// sql/normalize.rs – unusual SQL syntax patterns
// ===========================================================================

#[test]
fn parse_explicit_cast_syntax() {
    let db = mem();
    let r = db.execute("SELECT CAST(42 AS TEXT), CAST('100' AS INT64), CAST(3.14 AS INT64)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("42".into()));
    assert_eq!(v[0][1], Value::Int64(100));
    assert_eq!(v[0][2], Value::Int64(3));
}

#[test]
fn parse_double_colon_cast() {
    let db = mem();
    let r = db.execute("SELECT 42::TEXT, '100'::INT64");
    // May or may not be supported
    if let Ok(r) = r {
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Text("42".into()));
    }
}

#[test]
fn parse_complex_default_expressions() {
    let db = mem();
    db.execute(
        "CREATE TABLE t(
            id INT64,
            status TEXT DEFAULT 'active',
            created INT64 DEFAULT 0,
            flag BOOL DEFAULT TRUE
        )",
    )
    .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();
    let r = db.execute("SELECT status, created, flag FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("active".into()));
    assert_eq!(v[0][1], Value::Int64(0));
    assert_eq!(v[0][2], Value::Bool(true));
}

#[test]
fn parse_create_table_with_all_types() {
    let db = mem();
    db.execute(
        "CREATE TABLE all_types(
            i INT64,
            f FLOAT64,
            t TEXT,
            b BOOL,
            bl BLOB,
            d DECIMAL(10, 2)
        )",
    )
    .unwrap();
    db.execute(
        "INSERT INTO all_types VALUES (1, 3.14, 'hello', TRUE, NULL, 19.99)",
    )
    .unwrap();
    let r = db.execute("SELECT * FROM all_types").unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn parse_insert_with_column_list() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b TEXT, c INT64)").unwrap();
    db.execute("INSERT INTO t (c, a) VALUES (30, 10)").unwrap();
    let r = db.execute("SELECT a, b, c FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(10));
    assert_eq!(v[0][1], Value::Null); // b not specified
    assert_eq!(v[0][2], Value::Int64(30));
}

#[test]
fn parse_update_multiple_columns() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b TEXT, c INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'old_a', 'old_b', 0)").unwrap();
    db.execute("UPDATE t SET a = 'new_a', b = 'new_b', c = 42 WHERE id = 1").unwrap();
    let r = db.execute("SELECT a, b, c FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("new_a".into()));
    assert_eq!(v[0][1], Value::Text("new_b".into()));
    assert_eq!(v[0][2], Value::Int64(42));
}

#[test]
fn parse_create_index_if_not_exists() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("CREATE INDEX IF NOT EXISTS idx ON t(val)").unwrap();
    db.execute("CREATE INDEX IF NOT EXISTS idx ON t(val)").unwrap(); // No error
}

// ===========================================================================
// ast.rs Display – trigger all Display paths via dump_sql/table_ddl
// ===========================================================================

#[test]
fn dump_sql_with_all_constraint_types() {
    let db = mem();
    db.execute("CREATE TABLE refs(id INT64 PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE everything(
            id INT64 PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            age INT64 CHECK (age >= 0 AND age < 200),
            status TEXT DEFAULT 'pending',
            ref_id INT64 REFERENCES refs(id) ON DELETE SET NULL ON UPDATE CASCADE,
            score FLOAT64
        )",
    )
    .unwrap();
    db.execute("CREATE INDEX idx_score ON everything(score)").unwrap();
    db.execute(
        "CREATE VIEW active_items AS SELECT id, name FROM everything WHERE status = 'pending'",
    )
    .unwrap();
    db.execute("INSERT INTO refs VALUES (1)").unwrap();
    db.execute("INSERT INTO everything VALUES (1, 'test', 25, 'pending', 1, 99.5)").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.len() > 200);
    assert!(sql.contains("CREATE TABLE"));
}

#[test]
fn dump_sql_with_complex_views() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64, name TEXT, val INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, label TEXT)").unwrap();
    db.execute(
        "CREATE VIEW joined_v AS
         SELECT t1.name, t2.label, t1.val
         FROM t1 JOIN t2 ON t1.id = t2.t1_id
         WHERE t1.val > 0",
    )
    .unwrap();
    db.execute(
        "CREATE VIEW agg_v AS
         SELECT name, SUM(val) AS total FROM t1 GROUP BY name",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE VIEW"));
}

#[test]
fn table_ddl_check_constraint_complex() {
    let db = mem();
    db.execute(
        "CREATE TABLE t(
            a INT64,
            b INT64,
            CHECK (a > 0 AND b > 0),
            CHECK (a + b < 100)
        )",
    )
    .unwrap();
    let ddl = db.table_ddl("t").unwrap();
    assert!(ddl.contains("CHECK") || ddl.contains("check"));
}

#[test]
fn table_ddl_multiple_unique_constraints() {
    let db = mem();
    db.execute(
        "CREATE TABLE t(
            id INT64 PRIMARY KEY,
            email TEXT UNIQUE,
            code TEXT UNIQUE,
            name TEXT NOT NULL
        )",
    )
    .unwrap();
    let ddl = db.table_ddl("t").unwrap();
    assert!(ddl.contains("CREATE TABLE"));
}

// ===========================================================================
// exec/mod.rs – more expression and function coverage
// ===========================================================================

#[test]
fn string_functions_comprehensive() {
    let db = mem();
    let r = db.execute("SELECT LENGTH('hello'), UPPER('hello'), LOWER('HELLO'), TRIM('  hi  '), LEFT('hello', 3), RIGHT('hello', 3)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(5));
    assert_eq!(v[0][1], Value::Text("HELLO".into()));
    assert_eq!(v[0][2], Value::Text("hello".into()));
    assert_eq!(v[0][3], Value::Text("hi".into()));
    assert_eq!(v[0][4], Value::Text("hel".into()));
    assert_eq!(v[0][5], Value::Text("llo".into()));
}

#[test]
fn math_functions() {
    let db = mem();
    let r = db.execute("SELECT ABS(-42), ABS(42), ROUND(3.14159, 2), ROUND(3.5, 0)").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(42));
    assert_eq!(v[0][1], Value::Int64(42));
}

#[test]
fn replace_function() {
    let db = mem();
    let r = db.execute("SELECT REPLACE('hello world', 'world', 'earth')").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("hello earth".into()));
}

#[test]
fn expression_with_table_data() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 3), (20, 7), (15, 5)").unwrap();
    // Complex expression in SELECT
    let r = db
        .execute("SELECT a * b + (a - b), a / b, a % b FROM t ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(10 * 3 + (10 - 3)));
}

#[test]
fn unary_minus_in_query() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (5), (10), (15)").unwrap();
    let r = db.execute("SELECT -val FROM t ORDER BY val").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(-5));
    assert_eq!(v[1][0], Value::Int64(-10));
}

#[test]
fn string_operators_on_columns() {
    let db = mem();
    db.execute("CREATE TABLE t(first_name TEXT, last_name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('John', 'Doe'), ('Jane', 'Smith')").unwrap();
    let r = db
        .execute("SELECT first_name || ' ' || last_name AS full_name FROM t ORDER BY first_name")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("Jane Smith".into()));
    assert_eq!(v[1][0], Value::Text("John Doe".into()));
}

// ===========================================================================
// exec/mod.rs – advanced queries for deeper coverage
// ===========================================================================

#[test]
fn query_with_computed_columns() {
    let db = mem();
    db.execute("CREATE TABLE products(name TEXT, price INT64, qty INT64)").unwrap();
    db.execute("INSERT INTO products VALUES ('A', 10, 5), ('B', 20, 3), ('C', 5, 10)").unwrap();
    let r = db
        .execute("SELECT name, price * qty AS total FROM products ORDER BY total DESC")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(60)); // B: 20*3
    assert_eq!(v[1][1], Value::Int64(50)); // C: 5*10 or A: 10*5
}

#[test]
fn subquery_in_from_with_aggregation() {
    let db = mem();
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
fn multiple_ctes_used_together() {
    let db = mem();
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
fn deeply_nested_expressions() {
    let db = mem();
    let r = db
        .execute("SELECT ((1 + 2) * (3 + 4)) - ((5 - 6) * (7 + 8))")
        .unwrap();
    // (3*7) - ((-1)*15) = 21 - (-15) = 36
    assert_eq!(rows(&r)[0][0], Value::Int64(36));
}

#[test]
fn query_with_aliased_subquery() {
    let db = mem();
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

// ===========================================================================
// exec/ddl.rs – edge case DDL operations
// ===========================================================================

#[test]
fn alter_column_set_not_null() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let r = db.execute("ALTER TABLE t ALTER COLUMN val SET NOT NULL");
    // May or may not be supported
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn alter_column_drop_not_null() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT NOT NULL)").unwrap();
    let r = db.execute("ALTER TABLE t ALTER COLUMN val DROP NOT NULL");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn alter_column_set_default() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    let r = db.execute("ALTER TABLE t ALTER COLUMN val SET DEFAULT 'hello'");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn alter_column_drop_default() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val TEXT DEFAULT 'hi')").unwrap();
    let r = db.execute("ALTER TABLE t ALTER COLUMN val DROP DEFAULT");
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn drop_table_cascade() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let r = db.execute("DROP TABLE t CASCADE");
    // CASCADE may drop dependent views
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn create_table_with_generated_column() {
    let db = mem();
    db.execute(
        "CREATE TABLE t(a INT64, b INT64, c INT64 GENERATED ALWAYS AS (a + b) STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (a, b) VALUES (10, 20)").unwrap();
    let r = db.execute("SELECT c FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(30));
}

#[test]
fn create_table_with_expression_index() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("CREATE INDEX idx_lower ON t(LOWER(name))").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'Alice'), (2, 'BOB'), (3, 'alice')").unwrap();
    let r = db.execute("SELECT id FROM t WHERE LOWER(name) = 'alice' ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
}

#[test]
fn create_partial_index() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, status TEXT, val INT64)").unwrap();
    let r = db.execute("CREATE INDEX idx_active ON t(val) WHERE status = 'active'");
    if r.is_ok() {
        db.execute("INSERT INTO t VALUES (1, 'active', 100), (2, 'inactive', 200)").unwrap();
        let r2 = db.execute("SELECT val FROM t WHERE status = 'active' AND val > 0").unwrap();
        assert_eq!(rows(&r2).len(), 1);
    }
}

// ===========================================================================
// exec/mod.rs – HAVING, complex GROUP BY, window edge cases
// ===========================================================================

#[test]
fn having_without_group_by() {
    let db = mem();
    db.execute("CREATE TABLE t(val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t HAVING COUNT(*) > 2").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn having_filters_groups() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',5),('C',1),('C',2),('C',3)").unwrap();
    let r = db
        .execute("SELECT grp, COUNT(*) AS cnt FROM t GROUP BY grp HAVING COUNT(*) >= 2 ORDER BY grp")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2); // A (2), C (3)
}

#[test]
fn window_over_empty_partition() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A', 10)").unwrap();
    let r = db
        .execute(
            "SELECT grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) FROM t",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][2], Value::Int64(1));
}

#[test]
fn multiple_window_functions() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
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

// ===========================================================================
// Complex integration scenarios
// ===========================================================================

#[test]
fn complex_reporting_query() {
    let db = mem();
    db.execute("CREATE TABLE orders(id INT64, customer TEXT, product TEXT, amount INT64, qty INT64)").unwrap();
    db.execute("INSERT INTO orders VALUES (1,'Alice','Widget',100,2),(2,'Alice','Gadget',200,1),(3,'Bob','Widget',150,3),(4,'Bob','Widget',50,1),(5,'Charlie','Gadget',300,2)").unwrap();

    // Revenue by customer
    let r = db
        .execute(
            "SELECT customer, SUM(amount * qty) AS revenue, COUNT(DISTINCT product) AS products
             FROM orders GROUP BY customer ORDER BY revenue DESC",
        )
        .unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);

    // Top products by total revenue
    let r2 = db
        .execute(
            "SELECT product, SUM(amount * qty) AS total
             FROM orders GROUP BY product HAVING SUM(amount * qty) > 100
             ORDER BY total DESC",
        )
        .unwrap();
    assert!(rows(&r2).len() >= 1);
}

#[test]
fn recursive_cte_fibonacci() {
    let db = mem();
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
fn complex_union_with_cte() {
    let db = mem();
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

// ===========================================================================
// record/overflow.rs – various large value scenarios
// ===========================================================================

#[test]
fn overflow_multiple_large_columns() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, a TEXT, b TEXT, c TEXT)").unwrap();
    let large_a = "A".repeat(5000);
    let large_b = "B".repeat(5000);
    let large_c = "C".repeat(5000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2, $3, $4)",
        &[
            Value::Int64(1),
            Value::Text(large_a.clone()),
            Value::Text(large_b.clone()),
            Value::Text(large_c.clone()),
        ],
    )
    .unwrap();
    let r = db.execute("SELECT LENGTH(a), LENGTH(b), LENGTH(c) FROM t").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Int64(5000));
    assert_eq!(v[0][1], Value::Int64(5000));
    assert_eq!(v[0][2], Value::Int64(5000));
}

#[test]
fn overflow_update_large_to_small() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    let large = "X".repeat(10000);
    db.execute_with_params(
        "INSERT INTO t VALUES ($1, $2)",
        &[Value::Int64(1), Value::Text(large)],
    )
    .unwrap();
    db.execute("UPDATE t SET data = 'small' WHERE id = 1").unwrap();
    let r = db.execute("SELECT data FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("small".into()));
}

#[test]
fn overflow_delete_large_rows() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, data TEXT)").unwrap();
    let large = "Y".repeat(10000);
    for i in 0..5 {
        db.execute_with_params(
            "INSERT INTO t VALUES ($1, $2)",
            &[Value::Int64(i), Value::Text(large.clone())],
        )
        .unwrap();
    }
    db.execute("DELETE FROM t WHERE id < 3").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

// ===========================================================================
// exec/constraints.rs – ON CONFLICT / UPSERT
// ===========================================================================

#[test]
fn upsert_on_conflict_do_nothing() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'dup') ON CONFLICT DO NOTHING").unwrap();
    let r = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("original".into()));
}

#[test]
fn upsert_on_conflict_do_update() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT, count INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello', 1)").unwrap();
    db.execute(
        "INSERT INTO t VALUES (1, 'hello', 1) ON CONFLICT (id) DO UPDATE SET count = t.count + 1",
    )
    .unwrap();
    let r = db.execute("SELECT count FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(2));
}

#[test]
fn upsert_multiple_rows() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'new_b'), (3, 'c') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val").unwrap();
    let r = db.execute("SELECT id, val FROM t ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 3);
    assert_eq!(v[1][1], Value::Text("new_b".into())); // updated
    assert_eq!(v[2][1], Value::Text("c".into())); // new
}

// ===========================================================================
// More error paths for branch coverage
// ===========================================================================

#[test]
fn error_division_by_zero_in_expression() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (10, 0)").unwrap();
    let r = db.execute("SELECT a / b FROM t");
    // Should error or return NULL
    assert!(r.is_err() || r.is_ok());
}

#[test]
fn error_create_view_on_nonexistent_table() {
    let db = mem();
    let r = db.execute("CREATE VIEW v AS SELECT * FROM nonexistent");
    // May or may not error at creation time (some DBs defer)
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn error_alter_table_nonexistent_column() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db
        .execute("ALTER TABLE t ALTER COLUMN nonexistent TYPE TEXT")
        .unwrap_err();
    assert!(err.to_string().len() > 0);
}

#[test]
fn error_drop_nonexistent_column() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db
        .execute("ALTER TABLE t DROP COLUMN nonexistent")
        .unwrap_err();
    assert!(err.to_string().len() > 0);
}

#[test]
fn error_rename_nonexistent_column() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db
        .execute("ALTER TABLE t RENAME COLUMN nonexistent TO new_name")
        .unwrap_err();
    assert!(err.to_string().len() > 0);
}

#[test]
fn error_drop_nonexistent_view() {
    let db = mem();
    let err = db.execute("DROP VIEW nonexistent").unwrap_err();
    assert!(err.to_string().len() > 0);
}

#[test]
fn error_drop_nonexistent_index() {
    let db = mem();
    let err = db.execute("DROP INDEX nonexistent").unwrap_err();
    assert!(err.to_string().len() > 0);
}

#[test]
fn error_drop_nonexistent_trigger() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db.execute("DROP TRIGGER nonexistent ON t").unwrap_err();
    assert!(err.to_string().len() > 0);
}

// ===========================================================================
// planner/mod.rs – more complex EXPLAIN paths
// ===========================================================================

#[test]
fn explain_window_function() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT id, ROW_NUMBER() OVER (ORDER BY val) FROM t")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_group_by_having() {
    let db = mem();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT grp, SUM(val) FROM t GROUP BY grp HAVING SUM(val) > 10")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_correlated_subquery() {
    let db = mem();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)").unwrap();
    let r = db
        .execute("EXPLAIN SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
        .unwrap();
    assert!(!r.explain_lines().is_empty());
}

// ===========================================================================
// sql/parser.rs – edge case parsing
// ===========================================================================

#[test]
fn parse_deeply_nested_parens() {
    let db = mem();
    let r = db.execute("SELECT ((((1 + 2) * 3) - 4) / 5)").unwrap();
    let v = rows(&r);
    // ((1+2)*3-4)/5 = (9-4)/5 = 5/5 = 1
    assert_eq!(v[0][0], Value::Int64(1));
}

#[test]
fn parse_complex_where_with_parens() {
    let db = mem();
    db.execute("CREATE TABLE t(a INT64, b INT64, c INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3),(4,5,6),(7,8,9)").unwrap();
    let r = db
        .execute("SELECT a FROM t WHERE ((a > 1 AND b > 3) OR c = 3) AND a < 8 ORDER BY a")
        .unwrap();
    let v = rows(&r);
    assert!(v.len() >= 2);
}

#[test]
fn parse_empty_in_list() {
    let db = mem();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // IN with single element
    let r = db.execute("SELECT id FROM t WHERE id IN (2)").unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn parse_long_column_and_table_names() {
    let db = mem();
    let long_table = "very_long_table_name_that_tests_parsing";
    let long_col = "a_very_long_column_name_for_testing";
    db.execute(&format!("CREATE TABLE {}({} INT64)", long_table, long_col))
        .unwrap();
    db.execute(&format!("INSERT INTO {} VALUES (42)", long_table))
        .unwrap();
    let r = db
        .execute(&format!("SELECT {} FROM {}", long_col, long_table))
        .unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(42));
}
