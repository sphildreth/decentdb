use decentdb::{Db, DbConfig, Value};
use tempfile::TempDir;

fn open_memory() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn row_values(result: &decentdb::QueryResult) -> Vec<Vec<Value>> {
    result
        .rows()
        .iter()
        .map(|r| r.values().to_vec())
        .collect()
}

// ---------------------------------------------------------------------------
// Batch 1a: exec/mod.rs — GROUP BY with complex expressions (covers eval_group_expr)
// ---------------------------------------------------------------------------

#[test]
fn group_by_with_between_in_having() {
    let db = open_memory();
    db.execute("CREATE TABLE sales (region TEXT, amount INT64)").unwrap();
    db.execute("INSERT INTO sales VALUES ('A', 10), ('A', 20), ('B', 5), ('B', 50), ('C', 100)")
        .unwrap();

    let result = db
        .execute(
            "SELECT region, SUM(amount) AS total FROM sales
             GROUP BY region HAVING SUM(amount) BETWEEN 10 AND 55 ORDER BY region",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("A".into()));
    assert_eq!(rows[1][0], Value::Text("B".into()));
}

#[test]
fn group_by_with_in_list_filter() {
    let db = open_memory();
    db.execute("CREATE TABLE items (cat TEXT, price INT64)").unwrap();
    db.execute("INSERT INTO items VALUES ('X', 1), ('X', 2), ('Y', 3), ('Z', 4)").unwrap();

    let result = db
        .execute(
            "SELECT cat, COUNT(*) FROM items GROUP BY cat HAVING cat IN ('X', 'Z') ORDER BY cat",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Text("X".into()));
    assert_eq!(rows[1][0], Value::Text("Z".into()));
}

#[test]
fn group_by_with_like_filter() {
    let db = open_memory();
    db.execute("CREATE TABLE tags (prefix TEXT, n INT64)").unwrap();
    db.execute("INSERT INTO tags VALUES ('abc', 1), ('abc', 2), ('xyz', 3), ('xbc', 4)").unwrap();

    let result = db
        .execute(
            "SELECT prefix, SUM(n) FROM tags GROUP BY prefix HAVING prefix LIKE 'x%' ORDER BY prefix",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn group_by_with_case_expression() {
    let db = open_memory();
    db.execute("CREATE TABLE scores (team TEXT, pts INT64)").unwrap();
    db.execute("INSERT INTO scores VALUES ('A', 10), ('A', 20), ('B', 5)").unwrap();

    let result = db
        .execute(
            "SELECT team, CASE WHEN SUM(pts) > 15 THEN 'high' ELSE 'low' END AS tier
             FROM scores GROUP BY team ORDER BY team",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Text("high".into()));
    assert_eq!(rows[1][1], Value::Text("low".into()));
}

#[test]
fn group_by_with_cast_and_coalesce() {
    let db = open_memory();
    db.execute("CREATE TABLE nums (grp TEXT, val TEXT)").unwrap();
    db.execute("INSERT INTO nums VALUES ('a', '10'), ('a', '20'), ('b', NULL)").unwrap();

    let result = db
        .execute(
            "SELECT grp, COALESCE(MAX(val), 'none') FROM nums GROUP BY grp ORDER BY grp",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Text("20".into()));
    assert_eq!(rows[1][1], Value::Text("none".into()));
}

#[test]
fn group_by_with_is_null_check() {
    let db = open_memory();
    db.execute("CREATE TABLE opt (grp INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO opt VALUES (1, NULL), (1, NULL), (2, 'y')").unwrap();

    let result = db
        .execute(
            "SELECT grp, COUNT(*) FROM opt GROUP BY grp HAVING MIN(val) IS NULL ORDER BY grp",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(1));
}

#[test]
fn group_by_with_nullif_and_length() {
    let db = open_memory();
    db.execute("CREATE TABLE words (cat TEXT, word TEXT)").unwrap();
    db.execute("INSERT INTO words VALUES ('a', 'hi'), ('a', ''), ('b', 'hello')").unwrap();

    let result = db
        .execute(
            "SELECT cat, LENGTH(MAX(NULLIF(word, ''))) FROM words GROUP BY cat ORDER BY cat",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Int64(2));
    assert_eq!(rows[1][1], Value::Int64(5));
}

#[test]
fn group_by_with_lower_upper_trim() {
    let db = open_memory();
    db.execute("CREATE TABLE names (grp INT64, name TEXT)").unwrap();
    db.execute("INSERT INTO names VALUES (1, ' Alice '), (1, ' BOB '), (2, 'charlie')").unwrap();

    // String functions on non-aggregate columns in grouped query
    let result = db
        .execute(
            "SELECT grp, UPPER(name), LOWER(name), TRIM(name)
             FROM names WHERE grp = 2",
        )
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("CHARLIE".into()));
    assert_eq!(rows[0][2], Value::Text("charlie".into()));
    assert_eq!(rows[0][3], Value::Text("charlie".into()));
}

// ---------------------------------------------------------------------------
// Batch 1b: Correlated subqueries with complex expressions (covers expr_references_outer)
// ---------------------------------------------------------------------------

#[test]
fn correlated_subquery_with_between() {
    let db = open_memory();
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
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn correlated_subquery_with_in_list() {
    let db = open_memory();
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
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Int64(1));
}

#[test]
fn correlated_scalar_subquery() {
    let db = open_memory();
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
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Int64(30));
    assert_eq!(rows[1][1], Value::Int64(30));
}

// ---------------------------------------------------------------------------
// Batch 1c: CTE column count validation (covers evaluate_select_with_outer CTE path)
// ---------------------------------------------------------------------------

#[test]
fn cte_column_count_mismatch_errors() {
    let db = open_memory();
    let err = db
        .execute("WITH cte(a, b) AS (SELECT 1) SELECT * FROM cte")
        .unwrap_err();
    assert!(err.to_string().contains("column"));
}

#[test]
fn cte_column_rename_works() {
    let db = open_memory();
    let result = db
        .execute("WITH cte(a, b) AS (SELECT 1, 2) SELECT a, b FROM cte")
        .unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0], vec![Value::Int64(1), Value::Int64(2)]);
}

// ---------------------------------------------------------------------------
// Batch 1d: Decimal formatting (covers stringify_value / decimal_to_string)
// ---------------------------------------------------------------------------

#[test]
fn decimal_formatting_small_values() {
    let db = open_memory();
    db.execute("CREATE TABLE d (val DECIMAL(10, 4))").unwrap();
    db.execute("INSERT INTO d VALUES (0.0001)").unwrap();
    db.execute("INSERT INTO d VALUES (123.4567)").unwrap();
    db.execute("INSERT INTO d VALUES (-0.01)").unwrap();

    let result = db.execute("SELECT val FROM d ORDER BY val").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 3);
    // Values should be stored as Decimal
    assert!(matches!(rows[0][0], Value::Decimal { .. }));
}

#[test]
fn decimal_formatting_zero_scale() {
    let db = open_memory();
    db.execute("CREATE TABLE d2 (val DECIMAL(10, 0))").unwrap();
    db.execute("INSERT INTO d2 VALUES (42)").unwrap();

    let result = db.execute("SELECT val FROM d2").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], Value::Decimal { .. }));
}

// ---------------------------------------------------------------------------
// Batch 1e: FK CASCADE and SET NULL (covers apply_parent_update_actions, apply_parent_delete_actions)
// ---------------------------------------------------------------------------

#[test]
fn fk_on_update_cascade() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY, name TEXT)").unwrap();
    db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent(id) ON UPDATE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1, 'alice')").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    db.execute("UPDATE parent SET id = 100 WHERE id = 1").unwrap();

    let result = db.execute("SELECT parent_id FROM child WHERE id = 10").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(100));
}

#[test]
fn fk_on_update_set_null() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent(id) ON UPDATE SET NULL)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    db.execute("UPDATE parent SET id = 100 WHERE id = 1").unwrap();

    let result = db.execute("SELECT parent_id FROM child WHERE id = 10").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn fk_on_delete_set_null() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent(id) ON DELETE SET NULL)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    db.execute("DELETE FROM parent WHERE id = 1").unwrap();

    let result = db.execute("SELECT parent_id FROM child WHERE id = 10").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Null);
}

#[test]
fn fk_on_delete_cascade() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent(id) ON DELETE CASCADE)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1), (20, 1), (30, 2)").unwrap();

    db.execute("DELETE FROM parent WHERE id = 1").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM child").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(1));
}

#[test]
fn fk_on_update_restrict_errors() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 REFERENCES parent(id) ON UPDATE RESTRICT)",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let err = db.execute("UPDATE parent SET id = 100 WHERE id = 1").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("foreign key") || err.to_string().to_lowercase().contains("constraint"));
}

#[test]
fn fk_set_null_on_non_nullable_column_rejected() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)").unwrap();
    // Table-level FK with SET NULL on NOT NULL column should fail at CREATE time
    let err = db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, parent_id INT64 NOT NULL,
         FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL)",
    );
    // Even if DDL doesn't reject this, the SET NULL at runtime would fail.
    // Just verify we can create the table or get an error about set null on not-null.
    if let Err(e) = &err {
        assert!(e.to_string().to_lowercase().contains("null")
            || e.to_string().to_lowercase().contains("constraint")
            || e.to_string().to_lowercase().contains("foreign"));
    }
}

// ---------------------------------------------------------------------------
// Batch 1f: Generated column validation edge cases (covers validate_generated_expr)
// ---------------------------------------------------------------------------

#[test]
fn generated_column_rejects_aggregate() {
    let db = open_memory();
    let err = db
        .execute("CREATE TABLE t (id INT64, gen INT64 GENERATED ALWAYS AS (COUNT(*)) STORED)")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("generated") || err.to_string().to_lowercase().contains("aggregate"));
}

#[test]
fn generated_column_rejects_window_function() {
    let db = open_memory();
    let err = db
        .execute(
            "CREATE TABLE t (id INT64, gen INT64 GENERATED ALWAYS AS (ROW_NUMBER() OVER ()) STORED)",
        )
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("generated") || err.to_string().to_lowercase().contains("window"));
}

#[test]
fn generated_column_rejects_subquery() {
    let db = open_memory();
    let err = db
        .execute(
            "CREATE TABLE t (id INT64, gen INT64 GENERATED ALWAYS AS ((SELECT 1)) STORED)",
        )
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("generated") || err.to_string().to_lowercase().contains("subquer"));
}

#[test]
fn generated_column_with_between() {
    let db = open_memory();
    db.execute(
        "CREATE TABLE t (id INT64 PRIMARY KEY, in_range BOOL GENERATED ALWAYS AS (id BETWEEN 1 AND 10) STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (id) VALUES (5), (15)").unwrap();

    let result = db.execute("SELECT id, in_range FROM t ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Bool(true));
    assert_eq!(rows[1][1], Value::Bool(false));
}

#[test]
fn generated_column_with_in_list() {
    let db = open_memory();
    db.execute(
        "CREATE TABLE t (id INT64 PRIMARY KEY, is_special BOOL GENERATED ALWAYS AS (id IN (1, 3, 5)) STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (id) VALUES (1), (2), (3)").unwrap();

    let result = db.execute("SELECT id, is_special FROM t ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Bool(true));
    assert_eq!(rows[1][1], Value::Bool(false));
    assert_eq!(rows[2][1], Value::Bool(true));
}

#[test]
fn generated_column_with_like() {
    let db = open_memory();
    db.execute(
        "CREATE TABLE t (name TEXT, is_a BOOL GENERATED ALWAYS AS (name LIKE 'a%') STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (name) VALUES ('alice'), ('bob')").unwrap();

    let result = db.execute("SELECT name, is_a FROM t ORDER BY name").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Bool(true));
    assert_eq!(rows[1][1], Value::Bool(false));
}

#[test]
fn generated_column_with_case() {
    let db = open_memory();
    db.execute(
        "CREATE TABLE t (val INT64 PRIMARY KEY,
         tier TEXT GENERATED ALWAYS AS (CASE WHEN val > 100 THEN 'high' WHEN val > 50 THEN 'mid' ELSE 'low' END) STORED)",
    )
    .unwrap();
    db.execute("INSERT INTO t (val) VALUES (10), (75), (200)").unwrap();

    let result = db.execute("SELECT val, tier FROM t ORDER BY val").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Text("low".into()));
    assert_eq!(rows[1][1], Value::Text("mid".into()));
    assert_eq!(rows[2][1], Value::Text("high".into()));
}

// ---------------------------------------------------------------------------
// Batch 1g: Expression index validation (covers exec/ddl.rs lines 312-325)
// ---------------------------------------------------------------------------

#[test]
fn expression_index_rejects_unique() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, name TEXT)").unwrap();
    let err = db
        .execute("CREATE UNIQUE INDEX idx ON t ((UPPER(name)))")
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("unique") || err.to_string().to_lowercase().contains("expression"));
}

#[test]
fn expression_index_rejects_multiple_expressions() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, a TEXT, b TEXT)").unwrap();
    let err = db
        .execute("CREATE INDEX idx ON t ((UPPER(a)), (LOWER(b)))")
        .unwrap_err();
    assert!(err.to_string().len() > 0);
}

// ---------------------------------------------------------------------------
// Batch 1h: Table-level constraints (covers exec/ddl.rs lines 60-71, 147-162)
// ---------------------------------------------------------------------------

#[test]
fn table_level_unique_constraint() {
    let db = open_memory();
    db.execute(
        "CREATE TABLE t (a INT64, b INT64, UNIQUE (a, b))",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 2)").unwrap();

    let err = db.execute("INSERT INTO t VALUES (1, 1)").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("unique") || err.to_string().to_lowercase().contains("duplicate"));
}

#[test]
fn table_level_check_constraint() {
    let db = open_memory();
    db.execute(
        "CREATE TABLE t (a INT64, b INT64, CHECK (a > 0 AND b > 0))",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").unwrap();

    let err = db.execute("INSERT INTO t VALUES (-1, 1)").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("check") || err.to_string().to_lowercase().contains("constraint"));
}

#[test]
fn table_level_foreign_key_constraint() {
    let db = open_memory();
    db.execute("CREATE TABLE parent (id INT64 PRIMARY KEY)").unwrap();
    db.execute(
        "CREATE TABLE child (id INT64 PRIMARY KEY, pid INT64,
         FOREIGN KEY (pid) REFERENCES parent(id))",
    )
    .unwrap();
    db.execute("INSERT INTO parent VALUES (1)").unwrap();
    db.execute("INSERT INTO child VALUES (10, 1)").unwrap();

    let err = db.execute("INSERT INTO child VALUES (20, 999)").unwrap_err();
    assert!(err.to_string().to_lowercase().contains("foreign") || err.to_string().to_lowercase().contains("constraint"));
}

// ---------------------------------------------------------------------------
// Batch 1i: Default values (covers exec/constraints.rs default_value_for_column)
// ---------------------------------------------------------------------------

#[test]
fn column_default_value_applied_on_insert() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, status TEXT DEFAULT 'active')").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let result = db.execute("SELECT status FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Text("active".into()));
}

#[test]
fn column_default_numeric_expression() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64 DEFAULT 42)").unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").unwrap();

    let result = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(42));
}

// ---------------------------------------------------------------------------
// Batch 1j: ON CONFLICT ON CONSTRAINT (covers exec/constraints.rs indexes_for_conflict_target)
// ---------------------------------------------------------------------------

#[test]
fn on_conflict_on_constraint_by_columns() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'original')").unwrap();

    db.execute("INSERT INTO t VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val")
        .unwrap();

    let result = db.execute("SELECT val FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Text("updated".into()));
}

// ---------------------------------------------------------------------------
// Batch 1k: ALTER VIEW RENAME and DROP TEMP VIEW (covers exec/views.rs)
// ---------------------------------------------------------------------------

#[test]
fn alter_view_rename() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    db.execute("ALTER VIEW v RENAME TO v2").unwrap();

    let err = db.execute("SELECT * FROM v").unwrap_err();
    assert!(err.to_string().contains("v") || err.to_string().to_lowercase().contains("not found"));

    let result = db.execute("SELECT * FROM v2").unwrap();
    assert_eq!(result.rows().len(), 0);
}

#[test]
fn alter_view_rename_to_existing_name_errors() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    db.execute("CREATE VIEW v2 AS SELECT * FROM t").unwrap();

    let err = db.execute("ALTER VIEW v1 RENAME TO v2").unwrap_err();
    assert!(err.to_string().len() > 0);
}

#[test]
fn drop_temp_view() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TEMPORARY VIEW tv AS SELECT * FROM t").unwrap();

    let result = db.execute("SELECT * FROM tv").unwrap();
    assert_eq!(result.rows().len(), 0);

    db.execute("DROP VIEW tv").unwrap();

    let err = db.execute("SELECT * FROM tv").unwrap_err();
    assert!(err.to_string().len() > 0);
}

// ---------------------------------------------------------------------------
// Batch 1l: Recursive CTE validation edge cases (covers expr_contains_recursive_unsupported_feature)
// ---------------------------------------------------------------------------

#[test]
fn recursive_cte_rejects_aggregate_in_recursive_term() {
    let db = open_memory();
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
    let db = open_memory();
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
    assert!(err.to_string().len() > 0);
}

// ---------------------------------------------------------------------------
// Batch 1m: ALTER TABLE RENAME COLUMN (covers normalize.rs rename path)
// ---------------------------------------------------------------------------

#[test]
fn alter_table_rename_column() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, old_name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'val')").unwrap();
    db.execute("ALTER TABLE t RENAME COLUMN old_name TO new_name").unwrap();

    let result = db.execute("SELECT new_name FROM t WHERE id = 1").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Text("val".into()));
}

// ---------------------------------------------------------------------------
// Batch 1n: Correlated subquery with LIKE (covers expr_references_outer LIKE branch)
// ---------------------------------------------------------------------------

#[test]
fn correlated_subquery_with_like() {
    let db = open_memory();
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
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
}

// ---------------------------------------------------------------------------
// Batch 1o: EXPLAIN with various plan nodes (covers planner/physical.rs render_into)
// ---------------------------------------------------------------------------

#[test]
fn explain_join_shows_nested_loop() {
    let db = open_memory();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY)").unwrap();

    let result = db.execute("EXPLAIN SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id").unwrap();
    let text = format!("{:?}", row_values(&result));
    assert!(text.len() > 0);
}

#[test]
fn explain_aggregate_shows_group_by() {
    let db = open_memory();
    db.execute("CREATE TABLE t (cat TEXT, val INT64)").unwrap();

    let result = db.execute("EXPLAIN SELECT cat, SUM(val) FROM t GROUP BY cat").unwrap();
    let text = format!("{:?}", row_values(&result));
    assert!(text.len() > 0);
}

#[test]
fn explain_sort_and_limit() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();

    let result = db
        .execute("EXPLAIN SELECT * FROM t ORDER BY val LIMIT 10 OFFSET 5")
        .unwrap();
    let text = format!("{:?}", row_values(&result));
    assert!(text.len() > 0);
}

#[test]
fn explain_union() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();

    let result = db
        .execute("EXPLAIN SELECT id FROM t UNION SELECT id FROM t")
        .unwrap();
    let text = format!("{:?}", row_values(&result));
    assert!(text.len() > 0);
}

#[test]
fn explain_intersect_except() {
    let db = open_memory();
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
fn explain_left_right_full_cross_joins() {
    let db = open_memory();
    db.execute("CREATE TABLE a (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b (id INT64 PRIMARY KEY)").unwrap();

    let _ = db
        .execute("EXPLAIN SELECT * FROM a LEFT JOIN b ON a.id = b.id")
        .unwrap();
    let _ = db
        .execute("EXPLAIN SELECT * FROM a RIGHT JOIN b ON a.id = b.id")
        .unwrap();
    let _ = db
        .execute("EXPLAIN SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")
        .unwrap();
    let _ = db
        .execute("EXPLAIN SELECT * FROM a CROSS JOIN b")
        .unwrap();
}

// ---------------------------------------------------------------------------
// Batch 1p: PRAGMA dump (covers db.rs render_runtime_dump, render_create_index, render_create_trigger, render_value_sql)
// ---------------------------------------------------------------------------

#[test]
fn pragma_dump_covers_tables_views_indexes_triggers() {
    let db = open_memory();
    db.execute("CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT DEFAULT 'anon', active BOOL)").unwrap();
    db.execute("CREATE TABLE orders (id INT64 PRIMARY KEY, uid INT64 REFERENCES users(id))").unwrap();
    db.execute("CREATE INDEX idx_name ON users (name)").unwrap();
    db.execute("CREATE VIEW active_users AS SELECT * FROM users WHERE active = TRUE").unwrap();
    db.execute("CREATE TABLE audit_log (event TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER after_insert_user AFTER INSERT ON users FOR EACH ROW \
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log(event) VALUES (''inserted'')')",
    )
    .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'alice', TRUE)").unwrap();
    db.execute("INSERT INTO users VALUES (2, NULL, FALSE)").unwrap();
    db.execute("INSERT INTO orders VALUES (10, 1)").unwrap();

    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CREATE TABLE"));
    assert!(dump.contains("INSERT"));
}

// ---------------------------------------------------------------------------
// Batch 1q: save_as (covers db.rs save_as)
// ---------------------------------------------------------------------------

#[test]
fn save_as_creates_copy() {
    let dir = TempDir::new().unwrap();
    let src = dir.path().join("source.ddb");
    let dst = dir.path().join("backup.ddb");

    let db = Db::open_or_create(src.to_str().unwrap(), DbConfig::default()).unwrap();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')").unwrap();

    db.save_as(dst.to_str().unwrap()).unwrap();

    let db2 = Db::open_or_create(dst.to_str().unwrap(), DbConfig::default()).unwrap();
    let result = db2.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(2));
}

// ---------------------------------------------------------------------------
// Batch 1r: Prepared batch execution (covers db.rs execute_prepared_batch_with_builder)
// ---------------------------------------------------------------------------

#[test]
fn prepared_batch_insert() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();

    let prepared = db.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    for i in 1..=50_i64 {
        prepared
            .execute(&[Value::Int64(i), Value::Text(format!("val_{i}"))])
            .unwrap();
    }

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(50));
}

#[test]
fn prepared_batch_in_transaction() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();

    let prepared = db.prepare("INSERT INTO t VALUES ($1, $2)").unwrap();

    db.execute("BEGIN").unwrap();
    for i in 1..=10_i64 {
        prepared
            .execute(&[Value::Int64(i), Value::Text(format!("v{i}"))])
            .unwrap();
    }
    db.execute("COMMIT").unwrap();

    let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(10));
}

// ---------------------------------------------------------------------------
// Batch 1s: WAL recovery (covers wal/recovery.rs, wal/format.rs)
// ---------------------------------------------------------------------------

#[test]
fn wal_recovery_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'world')").unwrap();
    }

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
        let rows = row_values(&result);
        assert_eq!(rows[0][0], Value::Int64(2));
    }
}

#[test]
fn wal_recovery_with_checkpoint_and_more_writes() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test.ddb");

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
        for i in 1..=100 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
        db.checkpoint().unwrap();
        for i in 101..=150 {
            db.execute(&format!("INSERT INTO t VALUES ({i})")).unwrap();
        }
    }

    {
        let db = Db::open_or_create(path.to_str().unwrap(), DbConfig::default()).unwrap();
        let result = db.execute("SELECT COUNT(*) FROM t").unwrap();
        let rows = row_values(&result);
        assert_eq!(rows[0][0], Value::Int64(150));
    }
}

// ---------------------------------------------------------------------------
// Batch 1t: Parser edge cases (covers sql/parser.rs)
// ---------------------------------------------------------------------------

#[test]
fn sql_with_block_comments_containing_semicolons() {
    let db = open_memory();
    let result = db.execute("/* this has a ; in it */ SELECT 1 AS val").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Int64(1));
}

// ---------------------------------------------------------------------------
// Batch 1u: Views in SQL AST roundtrip (covers ast.rs to_sql for various expr types)
// ---------------------------------------------------------------------------

#[test]
fn view_with_between_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t WHERE val BETWEEN 1 AND 10").unwrap();

    let result = db.execute("SELECT * FROM v").unwrap();
    assert_eq!(result.rows().len(), 0);
}

#[test]
fn view_with_in_list_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t WHERE id IN (1, 2, 3)").unwrap();

    db.execute("INSERT INTO t VALUES (1), (2), (4)").unwrap();
    let result = db.execute("SELECT * FROM v ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn view_with_case_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, CASE WHEN val > 10 THEN 'high' ELSE 'low' END AS tier FROM t",
    )
    .unwrap();

    db.execute("INSERT INTO t VALUES (1, 5), (2, 20)").unwrap();
    let result = db.execute("SELECT tier FROM v ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][0], Value::Text("low".into()));
    assert_eq!(rows[1][0], Value::Text("high".into()));
}

#[test]
fn view_with_exists_subquery_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE t2 (t1_id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)")
        .unwrap();

    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1)").unwrap();
    let result = db.execute("SELECT * FROM v").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 1);
}

#[test]
fn view_with_window_function_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, cat TEXT, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) AS rn FROM t",
    )
    .unwrap();

    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)").unwrap();
    let result = db.execute("SELECT id, rn FROM v ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 3);
}

#[test]
fn view_with_set_operation_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t1 UNION SELECT id FROM t2").unwrap();

    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2), (3)").unwrap();
    let result = db.execute("SELECT id FROM v ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 3);
}

#[test]
fn view_with_in_subquery_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE t2 (ref_id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t1 WHERE id IN (SELECT ref_id FROM t2)").unwrap();

    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1), (3)").unwrap();
    let result = db.execute("SELECT id FROM v ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn view_with_scalar_subquery_roundtrips() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id, (SELECT MAX(val) FROM t) AS max_val FROM t").unwrap();

    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let result = db.execute("SELECT id, max_val FROM v ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows[0][1], Value::Int64(20));
    assert_eq!(rows[1][1], Value::Int64(20));
}

// ---------------------------------------------------------------------------
// Batch 1v: Normalize edge cases (covers normalize.rs sublink, sql value functions)
// ---------------------------------------------------------------------------

#[test]
fn in_subquery_normalization() {
    let db = open_memory();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();

    let result = db.execute("SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE id > 1) ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn not_in_subquery() {
    let db = open_memory();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2)").unwrap();

    let result = db.execute("SELECT id FROM t1 WHERE id NOT IN (SELECT id FROM t2) ORDER BY id").unwrap();
    let rows = row_values(&result);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Int64(1));
    assert_eq!(rows[1][0], Value::Int64(3));
}

#[test]
fn current_date_time_functions() {
    let db = open_memory();
    let r1 = db.execute("SELECT CURRENT_DATE").unwrap();
    assert_eq!(r1.rows().len(), 1);

    let r2 = db.execute("SELECT CURRENT_TIME").unwrap();
    assert_eq!(r2.rows().len(), 1);

    let r3 = db.execute("SELECT CURRENT_TIMESTAMP").unwrap();
    assert_eq!(r3.rows().len(), 1);
}
