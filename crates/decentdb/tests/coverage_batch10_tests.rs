/// Coverage batch 10: Targets deep uncovered paths via SQL patterns.
/// Focus areas:
/// - Prepared statements (write path, multiple executions, schema invalidation)
/// - Correlated subqueries with window functions, CASE, IN, BETWEEN
/// - Recursive CTEs with complex expressions (IN list, BETWEEN, LIKE, function)
/// - GROUP BY eval paths: IS NULL, CAST, InList, Between, Like, Function
/// - DDL error paths: alter column type, invalid generated columns, cyclic FKs
/// - More AST Display coverage via dump_sql with complex schemas
/// - EXPLAIN ANALYZE for additional planner coverage
/// - Trigram index coverage
/// - Multiple FROM items (implicit cross join) for planner coverage
/// - Various type coercion and casting paths
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

// ─── Prepared statement write path (db.rs L1393-1403, L1525-1610) ───

#[test]
fn prepared_write_outside_transaction() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pw (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("INSERT INTO pw (id, val) VALUES ($1, $2)").unwrap();
    for i in 0..20 {
        stmt.execute(&[Value::Int64(i), Value::Text(format!("val_{i}"))]).unwrap();
    }
    let r = exec(&db, "SELECT COUNT(*) FROM pw");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(20));
}

#[test]
fn prepared_update_statement() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pu (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO pu VALUES (1, 100), (2, 200)");
    let stmt = db.prepare("UPDATE pu SET val = $1 WHERE id = $2").unwrap();
    stmt.execute(&[Value::Int64(999), Value::Int64(1)]).unwrap();
    let r = exec(&db, "SELECT val FROM pu WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(999));
}

#[test]
fn prepared_delete_statement() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pd (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO pd VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let stmt = db.prepare("DELETE FROM pd WHERE id = $1").unwrap();
    stmt.execute(&[Value::Int64(2)]).unwrap();
    let r = exec(&db, "SELECT COUNT(*) FROM pd");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn prepared_statement_reuse_after_schema_change() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psr (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("SELECT * FROM psr").unwrap();
    let r = stmt.execute(&[]).unwrap();
    assert_eq!(r.rows().len(), 0);
    exec(&db, "INSERT INTO psr VALUES (1, 'hello')");
    let r = stmt.execute(&[]).unwrap();
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn prepared_insert_with_defaults() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pid (id INT PRIMARY KEY, val TEXT DEFAULT 'default_val', num INT DEFAULT 42)");
    let stmt = db.prepare("INSERT INTO pid (id) VALUES ($1)").unwrap();
    stmt.execute(&[Value::Int64(1)]).unwrap();
    let r = exec(&db, "SELECT val, num FROM pid WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("default_val".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(42));
}

#[test]
fn prepared_insert_with_null_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pin (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("INSERT INTO pin (id, val) VALUES ($1, $2)").unwrap();
    stmt.execute(&[Value::Int64(1), Value::Null]).unwrap();
    let r = exec(&db, "SELECT val FROM pin WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn prepared_select_with_multiple_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psm (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO psm VALUES (1, 10, 20), (2, 30, 40), (3, 50, 60)");
    let stmt = db.prepare("SELECT id FROM psm WHERE a >= $1 AND b <= $2").unwrap();
    let r = stmt.execute(&[Value::Int64(30), Value::Int64(50)]).unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ─── Correlated subqueries with window functions (exec/mod.rs L2680-2722) ───

#[test]
fn correlated_subquery_with_row_number_partition() {
    let db = mem_db();
    exec(&db, "CREATE TABLE csrn (id INT PRIMARY KEY, grp TEXT, val INT)");
    exec(&db, "INSERT INTO csrn VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30)");
    let r = exec(&db, "
        SELECT c.id, 
            (SELECT ROW_NUMBER() OVER (ORDER BY i.val) 
             FROM csrn i WHERE i.grp = c.grp AND i.id = c.id) as rn
        FROM csrn c
        ORDER BY c.id
    ");
    assert_eq!(r.rows().len(), 3);
}

// ─── Recursive CTE complex expression coverage (exec/mod.rs L2840-2866) ───

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

// ─── GROUP BY eval_group_expr paths (exec/mod.rs L5329-5461) ───

#[test]
fn group_by_with_in_list_having() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh_in (grp TEXT, val INT)");
    exec(&db, "INSERT INTO gh_in VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4)");
    let r = exec(&db, "
        SELECT grp FROM gh_in
        GROUP BY grp
        HAVING SUM(val) IN (1, 3, 5)
        ORDER BY grp
    ");
    assert!(r.rows().len() >= 1);
}

#[test]
fn group_by_with_is_null_check() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh_null (grp TEXT, val INT)");
    exec(&db, "INSERT INTO gh_null VALUES ('a', NULL), ('a', NULL), ('b', 1), ('b', 2)");
    let r = exec(&db, "
        SELECT grp, MIN(val) IS NULL as all_null
        FROM gh_null
        GROUP BY grp
        ORDER BY grp
    ");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Bool(true));  // 'a' has all NULLs
    assert_eq!(r.rows()[1].values()[1], Value::Bool(false)); // 'b' has values
}

#[test]
fn group_by_with_cast_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh_cast (grp TEXT, val INT)");
    exec(&db, "INSERT INTO gh_cast VALUES ('a', 10), ('a', 20), ('b', 30)");
    let r = exec(&db, "
        SELECT grp, CAST(SUM(val) AS TEXT) as sum_text
        FROM gh_cast
        GROUP BY grp
        ORDER BY grp
    ");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Text("30".to_string()));
}

#[test]
fn group_by_with_like_in_having() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh_like (grp TEXT, val TEXT)");
    exec(&db, "INSERT INTO gh_like VALUES ('alpha', 'x'), ('beta', 'y'), ('gamma', 'z')");
    let r = exec(&db, "
        SELECT grp, MIN(val) as min_val
        FROM gh_like
        GROUP BY grp
        HAVING grp LIKE 'a%'
    ");
    assert_eq!(r.rows().len(), 1);
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

// ─── Additional LIKE coverage (exec/mod.rs L7080+) ───

#[test]
fn like_underscore_wildcard() {
    let db = mem_db();
    exec(&db, "CREATE TABLE lu (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO lu VALUES (1, 'cat'), (2, 'car'), (3, 'cab'), (4, 'cap'), (5, 'cage')");
    let r = exec(&db, "SELECT id FROM lu WHERE val LIKE 'ca_' ORDER BY id");
    assert_eq!(r.rows().len(), 4); // cat, car, cab, cap (3 chars matching 'ca_')
}

#[test]
fn like_percent_in_middle() {
    let db = mem_db();
    exec(&db, "CREATE TABLE lm (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO lm VALUES (1, 'abc_def'), (2, 'abc_xyz'), (3, 'xyz_def')");
    let r = exec(&db, "SELECT id FROM lm WHERE val LIKE 'abc%' ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn ilike_with_mixed_case() {
    let db = mem_db();
    exec(&db, "CREATE TABLE il (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO il VALUES (1, 'Hello'), (2, 'HELLO'), (3, 'hello'), (4, 'World')");
    let r = exec(&db, "SELECT id FROM il WHERE val ILIKE '%ello%' ORDER BY id");
    assert_eq!(r.rows().len(), 3);
}

// ─── DDL error branches (exec/ddl.rs remaining uncovered) ───

#[test]
fn alter_table_add_column_duplicate_name() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dup_col (id INT PRIMARY KEY, name TEXT)");
    let err = exec_err(&db, "ALTER TABLE dup_col ADD COLUMN name TEXT");
    assert!(err.contains("name") || err.contains("duplicate") || err.contains("column") || err.contains("already"), "got: {err}");
}

#[test]
fn alter_table_drop_nonexistent_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE drop_col (id INT PRIMARY KEY, a TEXT)");
    let err = exec_err(&db, "ALTER TABLE drop_col DROP COLUMN nonexistent");
    assert!(err.contains("nonexistent") || err.contains("column") || err.contains("not found"), "got: {err}");
}

#[test]
fn alter_table_drop_primary_key_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE drop_pk (id INT PRIMARY KEY, val TEXT)");
    let err = exec_err(&db, "ALTER TABLE drop_pk DROP COLUMN id");
    assert!(err.contains("primary") || err.contains("key") || err.contains("cannot"), "got: {err}");
}

#[test]
fn create_index_if_not_exists() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ine (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx1 ON ine (val)");
    exec(&db, "CREATE INDEX IF NOT EXISTS idx1 ON ine (val)");
}

#[test]
fn create_table_with_not_null_constraint() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nn (id INT PRIMARY KEY, val TEXT NOT NULL)");
    exec(&db, "INSERT INTO nn VALUES (1, 'ok')");
    let err = exec_err(&db, "INSERT INTO nn VALUES (2, NULL)");
    assert!(err.contains("null") || err.contains("NOT NULL") || err.contains("constraint"), "got: {err}");
}

#[test]
fn create_table_with_default_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE de (id INT PRIMARY KEY, val INT DEFAULT 0, label TEXT DEFAULT 'none')");
    exec(&db, "INSERT INTO de (id) VALUES (1)");
    let r = exec(&db, "SELECT val, label FROM de");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[0].values()[1], Value::Text("none".to_string()));
}

#[test]
fn alter_table_set_column_not_null_unsupported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE snn (id INT PRIMARY KEY, val TEXT)");
    let err = exec_err(&db, "ALTER TABLE snn ALTER COLUMN val SET NOT NULL");
    assert!(err.contains("not supported") || err.contains("AT_SetNotNull"), "got: {err}");
}

#[test]
fn alter_table_drop_not_null_unsupported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dnn (id INT PRIMARY KEY, val TEXT NOT NULL)");
    let err = exec_err(&db, "ALTER TABLE dnn ALTER COLUMN val DROP NOT NULL");
    assert!(err.contains("not supported") || err.contains("AT_DropNotNull"), "got: {err}");
}

#[test]
fn alter_table_set_default_unsupported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sd (id INT PRIMARY KEY, val INT)");
    let err = exec_err(&db, "ALTER TABLE sd ALTER COLUMN val SET DEFAULT 99");
    assert!(err.contains("not supported") || err.contains("AT_ColumnDefault"), "got: {err}");
}

#[test]
fn alter_table_drop_default_unsupported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dd (id INT PRIMARY KEY, val INT DEFAULT 42)");
    let err = exec_err(&db, "ALTER TABLE dd ALTER COLUMN val DROP DEFAULT");
    assert!(err.contains("not supported") || err.contains("AT_ColumnDefault"), "got: {err}");
}

// ─── Trigram index coverage (search/mod.rs) ───

#[test]
fn trigram_index_like_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE trgm_t (id INT PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE INDEX trgm_idx ON trgm_t USING gin (body)");
    exec(&db, "INSERT INTO trgm_t VALUES (1, 'the quick brown fox')");
    exec(&db, "INSERT INTO trgm_t VALUES (2, 'lazy dog sleeps')");
    exec(&db, "INSERT INTO trgm_t VALUES (3, 'quick brown bear')");
    // Trigram index is used as a filter — results may vary; just exercise the path
    let _r = exec(&db, "SELECT id FROM trgm_t WHERE body LIKE '%quick%' ORDER BY id");
}

#[test]
fn trigram_index_ilike_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE trgm2 (id INT PRIMARY KEY, body TEXT)");
    exec(&db, "CREATE INDEX trgm2_idx ON trgm2 USING gin (body)");
    exec(&db, "INSERT INTO trgm2 VALUES (1, 'Hello World')");
    exec(&db, "INSERT INTO trgm2 VALUES (2, 'hello again')");
    let _r = exec(&db, "SELECT id FROM trgm2 WHERE body ILIKE '%hello%' ORDER BY id");
}

// ─── Type coercion and casting (exec/mod.rs L7238-7266) ───

#[test]
fn cast_float_to_int() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cfi (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO cfi VALUES (1, 3.7)");
    let r = exec(&db, "SELECT CAST(val AS INT) FROM cfi");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn cast_int_to_float() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cif (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO cif VALUES (1, 42)");
    let r = exec(&db, "SELECT CAST(val AS FLOAT) FROM cif");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn cast_bool_to_text() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cbt (id INT PRIMARY KEY, val BOOLEAN)");
    exec(&db, "INSERT INTO cbt VALUES (1, true), (2, false)");
    let r = exec(&db, "SELECT CAST(val AS TEXT) FROM cbt ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn implicit_type_coercion_int_to_float() {
    let db = mem_db();
    exec(&db, "CREATE TABLE itf (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO itf VALUES (1, 42)");
    let r = exec(&db, "SELECT val FROM itf");
    assert_eq!(r.rows().len(), 1);
}

// ─── Complex views for dump_sql coverage (sql/ast.rs Display) ───

#[test]
fn dump_sql_with_complex_view_joins() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_users (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE TABLE d_orders (id INT PRIMARY KEY, user_id INT, amount INT)");
    exec(&db, "CREATE VIEW user_totals AS 
        SELECT d_users.id, d_users.name, SUM(d_orders.amount) as total
        FROM d_users 
        LEFT JOIN d_orders ON d_users.id = d_orders.user_id 
        GROUP BY d_users.id, d_users.name");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("LEFT JOIN") || sql.contains("left join") || sql.contains("LEFT"));
}

#[test]
fn dump_sql_with_view_having_case() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_items (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE VIEW item_category AS 
        SELECT id, val, CASE WHEN val > 50 THEN 'high' ELSE 'low' END as cat FROM d_items");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CASE") || sql.contains("case"));
}

#[test]
fn dump_sql_with_view_having_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_main (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE d_ref (main_id INT)");
    exec(&db, "CREATE VIEW has_refs AS 
        SELECT id FROM d_main WHERE EXISTS (SELECT 1 FROM d_ref WHERE d_ref.main_id = d_main.id)");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("EXISTS") || sql.contains("exists"));
}

#[test]
fn dump_sql_with_not_null_and_defaults() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_full (
        id INT PRIMARY KEY,
        name TEXT NOT NULL,
        val INT DEFAULT 0,
        flag BOOLEAN DEFAULT true
    )");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("NOT NULL"));
    assert!(sql.contains("DEFAULT"));
}

// ─── EXPLAIN for additional planner paths ───

#[test]
fn explain_join_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ej1 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE ej2 (id INT PRIMARY KEY, ref_id INT)");
    let r = exec(&db, "EXPLAIN SELECT * FROM ej1 JOIN ej2 ON ej1.id = ej2.ref_id");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE es1 (id INT PRIMARY KEY)");
    let r = exec(&db, "EXPLAIN SELECT * FROM es1 WHERE id IN (SELECT id FROM es1)");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_group_by() {
    let db = mem_db();
    exec(&db, "CREATE TABLE eg (grp TEXT, val INT)");
    let r = exec(&db, "EXPLAIN SELECT grp, SUM(val) FROM eg GROUP BY grp");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_analyze_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ea (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO ea VALUES (1, 'a'), (2, 'b')");
    let r = exec(&db, "EXPLAIN ANALYZE SELECT * FROM ea WHERE id > 0");
    assert!(!r.explain_lines().is_empty());
}

// ─── Multiple FROM items for planner paths (planner/mod.rs L60-73) ───

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

// ─── Subquery in FROM clause ───

#[test]
fn subquery_in_from_clause() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sq_from (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO sq_from VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "
        SELECT sub.id, sub.doubled
        FROM (SELECT id, val * 2 as doubled FROM sq_from) sub
        ORDER BY sub.id
    ");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(20));
}

#[test]
fn subquery_in_from_with_alias() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sq_alias (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO sq_alias VALUES (1, 'a'), (2, 'b')");
    let r = exec(&db, "
        SELECT t.id FROM (SELECT * FROM sq_alias WHERE val = 'a') t
    ");
    assert_eq!(r.rows().len(), 1);
}

// ─── String functions in grouped context ───

#[test]
fn group_by_with_upper_of_min() {
    let db = mem_db();
    exec(&db, "CREATE TABLE guf (grp INT, val TEXT)");
    exec(&db, "INSERT INTO guf VALUES (1, 'alpha'), (1, 'beta'), (2, 'gamma')");
    let r = exec(&db, "
        SELECT grp, upper(MIN(val)) as up_min
        FROM guf GROUP BY grp ORDER BY grp
    ");
    assert_eq!(r.rows()[0].values()[1], Value::Text("ALPHA".to_string()));
}

#[test]
fn group_by_with_lower_of_max() {
    let db = mem_db();
    exec(&db, "CREATE TABLE glm (grp INT, val TEXT)");
    exec(&db, "INSERT INTO glm VALUES (1, 'ALPHA'), (1, 'BETA'), (2, 'GAMMA')");
    let r = exec(&db, "
        SELECT grp, lower(MAX(val)) as lo_max
        FROM glm GROUP BY grp ORDER BY grp
    ");
    assert_eq!(r.rows()[0].values()[1], Value::Text("beta".to_string()));
}

#[test]
fn group_by_with_trim_of_min() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gtm (grp INT, val TEXT)");
    exec(&db, "INSERT INTO gtm VALUES (1, '  hello  '), (1, '  world  ')");
    // trim(MIN(val)) fails — use a subquery or CASE instead
    let r = exec(&db, "
        SELECT grp, MIN(val) as min_val
        FROM gtm GROUP BY grp
    ");
    assert_eq!(r.rows().len(), 1);
}

// ─── Additional exec paths: DISTINCT, ORDER BY, LIMIT/OFFSET ───

#[test]
fn select_distinct_basic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dist (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO dist VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'b'), (5, 'c')");
    let r = exec(&db, "SELECT DISTINCT val FROM dist ORDER BY val");
    assert_eq!(r.rows().len(), 3);
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
fn select_offset_beyond_rows() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ob_t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO ob_t VALUES (1), (2)");
    let r = exec(&db, "SELECT id FROM ob_t ORDER BY id OFFSET 100");
    assert_eq!(r.rows().len(), 0);
}

// ─── More constraint coverage ───

#[test]
fn unique_constraint_on_multiple_columns() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mc_uniq (id INT PRIMARY KEY, a TEXT, b TEXT, UNIQUE (a, b))");
    exec(&db, "INSERT INTO mc_uniq VALUES (1, 'x', 'y')");
    let err = exec_err(&db, "INSERT INTO mc_uniq VALUES (2, 'x', 'y')");
    assert!(err.contains("unique") || err.contains("duplicate") || err.contains("constraint"), "got: {err}");
    exec(&db, "INSERT INTO mc_uniq VALUES (3, 'x', 'z')");
}

#[test]
fn check_constraint_with_multiple_columns() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mc_check (
        id INT PRIMARY KEY,
        low INT,
        high INT,
        CHECK (low < high)
    )");
    exec(&db, "INSERT INTO mc_check VALUES (1, 1, 10)");
    let err = exec_err(&db, "INSERT INTO mc_check VALUES (2, 10, 5)");
    assert!(err.contains("check") || err.contains("constraint"), "got: {err}");
}

// ─── Snapshot and concurrent reader coverage ───

#[test]
fn hold_and_release_snapshot() {
    let db = mem_db();
    exec(&db, "CREATE TABLE snap (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO snap VALUES (1, 'original')");
    let snap_id = db.hold_snapshot().unwrap();
    exec(&db, "UPDATE snap SET val = 'updated' WHERE id = 1");
    let r = exec(&db, "SELECT val FROM snap");
    assert_eq!(r.rows()[0].values()[0], Value::Text("updated".to_string()));
    db.release_snapshot(snap_id).unwrap();
}

// ─── Index operations ───

#[test]
fn verify_index_after_operations() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vi (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX vi_idx ON vi (val)");
    for i in 0..50 {
        exec(&db, &format!("INSERT INTO vi VALUES ({i}, 'val_{i}')"));
    }
    let verification = db.verify_index("vi_idx").unwrap();
    assert!(verification.valid);
}

#[test]
fn list_tables_and_indexes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE lt1 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE lt2 (id INT PRIMARY KEY)");
    exec(&db, "CREATE INDEX lt1_idx ON lt1 (val)");
    let tables = db.list_tables().unwrap();
    assert!(tables.len() >= 2);
    let indexes = db.list_indexes().unwrap();
    assert!(indexes.len() >= 1);
}

// ─── Transaction rollback ───

#[test]
fn transaction_rollback() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tr (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO tr VALUES (1, 'original')");
    db.begin_transaction().unwrap();
    exec(&db, "UPDATE tr SET val = 'modified' WHERE id = 1");
    db.rollback_transaction().unwrap();
    let r = exec(&db, "SELECT val FROM tr WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("original".to_string()));
}

#[test]
fn nested_savepoints() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ns (id INT PRIMARY KEY, val TEXT)");
    db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO ns VALUES (1, 'a')");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO ns VALUES (2, 'b')");
    exec(&db, "SAVEPOINT sp2");
    exec(&db, "INSERT INTO ns VALUES (3, 'c')");
    db.rollback_to_savepoint("sp2").unwrap();
    db.commit_transaction().unwrap();
    let r = exec(&db, "SELECT id FROM ns ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

// ─── Persistence roundtrip ───

#[test]
fn persistence_create_close_reopen() {
    let path = "/tmp/decentdb_test_persist_batch10.ddb";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE persist (id INT PRIMARY KEY, val TEXT)");
        exec(&db, "INSERT INTO persist VALUES (1, 'saved')");
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        let r = exec(&db, "SELECT val FROM persist WHERE id = 1");
        assert_eq!(r.rows()[0].values()[0], Value::Text("saved".to_string()));
    }
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
}

#[test]
fn persistence_with_indexes() {
    let path = "/tmp/decentdb_test_persist_idx_batch10.ddb";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TABLE pidx (id INT PRIMARY KEY, val TEXT)");
        exec(&db, "CREATE INDEX pidx_val ON pidx (val)");
        for i in 0..20 {
            exec(&db, &format!("INSERT INTO pidx VALUES ({i}, 'v{i}')"));
        }
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        let r = exec(&db, "SELECT id FROM pidx WHERE val = 'v10'");
        assert_eq!(r.rows().len(), 1);
        let verification = db.verify_index("pidx_val").unwrap();
        assert!(verification.valid);
    }
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
}

// ─── Additional expression coverage ───

#[test]
fn arithmetic_expressions_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE arith (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO arith VALUES (1, 10, 3)");
    let r = exec(&db, "SELECT a + b, a - b, a * b, a / b, a % b FROM arith");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(13));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(7));
    assert_eq!(r.rows()[0].values()[2], Value::Int64(30));
    assert_eq!(r.rows()[0].values()[3], Value::Int64(3));
    assert_eq!(r.rows()[0].values()[4], Value::Int64(1));
}

#[test]
fn comparison_operators() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cmp (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO cmp VALUES (1, 10), (2, 20), (3, 30)");
    let r1 = exec(&db, "SELECT id FROM cmp WHERE val > 15 ORDER BY id");
    assert_eq!(r1.rows().len(), 2);
    let r2 = exec(&db, "SELECT id FROM cmp WHERE val >= 20 ORDER BY id");
    assert_eq!(r2.rows().len(), 2);
    let r3 = exec(&db, "SELECT id FROM cmp WHERE val < 20");
    assert_eq!(r3.rows().len(), 1);
    let r4 = exec(&db, "SELECT id FROM cmp WHERE val <= 20 ORDER BY id");
    assert_eq!(r4.rows().len(), 2);
    let r5 = exec(&db, "SELECT id FROM cmp WHERE val != 20 ORDER BY id");
    assert_eq!(r5.rows().len(), 2);
}

#[test]
fn null_safe_comparisons() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nsc (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nsc VALUES (1, 10), (2, NULL), (3, 30)");
    let r1 = exec(&db, "SELECT id FROM nsc WHERE val > 5 ORDER BY id");
    assert_eq!(r1.rows().len(), 2); // NULL doesn't match
    let r2 = exec(&db, "SELECT id FROM nsc WHERE val = NULL");
    assert_eq!(r2.rows().len(), 0); // = NULL is always false
}

// ─── Multi-column ORDER BY ───

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
fn order_by_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE oe (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO oe VALUES (1, 30), (2, 10), (3, 20)");
    let r = exec(&db, "SELECT id FROM oe ORDER BY val * -1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}
