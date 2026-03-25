/// Coverage batch 9: Targets specific uncovered code paths discovered via
/// cargo-llvm-cov line analysis. Focus areas:
/// - Prepared statement batch execution (db.rs L1421-1525)
/// - Prepared simple insert positional params (exec/dml.rs L332-397)
/// - Correlated subqueries with complex expressions (exec/mod.rs L2680-2722)
/// - CTE column aliasing (exec/mod.rs L1612-1636)
/// - GROUP BY with unary/case/function expressions (exec/mod.rs L5329-5461)
/// - Decimal to_string formatting (exec/mod.rs L7060-7080, L7238-7266)
/// - LIKE with escape (exec/mod.rs L7080+)
/// - Recursive CTE feature checking (exec/mod.rs L2840-2866)
/// - exec/ddl.rs error branches (column rename, FK validation, etc.)
/// - exec/views.rs view creation/drop/rename
/// - planner aggregate detection in complex expressions
/// - sql/ast.rs Display impls for uncovered branches
/// - sql/normalize.rs uncovered normalization paths
/// - sql/parser.rs helper functions
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

// ─── Prepared statement batch execution (db.rs L1421-1525) ───

#[test]
fn prepared_insert_batch_positional_params() {
    let db = mem_db();
    exec(&db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)");
    let stmt = db.prepare("INSERT INTO items (id, name) VALUES ($1, $2)").unwrap();
    for i in 0..10 {
        let name = format!("item_{i}");
        stmt.execute(&[Value::Int64(i), Value::Text(name)]).unwrap();
    }
    let r = exec(&db, "SELECT COUNT(*) FROM items");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(10));
}

#[test]
fn prepared_select_read_only_batch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE data (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO data VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let stmt = db.prepare("SELECT * FROM data WHERE id = $1").unwrap();
    let r = stmt.execute(&[Value::Int64(2)]).unwrap();
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[1], Value::Text("b".to_string()));
}

#[test]
fn prepared_insert_in_transaction() {
    let db = mem_db();
    exec(&db, "CREATE TABLE txitems (id INT PRIMARY KEY, label TEXT)");
    let stmt = db.prepare("INSERT INTO txitems (id, label) VALUES ($1, $2)").unwrap();
    let mut txn = db.transaction().unwrap();
    for i in 0..5 {
        let label = format!("label_{i}");
        stmt.execute_in(&mut txn, &[Value::Int64(i), Value::Text(label)]).unwrap();
    }
    txn.commit().unwrap();
    let r = exec(&db, "SELECT COUNT(*) FROM txitems");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
}

#[test]
fn prepared_insert_auto_increment() {
    let db = mem_db();
    exec(&db, "CREATE TABLE autotbl (id INT PRIMARY KEY, val TEXT)");
    let stmt = db.prepare("INSERT INTO autotbl (val) VALUES ($1)").unwrap();
    stmt.execute(&[Value::Text("first".to_string())]).unwrap();
    stmt.execute(&[Value::Text("second".to_string())]).unwrap();
    let r = exec(&db, "SELECT id, val FROM autotbl ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

// ─── Correlated subqueries with complex expressions (exec/mod.rs L2680-2722) ───

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

// ─── CTE column aliasing (exec/mod.rs L1612-1636) ───

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
fn cte_column_alias_count_mismatch_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 2)");
    let err = exec_err(&db, "WITH c(x) AS (SELECT a, b FROM t) SELECT * FROM c");
    assert!(err.contains("expected") || err.contains("column"), "got: {err}");
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

// ─── GROUP BY with unary/case/function expressions (exec/mod.rs L5329-5461) ───

#[test]
fn group_by_with_negation_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE measurements (category TEXT, value INT)");
    exec(&db, "INSERT INTO measurements VALUES ('a', 10), ('a', 20), ('b', 30)");
    let r = exec(&db, "
        SELECT category, -SUM(value) as neg_sum 
        FROM measurements 
        GROUP BY category 
        ORDER BY category
    ");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(-30));
    assert_eq!(r.rows()[1].values()[1], Value::Int64(-30));
}

#[test]
fn group_by_with_not_in_having() {
    let db = mem_db();
    exec(&db, "CREATE TABLE flags (grp TEXT, active BOOLEAN)");
    exec(&db, "INSERT INTO flags VALUES ('x', true), ('x', false), ('y', true), ('y', true)");
    let r = exec(&db, "
        SELECT grp, COUNT(*) as cnt
        FROM flags 
        GROUP BY grp
        HAVING NOT (COUNT(*) < 2)
        ORDER BY grp
    ");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_with_case_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sales (region TEXT, amount INT)");
    exec(&db, "INSERT INTO sales VALUES ('east', 100), ('east', 200), ('west', 50), ('west', 150)");
    let r = exec(&db, "
        SELECT region,
            CASE WHEN SUM(amount) > 200 THEN 'high' ELSE 'low' END as level
        FROM sales
        GROUP BY region
        ORDER BY region
    ");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Text("high".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("low".to_string()));
}

#[test]
fn group_by_with_function_in_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE words (grp INT, word TEXT)");
    exec(&db, "INSERT INTO words VALUES (1, 'hello'), (1, 'WORLD'), (2, 'foo')");
    let r = exec(&db, "
        SELECT grp, upper(MIN(word)) as upper_min
        FROM words 
        GROUP BY grp 
        ORDER BY grp
    ");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn group_by_with_between_in_having() {
    let db = mem_db();
    exec(&db, "CREATE TABLE scores (team TEXT, score INT)");
    exec(&db, "INSERT INTO scores VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 6), ('c', 100)");
    let r = exec(&db, "
        SELECT team, SUM(score) as total
        FROM scores
        GROUP BY team
        HAVING SUM(score) BETWEEN 10 AND 50
        ORDER BY team
    ");
    assert_eq!(r.rows().len(), 2);
}

// ─── Decimal formatting (exec/mod.rs L7060-7080, L7238-7266) ───

#[test]
fn decimal_column_display() {
    let db = mem_db();
    exec(&db, "CREATE TABLE prices (id INT PRIMARY KEY, price DECIMAL(10, 2))");
    exec(&db, "INSERT INTO prices VALUES (1, 19.99), (2, 0.01), (3, 100.00)");
    let r = exec(&db, "SELECT price FROM prices ORDER BY id");
    assert_eq!(r.rows().len(), 3);
}

#[test]
fn decimal_zero_scale() {
    let db = mem_db();
    exec(&db, "CREATE TABLE counts (id INT PRIMARY KEY, cnt DECIMAL(10, 0))");
    exec(&db, "INSERT INTO counts VALUES (1, 42)");
    let r = exec(&db, "SELECT cnt FROM counts");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn decimal_negative_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ledger (id INT PRIMARY KEY, amount DECIMAL(10, 2))");
    exec(&db, "INSERT INTO ledger VALUES (1, -5.50), (2, -0.01)");
    let r = exec(&db, "SELECT amount FROM ledger ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn decimal_small_fraction() {
    let db = mem_db();
    exec(&db, "CREATE TABLE precise (id INT PRIMARY KEY, val DECIMAL(10, 4))");
    exec(&db, "INSERT INTO precise VALUES (1, 0.0001)");
    let r = exec(&db, "SELECT val FROM precise");
    assert_eq!(r.rows().len(), 1);
}

// ─── LIKE with escape (exec/mod.rs L7080+) ───

#[test]
fn like_with_escape_character() {
    let db = mem_db();
    exec(&db, "CREATE TABLE patterns (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO patterns VALUES (1, 'abc%def'), (2, 'abcXdef'), (3, 'abc')");
    let r = exec(&db, "SELECT id FROM patterns WHERE val LIKE 'abc!%def' ESCAPE '!'");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn ilike_pattern_matching() {
    let db = mem_db();
    exec(&db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO items VALUES (1, 'Hello'), (2, 'WORLD'), (3, 'hello')");
    let r = exec(&db, "SELECT id FROM items WHERE name ILIKE 'hello' ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn not_like_pattern() {
    let db = mem_db();
    exec(&db, "CREATE TABLE words (id INT PRIMARY KEY, word TEXT)");
    exec(&db, "INSERT INTO words VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')");
    let r = exec(&db, "SELECT id FROM words WHERE word NOT LIKE 'a%' ORDER BY id");
    // NOT LIKE 'a%' should exclude 'apple', keeping banana and cherry
    assert!(r.rows().len() >= 1);
}

// ─── exec/ddl.rs error branches ───

#[test]
fn create_table_duplicate_column_names() {
    let db = mem_db();
    let err = exec_err(&db, "CREATE TABLE bad (a INT, a TEXT)");
    assert!(err.contains("duplicate") || err.contains("column"), "got: {err}");
}

#[test]
fn create_table_fk_references_nonexistent_table() {
    let db = mem_db();
    let err = exec_err(&db, "
        CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES nonexistent(id)
        )
    ");
    assert!(err.contains("nonexistent") || err.contains("foreign key") || err.contains("table"), "got: {err}");
}

#[test]
fn create_table_fk_references_nonexistent_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE parent (id INT PRIMARY KEY)");
    let err = exec_err(&db, "
        CREATE TABLE child (
            id INT PRIMARY KEY,
            parent_id INT REFERENCES parent(nonexistent_col)
        )
    ");
    assert!(err.contains("column") || err.contains("nonexistent") || err.contains("foreign"), "got: {err}");
}

#[test]
fn drop_table_if_exists_nonexistent() {
    let db = mem_db();
    exec(&db, "DROP TABLE IF EXISTS nonexistent");
}

#[test]
fn drop_index_if_exists_nonexistent() {
    let db = mem_db();
    exec(&db, "DROP INDEX IF EXISTS nonexistent");
}

#[test]
fn create_index_on_nonexistent_table() {
    let db = mem_db();
    let err = exec_err(&db, "CREATE INDEX idx ON nonexistent (col)");
    assert!(err.contains("nonexistent") || err.contains("table"), "got: {err}");
}

#[test]
fn create_index_on_nonexistent_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    let err = exec_err(&db, "CREATE INDEX idx ON t (nonexistent)");
    assert!(err.contains("column") || err.contains("nonexistent"), "got: {err}");
}

#[test]
fn alter_table_add_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE evolve (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO evolve VALUES (1)");
    exec(&db, "ALTER TABLE evolve ADD COLUMN name TEXT DEFAULT 'unnamed'");
    let r = exec(&db, "SELECT name FROM evolve");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn alter_table_drop_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE multi (id INT PRIMARY KEY, a TEXT, b TEXT)");
    exec(&db, "INSERT INTO multi VALUES (1, 'x', 'y')");
    exec(&db, "ALTER TABLE multi DROP COLUMN b");
    let r = exec(&db, "SELECT * FROM multi");
    assert_eq!(r.columns().len(), 2);
}

#[test]
fn alter_table_rename_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE renamecol (id INT PRIMARY KEY, old_name TEXT)");
    exec(&db, "INSERT INTO renamecol VALUES (1, 'test')");
    exec(&db, "ALTER TABLE renamecol RENAME COLUMN old_name TO new_name");
    let r = exec(&db, "SELECT new_name FROM renamecol");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn alter_table_add_column_with_check_constraint() {
    let db = mem_db();
    exec(&db, "CREATE TABLE checked (id INT PRIMARY KEY)");
    exec(&db, "ALTER TABLE checked ADD COLUMN val INT CHECK (val > 0)");
    exec(&db, "INSERT INTO checked VALUES (1, 5)");
    let err = exec_err(&db, "INSERT INTO checked VALUES (2, -1)");
    assert!(err.contains("check") || err.contains("constraint") || err.contains("violat"), "got: {err}");
}

#[test]
fn create_table_with_generated_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gen (id INT PRIMARY KEY, a INT, b INT, sum_ab INT GENERATED ALWAYS AS (a + b) STORED)");
    exec(&db, "INSERT INTO gen (id, a, b) VALUES (1, 10, 20)");
    let r = exec(&db, "SELECT sum_ab FROM gen");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(30));
}

#[test]
fn create_table_with_multiple_check_constraints() {
    let db = mem_db();
    exec(&db, "CREATE TABLE multichecked (
        id INT PRIMARY KEY,
        age INT CHECK (age >= 0),
        name TEXT CHECK (length(name) > 0)
    )");
    exec(&db, "INSERT INTO multichecked VALUES (1, 25, 'Alice')");
    let err = exec_err(&db, "INSERT INTO multichecked VALUES (2, -1, 'Bob')");
    assert!(err.contains("check") || err.contains("constraint"), "got: {err}");
}

// ─── exec/views.rs view creation/drop/rename ───

#[test]
fn create_view_basic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE base_data (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO base_data VALUES (1, 'hello')");
    exec(&db, "CREATE VIEW v AS SELECT * FROM base_data");
    let r = exec(&db, "SELECT * FROM v");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn create_or_replace_view() {
    let db = mem_db();
    exec(&db, "CREATE TABLE base (id INT PRIMARY KEY, a TEXT, b TEXT)");
    exec(&db, "INSERT INTO base VALUES (1, 'x', 'y')");
    exec(&db, "CREATE VIEW v AS SELECT id, a FROM base");
    exec(&db, "CREATE OR REPLACE VIEW v AS SELECT id, b FROM base");
    let r = exec(&db, "SELECT * FROM v");
    assert_eq!(r.columns().len(), 2);
    assert_eq!(r.columns()[1], "b");
}

#[test]
fn drop_view_basic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "CREATE VIEW v AS SELECT * FROM t");
    exec(&db, "DROP VIEW v");
    let err = exec_err(&db, "SELECT * FROM v");
    assert!(err.contains("v") || err.contains("not found") || err.contains("unknown"), "got: {err}");
}

#[test]
fn drop_view_if_exists() {
    let db = mem_db();
    exec(&db, "DROP VIEW IF EXISTS nonexistent_view");
}

#[test]
fn rename_view() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO t VALUES (1)");
    exec(&db, "CREATE VIEW old_name AS SELECT * FROM t");
    exec(&db, "ALTER VIEW old_name RENAME TO new_name");
    let r = exec(&db, "SELECT * FROM new_name");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn view_depends_on_another_view() {
    let db = mem_db();
    exec(&db, "CREATE TABLE base (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO base VALUES (1, 'a')");
    exec(&db, "CREATE VIEW v1 AS SELECT * FROM base");
    exec(&db, "CREATE VIEW v2 AS SELECT * FROM v1");
    let r = exec(&db, "SELECT * FROM v2");
    assert_eq!(r.rows().len(), 1);
}

// ─── planner paths ───

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
fn planner_index_scan_with_equality_filter() {
    let db = mem_db();
    exec(&db, "CREATE TABLE indexed (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx_val ON indexed (val)");
    for i in 0..100 {
        exec(&db, &format!("INSERT INTO indexed VALUES ({i}, 'v{i}')"));
    }
    let r = exec(&db, "SELECT id FROM indexed WHERE val = 'v50'");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn planner_aggregation_detection_in_case() {
    let db = mem_db();
    exec(&db, "CREATE TABLE agg_test (grp TEXT, val INT)");
    exec(&db, "INSERT INTO agg_test VALUES ('a', 1), ('a', 2), ('b', 3)");
    let r = exec(&db, "
        SELECT grp, CASE WHEN COUNT(*) > 1 THEN 'many' ELSE 'one' END as cnt_label
        FROM agg_test
        GROUP BY grp
        ORDER BY grp
    ");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Text("many".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("one".to_string()));
}

#[test]
fn planner_aggregation_detection_in_between() {
    let db = mem_db();
    exec(&db, "CREATE TABLE between_agg (grp TEXT, val INT)");
    exec(&db, "INSERT INTO between_agg VALUES ('a', 1), ('a', 2), ('b', 10)");
    let r = exec(&db, "
        SELECT grp
        FROM between_agg
        GROUP BY grp
        HAVING SUM(val) BETWEEN 1 AND 5
    ");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn planner_aggregation_detection_in_like() {
    let db = mem_db();
    exec(&db, "CREATE TABLE like_agg (grp TEXT, val TEXT)");
    exec(&db, "INSERT INTO like_agg VALUES ('a', 'hello'), ('a', 'world'), ('b', 'hi')");
    let r = exec(&db, "
        SELECT grp, MIN(val) as min_val
        FROM like_agg
        GROUP BY grp
        HAVING MIN(val) LIKE 'h%'
        ORDER BY grp
    ");
    assert!(r.rows().len() >= 1);
}

// ─── sql/normalize.rs uncovered paths ───

#[test]
fn normalize_create_trigger_basic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE trig_table (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE audit_log (msg TEXT)");
    exec(&db, "
        CREATE TRIGGER trig_insert
        AFTER INSERT ON trig_table
        FOR EACH ROW
        EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''inserted'')')
    ");
    exec(&db, "INSERT INTO trig_table VALUES (1, 'test')");
    let r = exec(&db, "SELECT * FROM audit_log");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn normalize_insert_on_conflict_do_nothing() {
    let db = mem_db();
    exec(&db, "CREATE TABLE unique_tbl (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO unique_tbl VALUES (1, 'first')");
    exec(&db, "INSERT INTO unique_tbl VALUES (1, 'second') ON CONFLICT DO NOTHING");
    let r = exec(&db, "SELECT val FROM unique_tbl WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("first".to_string()));
}

#[test]
fn normalize_insert_on_conflict_do_update() {
    let db = mem_db();
    exec(&db, "CREATE TABLE upsert_tbl (id INT PRIMARY KEY, val TEXT, count INT DEFAULT 1)");
    exec(&db, "INSERT INTO upsert_tbl VALUES (1, 'first', 1)");
    exec(&db, "INSERT INTO upsert_tbl VALUES (1, 'second', 1) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, count = upsert_tbl.count + 1");
    let r = exec(&db, "SELECT val, count FROM upsert_tbl WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("second".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(2));
}

#[test]
fn update_from_clause_not_supported() {
    let db = mem_db();
    exec(&db, "CREATE TABLE target (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE source (id INT PRIMARY KEY, new_val TEXT)");
    exec(&db, "INSERT INTO target VALUES (1, 'old'), (2, 'old')");
    exec(&db, "INSERT INTO source VALUES (1, 'new')");
    // UPDATE...FROM is not supported in DecentDB 1.0
    let err = exec_err(&db, "UPDATE target SET val = source.new_val FROM source WHERE target.id = source.id");
    assert!(err.contains("not supported") || err.contains("FROM"), "got: {err}");
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

// ─── sql/parser.rs helper paths ───

#[test]
fn parse_multi_statement_batch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE batch1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE batch2 (id INT PRIMARY KEY)");
    // Both tables should exist
    let r1 = exec(&db, "SELECT * FROM batch1");
    let r2 = exec(&db, "SELECT * FROM batch2");
    assert_eq!(r1.rows().len(), 0);
    assert_eq!(r2.rows().len(), 0);
}

#[test]
fn parse_expression_with_nested_parens() {
    let db = mem_db();
    exec(&db, "CREATE TABLE expr_t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    exec(&db, "INSERT INTO expr_t VALUES (1, 2, 3, 4)");
    let r = exec(&db, "SELECT ((a + b) * c) FROM expr_t");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(20));
}

// ─── Additional coverage for exec/mod.rs error branches ───

#[test]
fn select_from_nonexistent_table() {
    let db = mem_db();
    let err = exec_err(&db, "SELECT * FROM ghost_table");
    assert!(err.contains("ghost_table") || err.contains("not found") || err.contains("unknown"), "got: {err}");
}

#[test]
fn insert_type_mismatch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE typed (id INT PRIMARY KEY, val INT)");
    // Text that can't be coerced to INT
    let err = exec_err(&db, "INSERT INTO typed VALUES (1, 'not_a_number')");
    assert!(err.contains("type") || err.contains("cast") || err.contains("convert") || err.contains("coer"), "got: {err}");
}

#[test]
fn division_by_zero_in_group_by() {
    let db = mem_db();
    exec(&db, "CREATE TABLE divzero (grp TEXT, val INT)");
    exec(&db, "INSERT INTO divzero VALUES ('a', 10), ('a', 0)");
    // Division by zero returns NULL in DecentDB, not an error
    let r = exec(&db, "SELECT grp, SUM(val) / 0 FROM divzero GROUP BY grp");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[1], Value::Null);
}

// ─── Recursive CTE complex expression paths ───

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

// ─── Additional exec paths: IN list, BETWEEN, NULL handling ───

#[test]
fn in_list_with_nulls() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nullcheck (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nullcheck VALUES (1, 10), (2, NULL), (3, 30)");
    let r = exec(&db, "SELECT id FROM nullcheck WHERE val IN (10, 30) ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn between_with_nulls() {
    let db = mem_db();
    exec(&db, "CREATE TABLE between_null (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO between_null VALUES (1, 5), (2, NULL), (3, 15)");
    let r = exec(&db, "SELECT id FROM between_null WHERE val BETWEEN 1 AND 10 ORDER BY id");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn is_null_and_is_not_null() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nulls (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO nulls VALUES (1, 'a'), (2, NULL), (3, 'c')");
    let r1 = exec(&db, "SELECT id FROM nulls WHERE val IS NULL");
    assert_eq!(r1.rows().len(), 1);
    let r2 = exec(&db, "SELECT id FROM nulls WHERE val IS NOT NULL ORDER BY id");
    assert_eq!(r2.rows().len(), 2);
}

#[test]
fn coalesce_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE coal (id INT PRIMARY KEY, a TEXT, b TEXT)");
    exec(&db, "INSERT INTO coal VALUES (1, NULL, 'fallback'), (2, 'primary', 'fallback')");
    let r = exec(&db, "SELECT COALESCE(a, b) FROM coal ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Text("fallback".to_string()));
    assert_eq!(r.rows()[1].values()[0], Value::Text("primary".to_string()));
}

#[test]
fn nullif_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nf (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO nf VALUES (1, 10, 10), (2, 10, 20)");
    let r = exec(&db, "SELECT NULLIF(a, b) FROM nf ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
    assert_eq!(r.rows()[1].values()[0], Value::Int64(10));
}

// ─── UNION/INTERSECT/EXCEPT in correlated subqueries (exec/mod.rs L1745-1767) ───

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

// ─── SQL expression coverage: CAST, complex CASE ───

#[test]
fn cast_int_to_text() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cast_t (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO cast_t VALUES (42)");
    let r = exec(&db, "SELECT CAST(id AS TEXT) FROM cast_t");
    assert_eq!(r.rows()[0].values()[0], Value::Text("42".to_string()));
}

#[test]
fn cast_text_to_int() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cast_t2 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO cast_t2 VALUES (1, '99')");
    let r = exec(&db, "SELECT CAST(val AS INT) FROM cast_t2");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(99));
}

#[test]
fn nested_case_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nested_case (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nested_case VALUES (1, 10), (2, 50), (3, 90)");
    let r = exec(&db, "
        SELECT id, 
            CASE 
                WHEN val < 30 THEN CASE WHEN val < 20 THEN 'very_low' ELSE 'low' END
                WHEN val < 70 THEN 'medium'
                ELSE 'high'
            END as category
        FROM nested_case
        ORDER BY id
    ");
    assert_eq!(r.rows()[0].values()[1], Value::Text("very_low".to_string()));
    assert_eq!(r.rows()[1].values()[1], Value::Text("medium".to_string()));
    assert_eq!(r.rows()[2].values()[1], Value::Text("high".to_string()));
}

#[test]
fn simple_case_expression_with_operand() {
    let db = mem_db();
    exec(&db, "CREATE TABLE simple_case (id INT PRIMARY KEY, status TEXT)");
    exec(&db, "INSERT INTO simple_case VALUES (1, 'active'), (2, 'inactive'), (3, 'pending')");
    let r = exec(&db, "
        SELECT id,
            CASE status
                WHEN 'active' THEN 1
                WHEN 'inactive' THEN 0
                ELSE -1
            END as code
        FROM simple_case
        ORDER BY id
    ");
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[1], Value::Int64(0));
    assert_eq!(r.rows()[2].values()[1], Value::Int64(-1));
}

// ─── Triggers coverage ───

#[test]
fn trigger_after_update() {
    let db = mem_db();
    exec(&db, "CREATE TABLE watched (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE update_log (msg TEXT)");
    exec(&db, "
        CREATE TRIGGER on_update
        AFTER UPDATE ON watched
        FOR EACH ROW
        EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO update_log VALUES (''updated'')')
    ");
    exec(&db, "INSERT INTO watched VALUES (1, 'old')");
    exec(&db, "UPDATE watched SET val = 'new' WHERE id = 1");
    let r = exec(&db, "SELECT * FROM update_log");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn trigger_after_delete() {
    let db = mem_db();
    exec(&db, "CREATE TABLE del_watched (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE delete_log (msg TEXT)");
    exec(&db, "
        CREATE TRIGGER on_delete
        AFTER DELETE ON del_watched
        FOR EACH ROW
        EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO delete_log VALUES (''deleted'')')
    ");
    exec(&db, "INSERT INTO del_watched VALUES (1, 'data')");
    exec(&db, "DELETE FROM del_watched WHERE id = 1");
    let r = exec(&db, "SELECT * FROM delete_log");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn drop_trigger() {
    let db = mem_db();
    exec(&db, "CREATE TABLE trig_t (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE trig_log (msg TEXT)");
    exec(&db, "
        CREATE TRIGGER trig1
        AFTER INSERT ON trig_t
        FOR EACH ROW
        EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO trig_log VALUES (''inserted'')')
    ");
    exec(&db, "DROP TRIGGER trig1 ON trig_t");
    exec(&db, "INSERT INTO trig_t VALUES (1)");
    let r = exec(&db, "SELECT * FROM trig_log");
    assert_eq!(r.rows().len(), 0);
}

// ─── Savepoint and nested transaction coverage ───

#[test]
fn savepoint_release_and_rollback() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sp_t (id INT PRIMARY KEY, val TEXT)");
    db.begin_transaction().unwrap();
    exec(&db, "INSERT INTO sp_t VALUES (1, 'committed')");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO sp_t VALUES (2, 'rolled_back')");
    db.rollback_to_savepoint("sp1").unwrap();
    exec(&db, "INSERT INTO sp_t VALUES (3, 'after_rollback')");
    db.commit_transaction().unwrap();
    let r = exec(&db, "SELECT id FROM sp_t ORDER BY id");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(3));
}

// ─── Additional coverage: EXPLAIN ───

#[test]
fn explain_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE explain_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx ON explain_t (val)");
    let r = exec(&db, "EXPLAIN SELECT * FROM explain_t WHERE val = 'x'");
    assert!(!r.explain_lines().is_empty());
}

#[test]
fn explain_insert() {
    let db = mem_db();
    exec(&db, "CREATE TABLE explain_ins (id INT PRIMARY KEY, val TEXT)");
    let r = exec(&db, "EXPLAIN INSERT INTO explain_ins VALUES (1, 'test')");
    assert!(!r.explain_lines().is_empty());
}

// ─── Temp tables ───

#[test]
fn create_temp_table() {
    let db = mem_db();
    exec(&db, "CREATE TEMP TABLE tmp (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO tmp VALUES (1, 'temp_data')");
    let r = exec(&db, "SELECT * FROM tmp");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn temp_table_not_visible_after_reconnect() {
    let path = "/tmp/decentdb_test_temp_reconnect.ddb";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        exec(&db, "CREATE TEMP TABLE tmp2 (id INT)");
        exec(&db, "INSERT INTO tmp2 VALUES (1)");
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(path, DbConfig::default()).unwrap();
        let err = exec_err(&db, "SELECT * FROM tmp2");
        assert!(err.contains("tmp2") || err.contains("not found") || err.contains("unknown"), "got: {err}");
    }
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
}

// ─── Multiple column types in a single table ───

#[test]
fn all_basic_types_roundtrip() {
    let db = mem_db();
    exec(&db, "CREATE TABLE all_types (
        id INT PRIMARY KEY,
        i INT,
        f FLOAT,
        t TEXT,
        b BOOLEAN
    )");
    exec(&db, "INSERT INTO all_types VALUES (1, 42, 3.14, 'hello', true)");
    let r = exec(&db, "SELECT * FROM all_types");
    assert_eq!(r.rows().len(), 1);
    let vals = r.rows()[0].values();
    assert_eq!(vals[1], Value::Int64(42));
    assert_eq!(vals[4], Value::Bool(true));
}

// ─── UUID handling ───

#[test]
fn uuid_column_type() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uuid_t (id INT PRIMARY KEY, uid UUID)");
    // Use parameterized insert for UUID
    db.execute_with_params("INSERT INTO uuid_t VALUES ($1, $2)", &[
        Value::Int64(1),
        Value::Uuid([0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                     0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00]),
    ]).unwrap();
    let r = exec(&db, "SELECT uid FROM uuid_t");
    assert_eq!(r.rows().len(), 1);
}

// ─── Additional DDL/constraint coverage ───

#[test]
fn create_unique_index() {
    let db = mem_db();
    exec(&db, "CREATE TABLE unique_idx_t (id INT PRIMARY KEY, code TEXT)");
    exec(&db, "CREATE UNIQUE INDEX idx_code ON unique_idx_t (code)");
    exec(&db, "INSERT INTO unique_idx_t VALUES (1, 'a')");
    let err = exec_err(&db, "INSERT INTO unique_idx_t VALUES (2, 'a')");
    assert!(err.contains("unique") || err.contains("duplicate") || err.contains("constraint"), "got: {err}");
}

#[test]
fn multi_column_index() {
    let db = mem_db();
    exec(&db, "CREATE TABLE multi_idx_t (id INT PRIMARY KEY, a TEXT, b TEXT)");
    exec(&db, "CREATE INDEX idx_ab ON multi_idx_t (a, b)");
    for i in 0..50 {
        exec(&db, &format!("INSERT INTO multi_idx_t VALUES ({i}, 'a{a}', 'b{b}')", a = i % 5, b = i % 10));
    }
    let r = exec(&db, "SELECT id FROM multi_idx_t WHERE a = 'a0' AND b = 'b0'");
    assert!(r.rows().len() >= 1);
}

#[test]
fn foreign_key_on_delete_set_null() {
    let db = mem_db();
    exec(&db, "CREATE TABLE fk_parent (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE fk_child (
        id INT PRIMARY KEY,
        parent_id INT REFERENCES fk_parent(id) ON DELETE SET NULL
    )");
    exec(&db, "INSERT INTO fk_parent VALUES (1), (2)");
    exec(&db, "INSERT INTO fk_child VALUES (10, 1), (20, 2)");
    exec(&db, "DELETE FROM fk_parent WHERE id = 1");
    let r = exec(&db, "SELECT parent_id FROM fk_child WHERE id = 10");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn foreign_key_on_update_cascade() {
    let db = mem_db();
    exec(&db, "CREATE TABLE fk_up_parent (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE fk_up_child (
        id INT PRIMARY KEY,
        parent_id INT REFERENCES fk_up_parent(id) ON UPDATE CASCADE
    )");
    exec(&db, "INSERT INTO fk_up_parent VALUES (1, 'old')");
    exec(&db, "INSERT INTO fk_up_child VALUES (10, 1)");
    exec(&db, "UPDATE fk_up_parent SET id = 100 WHERE id = 1");
    let r = exec(&db, "SELECT parent_id FROM fk_up_child WHERE id = 10");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(100));
}

// ─── Aggregate functions coverage ───

#[test]
fn aggregate_string_agg() {
    let db = mem_db();
    exec(&db, "CREATE TABLE str_agg (id INT PRIMARY KEY, grp TEXT, val TEXT)");
    exec(&db, "INSERT INTO str_agg VALUES (1, 'a', 'x'), (2, 'a', 'y'), (3, 'b', 'z')");
    let r = exec(&db, "SELECT grp, STRING_AGG(val, ',') FROM str_agg GROUP BY grp ORDER BY grp");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn aggregate_avg() {
    let db = mem_db();
    exec(&db, "CREATE TABLE avg_t (id INT PRIMARY KEY, val FLOAT)");
    exec(&db, "INSERT INTO avg_t VALUES (1, 10.0), (2, 20.0), (3, 30.0)");
    let r = exec(&db, "SELECT AVG(val) FROM avg_t");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn count_distinct() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cd (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO cd VALUES (1, 'a'), (2, 'b'), (3, 'a')");
    let r = exec(&db, "SELECT COUNT(DISTINCT val) FROM cd");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ─── Window functions in more complex contexts ───

#[test]
fn window_function_with_partition_and_order() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wf (id INT PRIMARY KEY, grp TEXT, val INT)");
    exec(&db, "INSERT INTO wf VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40)");
    let r = exec(&db, "
        SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
        FROM wf
        ORDER BY id
    ");
    assert_eq!(r.rows().len(), 4);
    assert_eq!(r.rows()[0].values()[3], Value::Int64(1));
    assert_eq!(r.rows()[1].values()[3], Value::Int64(2));
}

#[test]
fn window_function_lag_lead() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wf2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wf2 VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "
        SELECT id, val,
            LAG(val) OVER (ORDER BY id) as prev_val,
            LEAD(val) OVER (ORDER BY id) as next_val
        FROM wf2
        ORDER BY id
    ");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[2], Value::Null); // no previous for first row
    assert_eq!(r.rows()[0].values()[3], Value::Int64(20));
}

#[test]
fn window_function_rank_dense_rank() {
    let db = mem_db();
    exec(&db, "CREATE TABLE wf3 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO wf3 VALUES (1, 10), (2, 10), (3, 20), (4, 20), (5, 30)");
    let r = exec(&db, "
        SELECT id, val,
            RANK() OVER (ORDER BY val) as rnk,
            DENSE_RANK() OVER (ORDER BY val) as drnk
        FROM wf3
        ORDER BY id
    ");
    assert_eq!(r.rows().len(), 5);
}

// ─── dump_sql coverage for more AST Display paths ───

#[test]
fn dump_sql_with_indexes_and_views() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dump_base (id INT PRIMARY KEY, name TEXT, val INT)");
    exec(&db, "CREATE INDEX dump_idx ON dump_base (name)");
    exec(&db, "CREATE VIEW dump_view AS SELECT id, name FROM dump_base WHERE val > 10");
    exec(&db, "INSERT INTO dump_base VALUES (1, 'test', 20)");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE TABLE"));
    assert!(sql.contains("CREATE INDEX"));
    assert!(sql.contains("CREATE VIEW"));
}

#[test]
fn dump_sql_with_unique_constraint() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dump_unique (id INT PRIMARY KEY, email TEXT UNIQUE)");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("UNIQUE") || sql.contains("unique"));
}

#[test]
fn dump_sql_with_foreign_key() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dump_parent (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE dump_child (id INT PRIMARY KEY, parent_id INT REFERENCES dump_parent(id))");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("REFERENCES"));
}

#[test]
fn dump_sql_with_default_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dump_defaults (id INT PRIMARY KEY, val TEXT DEFAULT 'hello', num INT DEFAULT 42)");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("DEFAULT"));
}

#[test]
fn dump_sql_with_check_constraint() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dump_check (id INT PRIMARY KEY, val INT CHECK (val > 0))");
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CHECK"));
}

// ─── ANALYZE ───

#[test]
fn analyze_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE analyze_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE INDEX idx_analyze ON analyze_t (val)");
    for i in 0..100 {
        exec(&db, &format!("INSERT INTO analyze_t VALUES ({i}, 'v{v}')", v = i % 10));
    }
    exec(&db, "ANALYZE");
}

#[test]
fn analyze_specific_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE an1 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO an1 VALUES (1, 'a')");
    exec(&db, "ANALYZE an1");
}

// ─── Boolean expressions in WHERE ───

#[test]
fn boolean_and_or_not_in_where() {
    let db = mem_db();
    exec(&db, "CREATE TABLE bool_t (id INT PRIMARY KEY, a BOOLEAN, b BOOLEAN)");
    exec(&db, "INSERT INTO bool_t VALUES (1, true, false), (2, false, true), (3, true, true)");
    let r = exec(&db, "SELECT id FROM bool_t WHERE a AND b");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
    let r2 = exec(&db, "SELECT id FROM bool_t WHERE a OR b ORDER BY id");
    assert_eq!(r2.rows().len(), 3);
    let r3 = exec(&db, "SELECT id FROM bool_t WHERE NOT a ORDER BY id");
    assert_eq!(r3.rows().len(), 1);
}

// ─── String operations ───

#[test]
fn string_length_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE str_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO str_t VALUES (1, 'hello'), (2, ''), (3, 'world!')");
    let r = exec(&db, "SELECT length(val) FROM str_t ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
    assert_eq!(r.rows()[1].values()[0], Value::Int64(0));
    assert_eq!(r.rows()[2].values()[0], Value::Int64(6));
}

#[test]
fn string_lower_upper() {
    let db = mem_db();
    exec(&db, "CREATE TABLE case_t (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO case_t VALUES (1, 'Hello World')");
    let r1 = exec(&db, "SELECT lower(val) FROM case_t");
    assert_eq!(r1.rows()[0].values()[0], Value::Text("hello world".to_string()));
    let r2 = exec(&db, "SELECT upper(val) FROM case_t");
    assert_eq!(r2.rows()[0].values()[0], Value::Text("HELLO WORLD".to_string()));
}

#[test]
fn string_concatenation_operator() {
    let db = mem_db();
    exec(&db, "CREATE TABLE concat_t (id INT PRIMARY KEY, first TEXT, last TEXT)");
    exec(&db, "INSERT INTO concat_t VALUES (1, 'John', 'Doe')");
    let r = exec(&db, "SELECT first || ' ' || last FROM concat_t");
    assert_eq!(r.rows()[0].values()[0], Value::Text("John Doe".to_string()));
}

// ─── Overflow handling for large text/blobs ───

#[test]
fn large_text_storage_and_retrieval() {
    let db = mem_db();
    exec(&db, "CREATE TABLE large_t (id INT PRIMARY KEY, data TEXT)");
    let big_text = "x".repeat(100_000);
    db.execute_with_params("INSERT INTO large_t VALUES ($1, $2)", &[
        Value::Int64(1),
        Value::Text(big_text.clone()),
    ]).unwrap();
    let r = exec(&db, "SELECT data FROM large_t WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text(big_text));
}

#[test]
fn large_blob_storage_and_retrieval() {
    let db = mem_db();
    exec(&db, "CREATE TABLE large_b (id INT PRIMARY KEY, data BLOB)");
    let big_blob = vec![0xABu8; 100_000];
    db.execute_with_params("INSERT INTO large_b VALUES ($1, $2)", &[
        Value::Int64(1),
        Value::Blob(big_blob.clone()),
    ]).unwrap();
    let r = exec(&db, "SELECT data FROM large_b WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Blob(big_blob));
}
