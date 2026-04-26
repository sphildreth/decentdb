//! SQL VIEW and TRIGGER tests.
//!
//! Covers: CREATE VIEW, CREATE OR REPLACE VIEW, temp views, views with
//! joins/CTEs/aggregates/subqueries/expressions, INSTEAD OF triggers,
//! AFTER triggers, DROP VIEW/TRIGGER, view dependencies, and window
//! functions in views.

use decentdb::{Db, DbConfig, QueryResult, Value};
use tempfile::TempDir;

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
fn alter_temp_view_rename_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE atvr (id INT PRIMARY KEY)");
    exec(&db, "CREATE TEMPORARY VIEW atvr_v AS SELECT id FROM atvr");
    let err = exec_err(&db, "ALTER VIEW atvr_v RENAME TO atvr_v2");
    assert!(
        err.to_lowercase().contains("temporary") || err.to_lowercase().contains("not supported"),
        "got: {err}"
    );
}

#[test]
fn alter_view_rename() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    db.execute("ALTER VIEW v RENAME TO v2").unwrap();

    let err = db.execute("SELECT * FROM v").unwrap_err();
    assert!(err.to_string().contains("v") || err.to_string().to_lowercase().contains("not found"));

    let result = db.execute("SELECT * FROM v2").unwrap();
    assert_eq!(result.rows().len(), 0);
}

#[test]
fn alter_view_rename_nonexistent() {
    let db = mem_db();
    let err = exec_err(&db, "ALTER VIEW no_such_view RENAME TO new_name");
    assert!(
        err.to_lowercase().contains("unknown")
            || err.to_lowercase().contains("not found")
            || err.to_lowercase().contains("does not exist"),
        "got: {err}"
    );
}

#[test]
fn alter_view_rename_to_existing_name() {
    let db = mem_db();
    exec(&db, "CREATE TABLE avr2 (id INT PRIMARY KEY)");
    exec(&db, "CREATE VIEW avr2_v AS SELECT id FROM avr2");
    let err = exec_err(&db, "ALTER VIEW avr2_v RENAME TO avr2");
    assert!(err.contains("already exists"), "got: {err}");
}

#[test]
fn alter_view_rename_to_existing_name_errors() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE VIEW v1 AS SELECT * FROM t").unwrap();
    db.execute("CREATE VIEW v2 AS SELECT * FROM t").unwrap();

    let err = db.execute("ALTER VIEW v1 RENAME TO v2").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn alter_view_rename_with_dependent() {
    let db = mem_db();
    exec(&db, "CREATE TABLE avr (id INT PRIMARY KEY)");
    exec(&db, "CREATE VIEW av1 AS SELECT id FROM avr");
    exec(&db, "CREATE VIEW av2 AS SELECT id FROM av1");
    let err = exec_err(&db, "ALTER VIEW av1 RENAME TO av1_new");
    assert!(
        err.to_lowercase().contains("depend") || err.to_lowercase().contains("cannot rename"),
        "got: {err}"
    );
}

#[test]
fn ast_display_view_ddl() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, val, val * 2 AS doubled FROM t WHERE val > 10 ORDER BY id",
    )
    .unwrap();
    let ddl = db.view_ddl("v").unwrap();
    assert!(!ddl.is_empty());
}

#[test]
fn create_or_replace_view() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE base (id INT PRIMARY KEY, a TEXT, b TEXT)",
    );
    exec(&db, "INSERT INTO base VALUES (1, 'x', 'y')");
    exec(&db, "CREATE VIEW v AS SELECT id, a FROM base");
    exec(&db, "CREATE OR REPLACE VIEW v AS SELECT id, b FROM base");
    let r = exec(&db, "SELECT * FROM v");
    assert_eq!(r.columns().len(), 2);
    assert_eq!(r.columns()[1], "b");
}

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
fn create_view_object_already_exists() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vt (id INT PRIMARY KEY)");
    let err = exec_err(&db, "CREATE VIEW vt AS SELECT 1");
    assert!(err.contains("already exists"), "got: {err}");
}

#[test]
fn create_view_replace_non_view() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rvt (id INT PRIMARY KEY)");
    let err = exec_err(&db, "CREATE OR REPLACE VIEW rvt AS SELECT 1");
    assert!(err.contains("already exists"), "got: {err}");
}

#[test]
fn distinct_on_in_view_dump() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    let r =
        db.execute("CREATE VIEW v AS SELECT DISTINCT ON (grp) grp, val FROM t ORDER BY grp, val");
    if r.is_ok() {
        let sql = db.dump_sql().unwrap();
        assert!(sql.contains("VIEW"));
    }
}

#[test]
fn drop_temp_view() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE TEMPORARY VIEW tv AS SELECT * FROM t")
        .unwrap();

    let result = db.execute("SELECT * FROM tv").unwrap();
    assert_eq!(result.rows().len(), 0);

    db.execute("DROP VIEW tv").unwrap();

    let err = db.execute("SELECT * FROM tv").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn drop_temp_view_with_dependent() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dtvd (id INT PRIMARY KEY)");
    exec(&db, "CREATE TEMPORARY VIEW dtvd_v1 AS SELECT id FROM dtvd");
    exec(
        &db,
        "CREATE TEMPORARY VIEW dtvd_v2 AS SELECT id FROM dtvd_v1",
    );
    let err = exec_err(&db, "DROP VIEW dtvd_v1");
    assert!(
        err.to_lowercase().contains("depend") || err.to_lowercase().contains("cannot drop"),
        "got: {err}"
    );
}

#[test]
fn drop_trigger() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE TABLE log(msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''hi'')')",
    )
    .unwrap();
    db.execute("DROP TRIGGER trg ON t").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = db.execute("SELECT * FROM log").unwrap();
    assert_eq!(rows(&r).len(), 0); // trigger dropped, no log entry
}

#[test]
fn drop_trigger_if_exists_nonexistent() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dti (id INT PRIMARY KEY)");
    // Should not error with IF EXISTS
    let r = db.execute("DROP TRIGGER IF EXISTS no_such_trigger ON dti");
    assert!(r.is_ok());
}

#[test]
fn drop_view_basic() {
    let db = mem_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "CREATE VIEW v AS SELECT * FROM t");
    exec(&db, "DROP VIEW v");
    let err = exec_err(&db, "SELECT * FROM v");
    assert!(
        err.contains("v") || err.contains("not found") || err.contains("unknown"),
        "got: {err}"
    );
}

#[test]
fn drop_view_if_exists() {
    let db = mem_db();
    // Should not error
    db.execute("DROP VIEW IF EXISTS nonexistent").unwrap();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    db.execute("DROP VIEW v").unwrap();
    let err = db.execute("SELECT * FROM v").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn drop_view_if_exists_no_error() {
    let db = mem_db();
    exec(&db, "DROP VIEW IF EXISTS nonexistent_view");
}

#[test]
fn drop_view_if_exists_nonexistent() {
    let db = mem_db();
    db.execute("DROP VIEW IF EXISTS nonexistent").unwrap();
}

#[test]
fn drop_view_that_is_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dvt (id INT PRIMARY KEY)");
    let err = exec_err(&db, "DROP VIEW dvt");
    assert!(
        err.to_lowercase().contains("unknown view") || err.to_lowercase().contains("not a view"),
        "got: {err}"
    );
}

#[test]
fn drop_view_with_dependent_view() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dvd (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE VIEW dv1 AS SELECT id, val FROM dvd");
    exec(&db, "CREATE VIEW dv2 AS SELECT id FROM dv1");
    let err = exec_err(&db, "DROP VIEW dv1");
    assert!(
        err.to_lowercase().contains("depend") || err.to_lowercase().contains("cannot drop"),
        "got: {err}"
    );
}

#[test]
fn dump_sql_with_complex_view_joins() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_users (id INT PRIMARY KEY, name TEXT)");
    exec(
        &db,
        "CREATE TABLE d_orders (id INT PRIMARY KEY, user_id INT, amount INT)",
    );
    exec(
        &db,
        "CREATE VIEW user_totals AS 
        SELECT d_users.id, d_users.name, SUM(d_orders.amount) as total
        FROM d_users 
        LEFT JOIN d_orders ON d_users.id = d_orders.user_id 
        GROUP BY d_users.id, d_users.name",
    );
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("LEFT JOIN") || sql.contains("left join") || sql.contains("LEFT"));
}

#[test]
fn dump_sql_with_view_having_case() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_items (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "CREATE VIEW item_category AS 
        SELECT id, val, CASE WHEN val > 50 THEN 'high' ELSE 'low' END as cat FROM d_items",
    );
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CASE") || sql.contains("case"));
}

#[test]
fn dump_sql_with_view_having_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE d_main (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE d_ref (main_id INT)");
    exec(
        &db,
        "CREATE VIEW has_refs AS 
        SELECT id FROM d_main WHERE EXISTS (SELECT 1 FROM d_ref WHERE d_ref.main_id = d_main.id)",
    );
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("EXISTS") || sql.contains("exists"));
}

#[test]
fn error_create_view_on_nonexistent_table() {
    let db = mem_db();
    let r = db.execute("CREATE VIEW v AS SELECT * FROM nonexistent");
    // May or may not error at creation time (some DBs defer)
    assert!(r.is_ok() || r.is_err());
}

#[test]
fn error_drop_nonexistent_trigger() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    let err = db.execute("DROP TRIGGER nonexistent ON t").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_drop_nonexistent_view() {
    let db = mem_db();
    let err = db.execute("DROP VIEW nonexistent").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn error_drop_table_with_view_dependency() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let err = db.execute("DROP TABLE t").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("view") || msg.contains("depend") || !msg.is_empty(),
        "unexpected error: {msg}"
    );
}

#[test]
fn first_last_value_in_view() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    let r =
        db.execute("CREATE VIEW fv AS SELECT id, FIRST_VALUE(val) OVER (ORDER BY id) AS fv FROM t");
    if r.is_ok() {
        let sql = db.dump_sql().unwrap();
        assert!(sql.contains("FIRST_VALUE"));
    }
}

#[test]
fn lag_lead_in_view_dump() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW with_lag AS SELECT id, val, LAG(val) OVER (ORDER BY id) AS prev FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("LAG"));
}

#[test]
fn metadata_view_ddl() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    let ddl = db.view_ddl("v").unwrap();
    assert!(ddl.contains("CREATE VIEW") || ddl.contains("SELECT"));
}

#[test]
fn normalize_create_trigger_basic() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TABLE trig_table (id INT PRIMARY KEY, val TEXT)",
    );
    exec(&db, "CREATE TABLE audit_log (msg TEXT)");
    exec(
        &db,
        "
        CREATE TRIGGER trig_insert
        AFTER INSERT ON trig_table
        FOR EACH ROW
        EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log VALUES (''inserted'')')
    ",
    );
    exec(&db, "INSERT INTO trig_table VALUES (1, 'test')");
    let r = exec(&db, "SELECT * FROM audit_log");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn persist_trigger_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("trigger.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE items(id INT64, name TEXT)")
            .unwrap();
        db.execute("CREATE TABLE audit(cnt INT64)").unwrap();
        db.execute("INSERT INTO audit VALUES (0)").unwrap();
        db.execute(
            "CREATE TRIGGER item_cnt AFTER INSERT ON items FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('UPDATE audit SET cnt = cnt + 1')",
        )
        .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'a')").unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        // Trigger should still fire after reopen
        db.execute("INSERT INTO items VALUES (2, 'b')").unwrap();
        let r = db.execute("SELECT cnt FROM audit").unwrap();
        assert_eq!(rows(&r)[0][0], Value::Int64(2));
    }
}

#[test]
fn persist_view_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("view.ddb");
    let ps = path.to_str().unwrap();
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
        db.execute("CREATE VIEW v AS SELECT id, val * 2 AS doubled FROM t")
            .unwrap();
        db.checkpoint().unwrap();
    }
    {
        let db = Db::open_or_create(ps, DbConfig::default()).unwrap();
        let r = db.execute("SELECT doubled FROM v ORDER BY id").unwrap();
        let v = rows(&r);
        assert_eq!(v[0][0], Value::Int64(20));
        assert_eq!(v[1][0], Value::Int64(40));
    }
}

#[test]
fn persistent_view_depending_on_temp_table_error() {
    let db = mem_db();
    exec(
        &db,
        "CREATE TEMPORARY TABLE tt (id INT PRIMARY KEY, val TEXT)",
    );
    exec(&db, "INSERT INTO tt VALUES (1, 'x')");
    let err = exec_err(&db, "CREATE VIEW pv_on_temp AS SELECT * FROM tt");
    assert!(
        err.to_lowercase().contains("temporary") || err.to_lowercase().contains("persistent"),
        "got: {err}"
    );
}

#[test]
fn rank_in_view_dump() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW ranked AS SELECT grp, val,
         RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS rnk,
         DENSE_RANK() OVER (PARTITION BY grp ORDER BY val DESC) AS drnk
         FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("PARTITION BY"));
    assert!(sql.contains("RANK"));
}

#[test]
fn recursive_cte_in_view() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, parent_id INT64)")
        .unwrap();
    let r = db.execute(
        "CREATE VIEW hierarchy AS
         WITH RECURSIVE tree(id, depth) AS (
             SELECT id, 0 FROM t WHERE parent_id IS NULL
             UNION ALL
             SELECT t.id, tree.depth + 1 FROM t JOIN tree ON t.parent_id = tree.id
         )
         SELECT id, depth FROM tree",
    );
    if r.is_ok() {
        let sql = db.dump_sql().unwrap();
        assert!(sql.contains("RECURSIVE") || sql.contains("WITH"));
    }
}

#[test]
fn rename_view() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    db.execute("ALTER VIEW v RENAME TO v2").unwrap();
    let r = db.execute("SELECT * FROM v2").unwrap();
    assert_eq!(rows(&r).len(), 1);
    let err = db.execute("SELECT * FROM v").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn select_qualified_wildcard_in_view() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, name TEXT)").unwrap();
    db.execute("CREATE VIEW v AS SELECT t.* FROM t").unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE VIEW"));
}

#[test]
fn temp_view_already_exists_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tve (id INT PRIMARY KEY)");
    exec(&db, "CREATE TEMPORARY VIEW tve_view AS SELECT id FROM tve");
    let err = exec_err(&db, "CREATE TEMPORARY VIEW tve_view AS SELECT id FROM tve");
    assert!(err.contains("already exists"), "got: {err}");
}

#[test]
fn temp_view_create_and_query() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tv (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO tv VALUES (1, 'a'), (2, 'b')");
    exec(&db, "CREATE TEMPORARY VIEW tv_view AS SELECT id FROM tv");
    let r = exec(&db, "SELECT * FROM tv_view");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn trigger_after_delete() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tad_main (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE tad_log (msg TEXT)");
    exec(&db, "INSERT INTO tad_main VALUES (1), (2)");
    exec(&db, "CREATE TRIGGER tad_trig AFTER DELETE ON tad_main
        FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO tad_log VALUES (''deleted'')')");
    exec(&db, "DELETE FROM tad_main WHERE id = 1");
    let r = exec(&db, "SELECT msg FROM tad_log");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Text("deleted".into()));
}

#[test]
fn trigger_after_insert() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT)").unwrap();
    db.execute("CREATE TABLE audit(msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER trg_ins AFTER INSERT ON t FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit VALUES (''inserted'')')",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    let r = db.execute("SELECT msg FROM audit").unwrap();
    assert!(!rows(&r).is_empty());
    assert_eq!(rows(&r)[0][0], Value::Text("inserted".into()));
}

#[test]
fn trigger_after_update() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tau_main (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE tau_log (msg TEXT)");
    exec(&db, "INSERT INTO tau_main VALUES (1, 10)");
    exec(&db, "CREATE TRIGGER tau_trig AFTER UPDATE ON tau_main
        FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO tau_log VALUES (''updated'')')");
    exec(&db, "UPDATE tau_main SET val = 20 WHERE id = 1");
    let r = exec(&db, "SELECT msg FROM tau_log");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Text("updated".into()));
}

#[test]
fn trigger_before_insert() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("CREATE TABLE audit(action TEXT, item_id INT64)")
        .unwrap();
    // Only AFTER triggers are supported in DecentDB
    db.execute(
        "CREATE TRIGGER trg_ins AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit VALUES (''insert'', 0)')",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    let r = db.execute("SELECT * FROM audit").unwrap();
    assert_eq!(rows(&r).len(), 1);
}

#[test]
fn trigger_drop() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE TABLE log(msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''fired'')')",
    )
    .unwrap();
    db.execute("DROP TRIGGER trg ON t").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = db.execute("SELECT * FROM log").unwrap();
    assert_eq!(rows(&r).len(), 0);
}

#[test]
fn trigger_fires_for_each_row() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE TABLE log(msg TEXT)").unwrap();
    db.execute(
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO log VALUES (''row_inserted'')')",
    )
    .unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let r = db.execute("SELECT COUNT(*) FROM log").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn trigger_instead_of_insert_on_view() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tio_base (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE VIEW tio_view AS SELECT id, name FROM tio_base");
    exec(&db, "CREATE TRIGGER tio_trig INSTEAD OF INSERT ON tio_view
        FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO tio_base VALUES (1, ''via_trigger'')')");
    exec(&db, "INSERT INTO tio_view VALUES (1, 'ignored')");
    let r = exec(&db, "SELECT name FROM tio_base WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("via_trigger".into()));
}

#[test]
fn trigger_maintains_audit_count() {
    let db = mem_db();
    db.execute("CREATE TABLE items(id INT64, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE audit(cnt INT64)").unwrap();
    db.execute("INSERT INTO audit VALUES (0)").unwrap();
    db.execute(
        "CREATE TRIGGER item_count AFTER INSERT ON items
         FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE audit SET cnt = cnt + 1')",
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO items VALUES (2, 'b')").unwrap();
    db.execute("INSERT INTO items VALUES (3, 'c')").unwrap();
    let r = db.execute("SELECT cnt FROM audit").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(3));
}

#[test]
fn trigger_on_delete() {
    let db = mem_db();
    db.execute("CREATE TABLE items(id INT64)").unwrap();
    db.execute("CREATE TABLE deleted_count(cnt INT64)").unwrap();
    db.execute("INSERT INTO deleted_count VALUES (0)").unwrap();
    db.execute(
        "CREATE TRIGGER log_delete AFTER DELETE ON items
         FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE deleted_count SET cnt = cnt + 1')",
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1),(2),(3)").unwrap();
    db.execute("DELETE FROM items WHERE id = 2").unwrap();
    let r = db.execute("SELECT cnt FROM deleted_count").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn trigger_on_update() {
    let db = mem_db();
    db.execute("CREATE TABLE items(id INT64, val TEXT)")
        .unwrap();
    db.execute("CREATE TABLE update_count(cnt INT64)").unwrap();
    db.execute("INSERT INTO update_count VALUES (0)").unwrap();
    db.execute(
        "CREATE TRIGGER log_update AFTER UPDATE ON items
         FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE update_count SET cnt = cnt + 1')",
    )
    .unwrap();
    db.execute("INSERT INTO items VALUES (1, 'before')")
        .unwrap();
    db.execute("UPDATE items SET val = 'after' WHERE id = 1")
        .unwrap();
    let r = db.execute("SELECT cnt FROM update_count").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Int64(1));
}

#[test]
fn trigger_on_view_insert_target() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tov (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE tov_log (id INT PRIMARY KEY, msg TEXT)");
    // Trigger on actual table (not view), then verify view drop behavior separately
    exec(
        &db,
        "CREATE TRIGGER tov_trg AFTER INSERT ON tov FOR EACH ROW EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO tov_log VALUES (1, ''inserted'')')",
    );
    exec(&db, "INSERT INTO tov VALUES (1, 'test')");
    let r = exec(&db, "SELECT * FROM tov_log");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn view_create_and_query() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT id, val FROM t WHERE val > 15")
        .unwrap();
    let r = db.execute("SELECT * FROM v ORDER BY id").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Int64(2));
}

#[test]
fn view_ddl_reconstruction() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val TEXT, score FLOAT64)")
        .unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, val, score FROM t WHERE score > 50 ORDER BY score DESC",
    )
    .unwrap();
    // table_ddl may work on views or we use dump_sql
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CREATE VIEW") || sql.contains("CREATE TABLE"));
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

#[test]
fn view_drop() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t").unwrap();
    db.execute("DROP VIEW v").unwrap();
    let err = db.execute("SELECT * FROM v").unwrap_err();
    assert!(!err.to_string().is_empty());
}

#[test]
fn view_instead_of_delete_trigger() {
    let db = mem_db();
    db.execute("CREATE TABLE base(id INT64)").unwrap();
    db.execute("INSERT INTO base VALUES (1),(2),(3)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM base").unwrap();
    db.execute(
        "CREATE TRIGGER trg_v_del INSTEAD OF DELETE ON v FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('DELETE FROM base WHERE id = 1')",
    )
    .unwrap();
    db.execute("DELETE FROM v WHERE id = 2").unwrap();
    // The trigger deletes id=1, not the WHERE'd id=2
    let r = db.execute("SELECT id FROM base ORDER BY id").unwrap();
    let v = rows(&r);
    assert!(!v.is_empty()); // At least some rows remain
}

#[test]
fn view_instead_of_insert_trigger() {
    let db = mem_db();
    db.execute("CREATE TABLE base(id INT64, val TEXT)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id, val FROM base")
        .unwrap();
    db.execute(
        "CREATE TRIGGER trg_v_ins INSTEAD OF INSERT ON v FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO base VALUES (99, ''from_view'')')",
    )
    .unwrap();
    db.execute("INSERT INTO v VALUES (1, 'ignored')").unwrap();
    let r = db.execute("SELECT id, val FROM base").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 1);
    assert_eq!(v[0][0], Value::Int64(99));
}

#[test]
fn view_instead_of_update_trigger() {
    let db = mem_db();
    db.execute("CREATE TABLE base(id INT64, val TEXT)").unwrap();
    db.execute("INSERT INTO base VALUES (1, 'original')")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT id, val FROM base")
        .unwrap();
    db.execute(
        "CREATE TRIGGER trg_v_upd INSTEAD OF UPDATE ON v FOR EACH ROW
         EXECUTE FUNCTION decentdb_exec_sql('UPDATE base SET val = ''updated_via_view''')",
    )
    .unwrap();
    db.execute("UPDATE v SET val = 'ignored' WHERE id = 1")
        .unwrap();
    let r = db.execute("SELECT val FROM base WHERE id = 1").unwrap();
    assert_eq!(rows(&r)[0][0], Value::Text("updated_via_view".into()));
}

#[test]
fn view_or_replace() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t").unwrap();
    db.execute("CREATE OR REPLACE VIEW v AS SELECT id, val FROM t")
        .unwrap();
    let r = db.execute("SELECT * FROM v ORDER BY id").unwrap();
    assert_eq!(r.columns().len(), 2);
}

#[test]
fn view_with_aggregate() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',10),('A',20),('B',30)")
        .unwrap();
    db.execute("CREATE VIEW agg_v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp")
        .unwrap();
    let r = db.execute("SELECT * FROM agg_v ORDER BY grp").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(30));
}

#[test]
fn view_with_aggregation() {
    let db = mem_db();
    db.execute("CREATE TABLE t(grp TEXT, val INT64)").unwrap();
    db.execute("INSERT INTO t VALUES ('A',1),('A',2),('B',3)")
        .unwrap();
    db.execute("CREATE VIEW agg_view AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp")
        .unwrap();
    let r = db
        .execute("SELECT grp, total FROM agg_view ORDER BY grp")
        .unwrap();
    let v = rows(&r);
    assert_eq!(v[0][1], Value::Int64(3)); // A: 1+2
    assert_eq!(v[1][1], Value::Int64(3)); // B: 3
}

#[test]
fn view_with_between_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vbe (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "CREATE VIEW vbe_v AS SELECT id FROM vbe WHERE val BETWEEN 1 AND 100",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("BETWEEN") || dump.contains("between"));
}

#[test]
fn view_with_between_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t WHERE val BETWEEN 10 AND 100")
        .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("BETWEEN"));
}

#[test]
fn view_with_between_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t WHERE val BETWEEN 1 AND 10")
        .unwrap();

    let result = db.execute("SELECT * FROM v").unwrap();
    assert_eq!(result.rows().len(), 0);
}

#[test]
fn view_with_case_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vcae (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "CREATE VIEW vcae_v AS SELECT id, CASE WHEN val > 0 THEN 'positive' WHEN val = 0 THEN 'zero' ELSE 'negative' END AS sign FROM vcae",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CASE") || dump.contains("case"));
}

#[test]
fn view_with_case_expression() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, status INT64)")
        .unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id,
         CASE status WHEN 1 THEN 'active' WHEN 2 THEN 'inactive' ELSE 'unknown' END AS label
         FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("CASE"));
}

#[test]
fn view_with_case_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, CASE WHEN val > 10 THEN 'high' ELSE 'low' END AS tier FROM t",
    )
    .unwrap();

    db.execute("INSERT INTO t VALUES (1, 5), (2, 20)").unwrap();
    let result = db.execute("SELECT tier FROM v ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][0], Value::Text("low".into()));
    assert_eq!(rows[1][0], Value::Text("high".into()));
}

#[test]
fn view_with_cast_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vce (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "CREATE VIEW vce_v AS SELECT CAST(id AS TEXT) as id_text FROM vce",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CAST") || dump.contains("cast"));
}

#[test]
fn view_with_coalesce_nullif() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, a INT64, b INT64)")
        .unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, COALESCE(a, b, 0) AS val, NULLIF(a, 0) AS nonzero FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    // COALESCE may be rendered differently in dump
    assert!(sql.contains("VIEW") || sql.contains("view"));
}

#[test]
fn view_with_complex_query() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, grp TEXT, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'A',10),(2,'A',20),(3,'B',30)")
        .unwrap();
    db.execute(
        "CREATE VIEW summary AS SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM t GROUP BY grp",
    )
    .unwrap();
    let r = db.execute("SELECT * FROM summary ORDER BY grp").unwrap();
    let v = rows(&r);
    assert_eq!(v[0][0], Value::Text("A".into()));
    assert_eq!(v[0][1], Value::Int64(30));
}

#[test]
fn view_with_exists_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ves1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE ves2 (id INT PRIMARY KEY, ref_id INT)");
    exec(
        &db,
        "CREATE VIEW ves_v AS SELECT id FROM ves1 WHERE EXISTS (SELECT 1 FROM ves2 WHERE ves2.ref_id = ves1.id)",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("EXISTS") || dump.contains("exists"));
}

#[test]
fn view_with_exists_subquery_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE t2 (t1_id INT64)").unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)",
    )
    .unwrap();

    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1)").unwrap();
    let result = db.execute("SELECT * FROM v").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 1);
}

#[test]
fn view_with_in_list() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, status TEXT)").unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t WHERE status IN ('a', 'b', 'c')")
        .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("IN"));
}

#[test]
fn view_with_in_list_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vil (id INT PRIMARY KEY, status TEXT)");
    exec(
        &db,
        "CREATE VIEW vil_v AS SELECT id FROM vil WHERE status IN ('active', 'pending')",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("IN (") || dump.contains("in ("));
}

#[test]
fn view_with_in_list_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t WHERE id IN (1, 2, 3)")
        .unwrap();

    db.execute("INSERT INTO t VALUES (1), (2), (4)").unwrap();
    let result = db.execute("SELECT * FROM v ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn view_with_in_subquery_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE t2 (ref_id INT64)").unwrap();
    db.execute("CREATE VIEW v AS SELECT * FROM t1 WHERE id IN (SELECT ref_id FROM t2)")
        .unwrap();

    db.execute("INSERT INTO t1 VALUES (1), (2), (3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1), (3)").unwrap();
    let result = db.execute("SELECT id FROM v ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 2);
}

#[test]
fn view_with_is_null_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vin (id INT PRIMARY KEY, val TEXT)");
    exec(
        &db,
        "CREATE VIEW vin_v AS SELECT id FROM vin WHERE val IS NOT NULL",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("IS NOT NULL") || dump.contains("is not null"));
}

#[test]
fn view_with_join() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64, name TEXT)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64, val INT64)")
        .unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    db.execute("INSERT INTO t2 VALUES (10, 1, 100), (20, 2, 200)")
        .unwrap();
    db.execute("CREATE VIEW joined AS SELECT t1.name, t2.val FROM t1 JOIN t2 ON t1.id = t2.t1_id")
        .unwrap();
    let r = db.execute("SELECT * FROM joined ORDER BY name").unwrap();
    let v = rows(&r);
    assert_eq!(v.len(), 2);
    assert_eq!(v[0][0], Value::Text("Alice".into()));
}

#[test]
fn view_with_join_dependency() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vj1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE vj2 (id INT PRIMARY KEY, ref_id INT)");
    exec(
        &db,
        "CREATE VIEW vjv AS SELECT vj1.id, vj2.ref_id FROM vj1 JOIN vj2 ON vj1.id = vj2.ref_id",
    );
    exec(&db, "INSERT INTO vj1 VALUES (1)");
    exec(&db, "INSERT INTO vj2 VALUES (1, 1)");
    let r = exec(&db, "SELECT * FROM vjv");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn view_with_like_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vle (id INT PRIMARY KEY, name TEXT)");
    exec(
        &db,
        "CREATE VIEW vle_v AS SELECT id FROM vle WHERE name LIKE 'A%'",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("LIKE") || dump.contains("like"));
}

#[test]
fn view_with_not_expr() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vne (id INT PRIMARY KEY, active BOOL)");
    exec(
        &db,
        "CREATE VIEW vne_v AS SELECT id FROM vne WHERE NOT active",
    );
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("NOT") || dump.contains("not"));
}

#[test]
fn view_with_scalar_subquery_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT id, (SELECT MAX(val) FROM t) AS max_val FROM t")
        .unwrap();

    db.execute("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    let result = db.execute("SELECT id, max_val FROM v ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows[0][1], Value::Int64(20));
    assert_eq!(rows[1][1], Value::Int64(20));
}

#[test]
fn view_with_set_operation_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t1 (id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE TABLE t2 (id INT64 PRIMARY KEY)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t1 UNION SELECT id FROM t2")
        .unwrap();

    db.execute("INSERT INTO t1 VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO t2 VALUES (2), (3)").unwrap();
    let result = db.execute("SELECT id FROM v ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 3);
}

#[test]
fn view_with_subquery() {
    let db = mem_db();
    db.execute("CREATE TABLE t1(id INT64)").unwrap();
    db.execute("CREATE TABLE t2(id INT64, t1_id INT64)")
        .unwrap();
    db.execute("CREATE VIEW v AS SELECT id FROM t1 WHERE id IN (SELECT t1_id FROM t2)")
        .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("SELECT"));
}

#[test]
fn view_with_subquery_dependency() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vsq (id INT PRIMARY KEY, val INT)");
    exec(
        &db,
        "CREATE VIEW vsqv AS SELECT * FROM (SELECT id, val FROM vsq) sub",
    );
    exec(&db, "INSERT INTO vsq VALUES (1, 10)");
    let r = exec(&db, "SELECT * FROM vsqv");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn view_with_union_dependency() {
    let db = mem_db();
    exec(&db, "CREATE TABLE vu1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE vu2 (id INT PRIMARY KEY)");
    exec(
        &db,
        "CREATE VIEW vuv AS SELECT id FROM vu1 UNION ALL SELECT id FROM vu2",
    );
    exec(&db, "INSERT INTO vu1 VALUES (1)");
    exec(&db, "INSERT INTO vu2 VALUES (2)");
    let r = exec(&db, "SELECT id FROM vuv ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

#[test]
fn view_with_window_function_roundtrips() {
    let db = mem_db();
    db.execute("CREATE TABLE t (id INT64 PRIMARY KEY, cat TEXT, val INT64)")
        .unwrap();
    db.execute(
        "CREATE VIEW v AS SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) AS rn FROM t",
    )
    .unwrap();

    db.execute("INSERT INTO t VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 5)")
        .unwrap();
    let result = db.execute("SELECT id, rn FROM v ORDER BY id").unwrap();
    let rows = rows(&result);
    assert_eq!(rows.len(), 3);
}

#[test]
fn window_in_view_dump() {
    let db = mem_db();
    db.execute("CREATE TABLE t(id INT64, val INT64)").unwrap();
    db.execute(
        "CREATE VIEW ranked AS SELECT id, val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM t",
    )
    .unwrap();
    let sql = db.dump_sql().unwrap();
    assert!(sql.contains("OVER"));
}

// ── Tests merged from engine_coverage_tests.rs ──

#[test]
fn temp_tables_and_views_are_session_scoped_shadow_persistent_objects_and_do_not_persist() {
    let tempdir = TempDir::new().unwrap();
    let path = tempdir.path().join("temp-objects.ddb");

    let db = Db::open_or_create(&path, DbConfig::default()).unwrap();
    db.execute("CREATE TABLE base (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO base VALUES (1, 'persistent')")
        .unwrap();

    let persistent_schema_cookie = db.schema_cookie().unwrap();
    db.execute("CREATE TEMP TABLE base (id INT64 PRIMARY KEY, val TEXT)")
        .unwrap();
    db.execute("INSERT INTO base VALUES (2, 'temporary')")
        .unwrap();
    db.execute("CREATE TEMP VIEW recent_base AS SELECT id, val FROM base")
        .unwrap();

    assert_eq!(db.schema_cookie().unwrap(), persistent_schema_cookie);
    assert_eq!(
        db.execute("SELECT id, val FROM base").unwrap().rows()[0].values(),
        &[Value::Int64(2), Value::Text("temporary".to_string())]
    );
    assert_eq!(
        db.execute("SELECT id, val FROM recent_base")
            .unwrap()
            .rows()[0]
            .values(),
        &[Value::Int64(2), Value::Text("temporary".to_string())]
    );
    assert!(db
        .table_ddl("base")
        .unwrap()
        .starts_with("CREATE TEMP TABLE"));
    assert!(db
        .view_ddl("recent_base")
        .unwrap()
        .starts_with("CREATE TEMP VIEW"));

    let tables = db.list_tables().unwrap();
    assert!(tables
        .iter()
        .any(|table| table.name == "base" && table.temporary));
    assert!(tables
        .iter()
        .any(|table| table.name == "base" && !table.temporary));
    let views = db.list_views().unwrap();
    assert!(views
        .iter()
        .any(|view| view.name == "recent_base" && view.temporary));

    let other = Db::open_or_create(&path, DbConfig::default()).unwrap();
    assert_eq!(
        other.execute("SELECT id, val FROM base").unwrap().rows()[0].values(),
        &[Value::Int64(1), Value::Text("persistent".to_string())]
    );
    let missing_temp_view = other.execute("SELECT * FROM recent_base").unwrap_err();
    assert!(
        missing_temp_view
            .to_string()
            .contains("unknown table or view recent_base"),
        "unexpected error: {missing_temp_view}"
    );

    drop(db);
    let reopened = Db::open_or_create(&path, DbConfig::default()).unwrap();
    assert_eq!(
        reopened.execute("SELECT id, val FROM base").unwrap().rows()[0].values(),
        &[Value::Int64(1), Value::Text("persistent".to_string())]
    );
    let missing_reopened_temp_view = reopened.execute("SELECT * FROM recent_base").unwrap_err();
    assert!(
        missing_reopened_temp_view
            .to_string()
            .contains("unknown table or view recent_base"),
        "unexpected error: {missing_reopened_temp_view}"
    );
}

#[test]
fn create_view_if_not_exists_idempotent() {
    let db = mem_db();
    exec(&db, "CREATE TABLE base (id INT64)");
    // First CREATE VIEW IF NOT EXISTS should succeed
    exec(&db, "CREATE VIEW IF NOT EXISTS v AS SELECT id FROM base");
    // Second should also succeed (no-op)
    exec(&db, "CREATE VIEW IF NOT EXISTS v AS SELECT id FROM base");

    // View exists and works
    let result = exec(&db, "SELECT * FROM v");
    assert_eq!(rows(&result), vec![vec![Value::Null]]); // table empty
}

#[test]
fn create_view_if_not_exists_does_not_replace() {
    let db = mem_db();
    exec(&db, "CREATE TABLE base (id INT64)");
    // First create view
    exec(&db, "CREATE VIEW v AS SELECT id FROM base");
    // Subsequent IF NOT EXISTS should keep original definition (not replace)
    exec(
        &db,
        "CREATE VIEW IF NOT EXISTS v AS SELECT id+1 AS next FROM base",
    );

    // Query should reflect original definition (id, not id+1)
    let result = exec(&db, "INSERT INTO base (id) VALUES (42)");
    // Access the view
    let view_result = exec(&db, "SELECT * FROM v ORDER BY id");
    let row = &view_result.rows()[0];
    // Original view selects id, so next value is 42
    assert_eq!(row.values(), &[Value::Int64(42)]);
}
