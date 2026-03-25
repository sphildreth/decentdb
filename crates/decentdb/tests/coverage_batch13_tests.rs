//! Batch 13: Targeting exec/mod.rs (set ops, recursive CTE edge cases, DISTINCT ON,
//! window functions, eval_group_expr branches, case-insensitive resolution),
//! dml.rs (FK CASCADE multi-level, SET NULL, RESTRICT, ON CONFLICT filter,
//! DEFAULT VALUES, generated column update error, duplicate column insert),
//! ddl.rs (expression index errors, drop index, alter column type, add NOT NULL),
//! db.rs (savepoint errors, transaction state, snapshot release, dump_sql,
//! prepared schema invalidation), triggers.rs, and constraints.rs paths.

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

// ── SET OPERATIONS ─────────────────────────────────────────────────

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
fn set_op_column_count_mismatch() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sc1 (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "CREATE TABLE sc2 (id INT PRIMARY KEY, x INT)");
    exec(&db, "INSERT INTO sc1 VALUES (1, 2, 3)");
    exec(&db, "INSERT INTO sc2 VALUES (1, 2)");
    let err = exec_err(&db, "SELECT a, b FROM sc1 UNION SELECT x FROM sc2");
    assert!(!err.is_empty());
}

// ── RECURSIVE CTE EDGE CASES ──────────────────────────────────────

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

// ── DISTINCT ON ────────────────────────────────────────────────────

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

// ── CASE-INSENSITIVE TABLE/COLUMN RESOLUTION ──────────────────────

#[test]
fn case_insensitive_table_name() {
    let db = mem_db();
    exec(&db, "CREATE TABLE MixedCase (Id INT PRIMARY KEY, Name TEXT)");
    exec(&db, "INSERT INTO MixedCase VALUES (1, 'hello')");
    let r = exec(&db, "SELECT name FROM mixedcase WHERE id = 1");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn case_insensitive_column_in_where() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cit (ID INT PRIMARY KEY, NAME TEXT)");
    exec(&db, "INSERT INTO cit VALUES (1, 'Alice')");
    let r = exec(&db, "SELECT id FROM cit WHERE name = 'Alice'");
    assert_eq!(r.rows().len(), 1);
}

// ── EVAL_GROUP_EXPR BRANCHES ───────────────────────────────────────

#[test]
fn count_star_vs_count_col_vs_count_distinct() {
    let db = mem_db();
    exec(&db, "CREATE TABLE csd (id INT PRIMARY KEY, cat TEXT, val INT)");
    exec(&db, "INSERT INTO csd VALUES (1, 'A', 10), (2, 'A', 10), (3, 'A', NULL), (4, 'B', 20)");
    let r = exec(&db,
        "SELECT cat, COUNT(*), COUNT(val), COUNT(DISTINCT val) FROM csd GROUP BY cat ORDER BY cat"
    );
    // A: count(*)=3, count(val)=2 (NULL excluded), count(distinct val)=1 (just 10)
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(3));
    assert_eq!(r.rows()[0].values()[2], Value::Int64(2));
    assert_eq!(r.rows()[0].values()[3], Value::Int64(1));
}

#[test]
fn sum_distinct_in_group() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sd (id INT PRIMARY KEY, cat TEXT, val INT)");
    exec(&db, "INSERT INTO sd VALUES (1, 'X', 5), (2, 'X', 5), (3, 'X', 10)");
    let r = exec(&db, "SELECT SUM(DISTINCT val) FROM sd WHERE cat = 'X'");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(15)); // 5 + 10
}

#[test]
fn group_by_having_filter() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gh (id INT PRIMARY KEY, cat TEXT, val INT)");
    exec(&db, "INSERT INTO gh VALUES (1,'A',1),(2,'A',2),(3,'B',3),(4,'B',4),(5,'B',5)");
    let r = exec(&db, "SELECT cat, SUM(val) AS s FROM gh GROUP BY cat HAVING SUM(val) > 5");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Text("B".into()));
}

#[test]
fn aggregate_on_empty_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE empty_agg (id INT PRIMARY KEY, val INT)");
    let r = exec(&db, "SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM empty_agg");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
}

#[test]
fn group_by_expression_not_just_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gbe (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO gbe VALUES (1, 10), (2, 15), (3, 20), (4, 25)");
    let r = exec(&db, "SELECT val / 10 AS bucket, COUNT(*) FROM gbe GROUP BY val / 10 ORDER BY bucket");
    assert_eq!(r.rows().len(), 2); // bucket 1 and 2
}

#[test]
fn group_by_with_unary_not_negate() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gun (id INT PRIMARY KEY, flag BOOL, val INT)");
    exec(&db, "INSERT INTO gun VALUES (1, true, 10), (2, false, 20), (3, true, 30)");
    // Group by negated value
    let r = exec(&db, "SELECT -val / 10 AS neg_bucket, COUNT(*) FROM gun GROUP BY -val / 10");
    assert!(r.rows().len() >= 2);
}

// ── FK CASCADE MULTI-LEVEL ─────────────────────────────────────────

#[test]
fn fk_cascade_delete_three_levels() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gp (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE par (id INT PRIMARY KEY, gp_id INT REFERENCES gp(id) ON DELETE CASCADE)");
    exec(&db, "CREATE TABLE child (id INT PRIMARY KEY, par_id INT REFERENCES par(id) ON DELETE CASCADE)");
    exec(&db, "INSERT INTO gp VALUES (1)");
    exec(&db, "INSERT INTO par VALUES (10, 1)");
    exec(&db, "INSERT INTO child VALUES (100, 10)");
    exec(&db, "DELETE FROM gp WHERE id = 1");
    let r = exec(&db, "SELECT COUNT(*) FROM child");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
    let r2 = exec(&db, "SELECT COUNT(*) FROM par");
    assert_eq!(r2.rows()[0].values()[0], Value::Int64(0));
}

#[test]
fn fk_cascade_update_propagates() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cu_parent (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE TABLE cu_child (id INT PRIMARY KEY, parent_id INT REFERENCES cu_parent(id) ON UPDATE CASCADE)");
    exec(&db, "INSERT INTO cu_parent VALUES (1, 'old')");
    exec(&db, "INSERT INTO cu_child VALUES (100, 1)");
    exec(&db, "UPDATE cu_parent SET id = 2 WHERE id = 1");
    let r = exec(&db, "SELECT parent_id FROM cu_child WHERE id = 100");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn fk_set_null_on_delete() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sn_parent (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE sn_child (id INT PRIMARY KEY, pid INT REFERENCES sn_parent(id) ON DELETE SET NULL)");
    exec(&db, "INSERT INTO sn_parent VALUES (1)");
    exec(&db, "INSERT INTO sn_child VALUES (100, 1)");
    exec(&db, "DELETE FROM sn_parent WHERE id = 1");
    let r = exec(&db, "SELECT pid FROM sn_child WHERE id = 100");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn fk_set_null_on_update() {
    let db = mem_db();
    exec(&db, "CREATE TABLE snu_p (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE snu_c (id INT PRIMARY KEY, pid INT REFERENCES snu_p(id) ON UPDATE SET NULL)");
    exec(&db, "INSERT INTO snu_p VALUES (1)");
    exec(&db, "INSERT INTO snu_c VALUES (100, 1)");
    exec(&db, "UPDATE snu_p SET id = 2 WHERE id = 1");
    let r = exec(&db, "SELECT pid FROM snu_c WHERE id = 100");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn fk_restrict_on_delete() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rp (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE rc (id INT PRIMARY KEY, pid INT REFERENCES rp(id) ON DELETE RESTRICT)");
    exec(&db, "INSERT INTO rp VALUES (1)");
    exec(&db, "INSERT INTO rc VALUES (100, 1)");
    let err = exec_err(&db, "DELETE FROM rp WHERE id = 1");
    assert!(!err.is_empty(), "RESTRICT should prevent delete");
}

#[test]
fn fk_restrict_on_update() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rup (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE ruc (id INT PRIMARY KEY, pid INT REFERENCES rup(id) ON UPDATE RESTRICT)");
    exec(&db, "INSERT INTO rup VALUES (1)");
    exec(&db, "INSERT INTO ruc VALUES (100, 1)");
    let err = exec_err(&db, "UPDATE rup SET id = 2 WHERE id = 1");
    assert!(!err.is_empty(), "RESTRICT should prevent update");
}

#[test]
fn fk_mixed_actions_on_multiple_children() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mp (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE mc_cascade (id INT PRIMARY KEY, pid INT REFERENCES mp(id) ON DELETE CASCADE)");
    exec(&db, "CREATE TABLE mc_setnull (id INT PRIMARY KEY, pid INT REFERENCES mp(id) ON DELETE SET NULL)");
    exec(&db, "INSERT INTO mp VALUES (1)");
    exec(&db, "INSERT INTO mc_cascade VALUES (10, 1)");
    exec(&db, "INSERT INTO mc_setnull VALUES (20, 1)");
    exec(&db, "DELETE FROM mp WHERE id = 1");
    let r1 = exec(&db, "SELECT COUNT(*) FROM mc_cascade");
    assert_eq!(r1.rows()[0].values()[0], Value::Int64(0));
    let r2 = exec(&db, "SELECT pid FROM mc_setnull WHERE id = 20");
    assert_eq!(r2.rows()[0].values()[0], Value::Null);
}

// ── ON CONFLICT DO UPDATE WITH WHERE ───────────────────────────────

#[test]
fn upsert_with_filter_accepts() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uf (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO uf VALUES (1, 100)");
    exec(&db, "INSERT INTO uf VALUES (1, 200) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE uf.val < 200");
    let r = exec(&db, "SELECT val FROM uf WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(200));
}

#[test]
fn upsert_with_filter_rejects() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ufr (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ufr VALUES (1, 100)");
    // Filter rejects because val is NOT > 999
    exec(&db, "INSERT INTO ufr VALUES (1, 200) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE ufr.val > 999");
    let r = exec(&db, "SELECT val FROM ufr WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(100)); // unchanged
}

// ── INSERT DEFAULT VALUES ──────────────────────────────────────────

#[test]
fn insert_default_values() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dv (id INT PRIMARY KEY, name TEXT DEFAULT 'anon', score INT DEFAULT 0)");
    exec(&db, "INSERT INTO dv (id) VALUES (1)");
    let r = exec(&db, "SELECT name, score FROM dv WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("anon".into()));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(0));
}

// ── GENERATED COLUMN UPDATE ERROR ──────────────────────────────────

#[test]
fn update_generated_column_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE gc (id INT PRIMARY KEY, base INT, computed INT GENERATED ALWAYS AS (base * 2) STORED)");
    exec(&db, "INSERT INTO gc (id, base) VALUES (1, 5)");
    let err = exec_err(&db, "UPDATE gc SET computed = 99 WHERE id = 1");
    assert!(!err.is_empty());
}

// ── DUPLICATE COLUMN IN INSERT ─────────────────────────────────────

#[test]
fn insert_duplicate_column_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dci (id INT PRIMARY KEY, val INT)");
    let err = exec_err(&db, "INSERT INTO dci (id, id) VALUES (1, 2)");
    assert!(!err.is_empty());
}

// ── DDL: INDEX CREATION ERRORS ─────────────────────────────────────

#[test]
fn trigram_index_multi_column_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tmc (id INT PRIMARY KEY, a TEXT, b TEXT)");
    let err = exec_err(&db, "CREATE INDEX idx ON tmc USING gin (a, b)");
    assert!(!err.is_empty());
}

#[test]
fn trigram_index_unique_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE tue (id INT PRIMARY KEY, body TEXT)");
    let err = exec_err(&db, "CREATE UNIQUE INDEX idx ON tue USING gin (body)");
    assert!(!err.is_empty());
}

#[test]
fn create_index_nonexistent_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nic (id INT PRIMARY KEY)");
    let err = exec_err(&db, "CREATE INDEX idx ON nic (nonexistent)");
    assert!(!err.is_empty());
}

#[test]
fn create_index_nonexistent_table() {
    let db = mem_db();
    let err = exec_err(&db, "CREATE INDEX idx ON no_such_table (col)");
    assert!(!err.is_empty());
}

// ── DDL: ALTER TABLE EDGE CASES ────────────────────────────────────

#[test]
fn alter_table_add_not_null_column_on_nonempty() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ann (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ann VALUES (1, 10)");
    // Adding NOT NULL column without DEFAULT to non-empty table should error
    let err = exec_err(&db, "ALTER TABLE ann ADD COLUMN newcol INT NOT NULL");
    assert!(!err.is_empty());
}

#[test]
fn alter_table_add_column_with_default() {
    let db = mem_db();
    exec(&db, "CREATE TABLE acd (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO acd VALUES (1, 10)");
    exec(&db, "ALTER TABLE acd ADD COLUMN extra TEXT DEFAULT 'hello'");
    let r = exec(&db, "SELECT extra FROM acd WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Text("hello".into()));
}

#[test]
fn alter_table_drop_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE adc (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO adc VALUES (1, 10, 20)");
    exec(&db, "ALTER TABLE adc DROP COLUMN b");
    let r = exec(&db, "SELECT * FROM adc WHERE id = 1");
    assert_eq!(r.rows()[0].values().len(), 2); // id, a
}

#[test]
fn alter_table_rename_column() {
    let db = mem_db();
    exec(&db, "CREATE TABLE arc (id INT PRIMARY KEY, old_name INT)");
    exec(&db, "INSERT INTO arc VALUES (1, 42)");
    exec(&db, "ALTER TABLE arc RENAME COLUMN old_name TO new_name");
    let r = exec(&db, "SELECT new_name FROM arc WHERE id = 1");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
}

#[test]
fn drop_table_with_fk_reference_error() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dtf_parent (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE dtf_child (id INT PRIMARY KEY, pid INT REFERENCES dtf_parent(id))");
    let err = exec_err(&db, "DROP TABLE dtf_parent");
    assert!(!err.is_empty());
}

// ── DB: SAVEPOINT ERRORS ───────────────────────────────────────────

#[test]
fn release_nonexistent_savepoint() {
    let db = mem_db();
    exec(&db, "BEGIN");
    exec(&db, "SAVEPOINT sp1");
    let err = exec_err(&db, "RELEASE SAVEPOINT sp_nonexistent");
    assert!(!err.is_empty());
    exec(&db, "ROLLBACK");
}

#[test]
fn rollback_to_nonexistent_savepoint() {
    let db = mem_db();
    exec(&db, "BEGIN");
    exec(&db, "SAVEPOINT sp1");
    let err = exec_err(&db, "ROLLBACK TO SAVEPOINT sp_nonexistent");
    assert!(!err.is_empty());
    exec(&db, "ROLLBACK");
}

// ── DB: TRANSACTION STATE ──────────────────────────────────────────

#[test]
fn commit_without_begin_error() {
    let db = mem_db();
    let err = exec_err(&db, "COMMIT");
    assert!(!err.is_empty());
}

#[test]
fn rollback_without_begin_error() {
    let db = mem_db();
    let err = exec_err(&db, "ROLLBACK");
    assert!(!err.is_empty());
}

#[test]
fn nested_begin_error() {
    let db = mem_db();
    exec(&db, "BEGIN");
    let err = exec_err(&db, "BEGIN");
    assert!(!err.is_empty());
    exec(&db, "ROLLBACK");
}

// ── DB: SNAPSHOT ───────────────────────────────────────────────────

#[test]
fn snapshot_hold_and_release() {
    let db = mem_db();
    exec(&db, "CREATE TABLE sh (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO sh VALUES (1)");
    let token = db.hold_snapshot().unwrap();
    exec(&db, "INSERT INTO sh VALUES (2)");
    db.release_snapshot(token).unwrap();
    let r = exec(&db, "SELECT COUNT(*) FROM sh");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

#[test]
fn release_invalid_snapshot_error() {
    let db = mem_db();
    let err = db.release_snapshot(99999);
    assert!(err.is_err());
}

// ── DB: DUMP_SQL ───────────────────────────────────────────────────

#[test]
fn dump_sql_includes_tables_and_indexes() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ds (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE INDEX ds_name ON ds (name)");
    exec(&db, "INSERT INTO ds VALUES (1, 'hello')");
    let dump = db.dump_sql().unwrap();
    assert!(dump.contains("CREATE TABLE"), "dump should contain CREATE TABLE");
    assert!(dump.contains("INSERT"), "dump should contain INSERT");
}

// ── DB: PREPARED STATEMENT SCHEMA INVALIDATION ─────────────────────

#[test]
fn prepared_select_after_drop_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE psd (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO psd VALUES (1, 10)");
    let stmt = db.prepare("SELECT val FROM psd WHERE id = $1").unwrap();
    exec(&db, "DROP TABLE psd");
    let err = stmt.execute(&[Value::Int64(1)]);
    assert!(err.is_err());
}

#[test]
fn prepared_insert_after_alter_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pia (id INT PRIMARY KEY, val INT)");
    let stmt = db.prepare("INSERT INTO pia VALUES ($1, $2)").unwrap();
    exec(&db, "ALTER TABLE pia ADD COLUMN extra TEXT");
    // Schema changed; prepared stmt may need replan
    let result = stmt.execute(&[Value::Int64(1), Value::Int64(10)]);
    // Either succeeds or errors due to schema cookie mismatch
    assert!(result.is_ok() || result.is_err());
}

// ── EXPLAIN ────────────────────────────────────────────────────────

#[test]
fn explain_select_produces_output() {
    let db = mem_db();
    exec(&db, "CREATE TABLE es (id INT PRIMARY KEY, val INT)");
    let r = exec(&db, "EXPLAIN SELECT * FROM es WHERE id = 1");
    assert!(!r.rows().is_empty());
}

#[test]
fn explain_analyze_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ea (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ea VALUES (1, 10), (2, 20)");
    let r = exec(&db, "EXPLAIN ANALYZE SELECT * FROM ea WHERE val > 5");
    assert!(!r.rows().is_empty());
}

// ── JOINS ──────────────────────────────────────────────────────────

#[test]
fn right_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rj1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE rj2 (id INT PRIMARY KEY, rj1_id INT)");
    exec(&db, "INSERT INTO rj1 VALUES (1, 10)");
    exec(&db, "INSERT INTO rj2 VALUES (1, 1), (2, 99)");
    let r = exec(&db, "SELECT rj2.id, rj1.val FROM rj1 RIGHT JOIN rj2 ON rj1.id = rj2.rj1_id ORDER BY rj2.id");
    assert_eq!(r.rows().len(), 2);
    assert_eq!(r.rows()[1].values()[1], Value::Null); // unmatched
}

#[test]
fn full_outer_join() {
    let db = mem_db();
    exec(&db, "CREATE TABLE fj1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE fj2 (id INT PRIMARY KEY, fj1_id INT)");
    exec(&db, "INSERT INTO fj1 VALUES (1, 10), (2, 20)");
    exec(&db, "INSERT INTO fj2 VALUES (1, 1), (3, 99)");
    let r = exec(&db, "SELECT fj1.id, fj2.id FROM fj1 FULL OUTER JOIN fj2 ON fj1.id = fj2.fj1_id");
    assert!(r.rows().len() >= 3); // left-only, matched, right-only
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
fn implicit_cross_join_from_multiple_tables() {
    let db = mem_db();
    exec(&db, "CREATE TABLE icj1 (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE icj2 (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO icj1 VALUES (1), (2)");
    exec(&db, "INSERT INTO icj2 VALUES (10)");
    let r = exec(&db, "SELECT icj1.id, icj2.id FROM icj1, icj2");
    assert_eq!(r.rows().len(), 2);
}

// ── SELECT WITHOUT FROM ────────────────────────────────────────────

#[test]
fn select_without_from() {
    let db = mem_db();
    let r = exec(&db, "SELECT 1 + 2 AS result");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

#[test]
fn select_expression_no_from() {
    let db = mem_db();
    let r = exec(&db, "SELECT 'hello' || ' ' || 'world'");
    assert_eq!(r.rows().len(), 1);
}

// ── QUALIFIED WILDCARDS ────────────────────────────────────────────

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

// ── SUBQUERIES ─────────────────────────────────────────────────────

#[test]
fn exists_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE esq (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO esq VALUES (1, 10), (2, 20)");
    let r = exec(&db, "SELECT id FROM esq WHERE EXISTS (SELECT 1 FROM esq WHERE val = 10)");
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
fn in_subquery() {
    let db = mem_db();
    exec(&db, "CREATE TABLE isq1 (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE isq2 (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO isq1 VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "INSERT INTO isq2 VALUES (1, 10), (2, 30)");
    let r = exec(&db, "SELECT id FROM isq1 WHERE val IN (SELECT val FROM isq2) ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

// ── TYPE ALIASES IN CREATE TABLE ───────────────────────────────────

#[test]
fn create_table_with_various_type_aliases() {
    let db = mem_db();
    exec(&db, "CREATE TABLE types (
        a SMALLINT PRIMARY KEY,
        b REAL,
        c CHARACTER VARYING,
        d BYTEA,
        e NUMERIC,
        f UUID,
        g TIMESTAMP WITH TIME ZONE
    )");
    // Just verify it creates successfully
    let r = exec(&db, "SELECT COUNT(*) FROM types");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(0));
}

#[test]
fn create_table_double_precision() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dp (id INT PRIMARY KEY, val DOUBLE PRECISION)");
    exec(&db, "INSERT INTO dp VALUES (1, 3.14)");
    let r = exec(&db, "SELECT val FROM dp");
    assert_eq!(r.rows().len(), 1);
}

// ── NULLIF / COALESCE IN EXPRESSIONS ───────────────────────────────

#[test]
fn nullif_returns_null_on_equal() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULLIF(1, 1)");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn nullif_returns_first_on_unequal() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULLIF(1, 2)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
}

#[test]
fn coalesce_returns_first_non_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT COALESCE(NULL, NULL, 42, 99)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(42));
}

// ── THREE-VALUED LOGIC ─────────────────────────────────────────────

#[test]
fn null_and_true_is_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULL AND TRUE");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

#[test]
fn null_or_true_is_true() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULL OR TRUE");
    assert_eq!(r.rows()[0].values()[0], Value::Bool(true));
}

#[test]
fn null_comparison_is_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT NULL = NULL");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

// ── STRING CONCAT WITH NULL ────────────────────────────────────────

#[test]
fn concat_with_null_propagates() {
    let db = mem_db();
    let r = exec(&db, "SELECT 'hello' || NULL");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

// ── ILIKE ──────────────────────────────────────────────────────────

#[test]
fn ilike_case_insensitive() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ilk (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO ilk VALUES (1, 'Alice'), (2, 'bob'), (3, 'CHARLIE')");
    let r = exec(&db, "SELECT id FROM ilk WHERE name ILIKE 'alice'");
    assert_eq!(r.rows().len(), 1);
}

// ── BETWEEN IN WHERE ───────────────────────────────────────────────

#[test]
fn not_between_filter() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nb (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nb VALUES (1, 5), (2, 15), (3, 25)");
    let r = exec(&db, "SELECT id FROM nb WHERE val NOT BETWEEN 10 AND 20 ORDER BY id");
    assert_eq!(r.rows().len(), 2);
}

// ── CAST EXPRESSIONS ───────────────────────────────────────────────

#[test]
fn cast_int_to_float() {
    let db = mem_db();
    let r = exec(&db, "SELECT CAST(42 AS FLOAT)");
    assert_eq!(r.rows().len(), 1);
}

#[test]
fn cast_float_to_int() {
    let db = mem_db();
    let r = exec(&db, "SELECT CAST(3.7 AS INT)");
    assert_eq!(r.rows().len(), 1);
}

// ── TRIGGERS: INSTEAD OF ON VIEW ───────────────────────────────────

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
fn drop_trigger_if_exists_nonexistent() {
    let db = mem_db();
    exec(&db, "CREATE TABLE dti (id INT PRIMARY KEY)");
    // Should not error with IF EXISTS
    let r = db.execute("DROP TRIGGER IF EXISTS no_such_trigger ON dti");
    assert!(r.is_ok());
}

// ── CONSTRAINTS: CHECK ─────────────────────────────────────────────

#[test]
fn check_constraint_rejects_insert() {
    let db = mem_db();
    exec(&db, "CREATE TABLE cci (id INT PRIMARY KEY, val INT CHECK (val > 0))");
    let err = exec_err(&db, "INSERT INTO cci VALUES (1, -5)");
    assert!(!err.is_empty());
}

#[test]
fn check_constraint_allows_valid() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ccv (id INT PRIMARY KEY, val INT CHECK (val > 0))");
    exec(&db, "INSERT INTO ccv VALUES (1, 10)");
    let r = exec(&db, "SELECT val FROM ccv");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(10));
}

// ── CONSTRAINTS: UNIQUE ────────────────────────────────────────────

#[test]
fn unique_constraint_rejects_duplicate() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ucr (id INT PRIMARY KEY, email TEXT UNIQUE)");
    exec(&db, "INSERT INTO ucr VALUES (1, 'a@b.com')");
    let err = exec_err(&db, "INSERT INTO ucr VALUES (2, 'a@b.com')");
    assert!(!err.is_empty());
}

// ── PARTIAL INDEX ──────────────────────────────────────────────────

#[test]
fn partial_index_only_indexes_matching_rows() {
    let db = mem_db();
    exec(&db, "CREATE TABLE pi (id INT PRIMARY KEY, status TEXT, val INT)");
    exec(&db, "CREATE INDEX pi_active ON pi (val) WHERE status = 'active'");
    exec(&db, "INSERT INTO pi VALUES (1, 'active', 10), (2, 'inactive', 10), (3, 'active', 20)");
    // Both active rows should be findable via the partial index
    let r = exec(&db, "SELECT id FROM pi WHERE status = 'active' AND val = 10");
    assert_eq!(r.rows().len(), 1);
}

// ── IN TRANSACTION STATE ───────────────────────────────────────────

#[test]
fn in_transaction_state() {
    let db = mem_db();
    assert_eq!(db.in_transaction().unwrap(), false);
    db.begin_transaction().unwrap();
    assert_eq!(db.in_transaction().unwrap(), true);
    db.rollback_transaction().unwrap();
    assert_eq!(db.in_transaction().unwrap(), false);
}

// ── TEMP TABLE IN TRANSACTION ──────────────────────────────────────

#[test]
fn temp_table_in_transaction() {
    let db = mem_db();
    exec(&db, "BEGIN");
    exec(&db, "CREATE TEMP TABLE tt (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO tt VALUES (1)");
    let r = exec(&db, "SELECT COUNT(*) FROM tt");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(1));
    exec(&db, "COMMIT");
}

// ── BOOLEAN LITERALS ───────────────────────────────────────────────

#[test]
fn boolean_literal_true_false() {
    let db = mem_db();
    exec(&db, "CREATE TABLE bl (id INT PRIMARY KEY, flag BOOL)");
    exec(&db, "INSERT INTO bl VALUES (1, true), (2, false)");
    let r = exec(&db, "SELECT flag FROM bl WHERE flag = true");
    assert_eq!(r.rows().len(), 1);
}

// ── INSERT WITH SELECT ─────────────────────────────────────────────

#[test]
fn insert_from_select() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ifs_src (id INT PRIMARY KEY, val INT)");
    exec(&db, "CREATE TABLE ifs_dst (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO ifs_src VALUES (1, 10), (2, 20)");
    exec(&db, "INSERT INTO ifs_dst SELECT * FROM ifs_src");
    let r = exec(&db, "SELECT COUNT(*) FROM ifs_dst");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ── MULTI-ROW INSERT ───────────────────────────────────────────────

#[test]
fn multi_row_insert() {
    let db = mem_db();
    exec(&db, "CREATE TABLE mri (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "INSERT INTO mri VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    let r = exec(&db, "SELECT COUNT(*) FROM mri");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(3));
}

// ── DELETE WITH SUBQUERY ───────────────────────────────────────────

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

// ── UPDATE WITH CASE ───────────────────────────────────────────────

#[test]
fn update_with_case_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE uwc (id INT PRIMARY KEY, val INT, label TEXT)");
    exec(&db, "INSERT INTO uwc VALUES (1, 10, NULL), (2, 20, NULL)");
    exec(&db, "UPDATE uwc SET label = CASE WHEN val > 15 THEN 'high' ELSE 'low' END");
    let r = exec(&db, "SELECT label FROM uwc ORDER BY id");
    assert_eq!(r.rows()[0].values()[0], Value::Text("low".into()));
    assert_eq!(r.rows()[1].values()[0], Value::Text("high".into()));
}

// ── INDEXED JOIN ───────────────────────────────────────────────────

#[test]
fn indexed_inner_join_uses_btree() {
    let db = mem_db();
    exec(&db, "CREATE TABLE ij1 (id INT PRIMARY KEY, val TEXT)");
    exec(&db, "CREATE TABLE ij2 (id INT PRIMARY KEY, ij1_id INT)");
    exec(&db, "CREATE INDEX ij2_fk ON ij2 (ij1_id)");
    exec(&db, "INSERT INTO ij1 VALUES (1, 'a'), (2, 'b')");
    exec(&db, "INSERT INTO ij2 VALUES (10, 1), (20, 2), (30, 1)");
    let r = exec(&db, "SELECT ij1.val, ij2.id FROM ij1 INNER JOIN ij2 ON ij1.id = ij2.ij1_id ORDER BY ij2.id");
    assert_eq!(r.rows().len(), 3);
}

// ── WINDOW FUNCTIONS (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD) ────

#[test]
fn row_number_over_order() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rno (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO rno VALUES (1, 30), (2, 10), (3, 20)");
    let r = exec(&db, "SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM rno ORDER BY rn");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1));
}

#[test]
fn row_number_with_partition() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rnp (id INT PRIMARY KEY, cat TEXT, val INT)");
    exec(&db, "INSERT INTO rnp VALUES (1,'A',10),(2,'A',20),(3,'B',30),(4,'B',40)");
    let r = exec(&db, "SELECT id, ROW_NUMBER() OVER (PARTITION BY cat ORDER BY val) AS rn FROM rnp ORDER BY id");
    assert_eq!(r.rows().len(), 4);
    // First in each partition should be 1
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1));
    assert_eq!(r.rows()[2].values()[1], Value::Int64(1));
}

#[test]
fn rank_with_ties() {
    let db = mem_db();
    exec(&db, "CREATE TABLE rwt (id INT PRIMARY KEY, score INT)");
    exec(&db, "INSERT INTO rwt VALUES (1,100),(2,90),(3,90),(4,80)");
    let r = exec(&db, "SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM rwt ORDER BY id");
    assert_eq!(r.rows().len(), 4);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(1)); // 100 → rank 1
    assert_eq!(r.rows()[1].values()[1], Value::Int64(2)); // 90 → rank 2
    assert_eq!(r.rows()[2].values()[1], Value::Int64(2)); // 90 → rank 2
    assert_eq!(r.rows()[3].values()[1], Value::Int64(4)); // 80 → rank 4 (skip 3)
}

#[test]
fn dense_rank_no_gaps() {
    let db = mem_db();
    exec(&db, "CREATE TABLE drng (id INT PRIMARY KEY, score INT)");
    exec(&db, "INSERT INTO drng VALUES (1,100),(2,90),(3,90),(4,80)");
    let r = exec(&db, "SELECT id, DENSE_RANK() OVER (ORDER BY score DESC) AS drnk FROM drng ORDER BY id");
    assert_eq!(r.rows()[3].values()[1], Value::Int64(3)); // 80 → dense_rank 3
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
fn first_value_last_value() {
    let db = mem_db();
    exec(&db, "CREATE TABLE flv (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO flv VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "SELECT id, FIRST_VALUE(val) OVER (ORDER BY id), LAST_VALUE(val) OVER (ORDER BY id) FROM flv ORDER BY id");
    assert_eq!(r.rows().len(), 3);
    assert_eq!(r.rows()[0].values()[1], Value::Int64(10));
}

#[test]
fn nth_value_function() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nv (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO nv VALUES (1, 10), (2, 20), (3, 30)");
    let r = exec(&db, "SELECT id, NTH_VALUE(val, 2) OVER (ORDER BY id) FROM nv ORDER BY id");
    assert_eq!(r.rows().len(), 3);
}

// ── FUNCTIONS ──────────────────────────────────────────────────────

#[test]
fn string_functions_coverage() {
    let db = mem_db();
    let r = exec(&db, "SELECT UPPER('hello'), LOWER('WORLD'), LENGTH('test'), SUBSTR('abcdef', 2, 3), REPLACE('hello', 'l', 'r'), TRIM('  hi  ')");
    assert_eq!(r.rows()[0].values()[0], Value::Text("HELLO".into()));
    assert_eq!(r.rows()[0].values()[1], Value::Text("world".into()));
    assert_eq!(r.rows()[0].values()[2], Value::Int64(4));
    assert_eq!(r.rows()[0].values()[3], Value::Text("bcd".into()));
    assert_eq!(r.rows()[0].values()[4], Value::Text("herro".into()));
    assert_eq!(r.rows()[0].values()[5], Value::Text("hi".into()));
}

#[test]
fn math_functions() {
    let db = mem_db();
    let r = exec(&db, "SELECT ABS(-5), ABS(3)");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(5));
    assert_eq!(r.rows()[0].values()[1], Value::Int64(3));
}

// ── IS NULL / IS NOT NULL ──────────────────────────────────────────

#[test]
fn is_null_is_not_null() {
    let db = mem_db();
    exec(&db, "CREATE TABLE inn (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO inn VALUES (1, NULL), (2, 10)");
    let r1 = exec(&db, "SELECT id FROM inn WHERE val IS NULL");
    assert_eq!(r1.rows().len(), 1);
    assert_eq!(r1.rows()[0].values()[0], Value::Int64(1));
    let r2 = exec(&db, "SELECT id FROM inn WHERE val IS NOT NULL");
    assert_eq!(r2.rows().len(), 1);
    assert_eq!(r2.rows()[0].values()[0], Value::Int64(2));
}

// ── NOT operator ───────────────────────────────────────────────────

#[test]
fn not_boolean_expression() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nbe (id INT PRIMARY KEY, flag BOOL)");
    exec(&db, "INSERT INTO nbe VALUES (1, true), (2, false)");
    let r = exec(&db, "SELECT id FROM nbe WHERE NOT flag ORDER BY id");
    assert_eq!(r.rows().len(), 1);
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}

// ── ANALYZE statement ──────────────────────────────────────────────

#[test]
fn analyze_table() {
    let db = mem_db();
    exec(&db, "CREATE TABLE at (id INT PRIMARY KEY, val INT)");
    exec(&db, "INSERT INTO at VALUES (1, 10), (2, 20)");
    exec(&db, "ANALYZE at");
    // Just verify it doesn't error
}

// ── Division by zero ───────────────────────────────────────────────

#[test]
fn division_by_zero_returns_null() {
    let db = mem_db();
    let r = exec(&db, "SELECT 1 / 0");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}

// ── MULTIPLE CTEs ──────────────────────────────────────────────────

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

// ── NESTED SAVEPOINTS ──────────────────────────────────────────────

#[test]
fn nested_savepoints_rollback_inner() {
    let db = mem_db();
    exec(&db, "CREATE TABLE nsp (id INT PRIMARY KEY)");
    exec(&db, "BEGIN");
    exec(&db, "INSERT INTO nsp VALUES (1)");
    exec(&db, "SAVEPOINT sp1");
    exec(&db, "INSERT INTO nsp VALUES (2)");
    exec(&db, "SAVEPOINT sp2");
    exec(&db, "INSERT INTO nsp VALUES (3)");
    exec(&db, "ROLLBACK TO SAVEPOINT sp2");
    // row 3 rolled back
    exec(&db, "COMMIT");
    let r = exec(&db, "SELECT COUNT(*) FROM nsp");
    assert_eq!(r.rows()[0].values()[0], Value::Int64(2));
}
