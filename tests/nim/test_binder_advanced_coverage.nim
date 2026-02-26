## test_binder_advanced_coverage.nim
## Targets binder.nim and sql.nim uncovered paths:
##   - rewriteExprForViewRef for ekInList (L875-881)
##   - rewriteExprForViewRef for ekWindowRowNumber (L916-922)
##   - subqueryTableMeta with aliased FROM (L541-547)
##   - subqueryTableMeta with JOIN+alias in subquery (L553-559)
##   - subqueryTableMeta with ekFunc type inference (L511-522)
##   - directSelectDependencies via join subqueries (L689-697)
##   - ON CONFLICT with IN expression (qualifyInsertConflictExpr)
##   - hasParamsInExpr for ekInList/ekWindowRowNumber (L318-333)
##   - exprToCanonicalSql for ekInList (L1464-1467) - via partial index
##   - Various CTE patterns

import unittest, os, strutils, engine

proc freshDb(name: string): Db =
  let p = getTempDir() / name & ".ddb"
  removeFile(p)
  removeFile(p & "-wal")
  openDb(p).value

proc col0(rows: seq[string]): string =
  if rows.len == 0: return ""
  rows[0].split("|")[0]

# ─────────────────────────────────────────────────────────────────────────────
suite "VIEW with ekInList expression (rewriteExprForViewRef)":
# ─────────────────────────────────────────────────────────────────────────────
  test "CREATE VIEW with IN predicate and query it":
    let db = freshDb("view_in_pred")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d')").ok
    check execSql(db, "CREATE VIEW v_filtered AS SELECT id, v FROM t WHERE id IN (1, 3)").ok
    let r = execSql(db, "SELECT id FROM v_filtered ORDER BY id")
    check r.ok
    check r.value == @["1", "3"]
    discard closeDb(db)

  test "VIEW IN predicate with more values":
    let db = freshDb("view_in_more")
    check execSql(db, "CREATE TABLE products (id INT PRIMARY KEY, cat TEXT, price INT)").ok
    check execSql(db, "INSERT INTO products VALUES (1,'A',10), (2,'B',20), (3,'A',30), (4,'C',40), (5,'B',50)").ok
    check execSql(db, "CREATE VIEW ab_products AS SELECT id, cat, price FROM products WHERE cat IN ('A', 'B')").ok
    let r = execSql(db, "SELECT id FROM ab_products ORDER BY id")
    check r.ok
    check r.value == @["1", "2", "3", "5"]
    discard closeDb(db)

  test "VIEW IN predicate with integer list":
    let db = freshDb("view_in_int")
    check execSql(db, "CREATE TABLE data (id INT PRIMARY KEY, score INT)").ok
    check execSql(db, "INSERT INTO data VALUES (1,10), (2,20), (3,30), (4,40), (5,50)").ok
    check execSql(db, "CREATE VIEW selected AS SELECT id, score FROM data WHERE score IN (10, 30, 50)").ok
    let r = execSql(db, "SELECT COUNT(*) FROM selected")
    check r.ok
    check col0(r.value) == "3"
    discard closeDb(db)

  test "VIEW IN with additional WHERE filter":
    let db = freshDb("view_in_where")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, active INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,1,1),(2,2,0),(3,1,1),(4,3,1),(5,2,1)").ok
    check execSql(db, "CREATE VIEW active_12 AS SELECT id, v FROM t WHERE v IN (1, 2) AND active = 1").ok
    let r = execSql(db, "SELECT id FROM active_12 ORDER BY id")
    check r.ok
    check r.value == @["1", "3", "5"]
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "VIEW with window function (rewriteExprForViewRef ekWindowRowNumber)":
# ─────────────────────────────────────────────────────────────────────────────
  test "VIEW with ROW_NUMBER() OVER ORDER BY":
    let db = freshDb("view_rn")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, score INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,30), (2,10), (3,20)").ok
    check execSql(db, "CREATE VIEW ranked AS SELECT id, score, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM t").ok
    let r = execSql(db, "SELECT id, rn FROM ranked ORDER BY id")
    check r.ok
    check r.value.len == 3
    discard closeDb(db)

  test "VIEW with ROW_NUMBER() OVER PARTITION BY":
    let db = freshDb("view_rn_part")
    check execSql(db, "CREATE TABLE sales (id INT PRIMARY KEY, dept TEXT, amount INT)").ok
    check execSql(db, "INSERT INTO sales VALUES (1,'A',100),(2,'A',200),(3,'B',150),(4,'B',50)").ok
    check execSql(db, "CREATE VIEW dept_ranked AS SELECT id, dept, amount, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) AS rnk FROM sales").ok
    let r = execSql(db, "SELECT COUNT(*) FROM dept_ranked")
    check r.ok
    check col0(r.value) == "4"
    discard closeDb(db)

  test "VIEW with RANK window function":
    let db = freshDb("view_rank")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 1), (4, 3)").ok
    check execSql(db, "CREATE VIEW ranked AS SELECT id, v, RANK() OVER (ORDER BY v) AS rnk FROM t").ok
    let r = execSql(db, "SELECT COUNT(*) FROM ranked")
    check r.ok
    check col0(r.value) == "4"
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Subquery table metadata (subqueryTableMeta)":
# ─────────────────────────────────────────────────────────────────────────────
  test "subquery with aliased FROM table":
    let db = freshDb("subq_alias_from")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "SELECT s.id FROM (SELECT * FROM t AS x) s ORDER BY s.id")
    check r.ok
    check r.value == @["1", "2", "3"]
    discard closeDb(db)

  test "subquery with JOIN inside having alias":
    let db = freshDb("subq_join_alias")
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, w INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1, 10), (2, 20)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 1, 5), (2, 2, 8)").ok
    let r = execSql(db, "SELECT s.id FROM (SELECT a.id FROM a JOIN b AS bc ON a.id = bc.aid) s ORDER BY s.id")
    check r.ok
    check r.value.len == 2
    discard closeDb(db)

  test "subquery with COUNT in SELECT list":
    let db = freshDb("subq_count")
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1, 1), (2, 2), (3, 3)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 1), (2, 1), (3, 2)").ok
    let r = execSql(db, "SELECT a.id FROM a JOIN (SELECT aid, COUNT(*) AS cnt FROM b GROUP BY aid) s ON a.id = s.aid ORDER BY a.id")
    check r.ok
    check r.value.len == 2
    discard closeDb(db)

  test "subquery with SUM aggregation":
    let db = freshDb("subq_sum")
    check execSql(db, "CREATE TABLE orders (id INT PRIMARY KEY, cid INT, amt INT)").ok
    check execSql(db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").ok
    check execSql(db, "INSERT INTO orders VALUES (1, 1, 10), (2, 1, 20), (3, 2, 15)").ok
    let r = execSql(db, "SELECT c.name FROM customers c JOIN (SELECT cid, SUM(amt) AS total FROM orders GROUP BY cid) s ON c.id = s.cid ORDER BY c.name")
    check r.ok
    check r.value == @["Alice", "Bob"]
    discard closeDb(db)

  test "subquery star with join inside (multi-col)":
    let db = freshDb("subq_star_join")
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, w INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1, 10)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 1, 5)").ok
    let r = execSql(db, "SELECT a.id FROM a JOIN (SELECT * FROM a JOIN b ON a.id = b.aid) sub ON a.id = sub.id")
    check r.ok
    check r.value.len >= 1
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "directSelectDependencies (join subqueries, CTEs)":
# ─────────────────────────────────────────────────────────────────────────────
  test "JOIN with subquery triggers dependency tracking":
    let db = freshDb("dep_join_subq")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 5), (2, 10), (3, 15)").ok
    let r = execSql(db, "SELECT t.id FROM t JOIN (SELECT id FROM t WHERE v > 5) s ON t.id = s.id ORDER BY t.id")
    check r.ok
    check r.value == @["2", "3"]
    discard closeDb(db)

  test "CTE with UNION ALL (set op dependency)":
    let db = freshDb("dep_cte_union")
    check execSql(db, "CREATE TABLE t1 (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE t2 (id INT PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t1 VALUES (1), (2)").ok
    check execSql(db, "INSERT INTO t2 VALUES (3), (4)").ok
    let r = execSql(db, "WITH combined AS (SELECT id FROM t1 UNION ALL SELECT id FROM t2) SELECT id FROM combined ORDER BY id")
    check r.ok
    check r.value == @["1", "2", "3", "4"]
    discard closeDb(db)

  test "CTE with FROM subquery":
    let db = freshDb("dep_cte_fromsub")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1), (2, 4), (3, 9)").ok
    let r = execSql(db, "WITH sq AS (SELECT id, v FROM t WHERE v > 2) SELECT id FROM sq ORDER BY id")
    check r.ok
    check r.value == @["2", "3"]
    discard closeDb(db)

  test "CTE used multiple times":
    let db = freshDb("dep_cte_multi")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "WITH base AS (SELECT id, v FROM t) SELECT COUNT(*) FROM base WHERE v > 15")
    check r.ok
    check col0(r.value) == "2"
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "ON CONFLICT with complex expressions":
# ─────────────────────────────────────────────────────────────────────────────
  test "ON CONFLICT DO UPDATE with IN condition":
    let db = freshDb("conflict_in")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'old')").ok
    let r = execSql(db, "INSERT INTO t VALUES (1, 'new') ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v WHERE EXCLUDED.id IN (1, 2, 3)")
    check r.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "new"
    discard closeDb(db)

  test "ON CONFLICT DO UPDATE basic":
    let db = freshDb("conflict_basic")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Alice')").ok
    let r = execSql(db, "INSERT INTO t VALUES (1, 'Alicia') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name")
    check r.ok
    let sel = execSql(db, "SELECT name FROM t WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "Alicia"
    discard closeDb(db)

  test "ON CONFLICT DO NOTHING ignores duplicate":
    let db = freshDb("conflict_nothing")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10)").ok
    let r = execSql(db, "INSERT INTO t VALUES (1, 99) ON CONFLICT DO NOTHING")
    check r.ok
    let sel = execSql(db, "SELECT v FROM t WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "10"
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Parameterized queries with IN and window functions":
# ─────────────────────────────────────────────────────────────────────────────
  test "parameterized query with IN list via WHERE":
    let db = freshDb("param_in")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "SELECT id FROM t WHERE v IN (10, 30) ORDER BY id")
    check r.ok
    check r.value == @["1", "3"]
    discard closeDb(db)

  test "window function ROW_NUMBER in main query":
    let db = freshDb("win_rn_main")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)").ok
    let r = execSql(db, "SELECT id, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM t ORDER BY id")
    check r.ok
    check r.value.len == 3
    discard closeDb(db)

  test "window RANK function":
    let db = freshDb("win_rank")
    check execSql(db, "CREATE TABLE scores (id INT PRIMARY KEY, score INT)").ok
    check execSql(db, "INSERT INTO scores VALUES (1, 100), (2, 90), (3, 100), (4, 80)").ok
    let r = execSql(db, "SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores ORDER BY id")
    check r.ok
    check r.value.len == 4
    discard closeDb(db)

  test "window DENSE_RANK function":
    let db = freshDb("win_denserank")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 10)").ok
    let r = execSql(db, "SELECT id, DENSE_RANK() OVER (ORDER BY v) AS dr FROM t ORDER BY id")
    check r.ok
    check r.value.len == 3
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "CTE patterns":
# ─────────────────────────────────────────────────────────────────────────────
  test "simple CTE query":
    let db = freshDb("cte_simple")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)").ok
    let r = execSql(db, "WITH cte AS (SELECT id, v FROM t WHERE v > 1) SELECT id FROM cte ORDER BY id")
    check r.ok
    check r.value == @["2", "3"]
    discard closeDb(db)

  test "recursive CTE":
    let db = freshDb("cte_recursive")
    let r = execSql(db, "WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n < 5) SELECT n FROM nums")
    check r.ok
    check r.value.len == 5
    discard closeDb(db)

  test "chained CTEs":
    let db = freshDb("cte_chained")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 5), (4, 15)").ok
    let r = execSql(db, "WITH base AS (SELECT id, v FROM t), filtered AS (SELECT id, v FROM base WHERE v > 10) SELECT id FROM filtered ORDER BY id")
    check r.ok
    check r.value == @["2", "4"]
    discard closeDb(db)

  test "CTE with aggregate is now supported (subquery wrap)":
    let db = freshDb("cte_agg")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'A',10),(2,'A',20),(3,'B',5),(4,'B',15)").ok
    let r = execSql(db, "WITH sums AS (SELECT cat, SUM(v) AS total FROM t GROUP BY cat) SELECT cat, total FROM sums ORDER BY cat")
    check r.ok
    check r.value.len == 2
    check r.value[0] == "A|30"
    check r.value[1] == "B|20"
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "NATURAL JOIN and other JOIN types":
# ─────────────────────────────────────────────────────────────────────────────
  test "NATURAL JOIN on shared column":
    let db = freshDb("natural_join")
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, w INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1, 10), (2, 20)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 5), (3, 15)").ok
    let r = execSql(db, "SELECT a.id FROM a NATURAL JOIN b ORDER BY a.id")
    check r.ok
    check r.value == @["1"]
    discard closeDb(db)
