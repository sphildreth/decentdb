## test_engine_advanced_coverage.nim
## Targets engine.nim uncovered paths:
##   - CREATE UNIQUE INDEX on existing data single/multi-col (L3658-3730)
##   - INSERT...SELECT with DEFAULT and GENERATED columns (L2410-2441)
##   - FK enforcement with TEXT parent column (L1126-1171)
##   - SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK TO SAVEPOINT (L4116-4132)
##   - Batch UNIQUE constraint check for multiple-row INSERT (L995-1012)
##   - INSERT...SELECT outer path (L2383-2403)

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
suite "CREATE UNIQUE INDEX on existing data":
# ─────────────────────────────────────────────────────────────────────────────
  test "single-col unique index on non-duplicate data succeeds":
    let db = freshDb("uidx_nodup")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX t_v ON t(v)")
    check r.ok
    # Verify index works
    let dup = execSql(db, "INSERT INTO t VALUES (4, 'alpha')")
    check not dup.ok
    discard closeDb(db)

  test "single-col unique index on duplicate data fails":
    let db = freshDb("uidx_dup")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'same'), (2, 'same')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX t_v ON t(v)")
    check not r.ok
    discard closeDb(db)

  test "multi-col unique index on non-duplicate data succeeds":
    let db = freshDb("uidx_multi_nodup")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 2, 'x')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX t_ab ON t(a, b)")
    check r.ok
    discard closeDb(db)

  test "multi-col unique index on duplicate composite key fails":
    let db = freshDb("uidx_multi_dup")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1, 'x'), (2, 1, 'x')").ok
    let r = execSql(db, "CREATE UNIQUE INDEX t_ab ON t(a, b)")
    check not r.ok
    discard closeDb(db)

  test "unique index on empty table succeeds":
    let db = freshDb("uidx_empty")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    let r = execSql(db, "CREATE UNIQUE INDEX t_v ON t(v)")
    check r.ok
    discard closeDb(db)

  test "unique index on integer column":
    let db = freshDb("uidx_int")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)").ok
    let r = execSql(db, "CREATE UNIQUE INDEX t_code ON t(code)")
    check r.ok
    let ins = execSql(db, "INSERT INTO t VALUES (4, 100)")
    check not ins.ok
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "INSERT...SELECT with DEFAULT and GENERATED columns":
# ─────────────────────────────────────────────────────────────────────────────
  test "INSERT...SELECT fills DEFAULT for omitted columns":
    let db = freshDb("ins_sel_def")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, name TEXT, status TEXT DEFAULT 'active')").ok
    check execSql(db, "INSERT INTO src VALUES (1, 'Alice'), (2, 'Bob')").ok
    let r = execSql(db, "INSERT INTO dst (id, name) SELECT id, name FROM src")
    check r.ok
    let sel = execSql(db, "SELECT status FROM dst WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "active"
    discard closeDb(db)

  test "INSERT...SELECT with numeric DEFAULT":
    let db = freshDb("ins_sel_numdef")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, score INT DEFAULT 0)").ok
    check execSql(db, "INSERT INTO src VALUES (1), (2)").ok
    let r = execSql(db, "INSERT INTO dst (id) SELECT id FROM src")
    check r.ok
    let sel = execSql(db, "SELECT score FROM dst ORDER BY id")
    check sel.ok
    check sel.value == @["0", "0"]
    discard closeDb(db)

  test "INSERT...SELECT with GENERATED ALWAYS AS STORED column":
    let db = freshDb("ins_sel_gen")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, a INT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 3) STORED)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 4), (2, 7)").ok
    let r = execSql(db, "INSERT INTO dst (id, a) SELECT id, a FROM src")
    check r.ok
    let sel = execSql(db, "SELECT b FROM dst WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "12"
    discard closeDb(db)

  test "INSERT...SELECT multiple rows with DEFAULT":
    let db = freshDb("ins_sel_multidef")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, v INT, created TEXT DEFAULT 'now')").ok
    check execSql(db, "INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "INSERT INTO dst (id, v) SELECT id, v FROM src")
    check r.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM dst WHERE created = 'now'")
    check cnt.ok
    check col0(cnt.value) == "3"
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "FK enforcement with TEXT parent column":
# ─────────────────────────────────────────────────────────────────────────────
  test "FK on TEXT PK parent accepts valid child":
    let db = freshDb("fk_text_valid")
    check execSql(db, "CREATE TABLE cats (code TEXT PRIMARY KEY, label TEXT)").ok
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT REFERENCES cats(code))").ok
    check execSql(db, "INSERT INTO cats VALUES ('A', 'Alpha'), ('B', 'Beta')").ok
    let r = execSql(db, "INSERT INTO items VALUES (1, 'A')")
    check r.ok
    discard closeDb(db)

  test "FK on TEXT PK parent rejects invalid child":
    let db = freshDb("fk_text_invalid")
    check execSql(db, "CREATE TABLE cats (code TEXT PRIMARY KEY, label TEXT)").ok
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT REFERENCES cats(code))").ok
    check execSql(db, "INSERT INTO cats VALUES ('X', 'X-cat')").ok
    let r = execSql(db, "INSERT INTO items VALUES (1, 'NOPE')")
    check not r.ok
    discard closeDb(db)

  test "FK on TEXT parent with multiple refs batch":
    let db = freshDb("fk_text_batch")
    check execSql(db, "CREATE TABLE status (name TEXT PRIMARY KEY, label TEXT)").ok
    check execSql(db, "CREATE TABLE tasks (id INT PRIMARY KEY, st TEXT REFERENCES status(name))").ok
    check execSql(db, "INSERT INTO status VALUES ('open', 'Open'), ('done', 'Done')").ok
    let r1 = execSql(db, "INSERT INTO tasks VALUES (1, 'open')")
    check r1.ok
    let r2 = execSql(db, "INSERT INTO tasks VALUES (2, 'done')")
    check r2.ok
    let r3 = execSql(db, "INSERT INTO tasks VALUES (3, 'unknown')")
    check not r3.ok
    discard closeDb(db)

  test "FK TEXT parent NULL child value is allowed":
    let db = freshDb("fk_text_null")
    check execSql(db, "CREATE TABLE cats (code TEXT PRIMARY KEY, label TEXT)").ok
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, cat TEXT REFERENCES cats(code))").ok
    let r = execSql(db, "INSERT INTO items VALUES (1, NULL)")
    check r.ok
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "SAVEPOINT / RELEASE / ROLLBACK TO":
# ─────────────────────────────────────────────────────────────────────────────
  test "SAVEPOINT and RELEASE success":
    let db = freshDb("sp_basic")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "BEGIN").ok
    check execSql(db, "SAVEPOINT sp1").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10)").ok
    check execSql(db, "RELEASE SAVEPOINT sp1").ok
    check execSql(db, "COMMIT").ok
    let r = execSql(db, "SELECT id FROM t")
    check r.ok
    check r.value.len == 1
    discard closeDb(db)

  test "ROLLBACK TO SAVEPOINT undoes nested work":
    let db = freshDb("sp_rollback")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10)").ok
    check execSql(db, "BEGIN").ok
    check execSql(db, "SAVEPOINT sp1").ok
    check execSql(db, "INSERT INTO t VALUES (99, 99)").ok
    let before = execSql(db, "SELECT id FROM t WHERE id = 99")
    check before.ok
    check before.value.len == 1
    check execSql(db, "ROLLBACK TO SAVEPOINT sp1").ok
    check execSql(db, "RELEASE SAVEPOINT sp1").ok
    check execSql(db, "COMMIT").ok
    let after = execSql(db, "SELECT id FROM t WHERE id = 99")
    check after.ok
    check after.value.len == 0
    discard closeDb(db)

  test "nested SAVEPOINTs":
    let db = freshDb("sp_nested")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "BEGIN").ok
    check execSql(db, "SAVEPOINT outer1").ok
    check execSql(db, "SAVEPOINT inner1").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1)").ok
    check execSql(db, "RELEASE SAVEPOINT inner1").ok
    check execSql(db, "SAVEPOINT inner2").ok
    check execSql(db, "INSERT INTO t VALUES (2, 2)").ok
    check execSql(db, "RELEASE SAVEPOINT inner2").ok
    check execSql(db, "RELEASE SAVEPOINT outer1").ok
    check execSql(db, "COMMIT").ok
    let r = execSql(db, "SELECT COUNT(*) FROM t")
    check r.ok
    check col0(r.value) == "2"
    discard closeDb(db)

  test "RELEASE non-existent SAVEPOINT fails":
    let db = freshDb("sp_noexist")
    let r = execSql(db, "RELEASE SAVEPOINT nonexistent")
    check not r.ok
    discard closeDb(db)

  test "ROLLBACK TO non-existent SAVEPOINT fails":
    let db = freshDb("sp_rollback_noexist")
    let r = execSql(db, "ROLLBACK TO SAVEPOINT ghost")
    check not r.ok
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Batch UNIQUE constraint check":
# ─────────────────────────────────────────────────────────────────────────────
  test "multi-row INSERT with duplicate UNIQUE values fails":
    let db = freshDb("batch_uniq_dup")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    let r = execSql(db, "INSERT INTO t VALUES (1, 'abc'), (2, 'abc')")
    check not r.ok
    discard closeDb(db)

  test "multi-row INSERT with distinct UNIQUE values succeeds":
    let db = freshDb("batch_uniq_ok")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    let r = execSql(db, "INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')")
    check r.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    check cnt.ok
    check col0(cnt.value) == "3"
    discard closeDb(db)

  test "multi-row INSERT duplication against existing row fails":
    let db = freshDb("batch_uniq_existing")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'abc')").ok
    let r = execSql(db, "INSERT INTO t VALUES (2, 'abc')")
    check not r.ok
    discard closeDb(db)

  test "UPDATE to duplicate UNIQUE value fails":
    let db = freshDb("batch_uniq_update")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, 'b')").ok
    let r = execSql(db, "UPDATE t SET code = 'a' WHERE id = 2")
    check not r.ok
    discard closeDb(db)

  test "UPDATE to same UNIQUE value for same row succeeds":
    let db = freshDb("batch_uniq_self")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a')").ok
    let r = execSql(db, "UPDATE t SET code = 'a' WHERE id = 1")
    check r.ok
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "FK cascade actions":
# ─────────────────────────────────────────────────────────────────────────────
  test "CASCADE DELETE removes children":
    let db = freshDb("fk_casc_del")
    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)").ok
    check execSql(db, "INSERT INTO parent VALUES (1, 'p1'), (2, 'p2')").ok
    check execSql(db, "INSERT INTO child VALUES (1, 1), (2, 1), (3, 2)").ok
    check execSql(db, "DELETE FROM parent WHERE id = 1").ok
    let r = execSql(db, "SELECT COUNT(*) FROM child")
    check r.ok
    check col0(r.value) == "1"
    discard closeDb(db)

  test "SET NULL ON DELETE nullifies FK":
    let db = freshDb("fk_setnull")
    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL)").ok
    check execSql(db, "INSERT INTO parent VALUES (1)").ok
    check execSql(db, "INSERT INTO child VALUES (1, 1)").ok
    check execSql(db, "DELETE FROM parent WHERE id = 1").ok
    let r = execSql(db, "SELECT pid FROM child WHERE id = 1")
    check r.ok
    check col0(r.value) == "NULL"
    discard closeDb(db)

  test "RESTRICT prevents DELETE of referenced parent":
    let db = freshDb("fk_restrict")
    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE RESTRICT)").ok
    check execSql(db, "INSERT INTO parent VALUES (1)").ok
    check execSql(db, "INSERT INTO child VALUES (1, 1)").ok
    let r = execSql(db, "DELETE FROM parent WHERE id = 1")
    check not r.ok
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Complex WHERE and JOIN scenarios":
# ─────────────────────────────────────────────────────────────────────────────
  test "INNER JOIN with filtering":
    let db = freshDb("join_filter")
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, v INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1, 10), (2, 20)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 1, 5), (2, 1, 15), (3, 2, 8)").ok
    let r = execSql(db, "SELECT b.id FROM a JOIN b ON a.id = b.aid WHERE a.v > 15")
    check r.ok
    check r.value == @["3"]
    discard closeDb(db)

  test "LEFT JOIN returns NULL for non-matching":
    let db = freshDb("left_join_null")
    check execSql(db, "CREATE TABLE a (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, aid INT, v TEXT)").ok
    check execSql(db, "INSERT INTO a VALUES (1), (2), (3)").ok
    check execSql(db, "INSERT INTO b VALUES (1, 1, 'x'), (2, 3, 'y')").ok
    let r = execSql(db, "SELECT a.id, b.v FROM a LEFT JOIN b ON a.id = b.aid ORDER BY a.id")
    check r.ok
    check r.value.len == 3
    check r.value[1] == "2|NULL"
    discard closeDb(db)

  test "subquery in WHERE":
    let db = freshDb("subq_where")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t)")
    check r.ok
    check r.value == @["3"]
    discard closeDb(db)
