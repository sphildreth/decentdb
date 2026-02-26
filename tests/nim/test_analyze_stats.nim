## Tests for ANALYZE, catalog stats persistence, and incremental row-count maintenance.
import unittest
import os
import options
import engine
import catalog/catalog
import sql/sql
import sql/binder
import errors

# ─── helpers ────────────────────────────────────────────────────────────────

proc execOk(db: Db, q: string) =
  let r = db.execSql(q, @[])
  if not r.ok:
    checkpoint "execSql failed for: " & q & " — " & r.err.message
  check r.ok

proc tempPath(name: string): string =
  let path = getTempDir() / name & ".ddb"
  if fileExists(path): removeFile(path)
  let wal = path & "-wal"
  if fileExists(wal): removeFile(wal)
  result = path

proc cleanUp(path: string) =
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")

# ─── ANALYZE parsing ──────────────────────────────────────────────────────

suite "ANALYZE parsing":
  test "ANALYZE tableName parses as skAnalyze":
    let r = parseSql("ANALYZE users")
    check r.ok
    check r.value.statements.len == 1
    let s = r.value.statements[0]
    check s.kind == skAnalyze
    check s.analyzeTable == "users"

  test "bare ANALYZE parses as skAnalyze with empty table":
    let r = parseSql("ANALYZE")
    check r.ok
    check r.value.statements.len == 1
    check r.value.statements[0].kind == skAnalyze
    check r.value.statements[0].analyzeTable == ""

  test "ANALYZE binder rejects unknown table":
    let path = tempPath("analyze_bind_test")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    let r = parseSql("ANALYZE no_such_table")
    check r.ok
    let bindRes = bindStatement(db.catalog, r.value.statements[0])
    check not bindRes.ok

  test "ANALYZE binder accepts known table":
    let path = tempPath("analyze_bind_ok")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)").ok
    let r = parseSql("ANALYZE products")
    check r.ok
    let bindRes = bindStatement(db.catalog, r.value.statements[0])
    check bindRes.ok

# ─── ANALYZE execution ────────────────────────────────────────────────────

suite "ANALYZE execution":
  test "ANALYZE on empty table yields rowCount=0":
    let path = tempPath("analyze_empty")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "ANALYZE t").ok
    let st = db.catalog.getTableStats("t")
    check st.isSome
    check st.get.rowCount == 0

  test "ANALYZE counts rows correctly":
    let path = tempPath("analyze_count")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2),(3),(4),(5)").ok
    check execSql(db, "ANALYZE t").ok
    let st = db.catalog.getTableStats("t")
    check st.isSome
    check st.get.rowCount == 5

  test "ANALYZE stats are persisted and reloaded on DB reopen":
    let path = tempPath("analyze_persist")
    block:
      let dbRes = openDb(path)
      check dbRes.ok
      let db = dbRes.value
      check execSql(db, "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT)").ok
      check execSql(db, "INSERT INTO widgets VALUES (1,'a'),(2,'b'),(3,'c')").ok
      check execSql(db, "ANALYZE widgets").ok
      discard db.closeDb()
    let dbRes2 = openDb(path)
    check dbRes2.ok
    let db2 = dbRes2.value
    defer: discard db2.closeDb(); cleanUp(path)
    let st = db2.catalog.getTableStats("widgets")
    check st.isSome
    check st.get.rowCount == 3

  test "Re-ANALYZE after more inserts updates rowCount":
    let path = tempPath("analyze_rerun")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2),(3)").ok
    check execSql(db, "ANALYZE t").ok
    check db.catalog.getTableStats("t").get.rowCount == 3
    check execSql(db, "INSERT INTO t VALUES (4),(5)").ok
    check execSql(db, "ANALYZE t").ok
    check db.catalog.getTableStats("t").get.rowCount == 5

  test "ANALYZE inside explicit transaction is rejected":
    let path = tempPath("analyze_txn")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "BEGIN").ok
    let r = execSql(db, "ANALYZE t")
    check not r.ok
    check execSql(db, "ROLLBACK").ok

  test "ANALYZE computes index entry and distinct-key counts":
    let path = tempPath("analyze_idx")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, category TEXT)").ok
    check execSql(db, "CREATE INDEX idx_cat ON t(category)").ok
    check execSql(db, "INSERT INTO t VALUES (1,'A'),(2,'A'),(3,'B'),(4,'C')").ok
    check execSql(db, "ANALYZE t").ok
    let ist = db.catalog.getIndexStats("idx_cat")
    check ist.isSome
    check ist.get.entryCount == 4
    check ist.get.distinctKeyCount == 3

# ─── Incremental row-count maintenance ───────────────────────────────────

suite "Incremental row-count maintenance":
  test "INSERT commit applies row-count delta":
    let path = tempPath("delta_insert")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "ANALYZE t").ok
    check db.catalog.getTableStats("t").get.rowCount == 0
    check execSql(db, "INSERT INTO t VALUES (1),(2),(3)").ok
    let st = db.catalog.getTableStats("t")
    check st.isSome
    check st.get.rowCount == 3

  test "DELETE commit applies negative row-count delta":
    let path = tempPath("delta_delete")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2),(3),(4),(5)").ok
    check execSql(db, "ANALYZE t").ok
    check db.catalog.getTableStats("t").get.rowCount == 5
    check execSql(db, "DELETE FROM t WHERE id <= 2").ok
    let st = db.catalog.getTableStats("t")
    check st.isSome
    check st.get.rowCount == 3

  test "Rolled-back INSERT does not change row count":
    let path = tempPath("delta_rollback_insert")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2)").ok
    check execSql(db, "ANALYZE t").ok
    let before = db.catalog.getTableStats("t").get.rowCount
    check execSql(db, "BEGIN").ok
    check execSql(db, "INSERT INTO t VALUES (3),(4),(5)").ok
    check execSql(db, "ROLLBACK").ok
    let after = db.catalog.getTableStats("t")
    check after.isSome
    check after.get.rowCount == before

  test "Rolled-back DELETE does not change row count":
    let path = tempPath("delta_rollback_delete")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    defer: discard db.closeDb(); cleanUp(path)
    check execSql(db, "CREATE TABLE t (id INTEGER PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO t VALUES (1),(2),(3)").ok
    check execSql(db, "ANALYZE t").ok
    let before = db.catalog.getTableStats("t").get.rowCount
    check execSql(db, "BEGIN").ok
    check execSql(db, "DELETE FROM t WHERE id = 1").ok
    check execSql(db, "ROLLBACK").ok
    let after = db.catalog.getTableStats("t")
    check after.isSome
    check after.get.rowCount == before
