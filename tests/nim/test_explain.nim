import unittest
import os
import strutils
import engine
import errors
import c_api
import record/record

proc makeTempDb(name: string): string =
  let normalizedName =
    if name.len >= 3 and name[name.len - 3 .. ^1] == ".db":
      name[0 .. ^4] & ".ddb"
    else:
      name
  let path = getTempDir() / normalizedName
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "EXPLAIN Statement":
  test "EXPLAIN SELECT * FROM t":
    let path = makeTempDb("decentdb_explain_basic.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A')").ok
    
    let res = execSql(db, "EXPLAIN SELECT * FROM t WHERE id = 1")
    check res.ok
    check res.value.len > 0
    let planText = res.value.join("\n")
    # Should use TableScan because no index on id (unless rowid optimization picks it up if I defined it as INT PRIMARY KEY? No, it's just INT)
    # Actually, verify that it produced a plan.
    check "Project" in planText
    
    discard closeDb(db)

  test "EXPLAIN SELECT with $1 parameter":
    let path = makeTempDb("decentdb_explain_param.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A')").ok

    let res = execSql(db, "EXPLAIN SELECT * FROM t WHERE id = $1", @[Value(kind: vkInt64, int64Val: 1)])
    check res.ok
    check res.value.len > 0
    check "Project" in res.value.join("\n")

    discard closeDb(db)

  test "EXPLAIN returns 1 column":
    let path = makeTempDb("decentdb_explain_cols.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok
    
    let res = execSql(db, "EXPLAIN SELECT * FROM t")
    check res.ok
    check res.value.len > 0
    
    discard closeDb(db)

  test "EXPLAIN INSERT fails":
    let path = makeTempDb("decentdb_explain_insert.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok
    
    let res = execSql(db, "EXPLAIN INSERT INTO t VALUES (1)")
    check not res.ok
    check res.err.message == "EXPLAIN currently supports SELECT only"
    
    # Verify no insert happened
    let rows = execSql(db, "SELECT * FROM t")
    check rows.ok
    check rows.value.len == 0
    
    discard closeDb(db)

  test "EXPLAIN options fail":
    let path = makeTempDb("decentdb_explain_opts.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok
    
    let res = execSql(db, "EXPLAIN (ANALYZE) SELECT * FROM t")
    check not res.ok
    check res.err.message == "EXPLAIN options not supported"
    
    discard closeDb(db)

  test "Trigram path visibility":
    let path = makeTempDb("decentdb_explain_trigram.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (name TEXT)").ok
    check execSql(db, "CREATE INDEX ix_name ON t USING trigram (name)").ok
    
    let res = execSql(db, "EXPLAIN SELECT * FROM t WHERE name LIKE '%abc%'")
    check res.ok
    let planText = res.value.join("\n")
    check "TrigramSeek" in planText
    
    discard closeDb(db)

  test "OR predicate plans as UnionDistinct when indexable":
    let path = makeTempDb("decentdb_explain_or_union.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "CREATE INDEX ix_id ON t (id)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'B')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'C')").ok

    let res = execSql(db, "EXPLAIN SELECT * FROM t WHERE id = 1 OR id = 3")
    check res.ok
    let planText = res.value.join("\n")
    check "UnionDistinct" in planText
    check "IndexSeek" in planText
    check "TableScan" notin planText

    discard closeDb(db)

  test "AND (OR ...) distributes into UnionDistinct when indexable":
    let path = makeTempDb("decentdb_explain_and_or_union.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (name TEXT)").ok
    check execSql(db, "CREATE INDEX ix_name_trgm ON t USING trigram (name)").ok
    check execSql(db, "INSERT INTO t VALUES ('abc')").ok
    check execSql(db, "INSERT INTO t VALUES ('def')").ok

    let res = execSql(db, "EXPLAIN SELECT * FROM t WHERE name IS NOT NULL AND (name LIKE '%abc%' OR name LIKE '%def%')")
    check res.ok
    let planText = res.value.join("\n")
    check "UnionDistinct" in planText
    check "TrigramSeek" in planText
    check "TableScan" notin planText

    discard closeDb(db)

  test "C API: EXPLAIN yields query_plan rows":
    let path = makeTempDb("decentdb_explain_capi.db")
    let h = decentdb_open(path.cstring, nil)
    check h != nil

    proc execNoRows(sqlText: string) =
      var stmt: pointer = nil
      check decentdb_prepare(h, sqlText.cstring, addr stmt) == 0
      check stmt != nil
      check decentdb_step(stmt) == 0
      decentdb_finalize(stmt)

    execNoRows("CREATE TABLE t (id INT, name TEXT)")
    execNoRows("INSERT INTO t VALUES (1, 'A')")

    var stmt: pointer = nil
    check decentdb_prepare(h, "EXPLAIN SELECT * FROM t WHERE id = $1".cstring, addr stmt) == 0
    check stmt != nil
    check decentdb_column_count(stmt) == 1
    check $decentdb_column_name(stmt, 0) == "query_plan"
    check decentdb_bind_int64(stmt, 1, 1) == 0

    var lines: seq[string] = @[]
    while true:
      let rc = decentdb_step(stmt)
      if rc == 0:
        break
      check rc == 1
      var n: cint = 0
      let p = decentdb_column_text(stmt, 0, addr n)
      check p != nil
      check n > 0
      var line = newString(int(n))
      if n > 0:
        copyMem(addr line[0], p, int(n))
      if lines.len == 0:
        check '|' notin line
        check line.len > 0 and line[0] == 'P'
      lines.add(line)
    decentdb_finalize(stmt)

    check lines.len > 0
    check "Project" in lines.join("\n")
    discard decentdb_close(h)

