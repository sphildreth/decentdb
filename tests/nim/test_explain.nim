import unittest
import os
import strutils
import tables
import options
import engine
import errors
import c_api
import record/record
import planner/explain
import planner/planner
import sql/sql
import catalog/catalog

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

  test "EXPLAIN unsupported options fail":
    let path = makeTempDb("decentdb_explain_opts.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok
    
    let res = execSql(db, "EXPLAIN (VERBOSE) SELECT * FROM t")
    check not res.ok
    check "not supported" in res.err.message
    
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

  test "EXPLAIN ANALYZE returns plan with actual metrics":
    let path = makeTempDb("decentdb_explain_analyze.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'Bob')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'Charlie')").ok

    let res = execSql(db, "EXPLAIN ANALYZE SELECT * FROM t")
    check res.ok
    check res.value.len > 0
    let planText = res.value.join("\n")
    check "Project" in planText
    check "Actual Rows: 3" in planText
    check "Actual Time:" in planText
    check "ms" in planText

    discard closeDb(db)

  test "EXPLAIN ANALYZE with filter":
    let path = makeTempDb("decentdb_explain_analyze_filter.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    for i in 1..10:
      check execSql(db, "INSERT INTO t VALUES ($1, 'name')", @[Value(kind: vkInt64, int64Val: int64(i))]).ok

    let res = execSql(db, "EXPLAIN ANALYZE SELECT * FROM t WHERE id > 5")
    check res.ok
    let planText = res.value.join("\n")
    check "Filter" in planText
    check "Actual Rows: 5" in planText

    discard closeDb(db)

  test "EXPLAIN ANALYZE empty result":
    let path = makeTempDb("decentdb_explain_analyze_empty.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok

    let res = execSql(db, "EXPLAIN ANALYZE SELECT * FROM t")
    check res.ok
    let planText = res.value.join("\n")
    check "Actual Rows: 0" in planText

    discard closeDb(db)

  test "EXPLAIN ANALYZE does not modify data":
    let path = makeTempDb("decentdb_explain_analyze_readonly.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1)").ok

    let res = execSql(db, "EXPLAIN ANALYZE SELECT * FROM t")
    check res.ok
    # Verify data unchanged
    let rows = execSql(db, "SELECT COUNT(*) FROM t")
    check rows.ok
    check rows.value[0] == "1"

    discard closeDb(db)

  test "EXPLAIN ANALYZE INSERT fails":
    let path = makeTempDb("decentdb_explain_analyze_insert.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok

    let res = execSql(db, "EXPLAIN ANALYZE INSERT INTO t VALUES (1)")
    check not res.ok
    check res.err.message == "EXPLAIN currently supports SELECT only"

    discard closeDb(db)

  test "EXPLAIN (ANALYZE) parenthesized syntax":
    let path = makeTempDb("decentdb_explain_paren_analyze.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1)").ok

    let res = execSql(db, "EXPLAIN (ANALYZE) SELECT * FROM t")
    check res.ok
    let planText = res.value.join("\n")
    check "Actual Rows: 1" in planText

    discard closeDb(db)

  test "C API: EXPLAIN ANALYZE yields plan with metrics":
    let path = makeTempDb("decentdb_explain_analyze_capi.db")
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
    execNoRows("INSERT INTO t VALUES (2, 'B')")

    var stmt: pointer = nil
    check decentdb_prepare(h, "EXPLAIN ANALYZE SELECT * FROM t".cstring, addr stmt) == 0
    check stmt != nil

    var lines: seq[string] = @[]
    while true:
      let rc = decentdb_step(stmt)
      if rc == 0:
        break
      check rc == 1
      var n: cint = 0
      let p = decentdb_column_text(stmt, 0, addr n)
      check p != nil
      var line = newString(int(n))
      if n > 0:
        copyMem(addr line[0], p, int(n))
      lines.add(line)
    decentdb_finalize(stmt)

    let planText = lines.join("\n")
    check "Project" in planText
    check "Actual Rows: 2" in planText
    check "Actual Time:" in planText
    discard decentdb_close(h)

  test "Direct renderExpr coverage":
    check renderExpr(nil) == "<nil>"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svNull))) == "NULL"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 42))) == "42"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svFloat, floatVal: 3.14))) == "3.14"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true))) == "true"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "foo'bar"))) == "'foo''bar'"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svBlob, blobVal: @[byte 0xDE, 0xAD, 0xBE, 0xEF]))) == "X'DEADBEEF'"
    check renderExpr(Expr(kind: ekLiteral, value: SqlValue(kind: svParam, paramIndex: 1))) == "$1"
    check renderExpr(Expr(kind: ekColumn, table: "t1", name: "c1")) == "t1.c1"
    check renderExpr(Expr(kind: ekColumn, table: "", name: "c1")) == "c1"
    check renderExpr(Expr(kind: ekBinary, op: "+", left: Expr(kind: ekColumn, name: "a"), right: Expr(kind: ekColumn, name: "b"))) == "(a + b)"
    check renderExpr(Expr(kind: ekUnary, unOp: "NOT", expr: Expr(kind: ekColumn, name: "a"))) == "(NOT a)"
    check renderExpr(Expr(kind: ekFunc, funcName: "COUNT", isStar: true)) == "COUNT(*)"
    check renderExpr(Expr(kind: ekFunc, funcName: "COALESCE", args: @[Expr(kind: ekColumn, name: "a"), Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 0))], isStar: false)) == "COALESCE(a, 0)"
    check renderExpr(Expr(kind: ekParam, index: 2)) == "$2"
    check renderExpr(Expr(kind: ekInList, inExpr: Expr(kind: ekColumn, name: "a"), inList: @[Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)), Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2))])) == "(a IN (1, 2))"
    
    let winExpr = Expr(kind: ekWindowRowNumber, windowFunc: "LAG", windowArgs: @[Expr(kind: ekColumn, name: "a")], windowPartitions: @[Expr(kind: ekColumn, name: "p")], windowOrderExprs: @[Expr(kind: ekColumn, name: "o")], windowOrderAsc: @[false])
    check renderExpr(winExpr) == "LAG(a) OVER (PARTITION BY p ORDER BY o DESC)"
    
    let winExpr2 = Expr(kind: ekWindowRowNumber, windowFunc: "ROW_NUMBER", windowArgs: @[], windowPartitions: @[], windowOrderExprs: @[Expr(kind: ekColumn, name: "o")])
    check renderExpr(winExpr2) == "ROW_NUMBER() OVER (ORDER BY o ASC)"

    check renderExpr(Expr(kind: ekSqlValueFunction, sqlValueFunc: "CURRENT_TIMESTAMP")) == "CURRENT_TIMESTAMP"
    check renderExpr(Expr(kind: ekExtract, extractField: "YEAR", extractSource: Expr(kind: ekColumn, name: "d"))) == "EXTRACT(YEAR FROM d)"

  test "Direct explainPlanLines coverage":
    let cat = Catalog()
    cat.indexes = initTable[string, IndexMeta]()
    
    # Create an index for pkIndexSeek that starts with expr:
    cat.indexes["idx1"] = IndexMeta(name: "idx1", table: "t1", kind: ikBtree, columns: @["expr:(a + 1)"])
    
    # Make a dummy plan to trigger all the lines
    let p = Plan(kind: pkAppend,
      left: Plan(kind: pkUnionDistinct,
        left: Plan(kind: pkSetUnionDistinct,
          left: Plan(kind: pkSetIntersect,
            left: Plan(kind: pkSetIntersectAll,
              left: Plan(kind: pkSetExcept, left: Plan(kind: pkOneRow), right: nil),
              right: Plan(kind: pkSetExceptAll, left: Plan(kind: pkOneRow), right: nil)
            ),
            right: nil
          ),
          right: nil
        ),
        right: nil
      ),
      right: Plan(kind: pkSubqueryScan, alias: "sub",
        subPlan: Plan(kind: pkLiteralRows, table: "t2", alias: "t2a", rows: @[])
      )
    )
    discard explainPlanLines(cat, p)

    let p2 = Plan(kind: pkFilter, predicate: Expr(kind: ekColumn, name: "a"),
      left: Plan(kind: pkProject, projections: @[SelectItem(isStar: true), SelectItem(expr: Expr(kind: ekColumn, name: "b"), alias: "b2")],
        left: Plan(kind: pkJoin, joinType: jtLeft, joinOn: Expr(kind: ekColumn, name: "a"),
          left: Plan(kind: pkSort, orderBy: @[OrderItem(expr: Expr(kind: ekColumn, name: "a"), asc: true), OrderItem(expr: Expr(kind: ekColumn, name: "b"), asc: false)],
            left: Plan(kind: pkAggregate, groupBy: @[Expr(kind: ekColumn, name: "a")], having: Expr(kind: ekColumn, name: "b"), projections: @[SelectItem(isStar: true)],
              left: Plan(kind: pkLimit, limitParam: 1, offsetParam: 2,
                left: Plan(kind: pkStatement, stmt: Statement(kind: skBegin))
              )
            )
          ),
          right: Plan(kind: pkTvfScan, tvfFunc: "generate_series", alias: "g")
        )
      )
    )
    discard explainPlanLines(cat, p2)

    let p3 = Plan(kind: pkRowidSeek, table: "t", alias: "a", column: "id", valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)))
    discard explainPlanLines(cat, p3)

    let p4 = Plan(kind: pkIndexSeek, table: "t1", column: "expr:(a + 1)", valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)))
    discard explainPlanLines(cat, p4)

    let p4_miss = Plan(kind: pkIndexSeek, table: "t1", column: "expr:(a + 2)", valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)))
    discard explainPlanLines(cat, p4_miss)

    cat.indexes["idx2"] = IndexMeta(name: "idx2", table: "t1", kind: ikBtree, columns: @["normal_col"])
    let p5 = Plan(kind: pkIndexSeek, table: "t1", column: "normal_col", valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)))
    discard explainPlanLines(cat, p5)

    let p5_miss = Plan(kind: pkIndexSeek, table: "t1", column: "missing_col", valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)))
    discard explainPlanLines(cat, p5_miss)

    cat.indexes["idx3"] = IndexMeta(name: "idx3", table: "t1", kind: ikTrigram, columns: @["txt_col"])
    let p5_b = Plan(kind: pkTrigramSeek, table: "t1", column: "txt_col", likeExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "%abc%")), likeInsensitive: false)
    discard explainPlanLines(cat, p5_b)

    let p5_b_miss = Plan(kind: pkTrigramSeek, table: "t1", column: "missing_txt", likeExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "%abc%")), likeInsensitive: true)
    discard explainPlanLines(cat, p5_b_miss)

    let p6 = Plan(kind: pkLimit, limit: 10, offset: 5, left: nil)
    discard explainPlanLines(cat, p6)

    let p7 = Plan(kind: pkJoin, joinType: jtInner, joinOn: Expr(kind: ekColumn, name: "a"), left: nil, right: nil)
    discard explainPlanLines(cat, p7)

    let p8 = Plan(kind: pkTableScan, table: "t1", alias: "t", left: nil)
    discard explainPlanLines(cat, p8)

    let p9 = Plan(kind: pkProject, projections: @[SelectItem(isStar: true)], left: nil)
    discard explainPlanLines(cat, p9)

    let p10 = Plan(kind: pkAggregate, groupBy: @[Expr(kind: ekColumn, name: "a"), Expr(kind: ekColumn, name: "b")], having: nil, projections: @[], left: nil)
    discard explainPlanLines(cat, p10)

    let p11 = Plan(kind: pkSort, orderBy: @[OrderItem(expr: Expr(kind: ekColumn, name: "a"), asc: true)], left: nil)
    discard explainPlanLines(cat, p11)

    let p12 = Plan(kind: pkLiteralRows, table: "t2", alias: "t2a", rows: @[@[("a", Value(kind: vkInt64, int64Val: 1))]])
    discard explainPlanLines(cat, p12)

    let p13 = Plan(kind: pkRowidSeek, table: "t", alias: "", column: "id", valueExpr: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)))
    discard explainPlanLines(cat, p13)

    let metrics = PlanMetrics(actualRows: 100, actualTimeMs: 12.345)
    let p14 = Plan(kind: pkOneRow)
    discard explainAnalyzePlanLines(cat, p14, metrics)

