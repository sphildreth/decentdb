import unittest
import os
import tables

import engine
import sql/sql
import sql/binder
import catalog/catalog
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc addTable(db: Db, name: string, columns: seq[Column]): TableMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = TableMeta(name: name, rootPage: rootRes.value, nextRowId: 1, columns: columns)
  check db.catalog.saveTable(db.pager, meta).ok
  meta

proc addView(db: Db, name: string, sqlText: string, columnNames: seq[string]): ViewMeta =
  let meta = ViewMeta(name: name, sqlText: sqlText, columnNames: columnNames)
  check db.catalog.createViewMeta(meta).ok
  meta

proc parseSingle(sqlText: string): Statement =
  let astRes = parseSql(sqlText)
  check astRes.ok
  check astRes.value.statements.len == 1
  astRes.value.statements[0]

suite "Binder Reproduction":
  test "Views_CanBeMapped reproduction":
    let path = makeTempDb("decentdb_binder_repro_view.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table t1 (id int, val text)
    discard addTable(db, "t1", @[
      Column(name: "id", kind: ctInt64),
      Column(name: "val", kind: ctText)
    ])

    # Create view v_t1 AS SELECT * FROM t1
    # NOTE: In real execution, CREATE VIEW command binds the query to get columns.
    # Here we simulate what catalog would have.
    # The failing test executes: db.Execute("CREATE VIEW v_t1 AS SELECT * FROM t1");
    # So the catalog should have correct column names.
    discard addView(db, "v_t1", "SELECT * FROM t1", @["id", "val"])

    # Test query: SELECT * FROM v_t1
    let ast = parseSingle("SELECT * FROM v_t1")
    let bindRes = bindStatement(db.catalog, ast)
    
    if not bindRes.ok:
      echo "Views_CanBeMapped Failed: ", bindRes.err.message, " ", bindRes.err.context
    check bindRes.ok
    
    # db.close()

  test "CommonTableExpressions_Supported reproduction":
    let path = makeTempDb("decentdb_binder_repro_cte.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table t1
    discard addTable(db, "t1", @[
      Column(name: "id", kind: ctInt64),
      Column(name: "val", kind: ctText)
    ])

    # Test query: WITH cte_source AS (SELECT id, val FROM t1 WHERE id > 0) SELECT * FROM cte_source ORDER BY id
    let sql = """
      WITH cte_source AS (
          SELECT id, val FROM t1 WHERE id > 0
      )
      SELECT * FROM cte_source ORDER BY id
    """
    let ast = parseSingle(sql)
    let bindRes = bindStatement(db.catalog, ast)
    
    if not bindRes.ok:
      echo "CommonTableExpressions_Supported Failed: ", bindRes.err.message, " ", bindRes.err.context
    check bindRes.ok
    
    # db.close()

  test "SetOp View Output Column Names":
     # This tests the fix I made conceptually for UNION view columns
     let path = makeTempDb("decentdb_binder_repro_union.db")
     let dbRes = openDb(path)
     check dbRes.ok
     let db = dbRes.value

     discard addTable(db, "t1", @[
       Column(name: "a", kind: ctInt64)
     ])
     
     # View defined as union
     # We want to ensure resolving column names works
     discard addView(db, "v_union", "SELECT a FROM t1 UNION SELECT a FROM t1", @["a"])

     let ast = parseSingle("SELECT a FROM v_union")
     let bindRes = bindStatement(db.catalog, ast)
     if not bindRes.ok:
        echo "SetOp View Failed: ", bindRes.err.message
     check bindRes.ok

  test "CommonTableExpressions_Supported reproduction UNION":
    let path = makeTempDb("decentdb_binder_repro_cte_union.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Create table t1
    discard addTable(db, "t1", @[
      Column(name: "x", kind: ctInt64)
    ])

    # Test query: UNION CTE
    # WITH cte_source(x) AS (SELECT x FROM t1 UNION SELECT x FROM t1) SELECT x FROM cte_source
    let sql = """
      WITH cte_source AS (
          SELECT x FROM t1
          UNION
          SELECT x FROM t1
      )
      SELECT x FROM cte_source
    """
    let ast = parseSingle(sql)
    let bindRes = bindStatement(db.catalog, ast)
    
    if not bindRes.ok:
      echo "CommonTableExpressions_Supported UNION Failed: ", bindRes.err.message
    check bindRes.ok
    
    # db.close()

  test "Views_CanBeMapped Explicit Alias":
    let path = makeTempDb("decentdb_binder_repro_view_alias.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "items", @[
      Column(name: "id", kind: ctInt64),
      Column(name: "name", kind: ctText),
      Column(name: "value", kind: ctInt64)
    ])
    
    # CREATE VIEW items_view AS SELECT id, name, value * 2 AS val_doubled FROM items WHERE value > 10
    # Note: we need to setup the view meta manually as if it was created by engine
    # The output columns of the query are id, name, val_doubled.
    # The view definition query is stored as string.
    # But for expansion, we parse it.
    
    discard addView(db, "items_view", "SELECT id, name, value * 2 AS val_doubled FROM items WHERE value > 10", @["id", "name", "val_doubled"])

    # Query: SELECT id, name, val_doubled FROM items_view ORDER BY id
    let ast = parseSingle("SELECT id, name, val_doubled FROM items_view ORDER BY id")
    let bindRes = bindStatement(db.catalog, ast)
    
    if not bindRes.ok:
      echo "Views_CanBeMapped Alias Failed: ", bindRes.err.message, " ", bindRes.err.context
    check bindRes.ok
    
    # db.close()

  test "CommonTableExpressions_Supported reproduction Dotnet Exact":
    let path = makeTempDb("decentdb_binder_repro_cte_dotnet.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    # Test query from Dotnet test
    let sql = """
            WITH cte_source(x) AS (
              SELECT 1
              UNION ALL
              SELECT 2
              UNION ALL
              SELECT 3
            )
            SELECT x FROM cte_source WHERE x > 1 ORDER BY x
    """
    let ast = parseSingle(sql)
    let bindRes = bindStatement(db.catalog, ast)
    
    if not bindRes.ok:
      echo "CommonTableExpressions Dotnet Failed: ", bindRes.err.message
    check bindRes.ok
    
    # db.close()

  test "Views_CanBeMapped Full Cycle":
    let path = makeTempDb("decentdb_binder_repro_full_cycle.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "items", @[
      Column(name: "id", kind: ctInt64),
      Column(name: "name", kind: ctText),
      Column(name: "value", kind: ctInt64)
    ])

    let createViewSql = "CREATE VIEW items_view AS SELECT id, name, value * 2 AS val_doubled FROM items WHERE value > 10"
    let createAst = parseSingle(createViewSql)
    
    # Bind CREATE VIEW
    let boundCreateRes = bindStatement(db.catalog, createAst)
    if not boundCreateRes.ok:
      echo "FullCycle CREATE VIEW Failed: ", boundCreateRes.err.message
    check boundCreateRes.ok
    let boundCreate = boundCreateRes.value
    
    # Verify columns detected
    check boundCreate.createViewColumns == @["id", "name", "val_doubled"]
    
    # Simulate saving to catalog (engine does this)
    let viewMeta = ViewMeta(
        name: boundCreate.createViewName, 
        sqlText: boundCreate.createViewSqlText, 
        columnNames: boundCreate.createViewColumns
    )
    check db.catalog.createViewMeta(viewMeta).ok
    
    # Now Bind SELECT
    let selectSql = "SELECT id, name, val_doubled FROM items_view ORDER BY id"
    let selectAst = parseSingle(selectSql)
    let bindRes = bindStatement(db.catalog, selectAst)
    
    if not bindRes.ok:
       echo "FullCycle SELECT Failed: ", bindRes.err.message, " ", bindRes.err.context
    check bindRes.ok


     # db.close()
