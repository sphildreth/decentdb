import unittest
import os
import strutils
import options
import engine
import catalog/catalog
import sql/sql

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine execPreparedNonSelect Error Paths":
  test "skCreateTable invalid column type":
    let path = makeTempDb("execpns_ct_invalid.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    
    let stmt = Statement(
      kind: skCreateTable,
      createTableName: "t_bad",
      columns: @[ColumnDef(name: "id", typeName: "INVALID_TYPE")],
      createTableIsTemp: false
    )
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok

    let stmtTemp = Statement(
      kind: skCreateTable,
      createTableName: "t_bad_temp",
      columns: @[ColumnDef(name: "id", typeName: "INVALID_TYPE")],
      createTableIsTemp: true
    )
    let resTemp = execPreparedNonSelect(db, stmtTemp, @[])
    check not resTemp.ok
    discard closeDb(db)

  test "skAlterTable invalid action":
    let path = makeTempDb("execpns_alter_invalid.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")

    let stmt = Statement(
      kind: skAlterTable,
      alterTableName: "t_nonexistent",
      alterActions: @[AlterTableAction(kind: ataAddColumn, columnDef: ColumnDef(name: "new_col", typeName: "INT"))]
    )
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    discard closeDb(db)

  test "skCreateIndex invalid table":
    let path = makeTempDb("execpns_createidx_invalid.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    
    let stmt = Statement(
      kind: skCreateIndex,
      indexName: "idx_bad",
      indexTableName: "t_nonexistent",
      columnNames: @["id"],
      unique: true
    )
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    discard closeDb(db)

  test "execPreparedNonSelect with SELECT":
    let path = makeTempDb("execpns_select.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    
    let stmt = Statement(
      kind: skSelect,
      selectItems: @[]
    )
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    check res.err.message == "execPreparedNonSelect called with SELECT"
    discard closeDb(db)
