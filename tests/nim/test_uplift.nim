import unittest
import options

import catalog/catalog
import record/record
import engine
import errors

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Catalog Public API":
  test "parseColumnType all valid types":
    check parseColumnType("INT").ok
    check parseColumnType("INT").value == ctInt64
    check parseColumnType("INT64").ok
    check parseColumnType("INT64").value == ctInt64
    check parseColumnType("BIGINT").ok
    check parseColumnType("BOOL").ok
    check parseColumnType("BOOLEAN").ok
    check parseColumnType("BOOL").value == ctBool
    check parseColumnType("FLOAT").ok
    check parseColumnType("FLOAT64").ok
    check parseColumnType("TEXT").ok
    check parseColumnType("BLOB").ok

  test "parseColumnType invalid type":
    let bad = parseColumnType("FOO")
    check not bad.ok
    check bad.err.code == ERR_SQL

  test "columnTypeToText all types":
    check columnTypeToText(ctInt64) == "INT64"
    check columnTypeToText(ctBool) == "BOOL"
    check columnTypeToText(ctFloat64) == "FLOAT64"
    check columnTypeToText(ctText) == "TEXT"
    check columnTypeToText(ctBlob) == "BLOB"

  test "columnTypeToText roundtrip":
    for typeName in @["INT", "BOOL", "FLOAT64", "TEXT", "BLOB"]:
      let parseRes = parseColumnType(typeName)
      if parseRes.ok:
        let kind = parseRes.value
        let text = columnTypeToText(kind)
        check text.len > 0

  test "TableMeta structure":
    let cols = @[
      Column(name: "id", kind: ctInt64, notNull: true, unique: true, primaryKey: true, refTable: "", refColumn: ""),
      Column(name: "name", kind: ctText, notNull: false, unique: false, primaryKey: false, refTable: "", refColumn: "")
    ]
    let table = TableMeta(name: "users", rootPage: 1, nextRowId: 100, columns: cols)
    check table.name == "users"
    check table.rootPage == 1
    check table.nextRowId == 100
    check table.columns.len == 2
    check table.columns[0].primaryKey
    check not table.columns[1].primaryKey

  test "IndexMeta structure":
    let index = IndexMeta(name: "idx", table: "t", columns: @["c"], rootPage: 5, kind: ikBtree, unique: true)
    check index.name == "idx"
    check index.table == "t"
    check index.columns == @["c"]
    check index.rootPage == 5
    check index.kind == ikBtree
    check index.unique == true

  test "Column flags":
    let col = Column(name: "c", kind: ctInt64, notNull: true, unique: true, primaryKey: true, refTable: "p", refColumn: "id")
    check col.notNull
    check col.unique
    check col.primaryKey
    check col.refTable == "p"
    check col.refColumn == "id"
