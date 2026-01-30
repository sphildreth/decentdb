import unittest

import catalog/catalog
import record/record
import engine

suite "Coverage Uplift":
  test "parseColumnType and columnTypeToText roundtrip":
    check parseColumnType("INT").ok and parseColumnType("INT").value == ctInt64
    check parseColumnType("text").ok and columnTypeToText(parseColumnType("text").value) == "TEXT"
    let bad = parseColumnType("FOO")
    check not bad.ok

  test "encode/decode columns with flags":
    let cols = @[
      Column(name: "id", kind: ctInt64, notNull: true, unique: true, primaryKey: true, refTable: "", refColumn: ""),
      Column(name: "parent", kind: ctInt64, notNull: false, unique: false, primaryKey: false, refTable: "p", refColumn: "id")
    ]
    let bytes = encodeColumns(cols)
    let decoded = decodeColumns(bytes)
    check decoded.len == 2
    check decoded[0].primaryKey
    check decoded[1].refTable == "p" and decoded[1].refColumn == "id"

  test "make and parse catalog records (table)":
    let cols = @[Column(name: "a", kind: ctText, notNull: false, unique: false, primaryKey: false, refTable: "", refColumn: "")]
    let tblBytes = makeTableRecord("t", 5, 42, cols)
    let parsed = parseCatalogRecord(tblBytes)
    check parsed.ok
    check parsed.value.kind == crTable
    check parsed.value.table.name == "t"

  test "make and parse catalog records (index)":
    let idxBytes = makeIndexRecord("ix", "t", "a", 7, ikBtree, true)
    let parsed2 = parseCatalogRecord(idxBytes)
    check parsed2.ok
    check parsed2.value.kind == crIndex
    check parsed2.value.index.name == "ix"

  test "parseCatalogRecord errors on short data":
    let short = @[byte(1), byte(2), byte(3)]
    let p = parseCatalogRecord(short)
    check not p.ok

  test "varint decode errors":
    var off = 0
    # unexpected end
    let r1 = decodeVarint(@[], off)
    check not r1.ok
    # overflow: construct many continuation bytes
    var data: seq[byte] = @[]
    for i in 0 ..< 11:
      data.add(0x80.byte)
    off = 0
    let r2 = decodeVarint(data, off)
    check not r2.ok
    check r2.err.message.find("Varint overflow") >= 0

  test "decodeValue unknown kind and invalid lengths":
    var off = 0
    let unknown = @[byte(255)]
    let v1 = decodeValue(unknown, off)
    check not v1.ok
    # invalid bool length
    off = 0
    let badBool = @[byte(vkBool), 2, 0, 0]
    let v2 = decodeValue(badBool, off)
    check not v2.ok
    check v2.err.message.find("Invalid BOOL length") >= 0
    # invalid int64 length
    off = 0
    let badInt = @[byte(vkInt64), 1, 0]
    let v3 = decodeValue(badInt, off)
    check not v3.ok
    check v3.err.message.find("Invalid INT64 length") >= 0

  test "decodeRecord invalid count":
    let dr = decodeRecord(@[])
    check not dr.ok

  test "engine typeCheckValue and valuesEqual":
    check typeCheckValue(ctFloat64, Value(kind: vkInt64, int64Val: 5)).ok
    check not typeCheckValue(ctInt64, Value(kind: vkText)).ok
    check valuesEqual(Value(kind: vkNull), Value(kind: vkNull))
    check valuesEqual(Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 1))
    check not valuesEqual(Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 2))

  test "enforceNotNull detects violation":
    let table = TableMeta(name: "t", rootPage: 1, nextRowId: 1, columns: @[Column(name: "c", kind: ctInt64, notNull: true, unique: false, primaryKey: false, refTable: "", refColumn: "")])
    let res = enforceNotNull(table, @[Value(kind: vkNull)])
    check not res.ok
    check res.err.message.find("NOT NULL") >= 0


