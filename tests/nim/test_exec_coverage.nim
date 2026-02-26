import unittest
import os
import options
import strutils

import engine
import exec/exec
import record/record
import sql/sql

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Exec UUID Functions":
  test "parseUuid valid":
    let result = parseUuid("550e8400-e29b-41d4-a716-446655440000")
    require result.ok
    check result.value.len == 16

  test "parseUuid invalid":
    let result = parseUuid("not-a-uuid")
    check not result.ok

suite "Exec Value Functions":
  test "valueToString for int64":
    let val = Value(kind: vkInt64, int64Val: 42)
    let result = valueToString(val)
    check result == "42"

  test "valueToString for text":
    let val = Value(kind: vkText, bytes: toBytes("hello"))
    let result = valueToString(val)
    check result == "hello"

  test "valueToString for NULL":
    let val = Value(kind: vkNull)
    let result = valueToString(val)
    check result == "NULL"

suite "Exec Like Functions":
  test "likeMatch basic":
    check likeMatch("hello", "hel%", false) == true
    check likeMatch("hello", "world%", false) == false

  test "likeMatch with underscore":
    check likeMatch("abc", "a_c", false) == true
    check likeMatch("abcd", "a_c", false) == false

  test "likeMatch case insensitive":
    check likeMatch("HELLO", "hello%", true) == true
    check likeMatch("HELLO", "hello%", false) == false

  test "likeMatchChecked success":
    let res = likeMatchChecked("hello", "hel%", false)
    require res.ok
    check res.value == true

  test "likeMatchChecked failure":
    let res = likeMatchChecked("hello", "xyz%", false)
    require res.ok
    check res.value == false

suite "Exec Value Comparison":
  test "compareValues int64 equal":
    let a = Value(kind: vkInt64, int64Val: 42)
    let b = Value(kind: vkInt64, int64Val: 42)
    check compareValues(a, b) == 0

  test "compareValues int64 less":
    let a = Value(kind: vkInt64, int64Val: 10)
    let b = Value(kind: vkInt64, int64Val: 20)
    check compareValues(a, b) < 0

  test "compareValues text":
    let a = Value(kind: vkText, bytes: toBytes("apple"))
    let b = Value(kind: vkText, bytes: toBytes("banana"))
    check compareValues(a, b) < 0

  test "compareValues NULL vs NULL":
    let a = Value(kind: vkNull)
    let b = Value(kind: vkNull)
    check compareValues(a, b) == 0

suite "Exec Value To Bool":
  test "valueToBool true":
    let val = Value(kind: vkBool, boolVal: true)
    check valueToBool(val) == true

  test "valueToBool int":
    let val = Value(kind: vkInt64, int64Val: 1)
    check valueToBool(val) == true

  test "valueToBool zero":
    let val = Value(kind: vkInt64, int64Val: 0)
    check valueToBool(val) == false

suite "Exec Hash":
  test "hash int64":
    let val = Value(kind: vkInt64, int64Val: 42)
    let h = hash(val)
    check h != 0

  test "hash text":
    let val = Value(kind: vkText, bytes: toBytes("hello"))
    let h = hash(val)
    check h != 0

  test "hash consistency":
    let val = Value(kind: vkInt64, int64Val: 42)
    check hash(val) == hash(val)

suite "Exec Row Functions":
  test "makeRow":
    let row = makeRow(@["id", "name"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("test"))])
    check row.columns.len == 2
    check row.columns[0] == "id"
    check row.values.len == 2

  test "columnIndex exact table":
    let row = makeRow(@["t.id", "name"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("test"))])
    let res = columnIndex(row, "t", "id")
    require res.ok
    check res.value == 0

  test "columnIndex no table":
    let row = makeRow(@["id", "name"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("test"))])
    let res = columnIndex(row, "", "name")
    require res.ok
    check res.value == 1

  test "columnIndex not found":
    let row = makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 1)])
    let res = columnIndex(row, "", "nonexistent")
    check not res.ok

suite "Exec Apply Limit":
  test "applyLimit with rows":
    let rows = @[
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 2)]),
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 3)])
    ]
    let limited = applyLimit(rows, 2, 0)
    check limited.len == 2

  test "applyLimit with offset":
    let rows = @[
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 2)]),
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 3)])
    ]
    let result = applyLimit(rows, 10, 1)
    check result.len == 2

  test "applyLimit with limit 0":
    let rows = @[
      makeRow(@["id"], @[Value(kind: vkInt64, int64Val: 1)])
    ]
    let limited = applyLimit(rows, 0, 0)
    check limited.len == 0

suite "Exec Varint":
  test "varintLen":
    check varintLen(0) == 1
    check varintLen(127) == 1
    check varintLen(128) == 2
    check varintLen(16383) == 2
    check varintLen(16384) == 3

suite "Exec Row Estimate":
  test "estimateRowBytes":
    let row = makeRow(@["id", "name"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("test"))])
    let size = estimateRowBytes(row)
    check size > 0
