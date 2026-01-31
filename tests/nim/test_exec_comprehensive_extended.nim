import unittest
import os
import options
import strutils
import tables

import engine
import exec/exec
import sql/sql
import planner/planner
import record/record
import errors

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Exec Comprehensive":
  test "valueToString with all types":
    check valueToString(Value(kind: vkNull)) == "NULL"
    check valueToString(Value(kind: vkBool, boolVal: true)) == "true"
    check valueToString(Value(kind: vkBool, boolVal: false)) == "false"
    check valueToString(Value(kind: vkInt64, int64Val: 42)) == "42"
    check valueToString(Value(kind: vkFloat64, float64Val: 3.14)) == "3.14"
    check valueToString(Value(kind: vkText, bytes: toBytes("hello"))) == "hello"
    check valueToString(Value(kind: vkBlob, bytes: toBytes("world"))) == "world"

  test "likeMatch with exact match":
    check likeMatch("hello", "hello", false)

  test "likeMatch with percent wildcard":
    check likeMatch("hello", "h%", false)
    check likeMatch("hello", "%o", false)
    check likeMatch("hello", "%ell%", false)

  test "likeMatch with underscore wildcard":
    check likeMatch("hello", "h_llo", false)
    check likeMatch("hello", "he_lo", false)
    check likeMatch("hello", "hel_o", false)

  test "likeMatch with combined wildcards":
    check likeMatch("hello", "h%o", false)
    check likeMatch("hello", "h_l_o", false)

  test "likeMatch case sensitivity":
    check likeMatch("Hello", "hello", true)  # Case insensitive
    check not likeMatch("Hello", "hello", false)  # Case sensitive

  test "likeMatch edge cases":
    check likeMatch("", "", false)
    check likeMatch("", "%", false)
    check not likeMatch("", "_", false)  # _ matches exactly one char, so empty string doesn't match
    check likeMatch("a", "_", false)  # Single char should match _

  test "likeMatchChecked with long pattern":
    let longPattern = repeat("a", MaxLikePatternLen + 1)
    let result = likeMatchChecked("test", longPattern, false)
    check not result.ok
    check result.err.message == "LIKE pattern too long"

  test "likeMatchChecked with too many wildcards":
    let manyWildcards = repeat("%", MaxLikeWildcards + 1)
    let result = likeMatchChecked("test", manyWildcards, false)
    check not result.ok
    check result.err.message == "LIKE pattern has too many wildcards"

  test "likeMatchChecked normal case":
    let result = likeMatchChecked("hello", "h%o", false)
    check result.ok
    check result.value == true

  test "makeRow creates proper row":
    let row = makeRow(@["col1", "col2"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: toBytes("test"))], 100)
    check row.rowid == 100
    check row.columns == @["col1", "col2"]
    check row.values.len == 2

  test "rowCursorColumns with nil cursor":
    let cols = rowCursorColumns(nil)
    let expected: seq[string] = @[]
    check cols == expected

  test "valueToBool conversions":
    check valueToBool(Value(kind: vkBool, boolVal: true)) == true
    check valueToBool(Value(kind: vkBool, boolVal: false)) == false
    check valueToBool(Value(kind: vkInt64, int64Val: 1)) == true
    check valueToBool(Value(kind: vkInt64, int64Val: 0)) == false
    check valueToBool(Value(kind: vkInt64, int64Val: -1)) == true
    check valueToBool(Value(kind: vkFloat64, float64Val: 1.0)) == true
    check valueToBool(Value(kind: vkFloat64, float64Val: 0.0)) == false
    check valueToBool(Value(kind: vkText, bytes: toBytes("hello"))) == true
    check valueToBool(Value(kind: vkText, bytes: toBytes(""))) == false
    check valueToBool(Value(kind: vkBlob, bytes: toBytes("data"))) == true
    check valueToBool(Value(kind: vkBlob, bytes: toBytes(""))) == false
    check valueToBool(Value(kind: vkNull)) == false

  test "compareValues with same types":
    check compareValues(Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 2)) < 0
    check compareValues(Value(kind: vkInt64, int64Val: 2), Value(kind: vkInt64, int64Val: 1)) > 0
    check compareValues(Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 1)) == 0
    check compareValues(Value(kind: vkText, bytes: toBytes("a")), Value(kind: vkText, bytes: toBytes("b"))) < 0
    check compareValues(Value(kind: vkText, bytes: toBytes("b")), Value(kind: vkText, bytes: toBytes("a"))) > 0
    check compareValues(Value(kind: vkText, bytes: toBytes("a")), Value(kind: vkText, bytes: toBytes("a"))) == 0

  test "varintLen basic":
    check varintLen(0) == 1
    check varintLen(127) == 1  # 0x7F
    check varintLen(128) == 2  # 0x80
    check varintLen(0x4000) == 3  # 16384

  test "estimateRowBytes":
    let row = makeRow(@["col1"], @[Value(kind: vkInt64, int64Val: 42)])
    let size = estimateRowBytes(row)
    check size > 0

  test "columnIndex simple match":
    let row = makeRow(@["table.col"], @[Value(kind: vkInt64, int64Val: 1)])
    let idx = columnIndex(row, "table", "col")
    check idx.ok
    check idx.value == 0

  test "columnIndex qualified match":
    let row = makeRow(@["t.col"], @[Value(kind: vkInt64, int64Val: 1)])
    let idx = columnIndex(row, "t", "col")
    check idx.ok
    check idx.value == 0

  test "columnIndex unqualified match":
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let idx = columnIndex(row, "", "col")
    check idx.ok
    check idx.value == 0

  test "columnIndex no match":
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let idx = columnIndex(row, "", "other")
    check not idx.ok

  test "applyLimit basic":
    let rows = @[
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 2)]),
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 3)])
    ]
    let limited = applyLimit(rows, 2, 0)
    check limited.len == 2

  test "applyLimit with offset":
    let rows = @[
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 2)]),
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 3)])
    ]
    let limited = applyLimit(rows, -1, 1)
    check limited.len == 2
    check limited[0].values[0].int64Val == 2

  test "applyLimit with zero limit":
    let rows = @[
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 2)])
    ]
    let limited = applyLimit(rows, 0, 0)
    check limited.len == 0

  test "applyLimit with offset greater than rows":
    let rows = @[
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 1)])
    ]
    let limited = applyLimit(rows, -1, 5)
    check limited.len == 0

  test "applyLimit with negative offset":
    let rows = @[
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(@["val"], @[Value(kind: vkInt64, int64Val: 2)])
    ]
    let limited = applyLimit(rows, -1, -1)
    check limited.len == 2  # Should treat negative offset as 0

  test "evalExpr with NULL literal":
    let expr = Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkNull

  test "evalExpr with boolean literal":
    let expr = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true))
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with integer literal":
    let expr = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 42))
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkInt64
    check result.value.int64Val == 42

  test "evalExpr with float literal":
    let expr = Expr(kind: ekLiteral, value: SqlValue(kind: svFloat, floatVal: 3.14))
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkFloat64
    check result.value.float64Val == 3.14

  test "evalExpr with string literal":
    let expr = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "hello"))
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkText
    check valueToString(result.value) == "hello"

  test "evalExpr with parameter":
    let expr = Expr(kind: ekParam, index: 1)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let params = @[Value(kind: vkInt64, int64Val: 99)]
    let result = evalExpr(row, expr, params)
    check result.ok
    check result.value.kind == vkInt64
    check result.value.int64Val == 99

  test "evalExpr with missing parameter":
    let expr = Expr(kind: ekParam, index: 2)  # Index 2, but only 1 param
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let params = @[Value(kind: vkInt64, int64Val: 99)]
    let result = evalExpr(row, expr, params)
    check not result.ok
    check result.err.message == "Missing parameter"

  test "evalExpr with column reference":
    let expr = Expr(kind: ekColumn, table: "", name: "col")
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 42)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkInt64
    check result.value.int64Val == 42

  test "evalExpr with table-qualified column":
    let expr = Expr(kind: ekColumn, table: "t", name: "col")
    let row = makeRow(@["t.col"], @[Value(kind: vkInt64, int64Val: 42)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkInt64
    check result.value.int64Val == 42

  test "evalExpr with unknown column":
    let expr = Expr(kind: ekColumn, table: "", name: "missing")
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 42)])
    let result = evalExpr(row, expr, @[])
    check not result.ok

  test "evalExpr with NOT operator":
    let inner = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true))
    let expr = Expr(kind: ekUnary, unOp: "NOT", expr: inner)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == false

  test "evalExpr with AND operator":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: false))
    let expr = Expr(kind: ekBinary, op: "AND", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == false

  test "evalExpr with OR operator":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: false))
    let expr = Expr(kind: ekBinary, op: "OR", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with comparison operators":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 5))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 3))
    
    # Test equals
    let eqExpr = Expr(kind: ekBinary, op: "=", left: left, right: right)
    let eqResult = evalExpr(makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)]), eqExpr, @[])
    check eqResult.ok
    check eqResult.value.kind == vkBool
    check eqResult.value.boolVal == false
    
    # Test less than
    let ltExpr = Expr(kind: ekBinary, op: "<", left: left, right: right)
    let ltResult = evalExpr(makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)]), ltExpr, @[])
    check ltResult.ok
    check ltResult.value.kind == vkBool
    check ltResult.value.boolVal == false
    
    # Test greater than
    let gtExpr = Expr(kind: ekBinary, op: ">", left: left, right: right)
    let gtResult = evalExpr(makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)]), gtExpr, @[])
    check gtResult.ok
    check gtResult.value.kind == vkBool
    check gtResult.value.boolVal == true

  test "evalExpr with IS NULL":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
    let expr = Expr(kind: ekBinary, op: "IS", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with IS NOT NULL":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 5))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svNull))
    let expr = Expr(kind: ekBinary, op: "IS NOT", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with LIKE":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "hello"))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "h%o"))
    let expr = Expr(kind: ekBinary, op: "LIKE", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with ILIKE":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "Hello"))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "h%o"))
    let expr = Expr(kind: ekBinary, op: "ILIKE", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with true/false keywords":
    let trueExpr = Expr(kind: ekColumn, name: "true")  # Should be treated as literal true
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, trueExpr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true

  test "evalExpr with nil expression":
    let result = evalExpr(makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)]), nil, @[])
    check result.ok
    check result.value.kind == vkNull

  test "evalExpr with addition operator":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 5))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 3))
    let expr = Expr(kind: ekBinary, op: "+", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check result.ok
    check result.value.kind == vkInt64
    check result.value.int64Val == 8

  test "evalExpr with division by zero":
    let left = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 5))
    let right = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 0))
    let expr = Expr(kind: ekBinary, op: "/", left: left, right: right)
    let row = makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])
    let result = evalExpr(row, expr, @[])
    check not result.ok
    check result.err.message == "Division by zero"

  test "applyFilter with nil predicate":
    let rows = @[makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])]
    let emptyParams: seq[Value] = @[]
    let result = applyFilter(rows, nil, emptyParams)
    check result.ok
    check result.value == rows

  test "applyFilter with matching predicate":
    let rows = @[makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])]
    let predicate = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: true))
    let result = applyFilter(rows, predicate, @[])
    check result.ok
    check result.value.len == 1

  test "applyFilter with non-matching predicate":
    let rows = @[makeRow(@["col"], @[Value(kind: vkInt64, int64Val: 1)])]
    let predicate = Expr(kind: ekLiteral, value: SqlValue(kind: svBool, boolVal: false))
    let result = applyFilter(rows, predicate, @[])
    check result.ok
    check result.value.len == 0

  test "projectRows with star":
    let row = makeRow(@["col1", "col2"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 2)])
    let items = @[SelectItem(isStar: true)]
    let result = projectRows(@[row], items, @[])
    check result.ok
    check result.value.len == 1
    check result.value[0].columns == @["col1", "col2"]
    check result.value[0].values.len == 2

  test "projectRows with empty items":
    let row = makeRow(@["col1"], @[Value(kind: vkInt64, int64Val: 1)])
    let emptyItems: seq[SelectItem] = @[]
    let emptyParams: seq[Value] = @[]
    let result = projectRows(@[row], emptyItems, emptyParams)
    check result.ok
    let expected = @[row]
    check result.value == expected

  test "execPlan with pkStatement":
    let plan = Plan(kind: pkStatement)
    let path = makeTempDb("decentdb_exec_plan_stmt.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let result = execPlan(db.pager, db.catalog, plan, @[])
    check result.ok
    check result.value.len == 0
    discard closeDb(db)