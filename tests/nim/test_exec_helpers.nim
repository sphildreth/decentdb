import unittest
import os
import options
import strutils
import sql/sql
import record/record
import exec/exec

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

suite "Exec Helpers Extended":
  test "varint lengths and row sizing":
    check varintLen(0) == 1
    check varintLen(0x80'u64) == 2
    let row = makeRow(@["t.val"], @[Value(kind: vkInt64, int64Val: 123)])
    let size = estimateRowBytes(row)
    check size > 0

  test "columnIndex resolves tables and reports ambiguity":
    let row = makeRow(@["t.a", "a"], @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 2)])
    let exact = columnIndex(row, "t", "a")
    check exact.ok
    check exact.value == 0
    let ambiguous = columnIndex(row, "", "a")
    check not ambiguous.ok
    check ambiguous.err.message == "Ambiguous column"

  test "evalExpr handles literals, columns, comparisons, LIKE, and errors":
    let row = makeRow(@["t.num", "tag"], @[Value(kind: vkInt64, int64Val: 5), Value(kind: vkText, bytes: toBytes("abc"))])
    let lit = Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 5))
    let litRes = evalExpr(row, lit, @[])
    check litRes.ok
    check litRes.value.kind == vkInt64

    let col = Expr(kind: ekColumn, table: "t", name: "num")
    let eq = Expr(kind: ekBinary, op: "=", left: col, right: lit)
    let eqRes = evalExpr(row, eq, @[])
    check eqRes.ok
    check eqRes.value.boolVal

    let likePattern = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: "%B%"))
    let likeExpr = Expr(kind: ekBinary, op: "LIKE", left: Expr(kind: ekColumn, table: "", name: "tag"), right: likePattern)
    let likeRes = evalExpr(row, likeExpr, @[])
    check likeRes.ok
    check not likeRes.value.boolVal

    let ilikeExpr = Expr(kind: ekBinary, op: "ILIKE", left: Expr(kind: ekColumn, table: "", name: "tag"), right: likePattern)
    let ilikeRes = evalExpr(row, ilikeExpr, @[])
    check ilikeRes.ok
    check ilikeRes.value.boolVal

    let unsupported = Expr(kind: ekBinary, op: "XOR", left: col, right: lit)
    let unsupportedRes = evalExpr(row, unsupported, @[])
    check not unsupportedRes.ok
    check unsupportedRes.err.message == "Unsupported operator"

  test "LIKE guardrails reject very long patterns":
    let row = makeRow(@["tag"], @[Value(kind: vkText, bytes: toBytes("abc"))])
    let longPattern = repeat("%", MaxLikePatternLen + 1)
    let likePattern = Expr(kind: ekLiteral, value: SqlValue(kind: svString, strVal: longPattern))
    let likeExpr = Expr(kind: ekBinary, op: "LIKE", left: Expr(kind: ekColumn, table: "", name: "tag"), right: likePattern)
    let likeRes = evalExpr(row, likeExpr, @[])
    check not likeRes.ok
    check likeRes.err.message.contains("LIKE pattern too long")

  test "applyFilter removes rows and surfaces evalExpr errors":
    let columns = @["id"]
    let rows = @[
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 2)])
    ]
    let predicate = Expr(kind: ekBinary, op: "=", left: Expr(kind: ekColumn, name: "id"), right: Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2)))
    let filtered = applyFilter(rows, predicate, @[])
    check filtered.ok
    check filtered.value.len == 1
    let badPredicate = Expr(kind: ekParam, index: 1)
    let error = applyFilter(rows, badPredicate, @[])
    check not error.ok

  test "projectRows handles explicit columns and star":
    let columns = @["id", "name"]
    let baseRow = makeRow(columns, @[Value(kind: vkInt64, int64Val: 7), Value(kind: vkText, bytes: toBytes("T"))])
    let selectItems = @[
      SelectItem(expr: Expr(kind: ekColumn, name: "id"), alias: "identifier"),
      SelectItem(expr: nil, isStar: true)
    ]
    let projected = projectRows(@[baseRow], selectItems, @[])
    check projected.ok
    check projected.value[0].columns[0] == "identifier"
    check projected.value[0].columns.len == 3

  test "aggregateRows honors COUNT/SUM aggregation and errors":
    let columns = @["group", "value"]
    let rows = @[
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 2)]),
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkInt64, int64Val: 3)]),
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkInt64, int64Val: 1)])
    ]
    let groupBy = @[Expr(kind: ekColumn, name: "group")]
    let countFunc = Expr(kind: ekFunc, funcName: "COUNT", args: @[])
    let sumFunc = Expr(kind: ekFunc, funcName: "SUM", args: @[Expr(kind: ekColumn, name: "value")])
    let agg = aggregateRows(rows, @[SelectItem(expr: countFunc), SelectItem(expr: sumFunc)], groupBy, nil, @[])
    check agg.ok
    check agg.value.len == 2
    var sawSum5 = false
    var sawSum1 = false
    for row in agg.value:
      let sumIdx = columnIndex(row, "", "sum")
      check sumIdx.ok
      let sumVal = row.values[sumIdx.value].float64Val
      if sumVal == 5.0:
        sawSum5 = true
      elif sumVal == 1.0:
        sawSum1 = true
    check sawSum5 and sawSum1

    let badHaving = Expr(kind: ekBinary, op: "=", left: Expr(kind: ekColumn, name: "sum"), right: Expr(kind: ekParam, index: 1))
    let error = aggregateRows(rows, @[SelectItem(expr: countFunc), SelectItem(expr: sumFunc)], groupBy, badHaving, @[])
    check not error.ok
    check error.err.message == "Missing parameter"

  test "chunk reader reads written rows and handles EOF":
    let path = getTempDir() / "decentdb_exec_chunk.tmp"
    if fileExists(path):
      removeFile(path)
    let columns = @["col"]
    let rows = @[
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 2)])
    ]
    writeRowChunk(path, rows)
    let reader = openChunkReader(path, columns)
    let first = next(reader)
    check first != none(Row)
    check first.get.values[0].int64Val == 1
    let second = next(reader)
    check second != none(Row)
    check second.get.values[0].int64Val == 2
    let done = next(reader)
    check done == none(Row)
    reader.close()
    removeFile(path)

  test "sortRows enforces order and applyLimit slices":
    let columns = @["score"]
    let rows = @[
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 1)]),
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 3)]),
      makeRow(columns, @[Value(kind: vkInt64, int64Val: 2)])
    ]
    let order = @[OrderItem(expr: Expr(kind: ekColumn, name: "score"), asc: false)]
    let sorted = sortRows(rows, order, @[])
    check sorted.ok
    check sorted.value[0].values[0].int64Val == 3
    let limited = applyLimit(sorted.value, 2, 1)
    check limited.len == 2

  test "sortRowsWithConfig supports multi-pass merge and limit pushdown":
    proc listTempWithPrefix(prefix: string): seq[string] =
      let dir = getTempDir()
      for kind, path in walkDir(dir):
        if kind == pcFile and path.extractFilename.startsWith(prefix):
          result.add(path)

    let tempPrefix = "decentdb_sort_test_"
    let before = listTempWithPrefix(tempPrefix)

    let columns = @["score", "payload"]
    var rows: seq[Row] = @[]
    for i in 1 .. 25:
      var payload = ""
      for _ in 0 ..< 120:
        payload.add('x')
      rows.add(makeRow(columns, @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: toBytes(payload))
      ]))

    let order = @[OrderItem(expr: Expr(kind: ekColumn, name: "score"), asc: false)]
    let sortedLimited = sortRowsWithConfig(rows, order, @[], limit = 5, offset = 3, bufferBytes = 256, maxOpenRuns = 4, tempPrefix = tempPrefix)
    check sortedLimited.ok
    check sortedLimited.value.len == 5
    # Desc sort: [25,24,23,22,21,20,...], then offset=3 => starts at 22
    check sortedLimited.value[0].values[0].int64Val == 22
    check sortedLimited.value[1].values[0].int64Val == 21
    check sortedLimited.value[4].values[0].int64Val == 18

    let after = listTempWithPrefix(tempPrefix)
    for path in after:
      check path in before
