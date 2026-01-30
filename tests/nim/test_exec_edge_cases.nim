import unittest
import exec/exec
import errors
import record/record

suite "Exec Edge Cases":
  test "applyLimit with zero limit":
    let rows = @[makeRow(@["col1"], @[Value(kind: vkInt64, int64Val: 1'i64)])]
    let result = applyLimit(rows, 0, 0)
    check result.len == 0

  test "applyLimit with limit greater than rows":
    let rows = @[makeRow(@["col1"], @[Value(kind: vkInt64, int64Val: 1'i64)])]
    let result = applyLimit(rows, 10, 0)  # Limit 10 but only 1 row
    check result.len == 1

  test "applyLimit with offset greater than rows":
    let rows = @[makeRow(@["col1"], @[Value(kind: vkInt64, int64Val: 1'i64)])]
    let result = applyLimit(rows, 1, 5)  # Offset 5 but only 1 row
    check result.len == 0

  test "applyLimit with negative offset":
    let rows = @[makeRow(@["col1"], @[Value(kind: vkInt64, int64Val: 1'i64)])]
    let result = applyLimit(rows, 1, -1)  # Negative offset
    check result.len == 1

  test "estimateRowBytes with empty row":
    let row = makeRow(@[], @[])
    let size = estimateRowBytes(row)
    check size > 0  # Even empty rows have some overhead

  test "estimateRowBytes with different value types":
    let row = makeRow(@["col1", "col2", "col3"], @[
      Value(kind: vkInt64, int64Val: 123'i64),
      Value(kind: vkText, bytes: @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')]),
      Value(kind: vkBool, boolVal: true)
    ])
    let size = estimateRowBytes(row)
    check size > 0

  test "valueToString with different types":
    let intVal = Value(kind: vkInt64, int64Val: 42'i64)
    let intStr = valueToString(intVal)
    check intStr == "42"

    let textVal = Value(kind: vkText, bytes: @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')])
    let textStr = valueToString(textVal)
    check textStr == "hello"

    let boolVal = Value(kind: vkBool, boolVal: true)
    let boolStr = valueToString(boolVal)
    check boolStr == "true"

    let falseVal = Value(kind: vkBool, boolVal: false)
    let falseStr = valueToString(falseVal)
    check falseStr == "false"

  test "valueToBool with different types":
    let trueInt = Value(kind: vkInt64, int64Val: 1'i64)
    let trueResult = valueToBool(trueInt)
    check trueResult == true

    let falseInt = Value(kind: vkInt64, int64Val: 0'i64)
    let falseResult = valueToBool(falseInt)
    check falseResult == false

    # For text/blob, any non-empty content returns true
    let nonEmptyText = Value(kind: vkText, bytes: @[byte('t'), byte('r'), byte('u'), byte('e')])
    let nonEmptyResult = valueToBool(nonEmptyText)
    check nonEmptyResult == true

    # Empty text/blob returns false
    let emptyText = Value(kind: vkText, bytes: @[])
    let emptyResult = valueToBool(emptyText)
    check emptyResult == false

  test "compareValues with same types":
    let val1 = Value(kind: vkInt64, int64Val: 5'i64)
    let val2 = Value(kind: vkInt64, int64Val: 10'i64)
    let comparison = compareValues(val1, val2)
    check comparison < 0  # 5 < 10

    let val3 = Value(kind: vkInt64, int64Val: 10'i64)
    let comparison2 = compareValues(val2, val3)
    check comparison2 == 0  # 10 == 10

    let val4 = Value(kind: vkInt64, int64Val: 15'i64)
    let comparison3 = compareValues(val4, val2)
    check comparison3 > 0  # 15 > 10