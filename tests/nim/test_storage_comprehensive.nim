import unittest
import storage/storage
import errors
import record/record

suite "Storage Additional Edge Cases":
  test "StoredRow basic functionality":
    let row = StoredRow(rowid: 123'u64, values: @[])
    check row.rowid == 123'u64
    check row.values.len == 0

    let rowWithValues = StoredRow(rowid: 456'u64, values: @[Value(kind: vkInt64, int64Val: 789'i64)])
    check rowWithValues.rowid == 456'u64
    check rowWithValues.values.len == 1
    check rowWithValues.values[0].kind == vkInt64
    check rowWithValues.values[0].int64Val == 789'i64