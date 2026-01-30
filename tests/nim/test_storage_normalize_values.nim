import unittest
import os
import engine
import storage/storage
import errors
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path)
  path

suite "Storage Normalize Values":
  test "normalizeValues with different value types":
    let dbPath = makeTempDb("test_normalize_values.db")
    let dbRes = openDb(dbPath, 4096)
    if dbRes.ok:
      let db = dbRes.value
      let pager = db.pager

      # Test with various value types
      let originalValues = @[
        Value(kind: vkInt64, int64Val: 42'i64),
        Value(kind: vkText, bytes: @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')]),  # "hello"
        Value(kind: vkBool, boolVal: true),
        Value(kind: vkFloat64, float64Val: 3.14'f64)
      ]

      let normalizedRes = normalizeValues(pager, originalValues)
      check normalizedRes.ok

      let normalized = normalizedRes.value
      check normalized.len == originalValues.len

      # Clean up
      discard closeDb(db)
    removeFile(dbPath)