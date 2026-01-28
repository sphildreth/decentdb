import unittest
import os
import strutils
import engine
import record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0:
    return @[]
  row.split("|")

suite "Sort Spill":
  test "sort exceeds memory buffer and spills to disk":
    let path = makeTempDb("decentdb_sort_spill.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE bulky (id INT, payload TEXT)").ok
    
    # Generate 1KB string
    var payloadStr = ""
    for i in 0 ..< 1024:
      payloadStr.add('x')
    
    # Insert 18,000 rows (approx 18MB > 16MB limit)
    check execSql(db, "BEGIN").ok
    for i in 1 .. 18000:
      let sql = "INSERT INTO bulky (id, payload) VALUES ($1, $2)"
      check execSql(db, sql, @[
        Value(kind: vkInt64, int64Val: int64(i)),
        Value(kind: vkText, bytes: cast[seq[byte]](payloadStr))
      ]).ok
    check execSql(db, "COMMIT").ok

    # Verify count
    let countRes = execSql(db, "SELECT COUNT(*) FROM bulky")
    check countRes.ok
    check splitRow(countRes.value[0])[0] == "18000"

    # Sort descending - should spill
    let sortRes = execSql(db, "SELECT id FROM bulky ORDER BY id DESC LIMIT 5")
    check sortRes.ok
    let rows = sortRes.value
    check rows.len == 5
    # Expecting 18000, 17999, ...
    check splitRow(rows[0])[0] == "18000"
    check splitRow(rows[1])[0] == "17999"
    check splitRow(rows[2])[0] == "17998"
    check splitRow(rows[3])[0] == "17997"
    check splitRow(rows[4])[0] == "17996"

    discard closeDb(db)
