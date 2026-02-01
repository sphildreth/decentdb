import unittest
import os
import engine
import record/record
import strutils

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  if fileExists(path):
    removeFile(path)
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0:
    return @[]
  row.split("|")

suite "Record Persistence":
  test "persists and recovers various int64 encodings":
    let path = makeTempDb("decentdb_record_persist.db")
    
    # 1. Write data
    let dbA = openDb(path).value
    check execSql(dbA, "CREATE TABLE t (id INT, val INT)").ok
    check execSql(dbA, "BEGIN").ok
    
    # ZigZag/Varint boundaries:
    # 0 -> 0 (1 byte)
    check execSql(dbA, "INSERT INTO t VALUES (1, 0)").ok 
    # -1 -> 1 (1 byte)
    check execSql(dbA, "INSERT INTO t VALUES (2, -1)").ok
    # 63 -> 126 (1 byte)
    check execSql(dbA, "INSERT INTO t VALUES (3, 63)").ok
    # -64 -> 127 (1 byte)
    check execSql(dbA, "INSERT INTO t VALUES (4, -64)").ok
    # 64 -> 128 (2 bytes)
    check execSql(dbA, "INSERT INTO t VALUES (5, 64)").ok
    # -65 -> 129 (2 bytes)
    check execSql(dbA, "INSERT INTO t VALUES (6, -65)").ok
    # Large values (10 bytes)
    check execSql(dbA, "INSERT INTO t VALUES (7, $1)", @[Value(kind: vkInt64, int64Val: int64.high)]).ok
    check execSql(dbA, "INSERT INTO t VALUES (8, $1)", @[Value(kind: vkInt64, int64Val: int64.low)]).ok
    
    check execSql(dbA, "COMMIT").ok
    discard closeDb(dbA)

    # 2. Recover and Verify
    let dbB = openDb(path).value
    let res = execSql(dbB, "SELECT id, val FROM t ORDER BY id")
    check res.ok
    check res.value.len == 8
    
    let rows = res.value
    check splitRow(rows[0])[1] == "0"
    check splitRow(rows[1])[1] == "-1"
    check splitRow(rows[2])[1] == "63"
    check splitRow(rows[3])[1] == "-64"
    check splitRow(rows[4])[1] == "64"
    check splitRow(rows[5])[1] == "-65"
    check splitRow(rows[6])[1] == $int64.high
    check splitRow(rows[7])[1] == $int64.low
    
    discard closeDb(dbB)
