import unittest
import os
import engine
import record/record
import storage/storage
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  # Current DecentDB WAL naming convention is "<db>-wal".
  # Keep legacy cleanup for ".wal" to avoid leaving stale files.
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Bulk Load":
  test "bulk load rebuilds indexes":
    let path = makeTempDb("decentdb_bulk_load.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body_idx ON docs (body)").ok
    var rows: seq[seq[Value]] = @[]
    for i in 1 .. 5:
      rows.add(@[
        Value(kind: vkInt64, int64Val: i),
        Value(kind: vkText, bytes: @['A'.byte, byte(64 + i)])
      ])
    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = true
    opts.durability = dmNone
    let bulkRes = bulkLoad(db, "docs", rows, opts)
    check bulkRes.ok
    # Seek on 'body' index since 'id' (PK) no longer has a secondary index.
    let idxRes = indexSeek(db.pager, db.catalog, "docs", "body", Value(kind: vkText, bytes: @['A'.byte, byte(64 + 3)]))
    check idxRes.ok
    check idxRes.value.len == 1
    discard closeDb(db)

  test "bulk load rejects duplicate unique values":
    let path = makeTempDb("decentdb_bulk_load_dupe.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)").ok
    var rows: seq[seq[Value]] = @[
      @[Value(kind: vkInt64, int64Val: 1), Value(kind: vkText, bytes: @['A'.byte])],
      @[Value(kind: vkInt64, int64Val: 2), Value(kind: vkText, bytes: @['A'.byte])]
    ]
    var opts = defaultBulkLoadOptions()
    opts.disableIndexes = true
    opts.durability = dmNone
    let bulkRes = bulkLoad(db, "users", rows, opts)
    check not bulkRes.ok
    check bulkRes.err.code == ERR_CONSTRAINT
    discard closeDb(db)
