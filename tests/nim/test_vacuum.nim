import unittest
import os

import engine
import decentdb_cli

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path):
    removeFile(path)
  path

suite "Vacuum":
  test "vacuum rewrites into smaller file":
    let srcPath = makeTempDb("decentdb_vacuum_src.db")
    let dstPath = makeTempDb("decentdb_vacuum_dst.db")

    let dbRes = openDb(srcPath)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT NOT NULL, tag TEXT)").ok
    check execSql(db, "CREATE INDEX items_name_idx ON items (name)").ok

    for i in 0 ..< 5000:
      discard execSql(db, "INSERT INTO items (id, name, tag) VALUES (" & $i & ", 'name" & $i & "', 'tag')")

    # Delete half the rows to create free space.
    for i in 0 ..< 2500:
      discard execSql(db, "DELETE FROM items WHERE id = " & $i)

    discard checkpointDb(db)
    discard closeDb(db)

    # Run vacuum into a new file.
    let vacRes = vacuumCmd(db = srcPath, output = dstPath, overwrite = true)
    check vacRes == 0

    let src2 = openDb(srcPath)
    check src2.ok
    let dbSrc2 = src2.value
    let afterSrcPages = dbSrc2.pager.pageCount
    discard closeDb(dbSrc2)

    let dst2 = openDb(dstPath)
    check dst2.ok
    let dbDst2 = dst2.value
    let afterDstPages = dbDst2.pager.pageCount

    # Destination should be <= source in pages after deletes.
    check afterDstPages <= afterSrcPages

    let cntRes = execSql(dbDst2, "SELECT COUNT(*) FROM items")
    check cntRes.ok
    check cntRes.value.len == 1

    discard closeDb(dbDst2)
