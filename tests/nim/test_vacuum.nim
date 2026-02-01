import unittest
import os
import tables

import engine
import decentdb_cli
import catalog/catalog

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
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

  test "vacuum skips semantically redundant indexes":
    let srcPath = makeTempDb("decentdb_vacuum_redundant_src.db")
    let dstPath = makeTempDb("decentdb_vacuum_redundant_dst.db")

    let dbRes = openDb(srcPath)
    check dbRes.ok
    let db = dbRes.value

    # UNIQUE on name auto-creates a unique btree index.
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT UNIQUE NOT NULL)").ok
    # Add a redundant unique index on the same column with a different name.
    check execSql(db, "CREATE UNIQUE INDEX items_name_unique2 ON items (name)").ok

    for i in 0 ..< 1000:
      discard execSql(db, "INSERT INTO items (id, name) VALUES (" & $i & ", 'name" & $i & "')")

    discard checkpointDb(db)
    discard closeDb(db)

    let vacRes = vacuumCmd(db = srcPath, output = dstPath, overwrite = true)
    check vacRes == 0

    let dst2 = openDb(dstPath)
    check dst2.ok
    let dbDst2 = dst2.value

    # The redundant extra index should not be recreated.
    check not dbDst2.catalog.indexes.hasKey("items_name_unique2")
    var nameUniqueCount = 0
    for _, idx in dbDst2.catalog.indexes:
      if idx.table == "items" and idx.column == "name" and idx.kind == ikBtree and idx.unique:
        nameUniqueCount.inc
    check nameUniqueCount == 1

    discard closeDb(dbDst2)

  test "vacuum skips redundant FK child indexes":
    let srcPath = makeTempDb("decentdb_vacuum_fk_redundant_src.db")
    let dstPath = makeTempDb("decentdb_vacuum_fk_redundant_dst.db")

    let dbRes = openDb(srcPath)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE parents (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES parents(id))").ok

    # Source has the auto-created FK index plus an extra user-created index on the same column.
    check execSql(db, "CREATE INDEX children_parent_id_user_idx ON children (parent_id)").ok

    for i in 0 ..< 1000:
      discard execSql(db, "INSERT INTO parents (id) VALUES (" & $i & ")")
      discard execSql(db, "INSERT INTO children (id, parent_id) VALUES (" & $i & ", " & $i & ")")

    discard checkpointDb(db)
    discard closeDb(db)

    let vacRes = vacuumCmd(db = srcPath, output = dstPath, overwrite = true)
    check vacRes == 0

    let dst2 = openDb(dstPath)
    check dst2.ok
    let dbDst2 = dst2.value

    # The explicit user index should not be recreated since the destination already has
    # an FK auto-index on children.parent_id.
    check not dbDst2.catalog.indexes.hasKey("children_parent_id_user_idx")
    var fkChildIndexCount = 0
    for _, idx in dbDst2.catalog.indexes:
      if idx.table == "children" and idx.column == "parent_id" and idx.kind == ikBtree:
        fkChildIndexCount.inc
    check fkChildIndexCount == 1

    discard closeDb(dbDst2)

  test "vacuum recreates UNIQUE index not satisfied by FK auto-index":
    let srcPath = makeTempDb("decentdb_vacuum_fk_unique_src.db")
    let dstPath = makeTempDb("decentdb_vacuum_fk_unique_dst.db")

    let dbRes = openDb(srcPath)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE parents (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES parents(id))").ok

    # This UNIQUE index is stronger than the FK auto-index (which is non-unique).
    check execSql(db, "CREATE UNIQUE INDEX children_parent_id_unique_user_idx ON children (parent_id)").ok

    # Ensure parent_id values are unique so the UNIQUE index is valid.
    for i in 0 ..< 1000:
      discard execSql(db, "INSERT INTO parents (id) VALUES (" & $i & ")")
      discard execSql(db, "INSERT INTO children (id, parent_id) VALUES (" & $i & ", " & $i & ")")

    discard checkpointDb(db)
    discard closeDb(db)

    let vacRes = vacuumCmd(db = srcPath, output = dstPath, overwrite = true)
    check vacRes == 0

    let dst2 = openDb(dstPath)
    check dst2.ok
    let dbDst2 = dst2.value

    # The UNIQUE index should be recreated (non-unique FK index does not satisfy it).
    check dbDst2.catalog.indexes.hasKey("children_parent_id_unique_user_idx")

    var nonUniqueCount = 0
    var uniqueCount = 0
    for _, idx in dbDst2.catalog.indexes:
      if idx.table == "children" and idx.column == "parent_id" and idx.kind == ikBtree:
        if idx.unique: uniqueCount.inc else: nonUniqueCount.inc
    check nonUniqueCount == 1
    check uniqueCount == 1

    discard closeDb(dbDst2)
