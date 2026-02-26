import unittest
import os
import strutils
import options
import engine
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine commitTransaction Coverage":
  test "commitTransaction: Database not open":
    let path = makeTempDb("commit_not_open.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard closeDb(db)
    
    let res = commitTransaction(db)
    check not res.ok
    check res.err.code == ERR_INTERNAL
    check res.err.message == "Database not open"

  test "commitTransaction: No active transaction":
    let path = makeTempDb("commit_no_active_tx.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    
    let res = commitTransaction(db)
    check not res.ok
    check res.err.code == ERR_TRANSACTION
    check res.err.message == "No active transaction"

    discard closeDb(db)


import vfs/vfs
import vfs/os_vfs
import vfs/faulty_vfs

suite "Engine commitTransaction Faults":
  test "writePageDirect fails (single dirty page)":
    let path = makeTempDb("commit_single_fault.db")
    let base = newOsVfs()
    let faulty = newFaultyVfs(base)
    # the db is open with OsVfs... Wait, openDb takes a path. 
    # we need to pass the vfs! But openDb uses `newOsVfs` directly if not `:memory:`...
    # Let's check `openDb` signature.

  test "execSqlRows: Database not open":
    let path = makeTempDb("execsql_not_open.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard closeDb(db)
    
    let res = execSqlRows(db, "SELECT 1", @[])
    check not res.ok
    check res.err.code == ERR_INTERNAL
    check res.err.message == "Database not open"

  test "execSqlNoRows: Database not open":
    let path = makeTempDb("execsqlnorows_not_open.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard closeDb(db)
    
    let res = execSqlNoRows(db, "SELECT 1", @[])
    check not res.ok
    check res.err.code == ERR_INTERNAL
    check res.err.message == "Database not open"
