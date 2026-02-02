import unittest, os

import errors
import engine
import wal/wal
import sql/sql
import sql/binder
import planner/planner
import exec/exec

proc selectValAtSnapshot(db: Db, snapshot: uint64): Result[string] =
  let astRes = parseSql("SELECT val FROM foo WHERE id = 1")
  if not astRes.ok:
    return err[string](astRes.err.code, astRes.err.message, astRes.err.context)
  if astRes.value.statements.len != 1:
    return err[string](ERR_INTERNAL, "Expected single statement")
  let boundRes = bindStatement(db.catalog, astRes.value.statements[0])
  if not boundRes.ok:
    return err[string](boundRes.err.code, boundRes.err.message, boundRes.err.context)
  let planRes = plan(db.catalog, boundRes.value)
  if not planRes.ok:
    return err[string](planRes.err.code, planRes.err.message, planRes.err.context)

  db.pager.overlaySnapshot = snapshot
  defer:
    db.pager.overlaySnapshot = 0

  let rowsRes = execPlan(db.pager, db.catalog, planRes.value, @[])
  if not rowsRes.ok:
    return err[string](rowsRes.err.code, rowsRes.err.message, rowsRes.err.context)
  if rowsRes.value.len != 1 or rowsRes.value[0].values.len != 1:
    return err[string](ERR_INTERNAL, "Unexpected row shape", "rows=" & $rowsRes.value.len)
  ok(valueToString(rowsRes.value[0].values[0]))

suite "Checkpoint Reader Race":
  setup:
    let dbPath = "test_checkpoint_race.db"
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  teardown:
    let dbPath = "test_checkpoint_race.db"
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  test "active wal reader pins WAL and snapshot stays stable":
    let walPath = dbPath & "-wal"

    let dbRes = openDb(dbPath)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)

    require db.execSql("CREATE TABLE foo (id int, val text)").ok
    require db.execSql("INSERT INTO foo VALUES (1, 'v1')").ok

    # Hold a long-running reader at snapshot=v1.
    let txn1 = beginRead(db.wal)
    let snap1 = txn1.snapshot

    let v1Res = selectValAtSnapshot(db, snap1)
    require v1Res.ok
    check v1Res.value == "v1"

    # Commit a newer version while the old reader is still active.
    require db.execSql("UPDATE foo SET val = 'v2' WHERE id = 1").ok

    let stillV1Res = selectValAtSnapshot(db, snap1)
    require stillV1Res.ok
    check stillV1Res.value == "v1"

    # Checkpoint while reader is active: should NOT truncate WAL.
    require db.wal.checkpoint(db.pager).ok
    check getFileInfo(walPath).size > 0

    let stillV1AfterCkRes = selectValAtSnapshot(db, snap1)
    require stillV1AfterCkRes.ok
    check stillV1AfterCkRes.value == "v1"

    endRead(db.wal, txn1)

    # With no active readers and no new commits during the checkpoint, truncation is allowed.
    require db.wal.checkpoint(db.pager).ok
    check getFileInfo(walPath).size == 0

    let currentRes = db.execSql("SELECT val FROM foo WHERE id = 1")
    require currentRes.ok
    require currentRes.value.len == 1
    check currentRes.value[0] == "v2"
