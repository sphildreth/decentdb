import unittest
import os
import strutils
import options
import engine
import errors
import record/record
import catalog/catalog
import sql/binder
import sql/sql

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Engine execInsertStatement error paths":
  test "insert type validation":
    let path = makeTempDb("insert_type_valid.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v_text TEXT, v_bool BOOL, v_float FLOAT, v_blob BLOB, v_dec DECIMAL(10,2), v_uuid UUID)")

    # Insert valid values
    let resValid = execSql(db, "INSERT INTO t VALUES (1, 'text', true, 1.2, x'AABB', 10.5, '00000000-0000-0000-0000-000000000000')")
    # Wait, the binder handles UUID strings as cast to UUID automatically, but we need to see type validation.
    
    # Let's use parameters to bypass binder literal conversions if possible, or test binder type mismatch vs engine type mismatch.
    # The binder usually does strict type checking. To hit the engine's `typeCheckValue` error paths, we might need dynamic typing or views, or use params.
    let prepRes = prepare(db, "INSERT INTO t VALUES ($1, $2, $3, $4, $5, $6, $7)")
    check prepRes.ok
    
    # Test text expected, got float
    let badTextRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 2),
      Value(kind: vkFloat64, float64Val: 1.0),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull)
    ])
    check not badTextRes.ok
    check "Type mismatch: expected TEXT" in badTextRes.err.message

    # Test bool expected, got text
    let badBoolRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 3),
      Value(kind: vkNull),
      Value(kind: vkText, bytes: cast[seq[byte]]("true")),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull)
    ])
    check not badBoolRes.ok
    check "Type mismatch: expected BOOL" in badBoolRes.err.message

    # Test float expected, got text
    let badFloatRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 4),
      Value(kind: vkNull), Value(kind: vkNull),
      Value(kind: vkText, bytes: cast[seq[byte]]("1.0")),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull)
    ])
    check not badFloatRes.ok
    check "Type mismatch: expected FLOAT64" in badFloatRes.err.message

    # Test blob expected, got text
    let badBlobRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 5),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull),
      Value(kind: vkText, bytes: cast[seq[byte]]("blob")),
      Value(kind: vkNull), Value(kind: vkNull)
    ])
    check not badBlobRes.ok
    check "Type mismatch: expected BLOB" in badBlobRes.err.message

    # Test decimal expected, got text
    let badDecRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 6),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull),
      Value(kind: vkText, bytes: cast[seq[byte]]("10.0")),
      Value(kind: vkNull)
    ])
    check not badDecRes.ok
    check "Type mismatch: expected DECIMAL" in badDecRes.err.message

    # Test uuid expected, got integer
    let badUuidRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 7),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull),
      Value(kind: vkInt64, int64Val: 100)
    ])
    check not badUuidRes.ok
    check "Type mismatch: expected UUID" in badUuidRes.err.message
    
    # Test uuid bad length
    let badUuidLenRes = execPrepared(prepRes.value, @[
      Value(kind: vkInt64, int64Val: 8),
      Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull), Value(kind: vkNull),
      Value(kind: vkBlob, bytes: @[byte 1, 2, 3])
    ])
    check not badUuidLenRes.ok
    check "UUID must be 16 bytes" in badUuidLenRes.err.message

    discard closeDb(db)

  test "insert into inexistent table":
    let path = makeTempDb("insert_notable.db")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    # Create fake AST
    # Binder usually catches table not found, but we want engine coverage
    # so we mock a statement.
    let stmt = Statement(
      kind: skInsert,
      insertTable: "non_existent_table",
      insertColumns: @["id"],
      insertValues: @[Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1))]
    )
    # The binder catches it for prep, but if we call execPreparedNonSelect directly:
    let res = execPreparedNonSelect(db, stmt, @[])
    check not res.ok
    check res.err.message == "Table not found"
    
    discard closeDb(db)

