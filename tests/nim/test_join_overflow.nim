import unittest, os
import engine
import record/record


proc valInt(i: int): Value = Value(kind: vkInt64, int64Val: int64(i))
proc valText(s: string): Value = 
  var bytes = newSeq[byte](s.len)
  if s.len > 0: copyMem(addr bytes[0], unsafeAddr s[0], s.len)
  Value(kind: vkText, bytes: bytes)

suite "JOIN Fallback Correctness":
  setup:
    let dbPath = "test_join_overflow.db"
    if dirExists(dbPath): removeDir(dbPath)
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  teardown:
    let dbPath = "test_join_overflow.db"
    if dirExists(dbPath): removeDir(dbPath)
    if fileExists(dbPath): removeFile(dbPath)
    if fileExists(dbPath & "-wal"): removeFile(dbPath & "-wal")

  test "large left input join without index":
    let dbRes = openDb("test_join_overflow.db")
    check dbRes.ok
    let db = dbRes.value
    check db.execSql("CREATE TABLE users (id int, name text)").ok
    check db.execSql("CREATE TABLE logins (user_id int, ts int)").ok
    
    # Insert 150 users (triggering 'caching=false' since MaxLeftRowsForCache is likely 100)
    require db.execSql("BEGIN").ok
    for i in 1 .. 150:
      let insRes = db.execSql("INSERT INTO users VALUES ($1, $2)", @[valInt(i), valText("user" & $i)])
      if not insRes.ok:
        echo "users insert failed at i=", i, ": ", $insRes.err.code, " ", insRes.err.message, " (", insRes.err.context, ")"
      require insRes.ok
    require db.execSql("COMMIT").ok
    
    # Insert logins for each user
    require db.execSql("BEGIN").ok
    for i in 1 .. 150:
      let insRes = db.execSql("INSERT INTO logins VALUES ($1, $2)", @[valInt(i), valInt(1000 + i)])
      if not insRes.ok:
        echo "logins insert failed at i=", i, ": ", $insRes.err.code, " ", insRes.err.message, " (", insRes.err.context, ")"
      require insRes.ok
    require db.execSql("COMMIT").ok
    
    # JOIN - NO Index on logins(user_id)
    # This forces the executor to scan 'logins' for each 'user'.
    # If caching is disabled (due to > 100 rows) and right is not index seek, 
    # it might fallback to incorrect behavior (cachedRight empty).
    
    let rowsRes = db.execSql("SELECT users.name, logins.ts FROM users JOIN logins ON users.id = logins.user_id")
    check rowsRes.ok
    let rows = rowsRes.value
    
    check rows.len == 150
    if rows.len != 150:
      echo "Expected 150 rows, got ", rows.len
