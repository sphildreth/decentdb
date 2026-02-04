import src/engine
import src/record/record
import os, times

proc test() =
  let dbPath = "/tmp/test_nowal_bench.ddb"
  removeFile(dbPath)
  removeFile(dbPath & "-wal")
  
  # Open without WAL to skip beginRead/endRead overhead
  let db = openDb(dbPath, Options(walOverlayEnabled: false)).value
  defer: discard closeDb(db)
  
  discard execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)")
  
  for i in 1..1000:
    discard execSql(db, "INSERT INTO users VALUES ($1, $2, $3)", @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkText, bytes: cast[seq[byte]]("User" & $i)),
      Value(kind: vkText, bytes: cast[seq[byte]]("user" & $i & "@example.com"))
    ])
  
  let sql = "SELECT * FROM users WHERE id = $1"
  for _ in 1..5:
    discard execSql(db, sql, @[Value(kind: vkInt64, int64Val: 42)])
  
  for run in 1..3:
    let iterations = 10000
    let t0 = epochTime()
    for i in 1..iterations:
      discard execSql(db, sql, @[Value(kind: vkInt64, int64Val: int64((i mod 1000) + 1))])
    let t1 = epochTime()
    echo "Run ", run, " (no WAL overlay): ", (t1 - t0) * 1_000_000 / float(iterations), " us/op"

test()
