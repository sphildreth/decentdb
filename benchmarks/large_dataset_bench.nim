import os, strutils, times
import ../src/engine
import ../src/record/record

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path): removeFile(path)
  if fileExists(path & ".wal"): removeFile(path & ".wal")
  path

proc benchScan1M(): float =
  let path = makeTempDb("bench_scan.db")
  let db = openDb(path).value
  discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, p TEXT)")
  discard execSql(db, "BEGIN")
  var payload = "x".repeat(100)
  for i in 1..1_000_000:
    discard execSql(db, "INSERT INTO t (id, p) VALUES ($1, $2)", @[Value(kind: vkInt64, int64Val: int64(i)), Value(kind: vkText, bytes: cast[seq[byte]](payload))])
    if i mod 10000 == 0:
      discard execSql(db, "COMMIT")
      discard execSql(db, "BEGIN")
  discard execSql(db, "COMMIT")
  let start = epochTime()
  discard execSql(db, "SELECT COUNT(*) FROM t")
  result = epochTime() - start
  discard closeDb(db)

proc benchPointLookup(): float =
  let path = makeTempDb("bench_lookup.db")
  let db = openDb(path).value
  discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, d TEXT)")
  discard execSql(db, "BEGIN")
  for i in 1..100_000:
    discard execSql(db, "INSERT INTO t (id, d) VALUES ($1, 'd')", @[Value(kind: vkInt64, int64Val: int64(i))])
  discard execSql(db, "COMMIT")
  let start = epochTime()
  for i in 1..10_000:
    let target = (i * 997) mod 100_000 + 1
    discard execSql(db, "SELECT * FROM t WHERE id = $1", @[Value(kind: vkInt64, int64Val: int64(target))])
  result = epochTime() - start
  discard closeDb(db)

proc benchJoin(): float =
  let path = makeTempDb("bench_join.db")
  let db = openDb(path).value
  discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, n TEXT)")
  discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT REFERENCES a(id), t TEXT)")
  discard execSql(db, "BEGIN")
  for i in 1..1000:
    discard execSql(db, "INSERT INTO a (id, n) VALUES ($1, $2)", @[Value(kind: vkInt64, int64Val: int64(i)), Value(kind: vkText, bytes: cast[seq[byte]]("A" & $i))])
  for i in 1..10_000:
    discard execSql(db, "INSERT INTO b (id, a_id, t) VALUES ($1, $2, $3)", @[Value(kind: vkInt64, int64Val: int64(i)), Value(kind: vkInt64, int64Val: int64((i-1) div 10 + 1)), Value(kind: vkText, bytes: cast[seq[byte]]("B" & $i))])
  discard execSql(db, "COMMIT")
  let start = epochTime()
  for i in 1..100:
    let aid = (i mod 1000) + 1
    discard execSql(db, "SELECT a.n, b.t FROM a JOIN b ON a.id = b.a_id WHERE a.id = $1", @[Value(kind: vkInt64, int64Val: int64(aid))])
  result = epochTime() - start
  discard closeDb(db)

when isMainModule:
  echo "=== Large Dataset Benchmarks ==="
  echo "Scan 1M rows: ", benchScan1M() * 1000, " ms"
  echo "10K point lookups: ", benchPointLookup() * 1000, " ms"
  echo "100 FK joins: ", benchJoin() * 1000, " ms"
