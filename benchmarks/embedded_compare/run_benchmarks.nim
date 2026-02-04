import os
import strutils
import times
import std/monotimes
import json
import random
import math
import algorithm


# We need to import the engine from src
# Since we are in benchmarks/embedded_compare/, src is ../../src
import ../../src/engine
import ../../src/record/record
import ../../src/errors
import ../../src/vfs/vfs
when defined(fused_join_sum_stats):
  import ../../src/exec/exec

# --- SQLite FFI bindings ---
{.passL: "-lsqlite3".}

const
  SQLITE_OK* = 0
  SQLITE_ROW* = 100
  SQLITE_DONE* = 101

type
  Sqlite3* = pointer
  Sqlite3Stmt* = pointer

proc sqlite3_open*(filename: cstring, ppDb: ptr Sqlite3): cint {.cdecl, importc.}
proc sqlite3_close*(db: Sqlite3): cint {.cdecl, importc.}
proc sqlite3_exec*(db: Sqlite3, sql: cstring, callback: pointer, arg: pointer, errmsg: ptr cstring): cint {.cdecl, importc.}
proc sqlite3_prepare_v2*(db: Sqlite3, sql: cstring, nByte: cint, ppStmt: ptr Sqlite3Stmt, pzTail: ptr cstring): cint {.cdecl, importc.}
proc sqlite3_step*(stmt: Sqlite3Stmt): cint {.cdecl, importc.}
proc sqlite3_finalize*(stmt: Sqlite3Stmt): cint {.cdecl, importc.}
proc sqlite3_bind_int64*(stmt: Sqlite3Stmt, idx: cint, value: int64): cint {.cdecl, importc.}
proc sqlite3_bind_text*(stmt: Sqlite3Stmt, idx: cint, value: cstring, nBytes: cint, destructor: pointer): cint {.cdecl, importc.}
proc sqlite3_reset*(stmt: Sqlite3Stmt): cint {.cdecl, importc.}
proc sqlite3_column_int64*(stmt: Sqlite3Stmt, col: cint): int64 {.cdecl, importc.}
proc sqlite3_column_text*(stmt: Sqlite3Stmt, col: cint): cstring {.cdecl, importc.}
proc sqlite3_errmsg*(db: Sqlite3): cstring {.cdecl, importc.}
proc sqlite3_libversion*(): cstring {.cdecl, importc.}

const SQLITE_TRANSIENT = cast[pointer](-1)

# Global variable for benchmark data directory (on real disk, not tmpfs)
var gDataDir*: string = ""

# Paths to benchmark database files to remove at end.
var gCleanupPaths: seq[string] = @[]

proc registerCleanupPath(path: string) =
  if path.len == 0:
    return
  for p in gCleanupPaths:
    if p == path:
      return
  gCleanupPaths.add(path)

proc registerDbArtifacts(basePath: string, includeShm: bool) =
  ## Register database-related files for cleanup.
  registerCleanupPath(basePath)
  registerCleanupPath(basePath & "-wal")
  if includeShm:
    registerCleanupPath(basePath & "-shm")

proc cleanupRegisteredArtifacts() =
  ## Best-effort cleanup of benchmark DB files.
  for p in gCleanupPaths:
    if fileExists(p):
      try:
        removeFile(p)
      except CatchableError:
        echo "Warning: failed to remove ", p, ": ", getCurrentExceptionMsg()

proc getBenchDataDir(): string =
  ## Get the directory for benchmark database files.
  ## Uses --data-dir if specified, otherwise falls back to temp dir.
  if gDataDir.len > 0:
    createDir(gDataDir)
    return gDataDir
  else:
    let d = getTempDir() / "decentdb_bench_data"
    createDir(d)
    return d

proc toBytes(text: string): seq[byte] =
  ## Convert a Nim string to owned bytes (safe under ARC/ORC).
  ## Do NOT cast string -> seq[byte]; that can segfault.
  result = newSeq[byte](text.len)
  for i, ch in text:
    result[i] = byte(ch)

type
  BenchmarkMetrics = object
    latencies_us: seq[int]
    p50_us: int
    p95_us: int
    p99_us: int
    # Higher precision percentiles (nanoseconds).
    # Used by the aggregator to avoid microsecond quantization.
    p50_ns: int64
    p95_ns: int64
    p99_ns: int64
    ops_per_sec: float
    rows_processed: int
    checksum_u64: uint64

  BenchmarkArtifacts = object
    db_path: string
    db_size_bytes: int64
    wal_size_bytes: int64

  BenchmarkEnvironment = object
    os: string
    cpu: string
    ram_gb: int
    filesystem: string
    notes: string

  BenchmarkResult = object
    timestamp_utc: string
    engine: string
    engine_version: string
    dataset: string
    benchmark: string
    durability: string
    threads: int
    iterations: int
    metrics: BenchmarkMetrics
    artifacts: BenchmarkArtifacts
    environment: BenchmarkEnvironment

# --- Helpers ---

proc getIsoTime(): string =
  now().utc.format("yyyy-MM-dd'T'HH:mm:ss'Z'")

proc nanosBetween(t0, t1: MonoTime): int64 =
  ## Return nanoseconds between two monotonic timestamps.
  let d = t1 - t0
  int64(inNanoseconds(d))

proc secondsBetween(t0, t1: MonoTime): float =
  ## Return seconds between two monotonic timestamps.
  float(inMicroseconds(t1 - t0)) / 1_000_000.0

proc percentileNs(latencies: seq[int64], p: float): int64 =
  if latencies.len == 0: return 0'i64
  var sorted = latencies
  sorted.sort()
  let idx = int(ceil(float(sorted.len) * p / 100.0)) - 1
  return sorted[max(0, min(sorted.len - 1, idx))]

proc writeResult(outputDir: string, res: BenchmarkResult) =
  # Better name: engine__dataset__benchmark__timestamp.jsonl
  let timestampShort = $toUnix(now().toTime())
  let safeFilename = "$#__$#__$#__$#.jsonl" % [res.engine, res.dataset, res.benchmark, timestampShort]
  let path = outputDir / safeFilename
  
  let jsonNode = %*{
    "timestamp_utc": res.timestamp_utc,
    "engine": res.engine,
    "engine_version": res.engine_version,
    "dataset": res.dataset,
    "benchmark": res.benchmark,
    "durability": res.durability,
    "threads": res.threads,
    "iterations": res.iterations,
    "metrics": {
      "latencies_us": res.metrics.latencies_us,
      "p50_us": res.metrics.p50_us,
      "p95_us": res.metrics.p95_us,
      "p99_us": res.metrics.p99_us,
      "p50_ns": res.metrics.p50_ns,
      "p95_ns": res.metrics.p95_ns,
      "p99_ns": res.metrics.p99_ns,
      "ops_per_sec": res.metrics.ops_per_sec,
      "rows_processed": res.metrics.rows_processed,
      "checksum_u64": res.metrics.checksum_u64
    },
    "artifacts": {
      "db_path": res.artifacts.db_path,
      "db_size_bytes": res.artifacts.db_size_bytes,
      "wal_size_bytes": res.artifacts.wal_size_bytes
    },
    "environment": {
      "os": "Linux", # Todo: detect
      "cpu": "Unknown", # Todo: detect
      "ram_gb": 0,
      "filesystem": "unknown",
      "notes": ""
    }
  }
  
  createDir(outputDir)
  writeFile(path, $jsonNode)
  echo "Written result to ", path

# --- DecentDB Benchmarks ---

proc runDecentDbInsert(outputDir: string) =
  echo "Running DecentDB Insert Benchmark..."
  let dbPath = getBenchDataDir() / "bench_decentdb_insert.ddb"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")

  let db = openDb(dbPath).value
  defer: discard closeDb(db)

  discard execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)")
  
  let iterations = 1000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    let params = @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkText, bytes: toBytes(name)),
      Value(kind: vkText, bytes: toBytes(email))
    ]
    let t0 = getMonoTime()
    discard execSql(db, "INSERT INTO users VALUES ($1, $2, $3)", params)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "DecentDB",
    engine_version: "0.0.1",
    dataset: "sample",
    benchmark: "insert",
    durability: "safe", # default
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0 
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDecentDbCommitLatency(outputDir: string) =
  echo "Running DecentDB Commit Latency Benchmark..."
  let dbPath = getBenchDataDir() / "bench_decentdb_commit.ddb"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")

  let db = openDb(dbPath).value
  defer: discard closeDb(db)

  discard execSql(db, "CREATE TABLE kv (k INT PRIMARY KEY, v TEXT)")
  
  # Pre-insert a row to update
  discard execSql(db, "INSERT INTO kv VALUES (1, 'initial')")
  
  let iterations = 1000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let t0 = getMonoTime()
    # Each UPDATE is a separate transaction with durable commit
    discard execSql(db, "UPDATE kv SET v = $1 WHERE k = 1", @[
      Value(kind: vkText, bytes: toBytes("value" & $i))
    ])
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "DecentDB",
    engine_version: "0.0.1",
    dataset: "sample",
    benchmark: "commit_latency",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDecentDbPointRead(outputDir: string) =
  echo "Running DecentDB Point Read Benchmark..."
  let dbPath = getBenchDataDir() / "bench_decentdb_read.ddb"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")

  let db = openDb(dbPath, 50000).value
  defer: discard closeDb(db)

  discard execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)")
  
  # Pre-populate with data
  let dataSize = 1000
  for i in 1..dataSize:
    discard execSql(db, "INSERT INTO users VALUES ($1, $2, $3)", @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkText, bytes: toBytes("User" & $i)),
      Value(kind: vkText, bytes: toBytes("user" & $i & "@example.com"))
    ])
  
  let iterations = 100000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  var rowsProcessedTotal: int64 = 0
  var rng = initRand(42)
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let lookupId = rng.rand(1..dataSize)
    let t0 = getMonoTime()
    let nRes = execSqlNoRows(db, "SELECT * FROM users WHERE id = $1", @[
      Value(kind: vkInt64, int64Val: int64(lookupId))
    ])
    if nRes.ok:
      rowsProcessedTotal += nRes.value
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "DecentDB",
    engine_version: "0.0.1",
    dataset: "sample",
    benchmark: "point_read",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: int(rowsProcessedTotal),
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDecentDbJoin(outputDir: string) =
  echo "Running DecentDB Join Benchmark..."
  let dbPath = getBenchDataDir() / "bench_decentdb_join.ddb"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")

  let db = openDb(dbPath).value
  defer: discard closeDb(db)

  discard execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
  discard execSql(db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
  
  # Pre-populate
  let userCount = 100
  let orderCount = 1000
  for i in 1..userCount:
    discard execSql(db, "INSERT INTO users VALUES ($1, $2)", @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkText, bytes: toBytes("User" & $i))
    ])
  
  var rng = initRand(42)
  for i in 1..orderCount:
    discard execSql(db, "INSERT INTO orders VALUES ($1, $2, $3)", @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkInt64, int64Val: int64(rng.rand(1..userCount))),
      Value(kind: vkInt64, int64Val: int64(rng.rand(10..1000)))
    ])
  
  let iterations = 100
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  var rowsProcessedTotal: int64 = 0

  when defined(fused_join_sum_stats):
    resetFusedJoinSumStats()
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let t0 = getMonoTime()
    let nRes = execSqlNoRows(db, "SELECT u.name, SUM(o.amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name", @[])
    if nRes.ok:
      rowsProcessedTotal += nRes.value
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))

  when defined(fused_join_sum_stats):
    let st = fusedJoinSumStats()
    echo "Fused join+sum stats: attempts=", st.attempts, " hits=", st.hits, " dense=", st.dense, " sparse=", st.sparse
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "DecentDB",
    engine_version: "0.0.1",
    dataset: "sample",
    benchmark: "join",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: int(rowsProcessedTotal),
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

# --- SQLite Benchmarks ---

proc sqliteExec(db: Sqlite3, sql: string) =
  var errmsg: cstring
  let rc = sqlite3_exec(db, sql.cstring, nil, nil, addr errmsg)
  if rc != SQLITE_OK:
    let msg = if errmsg != nil: $errmsg else: "unknown error"
    raise newException(IOError, "SQLite exec error: " & msg)

proc runSqliteInsert(outputDir: string) =
  echo "Running SQLite Insert Benchmark..."
  let dbPath = getBenchDataDir() / "bench_sqlite_insert.db"
  registerDbArtifacts(dbPath, includeShm = true)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")
  if fileExists(dbPath & "-shm"):
    removeFile(dbPath & "-shm")

  var db: Sqlite3
  if sqlite3_open(dbPath.cstring, addr db) != SQLITE_OK:
    raise newException(IOError, "Failed to open SQLite database")
  defer: discard sqlite3_close(db)

  # Configure for fair durability comparison
  sqliteExec(db, "PRAGMA journal_mode = WAL")
  sqliteExec(db, "PRAGMA synchronous = FULL")  # Durable commits like DecentDB
  
  sqliteExec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
  
  var stmt: Sqlite3Stmt
  if sqlite3_prepare_v2(db, "INSERT INTO users VALUES (?, ?, ?)".cstring, -1, addr stmt, nil) != SQLITE_OK:
    raise newException(IOError, "Failed to prepare statement: " & $sqlite3_errmsg(db))
  defer: discard sqlite3_finalize(stmt)
  
  let iterations = 1000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    
    let t0 = getMonoTime()
    discard sqlite3_bind_int64(stmt, 1, int64(i))
    discard sqlite3_bind_text(stmt, 2, name.cstring, cint(name.len), SQLITE_TRANSIENT)
    discard sqlite3_bind_text(stmt, 3, email.cstring, cint(email.len), SQLITE_TRANSIENT)
    discard sqlite3_step(stmt)
    discard sqlite3_reset(stmt)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "SQLite",
    engine_version: $sqlite3_libversion(),
    dataset: "sample",
    benchmark: "insert",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runSqliteCommitLatency(outputDir: string) =
  echo "Running SQLite Commit Latency Benchmark..."
  let dbPath = getBenchDataDir() / "bench_sqlite_commit.db"
  registerDbArtifacts(dbPath, includeShm = true)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")
  if fileExists(dbPath & "-shm"):
    removeFile(dbPath & "-shm")

  var db: Sqlite3
  if sqlite3_open(dbPath.cstring, addr db) != SQLITE_OK:
    raise newException(IOError, "Failed to open SQLite database")
  defer: discard sqlite3_close(db)

  # Configure for fair durability comparison
  sqliteExec(db, "PRAGMA journal_mode = WAL")
  sqliteExec(db, "PRAGMA synchronous = FULL")  # Durable commits
  
  sqliteExec(db, "CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)")
  sqliteExec(db, "INSERT INTO kv VALUES (1, 'initial')")
  
  var stmt: Sqlite3Stmt
  if sqlite3_prepare_v2(db, "UPDATE kv SET v = ? WHERE k = 1".cstring, -1, addr stmt, nil) != SQLITE_OK:
    raise newException(IOError, "Failed to prepare statement: " & $sqlite3_errmsg(db))
  defer: discard sqlite3_finalize(stmt)
  
  let iterations = 1000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let value = "value" & $i
    
    let t0 = getMonoTime()
    discard sqlite3_bind_text(stmt, 1, value.cstring, cint(value.len), SQLITE_TRANSIENT)
    discard sqlite3_step(stmt)
    discard sqlite3_reset(stmt)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "SQLite",
    engine_version: $sqlite3_libversion(),
    dataset: "sample",
    benchmark: "commit_latency",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runSqlitePointRead(outputDir: string) =
  echo "Running SQLite Point Read Benchmark..."
  let dbPath = getBenchDataDir() / "bench_sqlite_read.db"
  registerDbArtifacts(dbPath, includeShm = true)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")
  if fileExists(dbPath & "-shm"):
    removeFile(dbPath & "-shm")

  var db: Sqlite3
  if sqlite3_open(dbPath.cstring, addr db) != SQLITE_OK:
    raise newException(IOError, "Failed to open SQLite database")
  defer: discard sqlite3_close(db)

  sqliteExec(db, "PRAGMA journal_mode = WAL")
  sqliteExec(db, "PRAGMA synchronous = FULL")
  
  sqliteExec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
  
  # Pre-populate
  var insertStmt: Sqlite3Stmt
  discard sqlite3_prepare_v2(db, "INSERT INTO users VALUES (?, ?, ?)".cstring, -1, addr insertStmt, nil)
  
  let dataSize = 1000
  for i in 1..dataSize:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    discard sqlite3_bind_int64(insertStmt, 1, int64(i))
    discard sqlite3_bind_text(insertStmt, 2, name.cstring, cint(name.len), SQLITE_TRANSIENT)
    discard sqlite3_bind_text(insertStmt, 3, email.cstring, cint(email.len), SQLITE_TRANSIENT)
    discard sqlite3_step(insertStmt)
    discard sqlite3_reset(insertStmt)
  discard sqlite3_finalize(insertStmt)
  
  var readStmt: Sqlite3Stmt
  if sqlite3_prepare_v2(db, "SELECT * FROM users WHERE id = ?".cstring, -1, addr readStmt, nil) != SQLITE_OK:
    raise newException(IOError, "Failed to prepare statement: " & $sqlite3_errmsg(db))
  defer: discard sqlite3_finalize(readStmt)
  
  let iterations = 100000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  var rng = initRand(42)
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let lookupId = rng.rand(1..dataSize)
    let t0 = getMonoTime()
    discard sqlite3_bind_int64(readStmt, 1, int64(lookupId))
    discard sqlite3_step(readStmt)
    discard sqlite3_reset(readStmt)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "SQLite",
    engine_version: $sqlite3_libversion(),
    dataset: "sample",
    benchmark: "point_read",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runSqliteJoin(outputDir: string) =
  echo "Running SQLite Join Benchmark..."
  let dbPath = getBenchDataDir() / "bench_sqlite_join.db"
  registerDbArtifacts(dbPath, includeShm = true)
  if fileExists(dbPath):
    removeFile(dbPath)
  if fileExists(dbPath & "-wal"):
    removeFile(dbPath & "-wal")
  if fileExists(dbPath & "-shm"):
    removeFile(dbPath & "-shm")

  var db: Sqlite3
  if sqlite3_open(dbPath.cstring, addr db) != SQLITE_OK:
    raise newException(IOError, "Failed to open SQLite database")
  defer: discard sqlite3_close(db)

  sqliteExec(db, "PRAGMA journal_mode = WAL")
  sqliteExec(db, "PRAGMA synchronous = FULL")
  
  sqliteExec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
  sqliteExec(db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)")
  
  # Pre-populate
  var userStmt: Sqlite3Stmt
  discard sqlite3_prepare_v2(db, "INSERT INTO users VALUES (?, ?)".cstring, -1, addr userStmt, nil)
  
  let userCount = 100
  for i in 1..userCount:
    let name = "User" & $i
    discard sqlite3_bind_int64(userStmt, 1, int64(i))
    discard sqlite3_bind_text(userStmt, 2, name.cstring, cint(name.len), SQLITE_TRANSIENT)
    discard sqlite3_step(userStmt)
    discard sqlite3_reset(userStmt)
  discard sqlite3_finalize(userStmt)
  
  var orderStmt: Sqlite3Stmt
  discard sqlite3_prepare_v2(db, "INSERT INTO orders VALUES (?, ?, ?)".cstring, -1, addr orderStmt, nil)
  
  var rng = initRand(42)
  let orderCount = 1000
  for i in 1..orderCount:
    discard sqlite3_bind_int64(orderStmt, 1, int64(i))
    discard sqlite3_bind_int64(orderStmt, 2, int64(rng.rand(1..userCount)))
    discard sqlite3_bind_int64(orderStmt, 3, int64(rng.rand(10..1000)))
    discard sqlite3_step(orderStmt)
    discard sqlite3_reset(orderStmt)
  discard sqlite3_finalize(orderStmt)
  
  var joinStmt: Sqlite3Stmt
  if sqlite3_prepare_v2(db, "SELECT u.name, SUM(o.amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name".cstring, -1, addr joinStmt, nil) != SQLITE_OK:
    raise newException(IOError, "Failed to prepare statement: " & $sqlite3_errmsg(db))
  defer: discard sqlite3_finalize(joinStmt)
  
  let iterations = 100
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let t0 = getMonoTime()
    while sqlite3_step(joinStmt) == SQLITE_ROW:
      discard  # Consume results
    discard sqlite3_reset(joinStmt)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration

  let p50ns = percentileNs(latenciesNs, 50.0)
  let p95ns = percentileNs(latenciesNs, 95.0)
  let p99ns = percentileNs(latenciesNs, 99.0)
  let p50 = int(p50ns div 1000)
  let p95 = int(p95ns div 1000)
  let p99 = int(p99ns div 1000)

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "SQLite",
    engine_version: $sqlite3_libversion(),
    dataset: "sample",
    benchmark: "join",
    durability: "safe",
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: p50,
      p95_us: p95,
      p99_us: p99,
      p50_ns: p50ns,
      p95_ns: p95ns,
      p99_ns: p99ns,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

# --- Main ---

import cligen

proc clearOldData(outputDir: string) =
  ## Remove existing benchmark files to ensure fresh run
  if dirExists(outputDir):
    for kind, path in walkDir(outputDir):
      if kind == pcFile and (path.endsWith(".jsonl") or path.endsWith(".json")):
        removeFile(path)
        echo "Removed stale: ", path

proc benchmark(engines: string = "all", clear: bool = true, data_dir: string = "", args: seq[string]) =
  defer:
    cleanupRegisteredArtifacts()

  if args.len == 0:
    echo "Error: output_dir is required as a positional argument."
    quit(1)
  
  let output_dir = args[0]
  
  # Set the global data directory for benchmark files (on real disk for fair fsync comparison)
  if data_dir.len > 0:
    gDataDir = data_dir
    echo "Using data directory: ", data_dir
  else:
    echo "Warning: No --data-dir specified, using system tmpdir which may be tmpfs (no real fsync)"
  
  echo "Starting benchmarks..."
  echo "Output directory: ", output_dir
  echo "Engines: ", engines
  
  createDir(output_dir)
  
  if clear:
    echo "Clearing old data from output directory..."
    clearOldData(output_dir)
  
  let runAll = engines == "all"
  let runDecent = runAll or "decentdb" in engines
  let runSqlite = runAll or "sqlite" in engines
  
  if runDecent:
    runDecentDbInsert(output_dir)
    runDecentDbCommitLatency(output_dir)
    runDecentDbPointRead(output_dir)
    runDecentDbJoin(output_dir)
  
  if runSqlite:
    runSqliteInsert(output_dir)
    runSqliteCommitLatency(output_dir)
    runSqlitePointRead(output_dir)
    runSqliteJoin(output_dir)

  echo "Benchmarks completed."

when isMainModule:
  dispatch(benchmark, help = {
    "engines": "Comma-separated list of engines to run (decentdb, sqlite) or 'all'",
    "clear": "Clear old benchmark data before running (default: true)",
    "data_dir": "Directory for benchmark database files (use real disk, not tmpfs for fair fsync comparison)"
  })
