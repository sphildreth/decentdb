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
import ../../src/version
when defined(bench_breakdown):
  import ../../src/wal/wal
  import ../../src/utils/bench_breakdown
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

# --- DuckDB FFI bindings ---
{.passL: "-lduckdb".}

type
  duckdb_database* = pointer
  duckdb_connection* = pointer
  duckdb_prepared_statement* = pointer
  duckdb_result* = object
    deprecated_column_count: uint64
    deprecated_row_count: uint64
    deprecated_rows_changed: uint64
    deprecated_columns: pointer
    deprecated_error_message: cstring
    internal_data: pointer
  duckdb_state* = enum
    DuckDBSuccess = 0
    DuckDBError = 1

proc duckdb_open*(path: cstring, out_database: ptr duckdb_database): duckdb_state {.cdecl, importc.}
proc duckdb_close*(database: ptr duckdb_database) {.cdecl, importc.}
proc duckdb_connect*(database: duckdb_database, out_connection: ptr duckdb_connection): duckdb_state {.cdecl, importc.}
proc duckdb_disconnect*(connection: ptr duckdb_connection) {.cdecl, importc.}
proc duckdb_query*(connection: duckdb_connection, query: cstring, out_result: ptr duckdb_result): duckdb_state {.cdecl, importc.}
proc duckdb_destroy_result*(result: ptr duckdb_result) {.cdecl, importc.}
proc duckdb_prepare*(connection: duckdb_connection, query: cstring, out_prepared_statement: ptr duckdb_prepared_statement): duckdb_state {.cdecl, importc.}
proc duckdb_destroy_prepare*(prepared_statement: ptr duckdb_prepared_statement) {.cdecl, importc.}
proc duckdb_bind_int64*(prepared_statement: duckdb_prepared_statement, param_idx: int64, val: int64): duckdb_state {.cdecl, importc.}
proc duckdb_bind_varchar_length*(prepared_statement: duckdb_prepared_statement, param_idx: int64, val: cstring, length: int64): duckdb_state {.cdecl, importc.}
proc duckdb_execute_prepared*(prepared_statement: duckdb_prepared_statement, out_result: ptr duckdb_result): duckdb_state {.cdecl, importc.}
proc duckdb_library_version*(): cstring {.cdecl, importc.}
proc duckdb_result_error*(result: duckdb_result): cstring {.cdecl, importc.}

# Helper for DuckDB execution
proc duckExec(con: duckdb_connection, sql: string) =
  var res: duckdb_result
  if duckdb_query(con, sql.cstring, addr res) != DuckDBSuccess:
    let err = $duckdb_result_error(res)
    duckdb_destroy_result(addr res)
    raise newException(IOError, "DuckDB exec failed: " & sql & " Error: " & err)
  duckdb_destroy_result(addr res)

# Global variable for benchmark data directory (on real disk, not tmpfs)
var gDataDir*: string = ""

# Global durability profile for benchmarks.
# - safe: aims for durable commits (SQLite synchronous=FULL; DecentDB default; DuckDB adds CHECKPOINT barriers)
# - default: engine defaults (SQLite synchronous=NORMAL; DuckDB no CHECKPOINT barrier)
var gDurability*: string = "safe"

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
    commit_breakdown_ns: JsonNode

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
  
  var metricsNode = %*{
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
  }
  if res.metrics.commit_breakdown_ns != nil and res.metrics.commit_breakdown_ns.kind != JNull:
    metricsNode["commit_breakdown_ns"] = res.metrics.commit_breakdown_ns

  let jsonNode = %*{
    "timestamp_utc": res.timestamp_utc,
    "engine": res.engine,
    "engine_version": res.engine_version,
    "dataset": res.dataset,
    "benchmark": res.benchmark,
    "durability": res.durability,
    "threads": res.threads,
    "iterations": res.iterations,
    "metrics": metricsNode,
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

proc runDecentDBInsert(outputDir: string) =
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
  
  let prepRes = prepare(db, "INSERT INTO users VALUES ($1, $2, $3)")
  if not prepRes.ok:
    raise newException(IOError, "Failed to prepare statement: " & prepRes.err.message)
  let stmt = prepRes.value

  let iterations = 10_000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()

  let beginRes = execSql(db, "BEGIN")
  if not beginRes.ok:
    raise newException(IOError, "DecentDB insert benchmark BEGIN failed: " & beginRes.err.message)

  when defined(bench_breakdown):
    resetInsertBenchBreakdown()

  for i in 1..iterations:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    let params = @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkText, bytes: toBytes(name)),
      Value(kind: vkText, bytes: toBytes(email))
    ]
    let t0 = getMonoTime()
    discard execPrepared(stmt, params)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))

  when defined(bench_breakdown):
    echo formatInsertBenchBreakdown()

  let commitRes = execSql(db, "COMMIT")
  if not commitRes.ok:
    raise newException(IOError, "DecentDB insert benchmark COMMIT failed: " & commitRes.err.message)
  
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
    engine_version: DecentDBVersion,
    dataset: "sample",
    benchmark: "insert",
    durability: gDurability,
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

proc runDecentDBCommitLatency(outputDir: string) =
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
  
  let prepRes = prepare(db, "UPDATE kv SET v = $1 WHERE k = 1")
  if not prepRes.ok:
    raise newException(IOError, "Failed to prepare statement: " & prepRes.err.message)
  let stmt = prepRes.value

  let iterations = 1000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  when defined(bench_breakdown):
    var marshalTotalNs: int64 = 0
    var preCommitTotalNs: int64 = 0
    var walEncodeWriteTotalNs: int64 = 0
    var walHeaderWriteTotalNs: int64 = 0
    var walFsyncTotalNs: int64 = 0
    var walIndexPublishTotalNs: int64 = 0
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    when defined(bench_breakdown):
      let marshalStart = getMonoTime()
    # Fair timing boundary: parameter construction/marshaling happens before t0,
    # matching SQLite's current benchmark structure.
    let value = "value" & $i
    let valueBytes = toBytes(value)
    let params = @[
      Value(kind: vkText, bytes: valueBytes)
    ]
    when defined(bench_breakdown):
      marshalTotalNs += nanosBetween(marshalStart, getMonoTime())

    when defined(bench_breakdown):
      clearLastCommitBreakdown()

    let t0 = getMonoTime()
    # Each UPDATE is a separate transaction with durable commit.
    let execRes = execPrepared(stmt, params)
    if not execRes.ok:
      raise newException(IOError, "DecentDB commit benchmark execution failed: " & execRes.err.message)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))

    when defined(bench_breakdown):
      let c = takeLastCommitBreakdown()
      walEncodeWriteTotalNs += c.walEncodeWriteNs
      walHeaderWriteTotalNs += c.walHeaderWriteNs
      walFsyncTotalNs += c.walFsyncNs
      walIndexPublishTotalNs += c.walIndexPublishNs
      let walTotal = c.walEncodeWriteNs + c.walHeaderWriteNs + c.walFsyncNs + c.walIndexPublishNs
      let preCommit = ns - walTotal
      if preCommit > 0:
        preCommitTotalNs += preCommit

  var commitBreakdownNode = newJNull()
  when defined(bench_breakdown):
    let walTotalNs = walEncodeWriteTotalNs + walHeaderWriteTotalNs + walFsyncTotalNs + walIndexPublishTotalNs
    commitBreakdownNode = %*{
      "param_marshal_ns_total": marshalTotalNs,
      "param_marshal_ns_avg": if iterations > 0: marshalTotalNs div int64(iterations) else: 0'i64,
      "statement_precommit_ns_total": preCommitTotalNs,
      "statement_precommit_ns_avg": if iterations > 0: preCommitTotalNs div int64(iterations) else: 0'i64,
      "wal_encode_write_ns_total": walEncodeWriteTotalNs,
      "wal_encode_write_ns_avg": if iterations > 0: walEncodeWriteTotalNs div int64(iterations) else: 0'i64,
      "wal_header_write_ns_total": walHeaderWriteTotalNs,
      "wal_header_write_ns_avg": if iterations > 0: walHeaderWriteTotalNs div int64(iterations) else: 0'i64,
      "wal_fsync_ns_total": walFsyncTotalNs,
      "wal_fsync_ns_avg": if iterations > 0: walFsyncTotalNs div int64(iterations) else: 0'i64,
      "wal_index_publish_ns_total": walIndexPublishTotalNs,
      "wal_index_publish_ns_avg": if iterations > 0: walIndexPublishTotalNs div int64(iterations) else: 0'i64,
      "wal_total_ns_total": walTotalNs,
      "wal_total_ns_avg": if iterations > 0: walTotalNs div int64(iterations) else: 0'i64
    }
  
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
    engine_version: DecentDBVersion,
    dataset: "sample",
    benchmark: "commit_latency",
    durability: gDurability,
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
      checksum_u64: 0,
      commit_breakdown_ns: commitBreakdownNode
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & "-wal"): getFileSize(dbPath & "-wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDecentDBPointRead(outputDir: string) =
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
    engine_version: DecentDBVersion,
    dataset: "sample",
    benchmark: "point_read",
    durability: gDurability,
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

proc runDecentDBJoin(outputDir: string) =
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
    engine_version: DecentDBVersion,
    dataset: "sample",
    benchmark: "join",
    durability: gDurability,
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
  if gDurability == "safe":
    sqliteExec(db, "PRAGMA synchronous = FULL")  # Durable commits
  else:
    sqliteExec(db, "PRAGMA synchronous = NORMAL")
  
  sqliteExec(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
  
  var stmt: Sqlite3Stmt
  if sqlite3_prepare_v2(db, "INSERT INTO users VALUES (?, ?, ?)".cstring, -1, addr stmt, nil) != SQLITE_OK:
    raise newException(IOError, "Failed to prepare statement: " & $sqlite3_errmsg(db))
  defer: discard sqlite3_finalize(stmt)
  
  let iterations = 10_000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()

  # Match DuckDB/DecentDB insert contract: measure bulk insert throughput in a
  # single transaction (commit cost is not charged per-row).
  sqliteExec(db, "BEGIN IMMEDIATE")
  
  for i in 1..iterations:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    
    let t0 = getMonoTime()
    discard sqlite3_bind_int64(stmt, 1, int64(i))
    discard sqlite3_bind_text(stmt, 2, name.cstring, cint(name.len), SQLITE_TRANSIENT)
    discard sqlite3_bind_text(stmt, 3, email.cstring, cint(email.len), SQLITE_TRANSIENT)
    let rc = sqlite3_step(stmt)
    if rc != SQLITE_DONE:
      raise newException(IOError, "SQLite insert failed: " & $sqlite3_errmsg(db))
    discard sqlite3_reset(stmt)
    let t1 = getMonoTime()
    let ns = nanosBetween(t0, t1)
    latenciesNs.add(ns)
    latencies.add(int(ns div 1000))

  sqliteExec(db, "COMMIT")
  
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
    durability: gDurability,
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
  if gDurability == "safe":
    sqliteExec(db, "PRAGMA synchronous = FULL")  # Durable commits
  else:
    sqliteExec(db, "PRAGMA synchronous = NORMAL")
  
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
    durability: gDurability,
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
  if gDurability == "safe":
    sqliteExec(db, "PRAGMA synchronous = FULL")
  else:
    sqliteExec(db, "PRAGMA synchronous = NORMAL")
  
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
    durability: gDurability,
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
  if gDurability == "safe":
    sqliteExec(db, "PRAGMA synchronous = FULL")
  else:
    sqliteExec(db, "PRAGMA synchronous = NORMAL")
  
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
    durability: gDurability,
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

# --- DuckDB Benchmarks ---

proc runDuckdbInsert(outputDir: string) =
  echo "Running DuckDB Insert Benchmark..."
  let dbPath = getBenchDataDir() / "bench_duckdb_insert.db"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath): removeFile(dbPath)
  if fileExists(dbPath & ".wal"): removeFile(dbPath & ".wal")

  var db: duckdb_database
  var con: duckdb_connection
  
  if duckdb_open(dbPath.cstring, addr db) != DuckDBSuccess:
    raise newException(IOError, "Failed to open DuckDB database")
  defer: duckdb_close(addr db)
  
  if duckdb_connect(db, addr con) != DuckDBSuccess:
    raise newException(IOError, "Failed to connect to DuckDB")
  defer: duckdb_disconnect(addr con)
  
  duckExec(con, "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, email VARCHAR)")
  
  var stmt: duckdb_prepared_statement
  if duckdb_prepare(con, "INSERT INTO users VALUES (?, ?, ?)".cstring, addr stmt) != DuckDBSuccess:
    raise newException(IOError, "Failed to prepare statement")
  defer: duckdb_destroy_prepare(addr stmt)
  
  let iterations = 10_000
  var latencies: seq[int] = @[] # Not used for insert
  
  let start = getMonoTime()
  
  duckExec(con, "BEGIN TRANSACTION")
  
  for i in 1..iterations:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    
    discard duckdb_bind_int64(stmt, 1, int64(i))
    discard duckdb_bind_varchar_length(stmt, 2, name.cstring, int64(name.len))
    discard duckdb_bind_varchar_length(stmt, 3, email.cstring, int64(email.len))
    
    var res: duckdb_result
    discard duckdb_execute_prepared(stmt, addr res)
    duckdb_destroy_result(addr res)
    # DuckDB prepare reuse is automatic/implied or we just re-bind?
    # Actually duckdb_execute_prepared doesn't reset bindings, we overwrite them.
    
  duckExec(con, "COMMIT")

  # DuckDB does not expose a SQLite-like synchronous setting.
  # For a closer "durable after commit" barrier in safe mode, force a CHECKPOINT
  # to synchronize the WAL into the database file.
  if gDurability == "safe":
    duckExec(con, "CHECKPOINT")
  
  let duration = secondsBetween(start, getMonoTime())
  let opsPerSec = float(iterations) / duration
  
  echo "DuckDB Insert Ops/Sec: ", opsPerSec

  let res = BenchmarkResult(
    timestamp_utc: getIsoTime(),
    engine: "DuckDB",
    engine_version: $duckdb_library_version(),
    dataset: "sample",
    benchmark: "insert",
    durability: gDurability,
    threads: 1,
    iterations: iterations,
    metrics: BenchmarkMetrics(
      latencies_us: latencies,
      p50_us: 0,
      p95_us: 0,
      p99_us: 0,
      p50_ns: 0,
      p95_ns: 0,
      p99_ns: 0,
      ops_per_sec: opsPerSec,
      rows_processed: iterations,
      checksum_u64: 0
    ),
    artifacts: BenchmarkArtifacts(
      db_path: dbPath,
      db_size_bytes: getFileSize(dbPath),
      wal_size_bytes: if fileExists(dbPath & ".wal"): getFileSize(dbPath & ".wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDuckdbCommitLatency(outputDir: string) =
  echo "Running DuckDB Commit Latency Benchmark..."
  let dbPath = getBenchDataDir() / "bench_duckdb_commit.db"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath): removeFile(dbPath)
  if fileExists(dbPath & ".wal"): removeFile(dbPath & ".wal")

  var db: duckdb_database
  var con: duckdb_connection
  
  if duckdb_open(dbPath.cstring, addr db) != DuckDBSuccess:
    raise newException(IOError, "Failed to open DuckDB database")
  defer: duckdb_close(addr db)
  
  if duckdb_connect(db, addr con) != DuckDBSuccess:
    raise newException(IOError, "Failed to connect to DuckDB")
  defer: duckdb_disconnect(addr con)
  
  duckExec(con, "CREATE TABLE kv (k BIGINT PRIMARY KEY, v VARCHAR)")
  duckExec(con, "INSERT INTO kv VALUES (1, 'initial')")
  
  var stmt: duckdb_prepared_statement
  if duckdb_prepare(con, "UPDATE kv SET v = ? WHERE k = 1".cstring, addr stmt) != DuckDBSuccess:
    raise newException(IOError, "Failed to prepare statement")
  defer: duckdb_destroy_prepare(addr stmt)
  
  let iterations = 1000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let value = "value" & $i
    
    let t0 = getMonoTime()
    discard duckdb_bind_varchar_length(stmt, 1, value.cstring, int64(value.len))
    
    var res: duckdb_result
    if duckdb_execute_prepared(stmt, addr res) != DuckDBSuccess:
       quit("Update failed")
    duckdb_destroy_result(addr res)

    # In safe mode, add a durability barrier by checkpointing after the transaction.
    # This synchronizes WAL contents into the DB file.
    if gDurability == "safe":
      duckExec(con, "CHECKPOINT")
    
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
    engine: "DuckDB",
    engine_version: $duckdb_library_version(),
    dataset: "sample",
    benchmark: "commit_latency",
    durability: gDurability,
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
      wal_size_bytes: if fileExists(dbPath & ".wal"): getFileSize(dbPath & ".wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDuckdbPointRead(outputDir: string) =
  echo "Running DuckDB Point Read Benchmark..."
  let dbPath = getBenchDataDir() / "bench_duckdb_read.db"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath): removeFile(dbPath)
  if fileExists(dbPath & ".wal"): removeFile(dbPath & ".wal")

  var db: duckdb_database
  var con: duckdb_connection
  
  if duckdb_open(dbPath.cstring, addr db) != DuckDBSuccess:
    raise newException(IOError, "Failed to open DuckDB database")
  defer: duckdb_close(addr db)
  
  if duckdb_connect(db, addr con) != DuckDBSuccess:
    raise newException(IOError, "Failed to connect to DuckDB")
  defer: duckdb_disconnect(addr con)
  
  duckExec(con, "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR, email VARCHAR)")
  
  var stmt: duckdb_prepared_statement
  if duckdb_prepare(con, "INSERT INTO users VALUES (?, ?, ?)".cstring, addr stmt) != DuckDBSuccess:
    raise newException(IOError, "Failed to prepare insert")
  defer: duckdb_destroy_prepare(addr stmt)
  
  let dataSize = 1000
  duckExec(con, "BEGIN TRANSACTION")
  for i in 1..dataSize:
    let name = "User" & $i
    let email = "user" & $i & "@example.com"
    discard duckdb_bind_int64(stmt, 1, int64(i))
    discard duckdb_bind_varchar_length(stmt, 2, name.cstring, int64(name.len))
    discard duckdb_bind_varchar_length(stmt, 3, email.cstring, int64(email.len))
    var res: duckdb_result
    discard duckdb_execute_prepared(stmt, addr res)
    duckdb_destroy_result(addr res)
  duckExec(con, "COMMIT")
  
  var readStmt: duckdb_prepared_statement
  if duckdb_prepare(con, "SELECT * FROM users WHERE id = ?".cstring, addr readStmt) != DuckDBSuccess:
    raise newException(IOError, "Failed to prepare read")
  defer: duckdb_destroy_prepare(addr readStmt)
  
  let iterations = 100000
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  var rng = initRand(42)
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let lookupId = rng.rand(1..dataSize)
    let t0 = getMonoTime()
    discard duckdb_bind_int64(readStmt, 1, int64(lookupId))
    var res: duckdb_result
    discard duckdb_execute_prepared(readStmt, addr res)
    duckdb_destroy_result(addr res)
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
    engine: "DuckDB",
    engine_version: $duckdb_library_version(),
    dataset: "sample",
    benchmark: "point_read",
    durability: gDurability,
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
      wal_size_bytes: if fileExists(dbPath & ".wal"): getFileSize(dbPath & ".wal") else: 0
    )
  )
  writeResult(outputDir, res)

proc runDuckdbJoin(outputDir: string) =
  echo "Running DuckDB Join Benchmark..."
  let dbPath = getBenchDataDir() / "bench_duckdb_join.db"
  registerDbArtifacts(dbPath, includeShm = false)
  if fileExists(dbPath): removeFile(dbPath)
  if fileExists(dbPath & ".wal"): removeFile(dbPath & ".wal")

  var db: duckdb_database
  var con: duckdb_connection
  
  if duckdb_open(dbPath.cstring, addr db) != DuckDBSuccess:
    raise newException(IOError, "Failed to open DuckDB database")
  defer: duckdb_close(addr db)
  
  if duckdb_connect(db, addr con) != DuckDBSuccess:
    raise newException(IOError, "Failed to connect to DuckDB")
  defer: duckdb_disconnect(addr con)
  
  duckExec(con, "CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR)")
  duckExec(con, "CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT, amount BIGINT)")
  
  duckExec(con, "BEGIN TRANSACTION")
  
  var userStmt: duckdb_prepared_statement
  discard duckdb_prepare(con, "INSERT INTO users VALUES (?, ?)".cstring, addr userStmt)
  let userCount = 100
  for i in 1..userCount:
    let name = "User" & $i
    discard duckdb_bind_int64(userStmt, 1, int64(i))
    discard duckdb_bind_varchar_length(userStmt, 2, name.cstring, int64(name.len))
    var res: duckdb_result
    discard duckdb_execute_prepared(userStmt, addr res)
    duckdb_destroy_result(addr res)
  duckdb_destroy_prepare(addr userStmt)
  
  var orderStmt: duckdb_prepared_statement
  discard duckdb_prepare(con, "INSERT INTO orders VALUES (?, ?, ?)".cstring, addr orderStmt)
  var rng = initRand(42)
  let orderCount = 1000
  for i in 1..orderCount:
    discard duckdb_bind_int64(orderStmt, 1, int64(i))
    discard duckdb_bind_int64(orderStmt, 2, int64(rng.rand(1..userCount)))
    discard duckdb_bind_int64(orderStmt, 3, int64(rng.rand(10..1000)))
    var res: duckdb_result
    discard duckdb_execute_prepared(orderStmt, addr res)
    duckdb_destroy_result(addr res)
  duckdb_destroy_prepare(addr orderStmt)
  
  duckExec(con, "COMMIT")
  
  var joinStmt: duckdb_prepared_statement
  if duckdb_prepare(con, "SELECT u.name, SUM(o.amount) FROM users u INNER JOIN orders o ON u.id = o.user_id GROUP BY u.id, u.name".cstring, addr joinStmt) != DuckDBSuccess:
    raise newException(IOError, "Failed to prepare join")
  defer: duckdb_destroy_prepare(addr joinStmt)
  
  let iterations = 100
  var latencies: seq[int] = @[]
  var latenciesNs: seq[int64] = @[]
  
  let start = getMonoTime()
  
  for i in 1..iterations:
    let t0 = getMonoTime()
    var res: duckdb_result
    discard duckdb_execute_prepared(joinStmt, addr res)
    duckdb_destroy_result(addr res)
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
    engine: "DuckDB",
    engine_version: $duckdb_library_version(),
    dataset: "sample",
    benchmark: "join",
    durability: gDurability,
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
      wal_size_bytes: if fileExists(dbPath & ".wal"): getFileSize(dbPath & ".wal") else: 0
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

proc benchmark(engines: string = "all", clear: bool = true, data_dir: string = "", durability: string = "safe", args: seq[string]) =
  defer:
    cleanupRegisteredArtifacts()

  if args.len == 0:
    echo "Error: output_dir is required as a positional argument."
    quit(1)
  
  let output_dir = args[0]

  # Durability profile
  gDurability = durability.toLowerAscii()
  if gDurability != "safe" and gDurability != "default":
    echo "Error: --durability must be 'safe' or 'default' (got: ", durability, ")"
    quit(1)
  
  # Set the global data directory for benchmark files (on real disk for fair fsync comparison)
  if data_dir.len > 0:
    gDataDir = data_dir
    echo "Using data directory: ", data_dir
  else:
    echo "Warning: No --data-dir specified, using system tmpdir which may be tmpfs (no real fsync)"
  
  echo "Starting benchmarks..."
  echo "Output directory: ", output_dir
  echo "Engines: ", engines
  echo "Durability profile: ", gDurability
  
  createDir(output_dir)
  
  if clear:
    echo "Clearing old data from output directory..."
    clearOldData(output_dir)
  
  let runAll = engines == "all"
  let runDecent = runAll or "decentdb" in engines
  let runSqlite = runAll or "sqlite" in engines
  let runDuck = runAll or "duckdb" in engines
  
  if runDecent:
    runDecentDBInsert(output_dir)
    runDecentDBCommitLatency(output_dir)
    runDecentDBPointRead(output_dir)
    runDecentDBJoin(output_dir)
  
  if runSqlite:
    runSqliteInsert(output_dir)
    runSqliteCommitLatency(output_dir)
    runSqlitePointRead(output_dir)
    runSqliteJoin(output_dir)

  if runDuck:
    runDuckdbInsert(output_dir)
    runDuckdbCommitLatency(output_dir)
    runDuckdbPointRead(output_dir)
    runDuckdbJoin(output_dir)

  echo "Benchmarks completed."

when isMainModule:
  dispatch(benchmark, help = {
    "engines": "Comma-separated list of engines to run (decentdb, sqlite, duckdb) or 'all'",
    "clear": "Clear old benchmark data before running (default: true)",
    "data_dir": "Directory for benchmark database files (use real disk, not tmpfs for fair fsync comparison)",
    "durability": "Durability profile: safe (durable-ish) or default (engine defaults)"
  })
