import os
import strutils
import times
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

type
  BenchmarkMetrics = object
    latencies_us: seq[int]
    p50_us: int
    p95_us: int
    p99_us: int
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

proc percentile(latencies: seq[int], p: float): int =
  if latencies.len == 0: return 0
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
  let dbPath = getTempDir() / "bench_decentdb_insert.ddb"
  removeFile(dbPath)
  removeFile(dbPath & ".wal")

  let db = openDb(dbPath).value
  defer: discard closeDb(db)

  discard execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)")
  
  let iterations = 1000
  var latencies: seq[int] = @[]
  
  let start = epochTime()
  
  for i in 1..iterations:
    let t0 = epochTime()
    discard execSql(db, "INSERT INTO users VALUES ($1, $2, $3)", @[
      Value(kind: vkInt64, int64Val: int64(i)),
      Value(kind: vkText, bytes: cast[seq[byte]]("User" & $i)),
      Value(kind: vkText, bytes: cast[seq[byte]]("user" & $i & "@example.com"))
    ])
    let t1 = epochTime()
    latencies.add(int((t1 - t0) * 1_000_000))
  
  let duration = epochTime() - start
  let opsPerSec = float(iterations) / duration

  let p50 = percentile(latencies, 50.0)
  let p95 = percentile(latencies, 95.0)
  let p99 = percentile(latencies, 99.0)

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

proc benchmark(engines: string = "all", args: seq[string]) =
  if args.len == 0:
    echo "Error: output_dir is required as a positional argument."
    quit(1)
  
  let output_dir = args[0]
  
  echo "Starting benchmarks..."
  echo "Output directory: ", output_dir
  echo "Engines: ", engines
  
  let runAll = engines == "all"
  let runDecent = runAll or "decentdb" in engines
  # let runSqlite = runAll or "sqlite" in engines
  # let runDuck = runAll or "duckdb" in engines
  
  if runDecent:
    runDecentDbInsert(output_dir)
  
  # Stub for other engines
  if not runDecent and not runAll:
    echo "Warning: Only DecentDB benchmarks are currently implemented in this runner."

  echo "Benchmarks completed."

when isMainModule:
  dispatch(benchmark, help = {
    "engines": "Comma-separated list of engines to run (decentdb, sqlite, duckdb) or 'all'"
  })
