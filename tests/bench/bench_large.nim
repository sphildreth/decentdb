import os
import times
import json
import algorithm
import random
import locks
import engine
import record/record
import vfs/os_vfs

type BenchResult = object
  name: string
  iterations: int
  samples: seq[float]
  p50Ms: float
  p95Ms: float

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

proc envInt(name: string, defaultVal: int): int =
  let v = getEnv(name)
  if v.len == 0:
    return defaultVal
  try:
    return parseInt(v)
  except ValueError:
    return defaultVal

proc makeTempFile(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

proc makeTempDb(name: string): string =
  let normalizedName =
    if name.len >= 3 and name[name.len - 3 .. ^1] == ".db":
      name[0 .. ^4] & ".ddb"
    else:
      name
  makeTempFile(normalizedName)

proc measureMs(action: proc()): float =
  let start = epochTime()
  action()
  (epochTime() - start) * 1000

proc percentile(samples: seq[float], pct: float): float =
  if samples.len == 0:
    return 0
  var sorted = samples
  sorted.sort()
  let idx = min(sorted.len - 1, int((pct / 100.0) * float(sorted.len - 1)))
  sorted[idx]

proc bulkLoadIds(db: Db, table: string, startId: int, count: int, makeRow: proc(id: int): seq[Value]): bool =
  var rows: seq[seq[Value]] = @[]
  rows.setLen(count)
  for i in 0 ..< count:
    rows[i] = makeRow(startId + i)
  var opts = defaultBulkLoadOptions()
  opts.disableIndexes = false
  opts.checkpointOnComplete = false
  opts.durability = dmNone
  bulkLoad(db, table, rows, opts).ok

proc runPointLookupLarge(): BenchResult =
  let rowCount = envInt("DECENTDB_BENCH_LARGE_ROWS", 200_000)
  let lookups = envInt("DECENTDB_BENCH_LARGE_LOOKUPS", 10_000)
  let batchSize = envInt("DECENTDB_BENCH_LARGE_BATCH", 10_000)
  let path = makeTempDb("decentdb_bench_large_point.db")
  let dbRes = openDb(path)
  if not dbRes.ok:
    quit(1)
  let db = dbRes.value
  if not execSql(db, "CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)").ok:
    quit(1)
  var inserted = 0
  while inserted < rowCount:
    let batch = min(batchSize, rowCount - inserted)
    if not bulkLoadIds(db, "artists", inserted + 1, batch, proc(id: int): seq[Value] =
      @[Value(kind: vkInt64, int64Val: id), Value(kind: vkText, bytes: toBytes("artist_" & $id))]
    ):
      quit(1)
    inserted += batch
  var rng = initRand(1337)
  var samples: seq[float] = @[]
  for _ in 0 ..< lookups:
    let id = rng.rand(rowCount - 1) + 1
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT name FROM artists WHERE id = $1", @[Value(kind: vkInt64, int64Val: id)])
    )
    samples.add(elapsed)
  discard closeDb(db)
  BenchResult(name: "point_lookup_large", iterations: lookups, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runOrderBySortLarge(): BenchResult =
  let rowCount = envInt("DECENTDB_BENCH_LARGE_SORT_ROWS", 200_000)
  let queries = envInt("DECENTDB_BENCH_LARGE_SORT_QUERIES", 20)
  let batchSize = envInt("DECENTDB_BENCH_LARGE_BATCH", 10_000)
  let path = makeTempDb("decentdb_bench_large_sort.db")
  let dbRes = openDb(path)
  if not dbRes.ok:
    quit(1)
  let db = dbRes.value
  if not execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok:
    quit(1)
  var inserted = 0
  while inserted < rowCount:
    let batch = min(batchSize, rowCount - inserted)
    if not bulkLoadIds(db, "docs", inserted + 1, batch, proc(id: int): seq[Value] =
      @[Value(kind: vkInt64, int64Val: id), Value(kind: vkText, bytes: toBytes("random text " & $id))]
    ):
      quit(1)
    inserted += batch
  var samples: seq[float] = @[]
  for _ in 0 ..< queries:
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT id FROM docs ORDER BY body LIMIT 1000")
    )
    samples.add(elapsed)
  discard closeDb(db)
  BenchResult(name: "order_by_sort_large", iterations: queries, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

type ReadWorkerArgs = object
  path: string
  reads: int
  pageSize: int
  fileSize: int64
  seed: int
  ok: ptr bool

proc readWorker(args: ReadWorkerArgs) {.thread.} =
  let vfs = newOsVfs()
  let fileRes = vfs.open(args.path, fmReadWrite, false)
  if not fileRes.ok:
    args.ok[] = false
    return
  let file = fileRes.value
  defer:
    discard vfs.close(file)
  var rng = initRand(args.seed)
  var buf = newString(args.pageSize)
  let maxPage = max(1, int(args.fileSize div int64(args.pageSize)))
  for _ in 0 ..< args.reads:
    let page = rng.rand(maxPage - 1) + 1
    let off = int64(page - 1) * int64(args.pageSize)
    let readRes = vfs.readStr(file, off, buf)
    if not readRes.ok or readRes.value < args.pageSize:
      args.ok[] = false
      return

proc runVfsConcurrentReads(): seq[BenchResult] =
  when not compileOption("threads"):
    return @[]
  let fileMb = envInt("DECENTDB_BENCH_VFS_MB", 64)
  let perThreadReads = envInt("DECENTDB_BENCH_VFS_READS", 20_000)
  let pageSize = 4096
  let path = makeTempFile("decentdb_bench_vfs.bin")
  let vfs = newOsVfs()
  let fileRes = vfs.open(path, fmReadWrite, true)
  if not fileRes.ok:
    quit(1)
  let file = fileRes.value
  defer:
    discard vfs.close(file)
    if fileExists(path):
      removeFile(path)
  var page = newSeq[byte](pageSize)
  let totalBytes = int64(fileMb) * 1024 * 1024
  var offset = 0'i64
  while offset < totalBytes:
    let wRes = vfs.write(file, offset, page)
    if not wRes.ok or wRes.value < pageSize:
      quit(1)
    offset += int64(pageSize)
  discard vfs.fsync(file)
  let size = getFileInfo(path).size

  let threadCounts = @[1, 2, 4, 8]
  for t in threadCounts:
    var samples: seq[float] = @[]
    for sampleIdx in 0 ..< 5:
      var threads: seq[Thread[ReadWorkerArgs]] = @[]
      threads.setLen(t)
      var oks: seq[bool] = newSeq[bool](t)
      for i in 0 ..< t:
        oks[i] = true
        let args = ReadWorkerArgs(path: path, reads: perThreadReads, pageSize: pageSize, fileSize: size, seed: 1000 + sampleIdx * 100 + i, ok: addr oks[i])
        createThread(threads[i], readWorker, args)
      let elapsed = measureMs(proc() =
        for i in 0 ..< t:
          joinThread(threads[i])
      )
      var allOk = true
      for okVal in oks:
        if not okVal:
          allOk = false
          break
      if not allOk:
        quit(1)
      let totalReads = float(t * perThreadReads)
      samples.add(elapsed / totalReads)
    result.add(BenchResult(name: "vfs_concurrent_read_t" & $t, iterations: t * perThreadReads, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95)))

proc toJson(bench: BenchResult): JsonNode =
  %*{
    "name": bench.name,
    "iterations": bench.iterations,
    "p50_ms": bench.p50Ms,
    "p95_ms": bench.p95Ms
  }

when isMainModule:
  let outPath = if paramCount() >= 1: paramStr(1) else: "tests/bench/results_large.json"
  var results: seq[BenchResult] = @[
    runPointLookupLarge(),
    runOrderBySortLarge()
  ]
  results.add(runVfsConcurrentReads())
  var jsonResults: seq[JsonNode] = @[]
  for res in results:
    jsonResults.add(toJson(res))
  let doc = %*{
    "generated_at": epochTime(),
    "benchmarks": jsonResults
  }
  createDir(parentDir(outPath))
  writeFile(outPath, $doc)
