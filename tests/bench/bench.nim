import os
import times
import json
import algorithm
import engine
import record/record
import errors

type BenchResult = object
  name: string
  iterations: int
  samples: seq[float]
  p50Ms: float
  p95Ms: float

proc requireOk[T](res: Result[T], what: string): T =
  if not res.ok:
    let ctx = if res.err.context.len > 0: " (" & res.err.context & ")" else: ""
    echo "bench error: " & what & ": " & $res.err.code & ": " & res.err.message & ctx
    quit(1)
  res.value

proc toBytes(text: string): seq[byte] =
  for ch in text:
    result.add(byte(ch))

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

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

proc runPointLookup(): BenchResult =
  let path = makeTempDb("decentdb_bench_point.db")
  let db = requireOk(openDb(path), "openDb(point)")
  discard requireOk(execSql(db, "CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)"), "create table artists")
  var rows: seq[seq[Value]] = @[]
  for i in 1 .. 1000:
    let name = "artist_" & $i
    rows.add(@[
      Value(kind: vkInt64, int64Val: i),
      Value(kind: vkText, bytes: toBytes(name))
    ])
  var opts = defaultBulkLoadOptions()
  opts.disableIndexes = true
  opts.durability = dmNone
  discard requireOk(bulkLoad(db, "artists", rows, opts), "bulkLoad artists")
  var samples: seq[float] = @[]
  for i in 1 .. 1000:
    let id = i
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT name FROM artists WHERE id = $1", @[Value(kind: vkInt64, int64Val: id)])
    )
    samples.add(elapsed)
  discard requireOk(closeDb(db), "closeDb(point)")
  BenchResult(name: "point_lookup", iterations: 1000, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runFkJoin(): BenchResult =
  let path = makeTempDb("decentdb_bench_fk.db")
  let db = requireOk(openDb(path), "openDb(fk)")
  discard requireOk(execSql(db, "CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)"), "create table artists(fk)")
  discard requireOk(execSql(db, "CREATE TABLE albums (id INT PRIMARY KEY, artistId INT REFERENCES artists(id))"), "create table albums")
  discard requireOk(execSql(db, "CREATE TABLE tracks (id INT PRIMARY KEY, albumId INT REFERENCES albums(id))"), "create table tracks")
  var artistRows: seq[seq[Value]] = @[]
  var albumRows: seq[seq[Value]] = @[]
  var trackRows: seq[seq[Value]] = @[]
  var albumId = 1
  var trackId = 1
  for artistId in 1 .. 100:
    artistRows.add(@[
      Value(kind: vkInt64, int64Val: artistId),
      Value(kind: vkText, bytes: toBytes("artist_" & $artistId))
    ])
    for _ in 0 ..< 5:
      albumRows.add(@[
        Value(kind: vkInt64, int64Val: albumId),
        Value(kind: vkInt64, int64Val: artistId)
      ])
      for _ in 0 ..< 10:
        trackRows.add(@[
          Value(kind: vkInt64, int64Val: trackId),
          Value(kind: vkInt64, int64Val: albumId)
        ])
        trackId.inc
      albumId.inc
  var opts = defaultBulkLoadOptions()
  opts.disableIndexes = true
  opts.durability = dmNone
  discard requireOk(bulkLoad(db, "artists", artistRows, opts), "bulkLoad artists(fk)")
  discard requireOk(bulkLoad(db, "albums", albumRows, opts), "bulkLoad albums")
  discard requireOk(bulkLoad(db, "tracks", trackRows, opts), "bulkLoad tracks")
  var samples: seq[float] = @[]
  for i in 1 .. 100:
    let artistId = (i mod 100) + 1
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT tracks.id FROM artists JOIN albums ON albums.artistId = artists.id JOIN tracks ON tracks.albumId = albums.id WHERE artists.id = $1", @[Value(kind: vkInt64, int64Val: artistId)])
    )
    samples.add(elapsed)
  discard requireOk(closeDb(db), "closeDb(fk)")
  BenchResult(name: "fk_join", iterations: 100, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runSubstringSearch(): BenchResult =
  let path = makeTempDb("decentdb_bench_like.db")
  let db = requireOk(openDb(path), "openDb(like)")
  discard requireOk(execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)"), "create table docs(like)")
  discard requireOk(execSql(db, "CREATE INDEX docs_body_trgm ON docs USING trigram (body)"), "create trigram index docs_body_trgm")
  var rows: seq[seq[Value]] = @[]
  for i in 1 .. 2000:
    let body = if i mod 5 == 0: "hello world " & $i else: "random text " & $i
    rows.add(@[
      Value(kind: vkInt64, int64Val: i),
      Value(kind: vkText, bytes: toBytes(body))
    ])
  var opts = defaultBulkLoadOptions()
  opts.disableIndexes = true
  opts.durability = dmNone
  discard requireOk(bulkLoad(db, "docs", rows, opts), "bulkLoad docs(like)")
  var samples: seq[float] = @[]
  for _ in 0 ..< 100:
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT id FROM docs WHERE body LIKE '%world%'")
    )
    samples.add(elapsed)
  discard requireOk(closeDb(db), "closeDb(like)")
  BenchResult(name: "substring_search", iterations: 100, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runOrderBySort(): BenchResult =
  let path = makeTempDb("decentdb_bench_sort.db")
  let db = requireOk(openDb(path), "openDb(sort)")
  discard requireOk(execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)"), "create table docs(sort)")
  var rows: seq[seq[Value]] = @[]
  for i in 1 .. 50000:
    let body = (if i mod 17 == 0: "hello world " else: "random text ") & $i
    rows.add(@[
      Value(kind: vkInt64, int64Val: i),
      Value(kind: vkText, bytes: toBytes(body))
    ])
  var opts = defaultBulkLoadOptions()
  opts.disableIndexes = true
  opts.durability = dmNone
  discard requireOk(bulkLoad(db, "docs", rows, opts), "bulkLoad docs(sort)")
  var samples: seq[float] = @[]
  for _ in 0 ..< 50:
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT id FROM docs ORDER BY body LIMIT 1000")
    )
    samples.add(elapsed)
  discard requireOk(closeDb(db), "closeDb(sort)")
  BenchResult(name: "order_by_sort", iterations: 50, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runBulkLoad(): BenchResult =
  let path = makeTempDb("decentdb_bench_bulk.db")
  let db = requireOk(openDb(path), "openDb(bulk)")
  discard requireOk(execSql(db, "CREATE TABLE bulk (id INT PRIMARY KEY, body TEXT)"), "create table bulk")
  var rows: seq[seq[Value]] = @[]
  for i in 1 .. 10000:
    rows.add(@[
      Value(kind: vkInt64, int64Val: i),
      Value(kind: vkText, bytes: toBytes("bulk_" & $i))
    ])
  var opts = defaultBulkLoadOptions()
  opts.disableIndexes = true
  opts.durability = dmNone
  let elapsed = measureMs(proc() =
    discard requireOk(bulkLoad(db, "bulk", rows, opts), "bulkLoad bulk")
  )
  discard requireOk(closeDb(db), "closeDb(bulk)")
  BenchResult(name: "bulk_load", iterations: 1, samples: @[elapsed], p50Ms: elapsed, p95Ms: elapsed)

proc toJson(bench: BenchResult): JsonNode =
  %*{
    "name": bench.name,
    "iterations": bench.iterations,
    "p50_ms": bench.p50Ms,
    "p95_ms": bench.p95Ms
  }

when isMainModule:
  let outPath = if paramCount() >= 1: paramStr(1) else: "tests/bench/results.json"
  let results = @[
    runPointLookup(),
    runFkJoin(),
    runSubstringSearch(),
    runOrderBySort(),
    runBulkLoad()
  ]
  var jsonResults: seq[JsonNode] = @[]
  for res in results:
    jsonResults.add(toJson(res))
  let doc = %*{
    "generated_at": epochTime(),
    "benchmarks": jsonResults
  }
  createDir(parentDir(outPath))
  writeFile(outPath, $doc)
