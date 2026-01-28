import os
import times
import json
import algorithm
import engine
import record/record

type BenchResult = object
  name: string
  iterations: int
  samples: seq[float]
  p50Ms: float
  p95Ms: float

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
  let dbRes = openDb(path)
  if not dbRes.ok:
    quit(1)
  let db = dbRes.value
  let createRes = execSql(db, "CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)")
  if not createRes.ok:
    quit(1)
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
  let bulkRes = bulkLoad(db, "artists", rows, opts)
  if not bulkRes.ok:
    quit(1)
  var samples: seq[float] = @[]
  for i in 1 .. 1000:
    let id = i
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT name FROM artists WHERE id = $1", @[Value(kind: vkInt64, int64Val: id)])
    )
    samples.add(elapsed)
  discard closeDb(db)
  BenchResult(name: "point_lookup", iterations: 1000, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runFkJoin(): BenchResult =
  let path = makeTempDb("decentdb_bench_fk.db")
  let dbRes = openDb(path)
  if not dbRes.ok:
    quit(1)
  let db = dbRes.value
  if not execSql(db, "CREATE TABLE artists (id INT PRIMARY KEY, name TEXT)").ok:
    quit(1)
  if not execSql(db, "CREATE TABLE albums (id INT PRIMARY KEY, artistId INT REFERENCES artists(id))").ok:
    quit(1)
  if not execSql(db, "CREATE TABLE tracks (id INT PRIMARY KEY, albumId INT REFERENCES albums(id))").ok:
    quit(1)
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
  if not bulkLoad(db, "artists", artistRows, opts).ok:
    quit(1)
  if not bulkLoad(db, "albums", albumRows, opts).ok:
    quit(1)
  if not bulkLoad(db, "tracks", trackRows, opts).ok:
    quit(1)
  var samples: seq[float] = @[]
  for i in 1 .. 100:
    let artistId = (i mod 100) + 1
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT tracks.id FROM artists JOIN albums ON albums.artistId = artists.id JOIN tracks ON tracks.albumId = albums.id WHERE artists.id = $1", @[Value(kind: vkInt64, int64Val: artistId)])
    )
    samples.add(elapsed)
  discard closeDb(db)
  BenchResult(name: "fk_join", iterations: 100, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runSubstringSearch(): BenchResult =
  let path = makeTempDb("decentdb_bench_like.db")
  let dbRes = openDb(path)
  if not dbRes.ok:
    quit(1)
  let db = dbRes.value
  if not execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok:
    quit(1)
  if not execSql(db, "CREATE INDEX docs_body_idx ON docs (body)").ok:
    quit(1)
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
  if not bulkLoad(db, "docs", rows, opts).ok:
    quit(1)
  var samples: seq[float] = @[]
  for _ in 0 ..< 100:
    let elapsed = measureMs(proc() =
      discard execSql(db, "SELECT id FROM docs WHERE body LIKE '%world%'")
    )
    samples.add(elapsed)
  discard closeDb(db)
  BenchResult(name: "substring_search", iterations: 100, samples: samples, p50Ms: percentile(samples, 50), p95Ms: percentile(samples, 95))

proc runBulkLoad(): BenchResult =
  let path = makeTempDb("decentdb_bench_bulk.db")
  let dbRes = openDb(path)
  if not dbRes.ok:
    quit(1)
  let db = dbRes.value
  if not execSql(db, "CREATE TABLE bulk (id INT PRIMARY KEY, body TEXT)").ok:
    quit(1)
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
    if not bulkLoad(db, "bulk", rows, opts).ok:
      quit(1)
  )
  discard closeDb(db)
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
