import unittest
import os
import strutils
import engine
import catalog/catalog
import options
import tables

proc makeTempDb(name: string): string =
  let normalizedName = if name.endsWith(".db"): name[0 .. ^4] & ".ddb" else: name
  let path = getTempDir() / normalizedName
  if fileExists(path): removeFile(path)
  path

suite "Trigram Durability":
  test "trigram deltas are deferred until checkpoint":
    let path = makeTempDb("decentdb_trigram_deferred.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    # 1. Create table with text column
    check execSql(db, "CREATE TABLE songs (id INT, title TEXT)", @[]).ok
    
    # 2. Create trigram index
    check execSql(db, "CREATE INDEX title_idx ON songs USING TRIGRAM (title)", @[]).ok
    
    # 3. Insert row (Auto-commit)
    check execSql(db, "INSERT INTO songs VALUES (1, 'Imagine')", @[]).ok
    
    # 4. Verify deltas are present in memory (not flushed)
    # We access the internal catalog directly to check this state
    check db.catalog.trigramDeltas.len > 0
    
    # 5. Verify query works (uses in-memory deltas + disk)
    let rowsRes = execSql(db, "SELECT id FROM songs WHERE title LIKE '%mag%'", @[])
    check rowsRes.ok
    check rowsRes.value.len == 1
    
    # 6. Checkpoint
    check checkpointDb(db).ok
    
    # 7. Verify deltas are cleared (flushed to disk)
    check db.catalog.trigramDeltas.len == 0
    
    # 8. Verify query still works (now reading from disk)
    let rowsRes2 = execSql(db, "SELECT id FROM songs WHERE title LIKE '%mag%'")
    check rowsRes2.ok
    check rowsRes2.value.len == 1
    
    discard closeDb(db)
