import unittest
import os
import strutils
import engine
import sql/sql

import tables
import catalog/catalog

proc makeTempDb(name: string): string =
  let normalizedName = if name.endsWith(".db"): name[0 .. ^4] & ".ddb" else: name
  let path = getTempDir() / normalizedName
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

suite "Trigram Persistence":
  test "trigram index survives clean close":
    let path = makeTempDb("decentdb_trigram_persist.db")
    
    # 1. Open, create, insert, close
    var dbRes = openDb(path)
    check dbRes.ok
    var db = dbRes.value
    check execSql(db, "CREATE TABLE songs (id INT, title TEXT)", @[]).ok
    check execSql(db, "CREATE INDEX title_idx ON songs USING TRIGRAM (title)", @[]).ok
    check execSql(db, "INSERT INTO songs VALUES (1, 'Imagine')", @[]).ok
    discard closeDb(db)
    
    # 2. Reopen and query
    dbRes = openDb(path)
    check dbRes.ok
    db = dbRes.value
    
    # Verify delta cache is empty (clean start)
    check db.catalog.trigramDeltas.len == 0
    
    # Query should find the row via index
    let rowsRes = execSql(db, "SELECT id FROM songs WHERE title LIKE '%mag%'", @[])
    check rowsRes.ok
    check rowsRes.value.len == 1
    
    discard closeDb(db)
