import ../src/engine
import os, strutils

let dbPath = "/tmp/test_many_tables.ddb"
removeFile(dbPath)

# Create DB with 52 tables (mimicking Melodee schema)
block:
  let res = openDb(dbPath)
  assert res.ok, "Failed to open: " & res.err.message
  let db = res.value

  # Create a Library table
  discard execSql(db, """
    CREATE TABLE "Libraries" (
      "Id" INTEGER PRIMARY KEY,
      "Name" TEXT NOT NULL,
      "AlbumCount" INTEGER,
      "ApiKey" TEXT NOT NULL,
      "ArtistCount" INTEGER,
      "CreatedAt" TEXT,
      "Description" TEXT,
      "IsLocked" INTEGER,
      "LastScanAt" TEXT,
      "LastUpdatedAt" TEXT,
      "Notes" TEXT,
      "Path" TEXT,
      "SongCount" INTEGER,
      "SortOrder" INTEGER,
      "Tags" TEXT,
      "Type" INTEGER
    )
  """)

  # Create Artists table with FK to Libraries
  discard execSql(db, """
    CREATE TABLE "Artists" (
      "Id" INTEGER PRIMARY KEY,
      "Name" TEXT NOT NULL,
      "NameNormalized" TEXT,
      "LibraryId" INTEGER NOT NULL,
      "AlbumCount" INTEGER,
      "AlternateNames" TEXT,
      "AmgId" TEXT,
      "ApiKey" TEXT NOT NULL,
      "Biography" TEXT,
      "CalculatedRating" REAL,
      "CreatedAt" TEXT NOT NULL,
      "DeezerId" TEXT,
      "Description" TEXT,
      "Directory" TEXT,
      "DiscogsId" TEXT,
      "ImageCount" INTEGER,
      "IsLocked" INTEGER,
      "ItunesId" TEXT,
      "LastFmId" TEXT,
      "LastMetaDataUpdatedAt" TEXT,
      "LastPlayedAt" TEXT,
      "LastUpdatedAt" TEXT,
      "MetaDataStatus" INTEGER,
      "MusicBrainzId" TEXT,
      "Notes" TEXT,
      "PlayedCount" INTEGER,
      "RealName" TEXT,
      "Roles" TEXT,
      "SongCount" INTEGER,
      "SortName" TEXT,
      "SortOrder" INTEGER,
      "SpotifyId" TEXT,
      "Tags" TEXT,
      "WikiDataId" TEXT,
      FOREIGN KEY ("LibraryId") REFERENCES "Libraries" ("Id")
    )
  """)

  # Create 50 more dummy tables to match the Melodee schema count
  for i in 1..50:
    let name = "Table" & $i
    let sql = "CREATE TABLE \"" & name & "\" (\"Id\" INTEGER PRIMARY KEY, \"Name\" TEXT, \"Value\" TEXT, \"Ref\" INTEGER)"
    let r = execSql(db, sql)
    assert r.ok, "Create " & name & " failed: " & r.err.message

  # Insert seed data - Library
  discard execSql(db, """
    INSERT INTO "Libraries" ("Id", "Name", "ApiKey", "CreatedAt") 
    VALUES (1, 'TestLib', 'test-key', '2025-01-01')
  """)

  # Insert 312 rows of seed data into a dummy table (mimicking Settings)
  for i in 1..312:
    let sql = "INSERT INTO \"Table1\" (\"Id\", \"Name\", \"Value\") VALUES (" & $i & ", 'Key" & $i & "', 'Value" & $i & "')"
    let r = execSql(db, sql)
    assert r.ok, "Insert row " & $i & " failed: " & r.err.message

  discard closeDb(db)
  echo "DB created with 52 tables and 312 seed rows, closed"

# Reopen and run INNER JOIN SELECT
block:
  let res = openDb(dbPath)
  assert res.ok, "Failed to reopen: " & res.err.message
  let db = res.value

  echo "About to run INNER JOIN SELECT..."
  let r = execSql(db, """
    SELECT "a"."Id", "a"."AlbumCount", "a"."AlternateNames", "a"."AmgId", "a"."ApiKey",
           "a"."Biography", "a"."CalculatedRating", "a"."CreatedAt", "a"."DeezerId",
           "a"."Description", "a"."Directory", "a"."DiscogsId", "a"."ImageCount",
           "a"."IsLocked", "a"."ItunesId", "a"."LastFmId", "a"."LastMetaDataUpdatedAt",
           "a"."LastPlayedAt", "a"."LastUpdatedAt", "a"."LibraryId",
           "a"."MetaDataStatus", "a"."MusicBrainzId", "a"."Name", "a"."NameNormalized",
           "a"."Notes", "a"."PlayedCount", "a"."RealName", "a"."Roles",
           "a"."SongCount", "a"."SortName", "a"."SortOrder", "a"."SpotifyId",
           "a"."Tags", "a"."WikiDataId",
           "l"."Id", "l"."AlbumCount", "l"."ApiKey", "l"."ArtistCount",
           "l"."CreatedAt", "l"."Description", "l"."IsLocked", "l"."LastScanAt",
           "l"."LastUpdatedAt", "l"."Name", "l"."Notes", "l"."Path",
           "l"."SongCount", "l"."SortOrder", "l"."Tags", "l"."Type"
    FROM "Artists" AS "a"
    INNER JOIN "Libraries" AS "l" ON "a"."LibraryId" = "l"."Id"
    WHERE "a"."Id" = 999999
    LIMIT 1
  """)
  
  if r.ok:
    echo "Query OK, rows: ", r.value.len
  else:
    echo "Query failed: ", r.err.message

  discard closeDb(db)
  echo "Done"

removeFile(dbPath)
