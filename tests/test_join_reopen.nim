import ../src/engine

import os

let dbPath = "/tmp/test_join_crash.ddb"
removeFile(dbPath)

# Create and populate
block:
  let res = openDb(dbPath)
  assert res.ok, "Failed to open: " & res.err.message
  let db = res.value

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
      "CreatedAt" TEXT NOT NULL,
      "Directory" TEXT,
      "Tags" TEXT
    )
  """)

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

  discard execSql(db, """
    INSERT INTO "Libraries" ("Id", "Name", "ApiKey", "CreatedAt") 
    VALUES (1, 'TestLib', 'test-key', '2025-01-01')
  """)

  discard closeDb(db)
  echo "DB created and closed"

# Reopen and query
block:
  let res = openDb(dbPath)
  assert res.ok, "Failed to reopen: " & res.err.message
  let db = res.value

  echo "About to run INNER JOIN SELECT..."
  let r = execSql(db, """
    SELECT "a"."Id", "a"."AlbumCount", "a"."AlternateNames", "a"."AmgId", "a"."ApiKey",
           "a"."Biography", "a"."CreatedAt", "a"."Directory", "a"."LibraryId",
           "a"."Name", "a"."NameNormalized", "a"."Tags",
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
