import ../src/c_api
import os, strutils

let dbPath = "/tmp/test_capi_reopen.ddb"
removeFile(dbPath)

proc exec(p: pointer, sql: string) =
  var stmt: pointer
  let rc = decentdb_prepare(p, sql.cstring, addr stmt)
  assert rc == 0, "prepare failed for: " & sql & " err=" & $decentdb_last_error_message(p)
  discard decentdb_step(stmt)
  decentdb_finalize(stmt)

# Phase 1: Create DB with many tables through C API
block:
  let p = decentdb_open(dbPath.cstring, nil)
  assert p != nil, "Failed to open: " & $decentdb_last_error_message(nil)

  exec(p, """CREATE TABLE "Libraries" ("Id" INTEGER PRIMARY KEY, "Name" TEXT NOT NULL, "AlbumCount" INTEGER, "ApiKey" TEXT NOT NULL, "ArtistCount" INTEGER, "CreatedAt" TEXT, "Description" TEXT, "IsLocked" INTEGER, "LastScanAt" TEXT, "LastUpdatedAt" TEXT, "Notes" TEXT, "Path" TEXT, "SongCount" INTEGER, "SortOrder" INTEGER, "Tags" TEXT, "Type" INTEGER)""")

  exec(p, """CREATE TABLE "Artists" ("Id" INTEGER PRIMARY KEY, "Name" TEXT NOT NULL, "NameNormalized" TEXT, "LibraryId" INTEGER NOT NULL REFERENCES "Libraries"("Id"), "AlbumCount" INTEGER, "AlternateNames" TEXT, "AmgId" TEXT, "ApiKey" TEXT NOT NULL, "Biography" TEXT, "CalculatedRating" REAL, "CreatedAt" TEXT NOT NULL, "Directory" TEXT, "Tags" TEXT)""")

  # Create 50 more tables to match Melodee scale
  for i in 1..50:
    exec(p, "CREATE TABLE \"Table" & $i & "\" (\"Id\" INTEGER PRIMARY KEY, \"Name\" TEXT, \"Value\" TEXT)")

  exec(p, """INSERT INTO "Libraries" ("Id", "Name", "ApiKey", "CreatedAt") VALUES (1, 'TestLib', 'test-key', '2025-01-01')""")

  for i in 1..100:
    exec(p, "INSERT INTO \"Table1\" (\"Id\", \"Name\", \"Value\") VALUES (" & $i & ", 'Key" & $i & "', 'Value" & $i & "')")

  discard decentdb_close(p)
  echo "Phase 1: Created 52 tables + seed data, closed"

# Phase 2: Reopen and query with INNER JOIN
block:
  let p = decentdb_open(dbPath.cstring, nil)
  assert p != nil, "Failed to reopen: " & $decentdb_last_error_message(nil)

  echo "About to run INNER JOIN SELECT..."
  var stmt: pointer
  let rc = decentdb_prepare(p, """SELECT "a"."Id", "a"."AlbumCount", "a"."AlternateNames", "a"."AmgId", "a"."ApiKey", "a"."Biography", "a"."CalculatedRating", "a"."CreatedAt", "a"."Directory", "a"."LibraryId", "a"."Name", "a"."NameNormalized", "a"."Tags", "l"."Id", "l"."AlbumCount", "l"."ApiKey", "l"."ArtistCount", "l"."CreatedAt", "l"."Description", "l"."IsLocked", "l"."LastScanAt", "l"."LastUpdatedAt", "l"."Name", "l"."Notes", "l"."Path", "l"."SongCount", "l"."SortOrder", "l"."Tags", "l"."Type" FROM "Artists" AS "a" INNER JOIN "Libraries" AS "l" ON "a"."LibraryId" = "l"."Id" WHERE "a"."Id" = 999999 LIMIT 1""".cstring, addr stmt)

  if rc != 0:
    echo "Prepare failed: ", $decentdb_last_error_message(p)
  else:
    let stepRc = decentdb_step(stmt)
    echo "Step result: ", stepRc
    decentdb_finalize(stmt)
    echo "Query completed successfully"

  discard decentdb_close(p)
  echo "Done"

removeFile(dbPath)
