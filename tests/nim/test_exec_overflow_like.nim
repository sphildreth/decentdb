## Coverage tests for exec.nim LIKE on overflow rows (matchLikeInRecord path)
## and trigram search fallback paths.
## Targets: matchLikeInRecord (exec.nim L391-461, 721 C lines)
##          trigram fallback (exec.nim L3661-3688)
import unittest
import strutils
import engine

proc db(): Db = openDb(":memory:").value

suite "LIKE on overflow rows":
  test "prefix LIKE matches on overflow rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE big (id INT PRIMARY KEY, content TEXT)")
    let large = 'A'.repeat(5000)
    for i in 1..5:
      discard execSql(d, "INSERT INTO big VALUES (" & $i & ", '" & large & $i & "')")
    let r = execSql(d, "SELECT id FROM big WHERE content LIKE 'A%'")
    require r.ok
    check r.value.len == 5

  test "contains LIKE matches in overflow content":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE big (id INT PRIMARY KEY, content TEXT)")
    let large = 'X'.repeat(5000)
    discard execSql(d, "INSERT INTO big VALUES (1, '" & large & "MARKER" & large & "')")
    discard execSql(d, "INSERT INTO big VALUES (2, '" & large & "')")
    let r = execSql(d, "SELECT id FROM big WHERE content LIKE '%MARKER%'")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "1"

  test "suffix LIKE on overflow rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE big (id INT PRIMARY KEY, content TEXT)")
    let large = 'Y'.repeat(5000)
    discard execSql(d, "INSERT INTO big VALUES (1, '" & large & "SUFFIX')")
    discard execSql(d, "INSERT INTO big VALUES (2, '" & large & "')")
    let r = execSql(d, "SELECT id FROM big WHERE content LIKE '%SUFFIX'")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "1"

  test "LIKE on mixed overflow and non-overflow rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE mixed (id INT PRIMARY KEY, content TEXT)")
    let large = 'Z'.repeat(5000)
    discard execSql(d, "INSERT INTO mixed VALUES (1, 'short')")
    discard execSql(d, "INSERT INTO mixed VALUES (2, '" & large & "')")
    discard execSql(d, "INSERT INTO mixed VALUES (3, 'also short')")
    let r = execSql(d, "SELECT id FROM mixed WHERE content LIKE '%short%'")
    require r.ok
    check r.value.len == 2

  test "LIKE no match on overflow row":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE big (id INT PRIMARY KEY, content TEXT)")
    let large = 'A'.repeat(5000)
    discard execSql(d, "INSERT INTO big VALUES (1, '" & large & "')")
    let r = execSql(d, "SELECT id FROM big WHERE content LIKE '%NOMATCH%'")
    require r.ok
    check r.value.len == 0

  test "LIKE with ILIKE case insensitive on overflow rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE big (id INT PRIMARY KEY, content TEXT)")
    let large = 'A'.repeat(5000) & "target"
    discard execSql(d, "INSERT INTO big VALUES (1, '" & large & "')")
    discard execSql(d, "INSERT INTO big VALUES (2, '" & 'B'.repeat(5000) & "')")
    let r = execSql(d, "SELECT id FROM big WHERE content ILIKE '%TARGET%'")
    require r.ok
    check r.value.len == 1

suite "Trigram search paths":
  test "trigram index LIKE with sufficient length":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(d, "CREATE INDEX docs_body_trgm ON docs(body) USING trigram")
    for i in 1..30:
      discard execSql(d, "INSERT INTO docs VALUES (" & $i & ", 'document number " & $i & " with content')")
    let r = execSql(d, "SELECT id FROM docs WHERE body LIKE '%content%'")
    require r.ok
    check r.value.len == 30

  test "trigram LIKE with short pattern falls back to scan":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(d, "CREATE INDEX docs_body_trgm ON docs(body) USING trigram")
    for i in 1..10:
      discard execSql(d, "INSERT INTO docs VALUES (" & $i & ", 'ab " & $i & "')")
    # Pattern 'ab' is only 2 chars — below trigram threshold, falls back to scan
    let r = execSql(d, "SELECT id FROM docs WHERE body LIKE '%ab%'")
    require r.ok
    check r.value.len == 10

  test "trigram LIKE no match":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(d, "CREATE INDEX docs_body_trgm ON docs(body) USING trigram")
    for i in 1..5:
      discard execSql(d, "INSERT INTO docs VALUES (" & $i & ", 'hello world " & $i & "')")
    let r = execSql(d, "SELECT id FROM docs WHERE body LIKE '%NOMATCH%'")
    require r.ok
    check r.value.len == 0

  test "trigram LIKE prefix pattern":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(d, "CREATE INDEX docs_body_trgm ON docs(body) USING trigram")
    for i in 1..10:
      discard execSql(d, "INSERT INTO docs VALUES (" & $i & ", 'prefix" & $i & " content')")
    let r = execSql(d, "SELECT id FROM docs WHERE body LIKE 'prefix%'")
    require r.ok
    check r.value.len == 10

  test "trigram ILIKE case-insensitive search":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(d, "CREATE INDEX docs_body_trgm ON docs(body) USING trigram")
    discard execSql(d, "INSERT INTO docs VALUES (1, 'Hello World')")
    discard execSql(d, "INSERT INTO docs VALUES (2, 'HELLO WORLD')")
    discard execSql(d, "INSERT INTO docs VALUES (3, 'other text')")
    let r = execSql(d, "SELECT id FROM docs WHERE body ILIKE '%hello%'")
    require r.ok
    check r.value.len == 2

  test "COUNT with trigram LIKE":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(d, "CREATE INDEX docs_body_trgm ON docs(body) USING trigram")
    for i in 1..20:
      discard execSql(d, "INSERT INTO docs VALUES (" & $i & ", 'searchable content " & $i & "')")
    let r = execSql(d, "SELECT COUNT(*) FROM docs WHERE body LIKE '%searchable%'")
    require r.ok
    check r.value[0] == "20"
