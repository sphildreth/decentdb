## Tests for FK ON DELETE CASCADE, ON DELETE SET NULL, and TEXT/BLOB FK parents.
## Targets engine.nim L1357-L1478 (enforceRestrictOnDelete cascade/set-null paths)
## and L1120-L1185 (enforceForeignKeysBatch TEXT/BLOB parent path).
import unittest
import os
import engine

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path): removeFile(path)
  if fileExists(path & "-wal"): removeFile(path & "-wal")
  path

suite "FK ON DELETE CASCADE":
  test "ON DELETE CASCADE removes child rows":
    let path = makeTempDb("fk_del_cascade1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    discard execSql(db, "INSERT INTO child VALUES (11, 1)")
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    require delRes.ok
    let countRes = execSql(db, "SELECT COUNT(*) FROM child")
    require countRes.ok
    check countRes.value == @["0"]

  test "ON DELETE CASCADE with no referencing rows succeeds":
    let path = makeTempDb("fk_del_cascade2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO parent VALUES (2)")
    discard execSql(db, "INSERT INTO child VALUES (10, 2)")
    # Delete parent row 1 (no referencing children) 
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    require delRes.ok
    let countRes = execSql(db, "SELECT COUNT(*) FROM child")
    require countRes.ok
    check countRes.value == @["1"]

  test "ON DELETE CASCADE multi-level":
    let path = makeTempDb("fk_del_cascade3.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE grandparent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, gpid INT REFERENCES grandparent(id) ON DELETE CASCADE)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)")
    discard execSql(db, "INSERT INTO grandparent VALUES (1)")
    discard execSql(db, "INSERT INTO parent VALUES (10, 1)")
    discard execSql(db, "INSERT INTO child VALUES (100, 10)")
    # Delete grandparent → cascades to parent → cascades to child
    let delRes = execSql(db, "DELETE FROM grandparent WHERE id = 1")
    require delRes.ok
    let countChild = execSql(db, "SELECT COUNT(*) FROM child")
    require countChild.ok
    check countChild.value == @["0"]
    let countParent = execSql(db, "SELECT COUNT(*) FROM parent")
    require countParent.ok
    check countParent.value == @["0"]

suite "FK ON DELETE SET NULL":
  test "ON DELETE SET NULL nullifies child FK column":
    let path = makeTempDb("fk_del_setnull1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    require delRes.ok
    let pidRes = execSql(db, "SELECT pid FROM child WHERE id = 10")
    require pidRes.ok
    check pidRes.value == @["NULL"]

  test "ON DELETE SET NULL with multiple child rows":
    let path = makeTempDb("fk_del_setnull2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL)")
    discard execSql(db, "INSERT INTO parent VALUES (1)")
    discard execSql(db, "INSERT INTO parent VALUES (2)")
    discard execSql(db, "INSERT INTO child VALUES (10, 1)")
    discard execSql(db, "INSERT INTO child VALUES (11, 1)")
    discard execSql(db, "INSERT INTO child VALUES (12, 2)")
    let delRes = execSql(db, "DELETE FROM parent WHERE id = 1")
    require delRes.ok
    # Rows 10 and 11 should have NULL pid; row 12 unaffected
    let nullCount = execSql(db, "SELECT COUNT(*) FROM child WHERE pid IS NULL")
    require nullCount.ok
    check nullCount.value == @["2"]
    let pid12 = execSql(db, "SELECT pid FROM child WHERE id = 12")
    require pid12.ok
    check pid12.value == @["2"]

suite "FK TEXT parent column":
  test "INSERT child references TEXT parent column":
    let path = makeTempDb("fk_text_parent1.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE parent (code TEXT PRIMARY KEY)")
    discard execSql(db, "CREATE INDEX idx_parent_code ON parent(code)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_code TEXT REFERENCES parent(code))")
    discard execSql(db, "INSERT INTO parent VALUES ('US')")
    discard execSql(db, "INSERT INTO parent VALUES ('UK')")
    let insRes = execSql(db, "INSERT INTO child VALUES (1, 'US')")
    require insRes.ok
    let insRes2 = execSql(db, "INSERT INTO child VALUES (2, 'UK')")
    require insRes2.ok
    # FK violation: 'DE' not in parent
    let badRes = execSql(db, "INSERT INTO child VALUES (3, 'DE')")
    check not badRes.ok

  test "INSERT with NULL FK references TEXT parent (NULL allowed)":
    let path = makeTempDb("fk_text_parent2.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE parent (code TEXT PRIMARY KEY)")
    discard execSql(db, "CREATE INDEX idx_parent_code ON parent(code)")
    discard execSql(db, "CREATE TABLE child (id INT PRIMARY KEY, parent_code TEXT REFERENCES parent(code))")
    discard execSql(db, "INSERT INTO parent VALUES ('X')")
    # NULL FK should be allowed
    let insRes = execSql(db, "INSERT INTO child VALUES (1, NULL)")
    require insRes.ok
    let res = execSql(db, "SELECT parent_code FROM child WHERE id = 1")
    require res.ok
    check res.value == @["NULL"]

  test "Multiple TEXT FK references in same batch":
    let path = makeTempDb("fk_text_parent3.ddb")
    let dbRes = openDb(path)
    require dbRes.ok
    let db = dbRes.value
    defer: discard closeDb(db)
    discard execSql(db, "CREATE TABLE country (code TEXT PRIMARY KEY)")
    discard execSql(db, "CREATE INDEX idx_country ON country(code)")
    discard execSql(db, "CREATE TABLE city (id INT PRIMARY KEY, country_code TEXT REFERENCES country(code))")
    discard execSql(db, "INSERT INTO country VALUES ('US')")
    discard execSql(db, "INSERT INTO country VALUES ('CA')")
    # Insert multiple rows referencing TEXT parent
    let ins1 = execSql(db, "INSERT INTO city VALUES (1, 'US')")
    require ins1.ok
    let ins2 = execSql(db, "INSERT INTO city VALUES (2, 'CA')")
    require ins2.ok
    let ins3 = execSql(db, "INSERT INTO city VALUES (3, 'US')")
    require ins3.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM city")
    require cnt.ok
    check cnt.value == @["3"]
