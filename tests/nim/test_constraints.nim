import unittest
import os
import engine
import errors
import record/record
import exec/exec

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Constraints":
  test "failed write before dirty pages does not corrupt cache":
    let path = makeTempDb("decentdb_constraints_cache.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))").ok
    let badChild = execSql(db, "INSERT INTO children (id, parent_id) VALUES (1, 99)")
    check not badChild.ok
    check badChild.err.code == ERR_CONSTRAINT
    check execSql(db, "INSERT INTO parents (id, name) VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO children (id, parent_id) VALUES (1, 1)").ok
    discard closeDb(db)

  test "failed write inside transaction does not corrupt cache":
    let path = makeTempDb("decentdb_constraints_cache_tx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))").ok
    check execSql(db, "BEGIN").ok
    let badChild = execSql(db, "INSERT INTO children (id, parent_id) VALUES (1, 99)")
    check not badChild.ok
    check badChild.err.code == ERR_CONSTRAINT
    check execSql(db, "ROLLBACK").ok
    check execSql(db, "INSERT INTO parents (id, name) VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO children (id, parent_id) VALUES (1, 1)").ok
    discard closeDb(db)

  test "not null, unique, and foreign keys":
    let path = makeTempDb("decentdb_constraints.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE parents (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE children (id INT PRIMARY KEY, parent_id INT REFERENCES parents(id))").ok
    let badChild = execSql(db, "INSERT INTO children (id, parent_id) VALUES (1, 99)")
    check not badChild.ok
    check badChild.err.code == ERR_CONSTRAINT
    check execSql(db, "INSERT INTO parents (id, name) VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO children (id, parent_id) VALUES (1, 1)").ok
    check execSql(db, "INSERT INTO children (id, parent_id) VALUES (2, 1)").ok
    check execSql(db, "DELETE FROM children WHERE id = 2").ok
    let delParent = execSql(db, "DELETE FROM parents WHERE id = 1")
    check not delParent.ok
    check delParent.err.code == ERR_CONSTRAINT
    let updParent = execSql(db, "UPDATE parents SET id = 2 WHERE id = 1")
    check not updParent.ok
    check updParent.err.code == ERR_CONSTRAINT
    check execSql(db, "DELETE FROM children WHERE id = 1").ok
    check execSql(db, "DELETE FROM parents WHERE id = 1").ok
    # INT PRIMARY KEY allows NULL → auto-increment (ADR-0092)
    let nullParent = execSql(db, "INSERT INTO parents (id, name) VALUES (NULL, 'B')")
    check nullParent.ok
    discard closeDb(db)

  test "unique constraint rejects duplicates":
    let path = makeTempDb("decentdb_unique.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE)").ok
    check execSql(db, "INSERT INTO users (id, email) VALUES (1, 'A')").ok
    let dup = execSql(db, "INSERT INTO users (id, email) VALUES (2, 'A')")
    check not dup.ok
    check dup.err.code == ERR_CONSTRAINT
    discard closeDb(db)

  test "CREATE UNIQUE INDEX rejects duplicates on INSERT":
    let path = makeTempDb("decentdb_unique_idx.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, code TEXT)").ok
    check execSql(db, "CREATE UNIQUE INDEX ux_code ON items(code)").ok
    check execSql(db, "INSERT INTO items (id, code) VALUES (1, 'X')").ok
    let dup = execSql(db, "INSERT INTO items (id, code) VALUES (2, 'X')")
    check not dup.ok
    check dup.err.code == ERR_CONSTRAINT
    discard closeDb(db)

  test "CREATE UNIQUE INDEX allows multiple NULLs":
    let path = makeTempDb("decentdb_unique_idx_null.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, code TEXT)").ok
    check execSql(db, "CREATE UNIQUE INDEX ux_code ON items(code)").ok
    check execSql(db, "INSERT INTO items (id, code) VALUES (1, NULL)").ok
    check execSql(db, "INSERT INTO items (id, code) VALUES (2, NULL)").ok
    discard closeDb(db)

  test "UNIQUE INDEX rejects duplicates on UPDATE":
    let path = makeTempDb("decentdb_unique_idx_upd.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, code TEXT)").ok
    check execSql(db, "CREATE UNIQUE INDEX ux_code ON items(code)").ok
    check execSql(db, "INSERT INTO items (id, code) VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO items (id, code) VALUES (2, 'B')").ok
    let upd = execSql(db, "UPDATE items SET code = 'A' WHERE id = 2")
    check not upd.ok
    check upd.err.code == ERR_CONSTRAINT
    discard closeDb(db)

  test "Self-referencing FK CREATE TABLE succeeds":
    let path = makeTempDb("decentdb_selfref_fk.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, """CREATE TABLE comments (
      id INT PRIMARY KEY,
      parent_id INT NULL REFERENCES comments(id) ON DELETE CASCADE,
      body TEXT NOT NULL
    )""").ok
    discard closeDb(db)

  test "Self-referencing FK INSERT root and child":
    let path = makeTempDb("decentdb_selfref_fk_insert.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, """CREATE TABLE comments (
      id INT PRIMARY KEY,
      parent_id INT NULL REFERENCES comments(id) ON DELETE CASCADE,
      body TEXT NOT NULL
    )""").ok
    check execSql(db, "INSERT INTO comments (id, parent_id, body) VALUES (1, NULL, 'root')").ok
    check execSql(db, "INSERT INTO comments (id, parent_id, body) VALUES (2, 1, 'reply')").ok
    let sel = execSqlRows(db, "SELECT id, parent_id, body FROM comments ORDER BY id", @[])
    check sel.ok
    check sel.value.len == 2
    check sel.value[0].values[2].kind == vkText
    check cast[string](sel.value[0].values[2].bytes) == "root"
    check cast[string](sel.value[1].values[2].bytes) == "reply"
    check sel.value[1].values[1].int64Val == 1
    discard closeDb(db)

  test "Self-referencing FK rejects invalid parent":
    let path = makeTempDb("decentdb_selfref_fk_reject.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, """CREATE TABLE comments (
      id INT PRIMARY KEY,
      parent_id INT NULL REFERENCES comments(id),
      body TEXT NOT NULL
    )""").ok
    let ins = execSql(db, "INSERT INTO comments (id, parent_id, body) VALUES (1, 999, 'orphan')")
    check not ins.ok
    discard closeDb(db)

  test "Self-referencing FK CASCADE DELETE removes children":
    let path = makeTempDb("decentdb_selfref_fk_cascade.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, """CREATE TABLE comments (
      id INT PRIMARY KEY,
      parent_id INT NULL REFERENCES comments(id) ON DELETE CASCADE,
      body TEXT NOT NULL
    )""").ok
    check execSql(db, "INSERT INTO comments (id, parent_id, body) VALUES (1, NULL, 'root')").ok
    check execSql(db, "INSERT INTO comments (id, parent_id, body) VALUES (2, 1, 'child')").ok
    check execSql(db, "INSERT INTO comments (id, parent_id, body) VALUES (3, 1, 'child2')").ok
    check execSql(db, "DELETE FROM comments WHERE id = 1").ok
    let sel = execSqlRows(db, "SELECT COUNT(*) FROM comments", @[])
    check sel.ok
    check sel.value[0].values[0].int64Val == 0
    discard closeDb(db)

  test "Self-referencing FK rejects bad column reference":
    let path = makeTempDb("decentdb_selfref_fk_badcol.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    let res = execSql(db, """CREATE TABLE comments (
      id INT PRIMARY KEY,
      parent_id INT NULL REFERENCES comments(nonexistent),
      body TEXT NOT NULL
    )""")
    check not res.ok
    discard closeDb(db)
