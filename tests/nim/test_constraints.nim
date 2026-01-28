import unittest
import os
import engine
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  path

suite "Constraints":
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
    let delParent = execSql(db, "DELETE FROM parents WHERE id = 1")
    check not delParent.ok
    check delParent.err.code == ERR_CONSTRAINT
    let updParent = execSql(db, "UPDATE parents SET id = 2 WHERE id = 1")
    check not updParent.ok
    check updParent.err.code == ERR_CONSTRAINT
    let nullParent = execSql(db, "INSERT INTO parents (id, name) VALUES (NULL, 'B')")
    check not nullParent.ok
    check nullParent.err.code == ERR_CONSTRAINT
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
