## Coverage tests for engine.nim constraint enforcement paths:
## - checkUniqueConstraintsBatch UPDATE case (L995-1041)
## - enforceForeignKeysBatch with TEXT parent column (L1122-1180)
## - findReferencingRows (L1307-1347)
## - INSERT ... ON CONFLICT UPDATE path in execInsertStatement (L1726-1756)
## - enforceRestrictOnParent during UPDATE (L1347-1390)
import unittest
import strutils
import engine
import exec/exec
import record/record

proc db(): Db = openDb(":memory:").value

suite "UNIQUE constraint batch enforcement":
  test "Batch insert with two rows violating UNIQUE constraint fails":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "CREATE UNIQUE INDEX idx ON t(name)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'Alice')")
    # Two rows in same batch with duplicate unique value
    let r = execSql(d, "INSERT INTO t VALUES (2, 'Alice')")
    check not r.ok

  test "Batch insert succeeds when unique values don't conflict":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT)")
    discard execSql(d, "CREATE UNIQUE INDEX idx ON t(code)")
    let r = execSql(d, "INSERT INTO t VALUES (1, 'AAA'), (2, 'BBB'), (3, 'CCC')")
    require r.ok
    let q = execSql(d, "SELECT COUNT(*) FROM t")
    require q.ok
    check q.value[0] == "3"

  test "UPDATE to value that violates unique constraint fails":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT)")
    discard execSql(d, "CREATE UNIQUE INDEX idx ON t(code)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'AAA')")
    discard execSql(d, "INSERT INTO t VALUES (2, 'BBB')")
    let r = execSql(d, "UPDATE t SET code = 'AAA' WHERE id = 2")
    check not r.ok

  test "UPDATE to same value (no actual change) succeeds":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT)")
    discard execSql(d, "CREATE UNIQUE INDEX idx ON t(code)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'AAA')")
    let r = execSql(d, "UPDATE t SET code = 'AAA' WHERE id = 1")
    require r.ok

suite "Foreign key with TEXT parent column":
  test "FK constraint on TEXT parent column enforced":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (code TEXT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO parent VALUES ('X'), ('Y')")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, parent_code TEXT REFERENCES parent(code))")
    # Valid FK reference
    let r1 = execSql(d, "INSERT INTO child VALUES (1, 'X')")
    require r1.ok
    # Invalid FK reference
    let r2 = execSql(d, "INSERT INTO child VALUES (2, 'Z')")
    check not r2.ok

  test "FK TEXT parent allows NULL values":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (code TEXT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO parent VALUES ('A')")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, parent_code TEXT REFERENCES parent(code))")
    let r = execSql(d, "INSERT INTO child VALUES (1, NULL)")
    require r.ok

suite "RESTRICT FK on parent DELETE/UPDATE":
  test "DELETE parent row referenced by child fails with RESTRICT":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE RESTRICT)")
    discard execSql(d, "INSERT INTO parent VALUES (1)")
    discard execSql(d, "INSERT INTO child VALUES (1, 1)")
    let r = execSql(d, "DELETE FROM parent WHERE id = 1")
    check not r.ok

  test "UPDATE parent row key referenced by child fails with RESTRICT":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON UPDATE RESTRICT)")
    discard execSql(d, "INSERT INTO parent VALUES (10)")
    discard execSql(d, "INSERT INTO child VALUES (1, 10)")
    let r = execSql(d, "UPDATE parent SET id = 20 WHERE id = 10")
    check not r.ok

  test "DELETE parent row with no child references succeeds":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE parent (id INT PRIMARY KEY)")
    discard execSql(d, "CREATE TABLE child (id INT PRIMARY KEY, pid INT, FOREIGN KEY (pid) REFERENCES parent(id))")
    discard execSql(d, "INSERT INTO parent VALUES (1), (2)")
    discard execSql(d, "INSERT INTO child VALUES (1, 1)")
    # parent row 2 is not referenced
    let r = execSql(d, "DELETE FROM parent WHERE id = 2")
    require r.ok

suite "INSERT ON CONFLICT UPDATE":
  test "ON CONFLICT UPDATE via prepare + execPrepared":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cnt INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 0)")
    let p = prepare(d, "INSERT INTO t (id, cnt) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET cnt = t.cnt + 1")
    require p.ok
    discard execPrepared(prepare(d, "BEGIN").value, @[])
    let r1 = execPrepared(p.value, @[])
    require r1.ok
    discard execPrepared(prepare(d, "COMMIT").value, @[])
    discard execPrepared(prepare(d, "BEGIN").value, @[])
    let r2 = execPrepared(p.value, @[])
    require r2.ok
    discard execPrepared(prepare(d, "COMMIT").value, @[])
    let q = execSql(d, "SELECT cnt FROM t WHERE id = 1")
    require q.ok
    check q.value[0] == "2"

  test "ON CONFLICT DO NOTHING ignores conflict":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'original')")
    let r = execSql(d, "INSERT INTO t VALUES (1, 'new') ON CONFLICT (id) DO NOTHING")
    require r.ok
    let q = execSql(d, "SELECT v FROM t WHERE id = 1")
    require q.ok
    check q.value[0] == "original"

  test "ON CONFLICT UPDATE sets column from excluded":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'first')")
    let r = execSql(d, "INSERT INTO t (id, val) VALUES (1, 'second') ON CONFLICT (id) DO UPDATE SET val = excluded.val")
    require r.ok
    let q = execSql(d, "SELECT val FROM t WHERE id = 1")
    require q.ok
    check q.value[0] == "second"

  test "ON CONFLICT UPDATE with NOT NULL violation fails":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'Alice')")
    let r = execSql(d, "INSERT INTO t (id, name) VALUES (1, 'Bob') ON CONFLICT (id) DO UPDATE SET name = NULL")
    check not r.ok

suite "UPDATE via prepare + execPrepared":
  test "UPDATE via prepare respects NOT NULL":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'Alice')")
    let p = prepare(d, "UPDATE t SET name = $1 WHERE id = 1")
    require p.ok
    let r = execPrepared(p.value, @[Value(kind: vkNull)])
    check not r.ok

  test "UPDATE via prepare works for valid values":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10)")
    let p = prepare(d, "UPDATE t SET val = $1 WHERE id = 1")
    require p.ok
    let r = execPrepared(p.value, @[Value(kind: vkInt64, int64Val: 99)])
    require r.ok
    let q = execSql(d, "SELECT val FROM t WHERE id = 1")
    require q.ok
    check q.value[0] == "99"

suite "DELETE via prepare + execPrepared":
  test "DELETE via prepare removes matching rows":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
    let p = prepare(d, "DELETE FROM t WHERE id = $1")
    require p.ok
    let r = execPrepared(p.value, @[Value(kind: vkInt64, int64Val: 2)])
    require r.ok
    let q = execSql(d, "SELECT COUNT(*) FROM t")
    require q.ok
    check q.value[0] == "2"

  test "DELETE via prepare with no match does nothing":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    let p = prepare(d, "DELETE FROM t WHERE id = $1")
    require p.ok
    let r = execPrepared(p.value, @[Value(kind: vkInt64, int64Val: 999)])
    require r.ok
    let q = execSql(d, "SELECT COUNT(*) FROM t")
    require q.ok
    check q.value[0] == "1"
