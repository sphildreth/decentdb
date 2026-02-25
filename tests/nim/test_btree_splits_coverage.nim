## Coverage tests targeting btree splits, merges, and large data operations
## Targets: btree.nim leaf splits (L1752–1767), merges/rebalances (L1115–1119),
##          large sequential and reverse inserts, delete with rebalancing
import unittest
import os
import strutils
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = if ext.len == 0: path else: path & ext
    if fileExists(f): removeFile(f)
  openDb(path).value

suite "BTree large insert (force leaf splits)":
  test "Sequential insert 500 rows forces multiple splits":
    let db = freshDb("btree_seq500.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..500:
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*10) & ")")
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "500"
    # Verify data integrity with range scan
    let rng = execSql(db, "SELECT COUNT(*) FROM t WHERE id BETWEEN 100 AND 200")
    require rng.ok
    check rng.value[0] == "101"
    discard closeDb(db)

  test "Reverse insert 500 rows (worst case for splits)":
    let db = freshDb("btree_rev500.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    for i in countdown(500, 1):
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", 'value" & $i & "')")
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "500"
    # Range scan to verify tree integrity
    let rng = execSql(db, "SELECT COUNT(*) FROM t WHERE id > 400")
    require rng.ok
    check rng.value[0] == "100"
    discard closeDb(db)

  test "Random-order insert 200 rows":
    let db = freshDb("btree_rand200.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    # Insert in pseudo-random order (interleaving)
    var keys: seq[int] = @[]
    for i in 1..200:
      keys.add(i)
    # Simple shuffle: even then odd
    var ordered: seq[int] = @[]
    for i in keys:
      if i mod 2 == 0: ordered.add(i)
    for i in keys:
      if i mod 2 != 0: ordered.add(i)
    for k in ordered:
      let r = execSql(db, "INSERT INTO t VALUES (" & $k & ", " & $(k*100) & ")")
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "200"
    discard closeDb(db)

  test "Large TEXT keys for splits":
    let db = freshDb("btree_text_splits.ddb")
    discard execSql(db, "CREATE TABLE t (k TEXT PRIMARY KEY, v INT)")
    let chars = "abcdefghijklmnopqrstuvwxyz"
    for i in 1..100:
      let key = chars[i mod 26] & chars[(i*3) mod 26] & chars[(i*7) mod 26] & $i
      let r = execSql(db, "INSERT INTO t VALUES ('" & key & "', " & $i & ")")
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "100"
    discard closeDb(db)

suite "BTree delete with rebalancing (force merges)":
  test "Insert 200 then delete 150 (force merges)":
    let db = freshDb("btree_del_merge.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)")
    for i in 1..200:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i*2) & ")")
    # Delete most rows to trigger page merges
    for i in 1..150:
      let r = execSql(db, "DELETE FROM t WHERE id = " & $i)
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "50"
    # Verify remaining rows are correct
    let rng = execSql(db, "SELECT COUNT(*) FROM t WHERE id > 150")
    require rng.ok
    check rng.value[0] == "50"
    discard closeDb(db)

  test "Delete all rows then re-insert":
    let db = freshDb("btree_del_reinsert.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..100:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ")")
    discard execSql(db, "DELETE FROM t")
    let cnt1 = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt1.ok
    check cnt1.value[0] == "0"
    # Re-insert
    for i in 1..50:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ")")
    let cnt2 = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt2.ok
    check cnt2.value[0] == "50"
    discard closeDb(db)

  test "Alternating insert and delete to stress tree":
    let db = freshDb("btree_alt.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    # Insert 100
    for i in 1..100:
      discard execSql(db, "INSERT INTO t VALUES (" & $i & ", 'v" & $i & "')")
    # Delete every other
    for i in 1..50:
      discard execSql(db, "DELETE FROM t WHERE id = " & $(i*2))
    # Insert another 50 in the gaps
    for i in 1..50:
      discard execSql(db, "INSERT INTO t VALUES (" & $(i*2+100) & ", 'new" & $(i*2+100) & "')")
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "100"
    discard closeDb(db)

suite "BTree secondary index with many keys":
  test "Secondary index with 300 entries":
    let db = freshDb("btree_sec300.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, score INT)")
    discard execSql(db, "CREATE INDEX idx_score ON t (score)")
    for i in 1..300:
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", 'cat" & $(i mod 10) & "', " & $(i mod 100) & ")")
      if not r.ok:
        fail()
    # Index scan
    let rng = execSql(db, "SELECT COUNT(*) FROM t WHERE score = 50")
    require rng.ok
    check rng.value[0] == "3"  # scores 50, 150, 250
    discard closeDb(db)

  test "Composite index with many entries":
    let db = freshDb("btree_composite.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)")
    discard execSql(db, "CREATE INDEX idx_ab ON t (a, b)")
    for i in 1..200:
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", " & $(i mod 20) & ", " & $(i mod 10) & ")")
      if not r.ok:
        fail()
    let rng = execSql(db, "SELECT COUNT(*) FROM t WHERE a = 5 AND b = 5")
    require rng.ok
    check rng.value[0] == "10"  # a=5 for i mod 20=5, all also have b=5 since 5 mod 10=5
    discard closeDb(db)

suite "BTree with large values":
  test "Long TEXT values cause oversized pages":
    let db = freshDb("btree_longval.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, data TEXT)")
    let longStr = "x".repeat(200)
    for i in 1..50:
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", '" & longStr & $i & "')")
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "50"
    discard closeDb(db)

  test "Mixed short and long values":
    let db = freshDb("btree_mixed.ddb")
    discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
    for i in 1..100:
      let v = if i mod 3 == 0: "short" else: "longer_value_" & "x".repeat(50) & $i
      let r = execSql(db, "INSERT INTO t VALUES (" & $i & ", '" & v & "')")
      if not r.ok:
        fail()
    let cnt = execSql(db, "SELECT COUNT(*) FROM t")
    require cnt.ok
    check cnt.value[0] == "100"
    discard closeDb(db)
