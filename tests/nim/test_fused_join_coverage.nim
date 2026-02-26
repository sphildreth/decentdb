## Coverage tests for tryFuseJoinSumAggregate:
## dense path (keys 1..N), sparse path (keys > 1M), reversed ON predicate.
## Targets exec.nim L4307-4594.
import unittest
import os
import strutils
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = (if ext.len == 0: path else: path & ext)
    if fileExists(f): removeFile(f)
  openDb(path).value

proc col0(rows: seq[string]): string =
  if rows.len == 0: return ""
  rows[0].split("|")[0]

# ---------------------------------------------------------------------------
# Basic fused join+sum (dense path, L4460-4547)
# ---------------------------------------------------------------------------
suite "tryFuseJoinSumAggregate dense path":
  test "JOIN + GROUP BY + SUM with int amount":
    # users(id PK, name), orders(id PK, user_id, amount INT)
    # SELECT u.name, SUM(o.amount) ... GROUP BY u.name
    let db = freshDb("tfjsa_d1.ddb")
    discard execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)")
    discard execSql(db, "INSERT INTO users VALUES (1, 'Alice')")
    discard execSql(db, "INSERT INTO users VALUES (2, 'Bob')")
    discard execSql(db, "INSERT INTO orders VALUES (1, 1, 100)")
    discard execSql(db, "INSERT INTO orders VALUES (2, 1, 200)")
    discard execSql(db, "INSERT INTO orders VALUES (3, 2, 150)")
    let res = execSql(db,
      "SELECT u.name, SUM(o.amount) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
    require res.ok
    check res.value.len == 2
    let r0 = res.value[0].split("|")
    check r0[0] == "Alice"
    check r0[1] == "300"
    let r1 = res.value[1].split("|")
    check r1[0] == "Bob"
    check r1[1] == "150"
    discard closeDb(db)

  test "JOIN + GROUP BY + SUM with REAL amount (float dense path)":
    # L4527-4531: vkFloat64 amount case in dense path
    let db = freshDb("tfjsa_d2.ddb")
    discard execSql(db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE sales (id INT PRIMARY KEY, cust_id INT, amount REAL)")
    discard execSql(db, "INSERT INTO customers VALUES (1, 'Alice')")
    discard execSql(db, "INSERT INTO customers VALUES (2, 'Bob')")
    discard execSql(db, "INSERT INTO sales VALUES (1, 1, 10.5)")
    discard execSql(db, "INSERT INTO sales VALUES (2, 1, 20.5)")
    discard execSql(db, "INSERT INTO sales VALUES (3, 2, 15.0)")
    let res = execSql(db,
      "SELECT c.name, SUM(s.amount) FROM customers c JOIN sales s ON c.id = s.cust_id GROUP BY c.name ORDER BY c.name")
    require res.ok
    check res.value.len == 2
    let r0 = res.value[0].split("|")
    check r0[0] == "Alice"
    check r0[1] == "31.0"
    discard closeDb(db)

  test "JOIN with reversed ON predicate (L4307-4309)":
    # ON o.user_id = u.id instead of ON u.id = o.user_id
    let db = freshDb("tfjsa_d3.ddb")
    discard execSql(db, "CREATE TABLE emps (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE wages (id INT PRIMARY KEY, emp_id INT, pay INT)")
    discard execSql(db, "INSERT INTO emps VALUES (1, 'Carol')")
    discard execSql(db, "INSERT INTO emps VALUES (2, 'Dave')")
    discard execSql(db, "INSERT INTO wages VALUES (1, 1, 500)")
    discard execSql(db, "INSERT INTO wages VALUES (2, 2, 600)")
    discard execSql(db, "INSERT INTO wages VALUES (3, 1, 300)")
    let res = execSql(db,
      "SELECT e.name, SUM(w.pay) FROM emps e JOIN wages w ON w.emp_id = e.id GROUP BY e.name ORDER BY e.name")
    require res.ok
    check res.value.len == 2
    let r0 = res.value[0].split("|")
    check r0[0] == "Carol"
    check r0[1] == "800"
    discard closeDb(db)

  test "JOIN with sum output first (nameFirst=false)":
    # Tests case where SUM comes first in SELECT list (L4546)
    let db = freshDb("tfjsa_d4.ddb")
    discard execSql(db, "CREATE TABLE grp (id INT PRIMARY KEY, lbl TEXT)")
    discard execSql(db, "CREATE TABLE vals (id INT PRIMARY KEY, grp_id INT, v INT)")
    discard execSql(db, "INSERT INTO grp VALUES (1, 'x')")
    discard execSql(db, "INSERT INTO vals VALUES (1, 1, 10)")
    discard execSql(db, "INSERT INTO vals VALUES (2, 1, 20)")
    let res = execSql(db,
      "SELECT SUM(v.v), g.lbl FROM grp g JOIN vals v ON g.id = v.grp_id GROUP BY g.lbl")
    require res.ok
    check res.value.len == 1
    let r0 = res.value[0].split("|")
    check r0[0] == "30"
    check r0[1] == "x"
    discard closeDb(db)

  test "JOIN with zero amount NULL handling":
    # vkNull amount in dense path (L4516-4519)
    let db = freshDb("tfjsa_d5.ddb")
    discard execSql(db, "CREATE TABLE ppl (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE txn (id INT PRIMARY KEY, ppl_id INT, amount INT)")
    discard execSql(db, "INSERT INTO ppl VALUES (1, 'Eve')")
    discard execSql(db, "INSERT INTO txn VALUES (1, 1, NULL)")
    discard execSql(db, "INSERT INTO txn VALUES (2, 1, 50)")
    let res = execSql(db,
      "SELECT p.name, SUM(t.amount) FROM ppl p JOIN txn t ON p.id = t.ppl_id GROUP BY p.name")
    require res.ok
    check res.value.len == 1
    let r0 = res.value[0].split("|")
    check r0[0] == "Eve"
    check r0[1] == "50"
    discard closeDb(db)

# ---------------------------------------------------------------------------
# Sparse path (keys > DenseMaxKey = 1_000_000, L4556-4594)
# ---------------------------------------------------------------------------
suite "tryFuseJoinSumAggregate sparse path":
  test "JOIN + GROUP BY + SUM with large key (sparse path)":
    # Key > 1_000_000 forces sparse mode (L4438: useDense=false)
    let db = freshDb("tfjsa_s1.ddb")
    discard execSql(db, "CREATE TABLE big_grp (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE big_vals (id INT PRIMARY KEY, grp_id INT, v INT)")
    discard execSql(db, "INSERT INTO big_grp VALUES (1000001, 'LargeKey')")
    discard execSql(db, "INSERT INTO big_vals VALUES (1, 1000001, 42)")
    discard execSql(db, "INSERT INTO big_vals VALUES (2, 1000001, 58)")
    let res = execSql(db,
      "SELECT g.name, SUM(v.v) FROM big_grp g JOIN big_vals v ON g.id = v.grp_id GROUP BY g.name")
    require res.ok
    check res.value.len == 1
    let r0 = res.value[0].split("|")
    check r0[0] == "LargeKey"
    check r0[1] == "100"
    discard closeDb(db)

  test "JOIN + GROUP BY + SUM with mixed dense/sparse keys":
    # Mix of small and large keys -> falls to sparse path
    let db = freshDb("tfjsa_s2.ddb")
    discard execSql(db, "CREATE TABLE mgrp (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE mvals (id INT PRIMARY KEY, grp_id INT, v INT)")
    discard execSql(db, "INSERT INTO mgrp VALUES (1, 'Small')")
    discard execSql(db, "INSERT INTO mgrp VALUES (1000002, 'Large')")
    discard execSql(db, "INSERT INTO mvals VALUES (1, 1, 10)")
    discard execSql(db, "INSERT INTO mvals VALUES (2, 1000002, 20)")
    let res = execSql(db,
      "SELECT g.name, SUM(v.v) FROM mgrp g JOIN mvals v ON g.id = v.grp_id GROUP BY g.name ORDER BY g.name")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "sparse path with float amounts":
    # Exercises L4572-4594 (sparse path with float amounts)
    let db = freshDb("tfjsa_s3.ddb")
    discard execSql(db, "CREATE TABLE sgrp (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "CREATE TABLE svals (id INT PRIMARY KEY, grp_id INT, v REAL)")
    discard execSql(db, "INSERT INTO sgrp VALUES (2000000, 'VeryLarge')")
    discard execSql(db, "INSERT INTO svals VALUES (1, 2000000, 1.5)")
    discard execSql(db, "INSERT INTO svals VALUES (2, 2000000, 2.5)")
    let res = execSql(db,
      "SELECT g.name, SUM(v.v) FROM sgrp g JOIN svals v ON g.id = v.grp_id GROUP BY g.name")
    require res.ok
    check res.value.len == 1
    let r0 = res.value[0].split("|")
    check r0[0] == "VeryLarge"
    check r0[1] == "4.0"
    discard closeDb(db)
