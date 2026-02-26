## Coverage tests for INTERSECT and EXCEPT set operations (exec.nim L5064-5125)
## Also covers DENSE_RANK/RANK with partition changes (L3827)
## and other set operation edge cases.
import unittest
import os
import strutils
import algorithm
import engine
import errors

proc freshDb(name: string): Db =
  let path = getTempDir() / name
  for ext in ["", "-wal"]:
    let f = (if ext.len == 0: path else: path & ext)
    if fileExists(f): removeFile(f)
  openDb(path).value

proc sorted(s: seq[string]): seq[string] =
  result = s
  result.sort()

suite "INTERSECT set operation":
  test "INTERSECT basic - common elements":
    let db = freshDb("tia1.ddb")
    discard execSql(db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO a VALUES (1, 1)")
    discard execSql(db, "INSERT INTO a VALUES (2, 2)")
    discard execSql(db, "INSERT INTO a VALUES (3, 3)")
    discard execSql(db, "INSERT INTO b VALUES (1, 2)")
    discard execSql(db, "INSERT INTO b VALUES (2, 3)")
    discard execSql(db, "INSERT INTO b VALUES (3, 4)")
    let res = execSql(db, "SELECT v FROM a INTERSECT SELECT v FROM b")
    require res.ok
    check sorted(res.value) == @["2", "3"]
    discard closeDb(db)

  test "INTERSECT with no common elements":
    let db = freshDb("tia2.ddb")
    discard execSql(db, "CREATE TABLE x (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE y (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO x VALUES (1)")
    discard execSql(db, "INSERT INTO y VALUES (2)")
    let res = execSql(db, "SELECT v FROM x INTERSECT SELECT v FROM y")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT empty left side":
    let db = freshDb("tia3.ddb")
    discard execSql(db, "CREATE TABLE ea (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE eb (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO eb VALUES (1)")
    let res = execSql(db, "SELECT v FROM ea INTERSECT SELECT v FROM eb")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT empty right side":
    let db = freshDb("tia4.ddb")
    discard execSql(db, "CREATE TABLE fa (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE fb (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO fa VALUES (1)")
    let res = execSql(db, "SELECT v FROM fa INTERSECT SELECT v FROM fb")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT with text values":
    let db = freshDb("tia5.ddb")
    discard execSql(db, "CREATE TABLE wa (id INT PRIMARY KEY, w TEXT)")
    discard execSql(db, "CREATE TABLE wb (id INT PRIMARY KEY, w TEXT)")
    discard execSql(db, "INSERT INTO wa VALUES (1, 'hello')")
    discard execSql(db, "INSERT INTO wa VALUES (2, 'world')")
    discard execSql(db, "INSERT INTO wb VALUES (1, 'hello')")
    discard execSql(db, "INSERT INTO wb VALUES (2, 'foo')")
    let res = execSql(db, "SELECT w FROM wa INTERSECT SELECT w FROM wb")
    require res.ok
    check res.value == @["hello"]
    discard closeDb(db)

  test "INTERSECT deduplicates results":
    let db = freshDb("tia6.ddb")
    discard execSql(db, "CREATE TABLE da (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE db2 (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO da VALUES (1, 5)")
    discard execSql(db, "INSERT INTO da VALUES (2, 5)")
    discard execSql(db, "INSERT INTO db2 VALUES (1, 5)")
    discard execSql(db, "INSERT INTO db2 VALUES (2, 5)")
    let res = execSql(db, "SELECT v FROM da INTERSECT SELECT v FROM db2")
    require res.ok
    # INTERSECT removes duplicates - should return just one 5
    check res.value == @["5"]
    discard closeDb(db)

  test "INTERSECT column count mismatch returns error":
    let db = freshDb("tia7.ddb")
    discard execSql(db, "CREATE TABLE mm1 (a INT PRIMARY KEY, b INT)")
    discard execSql(db, "CREATE TABLE mm2 (a INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO mm1 VALUES (1, 2)")
    discard execSql(db, "INSERT INTO mm2 VALUES (1)")
    let res = execSql(db, "SELECT a, b FROM mm1 INTERSECT SELECT a FROM mm2")
    check not res.ok
    discard closeDb(db)

suite "EXCEPT set operation":
  test "EXCEPT basic":
    let db = freshDb("tea1.ddb")
    discard execSql(db, "CREATE TABLE la (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE lb (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO la VALUES (1, 1)")
    discard execSql(db, "INSERT INTO la VALUES (2, 2)")
    discard execSql(db, "INSERT INTO la VALUES (3, 3)")
    discard execSql(db, "INSERT INTO lb VALUES (1, 2)")
    discard execSql(db, "INSERT INTO lb VALUES (2, 3)")
    let res = execSql(db, "SELECT v FROM la EXCEPT SELECT v FROM lb")
    require res.ok
    check res.value == @["1"]
    discard closeDb(db)

  test "EXCEPT empty result when all excluded":
    let db = freshDb("tea2.ddb")
    discard execSql(db, "CREATE TABLE ma (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE mb (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO ma VALUES (1)")
    discard execSql(db, "INSERT INTO mb VALUES (1)")
    let res = execSql(db, "SELECT v FROM ma EXCEPT SELECT v FROM mb")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "EXCEPT column count mismatch returns error":
    let db = freshDb("tea3.ddb")
    discard execSql(db, "CREATE TABLE ec1 (a INT PRIMARY KEY, b INT)")
    discard execSql(db, "CREATE TABLE ec2 (a INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO ec1 VALUES (1, 2)")
    discard execSql(db, "INSERT INTO ec2 VALUES (1)")
    let res = execSql(db, "SELECT a, b FROM ec1 EXCEPT SELECT a FROM ec2")
    check not res.ok
    discard closeDb(db)

  test "EXCEPT with right side empty returns all left":
    let db = freshDb("tea4.ddb")
    discard execSql(db, "CREATE TABLE na (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE nb (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO na VALUES (1)")
    discard execSql(db, "INSERT INTO na VALUES (2)")
    let res = execSql(db, "SELECT v FROM na EXCEPT SELECT v FROM nb")
    require res.ok
    check sorted(res.value) == @["1", "2"]
    discard closeDb(db)

suite "DENSE_RANK with partition change detection":
  test "DENSE_RANK OVER PARTITION BY resets rank at partition boundary":
    let db = freshDb("tdr_part.ddb")
    discard execSql(db, "CREATE TABLE dr (id INT PRIMARY KEY, grp TEXT, score INT)")
    discard execSql(db, "INSERT INTO dr VALUES (1, 'A', 10)")
    discard execSql(db, "INSERT INTO dr VALUES (2, 'A', 20)")
    discard execSql(db, "INSERT INTO dr VALUES (3, 'A', 20)")
    discard execSql(db, "INSERT INTO dr VALUES (4, 'B', 5)")
    discard execSql(db, "INSERT INTO dr VALUES (5, 'B', 5)")
    discard execSql(db, "INSERT INTO dr VALUES (6, 'B', 15)")
    let res = execSql(db,
      "SELECT id, DENSE_RANK() OVER (PARTITION BY grp ORDER BY score) AS dr FROM dr ORDER BY id")
    require res.ok
    check res.value.len == 6
    # Group A: 10→rank1, 20→rank2, 20→rank2
    check res.value[0] == "1|1"
    check res.value[1] == "2|2"
    check res.value[2] == "3|2"
    # Group B resets: 5→rank1, 5→rank1, 15→rank2
    check res.value[3] == "4|1"
    check res.value[4] == "5|1"
    check res.value[5] == "6|2"
    discard closeDb(db)

  test "DENSE_RANK single partition no reset":
    let db = freshDb("tdr_single.ddb")
    discard execSql(db, "CREATE TABLE drs (id INT PRIMARY KEY, score INT)")
    discard execSql(db, "INSERT INTO drs VALUES (1, 10)")
    discard execSql(db, "INSERT INTO drs VALUES (2, 10)")
    discard execSql(db, "INSERT INTO drs VALUES (3, 20)")
    let res = execSql(db,
      "SELECT id, DENSE_RANK() OVER (ORDER BY score) AS dr FROM drs ORDER BY id")
    require res.ok
    check res.value.len == 3
    check res.value[0] == "1|1"
    check res.value[1] == "2|1"
    check res.value[2] == "3|2"
    discard closeDb(db)

suite "RANK function coverage":
  test "RANK OVER PARTITION BY":
    let db = freshDb("trank1.ddb")
    discard execSql(db, "CREATE TABLE rk (id INT PRIMARY KEY, dept TEXT, sal INT)")
    discard execSql(db, "INSERT INTO rk VALUES (1, 'eng', 100)")
    discard execSql(db, "INSERT INTO rk VALUES (2, 'eng', 100)")
    discard execSql(db, "INSERT INTO rk VALUES (3, 'eng', 200)")
    discard execSql(db, "INSERT INTO rk VALUES (4, 'mkt', 50)")
    discard execSql(db, "INSERT INTO rk VALUES (5, 'mkt', 50)")
    let res = execSql(db,
      "SELECT id, RANK() OVER (PARTITION BY dept ORDER BY sal) AS rk FROM rk ORDER BY id")
    require res.ok
    check res.value.len == 5
    # eng: 100,100 → rank1,rank1; 200 → rank3 (RANK skips)
    check res.value[0] == "1|1"
    check res.value[1] == "2|1"
    check res.value[2] == "3|3"
    # mkt: 50,50 → rank1,rank1
    check res.value[3] == "4|1"
    check res.value[4] == "5|1"
    discard closeDb(db)

suite "INTERSECT ALL set operation":
  test "INTERSECT ALL returns duplicate matching rows":
    let db = freshDb("tia_all_1.ddb")
    discard execSql(db, "CREATE TABLE iall_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE iall_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO iall_a VALUES (1)")
    discard execSql(db, "INSERT INTO iall_a VALUES (2)")
    discard execSql(db, "INSERT INTO iall_b VALUES (2)")
    discard execSql(db, "INSERT INTO iall_b VALUES (3)")
    let res = execSql(db, "SELECT v FROM iall_a INTERSECT ALL SELECT v FROM iall_b")
    require res.ok
    check res.value == @["2"]
    discard closeDb(db)

  test "INTERSECT ALL with multiple duplicates":
    let db = freshDb("tia_all_2.ddb")
    discard execSql(db, "CREATE TABLE iall2_a (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE iall2_b (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO iall2_a VALUES (1, 10), (2, 10), (3, 20)")
    discard execSql(db, "INSERT INTO iall2_b VALUES (1, 10), (2, 10), (3, 10)")
    let res = execSql(db, "SELECT v FROM iall2_a INTERSECT ALL SELECT v FROM iall2_b")
    require res.ok
    # min(count_left=2, count_right=3) = 2 copies of 10
    check res.value.len == 2
    check res.value[0] == "10"
    check res.value[1] == "10"
    discard closeDb(db)

  test "INTERSECT ALL with no matching rows":
    let db = freshDb("tia_all_3.ddb")
    discard execSql(db, "CREATE TABLE iall3_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE iall3_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO iall3_a VALUES (1)")
    discard execSql(db, "INSERT INTO iall3_b VALUES (2)")
    let res = execSql(db, "SELECT v FROM iall3_a INTERSECT ALL SELECT v FROM iall3_b")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT ALL with empty left side":
    let db = freshDb("tia_all_4.ddb")
    discard execSql(db, "CREATE TABLE iall4_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE iall4_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO iall4_b VALUES (1)")
    let res = execSql(db, "SELECT v FROM iall4_a INTERSECT ALL SELECT v FROM iall4_b")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "INTERSECT ALL with empty right side":
    let db = freshDb("tia_all_5.ddb")
    discard execSql(db, "CREATE TABLE iall5_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE iall5_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO iall5_a VALUES (1)")
    let res = execSql(db, "SELECT v FROM iall5_a INTERSECT ALL SELECT v FROM iall5_b")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

suite "EXCEPT ALL set operation":
  test "EXCEPT ALL subtracts matching rows":
    let db = freshDb("tea_all_1.ddb")
    discard execSql(db, "CREATE TABLE eall_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE eall_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO eall_a VALUES (1)")
    discard execSql(db, "INSERT INTO eall_a VALUES (2)")
    discard execSql(db, "INSERT INTO eall_b VALUES (1)")
    let res = execSql(db, "SELECT v FROM eall_a EXCEPT ALL SELECT v FROM eall_b")
    require res.ok
    check res.value == @["2"]
    discard closeDb(db)

  test "EXCEPT ALL with duplicates counts":
    let db = freshDb("tea_all_2.ddb")
    discard execSql(db, "CREATE TABLE eall2_a (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "CREATE TABLE eall2_b (id INT PRIMARY KEY, v INT)")
    discard execSql(db, "INSERT INTO eall2_a VALUES (1, 10), (2, 10), (3, 10)")
    discard execSql(db, "INSERT INTO eall2_b VALUES (1, 10)")
    let res = execSql(db, "SELECT v FROM eall2_a EXCEPT ALL SELECT v FROM eall2_b")
    require res.ok
    # left has 3×10, right has 1×10 → keep 2×10
    check res.value.len == 2
    discard closeDb(db)

  test "EXCEPT ALL with no overlap returns all left":
    let db = freshDb("tea_all_3.ddb")
    discard execSql(db, "CREATE TABLE eall3_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE eall3_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO eall3_a VALUES (1)")
    discard execSql(db, "INSERT INTO eall3_a VALUES (2)")
    discard execSql(db, "INSERT INTO eall3_b VALUES (3)")
    let res = execSql(db, "SELECT v FROM eall3_a EXCEPT ALL SELECT v FROM eall3_b")
    require res.ok
    check sorted(res.value) == @["1", "2"]
    discard closeDb(db)

  test "EXCEPT ALL subtracts all matching":
    let db = freshDb("tea_all_4.ddb")
    discard execSql(db, "CREATE TABLE eall4_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE eall4_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO eall4_a VALUES (1)")
    discard execSql(db, "INSERT INTO eall4_b VALUES (1)")
    let res = execSql(db, "SELECT v FROM eall4_a EXCEPT ALL SELECT v FROM eall4_b")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "EXCEPT ALL with empty right side returns all left":
    let db = freshDb("tea_all_5.ddb")
    discard execSql(db, "CREATE TABLE eall5_a (v INT PRIMARY KEY)")
    discard execSql(db, "CREATE TABLE eall5_b (v INT PRIMARY KEY)")
    discard execSql(db, "INSERT INTO eall5_a VALUES (1), (2)")
    let res = execSql(db, "SELECT v FROM eall5_a EXCEPT ALL SELECT v FROM eall5_b")
    require res.ok
    check sorted(res.value) == @["1", "2"]
    discard closeDb(db)
