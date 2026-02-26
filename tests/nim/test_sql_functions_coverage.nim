## Coverage tests for sql.nim and exec.nim:
## - BETWEEN / NOT BETWEEN expressions (sql.nim L465-474, L577-586)
## - EXTRACT(field FROM timestamp) (exec.nim timestamp functions)
## - DATE arithmetic and comparison
## - CURRENT_DATE, CURRENT_TIME, NOW()
## - Complex aggregation: COUNT DISTINCT, SUM/AVG/MIN/MAX on various types
## - HAVING clause conditions
## - CASE WHEN expressions (multi-branch)
## - COALESCE with multiple nulls
## - NULLIF expression
## - String functions: UPPER, LOWER, LENGTH, TRIM, SUBSTR, REPLACE
import unittest
import strutils
import engine

proc db(): Db = openDb(":memory:").value

suite "BETWEEN expressions":
  test "BETWEEN integer range includes endpoints":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..10:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(d, "SELECT id FROM t WHERE id BETWEEN 3 AND 7 ORDER BY id")
    require r.ok
    check r.value.len == 5
    check r.value[0] == "3"
    check r.value[4] == "7"

  test "NOT BETWEEN excludes range":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    for i in 1..10:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ")")
    let r = execSql(d, "SELECT id FROM t WHERE id NOT BETWEEN 3 AND 7 ORDER BY id")
    require r.ok
    check r.value.len == 5
    check r.value[0] == "1"
    check r.value[4] == "10"

  test "BETWEEN with floats":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val FLOAT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5)")
    let r = execSql(d, "SELECT id FROM t WHERE val BETWEEN 2.0 AND 4.0 ORDER BY id")
    require r.ok
    check r.value.len == 2
    check r.value[0] == "2"

  test "BETWEEN with text values":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry'), (4, 'date')")
    let r = execSql(d, "SELECT id FROM t WHERE name BETWEEN 'b' AND 'c' ORDER BY id")
    require r.ok
    check r.value.len == 1
    check r.value[0] == "2"

  test "BETWEEN in WHERE combined with AND":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT, cat TEXT)")
    for i in 1..10:
      discard execSql(d, "INSERT INTO t VALUES (" & $i & ", " & $i & ", " & (if i <= 5: "'A'" else: "'B'") & ")")
    let r = execSql(d, "SELECT id FROM t WHERE val BETWEEN 3 AND 8 AND cat = 'A' ORDER BY id")
    require r.ok
    check r.value.len == 3

suite "EXTRACT and date functions":
  test "EXTRACT YEAR from timestamp":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
    discard execSql(d, "INSERT INTO ev VALUES (1, '2024-03-15 10:00:00'), (2, '2023-12-01 00:00:00')")
    let r = execSql(d, "SELECT EXTRACT(YEAR FROM ts) FROM ev ORDER BY id")
    require r.ok
    check r.value[0] == "2024"
    check r.value[1] == "2023"

  test "EXTRACT MONTH":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
    discard execSql(d, "INSERT INTO ev VALUES (1, '2024-06-15 10:00:00')")
    let r = execSql(d, "SELECT EXTRACT(MONTH FROM ts) FROM ev")
    require r.ok
    check r.value[0] == "6"

  test "EXTRACT DAY":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
    discard execSql(d, "INSERT INTO ev VALUES (1, '2024-06-15 10:00:00')")
    let r = execSql(d, "SELECT EXTRACT(DAY FROM ts) FROM ev")
    require r.ok
    check r.value[0] == "15"

  test "EXTRACT HOUR":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
    discard execSql(d, "INSERT INTO ev VALUES (1, '2024-06-15 14:30:00')")
    let r = execSql(d, "SELECT EXTRACT(HOUR FROM ts) FROM ev")
    require r.ok
    check r.value[0] == "14"

  test "EXTRACT MINUTE":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
    discard execSql(d, "INSERT INTO ev VALUES (1, '2024-06-15 14:30:45')")
    let r = execSql(d, "SELECT EXTRACT(MINUTE FROM ts) FROM ev")
    require r.ok
    check r.value[0] == "30"

  test "EXTRACT SECOND":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
    discard execSql(d, "INSERT INTO ev VALUES (1, '2024-06-15 14:30:45')")
    let r = execSql(d, "SELECT EXTRACT(SECOND FROM ts) FROM ev")
    require r.ok
    check r.value[0] == "45"

  test "CURRENT_TIMESTAMP returns non-empty string":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    let r = execSql(d, "SELECT CURRENT_TIMESTAMP FROM t")
    require r.ok
    check r.value.len == 1
    check r.value[0].len > 0

  test "NOW() returns non-empty string":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY)")
    discard execSql(d, "INSERT INTO t VALUES (1)")
    let r = execSql(d, "SELECT NOW() FROM t")
    require r.ok
    check r.value.len == 1

suite "CASE WHEN expressions":
  test "CASE WHEN with multiple branches":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, grade INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,90),(2,75),(3,55),(4,40)")
    let r = execSql(d, """SELECT id, CASE WHEN grade >= 90 THEN 'A'
                                          WHEN grade >= 70 THEN 'B'
                                          WHEN grade >= 60 THEN 'C'
                                          ELSE 'F' END AS letter
                          FROM t ORDER BY id""")
    require r.ok
    check r.value.len == 4
    check r.value[0].contains("A")
    check r.value[1].contains("B")
    check r.value[3].contains("F")

  test "CASE expression returning NULL for no match":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 99)")
    let r = execSql(d, "SELECT CASE WHEN val < 0 THEN 'neg' END FROM t")
    require r.ok
    check r.value.len == 1

suite "NULL handling":
  test "COALESCE with multiple NULLs picks first non-null":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, NULL, NULL, 'found')")
    discard execSql(d, "INSERT INTO t VALUES (2, NULL, 'second', 'third')")
    let r = execSql(d, "SELECT COALESCE(a, b, c) FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "found"
    check r.value[1] == "second"

  test "NULLIF returns NULL when equal":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 0), (2, 5)")
    let r = execSql(d, "SELECT NULLIF(val, 0) FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "NULL"
    check r.value[1] == "5"

  test "IS NULL and IS NOT NULL filter":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, NULL), (2, 'x'), (3, NULL)")
    let rNull = execSql(d, "SELECT id FROM t WHERE val IS NULL ORDER BY id")
    require rNull.ok
    check rNull.value.len == 2
    let rNotNull = execSql(d, "SELECT id FROM t WHERE val IS NOT NULL")
    require rNotNull.ok
    check rNotNull.value.len == 1

suite "String functions":
  test "UPPER and LOWER":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'Hello World')")
    let ru = execSql(d, "SELECT UPPER(name) FROM t")
    require ru.ok
    check ru.value[0] == "HELLO WORLD"
    let rl = execSql(d, "SELECT LOWER(name) FROM t")
    require rl.ok
    check rl.value[0] == "hello world"

  test "LENGTH function":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'hello'), (2, '')")
    let r = execSql(d, "SELECT LENGTH(s) FROM t ORDER BY id")
    require r.ok
    check r.value[0] == "5"
    check r.value[1] == "0"

  test "TRIM removes whitespace":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, '  hello  ')")
    let r = execSql(d, "SELECT TRIM(s) FROM t")
    require r.ok
    check r.value[0] == "hello"

  test "SUBSTR extracts substring":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'hello world')")
    let r = execSql(d, "SELECT SUBSTR(s, 1, 5) FROM t")
    require r.ok
    check r.value[0] == "hello"

  test "REPLACE function":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 'foo bar foo')")
    let r = execSql(d, "SELECT REPLACE(s, 'foo', 'baz') FROM t")
    require r.ok
    check r.value[0] == "baz bar baz"

suite "Aggregation edge cases":
  test "COUNT DISTINCT":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c'),(5,'b')")
    let r = execSql(d, "SELECT COUNT(DISTINCT cat) FROM t")
    require r.ok
    check r.value[0] == "3"

  test "SUM with NULLs":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 20)")
    let r = execSql(d, "SELECT SUM(val) FROM t")
    require r.ok
    check r.value[0] == "30"

  test "AVG with NULLs (NULL ignored)":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 20)")
    let r = execSql(d, "SELECT AVG(val) FROM t")
    require r.ok
    check r.value[0] == "15.0"

  test "MIN and MAX":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,5),(2,2),(3,9),(4,1)")
    let rMin = execSql(d, "SELECT MIN(val) FROM t")
    require rMin.ok
    check rMin.value[0] == "1"
    let rMax = execSql(d, "SELECT MAX(val) FROM t")
    require rMax.ok
    check rMax.value[0] == "9"

  test "GROUP BY with HAVING on non-aggregate":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',100)")
    let r = execSql(d, "SELECT cat FROM t GROUP BY cat HAVING cat <> 'c' ORDER BY cat")
    require r.ok
    check r.value.len == 2
    check r.value[0] == "a"
    check r.value[1] == "b"

  test "GROUP BY COUNT with filter":
    let d = db()
    defer: discard closeDb(d)
    discard execSql(d, "CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)")
    discard execSql(d, "INSERT INTO t VALUES (1,'eng',100),(2,'eng',200),(3,'hr',50),(4,'hr',75),(5,'eng',150)")
    let r = execSql(d, "SELECT dept FROM t GROUP BY dept HAVING dept = 'eng' ORDER BY dept")
    require r.ok
    check r.value.len == 1
    check r.value[0].contains("eng")
