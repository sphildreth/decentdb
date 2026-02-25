## Coverage tests for matchLikeInRecordStr / countLikeTableScan paths:
## - Table scan LIKE with lmContains (%needle%), lmPrefix (prefix%), lmSuffix (%suffix)
## - Both SELECT rows path (exec.nim L5249) and COUNT path (L1135)
## - Also covers parseLikePattern modes
import unittest
import os
import strutils
import sequtils
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

# Create a table WITHOUT trigram index to force table scan LIKE paths

suite "Table scan LIKE - contains mode":
  test "LIKE contains with multi-char needle":
    # Forces matchLikeInRecordStr lmContains with BMH table (needle >= 2 chars)
    let db = freshDb("tlk_c1.ddb")
    discard execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)")
    discard execSql(db, "INSERT INTO docs VALUES (1, 'hello world')")
    discard execSql(db, "INSERT INTO docs VALUES (2, 'goodbye earth')")
    discard execSql(db, "INSERT INTO docs VALUES (3, 'hello nim')")
    let res = execSql(db, "SELECT id FROM docs WHERE body LIKE '%ell%'")
    require res.ok
    let vals = res.value.mapIt(it.split("|")[0])
    check "1" in vals
    check "3" in vals
    check vals.len == 2
    discard closeDb(db)

  test "LIKE contains with 1-char needle":
    # Forces matchLikeInRecordStr lmContains without BMH (needle < 2)
    let db = freshDb("tlk_c2.ddb")
    discard execSql(db, "CREATE TABLE docs2 (id INT PRIMARY KEY, body TEXT)")
    discard execSql(db, "INSERT INTO docs2 VALUES (1, 'cat')")
    discard execSql(db, "INSERT INTO docs2 VALUES (2, 'dog')")
    discard execSql(db, "INSERT INTO docs2 VALUES (3, 'fish')")
    let res = execSql(db, "SELECT id FROM docs2 WHERE body LIKE '%a%'")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "1"
    discard closeDb(db)

  test "LIKE contains empty needle (%%  - match all)":
    # lmContains with empty needle - matches all non-null
    let db = freshDb("tlk_c3.ddb")
    discard execSql(db, "CREATE TABLE docs3 (id INT PRIMARY KEY, body TEXT)")
    discard execSql(db, "INSERT INTO docs3 VALUES (1, 'abc')")
    discard execSql(db, "INSERT INTO docs3 VALUES (2, 'xyz')")
    let res = execSql(db, "SELECT id FROM docs3 WHERE body LIKE '%%'")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "LIKE contains no match":
    let db = freshDb("tlk_c4.ddb")
    discard execSql(db, "CREATE TABLE docs4 (id INT PRIMARY KEY, body TEXT)")
    discard execSql(db, "INSERT INTO docs4 VALUES (1, 'hello')")
    let res = execSql(db, "SELECT id FROM docs4 WHERE body LIKE '%xyz%'")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

suite "Table scan LIKE - prefix mode":
  test "LIKE prefix matches":
    # Forces matchLikeInRecordStr lmPrefix
    let db = freshDb("tlk_p1.ddb")
    discard execSql(db, "CREATE TABLE docs5 (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO docs5 VALUES (1, 'apple')")
    discard execSql(db, "INSERT INTO docs5 VALUES (2, 'apricot')")
    discard execSql(db, "INSERT INTO docs5 VALUES (3, 'banana')")
    let res = execSql(db, "SELECT id FROM docs5 WHERE name LIKE 'ap%'")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "LIKE prefix no match":
    let db = freshDb("tlk_p2.ddb")
    discard execSql(db, "CREATE TABLE docs6 (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO docs6 VALUES (1, 'zebra')")
    let res = execSql(db, "SELECT id FROM docs6 WHERE name LIKE 'ap%'")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "LIKE prefix entire string matches":
    let db = freshDb("tlk_p3.ddb")
    discard execSql(db, "CREATE TABLE docs7 (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO docs7 VALUES (1, 'hello')")
    let res = execSql(db, "SELECT id FROM docs7 WHERE name LIKE 'hello%'")
    require res.ok
    check res.value.len == 1
    discard closeDb(db)

suite "Table scan LIKE - suffix mode":
  test "LIKE suffix matches":
    # Forces matchLikeInRecordStr lmSuffix
    let db = freshDb("tlk_s1.ddb")
    discard execSql(db, "CREATE TABLE docs8 (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO docs8 VALUES (1, 'hello')")
    discard execSql(db, "INSERT INTO docs8 VALUES (2, 'jello')")
    discard execSql(db, "INSERT INTO docs8 VALUES (3, 'world')")
    let res = execSql(db, "SELECT id FROM docs8 WHERE name LIKE '%llo'")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

  test "LIKE suffix no match":
    let db = freshDb("tlk_s2.ddb")
    discard execSql(db, "CREATE TABLE docs9 (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO docs9 VALUES (1, 'hello')")
    let res = execSql(db, "SELECT id FROM docs9 WHERE name LIKE '%xyz'")
    require res.ok
    check res.value.len == 0
    discard closeDb(db)

  test "LIKE suffix empty (match all - %%)":
    let db = freshDb("tlk_s3.ddb")
    discard execSql(db, "CREATE TABLE docs10 (id INT PRIMARY KEY, name TEXT)")
    discard execSql(db, "INSERT INTO docs10 VALUES (1, 'a')")
    discard execSql(db, "INSERT INTO docs10 VALUES (2, 'b')")
    # %'%' would be lmContains not lmSuffix; let's just use %ll for suffix
    let res = execSql(db, "SELECT id FROM docs10 WHERE name LIKE '%'")
    require res.ok
    check res.value.len == 2
    discard closeDb(db)

suite "COUNT LIKE table scan (countLikeTableScan paths)":
  test "COUNT(*) with prefix% LIKE via execSqlNoRows":
    # Covers countLikeTableScan lmPrefix path
    let db = freshDb("tlk_cnt1.ddb")
    discard execSql(db, "CREATE TABLE words (id INT PRIMARY KEY, w TEXT)")
    discard execSql(db, "INSERT INTO words VALUES (1, 'nimble')")
    discard execSql(db, "INSERT INTO words VALUES (2, 'nimrod')")
    discard execSql(db, "INSERT INTO words VALUES (3, 'python')")
    let res = execSqlNoRows(db, "SELECT * FROM words WHERE w LIKE 'nim%'", @[])
    require res.ok
    check res.value == 2
    discard closeDb(db)

  test "COUNT(*) with %suffix LIKE via execSqlNoRows":
    # Covers countLikeTableScan lmSuffix path
    let db = freshDb("tlk_cnt2.ddb")
    discard execSql(db, "CREATE TABLE words2 (id INT PRIMARY KEY, w TEXT)")
    discard execSql(db, "INSERT INTO words2 VALUES (1, 'program')")
    discard execSql(db, "INSERT INTO words2 VALUES (2, 'diagram')")
    discard execSql(db, "INSERT INTO words2 VALUES (3, 'hello')")
    let res = execSqlNoRows(db, "SELECT * FROM words2 WHERE w LIKE '%gram'", @[])
    require res.ok
    check res.value == 2
    discard closeDb(db)

  test "COUNT(*) with contains LIKE via execSqlNoRows":
    # Covers countLikeTableScan lmContains path
    let db = freshDb("tlk_cnt3.ddb")
    discard execSql(db, "CREATE TABLE words3 (id INT PRIMARY KEY, w TEXT)")
    discard execSql(db, "INSERT INTO words3 VALUES (1, 'elephant')")
    discard execSql(db, "INSERT INTO words3 VALUES (2, 'element')")
    discard execSql(db, "INSERT INTO words3 VALUES (3, 'hello')")
    let res = execSqlNoRows(db, "SELECT * FROM words3 WHERE w LIKE '%ele%'", @[])
    require res.ok
    check res.value == 2
    discard closeDb(db)

  test "LIKE with underscore wildcard (generic mode, no fast path)":
    # parseLikePattern returns lmGeneric for patterns with _
    let db = freshDb("tlk_gen1.ddb")
    discard execSql(db, "CREATE TABLE words4 (id INT PRIMARY KEY, w TEXT)")
    discard execSql(db, "INSERT INTO words4 VALUES (1, 'hello')")
    discard execSql(db, "INSERT INTO words4 VALUES (2, 'helo')")
    discard execSql(db, "INSERT INTO words4 VALUES (3, 'world')")
    let res = execSql(db, "SELECT id FROM words4 WHERE w LIKE 'h_llo'")
    require res.ok
    check res.value.len == 1
    check col0(res.value) == "1"
    discard closeDb(db)
