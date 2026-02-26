## test_exec_advanced_coverage.nim
## Targets exec.nim uncovered paths:
##   - CAST from vkDecimal column (L1585-1643)
##   - pkIndexEq cursor via secondary index equality scan (L924-958)
##   - ->> operator with all JSON value types (L2129-2161)
##   - fused join sum sparse path (keys > 1,000,000) (L4556-4577)
##   - trigram index COUNT (L1265-1283) and fallback rows (L3664-3685)
##   - INSERT...SELECT path in execSql (L2383-2441)

import unittest, os, strutils, engine

proc freshDb(name: string): Db =
  let p = getTempDir() / name & ".ddb"
  removeFile(p)
  removeFile(p & "-wal")
  openDb(p).value

proc col0(rows: seq[string]): string =
  if rows.len == 0: return ""
  rows[0].split("|")[0]

# ─────────────────────────────────────────────────────────────────────────────
suite "CAST from DECIMAL column":
# ─────────────────────────────────────────────────────────────────────────────
  test "CAST DECIMAL to INTEGER":
    let db = freshDb("cast_dec_int")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO t VALUES (1, 5.00), (2, 0.00)").ok
    let r = execSql(db, "SELECT CAST(v AS INTEGER) FROM t WHERE id = 1")
    check r.ok
    check r.value.len == 1
    discard closeDb(db)

  test "CAST DECIMAL to FLOAT":
    let db = freshDb("cast_dec_float")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO t VALUES (1, 3.00)").ok
    let r = execSql(db, "SELECT CAST(v AS FLOAT) FROM t WHERE id = 1")
    check r.ok
    check r.value.len == 1
    check r.value[0] == "3.0"
    discard closeDb(db)

  test "CAST DECIMAL to BOOLEAN nonzero":
    let db = freshDb("cast_dec_bool_t")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO t VALUES (1, 7.00)").ok
    let r = execSql(db, "SELECT CAST(v AS BOOLEAN) FROM t WHERE id = 1")
    check r.ok
    check col0(r.value) == "true"
    discard closeDb(db)

  test "CAST DECIMAL to BOOLEAN zero":
    let db = freshDb("cast_dec_bool_f")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO t VALUES (1, 0.00)").ok
    let r = execSql(db, "SELECT CAST(v AS BOOLEAN) FROM t WHERE id = 1")
    check r.ok
    check col0(r.value) == "false"
    discard closeDb(db)

  test "CAST DECIMAL to TEXT":
    let db = freshDb("cast_dec_text")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,2))").ok
    check execSql(db, "INSERT INTO t VALUES (1, 2.50)").ok
    let r = execSql(db, "SELECT CAST(v AS TEXT) FROM t WHERE id = 1")
    check r.ok
    check r.value.len == 1
    discard closeDb(db)

  test "CAST multiple DECIMAL rows":
    let db = freshDb("cast_dec_multi")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v DECIMAL(10,4))").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1.0000), (2, 2.5000), (3, 0.0000)").ok
    let r = execSql(db, "SELECT CAST(v AS INTEGER) FROM t ORDER BY id")
    check r.ok
    check r.value.len == 3
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Secondary index equality scan (pkIndexEq)":
# ─────────────────────────────────────────────────────────────────────────────
  test "basic equality via secondary index":
    let db = freshDb("pkidxeq_basic")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE INDEX t_v ON t(v)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "SELECT id FROM t WHERE v = 20")
    check r.ok
    check r.value == @["2"]
    discard closeDb(db)

  test "equality scan no match":
    let db = freshDb("pkidxeq_nomatch")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE INDEX t_v ON t(v)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 20)").ok
    let r = execSql(db, "SELECT id FROM t WHERE v = 99")
    check r.ok
    check r.value.len == 0
    discard closeDb(db)

  test "equality scan multi-match":
    let db = freshDb("pkidxeq_multi")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE INDEX t_v ON t(v)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20)").ok
    let r = execSql(db, "SELECT id FROM t WHERE v = 10 ORDER BY id")
    check r.ok
    check r.value == @["1", "2"]
    discard closeDb(db)

  test "equality scan on TEXT column":
    let db = freshDb("pkidxeq_text")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT)").ok
    check execSql(db, "CREATE INDEX t_code ON t(code)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'alpha'), (2, 'beta'), (3, 'alpha')").ok
    let r = execSql(db, "SELECT id FROM t WHERE code = 'alpha' ORDER BY id")
    check r.ok
    check r.value == @["1", "3"]
    discard closeDb(db)

  test "equality scan on float column":
    let db = freshDb("pkidxeq_float")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, score REAL)").ok
    check execSql(db, "CREATE INDEX t_score ON t(score)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 3.14), (2, 2.72), (3, 3.14)").ok
    let r = execSql(db, "SELECT id FROM t WHERE score = 3.14 ORDER BY id")
    check r.ok
    check r.value.len >= 1
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "->> operator JSON extraction":
# ─────────────────────────────────────────────────────────────────────────────
  test "->> extracts integer value":
    let db = freshDb("json_arrow_int")
    let r = execSql(db, """SELECT '{"a":42}' ->> 'a'""")
    check r.ok
    check col0(r.value) == "42"
    discard closeDb(db)

  test "->> extracts float value":
    let db = freshDb("json_arrow_float")
    let r = execSql(db, """SELECT '{"x":3.14}' ->> 'x'""")
    check r.ok
    check col0(r.value) == "3.14"
    discard closeDb(db)

  test "->> extracts boolean true":
    let db = freshDb("json_arrow_bool_t")
    let r = execSql(db, """SELECT '{"flag":true}' ->> 'flag'""")
    check r.ok
    check col0(r.value) == "true"
    discard closeDb(db)

  test "->> extracts boolean false":
    let db = freshDb("json_arrow_bool_f")
    let r = execSql(db, """SELECT '{"ok":false}' ->> 'ok'""")
    check r.ok
    check col0(r.value) == "false"
    discard closeDb(db)

  test "->> extracts null JSON":
    let db = freshDb("json_arrow_null")
    let r = execSql(db, """SELECT '{"n":null}' ->> 'n'""")
    check r.ok
    check col0(r.value) == "null"
    discard closeDb(db)

  test "->> extracts string value":
    let db = freshDb("json_arrow_str")
    let r = execSql(db, """SELECT '{"msg":"hello world"}' ->> 'msg'""")
    check r.ok
    check col0(r.value) == "hello world"
    discard closeDb(db)

  test "->> extracts array value":
    let db = freshDb("json_arrow_arr")
    let r = execSql(db, """SELECT '{"items":[1,2,3]}' ->> 'items'""")
    check r.ok
    check col0(r.value) == "[1,2,3]"
    discard closeDb(db)

  test "->> returns NULL for missing key":
    let db = freshDb("json_arrow_miss")
    let r = execSql(db, """SELECT '{"a":1}' ->> 'b'""")
    check r.ok
    check col0(r.value) == "NULL"
    discard closeDb(db)

  test "->> with dollar sign path returns NULL":
    let db = freshDb("json_arrow_dollar")
    let r = execSql(db, """SELECT '{"a":1}' ->> '$.a'""")
    check r.ok
    check col0(r.value) == "NULL"
    discard closeDb(db)

  test "-> operator extracts":
    let db = freshDb("json_arrow1")
    let r = execSql(db, """SELECT '{"score":99}' -> 'score'""")
    check r.ok
    check col0(r.value) == "99"
    discard closeDb(db)

  test "->> on NULL JSON input returns NULL":
    let db = freshDb("json_arrow_null_in")
    let r = execSql(db, "SELECT NULL ->> 'a'")
    check r.ok
    check col0(r.value) == "NULL"
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Fused join sum sparse path (keys > 1M)":
# ─────────────────────────────────────────────────────────────────────────────
  test "JOIN SUM with large user IDs uses sparse path":
    let db = freshDb("fused_sparse")
    check execSql(db, "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE sales (id INT PRIMARY KEY, cid INT, amount INT)").ok
    # IDs > 1,000,000 force the sparse path
    check execSql(db, "INSERT INTO customers VALUES (1000001, 'BigAlice'), (1000002, 'BigBob')").ok
    check execSql(db, "INSERT INTO sales VALUES (1, 1000001, 100), (2, 1000001, 50), (3, 1000002, 75)").ok
    let r = execSql(db, "SELECT c.name, SUM(s.amount) FROM customers c INNER JOIN sales s ON c.id = s.cid GROUP BY c.id, c.name ORDER BY c.name")
    check r.ok
    check r.value.len == 2
    check r.value.contains("BigAlice|150.0")
    check r.value.contains("BigBob|75.0")
    discard closeDb(db)

  test "JOIN SUM sparse with float amounts":
    let db = freshDb("fused_sparse_float")
    check execSql(db, "CREATE TABLE accts (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE txns (id INT PRIMARY KEY, aid INT, amount REAL)").ok
    check execSql(db, "INSERT INTO accts VALUES (2000001, 'X'), (2000002, 'Y')").ok
    check execSql(db, "INSERT INTO txns VALUES (1, 2000001, 1.5), (2, 2000001, 2.5), (3, 2000002, 10.0)").ok
    let r = execSql(db, "SELECT a.name, SUM(t.amount) FROM accts a INNER JOIN txns t ON a.id = t.aid GROUP BY a.id, a.name ORDER BY a.name")
    check r.ok
    check r.value.len == 2
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "Trigram index COUNT and fallback":
# ─────────────────────────────────────────────────────────────────────────────
  test "COUNT with trigram index LIKE":
    let db = freshDb("trig_count")
    check execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body ON docs USING TRIGRAM (body)").ok
    check execSql(db, "INSERT INTO docs VALUES (1, 'hello world'), (2, 'foo bar baz'), (3, 'hello nim')").ok
    let r = execSql(db, "SELECT COUNT(*) FROM docs WHERE body LIKE '%hello%'")
    check r.ok
    check col0(r.value) == "2"
    discard closeDb(db)

  test "trigram search exact match":
    let db = freshDb("trig_seek")
    check execSql(db, "CREATE TABLE articles (id INT PRIMARY KEY, title TEXT)").ok
    check execSql(db, "CREATE INDEX art_title ON articles USING TRIGRAM (title)").ok
    check execSql(db, "INSERT INTO articles VALUES (1, 'The Quick Brown Fox'), (2, 'Lazy Dog Story'), (3, 'Quick Silver')").ok
    let r = execSql(db, "SELECT id FROM articles WHERE title LIKE '%Quick%' ORDER BY id")
    check r.ok
    check r.value == @["1", "3"]
    discard closeDb(db)

  test "trigram search no match":
    let db = freshDb("trig_nomatch")
    check execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, text TEXT)").ok
    check execSql(db, "CREATE INDEX docs_text ON docs USING TRIGRAM (text)").ok
    check execSql(db, "INSERT INTO docs VALUES (1, 'apple banana'), (2, 'cherry date')").ok
    let r = execSql(db, "SELECT id FROM docs WHERE text LIKE '%xyz%'")
    check r.ok
    check r.value.len == 0
    discard closeDb(db)

  test "trigram with short pattern uses fallback scan":
    let db = freshDb("trig_short")
    check execSql(db, "CREATE TABLE docs (id INT PRIMARY KEY, body TEXT)").ok
    check execSql(db, "CREATE INDEX docs_body ON docs USING TRIGRAM (body)").ok
    check execSql(db, "INSERT INTO docs VALUES (1, 'ab test'), (2, 'cd other')").ok
    # Short pattern (< 3 chars) should fall back to table scan
    let r = execSql(db, "SELECT id FROM docs WHERE body LIKE '%ab%'")
    check r.ok
    check r.value.len == 1
    discard closeDb(db)

  test "trigram case insensitive search":
    let db = freshDb("trig_ci")
    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE INDEX items_name ON items USING TRIGRAM (name)").ok
    check execSql(db, "INSERT INTO items VALUES (1, 'Foo Bar'), (2, 'baz qux')").ok
    let r = execSql(db, "SELECT id FROM items WHERE name ILIKE '%foo%'")
    check r.ok
    check r.value.len == 1
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "INSERT...SELECT execution path":
# ─────────────────────────────────────────────────────────────────────────────
  test "INSERT...SELECT basic copy":
    let db = freshDb("ins_sel_basic")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 'a'), (2, 'b'), (3, 'c')").ok
    let r = execSql(db, "INSERT INTO dst SELECT id, v FROM src")
    check r.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM dst")
    check cnt.ok
    check col0(cnt.value) == "3"
    discard closeDb(db)

  test "INSERT...SELECT with DEFAULT column":
    let db = freshDb("ins_sel_default")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, v TEXT, ts TEXT DEFAULT 'created')").ok
    check execSql(db, "INSERT INTO src VALUES (1, 'hello')").ok
    let r = execSql(db, "INSERT INTO dst (id, v) SELECT id, v FROM src")
    check r.ok
    let sel = execSql(db, "SELECT ts FROM dst WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "created"
    discard closeDb(db)

  test "INSERT...SELECT with generated column":
    let db = freshDb("ins_sel_gen")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, a INT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, a INT, b INT GENERATED ALWAYS AS (a * 2) STORED)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 5), (2, 10)").ok
    let r = execSql(db, "INSERT INTO dst (id, a) SELECT id, a FROM src")
    check r.ok
    let sel = execSql(db, "SELECT b FROM dst WHERE id = 1")
    check sel.ok
    check col0(sel.value) == "10"
    discard closeDb(db)

  test "INSERT...SELECT with WHERE filter":
    let db = freshDb("ins_sel_where")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)").ok
    let r = execSql(db, "INSERT INTO dst SELECT id, v FROM src WHERE v > 15")
    check r.ok
    let cnt = execSql(db, "SELECT COUNT(*) FROM dst")
    check cnt.ok
    check col0(cnt.value) == "2"
    discard closeDb(db)

  test "INSERT...SELECT with column reordering":
    let db = freshDb("ins_sel_reorder")
    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, name TEXT, score INT)").ok
    check execSql(db, "CREATE TABLE dst (id INT PRIMARY KEY, score INT, name TEXT)").ok
    check execSql(db, "INSERT INTO src VALUES (1, 'Alice', 95)").ok
    let r = execSql(db, "INSERT INTO dst (id, name, score) SELECT id, name, score FROM src")
    check r.ok
    let sel = execSql(db, "SELECT name, score FROM dst WHERE id = 1")
    check sel.ok
    check sel.value == @["Alice|95"]
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "IS NULL / IS NOT NULL operators":
# ─────────────────────────────────────────────────────────────────────────────
  test "IS NULL finds null rows":
    let db = freshDb("is_null_basic")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, NULL)").ok
    let r = execSql(db, "SELECT id FROM t WHERE v IS NULL ORDER BY id")
    check r.ok
    check r.value == @["2", "3"]
    discard closeDb(db)

  test "IS NOT NULL finds non-null rows":
    let db = freshDb("is_not_null")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a'), (2, NULL), (3, 'b')").ok
    let r = execSql(db, "SELECT id FROM t WHERE v IS NOT NULL ORDER BY id")
    check r.ok
    check r.value == @["1", "3"]
    discard closeDb(db)

  test "IS NULL in SELECT item":
    let db = freshDb("is_null_select")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, NULL), (2, 5)").ok
    let r = execSql(db, "SELECT v IS NULL FROM t ORDER BY id")
    check r.ok
    check r.value == @["true", "false"]
    discard closeDb(db)

# ─────────────────────────────────────────────────────────────────────────────
suite "compareValues edge cases":
# ─────────────────────────────────────────────────────────────────────────────
  test "ORDER BY mixed NULL and INT":
    let db = freshDb("cmp_null_int")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, NULL), (2, 5), (3, -3), (4, NULL)").ok
    let r = execSql(db, "SELECT id FROM t ORDER BY v")
    check r.ok
    check r.value.len == 4
    discard closeDb(db)

  test "MIN MAX with various types":
    let db = freshDb("cmp_minmax")
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, a REAL, b TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 1.5, 'b'), (2, 0.5, 'a'), (3, 2.5, 'c')").ok
    let r = execSql(db, "SELECT MIN(a), MAX(b) FROM t")
    check r.ok
    check r.value == @["0.5|c"]
    discard closeDb(db)
