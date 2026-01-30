import unittest
import os

import engine
import catalog/catalog
import record/record
import sql/binder
import sql/sql
import errors

proc makeTempDb(name: string): string =
  let path = getTempDir() / name
  if fileExists(path):
    removeFile(path)
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

suite "Binder Error Paths":
  test "bind statement with non-existent table":
    let path = makeTempDb("decentdb_binder_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    let parseRes = parseSql("SELECT * FROM nonexistent")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind statement with non-existent column":
    let path = makeTempDb("decentdb_binder_bad_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT missing FROM items")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind INSERT with column count mismatch":
    let path = makeTempDb("decentdb_binder_insert_mismatch.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("INSERT INTO items VALUES (1, 'a', 'extra')")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind UPDATE with non-existent column":
    let path = makeTempDb("decentdb_binder_update_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("UPDATE items SET missing = 1 WHERE id = 1")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind DELETE with invalid WHERE clause":
    let path = makeTempDb("decentdb_binder_delete_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("DELETE FROM items WHERE missing = 1")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind SELECT with invalid column in WHERE":
    let path = makeTempDb("decentdb_binder_where_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items WHERE missing = 1")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind JOIN with non-existent table":
    let path = makeTempDb("decentdb_binder_join_bad_table.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items JOIN missing ON (items.id = missing.id)")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind JOIN with non-existent join column":
    let path = makeTempDb("decentdb_binder_join_bad_col.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    discard execSql(db, "CREATE TABLE other (id INT64, data TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items JOIN other ON (items.missing = other.id)")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind ORDER BY with non-existent column":
    let path = makeTempDb("decentdb_binder_orderby_missing.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT * FROM items ORDER BY missing")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

  test "bind aggregate with invalid column":
    let path = makeTempDb("decentdb_binder_agg_invalid.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    
    discard execSql(db, "CREATE TABLE items (id INT64, value TEXT)")
    
    let parseRes = parseSql("SELECT SUM(missing) FROM items")
    check parseRes.ok
    let bindRes = bindStatement(db.catalog, parseRes.value.statements[0])
    check not bindRes.ok
    check bindRes.err.code == ERR_SQL
    
    discard closeDb(db)

suite "Parser Error Paths":
  test "parse invalid SQL syntax":
    let res = parseSql("SELEC * FROM items")
    check not res.ok
    check res.err.code == ERR_SQL

  test "parse incomplete statement":
    let res = parseSql("CREATE TABLE items (id")
    check not res.ok
    check res.err.code == ERR_SQL

  test "parse with unmatched parentheses":
    let res = parseSql("SELECT * FROM items WHERE (id = 1")
    check not res.ok
    check res.err.code == ERR_SQL

  test "parse with invalid type":
    let res = parseSql("CREATE TABLE items (id BADTYPE)")
    # Parser may or may not catch invalid type
    # This tests the parsing path even if it succeeds
    discard res

  test "parse CREATE TABLE with duplicate column":
    let res = parseSql("CREATE TABLE items (id INT64, id INT64)")
    # May or may not be caught by parser
    # Just verify it doesn't crash

  test "parse invalid INSERT statement":
    let res = parseSql("INSERT INTO")
    check not res.ok
    check res.err.code == ERR_SQL

  test "parse invalid WHERE clause":
    let res = parseSql("SELECT * FROM items WHERE id IN")
    check not res.ok
    check res.err.code == ERR_SQL

  test "parse invalid LIKE pattern":
    let res = parseSql("SELECT * FROM items WHERE value LIKE 'abc%' ESCAPE")
    check not res.ok
    check res.err.code == ERR_SQL

  test "parse with invalid JOIN syntax":
    let res = parseSql("SELECT * FROM items JOIN ON id = 1")
    check not res.ok
    check res.err.code == ERR_SQL
