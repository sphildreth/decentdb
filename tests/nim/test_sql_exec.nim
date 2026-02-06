import unittest
import os
import strutils
import engine
import record/record
import errors
import tables
import sets
import sql/sql
import planner/planner
import catalog/catalog
import pager/pager

proc makeTempDb(name: string): string =
  let normalizedName =
    if name.len >= 3 and name[name.len - 3 .. ^1] == ".db":
      name[0 .. ^4] & ".ddb"
    else:
      name
  let path = getTempDir() / normalizedName
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  if fileExists(path):
    removeFile(path)
  path

proc splitRow(row: string): seq[string] =
  if row.len == 0:
    return @[]
  row.split("|")

suite "SQL Exec":
  test "basic DDL/DML and params":
    let path = makeTempDb("decentdb_sql_exec.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE users (id INT, name TEXT, active BOOL, score FLOAT)").ok
    check execSql(db, "CREATE TABLE posts (user_id INT, title TEXT)").ok
    check execSql(db, "CREATE INDEX users_id_idx ON users (id)").ok
    discard execSql(db, "INSERT INTO users (id, name, active, score) VALUES ($1, $2, $3, $4)", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: @['A'.byte]),
      Value(kind: vkBool, boolVal: true),
      Value(kind: vkFloat64, float64Val: 1.5)
    ])
    discard execSql(db, "INSERT INTO users (id, name, active, score) VALUES ($1, $2, $3, $4)", @[
      Value(kind: vkInt64, int64Val: 2),
      Value(kind: vkText, bytes: @['B'.byte]),
      Value(kind: vkBool, boolVal: false),
      Value(kind: vkFloat64, float64Val: 3.0)
    ])
    discard execSql(db, "INSERT INTO posts (user_id, title) VALUES ($1, $2)", @[
      Value(kind: vkInt64, int64Val: 1),
      Value(kind: vkText, bytes: @['P'.byte])
    ])
    let selectRes = execSql(db, "SELECT name FROM users WHERE id = 1")
    check selectRes.ok
    check splitRow(selectRes.value[0])[0] == "A"
    let joinRes = execSql(db, "SELECT u.name, p.title FROM users u LEFT JOIN posts p ON (u.id = p.user_id) ORDER BY u.id")
    check joinRes.ok
    check joinRes.value.len == 2
    let aggRes = execSql(db, "SELECT COUNT(*), SUM(score) FROM users")
    check aggRes.ok
    let aggRow = splitRow(aggRes.value[0])
    check aggRow[0] == "2"
    check aggRow[1] == "4.5"
    check execSql(db, "UPDATE users SET score = 2.5 WHERE id = 1").ok
    let updRes = execSql(db, "SELECT score FROM users WHERE id = 1")
    check updRes.ok
    check splitRow(updRes.value[0])[0] == "2.5"
    check execSql(db, "DELETE FROM users WHERE id = 2").ok
    let countRes = execSql(db, "SELECT COUNT(*) FROM users")
    check countRes.ok
    check splitRow(countRes.value[0])[0] == "1"
    discard closeDb(db)

  test "execSqlNoRows executes SELECT without materializing rows":
    let path = makeTempDb("decentdb_sql_exec_norows.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'A')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'B')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'A')").ok

    let res = execSqlNoRows(db, "SELECT * FROM t WHERE name = 'A'", @[])
    check res.ok
    check res.value == 2

    let resLike = execSqlNoRows(db, "SELECT * FROM t WHERE name LIKE '%A%'", @[])
    check resLike.ok
    check resLike.value == 2

    discard closeDb(db)

  test "execSql returns rows for simple LIKE":
    let path = makeTempDb("decentdb_sql_exec_like_rows.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'Hello Metallica')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'World')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'Bye Metallica')").ok

    let res = execSql(db, "SELECT * FROM t WHERE name LIKE '%Metallica'")
    check res.ok
    check res.value.len == 2
    check res.value.contains("1|Hello Metallica")
    check res.value.contains("3|Bye Metallica")

    discard closeDb(db)

  test "view DDL and read-only behavior":
    let path = makeTempDb("decentdb_sql_exec_view_basic.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'b')").ok
    check execSql(db, "CREATE VIEW v AS SELECT id, name FROM t").ok

    let rows = execSql(db, "SELECT name FROM v WHERE id = 2")
    check rows.ok
    check rows.value.len == 1
    check rows.value[0] == "b"

    let ins = execSql(db, "INSERT INTO v (id, name) VALUES (3, 'c')")
    check not ins.ok
    check ins.err.code == ERR_SQL

    let upd = execSql(db, "UPDATE v SET name = 'x' WHERE id = 1")
    check not upd.ok
    check upd.err.code == ERR_SQL

    let del = execSql(db, "DELETE FROM v WHERE id = 1")
    check not del.ok
    check del.err.code == ERR_SQL

    check execSql(db, "DROP VIEW v").ok
    discard closeDb(db)

  test "view dependency restrictions and replace revalidation":
    let path = makeTempDb("decentdb_sql_exec_view_dependencies.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a')").ok
    check execSql(db, "CREATE VIEW v1 AS SELECT id FROM t").ok
    check execSql(db, "CREATE VIEW v2 AS SELECT id FROM v1").ok

    let dropTable = execSql(db, "DROP TABLE t")
    check not dropTable.ok
    check dropTable.err.code == ERR_SQL

    let dropView = execSql(db, "DROP VIEW v1")
    check not dropView.ok
    check dropView.err.code == ERR_SQL

    let rename = execSql(db, "ALTER VIEW v1 RENAME TO v1_new")
    check not rename.ok
    check rename.err.code == ERR_SQL

    let replaceInvalid = execSql(db, "CREATE OR REPLACE VIEW v1 AS SELECT name FROM t")
    check not replaceInvalid.ok
    check replaceInvalid.err.code == ERR_SQL

    let rows = execSql(db, "SELECT id FROM v1")
    check rows.ok
    check rows.value == @["1"]

    discard closeDb(db)

  test "prepared statements re-prepare after view schema change":
    let path = makeTempDb("decentdb_sql_exec_view_prepared.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a')").ok
    check execSql(db, "CREATE VIEW v AS SELECT id FROM t").ok

    let prepRes = prepare(db, "SELECT id FROM v")
    check prepRes.ok
    let prepared = prepRes.value

    let firstExec = execPrepared(prepared, @[])
    check firstExec.ok
    check firstExec.value.len == 1
    check "int64Val: 1" in firstExec.value[0]

    check execSql(db, "CREATE OR REPLACE VIEW v AS SELECT name FROM t").ok
    let staleExec = execPrepared(prepared, @[])
    check not staleExec.ok
    check staleExec.err.code == ERR_SQL

    discard closeDb(db)

proc makeCatalog(): Catalog =
  Catalog(
    tables: initTable[string, TableMeta](),
    indexes: initTable[string, IndexMeta](),
    views: initTable[string, ViewMeta](),
    dependentViews: initTable[string, HashSet[string]](),
    catalogTree: nil,
    trigramDeltas: initTable[(string, uint32), TrigramDelta]()
  )

proc addTable(catalog: Catalog, name: string) =
  catalog.tables[name] = TableMeta(
    name: name,
    rootPage: PageId(1),
    nextRowId: 1,
    columns: @[]
  )

proc addBtreeIndex(catalog: Catalog, name: string, table: string, column: string) =
  catalog.indexes[name] = IndexMeta(
    name: name,
    table: table,
    columns: @[column],
    rootPage: PageId(1),
    kind: ikBtree,
    unique: false
  )

proc addTrigramIndex(catalog: Catalog, name: string, table: string, column: string) =
  catalog.indexes[name] = IndexMeta(
    name: name,
    table: table,
    columns: @[column],
    rootPage: PageId(1),
    kind: ikTrigram,
    unique: false
  )

proc parseSingle(sqlText: string): Statement =
  let astRes = parseSql(sqlText)
  check astRes.ok
  check astRes.value.statements.len == 1
  astRes.value.statements[0]

suite "Planner":
  test "uses index seek for equality predicate":
    var catalog = makeCatalog()
    addTable(catalog, "users")
    addBtreeIndex(catalog, "users_id_idx", "users", "id")
    let stmt = parseSingle("SELECT id FROM users WHERE id = 10")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let p = planRes.value
    check p.kind == pkProject
    check p.left.kind == pkIndexSeek
    check p.left.table == "users"
    check p.left.column == "id"
    check p.left.valueExpr != nil

  test "uses trigram seek for LIKE predicate":
    var catalog = makeCatalog()
    addTable(catalog, "docs")
    addTrigramIndex(catalog, "docs_body_trgm", "docs", "body")
    let stmt = parseSingle("SELECT id FROM docs WHERE body ILIKE '%abc%'")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let p = planRes.value
    check p.kind == pkProject
    check p.left.kind == pkTrigramSeek
    check p.left.table == "docs"
    check p.left.column == "body"
    check p.left.likeExpr != nil
    check p.left.likeInsensitive

  test "adds filter when no usable index":
    var catalog = makeCatalog()
    addTable(catalog, "items")
    let stmt = parseSingle("SELECT id FROM items WHERE id = 10")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let p = planRes.value
    check p.kind == pkProject
    check p.left.kind == pkFilter
    check p.left.predicate != nil
    check p.left.left.kind == pkTableScan

  test "join uses index seek on right when available":
    var catalog = makeCatalog()
    addTable(catalog, "users")
    addTable(catalog, "orders")
    addBtreeIndex(catalog, "orders_user_idx", "orders", "user_id")
    let stmt = parseSingle("SELECT users.id, orders.id FROM users INNER JOIN orders ON orders.user_id = users.id")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let p = planRes.value
    check p.kind == pkProject
    check p.left.kind == pkJoin
    check p.left.joinType == jtInner
    check p.left.right.kind == pkIndexSeek
    check p.left.right.table == "orders"
    check p.left.right.column == "user_id"

  test "aggregate plan used for GROUP BY and HAVING":
    var catalog = makeCatalog()
    addTable(catalog, "orders")
    let stmt = parseSingle("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 1")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let p = planRes.value
    check p.kind == pkAggregate
    check p.groupBy.len == 1
    check p.having != nil
    check p.left.kind == pkTableScan

  test "sort and limit wrap projection":
    var catalog = makeCatalog()
    addTable(catalog, "users")
    let stmt = parseSingle("SELECT id FROM users ORDER BY id DESC LIMIT 5 OFFSET 2")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let p = planRes.value
    check p.kind == pkLimit
    check p.limit == 5
    check p.offset == 2
    check p.left.kind == pkSort
    check p.left.left.kind == pkProject
    check p.left.left.left.kind == pkTableScan

  test "non-select statements return statement plan":
    var catalog = makeCatalog()
    addTable(catalog, "users")
    let stmt = parseSingle("INSERT INTO users (id) VALUES (1)")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    check planRes.value.kind == pkStatement

  test "statement rollback on bind error":
    let path = makeTempDb("decentdb_sql_rollback.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "INSERT INTO t (id, name) VALUES (1, 'X')").ok
    let badUpdate = execSql(db, "UPDATE t SET missing = 1 WHERE id = 1")
    check badUpdate.ok == false
    let rows = execSql(db, "SELECT name FROM t WHERE id = 1")
    check rows.ok
    check splitRow(rows.value[0])[0] == "X"
    discard closeDb(db)

  test "where clause complex logic":
    let path = makeTempDb("decentdb_sql_where.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE items (id INT, val INT)").ok
    check execSql(db, "INSERT INTO items (id, val) VALUES (1, 10)").ok
    check execSql(db, "INSERT INTO items (id, val) VALUES (2, 20)").ok
    check execSql(db, "INSERT INTO items (id, val) VALUES (3, 30)").ok
    
    # OR
    let res1 = execSql(db, "SELECT id FROM items WHERE id = 1 OR id = 3")
    check res1.ok
    check res1.value.len == 2
    
    # AND
    let res2 = execSql(db, "SELECT id FROM items WHERE val > 15 AND val < 25")
    check res2.ok
    check res2.value.len == 1
    check splitRow(res2.value[0])[0] == "2"
    
    # NULL logic
    check execSql(db, "INSERT INTO items (id, val) VALUES (4, NULL)").ok
    let res3 = execSql(db, "SELECT id FROM items WHERE val IS NULL")
    check res3.ok
    check res3.value.len == 1
    check splitRow(res3.value[0])[0] == "4"
    
    discard closeDb(db)
  
  test "type mismatch handling":
    let path = makeTempDb("decentdb_sql_types.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    check execSql(db, "CREATE TABLE t (id INT, flag BOOL)").ok
    
    # Insert wrong type (text for int)
    let badInsert = execSql(db, "INSERT INTO t (id, flag) VALUES ('bad', true)")
    check not badInsert.ok
    check badInsert.err.code == ERR_SQL
    
    let validInsert = execSql(db, "INSERT INTO t (id, flag) VALUES (1, true)")
    if not validInsert.ok:
      echo "Valid insert failed: ", validInsert.err.message
    check validInsert.ok
    
    # Update wrong type
    let badUpdate = execSql(db, "UPDATE t SET flag = 1 WHERE id = 1") # 1 is int, not bool in strict mode?
    check not badUpdate.ok
    check badUpdate.err.code == ERR_SQL
    
    discard closeDb(db)
