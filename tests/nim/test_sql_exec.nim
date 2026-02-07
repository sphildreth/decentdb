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

  test "insert ON CONFLICT DO NOTHING":
    let path = makeTempDb("decentdb_sql_exec_on_conflict.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT, name TEXT NOT NULL)").ok
    check execSql(db, "CREATE UNIQUE INDEX users_email_uq_idx ON users (email)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'a@x', 'alice')").ok

    let anyConflict = execSql(db, "INSERT INTO users VALUES (1, 'b@x', 'dup-id') ON CONFLICT DO NOTHING")
    check anyConflict.ok
    let countAfterAny = execSql(db, "SELECT COUNT(*) FROM users")
    check countAfterAny.ok
    check splitRow(countAfterAny.value[0])[0] == "1"

    let targetConflict = execSql(db, "INSERT INTO users VALUES (2, 'a@x', 'dup-email') ON CONFLICT (email) DO NOTHING")
    check targetConflict.ok
    let countAfterTarget = execSql(db, "SELECT COUNT(*) FROM users")
    check countAfterTarget.ok
    check splitRow(countAfterTarget.value[0])[0] == "1"

    let onConstraint = execSql(db, "INSERT INTO users VALUES (3, 'a@x', 'dup-email-2') ON CONFLICT ON CONSTRAINT users_email_uq_idx DO NOTHING")
    check onConstraint.ok
    let countAfterConstraint = execSql(db, "SELECT COUNT(*) FROM users")
    check countAfterConstraint.ok
    check splitRow(countAfterConstraint.value[0])[0] == "1"

    let mismatch = execSql(db, "INSERT INTO users VALUES (1, 'c@x', 'dup-id-mismatch') ON CONFLICT (email) DO NOTHING")
    check not mismatch.ok
    check mismatch.err.code == ERR_CONSTRAINT

    let notNullStillErrors = execSql(db, "INSERT INTO users VALUES (4, 'd@x', NULL) ON CONFLICT DO NOTHING")
    check not notNullStillErrors.ok
    check notNullStillErrors.err.code == ERR_CONSTRAINT

    discard closeDb(db)

  test "insert ON CONFLICT DO UPDATE":
    let path = makeTempDb("decentdb_sql_exec_on_conflict_update.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, visits INT NOT NULL)").ok
    check execSql(db, "CREATE UNIQUE INDEX users_email_uq_idx ON users (email)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'a@x', 1)").ok

    let updateOnId = execSql(
      db,
      "INSERT INTO users VALUES (1, 'b@x', 5) " &
      "ON CONFLICT (id) DO UPDATE SET email = EXCLUDED.email, visits = users.visits + EXCLUDED.visits"
    )
    check updateOnId.ok
    let afterId = execSql(db, "SELECT id, email, visits FROM users ORDER BY id")
    check afterId.ok
    check afterId.value == @["1|b@x|6"]

    let whereSkip = execSql(
      db,
      "INSERT INTO users VALUES (1, 'c@x', 9) " &
      "ON CONFLICT (id) DO UPDATE SET visits = EXCLUDED.visits WHERE users.email = 'nope'"
    )
    check whereSkip.ok
    let afterSkip = execSql(db, "SELECT id, email, visits FROM users ORDER BY id")
    check afterSkip.ok
    check afterSkip.value == @["1|b@x|6"]

    let onConstraint = execSql(
      db,
      "INSERT INTO users VALUES (2, 'b@x', 3) " &
      "ON CONFLICT ON CONSTRAINT users_email_uq_idx DO UPDATE SET visits = users.visits + 1"
    )
    check onConstraint.ok
    let afterConstraint = execSql(db, "SELECT id, email, visits FROM users ORDER BY id")
    check afterConstraint.ok
    check afterConstraint.value == @["1|b@x|7"]

    let nonTargetConflict = execSql(
      db,
      "INSERT INTO users VALUES (3, 'b@x', 1) " &
      "ON CONFLICT (id) DO UPDATE SET visits = EXCLUDED.visits"
    )
    check not nonTargetConflict.ok
    check nonTargetConflict.err.code == ERR_CONSTRAINT

    let targetlessDoUpdate = execSql(
      db,
      "INSERT INTO users VALUES (1, 'z@x', 1) ON CONFLICT DO UPDATE SET visits = EXCLUDED.visits"
    )
    check not targetlessDoUpdate.ok
    check targetlessDoUpdate.err.code == ERR_SQL

    discard closeDb(db)

  test "insert RETURNING":
    let path = makeTempDb("decentdb_sql_exec_insert_returning.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, email TEXT UNIQUE, visits INT NOT NULL)").ok

    let insReturning = execSql(db, "INSERT INTO users VALUES (1, 'a@x', 1) RETURNING id, email")
    check insReturning.ok
    check insReturning.value == @["1|a@x"]

    let insReturningStar = execSql(db, "INSERT INTO users VALUES (2, 'b@x', 3) RETURNING *")
    check insReturningStar.ok
    check insReturningStar.value == @["2|b@x|3"]

    let doNothingNoRow = execSql(
      db,
      "INSERT INTO users VALUES (1, 'dup@x', 9) ON CONFLICT DO NOTHING RETURNING id"
    )
    check doNothingNoRow.ok
    check doNothingNoRow.value.len == 0

    let doUpdateReturning = execSql(
      db,
      "INSERT INTO users VALUES (1, 'a@x', 4) " &
      "ON CONFLICT (id) DO UPDATE SET visits = users.visits + EXCLUDED.visits RETURNING visits"
    )
    check doUpdateReturning.ok
    check doUpdateReturning.value == @["5"]

    let doUpdateWhereSkip = execSql(
      db,
      "INSERT INTO users VALUES (1, 'a@x', 10) " &
      "ON CONFLICT (id) DO UPDATE SET visits = EXCLUDED.visits WHERE users.id = 999 RETURNING id"
    )
    check doUpdateWhereSkip.ok
    check doUpdateWhereSkip.value.len == 0

    discard closeDb(db)

  test "non-recursive CTE execution":
    let path = makeTempDb("decentdb_sql_exec_cte.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'a')").ok
    check execSql(db, "INSERT INTO users VALUES (2, 'b')").ok
    check execSql(db, "INSERT INTO users VALUES (3, 'c')").ok
    check execSql(db, "INSERT INTO t VALUES (99)").ok

    let basic = execSql(
      db,
      "WITH base AS (SELECT id, name FROM users WHERE id <= 2) " &
      "SELECT id, name FROM base ORDER BY id"
    )
    check basic.ok
    check basic.value == @[("1|a"), ("2|b")]

    let chain = execSql(
      db,
      "WITH a AS (SELECT id FROM users), b(x) AS (SELECT id FROM a WHERE id > 1) " &
      "SELECT x FROM b ORDER BY x"
    )
    check chain.ok
    check chain.value == @[("2"), ("3")]

    let shadow = execSql(db, "WITH t AS (SELECT id FROM users WHERE id = 1) SELECT id FROM t")
    check shadow.ok
    check shadow.value == @[("1")]

    let unsupportedShape = execSql(db, "WITH a AS (SELECT id FROM users ORDER BY id) SELECT id FROM a")
    check not unsupportedShape.ok

    discard closeDb(db)

  test "set operation execution":
    let path = makeTempDb("decentdb_sql_exec_union_all.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE a (id INT)").ok
    check execSql(db, "CREATE TABLE b (id INT)").ok
    check execSql(db, "INSERT INTO a VALUES (1)").ok
    check execSql(db, "INSERT INTO a VALUES (2)").ok
    check execSql(db, "INSERT INTO b VALUES (2)").ok
    check execSql(db, "INSERT INTO b VALUES (3)").ok

    let unionAll = execSql(db, "SELECT id FROM a UNION ALL SELECT id FROM b")
    check unionAll.ok
    check unionAll.value == @["1", "2", "2", "3"]

    let unionDistinct = execSql(db, "SELECT id FROM a UNION SELECT id FROM b")
    check unionDistinct.ok
    check unionDistinct.value == @["1", "2", "3"]

    let intersectRes = execSql(db, "SELECT id FROM a INTERSECT SELECT id FROM b")
    check intersectRes.ok
    check intersectRes.value == @["2"]

    let exceptRes = execSql(db, "SELECT id FROM a EXCEPT SELECT id FROM b")
    check exceptRes.ok
    check exceptRes.value == @["1"]

    discard closeDb(db)

  test "CHECK constraints enforce false-only failure and persist":
    let path = makeTempDb("decentdb_sql_exec_check_constraints.db")
    let dbRes = openDb(path)
    check dbRes.ok
    var db = dbRes.value

    check execSql(
      db,
      "CREATE TABLE accounts (" &
      "id INT PRIMARY KEY, " &
      "amount INT, " &
      "note TEXT, " &
      "CONSTRAINT amount_nonneg CHECK (amount >= 0), " &
      "CHECK (note IS NULL OR LENGTH(note) > 0))"
    ).ok

    check execSql(db, "INSERT INTO accounts VALUES (1, 10, 'ok')").ok
    check execSql(db, "INSERT INTO accounts VALUES (2, NULL, NULL)").ok

    let badInsert = execSql(db, "INSERT INTO accounts VALUES (3, -1, 'bad')")
    check not badInsert.ok
    check badInsert.err.code == ERR_CONSTRAINT

    let badUpdate = execSql(db, "UPDATE accounts SET amount = -5 WHERE id = 1")
    check not badUpdate.ok
    check badUpdate.err.code == ERR_CONSTRAINT

    let rowsBeforeClose = execSql(db, "SELECT id, amount, note FROM accounts ORDER BY id")
    check rowsBeforeClose.ok
    check rowsBeforeClose.value == @["1|10|ok", "2|NULL|NULL"]

    discard closeDb(db)

    let reopenRes = openDb(path)
    check reopenRes.ok
    db = reopenRes.value

    let badAfterReopen = execSql(db, "INSERT INTO accounts VALUES (4, -9, 'bad')")
    check not badAfterReopen.ok
    check badAfterReopen.err.code == ERR_CONSTRAINT

    let alterBlocked = execSql(db, "ALTER TABLE accounts ADD COLUMN extra INT")
    check not alterBlocked.ok
    check alterBlocked.err.code == ERR_SQL

    let rowsAfterReopen = execSql(db, "SELECT id, amount, note FROM accounts ORDER BY id")
    check rowsAfterReopen.ok
    check rowsAfterReopen.value == @["1|10|ok", "2|NULL|NULL"]

    discard closeDb(db)

  test "foreign key ON DELETE CASCADE and SET NULL":
    let path = makeTempDb("decentdb_sql_exec_fk_actions.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE parent (id INT PRIMARY KEY, code TEXT UNIQUE)").ok
    check execSql(db, "CREATE UNIQUE INDEX parent_id_uq ON parent (id)").ok
    check execSql(db, "CREATE TABLE child_cascade (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)").ok
    check execSql(db, "CREATE TABLE child_setnull (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id) ON DELETE SET NULL)").ok
    check execSql(db, "CREATE TABLE child_upd_cascade (id INT PRIMARY KEY, parent_code TEXT REFERENCES parent(code) ON UPDATE CASCADE)").ok
    check execSql(db, "CREATE TABLE child_upd_setnull (id INT PRIMARY KEY, parent_code TEXT REFERENCES parent(code) ON UPDATE SET NULL)").ok

    check execSql(db, "INSERT INTO parent VALUES (1, 'a')").ok
    check execSql(db, "INSERT INTO parent VALUES (2, 'b')").ok
    check execSql(db, "INSERT INTO child_cascade VALUES (10, 1)").ok
    check execSql(db, "INSERT INTO child_setnull VALUES (20, 1)").ok
    check execSql(db, "INSERT INTO child_cascade VALUES (11, 2)").ok
    check execSql(db, "INSERT INTO child_setnull VALUES (21, 2)").ok
    check execSql(db, "INSERT INTO child_upd_cascade VALUES (30, 'b')").ok
    check execSql(db, "INSERT INTO child_upd_setnull VALUES (40, 'b')").ok

    check execSql(db, "DELETE FROM parent WHERE id = 1").ok

    let cascadeRows = execSql(db, "SELECT id, parent_id FROM child_cascade ORDER BY id")
    check cascadeRows.ok
    check cascadeRows.value == @["11|2"]

    let setNullRows = execSql(db, "SELECT id, parent_id FROM child_setnull ORDER BY id")
    check setNullRows.ok
    check setNullRows.value == @["20|NULL", "21|2"]

    check execSql(db, "UPDATE parent SET code = 'b2' WHERE id = 2").ok
    let updCascadeRows = execSql(db, "SELECT id, parent_code FROM child_upd_cascade ORDER BY id")
    check updCascadeRows.ok
    check updCascadeRows.value == @["30|b2"]
    let updSetNullRows = execSql(db, "SELECT id, parent_code FROM child_upd_setnull ORDER BY id")
    check updSetNullRows.ok
    check updSetNullRows.value == @["40|NULL"]

    let setNullNotNull = execSql(
      db,
      "CREATE TABLE child_setnull_bad (id INT PRIMARY KEY, parent_id INT NOT NULL REFERENCES parent(id) ON DELETE SET NULL)"
    )
    check not setNullNotNull.ok
    check setNullNotNull.err.code == ERR_SQL

    let onUpdateSetNullNotNull = execSql(
      db,
      "CREATE TABLE child_upd_bad (id INT PRIMARY KEY, parent_code TEXT NOT NULL REFERENCES parent(code) ON UPDATE SET NULL)"
    )
    check not onUpdateSetNullNotNull.ok
    check onUpdateSetNullNotNull.err.code == ERR_SQL

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

  test "UNION ALL plans as append":
    var catalog = makeCatalog()
    addTable(catalog, "a")
    addTable(catalog, "b")
    let stmt = parseSingle("SELECT id FROM a UNION ALL SELECT id FROM b")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    check planRes.value.kind == pkAppend

  test "UNION plans as distinct set-union":
    var catalog = makeCatalog()
    addTable(catalog, "a")
    addTable(catalog, "b")
    let stmt = parseSingle("SELECT id FROM a UNION SELECT id FROM b")
    let planRes = plan(catalog, stmt)
    check planRes.ok
    check planRes.value.kind == pkSetUnionDistinct

  test "INTERSECT and EXCEPT plan kinds":
    var catalog = makeCatalog()
    addTable(catalog, "a")
    addTable(catalog, "b")
    let intersectStmt = parseSingle("SELECT id FROM a INTERSECT SELECT id FROM b")
    let intersectPlan = plan(catalog, intersectStmt)
    check intersectPlan.ok
    check intersectPlan.value.kind == pkSetIntersect

    let exceptStmt = parseSingle("SELECT id FROM a EXCEPT SELECT id FROM b")
    let exceptPlan = plan(catalog, exceptStmt)
    check exceptPlan.ok
    check exceptPlan.value.kind == pkSetExcept

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

  test "scalar functions and concatenation":
    let path = makeTempDb("decentdb_sql_scalar_funcs.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE items (id INT, val INT, name TEXT)").ok
    check execSql(db, "INSERT INTO items (id, val, name) VALUES (1, NULL, '  AbC  ')").ok
    check execSql(db, "INSERT INTO items (id, val, name) VALUES (2, 20, 'xy')").ok

    let coalesceRes = execSql(db, "SELECT COALESCE(val, 99) FROM items ORDER BY id")
    check coalesceRes.ok
    check coalesceRes.value == @["99", "20"]

    let nullifRes = execSql(db, "SELECT NULLIF(val, 20) FROM items ORDER BY id")
    check nullifRes.ok
    check nullifRes.value == @["NULL", "NULL"]

    let stringFnRes = execSql(db, "SELECT LENGTH(name), LOWER(name), UPPER(name), TRIM(name), TRIM(name) || '_x' FROM items WHERE id = 1")
    check stringFnRes.ok
    check stringFnRes.value == @["7|  abc  |  ABC  |AbC|AbC_x"]

    discard closeDb(db)

  test "CASE, CAST, BETWEEN, EXISTS, and LIKE ESCAPE":
    let path = makeTempDb("decentdb_sql_case_cast_exists.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT, name TEXT)").ok
    check execSql(db, "CREATE TABLE t2 (id INT)").ok
    check execSql(db, "INSERT INTO t (id, name) VALUES (1, 'a_%')").ok
    check execSql(db, "INSERT INTO t (id, name) VALUES (2, 'abc')").ok
    check execSql(db, "INSERT INTO t2 (id) VALUES (7)").ok

    let caseCastRes = execSql(db, "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END, CAST(id AS TEXT) FROM t ORDER BY id")
    check caseCastRes.ok
    check caseCastRes.value == @["small|1", "big|2"]

    let betweenRes = execSql(db, "SELECT id FROM t WHERE id BETWEEN 1 AND 1")
    check betweenRes.ok
    check betweenRes.value == @["1"]

    let existsRes = execSql(db, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t2)")
    check existsRes.ok
    check existsRes.value.len == 2

    let likeEscapeRes = execSql(db, "SELECT id FROM t WHERE name LIKE 'a#_%' ESCAPE '#'")
    check likeEscapeRes.ok
    check likeEscapeRes.value == @["1"]

    let corrExistsRes = execSql(db, "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)")
    check not corrExistsRes.ok

    discard closeDb(db)

  test "ROW_NUMBER window function subset":
    let path = makeTempDb("decentdb_sql_window_row_number.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT, grp TEXT)").ok
    check execSql(db, "INSERT INTO t VALUES (1, 'a')").ok
    check execSql(db, "INSERT INTO t VALUES (2, 'a')").ok
    check execSql(db, "INSERT INTO t VALUES (3, 'b')").ok
    check execSql(db, "INSERT INTO t VALUES (4, 'a')").ok

    let winRes = execSql(
      db,
      "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM t ORDER BY id"
    )
    check winRes.ok
    check winRes.value == @["1|1", "2|2", "3|1", "4|3"]

    let descRes = execSql(
      db,
      "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id DESC) AS rn FROM t WHERE grp = 'a' ORDER BY id"
    )
    check descRes.ok
    check descRes.value == @["1|3", "2|2", "4|1"]

    let badWindow = execSql(db, "SELECT ROW_NUMBER() OVER (PARTITION BY grp) FROM t")
    check not badWindow.ok

    discard closeDb(db)

  test "partial index (IS NOT NULL) maintenance and planning":
    let path = makeTempDb("decentdb_sql_partial_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE items (id INT PRIMARY KEY, val INT)").ok
    check execSql(db, "INSERT INTO items VALUES (1, NULL)").ok
    check execSql(db, "INSERT INTO items VALUES (2, 10)").ok
    check execSql(db, "INSERT INTO items VALUES (3, 10)").ok
    check execSql(db, "INSERT INTO items VALUES (4, 20)").ok
    check execSql(db, "CREATE INDEX items_val_partial ON items (val) WHERE val IS NOT NULL").ok

    let explainRes = execSql(db, "EXPLAIN SELECT id FROM items WHERE val = 10")
    check explainRes.ok
    var sawIndexSeek = false
    for line in explainRes.value:
      if line.contains("IndexSeek("):
        sawIndexSeek = true
        break
    check sawIndexSeek

    let q1 = execSql(db, "SELECT id FROM items WHERE val = 10 ORDER BY id")
    check q1.ok
    check q1.value == @["2", "3"]

    check execSql(db, "UPDATE items SET val = NULL WHERE id = 2").ok
    let q2 = execSql(db, "SELECT id FROM items WHERE val = 10 ORDER BY id")
    check q2.ok
    check q2.value == @["3"]

    check execSql(db, "UPDATE items SET val = 10 WHERE id = 1").ok
    let q3 = execSql(db, "SELECT id FROM items WHERE val = 10 ORDER BY id")
    check q3.ok
    check q3.value == @["1", "3"]

    check execSql(db, "DELETE FROM items WHERE id = 3").ok
    let q4 = execSql(db, "SELECT id FROM items WHERE val = 10 ORDER BY id")
    check q4.ok
    check q4.value == @["1"]

    discard closeDb(db)

  test "partial index unsupported shapes rejected":
    let path = makeTempDb("decentdb_sql_partial_index_reject.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, val INT, txt TEXT)").ok

    let badPredicate = execSql(db, "CREATE INDEX t_val_partial_bad ON t (val) WHERE val > 0")
    check not badPredicate.ok

    let badUnique = execSql(db, "CREATE UNIQUE INDEX t_val_partial_uq ON t (val) WHERE val IS NOT NULL")
    check not badUnique.ok

    let badMulti = execSql(db, "CREATE INDEX t_pair_partial ON t (id, val) WHERE id IS NOT NULL")
    check not badMulti.ok

    let badTrgm = execSql(db, "CREATE INDEX t_txt_partial_trgm ON t USING trigram (txt) WHERE txt IS NOT NULL")
    check not badTrgm.ok

    discard closeDb(db)

  test "expression index (LOWER(column)) maintenance and planning":
    let path = makeTempDb("decentdb_sql_expression_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").ok
    check execSql(db, "INSERT INTO users VALUES (1, 'Alice')").ok
    check execSql(db, "INSERT INTO users VALUES (2, 'ALICE')").ok
    check execSql(db, "INSERT INTO users VALUES (3, 'Bob')").ok

    check execSql(db, "CREATE INDEX users_name_lower_idx ON users ((LOWER(name)))").ok

    let explainRes = execSql(db, "EXPLAIN SELECT id FROM users WHERE LOWER(name) = 'alice'")
    check explainRes.ok
    check explainRes.value.len > 0
    var sawIndexSeek = false
    for line in explainRes.value:
      if "IndexSeek" in line:
        sawIndexSeek = true
        break
    check sawIndexSeek

    let q1 = execSql(db, "SELECT id FROM users WHERE LOWER(name) = 'alice' ORDER BY id")
    check q1.ok
    check q1.value == @["1", "2"]

    check execSql(db, "UPDATE users SET name = 'Charlie' WHERE id = 2").ok
    let q2 = execSql(db, "SELECT id FROM users WHERE LOWER(name) = 'alice' ORDER BY id")
    check q2.ok
    check q2.value == @["1"]

    check execSql(db, "DELETE FROM users WHERE id = 1").ok
    let q3 = execSql(db, "SELECT id FROM users WHERE LOWER(name) = 'alice' ORDER BY id")
    check q3.ok
    check q3.value.len == 0

    discard closeDb(db)

  test "expression index unsupported shapes rejected":
    let path = makeTempDb("decentdb_sql_expression_index_reject.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").ok

    let badExpr = execSql(db, "CREATE INDEX t_expr_bad ON t ((id + 1))")
    check not badExpr.ok

    let badUnique = execSql(db, "CREATE UNIQUE INDEX t_expr_uq ON t ((LOWER(name)))")
    check not badUnique.ok

    let badPartial = execSql(db, "CREATE INDEX t_expr_partial ON t ((LOWER(name))) WHERE name IS NOT NULL")
    check not badPartial.ok

    let badMixed = execSql(db, "CREATE INDEX t_expr_mixed ON t ((LOWER(name)), id)")
    check not badMixed.ok

    discard closeDb(db)

  test "INSTEAD OF view triggers fire per affected row":
    let path = makeTempDb("decentdb_sql_instead_triggers.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE base (id INT PRIMARY KEY, val INT)").ok
    check execSql(db, "CREATE TABLE audit (tag TEXT)").ok
    check execSql(db, "INSERT INTO base VALUES (1, 10)").ok
    check execSql(db, "INSERT INTO base VALUES (2, 20)").ok
    check execSql(db, "CREATE VIEW v AS SELECT id, val FROM base").ok

    check execSql(
      db,
      "CREATE TRIGGER trg_vi INSTEAD OF INSERT ON v FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''I'')')"
    ).ok
    check execSql(
      db,
      "CREATE TRIGGER trg_vu INSTEAD OF UPDATE ON v FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''U'')')"
    ).ok
    check execSql(
      db,
      "CREATE TRIGGER trg_vd INSTEAD OF DELETE ON v FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''D'')')"
    ).ok

    check execSql(db, "INSERT INTO v VALUES (9, 90)").ok
    check execSql(db, "UPDATE v SET val = val + 1 WHERE id <= 2").ok
    check execSql(db, "DELETE FROM v WHERE id = 1").ok

    let auditRes = execSql(db, "SELECT tag, COUNT(*) FROM audit GROUP BY tag ORDER BY tag")
    check auditRes.ok
    check auditRes.value.toHashSet == toHashSet(@["D|1", "I|1", "U|2"])

    let baseRes = execSql(db, "SELECT id, val FROM base ORDER BY id")
    check baseRes.ok
    check baseRes.value == @["1|10", "2|20"]

    check execSql(db, "DROP TRIGGER trg_vd ON v").ok
    let deleteNoTrig = execSql(db, "DELETE FROM v WHERE id = 2")
    check not deleteNoTrig.ok
    check deleteNoTrig.err.code == ERR_SQL

    check execSql(db, "DROP VIEW v").ok
    check execSql(db, "CREATE VIEW v AS SELECT id, val FROM base").ok
    let recreateTrig = execSql(
      db,
      "CREATE TRIGGER trg_vi INSTEAD OF INSERT ON v FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''I2'')')"
    )
    check recreateTrig.ok

    discard closeDb(db)

  test "AFTER triggers fire per-row for INSERT/UPDATE/DELETE and DROP TRIGGER stops firing":
    let path = makeTempDb("decentdb_sql_after_triggers.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY, val INT)").ok
    check execSql(db, "CREATE TABLE audit (tag TEXT)").ok

    check execSql(
      db,
      "CREATE TRIGGER trg_i AFTER INSERT ON src FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''I'')')"
    ).ok
    check execSql(db, "INSERT INTO src VALUES (1, 10)").ok
    check execSql(db, "INSERT INTO src VALUES (2, 20)").ok

    check execSql(db, "DROP TRIGGER trg_i ON src").ok
    check execSql(db, "INSERT INTO src VALUES (3, 30)").ok

    check execSql(
      db,
      "CREATE TRIGGER trg_u AFTER UPDATE ON src FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''U'')')"
    ).ok
    check execSql(
      db,
      "CREATE TRIGGER trg_d AFTER DELETE ON src FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (tag) VALUES (''D'')')"
    ).ok

    check execSql(db, "UPDATE src SET val = val + 1").ok
    check execSql(db, "DELETE FROM src WHERE id IN (1, 2)").ok

    let auditRes = execSql(db, "SELECT tag, COUNT(*) FROM audit GROUP BY tag ORDER BY tag")
    check auditRes.ok
    check auditRes.value.toHashSet == toHashSet(@["D|2", "I|2", "U|3"])

    discard closeDb(db)

  test "trigger action failure aborts parent DML statement":
    let path = makeTempDb("decentdb_sql_trigger_failure_rollback.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    check execSql(db, "CREATE TABLE src (id INT PRIMARY KEY)").ok
    check execSql(db, "CREATE TABLE audit (id INT PRIMARY KEY)").ok
    check execSql(
      db,
      "CREATE TRIGGER trg_fail AFTER INSERT ON src FOR EACH ROW " &
      "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES (1)')"
    ).ok

    check execSql(db, "INSERT INTO src VALUES (1)").ok
    let failInsert = execSql(db, "INSERT INTO src VALUES (2)")
    check not failInsert.ok

    let srcRows = execSql(db, "SELECT id FROM src ORDER BY id")
    check srcRows.ok
    check srcRows.value == @["1"]

    let auditRows = execSql(db, "SELECT id FROM audit ORDER BY id")
    check auditRows.ok
    check auditRows.value == @["1"]

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
