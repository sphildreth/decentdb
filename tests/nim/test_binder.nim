import unittest
import os
import tables

import engine
import sql/sql
import sql/binder
import catalog/catalog
import storage/storage

proc makeTempDb(name: string): string =
  let path = getTempDir() / (if name.len >= 3 and name[name.len - 3 .. ^1] == ".db": name[0 .. ^4] & ".ddb" else: name)
  if fileExists(path):
    removeFile(path)
  if fileExists(path & "-wal"):
    removeFile(path & "-wal")
  if fileExists(path & ".wal"):
    removeFile(path & ".wal")
  path

proc addTable(db: Db, name: string, columns: seq[Column]): TableMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = TableMeta(name: name, rootPage: rootRes.value, nextRowId: 1, columns: columns)
  check db.catalog.saveTable(db.pager, meta).ok
  meta

proc addIndex(db: Db, name: string, table: string, column: string, unique: bool): IndexMeta =
  let rootRes = initTableRoot(db.pager)
  check rootRes.ok
  let meta = IndexMeta(name: name, table: table, columns: @[column], rootPage: rootRes.value, kind: ikBtree, unique: unique)
  check db.catalog.createIndexMeta(meta).ok
  meta

proc addView(db: Db, name: string, sqlText: string, columnNames: seq[string], dependencies: seq[string]): ViewMeta =
  let meta = ViewMeta(name: name, sqlText: sqlText, columnNames: columnNames, dependencies: dependencies)
  check db.catalog.createViewMeta(meta).ok
  meta

proc parseSingle(sqlText: string): Statement =
  let astRes = parseSql(sqlText)
  check astRes.ok
  check astRes.value.statements.len == 1
  astRes.value.statements[0]

suite "Binder":
  test "bind non-recursive CTE scoping and shadowing":
    let path = makeTempDb("decentdb_binder_cte.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "users", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64)])

    let chain = bindStatement(
      db.catalog,
      parseSingle(
        "WITH a AS (SELECT id FROM users), b AS (SELECT id FROM a WHERE id > 1) " &
        "SELECT id FROM b"
      )
    )
    check chain.ok
    check chain.value.kind == skSelect
    check chain.value.fromTable == "users"
    check chain.value.cteNames.len == 0

    let shadow = bindStatement(
      db.catalog,
      parseSingle("WITH t AS (SELECT id FROM users WHERE id = 1) SELECT id FROM t")
    )
    check shadow.ok
    check shadow.value.fromTable == "users"

    let forwardRef = bindStatement(
      db.catalog,
      parseSingle(
        "WITH b AS (SELECT id FROM a), a AS (SELECT id FROM users) SELECT id FROM b"
      )
    )
    check not forwardRef.ok

    let badShape = bindStatement(
      db.catalog,
      parseSingle("WITH a AS (SELECT id FROM users ORDER BY id) SELECT id FROM a")
    )
    check not badShape.ok

    discard closeDb(db)

  test "bind set operations":
    let path = makeTempDb("decentdb_binder_union_all.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "a", @[Column(name: "id", kind: ctInt64)])
    discard addTable(db, "b", @[Column(name: "id", kind: ctInt64)])

    let unionAll = bindStatement(db.catalog, parseSingle("SELECT id FROM a UNION ALL SELECT id FROM b"))
    check unionAll.ok
    check unionAll.value.kind == skSelect
    check unionAll.value.setOpKind == sokUnionAll
    check unionAll.value.setOpLeft != nil
    check unionAll.value.setOpRight != nil

    let unionDistinct = bindStatement(db.catalog, parseSingle("SELECT id FROM a UNION SELECT id FROM b"))
    check unionDistinct.ok
    check unionDistinct.value.setOpKind == sokUnion

    let intersect = bindStatement(db.catalog, parseSingle("SELECT id FROM a INTERSECT SELECT id FROM b"))
    check intersect.ok
    check intersect.value.setOpKind == sokIntersect

    let exceptStmt = bindStatement(db.catalog, parseSingle("SELECT id FROM a EXCEPT SELECT id FROM b"))
    check exceptStmt.ok
    check exceptStmt.value.setOpKind == sokExcept

    discard closeDb(db)

  test "bind errors for unknown table and ambiguous column":
    let path = makeTempDb("decentdb_binder_errors.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "a", @[Column(name: "id", kind: ctInt64)])
    discard addTable(db, "b", @[Column(name: "id", kind: ctInt64)])

    let stmtUnknown = parseSingle("SELECT id FROM missing")
    let bindUnknown = bindStatement(db.catalog, stmtUnknown)
    check not bindUnknown.ok

    let stmtAmbig = parseSingle("SELECT id FROM a INNER JOIN b ON a.id = b.id")
    let bindAmbig = bindStatement(db.catalog, stmtAmbig)
    check not bindAmbig.ok

    let stmtNull = parseSingle("SELECT id FROM a WHERE NOT (id = NULL) OR id IS NULL OR id IN (1, NULL)")
    let bindNull = bindStatement(db.catalog, stmtNull)
    check bindNull.ok

    discard closeDb(db)

  test "bind insert/update column validation":
    let path = makeTempDb("decentdb_binder_cols.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])

    let stmtInsert = parseSingle("INSERT INTO t (id) VALUES (1, 2)")
    let bindInsert = bindStatement(db.catalog, stmtInsert)
    check not bindInsert.ok

    let stmtUpdate = parseSingle("UPDATE t SET missing = 1")
    let bindUpdate = bindStatement(db.catalog, stmtUpdate)
    check not bindUpdate.ok

    let stmtType = parseSingle("INSERT INTO t (id, name) VALUES ('bad', 'ok')")
    let bindType = bindStatement(db.catalog, stmtType)
    check not bindType.ok

    let stmtFuncs = parseSingle("SELECT COALESCE(name, 'x'), LENGTH(name), TRIM(name) || '_x' FROM t")
    let bindFuncs = bindStatement(db.catalog, stmtFuncs)
    check bindFuncs.ok

    discard addTable(db, "t2", @[Column(name: "id", kind: ctInt64)])
    let stmtExprs = parseSingle(
      "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END, CAST(id AS TEXT) " &
      "FROM t WHERE id BETWEEN 1 AND 3 AND EXISTS (SELECT 1 FROM t2)"
    )
    let bindExprs = bindStatement(db.catalog, stmtExprs)
    check bindExprs.ok

    discard closeDb(db)

  test "bind ON CONFLICT DO NOTHING targets":
    let path = makeTempDb("decentdb_binder_on_conflict.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "users", @[
      Column(name: "id", kind: ctInt64, primaryKey: true),
      Column(name: "email", kind: ctText, unique: true),
      Column(name: "name", kind: ctText)
    ])
    discard addIndex(db, "users_email_uq_idx", "users", "email", true)
    discard addIndex(db, "users_name_idx", "users", "name", false)

    let anyTarget = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT DO NOTHING")
    )
    check anyTarget.ok
    check anyTarget.value.insertConflictAction == icaDoNothing
    check anyTarget.value.insertConflictTargetCols.len == 0

    let colTarget = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT (email) DO NOTHING")
    )
    check colTarget.ok
    check colTarget.value.insertConflictTargetCols == @["email"]

    let badColTarget = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT (name) DO NOTHING")
    )
    check not badColTarget.ok

    let constraintTarget = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT ON CONSTRAINT users_email_uq_idx DO NOTHING")
    )
    check constraintTarget.ok
    check constraintTarget.value.insertConflictTargetCols == @["email"]

    let badConstraintTarget = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT ON CONSTRAINT users_name_idx DO NOTHING")
    )
    check not badConstraintTarget.ok

    let doUpdateTargeted = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name")
    )
    check doUpdateTargeted.ok
    check doUpdateTargeted.value.insertConflictAction == icaDoUpdate
    let updateAssigns = doUpdateTargeted.value.insertConflictUpdateAssignments
    var doUpdateAssignCount = 0
    for _, _ in updateAssigns.pairs:
      doUpdateAssignCount.inc
    check doUpdateAssignCount == 1

    let doUpdateNoTarget = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT DO UPDATE SET name = EXCLUDED.name")
    )
    check not doUpdateNoTarget.ok

    let doUpdateBadSource = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') ON CONFLICT (email) DO UPDATE SET name = missing.col")
    )
    check not doUpdateBadSource.ok

    let returningBind = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') RETURNING id, email")
    )
    check returningBind.ok
    check returningBind.value.insertReturning.len == 2

    let badReturningBind = bindStatement(
      db.catalog,
      parseSingle("INSERT INTO users (id, email, name) VALUES (1, 'a@x', 'a') RETURNING missing")
    )
    check not badReturningBind.ok

    discard closeDb(db)

  test "bind create table and index constraints":
    let path = makeTempDb("decentdb_binder_create.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "parent", @[Column(name: "id", kind: ctInt64)])

    let stmtPk = parseSingle("CREATE TABLE bad (a INT PRIMARY KEY, b INT PRIMARY KEY)")
    let bindPk = bindStatement(db.catalog, stmtPk)
    check bindPk.ok  # composite PKs are now supported

    let stmtFk = parseSingle("CREATE TABLE child (id INT, parent_id INT REFERENCES parent(id))")
    let bindFk = bindStatement(db.catalog, stmtFk)
    check not bindFk.ok

    discard addIndex(db, "parent_id_idx", "parent", "id", false)
    let stmtFk2 = parseSingle("CREATE TABLE child2 (id INT, parent_id INT REFERENCES parent(id))")
    let bindFk2 = bindStatement(db.catalog, stmtFk2)
    check not bindFk2.ok

    discard addIndex(db, "parent_id_uq", "parent", "id", true)
    let stmtFk3 = parseSingle("CREATE TABLE child3 (id INT, parent_id INT REFERENCES parent(id))")
    let bindFk3 = bindStatement(db.catalog, stmtFk3)
    check bindFk3.ok

    let stmtFkCascade = parseSingle("CREATE TABLE child_cascade (id INT, parent_id INT REFERENCES parent(id) ON DELETE CASCADE)")
    let bindFkCascade = bindStatement(db.catalog, stmtFkCascade)
    check bindFkCascade.ok

    let stmtFkSetNull = parseSingle("CREATE TABLE child_setnull (id INT, parent_id INT REFERENCES parent(id) ON DELETE SET NULL)")
    let bindFkSetNull = bindStatement(db.catalog, stmtFkSetNull)
    check bindFkSetNull.ok

    let stmtFkSetNullNotNull = parseSingle("CREATE TABLE child_setnull_bad (id INT, parent_id INT NOT NULL REFERENCES parent(id) ON DELETE SET NULL)")
    let bindFkSetNullNotNull = bindStatement(db.catalog, stmtFkSetNullNotNull)
    check not bindFkSetNullNotNull.ok

    let stmtFkOnUpdateCascade = parseSingle("CREATE TABLE child_upd (id INT, parent_id INT REFERENCES parent(id) ON UPDATE CASCADE)")
    let bindFkOnUpdateCascade = bindStatement(db.catalog, stmtFkOnUpdateCascade)
    check bindFkOnUpdateCascade.ok

    let stmtFkOnUpdateSetNull = parseSingle("CREATE TABLE child_upd_null (id INT, parent_id INT REFERENCES parent(id) ON UPDATE SET NULL)")
    let bindFkOnUpdateSetNull = bindStatement(db.catalog, stmtFkOnUpdateSetNull)
    check bindFkOnUpdateSetNull.ok

    let stmtFkOnUpdateSetNullBad = parseSingle("CREATE TABLE child_upd_null_bad (id INT, parent_id INT NOT NULL REFERENCES parent(id) ON UPDATE SET NULL)")
    let bindFkOnUpdateSetNullBad = bindStatement(db.catalog, stmtFkOnUpdateSetNullBad)
    check not bindFkOnUpdateSetNullBad.ok

    let stmtIdxBad = parseSingle("CREATE INDEX t_trgm ON parent USING trigram (id)")
    let bindIdxBad = bindStatement(db.catalog, stmtIdxBad)
    check not bindIdxBad.ok

    let stmtIdxUniq = parseSingle("CREATE UNIQUE INDEX t_trgm2 ON parent USING trigram (id)")
    let bindIdxUniq = bindStatement(db.catalog, stmtIdxUniq)
    check not bindIdxUniq.ok

    let stmtPartialOk = parseSingle("CREATE INDEX parent_id_partial ON parent (id) WHERE id IS NOT NULL")
    let bindPartialOk = bindStatement(db.catalog, stmtPartialOk)
    check bindPartialOk.ok

    let stmtPartialBadExpr = parseSingle("CREATE INDEX parent_id_partial_bad ON parent (id) WHERE id > 0")
    let bindPartialBadExpr = bindStatement(db.catalog, stmtPartialBadExpr)
    check bindPartialBadExpr.ok

    let stmtPartialBadUnique = parseSingle("CREATE UNIQUE INDEX parent_id_partial_uq ON parent (id) WHERE id IS NOT NULL")
    let bindPartialBadUnique = bindStatement(db.catalog, stmtPartialBadUnique)
    check bindPartialBadUnique.ok

    let stmtPartialBadMulti = parseSingle("CREATE INDEX parent_multi_partial ON parent (id, id) WHERE id IS NOT NULL")
    let bindPartialBadMulti = bindStatement(db.catalog, stmtPartialBadMulti)
    check bindPartialBadMulti.ok

    let stmtExprOk = parseSingle("CREATE INDEX parent_id_txt_expr ON parent ((CAST(id AS TEXT)))")
    let bindExprOk = bindStatement(db.catalog, stmtExprOk)
    check bindExprOk.ok

    let stmtExprBadShape = parseSingle("CREATE INDEX parent_id_expr_bad ON parent ((id + 1))")
    let bindExprBadShape = bindStatement(db.catalog, stmtExprBadShape)
    check not bindExprBadShape.ok

    let stmtExprUnique = parseSingle("CREATE UNIQUE INDEX parent_id_expr_uq ON parent ((LOWER(id)))")
    let bindExprUnique = bindStatement(db.catalog, stmtExprUnique)
    check not bindExprUnique.ok

    let stmtExprPartial = parseSingle("CREATE INDEX parent_id_expr_partial ON parent ((CAST(id AS TEXT))) WHERE id IS NOT NULL")
    let bindExprPartial = bindStatement(db.catalog, stmtExprPartial)
    check not bindExprPartial.ok

    let stmtCheckOk = parseSingle(
      "CREATE TABLE chk_ok (" &
      "id INT, amount INT, " &
      "CHECK (amount >= 0), " &
      "CONSTRAINT id_pos CHECK (id > 0 OR id IS NULL))"
    )
    let bindCheckOk = bindStatement(db.catalog, stmtCheckOk)
    check bindCheckOk.ok

    let stmtCheckBadCol = parseSingle("CREATE TABLE chk_bad_col (id INT, CHECK (missing > 0))")
    let bindCheckBadCol = bindStatement(db.catalog, stmtCheckBadCol)
    check not bindCheckBadCol.ok

    let stmtCheckParam = parseSingle("CREATE TABLE chk_param (id INT, CHECK (id > $1))")
    let bindCheckParam = bindStatement(db.catalog, stmtCheckParam)
    check not bindCheckParam.ok

    let stmtCheckExists = parseSingle("CREATE TABLE chk_exists (id INT, CHECK (EXISTS (SELECT 1)))")
    let bindCheckExists = bindStatement(db.catalog, stmtCheckExists)
    check not bindCheckExists.ok

    let stmtCheckUnsupportedFn = parseSingle("CREATE TABLE chk_fn (id INT, CHECK (ABS(id) > 0))")
    let bindCheckUnsupportedFn = bindStatement(db.catalog, stmtCheckUnsupportedFn)
    check not bindCheckUnsupportedFn.ok

    discard closeDb(db)

  test "bind create view and select expansion":
    let path = makeTempDb("decentdb_binder_view_basic.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])

    let createStmt = parseSingle("CREATE VIEW v AS SELECT id AS x, name FROM t")
    let createBind = bindStatement(db.catalog, createStmt)
    check createBind.ok
    check createBind.value.kind == skCreateView
    check createBind.value.createViewColumns == @["x", "name"]

    discard addView(db, "v", "SELECT id AS x, name FROM t", @["x", "name"], @["t"])
    let selectStmt = parseSingle("SELECT x FROM v WHERE x = 1")
    let selectBind = bindStatement(db.catalog, selectStmt)
    check selectBind.ok
    check selectBind.value.kind == skSelect
    check selectBind.value.fromTable == "t"

    discard closeDb(db)

  test "bind view validations and DML rejection":
    let path = makeTempDb("decentdb_binder_view_validation.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    discard addView(db, "v", "SELECT id FROM t", @["id"], @["t"])

    let mismatch = bindStatement(db.catalog, parseSingle("CREATE VIEW vm (a) AS SELECT id, name FROM t"))
    check not mismatch.ok

    let duplicate = bindStatement(db.catalog, parseSingle("CREATE VIEW vd AS SELECT id, id FROM t"))
    check not duplicate.ok

    let withParam = bindStatement(db.catalog, parseSingle("CREATE VIEW vp AS SELECT id FROM t WHERE id = $1"))
    check not withParam.ok

    let insertView = bindStatement(db.catalog, parseSingle("INSERT INTO v (id) VALUES (1)"))
    check not insertView.ok
    let updateView = bindStatement(db.catalog, parseSingle("UPDATE v SET id = 2"))
    check not updateView.ok
    let deleteView = bindStatement(db.catalog, parseSingle("DELETE FROM v"))
    check not deleteView.ok

    check db.catalog.createTriggerMeta(TriggerMeta(
      name: "v_ins",
      table: "v",
      eventsMask: TriggerTimingInsteadMask or TriggerEventInsertMask,
      actionSql: "INSERT INTO t (id, name) VALUES (1, 'x')"
    )).ok
    check db.catalog.createTriggerMeta(TriggerMeta(
      name: "v_upd",
      table: "v",
      eventsMask: TriggerTimingInsteadMask or TriggerEventUpdateMask,
      actionSql: "UPDATE t SET name = 'y' WHERE id = 1"
    )).ok
    check db.catalog.createTriggerMeta(TriggerMeta(
      name: "v_del",
      table: "v",
      eventsMask: TriggerTimingInsteadMask or TriggerEventDeleteMask,
      actionSql: "DELETE FROM t WHERE id = 1"
    )).ok

    let insertViewWithInstead = bindStatement(db.catalog, parseSingle("INSERT INTO v (id) VALUES (1)"))
    check insertViewWithInstead.ok
    let updateViewWithInstead = bindStatement(db.catalog, parseSingle("UPDATE v SET id = 2"))
    check updateViewWithInstead.ok
    let deleteViewWithInstead = bindStatement(db.catalog, parseSingle("DELETE FROM v"))
    check deleteViewWithInstead.ok

    discard closeDb(db)

  test "bind strict dependency semantics":
    let path = makeTempDb("decentdb_binder_view_dependencies.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64)])
    discard addView(db, "v1", "SELECT id FROM t", @["id"], @["t"])
    discard addView(db, "v2", "SELECT id FROM v1", @["id"], @["v1"])

    let dropTable = bindStatement(db.catalog, parseSingle("DROP TABLE t"))
    check not dropTable.ok

    let dropView = bindStatement(db.catalog, parseSingle("DROP VIEW v1"))
    check not dropView.ok

    let renameView = bindStatement(db.catalog, parseSingle("ALTER VIEW v1 RENAME TO v1_new"))
    check not renameView.ok

    let cycleReplace = bindStatement(db.catalog, parseSingle("CREATE OR REPLACE VIEW v1 AS SELECT id FROM v2"))
    check not cycleReplace.ok

    discard closeDb(db)

  test "bind ALTER TABLE RENAME COLUMN constraints":
    let path = makeTempDb("decentdb_binder_rename_column.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value
    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])

    let okRename = bindStatement(db.catalog, parseSingle("ALTER TABLE t RENAME COLUMN name TO full_name"))
    check okRename.ok

    let missing = bindStatement(db.catalog, parseSingle("ALTER TABLE t RENAME COLUMN missing TO full_name"))
    check not missing.ok

    let exists = bindStatement(db.catalog, parseSingle("ALTER TABLE t RENAME COLUMN name TO id"))
    check not exists.ok

    discard addView(db, "tv", "SELECT id FROM t", @["id"], @["t"])
    let blockedByView = bindStatement(db.catalog, parseSingle("ALTER TABLE t RENAME COLUMN name TO full_name"))
    check not blockedByView.ok

    discard closeDb(db)

  test "bind ALTER TABLE blocked for expression-indexed tables":
    let path = makeTempDb("decentdb_binder_alter_expr_index.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "name", kind: ctText)])
    discard addIndex(db, "t_name_lower_expr_idx", "t", IndexExpressionPrefix & "LOWER(name)", false)

    let alterAdd = bindStatement(db.catalog, parseSingle("ALTER TABLE t ADD COLUMN extra INT"))
    check not alterAdd.ok
    let alterRename = bindStatement(db.catalog, parseSingle("ALTER TABLE t RENAME COLUMN name TO full_name"))
    check not alterRename.ok

    discard closeDb(db)

  test "bind ALTER TABLE ALTER COLUMN TYPE constraints":
    let path = makeTempDb("decentdb_binder_alter_column_type.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(
      db,
      "parent",
      @[
        Column(name: "id", kind: ctInt64, primaryKey: true),
        Column(name: "code", kind: ctText, unique: true)
      ]
    )
    discard addTable(
      db,
      "child",
      @[
        Column(name: "id", kind: ctInt64, primaryKey: true),
        Column(name: "parent_id", kind: ctInt64, refTable: "parent", refColumn: "id"),
        Column(name: "code_ref", kind: ctText, refTable: "parent", refColumn: "code")
      ]
    )

    let okSetType = bindStatement(db.catalog, parseSingle("ALTER TABLE parent ALTER COLUMN code TYPE INT"))
    check not okSetType.ok

    let blockedPk = bindStatement(db.catalog, parseSingle("ALTER TABLE parent ALTER COLUMN id TYPE TEXT"))
    check not blockedPk.ok

    let blockedReferenced = bindStatement(db.catalog, parseSingle("ALTER TABLE parent ALTER COLUMN code TYPE BOOL"))
    check not blockedReferenced.ok

    let blockedFkChild = bindStatement(db.catalog, parseSingle("ALTER TABLE child ALTER COLUMN parent_id TYPE TEXT"))
    check not blockedFkChild.ok

    let badTarget = bindStatement(db.catalog, parseSingle("ALTER TABLE child ALTER COLUMN code_ref TYPE BLOB"))
    check not badTarget.ok

    discard addTable(db, "isolated", @[Column(name: "v", kind: ctText)])
    let isolatedOk = bindStatement(db.catalog, parseSingle("ALTER TABLE isolated ALTER COLUMN v TYPE BOOL"))
    check isolatedOk.ok

    discard closeDb(db)

  test "bind CREATE/DROP TRIGGER constraints":
    let path = makeTempDb("decentdb_binder_triggers.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64)])
    discard addTable(db, "audit", @[Column(name: "id", kind: ctInt64)])
    discard addView(db, "v", "SELECT id FROM t", @["id"], @["t"])

    let okTrigger = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg AFTER INSERT ON t FOR EACH ROW " &
        "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES (1)')"
      )
    )
    check okTrigger.ok

    let badFn = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg2 AFTER INSERT ON t FOR EACH ROW " &
        "EXECUTE FUNCTION other_fn('INSERT INTO audit (id) VALUES (1)')"
      )
    )
    check not badFn.ok

    let stmtLevel = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg3 AFTER INSERT ON t FOR EACH STATEMENT " &
        "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES (1)')"
      )
    )
    check not stmtLevel.ok

    let badActionKind = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg4 AFTER INSERT ON t FOR EACH ROW " &
        "EXECUTE FUNCTION decentdb_exec_sql('SELECT id FROM t')"
      )
    )
    check not badActionKind.ok

    let actionWithParams = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg5 AFTER INSERT ON t FOR EACH ROW " &
        "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES ($1)')"
      )
    )
    check not actionWithParams.ok

    let insteadOnView = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg_v INSTEAD OF INSERT ON v FOR EACH ROW " &
        "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES (2)')"
      )
    )
    check insteadOnView.ok

    let afterOnView = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg_v_bad AFTER INSERT ON v FOR EACH ROW " &
        "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES (3)')"
      )
    )
    check not afterOnView.ok

    let insteadOnTable = bindStatement(
      db.catalog,
      parseSingle(
        "CREATE TRIGGER trg_t_bad INSTEAD OF INSERT ON t FOR EACH ROW " &
        "EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit (id) VALUES (4)')"
      )
    )
    check not insteadOnTable.ok

    check db.catalog.createTriggerMeta(TriggerMeta(name: "existing", table: "t", eventsMask: TriggerEventInsertMask, actionSql: "INSERT INTO audit (id) VALUES (9)")).ok
    let dropOk = bindStatement(db.catalog, parseSingle("DROP TRIGGER existing ON t"))
    check dropOk.ok
    check db.catalog.createTriggerMeta(TriggerMeta(name: "existing_v", table: "v", eventsMask: TriggerTimingInsteadMask or TriggerEventInsertMask, actionSql: "INSERT INTO audit (id) VALUES (10)")).ok
    let dropViewTrig = bindStatement(db.catalog, parseSingle("DROP TRIGGER existing_v ON v"))
    check dropViewTrig.ok
    let dropMissing = bindStatement(db.catalog, parseSingle("DROP TRIGGER missing ON t"))
    check not dropMissing.ok
    let dropMissingIf = bindStatement(db.catalog, parseSingle("DROP TRIGGER IF EXISTS missing ON t"))
    check dropMissingIf.ok

    discard closeDb(db)

  test "bind ROW_NUMBER window subset constraints":
    let path = makeTempDb("decentdb_binder_window_row_number.db")
    let dbRes = openDb(path)
    check dbRes.ok
    let db = dbRes.value

    discard addTable(db, "t", @[Column(name: "id", kind: ctInt64), Column(name: "grp", kind: ctText)])

    let okWindow = bindStatement(
      db.catalog,
      parseSingle("SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM t")
    )
    check okWindow.ok

    let missingOrder = bindStatement(
      db.catalog,
      parseSingle("SELECT ROW_NUMBER() OVER (PARTITION BY grp) FROM t")
    )
    check not missingOrder.ok

    let windowInWhere = bindStatement(
      db.catalog,
      parseSingle("SELECT id FROM t WHERE ROW_NUMBER() OVER (ORDER BY id) > 1")
    )
    check not windowInWhere.ok

    discard closeDb(db)
