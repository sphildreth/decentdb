import unittest

import sql/sql
import tables

proc parseSingle(sqlText: string): Statement =
  let astRes = parseSql(sqlText)
  check astRes.ok
  check astRes.value.statements.len == 1
  astRes.value.statements[0]

suite "SQL Parser":
  test "parse select with joins and expressions":
    let stmt = parseSingle("SELECT a.id, b.name FROM a INNER JOIN b ON a.id = b.a_id WHERE (a.id = $1 AND b.name IS NOT NULL) OR a.id > 1 ORDER BY a.id DESC LIMIT 5 OFFSET 2")
    check stmt.kind == skSelect
    check stmt.joins.len == 1
    check stmt.whereExpr != nil
    check stmt.orderBy.len == 1
    check stmt.limit == 5
    check stmt.offset == 2

  test "parse NULL predicates and NULL literal comparisons":
    let stmt = parseSingle("SELECT id FROM t WHERE NOT (val = NULL) OR val IS NULL")
    check stmt.kind == skSelect
    check stmt.whereExpr != nil
    check stmt.whereExpr.kind == ekBinary
    check stmt.whereExpr.op == "OR"
    check stmt.whereExpr.left.kind == ekUnary
    check stmt.whereExpr.left.unOp == "NOT"
    check stmt.whereExpr.left.expr.kind == ekBinary
    check stmt.whereExpr.left.expr.op == "="
    check stmt.whereExpr.left.expr.right.kind == ekLiteral
    check stmt.whereExpr.left.expr.right.value.kind == svNull
    check stmt.whereExpr.right.kind == ekBinary
    check stmt.whereExpr.right.op == "IS"

    let inStmt = parseSingle("SELECT id FROM t WHERE id IN (1, NULL)")
    check inStmt.whereExpr != nil
    check inStmt.whereExpr.kind == ekInList
    check inStmt.whereExpr.inList.len == 2

  test "parse scalar functions and concatenation":
    let stmt = parseSingle("SELECT COALESCE(name, 'x'), LENGTH(name), TRIM(name) || '_x' FROM t")
    check stmt.kind == skSelect
    check stmt.selectItems.len == 3
    check stmt.selectItems[0].expr.kind == ekFunc
    check stmt.selectItems[0].expr.funcName == "COALESCE"
    check stmt.selectItems[1].expr.kind == ekFunc
    check stmt.selectItems[1].expr.funcName == "LENGTH"
    check stmt.selectItems[2].expr.kind == ekBinary
    check stmt.selectItems[2].expr.op == "||"

  test "parse CASE, CAST, BETWEEN, EXISTS, and LIKE ESCAPE":
    let stmt = parseSingle(
      "SELECT CASE WHEN id > 1 THEN 'big' ELSE 'small' END, CAST(id AS TEXT) " &
      "FROM t WHERE id BETWEEN 1 AND 3 AND EXISTS (SELECT 1 FROM t2) AND name LIKE 'a\\_%' ESCAPE '\\\\'"
    )
    check stmt.kind == skSelect
    check stmt.selectItems.len == 2
    check stmt.selectItems[0].expr.kind == ekFunc
    check stmt.selectItems[0].expr.funcName == "CASE"
    check stmt.selectItems[1].expr.kind == ekFunc
    check stmt.selectItems[1].expr.funcName == "CAST"
    check stmt.whereExpr != nil
    check stmt.whereExpr.kind == ekBinary

  test "parse insert/update/delete":
    let ins = parseSingle("INSERT INTO t (id, name) VALUES (1, 'x')")
    check ins.kind == skInsert
    check ins.insertValues.len == 2
    check ins.insertConflictAction == icaNone
    check ins.insertConflictTargetCols.len == 0
    check ins.insertConflictTargetConstraint.len == 0

    let insConflictAny = parseSingle("INSERT INTO t (id, name) VALUES (1, 'x') ON CONFLICT DO NOTHING")
    check insConflictAny.kind == skInsert
    check insConflictAny.insertConflictAction == icaDoNothing
    check insConflictAny.insertConflictTargetCols.len == 0

    let insConflictCols = parseSingle("INSERT INTO t (id, name) VALUES (1, 'x') ON CONFLICT (id, name) DO NOTHING")
    check insConflictCols.kind == skInsert
    check insConflictCols.insertConflictAction == icaDoNothing
    check insConflictCols.insertConflictTargetCols == @["id", "name"]

    let insConflictConstraint = parseSingle("INSERT INTO t (id, name) VALUES (1, 'x') ON CONFLICT ON CONSTRAINT t_name_uq DO NOTHING")
    check insConflictConstraint.kind == skInsert
    check insConflictConstraint.insertConflictAction == icaDoNothing
    check insConflictConstraint.insertConflictTargetConstraint == "t_name_uq"

    let insConflictUpdate = parseSingle(
      "INSERT INTO t (id, name) VALUES (1, 'x') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE t.id > 0"
    )
    check insConflictUpdate.kind == skInsert
    check insConflictUpdate.insertConflictAction == icaDoUpdate
    check insConflictUpdate.insertConflictTargetCols == @["id"]
    var conflictAssignCount = 0
    for _, _ in insConflictUpdate.insertConflictUpdateAssignments.pairs:
      conflictAssignCount.inc
    check conflictAssignCount == 1
    check insConflictUpdate.insertConflictUpdateWhere != nil

    let returningUnsupportedRes = parseSql("INSERT INTO t (id, name) VALUES (1, 'x') RETURNING id")
    check not returningUnsupportedRes.ok

    let upd = parseSingle("UPDATE t SET name = 'y' WHERE id = 1")
    check upd.kind == skUpdate
    var assignCount = 0
    for _, _ in upd.assignments.pairs:
      assignCount.inc
    check assignCount == 1

    let del = parseSingle("DELETE FROM t WHERE id = 1")
    check del.kind == skDelete

  test "parse create table and index":
    let crt = parseSingle("CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL UNIQUE, parent_id INT REFERENCES parent(id))")
    check crt.kind == skCreateTable
    check crt.columns.len == 3

    let idx = parseSingle("CREATE INDEX t_name_trgm ON t USING trigram (name)")
    check idx.kind == skCreateIndex
    check idx.indexKind == ikTrigram

  test "parse drop and transactions":
    let drt = parseSingle("DROP TABLE t")
    check drt.kind == skDropTable

    let dri = parseSingle("DROP INDEX idx")
    check dri.kind == skDropIndex

    let beginStmt = parseSingle("BEGIN")
    check beginStmt.kind == skBegin

    let commitStmt = parseSingle("COMMIT")
    check commitStmt.kind == skCommit

    let rollbackStmt = parseSingle("ROLLBACK")
    check rollbackStmt.kind == skRollback

  test "parse create/drop/alter view":
    let cv = parseSingle("CREATE VIEW v AS SELECT id, name FROM t")
    check cv.kind == skCreateView
    check cv.createViewName == "v"
    check cv.createViewIfNotExists == false
    check cv.createViewOrReplace == false
    check cv.createViewColumns.len == 0
    check cv.createViewQuery != nil
    check cv.createViewQuery.kind == skSelect

    let cvIf = parseSingle("CREATE VIEW IF NOT EXISTS v2 (a,b) AS SELECT id, name FROM t")
    check cvIf.kind == skCreateView
    check cvIf.createViewIfNotExists
    check cvIf.createViewOrReplace == false
    check cvIf.createViewColumns == @["a", "b"]

    let cvOrReplace = parseSingle("CREATE OR REPLACE VIEW v3 AS SELECT id FROM t")
    check cvOrReplace.kind == skCreateView
    check cvOrReplace.createViewOrReplace
    check cvOrReplace.createViewIfNotExists == false

    let dv = parseSingle("DROP VIEW IF EXISTS v")
    check dv.kind == skDropView
    check dv.dropViewName == "v"
    check dv.dropViewIfExists

    let av = parseSingle("ALTER VIEW v RENAME TO v_new")
    check av.kind == skAlterView
    check av.alterViewName == "v"
    check av.alterViewNewName == "v_new"
