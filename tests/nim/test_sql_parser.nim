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

  test "parse insert/update/delete":
    let ins = parseSingle("INSERT INTO t (id, name) VALUES (1, 'x')")
    check ins.kind == skInsert
    check ins.insertValues.len == 2

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
