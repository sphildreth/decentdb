# Unit tests for IN operator
import unittest
import ../../src/sql/sql
import ../../src/sql/binder
import ../../src/exec/exec
import ../../src/catalog/catalog
import ../../src/record/record

suite "IN Operator":
  test "parse IN expression with integer list":
    # Create a simple IN expression manually
    let inExpr = Expr(
      kind: ekInList,
      inExpr: Expr(kind: ekColumn, table: "", name: "id"),
      inList: @[
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 3))
      ]
    )
    check inExpr.kind == ekInList
    check inExpr.inList.len == 3
  
  test "evaluate IN expression with matching value":
    # Create a row
    let row = makeRow(
      @["id"],
      @[Value(kind: vkInt64, int64Val: 2)],
      1
    )
    
    # Create IN expression: id IN (1, 2, 3)
    let inExpr = Expr(
      kind: ekInList,
      inExpr: Expr(kind: ekColumn, table: "", name: "id"),
      inList: @[
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 3))
      ]
    )
    
    let result = evalExpr(row, inExpr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == true
  
  test "evaluate IN expression with non-matching value":
    # Create a row
    let row = makeRow(
      @["id"],
      @[Value(kind: vkInt64, int64Val: 99)],
      1
    )
    
    # Create IN expression: id IN (1, 2, 3)
    let inExpr = Expr(
      kind: ekInList,
      inExpr: Expr(kind: ekColumn, table: "", name: "id"),
      inList: @[
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 3))
      ]
    )
    
    let result = evalExpr(row, inExpr, @[])
    check result.ok
    check result.value.kind == vkBool
    check result.value.boolVal == false
  
  test "evaluate IN expression with NULL value":
    # Create a row with NULL
    let row = makeRow(
      @["id"],
      @[Value(kind: vkNull)],
      1
    )
    
    # Create IN expression: id IN (1, 2, 3)
    let inExpr = Expr(
      kind: ekInList,
      inExpr: Expr(kind: ekColumn, table: "", name: "id"),
      inList: @[
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 1)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 2)),
        Expr(kind: ekLiteral, value: SqlValue(kind: svInt, intVal: 3))
      ]
    )
    
    let result = evalExpr(row, inExpr, @[])
    check result.ok
    # NULL IN (...) should return NULL (3-valued logic)
    check result.value.kind == vkNull
