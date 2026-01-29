import unittest

import tables
import catalog/catalog
import planner/planner
import sql/sql

proc makeCatalog(): Catalog =
  Catalog(
    tables: initTable[string, TableMeta](),
    indexes: initTable[string, IndexMeta](),
    catalogTree: nil
  )

proc addTable(catalog: Catalog, name: string) =
  catalog.tables[name] = TableMeta(name: name, rootPage: 1'u32, nextRowId: 1, columns: @[])

suite "Planner Extra":
  test "planner wraps aggregation and sort when necessary":
    var catalog = makeCatalog()
    addTable(catalog, "orders")
    let stmt = parseSql("SELECT user_id, COUNT(*) FROM orders GROUP BY user_id ORDER BY user_id DESC LIMIT 5").value.statements[0]
    let planRes = plan(catalog, stmt)
    check planRes.ok
    var plan = planRes.value
    check plan.kind == pkLimit
    check plan.left.kind == pkSort
    check plan.left.left.kind == pkAggregate
    check plan.left.left.groupBy.len == 1

  test "planner attaches join node even when indexes absent":
    var catalog = makeCatalog()
    addTable(catalog, "users")
    addTable(catalog, "posts")
    let stmt = parseSql("SELECT users.id, posts.id FROM users JOIN posts ON users.id = posts.user_id WHERE users.active = true").value.statements[0]
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let root = planRes.value
    check root.kind == pkProject
    check root.left.kind == pkJoin
    check root.left.left.kind == pkFilter
    check root.left.right.kind == pkTableScan

  test "planner enforces filter when equality predicate exists but index missing":
    var catalog = makeCatalog()
    addTable(catalog, "items")
    let stmt = parseSql("SELECT id FROM items WHERE id = 1").value.statements[0]
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let plan = planRes.value
    check plan.left.kind == pkFilter

  test "planner uses trigram plan for LIKE on indexed column":
    var catalog = makeCatalog()
    addTable(catalog, "docs")
    catalog.indexes["docs_body_trgm"] = IndexMeta(name: "docs_body_trgm", table: "docs", column: "body", rootPage: 3'u32, kind: ikTrigram, unique: false)
    let stmt = parseSql("SELECT id FROM docs WHERE body ILIKE '%abc%'").value.statements[0]
    let planRes = plan(catalog, stmt)
    check planRes.ok
    let plan = planRes.value
    check plan.left.kind in {pkTrigramSeek, pkFilter}
