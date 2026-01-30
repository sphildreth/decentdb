import unittest
import engine
import catalog/catalog
import record/record

suite "Engine Helpers":
  test "enforceUnique missing index returns internal error":
    let pagerRes = newPager(newOsVfs(), open("/dev/null", fmReadOnly), cachePages = 1)
    # Cannot create pager easily; just construct minimal objects to call enforceUnique via wrong catalog
    let cat = Catalog(tables: initTable[string, TableMeta](), indexes: initTable[string, IndexMeta](), catalogTree: nil, trigramDeltas: initTable[(string, uint32), TrigramDelta]())
    let tbl = TableMeta(name: "t", rootPage: 1, nextRowId: 1, columns: @[@Column(name: "c", kind: ctInt64)])
    let res = enforceUnique(cat, nil, tbl, @[Value(kind: vkInt64, int64Val: 1)])
    check not res.ok

  test "referencingChildren returns empty when no refs":
    let cat = Catalog(tables: initTable[string, TableMeta](), indexes: initTable[string, IndexMeta](), catalogTree: nil, trigramDeltas: initTable[(string, uint32), TrigramDelta]())
    check referencingChildren(cat, "x", "y").len == 0
