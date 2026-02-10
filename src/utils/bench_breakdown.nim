when defined(bench_breakdown):
  import std/monotimes

  type InsertBenchBreakdown* = object
    engineCalls*: int64
    engineTotalNs*: int64
    engineEvalValuesNs*: int64
    engineTypeCheckNs*: int64
    engineConstraintsNs*: int64
    engineStorageCallNs*: int64

    storageCalls*: int64
    storageTotalNs*: int64
    storageNormalizeNs*: int64
    storageEncodeRecordNs*: int64
    storageBtreeInsertNs*: int64
    storageTableMetaNs*: int64

    btreeCalls*: int64
    btreeTotalNs*: int64

    pagerUpgradeCalls*: int64
    pagerUpgradeTotalNs*: int64

  var gInsertBreakdown {.threadvar.}: InsertBenchBreakdown

  proc resetInsertBenchBreakdown*() {.inline.} =
    gInsertBreakdown = default(InsertBenchBreakdown)

  proc snapshotInsertBenchBreakdown*(): InsertBenchBreakdown {.inline.} =
    gInsertBreakdown

  proc addEngineTotalNs*(ns: int64) {.inline.} =
    gInsertBreakdown.engineTotalNs += ns
    gInsertBreakdown.engineCalls.inc

  proc addEngineEvalValuesNs*(ns: int64) {.inline.} =
    gInsertBreakdown.engineEvalValuesNs += ns

  proc addEngineTypeCheckNs*(ns: int64) {.inline.} =
    gInsertBreakdown.engineTypeCheckNs += ns

  proc addEngineConstraintsNs*(ns: int64) {.inline.} =
    gInsertBreakdown.engineConstraintsNs += ns

  proc addEngineStorageCallNs*(ns: int64) {.inline.} =
    gInsertBreakdown.engineStorageCallNs += ns

  proc addStorageTotalNs*(ns: int64) {.inline.} =
    gInsertBreakdown.storageTotalNs += ns
    gInsertBreakdown.storageCalls.inc

  proc addStorageNormalizeNs*(ns: int64) {.inline.} =
    gInsertBreakdown.storageNormalizeNs += ns

  proc addStorageEncodeRecordNs*(ns: int64) {.inline.} =
    gInsertBreakdown.storageEncodeRecordNs += ns

  proc addStorageBtreeInsertNs*(ns: int64) {.inline.} =
    gInsertBreakdown.storageBtreeInsertNs += ns

  proc addStorageTableMetaNs*(ns: int64) {.inline.} =
    gInsertBreakdown.storageTableMetaNs += ns

  proc addBtreeTotalNs*(ns: int64) {.inline.} =
    gInsertBreakdown.btreeTotalNs += ns
    gInsertBreakdown.btreeCalls.inc

  proc addPagerUpgradeNs*(ns: int64) {.inline.} =
    gInsertBreakdown.pagerUpgradeTotalNs += ns
    gInsertBreakdown.pagerUpgradeCalls.inc

  func nsPerCall(ns: int64, calls: int64): float64 {.inline.} =
    if calls <= 0: 0.0 else: float64(ns) / float64(calls)

  func pct(part: int64, total: int64): float64 {.inline.} =
    if total <= 0: 0.0 else: (float64(part) * 100.0) / float64(total)

  proc formatInsertBenchBreakdown*(): string =
    let b = gInsertBreakdown
    let engCalls = b.engineCalls
    let storCalls = b.storageCalls
    let btreeCalls = b.btreeCalls

    result.add("Insert breakdown (bench_breakdown)\n")
    result.add("  engine:  total=" & $b.engineTotalNs & "ns calls=" & $engCalls & " avg=" & $(nsPerCall(b.engineTotalNs, engCalls)) & "ns\n")
    result.add("    evalValues:   " & $b.engineEvalValuesNs & "ns (" & $(pct(b.engineEvalValuesNs, b.engineTotalNs)) & "%)\n")
    result.add("    typeCheck:    " & $b.engineTypeCheckNs & "ns (" & $(pct(b.engineTypeCheckNs, b.engineTotalNs)) & "%)\n")
    result.add("    constraints:  " & $b.engineConstraintsNs & "ns (" & $(pct(b.engineConstraintsNs, b.engineTotalNs)) & "%)\n")
    result.add("    storage call: " & $b.engineStorageCallNs & "ns (" & $(pct(b.engineStorageCallNs, b.engineTotalNs)) & "%)\n")

    result.add("  storage: total=" & $b.storageTotalNs & "ns calls=" & $storCalls & " avg=" & $(nsPerCall(b.storageTotalNs, storCalls)) & "ns\n")
    result.add("    normalize:    " & $b.storageNormalizeNs & "ns (" & $(pct(b.storageNormalizeNs, b.storageTotalNs)) & "%)\n")
    result.add("    encodeRecord: " & $b.storageEncodeRecordNs & "ns (" & $(pct(b.storageEncodeRecordNs, b.storageTotalNs)) & "%)\n")
    result.add("    btree insert: " & $b.storageBtreeInsertNs & "ns (" & $(pct(b.storageBtreeInsertNs, b.storageTotalNs)) & "%)\n")
    result.add("    table meta:   " & $b.storageTableMetaNs & "ns (" & $(pct(b.storageTableMetaNs, b.storageTotalNs)) & "%)\n")

    result.add("  btree:   total=" & $b.btreeTotalNs & "ns calls=" & $btreeCalls & " avg=" & $(nsPerCall(b.btreeTotalNs, btreeCalls)) & "ns\n")
    result.add("  pager.upgradeToRw: total=" & $b.pagerUpgradeTotalNs & "ns calls=" & $b.pagerUpgradeCalls & " avg=" & $(nsPerCall(b.pagerUpgradeTotalNs, b.pagerUpgradeCalls)) & "ns\n")
else:
  # Stubs when instrumentation is disabled.
  type InsertBenchBreakdown* = object

  proc resetInsertBenchBreakdown*() {.inline.} = discard
  proc snapshotInsertBenchBreakdown*(): InsertBenchBreakdown {.inline.} = default(InsertBenchBreakdown)
  proc formatInsertBenchBreakdown*(): string = ""
  proc addEngineTotalNs*(ns: int64) {.inline.} = discard
  proc addEngineEvalValuesNs*(ns: int64) {.inline.} = discard
  proc addEngineTypeCheckNs*(ns: int64) {.inline.} = discard
  proc addEngineConstraintsNs*(ns: int64) {.inline.} = discard
  proc addEngineStorageCallNs*(ns: int64) {.inline.} = discard
  proc addStorageTotalNs*(ns: int64) {.inline.} = discard
  proc addStorageNormalizeNs*(ns: int64) {.inline.} = discard
  proc addStorageEncodeRecordNs*(ns: int64) {.inline.} = discard
  proc addStorageBtreeInsertNs*(ns: int64) {.inline.} = discard
  proc addStorageTableMetaNs*(ns: int64) {.inline.} = discard
  proc addBtreeTotalNs*(ns: int64) {.inline.} = discard
  proc addPagerUpgradeNs*(ns: int64) {.inline.} = discard
