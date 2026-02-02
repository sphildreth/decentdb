import atomics

var
  EvictionAttempts*: Atomic[int64]
  Evictions*: Atomic[int64]
  EvictionBlocked*: Atomic[int64]
  WalGrowthWriter*: Atomic[int64]
  WalGrowthReader*: Atomic[int64]
  CheckpointDurationUs*: Atomic[int64]
  CheckpointWaitUs*: Atomic[int64]
  OverlayHits*: Atomic[int64]
  OverlayMisses*: Atomic[int64]

proc inc*(counter: var Atomic[int64]) {.inline.} =
  discard counter.fetchAdd(1, moRelaxed)

proc add*(counter: var Atomic[int64], val: int64) {.inline.} =
  discard counter.fetchAdd(val, moRelaxed)

proc load*(counter: var Atomic[int64]): int64 {.inline.} =
  counter.load(moRelaxed)
