type TrigramIndex* = ref object
  columns*: seq[string]

proc newTrigramIndex*(): TrigramIndex =
  TrigramIndex(columns: @[])
