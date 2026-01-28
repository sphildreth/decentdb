type Record* = ref object
  bytes*: seq[byte]

proc newRecord*(): Record =
  Record(bytes: @[])
