type BTree* = ref object
  rootPage*: int

proc newBTree*(): BTree =
  BTree(rootPage: 0)
