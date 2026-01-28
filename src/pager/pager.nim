type Pager* = ref object
  pageSize*: int

proc newPager*(): Pager =
  Pager(pageSize: 4096)
