type Catalog* = ref object
  schemaCookie*: int

proc newCatalog*(): Catalog =
  Catalog(schemaCookie: 0)
