type SqlAst* = ref object
  raw*: string

proc parseSql*(sql: string): SqlAst =
  SqlAst(raw: sql)
