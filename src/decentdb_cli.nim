import parseopt
import json
import ./engine
import ./errors

proc resultJson(ok: bool, err: DbError = DbError(), rows: seq[string] = @[]): JsonNode =
  let errorNode = if ok: newJNull() else: errorToJson(err)
  %*{
    "ok": ok,
    "error": errorNode,
    "rows": rows
  }

proc main() =
  var dbPath = ""
  var sql = ""
  var openClose = false

  for kind, key, val in getOpt():
    case key
    of "db":
      dbPath = val
    of "sql":
      sql = val
    of "open-close":
      openClose = true
    else:
      discard

  if dbPath.len == 0:
    echo resultJson(false, DbError(code: ERR_IO, message: "Missing --db argument"))
    quit(1)

  let openRes = openDb(dbPath)
  if not openRes.ok:
    echo resultJson(false, openRes.err)
    quit(1)

  let db = openRes.value
  if not openClose and sql.len > 0:
    let execRes = execSql(db, sql)
    if not execRes.ok:
      discard closeDb(db)
      echo resultJson(false, execRes.err)
      quit(1)
    let rows = execRes.value
    discard closeDb(db)
    echo resultJson(true, rows = rows)
    quit(0)

  discard closeDb(db)
  echo resultJson(true)

when isMainModule:
  main()
