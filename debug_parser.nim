import json
import sql/sql

let res = parseSql("BEGIN")
if res.ok:
  echo "Parse OK"
  for stmt in res.value.statements:
    echo "Statement Kind: ", stmt.kind
else:
  echo "Parse Failed: ", res.err.message
  echo "Context: ", res.err.context

let res2 = parseSql("SELECT 1 ORDER BY 1 DESC")
if res2.ok:
  echo "Select OK"
  for stmt in res2.value.statements:
    echo "Statement Kind: ", stmt.kind
    if stmt.orderBy.len > 0:
      echo "OrderBy Asc: ", stmt.orderBy[0].asc
else:
  echo "Select Failed: ", res2.err.message
