import src/engine
import src/errors

let dbRes = openDb("/tmp/bench_data_profile_3/bench_decentdb_read.ddb")
if not dbRes.ok:
  quit("Failed to open DB: " & dbRes.err.message)
let db = dbRes.value

let res = execSql(db, "EXPLAIN SELECT * FROM users WHERE id = 1")
if not res.ok:
  quit("Explain failed: " & res.err.message)

for line in res.value:
  echo line
