import engine, os
let p = getTempDir()/"debug_cast_col.ddb"
removeFile(p)
let db = openDb(p).value

discard execSql(db, "CREATE TABLE t (id INT PRIMARY KEY, v INT, f REAL, txt TEXT)")
discard execSql(db, "INSERT INTO t VALUES (1, 42, 3.14, '123')")

# CAST from column (should go through castValue at runtime)
var r = execSql(db, "SELECT CAST(v AS TEXT) FROM t")
echo "CAST INT->TEXT: ", r.value

r = execSql(db, "SELECT CAST(f AS INTEGER) FROM t")
echo "CAST REAL->INT: ", r.value

r = execSql(db, "SELECT CAST(txt AS INTEGER) FROM t")
echo "CAST TEXT->INT: ", r.value

r = execSql(db, "SELECT CAST(v AS REAL) FROM t")
echo "CAST INT->REAL: ", r.value

r = execSql(db, "SELECT CAST(f AS BOOL) FROM t")
echo "CAST REAL->BOOL: ", r.value

r = execSql(db, "SELECT CAST(txt AS BOOL) FROM t")
echo "CAST TEXT->BOOL: ", r.value

discard closeDb(db)
