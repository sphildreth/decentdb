import pytest
import os
import decentdb
import time

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.ddb")

def test_connect(db_path):
    conn = decentdb.connect(db_path)
    assert conn is not None
    conn.close()

def test_ddl_and_insert(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    
    cur.execute("CREATE TABLE foo (id INT64, name TEXT)")
    cur.execute("INSERT INTO foo VALUES (1, 'alice')")
    cur.execute("INSERT INTO foo VALUES (2, 'bob')")
    
    conn.commit()
    conn.close()
    
    # Reopen and verify
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM foo ORDER BY id")
    rows = cur.fetchall()
    
    assert len(rows) == 2
    assert rows[0] == (1, 'alice')
    assert rows[1] == (2, 'bob')
    
    conn.close()

def test_parameters(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
    
    # Positional args (qmark style - rewritten by driver)
    cur.execute("INSERT INTO foo VALUES (?, ?)", (1, "a"))
    
    # Named args
    cur.execute("INSERT INTO foo VALUES (:id, :val)", {"id": 2, "val": "b"})
    
    conn.commit()
    
    cur.execute("SELECT * FROM foo WHERE id = ?", (1,))
    row = cur.fetchone()
    assert row == (1, "a")
    
    cur.execute("SELECT * FROM foo WHERE id = :target", {"target": 2})
    row = cur.fetchone()
    assert row == (2, "b")
    
    conn.close()

def test_parameters_reject_mixed_styles(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
    conn.commit()

    # Named params + qmark placeholders
    with pytest.raises(decentdb.ProgrammingError):
        cur.execute("SELECT * FROM foo WHERE id = ? AND val = :val", {"val": "x"})

    # Positional params + named placeholders
    with pytest.raises(decentdb.ProgrammingError):
        cur.execute("SELECT * FROM foo WHERE id = :id", (1,))

    conn.close()

def test_parameters_named_reuse(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE foo (id INT64, val TEXT)")
    cur.execute("INSERT INTO foo VALUES (1, 'a')")
    cur.execute("INSERT INTO foo VALUES (2, 'b')")
    conn.commit()

    # The same named parameter appears multiple times; it should map to one $N.
    cur.execute("SELECT id FROM foo WHERE id = :target OR id = :target ORDER BY id", {"target": 2})
    rows = cur.fetchall()
    assert rows == [(2,)]

    conn.close()

def test_fetchmany(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE foo (id INT64)")
    
    for i in range(10):
        cur.execute("INSERT INTO foo VALUES (?)", (i,))
    conn.commit()
    
    cur.execute("SELECT * FROM foo ORDER BY id")
    batch = cur.fetchmany(3)
    assert len(batch) == 3
    assert batch[0][0] == 0
    
    batch = cur.fetchmany(3)
    assert len(batch) == 3
    assert batch[0][0] == 3
    
    batch = cur.fetchmany(5) # Remaining 4
    assert len(batch) == 4
    
    conn.close()

def test_types(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE types (i INT64, f FLOAT64, t TEXT, b BLOB, bool BOOL, n TEXT)")
    
    blob_data = b'\x00\x01\x02'
    cur.execute("INSERT INTO types VALUES (?, ?, ?, ?, ?, ?)", 
                (123, 1.23, "hello", blob_data, True, None))
    conn.commit()
    
    cur.execute("SELECT * FROM types")
    row = cur.fetchone()
    
    print(f"Row: {row!r}")
    
    assert row[0] == 123
    assert isinstance(row[1], float)
    assert abs(row[1] - 1.23) < 0.0001
    assert row[2] == "hello"
    assert row[3] == blob_data
    assert row[4] is True
    assert row[5] is None
    
    conn.close()

def test_error_includes_sql_and_code(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()

    with pytest.raises(decentdb.ProgrammingError) as excinfo:
        cur.execute("SELEC 1")

    msg = str(excinfo.value)
    assert "Context:" in msg
    assert "native_code" in msg
    assert "\"sql\":" in msg

    conn.close()
