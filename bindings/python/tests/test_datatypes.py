import pytest
import decentdb
import uuid
import os

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_datatypes.db")

def test_bool(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_bool (b BOOL)")
    
    cur.execute("INSERT INTO t_bool VALUES (?)", (True,))
    cur.execute("INSERT INTO t_bool VALUES (?)", (False,))
    
    cur.execute("SELECT b FROM t_bool")
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0][0] is True
    assert rows[1][0] is False
    
    conn.close()

def test_uuid(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_uuid (u UUID)")
    
    u1 = uuid.uuid4()
    u2 = uuid.uuid4()
    
    cur.execute("INSERT INTO t_uuid VALUES (?)", (u1,))
    cur.execute("INSERT INTO t_uuid VALUES (?)", (u2,))
    
    cur.execute("SELECT u FROM t_uuid")
    rows = cur.fetchall()
    assert len(rows) == 2
    
    # DecentDB returns UUIDs as bytes/blob by default since there's no native UUID type on read-back yet
    # unless we implemented special handling in row_view/fetch?
    # Let's check what we get.
    r1 = rows[0][0]
    r2 = rows[1][0]
    
    assert isinstance(r1, bytes)
    assert len(r1) == 16
    assert r1 == u1.bytes
    
    assert isinstance(r2, bytes)
    assert len(r2) == 16
    assert r2 == u2.bytes
    
    conn.close()

def test_blob(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_blob (id INT64, data BLOB)")

    blobs = [
        b'',
        b'\x00',
        b'\xDE\xAD\xBE\xEF',
        bytes(range(256)),
    ]

    for i, b in enumerate(blobs):
        cur.execute("INSERT INTO t_blob VALUES (?, ?)", (i, b))

    cur.execute("SELECT data FROM t_blob ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == len(blobs)

    for i, expected in enumerate(blobs):
        assert rows[i][0] == expected, f"blob[{i}] mismatch"

    conn.close()

def test_float64(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_float (id INT64, v FLOAT64)")

    values = [0.0, 1.0, -1.0, 3.141592653589793, 1.7976931348623157e+308, 5e-324]
    for i, v in enumerate(values):
        cur.execute("INSERT INTO t_float VALUES (?, ?)", (i, v))

    cur.execute("SELECT v FROM t_float ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == len(values)

    for i, expected in enumerate(values):
        assert rows[i][0] == expected, f"float[{i}]: expected {expected}, got {rows[i][0]}"

    conn.close()

def test_null(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t_null (id INT64, i INT64, t TEXT, b BOOL, f FLOAT64)")

    cur.execute("INSERT INTO t_null VALUES (?, ?, ?, ?, ?)", (1, None, None, None, None))

    cur.execute("SELECT i, t, b, f FROM t_null WHERE id = 1")
    row = cur.fetchone()
    assert row[0] is None
    assert row[1] is None
    assert row[2] is None
    assert row[3] is None

    conn.close()
