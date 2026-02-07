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
