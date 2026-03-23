import pytest
import os
import tempfile
import decentdb


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.ddb")


def test_save_as_exports_memory_to_disk(tmp_path):
    dest = str(tmp_path / "exported.ddb")
    conn = decentdb.connect(":memory:")
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE foo (id INT64, name TEXT)")
        cur.execute("INSERT INTO foo VALUES (1, 'alice')")
        cur.execute("INSERT INTO foo VALUES (2, 'bob')")
        conn.commit()
        conn.save_as(dest)
    finally:
        conn.close()

    assert os.path.exists(dest)

    conn2 = decentdb.connect(dest)
    try:
        cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM foo ORDER BY id")
        rows = cur2.fetchall()
        assert len(rows) == 2
        assert rows[0] == (1, 'alice')
        assert rows[1] == (2, 'bob')
    finally:
        conn2.close()


def test_save_as_preserves_schema_and_indexes(tmp_path):
    dest = str(tmp_path / "indexed.ddb")
    conn = decentdb.connect(":memory:")
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE items (id INT64, val TEXT)")
        cur.execute("CREATE INDEX idx_items_val ON items (val)")
        cur.execute("INSERT INTO items VALUES (1, 'x')")
        cur.execute("INSERT INTO items VALUES (2, 'y')")
        cur.execute("INSERT INTO items VALUES (3, 'x')")
        conn.commit()
        conn.save_as(dest)
    finally:
        conn.close()

    conn2 = decentdb.connect(dest)
    try:
        cur2 = conn2.cursor()
        cur2.execute("SELECT * FROM items WHERE val = 'x' ORDER BY id")
        rows = cur2.fetchall()
        assert len(rows) == 2
        assert rows[0] == (1, 'x')
        assert rows[1] == (3, 'x')
    finally:
        conn2.close()


def test_save_as_errors_if_dest_exists(tmp_path):
    dest = str(tmp_path / "existing.ddb")

    # Create the destination file so it already exists.
    conn_pre = decentdb.connect(dest)
    conn_pre.close()

    conn = decentdb.connect(":memory:")
    try:
        with pytest.raises(decentdb.Error):
            conn.save_as(dest)
    finally:
        conn.close()


def test_save_as_empty_database(tmp_path):
    dest = str(tmp_path / "empty.ddb")
    conn = decentdb.connect(":memory:")
    try:
        conn.save_as(dest)
    finally:
        conn.close()

    assert os.path.exists(dest)

    conn2 = decentdb.connect(dest)
    try:
        # Just verify it opens successfully — no tables expected.
        cur2 = conn2.cursor()
        cur2.execute("SELECT 1")
        assert cur2.fetchone() == (1,)
    finally:
        conn2.close()
