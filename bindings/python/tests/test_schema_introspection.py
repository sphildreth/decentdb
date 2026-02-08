"""Tests for schema introspection and checkpoint APIs."""

import os
import tempfile
import pytest
import decentdb


@pytest.fixture
def db():
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "test.ddb")
    conn = decentdb.connect(path)
    yield conn
    conn.close()
    # cleanup
    import shutil
    shutil.rmtree(tmpdir, ignore_errors=True)


def test_list_tables_empty(db):
    tables = db.list_tables()
    assert tables == []


def test_list_tables_after_create(db):
    db.execute("CREATE TABLE alpha (id INTEGER PRIMARY KEY)")
    db.execute("CREATE TABLE beta (id INTEGER PRIMARY KEY, name TEXT)")
    db.commit()
    tables = db.list_tables()
    assert sorted(tables) == ["alpha", "beta"]


def test_get_table_columns(db):
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)")
    db.commit()
    cols = db.get_table_columns("users")
    assert len(cols) == 3
    assert cols[0]["name"] == "id"
    assert cols[0]["primary_key"] is True
    assert cols[1]["name"] == "name"
    assert cols[1]["not_null"] is True
    assert cols[2]["name"] == "email"


def test_list_indexes(db):
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
    db.execute("CREATE INDEX idx_items_name ON items (name)")
    db.commit()
    indexes = db.list_indexes()
    assert len(indexes) >= 1
    names = [idx["name"] for idx in indexes]
    assert "idx_items_name" in names


def test_checkpoint(db):
    db.execute("CREATE TABLE chk (id INTEGER PRIMARY KEY, v TEXT)")
    db.execute("INSERT INTO chk (v) VALUES ($1)", ["hello"])
    db.commit()
    # Should not raise
    db.checkpoint()


def test_auto_increment_insert(db):
    db.execute("CREATE TABLE auto (id INTEGER PRIMARY KEY, val TEXT)")
    db.execute("INSERT INTO auto (val) VALUES ($1)", ["a"])
    db.execute("INSERT INTO auto (val) VALUES ($1)", ["b"])
    db.commit()
    cur = db.execute("SELECT id, val FROM auto ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0][0] < rows[1][0], "auto-increment IDs should increase"


def test_auto_increment_with_explicit_id(db):
    db.execute("CREATE TABLE auto2 (id INTEGER PRIMARY KEY, val TEXT)")
    db.execute("INSERT INTO auto2 (id, val) VALUES ($1, $2)", [100, "x"])
    db.execute("INSERT INTO auto2 (val) VALUES ($1)", ["y"])
    db.commit()
    cur = db.execute("SELECT id, val FROM auto2 ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0] == (100, "x")
    assert rows[1][0] > 0  # auto-assigned
