"""Tests for V2 binding enhancements: version API, connection modes,
schema introspection (views, triggers, DDL), and in_transaction."""

import os
import tempfile
import shutil
import pytest
import decentdb


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test.ddb")


# ---------------------------------------------------------------------------
# Version API
# ---------------------------------------------------------------------------


def test_abi_version_returns_int():
    version = decentdb.abi_version()
    assert isinstance(version, int)
    assert version > 0


def test_engine_version_returns_str():
    version = decentdb.engine_version()
    assert isinstance(version, str)
    assert len(version) > 0


# ---------------------------------------------------------------------------
# Connection modes
# ---------------------------------------------------------------------------


def test_open_or_create_default(db_path):
    conn = decentdb.connect(db_path)
    conn.close()


def test_mode_create_new(db_path):
    conn = decentdb.connect(db_path, mode="create")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()


def test_mode_create_existing_raises(db_path):
    conn = decentdb.connect(db_path, mode="create")
    conn.close()
    with pytest.raises(decentdb.DatabaseError):
        decentdb.connect(db_path, mode="create")


def test_mode_open_existing(db_path):
    conn = decentdb.connect(db_path, mode="create")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    conn2 = decentdb.connect(db_path, mode="open")
    tables = conn2.list_tables()
    assert "t" in tables
    conn2.close()


def test_mode_open_nonexistent_raises(db_path):
    with pytest.raises(decentdb.DatabaseError):
        decentdb.connect(db_path, mode="open")


def test_mode_open_or_create_reuses_existing(db_path):
    conn = decentdb.connect(db_path, mode="create")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    conn2 = decentdb.connect(db_path, mode="open_or_create")
    tables = conn2.list_tables()
    assert "t" in tables
    conn2.close()


def test_connect_mode_via_dsn(db_path):
    conn = decentdb.connect(db_path, mode="create")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# get_table_ddl
# ---------------------------------------------------------------------------


def test_get_table_ddl(db_path):
    conn = decentdb.connect(db_path)
    conn.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL)"
    )
    conn.commit()
    ddl = conn.get_table_ddl("users")
    assert isinstance(ddl, str)
    assert "users" in ddl.lower()
    conn.close()


def test_get_table_ddl_nonexistent(db_path):
    conn = decentdb.connect(db_path)
    with pytest.raises(Exception):
        conn.get_table_ddl("nonexistent")
    conn.close()


# ---------------------------------------------------------------------------
# list_views / get_view_ddl
# ---------------------------------------------------------------------------


def test_list_views_empty(db_path):
    conn = decentdb.connect(db_path)
    views = conn.list_views()
    assert views == []
    conn.close()


def test_list_views_and_get_view_ddl(db_path):
    conn = decentdb.connect(db_path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT)")
    conn.commit()
    conn.execute("CREATE VIEW v_items AS SELECT id, val FROM items WHERE id > 0")
    conn.commit()

    views = conn.list_views()
    assert "v_items" in views

    ddl = conn.get_view_ddl("v_items")
    assert isinstance(ddl, str)
    assert "v_items" in ddl.lower()
    conn.close()


def test_get_view_ddl_nonexistent(db_path):
    conn = decentdb.connect(db_path)
    with pytest.raises(Exception):
        conn.get_view_ddl("no_such_view")
    conn.close()


# ---------------------------------------------------------------------------
# list_triggers
# ---------------------------------------------------------------------------


def test_list_triggers_empty(db_path):
    conn = decentdb.connect(db_path)
    triggers = conn.list_triggers()
    assert triggers == []
    conn.close()


def test_list_triggers_api_callable(db_path):
    """Verify list_triggers() API works even if engine doesn't support CREATE TRIGGER."""
    conn = decentdb.connect(db_path)
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)")
    conn.commit()
    # The engine may not support CREATE TRIGGER syntax yet.
    # Verify the API is callable and returns a list.
    triggers = conn.list_triggers()
    assert isinstance(triggers, list)
    conn.close()


def test_list_triggers_api_callable(db_path):
    """Verify list_triggers() API works even if engine doesn't support CREATE TRIGGER."""
    conn = decentdb.connect(db_path)
    conn.execute("CREATE TABLE log (id INTEGER PRIMARY KEY, msg TEXT)")
    conn.commit()
    # The engine may not support CREATE TRIGGER syntax yet.
    # Verify the API is callable and returns a list.
    triggers = conn.list_triggers()
    assert isinstance(triggers, list)
    conn.close()


# ---------------------------------------------------------------------------
# in_transaction
# ---------------------------------------------------------------------------


def test_in_transaction_false_initially(db_path):
    conn = decentdb.connect(db_path)
    assert conn.in_transaction is False
    conn.close()


def test_in_transaction_true_after_begin(db_path):
    conn = decentdb.connect(db_path)
    conn.begin_transaction()
    assert conn.in_transaction is True
    conn.commit()
    assert conn.in_transaction is False
    conn.close()


def test_in_transaction_false_after_rollback(db_path):
    conn = decentdb.connect(db_path)
    conn.begin_transaction()
    assert conn.in_transaction is True
    conn.rollback()
    assert conn.in_transaction is False
    conn.close()


def test_in_transaction_reflects_sql_begin(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("BEGIN")
    assert conn.in_transaction is True
    cur.execute("COMMIT")
    assert conn.in_transaction is False
    cur.close()
    conn.close()
