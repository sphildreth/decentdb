import sqlite3

import pytest

import decentdb
from decentdb.tools.sqlite_import import convert_sqlite_to_decentdb, write_report_json


def _make_sqlite_source(path: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("PRAGMA foreign_keys=ON")

        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id), payload BLOB)"
        )
        conn.execute("CREATE INDEX idx_child_parent_id ON child(parent_id)")

        # UNIQUE constraint should show up as sqlite_autoindex_* and be translated to a DecentDB column UNIQUE.
        conn.execute("CREATE TABLE flags (id INTEGER PRIMARY KEY, ok BOOLEAN NOT NULL UNIQUE)")

        conn.execute("INSERT INTO parent VALUES (1, 'p1')")
        conn.execute("INSERT INTO parent VALUES (2, 'p2')")

        conn.execute("INSERT INTO child VALUES (10, 1, ?)", (sqlite3.Binary(b"\x00\x01\x02"),))
        conn.execute("INSERT INTO child VALUES (11, 2, ?)", (sqlite3.Binary(b"hello"),))

        # Store bool-like values as ints; importer should adapt 0/1 -> False/True for BOOL.
        conn.execute("INSERT INTO flags VALUES (1, 0)")
        conn.execute("INSERT INTO flags VALUES (2, 1)")

        conn.commit()
    finally:
        conn.close()


def test_sqlite_to_decentdb_convert(tmp_path):
    sqlite_path = str(tmp_path / "src.sqlite")
    decent_path = str(tmp_path / "dst.decentdb")

    _make_sqlite_source(sqlite_path)

    report = convert_sqlite_to_decentdb(
        sqlite_path=sqlite_path,
        decentdb_path=decent_path,
        overwrite=False,
        show_progress=False,
    )

    assert report.sqlite_path == sqlite_path
    assert report.decentdb_path == decent_path
    assert set(report.tables) == {"parent", "child", "flags"}
    assert report.rows_copied.get("parent") == 2
    assert report.rows_copied.get("child") == 2
    assert report.rows_copied.get("flags") == 2

    report_path = str(tmp_path / "report.json")
    write_report_json(report, report_path)
    assert (tmp_path / "report.json").exists()

    conn = decentdb.connect(decent_path)
    try:
        # Basic sanity: tables exist
        listed = conn.list_tables()
        if listed and isinstance(listed[0], dict):
            names = {t["name"] for t in listed}
        else:
            names = {str(t) for t in listed}
        assert {"parent", "child", "flags"}.issubset(names)

        # Row counts preserved
        assert conn.execute("SELECT COUNT(*) FROM parent").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM child").fetchone()[0] == 2
        assert conn.execute("SELECT COUNT(*) FROM flags").fetchone()[0] == 2

        # Types: blob preserved
        rows = conn.execute("SELECT id, payload FROM child ORDER BY id").fetchall()
        assert rows[0][1] == b"\x00\x01\x02"
        assert rows[1][1] == b"hello"

        # Types: bool mapped
        flags = conn.execute("SELECT id, ok FROM flags ORDER BY id").fetchall()
        assert flags == [(1, False), (2, True)]

        # Constraints: FK should be enforced (statement-time)
        with pytest.raises(decentdb.IntegrityError):
            conn.execute("INSERT INTO child (id, parent_id, payload) VALUES (?, ?, ?)", (99, 999, b"x"))

        # Constraints: UNIQUE index should be enforced
        with pytest.raises(decentdb.IntegrityError):
            conn.execute("INSERT INTO flags (id, ok) VALUES (?, ?)", (3, True))

    finally:
        conn.close()
