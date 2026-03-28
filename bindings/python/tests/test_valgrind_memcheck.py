"""Valgrind-friendly connection churn regression.

This exercises repeated open/query/close cycles without asserting on process
RSS. Under Memcheck, the leak detector itself is the oracle.
"""

import gc

import decentdb


def test_repeated_open_close_churn_is_correct(tmp_path):
    db_path = str(tmp_path / "valgrind_memcheck.ddb")
    payload = "x" * 256
    row_count = 128

    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE churn (id INT64 PRIMARY KEY, payload TEXT)")
    for i in range(row_count):
        cur.execute("INSERT INTO churn VALUES (?, ?)", (i, payload))
    conn.commit()
    conn.close()

    for iteration in range(200):
        conn = decentdb.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, payload FROM churn ORDER BY id")
        rows = cur.fetchall()
        conn.close()
        del cur
        del conn

        assert len(rows) == row_count
        assert rows[0] == (0, payload)
        assert rows[-1] == (row_count - 1, payload)

        if (iteration + 1) % 25 == 0:
            gc.collect()
