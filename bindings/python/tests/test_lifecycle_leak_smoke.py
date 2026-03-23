import gc
import os

import pytest

import decentdb

psutil = pytest.importorskip("psutil")


def rss_bytes() -> int:
    return psutil.Process(os.getpid()).memory_info().rss


def test_cross_connection_and_error_lifecycle_smoke(tmp_path):
    db_path = str(tmp_path / "lifecycle_smoke.ddb")

    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT64 PRIMARY KEY, v TEXT)")
    conn.commit()
    conn.close()

    gc.collect()
    before = rss_bytes()

    for i in range(240):
        a = decentdb.connect(db_path)
        b = decentdb.connect(db_path)

        ac = a.cursor()
        ac.execute("INSERT INTO t VALUES (?, ?)", (i * 2 + 1, f"a_{i}"))
        a.commit()

        bc = b.cursor()
        bc.execute("INSERT INTO t VALUES (?, ?)", (i * 2 + 2, f"b_{i}"))
        b.commit()

        bc.execute("SELECT COUNT(*) FROM t")
        _ = bc.fetchone()

        with pytest.raises(decentdb.ProgrammingError):
            bc.execute("SELECT * FROM missing_table_lifecycle")

        if i % 2 == 0:
            a.close()
            b.close()
        else:
            b.close()
            a.close()

        if i % 40 == 0:
            gc.collect()

    gc.collect()
    gc.collect()
    after = rss_bytes()

    # Allow allocator noise and cache warmup, but catch unbounded growth.
    assert (after - before) < 14 * 1024 * 1024
