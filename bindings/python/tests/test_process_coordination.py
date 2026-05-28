import os

import decentdb


def test_process_coordination_options_and_sys_views(db_path):
    conn = decentdb.connect(
        db_path,
        process_coordination="required",
        process_coordination_timeout_ms=250,
    )
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE t (id INT64 PRIMARY KEY, value TEXT)")
        cur.execute("INSERT INTO t VALUES (?, ?)", (1, "one"))
        conn.commit()

        cur.execute("SELECT * FROM sys.process_coordination")
        row = cur.fetchone()
        assert row[0] == "required"
        assert row[1] is True
        assert row[2] is True
        assert row[3].endswith(".coord")

        cur.execute("SELECT * FROM sys.process_lock_metrics")
        metrics = cur.fetchone()
        assert metrics[0] >= 0
        assert metrics[8] >= 0
    finally:
        conn.close()

    assert os.path.exists(db_path + ".coord")
