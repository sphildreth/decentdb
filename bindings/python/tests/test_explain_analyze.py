import pytest
import decentdb


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_explain_analyze.ddb")


def test_explain_analyze_returns_actual_metrics(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT64, name TEXT)")
    cur.execute("INSERT INTO t VALUES (1, 'Alice')")
    cur.execute("INSERT INTO t VALUES (2, 'Bob')")
    cur.execute("INSERT INTO t VALUES (3, 'Charlie')")
    conn.commit()

    cur.execute("EXPLAIN ANALYZE SELECT * FROM t")
    rows = cur.fetchall()
    assert len(rows) > 0

    plan_text = "\n".join(str(r[0]) for r in rows)
    assert "Project" in plan_text
    assert "Actual Rows: 3" in plan_text
    assert "Actual Time:" in plan_text
    assert "ms" in plan_text
    conn.close()


def test_explain_without_analyze_no_metrics(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT64)")
    conn.commit()

    cur.execute("EXPLAIN SELECT * FROM t")
    rows = cur.fetchall()
    plan_text = "\n".join(str(r[0]) for r in rows)
    assert "Project" in plan_text
    assert "Actual Rows:" not in plan_text
    assert "Actual Time:" not in plan_text
    conn.close()


def test_explain_analyze_empty_table(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT64)")
    conn.commit()

    cur.execute("EXPLAIN ANALYZE SELECT * FROM t")
    rows = cur.fetchall()
    plan_text = "\n".join(str(r[0]) for r in rows)
    assert "Actual Rows: 0" in plan_text
    conn.close()


def test_explain_analyze_with_filter(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INT64)")
    for i in range(1, 11):
        cur.execute("INSERT INTO t VALUES (?)", (i,))
    conn.commit()

    cur.execute("EXPLAIN ANALYZE SELECT * FROM t WHERE id > 5")
    rows = cur.fetchall()
    plan_text = "\n".join(str(r[0]) for r in rows)
    assert "Actual Rows: 5" in plan_text
    conn.close()
