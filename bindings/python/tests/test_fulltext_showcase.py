import decentdb


def test_fulltext_search_and_bm25_showcase(db_path):
    conn = decentdb.connect(db_path)
    cur = conn.cursor()

    cur.execute("CREATE TABLE docs (id INT64 PRIMARY KEY, title TEXT, body TEXT)")
    cur.execute(
        "CREATE INDEX idx_docs_search ON docs USING fulltext (title, body) "
        "WITH (prefix = '2,3')"
    )
    cur.execute(
        "INSERT INTO docs VALUES (?, ?, ?)",
        (
            1,
            "Embedded database search",
            "DecentDB adds rust database search search primitives.",
        ),
    )
    cur.execute(
        "INSERT INTO docs VALUES (?, ?, ?)",
        (2, "Rust database notes", "Durable local database storage."),
    )
    cur.execute(
        "INSERT INTO docs VALUES (?, ?, ?)",
        (3, "Calendar entry", "Lunch and project planning."),
    )
    conn.commit()

    cur.execute(
        "SELECT id, title, bm25('idx_docs_search') AS rank "
        "FROM docs "
        "WHERE fulltext_match('idx_docs_search', ?) "
        "ORDER BY rank DESC, id",
        ("database OR search",),
    )
    rows = cur.fetchall()

    assert [row[0] for row in rows] == [1, 2]
    assert rows[0][1] == "Embedded database search"
    assert isinstance(rows[0][2], float)
    assert rows[0][2] > rows[1][2]

    cur.execute(
        "SELECT id FROM docs "
        "WHERE fulltext_match('idx_docs_search', ?) "
        "ORDER BY id",
        ("dec*",),
    )
    assert cur.fetchall() == [(1,)]

    conn.close()
