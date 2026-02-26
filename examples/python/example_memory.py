"""Example: DecentDB in-memory database with Python DB-API 2.0 interface.

Demonstrates using :memory: for ephemeral databases — no files created on disk.

Build the native library first:
    nim c -d:release --mm:arc --threads:on --app:lib --out:libdecentdb.so src/c_api.nim

Then run:
    DECENTDB_LIB_PATH=/path/to/libdecentdb.so python example_memory.py
"""

import decentdb


def main():
    # Open an in-memory database — no file is created on disk.
    conn = decentdb.connect(":memory:")
    cursor = conn.cursor()

    # Create a table.
    cursor.execute("""
        CREATE TABLE users (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            email TEXT UNIQUE
        )
    """)

    # Insert rows using positional parameters (?-style, auto-rewritten to $N).
    users = [
        ("Alice", "alice@example.com"),
        ("Bob", "bob@example.com"),
        ("Carol", "carol@example.com"),
    ]
    cursor.executemany(
        "INSERT INTO users (name, email) VALUES (?, ?)",
        users,
    )

    # Query all users.
    cursor.execute("SELECT id, name, email FROM users ORDER BY id")
    print("All users:")
    for row in cursor.fetchall():
        print(f"  id={row[0]}  name={row[1]}  email={row[2]}")

    # Parameterised lookup.
    cursor.execute("SELECT name FROM users WHERE email = ?", ("bob@example.com",))
    row = cursor.fetchone()
    print(f"\nLookup by email: {row[0]}")

    # Transaction example.
    cursor.execute("BEGIN")
    cursor.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Dave", "dave@example.com"))
    conn.commit()

    cursor.execute("SELECT count(*) FROM users")
    count = cursor.fetchone()[0]
    print(f"\nTotal users after transaction: {count}")

    # ── Window Functions ──
    cursor.execute("""
        CREATE TABLE scores (
            id    INTEGER PRIMARY KEY,
            name  TEXT NOT NULL,
            dept  TEXT NOT NULL,
            score INTEGER NOT NULL
        )
    """)
    for name, dept, score in [
        ("Alice", "eng", 95), ("Bob", "eng", 95),
        ("Carol", "eng", 80), ("Dave", "sales", 90),
        ("Eve", "sales", 85),
    ]:
        cursor.execute(
            "INSERT INTO scores (name, dept, score) VALUES (?, ?, ?)",
            (name, dept, score),
        )

    print("\n── Window Functions ──")

    # ROW_NUMBER
    cursor.execute("""
        SELECT name, dept, score,
               ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn
        FROM scores ORDER BY dept, score DESC
    """)
    print("\nROW_NUMBER (ranking within department):")
    for row in cursor.fetchall():
        print(f"  {row[0]:6s}  dept={row[1]:5s}  score={row[2]}  rn={row[3]}")

    # RANK (ties get same rank, with gaps)
    cursor.execute("""
        SELECT name, score,
               RANK() OVER (ORDER BY score DESC) AS rank
        FROM scores ORDER BY score DESC, name
    """)
    print("\nRANK (global, with gaps for ties):")
    for row in cursor.fetchall():
        print(f"  {row[0]:6s}  score={row[1]}  rank={row[2]}")

    # DENSE_RANK (ties get same rank, no gaps)
    cursor.execute("""
        SELECT name, score,
               DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank
        FROM scores ORDER BY score DESC, name
    """)
    print("\nDENSE_RANK (no gaps):")
    for row in cursor.fetchall():
        print(f"  {row[0]:6s}  score={row[1]}  dense_rank={row[2]}")

    # LAG (previous row's score)
    cursor.execute("""
        SELECT name, score,
               LAG(score, 1, 0) OVER (ORDER BY score DESC) AS prev_score
        FROM scores ORDER BY score DESC
    """)
    print("\nLAG (previous score, default 0):")
    for row in cursor.fetchall():
        print(f"  {row[0]:6s}  score={row[1]}  prev_score={row[2]}")

    # LEAD (next row's score)
    cursor.execute("""
        SELECT name, score,
               LEAD(score) OVER (PARTITION BY dept ORDER BY score DESC) AS next_score
        FROM scores ORDER BY dept, score DESC
    """)
    print("\nLEAD (next score in dept, NULL at end):")
    for row in cursor.fetchall():
        print(f"  {row[0]:6s}  score={row[1]}  next_score={row[2]}")

    # Schema introspection.
    tables = conn.list_tables()
    print(f"\nTables: {tables}")

    columns = conn.get_table_columns("users")
    print("Columns:")
    for col in columns:
        print(f"  {col['name']} ({col['type']})"
              f"{'  PK' if col.get('primary_key') else ''}"
              f"{'  NOT NULL' if col.get('not_null') else ''}"
              f"{'  UNIQUE' if col.get('unique') else ''}")

    cursor.close()
    conn.close()

    # No cleanup needed — in-memory database is automatically discarded.
    print("\nDone.")


if __name__ == "__main__":
    main()
