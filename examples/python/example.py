"""Example: Basic DecentDB usage with Python DB-API 2.0 interface.

Build the native library first:
    nim c -d:release --mm:arc --threads:on --app:lib --out:libdecentdb.so src/c_api.nim

Then run:
    DECENTDB_LIB_PATH=/path/to/libdecentdb.so python example.py
"""

import os
import tempfile
import decentdb


def main():
    # Create a temporary database file for this example.
    db_path = os.path.join(tempfile.gettempdir(), "decentdb_example.ddb")

    conn = decentdb.connect(db_path)
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

    # Clean up.
    for suffix in ("", "-wal"):
        try:
            os.unlink(db_path + suffix)
        except FileNotFoundError:
            pass

    print("\nDone.")


if __name__ == "__main__":
    main()
