"""Example: DateTime (TIMESTAMP) usage with the Python DB-API 2.0 interface.

Build the native library first:
    nim c -d:release --mm:arc --threads:on --app:lib --out:libdecentdb.so src/c_api.nim

Then run:
    DECENTDB_LIB_PATH=/path/to/libdecentdb.so python example_datetime.py
"""

import os
import tempfile
import datetime
import decentdb


def main():
    db_path = os.path.join(tempfile.gettempdir(), "decentdb_datetime_example.ddb")
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = decentdb.connect(db_path)
    cursor = conn.cursor()

    # Create a table with a TIMESTAMP column.
    cursor.execute("""
        CREATE TABLE events (
            id         INTEGER PRIMARY KEY,
            name       TEXT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
        )
    """)
    conn.commit()

    # Insert rows using ISO-8601 string literals.
    cursor.execute(
        "INSERT INTO events VALUES (1, 'Launch', '2024-06-15 09:00:00', NULL)"
    )
    cursor.execute(
        "INSERT INTO events VALUES (2, 'Update', '2024-06-15 10:30:00', '2024-07-01 08:00:00')"
    )

    # Insert a row using a Python datetime.datetime parameter.
    dt = datetime.datetime(2024, 8, 20, 14, 0, 0, tzinfo=datetime.timezone.utc)
    cursor.execute(
        "INSERT INTO events VALUES (3, 'Release', ?, ?)",
        (dt, datetime.datetime.now(datetime.timezone.utc)),
    )
    conn.commit()

    # Query and display results — TIMESTAMP columns are returned as datetime.datetime.
    cursor.execute("SELECT id, name, created_at, updated_at FROM events ORDER BY created_at")
    rows = cursor.fetchall()
    print("Events ordered by created_at:")
    for row in rows:
        print(f"  id={row[0]}  name={row[1]!r:10}  created_at={row[2]}  updated_at={row[3]}")

    # Use NOW() to get the current time as a TIMESTAMP.
    cursor.execute("SELECT NOW()")
    (now_val,) = cursor.fetchone()
    print(f"\nNOW() = {now_val}  (type: {type(now_val).__name__})")

    # Use EXTRACT to pull year/month/day from a TIMESTAMP column.
    cursor.execute("""
        SELECT name,
               EXTRACT(YEAR  FROM created_at),
               EXTRACT(MONTH FROM created_at),
               EXTRACT(DAY   FROM created_at)
        FROM events
    """)
    print("\nEXTRACT year/month/day:")
    for row in cursor.fetchall():
        print(f"  {row[0]:10}  {int(row[1])}-{int(row[2]):02d}-{int(row[3]):02d}")

    # ORDER BY works on TIMESTAMP columns.
    cursor.execute("SELECT name, created_at FROM events ORDER BY created_at DESC LIMIT 1")
    (name, latest) = cursor.fetchone()
    print(f"\nLatest event: {name!r} at {latest}")

    conn.close()
    os.remove(db_path)
    print("\nDone.")


if __name__ == "__main__":
    main()
