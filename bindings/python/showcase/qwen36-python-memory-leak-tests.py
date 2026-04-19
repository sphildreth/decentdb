#!/usr/bin/env python3
"""
DecentDB Python Bindings - Memory Leak Showcase

A comprehensive stress-test script that exercises the DecentDB engine core
and Python bindings through repeated open/close cycles, schema churn, bulk
inserts, indexed queries, index/table drops, and connection lifecycle
management. The goal is to surface memory leaks in either the native engine
or the Python binding layer.

Usage:
    python qwen36-python-memory-leak-tests.py [--iterations N] [--records N] [--db-dir DIR] [--strict]

The script tracks:
  - Process RSS memory at every phase boundary
  - Storage state (WAL pages, page cache, file sizes)
  - Timing for each operation
  - Delta memory per iteration to detect drift

Exit code 0 = no leak detected (within thresholds)
Exit code 1 = leak detected (memory grew beyond allowed delta)
"""

import argparse
import datetime
import gc
import os
import random
import shutil
import sys
import tempfile
import time
import uuid

import decentdb
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.columns import Columns
from rich.text import Text

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_memory_mb():
    """Return current process RSS in MB."""
    try:
        import psutil
        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        pass
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        pass
    return 0.0


def format_mem(mb):
    if mb >= 1024:
        return f"{mb/1024:.2f} GB"
    return f"{mb:.2f} MB"


def db_artifact_paths(db_path):
    return [
        db_path,
        db_path + ".wal",
        db_path + ".shm",
        db_path + "-wal",
        db_path + "-shm",
        db_path + "-lock",
    ]


def total_db_artifact_size_mb(db_path):
    total = 0
    for p in db_artifact_paths(db_path):
        try:
            total += os.path.getsize(p)
        except OSError:
            pass
    return total / 1024 / 1024


def rand_str(prefix="", length=10):
    return prefix + "".join(
        random.choice("abcdefghijklmnopqrstuvwxyz0123456789")
        for _ in range(length)
    )


def rand_email():
    domains = ["gmail.com", "yahoo.com", "outlook.com", "company.io"]
    return f"{rand_str(length=random.randint(5, 12))}@{random.choice(domains)}"


def rand_datetime(start_year=2020, end_year=2025):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    dt = start + datetime.timedelta(days=random.randint(0, (end - start).days))
    return datetime.datetime(dt.year, dt.month, dt.day)


def cleanup_db(db_path):
    for p in db_artifact_paths(db_path):
        try:
            os.remove(p)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Memory snapshot tracker
# ---------------------------------------------------------------------------

class MemoryTracker:
    def __init__(self):
        self.snapshots = []
        self.baseline_mb = None

    def snapshot(self, label):
        gc.collect()
        mem = get_memory_mb()
        if self.baseline_mb is None:
            self.baseline_mb = mem
        delta = mem - self.baseline_mb
        entry = {
            "label": label,
            "rss_mb": mem,
            "delta_mb": delta,
            "ts": time.perf_counter(),
        }
        self.snapshots.append(entry)
        return entry

    @property
    def peak_delta(self):
        if not self.snapshots:
            return 0.0
        return max(s["delta_mb"] for s in self.snapshots)

    @property
    def final_delta(self):
        if not self.snapshots:
            return 0.0
        return self.snapshots[-1]["delta_mb"]


# ---------------------------------------------------------------------------
# Phase runners
# ---------------------------------------------------------------------------

def phase_open_close_cycle(tracker, db_path, iteration):
    """Open and immediately close a connection. Repeated cycles expose
    leaks in the FFI layer, connection pooling, and native handle cleanup."""
    label = f"open_close_cycle_{iteration}"
    conn = decentdb.connect(db_path, mode="open_or_create")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()
    conn.close()
    return tracker.snapshot(label)


def phase_create_schema(tracker, db_path):
    """Create a set of tables with various column types."""
    conn = decentdb.connect(db_path, mode="create")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE users (
            id INT64 PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            score FLOAT64,
            active BOOL,
            created_at TIMESTAMP,
            notes TEXT,
            payload BLOB,
            balance DECIMAL(10,2),
            uid UUID
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            id INT64 PRIMARY KEY,
            user_id INT64 NOT NULL,
            total DECIMAL(10,2),
            status TEXT,
            placed_at TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE tags (
            id INT64 PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE user_tags (
            user_id INT64,
            tag_id INT64,
            PRIMARY KEY (user_id, tag_id)
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot("schema_created")


def phase_bulk_insert(tracker, db_path, n_records):
    """Insert n_records into users + orders tables."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()

    users = []
    orders = []
    for i in range(n_records):
        uid = i + 1
        name = rand_str("user_")
        email = rand_email()
        score = round(random.uniform(0, 100), 2)
        active = random.choice([True, False])
        created = rand_datetime()
        notes = rand_str("note_", 50)
        payload = os.urandom(64)
        balance = round(random.uniform(-500, 5000), 2)
        user_uuid = uuid.uuid4()
        users.append((uid, name, email, score, active, created, notes, payload, balance, user_uuid))

        oid = i + 1
        total = round(random.uniform(10, 500), 2)
        status = random.choice(["pending", "shipped", "delivered", "cancelled"])
        placed = rand_datetime()
        orders.append((oid, uid, total, status, placed))

    cur.executemany(
        "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        users,
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        orders,
    )
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"bulk_insert_{n_records}")


def phase_create_indexes(tracker, db_path):
    """Create indexes on various columns."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE INDEX idx_users_email ON users (email)")
    cur.execute("CREATE INDEX idx_users_score ON users (score)")
    cur.execute("CREATE INDEX idx_users_active ON users (active)")
    cur.execute("CREATE INDEX idx_orders_user_id ON orders (user_id)")
    cur.execute("CREATE INDEX idx_orders_status ON orders (status)")
    cur.execute("CREATE INDEX idx_orders_placed_at ON orders (placed_at)")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot("indexes_created")


def phase_indexed_queries(tracker, db_path, n_queries):
    """Run queries that exercise the indexes."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()

    # Point lookups on indexed columns
    for _ in range(n_queries):
        cur.execute("SELECT id, name, email FROM users WHERE email = ?", (rand_email(),))
        cur.fetchall()

        cur.execute("SELECT id, name FROM users WHERE score > ?", (random.uniform(0, 100),))
        cur.fetchall()

        cur.execute("SELECT id FROM users WHERE active = ?", (random.choice([True, False]),))
        cur.fetchall()

        cur.execute("SELECT id, total FROM orders WHERE user_id = ?", (random.randint(1, n_queries),))
        cur.fetchall()

        cur.execute("SELECT id, total FROM orders WHERE status = ?", (random.choice(["pending", "shipped", "delivered", "cancelled"]),))
        cur.fetchall()

    # Range scans
    cur.execute("SELECT id, name, score FROM users WHERE score BETWEEN ? AND ? ORDER BY score",
                (10.0, 90.0))
    cur.fetchall()

    # Join query
    cur.execute("""
        SELECT u.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.id
        ORDER BY total_spent DESC
        LIMIT 100
    """)
    cur.fetchall()

    # Aggregations
    cur.execute("SELECT COUNT(*), AVG(score), MIN(score), MAX(score) FROM users")
    cur.fetchone()

    cur.execute("SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status")
    cur.fetchall()

    cur.close()
    conn.close()
    return tracker.snapshot(f"indexed_queries_{n_queries}")


def phase_delete_indexes(tracker, db_path):
    """Drop all indexes."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("DROP INDEX idx_users_email")
    cur.execute("DROP INDEX idx_users_score")
    cur.execute("DROP INDEX idx_users_active")
    cur.execute("DROP INDEX idx_orders_user_id")
    cur.execute("DROP INDEX idx_orders_status")
    cur.execute("DROP INDEX idx_orders_placed_at")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot("indexes_deleted")


def phase_delete_records(tracker, db_path, delete_fraction):
    """Delete a fraction of records to exercise the delete path."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("DELETE FROM orders WHERE status = 'cancelled'")
    deleted_orders = cur.rowcount
    conn.commit()

    # Delete some users (and their orders via cascade-like manual delete)
    cur.execute("SELECT id FROM users WHERE active = 0 LIMIT ?", (int(delete_fraction * 1000),))
    inactive_ids = [row[0] for row in cur.fetchall()]
    if inactive_ids:
        placeholders = ", ".join("?" for _ in inactive_ids)
        cur.execute(f"DELETE FROM orders WHERE user_id IN ({placeholders})", inactive_ids)
        cur.execute(f"DELETE FROM user_tags WHERE user_id IN ({placeholders})", inactive_ids)
        cur.execute(f"DELETE FROM users WHERE id IN ({placeholders})", inactive_ids)
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"delete_records_{delete_fraction}")


def phase_update_records(tracker, db_path, n_updates):
    """Update records to exercise the update path."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    for i in range(n_updates):
        cur.execute(
            "UPDATE users SET score = ?, notes = ? WHERE id = ?",
            (round(random.uniform(0, 100), 2), rand_str("upd_", 20), random.randint(1, n_updates)),
        )
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"update_records_{n_updates}")


def phase_schema_introspection(tracker, db_path):
    """Exercise schema introspection APIs."""
    conn = decentdb.connect(db_path, mode="open")
    conn.list_tables()
    conn.list_indexes()
    conn.list_views()
    conn.list_triggers()
    conn.get_table_columns("users")
    conn.get_table_columns("orders")
    conn.get_table_ddl("users")
    conn.get_table_ddl("orders")
    conn.inspect_storage_state()
    conn.close()
    return tracker.snapshot("schema_introspection")


def phase_checkpoint(tracker, db_path):
    """Force a WAL checkpoint."""
    conn = decentdb.connect(db_path, mode="open")
    conn.checkpoint()
    conn.close()
    return tracker.snapshot("checkpoint")


def phase_drop_tables(tracker, db_path):
    """Drop all tables."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS user_tags")
    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("DROP TABLE IF EXISTS tags")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot("tables_dropped")


def phase_vacuum_like(tracker, db_path):
    """Re-create and drop tables to exercise allocation/deallocation."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    for i in range(5):
        cur.execute(f"CREATE TABLE temp_table_{i} (id INT64 PRIMARY KEY, data TEXT)")
        conn.commit()
        # Insert some rows
        rows = [(j, rand_str("data_", 100)) for j in range(100)]
        cur.executemany("INSERT INTO temp_table_{i} VALUES (?, ?)".format(i=i), rows)
        conn.commit()
        # Drop
        cur.execute(f"DROP TABLE temp_table_{i}")
        conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot("vacuum_like")


def phase_transactions(tracker, db_path, n_txns):
    """Run many small transactions to exercise txn lifecycle."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS txn_test (id INT64 PRIMARY KEY, val TEXT)")

    for i in range(n_txns):
        conn.begin_transaction()
        cur.execute("INSERT INTO txn_test VALUES (?, ?)", (i, rand_str("txn_", 20)))
        conn.commit()

    # Rollback a transaction
    conn.begin_transaction()
    cur.execute("INSERT INTO txn_test VALUES (?, ?)", (-1, "rollback_me"))
    conn.rollback()

    # Verify rollback
    cur.execute("SELECT val FROM txn_test WHERE id = -1")
    assert cur.fetchone() is None, "Rollback did not work"

    cur.execute("DROP TABLE txn_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"transactions_{n_txns}")


def phase_context_manager(tracker, db_path):
    """Exercise context manager semantics."""
    with decentdb.connect(db_path, mode="open") as conn:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS ctx_test (id INT64 PRIMARY KEY, val TEXT)")
        cur.execute("INSERT INTO ctx_test VALUES (1, 'ctx_manager')")
        cur.close()
    # Connection should be committed and closed

    with decentdb.connect(db_path, mode="open") as conn:
        cur = conn.cursor()
        cur.execute("SELECT val FROM ctx_test WHERE id = 1")
        row = cur.fetchone()
        assert row is not None and row[0] == "ctx_manager"
        cur.execute("DROP TABLE ctx_test")
        cur.close()

    return tracker.snapshot("context_manager")


def phase_blob_stress(tracker, db_path, n_blobs):
    """Insert and query large BLOBs to stress memory handling."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS blob_test (id INT64 PRIMARY KEY, data BLOB)")

    for i in range(n_blobs):
        blob_data = os.urandom(random.randint(256, 4096))
        cur.execute("INSERT INTO blob_test VALUES (?, ?)", (i, blob_data))
    conn.commit()

    # Query them back
    cur.execute("SELECT id, data FROM blob_test ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == n_blobs

    cur.execute("DROP TABLE blob_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"blob_stress_{n_blobs}")


def phase_text_stress(tracker, db_path, n_rows):
    """Insert and query large TEXT values."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS text_test (id INT64 PRIMARY KEY, data TEXT)")

    for i in range(n_rows):
        text_data = rand_str("text_", random.randint(500, 5000))
        cur.execute("INSERT INTO text_test VALUES (?, ?)", (i, text_data))
    conn.commit()

    cur.execute("SELECT id, data FROM text_test WHERE id = ?", (0,))
    row = cur.fetchone()
    assert row is not None

    cur.execute("SELECT COUNT(*) FROM text_test")
    count = cur.fetchone()[0]
    assert count == n_rows

    cur.execute("DROP TABLE text_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"text_stress_{n_rows}")


def phase_decimal_stress(tracker, db_path, n_rows):
    """Insert and query DECIMAL values."""
    import decimal
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS decimal_test (id INT64 PRIMARY KEY, amount DECIMAL(18,6))")

    for i in range(n_rows):
        amount = decimal.Decimal(str(round(random.uniform(-1_000_000, 1_000_000), 6)))
        cur.execute("INSERT INTO decimal_test VALUES (?, ?)", (i, amount))
    conn.commit()

    cur.execute("SELECT SUM(amount), AVG(amount) FROM decimal_test")
    row = cur.fetchone()
    assert row is not None

    cur.execute("DROP TABLE decimal_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"decimal_stress_{n_rows}")


def phase_uuid_stress(tracker, db_path, n_rows):
    """Insert and query UUID values."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS uuid_test (id INT64 PRIMARY KEY, uid UUID)")

    uids = [uuid.uuid4() for _ in range(n_rows)]
    rows = [(i, uids[i]) for i in range(n_rows)]
    cur.executemany("INSERT INTO uuid_test VALUES (?, ?)", rows)
    conn.commit()

    # Query back
    cur.execute("SELECT uid FROM uuid_test WHERE id = 0")
    row = cur.fetchone()
    assert row is not None

    cur.execute("DROP TABLE uuid_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"uuid_stress_{n_rows}")


def phase_timestamp_stress(tracker, db_path, n_rows):
    """Insert and query TIMESTAMP values."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS ts_test (id INT64 PRIMARY KEY, ts TIMESTAMP)")

    timestamps = [rand_datetime(2000, 2030) for _ in range(n_rows)]
    rows = [(i, timestamps[i]) for i in range(n_rows)]
    cur.executemany("INSERT INTO ts_test VALUES (?, ?)", rows)
    conn.commit()

    cur.execute("SELECT ts FROM ts_test WHERE ts > ?", (datetime.datetime(2025, 1, 1),))
    rows = cur.fetchall()

    cur.execute("DROP TABLE ts_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"timestamp_stress_{n_rows}")


def phase_null_stress(tracker, db_path, n_rows):
    """Insert and query NULL values."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS null_test (id INT64 PRIMARY KEY, a TEXT, b INT64, c FLOAT64)")

    rows = []
    for i in range(n_rows):
        a = None if i % 3 == 0 else rand_str()
        b = None if i % 3 == 1 else i
        c = None if i % 3 == 2 else round(random.uniform(0, 100), 2)
        rows.append((i, a, b, c))
    cur.executemany("INSERT INTO null_test VALUES (?, ?, ?, ?)", rows)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM null_test WHERE a IS NULL")
    assert cur.fetchone()[0] > 0

    cur.execute("SELECT COUNT(*) FROM null_test WHERE b IS NULL")
    assert cur.fetchone()[0] > 0

    cur.execute("SELECT COUNT(*) FROM null_test WHERE c IS NULL")
    assert cur.fetchone()[0] > 0

    cur.execute("DROP TABLE null_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"null_stress_{n_rows}")


def phase_cursor_reuse(tracker, db_path, n_reuses):
    """Reuse a single cursor for many operations."""
    conn = decentdb.connect(db_path, mode="open")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS cursor_test (id INT64 PRIMARY KEY, val TEXT)")

    for i in range(n_reuses):
        cur.execute("INSERT INTO cursor_test VALUES (?, ?)", (i, rand_str("reuse_")))
    conn.commit()

    # Fetch in chunks
    cur.execute("SELECT id, val FROM cursor_test")
    count = 0
    while True:
        rows = cur.fetchmany(100)
        if not rows:
            break
        count += len(rows)
    assert count == n_reuses

    cur.execute("DROP TABLE cursor_test")
    conn.commit()
    cur.close()
    conn.close()
    return tracker.snapshot(f"cursor_reuse_{n_reuses}")


def phase_save_as(tracker, db_path, save_path):
    """Exercise the save_as API."""
    conn = decentdb.connect(db_path, mode="open")
    conn.save_as(save_path)
    conn.close()

    # Verify the saved copy is readable
    conn2 = decentdb.connect(save_path, mode="open")
    tables = conn2.list_tables()
    assert len(tables) > 0
    conn2.close()

    cleanup_db(save_path)
    return tracker.snapshot("save_as")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def build_status_panel(tracker, iteration, total_iterations, phase_name):
    """Build a rich panel showing current status."""
    current = tracker.snapshots[-1] if tracker.snapshots else None
    peak = tracker.peak_delta
    final = tracker.final_delta

    table = Table.grid(padding=(0, 2))
    table.add_column(style="cyan", justify="right")
    table.add_column(style="white")

    table.add_row("Iteration:", f"{iteration}/{total_iterations}")
    table.add_row("Phase:", phase_name)
    table.add_row("Baseline RSS:", format_mem(tracker.baseline_mb or 0))

    if current:
        table.add_row("Current RSS:", format_mem(current["rss_mb"]))
        table.add_row("Delta from baseline:", f"{current['delta_mb']:+.2f} MB")

    table.add_row("Peak delta:", f"{peak:+.2f} MB")
    table.add_row("Final delta:", f"{final:+.2f} MB")

    elapsed = time.perf_counter() - (tracker.snapshots[0]["ts"] if tracker.snapshots else time.perf_counter())
    table.add_row("Elapsed:", f"{elapsed:.1f}s")

    return Panel(table, title="[bold green]Memory Leak Test[/bold green]", border_style="green")


def run_full_iteration(tracker, db_path, records_per_phase):
    """Run one complete iteration of all test phases."""
    n = records_per_phase

    phase_create_schema(tracker, db_path)
    phase_bulk_insert(tracker, db_path, n)
    phase_create_indexes(tracker, db_path)
    phase_indexed_queries(tracker, db_path, n)
    phase_delete_indexes(tracker, db_path)
    phase_delete_records(tracker, db_path, 0.3)
    phase_update_records(tracker, db_path, n)
    phase_schema_introspection(tracker, db_path)
    phase_checkpoint(tracker, db_path)
    phase_transactions(tracker, db_path, n)
    phase_context_manager(tracker, db_path)
    phase_blob_stress(tracker, db_path, n // 4)
    phase_text_stress(tracker, db_path, n // 4)
    phase_decimal_stress(tracker, db_path, n // 4)
    phase_uuid_stress(tracker, db_path, n // 4)
    phase_timestamp_stress(tracker, db_path, n // 4)
    phase_null_stress(tracker, db_path, n // 4)
    phase_cursor_reuse(tracker, db_path, n)
    phase_vacuum_like(tracker, db_path)
    phase_save_as(tracker, db_path, db_path + ".backup")
    phase_drop_tables(tracker, db_path)

    # Multiple open/close cycles after tables are dropped
    for i in range(5):
        phase_open_close_cycle(tracker, db_path, i)


def main():
    parser = argparse.ArgumentParser(description="DecentDB Python Memory Leak Showcase")
    parser.add_argument("--iterations", type=int, default=10, help="Number of full test iterations")
    parser.add_argument("--records", type=int, default=1000, help="Records per phase")
    parser.add_argument("--db-dir", type=str, default=None, help="Directory for test databases")
    parser.add_argument("--strict", action="store_true", help="Fail if memory delta exceeds 50 MB")
    parser.add_argument("--max-delta-mb", type=float, default=50.0, help="Max allowed memory delta in MB (default: 50)")
    args = parser.parse_args()

    console.print(Panel.fit(
        "[bold]DecentDB Python Bindings - Memory Leak Showcase[/bold]\n"
        f"Engine version: {decentdb.engine_version()}\n"
        f"ABI version: {decentdb.abi_version()}\n"
        f"Iterations: {args.iterations} | Records/phase: {args.records} | Max delta: {args.max_delta_mb} MB",
        border_style="blue",
    ))

    db_dir = args.db_dir or tempfile.mkdtemp(prefix="decentdb_leak_test_")
    os.makedirs(db_dir, exist_ok=True)
    db_path = os.path.join(db_dir, "leak_test.ddb")

    tracker = MemoryTracker()
    tracker.snapshot("baseline")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Running memory leak tests...", total=args.iterations)

        for iteration in range(1, args.iterations + 1):
            # Clean slate for each iteration
            cleanup_db(db_path)

            phase_label = f"iteration_{iteration}"
            run_full_iteration(tracker, db_path, args.records)
            tracker.snapshot(f"end_of_iteration_{iteration}")

            # Show status
            panel = build_status_panel(tracker, iteration, args.iterations, phase_label)
            console.print(panel)

            progress.advance(task)

    # Final summary
    console.print()
    console.rule("[bold]Final Summary[/bold]")

    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="cyan", justify="right")
    summary.add_column(style="white")

    summary.add_row("Total iterations:", str(args.iterations))
    summary.add_row("Records per phase:", str(args.records))
    summary.add_row("Baseline RSS:", format_mem(tracker.baseline_mb or 0))
    summary.add_row("Final RSS:", format_mem(tracker.snapshots[-1]["rss_mb"] if tracker.snapshots else 0))
    summary.add_row("Peak delta:", f"{tracker.peak_delta:+.2f} MB")
    summary.add_row("Final delta:", f"{tracker.final_delta:+.2f} MB")

    db_size = total_db_artifact_size_mb(db_path)
    summary.add_row("DB artifact size:", f"{db_size:.2f} MB")

    console.print(summary)

    # Memory trend table
    console.print()
    console.print("[bold]Memory Trend[/bold]")
    trend = Table("Snapshot", "RSS (MB)", "Delta (MB)", title="Memory Snapshots")
    for s in tracker.snapshots:
        trend.add_row(
            s["label"],
            f"{s['rss_mb']:.2f}",
            f"{s['delta_mb']:+.2f}",
        )
    console.print(trend)

    # Verdict
    console.print()
    passed = tracker.peak_delta <= args.max_delta_mb
    if passed:
        console.print(Panel(
            "[bold green]PASS[/bold green] - No memory leak detected.\n"
            f"Peak memory delta: {tracker.peak_delta:+.2f} MB (limit: {args.max_delta_mb} MB)",
            border_style="green",
        ))
    else:
        console.print(Panel(
            "[bold red]FAIL[/bold red] - Possible memory leak detected.\n"
            f"Peak memory delta: {tracker.peak_delta:+.2f} MB (limit: {args.max_delta_mb} MB)",
            border_style="red",
        ))

    # Cleanup
    cleanup_db(db_path)
    if not args.db_dir:
        try:
            shutil.rmtree(db_dir)
        except OSError:
            pass

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
