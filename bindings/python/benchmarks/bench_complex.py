"""
Apples-to-apples Python benchmark for DecentDB vs sqlite3 using a complex schema.

Workloads:
- Catalog Setup: Bulk insert Users and Items.
- Order Processing: Simulate an OLTP workload inserting Orders, Order Items, and Payments.
- Reporting Query: A complex aggregation query joining Items, OrderItems, and Orders.
- User History: Repeated point lookups joining Users, Orders, OrderItems, Payments, and Items.

Usage:
    python benchmarks/bench_complex.py
    python benchmarks/bench_complex.py --engine decentdb
    python benchmarks/bench_complex.py --users 50000 --items 5000 --orders 150000
"""

import argparse
import gc
import os
import random
import sqlite3
import time

import decentdb
from decentdb.native import load_library as load_decentdb_library


def remove_if_exists(path):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def cleanup_db_files(base_path):
    remove_if_exists(base_path)
    remove_if_exists(base_path + ".wal")
    remove_if_exists(base_path + "-wal")
    remove_if_exists(base_path + "-shm")


def percentile_sorted(sorted_values, pct):
    if not sorted_values:
        return 0.0
    idx = int(round((pct / 100.0) * (len(sorted_values) - 1)))
    idx = min(max(idx, 0), len(sorted_values) - 1)
    return sorted_values[idx]


def to_ms(ns):
    return ns / 1_000_000.0


def _run_with_gc_disabled(fn):
    gc_was_enabled = gc.isenabled()
    if gc_was_enabled:
        gc.disable()
    try:
        return fn()
    finally:
        if gc_was_enabled:
            gc.enable()


def setup_schema(conn, engine_name):
    cur = conn.cursor()
    # Apply type differences if any between engines
    type_int = "INTEGER" if engine_name == "sqlite" else "INT64"
    type_float = "REAL" if engine_name == "sqlite" else "FLOAT64"
    pk_auto = "INTEGER PRIMARY KEY AUTOINCREMENT" if engine_name == "sqlite" else "INT64 PRIMARY KEY"

    # In SQLite, foreign keys are OFF by default. In DecentDB they might be enforcing by default.
    if engine_name == "sqlite":
        cur.execute("PRAGMA foreign_keys = ON")

    # Users
    cur.execute(f"""
        CREATE TABLE users (
            id {type_int} PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)
    
    # Items
    cur.execute(f"""
        CREATE TABLE items (
            id {type_int} PRIMARY KEY,
            name TEXT,
            price {type_float},
            stock {type_int}
        )
    """)
    
    # Orders
    cur.execute(f"""
        CREATE TABLE orders (
            id {type_int} PRIMARY KEY,
            user_id {type_int},
            status TEXT,
            total_amount {type_float},
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
    """)
    
    # Order Items
    cur.execute(f"""
        CREATE TABLE order_items (
            order_id {type_int},
            item_id {type_int},
            quantity {type_int},
            price {type_float},
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(item_id) REFERENCES items(id)
        )
    """)

    # Payments
    cur.execute(f"""
        CREATE TABLE payments (
            id {type_int} PRIMARY KEY,
            order_id {type_int},
            amount {type_float},
            method TEXT,
            status TEXT,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
    """)

    # Indexes
    cur.execute("CREATE INDEX idx_orders_user_id ON orders(user_id)")
    cur.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
    cur.execute("CREATE INDEX idx_order_items_item_id ON order_items(item_id)")
    cur.execute("CREATE INDEX idx_payments_order_id ON payments(order_id)")


def setup_decentdb(db_path):
    conn = decentdb.connect(db_path)
    setup_schema(conn, "decentdb")
    return conn


def setup_sqlite(db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA synchronous=FULL")
    cur.execute("PRAGMA wal_autocheckpoint=0")
    setup_schema(conn, "sqlite")
    return conn


def generate_catalog_data(users_count, items_count):
    users = [(i, f"User_{i}", f"user{i}@example.com") for i in range(1, users_count + 1)]
    items = [(i, f"Item_{i}", round(random.uniform(5.0, 500.0), 2), random.randint(10, 10000)) for i in range(1, items_count + 1)]
    return users, items


def generate_orders_data(orders_count, users_count, items_data):
    orders = []
    order_items = []
    payments = []
    
    for order_id in range(1, orders_count + 1):
        user_id = random.randint(1, users_count)
        num_items = random.randint(1, 5)
        
        total_amount = 0.0
        for _ in range(num_items):
            item = random.choice(items_data)
            item_id = item[0]
            price = item[2]
            quantity = random.randint(1, 3)
            
            total_amount += (price * quantity)
            order_items.append((order_id, item_id, quantity, price))
            
        status = random.choice(["COMPLETED", "PENDING", "SHIPPED"])
        orders.append((order_id, user_id, status, total_amount))
        
        # Payment for the order
        method = random.choice(["CREDIT_CARD", "PAYPAL", "CRYPTO"])
        payment_status = "PAID" if status in ["COMPLETED", "SHIPPED"] else "PENDING"
        payments.append((order_id, order_id, total_amount, method, payment_status))

    return orders, order_items, payments


def run_engine_benchmark(
    engine_name,
    db_path,
    users_count,
    items_count,
    orders_count,
    history_reads,
    seed,
    keep_db,
):
    cleanup_db_files(db_path)
    print(f"\n=== {engine_name} ===")
    
    random.seed(seed)
    print("Generating memory dataset...")
    users_data, items_data = generate_catalog_data(users_count, items_count)
    orders_data, order_items_data, payments_data = generate_orders_data(orders_count, users_count, items_data)

    print("Setting up schema...")
    if engine_name == "decentdb":
        lib = load_decentdb_library()
        lib_path = getattr(lib, "_name", "<unknown>")
        print(f"DecentDB native library: {lib_path}")
        conn = setup_decentdb(db_path)
    elif engine_name == "sqlite":
        conn = setup_sqlite(db_path)
    else:
        raise ValueError(f"Unknown engine: {engine_name}")

    cur = conn.cursor()

    # 1. Catalog Insert Benchmark
    def run_catalog_inserts():
        started = time.perf_counter()
        cur.execute("BEGIN")
        try:
            cur.executemany("INSERT INTO users (id, name, email) VALUES (?, ?, ?)", users_data)
            cur.executemany("INSERT INTO items (id, name, price, stock) VALUES (?, ?, ?, ?)", items_data)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return time.perf_counter() - started

    catalog_s = _run_with_gc_disabled(run_catalog_inserts)
    catalog_rows = len(users_data) + len(items_data)
    print(f"Catalog Insert ({catalog_rows} rows): {catalog_s:.9f}s")

    # 2. OTLP / Order Insert Benchmark
    def run_order_inserts():
        started = time.perf_counter()
        cur.execute("BEGIN")
        try:
            cur.executemany("INSERT INTO orders (id, user_id, status, total_amount) VALUES (?, ?, ?, ?)", orders_data)
            cur.executemany("INSERT INTO order_items (order_id, item_id, quantity, price) VALUES (?, ?, ?, ?)", order_items_data)
            cur.executemany("INSERT INTO payments (id, order_id, amount, method, status) VALUES (?, ?, ?, ?, ?)", payments_data)
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return time.perf_counter() - started

    orders_s = _run_with_gc_disabled(run_order_inserts)
    orders_rows = len(orders_data) + len(order_items_data) + len(payments_data)
    print(f"Orders Insert ({orders_rows} rows): {orders_s:.9f}s")

    # 3. Complex Reporting Join (OLAP)
    report_sql = """
        SELECT i.name, SUM(oi.quantity), SUM(oi.quantity * oi.price) as revenue
        FROM items i
        JOIN order_items oi ON i.id = oi.item_id
        JOIN orders o ON oi.order_id = o.id
        WHERE o.status = 'COMPLETED'
        GROUP BY i.id, i.name
        ORDER BY revenue DESC
        LIMIT 100
    """
    
    # Warm up query compilation
    cur.execute(report_sql)
    cur.fetchall()

    def run_report_query():
        started = time.perf_counter()
        cur.execute(report_sql)
        _ = cur.fetchall()
        return time.perf_counter() - started

    report_s = _run_with_gc_disabled(run_report_query)
    print(f"Complex Sales Report Query: {report_s:.9f}s")

    # 4. User History Point Reads with Joins
    history_sql = """
        SELECT o.id, o.total_amount, p.status, i.name, oi.quantity, oi.price
        FROM orders o
        JOIN payments p ON o.id = p.order_id
        JOIN order_items oi ON o.id = oi.order_id
        JOIN items i ON oi.item_id = i.id
        WHERE o.user_id = ?
        ORDER BY o.id DESC
    """
    
    # Warm up
    cur.execute(history_sql, (users_data[0][0],))
    cur.fetchall()

    target_user_ids = [random.randint(1, users_count) for _ in range(history_reads)]

    latencies_ns = []
    for uid in target_user_ids:
        started_ns = time.perf_counter_ns()
        cur.execute(history_sql, (uid,))
        rows = cur.fetchall()
        elapsed_ns = time.perf_counter_ns() - started_ns
        latencies_ns.append(elapsed_ns)

    latencies_ns.sort()
    p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(f"User History Joins ({history_reads} lookups): p50={p50_ms:.6f}ms p95={p95_ms:.6f}ms")

    if engine_name == "sqlite":
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    if not keep_db:
        cleanup_db_files(db_path)

    return {
        "catalog_insert_s": catalog_s,
        "orders_insert_rps": orders_rows / orders_s,
        "report_query_s": report_s,
        "history_p50_ms": p50_ms,
        "history_p95_ms": p95_ms,
    }


def print_comparison(results, *, tie_threshold=0.0):
    if "decentdb" not in results or "sqlite" not in results:
        return

    d = results["decentdb"]
    s = results["sqlite"]

    metrics = [
        {
            "name": "Catalog Insert Time",
            "decent": d["catalog_insert_s"],
            "sqlite": s["catalog_insert_s"],
            "unit": "s",
            "higher_is_better": False,
            "fmt": ".9f",
        },
        {
            "name": "Orders Insert throughput",
            "decent": d["orders_insert_rps"],
            "sqlite": s["orders_insert_rps"],
            "unit": " rows/s",
            "higher_is_better": True,
            "fmt": ".2f",
        },
        {
            "name": "Complex Report Query",
            "decent": d["report_query_s"],
            "sqlite": s["report_query_s"],
            "unit": "s",
            "higher_is_better": False,
            "fmt": ".9f",
        },
        {
            "name": "User History Join p50",
            "decent": d["history_p50_ms"],
            "sqlite": s["history_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "User History Join p95",
            "decent": d["history_p95_ms"],
            "sqlite": s["history_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
    ]

    decent_better = []
    sqlite_better = []
    ties = []

    for metric in metrics:
        name = metric["name"]
        decent = metric["decent"]
        sqlite = metric["sqlite"]
        unit = metric["unit"]
        fmt = metric["fmt"]
        higher_is_better = metric["higher_is_better"]

        if decent == sqlite:
            ties.append(f"{name}: tie ({decent:{fmt}}{unit})")
            continue
        max_val = max(abs(decent), abs(sqlite))
        if tie_threshold > 0.0 and max_val > 0.0:
            rel_delta = abs(decent - sqlite) / max_val
            if rel_delta <= tie_threshold:
                ties.append(
                    f"{name}: statistical tie "
                    f"({decent:{fmt}}{unit} vs {sqlite:{fmt}}{unit})"
                )
                continue

        if higher_is_better:
            decent_wins = decent > sqlite
            winner_val = decent if decent_wins else sqlite
            loser_val = sqlite if decent_wins else decent
            ratio = winner_val / loser_val if loser_val else float("inf")
            detail = (
                f"{name}: {winner_val:{fmt}}{unit} vs {loser_val:{fmt}}{unit} "
                f"({ratio:.3f}x higher)"
            )
        else:
            decent_wins = decent < sqlite
            winner_val = decent if decent_wins else sqlite
            loser_val = sqlite if decent_wins else decent
            ratio = loser_val / winner_val if winner_val else float("inf")
            detail = (
                f"{name}: {winner_val:{fmt}}{unit} vs {loser_val:{fmt}}{unit} "
                f"({ratio:.3f}x faster/lower)"
            )

        if decent_wins:
            decent_better.append(detail)
        else:
            sqlite_better.append(detail)

    print("\n=== Comparison (DecentDB vs SQLite) ===")
    print("DecentDB better at:")
    if decent_better:
        for line in decent_better:
            print(f"- {line}")
    else:
        print("- none")

    print("SQLite better at:")
    if sqlite_better:
        for line in sqlite_better:
            print(f"- {line}")
    else:
        print("- none")

    if ties:
        print("Ties:")
        for line in ties:
            print(f"- {line}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Complex Python benchmark: DecentDB bindings vs sqlite3"
    )
    parser.add_argument(
        "--engine",
        choices=["all", "decentdb", "sqlite"],
        default="all",
        help="Engine to run (default: all)",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=20000,
        help="Number of users to generate (default: 20000)",
    )
    parser.add_argument(
        "--items",
        type=int,
        default=5000,
        help="Number of items to generate (default: 5000)",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=100000,
        help="Number of orders to generate (default: 100000)",
    )
    parser.add_argument(
        "--history-reads",
        type=int,
        default=5000,
        help="Number of random user history points reads (default: 5000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=1337,
        help="RNG seed for data generation (default: 1337)",
    )
    parser.add_argument(
        "--db-prefix",
        default="bench_complex",
        help="Database file prefix (default: bench_complex)",
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep generated database files after benchmark run",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    engines = (
        ["decentdb", "sqlite"]
        if args.engine == "all"
        else [args.engine]
    )
    results = {}

    for engine in engines:
        suffix = "ddb" if engine == "decentdb" else "db"
        path = f"{args.db_prefix}_{engine}.{suffix}"
        results[engine] = run_engine_benchmark(
            engine_name=engine,
            db_path=path,
            users_count=args.users,
            items_count=args.items,
            orders_count=args.orders,
            history_reads=args.history_reads,
            seed=args.seed,
            keep_db=args.keep_db,
        )

    print_comparison(results)


if __name__ == "__main__":
    main()
