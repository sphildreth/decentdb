"""
Apples-to-apples Python benchmark for DecentDB vs sqlite3 using a complex schema.

Workloads:
- Catalog Insert: Bulk insert Users and Items.
- Orders Insert: Simulate an OLTP workload inserting Orders, Order Items, and Payments.
- Point Lookup: Simple primary key lookups.
- Range Scan: Range queries on indexed and non-indexed columns.
- Join Query: Two-table join operations.
- Aggregate Query: GROUP BY aggregation with filtering.
- Reporting Query: A complex aggregation query joining Items, OrderItems, and Orders.
- User History: Repeated point lookups joining Users, Orders, OrderItems, Payments, and Items.
- Update: Row update operations.
- Delete: Row delete operations.
- Full Table Scan: Full table scan without filters.
- MovieDB Bulk Load: Movie, People, Roles, Reviews, Tags, MovieTags, Watchlist.
- MovieDB Point Reads: 1,000 UUID primary-key reads.
- MovieDB Relational Queries: top-rated-by-year, tag search, busiest people,
  watchlist with LEFT JOIN aggregate.
- MovieDB Mutations: 1k box-office update batch and 10 movie ON DELETE CASCADE
  batch deletes.
- MovieDB Maintenance: checkpoint, checkpoint-after-mutations, compact/vacuum,
  and final file size.
- Showdown Bulk Load: Integer-key movie schema from the second .NET showdown
  harness, including people, movies, genres, roles, reviews, keywords, and
  bridge tables.
- Showdown Query Matrix: full/range scans, pagination, 3-table joins,
  COUNT DISTINCT, GROUP BY, window functions, recursive and multi-CTE queries,
  substring search, fulltext BM25, UNION, RETURNING, UPSERT, bulk updates, and
  bulk deletes.

This benchmark is designed to predict performance across all metrics tested in the
python_embedded_compare framework. If DecentDB leads in all these metrics, it is
highly likely to be the leader in the full comparison framework.

Usage:
    python benchmarks/bench_complex.py
    python benchmarks/bench_complex.py --engine decentdb
    python benchmarks/bench_complex.py --users 50000 --items 5000 --orders 150000

The default no-argument run uses a smoke-scale workload so it completes quickly
enough for local comparison runs.
"""

import argparse
import datetime as _dt
import gc
import hashlib
import json
import os
import platform
import random
import sqlite3
import sys
import time
import uuid

import decentdb
from decentdb.native import load_library as load_decentdb_library

DEFAULT_USERS = 1000
DEFAULT_ITEMS = 50
DEFAULT_ORDERS = 100

DEFAULT_MOVIES = 2_000
DEFAULT_PEOPLE = 1_000
DEFAULT_ROLES = 10_000
DEFAULT_REVIEWS = 20_000
DEFAULT_TAGS = 100
DEFAULT_MOVIE_TAGS = 6_000
DEFAULT_WATCHLIST = 4_000
DEFAULT_MOVIE_POINT_READS = 1_000
DEFAULT_MOVIE_UPDATE_COUNT = 1_000
DEFAULT_MOVIE_DELETE_COUNT = 10

SCRATCH_MOVIES = 50_000
SCRATCH_PEOPLE = 25_000
SCRATCH_ROLES = 250_000
SCRATCH_REVIEWS = 500_000
SCRATCH_TAGS = 500
SCRATCH_MOVIE_TAGS = 150_000
SCRATCH_WATCHLIST = 100_000

DEFAULT_SHOWDOWN_MOVIES = 700
DEFAULT_SHOWDOWN_PEOPLE_MULT = 3
DEFAULT_SHOWDOWN_REVIEWS_PER_MOVIE = 8
DEFAULT_SHOWDOWN_POINT_READS = 1_000

GLM52_SHOWDOWN_MOVIES = 20_000

DECENTDB_EMBEDDED_FAST_OPTIONS = (
    "cache_size=64MB;"
    "retain_paged_row_sources_after_commit=true;"
    "paged_row_storage=false;"
    "wal_autocheckpoint=0;"
    "process_coordination=single_process_unsafe;"
    # Match SQLite's default benchmark PRAGMA synchronous=NORMAL so both
    # engines use the same reduced-sync WAL durability. SQLite WAL mode with
    # synchronous=NORMAL does NOT fsync per commit; it fsyncs the WAL only at
    # checkpoint. WalSyncMode::Normal still fsyncs per commit (only omitting
    # metadata sync), which is inconsistent with the target. Use async_commit
    # with a 10ms background flusher so per-commit latency matches SQLite while
    # retaining a tight durability window via the background flusher.
    "wal_sync_mode=async_commit:10"
)

MOVIE_FIRST_NAMES = [
    "Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "William", "Mia", "James", "Charlotte", "Benjamin",
    "Amelia", "Lucas", "Harper", "Henry", "Evelyn", "Alexander",
    "Abigail", "Michael", "Ella", "Daniel", "Scarlett", "Jackson",
    "Grace", "Sebastian", "Chloe", "Aiden",
]

MOVIE_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Ramirez", "Lewis", "Robinson",
]

MOVIE_ADJECTIVES = [
    "Dark", "Lost", "Hidden", "Silent", "Last", "Eternal", "Broken",
    "Golden", "Forbidden", "Invisible", "Midnight", "Secret", "Frozen",
    "Burning", "Shadow", "Rising", "Fallen", "Endless", "Wicked", "Brave",
]

MOVIE_NOUNS = [
    "King", "Queen", "Knight", "Empire", "Garden", "City", "Dream", "Storm",
    "Echo", "Horizon", "Legend", "Voyage", "Promise", "Memory",
    "Reflection", "Odyssey", "Kingdom", "Whisper", "Destiny", "Chronicle",
]

MOVIE_TAG_NAMES = [
    "Action", "Drama", "Comedy", "Sci-Fi", "Horror", "Thriller", "Romance",
    "Mystery", "Adventure", "Fantasy", "Crime", "Documentary", "Animation",
    "War", "Western", "Musical", "Biography", "Family", "Film-Noir",
    "Sport", "Superhero", "Time Travel", "Space", "Heist", "Revenge",
    "Survival", "Psychological", "Coming of Age", "Dystopian", "Noir",
]

MOVIE_RATINGS = ["G", "PG", "PG-13", "R", "NC-17"]


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


def _json_safe_value(value):
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (_dt.date, _dt.datetime, uuid.UUID)):
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    return str(value)


def _json_safe_row(row):
    return [_json_safe_value(value) for value in row]


def _query_signature(rows, compare_columns=None):
    safe_rows = [_json_safe_row(row) for row in rows]
    payload = json.dumps(safe_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    unordered_rows = sorted(
        safe_rows,
        key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
    )
    unordered_payload = json.dumps(
        unordered_rows,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    signature = {
        "rows": len(safe_rows),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "unordered_sha256": hashlib.sha256(unordered_payload).hexdigest(),
        "first_row": safe_rows[0] if safe_rows else None,
        "last_row": safe_rows[-1] if safe_rows else None,
    }
    if compare_columns is not None:
        compare_rows = [
            [row[index] for index in compare_columns if index < len(row)]
            for row in safe_rows
        ]
        compare_payload = json.dumps(
            compare_rows,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        unordered_compare_rows = sorted(
            compare_rows,
            key=lambda row: json.dumps(row, sort_keys=True, separators=(",", ":")),
        )
        unordered_compare_payload = json.dumps(
            unordered_compare_rows,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        signature.update(
            {
                "compare_columns": list(compare_columns),
                "compare_sha256": hashlib.sha256(compare_payload).hexdigest(),
                "compare_unordered_sha256": hashlib.sha256(
                    unordered_compare_payload
                ).hexdigest(),
            }
        )
    return signature


def _record_query_signature(results, key, label, rows, compare_columns=None):
    checks = results.setdefault("_checks", {})
    checks[key] = {"label": label, **_query_signature(rows, compare_columns)}


def _variant_metric_key(key, variant):
    if key.endswith("_s"):
        return f"{key[:-2]}_{variant}_s"
    return f"{key}_{variant}"


def _variant_check_key(key, variant):
    if key.endswith("_s"):
        return f"{key[:-2]}_{variant}"
    return f"{key}_{variant}"


def _fetch_rows(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def _safe_artifact_name(value):
    allowed = []
    for ch in value.lower():
        if ch.isalnum():
            allowed.append(ch)
        elif ch in (" ", "-", "_", "/", "+"):
            allowed.append("_")
    slug = "".join(allowed).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or "query"


def _capture_explain(
    cur,
    *,
    engine_name,
    workload,
    label,
    sql,
    params=(),
    output_dir=None,
    analyze=False,
):
    if not output_dir:
        return
    os.makedirs(output_dir, exist_ok=True)
    if engine_name == "sqlite":
        explain_sql = f"EXPLAIN QUERY PLAN {sql}"
        mode = "EXPLAIN QUERY PLAN"
    else:
        mode = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
        explain_sql = f"{mode} {sql}"
    try:
        rows = _fetch_rows(cur, explain_sql, params)
        payload = {
            "engine": engine_name,
            "workload": workload,
            "label": label,
            "mode": mode,
            "sql": " ".join(sql.split()),
            "params": [_json_safe_value(value) for value in params],
            "rows": [_json_safe_row(row) for row in rows],
        }
    except Exception as exc:
        payload = {
            "engine": engine_name,
            "workload": workload,
            "label": label,
            "mode": mode,
            "sql": " ".join(sql.split()),
            "params": [_json_safe_value(value) for value in params],
            "error": str(exc),
        }
    filename = f"{workload}_{engine_name}_{_safe_artifact_name(label)}.json"
    with open(os.path.join(output_dir, filename), "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _time_query_with_mode(
    *,
    engine_name,
    cur,
    workload,
    label,
    sql,
    results,
    metric_key,
    check_key,
    params=(),
    query_mode="warm",
    note=None,
    explain_output_dir=None,
    explain_analyze=False,
    print_rows=False,
    compare_columns=None,
):
    def run_fetch():
        return _fetch_rows(cur, sql, params)

    def record_rows(elapsed, rows, key, row_key):
        results[key] = elapsed
        if row_key:
            results[row_key] = len(rows)

    row_key = metric_key + "_rows"
    suffix = f" ({note})" if note else ""
    explain_captured = False

    if query_mode in ("cold", "both"):
        cold_metric_key = metric_key if query_mode == "cold" else _variant_metric_key(metric_key, "cold")
        cold_check_key = check_key if query_mode == "cold" else _variant_check_key(check_key, "cold")
        cold_row_key = row_key if query_mode == "cold" else cold_metric_key + "_rows"
        cold_label = label if query_mode == "cold" else f"{label} [cold]"
        duration, cold_rows = _time_movie_operation(engine_name, cold_label, 0, run_fetch)
        record_rows(duration, cold_rows, cold_metric_key, cold_row_key)
        _record_query_signature(
            results,
            cold_check_key,
            cold_label,
            cold_rows,
            compare_columns,
        )
        if print_rows:
            print(f"    rows={len(cold_rows):,}{suffix}")
        _capture_explain(
            cur,
            engine_name=engine_name,
            workload=workload,
            label=label,
            sql=sql,
            params=params,
            output_dir=explain_output_dir,
            analyze=explain_analyze,
        )
        explain_captured = True
        if query_mode == "cold":
            return duration, len(cold_rows)

    warm_rows = _fetch_rows(cur, sql, params) if query_mode == "warm" else None
    if warm_rows is not None:
        _record_query_signature(results, check_key, label, warm_rows, compare_columns)
    if not explain_captured:
        _capture_explain(
            cur,
            engine_name=engine_name,
            workload=workload,
            label=label,
            sql=sql,
            params=params,
            output_dir=explain_output_dir,
            analyze=explain_analyze,
        )
    duration, timed_rows = _time_movie_operation(engine_name, label, 0, run_fetch)
    record_rows(duration, timed_rows, metric_key, row_key)
    if query_mode == "both":
        _record_query_signature(results, check_key, label, timed_rows, compare_columns)
    if print_rows:
        print(f"    rows={len(timed_rows):,}{suffix}")
    return duration, len(timed_rows)


def setup_schema(conn, engine_name):
    cur = conn.cursor()
    # Apply type differences if any between engines
    type_int = "INTEGER" if engine_name == "sqlite" else "INT64"
    type_float = "REAL" if engine_name == "sqlite" else "FLOAT64"
    pk_auto = (
        "INTEGER PRIMARY KEY AUTOINCREMENT"
        if engine_name == "sqlite"
        else "INT64 PRIMARY KEY"
    )

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
    cur.execute("CREATE INDEX idx_orders_status ON orders(status)")
    cur.execute("CREATE INDEX idx_orders_total_amount ON orders(total_amount)")
    cur.execute("CREATE INDEX idx_items_price ON items(price)")
    cur.execute("CREATE INDEX idx_order_items_order_id ON order_items(order_id)")
    cur.execute("CREATE INDEX idx_order_items_item_id ON order_items(item_id)")
    cur.execute("CREATE INDEX idx_payments_order_id ON payments(order_id)")


def _cleanup_db_files(db_path):
    for suffix in ("", ".wal", "-wal", ".shm", "-shm"):
        try:
            os.unlink(db_path + suffix)
        except OSError:
            pass


def storage_size_bytes(db_path):
    total = 0
    for suffix in ("", ".wal", "-wal", ".shm", "-shm"):
        path = db_path + suffix
        if os.path.exists(path):
            total += os.path.getsize(path)
    return total


def setup_decentdb(db_path, *, options="", stmt_cache_size=128, initialize_complex=True):
    _cleanup_db_files(db_path)
    conn = decentdb.connect(db_path, options=options, stmt_cache_size=stmt_cache_size)
    if initialize_complex:
        setup_schema(conn, "decentdb")
    return conn


def setup_sqlite(
    db_path,
    *,
    profile="wal_full",
    cache_mb=64,
    initialize_complex=True,
):
    _cleanup_db_files(db_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    if profile in ("wal_full", "wal_normal"):
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=FULL" if profile == "wal_full" else "PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA wal_autocheckpoint=0")
    elif profile == "delete_full":
        cur.execute("PRAGMA journal_mode=DELETE")
        cur.execute("PRAGMA synchronous=FULL")
    else:
        raise ValueError(f"unknown SQLite profile: {profile}")
    cur.execute("PRAGMA temp_store=MEMORY")
    cur.execute(f"PRAGMA cache_size=-{cache_mb * 1000}")
    cur.execute("PRAGMA foreign_keys=ON")
    if initialize_complex:
        setup_schema(conn, "sqlite")
    return conn


def _sqlite_pragmas(profile, cache_mb):
    if profile in ("wal_full", "wal_normal"):
        journal_mode = "WAL"
        synchronous = "FULL" if profile == "wal_full" else "NORMAL"
        wal_autocheckpoint = 0
    elif profile == "delete_full":
        journal_mode = "DELETE"
        synchronous = "FULL"
        wal_autocheckpoint = None
    else:
        journal_mode = None
        synchronous = None
        wal_autocheckpoint = None
    pragmas = {
        "journal_mode": journal_mode,
        "synchronous": synchronous,
        "temp_store": "MEMORY",
        "cache_size_kib": -(cache_mb * 1000),
        "foreign_keys": "ON",
    }
    if wal_autocheckpoint is not None:
        pragmas["wal_autocheckpoint"] = wal_autocheckpoint
    return pragmas


def _decentdb_version_info():
    info = {
        "python_package_version": getattr(decentdb, "__version__", None),
        "native_library": None,
        "native_version": None,
        "abi_version": None,
    }
    try:
        lib = load_decentdb_library()
        info["native_library"] = getattr(lib, "_name", None)
        version = lib.ddb_version()
        if isinstance(version, bytes):
            version = version.decode("utf-8", errors="replace")
        info["native_version"] = version
        info["abi_version"] = int(lib.ddb_abi_version())
    except Exception as exc:
        info["error"] = str(exc)
    return info


def _engine_profile_metadata(
    engine_name,
    *,
    decentdb_options,
    decentdb_stmt_cache_size,
    sqlite_profile,
    sqlite_cache_mb,
):
    if engine_name == "decentdb":
        return {
            "options": decentdb_options or "",
            "stmt_cache_size": decentdb_stmt_cache_size,
        }
    return {
        "profile": sqlite_profile,
        "cache_mb": sqlite_cache_mb,
        "pragmas": _sqlite_pragmas(sqlite_profile, sqlite_cache_mb),
    }


def _numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _metric_direction(key):
    if key.endswith("_rps"):
        return "higher_is_better"
    if key.endswith(("_s", "_ms", "_bytes")):
        return "lower_is_better"
    if key.endswith("_rows") or key.endswith("_before") or key.endswith("_after"):
        return "equivalence"
    return "informational"


def _comparison_ratios(results):
    if "decentdb" not in results or "sqlite" not in results:
        return {}
    d = results["decentdb"]
    s = results["sqlite"]
    ratios = {}
    for key in sorted(set(d) & set(s)):
        if key.startswith("_") or not _numeric(d[key]) or not _numeric(s[key]):
            continue
        sqlite = s[key]
        decent = d[key]
        ratio = decent / sqlite if sqlite else None
        direction = _metric_direction(key)
        winner = None
        if direction == "higher_is_better" and decent != sqlite:
            winner = "decentdb" if decent > sqlite else "sqlite"
        elif direction == "lower_is_better" and decent != sqlite:
            winner = "decentdb" if decent < sqlite else "sqlite"
        elif direction == "equivalence":
            winner = "tie" if decent == sqlite else "mismatch"
        ratios[key] = {
            "decentdb": decent,
            "sqlite": sqlite,
            "decentdb_vs_sqlite": ratio,
            "direction": direction,
            "winner": winner or "tie",
        }
    return ratios


def _equivalence_report(results):
    if "decentdb" not in results or "sqlite" not in results:
        return {"status": "skipped", "reason": "requires both engines"}
    d_checks = results["decentdb"].get("_checks", {})
    s_checks = results["sqlite"].get("_checks", {})
    checks = {}
    failures = []
    for key in sorted(set(d_checks) | set(s_checks)):
        d = d_checks.get(key)
        s = s_checks.get(key)
        if d is None or s is None:
            if results["decentdb"].get(key) is None or results["sqlite"].get(key) is None:
                checks[key] = {"status": "skipped", "decentdb": d, "sqlite": s}
                continue
            checks[key] = {"status": "missing", "decentdb": d, "sqlite": s}
            failures.append(key)
            continue
        same_rows = d["rows"] == s["rows"]
        compare_columns = d.get("compare_columns")
        compare_ok = compare_columns is not None and compare_columns == s.get("compare_columns")
        sha_key = "compare_sha256" if compare_ok else "sha256"
        unordered_key = "compare_unordered_sha256" if compare_ok else "unordered_sha256"
        ordered_ok = same_rows and d[sha_key] == s[sha_key]
        unordered_ok = same_rows and d.get(unordered_key) == s.get(unordered_key)
        ok = ordered_ok or unordered_ok
        status_prefix = "ok_compare_projection" if compare_ok else "ok"
        checks[key] = {
            "status": status_prefix
            if ordered_ok
            else (
                f"{status_prefix}_unordered"
                if unordered_ok
                else "mismatch"
            ),
            "label": d.get("label") or s.get("label"),
            "decentdb": d,
            "sqlite": s,
        }
        if not ok:
            failures.append(key)
    return {
        "status": "ok" if not failures else "failed",
        "failures": failures,
        "checks": checks,
    }


def _print_equivalence_report(name, report):
    status = report.get("status")
    if status == "skipped":
        return
    failures = report.get("failures", [])
    if not failures:
        print(f"{name} result equivalence: ok")
        return
    print(f"{name} result equivalence: failed")
    for key in failures:
        check = report.get("checks", {}).get(key, {})
        print(f"- {key}: {check.get('status')}")


def _json_report(args, complex_results, movie_results, showdown_results, equivalence):
    return {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "argv": sys.argv,
        "python": {
            "version": platform.python_version(),
            "executable": sys.executable,
        },
        "engine_versions": {
            "decentdb": _decentdb_version_info(),
            "sqlite": {
                "sqlite_version": sqlite3.sqlite_version,
                "python_sqlite_version": getattr(sqlite3, "version", None),
            },
        },
        "config": {
            "workload": args.workload,
            "engine": args.engine,
            "engine_order": args.engine_order,
            "query_mode": args.query_mode,
            "seed": args.seed,
            "db_prefix": args.db_prefix,
            "keep_db": args.keep_db,
            "decentdb_options": args.decentdb_options,
            "decentdb_stmt_cache_size": args.decentdb_stmt_cache_size,
            "sqlite_profile": args.sqlite_profile,
            "sqlite_cache_mb": args.sqlite_cache_mb,
            "sqlite_pragmas": _sqlite_pragmas(args.sqlite_profile, args.sqlite_cache_mb),
            "movie_watchlist_movie_index": args.movie_watchlist_movie_index,
            "explain_output_dir": args.explain_output_dir,
            "explain_analyze": args.explain_analyze,
        },
        "results": {
            "complex": complex_results,
            "movie": movie_results,
            "showdown": showdown_results,
        },
        "comparisons": {
            "complex": _comparison_ratios(complex_results),
            "movie": _comparison_ratios(movie_results),
            "showdown": _comparison_ratios(showdown_results),
        },
        "equivalence": equivalence,
    }


def _write_json_report(path, payload):
    if not path:
        return
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    print(f"\nWrote JSON benchmark report: {path}")


def generate_catalog_data(users_count, items_count):
    users = [
        (i, f"User_{i}", f"user{i}@example.com") for i in range(1, users_count + 1)
    ]
    items = [
        (
            i,
            f"Item_{i}",
            round(random.uniform(5.0, 500.0), 2),
            random.randint(10, 10000),
        )
        for i in range(1, items_count + 1)
    ]
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

            total_amount += price * quantity
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
    point_lookups,
    range_scans,
    joins,
    aggregates,
    updates,
    deletes,
    table_scans,
    seed,
    keep_db,
    decentdb_options,
    decentdb_stmt_cache_size,
    sqlite_profile,
    sqlite_cache_mb,
):
    cleanup_db_files(db_path)
    print(f"\n=== {engine_name} ===")

    random.seed(seed)
    print("Generating memory dataset...")
    users_data, items_data = generate_catalog_data(users_count, items_count)
    orders_data, order_items_data, payments_data = generate_orders_data(
        orders_count, users_count, items_data
    )

    print("Setting up schema...")
    if engine_name == "decentdb":
        lib = load_decentdb_library()
        lib_path = getattr(lib, "_name", "<unknown>")
        print(f"DecentDB native library: {lib_path}")
        print(
            "DecentDB options: "
            f"{decentdb_options or '<default>'}; stmt_cache_size={decentdb_stmt_cache_size}"
        )
        conn = setup_decentdb(
            db_path,
            options=decentdb_options,
            stmt_cache_size=decentdb_stmt_cache_size,
        )
    elif engine_name == "sqlite":
        print(f"SQLite profile: {sqlite_profile}; cache_mb={sqlite_cache_mb}")
        conn = setup_sqlite(
            db_path,
            profile=sqlite_profile,
            cache_mb=sqlite_cache_mb,
        )
    else:
        raise ValueError(f"Unknown engine: {engine_name}")

    cur = conn.cursor()

    # 1. Catalog Insert Benchmark
    def run_catalog_inserts():
        started = time.perf_counter()
        cur.execute("BEGIN")
        try:
            cur.executemany(
                "INSERT INTO users (id, name, email) VALUES (?, ?, ?)", users_data
            )
            cur.executemany(
                "INSERT INTO items (id, name, price, stock) VALUES (?, ?, ?, ?)",
                items_data,
            )
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
            cur.executemany(
                "INSERT INTO orders (id, user_id, status, total_amount) VALUES (?, ?, ?, ?)",
                orders_data,
            )
            cur.executemany(
                "INSERT INTO order_items (order_id, item_id, quantity, price) VALUES (?, ?, ?, ?)",
                order_items_data,
            )
            cur.executemany(
                "INSERT INTO payments (id, order_id, amount, method, status) VALUES (?, ?, ?, ?, ?)",
                payments_data,
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return time.perf_counter() - started

    orders_s = _run_with_gc_disabled(run_order_inserts)
    orders_rows = len(orders_data) + len(order_items_data) + len(payments_data)
    print(f"Orders Insert ({orders_rows} rows): {orders_s:.9f}s")

    # 3. Simple Point Lookup (PK lookup)
    point_lookup_sql = "SELECT id, name, email FROM users WHERE id = ?"

    cur.execute(point_lookup_sql, (users_data[0][0],))
    cur.fetchall()

    target_user_ids = [random.randint(1, users_count) for _ in range(point_lookups)]

    latencies_ns = []
    for uid in target_user_ids:
        started_ns = time.perf_counter_ns()
        cur.execute(point_lookup_sql, (uid,))
        _ = cur.fetchall()
        elapsed_ns = time.perf_counter_ns() - started_ns
        latencies_ns.append(elapsed_ns)

    latencies_ns.sort()
    point_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    point_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Simple Point Lookup ({point_lookups} lookups): p50={point_p50_ms:.6f}ms p95={point_p95_ms:.6f}ms"
    )

    # 4. Range Scan (items by price range)
    range_scan_sql = "SELECT id, name, price FROM items WHERE price >= ? AND price < ? ORDER BY price LIMIT 100"

    min_price = min(item[2] for item in items_data)
    max_price = max(item[2] for item in items_data)
    price_range = max_price - min_price

    cur.execute(range_scan_sql, (min_price, min_price + price_range * 0.1))
    cur.fetchall()

    range_params = [
        (
            min_price + price_range * random.random(),
            min_price + price_range * (random.random() + 0.01),
        )
        for _ in range(range_scans)
    ]

    latencies_ns = []
    for low, high in range_params:
        started_ns = time.perf_counter_ns()
        cur.execute(range_scan_sql, (low, high))
        _ = cur.fetchall()
        elapsed_ns = time.perf_counter_ns() - started_ns
        latencies_ns.append(elapsed_ns)

    latencies_ns.sort()
    range_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    range_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Range Scan ({range_scans} scans): p50={range_p50_ms:.6f}ms p95={range_p95_ms:.6f}ms"
    )

    # 5. Simple Join Query (orders + users)
    join_sql = """
        SELECT o.id, o.total_amount, u.name
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.status = ?
        LIMIT 50
    """

    cur.execute(join_sql, ("COMPLETED",))
    cur.fetchall()

    statuses = ["COMPLETED", "PENDING", "SHIPPED"]
    target_statuses = [random.choice(statuses) for _ in range(joins)]

    latencies_ns = []
    for status in target_statuses:
        started_ns = time.perf_counter_ns()
        cur.execute(join_sql, (status,))
        _ = cur.fetchall()
        elapsed_ns = time.perf_counter_ns() - started_ns
        latencies_ns.append(elapsed_ns)

    latencies_ns.sort()
    join_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    join_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Join Query ({joins} joins): p50={join_p50_ms:.6f}ms p95={join_p95_ms:.6f}ms"
    )

    # 6. Aggregate Query
    aggregate_sql = """
        SELECT status, COUNT(*) as count, SUM(total_amount) as total
        FROM orders
        WHERE total_amount >= ? AND total_amount < ?
        GROUP BY status
    """

    min_amount = min(order[3] for order in orders_data)
    max_amount = max(order[3] for order in orders_data)
    amount_range = max_amount - min_amount

    cur.execute(aggregate_sql, (min_amount, min_amount + amount_range * 0.1))
    cur.fetchall()

    aggregate_params = [
        (
            min_amount + amount_range * random.random(),
            min_amount + amount_range * (random.random() + 0.01),
        )
        for _ in range(aggregates)
    ]

    latencies_ns = []
    for low, high in aggregate_params:
        started_ns = time.perf_counter_ns()
        cur.execute(aggregate_sql, (low, high))
        _ = cur.fetchall()
        elapsed_ns = time.perf_counter_ns() - started_ns
        latencies_ns.append(elapsed_ns)

    latencies_ns.sort()
    aggregate_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    aggregate_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Aggregate Query ({aggregates} aggregates): p50={aggregate_p50_ms:.6f}ms p95={aggregate_p95_ms:.6f}ms"
    )

    # 7. Complex Reporting Join (OLAP)
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
    print(
        f"User History Joins ({history_reads} lookups): p50={p50_ms:.6f}ms p95={p95_ms:.6f}ms"
    )

    # 8. Update Operations
    update_sql = "UPDATE users SET email = ? WHERE id = ?"

    cur.execute("BEGIN")
    try:
        test_user_id = users_data[0][0]
        cur.execute(update_sql, (f"updated_{test_user_id}@example.com", test_user_id))
        cur.execute("ROLLBACK")
    except Exception:
        cur.execute("ROLLBACK")
        raise

    target_user_ids = [random.randint(1, users_count) for _ in range(updates)]

    latencies_ns = []
    for uid in target_user_ids:
        cur.execute("BEGIN")
        try:
            started_ns = time.perf_counter_ns()
            cur.execute(update_sql, (f"updated_{uid}@example.com", uid))
            elapsed_ns = time.perf_counter_ns() - started_ns
            cur.execute("COMMIT")
            latencies_ns.append(elapsed_ns)
        except Exception:
            cur.execute("ROLLBACK")
            raise

    latencies_ns.sort()
    update_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    update_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Update Operations ({updates} updates): p50={update_p50_ms:.6f}ms p95={update_p95_ms:.6f}ms"
    )

    # 9. Delete Operations (delete from orders, including dependent order_items and payments)
    delete_order_items_sql = "DELETE FROM order_items WHERE order_id = ?"
    delete_payments_sql = "DELETE FROM payments WHERE order_id = ?"
    delete_orders_sql = "DELETE FROM orders WHERE id = ?"

    max_order_id = max(order[0] for order in orders_data)
    if deletes > len(orders_data):
        deletes = len(orders_data)

    order_ids_to_delete = [max_order_id - i for i in range(deletes)]

    cur.execute("BEGIN")
    try:
        test_order_id = order_ids_to_delete[0]
        cur.execute(delete_order_items_sql, (test_order_id,))
        cur.execute(delete_payments_sql, (test_order_id,))
        cur.execute(delete_orders_sql, (test_order_id,))
        cur.execute("ROLLBACK")
    except Exception:
        cur.execute("ROLLBACK")
        raise

    latencies_ns = []
    for order_id in order_ids_to_delete:
        cur.execute("BEGIN")
        try:
            started_ns = time.perf_counter_ns()
            cur.execute(delete_order_items_sql, (order_id,))
            cur.execute(delete_payments_sql, (order_id,))
            cur.execute(delete_orders_sql, (order_id,))
            elapsed_ns = time.perf_counter_ns() - started_ns
            cur.execute("COMMIT")
            latencies_ns.append(elapsed_ns)
        except Exception:
            cur.execute("ROLLBACK")
            raise

    latencies_ns.sort()
    delete_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    delete_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Delete Operations ({deletes} deletes): p50={delete_p50_ms:.6f}ms p95={delete_p95_ms:.6f}ms"
    )

    # 10. Full Table Scan
    table_scan_sql = "SELECT COUNT(*) FROM items"

    latencies_ns = []
    for _ in range(table_scans):
        started_ns = time.perf_counter_ns()
        cur.execute(table_scan_sql)
        _ = cur.fetchall()
        elapsed_ns = time.perf_counter_ns() - started_ns
        latencies_ns.append(elapsed_ns)

    latencies_ns.sort()
    table_scan_p50_ms = to_ms(percentile_sorted(latencies_ns, 50))
    table_scan_p95_ms = to_ms(percentile_sorted(latencies_ns, 95))
    print(
        f"Full Table Scan ({table_scans} scans): p50={table_scan_p50_ms:.6f}ms p95={table_scan_p95_ms:.6f}ms"
    )

    if engine_name == "sqlite":
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    conn.close()
    if not keep_db:
        cleanup_db_files(db_path)

    return {
        "catalog_insert_s": catalog_s,
        "orders_insert_rps": orders_rows / orders_s,
        "point_lookup_p50_ms": point_p50_ms,
        "point_lookup_p95_ms": point_p95_ms,
        "range_scan_p50_ms": range_p50_ms,
        "range_scan_p95_ms": range_p95_ms,
        "join_p50_ms": join_p50_ms,
        "join_p95_ms": join_p95_ms,
        "aggregate_p50_ms": aggregate_p50_ms,
        "aggregate_p95_ms": aggregate_p95_ms,
        "report_query_s": report_s,
        "history_p50_ms": p50_ms,
        "history_p95_ms": p95_ms,
        "update_p50_ms": update_p50_ms,
        "update_p95_ms": update_p95_ms,
        "delete_p50_ms": delete_p50_ms,
        "delete_p95_ms": delete_p95_ms,
        "table_scan_p50_ms": table_scan_p50_ms,
        "table_scan_p95_ms": table_scan_p95_ms,
    }


def _movie_table_suffix(engine_name):
    return " WITHOUT ROWID" if engine_name == "sqlite" else ""


def _movie_id_type(engine_name):
    return "BLOB" if engine_name == "sqlite" else "UUID"


def _movie_float_type(engine_name):
    return "REAL" if engine_name == "sqlite" else "FLOAT64"


def _movie_id_value(engine_name, value):
    return value.bytes if engine_name == "sqlite" else str(value)


def _movie_uuid_expr(engine_name):
    return "?" if engine_name == "sqlite" else "CAST(? AS UUID)"


def _movie_convert_row(engine_name, row, uuid_indexes):
    return tuple(
        _movie_id_value(engine_name, value) if index in uuid_indexes else value
        for index, value in enumerate(row)
    )


def _movie_convert_rows(engine_name, rows, uuid_indexes):
    return [_movie_convert_row(engine_name, row, uuid_indexes) for row in rows]


def _execute_script_statements(conn, sql):
    cur = conn.cursor()
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            cur.execute(statement)


def setup_movie_schema(conn, engine_name, *, watchlist_movie_index=False):
    id_type = _movie_id_type(engine_name)
    float_type = _movie_float_type(engine_name)
    suffix = _movie_table_suffix(engine_name)
    ddl = f"""
        CREATE TABLE IF NOT EXISTS Movies (
            Id {id_type} PRIMARY KEY,
            Title TEXT NOT NULL,
            ReleaseYear INTEGER NOT NULL,
            Synopsis TEXT,
            BudgetUsd {float_type} NOT NULL,
            BoxOfficeUsd {float_type},
            MpaaRating TEXT NOT NULL,
            RuntimeMinutes INTEGER NOT NULL,
            AddedAt TEXT NOT NULL
        ){suffix};

        CREATE TABLE IF NOT EXISTS People (
            Id {id_type} PRIMARY KEY,
            FullName TEXT NOT NULL,
            BirthDate TEXT,
            Biography TEXT
        ){suffix};

        CREATE TABLE IF NOT EXISTS Roles (
            Id {id_type} PRIMARY KEY,
            MovieId {id_type} NOT NULL REFERENCES Movies(Id) ON DELETE CASCADE,
            PersonId {id_type} NOT NULL REFERENCES People(Id) ON DELETE CASCADE,
            CharacterName TEXT NOT NULL,
            BillingOrder INTEGER NOT NULL,
            IsLead INTEGER NOT NULL
        ){suffix};
        CREATE INDEX IF NOT EXISTS ix_roles_movie ON Roles(MovieId);
        CREATE INDEX IF NOT EXISTS ix_roles_person ON Roles(PersonId);

        CREATE TABLE IF NOT EXISTS Reviews (
            Id {id_type} PRIMARY KEY,
            MovieId {id_type} NOT NULL REFERENCES Movies(Id) ON DELETE CASCADE,
            ReviewerHandle TEXT NOT NULL,
            Score INTEGER NOT NULL,
            Text TEXT,
            ReviewedAt TEXT NOT NULL,
            Verified INTEGER NOT NULL
        ){suffix};
        CREATE INDEX IF NOT EXISTS ix_reviews_movie ON Reviews(MovieId);
        CREATE INDEX IF NOT EXISTS ix_reviews_handle ON Reviews(ReviewerHandle);

        CREATE TABLE IF NOT EXISTS Tags (
            Id {id_type} PRIMARY KEY,
            Name TEXT NOT NULL UNIQUE
        ){suffix};

        CREATE TABLE IF NOT EXISTS MovieTags (
            MovieId {id_type} NOT NULL REFERENCES Movies(Id) ON DELETE CASCADE,
            TagId {id_type} NOT NULL REFERENCES Tags(Id) ON DELETE CASCADE,
            PRIMARY KEY (MovieId, TagId)
        ){suffix};
        CREATE INDEX IF NOT EXISTS ix_movietags_tag ON MovieTags(TagId);

        CREATE TABLE IF NOT EXISTS Watchlist (
            Id {id_type} PRIMARY KEY,
            UserHandle TEXT NOT NULL,
            MovieId {id_type} NOT NULL REFERENCES Movies(Id) ON DELETE CASCADE,
            Priority INTEGER NOT NULL,
            AddedAt TEXT NOT NULL
        ){suffix};
        CREATE INDEX IF NOT EXISTS ix_watchlist_user ON Watchlist(UserHandle);
    """
    if engine_name == "sqlite":
        conn.execute("PRAGMA foreign_keys=ON")
    _execute_script_statements(conn, ddl)
    if watchlist_movie_index:
        conn.execute("CREATE INDEX IF NOT EXISTS ix_watchlist_movie ON Watchlist(MovieId)")


def _movie_uuid(rng):
    return uuid.UUID(int=rng.getrandbits(128))


def _movie_iso_datetime(year, minute_offset):
    return (_dt.datetime(year, 1, 1) + _dt.timedelta(minutes=minute_offset)).isoformat()


def _movie_date(year, month, day):
    return _dt.date(year, month, day).isoformat()


def _movie_synopsis(rng):
    phrases = [
        f"{rng.choice(MOVIE_ADJECTIVES).lower()} {rng.choice(MOVIE_NOUNS).lower()}"
        for _ in range(3 + rng.randrange(5))
    ]
    return f"A tale of {', '.join(phrases)}."


def _movie_review_text(rng):
    words = [rng.choice(MOVIE_ADJECTIVES).lower() for _ in range(10 + rng.randrange(50))]
    return " ".join(words) + "."


def _movie_bio(rng):
    parts = [
        f"{rng.choice(MOVIE_ADJECTIVES)} performer from {rng.choice(MOVIE_NOUNS)}."
        for _ in range(2 + rng.randrange(3))
    ]
    return " ".join(parts)


def generate_movie_data(
    movies_count,
    people_count,
    roles_count,
    reviews_count,
    tags_count,
    movie_tags_count,
    watchlist_count,
    seed,
):
    rng = random.Random(seed)
    tags = []
    for i in range(tags_count):
        suffix = f"-{i // len(MOVIE_TAG_NAMES) + 1}" if i >= len(MOVIE_TAG_NAMES) else ""
        tags.append((_movie_uuid(rng), MOVIE_TAG_NAMES[i % len(MOVIE_TAG_NAMES)] + suffix))

    people = []
    for _ in range(people_count):
        birth = (
            _movie_date(1950 + rng.randrange(50), 1 + rng.randrange(12), 1 + rng.randrange(27))
            if rng.random() < 0.9
            else None
        )
        people.append(
            (
                _movie_uuid(rng),
                f"{rng.choice(MOVIE_FIRST_NAMES)} {rng.choice(MOVIE_LAST_NAMES)}",
                birth,
                _movie_bio(rng) if rng.random() < 0.5 else None,
            )
        )

    movies = []
    for i in range(movies_count):
        movies.append(
            (
                _movie_uuid(rng),
                f"{rng.choice(MOVIE_ADJECTIVES)} {rng.choice(MOVIE_NOUNS)} {i + 1:05d}",
                1980 + rng.randrange(45),
                _movie_synopsis(rng),
                1_000_000 + rng.random() * 199_000_000,
                None if rng.random() < 0.2 else 500_000 + rng.random() * 990_000_000,
                rng.choice(MOVIE_RATINGS),
                75 + rng.randrange(90),
                _movie_iso_datetime(2020, rng.randrange(2_000_000)),
            )
        )

    roles = []
    if movies and people:
        for i in range(roles_count):
            movie = movies[i % len(movies)]
            person = people[rng.randrange(len(people))]
            billing_order = 1 + (i % 20)
            roles.append(
                (
                    _movie_uuid(rng),
                    movie[0],
                    person[0],
                    f"{rng.choice(MOVIE_ADJECTIVES)} {rng.choice(MOVIE_NOUNS)}",
                    billing_order,
                    1 if billing_order <= 3 else 0,
                )
            )

    reviewer_handles = [f"user{i:05d}" for i in range(20_000)]
    reviews = []
    if movies:
        for _ in range(reviews_count):
            movie = movies[rng.randrange(len(movies))]
            reviews.append(
                (
                    _movie_uuid(rng),
                    movie[0],
                    rng.choice(reviewer_handles),
                    1 + rng.randrange(10),
                    _movie_review_text(rng) if rng.random() < 0.7 else None,
                    _movie_iso_datetime(2021, rng.randrange(2_000_000)),
                    1 if rng.random() < 0.15 else 0,
                )
            )

    movie_tags = []
    seen_movie_tags = set()
    if movies and tags:
        target = min(movie_tags_count, len(movies) * len(tags))
        attempts = 0
        while len(movie_tags) < target and attempts < target * 20 + 100:
            attempts += 1
            movie = movies[rng.randrange(len(movies))]
            tag = tags[rng.randrange(len(tags))]
            key = (movie[0], tag[0])
            if key in seen_movie_tags:
                continue
            seen_movie_tags.add(key)
            movie_tags.append(key)

        first_tag = tags[0][0]
        if not any(tag_id == first_tag for _, tag_id in movie_tags):
            movie_tags.append((movies[0][0], first_tag))

    watchlist_users = [f"watcher{i:04d}" for i in range(5_000)]
    watchlist = []
    seen_watchlist = set()
    if movies:
        target = min(watchlist_count, len(movies) * len(watchlist_users))
        attempts = 0
        while len(watchlist) < target and attempts < target * 20 + 100:
            attempts += 1
            movie = movies[rng.randrange(len(movies))]
            user = rng.choice(watchlist_users)
            key = (user, movie[0])
            if key in seen_watchlist:
                continue
            seen_watchlist.add(key)
            watchlist.append(
                (
                    _movie_uuid(rng),
                    user,
                    movie[0],
                    1 + rng.randrange(5),
                    _movie_iso_datetime(2023, rng.randrange(1_000_000)),
                )
            )

    return {
        "movies": movies,
        "people": people,
        "roles": roles,
        "reviews": reviews,
        "tags": tags,
        "movie_tags": movie_tags,
        "watchlist": watchlist,
    }


def movie_total_rows(data):
    return sum(len(rows) for rows in data.values())


def _time_movie_operation(engine_name, label, rows, fn):
    gc.collect()
    gc_wait = getattr(gc, "wait_for_pending_finalizers", None)
    if gc_wait:
        gc_wait()
    started = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - started
    rows_per_sec = rows / elapsed if rows and elapsed > 0 else 0.0
    if rows:
        print(f"  {label:<38} {elapsed:12.6f}s  ({rows:,} rows, {rows_per_sec:,.0f} rows/s)")
    else:
        print(f"  {label:<38} {elapsed:12.6f}s")
    return elapsed, result


def _movie_fetch_count(cur, sql, params=()):
    cur.execute(sql, params)
    rows = cur.fetchall()
    return len(rows)


def _movie_checkpoint(conn, engine_name):
    if engine_name == "sqlite":
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return
    cur = conn.cursor()
    try:
        cur._execute_direct("PRAGMA wal_checkpoint(TRUNCATE)", ())
    finally:
        cur.close()


def _movie_vacuum(conn, engine_name, db_path):
    if engine_name == "sqlite":
        conn.execute("VACUUM")
    else:
        dest = db_path + ".vacuumed"
        remove_if_exists(dest)
        conn.save_as(dest)


def _movie_insert_all(cur, engine_name, data):
    uid = _movie_uuid_expr(engine_name)
    cur.execute("BEGIN")
    try:
        cur.executemany(
            f"INSERT INTO Movies (Id, Title, ReleaseYear, Synopsis, BudgetUsd, BoxOfficeUsd, MpaaRating, RuntimeMinutes, AddedAt) VALUES ({uid}, ?, ?, ?, ?, ?, ?, ?, ?)",
            _movie_convert_rows(engine_name, data["movies"], {0}),
        )
        cur.executemany(
            f"INSERT INTO People (Id, FullName, BirthDate, Biography) VALUES ({uid}, ?, ?, ?)",
            _movie_convert_rows(engine_name, data["people"], {0}),
        )
        cur.executemany(
            f"INSERT INTO Roles (Id, MovieId, PersonId, CharacterName, BillingOrder, IsLead) VALUES ({uid}, {uid}, {uid}, ?, ?, ?)",
            _movie_convert_rows(engine_name, data["roles"], {0, 1, 2}),
        )
        cur.executemany(
            f"INSERT INTO Reviews (Id, MovieId, ReviewerHandle, Score, Text, ReviewedAt, Verified) VALUES ({uid}, {uid}, ?, ?, ?, ?, ?)",
            _movie_convert_rows(engine_name, data["reviews"], {0, 1}),
        )
        cur.executemany(
            f"INSERT INTO Tags (Id, Name) VALUES ({uid}, ?)",
            _movie_convert_rows(engine_name, data["tags"], {0}),
        )
        cur.executemany(
            f"INSERT INTO MovieTags (MovieId, TagId) VALUES ({uid}, {uid})",
            _movie_convert_rows(engine_name, data["movie_tags"], {0, 1}),
        )
        cur.executemany(
            f"INSERT INTO Watchlist (Id, UserHandle, MovieId, Priority, AddedAt) VALUES ({uid}, ?, {uid}, ?, ?)",
            _movie_convert_rows(engine_name, data["watchlist"], {0, 2}),
        )
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise


def run_movie_benchmark(
    engine_name,
    db_path,
    data,
    *,
    point_reads,
    update_count,
    delete_count,
    keep_db,
    decentdb_options,
    decentdb_stmt_cache_size,
    sqlite_profile,
    sqlite_cache_mb,
    watchlist_movie_index=False,
    explain_output_dir=None,
    explain_analyze=False,
    query_mode="warm",
    compare_columns=None,
):
    cleanup_db_files(db_path)
    remove_if_exists(db_path + ".vacuumed")
    print(f"\n=== {engine_name} MovieDB ===")

    if engine_name == "decentdb":
        lib = load_decentdb_library()
        lib_path = getattr(lib, "_name", "<unknown>")
        print(f"DecentDB native library: {lib_path}")
        print(
            "DecentDB options: "
            f"{decentdb_options or '<default>'}; stmt_cache_size={decentdb_stmt_cache_size}"
        )
        conn = setup_decentdb(
            db_path,
            options=decentdb_options,
            stmt_cache_size=decentdb_stmt_cache_size,
            initialize_complex=False,
        )
    elif engine_name == "sqlite":
        print(f"SQLite profile: {sqlite_profile}; cache_mb={sqlite_cache_mb}")
        conn = setup_sqlite(
            db_path,
            profile=sqlite_profile,
            cache_mb=sqlite_cache_mb,
            initialize_complex=False,
        )
        conn.execute("PRAGMA mmap_size=268435456")
    else:
        raise ValueError(f"Unknown engine: {engine_name}")

    print("Initializing MovieDB schema...")
    setup_movie_schema(conn, engine_name, watchlist_movie_index=watchlist_movie_index)
    cur = conn.cursor()
    results = {
        "_metadata": {
            "engine": engine_name,
            "db_path": db_path,
            "profile": _engine_profile_metadata(
                engine_name,
                decentdb_options=decentdb_options,
                decentdb_stmt_cache_size=decentdb_stmt_cache_size,
                sqlite_profile=sqlite_profile,
                sqlite_cache_mb=sqlite_cache_mb,
            ),
            "schema_variants": {
                "watchlist_movie_index": bool(watchlist_movie_index),
            },
            "query_mode": query_mode,
        }
    }
    counts = {}

    total_rows = movie_total_rows(data)
    duration, _ = _time_movie_operation(
        engine_name,
        "MovieDB bulk load",
        total_rows,
        lambda: _movie_insert_all(cur, engine_name, data),
    )
    results["movie_bulk_load_s"] = duration
    results["movie_bulk_load_rps"] = total_rows / duration if duration else 0.0

    duration, _ = _time_movie_operation(
        engine_name, "MovieDB checkpoint", 0, lambda: _movie_checkpoint(conn, engine_name)
    )
    results["movie_checkpoint_s"] = duration

    cur.execute("SELECT COUNT(*) FROM Movies")
    counts["movies_before"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Reviews")
    counts["reviews_before"] = cur.fetchone()[0]
    print(
        f"  Loaded {counts['movies_before']:,} movies / "
        f"{counts['reviews_before']:,} reviews"
    )

    movies = data["movies"]
    tags = data["tags"]
    watchlist = data["watchlist"]
    if not movies:
        raise ValueError("Movie benchmark requires at least one movie")
    if not tags:
        raise ValueError("Movie benchmark requires at least one tag")
    if not watchlist:
        raise ValueError("Movie benchmark requires at least one watchlist entry")

    point_ids = [row[0] for row in movies[: min(point_reads, len(movies))]]
    point_sql = (
        "SELECT Id, Title, ReleaseYear, Synopsis, BudgetUsd, BoxOfficeUsd, "
        f"MpaaRating, RuntimeMinutes, AddedAt FROM Movies WHERE Id = {_movie_uuid_expr(engine_name)}"
    )

    def run_point_reads():
        total = 0
        for movie_id in point_ids:
            cur.execute(point_sql, (_movie_id_value(engine_name, movie_id),))
            total += len(cur.fetchall())
        return total

    if query_mode in ("cold", "both"):
        cold_label = (
            "MovieDB point reads by UUID"
            if query_mode == "cold"
            else "MovieDB point reads by UUID [cold]"
        )
        duration, total = _time_movie_operation(
            engine_name,
            cold_label,
            len(point_ids),
            run_point_reads,
        )
        if query_mode == "cold":
            results["movie_point_reads_s"] = duration
            results["movie_point_reads_rows"] = total
        else:
            results["movie_point_reads_cold_s"] = duration
            results["movie_point_reads_cold_rows"] = total
        cold_rows = _fetch_rows(cur, point_sql, (_movie_id_value(engine_name, point_ids[0]),))
        _record_query_signature(
            results,
            "movie_point_read_first" if query_mode == "cold" else "movie_point_read_first_cold",
            cold_label,
            cold_rows,
        )

    if query_mode in ("warm", "both"):
        point_warm_rows = _fetch_rows(
            cur,
            point_sql,
            (_movie_id_value(engine_name, point_ids[0]),),
        )
        _record_query_signature(
            results,
            "movie_point_read_first",
            "MovieDB point read first UUID",
            point_warm_rows,
        )
        duration, total = _time_movie_operation(
            engine_name,
            "MovieDB point reads by UUID",
            len(point_ids),
            run_point_reads,
        )
        results["movie_point_reads_s"] = duration
        results["movie_point_reads_rows"] = total

    year_counts = {}
    for movie in movies:
        year_counts[movie[2]] = year_counts.get(movie[2], 0) + 1
    sample_year = max(year_counts, key=year_counts.get)
    sample_tag = tags[0][1]
    sample_user = watchlist[0][1]

    top_rated_sql = """
        SELECT m.Id, m.Title, m.ReleaseYear, m.Synopsis, m.BudgetUsd,
               m.BoxOfficeUsd, m.MpaaRating, m.RuntimeMinutes, m.AddedAt,
               AVG(r.Score) as AvgScore, COUNT(r.Id) as ReviewCount
        FROM Movies m
        JOIN Reviews r ON r.MovieId = m.Id
        WHERE m.ReleaseYear = ?
        GROUP BY m.Id
        HAVING COUNT(r.Id) >= ?
        ORDER BY AvgScore DESC, m.Title
        LIMIT ?
    """
    top_params = (sample_year, 20, 25)
    _, rows = _time_query_with_mode(
        engine_name=engine_name,
        cur=cur,
        workload="movie",
        label="MovieDB top-rated by year",
        sql=top_rated_sql,
        results=results,
        metric_key="movie_top_rated_s",
        check_key="movie_top_rated",
        params=top_params,
        query_mode=query_mode,
        explain_output_dir=explain_output_dir,
        explain_analyze=explain_analyze,
    )
    counts["top_rated_rows"] = rows

    tag_sql = """
        SELECT m.Id, m.Title, m.ReleaseYear, m.Synopsis, m.BudgetUsd,
               m.BoxOfficeUsd, m.MpaaRating, m.RuntimeMinutes, m.AddedAt
        FROM Movies m
        JOIN MovieTags mt ON mt.MovieId = m.Id
        JOIN Tags t ON t.Id = mt.TagId
        WHERE t.Name = ?
        ORDER BY m.ReleaseYear DESC, m.Id ASC
        LIMIT ?
    """
    tag_params = (sample_tag, 50)
    _, rows = _time_query_with_mode(
        engine_name=engine_name,
        cur=cur,
        workload="movie",
        label="MovieDB search movies by tag",
        sql=tag_sql,
        results=results,
        metric_key="movie_tag_search_s",
        check_key="movie_tag_search",
        params=tag_params,
        query_mode=query_mode,
        explain_output_dir=explain_output_dir,
        explain_analyze=explain_analyze,
    )
    counts["tag_search_rows"] = rows

    busiest_sql = """
        SELECT p.Id, p.FullName, p.BirthDate, p.Biography, COUNT(r.Id) as RoleCount
        FROM People p
        JOIN Roles r ON r.PersonId = p.Id
        GROUP BY p.Id
        ORDER BY RoleCount DESC
        LIMIT ?
    """
    busiest_params = (20,)
    _, rows = _time_query_with_mode(
        engine_name=engine_name,
        cur=cur,
        workload="movie",
        label="MovieDB busiest people",
        sql=busiest_sql,
        results=results,
        metric_key="movie_busiest_people_s",
        check_key="movie_busiest_people",
        params=busiest_params,
        query_mode=query_mode,
        explain_output_dir=explain_output_dir,
        explain_analyze=explain_analyze,
    )
    counts["busiest_people_rows"] = rows

    watchlist_sql = """
        SELECT m.Id, m.Title, w.Priority, AVG(r.Score) as Avg
        FROM Watchlist w
        JOIN Movies m ON m.Id = w.MovieId
        LEFT JOIN Reviews r ON r.MovieId = m.Id
        WHERE w.UserHandle = ?
        GROUP BY m.Id
        ORDER BY w.Priority DESC, Avg DESC NULLS LAST
        LIMIT ?
    """
    watchlist_params = (sample_user, 20)
    _, rows = _time_query_with_mode(
        engine_name=engine_name,
        cur=cur,
        workload="movie",
        label="MovieDB watchlist query",
        sql=watchlist_sql,
        results=results,
        metric_key="movie_watchlist_s",
        check_key="movie_watchlist",
        params=watchlist_params,
        query_mode=query_mode,
        explain_output_dir=explain_output_dir,
        explain_analyze=explain_analyze,
    )
    counts["watchlist_rows"] = rows

    update_ids = [row[0] for row in movies[: min(update_count, len(movies))]]
    update_sql = f"UPDATE Movies SET BoxOfficeUsd = ? WHERE Id = {_movie_uuid_expr(engine_name)}"

    def run_updates():
        affected = 0
        cur.execute("BEGIN")
        try:
            for movie_id in update_ids:
                cur.execute(
                    update_sql,
                    (123_456_789.0, _movie_id_value(engine_name, movie_id)),
                )
                if cur.rowcount > 0:
                    affected += cur.rowcount
            cur.execute("COMMIT")
            return affected
        except Exception:
            cur.execute("ROLLBACK")
            raise

    duration, affected = _time_movie_operation(
        engine_name,
        "MovieDB update box-office batch",
        len(update_ids),
        run_updates,
    )
    results["movie_update_batch_s"] = duration
    results["movie_update_batch_rows"] = affected

    delete_start = min(len(update_ids), len(movies))
    delete_ids = [
        row[0]
        for row in movies[delete_start : delete_start + min(delete_count, len(movies) - delete_start)]
    ]
    delete_sql = f"DELETE FROM Movies WHERE Id = {_movie_uuid_expr(engine_name)}"

    def run_deletes():
        affected = 0
        cur.execute("BEGIN")
        try:
            for movie_id in delete_ids:
                cur.execute(delete_sql, (_movie_id_value(engine_name, movie_id),))
                if cur.rowcount > 0:
                    affected += cur.rowcount
            cur.execute("COMMIT")
            return affected
        except Exception:
            cur.execute("ROLLBACK")
            raise

    duration, affected = _time_movie_operation(
        engine_name,
        "MovieDB delete movies cascade",
        len(delete_ids),
        run_deletes,
    )
    results["movie_delete_cascade_s"] = duration
    results["movie_delete_cascade_rows"] = affected

    duration, _ = _time_movie_operation(
        engine_name,
        "MovieDB checkpoint after mutations",
        0,
        lambda: _movie_checkpoint(conn, engine_name),
    )
    results["movie_checkpoint_after_mutations_s"] = duration

    duration, _ = _time_movie_operation(
        engine_name,
        "MovieDB vacuum/compact",
        0,
        lambda: _movie_vacuum(conn, engine_name, db_path),
    )
    results["movie_vacuum_s"] = duration

    cur.execute("SELECT COUNT(*) FROM Movies")
    counts["movies_after"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM Reviews")
    counts["reviews_after"] = cur.fetchone()[0]
    print(
        f"  Final counts: {counts['movies_after']:,} movies / "
        f"{counts['reviews_after']:,} reviews"
    )

    conn.close()
    results["movie_final_file_size_bytes"] = os.path.getsize(db_path) if os.path.exists(db_path) else 0
    print(
        f"  Final file size: {results['movie_final_file_size_bytes']:,} bytes "
        f"({results['movie_final_file_size_bytes'] / (1024.0 * 1024.0):.2f} MiB)"
    )

    if not keep_db:
        cleanup_db_files(db_path)
        remove_if_exists(db_path + ".vacuumed")

    results.update(counts)
    return results


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
            "name": "Point Lookup p50",
            "decent": d["point_lookup_p50_ms"],
            "sqlite": s["point_lookup_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Point Lookup p95",
            "decent": d["point_lookup_p95_ms"],
            "sqlite": s["point_lookup_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Range Scan p50",
            "decent": d["range_scan_p50_ms"],
            "sqlite": s["range_scan_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Range Scan p95",
            "decent": d["range_scan_p95_ms"],
            "sqlite": s["range_scan_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Join Query p50",
            "decent": d["join_p50_ms"],
            "sqlite": s["join_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Join Query p95",
            "decent": d["join_p95_ms"],
            "sqlite": s["join_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Aggregate Query p50",
            "decent": d["aggregate_p50_ms"],
            "sqlite": s["aggregate_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Aggregate Query p95",
            "decent": d["aggregate_p95_ms"],
            "sqlite": s["aggregate_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
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
        {
            "name": "Update p50",
            "decent": d["update_p50_ms"],
            "sqlite": s["update_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Update p95",
            "decent": d["update_p95_ms"],
            "sqlite": s["update_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Delete p50",
            "decent": d["delete_p50_ms"],
            "sqlite": s["delete_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Delete p95",
            "decent": d["delete_p95_ms"],
            "sqlite": s["delete_p95_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Full Table Scan p50",
            "decent": d["table_scan_p50_ms"],
            "sqlite": s["table_scan_p50_ms"],
            "unit": "ms",
            "higher_is_better": False,
            "fmt": ".6f",
        },
        {
            "name": "Full Table Scan p95",
            "decent": d["table_scan_p95_ms"],
            "sqlite": s["table_scan_p95_ms"],
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


def print_movie_comparison(results, *, tie_threshold=0.0):
    if "decentdb" not in results or "sqlite" not in results:
        return

    d = results["decentdb"]
    s = results["sqlite"]
    metrics = [
        ("MovieDB Bulk Load Time", "movie_bulk_load_s", "s", False, ".6f"),
        ("MovieDB Bulk Load throughput", "movie_bulk_load_rps", " rows/s", True, ".2f"),
        ("MovieDB Checkpoint", "movie_checkpoint_s", "s", False, ".6f"),
        ("MovieDB Point Reads", "movie_point_reads_s", "s", False, ".6f"),
        ("MovieDB Top-rated by year", "movie_top_rated_s", "s", False, ".6f"),
        ("MovieDB Search by tag", "movie_tag_search_s", "s", False, ".6f"),
        ("MovieDB Busiest people", "movie_busiest_people_s", "s", False, ".6f"),
        ("MovieDB Watchlist query", "movie_watchlist_s", "s", False, ".6f"),
        ("MovieDB Update batch", "movie_update_batch_s", "s", False, ".6f"),
        ("MovieDB Cascade delete batch", "movie_delete_cascade_s", "s", False, ".6f"),
        (
            "MovieDB Checkpoint after mutations",
            "movie_checkpoint_after_mutations_s",
            "s",
            False,
            ".6f",
        ),
        ("MovieDB Vacuum/compact", "movie_vacuum_s", "s", False, ".6f"),
        ("MovieDB Final file size", "movie_final_file_size_bytes", " bytes", False, ".0f"),
    ]

    decent_better = []
    sqlite_better = []
    ties = []
    for name, key, unit, higher_is_better, fmt in metrics:
        decent = d[key]
        sqlite = s[key]
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

    print("\n=== MovieDB Comparison (DecentDB vs SQLite) ===")
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


SHOWDOWN_GENRE_NAMES = [
    "Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary",
    "Drama", "Family", "Fantasy", "History", "Horror", "Music", "Mystery",
    "Romance", "Science Fiction", "Thriller", "War", "Western",
]

SHOWDOWN_FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
]

SHOWDOWN_LAST_NAMES = [
    "Anderson", "Bennett", "Carter", "Daniels", "Evans", "Foster", "Grant",
    "Harris", "Iverson", "Jenkins", "Keller", "Lawrence", "Mitchell",
    "Nelson", "Owens", "Parker", "Quinn", "Reynolds", "Sullivan", "Thompson",
]

SHOWDOWN_TITLE_WORDS = [
    "Last", "First", "Eternal", "Hidden", "Broken", "Silver", "Golden",
    "Crimson", "Midnight", "Shadow", "Forgotten", "Lost", "Final", "Dark",
    "Bright", "Silent", "Wild", "Brave", "Royal", "Secret", "Endless",
    "Storm", "Thunder", "Dawn", "Dusk", "Reckoning", "Genesis", "Protocol",
    "Paradox", "Horizon", "Legacy", "Empire", "Kingdom", "Rebellion",
]

SHOWDOWN_NOUNS = [
    "Dawn", "Empire", "Protocol", "Reckoning", "Legacy", "Horizon", "Code",
    "Gate", "Circle", "Crown", "Veil", "Storm", "Fire", "Ice", "Light",
    "Shadow", "River", "Mountain", "City", "Road", "War", "Treaty", "Pact",
    "Vow", "Promise", "Quest", "Journey", "Return", "Rising", "Fall",
    "Awakening", "Conspiracy", "Mirage", "Echo", "Genesis", "Paradox",
]

SHOWDOWN_REVIEW_ADJECTIVES = [
    "stunning", "boring", "thrilling", "predictable", "breathtaking",
    "forgettable", "masterful", "mediocre", "riveting", "disappointing",
    "brilliant", "tedious", "hilarious", "dull", "mesmerizing", "weak",
    "powerful", "formulaic", "electric", "lifeless",
]

SHOWDOWN_REVIEW_NOUNS = [
    "performances", "pacing", "cinematography", "score", "script", "ending",
    "plot", "visuals", "dialogue", "action", "tension", "direction",
    "characters", "world-building", "set pieces", "sound design",
]

SHOWDOWN_KEYWORD_TERMS = [
    "time travel", "artificial intelligence", "space", "war", "love",
    "betrayal", "revenge", "family", "friendship", "survival", "magic",
    "robot", "alien", "spy", "heist", "courtroom", "escape", "disaster",
    "island", "detective", "vampire", "zombie", "dragon", "ghost",
    "amnesia", "undercover", "witness", "rivalry", "redemption", "sacrifice",
]

SHOWDOWN_COLLECTIONS = [
    "", "", "", "Saga Collection", "Anthology", "Trilogy Box",
    "Director Series", "Universe", "Chronicles", "Tales",
]

SHOWDOWN_STATUSES = ["Released", "Post Production", "Rumored", "Planned"]
SHOWDOWN_MPA_RATINGS = ["G", "PG", "PG-13", "R", "NC-17", "NR"]
SHOWDOWN_CHAR_FIRST = [
    "Alex", "Sam", "Jordan", "Casey", "Taylor", "Morgan", "Riley", "Quinn",
    "Avery", "Drew", "Reese", "Skyler", "Hayden", "Parker", "Rowan",
]
SHOWDOWN_CHAR_LAST = [
    "Stone", "Cross", "Vance", "Hayes", "Reed", "Cole", "West", "Lane",
    "Kane", "Mercer", "Sloan", "Drake", "Bishop", "Hart",
]


def _showdown_int_type(engine_name):
    return "INTEGER" if engine_name == "sqlite" else "INT"


def _showdown_int64_type(engine_name):
    return "INTEGER" if engine_name == "sqlite" else "INT64"


def _showdown_float_type(engine_name):
    return "REAL" if engine_name == "sqlite" else "FLOAT64"


def _showdown_date_type(engine_name):
    return "TEXT" if engine_name == "sqlite" else "DATE"


def _showdown_timestamp_type(engine_name):
    return "TEXT" if engine_name == "sqlite" else "TIMESTAMP"


def _showdown_date_param(engine_name):
    return "?" if engine_name == "sqlite" else "CAST(? AS DATE)"


def _showdown_timestamp_param(engine_name):
    return "?" if engine_name == "sqlite" else "CAST(? AS TIMESTAMP)"


def setup_showdown_schema(conn, engine_name):
    int_type = _showdown_int_type(engine_name)
    int64_type = _showdown_int64_type(engine_name)
    float_type = _showdown_float_type(engine_name)
    date_type = _showdown_date_type(engine_name)
    timestamp_type = _showdown_timestamp_type(engine_name)

    if engine_name == "sqlite":
        conn.execute("PRAGMA foreign_keys=ON")

    ddl = f"""
        CREATE TABLE people (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            born {date_type} NOT NULL,
            birthplace TEXT
        );
        CREATE TABLE movies (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            overview TEXT NOT NULL,
            released {date_type} NOT NULL,
            budget_cents {int64_type} NOT NULL,
            revenue_cents {int64_type} NOT NULL,
            runtime_minutes {int_type} NOT NULL,
            status TEXT NOT NULL,
            mpa_rating TEXT NOT NULL,
            rating {float_type} NOT NULL,
            vote_count {int_type} NOT NULL,
            collection TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE genres (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        CREATE TABLE movie_genres (
            movie_id {int_type} NOT NULL REFERENCES movies(id),
            genre_id {int_type} NOT NULL REFERENCES genres(id),
            PRIMARY KEY (movie_id, genre_id)
        );
        CREATE TABLE roles (
            id INTEGER PRIMARY KEY,
            movie_id {int_type} NOT NULL REFERENCES movies(id),
            person_id {int_type} NOT NULL REFERENCES people(id),
            character TEXT NOT NULL DEFAULT '',
            department TEXT NOT NULL,
            job TEXT NOT NULL,
            billing_order {int_type} NOT NULL DEFAULT 0
        );
        CREATE TABLE reviews (
            id INTEGER PRIMARY KEY,
            movie_id {int_type} NOT NULL REFERENCES movies(id),
            author TEXT NOT NULL,
            score {int_type} NOT NULL CHECK (score BETWEEN 1 AND 10),
            body TEXT NOT NULL,
            created_at {timestamp_type} NOT NULL
        );
        CREATE TABLE keywords (
            id INTEGER PRIMARY KEY,
            term TEXT NOT NULL UNIQUE
        );
        CREATE TABLE movie_keywords (
            movie_id {int_type} NOT NULL REFERENCES movies(id),
            keyword_id {int_type} NOT NULL REFERENCES keywords(id),
            PRIMARY KEY (movie_id, keyword_id)
        );
    """
    _execute_script_statements(conn, ddl)


def setup_showdown_indexes(conn):
    ddl = """
        CREATE INDEX idx_movies_released ON movies(released);
        CREATE INDEX idx_movies_rating ON movies(rating);
        CREATE INDEX idx_movies_status ON movies(status);
        CREATE INDEX idx_movies_collection ON movies(collection) WHERE collection <> '';
        CREATE INDEX idx_people_name ON people(name);
        CREATE INDEX idx_roles_movie ON roles(movie_id);
        CREATE INDEX idx_roles_person ON roles(person_id);
        CREATE INDEX idx_roles_dept_job ON roles(department, job);
        CREATE INDEX idx_reviews_movie ON reviews(movie_id);
        CREATE INDEX idx_reviews_author ON reviews(author);
        CREATE INDEX idx_reviews_score ON reviews(score);
        CREATE INDEX idx_reviews_created ON reviews(created_at);
        CREATE INDEX idx_mgenres_genre ON movie_genres(genre_id);
        CREATE INDEX idx_mkeywords_keyword ON movie_keywords(keyword_id);
    """
    _execute_script_statements(conn, ddl)


def setup_showdown_search_indexes(conn, engine_name):
    if engine_name == "decentdb":
        ddl = """
            CREATE INDEX idx_movies_search_ft ON movies USING fulltext(title, overview) WITH (prefix='2,3');
            CREATE INDEX idx_reviews_body_ft ON reviews USING fulltext(body) WITH (prefix='2,3');
        """
        _execute_script_statements(conn, ddl)
        return

    cur = conn.cursor()
    cur.execute(
        "CREATE VIRTUAL TABLE movies_fts USING fts5("
        "title, overview, content='movies', content_rowid='id', "
        "tokenize='porter unicode61', prefix='2 3')"
    )
    cur.execute(
        "CREATE VIRTUAL TABLE reviews_fts USING fts5("
        "body, content='reviews', content_rowid='id', "
        "tokenize='porter unicode61', prefix='2 3')"
    )
    cur.execute("INSERT INTO movies_fts(movies_fts) VALUES('rebuild')")
    cur.execute("INSERT INTO reviews_fts(reviews_fts) VALUES('rebuild')")
    cur.executescript(
        """
        CREATE TRIGGER movies_fts_ai AFTER INSERT ON movies BEGIN
            INSERT INTO movies_fts(rowid, title, overview)
            VALUES (new.id, new.title, new.overview);
        END;
        CREATE TRIGGER movies_fts_ad AFTER DELETE ON movies BEGIN
            INSERT INTO movies_fts(movies_fts, rowid, title, overview)
            VALUES('delete', old.id, old.title, old.overview);
        END;
        CREATE TRIGGER movies_fts_au AFTER UPDATE OF title, overview ON movies BEGIN
            INSERT INTO movies_fts(movies_fts, rowid, title, overview)
            VALUES('delete', old.id, old.title, old.overview);
            INSERT INTO movies_fts(rowid, title, overview)
            VALUES (new.id, new.title, new.overview);
        END;
        CREATE TRIGGER reviews_fts_ai AFTER INSERT ON reviews BEGIN
            INSERT INTO reviews_fts(rowid, body)
            VALUES (new.id, new.body);
        END;
        CREATE TRIGGER reviews_fts_ad AFTER DELETE ON reviews BEGIN
            INSERT INTO reviews_fts(reviews_fts, rowid, body)
            VALUES('delete', old.id, old.body);
        END;
        CREATE TRIGGER reviews_fts_au AFTER UPDATE OF body ON reviews BEGIN
            INSERT INTO reviews_fts(reviews_fts, rowid, body)
            VALUES('delete', old.id, old.body);
            INSERT INTO reviews_fts(rowid, body)
            VALUES (new.id, new.body);
        END;
        """
    )
    conn.commit()


def _showdown_make_title(rng):
    form = rng.randrange(6)
    if form == 0:
        return f"{rng.choice(SHOWDOWN_TITLE_WORDS)} {rng.choice(SHOWDOWN_NOUNS)}"
    if form == 1:
        return f"The {rng.choice(SHOWDOWN_NOUNS)}"
    if form == 2:
        return f"{rng.choice(SHOWDOWN_NOUNS)} of {rng.choice(SHOWDOWN_TITLE_WORDS)}"
    if form == 3:
        return (
            f"{rng.choice(SHOWDOWN_TITLE_WORDS)} {rng.choice(SHOWDOWN_NOUNS)}: "
            f"{rng.choice(SHOWDOWN_NOUNS)}"
        )
    if form == 4:
        return f"A {rng.choice(SHOWDOWN_TITLE_WORDS)} {rng.choice(SHOWDOWN_NOUNS)}"
    return f"{rng.choice(SHOWDOWN_NOUNS)} {rng.randrange(2, 6)}"


def _showdown_make_overview(rng, force_search_terms=False):
    sentences = []
    for _ in range(3 + rng.randrange(4)):
        if rng.random() < 0.5:
            sentences.append(
                "In a world of "
                f"{rng.choice(SHOWDOWN_NOUNS).lower()}, a reluctant hero confronts "
                f"the {rng.choice(SHOWDOWN_REVIEW_ADJECTIVES)} truth behind the "
                f"{rng.choice(SHOWDOWN_NOUNS).lower()}."
            )
        else:
            sentences.append(
                f"When the {rng.choice(SHOWDOWN_NOUNS).lower()} threatens everything, "
                f"an unlikely alliance races to protect the "
                f"{rng.choice(SHOWDOWN_NOUNS).lower()} before dawn."
            )
    if force_search_terms:
        sentences.append("War, revenge, and sacrifice reshape every choice.")
    return " ".join(sentences)


def _showdown_make_review(rng, force_search_terms=False):
    adj = rng.choice(SHOWDOWN_REVIEW_ADJECTIVES)
    noun = rng.choice(SHOWDOWN_REVIEW_NOUNS)
    adj2 = rng.choice(SHOWDOWN_REVIEW_ADJECTIVES)
    noun2 = rng.choice(SHOWDOWN_REVIEW_NOUNS)
    text = (
        f"A {adj} film elevated by its {noun}. "
        f"Despite {adj2} {noun2}, every frame has intent."
    )
    if force_search_terms:
        text += " War and revenge give the sacrifice real weight."
    return text


def _showdown_city(rng):
    return rng.choice([
        "Springfield", "Riverdale", "Fairview", "Kingston",
        "Madison", "Georgetown", "Ashford", "Westbrook",
    ])


def _showdown_state(rng):
    return rng.choice(["CA", "NY", "TX", "IL", "GA", "WA", "MA", "CO"])


def _showdown_date(year, days):
    return (_dt.date(year, 1, 1) + _dt.timedelta(days=days)).isoformat()


def _showdown_timestamp(days, seconds):
    return (
        _dt.datetime(2000, 1, 1) + _dt.timedelta(days=days, seconds=seconds)
    ).strftime("%Y-%m-%d %H:%M:%S")


def generate_showdown_data(movies_count, people_multiplier, reviews_per_movie, seed):
    if movies_count <= 0:
        raise ValueError("Showdown benchmark requires at least one movie")
    if people_multiplier <= 0:
        raise ValueError("Showdown benchmark requires a positive people multiplier")
    if reviews_per_movie < 0:
        raise ValueError("Showdown reviews per movie cannot be negative")

    rng = random.Random(seed)
    people_count = movies_count * people_multiplier
    data = {
        "people": [],
        "movies": [],
        "genres": [(i + 1, name) for i, name in enumerate(SHOWDOWN_GENRE_NAMES)],
        "movie_genres": [],
        "roles": [],
        "reviews": [],
        "keywords": [(i + 1, term) for i, term in enumerate(SHOWDOWN_KEYWORD_TERMS)],
        "movie_keywords": [],
    }

    for person_id in range(1, people_count + 1):
        name = f"{rng.choice(SHOWDOWN_FIRST_NAMES)} {rng.choice(SHOWDOWN_LAST_NAMES)}"
        data["people"].append(
            (
                person_id,
                name,
                _showdown_date(1940, rng.randrange(28 * 365)),
                f"{_showdown_city(rng)},{_showdown_state(rng)}",
            )
        )

    for movie_id in range(1, movies_count + 1):
        released = _showdown_date(1960, rng.randrange(63 * 365))
        budget_cents = int(rng.random() * 300_000_000) * 100
        revenue_cents = int(rng.random() * 1_200_000_000) * 100
        status = (
            "Released"
            if rng.random() < 0.85
            else rng.choice(SHOWDOWN_STATUSES[1:])
        )
        data["movies"].append(
            (
                movie_id,
                f"{_showdown_make_title(rng)} {movie_id:05d}",
                _showdown_make_overview(rng, movie_id % 17 == 0),
                released,
                budget_cents,
                revenue_cents,
                75 + rng.randrange(135),
                status,
                rng.choice(SHOWDOWN_MPA_RATINGS),
                round(rng.random() * 9.0 + 1.0, 1),
                rng.randrange(50, 500_000),
                rng.choice(SHOWDOWN_COLLECTIONS),
            )
        )

    for movie_id in range(1, movies_count + 1):
        genre_count = 2 + rng.randrange(3)
        for genre_id in rng.sample(range(1, len(data["genres"]) + 1), genre_count):
            data["movie_genres"].append((movie_id, genre_id))

    role_id = 0
    for movie_id in range(1, movies_count + 1):
        role_id += 1
        data["roles"].append(
            (role_id, movie_id, rng.randrange(1, people_count + 1), "", "Directing", "Director", 0)
        )
        role_id += 1
        data["roles"].append(
            (role_id, movie_id, rng.randrange(1, people_count + 1), "", "Writing", "Screenplay", 0)
        )
        cast_count = 8 + rng.randrange(8)
        cast_people = rng.sample(range(1, people_count + 1), min(cast_count, people_count))
        for billing_order, person_id in enumerate(cast_people, start=1):
            role_id += 1
            character = f"{rng.choice(SHOWDOWN_CHAR_FIRST)} {rng.choice(SHOWDOWN_CHAR_LAST)}"
            data["roles"].append(
                (role_id, movie_id, person_id, character, "Acting", "Actor", billing_order)
            )

    review_id = 0
    for movie_id in range(1, movies_count + 1):
        if reviews_per_movie == 0 or rng.random() < 0.12:
            review_count = 0
        else:
            review_count = 1 + rng.randrange(reviews_per_movie)
        for _ in range(review_count):
            review_id += 1
            data["reviews"].append(
                (
                    review_id,
                    movie_id,
                    f"{rng.choice(SHOWDOWN_FIRST_NAMES).lower()}{rng.randrange(1, 9999)}",
                    1 + rng.randrange(10),
                    _showdown_make_review(rng, review_id % 19 == 0),
                    _showdown_timestamp(rng.randrange(9000), rng.randrange(86400)),
                )
            )

    for movie_id in range(1, movies_count + 1):
        keyword_count = 1 + rng.randrange(5)
        for keyword_id in rng.sample(range(1, len(data["keywords"]) + 1), keyword_count):
            data["movie_keywords"].append((movie_id, keyword_id))

    return data


def showdown_total_rows(data):
    return sum(len(rows) for rows in data.values())


def _showdown_insert_all(cur, engine_name, data):
    date_param = _showdown_date_param(engine_name)
    timestamp_param = _showdown_timestamp_param(engine_name)
    cur.execute("BEGIN")
    try:
        cur.executemany(
            f"INSERT INTO people (id, name, born, birthplace) VALUES (?, ?, {date_param}, ?)",
            data["people"],
        )
        cur.executemany(
            "INSERT INTO genres (id, name) VALUES (?, ?)",
            data["genres"],
        )
        cur.executemany(
            f"""
            INSERT INTO movies (
                id, title, overview, released, budget_cents, revenue_cents,
                runtime_minutes, status, mpa_rating, rating, vote_count, collection
            ) VALUES (?, ?, ?, {date_param}, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data["movies"],
        )
        cur.executemany(
            "INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?)",
            data["movie_genres"],
        )
        cur.executemany(
            "INSERT INTO keywords (id, term) VALUES (?, ?)",
            data["keywords"],
        )
        cur.executemany(
            """
            INSERT INTO roles (
                id, movie_id, person_id, character, department, job, billing_order
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            data["roles"],
        )
        cur.executemany(
            f"""
            INSERT INTO reviews (id, movie_id, author, score, body, created_at)
            VALUES (?, ?, ?, ?, ?, {timestamp_param})
            """,
            data["reviews"],
        )
        cur.executemany(
            "INSERT INTO movie_keywords (movie_id, keyword_id) VALUES (?, ?)",
            data["movie_keywords"],
        )
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise


def _showdown_fetch_count(cur, sql, params=()):
    cur.execute(sql, params)
    return len(cur.fetchall())


def _showdown_time_query(
    engine_name,
    cur,
    label,
    sql,
    results,
    key,
    params=(),
    note=None,
    explain_output_dir=None,
    explain_analyze=False,
    query_mode="warm",
    compare_columns=None,
):
    return _time_query_with_mode(
        engine_name=engine_name,
        cur=cur,
        workload="showdown",
        label=label,
        sql=sql,
        results=results,
        metric_key=key,
        check_key=key,
        params=params,
        query_mode=query_mode,
        note=note,
        explain_output_dir=explain_output_dir,
        explain_analyze=explain_analyze,
        print_rows=True,
        compare_columns=compare_columns,
    )


def _showdown_skip(results, key, label, exc):
    print(f"  {label:<38} skipped: {exc}")
    results[key] = None
    results[key + "_error"] = str(exc)


def _showdown_try_query(
    engine_name,
    cur,
    label,
    sql,
    results,
    key,
    params=(),
    note=None,
    explain_output_dir=None,
    explain_analyze=False,
    query_mode="warm",
    compare_columns=None,
):
    try:
        return _showdown_time_query(
            engine_name,
            cur,
            label,
            sql,
            results,
            key,
            params,
            note,
            explain_output_dir,
            explain_analyze,
            query_mode,
            compare_columns,
        )
    except Exception as exc:
        _showdown_skip(results, key, label, exc)
        return None, 0


def _showdown_exec(cur, sql, params=()):
    cur.execute(sql, params)
    try:
        return len(cur.fetchall())
    except Exception:
        return 0


def _showdown_commit_if_supported(conn):
    commit = getattr(conn, "commit", None)
    if callable(commit):
        try:
            commit()
        except Exception:
            pass


def _showdown_rollback_if_supported(conn):
    rollback = getattr(conn, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            pass


def run_showdown_benchmark(
    engine_name,
    db_path,
    data,
    *,
    point_reads,
    keep_db,
    decentdb_options,
    decentdb_stmt_cache_size,
    sqlite_profile,
    sqlite_cache_mb,
    explain_output_dir=None,
    explain_analyze=False,
    query_mode="warm",
):
    cleanup_db_files(db_path)
    print(f"\n=== {engine_name} Showdown ===")

    if engine_name == "decentdb":
        lib = load_decentdb_library()
        lib_path = getattr(lib, "_name", "<unknown>")
        print(f"DecentDB native library: {lib_path}")
        print(
            "DecentDB options: "
            f"{decentdb_options or '<default>'}; stmt_cache_size={decentdb_stmt_cache_size}"
        )
        conn = setup_decentdb(
            db_path,
            options=decentdb_options,
            stmt_cache_size=decentdb_stmt_cache_size,
            initialize_complex=False,
        )
    elif engine_name == "sqlite":
        print(f"SQLite profile: {sqlite_profile}; cache_mb={sqlite_cache_mb}")
        conn = setup_sqlite(
            db_path,
            profile=sqlite_profile,
            cache_mb=sqlite_cache_mb,
            initialize_complex=False,
        )
        conn.execute("PRAGMA page_size=4096")
        conn.execute("PRAGMA mmap_size=268435456")
    else:
        raise ValueError(f"Unknown engine: {engine_name}")

    print("Initializing Showdown schema...")
    setup_showdown_schema(conn, engine_name)
    cur = conn.cursor()
    results = {
        "_metadata": {
            "engine": engine_name,
            "db_path": db_path,
            "profile": _engine_profile_metadata(
                engine_name,
                decentdb_options=decentdb_options,
                decentdb_stmt_cache_size=decentdb_stmt_cache_size,
                sqlite_profile=sqlite_profile,
                sqlite_cache_mb=sqlite_cache_mb,
            ),
            "query_mode": query_mode,
        }
    }
    total_rows = showdown_total_rows(data)

    duration, _ = _time_movie_operation(
        engine_name,
        "Showdown bulk load",
        total_rows,
        lambda: _showdown_insert_all(cur, engine_name, data),
    )
    results["showdown_bulk_load_s"] = duration
    results["showdown_bulk_load_rps"] = total_rows / duration if duration else 0.0

    duration, _ = _time_movie_operation(
        engine_name,
        "Showdown btree index build",
        0,
        lambda: setup_showdown_indexes(conn),
    )
    results["showdown_index_build_s"] = duration

    duration, _ = _time_movie_operation(
        engine_name,
        "Showdown search index build",
        0,
        lambda: setup_showdown_search_indexes(conn, engine_name),
    )
    results["showdown_search_index_build_s"] = duration

    try:
        duration, _ = _time_movie_operation(
            engine_name,
            "Showdown ANALYZE",
            0,
            lambda: cur.execute("ANALYZE"),
        )
        results["showdown_analyze_s"] = duration
    except Exception as exc:
        _showdown_skip(results, "showdown_analyze_s", "Showdown ANALYZE", exc)

    cur.execute("SELECT COUNT(*) FROM movies")
    results["showdown_movies"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM reviews")
    results["showdown_reviews"] = cur.fetchone()[0]
    print(
        f"  Loaded {results['showdown_movies']:,} movies / "
        f"{results['showdown_reviews']:,} reviews"
    )

    point_limit = min(point_reads, len(data["movies"]))
    point_ids = list(range(1, point_limit + 1))
    point_sql = "SELECT id, title, rating, runtime_minutes FROM movies WHERE id = ?"

    def run_point_lookups():
        total = 0
        for movie_id in point_ids:
            cur.execute(point_sql, (movie_id,))
            total += len(cur.fetchall())
        return total

    if query_mode in ("cold", "both"):
        cold_label = (
            "Showdown point lookup by PK"
            if query_mode == "cold"
            else "Showdown point lookup by PK [cold]"
        )
        duration, total = _time_movie_operation(
            engine_name,
            cold_label,
            len(point_ids),
            run_point_lookups,
        )
        print(f"    rows={total:,}")
        if query_mode == "cold":
            results["showdown_point_lookup_s"] = duration
            results["showdown_point_lookup_rows"] = total
        else:
            results["showdown_point_lookup_cold_s"] = duration
            results["showdown_point_lookup_cold_rows"] = total
        cold_rows = _fetch_rows(cur, point_sql, (1,))
        _record_query_signature(
            results,
            "showdown_point_lookup_s" if query_mode == "cold" else "showdown_point_lookup_cold",
            cold_label,
            cold_rows,
        )

    if query_mode in ("warm", "both"):
        warm_rows = _fetch_rows(cur, point_sql, (1,))
        _record_query_signature(
            results,
            "showdown_point_lookup_s",
            "Showdown point lookup by PK",
            warm_rows,
        )
        duration, total = _time_movie_operation(
            engine_name,
            "Showdown point lookup by PK",
            len(point_ids),
            run_point_lookups,
        )
        print(f"    rows={total:,}")
        results["showdown_point_lookup_s"] = duration
        results["showdown_point_lookup_rows"] = total

    year_expr = "strftime('%Y', released)"
    decade_expr = f"(CAST({year_expr} AS INTEGER) / 10 * 10)"
    date_2010 = "'2010-01-01'" if engine_name == "sqlite" else "CAST('2010-01-01' AS DATE)"

    scenarios = [
        (
            "Showdown full table scan",
            "showdown_full_scan_s",
            "SELECT id, title, rating, runtime_minutes, vote_count FROM movies",
            (),
            None,
        ),
        (
            "Showdown filtered range scan",
            "showdown_filtered_range_s",
            "SELECT id, title, rating FROM movies WHERE rating >= 7.5 AND rating <= 9.0 AND runtime_minutes > 120",
            (),
            None,
        ),
        (
            "Showdown index range/order/limit",
            "showdown_index_range_order_s",
            f"SELECT id, title, rating, released FROM movies WHERE released >= {date_2010} ORDER BY rating DESC, id ASC LIMIT 50",
            (),
            None,
        ),
        (
            "Showdown keyset pagination",
            "showdown_keyset_pagination_s",
            "SELECT id, title, rating FROM movies WHERE id > 500 ORDER BY id LIMIT 25",
            (),
            None,
        ),
        (
            "Showdown offset pagination",
            "showdown_offset_pagination_s",
            "SELECT id, title, rating FROM movies ORDER BY id LIMIT 25 OFFSET 500",
            (),
            None,
        ),
        (
            "Showdown movie genres join",
            "showdown_movie_genres_join_s",
            """
            SELECT m.id, m.title, g.name
            FROM movies m
            JOIN movie_genres mg ON mg.movie_id = m.id
            JOIN genres g ON g.id = mg.genre_id
            ORDER BY m.id
            """,
            (),
            "3-table join",
        ),
        (
            "Showdown cast/crew join",
            "showdown_cast_crew_join_s",
            """
            SELECT m.id, m.title, p.name, r.character, r.job, r.billing_order
            FROM movies m
            JOIN roles r ON r.movie_id = m.id
            JOIN people p ON p.id = r.person_id
            ORDER BY m.id, r.billing_order
            """,
            (),
            "3-table join",
        ),
        (
            "Showdown review aggregate join",
            "showdown_review_aggregate_join_s",
            """
            SELECT m.id, m.title, m.rating,
                   COUNT(r.id) AS review_count,
                   AVG(r.score) AS avg_review,
                   MIN(r.score) AS min_score,
                   MAX(r.score) AS max_score
            FROM movies m
            LEFT JOIN reviews r ON r.movie_id = m.id
            GROUP BY m.id, m.title, m.rating
            ORDER BY m.id
            """,
            (),
            "LEFT JOIN + GROUP BY",
        ),
        (
            "Showdown person filmography",
            "showdown_person_filmography_s",
            """
            SELECT p.id, p.name, COUNT(DISTINCT r.movie_id) AS films, COUNT(*) AS roles
            FROM people p
            JOIN roles r ON r.person_id = p.id
            GROUP BY p.id, p.name
            ORDER BY films DESC, p.id
            LIMIT 50
            """,
            (),
            "COUNT DISTINCT",
        ),
        (
            "Showdown genre popularity",
            "showdown_genre_popularity_s",
            """
            SELECT g.name, COUNT(*) AS movie_count, AVG(m.rating) AS avg_rating
            FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.id
            JOIN movies m ON m.id = mg.movie_id
            GROUP BY g.name
            ORDER BY movie_count DESC, g.name
            """,
            (),
            "GROUP BY + AVG",
        ),
        (
            "Showdown yearly counts",
            "showdown_yearly_counts_s",
            f"""
            SELECT {year_expr} AS yr, COUNT(*) AS cnt
            FROM movies
            GROUP BY {year_expr}
            ORDER BY yr
            """,
            (),
            "strftime + GROUP BY",
        ),
        (
            "Showdown top by decade",
            "showdown_top_by_decade_s",
            f"""
            SELECT {decade_expr} AS decade,
                   COUNT(*) AS films, AVG(rating) AS avg_rating
            FROM movies
            WHERE status = 'Released'
            GROUP BY {decade_expr}
            ORDER BY decade
            """,
            (),
            "computed GROUP key",
        ),
        (
            "Showdown review ranking",
            "showdown_review_ranking_s",
            """
            SELECT movie_id, score, author,
                   RANK() OVER (PARTITION BY movie_id ORDER BY score DESC) AS rk,
                   DENSE_RANK() OVER (PARTITION BY movie_id ORDER BY score DESC) AS drk
            FROM reviews
            ORDER BY movie_id, rk
            """,
            (),
            "RANK/DENSE_RANK",
        ),
        (
            "Showdown cast billing window",
            "showdown_cast_billing_window_s",
            """
            SELECT movie_id, person_id, billing_order,
                   ROW_NUMBER() OVER (PARTITION BY movie_id ORDER BY billing_order) AS rn,
                   LAG(billing_order) OVER (PARTITION BY movie_id ORDER BY billing_order) AS prev
            FROM roles
            WHERE department = 'Acting'
            ORDER BY movie_id, rn
            """,
            (),
            "ROW_NUMBER/LAG",
        ),
        (
            "Showdown recursive CTE",
            "showdown_recursive_cte_s",
            """
            WITH RECURSIVE series(n) AS (
                SELECT 1
                UNION ALL
                SELECT n + 1 FROM series WHERE n < 100
            )
            SELECT n FROM series
            """,
            (),
            "1..100",
        ),
        (
            "Showdown directors CTE",
            "showdown_directors_cte_s",
            """
            WITH directed AS (
                SELECT r.person_id, r.movie_id, m.title, m.rating
                FROM roles r
                JOIN movies m ON m.id = r.movie_id
                WHERE r.job = 'Director'
            ),
            top_dirs AS (
                SELECT person_id, COUNT(*) AS films, AVG(rating) AS avg_rating
                FROM directed
                GROUP BY person_id
                HAVING COUNT(*) >= 2
            )
            SELECT d.person_id, d.films, d.avg_rating,
                   STRING_AGG(dir.title, ', ') AS titles
            FROM top_dirs d
            JOIN directed dir ON dir.person_id = d.person_id
            GROUP BY d.person_id, d.films, d.avg_rating
            ORDER BY d.avg_rating DESC
            LIMIT 20
            """,
            (),
            "multi-CTE + STRING_AGG",
        ),
        (
            "Showdown substring LIKE",
            "showdown_substring_like_s",
            "SELECT id, title FROM movies WHERE title LIKE '%Shadow%'",
            (),
            "DecentDB trigram vs SQLite scan",
        ),
        (
            "Showdown UNION",
            "showdown_union_s",
            """
            SELECT genre_id FROM movie_genres WHERE genre_id <= 6
            UNION
            SELECT genre_id FROM movie_genres WHERE genre_id >= 13
            ORDER BY genre_id
            """,
            (),
            None,
        ),
        (
            "Showdown rolling avg frame",
            "showdown_rolling_average_s",
            """
            SELECT id, rating,
                   AVG(rating) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling
            FROM movies
            ORDER BY id
            """,
            (),
            "ROWS BETWEEN frame",
        ),
    ]

    for label, key, sql, params, note in scenarios:
        _showdown_try_query(
            engine_name,
            cur,
            label,
            sql,
            results,
            key,
            params,
            note,
            explain_output_dir=explain_output_dir,
            explain_analyze=explain_analyze,
            query_mode=query_mode,
        )

    if engine_name == "decentdb":
        fts_sql = """
            SELECT id, title, bm25('idx_movies_search_ft') AS rank
            FROM movies
            WHERE fulltext_match('idx_movies_search_ft', ?)
            ORDER BY rank DESC
            LIMIT 50
        """
    else:
        fts_sql = """
            SELECT m.id, m.title, bm25(movies_fts) AS rank
            FROM movies_fts
            JOIN movies m ON m.id = movies_fts.rowid
            WHERE movies_fts MATCH ?
            ORDER BY rank
            LIMIT 50
        """
    _showdown_try_query(
        engine_name,
        cur,
        "Showdown fulltext BM25",
        fts_sql,
        results,
        "showdown_fulltext_bm25_s",
        ("war OR revenge OR sacrifice",),
        "fulltext index",
        explain_output_dir=explain_output_dir,
        explain_analyze=explain_analyze,
        query_mode=query_mode,
        compare_columns=(0, 1),
    )

    insert_date = _showdown_date_param(engine_name)
    start_id = len(data["movies"]) + 10_000
    cur.execute(f"DELETE FROM movies WHERE id >= {start_id} AND id < {start_id + 100}")
    _showdown_commit_if_supported(conn)
    insert_sql = f"""
        INSERT INTO movies (
            id, title, overview, released, budget_cents, revenue_cents,
            runtime_minutes, status, mpa_rating, rating, vote_count, collection
        ) VALUES (?, ?, ?, {insert_date}, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id, title
    """

    def run_insert_returning():
        total = 0
        cur.execute("BEGIN")
        try:
            for i in range(100):
                cur.execute(
                    insert_sql,
                    (
                        start_id + i,
                        f"RETURNING Test {i}",
                        "RETURNING benchmark row",
                        "2024-01-01",
                        100_000_000,
                        500_000_000,
                        120,
                        "Released",
                        "PG-13",
                        7.5,
                        100,
                        "",
                    ),
                )
                total += len(cur.fetchall())
            cur.execute("COMMIT")
            return total
        except Exception:
            cur.execute("ROLLBACK")
            raise

    try:
        duration, rows = _time_movie_operation(
            engine_name,
            "Showdown INSERT RETURNING",
            100,
            run_insert_returning,
        )
        print(f"    rows={rows:,}")
        results["showdown_insert_returning_s"] = duration
        results["showdown_insert_returning_rows"] = rows
    except Exception as exc:
        _showdown_rollback_if_supported(conn)
        _showdown_skip(results, "showdown_insert_returning_s", "Showdown INSERT RETURNING", exc)
    cur.execute(f"DELETE FROM movies WHERE id >= {start_id}")
    _showdown_commit_if_supported(conn)

    def run_update_returning():
        cur.execute("BEGIN")
        try:
            cur.execute(
                """
                UPDATE movies SET rating = rating + 0.01
                WHERE id BETWEEN 1 AND 100
                RETURNING id, rating
                """
            )
            rows = len(cur.fetchall())
            cur.execute("UPDATE movies SET rating = rating - 0.01 WHERE id BETWEEN 1 AND 100")
            cur.execute("COMMIT")
            return rows
        except Exception:
            cur.execute("ROLLBACK")
            raise

    try:
        duration, rows = _time_movie_operation(
            engine_name,
            "Showdown UPDATE RETURNING",
            100,
            run_update_returning,
        )
        print(f"    rows={rows:,}")
        results["showdown_update_returning_s"] = duration
        results["showdown_update_returning_rows"] = rows
    except Exception as exc:
        _showdown_rollback_if_supported(conn)
        _showdown_skip(results, "showdown_update_returning_s", "Showdown UPDATE RETURNING", exc)

    try:
        duration, rows = _time_movie_operation(
            engine_name,
            "Showdown UPSERT",
            0,
            lambda: _showdown_exec(
                cur,
                """
                INSERT INTO genres (id, name) VALUES (1, 'Action')
                ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
                """,
            ),
        )
        print(f"    rows={rows:,}")
        results["showdown_upsert_s"] = duration
        results["showdown_upsert_rows"] = rows
        _showdown_commit_if_supported(conn)
    except Exception as exc:
        _showdown_rollback_if_supported(conn)
        _showdown_skip(results, "showdown_upsert_s", "Showdown UPSERT", exc)

    cur.execute("SELECT COUNT(*) FROM movies WHERE status = 'Released'")
    released_count = cur.fetchone()[0]

    def run_bulk_update():
        cur.execute("BEGIN")
        try:
            cur.execute("UPDATE movies SET vote_count = vote_count + 1 WHERE status = 'Released'")
            cur.execute("UPDATE movies SET vote_count = vote_count - 1 WHERE status = 'Released'")
            cur.execute("COMMIT")
            return released_count
        except Exception:
            cur.execute("ROLLBACK")
            raise

    duration, rows = _time_movie_operation(
        engine_name,
        "Showdown bulk UPDATE",
        released_count,
        run_bulk_update,
    )
    print(f"    rows={rows:,}")
    results["showdown_bulk_update_s"] = duration
    results["showdown_bulk_update_rows"] = rows

    base_delete_id = len(data["movies"]) + 20_000
    delete_insert_sql = f"""
        INSERT INTO movies (
            id, title, overview, released, budget_cents, revenue_cents,
            runtime_minutes, status, mpa_rating, rating, vote_count, collection
        ) VALUES (?, 'DEL', 'x', {insert_date}, 0, 0, 1, 'Rumored', 'NR', 0, 0, '')
    """
    cur.execute("BEGIN")
    try:
        for i in range(500):
            cur.execute(delete_insert_sql, (base_delete_id + i, "2024-01-01"))
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise

    def run_bulk_delete():
        cur.execute("BEGIN")
        try:
            cur.execute(
                f"DELETE FROM movies WHERE id BETWEEN {base_delete_id} AND {base_delete_id + 499}"
            )
            cur.execute("COMMIT")
            return 500
        except Exception:
            cur.execute("ROLLBACK")
            raise

    duration, rows = _time_movie_operation(
        engine_name,
        "Showdown bulk DELETE",
        500,
        run_bulk_delete,
    )
    print(f"    rows={rows:,}")
    results["showdown_bulk_delete_s"] = duration
    results["showdown_bulk_delete_rows"] = rows

    if engine_name == "decentdb":
        _showdown_try_query(
            engine_name,
            cur,
            "Showdown stat aggregates",
            """
            SELECT STDDEV(score) AS stddev,
                   VARIANCE(score) AS variance,
                   MEDIAN(score) AS median,
                   AVG(score) AS mean
            FROM reviews
            """,
            results,
            "showdown_stat_aggregates_s",
            note="DecentDB built-in",
            explain_output_dir=explain_output_dir,
            explain_analyze=explain_analyze,
            query_mode=query_mode,
        )
    else:
        print("  Showdown stat aggregates             skipped: n/a in stock SQLite")
        results["showdown_stat_aggregates_s"] = None

    duration, _ = _time_movie_operation(
        engine_name,
        "Showdown checkpoint",
        0,
        lambda: _movie_checkpoint(conn, engine_name),
    )
    results["showdown_checkpoint_s"] = duration

    conn.close()
    results["showdown_final_file_size_bytes"] = storage_size_bytes(db_path)
    print(
        f"  Final file size: {results['showdown_final_file_size_bytes']:,} bytes "
        f"({results['showdown_final_file_size_bytes'] / (1024.0 * 1024.0):.2f} MiB)"
    )

    if not keep_db:
        cleanup_db_files(db_path)

    return results


def print_showdown_comparison(results, *, tie_threshold=0.0):
    if "decentdb" not in results or "sqlite" not in results:
        return

    d = results["decentdb"]
    s = results["sqlite"]
    metrics = [
        ("Showdown Bulk Load Time", "showdown_bulk_load_s", "s", False, ".6f"),
        ("Showdown Bulk Load throughput", "showdown_bulk_load_rps", " rows/s", True, ".2f"),
        ("Showdown B-tree index build", "showdown_index_build_s", "s", False, ".6f"),
        ("Showdown Search index build", "showdown_search_index_build_s", "s", False, ".6f"),
        ("Showdown ANALYZE", "showdown_analyze_s", "s", False, ".6f"),
        ("Showdown Point lookup", "showdown_point_lookup_s", "s", False, ".6f"),
        ("Showdown Full table scan", "showdown_full_scan_s", "s", False, ".6f"),
        ("Showdown Filtered range", "showdown_filtered_range_s", "s", False, ".6f"),
        ("Showdown Index range/order", "showdown_index_range_order_s", "s", False, ".6f"),
        ("Showdown Keyset pagination", "showdown_keyset_pagination_s", "s", False, ".6f"),
        ("Showdown Offset pagination", "showdown_offset_pagination_s", "s", False, ".6f"),
        ("Showdown Movie genres join", "showdown_movie_genres_join_s", "s", False, ".6f"),
        ("Showdown Cast/crew join", "showdown_cast_crew_join_s", "s", False, ".6f"),
        ("Showdown Review aggregate join", "showdown_review_aggregate_join_s", "s", False, ".6f"),
        ("Showdown Person filmography", "showdown_person_filmography_s", "s", False, ".6f"),
        ("Showdown Genre popularity", "showdown_genre_popularity_s", "s", False, ".6f"),
        ("Showdown Yearly counts", "showdown_yearly_counts_s", "s", False, ".6f"),
        ("Showdown Top by decade", "showdown_top_by_decade_s", "s", False, ".6f"),
        ("Showdown Review ranking", "showdown_review_ranking_s", "s", False, ".6f"),
        ("Showdown Cast billing window", "showdown_cast_billing_window_s", "s", False, ".6f"),
        ("Showdown Recursive CTE", "showdown_recursive_cte_s", "s", False, ".6f"),
        ("Showdown Directors CTE", "showdown_directors_cte_s", "s", False, ".6f"),
        ("Showdown Substring LIKE", "showdown_substring_like_s", "s", False, ".6f"),
        ("Showdown Fulltext BM25", "showdown_fulltext_bm25_s", "s", False, ".6f"),
        ("Showdown UNION", "showdown_union_s", "s", False, ".6f"),
        ("Showdown Rolling avg frame", "showdown_rolling_average_s", "s", False, ".6f"),
        ("Showdown INSERT RETURNING", "showdown_insert_returning_s", "s", False, ".6f"),
        ("Showdown UPDATE RETURNING", "showdown_update_returning_s", "s", False, ".6f"),
        ("Showdown UPSERT", "showdown_upsert_s", "s", False, ".6f"),
        ("Showdown Bulk UPDATE", "showdown_bulk_update_s", "s", False, ".6f"),
        ("Showdown Bulk DELETE", "showdown_bulk_delete_s", "s", False, ".6f"),
        ("Showdown Checkpoint", "showdown_checkpoint_s", "s", False, ".6f"),
        ("Showdown Final file size", "showdown_final_file_size_bytes", " bytes", False, ".0f"),
    ]

    decent_better = []
    sqlite_better = []
    ties = []
    skipped = []

    for name, key, unit, higher_is_better, fmt in metrics:
        decent = d.get(key)
        sqlite = s.get(key)
        if decent is None or sqlite is None:
            skipped.append(f"{name}: skipped ({decent!r} vs {sqlite!r})")
            continue
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

    print("\n=== Showdown Comparison (DecentDB vs SQLite) ===")
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

    if skipped:
        print("Skipped/unsupported:")
        for line in skipped:
            print(f"- {line}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Comprehensive Python benchmark: DecentDB bindings vs sqlite3"
    )
    parser.add_argument(
        "--workload",
        choices=["complex", "movie", "showdown", "both", "all"],
        default="both",
        help=(
            "Benchmark workload to run. both runs complex+movie for backwards "
            "compatibility; all also runs the GLM52-style showdown workload "
            "(default: both)."
        ),
    )
    parser.add_argument(
        "--engine",
        choices=["all", "decentdb", "sqlite"],
        default="all",
        help="Engine to run (default: all)",
    )
    parser.add_argument(
        "--engine-order",
        choices=["decentdb-first", "sqlite-first", "random"],
        default="decentdb-first",
        help=(
            "Engine order when --engine=all. Use sqlite-first or random to "
            "control order effects (default: decentdb-first)."
        ),
    )
    parser.add_argument(
        "--query-mode",
        choices=["warm", "cold", "both"],
        default="warm",
        help=(
            "MovieDB/Showdown SELECT timing mode. warm preserves the historical "
            "behavior of timing after an initial result/signature fetch; cold "
            "times the first execution of each captured query shape; both records "
            "cold variants with *_cold_s keys and warm timings under the historical "
            "metric names (default: warm)."
        ),
    )
    parser.add_argument(
        "--users",
        type=int,
        default=DEFAULT_USERS,
        help=f"Number of users to generate (default: {DEFAULT_USERS})",
    )
    parser.add_argument(
        "--items",
        type=int,
        default=DEFAULT_ITEMS,
        help=f"Number of items to generate (default: {DEFAULT_ITEMS})",
    )
    parser.add_argument(
        "--orders",
        type=int,
        default=DEFAULT_ORDERS,
        help=f"Number of orders to generate (default: {DEFAULT_ORDERS})",
    )
    parser.add_argument(
        "--decentdb-options",
        default=DECENTDB_EMBEDDED_FAST_OPTIONS,
        help=(
            "DecentDB native open options. Default matches the embedded-fast "
            "profile used to make this comparison fair against tuned SQLite. "
            "Pass an empty string to test native defaults."
        ),
    )
    parser.add_argument(
        "--decentdb-stmt-cache-size",
        type=int,
        default=512,
        help="DecentDB Python statement cache size (default: 512)",
    )
    parser.add_argument(
        "--sqlite-profile",
        choices=["wal_normal", "wal_full", "delete_full"],
        default="wal_normal",
        help="SQLite PRAGMA profile (default: wal_normal, matching the MovieDB harness)",
    )
    parser.add_argument(
        "--sqlite-cache-mb",
        type=int,
        default=64,
        help="SQLite page cache size in MiB (default: 64)",
    )
    parser.add_argument(
        "--history-reads",
        type=int,
        default=5000,
        help="Number of random user history points reads (default: 5000)",
    )
    parser.add_argument(
        "--point-lookups",
        type=int,
        default=5000,
        help="Number of simple point lookup operations (default: 5000)",
    )
    parser.add_argument(
        "--range-scans",
        type=int,
        default=5000,
        help="Number of range scan operations (default: 5000)",
    )
    parser.add_argument(
        "--joins",
        type=int,
        default=5000,
        help="Number of join query operations (default: 5000)",
    )
    parser.add_argument(
        "--aggregates",
        type=int,
        default=5000,
        help="Number of aggregate query operations (default: 5000)",
    )
    parser.add_argument(
        "--updates",
        type=int,
        default=5000,
        help="Number of update operations (default: 5000)",
    )
    parser.add_argument(
        "--deletes",
        type=int,
        default=5000,
        help="Number of delete operations (default: 5000)",
    )
    parser.add_argument(
        "--table-scans",
        type=int,
        default=500,
        help="Number of full table scan operations (default: 500)",
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
        "--json-output",
        default=".tmp/bench_complex_results.json",
        help=(
            "Write machine-readable benchmark results to this path. "
            "Pass an empty string to disable (default: .tmp/bench_complex_results.json)."
        ),
    )
    parser.add_argument(
        "--strict-equivalence",
        action="store_true",
        help="Exit non-zero when captured DecentDB and SQLite query result signatures differ.",
    )
    parser.add_argument(
        "--explain-output-dir",
        default=None,
        help="Directory for per-query EXPLAIN artifacts for MovieDB/Showdown slow queries.",
    )
    parser.add_argument(
        "--explain-analyze",
        action="store_true",
        help=(
            "Use EXPLAIN ANALYZE for DecentDB explain artifacts. SQLite still "
            "uses EXPLAIN QUERY PLAN."
        ),
    )
    parser.add_argument(
        "--keep-db",
        action="store_true",
        help="Keep generated database files after benchmark run",
    )
    parser.add_argument(
        "--movie-scale",
        choices=["smoke", "scratch"],
        default="smoke",
        help=(
            "MovieDB scale preset. scratch uses the out-of-repo .NET harness "
            "sizes: 50k movies, 25k people, 250k roles, 500k reviews, "
            "500 tags, 150k movie-tags, 100k watchlist entries."
        ),
    )
    parser.add_argument("--movie-movies", type=int, default=None)
    parser.add_argument("--movie-people", type=int, default=None)
    parser.add_argument("--movie-roles", type=int, default=None)
    parser.add_argument("--movie-reviews", type=int, default=None)
    parser.add_argument("--movie-tags", type=int, default=None)
    parser.add_argument("--movie-movie-tags", type=int, default=None)
    parser.add_argument("--movie-watchlist", type=int, default=None)
    parser.add_argument(
        "--movie-watchlist-movie-index",
        action="store_true",
        help="Add ix_watchlist_movie on Watchlist(MovieId) for cascade schema-variant runs.",
    )
    parser.add_argument(
        "--movie-point-reads",
        type=int,
        default=DEFAULT_MOVIE_POINT_READS,
        help=f"MovieDB UUID point reads (default: {DEFAULT_MOVIE_POINT_READS})",
    )
    parser.add_argument(
        "--movie-update-count",
        type=int,
        default=DEFAULT_MOVIE_UPDATE_COUNT,
        help=f"MovieDB box-office batch updates (default: {DEFAULT_MOVIE_UPDATE_COUNT})",
    )
    parser.add_argument(
        "--movie-delete-count",
        type=int,
        default=DEFAULT_MOVIE_DELETE_COUNT,
        help=f"MovieDB cascade parent deletes (default: {DEFAULT_MOVIE_DELETE_COUNT})",
    )
    parser.add_argument(
        "--showdown-scale",
        choices=["smoke", "glm52"],
        default="smoke",
        help=(
            "Showdown scale preset. glm52 uses the second out-of-repo "
            ".NET project default of 20k movies with people=movies*3 and "
            "up to 8 reviews/movie."
        ),
    )
    parser.add_argument(
        "--showdown-movies",
        type=int,
        default=None,
        help=(
            f"Showdown movie count (default: {DEFAULT_SHOWDOWN_MOVIES}; "
            f"glm52 preset: {GLM52_SHOWDOWN_MOVIES})"
        ),
    )
    parser.add_argument(
        "--showdown-people-mult",
        type=int,
        default=DEFAULT_SHOWDOWN_PEOPLE_MULT,
        help=(
            "Showdown people multiplier, people=movies*mult "
            f"(default: {DEFAULT_SHOWDOWN_PEOPLE_MULT})"
        ),
    )
    parser.add_argument(
        "--showdown-reviews-per-movie",
        type=int,
        default=DEFAULT_SHOWDOWN_REVIEWS_PER_MOVIE,
        help=(
            "Showdown max generated reviews per movie "
            f"(default: {DEFAULT_SHOWDOWN_REVIEWS_PER_MOVIE})"
        ),
    )
    parser.add_argument(
        "--showdown-point-reads",
        type=int,
        default=DEFAULT_SHOWDOWN_POINT_READS,
        help=f"Showdown integer PK point reads (default: {DEFAULT_SHOWDOWN_POINT_READS})",
    )
    return parser.parse_args()


def apply_movie_scale_defaults(args):
    if args.movie_scale == "scratch":
        defaults = {
            "movie_movies": SCRATCH_MOVIES,
            "movie_people": SCRATCH_PEOPLE,
            "movie_roles": SCRATCH_ROLES,
            "movie_reviews": SCRATCH_REVIEWS,
            "movie_tags": SCRATCH_TAGS,
            "movie_movie_tags": SCRATCH_MOVIE_TAGS,
            "movie_watchlist": SCRATCH_WATCHLIST,
        }
    else:
        defaults = {
            "movie_movies": DEFAULT_MOVIES,
            "movie_people": DEFAULT_PEOPLE,
            "movie_roles": DEFAULT_ROLES,
            "movie_reviews": DEFAULT_REVIEWS,
            "movie_tags": DEFAULT_TAGS,
            "movie_movie_tags": DEFAULT_MOVIE_TAGS,
            "movie_watchlist": DEFAULT_WATCHLIST,
        }
    for name, value in defaults.items():
        if getattr(args, name) is None:
            setattr(args, name, value)


def apply_showdown_scale_defaults(args):
    if args.showdown_movies is None:
        args.showdown_movies = (
            GLM52_SHOWDOWN_MOVIES
            if args.showdown_scale == "glm52"
            else DEFAULT_SHOWDOWN_MOVIES
        )


def main():
    args = parse_args()
    apply_movie_scale_defaults(args)
    apply_showdown_scale_defaults(args)
    if args.explain_analyze and not args.explain_output_dir:
        args.explain_output_dir = ".tmp/bench_complex_explain"
    if args.engine == "all":
        engines = ["decentdb", "sqlite"]
        if args.engine_order == "sqlite-first":
            engines = ["sqlite", "decentdb"]
        elif args.engine_order == "random":
            random.Random(args.seed).shuffle(engines)
    else:
        engines = [args.engine]
    results = {}
    movie_results = {}
    showdown_results = {}

    if args.workload in ("complex", "both", "all"):
        print(
            "Running complex benchmark with "
            f"engines={','.join(engines)} users={args.users} items={args.items} "
            f"orders={args.orders} history_reads={args.history_reads} "
            f"point_lookups={args.point_lookups} range_scans={args.range_scans} "
            f"joins={args.joins} aggregates={args.aggregates} updates={args.updates} "
            f"deletes={args.deletes} table_scans={args.table_scans}"
        )

        for engine in engines:
            suffix = "ddb" if engine == "decentdb" else "db"
            path = f"{args.db_prefix}_complex_{engine}.{suffix}"
            results[engine] = run_engine_benchmark(
                engine_name=engine,
                db_path=path,
                users_count=args.users,
                items_count=args.items,
                orders_count=args.orders,
                history_reads=args.history_reads,
                point_lookups=args.point_lookups,
                range_scans=args.range_scans,
                joins=args.joins,
                aggregates=args.aggregates,
                updates=args.updates,
                deletes=args.deletes,
                table_scans=args.table_scans,
                seed=args.seed,
                keep_db=args.keep_db,
                decentdb_options=args.decentdb_options,
                decentdb_stmt_cache_size=args.decentdb_stmt_cache_size,
                sqlite_profile=args.sqlite_profile,
                sqlite_cache_mb=args.sqlite_cache_mb,
            )

        print_comparison(results)

    if args.workload in ("movie", "both", "all"):
        print(
            "\nRunning MovieDB benchmark with "
            f"engines={','.join(engines)} scale={args.movie_scale} "
            f"movies={args.movie_movies} people={args.movie_people} "
            f"roles={args.movie_roles} reviews={args.movie_reviews} "
            f"tags={args.movie_tags} movie_tags={args.movie_movie_tags} "
            f"watchlist={args.movie_watchlist}"
        )
        print("Generating shared MovieDB dataset...")
        movie_data = generate_movie_data(
            movies_count=args.movie_movies,
            people_count=args.movie_people,
            roles_count=args.movie_roles,
            reviews_count=args.movie_reviews,
            tags_count=args.movie_tags,
            movie_tags_count=args.movie_movie_tags,
            watchlist_count=args.movie_watchlist,
            seed=args.seed,
        )
        print(f"MovieDB dataset rows: {movie_total_rows(movie_data):,}")

        for engine in engines:
            suffix = "ddb" if engine == "decentdb" else "db"
            path = f"{args.db_prefix}_movie_{engine}.{suffix}"
            movie_results[engine] = run_movie_benchmark(
                engine_name=engine,
                db_path=path,
                data=movie_data,
                point_reads=args.movie_point_reads,
                update_count=args.movie_update_count,
                delete_count=args.movie_delete_count,
                keep_db=args.keep_db,
                decentdb_options=args.decentdb_options,
                decentdb_stmt_cache_size=args.decentdb_stmt_cache_size,
                sqlite_profile=args.sqlite_profile,
                sqlite_cache_mb=args.sqlite_cache_mb,
                watchlist_movie_index=args.movie_watchlist_movie_index,
                explain_output_dir=args.explain_output_dir,
                explain_analyze=args.explain_analyze,
                query_mode=args.query_mode,
            )

        print_movie_comparison(movie_results)
        movie_equivalence = _equivalence_report(movie_results)
        _print_equivalence_report("MovieDB", movie_equivalence)
    else:
        movie_equivalence = {"status": "skipped", "reason": "workload not run"}

    if args.workload in ("showdown", "all"):
        print(
            "\nRunning Showdown benchmark with "
            f"engines={','.join(engines)} scale={args.showdown_scale} "
            f"movies={args.showdown_movies} "
            f"people_mult={args.showdown_people_mult} "
            f"reviews_per_movie={args.showdown_reviews_per_movie}"
        )
        print("Generating shared Showdown dataset...")
        showdown_data = generate_showdown_data(
            movies_count=args.showdown_movies,
            people_multiplier=args.showdown_people_mult,
            reviews_per_movie=args.showdown_reviews_per_movie,
            seed=args.seed,
        )
        print(f"Showdown dataset rows: {showdown_total_rows(showdown_data):,}")

        for engine in engines:
            suffix = "ddb" if engine == "decentdb" else "db"
            path = f"{args.db_prefix}_showdown_{engine}.{suffix}"
            showdown_results[engine] = run_showdown_benchmark(
                engine_name=engine,
                db_path=path,
                data=showdown_data,
                point_reads=args.showdown_point_reads,
                keep_db=args.keep_db,
                decentdb_options=args.decentdb_options,
                decentdb_stmt_cache_size=args.decentdb_stmt_cache_size,
                sqlite_profile=args.sqlite_profile,
                sqlite_cache_mb=args.sqlite_cache_mb,
                explain_output_dir=args.explain_output_dir,
                explain_analyze=args.explain_analyze,
                query_mode=args.query_mode,
            )

        print_showdown_comparison(showdown_results)
        showdown_equivalence = _equivalence_report(showdown_results)
        _print_equivalence_report("Showdown", showdown_equivalence)
    else:
        showdown_equivalence = {"status": "skipped", "reason": "workload not run"}

    complex_equivalence = {"status": "skipped", "reason": "complex workload does not capture signatures"}
    equivalence = {
        "complex": complex_equivalence,
        "movie": movie_equivalence,
        "showdown": showdown_equivalence,
    }
    if args.json_output:
        _write_json_report(
            args.json_output,
            _json_report(args, results, movie_results, showdown_results, equivalence),
        )
    if args.strict_equivalence:
        failed = [
            name
            for name, report in equivalence.items()
            if report.get("status") == "failed"
        ]
        if failed:
            raise SystemExit(f"result equivalence failed for: {', '.join(failed)}")


if __name__ == "__main__":
    main()
