"""Canonical benchmark workloads for embedded database comparisons.

This module defines the standard workloads that all engines must implement
to ensure fair comparison.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from drivers.base_driver import DatabaseDriver
from utils.dataset_generator import Customer, Order, Event
from utils.performance_timer import BenchmarkRunner, LatencyTracker, Timer


@dataclass
class BenchmarkResult:
    """Result of a single benchmark operation."""

    benchmark_name: str
    operations: int
    duration_sec: float
    latency_ms: Dict[str, float]
    throughput_ops_sec: float
    errors: int = 0


def empty_benchmark_result(benchmark_name: str) -> BenchmarkResult:
    """Return a placeholder result for unsupported operations."""

    return BenchmarkResult(
        benchmark_name=benchmark_name,
        operations=0,
        duration_sec=0,
        latency_ms={
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "ops_count": 0,
            "error_count": 0,
        },
        throughput_ops_sec=0,
    )


class Workload(ABC):
    """Abstract base class for benchmark workloads."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Workload name."""
        pass

    @abstractmethod
    def get_schema_sql(self) -> str:
        """Get CREATE TABLE statements."""
        pass

    @abstractmethod
    def load_data(
        self,
        driver: DatabaseDriver,
        customers: List[Customer],
        orders: List[Order],
        events: List[Event],
        transaction_mode: str,
        batch_size: int = 1000,
    ):
        """Load generated data into the database.

        Args:
            driver: Database driver
            customers: Customer records
            orders: Order records
            events: Event records
            transaction_mode: autocommit, batched, or explicit
            batch_size: Number of rows per batch (for batched mode)
        """
        pass

    @abstractmethod
    def run_point_lookup(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        """Run point lookup benchmark."""
        pass

    @abstractmethod
    def run_range_scan(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        """Run range scan benchmark."""
        pass

    @abstractmethod
    def run_join(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        """Run join benchmark."""
        pass

    @abstractmethod
    def run_aggregate(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        """Run aggregate benchmark."""
        pass

    @abstractmethod
    def run_update(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        """Run update benchmark."""
        pass

    @abstractmethod
    def run_delete(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        """Run delete benchmark."""
        pass


class OrdersWorkload(Workload):
    """OLTP-ish Orders workload (Workload A)."""

    def __init__(self):
        self._customers: List[Customer] = []
        self._orders: List[Order] = []

    @property
    def name(self) -> str:
        return "workload_a_orders"

    def get_schema_sql(self) -> str:
        return """
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    status TEXT NOT NULL,
    total_cents INTEGER NOT NULL
);

CREATE INDEX idx_orders_customer_created ON orders(customer_id, created_at);
CREATE INDEX idx_orders_status_created ON orders(status, created_at);
"""

    def _get_queries(self) -> Dict[str, str]:
        """Get SQL queries for this workload."""
        return {
            "point_lookup": "SELECT * FROM customers WHERE customer_id = ?",
            "range_scan": "SELECT * FROM orders WHERE customer_id = ? AND created_at >= ? AND created_at < ? ORDER BY created_at LIMIT ?",
            "join": "SELECT o.order_id, o.total_cents, c.email FROM (SELECT order_id, customer_id, created_at, total_cents FROM orders WHERE created_at >= ? AND created_at < ? ORDER BY created_at LIMIT ?) o INNER JOIN customers c ON c.customer_id = o.customer_id ORDER BY o.created_at",
            "aggregate": "SELECT status, COUNT(*) AS n, SUM(total_cents) AS sum_cents FROM orders WHERE created_at >= ? AND created_at < ? GROUP BY status ORDER BY n DESC",
            "update": "UPDATE orders SET status = ? WHERE order_id = ?",
            "delete": "DELETE FROM orders WHERE order_id = ?",
            "insert_order": "INSERT INTO orders (order_id, customer_id, created_at, status, total_cents) VALUES (?, ?, ?, ?, ?)",
            "insert_customer": "INSERT INTO customers (customer_id, email, created_at) VALUES (?, ?, ?)",
        }

    def load_data(
        self,
        driver: DatabaseDriver,
        customers: List[Customer],
        orders: List[Order],
        events: List[Event],
        transaction_mode: str,
        batch_size: int = 1000,
    ):
        queries = self._get_queries()

        if transaction_mode == "autocommit":
            # Insert customers
            for c in customers:
                driver.execute_update(
                    queries["insert_customer"], (c.customer_id, c.email, c.created_at)
                )
                driver.commit()

            # Insert orders
            for o in orders:
                driver.execute_update(
                    queries["insert_order"],
                    (o.order_id, o.customer_id, o.created_at, o.status, o.total_cents),
                )
                driver.commit()

        elif transaction_mode == "batched":
            # Insert customers in batches
            for i in range(0, len(customers), batch_size):
                driver.begin_transaction()
                batch = customers[i : i + batch_size]
                for c in batch:
                    driver.execute_update(
                        queries["insert_customer"],
                        (c.customer_id, c.email, c.created_at),
                    )
                driver.commit()

            # Insert orders in batches
            for i in range(0, len(orders), batch_size):
                driver.begin_transaction()
                batch = orders[i : i + batch_size]
                for o in batch:
                    driver.execute_update(
                        queries["insert_order"],
                        (
                            o.order_id,
                            o.customer_id,
                            o.created_at,
                            o.status,
                            o.total_cents,
                        ),
                    )
                driver.commit()

        elif transaction_mode == "explicit":
            # Single large transaction for everything
            driver.begin_transaction()
            for c in customers:
                driver.execute_update(
                    queries["insert_customer"], (c.customer_id, c.email, c.created_at)
                )
            for o in orders:
                driver.execute_update(
                    queries["insert_order"],
                    (o.order_id, o.customer_id, o.created_at, o.status, o.total_cents),
                )
            driver.commit()

        self._customers = list(customers)
        self._orders = list(orders)

    def _require_loaded_data(self):
        if not self._customers or not self._orders:
            raise ValueError("Workload data must be loaded before running benchmarks")

    def _generate_point_lookup_params(
        self, customers: List[Customer], n: int
    ) -> List[Tuple]:
        """Generate parameters for point lookup queries."""
        import random

        rng = random.Random(42)  # Deterministic
        customer_ids = [customer.customer_id for customer in customers]
        return [(customer_ids[rng.randint(0, len(customer_ids) - 1)],) for _ in range(n)]

    def _generate_range_params(self, orders: List[Order], n: int) -> List[Tuple]:
        """Generate parameters for range scan queries."""
        import random

        rng = random.Random(43)  # Different seed for variety
        times = [o.created_at for o in orders]
        customer_ids = sorted({order.customer_id for order in orders})
        min_t, max_t = min(times), max(times)
        window = (max_t - min_t) // 10
        window = max(window, 1)

        params = []
        for _ in range(n):
            customer_id = customer_ids[rng.randint(0, len(customer_ids) - 1)]
            start = rng.randint(min_t, min_t + window)
            end = start + window
            limit = rng.randint(10, 100)
            params.append((customer_id, start, end, limit))

        return params

    def _generate_join_params(self, orders: List[Order], n: int) -> List[Tuple]:
        """Generate parameters for join queries."""
        import random

        rng = random.Random(44)
        times = [o.created_at for o in orders]
        min_t, max_t = min(times), max(times)

        params = []
        for _ in range(n):
            start = rng.randint(min_t, max_t)
            end = start + 86400  # 1 day window
            limit = rng.randint(10, 100)
            params.append((start, end, limit))

        return params

    def _generate_aggregate_params(self, orders: List[Order], n: int) -> List[Tuple]:
        """Generate parameters for aggregate queries."""
        import random

        rng = random.Random(45)
        times = [o.created_at for o in orders]
        min_t, max_t = min(times), max(times)

        params = []
        for _ in range(n):
            start = rng.randint(min_t, max_t)
            end = start + 86400 * 7  # 1 week window
            params.append((start, end))

        return params

    def _generate_update_params(self, orders: List[Order], n: int) -> List[Tuple]:
        """Generate parameters for update queries."""
        import random

        rng = random.Random(46)

        params = []
        order_ids = [order.order_id for order in orders]
        statuses = ["shipped", "delivered", "completed"]
        for _ in range(n):
            order_id = order_ids[rng.randint(0, len(order_ids) - 1)]
            status = rng.choice(statuses)
            params.append((status, order_id))

        return params

    def _generate_delete_params(self, orders: List[Order], n: int) -> List[Tuple]:
        """Generate parameters for delete queries."""
        # Delete from the end to avoid affecting other queries
        order_ids = [order.order_id for order in sorted(orders, key=lambda order: order.order_id, reverse=True)]
        if n > len(order_ids):
            raise ValueError(
                f"Delete benchmark requested {n} operations but only {len(order_ids)} rows are loaded"
            )
        return [(order_id,) for order_id in order_ids[:n]]

    def _warmup_mutation(self, driver: DatabaseDriver, sql: str, params_list: List[Tuple]):
        if not params_list:
            return
        driver.begin_transaction()
        try:
            for params in params_list:
                driver.execute_update(sql, params)
        finally:
            driver.rollback()

    def run_point_lookup(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        queries = self._get_queries()
        params_list = self._generate_point_lookup_params(
            self._customers, operations + warmup
        )

        # Warmup
        for params in params_list[:warmup]:
            driver.execute_query(queries["point_lookup"], params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(queries["point_lookup"], params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="point_select",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_range_scan(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        queries = self._get_queries()
        params_list = self._generate_range_params(self._orders, operations + warmup)

        # Warmup
        for params in params_list[:warmup]:
            driver.execute_query(queries["range_scan"], params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(queries["range_scan"], params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="range_scan",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_join(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        queries = self._get_queries()
        params_list = self._generate_join_params(self._orders, operations + warmup)

        # Warmup
        for params in params_list[:warmup]:
            driver.execute_query(queries["join"], params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(queries["join"], params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="join",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_aggregate(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        queries = self._get_queries()
        params_list = self._generate_aggregate_params(self._orders, operations + warmup)

        # Warmup
        for params in params_list[:warmup]:
            driver.execute_query(queries["aggregate"], params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(queries["aggregate"], params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="aggregate",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_update(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        queries = self._get_queries()
        params_list = self._generate_update_params(self._orders, operations + warmup)

        self._warmup_mutation(driver, queries["update"], params_list[:warmup])

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_update(queries["update"], params)
                driver.commit()
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="update",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_delete(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        queries = self._get_queries()
        warmup_params = self._generate_delete_params(self._orders, min(warmup, len(self._orders)))
        benchmark_params = self._generate_delete_params(self._orders, operations)

        self._warmup_mutation(driver, queries["delete"], warmup_params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in benchmark_params:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_update(queries["delete"], params)
                driver.commit()
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="delete",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )


class EventsWorkload(Workload):
    """Web Analytics Events workload (Workload B)."""

    def __init__(self):
        self._events: List[Event] = []

    @property
    def name(self) -> str:
        return "workload_b_events"

    def get_schema_sql(self) -> str:
        return """
CREATE TABLE events (
    event_id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    ts INTEGER NOT NULL,
    path TEXT NOT NULL,
    referrer TEXT,
    bytes INTEGER NOT NULL
);

CREATE INDEX idx_events_user_ts ON events(user_id, ts);
CREATE INDEX idx_events_ts ON events(ts);
"""

    def load_data(
        self,
        driver: DatabaseDriver,
        customers: List[Customer],
        orders: List[Order],
        events: List[Event],
        transaction_mode: str,
        batch_size: int = 1000,
    ):
        insert_sql = "INSERT INTO events (event_id, user_id, ts, path, referrer, bytes) VALUES (?, ?, ?, ?, ?, ?)"

        if transaction_mode == "autocommit":
            for e in events:
                driver.execute_update(
                    insert_sql,
                    (e.event_id, e.user_id, e.ts, e.path, e.referrer, e.bytes),
                )
                driver.commit()

        elif transaction_mode == "batched":
            for i in range(0, len(events), batch_size):
                driver.begin_transaction()
                batch = events[i : i + batch_size]
                for e in batch:
                    driver.execute_update(
                        insert_sql,
                        (e.event_id, e.user_id, e.ts, e.path, e.referrer, e.bytes),
                    )
                driver.commit()

        elif transaction_mode == "explicit":
            driver.begin_transaction()
            for e in events:
                driver.execute_update(
                    insert_sql,
                    (e.event_id, e.user_id, e.ts, e.path, e.referrer, e.bytes),
                )
            driver.commit()

        self._events = list(events)

    def _require_loaded_data(self):
        if not self._events:
            raise ValueError("Workload data must be loaded before running benchmarks")

    def _generate_recent_events_params(
        self, events: List[Event], n: int
    ) -> List[Tuple]:
        import random

        rng = random.Random(50)
        times = [e.ts for e in events]
        min_t, max_t = min(times), max(times)

        params = []
        for _ in range(n):
            start = rng.randint(min_t, max_t)
            end = start + 3600  # 1 hour window
            limit = rng.randint(10, 100)
            params.append((start, end, limit))

        return params

    def _generate_rollup_params(self, events: List[Event], n: int) -> List[Tuple]:
        import random

        rng = random.Random(51)
        times = [e.ts for e in events]
        min_t, max_t = min(times), max(times)

        params = []
        for _ in range(n):
            start = rng.randint(min_t, max_t)
            end = start + 86400  # 1 day window
            limit = rng.randint(10, 100)
            params.append((start, end, limit))

        return params

    def run_point_lookup(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        # For events workload, point lookup = recent events query
        return self.run_range_scan(driver, operations, warmup)

    def run_range_scan(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        sql = "SELECT user_id, ts, path, bytes FROM events WHERE ts >= ? AND ts < ? ORDER BY ts LIMIT ?"
        params_list = self._generate_recent_events_params(
            self._events, operations + warmup
        )

        # Warmup
        for params in params_list[:warmup]:
            driver.execute_query(sql, params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(sql, params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="point_select",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_join(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("join")

    def run_aggregate(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        sql = "SELECT user_id, COUNT(*) AS n, SUM(bytes) AS sum_bytes FROM events WHERE ts >= ? AND ts < ? GROUP BY user_id ORDER BY n DESC LIMIT ?"
        params_list = self._generate_rollup_params(self._events, operations + warmup)

        # Warmup
        for params in params_list[:warmup]:
            driver.execute_query(sql, params)

        # Benchmark
        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(sql, params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()

        return BenchmarkResult(
            benchmark_name="aggregate",
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_update(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("update")

    def run_delete(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("delete")


class BindingParityWorkload(Workload):
    """Flat-table indexed read workload aligned with the Python binding benchmark."""

    def __init__(self):
        self._row_count = 0

    @property
    def name(self) -> str:
        return "workload_c_binding_parity"

    def get_schema_sql(self) -> str:
        return """
CREATE TABLE bench (
    id INTEGER PRIMARY KEY,
    val TEXT NOT NULL,
    f REAL NOT NULL
);

CREATE INDEX bench_id_idx ON bench(id);
"""

    def load_data(
        self,
        driver: DatabaseDriver,
        customers: List[Customer],
        orders: List[Order],
        events: List[Event],
        transaction_mode: str,
        batch_size: int = 1000,
    ):
        insert_sql = "INSERT INTO bench (id, val, f) VALUES (?, ?, ?)"
        rows = [
            (order.order_id, f"value_{order.order_id}", float(order.total_cents))
            for order in orders
        ]

        if transaction_mode == "autocommit":
            for row in rows:
                driver.execute_update(insert_sql, row)
                driver.commit()
        elif transaction_mode == "batched":
            for i in range(0, len(rows), batch_size):
                driver.begin_transaction()
                batch = rows[i : i + batch_size]
                for row in batch:
                    driver.execute_update(insert_sql, row)
                driver.commit()
        elif transaction_mode == "explicit":
            driver.begin_transaction()
            for row in rows:
                driver.execute_update(insert_sql, row)
            driver.commit()

        self._row_count = len(rows)

    def _require_loaded_data(self):
        if self._row_count == 0:
            raise ValueError("Workload data must be loaded before running benchmarks")

    def _generate_point_lookup_params(self, n: int) -> List[Tuple]:
        import random

        rng = random.Random(60)
        return [(rng.randrange(self._row_count),) for _ in range(n)]

    def _run_query_benchmark(
        self,
        driver: DatabaseDriver,
        sql: str,
        params_list: List[Optional[Tuple]],
        benchmark_name: str,
        warmup: int,
    ) -> BenchmarkResult:
        for params in params_list[:warmup]:
            driver.execute_query(sql, params)

        tracker = LatencyTracker()
        timer = Timer()
        timer.start()

        for params in params_list[warmup:]:
            op_timer = Timer()
            op_timer.start()
            try:
                driver.execute_query(sql, params)
                tracker.record(op_timer.stop() * 1000)
            except Exception:
                tracker.record_error()

        duration = timer.stop()
        stats = tracker.get_statistics()
        operations = len(params_list) - warmup

        return BenchmarkResult(
            benchmark_name=benchmark_name,
            operations=operations,
            duration_sec=duration,
            latency_ms=stats,
            throughput_ops_sec=operations / duration if duration > 0 else 0,
            errors=stats["error_count"],
        )

    def run_point_lookup(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        params_list = self._generate_point_lookup_params(operations + warmup)
        return self._run_query_benchmark(
            driver,
            "SELECT id, val, f FROM bench WHERE id = ?",
            params_list,
            "point_select",
            warmup,
        )

    def run_range_scan(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        self._require_loaded_data()
        params_list = [None] * (operations + warmup)
        return self._run_query_benchmark(
            driver,
            "SELECT id, val, f FROM bench",
            params_list,
            "full_scan",
            warmup,
        )

    def run_join(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("join")

    def run_aggregate(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("aggregate")

    def run_update(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("update")

    def run_delete(
        self,
        driver: DatabaseDriver,
        operations: int,
        warmup: int = 100,
    ) -> BenchmarkResult:
        return empty_benchmark_result("delete")


# Registry of available workloads
WORKLOADS = {
    "workload_a": OrdersWorkload,
    "workload_b": EventsWorkload,
    "workload_c": BindingParityWorkload,
}


def get_workload(name: str) -> Workload:
    """Get workload by name."""
    workload_class = WORKLOADS.get(name)
    if workload_class is None:
        raise ValueError(f"Unknown workload: {name}")
    return workload_class()
