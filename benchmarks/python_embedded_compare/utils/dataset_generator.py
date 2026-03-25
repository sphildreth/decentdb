"""Deterministic dataset generator for benchmark comparisons.

This module generates reproducible datasets using a fixed seed,
ensuring fair comparisons across different database engines.
"""

import hashlib
import random
from dataclasses import dataclass
from typing import Dict, Iterator, List, Tuple

# Generator version - bump when generator logic changes
GENERATOR_VERSION = "1.0.0"

# Status distribution for orders
ORDER_STATUSES = [
    "paid",
    "paid",
    "paid",
    "paid",
    "paid",
    "paid",
    "paid",  # 70%
    "shipped",
    "shipped",
    "shipped",
    "shipped",  # 20%
    "cancelled",
    "cancelled",  # 8%
    "refunded",
]  # 2%


@dataclass
class GeneratorConfig:
    """Configuration for dataset generation."""

    seed: int = 42
    customers_n: int = 1000
    orders_n: int = 10000
    events_n: int = 10000
    time_range_seconds: int = 86400 * 30  # 30 days
    path_cardinality: int = 100


@dataclass
class Customer:
    """Customer record."""

    customer_id: int
    email: str
    created_at: int


@dataclass
class Order:
    """Order record."""

    order_id: int
    customer_id: int
    created_at: int
    status: str
    total_cents: int


@dataclass
class Event:
    """Web analytics event record."""

    event_id: int
    user_id: int
    ts: int
    path: str
    referrer: str | None
    bytes: int


class DatasetGenerator:
    """Deterministic dataset generator for benchmark workloads.

    Uses a fixed seed to ensure reproducible datasets across runs.
    """

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self.rng = random.Random(config.seed)
        self._path_pool: List[str] = []
        self._email_pool: List[str] = []

    def generate(self) -> Tuple[List[Customer], List[Order], List[Event]]:
        """Generate all dataset components.

        Returns:
            Tuple of (customers, orders, events)
        """
        # Generate pools
        self._generate_pools()

        # Generate entities
        customers = self._generate_customers()
        orders = self._generate_orders(customers)
        events = self._generate_events()

        return customers, orders, events

    def _generate_pools(self):
        """Generate reusable data pools."""
        # Path pool for web analytics
        paths = [
            "/",
            "/about",
            "/contact",
            "/products",
            "/products/item",
            "/blog",
            "/blog/post",
            "/search",
            "/cart",
            "/checkout",
            "/user/profile",
            "/user/dashboard",
            "/api/v1/data",
            "/static/main.js",
            "/static/style.css",
            "/images/logo.png",
        ]

        # Generate more paths to reach cardinality
        for i in range(self.config.path_cardinality - len(paths)):
            paths.append(f"/category/{chr(65 + i % 26)}/item{i}")

        self._path_pool = paths[: self.config.path_cardinality]

        # Email domains
        domains = ["example.com", "test.org", "sample.net", "demo.io"]

        # Generate email prefixes
        self._email_pool = []
        for i in range(1000):
            prefix = f"user{i:04d}"
            domain = self.rng.choice(domains)
            self._email_pool.append(f"{prefix}@{domain}")

    def _generate_customers(self) -> List[Customer]:
        """Generate customer records."""
        customers = []

        # Deterministic time window
        base_time = 1700000000  # Fixed base timestamp

        for i in range(self.config.customers_n):
            # Use hash for deterministic but seemingly random ordering
            seed_hash = hashlib.md5(
                f"{self.config.seed}:customer:{i}".encode()
            ).hexdigest()
            seed_val = int(seed_hash[:8], 16)
            local_rng = random.Random(seed_val)

            customer_id = i
            email_idx = local_rng.randint(0, len(self._email_pool) - 1)
            email = self._email_pool[email_idx]
            created_at = base_time + local_rng.randint(
                0, self.config.time_range_seconds
            )

            customers.append(
                Customer(customer_id=customer_id, email=email, created_at=created_at)
            )

        # Sort by customer_id for deterministic ordering
        customers.sort(key=lambda c: c.customer_id)
        return customers

    def _generate_orders(self, customers: List[Customer]) -> List[Order]:
        """Generate order records with referential integrity."""
        orders = []

        # Use customer times as basis for order times
        customer_times = [(c.customer_id, c.created_at) for c in customers]

        for i in range(self.config.orders_n):
            # Deterministic seed
            seed_hash = hashlib.md5(
                f"{self.config.seed}:order:{i}".encode()
            ).hexdigest()
            seed_val = int(seed_hash[:8], 16)
            local_rng = random.Random(seed_val)

            order_id = i
            # Reference valid customer
            customer_idx = local_rng.randint(0, len(customers) - 1)
            customer_id = customer_times[customer_idx][0]
            base_time = customer_times[customer_idx][1]

            # Order time after customer creation
            created_at = base_time + local_rng.randint(0, 86400 * 7)  # Within 7 days
            status = local_rng.choice(ORDER_STATUSES)

            # Total cents - log-normal-ish distribution
            # Mean around $50, range $1-$500
            cents = int(50 + local_rng.lognormvariate(0, 1) * 100)
            cents = max(100, min(50000, cents))  # Clamp to $1-$500

            orders.append(
                Order(
                    order_id=order_id,
                    customer_id=customer_id,
                    created_at=created_at,
                    status=status,
                    total_cents=cents,
                )
            )

        # Sort by order_id for deterministic ordering
        orders.sort(key=lambda o: o.order_id)
        return orders

    def _generate_events(self) -> List[Event]:
        """Generate web analytics events."""
        events = []

        for i in range(self.config.events_n):
            # Deterministic seed
            seed_hash = hashlib.md5(
                f"{self.config.seed}:event:{i}".encode()
            ).hexdigest()
            seed_val = int(seed_hash[:8], 16)
            local_rng = random.Random(seed_val)

            event_id = i
            # User IDs from 1 to path_cardinality (simulates distinct users)
            user_id = local_rng.randint(1, self.config.path_cardinality)

            base_time = 1700000000
            ts = base_time + local_rng.randint(0, self.config.time_range_seconds)

            path_idx = local_rng.randint(0, len(self._path_pool) - 1)
            path = self._path_pool[path_idx]

            # 20% have referrer
            if local_rng.random() < 0.2:
                ref_idx = local_rng.randint(0, len(self._path_pool) - 1)
                referrer = self._path_pool[ref_idx]
            else:
                referrer = None

            # Bytes - log-normal-ish, mean 50KB, range 100B-1MB
            bytes_val = int(50000 + local_rng.lognormvariate(0, 2) * 10000)
            bytes_val = max(100, min(1000000, bytes_val))

            events.append(
                Event(
                    event_id=event_id,
                    user_id=user_id,
                    ts=ts,
                    path=path,
                    referrer=referrer,
                    bytes=bytes_val,
                )
            )

        # Sort by event_id for deterministic ordering
        events.sort(key=lambda e: e.event_id)
        return events

    def get_schema_sql(self, engine: str = "generic") -> str:
        """Get CREATE TABLE statements for the schema.

        Args:
            engine: Target engine (generic, sqlite, duckdb, decentdb)

        Returns:
            SQL DDL statements
        """

        # Common SQL subset - avoid engine-specific types
        sql = """
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
        return sql.strip()

    def get_insert_statements(self) -> Tuple[List[Tuple], List[Tuple], List[Tuple]]:
        """Get parameterized insert statements as tuples.

        Returns:
            Tuple of (customer_tuples, order_tuples, event_tuples)
        """
        customers, orders, events = self.generate()

        customer_tuples = [(c.customer_id, c.email, c.created_at) for c in customers]

        order_tuples = [
            (o.order_id, o.customer_id, o.created_at, o.status, o.total_cents)
            for o in orders
        ]

        event_tuples = [
            (e.event_id, e.user_id, e.ts, e.path, e.referrer, e.bytes) for e in events
        ]

        return customer_tuples, order_tuples, event_tuples


def get_generator_metadata(config: GeneratorConfig) -> Dict:
    """Get metadata about the generator configuration.

    Args:
        config: Generator configuration

    Returns:
        Dictionary with generator metadata
    """
    return {
        "generator_version": GENERATOR_VERSION,
        "seed": config.seed,
        "customers_n": config.customers_n,
        "orders_n": config.orders_n,
        "events_n": config.events_n,
        "time_range_seconds": config.time_range_seconds,
        "path_cardinality": config.path_cardinality,
        "order_status_distribution": {
            "paid": 0.70,
            "shipped": 0.20,
            "cancelled": 0.08,
            "refunded": 0.02,
        },
    }
