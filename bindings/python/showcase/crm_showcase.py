#!/usr/bin/env python3
"""
DecentDB CRM Showcase Application

This script demonstrates all features of the DecentDB Python bindings through
a classic CRM database schema with rich TUI progress bars and memory tracking.

Usage:
    # Standard run
    python crm_showcase.py --records 10000 [--db-path PATH] [--keep-db]

    # Add reproducibility and memory guardrails
    python crm_showcase.py --records 10000 --seed 42 --max-memory-mb 512 [--strict-memory-checks]

    # Escalate until it fails a memory check (single-flag mode)
    python crm_showcase.py --explode

    # Optional explode tuning
    python crm_showcase.py --explode --records 1000 --explode-growth 1.5 --explode-max-runs 100 --explode-max-records 100000000 [--max-memory-mb 512]
"""

import argparse
import datetime
import decimal
import gc
import os
import random
import sys
import tempfile
import time
import uuid

import decentdb
from rich.console import Console
from rich.progress import (
    BarColumn, Progress, SpinnerColumn, TextColumn,
    TimeElapsedColumn, TimeRemainingColumn,
    MofNCompleteColumn, TaskProgressColumn,
)
from rich.table import Table

console = Console()


def db_artifact_paths(db_path):
    return [
        db_path,
        db_path + ".wal",
        db_path + ".shm",
        db_path + "-wal",
        db_path + "-shm",
        db_path + "-lock",
    ]


def get_memory_mb():
    try:
        import psutil

        return psutil.Process().memory_info().rss / 1024 / 1024
    except ImportError:
        pass
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    return int(parts[1]) / 1024
    except OSError:
        pass
    try:
        import resource

        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        pass
    return 0.0


def format_mem(mb):
    return f"{mb/1024:.2f} GB" if mb >= 1024 else f"{mb:.2f} MB"


def total_db_artifact_size_mb(db_path):
    total_bytes = 0
    for path in db_artifact_paths(db_path):
        try:
            total_bytes += os.path.getsize(path)
        except OSError:
            pass
    return total_bytes / 1024 / 1024


def rand_str(prefix="", length=10):
    return prefix + "".join(random.choice("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789") for _ in range(length))


def rand_email():
    return f"{rand_str(length=random.randint(5,15))}@{random.choice(['gmail.com','yahoo.com','outlook.com','company.com'])}"


def rand_phone():
    return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"


def rand_datetime(start_year=2000, end_year=2025):
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    dt = start + datetime.timedelta(days=random.randint(0, (end-start).days))
    return datetime.datetime(dt.year, dt.month, dt.day, random.randint(0,23), random.randint(0,59), random.randint(0,59))


class CRMShowcase:
    def __init__(
        self,
        db_path,
        num_records,
        max_memory_mb=None,
        strict_memory_checks=False,
        auto_memory_cap_delta_mb=None,
    ):
        self.db_path = db_path
        self.num_records = num_records
        self.conn = None
        self.timings = {}
        self.run_started_at = time.perf_counter()
        self.max_memory_mb = max_memory_mb
        self.auto_memory_cap_delta_mb = auto_memory_cap_delta_mb
        self.strict_memory_checks = strict_memory_checks
        self.initial_memory_mb = None
        self.memory_checks = []
        self.memory_check_failed = False

    def mark_timing(self, name, started_at):
        self.timings[name] = self.timings.get(name, 0.0) + (time.perf_counter() - started_at)

    def expected_memory_limit_mb(self, phase):
        if self.initial_memory_mb is None:
            return None
        # Conservative expectations derived from this workload profile.
        # Scale linearly with record count so checks stay useful at higher loads.
        customers_scale = self.num_records * 0.0030
        orders_scale = self.num_records * 0.0065
        enrich_scale = self.num_records * 0.0080
        final_scale = self.num_records * 0.0085
        limits = {
            "after_schema": self.initial_memory_mb + 18.0,
            "after_customers": self.initial_memory_mb + 18.0 + customers_scale,
            "after_orders": self.initial_memory_mb + 26.0 + orders_scale,
            "after_enrichment": self.initial_memory_mb + 32.0 + enrich_scale,
            "final": self.initial_memory_mb + 36.0 + final_scale,
        }
        return limits.get(phase)

    def evaluate_memory(self, phase, observed_mb):
        if phase == "initial":
            self.initial_memory_mb = observed_mb
            if self.max_memory_mb is None and self.auto_memory_cap_delta_mb is not None:
                self.max_memory_mb = observed_mb + self.auto_memory_cap_delta_mb
            self.memory_checks.append(
                {
                    "phase": phase,
                    "observed_mb": observed_mb,
                    "expected_limit_mb": None,
                    "hard_cap_mb": self.max_memory_mb,
                    "status": "PASS",
                    "detail": (
                        f"baseline established; auto hard cap {self.max_memory_mb:.2f} MB"
                        if self.auto_memory_cap_delta_mb is not None
                        else "baseline established"
                    ),
                }
            )
            return
        expected_limit = self.expected_memory_limit_mb(phase)
        status = "PASS"
        detail = "within expected range"
        if expected_limit is not None and observed_mb > expected_limit:
            status = "WARN"
            detail = f"above expected limit by {observed_mb - expected_limit:.2f} MB"
        if self.max_memory_mb is not None and observed_mb > self.max_memory_mb:
            status = "FAIL"
            detail = f"above hard cap by {observed_mb - self.max_memory_mb:.2f} MB"
        if self.strict_memory_checks and status in {"WARN", "FAIL"}:
            self.memory_check_failed = True
        if status == "FAIL":
            self.memory_check_failed = True
        self.memory_checks.append(
            {
                "phase": phase,
                "observed_mb": observed_mb,
                "expected_limit_mb": expected_limit,
                "hard_cap_mb": self.max_memory_mb,
                "status": status,
                "detail": detail,
            }
        )

    def cleanup(self):
        for path in db_artifact_paths(self.db_path):
            try:
                os.remove(path)
            except OSError:
                pass

    def show_mem(self, label="", phase=None):
        gc.collect()
        mem = get_memory_mb()
        if mem > 0:
            console.print(f"[cyan]Memory:[/cyan] {format_mem(mem)} {label}")
            if phase:
                self.evaluate_memory(phase, mem)
        return mem

    def create_schema(self):
        started_at = time.perf_counter()
        console.print("\n[bold blue]Creating schema...[/bold blue]")
        conn = decentdb.connect(self.db_path, mode="create")
        self.conn = conn
        cur = conn.cursor()

        # Using named parameters ($1, $2, etc.) to avoid ? count confusion
        cur.execute("""
            CREATE TABLE customers (
                id INT64 PRIMARY KEY, name TEXT NOT NULL, email TEXT,
                phone TEXT, company TEXT, created_at TIMESTAMP,
                updated_at TIMESTAMP, is_active BOOL,
                credit_limit DECIMAL(10,2), notes TEXT, external_id UUID
            )
        """)
        cur.execute("""
            CREATE TABLE contacts (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                first_name TEXT NOT NULL, last_name TEXT, email TEXT,
                phone TEXT, role TEXT, is_primary BOOL DEFAULT 0,
                created_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE addresses (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                address_type TEXT NOT NULL, street TEXT, city TEXT,
                state TEXT, postal_code TEXT, country TEXT DEFAULT 'USA',
                is_default BOOL DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE products (
                id INT64 PRIMARY KEY, sku TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL, description TEXT, category TEXT,
                price DECIMAL(10,2) NOT NULL, cost DECIMAL(10,2),
                stock_quantity INT64 DEFAULT 0, is_active BOOL DEFAULT 1,
                created_at TIMESTAMP, updated_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE orders (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                order_number TEXT UNIQUE NOT NULL, order_date TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'pending', subtotal DECIMAL(10,2),
                tax DECIMAL(10,2), shipping DECIMAL(10,2), total DECIMAL(10,2),
                notes TEXT, shipped_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE order_items (
                id INT64 PRIMARY KEY, order_id INT64 NOT NULL,
                product_id INT64 NOT NULL, quantity INT64 NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL, discount DECIMAL(10,2) DEFAULT 0
            )
        """)
        cur.execute("""
            CREATE TABLE invoices (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                order_id INT64, invoice_number TEXT UNIQUE NOT NULL,
                invoice_date TIMESTAMP NOT NULL, due_date TIMESTAMP,
                status TEXT DEFAULT 'draft', subtotal DECIMAL(10,2),
                tax DECIMAL(10,2), total DECIMAL(10,2), paid_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE invoice_items (
                id INT64 PRIMARY KEY, invoice_id INT64 NOT NULL,
                description TEXT NOT NULL, quantity INT64 NOT NULL,
                unit_price DECIMAL(10,2) NOT NULL, amount DECIMAL(10,2) NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE notes (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                title TEXT NOT NULL, content TEXT, note_type TEXT DEFAULT 'general',
                created_at TIMESTAMP, created_by TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE tasks (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                title TEXT NOT NULL, description TEXT, status TEXT DEFAULT 'pending',
                priority TEXT DEFAULT 'medium', due_date TIMESTAMP,
                completed_at TIMESTAMP, assigned_to TEXT, created_at TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE activity_log (
                id INT64 PRIMARY KEY, customer_id INT64 NOT NULL,
                action TEXT NOT NULL, details TEXT, performed_by TEXT,
                performed_at TIMESTAMP, ip_address TEXT
            )
        """)

        # Indexes
        cur.execute("CREATE INDEX idx_contacts_cust ON contacts(customer_id)")
        cur.execute("CREATE INDEX idx_addresses_cust ON addresses(customer_id)")
        cur.execute("CREATE INDEX idx_orders_cust ON orders(customer_id)")
        cur.execute("CREATE INDEX idx_orders_num ON orders(order_number)")
        cur.execute("CREATE INDEX idx_order_items_order ON order_items(order_id)")
        cur.execute("CREATE INDEX idx_order_items_prod ON order_items(product_id)")
        cur.execute("CREATE INDEX idx_invoices_cust ON invoices(customer_id)")
        cur.execute("CREATE INDEX idx_invoices_num ON invoices(invoice_number)")
        cur.execute("CREATE INDEX idx_notes_cust ON notes(customer_id)")
        cur.execute("CREATE INDEX idx_tasks_cust ON tasks(customer_id)")
        cur.execute("CREATE INDEX idx_tasks_status ON tasks(status)")
        cur.execute("CREATE INDEX idx_activity_cust ON activity_log(customer_id)")
        cur.execute("CREATE INDEX idx_products_sku ON products(sku)")
        cur.execute("CREATE INDEX idx_products_cat ON products(category)")

        conn.commit()
        console.print("[green]Created 11 tables with indexes[/green]")
        self.mark_timing("schema_create", started_at)
        return conn

    def insert_data(self, progress):
        insert_started_at = time.perf_counter()
        conn = self.conn
        cur = conn.cursor()

        # Insert customers - using named params to be explicit
        customers_started_at = time.perf_counter()
        p1 = progress.add_task("[green]Customers", total=self.num_records)
        cur.execute("BEGIN")
        for i in range(self.num_records):
            cur.execute(
                "INSERT INTO customers VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                (i+1, f"Customer_{i+1}", f"c{i+1}@test.com", rand_phone(),
                 f"Company_{random.randint(1,100)}", rand_datetime(), rand_datetime(),
                 random.choice([True, False]),
                 decimal.Decimal(f"{random.uniform(1000,50000):.2f}"),
                 rand_str(length=random.randint(0,200)) if random.random() > 0.5 else None,
                 uuid.uuid4() if random.random() > 0.7 else None)
            )
            if i % 5000 == 4999:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
            progress.update(p1, completed=i+1)
        conn.commit()
        self.mark_timing("insert_customers", customers_started_at)
        self.show_mem(" after customers", phase="after_customers")

        # Contacts
        contacts_started_at = time.perf_counter()
        contact_counts = [
            random.choices([1, 2, 3], weights=[50, 30, 20])[0]
            for _ in range(self.num_records)
        ]
        p2 = progress.add_task("[green]Contacts", total=sum(contact_counts))
        cur.execute("BEGIN")
        contact_id = 1
        for c, n in enumerate(contact_counts, start=1):
            for _ in range(n):
                cur.execute(
                    "INSERT INTO contacts VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    (contact_id, c, rand_str(length=random.randint(5,15)),
                     rand_str(length=random.randint(5,15)), rand_email(),
                     rand_phone(), random.choice(["owner","manager","sales"]),
                     random.choice([True, False]), rand_datetime())
                )
                contact_id += 1
                progress.update(p2, completed=contact_id - 1)
        conn.commit()
        self.mark_timing("insert_contacts", contacts_started_at)

        # Products
        products_started_at = time.perf_counter()
        p3 = progress.add_task("[green]Products", total=100)
        cats = ["Electronics", "Software", "Hardware", "Services"]
        cur.execute("BEGIN")
        for i in range(100):
            cur.execute(
                "INSERT INTO products VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                (i+1, f"SKU-{i+1:05d}", f"Product {i+1}", f"Desc {i+1}",
                 random.choice(cats), decimal.Decimal(f"{random.uniform(10,5000):.2f}"),
                 decimal.Decimal(f"{random.uniform(5,2500):.2f}"),
                 random.randint(0,1000), True, rand_datetime(), rand_datetime())
            )
            progress.update(p3, completed=i+1)
        conn.commit()
        self.mark_timing("insert_products", products_started_at)

        # Orders
        orders_started_at = time.perf_counter()
        order_count = min(self.num_records//2, 500000)
        p4 = progress.add_task("[green]Orders", total=order_count)
        cur.execute("BEGIN")
        for i in range(order_count):
            cust = random.randint(1, self.num_records)
            dt = rand_datetime()
            sub = decimal.Decimal(f"{random.uniform(50,5000):.2f}")
            tax = sub * decimal.Decimal("0.08")
            ship = decimal.Decimal(f"{random.uniform(0,50):.2f}")
            tot = sub + tax + ship
            status = random.choice(["pending","processing","shipped","delivered"])
            cur.execute(
                "INSERT INTO orders VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                (i+1, cust, f"ORD-{i+1:010d}", dt, status, sub, tax, ship, tot,
                 rand_str(length=100) if random.random() > 0.7 else None,
                 dt + datetime.timedelta(days=random.randint(1,7)) if status in ["shipped","delivered"] else None)
            )
            if i % 5000 == 4999:
                cur.execute("COMMIT")
                cur.execute("BEGIN")
            progress.update(p4, completed=i+1)
        conn.commit()
        self.mark_timing("insert_orders", orders_started_at)
        self.show_mem(" after orders", phase="after_orders")

        # Addresses
        enrichment_started_at = time.perf_counter()
        cur.execute("BEGIN")
        address_id = 1
        for customer_id in range(1, self.num_records + 1):
            if random.random() < 0.6:
                cur.execute(
                    "INSERT INTO addresses VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                    (
                        address_id,
                        customer_id,
                        random.choice(["billing", "shipping", "hq"]),
                        f"{random.randint(100,9999)} {rand_str(length=12)} St",
                        random.choice(["Seattle", "Austin", "Denver", "Miami", "Boston"]),
                        random.choice(["WA", "TX", "CO", "FL", "MA"]),
                        f"{random.randint(10000,99999)}",
                        "USA",
                        random.choice([True, False]),
                    ),
                )
                address_id += 1
        conn.commit()

        # Order items + invoices + invoice items
        cur.execute("BEGIN")
        order_item_id = 1
        invoice_id = 1
        invoice_item_id = 1
        for order_id in range(1, order_count + 1):
            item_count = random.randint(1, 3)
            subtotal = decimal.Decimal("0.00")
            for _ in range(item_count):
                product_id = random.randint(1, 100)
                quantity = random.randint(1, 5)
                unit_price = decimal.Decimal(f"{random.uniform(10,5000):.2f}")
                discount = decimal.Decimal(f"{random.uniform(0,20):.2f}") if random.random() < 0.2 else decimal.Decimal("0.00")
                cur.execute(
                    "INSERT INTO order_items VALUES ($1, $2, $3, $4, $5, $6)",
                    (order_item_id, order_id, product_id, quantity, unit_price, discount),
                )
                line_total = (unit_price * quantity) - discount
                subtotal += line_total
                order_item_id += 1

            if random.random() < 0.5:
                customer_id = random.randint(1, self.num_records)
                invoice_number = f"INV-{invoice_id:010d}"
                invoice_date = rand_datetime()
                due_date = invoice_date + datetime.timedelta(days=30)
                tax = subtotal * decimal.Decimal("0.08")
                total = subtotal + tax
                status = random.choice(["draft", "sent", "paid"])
                paid_at = invoice_date + datetime.timedelta(days=random.randint(1, 20)) if status == "paid" else None
                cur.execute(
                    "INSERT INTO invoices VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                    (
                        invoice_id,
                        customer_id,
                        order_id,
                        invoice_number,
                        invoice_date,
                        due_date,
                        status,
                        subtotal,
                        tax,
                        total,
                        paid_at,
                    ),
                )
                cur.execute(
                    "INSERT INTO invoice_items VALUES ($1, $2, $3, $4, $5, $6)",
                    (
                        invoice_item_id,
                        invoice_id,
                        f"Order {order_id} summary",
                        1,
                        total,
                        total,
                    ),
                )
                invoice_id += 1
                invoice_item_id += 1
        conn.commit()

        # Notes, tasks, and activity log
        cur.execute("BEGIN")
        note_id = 1
        task_id = 1
        activity_id = 1
        for customer_id in range(1, self.num_records + 1):
            if random.random() < 0.35:
                cur.execute(
                    "INSERT INTO notes VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    (
                        note_id,
                        customer_id,
                        rand_str(prefix="Note_", length=10),
                        rand_str(length=120),
                        random.choice(["general", "support", "sales"]),
                        rand_datetime(),
                        random.choice(["alice", "bob", "carol"]),
                    ),
                )
                note_id += 1
            if random.random() < 0.30:
                created_at = rand_datetime()
                cur.execute(
                    "INSERT INTO tasks VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    (
                        task_id,
                        customer_id,
                        rand_str(prefix="Task_", length=10),
                        rand_str(length=80),
                        random.choice(["pending", "in_progress", "done"]),
                        random.choice(["low", "medium", "high"]),
                        created_at + datetime.timedelta(days=random.randint(1, 30)),
                        None,
                        random.choice(["alice", "bob", "carol"]),
                        created_at,
                    ),
                )
                task_id += 1
            if random.random() < 0.50:
                cur.execute(
                    "INSERT INTO activity_log VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    (
                        activity_id,
                        customer_id,
                        random.choice(["created", "updated", "emailed", "called", "invoice_sent"]),
                        rand_str(length=80),
                        random.choice(["system", "alice", "bob"]),
                        rand_datetime(),
                        f"10.0.{random.randint(0,255)}.{random.randint(1,254)}",
                    ),
                )
                activity_id += 1
        conn.commit()
        self.mark_timing("insert_enrichment", enrichment_started_at)
        self.show_mem(" after enrichment", phase="after_enrichment")

        # Query demos
        query_started_at = time.perf_counter()
        console.print("\n[bold cyan]Queries[/bold cyan]")
        q1_started_at = time.perf_counter()
        cur.execute("SELECT COUNT(*) FROM customers")
        self.mark_timing("query_customer_count", q1_started_at)
        console.print(f"[green]Customers: {cur.fetchone()[0]:,}[/green]")
        
        q2_started_at = time.perf_counter()
        cur.execute("SELECT c.name, SUM(o.total) FROM customers c JOIN orders o ON c.id=o.customer_id GROUP BY c.name ORDER BY 2 DESC LIMIT 5")
        self.mark_timing("query_top_customers", q2_started_at)
        console.print("[green]Top customers:")
        for r in cur.fetchall():
            console.print(f"  {r[0]}: ${r[1]:,.2f}")
        
        q3_started_at = time.perf_counter()
        cur.execute("SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category")
        self.mark_timing("query_by_category", q3_started_at)
        console.print("[green]By category:")
        for r in cur.fetchall():
            console.print(f"  {r[0]}: {r[1]} @ ${r[2]:.2f}")
        self.mark_timing("query_demo_total", query_started_at)
        self.mark_timing("insert_data_total", insert_started_at)

    def demo_features(self, progress):
        features_started_at = time.perf_counter()
        conn = self.conn
        cur = conn.cursor()
        console.print("\n[bold cyan]Features[/bold cyan]")

        # Schema introspection
        console.print(f"[green]Tables: {len(conn.list_tables())}[/green]")
        console.print(f"[green]Indexes: {len(conn.list_indexes())}[/green]")
        
        cols = conn.get_table_columns("customers")
        console.print(f"[green]Customer columns: {len(cols)}[/green]")

        # Parameter styles
        cur.execute("SELECT name FROM customers WHERE id=?", (1,))
        console.print(f"[green]Positional: {cur.fetchone()}[/green]")
        cur.execute("SELECT name FROM customers WHERE id=:id", {"id": 1})
        console.print(f"[green]Named: {cur.fetchone()}[/green]")

        # Fetch methods
        cur.execute("SELECT name FROM customers LIMIT 5")
        console.print(f"[green]fetchone: {cur.fetchone()}[/green]")
        cur.execute("SELECT name FROM customers LIMIT 10")
        console.print(f"[green]fetchall: {len(cur.fetchall())} rows[/green]")
        cur.execute("SELECT name FROM customers LIMIT 20")
        console.print(f"[green]fetchmany(5): {len(cur.fetchmany(5))} rows[/green]")

        # Storage
        try:
            before = conn.inspect_storage_state()
            console.print(
                "[green]Storage before checkpoint: "
                f"WAL versions={before.get('wal_versions', 'N/A')}, "
                f"WAL size={before.get('wal_file_size', 0) / 1024 / 1024:.2f} MB, "
                f"pages={before.get('page_count', 'N/A')}[/green]"
            )
        except Exception as e:
            console.print(f"[yellow]Inspect: {e}[/yellow]")

        checkpoint_started_at = time.perf_counter()
        conn.checkpoint()
        self.mark_timing("checkpoint", checkpoint_started_at)
        try:
            after = conn.inspect_storage_state()
            console.print(
                "[green]Checkpoint done: "
                f"WAL versions={after.get('wal_versions', 'N/A')}, "
                f"WAL size={after.get('wal_file_size', 0) / 1024 / 1024:.2f} MB[/green]"
            )
        except Exception as e:
            console.print(f"[yellow]Checkpoint inspect: {e}[/yellow]")
        self.mark_timing("demo_features_total", features_started_at)

    def report(self):
        report_started_at = time.perf_counter()
        conn = self.conn
        cur = conn.cursor()
        console.print("\n" + "="*50 + "\n[bold]REPORT[/bold]\n" + "="*50)
        
        tbl = Table(show_header=True)
        tbl.add_column("Table", style="cyan")
        tbl.add_column("Count", justify="right", style="green")
        
        for t in [
            "customers",
            "contacts",
            "addresses",
            "products",
            "orders",
            "order_items",
            "invoices",
            "invoice_items",
            "notes",
            "tasks",
            "activity_log",
        ]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t}")
                tbl.add_row(t, f"{cur.fetchone()[0]:,}")
            except Exception as exc:
                tbl.add_row(t, "ERR")
                console.print(f"[yellow]Count failed for {t}: {exc}[/yellow]")
        console.print(tbl)
        
        self.show_mem(" final", phase="final")
        try:
            data_mb = os.path.getsize(self.db_path) / 1024 / 1024
            total_mb = total_db_artifact_size_mb(self.db_path)
            console.print(
                f"[cyan]DB file: {format_mem(data_mb)}  Total (db+wal+shm): {format_mem(total_mb)}[/cyan]"
            )
        except OSError:
            pass

        timing_table = Table(show_header=True)
        timing_table.add_column("Timing metric", style="cyan")
        timing_table.add_column("Value", justify="right", style="green")
        timing_order = [
            "schema_create",
            "insert_customers",
            "insert_contacts",
            "insert_products",
            "insert_orders",
            "insert_enrichment",
            "insert_data_total",
            "query_customer_count",
            "query_top_customers",
            "query_by_category",
            "query_demo_total",
            "checkpoint",
            "demo_features_total",
        ]
        for metric in timing_order:
            if metric in self.timings:
                timing_table.add_row(metric, f"{self.timings[metric]:.3f}s")
        if "insert_data_total" in self.timings and self.timings["insert_data_total"] > 0:
            rows_per_sec = self.num_records / self.timings["insert_data_total"]
            timing_table.add_row("throughput_customers", f"{rows_per_sec:,.1f} rows/s")
        total_run_seconds = time.perf_counter() - self.run_started_at
        timing_table.add_row("total_run", f"{total_run_seconds:.3f}s")
        console.print("\n[bold]TIMING[/bold]")
        console.print(timing_table)

        checks_table = Table(show_header=True)
        checks_table.add_column("Phase", style="cyan")
        checks_table.add_column("Observed", justify="right")
        checks_table.add_column("Expected max", justify="right")
        checks_table.add_column("Hard cap", justify="right")
        checks_table.add_column("Status", justify="right")
        checks_table.add_column("Detail", style="yellow")
        status_style = {"PASS": "green", "WARN": "yellow", "FAIL": "red"}
        for check in self.memory_checks:
            expected = (
                f"{check['expected_limit_mb']:.2f} MB"
                if check["expected_limit_mb"] is not None
                else "N/A"
            )
            hard_cap = (
                f"{check['hard_cap_mb']:.2f} MB"
                if check["hard_cap_mb"] is not None
                else "N/A"
            )
            checks_table.add_row(
                check["phase"],
                f"{check['observed_mb']:.2f} MB",
                expected,
                hard_cap,
                f"[{status_style.get(check['status'], 'white')}]{check['status']}[/]",
                check["detail"],
            )
        console.print("\n[bold]MEMORY CHECKS[/bold]")
        console.print(checks_table)

        self.mark_timing("report_total", report_started_at)
        console.print("\nDone!")

    def close(self):
        if self.conn:
            self.conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--records", type=int, default=1000)
    ap.add_argument("--db-path", type=str)
    ap.add_argument("--keep-db", action="store_true")
    ap.add_argument("--seed", type=int)
    ap.add_argument("--max-memory-mb", type=float, default=None)
    ap.add_argument("--strict-memory-checks", action="store_true")
    ap.add_argument("--explode", action="store_true")
    ap.add_argument("--explode-growth", type=float, default=1.5)
    ap.add_argument("--explode-max-runs", type=int, default=100)
    ap.add_argument("--explode-max-records", type=int, default=100_000_000)
    args = ap.parse_args()

    if args.explode_growth <= 1.0:
        ap.error("--explode-growth must be greater than 1.0")
    if args.explode_max_runs < 1:
        ap.error("--explode-max-runs must be at least 1")
    if args.explode_max_records < 1:
        ap.error("--explode-max-records must be at least 1")

    if args.seed is not None:
        random.seed(args.seed)

    def run_one(records, db_path, strict_checks, auto_cap_delta_mb=None):
        console.print(f"[bold cyan]DecentDB CRM Showcase[/bold cyan] Records:{records:,} DB:{db_path}")
        app = CRMShowcase(
            db_path,
            records,
            max_memory_mb=args.max_memory_mb,
            strict_memory_checks=strict_checks,
            auto_memory_cap_delta_mb=auto_cap_delta_mb,
        )
        result_code = 0
        final_check = None
        total_run_seconds = None
        final_memory_mb = None
        final_check_status = "N/A"
        final_check_detail = "N/A"
        memory_failed = False
        run_error = None
        try:
            app.cleanup()
            app.show_mem(" initial", phase="initial")

            with Progress(SpinnerColumn(), TextColumn("{task.description}"), BarColumn(),
                         TaskProgressColumn(), MofNCompleteColumn(), TimeElapsedColumn(),
                         TimeRemainingColumn(), console=console) as p:
                app.create_schema()
                app.show_mem(" after schema", phase="after_schema")
                app.insert_data(p)
                app.demo_features(p)
                app.report()
            app.show_mem(" final")
            if app.memory_check_failed:
                memory_failed = True
                console.print("[red]Memory checks failed.[/red]")
                result_code = 2
        except Exception as exc:
            run_error = exc
            result_code = 3
            console.print(f"[red]Run failed with exception:[/red] {exc}")
        finally:
            if app.memory_checks:
                final_check = app.memory_checks[-1]
                final_memory_mb = final_check.get("observed_mb")
                final_check_status = final_check.get("status", "N/A")
                final_check_detail = final_check.get("detail", "N/A")
            total_run_seconds = time.perf_counter() - app.run_started_at
            app.close()
            if not args.keep_db:
                for path in db_artifact_paths(db_path):
                    try:
                        os.remove(path)
                    except OSError:
                        pass
        return {
            "result_code": result_code,
            "records": records,
            "db_path": db_path,
            "memory_failed": memory_failed,
            "final_check": final_check,
            "final_memory_mb": final_memory_mb,
            "final_check_status": final_check_status,
            "final_check_detail": final_check_detail,
            "total_run_seconds": total_run_seconds,
            "run_error": run_error,
        }

    if args.explode:
        strict_checks = True if not args.strict_memory_checks else args.strict_memory_checks
        auto_cap_delta_mb = 128.0 if args.max_memory_mb is None else None
        records = max(1, args.records)
        run_summaries = []
        console.print(
            "[bold magenta]Explode mode[/bold magenta] "
            f"start={records:,} growth={args.explode_growth} "
            f"max_runs={args.explode_max_runs} max_records={args.explode_max_records:,} "
            f"strict_checks={strict_checks} "
            f"hard_cap={'auto(+128MB from baseline)' if auto_cap_delta_mb is not None else f'{args.max_memory_mb:.2f} MB'}"
        )
        for run_idx in range(1, args.explode_max_runs + 1):
            if args.db_path:
                base, ext = os.path.splitext(args.db_path)
                ext = ext or ".ddb"
                run_db_path = f"{base}.explode{run_idx}{ext}"
            else:
                run_db_path = os.path.join(tempfile.mkdtemp(), f"crm_explode_{run_idx}.ddb")

            if args.seed is not None:
                random.seed(args.seed + run_idx - 1)

            console.print(
                f"\n[bold yellow]Explode run {run_idx}/{args.explode_max_runs}[/bold yellow] "
                f"(records={records:,})"
            )
            summary = run_one(records, run_db_path, strict_checks, auto_cap_delta_mb=auto_cap_delta_mb)
            summary["run"] = run_idx
            run_summaries.append(summary)
            if summary["result_code"] != 0:
                break

            next_records = max(records + 1, int(records * args.explode_growth))
            if next_records > args.explode_max_records:
                break
            records = next_records

        explode_table = Table(show_header=True)
        explode_table.add_column("Run", justify="right", style="cyan")
        explode_table.add_column("Records", justify="right")
        explode_table.add_column("Result", justify="right")
        explode_table.add_column("Final memory", justify="right")
        explode_table.add_column("Final check", justify="right")
        explode_table.add_column("Elapsed", justify="right")
        explode_table.add_column("Detail", style="yellow")
        for summary in run_summaries:
            result_label = "PASS" if summary["result_code"] == 0 else f"STOP({summary['result_code']})"
            final_mem = (
                f"{summary['final_memory_mb']:.2f} MB"
                if summary["final_memory_mb"] is not None
                else "N/A"
            )
            explode_table.add_row(
                str(summary["run"]),
                f"{summary['records']:,}",
                result_label,
                final_mem,
                summary["final_check_status"],
                f"{summary['total_run_seconds']:.3f}s",
                summary["final_check_detail"],
            )
        console.print("\n[bold]EXPLODE SUMMARY[/bold]")
        console.print(explode_table)
        if run_summaries and run_summaries[-1]["result_code"] != 0:
            return run_summaries[-1]["result_code"]
        if run_summaries and run_summaries[-1]["records"] >= args.explode_max_records:
            console.print("[green]Reached explode record limit without failing memory checks.[/green]")
        elif len(run_summaries) >= args.explode_max_runs and run_summaries[-1]["result_code"] == 0:
            console.print("[green]Reached explode run limit without failing memory checks.[/green]")
        return 0

    db_path = args.db_path or os.path.join(tempfile.mkdtemp(), "crm.ddb")
    summary = run_one(args.records, db_path, args.strict_memory_checks)
    if summary["result_code"] != 0:
        return summary["result_code"]
    return 0


if __name__ == "__main__":
    sys.exit(main())
