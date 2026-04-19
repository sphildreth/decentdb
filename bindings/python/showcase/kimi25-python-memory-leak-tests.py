#!/usr/bin/env python3
"""
DecentDB Memory Leak Detection Showcase

A comprehensive test suite that exercises the DecentDB engine core and Python
bindings to detect memory leaks through repeated database operations.

This script performs various database operations in cycles:
- Opening/closing connections
- Creating/dropping tables
- Inserting/querying records
- Creating/dropping indexes
- Transaction management
- Schema introspection

Usage:
    python kimi25-python-memory-leak-tests.py [--cycles N] [--records-per-cycle N] [--db-path PATH]

Dependencies:
    - decentdb (Python bindings)
    - rich (for TUI output)
    - psutil (for memory tracking)
"""

import argparse
import datetime
import decimal
import gc
import os
import random
import string
import sys
import tempfile
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import decentdb

from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text
from rich import box

# Try to import psutil for memory tracking
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

console = Console()


@dataclass
class MemorySnapshot:
    """Represents a memory measurement at a point in time."""
    timestamp: float
    rss_mb: float
    vms_mb: float
    phase: str
    iteration: int


@dataclass
class PerformanceMetrics:
    """Tracks performance metrics for a test phase."""
    phase_name: str
    total_time_ms: float = 0.0
    operation_count: int = 0
    min_time_ms: float = float('inf')
    max_time_ms: float = 0.0

    @property
    def avg_time_ms(self) -> float:
        if self.operation_count == 0:
            return 0.0
        return self.total_time_ms / self.operation_count

    def add_measurement(self, duration_ms: float):
        self.total_time_ms += duration_ms
        self.operation_count += 1
        self.min_time_ms = min(self.min_time_ms, duration_ms)
        self.max_time_ms = max(self.max_time_ms, duration_ms)


@dataclass
class TestStatistics:
    """Overall test statistics."""
    total_cycles: int = 0
    completed_cycles: int = 0
    total_operations: int = 0
    errors: List[str] = field(default_factory=list)
    memory_snapshots: List[MemorySnapshot] = field(default_factory=list)
    phase_metrics: Dict[str, PerformanceMetrics] = field(default_factory=dict)
    start_time: float = field(default_factory=time.perf_counter)

    def get_metric(self, phase_name: str) -> PerformanceMetrics:
        if phase_name not in self.phase_metrics:
            self.phase_metrics[phase_name] = PerformanceMetrics(phase_name)
        return self.phase_metrics[phase_name]


class MemoryTracker:
    """Tracks memory usage throughout the test."""

    def __init__(self, history_size: int = 100):
        self.history_size = history_size
        self.snapshots: deque = deque(maxlen=history_size)
        self.baseline_memory: Optional[float] = None
        self.peak_memory: float = 0.0
        self._process = psutil.Process() if HAS_PSUTIL else None

    def get_current_memory(self) -> Tuple[float, float]:
        """Get current RSS and VMS memory in MB."""
        if self._process:
            mem_info = self._process.memory_info()
            rss_mb = mem_info.rss / 1024 / 1024
            vms_mb = mem_info.vms / 1024 / 1024
        else:
            # Fallback: try reading /proc/self/status
            rss_mb, vms_mb = self._read_proc_status()

        self.peak_memory = max(self.peak_memory, rss_mb)
        return rss_mb, vms_mb

    def _read_proc_status(self) -> Tuple[float, float]:
        """Read memory from /proc/self/status as fallback."""
        rss_mb = 0.0
        vms_mb = 0.0
        try:
            with open("/proc/self/status", "r") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        parts = line.split()
                        rss_mb = int(parts[1]) / 1024
                    elif line.startswith("VmSize:"):
                        parts = line.split()
                        vms_mb = int(parts[1]) / 1024
        except (OSError, IOError):
            pass
        return rss_mb, vms_mb

    def take_snapshot(self, phase: str, iteration: int) -> MemorySnapshot:
        """Record a memory snapshot."""
        rss, vms = self.get_current_memory()
        snapshot = MemorySnapshot(
            timestamp=time.perf_counter(),
            rss_mb=rss,
            vms_mb=vms,
            phase=phase,
            iteration=iteration
        )
        self.snapshots.append(snapshot)

        if self.baseline_memory is None and iteration > 10:
            # Set baseline after initial warmup
            self.baseline_memory = rss

        return snapshot

    def get_memory_trend(self, window_size: int = 20) -> float:
        """Calculate memory trend (MB per iteration) over recent window."""
        if len(self.snapshots) < window_size:
            return 0.0

        recent = list(self.snapshots)[-window_size:]
        if len(recent) < 2:
            return 0.0

        first = recent[0]
        last = recent[-1]
        iterations = last.iteration - first.iteration
        if iterations == 0:
            return 0.0

        return (last.rss_mb - first.rss_mb) / iterations

    def detect_leak(self, threshold_mb_per_100: float = 5.0) -> Tuple[bool, float]:
        """
        Detect if there's a memory leak.
        Returns (is_leak_detected, growth_rate_mb_per_100_iterations)
        """
        trend = self.get_memory_trend()
        growth_per_100 = trend * 100
        return growth_per_100 > threshold_mb_per_100, growth_per_100


class DecentDBMemoryTest:
    """
    Comprehensive memory leak test for DecentDB engine and Python bindings.
    """

    def __init__(
        self,
        db_path: str,
        num_cycles: int = 1000,
        records_per_cycle: int = 50,
        enable_gc: bool = True
    ):
        self.db_path = db_path
        self.num_cycles = num_cycles
        self.records_per_cycle = records_per_cycle
        self.enable_gc = enable_gc

        self.memory_tracker = MemoryTracker()
        self.stats = TestStatistics(total_cycles=num_cycles)

        # Test data generators
        self.random_seed = 42
        random.seed(self.random_seed)

    def generate_random_string(self, length: int = 20) -> str:
        """Generate a random string."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_random_email(self) -> str:
        """Generate a random email address."""
        user = self.generate_random_string(10)
        domain = random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 'company.com', 'test.org'])
        return f"{user}@{domain}"

    def generate_random_datetime(self) -> datetime.datetime:
        """Generate a random datetime within the last 10 years."""
        now = datetime.datetime.now()
        days_ago = random.randint(0, 3650)
        return now - datetime.timedelta(days=days_ago, hours=random.randint(0, 23), minutes=random.randint(0, 59))

    def _cleanup_database(self):
        """Remove any existing database files before starting a cycle."""
        for ext in ["", ".wal", ".shm", "-wal", "-shm", "-lock"]:
            try:
                path = self.db_path + ext
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass

    def run_single_cycle(self, cycle_num: int) -> bool:
        """
        Execute a single test cycle with various database operations.
        Returns True if successful, False if an error occurred.
        """
        conn = None
        cursor = None

        # Clean up any existing database files first
        self._cleanup_database()

        try:
            # Phase 1: Open connection
            phase_start = time.perf_counter()
            conn = decentdb.connect(self.db_path)
            self.stats.get_metric("open_connection").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 2: Create tables
            phase_start = time.perf_counter()
            cursor = conn.cursor()

            # Create a users table with various data types
            cursor.execute("""
                CREATE TABLE users (
                    id INT64 PRIMARY KEY,
                    username TEXT NOT NULL,
                    email TEXT,
                    age INT64,
                    balance DECIMAL(18,2),
                    is_active BOOL,
                    created_at TIMESTAMP,
                    profile_data BLOB
                )
            """)

            # Create an orders table with foreign key
            cursor.execute("""
                CREATE TABLE orders (
                    order_id INT64 PRIMARY KEY,
                    user_id INT64,
                    product_name TEXT,
                    quantity INT64,
                    price DECIMAL(18,2),
                    order_date TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            """)
            conn.commit()
            self.stats.get_metric("create_tables").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 3: Insert records (without indexes)
            phase_start = time.perf_counter()
            for i in range(self.records_per_cycle):
                user_id = cycle_num * self.records_per_cycle + i
                cursor.execute(
                    """INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        user_id,
                        self.generate_random_string(15),
                        self.generate_random_email(),
                        random.randint(18, 80),
                        decimal.Decimal(f"{random.uniform(0, 10000):.2f}"),
                        random.choice([True, False]),
                        self.generate_random_datetime(),
                        os.urandom(random.randint(50, 200))  # Random blob data
                    )
                )
            conn.commit()
            self.stats.get_metric("insert_users").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 4: Query records (full scan)
            phase_start = time.perf_counter()
            cursor.execute("SELECT COUNT(*) FROM users WHERE age > 30")
            cursor.fetchone()

            cursor.execute("SELECT * FROM users WHERE email LIKE '%@gmail.com%'")
            cursor.fetchall()
            self.stats.get_metric("query_full_scan").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 5: Create indexes
            phase_start = time.perf_counter()
            cursor.execute("CREATE INDEX idx_users_email ON users(email)")
            cursor.execute("CREATE INDEX idx_users_age ON users(age)")
            cursor.execute("CREATE INDEX idx_users_created ON users(created_at)")
            conn.commit()
            self.stats.get_metric("create_indexes").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 6: Insert more records (with indexes active)
            phase_start = time.perf_counter()
            for i in range(self.records_per_cycle // 2):
                order_id = cycle_num * (self.records_per_cycle // 2) + i
                user_id = cycle_num * self.records_per_cycle + (i % self.records_per_cycle)
                cursor.execute(
                    """INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        order_id,
                        user_id,
                        self.generate_random_string(20),
                        random.randint(1, 100),
                        decimal.Decimal(f"{random.uniform(10, 1000):.2f}"),
                        self.generate_random_datetime()
                    )
                )
            conn.commit()
            self.stats.get_metric("insert_orders").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 7: Query with index usage
            phase_start = time.perf_counter()
            cursor.execute("SELECT * FROM users WHERE email = ?", (self.generate_random_email(),))
            cursor.fetchall()

            cursor.execute("SELECT * FROM users WHERE age BETWEEN 25 AND 35")
            cursor.fetchall()

            cursor.execute("""
                SELECT u.username, o.product_name, o.price
                FROM users u
                JOIN orders o ON u.id = o.user_id
                WHERE u.age > 21
                LIMIT 10
            """)
            cursor.fetchall()
            self.stats.get_metric("query_indexed").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 8: Update records
            phase_start = time.perf_counter()
            cursor.execute(
                "UPDATE users SET balance = ? WHERE age < 30",
                (decimal.Decimal("9999.99"),)
            )
            conn.commit()
            self.stats.get_metric("update_records").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 9: Schema introspection
            phase_start = time.perf_counter()
            tables = conn.list_tables()
            for table in tables:
                columns = conn.get_table_columns(table)
            indexes = conn.list_indexes()
            self.stats.get_metric("schema_introspection").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 10: Delete some records (delete from child table first due to FK constraints)
            phase_start = time.perf_counter()
            cursor.execute("DELETE FROM orders WHERE quantity < 5")
            conn.commit()
            # Delete users who have no orders (to avoid FK violations)
            cursor.execute("""
                DELETE FROM users
                WHERE is_active = FALSE
                AND id NOT IN (SELECT user_id FROM orders WHERE user_id IS NOT NULL)
            """)
            conn.commit()
            self.stats.get_metric("delete_records").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 11: Drop indexes
            phase_start = time.perf_counter()
            cursor.execute("DROP INDEX idx_users_email")
            cursor.execute("DROP INDEX idx_users_age")
            cursor.execute("DROP INDEX idx_users_created")
            conn.commit()
            self.stats.get_metric("drop_indexes").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 12: Drop tables
            phase_start = time.perf_counter()
            cursor.execute("DROP TABLE orders")
            cursor.execute("DROP TABLE users")
            conn.commit()
            self.stats.get_metric("drop_tables").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Phase 13: Close connection
            phase_start = time.perf_counter()
            cursor.close()
            conn.close()
            self.stats.get_metric("close_connection").add_measurement(
                (time.perf_counter() - phase_start) * 1000
            )

            # Optional: Force garbage collection
            if self.enable_gc and cycle_num % 10 == 0:
                gc.collect()

            self.stats.completed_cycles += 1
            return True

        except Exception as e:
            error_msg = f"Cycle {cycle_num}: {type(e).__name__}: {str(e)}"
            self.stats.errors.append(error_msg)

            # Cleanup on error
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            except Exception:
                pass

            return False

    def run_test(self):
        """Run the complete memory leak test with live display."""

        # Setup progress display
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("["),
            TimeElapsedColumn(),
            TextColumn("<"),
            TimeRemainingColumn(),
            TextColumn("]"),
            expand=True
        )
        task_id = progress.add_task("[cyan]Running memory leak test...", total=self.num_cycles)

        # Create layout
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=5),
            Layout(name="memory", size=12),
            Layout(name="performance", size=10),
            Layout(name="footer", size=3)
        )

        with Live(layout, refresh_per_second=10, console=console) as live:
            for cycle in range(1, self.num_cycles + 1):
                # Run the test cycle
                success = self.run_single_cycle(cycle)

                # Take memory snapshot
                snapshot = self.memory_tracker.take_snapshot(
                    phase="post_cycle",
                    iteration=cycle
                )

                # Update progress
                progress.update(task_id, advance=1)

                # Update display every cycle or on error
                if cycle % 1 == 0 or not success:
                    layout["header"].update(self._create_header_panel(cycle))
                    layout["memory"].update(self._create_memory_panel(cycle))
                    layout["performance"].update(self._create_performance_panel())
                    layout["footer"].update(Panel(progress, style="on black"))

        # Final summary
        return self._print_final_summary()

    def _create_header_panel(self, current_cycle: int) -> Panel:
        """Create the header panel with test info."""
        elapsed = time.perf_counter() - self.stats.start_time
        cycles_per_sec = current_cycle / elapsed if elapsed > 0 else 0

        content = (
            f"[bold]DecentDB Memory Leak Test[/bold]\n"
            f"Database: [cyan]{self.db_path}[/cyan] | "
            f"Cycles: [green]{current_cycle}/{self.num_cycles}[/green] | "
            f"Records/Cycle: [yellow]{self.records_per_cycle}[/yellow] | "
            f"Speed: [magenta]{cycles_per_sec:.1f} cycles/sec[/magenta]"
        )
        return Panel(content, style="on blue", box=box.ROUNDED)

    def _create_memory_panel(self, current_cycle: int) -> Panel:
        """Create the memory usage panel."""
        rss, vms = self.memory_tracker.get_current_memory()
        baseline = self.memory_tracker.baseline_memory or rss
        delta = rss - baseline

        # Calculate trend
        trend = self.memory_tracker.get_memory_trend()
        is_leak, growth_rate = self.memory_tracker.detect_leak()

        # Create memory history table
        mem_table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
        mem_table.add_column("Iteration", justify="right", style="cyan")
        mem_table.add_column("RSS Memory", justify="right", style="magenta")
        mem_table.add_column("VMS Memory", justify="right", style="blue")
        mem_table.add_column("Delta from Baseline", justify="right")
        mem_table.add_column("Phase", style="green")

        # Show last 5 snapshots
        for snap in list(self.memory_tracker.snapshots)[-5:]:
            delta_str = f"+{snap.rss_mb - baseline:.2f}" if snap.rss_mb > baseline else f"{snap.rss_mb - baseline:.2f}"
            delta_style = "red" if snap.rss_mb > baseline + 10 else "yellow" if snap.rss_mb > baseline + 5 else "green"
            mem_table.add_row(
                str(snap.iteration),
                f"{snap.rss_mb:.2f} MB",
                f"{snap.vms_mb:.2f} MB",
                f"[{delta_style}]{delta_str} MB[/{delta_style}]",
                snap.phase
            )

        # Memory stats summary
        trend_color = "red" if trend > 0.1 else "yellow" if trend > 0.01 else "green"
        leak_status = "[red]LEAK DETECTED[/red]" if is_leak else "[green]OK[/green]"

        summary = (
            f"Current RSS: [bold magenta]{rss:.2f} MB[/bold magenta] | "
            f"Peak RSS: [bold]{self.memory_tracker.peak_memory:.2f} MB[/bold] | "
            f"Baseline: [bold]{baseline:.2f} MB[/bold] | "
            f"Delta: [bold]{'+' if delta > 0 else ''}{delta:.2f} MB[/bold]\n"
            f"Trend: [bold {trend_color}]{trend:+.4f} MB/iter[/bold {trend_color}] | "
            f"Growth/100 cycles: {growth_rate:.2f} MB | "
            f"Status: {leak_status}"
        )

        return Panel(
            Group(mem_table, Text(summary)),
            title="[bold]Memory Usage[/bold]",
            border_style="red" if is_leak else "green",
            box=box.ROUNDED
        )

    def _create_performance_panel(self) -> Panel:
        """Create the performance metrics panel."""
        perf_table = Table(box=box.SIMPLE, show_header=True, header_style="bold")
        perf_table.add_column("Phase", style="cyan")
        perf_table.add_column("Ops", justify="right")
        perf_table.add_column("Avg Time (ms)", justify="right")
        perf_table.add_column("Min (ms)", justify="right")
        perf_table.add_column("Max (ms)", justify="right")
        perf_table.add_column("Total (ms)", justify="right")

        for name, metric in sorted(self.stats.phase_metrics.items()):
            perf_table.add_row(
                name.replace("_", " ").title(),
                str(metric.operation_count),
                f"{metric.avg_time_ms:.2f}",
                f"{metric.min_time_ms:.2f}" if metric.min_time_ms != float('inf') else "N/A",
                f"{metric.max_time_ms:.2f}",
                f"{metric.total_time_ms:.2f}"
            )

        return Panel(
            perf_table,
            title="[bold]Performance Metrics[/bold]",
            border_style="blue",
            box=box.ROUNDED
        )

    def _print_final_summary(self):
        """Print the final test summary."""
        console.print("\n")

        elapsed = time.perf_counter() - self.stats.start_time
        is_leak, growth_rate = self.memory_tracker.detect_leak()

        # Create summary table
        summary_table = Table(title="Test Summary", box=box.DOUBLE_EDGE)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="magenta")

        summary_table.add_row("Total Cycles", str(self.stats.completed_cycles))
        summary_table.add_row("Failed Cycles", str(len(self.stats.errors)))
        summary_table.add_row("Total Time", f"{elapsed:.2f} seconds")
        summary_table.add_row("Avg Cycles/sec", f"{self.stats.completed_cycles / elapsed:.2f}")
        summary_table.add_row("", "")
        summary_table.add_row("Start Memory", f"{self.memory_tracker.snapshots[0].rss_mb if self.memory_tracker.snapshots else 0:.2f} MB")
        summary_table.add_row("Final Memory", f"{self.memory_tracker.snapshots[-1].rss_mb if self.memory_tracker.snapshots else 0:.2f} MB")
        summary_table.add_row("Peak Memory", f"{self.memory_tracker.peak_memory:.2f} MB")
        summary_table.add_row("Memory Growth/100 cycles", f"{growth_rate:.2f} MB")
        summary_table.add_row("Leak Detected", "YES" if is_leak else "NO")

        console.print(summary_table)

        # Error summary if any
        if self.stats.errors:
            error_table = Table(title=f"Errors ({len(self.stats.errors)} total)", box=box.ROUNDED)
            error_table.add_column("Error Message", style="red")
            for err in self.stats.errors[:10]:  # Show first 10 errors
                error_table.add_row(err)
            if len(self.stats.errors) > 10:
                error_table.add_row(f"... and {len(self.stats.errors) - 10} more errors")
            console.print(error_table)

        # Final verdict
        if is_leak:
            console.print(Panel(
                "[bold red]MEMORY LEAK DETECTED[/bold red]\n\n"
                f"The test detected a memory growth rate of {growth_rate:.2f} MB per 100 cycles.\n"
                "This suggests a potential memory leak in either the DecentDB engine core or Python bindings.",
                border_style="red",
                box=box.DOUBLE
            ))
            return 1
        else:
            console.print(Panel(
                "[bold green]NO MEMORY LEAKS DETECTED[/bold green]\n\n"
                f"Memory growth rate of {growth_rate:.2f} MB per 100 cycles is within acceptable limits.\n"
                "The DecentDB engine core and Python bindings appear to be properly managing memory.",
                border_style="green",
                box=box.DOUBLE
            ))
            return 0


def main():
    parser = argparse.ArgumentParser(
        description="DecentDB Memory Leak Detection Test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run default test (1000 cycles, 50 records per cycle)
    python kimi25-python-memory-leak-tests.py

    # Run extended test with more cycles
    python kimi25-python-memory-leak-tests.py --cycles 5000 --records-per-cycle 100

    # Use specific database path
    python kimi25-python-memory-leak-tests.py --db-path /tmp/my_test.ddb

    # Disable garbage collection to see raw memory behavior
    python kimi25-python-memory-leak-tests.py --no-gc
        """
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=1000,
        help="Number of test cycles to run (default: 1000)"
    )
    parser.add_argument(
        "--records-per-cycle",
        type=int,
        default=50,
        help="Number of records to insert per cycle (default: 50)"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path for test database (default: temp file)"
    )
    parser.add_argument(
        "--no-gc",
        action="store_true",
        help="Disable forced garbage collection between cycles"
    )

    args = parser.parse_args()

    # Validate dependencies
    if not HAS_PSUTIL:
        console.print("[yellow]Warning: psutil not installed. Using fallback memory tracking.[/yellow]")
        console.print("[yellow]For best results, install psutil: pip install psutil[/yellow]\n")

    # Setup database path
    if args.db_path:
        db_path = args.db_path
    else:
        db_path = os.path.join(tempfile.gettempdir(), f"decentdb_memtest_{int(time.time())}.ddb")

    # Ensure we can import decentdb
    try:
        version = decentdb.engine_version()
        console.print(f"[green]Using DecentDB engine version: {version}[/green]\n")
    except Exception as e:
        console.print(f"[red]Error: Could not initialize DecentDB: {e}[/red]")
        sys.exit(1)

    # Create and run test
    test = DecentDBMemoryTest(
        db_path=db_path,
        num_cycles=args.cycles,
        records_per_cycle=args.records_per_cycle,
        enable_gc=not args.no_gc
    )

    try:
        exit_code = test.run_test()
    except KeyboardInterrupt:
        console.print("\n[yellow]Test interrupted by user[/yellow]")
        exit_code = 130
    finally:
        # Cleanup
        for ext in ["", ".wal", ".shm", "-wal", "-shm", "-lock"]:
            try:
                path = db_path + ext
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass

    sys.exit(exit_code if isinstance(exit_code, int) else 0)


if __name__ == "__main__":
    main()
