"""High-precision performance timer for benchmarks.

Provides nanosecond-resolution timing using time.perf_counter().
"""

import time
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Timer:
    """High-precision timer for benchmark measurements."""

    _start: Optional[float] = field(default=None, init=False, repr=False)
    _end: Optional[float] = field(default=None, init=False, repr=False)

    def start(self):
        """Start the timer."""
        self._start = time.perf_counter()
        self._end = None

    def stop(self) -> float:
        """Stop the timer and return elapsed time in seconds.

        Returns:
            Elapsed time in seconds
        """
        self._end = time.perf_counter()
        return self.elapsed()

    def elapsed(self) -> float:
        """Get elapsed time in seconds.

        Returns:
            Elapsed time in seconds, or 0 if not started
        """
        if self._start is None:
            return 0.0
        end = self._end if self._end is not None else time.perf_counter()
        return end - self._start

    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds.

        Returns:
            Elapsed time in ms
        """
        return self.elapsed() * 1000.0

    def elapsed_us(self) -> float:
        """Get elapsed time in microseconds.

        Returns:
            Elapsed time in microseconds
        """
        return self.elapsed() * 1_000_000.0

    def elapsed_ns(self) -> float:
        """Get elapsed time in nanoseconds.

        Returns:
            Elapsed time in nanoseconds
        """
        return self.elapsed() * 1_000_000_000.0


class LatencyTracker:
    """Track latency measurements and compute statistics."""

    def __init__(self):
        self.latencies_ms: List[float] = []
        self.operation_count: int = 0
        self.error_count: int = 0

    def record(self, latency_ms: float):
        """Record a latency measurement in milliseconds."""
        self.latencies_ms.append(latency_ms)
        self.operation_count += 1

    def record_error(self):
        """Record an error (no latency)."""
        self.error_count += 1

    def get_statistics(self) -> dict:
        """Compute latency statistics.

        Returns:
            Dictionary with min, max, mean, p50, p95, p99, ops_count
        """
        if not self.latencies_ms:
            return {
                "min_ms": 0,
                "max_ms": 0,
                "mean_ms": 0,
                "p50_ms": 0,
                "p95_ms": 0,
                "p99_ms": 0,
                "ops_count": 0,
                "error_count": 0,
            }

        sorted_latencies = sorted(self.latencies_ms)
        n = len(sorted_latencies)

        return {
            "min_ms": sorted_latencies[0],
            "max_ms": sorted_latencies[-1],
            "mean_ms": sum(sorted_latencies) / n,
            "p50_ms": sorted_latencies[int(n * 0.50)],
            "p95_ms": sorted_latencies[int(n * 0.95)],
            "p99_ms": sorted_latencies[int(n * 0.99)]
            if n >= 100
            else sorted_latencies[-1],
            "ops_count": self.operation_count,
            "error_count": self.error_count,
        }

    def get_throughput(self, duration_sec: float) -> float:
        """Calculate operations per second.

        Args:
            duration_sec: Total duration in seconds

        Returns:
            Operations per second
        """
        if duration_sec <= 0:
            return 0.0
        return self.operation_count / duration_sec


class WarmupRunner:
    """Run warmup iterations before actual benchmarking."""

    def __init__(self, warmup_ops: int = 100):
        self.warmup_ops = warmup_ops

    def run(self, op_func, *args, **kwargs):
        """Run warmup operations.

        Args:
            op_func: Function to execute
            *args, **kwargs: Arguments to pass to op_func
        """
        for _ in range(self.warmup_ops):
            op_func(*args, **kwargs)


class BenchmarkRunner:
    """Orchestrate benchmark runs with timing and statistics."""

    def __init__(self, ops: int = 10000, warmup_ops: int = 100):
        self.ops = ops
        self.warmup_ops = warmup_ops
        self.timer = Timer()
        self.latency_tracker = LatencyTracker()

    def run(self, op_func, *args, **kwargs) -> dict:
        """Run benchmark with timing and statistics.

        Args:
            op_func: Function to execute for each operation
            *args, **kwargs: Arguments to pass to op_func

        Returns:
            Dictionary with benchmark results
        """
        # Warmup phase
        for _ in range(self.warmup_ops):
            op_func(*args, **kwargs)

        # Benchmark phase
        self.timer.start()
        for _ in range(self.ops):
            op_timer = Timer()
            op_timer.start()
            try:
                op_func(*args, **kwargs)
                latency = op_timer.stop()
                self.latency_tracker.record(latency * 1000)  # Convert to ms
            except Exception:
                self.latency_tracker.record_error()

        total_duration = self.timer.stop()

        # Compute results
        stats = self.latency_tracker.get_statistics()
        throughput = self.latency_tracker.get_throughput(total_duration)

        return {
            "operations": self.ops,
            "duration_sec": total_duration,
            "throughput_ops_sec": throughput,
            "latency_ms": stats,
        }

    def run_with_params(self, op_func, params_list: list) -> dict:
        """Run benchmark with different parameters for each operation.

        Args:
            op_func: Function to execute for each operation
            params_list: List of parameter tuples

        Returns:
            Dictionary with benchmark results
        """
        ops = len(params_list)

        # Warmup phase
        warmup_count = min(self.warmup_ops, ops)
        for params in params_list[:warmup_count]:
            op_func(*params)

        # Benchmark phase
        self.timer.start()
        for params in params_list[warmup_count:]:
            op_timer = Timer()
            op_timer.start()
            try:
                op_func(*params)
                latency = op_timer.stop()
                self.latency_tracker.record(latency * 1000)
            except Exception:
                self.latency_tracker.record_error()

        total_duration = self.timer.stop()

        # Compute results
        stats = self.latency_tracker.get_statistics()
        actual_ops = ops - warmup_count
        throughput = actual_ops / total_duration if total_duration > 0 else 0

        return {
            "operations": actual_ops,
            "duration_sec": total_duration,
            "throughput_ops_sec": throughput,
            "latency_ms": stats,
        }
