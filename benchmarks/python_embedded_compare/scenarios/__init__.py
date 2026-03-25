"""Benchmark scenarios for embedded database comparisons."""

from scenarios.canonical_workloads import (
    OrdersWorkload,
    EventsWorkload,
    get_workload,
    WORKLOADS,
)

__all__ = [
    "OrdersWorkload",
    "EventsWorkload",
    "get_workload",
    "WORKLOADS",
]
