"""Benchmark scenarios for embedded database comparisons."""

from scenarios.canonical_workloads import (
    BindingParityWorkload,
    OrdersWorkload,
    EventsWorkload,
    get_workload,
    WORKLOADS,
)

__all__ = [
    "BindingParityWorkload",
    "OrdersWorkload",
    "EventsWorkload",
    "get_workload",
    "WORKLOADS",
]
