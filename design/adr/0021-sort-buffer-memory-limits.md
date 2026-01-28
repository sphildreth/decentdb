# 21. Increased Sort Buffer & Memory Limits

Date: 2026-01-28

## Status

Superseded by ADR-0022

## Context

The initial SPEC defined a default `sort_buffer_size` of **1MB** and implied that exceeding this would trigger a "Spill to Disk" (External Merge Sort). 

1.  **Complexity**: Implementing a robust external merge sort is a significant engineering task (temp file management, serialization, I/O scheduling), risky for MVP.
2.  **Reality**: Modern machines (even low-end embedded vs) have GBs of RAM. 1MB is artificially low.
3.  **Dataset**: Sorting 9.5M tracks requires significant memory just for the sort keys/pointers.

## Decision

1.  **Increase Default Limit**: Raise default `sort_buffer_size` to **64MB**.
2.  **In-Memory Sort Only (MVP)**: We will **not** implement disk spilling for the MVP.
    *   If a `ORDER BY` query exceeds the `sort_buffer_size`, the query will abort with `ERR_MEMORY_LIMIT`.
    *   This significantly simplifies the `Exec` module.
3.  **Configurable**: The limit remains configurable. Users on constrained devices can lower it; users on servers can raise it (e.g., 512MB).

## Consequences

*   **Simplification**: Removes need for temp-file management in the Execution engine.
*   **Limitation**: Massive sorts (e.g., `SELECT * FROM tracks ORDER BY title`) might fail if they don't fit in RAM.
*   **Mitigation**: Users are encouraged to `ORDER BY` indexed columns (which don't require sorting) or increase the memory config. This is a "Decent" trade-off for an embedded engine MVP.
