# ADR 0163: Operational `sys.*` Metrics

**Date:** 2026-05-20

**Status:** Accepted

## Context

Roadmap item #2 in `design/FUTURE_WINS.md` requires built-in operational
metrics for WAL state, write-queue pressure, storage state, and sync status.
DecentDB already exposes several Rust/C ABI snapshots and legacy sync
inspection names, but applications need a stable SQL inspection surface that
does not require binding-specific APIs.

The feature must stay low overhead by default. It must not add runtime tracing,
advisors, Doctor findings, slow-query history, lock-wait history, index-usage
tracking, or telemetry writes.

## Decision

Expose four canonical read-only SQL inspection surfaces:

- `sys.sync_status`
- `sys.wal_metrics`
- `sys.write_queue_metrics`
- `sys.storage_metrics`

These surfaces are virtual one-row snapshots built from existing in-memory
state and metadata. They are not persistent catalog tables, do not write rows,
and do not create a real `sys` schema on disk. Only the documented
`SELECT * FROM ...` forms are stable in this release; arbitrary projection,
joins, predicates, limits, and bind parameters are intentionally out of scope.

`sys.sync_status` is the canonical sync-status name. Existing `sys_sync_*`
inspection names remain supported for compatibility, including
`sys_sync_status`.

`sys.write_queue_metrics` reads the same queue snapshot as
`Db::write_queue_metrics` and the C ABI `ddb_db_write_queue_metrics` structure.
Querying it may initialize the lazy per-`Db` queue object, but it does not route
direct execution through queued execution. Queue counters live for the current
queue handle lifetime and reset on reopen.

`sys.storage_metrics` reads the stable fields of `Db::storage_info`.
`sys.wal_metrics` reads current WAL runtime state from the shared WAL handle.
All counter and size values are returned as `INT64`; boolean flags are `BOOL`;
paths and identifiers are `TEXT`; nullable fields are limited to the documented
sync status fields.

## Consequences

Applications and bindings can inspect operational state with ordinary SQL:

```sql
SELECT * FROM sys.wal_metrics;
SELECT * FROM sys.write_queue_metrics;
SELECT * FROM sys.storage_metrics;
SELECT * FROM sys.sync_status;
```

The implementation keeps the hot path unchanged: the surfaces read already
maintained counters and metadata instead of adding recursive telemetry writes or
query tracing. Because the first contract is exact `SELECT *` forms, future
work can still replace the dispatch mechanism with richer virtual-table support
without changing the stable row contracts.

Doctor, advisors, slow-query tracing, lock-wait tracing, and index-usage
surfaces remain roadmap item #11.
