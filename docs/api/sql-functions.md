# SQL Function Reference

This page documents SQL functions and aggregate/window additions recently implemented in DecentDB.

For broader syntax coverage, see the SQL reference and feature matrix.

## Operational inspection views

DecentDB exposes operational inspection surfaces through stable, read-only `sys.*`
views. These are not persistent catalog tables and do not accept bind
parameters. Use the documented `SELECT *` forms; arbitrary projection, joins,
`LIMIT`, and bind parameters are not part of this surface.

The canonical operational surfaces are:

```sql
SELECT * FROM sys.sync_status;
SELECT * FROM sys.wal_metrics;
SELECT * FROM sys.write_queue_metrics;
SELECT * FROM sys.storage_metrics;
SELECT * FROM sys.reactive_metrics;
SELECT * FROM sys.reactive_subscriptions;
```

Legacy `sys_sync_*` names remain for sync inspection compatibility:

- `sys_sync_status`
- `sys_sync_journal`
- `sys_sync_retention`
- `sys_sync_peer_lag`
- `sys_sync_peers`
- `sys_sync_scopes`
- `sys_sync_scope_tables`
- `sys_sync_peer_scopes`
- `sys_sync_sessions`
- `sys_sync_conflict_policy`
- `sys_sync_conflicts`
- `sys_sync_doctor`

### `sys.sync_status`

One row describing the local sync state. It has the same shape and values as
legacy `sys_sync_status`.

| Column | Type | Nullable | Unit / meaning |
|---|---|---:|---|
| `enabled` | `BOOL` | no | Whether sync capture is enabled. |
| `replica_id` | `TEXT` | yes | Local replica identity, or `NULL` before sync initialization. |
| `next_sequence` | `INT64` | no | Next local sync journal sequence number. |
| `journal_path` | `TEXT` | no | Sync journal sidecar path for this database handle. |
| `journal_size_bytes` | `INT64` | no | Current sync journal size in bytes. |

Example:

```sql
SELECT * FROM sys.sync_status;
SELECT * FROM sys_sync_status; -- legacy compatibility
```

### `sys.write_queue_metrics`

One row describing the current engine-owned write queue snapshot. All columns
are non-null. Calling this view may initialize the lazy queue object, but it
does not route direct writes through queued execution.

| Column | Type | Unit / meaning |
|---|---|---|
| `capacity` | `INT64` | Maximum admitted queued requests for this handle. |
| `current_depth` | `INT64` | Requests currently waiting in the queue. |
| `admitted` | `INT64` | Requests admitted since this handle's queue was initialized. |
| `rejected` | `INT64` | Requests rejected because immediate admission was impossible. |
| `timed_out` | `INT64` | Requests timed out before admission or execution start. |
| `canceled` | `INT64` | Requests canceled before execution start. |
| `executed` | `INT64` | Requests whose SQL execution started. |
| `committed` | `INT64` | Successfully committed queued requests. |
| `failed` | `INT64` | Queued requests that failed during execution or group sync. |
| `group_commit_batches` | `INT64` | Successful queued batches covered by strict group commit accounting. |
| `group_commit_syncs` | `INT64` | Physical WAL syncs performed for grouped queued batches. |
| `group_commit_max_batch` | `INT64` | Largest successful queued batch size observed. |
| `group_commit_commits_covered` | `INT64` | Successful queued commits covered by group commit accounting. |
| `physical_syncs_saved` | `INT64` | Estimated syncs avoided by grouped queued commits. |
| `total_queue_wait_ns` | `INT64` | Sum of queue wait time for executed requests, in nanoseconds. |

Example:

```sql
SELECT * FROM sys.write_queue_metrics;
```

### `sys.wal_metrics`

One row describing the current WAL runtime state. All columns are non-null.

| Column | Type | Unit / meaning |
|---|---|---|
| `latest_lsn` | `INT64` | Current WAL end offset / latest visible snapshot boundary. |
| `file_size_bytes` | `INT64` | WAL sidecar file size in bytes. |
| `active_readers` | `INT64` | Active WAL reader snapshots registered on this shared WAL. |
| `max_page_count` | `INT64` | Maximum page count currently known to the WAL handle. |
| `checkpoint_epoch` | `INT64` | In-memory checkpoint epoch counter for this WAL handle. |
| `warning_count` | `INT64` | Current reader-retention warning count. |
| `version_count` | `INT64` | WAL page versions tracked in memory or sidecar index. |
| `resident_versions` | `INT64` | WAL page versions with resident payloads. |
| `on_disk_versions` | `INT64` | WAL page versions whose payload is read back from WAL storage. |
| `shared_wal` | `BOOL` | Whether this handle is using the process shared-WAL registry. |

Example:

```sql
SELECT * FROM sys.wal_metrics;
```

### `sys.storage_metrics`

One row describing the current database file and storage snapshot. All columns
are non-null.

| Column | Type | Unit / meaning |
|---|---|---|
| `path` | `TEXT` | Database path for this handle. |
| `wal_path` | `TEXT` | WAL sidecar path for this handle. |
| `format_version` | `INT64` | Decoded database file-format version. |
| `page_size` | `INT64` | Database page size in bytes. |
| `cache_size_mb` | `INT64` | Configured page cache size in MiB. |
| `page_count` | `INT64` | Database file page count on disk. |
| `schema_cookie` | `INT64` | Current schema cookie from the database header. |
| `wal_end_lsn` | `INT64` | Current WAL end offset / latest visible snapshot boundary. |
| `wal_file_size` | `INT64` | WAL sidecar file size in bytes. |
| `last_checkpoint_lsn` | `INT64` | Last checkpoint LSN persisted in the database header. |
| `active_readers` | `INT64` | Active WAL reader snapshots. |
| `wal_versions` | `INT64` | WAL page versions tracked in memory or sidecar index. |
| `warning_count` | `INT64` | Current reader-retention warning count. |
| `shared_wal` | `BOOL` | Whether this handle is using the process shared-WAL registry. |

Example:

```sql
SELECT * FROM sys.storage_metrics;
```

### `sys.reactive_metrics`

One row describing in-process reactive subscription state.

| Column | Type | Unit / meaning |
|---|---|---|
| `active_watch_count` | `INT64` | Active table, range, query, and change-stream watches. |
| `table_watch_count` | `INT64` | Active table watches. |
| `range_watch_count` | `INT64` | Active primary-key range watches. |
| `query_watch_count` | `INT64` | Active query watches. |
| `change_stream_count` | `INT64` | Active change streams. |
| `events_published` | `INT64` | Commit events published by the reactive hub. |
| `events_delivered` | `INT64` | Events delivered to watch queues without overflow. |
| `events_dropped` | `INT64` | Events dropped because a watch queue lagged. |
| `lagged_watch_count` | `INT64` | Watches currently marked lagged. |
| `row_change_events_truncated` | `INT64` | Commit events whose row details were reduced to table invalidation. |

Example:

```sql
SELECT * FROM sys.reactive_metrics;
```

### `sys.reactive_subscriptions`

One row per active in-process watch.

| Column | Type | Unit / meaning |
|---|---|---|
| `watch_id` | `INT64` | In-process watch identifier. |
| `kind` | `TEXT` | `table`, `range`, `query`, or `change_stream`. |
| `created_at_micros` | `INT64` | Watch creation timestamp in Unix microseconds. |
| `queue_capacity` | `INT64` | Per-watch event queue capacity. |
| `queue_depth` | `INT64` | Events currently waiting in the watch queue. |
| `last_delivered_event_id` | `INT64` | Last event ID read by the watch handle. |
| `dropped_events` | `INT64` | Events dropped for this watch because of queue overflow. |
| `lagged` | `BOOL` | Whether the watch is currently lagged and must resynchronize. |
| `dependencies_json` | `TEXT` | Watch dependency description as JSON. |

Example:

```sql
SELECT * FROM sys.reactive_subscriptions ORDER BY watch_id;
```

### Lifecycle and compatibility notes

- `sys.write_queue_metrics` is a one-row snapshot of `Db::write_queue_metrics`
  and the C ABI `ddb_db_write_queue_metrics` values. Counter values are
  accumulated for the current database handle's lazy queue lifetime and reset
  when the database is reopened through a new handle.
- `sys.reactive_metrics` and `sys.reactive_subscriptions` describe only
  in-process watch handles. They are not durable changefeed state and reset
  when the process exits.
- `sys.storage_metrics` is a one-row snapshot equivalent to `Db::storage_info`
  for stable fields, and includes both database and WAL paths.
- `sys.wal_metrics` is a one-row snapshot of internal WAL runtime counters such
  as active readers, warning state, payload versions, and checkpoint state.
- `sys.sync_status` is the canonical name for the sync status row. The
  `sys_sync_status` compatibility name remains supported.
- These surfaces do not write telemetry rows, create catalog objects, or enable
  slow-query, lock-wait, index-usage, advisor, or Doctor findings tracing.

### `sys_sync_status`

Columns:

- `enabled`
- `replica_id`
- `next_sequence`
- `journal_path`
- `journal_size_bytes`

Example:

```sql
SELECT * FROM sys_sync_status;
```

### `sys_sync_journal`

Columns:

- `sequence`
- `replica_id`
- `transaction_lsn`
- `table_name`
- `operation`
- `primary_key_json`
- `after_json`
- `schema_cookie`
- `committed_at_micros`

Notes:

- `SELECT * FROM sys_sync_journal` returns the full journal.
- `SELECT * FROM sys_sync_journal WHERE sequence > 42` is also recognized by
  the engine for incremental inspection.
- Ordered variants on `sequence` are accepted.

Example:

```sql
SELECT * FROM sys_sync_journal WHERE sequence > 100 ORDER BY sequence;
```

### `sys_sync_peers`

Columns:

- `name`
- `endpoint`
- `token_env`
- `created_at_micros`
- `updated_at_micros`

### `sys_sync_retention`

Columns:

- `journal_records`
- `first_sequence`
- `last_sequence`
- `safe_prune_through`
- `prunable_records`
- `blocked_by_json`
- `journal_size_bytes`

### `sys_sync_peer_lag`

Columns:

- `peer_name`
- `remote_replica_id`
- `in_watermark`
- `out_watermark`
- `local_high_watermark`
- `in_lag`
- `out_lag`

### `sys_sync_doctor`

Columns:

- `enabled`
- `replica_id`
- `highest_severity`
- `journal_records`
- `journal_size_bytes`
- `unresolved_conflicts`
- `guidance_json`

### `sys_sync_scopes`

Columns:

- `name`
- `include_tables_json`
- `row_filter`
- `filter_columns_json`
- `created_at_micros`
- `updated_at_micros`

### `sys_sync_scope_tables`

Columns:

- `scope_name`
- `table_name`

### `sys_sync_peer_scopes`

Columns:

- `peer_name`
- `scope_name`
- `created_at_micros`
- `updated_at_micros`

### `sys_sync_sessions`

Columns:

- `session_id`
- `peer_name`
- `direction`
- `remote_replica_id`
- `started_at_micros`
- `ended_at_micros`
- `status`
- `error`
- `pushed_batch_id`
- `pulled_batch_id`
- `pushed_seen`
- `pushed_applied`
- `pushed_skipped`
- `pushed_conflicted`
- `pulled_seen`
- `pulled_applied`
- `pulled_skipped`
- `pulled_conflicted`
- `retry_count`

### `sys_sync_conflict_policy`

Columns:

- `default_policy`
- `origin_priority_json`

### `sys_sync_conflicts`

Columns:

- `conflict_id`
- `batch_id`
- `remote_replica_id`
- `remote_sequence`
- `table_name`
- `operation`
- `conflict_type`
- `message`
- `primary_key_json`
- `remote_record_json`
- `local_row_json`
- `created_at_micros`
- `resolved`
- `resolution`
- `resolved_at_micros`
- `resolved_by`
- `resolution_note`
- `policy_name`
- `local_record_json`

Example:

```sql
SELECT * FROM sys_sync_conflicts ORDER BY conflict_id;
SELECT * FROM sys_sync_conflict_policy;
```

## Subquery operators

Supported:

- `EXISTS (subquery)` / `NOT EXISTS (subquery)`
- `expr op ANY (subquery)` and `expr op SOME (subquery)` (`SOME` is a synonym)
- `expr op ALL (subquery)`

Behavior notes:

- Subquery comparison operators support `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`.
- `ANY` returns `TRUE` if at least one comparison is true; `ALL` returns `TRUE` only if all comparisons are true.
- Empty subquery semantics follow SQL quantifier rules: `ANY` yields `FALSE`, `ALL` yields `TRUE`.
- `NULL` comparison propagation follows SQL three-valued logic.

Examples:

```sql
SELECT * FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id);

SELECT * FROM employees
WHERE salary > ANY (SELECT salary FROM peers);

SELECT * FROM employees
WHERE salary >= ALL (SELECT salary FROM peers);
```

## Regex comparison operators

Supported:

- `left ~ pattern` (case-sensitive match)
- `left ~* pattern` (case-insensitive match)
- `left !~ pattern` (case-sensitive non-match)
- `left !~* pattern` (case-insensitive non-match)

Behavior notes:

- Both operands must be `TEXT`; otherwise an SQL type error is raised.
- `NULL` operands yield `NULL`.
- Invalid regex patterns return an SQL error.

Examples:

```sql
SELECT name FROM users WHERE name ~ '^A';
SELECT name FROM users WHERE name ~* '^admin';
SELECT name FROM users WHERE name !~ 'bot$';
SELECT name FROM users WHERE name !~* '^test_';
```

## Math functions

### Trigonometric

Supported:

- `SIN(x)`
- `COS(x)`
- `TAN(x)`
- `ASIN(x)`
- `ACOS(x)`
- `ATAN(x)`
- `ATAN2(y, x)`
- `PI()`
- `DEGREES(x)`
- `RADIANS(x)`
- `COT(x)`

Behavior notes:

- Numeric inputs are accepted (`INT64`, `FLOAT64`, `DECIMAL`); outputs are `FLOAT64`.
- `ASIN` and `ACOS` return `NULL` for out-of-domain values outside `[-1, 1]`.
- `TAN` returns `NULL` near undefined points (odd multiples of `π/2`).
- `COT` returns `NULL` when `tan(x)` is approximately zero.

Examples:

```sql
SELECT SIN(PI() / 2), COS(0), TAN(PI() / 4);
SELECT ASIN(1), ACOS(0), ATAN2(1, 1);
SELECT DEGREES(PI()), RADIANS(180), COT(PI() / 4);
```

## Conditional functions

Supported:

- `GREATEST(value1, value2, ...)`
- `LEAST(value1, value2, ...)`
- `IIF(condition, then_value, else_value)`

Behavior notes:

- `GREATEST`/`LEAST` return `NULL` if any argument is `NULL`.
- `IIF` follows `CASE`-like behavior and uses DecentDB truthiness semantics for the condition.

Examples:

```sql
SELECT GREATEST(10, 20, 15), LEAST(10, 20, 15);
SELECT IIF(score >= 60, 'pass', 'fail') FROM exams;
```

## Date/time functions

Supported:

- `DATE_TRUNC(precision, timestamp)`
- `DATE_PART(field, timestamp)`
- `DATE_DIFF(part, start, end)`
- `LAST_DAY(timestamp)`
- `NEXT_DAY(timestamp, weekday)`
- `MAKE_DATE(year, month, day)`
- `MAKE_TIMESTAMP(year, month, day, hour, minute, second)`
- `TO_TIMESTAMP(epoch_or_text [, format])`
- `AGE(timestamp [, timestamp])`
- `INTERVAL '...'` (for timestamp arithmetic)

Behavior notes:

- `DATE_TRUNC` supports: microsecond, millisecond, second, minute, hour, day, week, month, quarter, year, decade, century, millennium.
- `TO_TIMESTAMP(text, format)` currently supports formats: `YYYY-MM-DD HH24:MI:SS`, `YYYY-MM-DD`, and `DD/MM/YYYY`.
- `AGE` returns a textual interval (for example, `"1 days 00:00:00"`).
- `INTERVAL` literal parsing supports integer `year/month/week/day/hour/minute/second` units in amount-unit pairs.
- Timestamp interval arithmetic supports `timestamp +/- INTERVAL '...'` and date/timestamp text on the left side.

Examples:

```sql
SELECT DATE_TRUNC('month', '2024-03-15 14:30:45');
SELECT DATE_PART('doy', '2024-03-15');
SELECT DATE_DIFF('day', '2024-03-10', '2024-03-15');
SELECT LAST_DAY('2024-02-11'), NEXT_DAY('2024-03-15', 'Monday');
SELECT MAKE_DATE(2024, 3, 15), MAKE_TIMESTAMP(2024, 3, 15, 14, 30, 0);
SELECT TO_TIMESTAMP(1710505800), TO_TIMESTAMP('15/03/2024', 'DD/MM/YYYY');
SELECT AGE('2024-03-15', '2024-03-14');
SELECT '2024-03-15 14:30:00'::timestamp + INTERVAL '1 day';
```

## String functions

Supported:

- `CONCAT(expr, ...)`
- `CONCAT_WS(separator, expr, ...)`
- `POSITION(substring IN string)`
- `INITCAP(string)`
- `ASCII(string)`
- `REGEXP_REPLACE(string, pattern, replacement [, flags])`
- `SPLIT_PART(string, delimiter, index)`
- `STRING_TO_ARRAY(string, delimiter)`
- `QUOTE_IDENT(string)`
- `QUOTE_LITERAL(string)`
- `MD5(string)`
- `SHA256(string)`

Behavior notes:

- `CONCAT` treats `NULL` arguments as empty strings.
- `CONCAT_WS` skips `NULL` value arguments; `NULL` separator returns `NULL`.
- `POSITION` returns 1-based positions, and `0` if no match exists.
- `REGEXP_REPLACE` supports `g` (global) and `i` (case-insensitive) flags.
- `STRING_TO_ARRAY` returns a JSON text array.

Examples:

```sql
SELECT CONCAT('hello', ' ', 'world');
SELECT CONCAT_WS(', ', 'Alice', NULL, 'Bob');
SELECT POSITION('world' IN 'hello world');
SELECT INITCAP('hello world from decentdb');
SELECT ASCII('A');
SELECT REGEXP_REPLACE('abc123def', '\d', '', 'g');
SELECT SPLIT_PART('a,b,c', ',', 2);
SELECT STRING_TO_ARRAY('a,b,c', ',');
SELECT QUOTE_IDENT('table name'), QUOTE_LITERAL('O''Brien');
SELECT MD5('hello'), SHA256('hello');
```

## Spatial functions

Spatial functions operate on native `GEOMETRY` and `GEOGRAPHY` values. Spatial values are stored as normalized EWKB; `GEOGRAPHY` uses SRID 4326 and lon/lat coordinates.

Supported:

- Constructors: `ST_Point`, `ST_MakePoint`, `ST_PointZ`, `ST_PointM`, `ST_PointZM`
- Geography point constructors: `ST_GeogPoint`, `ST_GeogPointZ`, `ST_GeogPointM`, `ST_GeogPointZM`
- Import/export: `ST_GeomFromText`, `ST_GeogFromText`, `ST_GeomFromWKB`, `ST_GeogFromWKB`, `ST_GeomFromGeoJSON`, `ST_GeogFromGeoJSON`, `ST_AsText`, `ST_AsBinary`, `ST_AsGeoJSON`
- Accessors: `ST_SRID`, `ST_SetSRID`, `ST_GeometryType`, `ST_X`, `ST_Y`, `ST_Z`, `ST_M`, `ST_IsValid`
- Predicates: `ST_DWithin`, `ST_Intersects`, `ST_Contains`, `ST_Within`, `ST_Equals`
- Measurements: `ST_Distance`, `ST_Length`, `ST_Area`
- Distance ordering: `<->`

Behavior notes:

- `ST_Distance` returns meters for `GEOGRAPHY` point-to-point distance and planar units for `GEOMETRY`.
- `ST_DWithin` uses the same units as `ST_Distance`.
- `ST_Length` and `ST_Area` are planar for `GEOMETRY`; GEOGRAPHY uses spherical approximations.
- Spatial indexes (`CREATE INDEX ... USING spatial`) are single-column indexes for `GEOMETRY` and `GEOGRAPHY`.

Examples:

```sql
CREATE TABLE places (id INT PRIMARY KEY, geog GEOGRAPHY(POINT,4326));
CREATE INDEX idx_places_geog ON places USING spatial(geog);

INSERT INTO places VALUES (1, ST_GeogPoint(-97.7431, 30.2672));

SELECT id
FROM places
WHERE ST_DWithin(geog, ST_GeogPoint(-97.7431, 30.2672), 5000);

SELECT ST_AsText(ST_GeomFromText('POINT(1 2)'));
SELECT ST_Area(ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0))'));
SELECT id FROM places ORDER BY geog <-> ST_GeogPoint(-97.7431, 30.2672) LIMIT 10;
```

## Aggregate functions

### Statistical aggregates

Supported:

- `STDDEV(expr)` (alias of `STDDEV_SAMP`)
- `STDDEV_SAMP(expr)`
- `STDDEV_POP(expr)`
- `VARIANCE(expr)` (alias of `VAR_SAMP`)
- `VAR_SAMP(expr)`
- `VAR_POP(expr)`

Behavior notes:

- Implemented using a numerically stable online (Welford-style) accumulation strategy.
- `*_SAMP` forms return `NULL` when fewer than 2 non-`NULL` values exist.
- Population forms return `NULL` for empty input sets.
- `DISTINCT` is supported.

### Boolean aggregates

Supported:

- `BOOL_AND(expr)`
- `BOOL_OR(expr)`

Behavior notes:

- `NULL` inputs are ignored.
- If all values are `NULL`, result is `NULL`.
- Non-boolean non-`NULL` inputs are rejected.

### Collection and ordered-set aggregates

Supported:

- `ARRAY_AGG(expr [ORDER BY ...])`
- `MEDIAN(expr)`
- `PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY expr)`
- `PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY expr)`

Behavior notes:

- `ARRAY_AGG` returns JSON text arrays (for example, `"[1,null,2]"`).
- `ARRAY_AGG(DISTINCT ...)` is supported.
- `MEDIAN` returns `FLOAT64` and ignores `NULL` inputs.
- Percentile fraction must be between `0` and `1` inclusive.
- `PERCENTILE_CONT` interpolates and returns `FLOAT64`.
- `PERCENTILE_DISC` returns a value from the ordered input domain.

Examples:

```sql
SELECT STDDEV(amount), VARIANCE(amount), BOOL_AND(amount > 0), BOOL_OR(amount > 100) FROM orders;

SELECT ARRAY_AGG(amount ORDER BY created_at) FROM orders;
SELECT MEDIAN(amount) FROM orders;

SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) FROM orders;
SELECT PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY amount) FROM orders;
```

## Window functions

Additional supported window features include:

- `NTILE(n)`
- `PERCENT_RANK()`
- `CUME_DIST()`
- Aggregate window functions such as `SUM(...) OVER (...)`, `COUNT(...) OVER (...)`, `MIN/MAX/AVG/... OVER (...)`
- `ROWS` frame clauses
- `RANGE` frames for `UNBOUNDED`/`CURRENT ROW` style bounds (offset-based `RANGE` bounds are not yet supported)

Examples:

```sql
SELECT id, NTILE(4) OVER (ORDER BY salary DESC) AS quartile FROM employees;

SELECT id,
       PERCENT_RANK() OVER (ORDER BY score) AS pct_rank,
       CUME_DIST() OVER (ORDER BY score) AS cume_dist
FROM results;

SELECT created_at, amount,
       SUM(amount) OVER (
         ORDER BY created_at
         ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
       ) AS rolling_sum
FROM orders;
```
