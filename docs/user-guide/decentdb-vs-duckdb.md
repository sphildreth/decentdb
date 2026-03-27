# DecentDB vs DuckDB: When to Choose Which

This document helps developers decide between **DecentDB** and **DuckDB** for embedded database workloads. Both are embeddable, but they target fundamentally different use cases.

> **Versions compared:** DecentDB 2.0.0 vs DuckDB 1.x (as of 2024).
>
> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for a per-feature support grid, and [SQL Reference](sql-reference.md) for DecentDB's full SQL surface.

## They Solve Different Problems

DecentDB and DuckDB are not direct competitors. They share the "embedded database" label but optimize for opposite ends of the workload spectrum:

- **DecentDB** is an OLTP engine: durability-first, one writer, many concurrent reader threads, optimized for point lookups, small transactions, and application state.
- **DuckDB** is an OLAP engine: throughput-first, parallel columnar execution, optimized for scans, aggregations, joins over large datasets, and analytics.

Choosing between them is less about feature checklists and more about what your application *does*.

## At a Glance

| Dimension | DecentDB | DuckDB |
|-----------|----------|--------|
| **Design priority** | Durability, then read performance | Analytical throughput, then flexibility |
| **Query engine** | Row-oriented, B-tree index seeks | Columnar, vectorized, parallel |
| **Concurrency model** | One writer, many concurrent reader threads (single process) | Single-connection writes; parallel within one query |
| **Default durability** | WAL + fsync-on-commit, always | Configurable; optimized for bulk analytics workflows |
| **Crash safety testing** | Built-in FaultyVFS + WAL failpoint hooks | Not a first-class testing surface |
| **Extension ecosystem** | None (extend via core contribution) | Rich (install extensions, UDFs) |
| **External data access** | Single `.ddb` file | Parquet, CSV, JSON, Iceberg, S3/GCS/Azure, HTTP |
| **SQL breadth** | Deliberate Postgres-like subset | Very broad, Postgres-compatible dialect |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | C/C++, Python, Java, Node.js, R, Go, Rust, Julia |
| **License** | MIT or Apache-2.0 | MIT |
| **Binary size** | ~2-3 MB | ~15-20 MB |
| **Platform support** | Tier 1 Rust platforms | Windows, macOS, Linux, WASM |
| **File format stability** | Stable from 2.0.0 | Stable, backward compatible |

## When DecentDB Is the Better Fit

### 1. Your workload is transactional, not analytical

DecentDB is built for workloads that look like: "insert a row, update a row, look up a row by key, repeat." If your application is an API backend, a session store, a queue, or anything that issues many small read/write operations, DecentDB's row-oriented B-tree engine is designed for this pattern. DuckDB's columnar engine adds overhead per transaction because it's optimized for scanning millions of rows, not touching one.

```sql
-- Typical OLTP workload: point lookups and small writes
SELECT * FROM sessions WHERE token = $1;
UPDATE carts SET item_count = item_count + 1 WHERE user_id = $1;
INSERT INTO orders (user_id, total) VALUES ($1, $2) RETURNING id;
```

### 2. You need guaranteed durability by default

DecentDB fsyncs on every commit. There is no configuration knob to disable it. If your application stores data that must survive power loss, kernel panics, or process crashes, DecentDB makes the safe thing the default.

```sql
-- DecentDB: every COMMIT is durable. No settings to tune.
BEGIN;
UPDATE account SET balance = balance - 100 WHERE id = 1;
UPDATE account SET balance = balance + 100 WHERE id = 2;
COMMIT;  -- fsync'd before returning to caller
```

DuckDB's durability model depends on the storage mode and configuration. It is designed for analytics workflows where re-running a pipeline on failure is acceptable.

### 3. You need full foreign key support including CASCADE

Both databases enforce foreign key constraints on INSERT and UPDATE. However, DecentDB supports `ON DELETE CASCADE`, `ON DELETE SET NULL`, and `ON DELETE SET DEFAULT`. DuckDB currently does not support cascading operations.

```sql
-- DecentDB: full FK support including CASCADE
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT REFERENCES users(id) ON DELETE CASCADE
);
INSERT INTO orders (user_id) VALUES (999);  -- ERROR: FK violation
DELETE FROM users WHERE id = 1;             -- Cascades to orders

-- DuckDB: FK enforced, but CASCADE not supported
CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT REFERENCES users(id)  -- CASCADE syntax parsed but not enforced
);
INSERT INTO orders (user_id) VALUES (999);  -- ERROR: FK violation (enforced!)
```

If you need cascading deletes or set-null behavior, DecentDB provides it. With DuckDB, you must implement cascade logic in application code.

### 4. You need triggers

DecentDB supports `AFTER` row triggers on tables and `INSTEAD OF` row triggers on views. DuckDB does not support triggers at all.

```sql
-- DecentDB: audit trail via trigger
CREATE TABLE audit_log (id INT PRIMARY KEY, msg TEXT, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP);

CREATE TRIGGER log_user_insert AFTER INSERT ON users
FOR EACH ROW
EXECUTE FUNCTION decentdb_exec_sql('INSERT INTO audit_log (msg) VALUES (''user created'')');
```

If you need automatic side-effects on writes (auditing, cascading logic, materialized view refresh), triggers are the natural mechanism, and DuckDB lacks them entirely.

### 5. You need savepoints for partial rollback

DecentDB supports `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` within transactions. DuckDB does not.

```sql
-- DecentDB: partial rollback within a transaction
BEGIN;
INSERT INTO orders (user_id, total) VALUES (1, 29.99);
SAVEPOINT sp1;
INSERT INTO order_items (order_id, sku) VALUES (1, 'INVALID-SKU');
ROLLBACK TO SAVEPOINT sp1;  -- undo the bad insert only
INSERT INTO order_items (order_id, sku) VALUES (1, 'SKU-123');
RELEASE SAVEPOINT sp1;
COMMIT;  -- order + valid item committed
```

### 6. You need recursive JSON traversal

DecentDB supports `json_tree()` for recursive traversal of nested JSON. DuckDB does not.

```sql
-- DecentDB: recursively walk nested JSON
SELECT key, value, type, path
FROM json_tree('{"config":{"db":{"host":"localhost","port":5432},"cache":{"ttl":300}}}');

-- Returns:
-- config | {"db":{"host":"localhost","port":5432},"cache":{"ttl":300}} | object | $.config
-- db     | {"host":"localhost","port":5432}                          | object | $.config.db
-- host   | localhost                                                  | text   | $.config.db.host
-- port   | 5432                                                       | number | $.config.db.port
-- cache  | {"ttl":300}                                                | object | $.config.cache
-- ttl    | 300                                                        | number | $.config.cache.ttl
```

### 7. You need trigram substring search

DecentDB offers a built-in trigram index type for fast `LIKE '%pattern%'` queries. DuckDB does not have an equivalent native index type.

```sql
-- DecentDB: trigram index for substring search
CREATE INDEX idx_users_name_trgm ON users USING trigram(name);

-- Uses the trigram index instead of a full scan
SELECT * FROM users WHERE name LIKE '%alice%';
```

Without a trigram index, `LIKE '%pattern%'` requires a full table scan in both databases.

### 8. You need `TOTAL()` (NULL-safe sum)

DecentDB provides `TOTAL()`, which returns `0.0` for empty sets instead of `NULL`. DuckDB does not have this function.

```sql
-- DecentDB
SELECT TOTAL(amount) FROM orders WHERE category = 'nonexistent';  -- returns 0.0
SELECT SUM(amount) FROM orders WHERE category = 'nonexistent';    -- returns NULL

-- DuckDB: no TOTAL(), must use COALESCE
SELECT COALESCE(SUM(amount), 0.0) FROM orders WHERE category = 'nonexistent';
```

Note: Both databases support `GROUP_CONCAT` (DuckDB has it as an alias for `STRING_AGG`).



### 9. You need a single-process, multi-threaded reader architecture

DecentDB is designed for one process with multiple concurrent reader threads that get lock-free snapshot isolation. DuckDB executes queries in parallel within a single connection but is not designed for many concurrent connections issuing queries simultaneously.

```
DecentDB model:
  Process
    ├─ Writer thread (holds commit lock)
    ├─ Reader thread 1 (snapshot, no blocking)
    ├─ Reader thread 2 (snapshot, no blocking)
    └─ Reader thread N (snapshot, no blocking)

DuckDB model:
  Connection
    └─ Query runs with internal parallelism (multi-core within one query)
```

If your application is a web server or API that must handle many concurrent read requests on separate threads, DecentDB's model is the natural fit.

## When DuckDB Is the Better Fit

### 1. Your workload is analytical (aggregations, scans, joins over large data)

DuckDB's columnar, vectorized, parallel engine is purpose-built for analytical queries. It will dramatically outperform DecentDB on queries that scan, filter, aggregate, or join millions of rows.

```sql
-- Analytical query: DuckDB excels here
SELECT
  DATE_TRUNC('month', order_date) AS month,
  product_category,
  COUNT(*) AS orders,
  SUM(amount) AS revenue,
  AVG(amount) AS avg_order_value
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

DecentDB can execute this query, but DuckDB's columnar execution and parallelism make it orders of magnitude faster on large datasets.

### 2. You need to query external data directly

DuckDB can read Parquet, CSV, JSON, Arrow, Iceberg, and remote files (S3, GCS, Azure, HTTP) directly in SQL without loading them into a database first. DecentDB operates on a single `.ddb` file.

```sql
-- DuckDB: query Parquet files directly
SELECT * FROM 'data/orders.parquet' WHERE amount > 100;

-- DuckDB: query S3 directly
SELECT * FROM 's3://my-bucket/logs/*.parquet' WHERE status = 'error';

-- DuckDB: query CSV with automatic schema inference
SELECT * FROM read_csv('data/users.csv', auto_detect = true) WHERE age > 30;

-- DuckDB: join a local table with a remote Parquet file
SELECT l.name, r.total
FROM local_users l
JOIN 's3://analytics/revenue.parquet' r ON l.id = r.user_id;
```

DecentDB requires you to load data into the `.ddb` file first.

### 3. You need to read multiple formats and export to others

DuckDB is a Swiss-army knife for data format conversion. It reads Parquet, CSV, JSON, Arrow, and writes to all of them.

```sql
-- DuckDB: convert CSV to Parquet
COPY (SELECT * FROM read_csv('raw.csv')) TO 'output.parquet' (FORMAT PARQUET);

-- DuckDB: export query results to JSON
COPY (SELECT * FROM users WHERE active) TO 'active_users.json' (FORMAT JSON);

-- DuckDB: write to Parquet with compression and partitioning
COPY orders TO 'orders/' (FORMAT PARQUET, PARTITION_BY (year, month), COMPRESSION zstd);
```

### 4. You need parallel query execution

DuckDB parallelizes individual queries across multiple cores. A single `SELECT` with a large scan or aggregation will use all available CPU cores automatically.

```sql
-- DuckDB: this query runs in parallel across cores automatically
SELECT category, COUNT(*), AVG(price), SUM(quantity)
FROM line_items
GROUP BY category;
```

DecentDB executes queries on a single core per query. You get concurrency through multiple reader threads running different queries, not parallelism within one query.

### 5. You need a rich type system for analytics

DuckDB supports types that DecentDB does not: `LIST`, `STRUCT`, `MAP`, `ARRAY`, `INTERVAL`, `HUGEINT` (128-bit integer), `BIT`, `ENUM`, `UNION`, `TIME`, `TIMETZ`, and more.

```sql
-- DuckDB: nested and complex types
CREATE TABLE events (
  id INT,
  tags LIST<TEXT>,
  metadata STRUCT(host TEXT, port INT),
  properties MAP(TEXT, TEXT)
);

SELECT tags FROM events WHERE 'error' IN tags;
SELECT metadata.host FROM events WHERE metadata.port = 8080;
```

### 6. You need built-in statistical and machine-learning functions

DuckDB has an extensive library of statistical, mathematical, and analytical functions that go far beyond what DecentDB offers.

```sql
-- DuckDB: statistical functions
SELECT
  CORR(x, y) AS correlation,
  COVAR_POP(x, y) AS covariance,
  REGR_SLOPE(y, x) AS slope,
  REGR_INTERCEPT(y, x) AS intercept
FROM measurements;

-- DuckDB: quantile with multiple methods
SELECT QUANTILE_CONT(amount, 0.95) FROM orders;
SELECT QUANTILE_DISC(amount, 0.5) FROM orders;
SELECT MEDIAN(amount) FROM orders;
```

### 7. You need window frame clauses

DuckDB supports `ROWS BETWEEN` and `RANGE BETWEEN` frame specifications. DecentDB does not currently support frame clauses.

```sql
-- DuckDB: rolling 7-day average
SELECT date, amount,
  AVG(amount) OVER (
    ORDER BY date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_avg
FROM daily_sales;

-- DuckDB: cumulative sum with frame
SELECT date, amount,
  SUM(amount) OVER (
    ORDER BY date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total
FROM daily_sales;
```

### 8. You need extensions

DuckDB has a rich extension ecosystem: `spatial` (GIS), `httpfs` (S3/HTTP), `parquet`, `json`, `autocomplete`, `delta`, `iceberg`, and community extensions. Extensions are installed with a single command.

```sql
-- DuckDB: install and load extensions
INSTALL spatial;
LOAD spatial;

-- Query GeoJSON with the spatial extension
SELECT name, ST_Area(geom) FROM countries;

-- DuckDB: httpfs for remote data
INSTALL httpfs;
LOAD httpfs;
SELECT * FROM 'https://example.com/data.parquet';
```

DecentDB has no extension mechanism. All features are built into the core.

### 9. You need to work with Pandas, Arrow, or Polars

DuckDB has first-class integration with Python data frameworks. You can query Pandas DataFrames, Arrow tables, and Polars DataFrames directly in SQL.

```python
import duckdb
import pandas as pd

df = pd.read_csv("sales.csv")
# Query the DataFrame directly with SQL
result = duckdb.sql("SELECT category, SUM(amount) FROM df GROUP BY category").df()
```

### 10. You need `LISTAGG` or ordered string aggregation with rich syntax

DuckDB supports `LISTAGG` with full ordering and distinct control, which is more flexible than DecentDB's `STRING_AGG`/`GROUP_CONCAT`.

```sql
-- DuckDB: LISTAGG with ordering and distinct
SELECT department,
  LISTAGG(name, ', ') WITHIN GROUP (ORDER BY name) AS team
FROM employees
GROUP BY department;

-- DuckDB: LISTAGG with DISTINCT
SELECT department,
  LISTAGG(DISTINCT role, ', ') WITHIN GROUP (ORDER BY role) AS roles
FROM employees
GROUP BY department;
```

## Side-by-Side SQL Examples

### Aggregation: statistical summary

```sql
-- Both support standard aggregates
SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders;

-- Both support statistical aggregates
SELECT STDDEV(amount), VARIANCE(amount) FROM orders;

-- DuckDB: additional analytics functions
SELECT
  QUANTILE_CONT(amount, 0.5) AS median,
  QUANTILE_CONT(amount, 0.95) AS p95,
  SKEWNESS(amount) AS skew,
  KURTOSIS(amount) AS kurtosis
FROM orders;

-- DecentDB: equivalent using built-in functions
SELECT
  MEDIAN(amount) AS median,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95
FROM orders;
```

### Transaction safety

```sql
-- DecentDB: durable by default, savepoints supported
BEGIN;
UPDATE inventory SET qty = qty - 1 WHERE sku = 'WIDGET';
SAVEPOINT sp1;
INSERT INTO shipments (order_id) VALUES (42);
ROLLBACK TO SAVEPOINT sp1;  -- undo shipment, keep inventory update
COMMIT;

-- DuckDB: savepoints not supported, durability is configurable
BEGIN;
UPDATE inventory SET qty = qty - 1 WHERE sku = 'WIDGET';
COMMIT;
-- No partial rollback available
```

### Querying external data

```sql
-- DuckDB: query Parquet directly, no import step
SELECT user_id, SUM(amount) AS total
FROM 's3://analytics/2024/orders/*.parquet'
WHERE region = 'us-east'
GROUP BY user_id
HAVING total > 1000;

-- DecentDB: must import first, then query
-- Step 1: Load data into the .ddb file
-- Step 2: Run the query against the local table
SELECT user_id, SUM(amount) AS total
FROM orders
WHERE region = 'us-east'
GROUP BY user_id
HAVING total > 1000;
```

### JSON traversal

```sql
-- DecentDB: recursive JSON traversal
SELECT key, value, path FROM json_tree('{"a":{"b":1},"c":[2,3]}');

-- DuckDB: no json_tree(), use repeated json_extract() for known paths
SELECT
  json_extract(config, '$.a.b') AS a_b,
  json_extract(config, '$.c[0]') AS c_0
FROM settings;
```

### INSERT with RETURNING

```sql
-- DecentDB: RETURNING is supported
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING id;

-- DuckDB: RETURNING is also supported (both have it)
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING id;
```

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| OLTP workload: point lookups, small transactions | **DecentDB** |
| Non-negotiable durability, fsync on every commit | **DecentDB** |
| Full FK support including CASCADE | **DecentDB** |
| Need triggers (audit, cascading logic) | **DecentDB** |
| Need savepoints for partial rollback | **DecentDB** |
| Single-process, multi-threaded concurrent readers | **DecentDB** |
| Recursive JSON traversal (`json_tree`) | **DecentDB** |
| Built-in trigram substring search | **DecentDB** |
| `TOTAL()` for NULL-safe sums | **DecentDB** |
| OLAP workload: aggregations, scans over large data | **DuckDB** |
| Query Parquet/CSV/JSON/S3 directly in SQL | **DuckDB** |
| Parallel query execution across cores | **DuckDB** |
| Rich analytics: quantiles, regression, skewness | **DuckDB** |
| Window frame clauses (`ROWS BETWEEN`) | **DuckDB** |
| Extensions (spatial, httpfs, Iceberg, etc.) | **DuckDB** |
| Integration with Pandas/Arrow/Polars | **DuckDB** |
| Complex types: LIST, STRUCT, MAP, ENUM | **DuckDB** |
| Data format conversion (CSV to Parquet, etc.) | **DuckDB** |

Many applications use both: DuckDB for analytics and reporting pipelines, DecentDB for the transactional application store. They are complementary, not competing.
