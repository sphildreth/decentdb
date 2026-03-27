# DecentDB vs SQLite: When to Choose Which

This document helps developers decide between **DecentDB** and **SQLite** for embedded database workloads. Both are single-file, embedded relational databases with ACID transactions, but they make different design trade-offs.

> **Versions compared:** DecentDB 2.0.0 vs SQLite 3.45+ (as of 2024).
>
> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for a per-feature support grid, and [SQL Reference](sql-reference.md) for DecentDB's full SQL surface.

## At a Glance

| Dimension | DecentDB | SQLite |
|-----------|----------|--------|
| **Design priority** | Durability-first, then performance | Portability-first, then breadth |
| **Concurrency model** | One writer, many concurrent reader threads (single process) | One writer, many readers (process-safe, file-locking) |
| **Default durability** | WAL + fsync-on-commit, always | WAL or rollback journal, configurable via PRAGMA |
| **Crash safety testing** | Built-in fault-injection hooks (FaultyVFS, WAL failpoints) | Relies on external testing |
| **Extension ecosystem** | None (extend via core contribution) | Rich (loadable extensions, virtual tables, FTS5, JSON1, etc.) |
| **SQL breadth** | Deliberate Postgres-like subset | Very broad, plus extensions |
| **Bindings** | C ABI, Rust, Python, .NET, Go, Java, Node.js, Dart | Ubiquitous (every language has mature SQLite bindings) |
| **File format stability** | Stable from 2.0.0 | Decades-stable, specification published |
| **License** | MIT or Apache-2.0 | Public domain |
| **Binary size** | ~2-3 MB (Rust release build) | ~600 KB amalgamation |
| **Platform support** | Tier 1 Rust platforms | Virtually everywhere |

## When DecentDB Is the Better Fit

### 1. You need guaranteed durability without tuning

DecentDB fsyncs on every commit by default. There is no `PRAGMA synchronous` to misconfigure. If your application cannot tolerate data loss on power failure and you don't want to reason about journal modes or sync levels, DecentDB makes the safe thing the default.

```sql
-- DecentDB: every COMMIT is durable. No PRAGMA needed.
BEGIN;
INSERT INTO orders (user_id, amount) VALUES (1, 29.99);
COMMIT;  -- fsync'd to disk before returning
```

With SQLite, the equivalent safety requires explicit configuration:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;  -- or NORMAL with WAL + checkpoint risks
```

### 2. You want crash-safety verification built in

DecentDB ships with FaultyVFS and WAL failpoint hooks that let you deterministically inject I/O errors and simulate torn writes in tests. If you're building a system where you need to *prove* crash correctness, this is a first-class feature rather than something you bolt on externally.

### 3. Your workload is OLTP with concurrent readers

DecentDB is designed for one writer with many concurrent reader threads in a single process. Readers get lock-free snapshot isolation -- they never block the writer and never block each other. SQLite's concurrency model relies on file-level locking, which is process-safe but introduces contention under heavy concurrent read workloads within the same process.

```rust
// DecentDB: readers and writer share one Database handle.
// Readers get snapshots; the writer holds the commit lock.
let db = Database::open("app.ddb")?;
// Multiple reader threads can execute concurrently
// without any locking protocol on the application side.
```

### 4. You need `INSERT ... RETURNING` or `UPDATE ... RETURNING`

Both DecentDB and SQLite 3.35.0+ support `RETURNING` on INSERT, UPDATE, and DELETE. This is common in API backends that need the generated ID or computed values back immediately.

```sql
-- Both DecentDB and SQLite 3.35.0+
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING id, name;

UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING id, name, email;

DELETE FROM users WHERE id = 1 RETURNING id;
```

If you need to support older SQLite versions (pre-3.35.0), you would need a workaround:

```sql
-- SQLite pre-3.35.0 workaround (two statements)
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
SELECT last_insert_rowid();  -- only gives the id, not the full row
```

### 5. You need `TRUNCATE TABLE`

DecentDB supports `TRUNCATE TABLE` for fast bulk deletion. Unlike `DELETE FROM`, `TRUNCATE`:
- Does not scan rows individually
- Does not fire `DELETE` triggers
- Resets auto-increment counters
- Is minimally logged (faster on large tables)

SQLite requires `DELETE FROM table;` which scans row-by-row and fires triggers.

```sql
-- DecentDB
TRUNCATE TABLE temp_events;
```

### 6. You want `DISTINCT ON` (Postgres-style)

DecentDB supports `DISTINCT ON` for "first row per group" queries without a window function + CTE workaround.

```sql
-- DecentDB: get the most recent order per user
SELECT DISTINCT ON (user_id) user_id, id, created_at, amount
FROM orders
ORDER BY user_id, created_at DESC;
```

SQLite equivalent (verbose):

```sql
WITH ranked AS (
  SELECT user_id, id, created_at, amount,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at DESC) AS rn
  FROM orders
)
SELECT user_id, id, created_at, amount FROM ranked WHERE rn = 1;
```

### 7. You need built-in substring search (trigram indexes)

DecentDB offers a native trigram index type designed for fast `LIKE '%pattern%'` queries. SQLite requires FTS5 or a full table scan.

```sql
-- DecentDB: create a trigram index for substring search
CREATE INDEX idx_users_name_trgm ON users USING trigram(name);

-- This query uses the trigram index instead of a full scan
SELECT * FROM users WHERE name LIKE '%alice%';
```

Without a trigram index, `LIKE '%pattern%'` requires a full table scan in both databases. SQLite's FTS5 can help with full-text search but is not optimized for arbitrary substring matching.

### 8. You want a richer set of aggregate and window functions out of the box

DecentDB includes several analytical functions that SQLite lacks without extensions:

```sql
-- These all work in DecentDB, none work in stock SQLite:
SELECT STDDEV(amount), VARIANCE(amount) FROM orders;
SELECT BOOL_AND(active), BOOL_OR(admin) FROM users;
SELECT MEDIAN(salary) FROM employees;
SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY response_time) FROM requests;
SELECT DATE_DIFF('day', start_date, end_date) FROM projects;
SELECT LAST_DAY('2024-02-15');  -- '2024-02-29'
```

### 9. You want native UUID support

DecentDB has a first-class `UUID` type. SQLite stores UUIDs as text or blobs and lacks built-in UUID generation.

```sql
-- DecentDB
CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT GEN_RANDOM_UUID(),
    user_id INT NOT NULL
);
INSERT INTO sessions (user_id) VALUES (1) RETURNING id;
-- Returns a proper UUID, not a string
```

### 10. You prefer Postgres-style SQL syntax

DecentDB targets Postgres compatibility for its SQL surface. If your application also talks to Postgres, DecentDB reduces the dialect gap. Features like `$1` positional parameters, `ILIKE`, `RETURNING`, `DISTINCT ON`, and `STRING_AGG` all follow Postgres conventions.

```sql
-- DecentDB: Postgres-style positional parameters
SELECT * FROM users WHERE email = $1 AND active = $2;

-- Postgres-style case-insensitive LIKE
SELECT * FROM users WHERE name ILIKE '%alice%';

-- Postgres-style string aggregation
SELECT department, STRING_AGG(name, ', ' ORDER BY name) FROM employees GROUP BY department;
```

## When SQLite Is the Better Fit

### 1. You need a battle-tested, decades-stable file format

SQLite is one of the most tested pieces of software ever written. Its file format has been stable for over 20 years. If you need to write a `.sqlite` file today and read it in 2040, SQLite is the safe bet. DecentDB is pre-1.0 with an evolving on-disk format.

### 2. You need broad SQL coverage or extensions

SQLite's SQL surface is far wider than DecentDB's, and its extension ecosystem (FTS5, R-Tree, JSON1, virtual tables, custom functions via C) is unmatched in the embedded space.

```sql
-- SQLite: FTS5 full-text search (no equivalent in DecentDB)
CREATE VIRTUAL TABLE docs USING fts5(title, body);
INSERT INTO docs (title, body) VALUES ('Guide', 'How to use the database');
SELECT * FROM docs WHERE docs MATCH 'database';

-- SQLite: virtual tables, custom collations, loadable extensions
.load ./my_extension
```

DecentDB has no extension/plugin mechanism. If you need a SQL feature DecentDB doesn't have, you contribute to the core or work around it in application code.

### 3. You need cross-process access

SQLite uses file-level locking to allow multiple processes to safely share one database file. DecentDB is single-process. If you have multiple processes (e.g., separate worker processes, CLI tools, and a web server) that need to read/write the same database, SQLite is the right choice.

```
# SQLite: multiple processes can share the file
sqlite3 app.db "INSERT INTO jobs VALUES (1, 'pending')"   # process A
sqlite3 app.db "SELECT * FROM jobs"                        # process B
```

### 4. You need an ecosystem of mature language bindings

SQLite has the most ubiquitous bindings of any database in history. Every language -- from C to Zig -- has multiple well-maintained SQLite libraries. DecentDB's bindings are growing (Rust, Python, .NET, Go, Java, Node.js, Dart) but SQLite's ecosystem is decades ahead in maturity and coverage.

### 5. You need `ATTACH DATABASE`

SQLite can query multiple database files in a single statement via `ATTACH DATABASE`. DecentDB does not support this. If your application needs to join across separate `.db` files, SQLite handles this natively.

```sql
-- SQLite: query across two database files
ATTACH DATABASE 'analytics.db' AS analytics;
SELECT u.name, a.revenue
FROM main.users u JOIN analytics.revenue a ON u.id = a.user_id;
```

### 6. You need `WITHOUT ROWID` tables or `rowid` access

SQLite's `WITHOUT ROWID` tables and implicit `rowid` pseudo-column are useful for certain schema designs. DecentDB does not expose `rowid` to SQL and has no `WITHOUT ROWID` concept.

```sql
-- SQLite
SELECT rowid, * FROM users WHERE rowid BETWEEN 100 AND 200;

CREATE TABLE lookup (key TEXT PRIMARY KEY, val TEXT) WITHOUT ROWID;
```

DecentDB alternative: use an explicit `INT PRIMARY KEY` (which auto-assigns on INSERT).

### 7. You need window frame clauses (`ROWS BETWEEN ...`)

SQLite supports `ROWS BETWEEN` and `RANGE BETWEEN` frame specifications in window functions. DecentDB does not currently support frame clauses.

```sql
-- SQLite: rolling 3-row average
SELECT date, amount,
       AVG(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg
FROM sales;

-- DecentDB: frame clauses are not supported
-- You would need to compute this in application code
```

### 8. You need a small, portable, zero-dependency library

SQLite compiles to a single C file (`sqlite3.c`) with no external dependencies. It runs on virtually every platform ever made -- from embedded microcontrollers to mainframes. DecentDB is a Rust crate with a more conventional build system and a larger binary footprint.

### 9. You need `PRAGMA` configuration

SQLite offers dozens of PRAGMAs for tuning cache size, page size, journal mode, locking mode, foreign key enforcement, and more. DecentDB exposes a small subset (`page_size`, `cache_size`, `integrity_check`, `database_list`, `table_info`) and intentionally limits configurability to reduce misconfiguration risk.

```sql
-- SQLite: extensive runtime tuning
PRAGMA cache_size = -64000;    -- 64 MB cache
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
```

If your application depends on these tuning knobs, SQLite is the right tool.

### 10. You need `INTERSECT ALL` or `EXCEPT ALL` (multiset operations)

DecentDB does not support `INTERSECT ALL` or `EXCEPT ALL` (as noted in the [SQL Reference](sql-reference.md#set-operations)). SQLite does.

```sql
-- SQLite: multiset difference (preserves duplicate counts)
SELECT item_id FROM inventory EXCEPT ALL SELECT item_id FROM sold;

-- DecentDB: must rewrite as a grouped query
```

## Side-by-Side SQL Examples

### UPSERT (both supported, different syntax feel)

```sql
-- DecentDB (Postgres-style)
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@new.com')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, email = EXCLUDED.email;

-- SQLite (same feature, compatible syntax)
INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@new.com')
ON CONFLICT (id) DO UPDATE SET name = excluded.name, email = excluded.email;
```

### String aggregation

```sql
-- DecentDB: both styles
SELECT STRING_AGG(name, ', ' ORDER BY name) FROM employees;
SELECT GROUP_CONCAT(name, ', ') FROM employees;

-- SQLite: GROUP_CONCAT only
SELECT GROUP_CONCAT(name, ', ') FROM employees;
```

### Date/time arithmetic

```sql
-- DecentDB: rich date/time functions
SELECT DATE_TRUNC('month', '2024-03-15 14:30:45');     -- '2024-03-01 00:00:00'
SELECT DATE_DIFF('day', '2024-03-10', '2024-03-15');    -- 5
SELECT LAST_DAY('2024-02-11');                           -- '2024-02-29'
SELECT '2024-03-15'::timestamp + INTERVAL '1 month';     -- '2024-04-15'

-- SQLite: limited date/time, no INTERVAL arithmetic
SELECT date('2024-03-15', '+1 month');                   -- '2024-04-15'
SELECT strftime('%Y-%m', '2024-03-15');                  -- '2024-03'
```

### Statistical aggregates

```sql
-- DecentDB: built-in statistics
SELECT STDDEV(salary), VARIANCE(salary), MEDIAN(salary),
       PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY salary)
FROM employees;

-- SQLite: not available without extensions
-- Requires custom C extension or application-level computation
```

## Summary Decision Matrix

| Your situation | Recommendation |
|----------------|----------------|
| Durability is non-negotiable, no PRAGMA tuning wanted | **DecentDB** |
| Single-process, multi-threaded reader workload | **DecentDB** |
| Need `TRUNCATE`, `DISTINCT ON` | **DecentDB** |
| Need built-in trigram substring search | **DecentDB** |
| Need rich statistical/window aggregates out of the box | **DecentDB** |
| Need Postgres-like SQL to reduce dialect drift | **DecentDB** |
| Need crash-injection testing hooks | **DecentDB** |
| Decades-stable file format, maximum compatibility | **SQLite** |
| Need loadable extensions, FTS5, R-Tree, virtual tables | **SQLite** |
| Multiple processes sharing one database file | **SQLite** |
| Embedded on exotic platforms (microcontrollers, etc.) | **SQLite** |
| Need extensive `PRAGMA` runtime tuning | **SQLite** |
| Need window frame clauses (`ROWS BETWEEN`) | **SQLite** |
| Need `ATTACH DATABASE` across files | **SQLite** |
| Largest possible language binding ecosystem | **SQLite** |
| Absolute smallest binary with zero dependencies | **SQLite** |

Both databases are solid choices for embedded workloads. Pick the one whose defaults and constraints align with your application's requirements.
