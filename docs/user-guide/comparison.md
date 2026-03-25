# Comparison: DecentDB vs SQLite vs DuckDB

This page summarizes high-level feature differences between **DecentDB**, **SQLite**, and **DuckDB**.

## Versions (as of 2026-02-24)

This comparison was written against:
- SQLite `3.51.2` (sqlite3 CLI)
- DuckDB `v1.4.3` (duckdb CLI)

DecentDB is currently at **v1.8.1**. This document describes the current feature set and constraints; details may change as DecentDB continues to evolve.

DecentDB is intentionally scoped around:
- **Priority #1:** durable ACID writes (WAL-based)
- **Priority #2:** fast reads
- **Concurrency model:** single process, **one writer**, many concurrent reader threads
- **SQL goal:** a practical, Postgres-like subset for common application queries

SQLite and DuckDB are used as behavioral baselines for many SQL features, but DecentDB does **not** aim to be a drop-in replacement for either.

> **See also:** [SQL Feature Matrix](sql-feature-matrix.md) for a concise per-feature support grid comparing DecentDB, SQLite, and DuckDB.

## Quick summary

| Area | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| Primary focus | OLTP-style embedded DB, durability-first | Embedded general-purpose DB | Embedded analytics (OLAP) |
| Durability model | WAL-based, fsync-on-commit by default | WAL or rollback journal, configurable | Depends on storage mode; optimized for analytics workflows |
| Concurrency | Single writer, many readers (threads, same process) | Multi-reader, single-writer (process-safe) | Parallel query execution; analytics-oriented |
| SQL breadth | Subset (deliberately small) | Very broad (plus extensions) | Very broad (esp. analytical SQL) |
| Extensibility | No loadable extension / UDF plugin surface in the current baseline (extend by contributing to core) | Rich extension ecosystem (loadable extensions, virtual tables, UDFs) | Rich extension ecosystem (install/load extensions, UDFs) |
| Substring search (`LIKE '%pattern%'`) | Built-in trigram index option (purpose-built for interactive “contains” queries) | Typically full scan or use FTS/extensions | Typically scan or use extensions (e.g., FTS) |
| Durability fault-injection hooks | Built-in WAL failpoints + FaultyVFS for deterministic crash/torn-write testing | Not typically exposed as a first-class user feature | Not typically exposed as a first-class user feature |

Notes:
- SQLite and DuckDB can often match many of these behaviors via extensions or different usage patterns; the point here is what DecentDB bakes in and optimizes for by default.

What “extensions” means here:
- The ability to add new SQL features without modifying the database core (e.g., new scalar/aggregate functions, new table-like modules such as SQLite virtual tables, or optional subsystems like full-text search).
- DecentDB does support multiple language bindings, but those bindings are about how you *call* DecentDB, not a general-purpose SQL extension/plugin system.

## SQL surface area

DecentDB's current baseline includes:
- DDL: `CREATE TABLE`, `CREATE INDEX`, `CREATE TRIGGER`, `CREATE VIEW`, `CREATE TEMP TABLE`, `CREATE TEMP VIEW`, `DROP TABLE`, `DROP INDEX`, `DROP TRIGGER`, `DROP VIEW`, `ALTER TABLE`, `ALTER VIEW ... RENAME TO ...`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `INSERT ... RETURNING`, `INSERT ... ON CONFLICT`
- Joins: `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, `CROSS JOIN`, `NATURAL JOIN`
- Clauses: `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `FETCH`, `GROUP BY`, `HAVING`, `DISTINCT`, `DISTINCT ON`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `TOTAL`, `GROUP_CONCAT`, `STRING_AGG` (with `DISTINCT` modifier)
- Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL`
- CTEs: `WITH ... AS` (recursive and non-recursive)
- Window functions: `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE()` with `OVER (...)`
- Predicates: comparisons (`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`), `AND`/`OR`/`NOT`, `LIKE`/`ILIKE`, `IN`, `BETWEEN`, `EXISTS`, `IS NULL`/`IS NOT NULL`
- Math functions: `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `SQRT`, `POWER`/`POW`, `MOD`, `SIGN`, `LOG`, `LN`, `EXP`, `RANDOM`
- String functions: `LENGTH`, `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`, `REPLACE`, `SUBSTRING`/`SUBSTR`, `INSTR`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPEAT`, `REVERSE`, `CHR`, `HEX`
- Date/time functions: `NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `date()`, `datetime()`, `strftime()`, `EXTRACT()`
- JSON functions: `JSON_EXTRACT`, `JSON_ARRAY_LENGTH`, `json_type`, `json_valid`, `json_object`, `json_array`, `->`, `->>`
- Table-valued functions: `json_each()`, `json_tree()`
- Generated columns: `GENERATED ALWAYS AS (expr) STORED`
- Other functions: `COALESCE`, `NULLIF`, `CAST`, `CASE`, `GEN_RANDOM_UUID`, `UUID_PARSE`, `UUID_TO_STRING`, `PRINTF`
- Operators: `+`, `-`, `*`, `/`, `%` (modulo), `||` (string concatenation)
- Transaction control: `BEGIN`, `BEGIN IMMEDIATE`/`BEGIN EXCLUSIVE` (treated as `BEGIN`), `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT`
- Parameters: positional `$1, $2, ...` (Postgres-style)
- `EXPLAIN` / `EXPLAIN ANALYZE` plan output

SQLite and DuckDB generally include all of the above, plus substantial additional SQL.

### Views

| Feature | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| Non-materialized views (`CREATE VIEW ... AS SELECT ...`) | Yes; read-only views | Yes | Yes |
| `CREATE OR REPLACE VIEW` | Yes | Not supported as a single statement | Yes |
| Updatable views | Limited: via narrow `INSTEAD OF` trigger subset (`decentdb_exec_sql('<single DML>')`, no `NEW`/`OLD`) | Via `INSTEAD OF` triggers | Limited / generally not the default |
| `TEMP` views | Yes (session-scoped, not persisted) | Yes | Yes |

### SQL roadmap

DecentDB has implemented many previously planned baseline features, including:
- Richer expression support (`IS NULL`, `CASE`, `CAST`, `BETWEEN`, `IN`, `EXISTS`, `LIKE ... ESCAPE`, `||`, core scalar functions)
- UPSERT and DML conveniences (`INSERT ... ON CONFLICT DO NOTHING/DO UPDATE`, `INSERT ... RETURNING`)
- Recursive and non-recursive CTEs, set operations (`UNION ALL`, `UNION`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL`)
- `CHECK` constraints, FK `CASCADE` / `SET NULL` actions, table-level FOREIGN KEY constraints
- Broader `ALTER TABLE` (`ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`)
- Trigger subsets (`AFTER` row triggers on tables, `INSTEAD OF` row triggers on views)
- Window functions (`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `LAG()`, `LEAD()`, `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE()` with `OVER (...)`)
- Date/time functions (`NOW()`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `date()`, `datetime()`, `strftime()`, `EXTRACT()`)
- Math functions (`SQRT`, `POWER`/`POW`, `MOD`, `SIGN`, `LOG`, `LN`, `EXP`, `RANDOM`)
- String functions (`LTRIM`, `RTRIM`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPEAT`, `REVERSE`, `CHR`, `HEX`, `INSTR`)
- JSON functions (`JSON_EXTRACT`, `JSON_ARRAY_LENGTH`, `json_type`, `json_valid`, `json_object`, `json_array`, `->`, `->>`, `json_each()`, `json_tree()`)
- Indexing options (multi-column, partial v0 subset, expression index v0 subset)
- `EXPLAIN` / `EXPLAIN ANALYZE` plan output
- Join types (`INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`, `NATURAL`)
- `DISTINCT ON`, `DISTINCT` aggregates (`COUNT(DISTINCT ...)`, `SUM(DISTINCT ...)`, `AVG(DISTINCT ...)`)
- `DEFAULT` and generated columns (STORED)
- `CREATE TEMP TABLE` / `CREATE TEMP VIEW` (session-scoped)
- `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT`

For remaining roadmap and deferred capabilities, see:
- [Road to RTM](../design/road-to-rtm.md)
- [ADRs](../design/adr.md)

## Data types and functions

DecentDB’s baseline types cover the most common application needs:
- `NULL`, `INT64`, `BOOL`, `FLOAT64`, `TEXT` (UTF-8), `BLOB`, `UUID`, `DECIMAL(p,s)` / `NUMERIC(p,s)`, `DATE`, `TIMESTAMP`

SQLite and DuckDB both offer larger built-in ecosystems of types and functions. DuckDB, in particular, has many analytics-oriented types and functions (nested types, extensive math/statistics), while SQLite’s strength is portability, flexibility, and a long list of optional extensions.

## Indexing and search

| Capability | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| B-tree secondary indexes | Yes | Yes | Yes |
| Fast substring search for `LIKE '%pattern%'` | Yes, via trigram index on configured columns | Usually via FTS extension or full scans | Often via functions/extensions; not a primary focus |
| Advanced index options (partial/expression/multi-column, etc.) | Supported with v0 limits (multi-column BTREE; partial `col IS NOT NULL`; narrow deterministic single-expression BTREE) | Many are available | Many are available |

DecentDB emphasizes predictable behavior, durability, and correctness testing rather than broad operational surface area.

## SQLite-Specific Features: Explicit Decisions

DecentDB intentionally does not support certain SQLite-specific features. This section documents those decisions and provides alternatives where applicable.

### PRAGMA

SQLite's runtime configuration mechanism (hundreds of directives) is not supported.

| Common PRAGMA | DecentDB Alternative |
|---|---|
| `PRAGMA journal_mode` | Not applicable; DecentDB uses WAL-only mode |
| `PRAGMA foreign_keys` | Always enabled; cannot be disabled |
| `PRAGMA table_info(t)` | Use `decentdb describe --db=... --table=t` |
| `PRAGMA synchronous` | Not configurable; commits are durable by default (fsync-on-commit). Use `bulk-load --durability=...` for ingestion tradeoffs |
| `PRAGMA cache_size` | Configure cache at open: CLI `--cachePages/--cacheMb`, Rust `openDb(..., cachePages=...)` |

### rowid / _rowid_ Pseudo-Columns

SQLite exposes implicit rowid as a queryable pseudo-column. DecentDB has an internal rowid but does not expose it to SQL.

**Recommendation:** Use an explicit INT64 primary key (e.g. `id INT PRIMARY KEY`). If the table has a single INT64 primary key column, omitting the value on INSERT will auto-assign an ID.

### WITHOUT ROWID Tables

SQLite optimization for tables where the PRIMARY KEY is the clustering key. Not applicable to DecentDB's storage architecture.

### ATTACH DATABASE

SQLite's mechanism for querying multiple database files simultaneously. Not supported.

**Recommendation:** Use application-level multi-database coordination.

### Recursive CTEs

`WITH RECURSIVE` is supported for iterative fixpoint queries (counting, tree/graph traversal). Recursive CTEs use `UNION` or `UNION ALL` between anchor and recursive terms and are limited to 1000 iterations per statement.
