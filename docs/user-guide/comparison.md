# Comparison: DecentDB vs SQLite vs DuckDB

This page summarizes high-level feature differences between **DecentDB**, **SQLite**, and **DuckDB**.

## Versions (as of 2026-02-06)

This comparison was written against:
- SQLite `3.51.2` (sqlite3 CLI)
- DuckDB `v1.4.3` (duckdb CLI)

DecentDB is currently **pre-1.0**. This document describes the **current baseline** feature set and constraints; details may change as DecentDB approaches 1.0.

DecentDB is intentionally scoped around:
- **Priority #1:** durable ACID writes (WAL-based)
- **Priority #2:** fast reads
- **Concurrency model:** single process, **one writer**, many concurrent reader threads
- **SQL goal:** a practical, Postgres-like subset for common application queries

SQLite and DuckDB are used as behavioral baselines for many SQL features, but DecentDB does **not** aim to be a drop-in replacement for either.

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
- DDL: `CREATE TABLE`, `CREATE INDEX`, `CREATE TRIGGER`, `CREATE VIEW`, `DROP TABLE`, `DROP INDEX`, `DROP TRIGGER`, `DROP VIEW`, `ALTER TABLE`, `ALTER VIEW ... RENAME TO ...`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `INSERT ... RETURNING`, `INSERT ... ON CONFLICT`
- Joins: `INNER JOIN`, `LEFT JOIN`
- Clauses: `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`, `HAVING`, `DISTINCT`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- Set operations: `UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`
- CTEs: non-recursive `WITH ... AS`
- Window functions: `ROW_NUMBER() OVER (...)`
- Predicates: comparisons (`=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`), `AND`/`OR`/`NOT`, `LIKE`/`ILIKE`, `IN`, `BETWEEN`, `EXISTS`, `IS NULL`/`IS NOT NULL`
- Scalar functions: `COALESCE`, `NULLIF`, `CAST`, `CASE`, `LENGTH`, `LOWER`, `UPPER`, `TRIM`, `GEN_RANDOM_UUID`, `UUID_PARSE`, `UUID_TO_STRING`
- Operators: `+`, `-`, `*`, `/`, `||` (string concatenation)
- Parameters: positional `$1, $2, ...` (Postgres-style)
- `EXPLAIN` / `EXPLAIN ANALYZE` plan output

SQLite and DuckDB generally include all of the above, plus substantial additional SQL.

### Views

| Feature | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| Non-materialized views (`CREATE VIEW ... AS SELECT ...`) | Yes; read-only views | Yes | Yes |
| `CREATE OR REPLACE VIEW` | Yes | Not supported as a single statement | Yes |
| Updatable views | Limited: via narrow `INSTEAD OF` trigger subset (`decentdb_exec_sql('<single DML>')`, no `NEW`/`OLD`) | Via `INSTEAD OF` triggers | Limited / generally not the default |
| `TEMP` views | No | Yes | Yes |

### SQL roadmap

DecentDB has implemented many previously planned baseline features, including:
- Richer expression support (`IS NULL`, `CASE`, `CAST`, `BETWEEN`, `IN`, `EXISTS`, `LIKE ... ESCAPE`, `||`, core scalar functions)
- UPSERT and DML conveniences (`INSERT ... ON CONFLICT DO NOTHING/DO UPDATE`, `INSERT ... RETURNING`)
- Non-recursive CTEs, set operations (`UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`)
- `CHECK` constraints and FK `CASCADE` / `SET NULL` actions
- Broader `ALTER TABLE` (`ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN`, `ALTER COLUMN TYPE`)
- Trigger subsets (`AFTER` row triggers on tables, `INSTEAD OF` row triggers on views)
- Window subset (`ROW_NUMBER() OVER (...)`)
- Indexing options (multi-column, partial v0 subset, expression index v0 subset)
- `EXPLAIN` / `EXPLAIN ANALYZE` plan output

For remaining roadmap and deferred capabilities, use:
- [DecentDB SQL Enhancements Plan](../../design/SQL_ENHANCEMENTS_PLAN.md)

## Data types and functions

DecentDB’s baseline types are intentionally small:
- `NULL`, `INT64`, `BOOL`, `FLOAT64`, `TEXT` (UTF-8), `BLOB`, `UUID`, `DECIMAL(p,s)` / `NUMERIC(p,s)`

SQLite and DuckDB both offer larger built-in ecosystems of types and functions. DuckDB, in particular, has many analytics-oriented types and functions (dates/times, decimals, nested types, extensive math/statistics), while SQLite’s strength is portability, flexibility, and a long list of optional extensions.

## Indexing and search

| Capability | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| B-tree secondary indexes | Yes | Yes | Yes |
| Fast substring search for `LIKE '%pattern%'` | Yes, via trigram index on configured columns | Usually via FTS extension or full scans | Often via functions/extensions; not a primary focus |
| Advanced index options (partial/expression/multi-column, etc.) | Supported with v0 limits (multi-column BTREE; partial `col IS NOT NULL`; narrow deterministic single-expression BTREE) | Many are available | Many are available |

DecentDB emphasizes predictable behavior, durability, and correctness testing rather than broad operational surface area.
