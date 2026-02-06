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

DecentDB’s current baseline includes:
- DDL: `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `DROP TABLE`, `DROP INDEX`, `DROP VIEW`, `ALTER VIEW ... RENAME TO ...`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- Joins: `INNER JOIN`, `LEFT JOIN`
- Clauses: `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`, `GROUP BY`
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
- Predicates: basic comparisons (`=`, `!=`, `<`, `<=`, `>`, `>=`), `AND`/`OR`, `LIKE` (plus trigram acceleration on configured columns)
- Parameters: positional `$1, $2, ...` (Postgres-style)

SQLite and DuckDB generally include all of the above, plus substantial additional SQL.

### Views

| Feature | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| Non-materialized views (`CREATE VIEW ... AS SELECT ...`) | Yes; read-only views | Yes | Yes |
| `CREATE OR REPLACE VIEW` | Yes | Not supported as a single statement | Yes |
| Updatable views | Not supported | Via `INSTEAD OF` triggers | Limited / generally not the default |
| `TEMP` views | No | Yes | Yes |

### Common SQL features not in DecentDB’s current baseline

The following are widely available in SQLite and/or DuckDB, but are not part of DecentDB’s currently documented baseline scope:

Suggested implementation priority (highest to lowest), based on practical app demand and keeping DecentDB’s core durability/read-performance goals intact:

Legend: **Must-have** = expected by most real-world application SQL (not necessarily required for DecentDB’s core storage correctness).

1. **Richer expression language and built-ins** (**Must-have**)
	- Start with the “small but essential” pieces that appear everywhere in application SQL:
	  - NULL handling: `IS [NOT] NULL`, `COALESCE`, `NULLIF`
	  - Conditionals: `CASE WHEN ... THEN ... ELSE ... END`
	  - Type conversion: `CAST(x AS type)` (and/or equivalent)
	  - Distinctness semantics: `DISTINCT` in `SELECT`, and (optionally) `IS DISTINCT FROM`
	  - Boolean predicates: `BETWEEN`, `IN (...)`, `EXISTS (...)`
	  - String basics: `||` concatenation, `length()`, `lower()`, `upper()`, `trim()`, `substr()`/`substring()`, `replace()`
	  - Numeric basics: `abs()`, `round()`, `min()`/`max()` (already as aggregates), simple math operators with correct precedence
	  - Pattern matching: `LIKE` edge cases (escaping, NULL behavior) nailed down and tested
	- Larger ecosystems (date/time/JSON/regex) tend to come later and/or behind careful scope.
2. **UPSERT and DML conveniences** (`INSERT ... ON CONFLICT ...`, `RETURNING`) (**Must-have**)
	- High leverage for application code (fewer round trips, simpler write paths), without implying a full analytics SQL surface.
3. **CTEs** (`WITH ...`) (**Must-have**)
	- Non-recursive CTEs are primarily a readability/composability feature and often unblock view-like patterns.
	- Recursive CTEs (`WITH RECURSIVE`) are typically a separate, later step.
4. **Set operations** (`UNION [ALL]`, `INTERSECT`, `EXCEPT`) (**Must-have**)
	- Useful for combining query results; tends to be straightforward conceptually but still requires careful planner/exec support.
5. **Advanced constraints** (`CHECK`, deferrable constraints, richer FK actions like cascades) (**Partially must-have**)
	- `CHECK` constraints are often considered **Must-have** for application correctness.
	- Deferrable constraints and cascading actions expand transactional semantics and need heavy test coverage.
6. **Broader `ALTER TABLE`** (migration ergonomics)
	- Important for migrations, but often touches catalog semantics and data rewrite/migration behavior.

7. **Advanced index options** (multi-column / partial / expression indexes)
	- Common in real-world schemas for performance, but not required for core correctness.
	- Partial/expression indexes also imply more complex planner rules and predicate semantics.
	- **Must-have:** No.

8. **Window functions** (`... OVER (...)`) (analytics-oriented)
	- Powerful, but primarily an analytics feature and can require non-trivial executor/planner work.
9. **Triggers** (`CREATE TRIGGER`, including `INSTEAD OF` triggers)
	- Large surface area and complexity; also the usual prerequisite for updatable views.

The following are not strictly “SQL features”, but commonly expected operational capabilities that SQLite and DuckDB provide:

10. **Introspection and settings** (PRAGMAs / catalog-like queries)
	- Examples: broader schema introspection, pragmas/settings, engine/connection configuration.
	- **Must-have:** No (useful, but not required for typical application queries).
11. **Explain and profiling tooling** (`EXPLAIN`, query profiling)
	- Useful for performance work, but not required for correctness or basic query execution.
	- **Must-have:** No.
12. **Loadable extension ecosystem** (plugins/UDFs/virtual tables/tooling)
	- SQLite: loadable extensions + virtual tables; DuckDB: install/load extensions and file-format tooling.
	- DecentDB deliberately avoids a plugin surface in the current baseline to keep durability/correctness scope tight.
	- **Must-have:** No.

Notes:
- Items marked **Must-have** above are commonly expected in practical application SQL, but DecentDB intentionally grows its SQL surface with tests-first discipline.
- If/when DecentDB adds features that affect persistence formats, WAL/checkpointing, isolation semantics, or dialect behavior, the project requires an ADR before implementation.

## Data types and functions

DecentDB’s baseline types are intentionally minimal:
- `NULL`, `INT64`, `BOOL`, `FLOAT64`, `TEXT` (UTF-8), `BLOB`

SQLite and DuckDB both offer larger built-in ecosystems of types and functions. DuckDB, in particular, has many analytics-oriented types and functions (dates/times, decimals, nested types, extensive math/statistics), while SQLite’s strength is portability, flexibility, and a long list of optional extensions.

## Indexing and search

| Capability | DecentDB | SQLite | DuckDB |
|---|---|---|---|
| B-tree secondary indexes | Yes | Yes | Yes |
| Fast substring search for `LIKE '%pattern%'` | Yes, via trigram index on configured columns | Usually via FTS extension or full scans | Often via functions/extensions; not a primary focus |
| Advanced index options (partial/expression/multi-column, etc.) | Not part of current baseline | Many are available | Many are available |

DecentDB emphasizes predictable behavior, durability, and correctness testing rather than broad operational surface area.