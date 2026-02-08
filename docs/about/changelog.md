# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-07

### Added
- **Data Types**:
  - `UUID` type with `GEN_RANDOM_UUID()`, `UUID_PARSE()`, and `UUID_TO_STRING()` functions.
  - `DECIMAL(p,s)` / `NUMERIC(p,s)` type for exact fixed-point arithmetic.
- **SQL Features**:
  - `CAST` support for `UUID` and `DECIMAL`.
  - Common Table Expressions (CTEs) - Non-recursive `WITH` clauses.
  - Window Functions - `ROW_NUMBER()`.
  - Non-materialized, read-only `VIEW`s (`CREATE VIEW`, `CREATE OR REPLACE VIEW`, `DROP VIEW`, `ALTER VIEW ... RENAME TO ...`).
  - Triggers (`CREATE TRIGGER`, `DROP TRIGGER`): `AFTER` row triggers on tables and `INSTEAD OF` row triggers on views, with a constrained action surface.
  - `INSERT ... RETURNING` for retrieving auto-assigned values.
  - `INSERT ... ON CONFLICT DO NOTHING` and `INSERT ... ON CONFLICT DO UPDATE` (upsert).
  - `INTEGER PRIMARY KEY` auto-increment: columns auto-assign sequential IDs when omitted from INSERT.
  - `<>` operator (alias for `!=`).
  - `EXPLAIN ANALYZE` for query plan output with actual execution metrics (row counts, timing).
- **C API**:
  - `decentdb_checkpoint()` for WAL-to-database synchronization.
  - `decentdb_free()` for API-allocated memory.
  - `decentdb_list_tables_json()`, `decentdb_get_table_columns_json()`, `decentdb_list_indexes_json()` for schema introspection.
  - INSERT RETURNING support in prepare/step path.
- **Language Bindings**:
  - **.NET**: Full ADO.NET provider (DbConnection, DbCommand, DbDataReader), MicroOrm (DbSet, DecentDBContext), ConnectionStringBuilder, DbProviderFactory, GetSchema(), UpsertAsync, SelectAsync projection, raw SQL methods.
  - **Go**: `database/sql` driver, `OpenDirect` API with Checkpoint, ListTables, GetTableColumns, ListIndexes, Decimal type.
  - **Python**: DB-API 2.0, SQLAlchemy dialect, checkpoint, list_indexes, import tools.
  - **Node.js**: N-API addon with Database/Statement classes, async iteration, checkpoint, schema introspection, Knex integration.

## [0.0.1] - 2026-01-30

### Added
- Initial stable release of DecentDB
- ACID transactions with WAL-based durability
- PostgreSQL-like SQL subset
- B+Tree storage engine with page cache
- Trigram inverted index for fast text search
- Single writer + multiple readers concurrency model
- Snapshot isolation for consistent reads
- Foreign key constraints with automatic indexing
- Bulk load API for high-performance data import
- Comprehensive CLI with SQL execution and maintenance commands
- Complete test suite (unit, property, crash-injection, differential)
- Performance benchmarks (7 benchmarks covering all key operations)
- Nim API for embedded applications
- Cross-platform support (Linux, macOS, Windows)
- Full documentation site with MkDocs

### SQL Support
- CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, ALTER TABLE (ADD/DROP/RENAME COLUMN, ALTER COLUMN TYPE)
- SELECT (with DISTINCT), INSERT, UPDATE, DELETE
- WHERE, ORDER BY (ASC/DESC), LIMIT, OFFSET
- INNER JOIN, LEFT JOIN
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- GROUP BY, HAVING
- SET operations: UNION, UNION ALL, INTERSECT, EXCEPT
- BETWEEN, EXISTS, IS NULL, IS NOT NULL
- COALESCE, NULLIF, CASE WHEN, CAST
- LOWER, UPPER, TRIM, LENGTH
- Parameters: $1, $2, ... (Postgres-style)
- Data types: NULL, INT64, TEXT, BLOB, BOOL, FLOAT64
- Constraints: PRIMARY KEY, FOREIGN KEY (CASCADE/SET NULL/RESTRICT), UNIQUE, NOT NULL, CHECK, DEFAULT
- Transactions: BEGIN, COMMIT, ROLLBACK
- LIKE, ILIKE pattern matching with ESCAPE clause and trigram index support
- IN operator for list membership
- String concatenation: ||
- Arithmetic operators: +, -, *, /
- EXPLAIN query plans

### Performance
- Point lookups: P95 < 10ms
- FK joins: P95 < 100ms  
- Text search: P95 < 200ms
- Bulk load: 100k records < 20 seconds
- Crash recovery: < 5 seconds for 100MB database

### Architecture
- Modular design with clean separation of concerns
- Write-Ahead Logging (WAL) for durability
- B+Tree with overflow page support for large values
- Page cache with LRU eviction
- External merge sort for large ORDER BY operations
- Comprehensive error handling with specific error codes
- Memory-safe Nim implementation
- Extensive test coverage (>90% core modules)

### Documentation
- Complete user guide with SQL reference
- Nim API documentation
- CLI reference
- Architecture documentation
- Design documents (PRD, SPEC, ADRs)
- MkDocs-based documentation site at https://decentdb.org

## Known Limitations

- Single writer only (no concurrent write transactions)
- Single process access (no multi-process concurrency)
- Subqueries are limited: only `EXISTS (SELECT ...)` is supported (no scalar subqueries, including in SELECT lists)
- Window functions limited to `ROW_NUMBER()` (no RANK, DENSE_RANK, LAG, LEAD, etc.)
- Only non-recursive CTEs supported (no `WITH RECURSIVE`)
- Views are read-only (no `INSERT`/`UPDATE`/`DELETE` targeting a view); parameters are not allowed in view definitions
- Triggers are intentionally narrow: `AFTER` (tables) and `INSTEAD OF` (views), `FOR EACH ROW` only, and actions must be `EXECUTE FUNCTION decentdb_exec_sql('<single DML SQL>')` (no `NEW`/`OLD`)
- No stored procedures
- Statement-time foreign key enforcement only (no deferred/deferrable constraints)
- No full-text search with ranking (trigram substring matching only)
- No replication
- No built-in encryption
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` are not supported (only `INSERT ... RETURNING`)
- `ADD CONSTRAINT` (post-creation) is not supported
- Targetless `ON CONFLICT DO UPDATE` is not supported

## Contributors
