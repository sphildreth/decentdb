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
- CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, ALTER TABLE
- SELECT, INSERT, UPDATE, DELETE
- WHERE, ORDER BY, LIMIT, OFFSET
- INNER JOIN, LEFT JOIN
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- GROUP BY, HAVING
- Parameters: $1, $2, ...
- Data types: NULL, INT64, TEXT, BLOB, BOOL, FLOAT64
- Constraints: PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL
- Transactions: BEGIN, COMMIT, ROLLBACK
- LIKE, ILIKE pattern matching with trigram index support
- IN operator for list membership

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
- Advanced window functions (only `ROW_NUMBER()` is supported)
- Recursive CTEs (`WITH RECURSIVE` not supported)
- Views are read-only (no `INSERT`/`UPDATE`/`DELETE` targeting a view); parameters are not allowed in view definitions
- Triggers are intentionally narrow in 0.x: `AFTER` (tables) and `INSTEAD OF` (views), `FOR EACH ROW` only, and actions must be `EXECUTE FUNCTION decentdb_exec_sql('<single DML SQL>')` (no `NEW`/`OLD`)
- No stored procedures
- Statement-time foreign key enforcement only (no deferred/deferrable constraints)
- No full-text search with ranking (trigram only)
- No replication
- No built-in encryption

## Contributors
