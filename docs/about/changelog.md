# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - 2026-02-21

### Added
- **SQL Engine**: Hex blob literal (`X'DEADBEEF'`) syntax support — new `svBlob` value kind parsed from libpg_query `bsval` nodes, with full support across parser, binder, executor, EXPLAIN output, and storage predicate evaluator.

### Fixed
- **SQL Engine**: Self-referencing foreign keys — `CREATE TABLE` with a foreign key referencing the same table (e.g., `parent_id REFERENCES self(id)`) now validates against the columns being defined instead of failing with "Table not found".
- **SQL Engine**: UUID ↔ Blob type coercion — blob literals (e.g., `X'...'` with 16 bytes) are now accepted for UUID columns in INSERT/UPDATE, and vice versa. Previously the binder rejected these with "Type mismatch" despite the runtime already supporting 16-byte blobs as UUIDs.
- .NET: `DecentDB.EntityFrameworkCore.NodaTime` DECIMAL type mapping now respects precision and scale from EF Core model configuration (e.g., `HasPrecision(18, 6)`). Previously ignored model-specified precision/scale and always emitted `DECIMAL(18,4)`.

## [1.2.0] - 2026-02-21

### Added
- **SQL Engine**: `GROUP_CONCAT` and `STRING_AGG` aggregate functions.
- **SQL Engine**: `INSERT INTO ... SELECT` statement support across all execution paths (prepared, non-prepared, non-select API).
- **SQL Engine**: `IN (subquery)` predicate support (e.g. `WHERE id IN (SELECT id FROM other_table)`).
- **SQL Engine**: `printf()` scalar function.
- **SQL Engine**: General filtered/partial index support — predicates beyond `IS NOT NULL` are now allowed, including `UNIQUE` filtered indexes and complex `WHERE` clauses.
- .NET: `SqliteCompatibilityTests` — 18 SQL-level compatibility tests covering patterns used by Melodee (GROUP_CONCAT, INSERT INTO...SELECT, IN subquery, printf, filtered indexes, etc.).
- .NET: `EfFunctionsLikeTests` — EF Core `EF.Functions.Like()` integration test.

### Fixed
- **SQL Engine**: SUM now preserves input type — returns `INT64` when all accumulated values are integers, `FLOAT64` when any float is present. Previously always returned `FLOAT64`.
- **SQL Engine**: SUM/AVG/MIN/MAX on empty result sets now return `NULL` per SQL standard. Previously returned `0`/`0.0`/default values. COUNT on empty sets correctly returns `0`.
- **SQL Engine**: AND/OR operators now implement SQL three-valued logic — `FALSE AND NULL` correctly evaluates to `FALSE`, and `TRUE OR NULL` correctly evaluates to `TRUE`. Previously any `NULL` operand propagated `NULL` regardless.
- **SQL Engine**: `IS`/`IS NOT` operators now support `IS TRUE`, `IS FALSE`, `IS NOT TRUE`, and `IS NOT FALSE` in addition to `IS NULL`/`IS NOT NULL`.
- **SQL Engine**: `INSERT INTO ... SELECT` now correctly fires AFTER INSERT triggers. Previously triggers were only executed for regular INSERT statements.
- **SQL Engine**: Float-to-DECIMAL coercion in INSERT/UPDATE — float literals (e.g. `3.14`) are now correctly coerced to DECIMAL type when the target column has DECIMAL precision/scale.
- **SQL Engine**: Filtered index predicate evaluator rewritten to correctly evaluate rows against arbitrary `WHERE` clauses, fixing false negatives in index maintenance.
- **SQL Engine**: Filtered index predicate cache now defensively initialized per thread, consistent with other threadvar patterns.
- **SQL Engine**: `GROUP_CONCAT`/`STRING_AGG` now correctly recognized by the query planner as aggregate functions, preventing "evaluated elsewhere" errors when used without `GROUP BY`.
- .NET: EF Core `DecentDBTypeMappingSource` now respects precision and scale from model configuration for DECIMAL columns, instead of always using `DECIMAL(18,4)`.

## [1.1.3] - 2026-02-18

### Fixed
- .NET: NuGet packages now statically link `libpg_query`, `xxhash`, and `protobuf-c` into `libdecentdb.so`. Previously these were dynamic dependencies not included in the package, causing `DllNotFoundException` on systems without them installed (e.g. `dotnet publish` to a clean server).

### Changed
- Build: Linux and macOS CI steps now use the `libpg_query.a` static archive instead of `sudo make install`, matching the Windows build and ensuring the shared library is self-contained.

## [1.1.2] - 2026-02-15

### Fixed
- **SQL Engine**: `FALSE` literal now parsed correctly. libpg_query's protobuf encoding omits the `boolval` field for `false` (protobuf default elision), so `WHERE col = FALSE` and `UPDATE ... SET flag = FALSE` were rejected as parse errors.
- **SQL Engine**: Large integer literals (values > 2,147,483,647) are now handled correctly. libpg_query represents these as Float AST nodes internally; the parser now recovers whole-number floats as `svInt` values.
- **SQL Engine**: Type coercion for cross-type literals — `bool` ↔ `INT64` and `FLOAT64` → `INT64` (whole numbers) are now allowed in INSERT and UPDATE statements. This fixes UPDATE failures on SQLite-imported databases where `BOOLEAN` columns are stored as `INT64`.
- **SQL Engine**: `ORDER BY` now resolves SELECT-list aliases (e.g. `SELECT COUNT(*) AS "AlbumCount" ... ORDER BY "AlbumCount"`).
- .NET: EF Core DECIMAL type mapping now uses `DECIMAL(18,4)` instead of bare `DECIMAL`, which DecentDB requires precision and scale for.

### Added
- **SQL Engine**: Scalar subquery deferral past Sort+Limit — queries with correlated scalar subqueries in the SELECT list (e.g. `SELECT ..., (SELECT COUNT(*) ...) ... ORDER BY ... LIMIT N`) now defer subquery evaluation until after sorting and limiting, yielding up to **14× speedup** on large tables.
- .NET: 15 comprehensive EF Core CRUD tests covering all 17 CLR data types (bool, byte, short, int, long, float, double, decimal, string, byte[], DateTime, DateTimeOffset, DateOnly, TimeOnly, TimeSpan, Guid, enum), nullable variant lifecycle, edge-case values, SQLite-imported schema with type coercion, async operations, bulk delete, pagination with correlated COUNT subquery, and ChangeTracker.Clear recovery.

## [1.1.1] - 2026-02-15

### Fixed
- **SQL Engine**: `ORDER BY DESC` with table-aliased columns now sorts correctly. Previously, post-projection columns lost their table alias prefix, causing qualified lookups to fail silently — rows were returned in insertion order instead of descending order.
- **SQL Engine**: `CREATE UNIQUE INDEX` now correctly enforces uniqueness on INSERT and UPDATE. Previously, `enforceUnique` used raw byte reads to extract rowids from TEXT/BLOB index entries, reading from the wrong offset — duplicate values were silently accepted.
- **SQL Engine**: Scalar subqueries and `EXISTS` expressions now work correctly with mixed-case (quoted) table and column names.
- **SQL Engine**: Correlated `EXISTS` subqueries now correctly substitute outer-row column references.

### Added
- Python: SQLite import tool now maps `GUID`, `UNIQUEIDENTIFIER`, and `CHAR(36)` declared types to `UUID`.
- Python: SQLite import tool `--detect-uuid` flag inspects TEXT column data and promotes to UUID if values match UUID format.
- .NET: 23 EF Core integration tests mirroring Melodee `ArtistSearchEngineServiceDbContext` query patterns.

### Changed
- .NET: Updated `EntityFrameworkDemo` example NuGet dependencies to latest versions.

## [1.1.0] - 2026-02-14

### Added
- **SQL Engine**: Subquery-in-FROM support — EF Core and hand-written queries can now use subqueries as table sources in FROM clauses.
- **SQL Engine**: Correlated scalar subqueries (`SELECT (SELECT ...)`) and `EXISTS(...)` expressions in SELECT lists.
- **SQL Engine**: SQL functions: `ABS`, `ROUND`, `CEIL`/`CEILING`, `FLOOR`, `REPLACE`, `SUBSTRING`/`SUBSTR`.
- **SQL Engine**: Row cursor eval context — `EXISTS` and `SCALAR_SUBQUERY` expressions now work correctly during row-by-row cursor streaming (used by the C API `decentdb_step` path).
- .NET: `DecentDB.EntityFrameworkCore` runtime provider package (query pipeline, SaveChanges update pipeline, runtime migrations support).
- .NET: `DecentDB.EntityFrameworkCore.Design` package and design-time tooling support for `dotnet ef migrations add`, `dotnet ef database update`, and `dotnet ef dbcontext scaffold`.
- .NET: `DecentDB.EntityFrameworkCore.NodaTime` optional package with `UseNodaTime()` mappings for `Instant`, `LocalDate`, and `LocalDateTime`.
- .NET: EF Core string method LINQ translation: `Contains`, `StartsWith`, `EndsWith`, `ToUpper`, `ToLower`, `Trim`, `TrimStart`, `TrimEnd`, `Substring`, `Replace`.
- .NET: EF Core member translation: `string.Length` → `LENGTH()`.
- .NET: EF Core math method LINQ translation: `Math.Abs`, `Math.Round`, `Math.Ceiling`, `Math.Floor`, `Math.Max`, `Math.Min` (scalar two-argument via `CASE WHEN`).
- .NET: ADO.NET `SqlParameterRewriter` supporting `@named`, `$N`, and `?` positional parameter styles with automatic conversion.
- .NET: Comprehensive EF Core demo (`examples/dotnet/entityframework/`) showcasing 67 operations.

### Changed
- .NET: `DecentDBModificationCommandBatch` now reuses prepared statements across batch rows — **~7× seeding speedup**.
- NuGet publishing now packs and pushes EF Core provider packages alongside `DecentDB.AdoNet` and `DecentDB.MicroOrm`.

### Fixed
- **SQL Engine**: `findMaxParam()` in C API now scans `EXISTS`/`SCALAR_SUBQUERY` literal SQL text for `$N` parameter references.
- **SQL Engine**: Resolved `ResultShadowed` warning in `exec.nim` aggregate path.
- .NET: EF Core lazy loading proxy support (opt-in via `UseLazyLoadingProxies()`).

## [1.0.2] - 2026-02-11

### Changed
- NuGet package now publishes to both GitHub Packages and NuGet.org

## [1.0.1] - 2026-02-11

### Fixed
- .NET: NuGet package packing now places managed assemblies under `lib/net10.0/` so nuget.org correctly reports supported frameworks.

## [1.0.0] - 2026-02-10

### Changed
- Optimized pager with transaction-scoped dirty page tracking for faster commit processing
- Introduced zero-copy WAL writes to avoid data copying during commit operations
- Added thread-local reusable buffers in storage layer to reduce heap allocations during row inserts
- Added `insertRowDirect` fast path for inserts with known schema, bypassing normalization and redundant lookups
- Optimized `TableMeta` updates with in-place `nextRowId`/`rootPage` mutation instead of full struct copies
- Added insert write profiling to precompute constraint/index metadata for optimized execution paths
- Added fast path in value normalization to skip unnecessary work for small TEXT/BLOB values
- Introduced reverse foreign key cache in catalog for efficient parent-table constraint lookups
- Enhanced VFS buffered writes with atomic operations
- Added WAL commit pruning logic to maintain snapshot correctness based on active readers

### Added
- Composite primary key support in SQLite import tool
- CI workflow for automated testing
- Benchmarking support for Firebird in embedded database comparison

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
