# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.1] - 2026-02-15

### Fixed
- **SQL Engine**: `ORDER BY DESC` with table-aliased columns now sorts correctly. Previously, post-projection columns lost their table alias prefix, causing qualified lookups like `"e"."name"` to fail silently — rows were returned in insertion order instead of descending order.
- **SQL Engine**: `CREATE UNIQUE INDEX` now correctly enforces uniqueness on INSERT and UPDATE. Previously, `enforceUnique` used raw byte reads to extract rowids from index entries, but TEXT/BLOB index entries use a variable-length encoding — the rowid was read from the wrong offset, so duplicate values were silently accepted.
- **SQL Engine**: Scalar subqueries and `EXISTS` expressions now work correctly with mixed-case (quoted) table and column names. Previously, `selectToCanonicalSql` emitted identifiers without double-quoting, causing libpg_query to lowercase them on re-parse, leading to "Table not found" errors for tables created with quoted mixed-case names (e.g. `"Artists"`).
- **SQL Engine**: Correlated `EXISTS` subqueries now correctly substitute outer-row column references. Previously, `EXISTS` evaluation did not call `substituteCorrelatedStmt`, so any `EXISTS (SELECT ... WHERE inner.col = outer.col)` failed with "Unknown table".

### Added
- Python: SQLite import tool now maps `GUID`, `UNIQUEIDENTIFIER`, and `CHAR(36)` declared types to `UUID`.
- Python: SQLite import tool `--detect-uuid` flag inspects TEXT column data and promotes to UUID if values match UUID format.
- .NET: 23 EF Core integration tests mirroring Melodee `ArtistSearchEngineServiceDbContext` query patterns (INSERT, UPDATE, DELETE, ORDER BY, Include, correlated subqueries, pagination, etc.).

### Changed
- .NET: Updated `EntityFrameworkDemo` example NuGet dependencies — EF Core 10.0.0 → 10.0.3, DependencyInjection 10.0.0 → 10.0.3, Logging.Console 10.0.0 → 10.0.3, NodaTime 3.2.2 → 3.3.0.

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
- .NET: Comprehensive EF Core demo (`examples/dotnet/entityframework/`) showcasing 67 operations — CRUD, pagination, Include/ThenInclude, AsSplitQuery, filtered Include, GroupBy, DISTINCT, projections (DTOs), CASE WHEN, string operations, Any/All, Min/Max, Math.Round, FromSqlRaw, and NodaTime (Instant + LocalDate + DateTime coexistence).

### Changed
- .NET: `DecentDBModificationCommandBatch` now reuses prepared statements across batch rows — **~7× seeding speedup** (e.g. 27s → 3.6s for 14K track inserts).
- .NET: `DecentDBCommand` properly propagates `CommandTimeout` and handles parameterized queries with subquery-generated SQL.
- NuGet publishing now packs and pushes EF Core provider packages alongside `DecentDB.AdoNet` and `DecentDB.MicroOrm`.
- Versioning policy for published .NET packages remains synchronized via CI-supplied package version.

### Fixed
- **SQL Engine**: `findMaxParam()` in C API now scans `EXISTS`/`SCALAR_SUBQUERY` literal SQL text for `$N` parameter references that were previously invisible to expression tree walkers.
- **SQL Engine**: Resolved `ResultShadowed` warning in `exec.nim` aggregate path.
- .NET: EF Core lazy loading proxy support (opt-in via `UseLazyLoadingProxies()`).

### Known limitations
- Window functions limited to `ROW_NUMBER` (no `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, etc.).
- Only non-recursive CTEs supported (no `WITH RECURSIVE`).
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` not supported (only `INSERT ... RETURNING`).

## [1.0.2] - 2026-02-13

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

## [0.1.0] - 2026-02-08

### Added
- `INTEGER PRIMARY KEY` columns now support auto-increment: omit the column from INSERT and the next sequential ID is assigned automatically (ADR-0092)
- `PRIMARY KEY` implies `NOT NULL` for single-column integer primary keys
- `INSERT ... RETURNING` for retrieving auto-assigned values
- `INSERT ... ON CONFLICT DO NOTHING` and `ON CONFLICT DO UPDATE` (upsert)
- `<>` operator (alias for `!=`)
- C API: `decentdb_checkpoint()`, `decentdb_free()`, `decentdb_list_tables_json()`, `decentdb_get_table_columns_json()`, `decentdb_list_indexes_json()`
- .NET: Full ADO.NET provider, MicroOrm with DbSet/Context, ConnectionStringBuilder, DbProviderFactory, GetSchema(), UpsertAsync, SelectAsync projection, raw SQL
- Go: `database/sql` driver, OpenDirect API with Checkpoint, ListTables, GetTableColumns, ListIndexes
- Python: DB-API 2.0 with checkpoint, list_indexes, SQLAlchemy dialect
- Node.js: N-API addon with Database/Statement, async iteration, checkpoint, schema introspection, Knex integration

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
- No stored procedures
- Statement-time foreign key enforcement (not deferred)
- No full-text search with ranking (trigram substring matching only)
- No replication
- No built-in encryption
- Window functions limited to ROW_NUMBER (no RANK, DENSE_RANK, LAG, LEAD, etc.)
- Only non-recursive CTEs supported (no `WITH RECURSIVE`)
- `UPDATE ... RETURNING` and `DELETE ... RETURNING` are not supported (only `INSERT ... RETURNING`)
- `ADD CONSTRAINT` (post-creation) is not supported
- Targetless `ON CONFLICT DO UPDATE` is not supported

## Contributors

Thanks to all contributors who made this release possible!

See [GitHub contributors](https://github.com/sphildreth/decentdb/graphs/contributors) for the full list.
