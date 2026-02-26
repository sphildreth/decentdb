# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0] - 2026-02-27

### Added
- **SQL Engine**: Views with GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET, and DISTINCT ON — view and CTE bodies that aggregate, sort, or limit rows are now expanded as derived tables (subqueries in FROM) instead of being rejected. See ADR-0113.
- **Demo Database**: Comprehensive `make_demo_db` script now showcases all supported features including DEFAULT values, FK actions (SET NULL, CASCADE), UNIQUE INDEX, INSERT RETURNING, ON CONFLICT, auto-increment, 12 views (aggregates, window functions, CTEs, CASE, COALESCE, JSON operators, subqueries, UNION ALL, LIKE/ILIKE), INSTEAD OF trigger, and ANALYZE.
- **Engine**: In-memory database support via `:memory:` connection string — each `openDb(":memory:")` creates a new, isolated, ephemeral database backed by `MemVfs`. WAL remains enabled for consistent transaction semantics. See ADR-0105.
- **Engine**: `saveAs` — export any open database (including `:memory:`) to a new on-disk file. Performs a full checkpoint, then streams pages to the destination via atomic temp-file + rename. Available as a Nim proc, C API function, CLI command, and in all bindings (.NET, Go, Node, Python).
- **SQL Engine**: Window functions `RANK()`, `DENSE_RANK()`, `LAG()`, and `LEAD()` — extends the existing `ROW_NUMBER()` support with additional SQL:2003 window functions. All support `PARTITION BY` and `ORDER BY` clauses. `LAG`/`LEAD` accept 1–3 arguments (expression, offset, default). See ADR-0106.
- **SQL Engine**: Math scalar functions `SQRT(x)`, `POWER(x, y)` / `POW(x, y)`, and `MOD(x, y)` — extends numeric function coverage for SQLite parity. All handle INT64, FLOAT64, and DECIMAL inputs; return FLOAT64. NULL propagation follows SQL standard. See issue #37.
- **SQL Engine**: String scalar functions `INSTR(str, substr)`, `CHR(n)`, and `HEX(val)` — `INSTR` returns 1-based position (0 if not found), `CHR` converts ASCII code point to character (PostgreSQL syntax), `HEX` encodes integers/text/blobs as uppercase hexadecimal. See issue #37.
- **SQL Engine**: `%` modulo binary operator for INT64, FLOAT64, and DECIMAL types — complements the `MOD()` function with operator syntax (`SELECT 17 % 5`). Division-by-zero returns an error. See issue #37.
- **SQL Engine**: `TOTAL(expr)` aggregate function — like `SUM` but always returns FLOAT64 and 0.0 for empty sets (never NULL), matching SQLite semantics. See issue #37.
- **SQL Engine**: `DISTINCT` aggregate modifier — `COUNT(DISTINCT expr)`, `SUM(DISTINCT expr)`, and `AVG(DISTINCT expr)` now de-duplicate values per group before aggregating. NULL values are excluded. See issue #37.
- **SQL Engine**: Window functions `FIRST_VALUE(expr)`, `LAST_VALUE(expr)`, and `NTH_VALUE(expr, n)` — extends window function coverage with value-access functions over ordered partitions. See issue #37.
- **SQL Engine**: `json_array(...)` scalar function — constructs a JSON array from arguments. See issue #37.
- **SQL Engine**: `json_each(json)` and `json_tree(json)` table-valued functions — `json_each` iterates top-level keys/values of a JSON object or array; `json_tree` recursively walks nested structures. Returns rows with `key`, `value`, `type` (and `path` for `json_tree`). See ADR-0111.
- **SQL Engine**: `WITH RECURSIVE` common table expressions — supports recursive CTEs for hierarchical queries (tree traversal, graph walks, series generation). See ADR-0107.
- **SQL Engine**: `RIGHT JOIN` (via LEFT JOIN rewrite), `FULL OUTER JOIN`, `CROSS JOIN`, and `NATURAL JOIN` support. See issue #37.
- **SQL Engine**: `DISTINCT ON (expr, ...)` — keeps only the first row per distinct group, ordered by the specified expressions. PostgreSQL-compatible syntax. See issue #37.
- **SQL Engine**: `DEFAULT` column constraint — columns with `DEFAULT` values are automatically populated when omitted from `INSERT` statements. See issue #37.
- **SQL Engine**: Generated columns (`STORED`) — columns defined with `GENERATED ALWAYS AS (expr) STORED` are computed on INSERT/UPDATE and persisted. See ADR-0108.
- **SQL Engine**: `CREATE TEMP TABLE` and `CREATE TEMP VIEW` — session-scoped temporary objects that are not persisted to disk. See ADR-0109.
- **SQL Engine**: `SAVEPOINT name` / `RELEASE SAVEPOINT name` / `ROLLBACK TO SAVEPOINT name` — nested transaction control with page-level snapshot rollback. See ADR-0110.
- **SQL Engine**: `OFFSET n ROWS FETCH FIRST n ROWS ONLY` (SQL:2008 syntax) as an alias for `LIMIT`/`OFFSET`. See issue #37.
- **SQL Engine**: `BEGIN IMMEDIATE` and `BEGIN EXCLUSIVE` accepted as synonyms for `BEGIN`. See issue #37.
- **SQL Engine**: `DATE` and `TIMESTAMP` column type keywords accepted in DDL (mapped to TEXT storage). See issue #37.
- **CLI**: `save-as` command — `decentdb save-as --db=:memory: --output=backup.ddb` exports a database snapshot to a new file.
- **VFS**: `MemVfs` implementation — memory-backed Virtual File System with `seq[byte]` storage, per-file locking, and full VFS interface compliance (no `mmap` support).
- **VFS**: `getFileSize`, `fileExists`, and `removeFile` methods added to the VFS interface, replacing direct OS calls in the engine, pager, and WAL.
- **VFS**: `OsVfsFile` subclass introduced — `VfsFile` refactored from concrete type to base class (`ref object of RootObj`) to support polymorphic VFS file implementations.
- **C API**: `decentdb_save_as(db, dest_path_utf8)` — export database to on-disk file via FFI.
- **.NET**: `DecentDBConnection.SaveAs(destPath)` and `DecentDB.Native.DecentDB.SaveAs(destPath)` methods for exporting databases.
- **Go**: `DB.SaveAs(destPath)` method for exporting databases.
- **Node**: `Database.saveAs(destPath)` method for exporting databases.
- **Python**: `Connection.save_as(dest_path)` method for exporting databases.

### Changed
- **SQL Engine**: Expanded CHECK constraint function allowlist — deterministic scalar functions (`ABS`, `ROUND`, `CEIL`, `CEILING`, `FLOOR`, `SQRT`, `POWER`, `POW`, `MOD`, `INSTR`, `CHR`, `CHAR`, `HEX`, `REPLACE`, `SUBSTR`, `SUBSTRING`) are now permitted in CHECK expressions. Previously only `CASE`, `CAST`, `COALESCE`, `NULLIF`, `LENGTH`, `LOWER`, `UPPER`, `TRIM`, and `LIKE_ESCAPE` were allowed.

### Fixed
- **VFS**: Double `deinitLock` undefined behavior — `MemVfs.removeFile` no longer calls `deinitLock`; `close()` is the sole owner of lock lifecycle.
- **Engine**: `getFileSize` error in `openDb` now properly propagated instead of silently returning 0.
- **WAL**: `getFileSize` error in `ensureWalMmapCapacity` now properly propagated instead of silently returning 0.
- **WAL**: Removed unused `import os` (all OS operations now go through VFS).
- **Pager**: Transactional freelist header updates — `allocatePage()` and `freePage()` no longer fsync the DB header mid-transaction. Header is now only persisted at checkpoint, eliminating a crash-safety window where the on-disk header could reflect uncommitted freelist state. Freelist header is reconstructed from the page chain on open. See ADR-0057.
- **B-tree**: Stale page cache `aux` index — cached `InternalNodeIndex` was only invalidated on first dirty transition; subsequent writes to already-dirty pages left stale navigation data causing `find()` to follow wrong child pointers when multiple B-trees were modified in the same transaction. Manifested as false FK constraint failures during interleaved multi-table inserts.
- **SQL Engine**: HAVING aggregate evaluation — aggregate functions in HAVING expressions now have their results substituted before evaluation, fixing "aggregate not allowed in scalar context" errors.
- **SQL Engine**: View expansion for complex views — views with GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET are now wrapped as derived tables instead of being rejected at bind time.


## [1.4.0] - 2026-02-22

### Added
- **SQL Engine**: `json_array_length(json [, path])` scalar function — returns the number of elements in a JSON array. Supports optional JSONPath for nested access. See ADR-0102.
- **SQL Engine**: `json_extract(json, path)` scalar function — extracts a value from a JSON document using JSONPath notation (`$`, `$[N]`, `$.key`). Returns the appropriate SQL type (TEXT, INT64, FLOAT64, BOOL, NULL). See ADR-0102.
- .NET: EF Core primitive collection support — `string[]` properties stored as JSON are now fully queryable via LINQ (`.Any()`, `.Count()`, `.Contains()`, `array[index]`, `.Select()`). See ADR-0103.
- .NET: NodaTime member translation plugin — `Instant.InUtc().Year/Month/Day` and other date part extractions now translate to SQL expressions. See ADR-0101.
- .NET: `SqlStatementSplitter` for batch SQL execution in ADO.NET layer.
- .NET: `SqlParameterRewriter` tests covering named, positional, and mixed parameter styles.
- .NET: Primitive collection tests (11 tests covering Any, Count, Contains, ElementAt, Select, null/empty arrays).

### Fixed
- **SQL Engine**: Case-insensitive identifier resolution following PostgreSQL semantics — unquoted identifiers (lowercased by the parser) now correctly match tables, columns, and indexes created with quoted identifiers and vice versa. See ADR-0096.
- **SQL Engine**: SQLite-compatible type affinity in comparisons — TEXT values are coerced to INTEGER/FLOAT for comparison operators and rowid seeks, matching SQLite behavior. See ADR-0099.
- **SQL Engine**: Partial index query planner exclusion — partial indexes are no longer selected by the general query planner, preventing incorrect results when the query predicate doesn't match the index predicate. See ADR-0100.
- **SQL Engine**: Composite primary key tables no longer incorrectly use rowid seeks — individual columns in composite PKs are not rowid aliases. See ADR-0104.
- **SQL Engine**: `ORDER BY` on aggregate aliases (e.g., `ORDER BY "AlbumCount"`) now correctly references the materialized aggregate output instead of re-evaluating the aggregate function. See ADR-0104.
- **SQL Engine**: `FROM (SELECT ... UNION SELECT ...)` subqueries now correctly derive column metadata from the left operand. See ADR-0104.
- **SQL Engine**: LEFT JOIN column resolution with subquery returning zero rows — correctly derive column names from inner projection and pad NULLs. See ADR-0098.
- **SQL Engine**: `IN (SELECT ...)` parameter scanning now correctly finds `$N` references inside subquery SQL text.
- **Shared Library**: Disable Nim's signal handler (`-d:noSignalHandler`) and use system allocator (`-d:useMalloc`) to prevent conflicts with host runtimes (.NET, JVM). See ADR-0097.
- **Engine**: Evict stale Pager references from threadvar caches in `closeDb()` to prevent memory leaks under ARC. See ADR-0097.
- .NET: NodaTime `Instant` type mapping now uses tick-level precision (100ns ticks since Unix epoch) instead of millisecond precision, matching .NET `DateTimeOffset.Ticks` behavior.
- .NET: EF Core database creator now handles table/index creation failures gracefully during `EnsureCreated()`.
- .NET: ADO.NET `SqlParameterRewriter` correctly handles parameters in complex subqueries and multi-statement SQL.


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
- Window functions support `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD` but no frame clauses (`ROWS BETWEEN`, `RANGE BETWEEN`) or `NTILE`/`PERCENT_RANK`/`CUME_DIST`
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
