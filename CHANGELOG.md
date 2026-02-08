# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
