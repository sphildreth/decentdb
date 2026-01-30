# Changelog

All notable changes to DecentDb will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- SQL IN operator support
- Page utilization monitoring for B+Tree
- Full documentation site with MkDocs
- Additional performance benchmarks

### Changed
- Updated ROAD_TO_RTM.md with completion status
- Improved documentation organization

## [1.0.0] - 2026-01-30

### Added
- Initial stable release
- ACID transactions with WAL-based durability
- PostgreSQL-like SQL subset
- B+Tree storage with page cache
- Trigram inverted index for text search
- Single writer + multiple readers concurrency
- Snapshot isolation
- Foreign key constraints
- Bulk load API
- CLI with comprehensive commands
- Comprehensive test suite (unit, property, crash, differential)
- 7 performance benchmarks
- Full Nim API
- Cross-platform support (Linux, macOS, Windows)

### SQL Support
- CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX
- SELECT, INSERT, UPDATE, DELETE
- WHERE, ORDER BY, LIMIT, OFFSET
- INNER JOIN, LEFT JOIN
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- GROUP BY, HAVING
- Parameters: $1, $2, ...
- Data types: NULL, INT64, TEXT, BLOB, BOOL, FLOAT64
- Constraints: PRIMARY KEY, FOREIGN KEY, UNIQUE, NOT NULL
- Transactions: BEGIN, COMMIT, ROLLBACK
- LIKE, ILIKE pattern matching

### Features
- B-Tree indexes (automatic for PK, FK)
- Trigram indexes for fast text search
- External merge sort for large ORDER BY
- Page utilization monitoring
- Configurable cache size
- Multiple durability modes
- Database integrity verification
- Import/export to CSV

### Architecture
- Modular design (VFS, Pager, WAL, BTree, Storage, SQL, Planner, Exec)
- Comprehensive error handling
- Memory-safe Nim implementation
- Property-based testing
- Crash injection testing
- Differential testing vs PostgreSQL

### Documentation
- README with quick start
- Full API reference
- SQL reference guide
- Architecture documentation
- Design documents (PRD, SPEC, ADRs)
- MkDocs-based documentation site

### Performance
- Point lookups: P95 < 10ms
- FK joins: P95 < 100ms
- Text search: P95 < 200ms
- Bulk load: 100k records < 20s

## Known Limitations

As of version 1.0.0:

- **Single writer only** - No concurrent write transactions
- **Single process** - No multi-process access
- **No subqueries** - In SELECT list
- **No window functions** - ROW_NUMBER, RANK, etc.
- **No CTEs** - Common Table Expressions (WITH)
- **No ALTER TABLE** - Schema changes limited to CREATE/DROP
- **No views** - Virtual tables not supported
- **No stored procedures** - Server-side logic
- **Statement-time FK checks** - Not deferred to COMMIT
- **No full-text search** - Only trigram indexes
- **No replication** - Single node only
- **No encryption** - Data at rest not encrypted

## Future Roadmap

### Version 1.1 (Planned)
- Subquery support
- Additional SQL functions
- Query plan caching
- Improved error messages

### Version 1.2 (Planned)
- ALTER TABLE support
- SAVEPOINT for nested transactions
- More indexing options

### Version 2.0 (Future)
- Multi-process concurrency
- Replication support
- Query optimizer improvements
- Full-text search with ranking

## Release Notes Archive

For detailed release notes of each version, see [GitHub Releases](https://github.com/sphildreth/decentdb/releases).

## Contributing to Changelog

When making changes:
1. Add entry under [Unreleased]
2. Categorize as Added, Changed, Deprecated, Removed, Fixed, or Security
3. Reference issue/PR numbers when applicable
4. Move to version section on release
