# Changelog

All notable changes to DecentDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.1] - 2026-03-28

### Fixed
- Python binding cursor semantics: `Connection.execute(...)` once again returns a fresh cursor per call instead of mutating a single shared cursor object across successive executions on the same connection.
- Release automation validation for the Python binding suite and Java FFM smoke path so the GitHub release workflow matches the supported dependency/toolchain setup.
- Memory-safety nightly stress coverage to use valid SQL, fail fast on unexpected database errors, and keep the working set bounded with periodic checkpoints instead of drifting into self-inflicted I/O failures.
- Benchmark asset refresh automation on protected `main` branches by pushing the generated asset commit to `automation/benchmark-assets` and opening/updating a PR (or surfacing a manual PR URL) instead of attempting a direct protected-branch push.

### Changed
- Docs deployment now publishes from docs changes merged to `main`, which keeps benchmark-asset refreshes compatible with pull-request-only repository rules without reintroducing the earlier double-publish path.

## [2.0.0] - 2026-03-28

> **v2.0.0 marks the first release of the Rust-native DecentDB engine.**
> The original DecentDB engine was written in [Nim](https://nim-lang.org/).
> For v2.0.0 the engine core was rewritten from scratch in Rust, delivering
> substantially improved memory safety, ACID durability guarantees, a stable
> C ABI (`include/decentdb.h`), and a fully updated binding ecosystem
> (.NET, Python, Go, Java/JDBC/DBeaver, Node.js, Dart). The v1.x Nim-era
> releases are preserved in repository history but are no longer maintained.

### Added
- SQL window enhancements: `NTILE`, `PERCENT_RANK`, `CUME_DIST`, aggregate window functions, and frame-aware execution for `ROWS` and supported `RANGE` bounds.
- Statistical and ordered-set aggregates: `ARRAY_AGG`, `MEDIAN`, `PERCENTILE_CONT ... WITHIN GROUP`, and `PERCENTILE_DISC ... WITHIN GROUP`.
- Trigonometric math functions: `SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `PI`, `DEGREES`, `RADIANS`, and `COT`.
- Conditional scalar functions: `GREATEST`, `LEAST`, and `IIF`.
- DML enhancements: `UPDATE ... RETURNING`, `DELETE ... RETURNING`, and `TRUNCATE TABLE` with `RESTART IDENTITY`, `CONTINUE IDENTITY`, and `CASCADE`.
- Subquery comparison operators: `expr op ANY/SOME (subquery)` and `expr op ALL (subquery)`.
- Regex comparison operators: `~`, `~*`, `!~`, `!~*`.
- Extended date/time functions: `DATE_TRUNC`, `DATE_PART`, `DATE_DIFF`, `LAST_DAY`, `NEXT_DAY`, `MAKE_DATE`, `MAKE_TIMESTAMP`, `TO_TIMESTAMP`, `AGE`, and timestamp `INTERVAL` arithmetic.
- Extended string functions: `CONCAT`, `CONCAT_WS`, `POSITION`, `INITCAP`, `ASCII`, `REGEXP_REPLACE`, `SPLIT_PART`, `STRING_TO_ARRAY`, `QUOTE_IDENT`, `QUOTE_LITERAL`, `MD5`, and `SHA256`.
- Query features slice (S9): standalone `VALUES` query bodies, `VALUES` table sources with alias column naming, `CREATE TABLE ... AS SELECT` (including `WITH NO DATA` and `IF NOT EXISTS`), row-value `IN (VALUES ...)`, and `LATERAL` subqueries/table-function joins.
- DDL enhancements slice (S11): `ALTER TABLE ... RENAME TO ...` and `ALTER TABLE ADD/DROP CONSTRAINT` for named `CHECK`, named `FOREIGN KEY`, and named `UNIQUE` constraints with existing-row validation.
- Utility commands slice (S12): SQL-level `PRAGMA` support for `page_size`, `cache_size`, `integrity_check`, `database_list`, and `table_info(table)` with read/query behavior and constrained assignment semantics.
- DDL enhancements slice (S11) completion: `GENERATED ALWAYS AS (...) VIRTUAL` columns with parser support, read-time computation semantics, persistence metadata support, and `table_ddl` rendering of `VIRTUAL` vs `STORED`.
- S13 increment: covering index syntax `CREATE INDEX ... INCLUDE (...)` for BTREE key-column indexes, including parser normalization, catalog persistence, metadata APIs, and SQL dump rendering.
- S13 increment: `CREATE SCHEMA` with catalog namespace registration, `IF NOT EXISTS` semantics, and persistence across restart/checkpoint.

### Changed
- Refreshed repository documentation to present DecentDB as the current Rust engine and binding ecosystem.
- Clarified release/versioning docs around the current public `v2.x` release line.
