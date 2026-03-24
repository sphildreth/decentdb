# Rust C ABI Gap Audit for Packaged Bindings

## Purpose

This document compares the current Rust C ABI to the native surface assumed by
higher-level packaged integrations. Historical package layouts are reference
material only; they are not compatibility targets.

## Baseline

Current Rust C ABI:

- `include/decentdb.h`
- handle-based database lifecycle
- immediate `execute` with typed parameter array
- materialized result handle with copied cell access
- transaction control
- checkpoint and `save_as`

Reference legacy native surface:

- archived packaged binding headers and host-language tests
- statement-oriented API with `prepare`, typed binds, `step`, and borrowed row
  accessors
- JSON schema introspection helpers

## Summary

The current Rust C ABI is sufficient for:

- C-level smoke tests
- simple language validation
- low-volume immediate execution APIs

The current Rust C ABI is not yet sufficient for full parity with the higher
level packages that previously existed for Python, .NET, Go, Java, Node, and
Dart.

## Capability Matrix

| Capability | Rust C ABI | Legacy shape | Status | Notes |
|---|---|---|---|---|
| Open / close database | Yes | Yes | Ready | `ddb_db_open`, `ddb_db_create`, `ddb_db_free` exist. |
| Stable status codes | Yes | Partial | Ready | Rust returns numeric status per call. |
| Last error text | Yes | Yes | Ready | Rust uses thread-local error text instead of handle-local lookup. |
| Immediate execute with params | Yes | Partial | Ready | `ddb_db_execute` covers simple command/query paths. |
| Materialized result access | Yes | Partial | Ready | Row/column count, column names, and typed copied values exist. |
| Transactions | Yes | Partial | Ready | Begin/commit/rollback are available. |
| Checkpoint / save-as | Yes | Yes | Ready | Present in both worlds. |
| Prepared statement handle | No | Yes | Gap | Needed by most serious drivers/providers. |
| Statement reset / clear bindings | No | Yes | Gap | Needed for statement reuse and cursor semantics. |
| Streaming row iteration | No | Yes | Gap | Current API materializes the whole result. |
| Borrowed zero-copy row view | No | Yes | Gap | Old low-level APIs exposed borrowed buffers for efficiency. |
| Column declared type metadata | No | Partial | Gap | Current API exposes values, not result-set schema types. |
| Schema introspection via C ABI | No | Yes | Gap | Needed for JDBC metadata, ADO.NET schema APIs, and tooling. |
| Open options / capability flags | No | Partial | Gap | Useful for read-only, create/open policy, cache knobs, feature discovery. |
| Statement-scoped rows affected | Partial | Yes | Gap | Result-level affected rows exist, but not statement handles. |

## Language Impact

### Python

Can bootstrap a DB-API layer on the current Rust C ABI, but performance and
cursor semantics will be limited without:

- prepared statements
- incremental row fetch
- schema metadata

SQLAlchemy can wait until the base DB-API package is stable.

### .NET

Smoke validation is already possible, but real ADO.NET and EF Core integration
will want:

- command and reader semantics
- schema tables and metadata
- prepared statement reuse

### Go

`database/sql` strongly prefers a statement and rows model. A production-quality
driver should not be forced to buffer all rows eagerly.

### Java

JDBC has the strongest metadata pressure:

- `PreparedStatement`
- `ResultSet`
- `ResultSetMetaData`
- `DatabaseMetaData`

This package likely needs both statement APIs and C ABI schema introspection.

### Node

A low-level package can start with immediate execute and materialized results.
Knex should wait until the base package is stable.

### Dart

A thin FFI layer can start on the current ABI, but prepared statements and
streaming would still improve parity and performance.

## Recommended ABI Expansion Order

### Step 1: Statement API

Add a handle-based statement surface:

- `prepare`
- `bind_*`
- `clear_bindings`
- `reset`
- `step`
- `finalize`

Keep ownership explicit and panic-safe.

### Step 2: Result and Metadata Schema

Add result-set schema helpers:

- declared column type
- nullable flag where known
- column origin data where practical

Add schema introspection functions for:

- list tables
- describe table
- list indexes
- list views
- list triggers

### Step 3: Open Options and Capability Discovery

Add a small explicit options struct or open-flags surface for:

- open existing
- create if missing
- read-only
- optional tuning knobs that matter at the binding layer

Add capability/version queries so higher-level packages can gate features safely.

## Migration Implication

Porting packaged bindings should begin only after the statement API and metadata
direction are settled. Otherwise each language package will invent its own
workarounds around `ddb_db_execute`, and those workarounds will become the
maintenance burden.
