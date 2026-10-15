# ADR 0216: Native SQL Integration for the Go Bun ORM

**Date:** 2026-10-13
**Status:** Accepted

## Context

Bun builds PostgreSQL-flavored SQL on top of a database/sql driver. Its normal
models emit BIGSERIAL primary keys, DEFAULT insert values, JSONB columns, and
target aliases in UPDATE/DELETE. DecentDB currently rejects or incorrectly
resolves these constructs. An example with disabled dialect features or special
model tags would hide gaps rather than provide a dependable integration.

## Decision

- Support target-table aliases in native INSERT, UPDATE, and DELETE name
  resolution, including assignments, predicates, RETURNING, and correlated
  subqueries. Alias binding is part of SQL normalization; inner query bindings
  retain their own scope. Alpha-rename local AST bindings when necessary to
  prevent capture of a correlated target qualifier. Do not rewrite SQL strings
  in the Go driver.
- Accept SERIAL/BIGSERIAL declarations for a single integer primary key, using
  the existing durable auto-assigned ID contract. Reject unsupported serial
  declarations explicitly rather than pretending to implement independent
  PostgreSQL sequence objects. Persisted ID and rollback behavior remain the
  native DecentDB contract.
- Support DEFAULT in INSERT VALUES and UPDATE assignments, evaluating the
  declared default or native auto-assignment/NULL behavior in column context.
  DEFAULT outside a valid column-value context returns a SQL error.
- Accept JSON/JSONB declarations with the engine's existing JSON text storage
  and JSON functions. Add JSON_VALID checks for declared JSON columns and
  validate explicit JSON casts. JSONB does not imply a new binary storage format
  or PostgreSQL JSONB-specific operators beyond the supported JSON SQL surface.
- Decode PostgreSQL hex BYTEA input into the existing native BLOB value.
- Resolve quoted numeric literals in DECIMAL arithmetic and column-aware index
  probes without float conversion. Preserve ADR 0099 comparison affinity for
  invalid numeric text and explicitly typed TEXT values.
- SUM/AVG of DECIMAL remain DECIMAL, with checked i128 intermediates and the
  existing i64 coefficient/scale limits. AVG follows ADR 0215 division rounding
  (at least six fractional digits; ties away from zero). Explicit FLOAT64
  operands promote the aggregate, integer AVG remains FLOAT64, and TOTAL always
  returns FLOAT64. Normalize transient decimal equality keys for consistent
  DISTINCT/grouping/join behavior; persisted encoding is unchanged.
- Expose exact decimal Scanner, Valuer, and text formatting on the Go binding's
  Decimal type using standard database/sql interfaces. Bun remains a dependency
  of the standalone example only, not of the engine or shared Go driver.
  Validate scales at every driver binding entry point; never silently clamp
  a caller's coefficient/scale value.
- The demo uses Bun's ordinary PostgreSQL dialect, model-generated schema and
  CRUD, relations, transactions, defaults, native numeric/JSON/timestamp/blob
  values, exact financial reports, indexed queries, Bun migrations and soft
  deletes, and persistence. It contains no dialect feature masks,
  SQL-text rewrites, or manually omitted ID columns to bypass engine failures.

No persisted layout/version, C ABI tags, locking model, or core dependency changes.

## Validation

Native SQL regressions cover alias scopes, DEFAULT semantics, bulk returning,
serial IDs and persistence, JSON validation, and BYTEA round trips. Go driver
tests verify exact decimal standard-interface behavior. Bun integration tests
run the ordinary dialect against the native library and cover CRUD, generated
IDs, bulk inserts, upserts, relationships, rollback, defaults, semantic values,
and repeated file-backed execution. Run formatting, workspace Clippy/tests,
impacted binding checks, MSRV/WASM checks, and relevant prepared-query benchmarks.
