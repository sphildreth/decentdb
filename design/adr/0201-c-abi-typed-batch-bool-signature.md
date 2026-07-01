# ADR 0201: C ABI Typed Batch Bool Signature

**Date:** 2026-06-30
**Status:** Accepted

## Context

The existing `ddb_stmt_execute_batch_typed` C ABI function accepts a
NUL-terminated type signature with `i`, `f`, and `t` characters for `INT64`,
`FLOAT64`, and `TEXT` values. This is enough for many simple insert paths, but
ordinary relational schemas often include `BOOLEAN` columns.

The .NET CRM benchmark in `design/2026-06-30_PERF_PLAN.md` inserts invoices
with an explicit `paid` boolean value. Without a boolean typed-batch character,
ADO.NET must either bind the boolean through the slower generic path or encode
the value as a SQL literal, which prevents the engine's direct positional
prepared-insert path from being used.

## Decision

Extend the existing typed-batch signature grammar with `b` for `BOOLEAN`.

The C function signature does not change. `b` values are encoded in the
existing `values_i64` array as `0` for `FALSE` and non-zero for `TRUE`, packed
in row order alongside `i` values. During parameter materialization, the engine
turns `b` entries into `Value::Bool`.

The resulting signature grammar is:

- `i`: `INT64`, read from `values_i64`
- `b`: `BOOLEAN`, read from `values_i64` as `0`/non-zero
- `f`: `FLOAT64`, read from `values_f64`
- `t`: `TEXT`, read from `values_text_ptrs` and `values_text_lens`

This is an additive source-level contract extension. Existing callers using
only `i`, `f`, and `t` continue to work unchanged.

The .NET binding exposes this through a thin `DecentDBConnection`
`ExecutePreparedBatchTyped` wrapper over the existing native prepared-batch
function. The wrapper intentionally remains low-level for this phase: callers
provide the SQL text, signature, row count, and packed typed arrays. A higher
level bulk-copy API can build on top of it in a separate API design.

## Consequences

.NET and other maintained bindings can route prepared one-row and batch DML
containing boolean values through the existing typed-batch API without a new
C symbol or ownership model.

The encoding intentionally reuses `values_i64` to avoid adding pointer
arguments and to preserve the existing ABI function shape. Binding
documentation must state that `values_i64` contains both integer and boolean
slots in signature order.

Unsupported signature characters remain errors.

## Validation

Validation requires:

- C ABI tests or binding tests for a typed batch containing `b`;
- ADO.NET tests for prepared inserts with boolean parameters;
- regression coverage that an all-positional prepared insert containing a
  boolean remains eligible for the engine direct insert path;
- existing typed-batch tests for `i`, `f`, and `t` remain passing.
