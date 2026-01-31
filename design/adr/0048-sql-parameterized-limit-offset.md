# ADR-0048: Parameterized LIMIT/OFFSET

**Status**: Accepted
**Date**: 2026-01-30

## Context
DecentDB uses Postgres-style positional parameters (`$1`, `$2`, ...) per ADR-0005.

In the current SQL implementation, `LIMIT` and `OFFSET` are parsed only when they are integer literals. When a parameter is used (e.g. `LIMIT $2`), the parser silently ignores it (treating it as “no limit”), but the statement still contains `$2` in the SQL text.

This causes two practical problems:
- ADO.NET/Dapper workflows commonly parameterize `LIMIT`/`OFFSET` (e.g. `LIMIT @limit`, `OFFSET @offset`).
- The C API / provider will attempt to bind all parameters referenced in the SQL string, but the engine only allocates parameters for those referenced in expressions it tracks. This mismatch can yield runtime errors like “Bind index out of bounds”.

We want embedded consumers (especially Dapper) to work out-of-the-box while keeping parameter semantics predictable and safe.

## Decision
DecentDB SQL will support positional parameters in `LIMIT` and `OFFSET`.

Rules:
- `LIMIT` and `OFFSET` may be:
  - an integer literal, or
  - a positional parameter reference (`$N`).
- Other expression forms in `LIMIT`/`OFFSET` are rejected with `ERR_SQL` (instead of being silently ignored).
- At execution time, parameterized `LIMIT`/`OFFSET` values must evaluate to a non-negative integer (INT64) and must fit into the engine’s `int` range.

This change is additive for the intended Postgres-like subset and fixes a correctness issue (previous silent ignore) that can lead to mismatched parameter binding.

## Consequences
- Dapper-style SQL such as `... LIMIT @limit OFFSET @offset` (rewritten to `$N`) works correctly.
- Queries that previously used non-integer expressions for `LIMIT`/`OFFSET` (and were silently treated as unlimited) will now return an error. This is considered a correctness improvement.
- Planner/executor will need to carry optional parameter indices for `LIMIT`/`OFFSET` and resolve them using the bound parameter array.

## Alternatives Considered
1. Provider-side literal inlining for `LIMIT`/`OFFSET`
   - Pros: avoids SQL dialect change.
   - Cons: requires SQL-context-aware rewriting; risks subtle bugs; inconsistent with ADR-0005 parameter model.

2. Continue silently ignoring non-literal `LIMIT`/`OFFSET`
   - Pros: no changes.
   - Cons: breaks Dapper ergonomics and can lead to runtime bind mismatches.
