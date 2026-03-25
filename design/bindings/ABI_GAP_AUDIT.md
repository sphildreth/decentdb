# Rust C ABI Gap Audit for Packaged Bindings

## Purpose

This document compares the current Rust C ABI to the native surface required by
the packaged bindings that exist in this repository today.

Historical package layouts are reference material only; they are not
compatibility targets.

This document is intentionally narrower than
`design/RUST_MISSING_FEATURE_PLAN.md`:

- `design/RUST_MISSING_FEATURE_PLAN.md` tracks SQL feature-surface and
  documentation drift in the Rust engine.
- this audit tracks binding-facing C ABI capabilities and remaining native
  integration gaps.

Work in motion on `design/RUST_MISSING_FEATURE_PLAN.md` should improve overall
engine parity and reduce binding surprises, but it does not by itself close the
remaining C ABI backlog identified here.

## Baseline

Current Rust C ABI:

- `include/decentdb.h`
- handle-based database lifecycle
- immediate `execute` with typed parameter array
- handle-based statement API with `prepare`, typed binds, `reset`,
  `clear_bindings`, `step`, and `free`
- copied value access plus borrowed row-view helpers
- transaction control
- checkpoint and `save_as`
- schema introspection helpers returning JSON
- ABI/version queries

Current first-party binding usage:

- Python, .NET, Go, Java, Node, and Dart already use the current statement API
  and/or schema helpers rather than relying only on `ddb_db_execute`
- several bindings already map directly onto statement/rows-style host-language
  APIs

## Summary

The current Rust C ABI is sufficient for:

- C-level smoke tests
- real packaged bindings in this repository
- statement-oriented execution models
- incremental row iteration
- schema discovery through JSON metadata helpers

The current Rust C ABI is still missing some higher-fidelity metadata and
capability surfaces that would reduce host-language workarounds for the most
ambitious provider layers.

The main remaining gaps are now:

- result-set metadata richness such as declared column type and nullability
- explicit open options such as read-only vs create/open policy
- capability discovery beyond the coarse ABI/version surface

## Capability Matrix

| Capability | Rust C ABI | Legacy shape | Status | Notes |
|---|---|---|---|---|
| Open / close database | Yes | Yes | Ready | `ddb_db_open`, `ddb_db_create`, `ddb_db_open_or_create`, and `ddb_db_free` exist. |
| Stable status codes | Yes | Partial | Ready | Rust returns numeric status per call. |
| Last error text | Yes | Yes | Ready | Rust uses thread-local error text instead of handle-local lookup. |
| ABI / version query | Yes | Partial | Ready | `ddb_abi_version` and `ddb_version` exist. |
| Immediate execute with params | Yes | Partial | Ready | `ddb_db_execute` still covers simple command/query paths. |
| Materialized result access | Yes | Partial | Ready | Row/column count, column names, affected rows, and typed copied values exist on result handles. |
| Transactions | Yes | Partial | Ready | Begin/commit/rollback and transaction-state query are available. |
| Checkpoint / save-as | Yes | Yes | Ready | Present in the current ABI. |
| Prepared statement handle | Yes | Yes | Ready | `ddb_db_prepare` and `ddb_stmt_free` exist and are used by multiple bindings. |
| Statement reset / clear bindings | Yes | Yes | Ready | `ddb_stmt_reset` and `ddb_stmt_clear_bindings` exist. |
| Streaming row iteration | Yes | Yes | Ready | `ddb_stmt_step`, `ddb_stmt_step_row_view`, and fetch helpers support incremental consumption. |
| Borrowed zero-copy row view | Yes | Yes | Ready | `ddb_value_view_t`, `ddb_stmt_row_view`, and row-view fetch helpers exist with explicit borrowed-lifetime rules. |
| Statement-scoped rows affected | Yes | Yes | Ready | `ddb_stmt_affected_rows` exists. |
| Batch execution helpers | Yes | Partial | Ready | The ABI exposes batch helpers for common typed statement shapes. |
| Schema introspection via C ABI | Yes | Yes | Ready | Table, column, index, view, trigger, and DDL helpers exist through JSON and string-returning APIs. |
| Column declared type metadata on result sets | No | Partial | Gap | Current result/statement APIs expose names and runtime values, but not declared result-column type metadata. |
| Result-set nullability metadata | No | Partial | Gap | Useful for JDBC `ResultSetMetaData`, ADO.NET schema tables, and richer provider diagnostics. |
| Column origin metadata | No | Partial | Gap | Table/source-column origin remains unavailable through the C ABI. |
| Open options / access mode flags | No | Partial | Gap | There is still no explicit read-only / open-existing / create-if-missing options surface. |
| Fine-grained capability discovery | No | Partial | Gap | ABI/version exists, but bindings still lack a native way to query feature/capability flags. |

## Language Impact

### Python

The current ABI is sufficient for the packaged Python DB-API layer and current
SQLAlchemy work in this repository:

- prepared statements already exist
- incremental row fetch already exists
- schema introspection helpers already exist

Remaining native improvements for Python are mostly about metadata fidelity and
feature discovery, not baseline viability.

### .NET

The current ABI is sufficient for the packaged .NET native layer, ADO.NET work,
and EF-related schema discovery in this repository.

The main remaining native gaps for .NET are:

- richer result metadata for data-reader/schema-table fidelity
- explicit capability discovery
- explicit open/configuration flags if provider behavior needs them

### Go

The current ABI already fits the `database/sql` statement-and-rows model much
better than the earlier immediate-execute-only surface.

The remaining gaps are mostly metadata richness and capability negotiation, not
the absence of a viable native execution model.

### Java

The current JDBC work can already build on:

- `PreparedStatement`-style native handles
- step-based row iteration
- schema introspection helpers

The strongest remaining pressure is still metadata fidelity for
`ResultSetMetaData` and `DatabaseMetaData`, especially where host-language code
would otherwise need to infer more than the C ABI directly exposes.

### Node

The current low-level Node package can use the existing statement API and schema
helpers. Knex integration no longer depends on a future statement surface
appearing first.

Remaining native improvements are the same narrower metadata and capability
discovery items.

### Dart

The current ABI is sufficient for a thin FFI layer and statement-oriented
execution.

Prepared statements and schema helpers are no longer blockers; remaining gaps
are again around richer metadata and capability negotiation.

## Recommended ABI Expansion Order

### Step 1: Result-Set Metadata Surface

Add statement/result metadata helpers for:

- declared column type
- nullability where known
- column origin data where practical

This is now the highest-value remaining ABI work for richer ADO.NET, JDBC, and
tooling integration.

### Step 2: Open Options and Capability Discovery

Add a small explicit options struct or open-flags surface for:

- open existing
- create if missing
- read-only
- optional binding-relevant tuning knobs only where they are stable enough to
  expose

Add capability queries so bindings can gate features without inferring behavior
from version numbers alone.

### Step 3: Keep Schema Metadata Stable While Engine Feature Work Lands

As `design/RUST_MISSING_FEATURE_PLAN.md` continues closing SQL-surface gaps,
keep the existing C ABI schema helpers aligned with real engine behavior and
avoid introducing binding-only workarounds for temporary engine drift.

The current statement API does not need a new baseline expansion to make
bindings viable; the priority is now correctness and metadata completeness.

## Migration Implication

Porting or extending packaged bindings no longer needs to wait for a future
statement API or for first-pass schema introspection support. Those surfaces now
exist and are already in use.

New binding work should treat the current ABI as viable, while focusing any new
native design effort on:

- richer result metadata
- explicit open/configuration semantics
- capability discovery

SQL feature completion should continue under `design/RUST_MISSING_FEATURE_PLAN.md`,
but that plan should not be treated as a substitute for ABI-specific review.
