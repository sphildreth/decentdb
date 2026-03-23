# ADR-0115: Dart/Flutter FFI Binding Design

**Status:** Accepted  
**Date:** 2026-02-27  
**Updated:** 2026-03-23

## Context

The Rust rewrite needs an in-tree Dart/Flutter desktop binding that stays aligned
with the stable native C ABI exported by `include/decentdb.h` and
`crates/decentdb/src/c_api.rs`.

Earlier draft examples in this ADR used a legacy `decentdb_*` API shape from the
rewrite transition. The implemented Rust surface is the `ddb_*` ABI.

## Decision

### 1. Binding strategy: reuse the stable `ddb_*` C ABI

**Decision:** The Dart package uses `dart:ffi` directly against the Rust `ddb_*`
shared library surface. The native header source of truth is `include/decentdb.h`.

**Rationale:** Keeping Dart on the same ABI as the other bindings avoids a second
native contract and keeps the Rust implementation authoritative.

### 2. ABI versioning

**Decision:** Expose `ddb_abi_version()` and require the Dart package to check it
at load time.

**Rationale:** Hand-written FFI is easy to keep explicit, but load-time ABI
validation prevents silent drift.

### 3. Query/result model

**Decision:** The native layer remains handle/result based:

- `ddb_db_execute(...)`
- `ddb_result_row_count(...)`
- `ddb_result_column_count(...)`
- `ddb_result_column_name_copy(...)`
- `ddb_result_value_copy(...)`

The high-level Dart `Statement` API is implemented in Dart as a convenience
wrapper over that result API.

**Rationale:** This preserves a compact native ABI while still giving Dart users
prepared-statement-like ergonomics (`bind*`, `execute()`, `nextPage()`, `step()`).

### 4. Transaction control

**Decision:** The Dart package uses the native transaction helpers already
exported by the Rust C ABI:

- `ddb_db_begin_transaction()`
- `ddb_db_commit_transaction()`
- `ddb_db_rollback_transaction()`

**Rationale:** These are thin, explicit operations over the engine's transaction
state and avoid routing transaction control through SQL strings in the wrapper.

### 5. Schema metadata

**Decision:** Expose schema metadata to Dart via JSON-returning C ABI helpers:

- `ddb_db_list_tables_json()`
- `ddb_db_describe_table_json()`
- `ddb_db_get_table_ddl()`
- `ddb_db_list_indexes_json()`
- `ddb_db_list_views_json()`
- `ddb_db_get_view_ddl()`
- `ddb_db_list_triggers_json()`

**Rationale:** The Rust engine already has stable inspection structs. Serializing
those across the C ABI keeps the Dart wrapper simple and explicit without adding
native object graphs.

### 6. Memory ownership

**Decision:** The Dart binding follows the native ABI's ownership rules:

- borrowed strings from `ddb_version()` / `ddb_last_error_message()` are valid
  until the next DecentDB call on the same thread
- JSON/column-name strings allocated by the C ABI are released with
  `ddb_string_free()`
- copied cell values returned through `ddb_result_value_copy()` are released with
  `ddb_value_dispose()`
- opaque handles (`ddb_db_t*`, `ddb_result_t*`) are released by the matching
  free functions

### 7. FFI generation approach

**Decision:** Keep the Dart bindings hand-written.

**Rationale:** The surface is still small enough that explicit Dart definitions
remain readable, reviewable, and easy to keep aligned with `include/decentdb.h`.

## Consequences

- Dart now shares the same native ABI story as the rest of the Rust rewrite
- ABI drift is caught immediately at load time
- The package can offer higher-level ergonomics without requiring a second native
  statement ABI
- Metadata and DDL inspection are available through explicit, testable JSON/string
  helpers rather than ad hoc FFI structs
