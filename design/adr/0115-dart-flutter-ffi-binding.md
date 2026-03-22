# ADR-0115: Dart/Flutter FFI Binding Design

**Status:** Accepted
**Date:** 2026-02-27

## Context

The Gridlock project needs a Dart/Flutter desktop binding for DecentDB. DecentDB already exposes a stable C ABI (`src/c_api.rs`) used by Go, Python, .NET, Java, and Node.js bindings.

## Decisions

### 1. Binding strategy: Reuse existing C ABI

**Decision:** Direct Dart FFI (`dart:ffi`) against the existing `libc_api.so`/`.dylib`/`.dll`. No C shim needed.

**Rationale:** The C ABI is stable, versioned, and battle-tested across five language bindings. Adding a shim would create maintenance burden with no benefit.

### 2. ABI versioning

**Decision:** Add `decentdb_abi_version()` returning an integer (starting at 1). Bumped on any breaking ABI change. Dart package checks at load time and fails fast on mismatch.

### 3. Transaction control

**Decision:** Add `decentdb_begin()`, `decentdb_commit()`, `decentdb_rollback()` as thin C API wrappers around `engine.beginTransaction/commitTransaction/rollbackTransaction`. This avoids prepare+step overhead for transaction control.

Existing bindings use SQL strings ("BEGIN"/"COMMIT"/"ROLLBACK") through the prepare/step path. Both approaches remain valid.

### 4. Result paging representation

**Decision:** Row-at-a-time via `prepare/step/column_*` pattern (same as all other bindings). High-level Dart API provides cursor abstraction with configurable page size that batches rows in Dart memory.

**Rationale:** The C API streams one row at a time. Batching in Dart gives the caller control over memory vs. latency trade-offs without engine changes.

### 5. Memory ownership

**Decision:** Follow existing C API rules:
- **Borrowed pointers** from `column_text/column_blob/row_view`: valid until next `step/reset/finalize`. Dart must copy before next step.
- **Allocated pointers** from `list_tables_json/get_table_columns_json/list_indexes_json/list_views_json/get_view_ddl`: caller frees with `decentdb_free()`.
- **Opaque handles** (`decentdb_db*`, `decentdb_stmt*`): managed by open/close and prepare/finalize respectively.

### 6. Cancellation semantics

**Decision:** Best-effort via `decentdb_finalize()` from a different isolate. DecentDB does not currently support mid-query interruption. The Dart API documents this limitation and provides `Statement.dispose()` as the cancellation primitive.

### 7. FFI generation approach

**Decision:** Hand-written Dart FFI bindings (not `ffigen`). The API surface is small (~40 functions) and hand-written bindings give full control over nullability, naming, and documentation without adding a build-time code generation dependency.

## Consequences

- Dart binding follows established patterns; no engine changes to result streaming
- ABI version check prevents silent incompatibility
- Native transaction calls reduce overhead for high-frequency commit patterns
- Hand-written FFI means manual updates when C API changes (mitigated by ABI version check)
