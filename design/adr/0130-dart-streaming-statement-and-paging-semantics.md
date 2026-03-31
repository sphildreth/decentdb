# ADR-0130: Dart Streaming Statement and Paging Semantics

**Status:** Proposed
**Date:** 2026-03-29

## Context

The current Dart `Statement` implementation prepares SQL once, but then
materializes the full result set in `_fetchAll()` before `step()` or
`nextPage()` can return anything. That defeats the purpose of paging and causes
avoidable memory growth on large result sets.

The stable C ABI already exposes row-view and batch-oriented fast paths:

- `ddb_stmt_step_row_view`
- `ddb_stmt_fetch_row_views`
- `ddb_stmt_bind_int64_step_row_view`
- `ddb_stmt_bind_int64_step_i64_text_f64`
- `ddb_stmt_execute_batch_*`
- `ddb_stmt_rebind_*`

The Dart binding is therefore leaving existing engine/ABI performance on the
table.

## Decision

### 1. `query()` may materialize; `step()` and `nextPage()` may not

**Decision:** Keep `Statement.query()` as the explicit full-materialization API.

`Statement.step()` and `Statement.nextPage()` must become truly streaming. They
must not call a helper that reads the entire result set into memory first.

### 2. Use row-view C ABI paths internally

**Decision:** Implement streaming in Dart on top of:

- `ddb_stmt_step_row_view`
- `ddb_stmt_fetch_row_views`

Raw borrowed row-view pointers remain internal to the binding. They are not
exposed as a public Dart API because their lifetime is tied to native statement
state and the next DecentDB call on the same thread.

### 3. Define the statement execution state machine explicitly

**Decision:** The Dart `Statement` object follows this state model:

- prepared, not yet executing
- actively executing a row-producing statement
- exhausted
- reset/invalidated
- disposed

The following operations invalidate streaming state and restart execution on the
next read:

- `bind*`
- `bindAll`
- `reset`
- `clearBindings`

### 4. Mixed `step()` / `readRow()` / `nextPage()` semantics are fixed

**Decision:** Behavioral rules:

- `step()` advances by one row and makes that row available through `readRow()`
- a row consumed by `step()` is not returned again by `nextPage()`
- `nextPage()` advances from the current cursor position and returns only future
  rows
- `nextPage()` invalidates the current row for `readRow()`
- `readRow()` is valid only immediately after a successful `step()`

### 5. `query()` uses chunked streaming internally

**Decision:** `query()` remains available, but it must materialize results by
reading chunked pages from the streaming path rather than by maintaining a
separate full-fetch code path.

Use a fixed internal chunk size of `256` rows for `query()`.

### 6. Expose missing high-value statement fast paths

**Decision:** The Dart public API will add high-value convenience wrappers for:

- batch execution
- re-execute helpers
- shared WAL eviction

The raw row-view accessors stay internal. The typed fused bind+step helpers may
be used internally later, but this ADR does not require a public typed row-view
API.

### 7. Performance validation is part of the contract

**Decision:** The refactor is not complete without targeted streaming and batch
tests plus an updated Dart benchmark run.

PR-fast tests remain mandatory; heavier benchmark comparison stays outside the
default PR gate.

## Consequences

- `nextPage()` and `step()` become usable for large result sets without full
  materialization
- the Dart binding closes the biggest correctness/performance gap called out in
  the v2 binding review
- `Statement` internals become more stateful and therefore require stronger
  tests for reset/rebind/mixed-cursor behavior
- borrowed row-view memory remains safely encapsulated inside the binding rather
  than leaking lifetime hazards into user code

