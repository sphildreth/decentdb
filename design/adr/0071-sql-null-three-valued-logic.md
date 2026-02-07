## SQL NULL Three-Valued Logic Semantics
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Adopt SQL three-valued logic (3VL) semantics for predicate evaluation involving `NULL` in DecentDB.

This ADR defines the behavior for:

1. **Boolean operators**
   - `NOT NULL` → `NULL`
   - `TRUE AND NULL` → `NULL`
   - `FALSE AND NULL` → `FALSE`
   - `TRUE OR NULL` → `TRUE`
   - `FALSE OR NULL` → `NULL`

2. **Comparison operators**
   - For `=`, `!=`, `<`, `<=`, `>`, `>=`: if either operand is `NULL`, result is `NULL`.

3. **`IN (...)` predicate (non-subquery form)**
   - If left operand is `NULL`, result is `NULL`.
   - If any non-`NULL` list item matches, result is `TRUE`.
   - If no match and at least one list item is `NULL`, result is `NULL`.
   - If no match and no list item is `NULL`, result is `FALSE`.

4. **`LIKE` / `ILIKE`**
   - If either operand is `NULL`, result is `NULL`.

5. **`WHERE` filtering rule**
   - Rows are retained only when predicate evaluates to `TRUE`.
   - `FALSE` and `NULL` both filter the row out.

`IS NULL` and `IS NOT NULL` remain explicit null tests and return boolean (`TRUE`/`FALSE`) results.

### Rationale

- This aligns DecentDB with standard SQL behavior and PostgreSQL expectations for nullable data.
- It removes current correctness gaps where `NULL` was being coerced to boolean false too early in expression evaluation.
- It unblocks higher-level SQL features in the roadmap (e.g., `CASE`, `CHECK`, partial indexes, trigger predicates) that depend on predictable 3VL semantics.

### Alternatives Considered

1. **Keep two-valued logic with `NULL` as false**
   - Simpler implementation.
   - Rejected: incorrect vs SQL/PostgreSQL semantics and produces surprising results.

2. **Treat `NULL` comparisons as `FALSE` only**
   - Slightly less invasive than full 3VL.
   - Rejected: still incorrect for `NOT`, `IN`, and boolean composition.

### Trade-offs

- Slightly more executor complexity for boolean/predicate operators.
- Existing tests relying on implicit `NULL -> FALSE` behavior must be updated.
- No storage/catalog/WAL impact.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.1, NULL semantics gate)
- SQL parser ADR: `design/adr/0035-sql-parser-libpg-query.md`
- Repo workflow and ADR gating: `AGENTS.md`
