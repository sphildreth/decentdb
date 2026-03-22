## LIKE ESCAPE and NULL Semantics
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Define `LIKE` / `ILIKE` semantics for escaping and NULL handling in 0.x:

1. NULL handling
- If either side of `LIKE`/`ILIKE` is `NULL`, result is `NULL`.
- In `WHERE`, `NULL` behaves as unknown and filters out rows (per ADR-0071).

2. Escaping
- Support `... LIKE pattern ESCAPE esc`.
- Escape processing follows SQL intent: the escape character makes the next character literal.
- Internally, custom escapes are normalized to backslash-escape form before pattern matching.
- Escaped `%` and `_` are treated as literal characters.

3. Validation
- `ESCAPE` must resolve to a single-character string at evaluation time.
- Invalid/trailing escape usage returns `ERR_SQL` (no silent fallback).

4. Optimization boundary
- Existing LIKE fast-path optimizations remain enabled only for patterns without escapes.
- Escaped patterns may take the generic matcher path for correctness.

### Rationale

- Roadmap explicitly gates LIKE escaping semantics behind an ADR.
- This preserves SQL compatibility expectations while keeping implementation bounded.
- Rejecting invalid escapes with `ERR_SQL` prevents ambiguous matching behavior.

### Alternatives Considered

1. **Ignore `ESCAPE` clause**
- Rejected: incorrect for SQL semantics.

2. **Support only backslash escapes and reject custom `ESCAPE`**
- Rejected: unnecessarily restrictive and less SQL-compatible.

3. **Silently treat invalid escapes as literals**
- Rejected: masks errors and diverges from predictable semantics.

### Trade-offs

- Generic path for escaped patterns can be slower than unescaped optimized cases.
- SQL compatibility improves without changing persistent formats.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (LIKE edge-case gate)
- NULL semantics ADR: `design/adr/0071-sql-null-three-valued-logic.md`
