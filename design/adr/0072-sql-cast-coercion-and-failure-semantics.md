## SQL CAST Coercion and Failure Semantics
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Implement `CAST(expr AS type)` with a narrow, explicit conversion matrix for 0.x.

Supported target types:
- `INT`, `INTEGER`, `INT64`
- `FLOAT`, `FLOAT64`, `REAL`
- `TEXT`
- `BOOL`, `BOOLEAN`

`NULL` input always returns `NULL` for supported target types.

Supported conversions:

1. Target `INT64`
- `INT64 -> INT64` (identity)
- `FLOAT64 -> INT64` (truncate toward zero)
- `BOOL -> INT64` (`TRUE=1`, `FALSE=0`)
- `TEXT -> INT64` (strict parse, must be valid integer text)

2. Target `FLOAT64`
- `FLOAT64 -> FLOAT64` (identity)
- `INT64 -> FLOAT64`
- `BOOL -> FLOAT64` (`TRUE=1.0`, `FALSE=0.0`)
- `TEXT -> FLOAT64` (strict parse, must be valid float text)

3. Target `TEXT`
- `TEXT -> TEXT` (identity)
- `INT64/FLOAT64/BOOL -> TEXT` via canonical engine string formatting

4. Target `BOOL`
- `BOOL -> BOOL` (identity)
- `INT64 -> BOOL` (`0=false`, non-zero=true)
- `FLOAT64 -> BOOL` (`0.0=false`, non-zero=true)
- `TEXT -> BOOL` accepting case-insensitive: `true`, `false`, `1`, `0`

Failure behavior:
- Unsupported target types or source-target pairs return `ERR_SQL`.
- Invalid text parses return `ERR_SQL` with cast context.
- No silent fallback to `NULL` on conversion failure.

### Rationale

- Roadmap requires a narrow initial cast matrix with explicit failure behavior.
- Explicit rules reduce ambiguity and match DecentDB’s correctness-first posture.
- The chosen matrix covers practical application needs without widening type semantics prematurely.

### Alternatives Considered

1. **Very permissive casts (PostgreSQL-like breadth)**
- Rejected for 0.x scope complexity and higher semantic risk.

2. **Cast failures return `NULL`**
- Rejected because silent failures hide data quality issues.

3. **Only identity casts**
- Rejected as too limited for common SQL usage.

### Trade-offs

- Some PostgreSQL casts remain unsupported in 0.x.
- Strict parsing is less permissive but easier to reason about.
- No storage/catalog/WAL format impact.

### References

- SQL enhancements roadmap: `design/SQL_ENHANCEMENTS_PLAN.md` (Section 5.1 CAST gate)
- NULL semantics ADR: `design/adr/0071-sql-null-three-valued-logic.md`
