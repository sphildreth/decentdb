## SQL Parser Choice (libpg_query from start)
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Adopt `libpg_query` via FFI as the SQL parser from the start of Phase 4 and beyond.

### Rationale
- Aligns with the baseline recommendation in `design/SPEC.md` §6.1.
- Ensures Postgres-compatible syntax and parse tree structure early.
- Avoids a later parser migration that would impact tests and planner/exec logic.

### Alternatives Considered
- Rust-native parser for the Phase 4 subset (rejected).

### Trade-offs
- Adds a C dependency that must be available at build time.
- Requires an FFI layer and parse-tree normalization in Rust.

### References
- `design/SPEC.md` §6.1 (parser choice)
- Replaces deleted ADR `0034-sql-parser-choice.md`
