# Foreign Key Index Creation
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Auto-create indexes on child FK columns if not present.

### Rationale
- Ensures FK checks are efficient (avoids full table scans)
- Reduces user burden (don't need to remember to create indexes)
- Consistent with PostgreSQL behavior
- Index name follows predictable pattern (`fk_<table>_<column>_idx`)

### Alternatives Considered
- Require explicit index creation: More control but higher burden
- No index requirement: Simpler but terrible performance

### Trade-offs
- **Pros**: Good performance by default, less user burden
- **Cons**: Additional indexes increase storage and write overhead

### References
- SPEC.md §7.2 (Foreign keys)
