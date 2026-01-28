# Foreign Key Enforcement Timing
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Enforce foreign key constraints at statement time (MVP).

### Rationale
- Simpler implementation (no need to track deferred constraints)
- Errors are caught immediately (easier debugging)
- Consistent with many databases' default behavior
- Sufficient for most use cases

### Alternatives Considered
- Commit-time enforcement: More flexible but more complex
- Configurable per-constraint: Most flexible but most complex

### Trade-offs
- **Pros**: Simple, immediate error detection
- **Cons**: Less flexible for complex multi-statement transactions

### References
- SPEC.md §7.2 (Foreign keys)
