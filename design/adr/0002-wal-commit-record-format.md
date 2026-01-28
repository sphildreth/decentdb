# WAL Commit Record Format
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use a dedicated commit record type instead of a boolean commit flag in each frame.

### Rationale
- Clearer separation between data frames and commit markers
- Easier to extend with additional commit metadata (transaction_id, timestamp)
- Simplifies recovery logic (scan for commit records rather than checking flags)
- More robust for future features (e.g., savepoints, nested transactions)

### Alternatives Considered
- Boolean commit flag per frame: Simpler but less extensible
- Commit marker as special page_id: Confusing and error-prone

### Trade-offs
- **Pros**: Clear semantics, extensible, easier recovery
- **Cons**: Slightly more complex frame format

### References
- SPEC.md §4.1 (WAL frame format)
