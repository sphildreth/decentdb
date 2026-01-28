## Overflow Page Format
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Store large TEXT/BLOB payloads in a chained sequence of overflow pages. Each page begins with a 4-byte next pointer and 4-byte data length.

### Rationale
- Minimal header overhead
- Works with single-page reads and simple chaining

### Alternatives Considered
- Multi-level indirection tables (more complex)
- Fixed-size chunking with external metadata (more format surface)

### Trade-offs
- Sequential reads for large values
- Chained pages require pointer traversal

### References
- `design/SPEC.md` §2.1 record/
- `design/IMPLEMENTATION_PHASES.md` Phase 2 checklist
