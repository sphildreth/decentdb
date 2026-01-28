# B+Tree Space Management
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Implement node split for MVP, defer merge/rebalance, use compaction for space recovery.

### Rationale
- Split is necessary for correctness (cannot overflow pages)
- Merge/rebalance adds significant complexity
- Compaction provides equivalent space recovery
- Delete-heavy workloads are less common in target use case

### Alternatives Considered
- Implement merge/rebalance in MVP: More complex but better space efficiency
- No compaction: Simpler but space bloat over time

### Trade-offs
- **Pros**: Simpler MVP, compaction provides space recovery
- **Cons**: May have temporary space bloat, requires periodic compaction

### References
- SPEC.md §17 (B+Tree space management)
