# B+Tree Space Management
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Implement node split for the 0.x baseline, defer merge/rebalance, use compaction for space recovery.

### Rationale
- Split is necessary for correctness (cannot overflow pages)
- Merge/rebalance adds significant complexity
- Compaction provides equivalent space recovery
- Delete-heavy workloads are less common in target use case

### Alternatives Considered
- Implement merge/rebalance in 0.x: More complex but better space efficiency
- No compaction: Simpler but space bloat over time

### Trade-offs
- **Pros**: Simpler 0.x baseline, compaction provides space recovery
- **Cons**: May have temporary space bloat, requires periodic compaction

### References
- SPEC.md §17 (B+Tree space management)
