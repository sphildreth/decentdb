# Index Statistics Strategy
**Date:** 2026-01-28
**Status:** Accepted

### Decision
Use heuristic-based selectivity estimates for the 0.x baseline, defer full statistics collection.

### Rationale
- Heuristics are sufficient for rule-based planner
- Full statistics collection adds complexity (maintenance, updates)
- Target workload has predictable access patterns
- Can add statistics later if needed

### Alternatives Considered
- Full statistics collection in 0.x: More accurate but more complex
- No selectivity estimates: Too naive for good planning

### Trade-offs
- **Pros**: Simple, sufficient for the 0.x baseline
- **Cons**: Less accurate than full statistics, may make suboptimal plan choices

### References
- SPEC.md §9.1 (Index statistics)
