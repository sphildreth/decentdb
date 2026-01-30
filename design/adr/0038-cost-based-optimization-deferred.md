## Cost-Based Optimization and Full Statistics (Post-MVP)
**Date:** 2026-01-30
**Status:** Accepted (Post-MVP Deferral)

### Decision

Cost-based optimization and full statistics collection are **deferred as post-MVP features**. The MVP uses heuristic-based selectivity estimates and rule-based planning.

### Rationale

1. **MVP Scope:** Cost-based optimization requires significant infrastructure:
   - Statistics collection and maintenance
   - Histogram building and storage
   - Cardinality estimation functions
   - Cost model calibration
   - Plan enumeration and comparison

2. **Heuristics are Sufficient:** For the MVP's target workload (FK joins, point lookups, simple predicates), basic heuristics provide acceptable plans:
   - Index selectivity: Fixed estimates (e.g., 10% for range, 1% for equality)
   - Join ordering: Left-deep plans with smallest table first
   - No correlation tracking needed for simple schemas

3. **Risk Reduction:** Statistics bugs can cause catastrophic plan choices. Deferring allows MVP to ship with predictable (if not optimal) performance.

### Current MVP Approach

- **Selectivity Estimates:** Hardcoded heuristics (see ADR-0013)
- **Join Planning:** Rule-based with simple heuristics
- **Index Selection:** Prefer index when available, no cost comparison

### Future Implementation (Post-MVP)

When cost-based optimization is implemented:

1. **Minimal Statistics:** Start with table-level stats only:
   - Row count
   - Approximate distinct count (HyperLogLog)
   - Min/max values per column

2. **Incremental Enhancement:**
   - Column histograms for range predicates
   - Correlation tracking for multi-column predicates
   - Join cardinality estimation

3. **Validation:**
   - Differential testing against PostgreSQL planner choices
   - Regression tests for plan stability
   - Performance benchmarks comparing heuristics vs. cost-based

### References

- ADR-0013: Index Statistics Strategy (heuristics for MVP)
- design/reviews/2026-01-28-SUMMARY.md: P3 items (cost-based optimization deferred)
- SPEC.md §9.1: Statistics requirements
