## Cost-Based Optimization and Full Statistics (Post-1.0)
**Date:** 2026-01-30
**Status:** Accepted (Post-1.0 Deferral)

### Decision

Cost-based optimization and full statistics collection are **deferred until post-1.0**. The 0.x baseline uses heuristic-based selectivity estimates and rule-based planning.

### Rationale

1. **0.x Scope:** Cost-based optimization requires significant infrastructure:
   - Statistics collection and maintenance
   - Histogram building and storage
   - Cardinality estimation functions
   - Cost model calibration
   - Plan enumeration and comparison

2. **Heuristics are Sufficient:** For the 0.x baseline workload (FK joins, point lookups, simple predicates), basic heuristics provide acceptable plans:
   - Index selectivity: Fixed estimates (e.g., 10% for range, 1% for equality)
   - Join ordering: Left-deep plans with smallest table first
   - No correlation tracking needed for simple schemas

3. **Risk Reduction:** Statistics bugs can cause catastrophic plan choices. Deferring allows the 0.x baseline to ship with predictable (if not optimal) performance.

### Current 0.x Baseline Approach

- **Selectivity Estimates:** Hardcoded heuristics (see ADR-0013)
- **Join Planning:** Rule-based with simple heuristics
- **Index Selection:** Prefer index when available, no cost comparison

### Future Implementation (Post-1.0)

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

- ADR-0013: Index Statistics Strategy (heuristics for 0.x baseline)
- design/reviews/2026-01-28-SUMMARY.md: P3 items (cost-based optimization deferred)
- SPEC.md §9.1: Statistics requirements
