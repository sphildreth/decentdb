# ADR-0127: Planner-Native Spatial Access Paths
**Date:** 2026-03-28
**Status:** Proposed

## Decision

Implement spatial query acceleration as a planner-native capability rather than
as executor-only scalar function evaluation.

The required decisions are:

1. Add planner recognition for supported spatial predicate shapes.
2. Add new physical plan variants:
   - `SpatialFilter`
   - `SpatialKnn`
3. Use a two-phase execution model:
   - coarse candidate generation from the spatial index
   - exact refinement in the executor
4. Support both literals and bound parameters as the query-shape operand for
   index-eligible predicates.
5. Restrict initial KNN support to point columns with `ORDER BY <-> ... LIMIT`.
6. Define initial planner-visible spatial semantics on the **XY projection** of
   the stored value. Preserved `Z/M/ZM` ordinates do not affect initial access
   path selection or predicate truth.
7. Prefer spatial indexes heuristically in the initial release; do not block on
   new spatial statistics in `ANALYZE`.
8. Extend `EXPLAIN` / `EXPLAIN ANALYZE` to show spatial plan nodes and their
   candidate/refinement counters.

## Rationale

### 1. Spatial Functions Are Not Ordinary Scalar Filters

If the planner treats `ST_DWithin` or `ST_Intersects` as opaque scalar
functions, it cannot exploit the spatial index or produce efficient candidate
plans.

### 2. Candidate-Then-Refine Is the Correct Execution Shape

Spatial covering indexes naturally produce approximate candidates. Exact
geometry or geography evaluation belongs in the executor after candidate
generation.

### 3. Parameterized Prepared Statements Must Still Use Spatial Indexes

Prepared statements are central to embedded workloads. Requiring literal-only
queries for spatial index use would be a poor fit for DecentDB.

### 4. KNN Needs Its Own Plan Shape

Nearest-neighbor ordering is not the same as a boolean filter. It should not be
shoehorned into a generic filter operator.

### 5. Dimensions Must Be Planned For Now Without Blocking v1

Preserving `Z/M/ZM` while defining initial query semantics on XY keeps the core
contract future-proof without forcing full 3D or measure-aware planning into
the first slice.

## Alternatives Considered

### 1. Executor-Only Spatial Evaluation

Rejected.

This would degrade geospatial support to scan-time function calls.

### 2. Literal-Only Spatial Index Use

Rejected.

Prepared statements with bound parameters must still benefit from spatial
indexes.

### 3. Full Spatial Join Operator in the First Slice

Rejected for the initial release.

The first steps are single-table spatial filters and point KNN. Spatial joins
can follow after those paths are stable and benchmarked.

### 4. Add Spatial Statistics Before Any Spatial Planning

Rejected.

Initial delivery should use stable heuristics and not wait on a larger stats
project.

## Trade-offs

### Positive

- Real spatial query acceleration
- Prepared statements remain first-class
- Query plans are inspectable
- Clean separation between approximate index phase and exact geometry phase
- Dimension-aware storage does not force immediate 3D planning complexity

### Negative

- Planner complexity increases
- Initial spatial plan recognition is intentionally narrower than the full SQL
  surface
- Some complex boolean forms will still fall back to scan until later slices
- `Z/M` are preserved but ignored by initial planner-visible semantics

## References

- `design/WIN03_GEOSPATIAL_DATA_SUPPORT.md`
- `design/adr/0112-cost-based-optimizer-with-stats.md`
- `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`
- DuckDB spatial join discussion:
  `https://duckdb.org/2025/05/21/announcing-duckdb-130.html`
- PostGIS KNN `<->` operator:
  `https://postgis.net/docs/geometry_distance_knn.html`
