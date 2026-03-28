# ADR-0125: Spatial Covering-Cell Secondary Index
**Date:** 2026-03-28
**Status:** Proposed

## Decision

Implement the initial geospatial secondary index as a new `SPATIAL` index kind
stored in the existing B+Tree subsystem using hierarchical covering cells.

The core decisions are:

1. Add `IndexKind::Spatial`.
2. Add `RuntimeIndex::Spatial`.
3. Represent spatial index entries as repeated B+Tree keys of the form:

```text
[strategy_tag:u8][level:u8][cell_id_be:u64][rowid_sortable_u64]
```

4. Store a small value payload for cheap refinement:
   - point columns: exact point coordinates
   - non-point columns: envelope
5. Use type-family-specific covering strategies behind the same public
   abstraction:
   - `GEOGRAPHY`: spherical cell covering (`s2` module)
   - `GEOMETRY`: planar quadtree covering (`quadcell` module)
6. Do **not** implement the first release as a dedicated R*-Tree / R-Tree page
   subsystem.
7. Restrict initial `SPATIAL` indexes to:
   - one plain spatial column
   - non-`UNIQUE`
   - non-partial
   - non-expression
   - no `INCLUDE` columns
8. Compute index keys and coverings from the **XY projection only** in the
   initial release. Preserve any `Z/M/ZM` ordinates in the base row value but
   do not index them.

## Rationale

### 1. DecentDB Is Explicitly B+Tree-Centered

The accepted storage direction for DecentDB is B+Tree-based. Reusing the
existing page cache, WAL, and B+Tree storage path for spatial indexing is the
lowest-risk way to ship a durable spatial index early.

### 2. Covering-Cell Indexes Fit Both `GEOGRAPHY` and `GEOMETRY`

An object-tree design is a natural fit for some planar geometry workloads, but
covering-cell indexing provides a single abstraction that works for:

- spherical geography
- planar geometry
- candidate generation for `DWithin`
- candidate generation for `Intersects` / `Contains` / `Within`
- future planner-native joins

### 3. The Engine Already Has a Candidate-Then-Refine Pattern

DecentDB's trigram search path already relies on:

- coarse candidate generation
- exact post-filtering

Spatial indexing follows the same proven shape:

- covering overlap gives candidate row ids
- exact geometry evaluation refines them

### 4. This Avoids a Second Durable Index Stack in the First Slice

Introducing a new R*-Tree page format immediately would increase risk in:

- WAL/recovery
- checkpointing
- crash testing
- page layout evolution
- maintenance code paths

The first release should minimize storage-surface expansion while still
delivering real geospatial performance.

## Alternatives Considered

### 1. Dedicated R*-Tree as the Initial Spatial Index

Rejected for the first implementation.

It may still be worth revisiting later if benchmark evidence shows a major
advantage for targeted workloads, but it is not the right starting point for
DecentDB's current architecture.

### 2. No Spatial Index Initially, Scan Only

Rejected.

This would not meet the product goal for modern, high-performance geospatial
support.

### 3. B+Tree on Derived Bounding-Box Columns Only

Rejected.

This would force user-visible schema hacks, weaken type safety, and provide a
poor developer experience compared with a real spatial index abstraction.

### 4. Separate Index Kinds for S2 and Quadtree

Rejected.

The public SQL surface should expose one `SPATIAL` index kind. The physical
strategy can be derived from the spatial type family and column metadata.

## Trade-offs

### Positive

- Strong fit with existing B+Tree storage
- Reuses WAL/page-cache machinery
- One public abstraction for both geometry and geography
- Supports point workloads and non-point candidate filtering
- Easier incremental delivery than a brand-new tree implementation
- Leaves room for later 3D-aware indexing without breaking stored values

### Negative

- Covering-cell indexes produce false positives and require exact refinement
- Tuning is a size vs precision tradeoff
- Some workloads may eventually benefit from a dedicated object-tree index
- KNN logic is more involved than a pure one-dimensional ordered seek
- `Z` and `M` do not participate in initial spatial index semantics

## References

- `design/WIN03_GEOSPATIAL_DATA_SUPPORT.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `design/adr/0123-phase1-table-btree-foundation.md`
- `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`
- CockroachDB spatial indexes:
  `https://www.cockroachlabs.com/docs/stable/spatial-indexes`
- BigQuery geography functions:
  `https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions`
