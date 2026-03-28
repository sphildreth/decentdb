# ADR-0128: True 3D Semantics and 3D-Aware Spatial Indexing
**Date:** 2026-03-28
**Status:** Proposed

## Decision

Track true 3D and measure-aware spatial behavior as an explicit follow-on
architecture slice with strict backward-compatibility constraints.

The decisions in this ADR are:

1. The initial geospatial release preserves `Z`, `M`, and `ZM` ordinates in the
   type system, storage, and ABI, but keeps planner-visible query semantics on
   the **XY projection** only.
2. Any future implementation of **true 3D semantics** must **not silently
   change** the behavior of the initial XY-based surface:
   - `ST_Distance`
   - `ST_DWithin`
   - `ST_Intersects`
   - `ST_Contains`
   - `ST_Within`
   - `ST_Equals`
   - `<->`
   - current `USING SPATIAL`
3. True 3D behavior must be introduced through **explicit SQL surface area**,
   not by quietly redefining existing functions or index semantics.
4. `M` remains **non-spatial measure metadata** by default. Any measure-aware
   semantics must also be introduced explicitly and must not change XY or XYZ
   predicate behavior.
5. Any 3D-aware spatial index must be represented with **explicit catalog
   metadata** distinguishing it from the initial XY-projected index behavior.
6. Before implementation begins, a benchmark-backed design must choose the
   physical 3D indexing strategy and define exact semantics separately for:
   - `GEOMETRY`
   - `GEOGRAPHY`

## Rationale

### 1. We Need a Real Plan for 3D, Not a Hand-Wave

Now that the geospatial foundation is dimension-aware, the project should also
reserve an explicit design home for future 3D work. Otherwise, the natural risk
is that 3D behavior gets added incrementally and silently changes the semantics
of previously shipped XY operators.

### 2. Backward Compatibility Matters

Once users write queries against:

- `ST_Distance`
- `ST_DWithin`
- `ST_Intersects`
- `ST_Contains`
- KNN `<->`

they should not see those queries change meaning just because later builds add
true 3D support.

### 3. `Z` and `M` Are Not the Same Thing

`Z` is a spatial axis candidate.
`M` is measure metadata.

Treating them as the same kind of "extra coordinate" would make later function
semantics unclear and would confuse users and bindings.

### 4. 3D Indexing Is Not a Small Extension of XY Indexing

A true 3D-aware index has different tradeoffs:

- covering strategy
- bounding logic
- KNN lower bounds
- storage cost
- false-positive rate
- exact refinement costs

That deserves its own design gate.

### 5. `GEOMETRY` and `GEOGRAPHY` Need Separate 3D Semantics

For `GEOMETRY`, 3D semantics are likely planar/cartesian.
For `GEOGRAPHY`, 3D semantics involve lon/lat plus altitude or elevation and
must be defined much more carefully.

The project should not pretend these are the same problem.

## Required Constraints for Future 3D Work

When DecentDB eventually implements true 3D behavior, the design must satisfy
all of the following constraints.

### 1. Existing XY Surface Remains Stable

The current XY semantics remain the default behavior for the initial function
family and the initial spatial index mode.

### 2. New 3D Semantics Are Explicit

Examples of acceptable directions:

- dedicated `ST_3D*` function family
- explicit 3D KNN operator
- explicit index mode metadata such as `XY` vs `XYZ`

Examples of unacceptable directions:

- making `ST_Distance` start using `Z` automatically
- making current `USING SPATIAL` indexes silently change search semantics

### 3. Index Mode Must Be Explicit

Any 3D-aware spatial index must persist an explicit mode in catalog/index
metadata so that:

- planner behavior is deterministic
- `EXPLAIN` output is accurate
- recovery does not depend on inferred behavior
- bindings and tooling can inspect the intended semantics

### 4. `M` Stays Non-Spatial Unless a Future Surface Says Otherwise

`M` must not automatically participate in:

- distance
- containment
- intersection
- KNN ordering

If measure-aware analytics are added later, they need explicit functions and
docs.

### 5. Dedicated Validation and Test Expansion Is Mandatory

A true 3D slice must add:

- 3D differential validation strategy
- new property tests for XYZ behavior
- new index-vs-scan equivalence tests for 3D predicates
- crash/recovery tests for any 3D-aware index mode
- benchmark comparison against XY mode

## Alternatives Considered

### 1. Defer 3D Entirely Without an ADR

Rejected.

Now that the storage and ABI foundation preserve dimensions, the repository
should also reserve a clear architecture path for real 3D behavior.

### 2. Make Existing `ST_*` Functions Dimension-Sensitive Later

Rejected.

That would create a silent behavioral change for already-deployed queries.

### 3. Treat `M` as a Spatial Axis Later by Default

Rejected.

`M` is fundamentally different from `Z` and should not be pulled into spatial
predicates implicitly.

### 4. Reuse the XY Index Mode and Quietly Add 3D Behavior

Rejected.

3D-aware indexing requires explicit mode metadata and explicit planner support.

## Trade-offs

### Positive

- Makes the 3D roadmap explicit now
- Protects backward compatibility of the initial XY surface
- Prevents accidental semantic drift
- Forces a real design gate for 3D-aware indexing

### Negative

- Adds another ADR boundary before true 3D implementation
- Delays the exact 3D function/index design until benchmark-backed work is done
- Keeps the first shipping semantics intentionally narrower than the stored data

## References

- `design/WIN03_GEOSPATIAL_DATA_SUPPORT.md`
- `design/adr/0124-geospatial-type-system-and-ewkb-storage.md`
- `design/adr/0125-spatial-covering-cell-secondary-index.md`
- `design/adr/0127-planner-native-spatial-access-paths.md`

