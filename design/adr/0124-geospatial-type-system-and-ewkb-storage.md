# ADR-0124: Geospatial Type System and Normalized EWKB Storage
**Date:** 2026-03-28
**Status:** Proposed

## Decision

Introduce first-class geospatial SQL types and a canonical binary storage
format:

1. Add two SQL-visible type families:
   - `GEOMETRY`
   - `GEOGRAPHY`
2. Add explicit spatial type metadata alongside the base type:
   - subtype
   - dimensions
   - SRID
3. Make the type system and binary storage **dimension-aware from the first
   release**:
   - `XY`
   - `XYZ`
   - `XYM`
   - `XYZM`
4. Restrict the initial **query and index semantics** to the **XY projection**
   of those values.
5. Restrict the initial `GEOGRAPHY` implementation to **SRID 4326** only.
6. Store spatial values in rows and return them through the ABI as
   **normalized EWKB**:
   - little-endian
   - dimensionality flags preserved
   - SRID always present
   - all ordinates preserved
7. Reject implicit CRS transformations and generic ordered comparisons on
   spatial values.

The intended normalized internal shapes are:

```rust
enum ColumnType {
    Int64,
    Float64,
    Text,
    Bool,
    Blob,
    Decimal,
    Uuid,
    Timestamp,
    Geometry,
    Geography,
}

enum SpatialSubtype {
    Any,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

enum SpatialDimensions {
    Any,
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

struct SpatialTypeInfo {
    subtype: SpatialSubtype,
    dimensions: SpatialDimensions,
    srid: i32,
}
```

`ColumnSchema` carries `spatial_type: Option<SpatialTypeInfo>`.

Runtime values add:

```rust
Value::Geometry(Vec<u8>)
Value::Geography(Vec<u8>)
```

where the payload is normalized EWKB.

## Rationale

### 1. Geospatial Must Be Typed

DecentDB's roadmap and product direction call for first-class geospatial data,
not an extension-shaped "blob plus helper functions" story.

Adding explicit `GEOMETRY` / `GEOGRAPHY` types allows:

- type checking at DDL and DML boundaries
- subtype enforcement
- SRID enforcement
- planner recognition of spatial columns
- consistent behavior across Rust API, SQL, C ABI, and bindings

### 2. `GEOMETRY` and `GEOGRAPHY` Must Stay Separate

The distinction used by mature engines is valuable:

- `GEOMETRY`: planar semantics
- `GEOGRAPHY`: spherical lon/lat semantics

Combining these into one generic spatial blob type would make function
semantics, index strategy, and user expectations ambiguous.

### 3. EWKB Is the Right Canonical Binary Contract

Normalized EWKB is selected because it:

- is interoperable with existing geospatial tooling
- can encode SRID
- works naturally as a stable FFI/binding payload
- avoids inventing a bespoke external binary format

### 4. Dimensions Must Be Planned Now, Even If Semantics Stay XY Initially

Restricting the **query semantics** to XY still reduces first-release risk, but
the type system and storage should not be locked into a 2D-only contract.

Supporting dimension-aware storage now, while deferring true 3D / measure-aware
query semantics, avoids a future persistent-format and ABI redesign.

What still expands when true 3D semantics are implemented later:

- parser surface
- function semantics
- index semantics
- binding complexity

The initial release should therefore preserve `Z/M/ZM` correctly while defining
all planner-visible behavior on XY only.

### 5. No Implicit Transforms

Implicit reprojection would create correctness and predictability problems and
would require a CRS transformation stack that is out of scope for the first
release.

### 6. No Ordered Comparison Semantics

Spatial values do not have a meaningful generic total order. Rejecting generic
`=`, `<`, `>`, and related ordering comparisons avoids accidental misuse and
misleading B-Tree semantics.

## Alternatives Considered

### 1. Store Spatial Data as Untyped `BLOB`

Rejected.

This would make subtype/SRID enforcement impossible at the type-system level
and would prevent planner-native spatial support from fitting cleanly into the
existing engine architecture.

### 2. Store Spatial Data as WKT Text

Rejected.

This would inflate storage, slow parsing, complicate exact binary roundtrips,
and make FFI less efficient.

### 3. Invent a New Custom External Binary Format

Rejected.

DecentDB may still maintain internal helper representations during execution,
but the canonical persisted and ABI-visible representation should be an
established format.

### 4. Support Arbitrary `GEOGRAPHY` SRIDs Immediately

Rejected.

This would imply transformation or ambiguous semantics. The initial
implementation should keep `GEOGRAPHY` tightly scoped to WGS84.

### 5. Make the Core Contract 2D-Only and Add Dimensions Later

Rejected.

That would save some short-term implementation effort but would freeze the
wrong persistent and ABI boundary.

## Trade-offs

### Positive

- Clear and enforceable spatial type system
- Stable binary interchange format
- Strong fit with ABI and bindings
- Planner can reason about geospatial columns explicitly
- Lower first-release risk than a larger CRS/3D surface

### Negative

- No `ST_Transform` in the initial release
- No generic `CAST(text AS geometry)` convenience path
- No generic equality or ordering operators for spatial values
- Initial predicates, distances, KNN, and indexing operate on XY only
- True 3D or measure-aware semantics remain deferred
- Full topological canonicalization is not provided by storage normalization

## References

- `design/WIN03_GEOSPATIAL_DATA_SUPPORT.md`
- `design/DECENTDB_FUTURE_WINS_V2.md`
- `design/adr/0072-new-data-types-decimal-uuid.md`
- PostGIS geometry vs geography FAQ:
  `https://postgis.net/documentation/faq/geometry-or-geography/`
