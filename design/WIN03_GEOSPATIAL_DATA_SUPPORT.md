# DecentDB Geospatial Data Support Implementation SPEC
**Date:** 2026-03-28  
**Status:** Proposed  
**Audience:** Core engine developers, parser/planner/executor maintainers, storage/indexing implementers, C ABI maintainers, binding maintainers, CLI maintainers, documentation authors, coding agents  
**Related roadmap item:** `7. Native Geospatial Data Support`  
**Related vision documents:** `design/DECENTDB_FUTURE_WINS_V2.md`, `design/PRD.md`, `design/TESTING_STRATEGY.md`, `docs/design/spec.md`
**Related ADR drafts:** `design/adr/0124-geospatial-type-system-and-ewkb-storage.md`, `design/adr/0125-spatial-covering-cell-secondary-index.md`, `design/adr/0126-geospatial-c-abi-contract.md`, `design/adr/0127-planner-native-spatial-access-paths.md`, `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`

---

## 1. Executive Summary

This document defines the implementation plan for **first-class geospatial data
support** in DecentDB.

The goal is not to bolt on a legacy SQLite-style geospatial story. The goal is
to make spatial data a native, typed, planner-aware capability that fits
DecentDB's actual architecture:

- Rust-native engine
- single-process, one-writer / many-readers concurrency model
- B+Tree-centered storage engine
- WAL durability
- stable C ABI and multi-language bindings
- browser/WASM compatibility as a product requirement for future features

The core decisions in this SPEC are:

1. Add native SQL-visible `GEOMETRY` and `GEOGRAPHY` types.
2. Use a **dimension-aware normalized EWKB** contract as the canonical
   persisted and ABI interchange format for spatial values.
3. Implement a new `SPATIAL` secondary index kind.
4. Back the initial `SPATIAL` index with **hierarchical covering cells stored in
   the existing B+Tree infrastructure**, not with a standalone SQLite-style
   R-Tree subsystem.
5. Make spatial access **planner-native** via coarse candidate generation plus
   exact refinement.
6. Keep the implementation **pure Rust** in the engine. Do not depend on GEOS,
   PROJ, GDAL, or other large C/C++ native stacks in the initial slices.
7. Preserve `Z/M/ZM` dimensions from the first release, while defining initial
   query and index semantics on the **XY projection** only.
8. Deliver the feature in explicit slices, starting with the highest-value,
   easiest-to-validate workload: **`GEOGRAPHY(POINT,4326)` nearest-neighbor and
   radius search**.

This document is intentionally written to be implementation-ready. It specifies:

- SQL syntax
- catalog and type-system changes
- row/value encoding
- index physical layout
- planner/operator behavior
- ABI changes
- testing requirements
- phased delivery and acceptance criteria

It is expected that this SPEC will lead to one or more ADRs before coding
begins, because it crosses multiple ADR-required boundaries:

- new data types and persistent metadata
- new index kind and physical indexing strategy
- planner/executor semantics changes
- C ABI extension

---

## 2. Why This Needs a Dedicated Plan

`design/DECENTDB_FUTURE_WINS_V2.md` identifies geospatial support as a valuable
future win and sketches a direction that includes native types, spatial
indexing, and `ST_*` functions.

That document is the right product-level prompt, but geospatial work is not
"just add a few functions":

- it changes the SQL type system
- it introduces new persistent catalog metadata
- it introduces a new secondary index kind
- it changes the planner's access-path logic
- it affects the C ABI and every binding
- it adds non-trivial correctness and performance testing requirements
- it must remain WASM-compatible

This means geospatial support must be treated like a storage/planner feature,
not like a convenience library.

This also means the implementation must be deliberately shaped to DecentDB's
existing architecture, not copied blindly from another engine.

---

## 3. Design Inputs

This SPEC is derived from the following local project documents:

- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
- `docs/design/spec.md`
- `design/DECENTDB_FUTURE_WINS_V2.md`
- `design/adr/0072-new-data-types-decimal-uuid.md`
- `design/adr/0091-decimal-uuid-implementation.md`
- `design/adr/0088-expression-indexes-v0.md`
- `design/adr/0111-table-valued-functions.md`
- `design/adr/0112-cost-based-optimizer-with-stats.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `design/adr/0123-phase1-table-btree-foundation.md`
- `design/adr/0061-typed-index-key-encoding-text-blob.md`
- `design/adr/0124-geospatial-type-system-and-ewkb-storage.md`
- `design/adr/0125-spatial-covering-cell-secondary-index.md`
- `design/adr/0126-geospatial-c-abi-contract.md`
- `design/adr/0127-planner-native-spatial-access-paths.md`
- `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`

This SPEC is also informed by current external geospatial patterns:

- **PostGIS**
  - strongest SQL surface blueprint
  - important split between `geometry` and `geography`
  - index-assisted `ST_DWithin`
  - index-assisted KNN via `<->`
- **DuckDB**
  - evidence that spatial support should be planner-native, not opaque scalar
    functions only
  - dedicated spatial operators are the right long-term shape
- **CockroachDB**
  - strong example of using covering cells over a general-purpose index rather
    than an object-tree-only design
  - especially relevant for `GEOGRAPHY`
- **BigQuery**
  - evidence that exposing S2 cell helpers is valuable for modern geospatial
    workflows
- **SingleStore**
  - evidence that point workloads deserve a specialized fast path, even when a
    general geospatial type also exists

---

## 4. Product Thesis

The geospatial win for DecentDB is not:

> "DecentDB can store WKB blobs and has a few GIS helper functions."

The intended differentiator is:

> **DecentDB provides typed, indexable, planner-aware geospatial data support
> without requiring an external extension model.**

This should feel native in the same way that `UUID`, `DECIMAL`, JSON functions,
and generated columns are native.

---

## 5. Non-Negotiable Decisions

The following are fixed decisions for this implementation plan.

### 5.1 Pure Rust Only in Initial Delivery

The engine implementation must remain pure Rust in the initial delivery.

Do not introduce these as mandatory runtime dependencies:

- GEOS
- PROJ
- GDAL
- other native C/C++ geometry stacks

Reasons:

- ABI complexity
- portability burden
- WASM/browser incompatibility risk
- larger dependency and security surface

Pure Rust parsing, encoding, geometry math, and covering generation are
required for the initial slices.

### 5.2 Dimension-Aware Vector Foundation in Initial Delivery

The initial geospatial design must be **dimension-aware from day one** for
vector data.

Persisted and ABI-visible vector values may be:

- `XY`
- `XYZ`
- `XYM`
- `XYZM`

Initial semantic rules:

- `Z` is preserved on roundtrip
- `M` is preserved on roundtrip
- spatial indexes operate on **XY only**
- initial `ST_Distance`, `ST_DWithin`, `ST_Intersects`, `ST_Contains`,
  `ST_Within`, `ST_Equals`, and `<->` operate on **XY only**

Still not supported initially:

- curved types
- raster types
- true 3D predicate semantics
- measure-aware predicate semantics

### 5.3 Canonical Stored Value Format Is Normalized EWKB

The canonical stored and ABI-visible format for spatial values is **normalized
EWKB**:

- little-endian
- dimensionality flags preserved
- SRID always present in the stored payload
- type code must match the logical subtype
- all ordinates preserved in the payload

Reasons:

- interoperable with established geospatial tooling
- compact binary representation
- works well as an FFI/binding boundary
- avoids inventing a second public interchange format

### 5.4 No Implicit CRS Transformations

The engine must **not** silently transform between SRIDs.

Rules:

- `GEOGRAPHY` supports SRID `4326` only in initial delivery
- `GEOMETRY` binary predicates require matching SRIDs
- mismatched SRIDs produce a SQL error
- there is no `ST_Transform` in the initial release

### 5.5 Initial Spatial Index Strategy Is Covering-Cell Based

Although `design/DECENTDB_FUTURE_WINS_V2.md` mentions an R*-Tree, this SPEC
standardizes the **initial implementation** on a covering-cell spatial index
stored in the existing B+Tree subsystem.

This is an intentional change in implementation direction.

Rationale:

- fits DecentDB's accepted B+Tree-centered storage direction
- reuses the page cache and WAL path already central to the engine
- avoids inventing a second durable index page-layout family in the first slice
- aligns with the current "candidate set first, exact refinement later" pattern
  already present in the trigram subsystem
- supports both `GEOGRAPHY` and `GEOMETRY` under one index abstraction

This does **not** rule out a future dedicated R*-Tree if benchmarks later show
it is justified for specific workloads. It does mean that the first
implementation will not block on a new object-tree storage subsystem.

### 5.6 Planner Must Understand Spatial Access Paths

Spatial predicates must not be treated as opaque scalar functions only.

The planner must explicitly recognize supported query shapes and be able to
produce spatial candidate-generation plans that are refined exactly by the
executor.

### 5.7 Phased Delivery Is Mandatory

This feature must not be implemented as a single large change.

The initial slices are:

1. type-system and binary format foundation
2. `GEOGRAPHY(POINT,4326)` + spatial radius / KNN
3. planar `GEOMETRY` + polygon/line support
4. broader geography and planner/operator improvements

---

## 6. Goals

### 6.1 Primary Goals

1. Add native `GEOMETRY` and `GEOGRAPHY` column types.
2. Provide planner-usable spatial indexes for high-value queries.
3. Support performant point lookup workloads:
   - nearest neighbor
   - radius filtering
   - exact distance ordering
4. Support exact spatial predicates for supported shapes.
5. Preserve DecentDB's existing durability and single-writer safety model.
6. Keep the C ABI and all bindings aligned on one canonical spatial format.
7. Preserve compatibility with WASM/browser plans.

### 6.2 Secondary Goals

1. Make spatial behavior inspectable in `EXPLAIN`.
2. Provide GeoJSON/WKT/WKB input and output.
3. Allow later exposure of cell-covering helper functions.
4. Lay the groundwork for planner-native spatial joins.

---

## 7. Non-Goals

The initial implementation will not attempt to provide all of GIS.

### 7.1 Explicit Non-Goals for Initial Slices

1. Full PostGIS compatibility.
2. `ST_Transform` or full CRS reprojection support.
3. A full EPSG registry stored in-engine.
4. `GEOMETRYCOLLECTION` support in the initial release.
5. True 3D spatial predicate semantics.
6. Measure-aware predicate semantics.
7. Topology editing functions.
8. Raster support.
9. Arbitrary user-defined spatial reference systems with transformation logic.
10. Unique spatial indexes.
11. Multi-column spatial indexes.
12. Partial spatial indexes.
13. Expression spatial indexes.
14. Full planner-native spatial joins in the first shipping slice.

### 7.2 Deferred Areas

- `ST_Transform`
- `ST_Buffer`
  - until `ST_Buffer` lands, `ST_DWithin(geom, other, distance)` is the
    recommended substitute for common proximity-search workflows
- advanced polygon overlay functions
- true 3D semantics and 3D-aware indexing
- measure-aware spatial semantics
- `GEOGRAPHY` non-point KNN
- `GEOMETRYCOLLECTION`
- explicit geohash/H3 helper functions
- planner-native `SPATIAL_JOIN`
- spatial statistics in `ANALYZE`

---

## 8. Final Target Capability Shape

The intended end state for this workstream is:

- typed `GEOMETRY` and `GEOGRAPHY`
- indexable spatial columns through `USING SPATIAL`
- exact distance and predicate functions
- parameterized prepared statements that still use spatial indexes
- KNN ordering support for point columns
- WKB, WKT, and GeoJSON I/O
- stable ABI-visible binary value transport
- full docs and binding support

The initial shipping slices will deliver this incrementally rather than all at
once.

---

## 9. SQL Surface

## 9.1 Type Syntax

The parser and binder must support the following type forms.

### 9.1.1 `GEOMETRY`

Supported forms:

```sql
GEOMETRY
GEOMETRY(POINT)
GEOMETRY(POINTZ)
GEOMETRY(POINTM)
GEOMETRY(POINTZM)
GEOMETRY(LINESTRING)
GEOMETRY(LINESTRINGZ)
GEOMETRY(POLYGON)
GEOMETRY(POLYGONZM)
GEOMETRY(MULTIPOINT)
GEOMETRY(MULTILINESTRING)
GEOMETRY(MULTIPOLYGON)
GEOMETRY(POINT, 3857)
GEOMETRY(POINTZ, 3857)
GEOMETRY(POLYGON, 4326)
```

Rules:

- bare `GEOMETRY` means "any supported geometry subtype, any supported
  dimensions, SRID 0"
- `SRID 0` means "planar coordinates with unspecified CRS"
- `GEOMETRY` does not imply lon/lat or meters
- subtype tokens with `Z`, `M`, or `ZM` suffixes constrain the allowed
  dimensions

### 9.1.2 `GEOGRAPHY`

Supported forms:

```sql
GEOGRAPHY
GEOGRAPHY(POINT)
GEOGRAPHY(POINTZ)
GEOGRAPHY(POINTM)
GEOGRAPHY(POINTZM)
GEOGRAPHY(POLYGON)
GEOGRAPHY(MULTIPOLYGON)
GEOGRAPHY(POINT, 4326)
GEOGRAPHY(POINTZM, 4326)
GEOGRAPHY(POLYGON, 4326)
```

Rules:

- bare `GEOGRAPHY` means "any supported geography subtype, any supported
  dimensions, SRID 4326"
- `GEOGRAPHY(..., 4326)` is accepted
- any other SRID on `GEOGRAPHY` is rejected in the initial implementation
- coordinates are longitude/latitude in degrees

### 9.1.3 Supported Subtypes by Delivery Slice

#### Slice 1

- `GEOGRAPHY(POINT,4326)` only
- point dimensions `XY`, `XYZ`, `XYM`, and `XYZM` are preserved
- bare `GEOGRAPHY` allowed, but values inserted must currently be points

#### Slice 2

- `GEOMETRY(POINT|LINESTRING|POLYGON, srid)`
- those geometry subtypes may carry `XY`, `XYZ`, `XYM`, or `XYZM`
- bare `GEOMETRY` allowed for those supported subtypes

#### Slice 3

- `MULTI*` geometry/geography types
- geography polygons

### 9.1.4 Rejected Forms

These must fail with explicit SQL errors in initial delivery:

- `GEOMETRY(GEOMETRYCOLLECTION)`
- `GEOMETRY(POINTZZ)`
- `GEOGRAPHY(POINTM, 3857)`
- `GEOGRAPHY(POINT, 3857)`
- `GEOGRAPHY(LINESTRING, 0)`
- `GEOMETRY(POINT, '4326')`

---

## 9.2 Constructors, Parsers, and Serializers

The following SQL functions are required.

### 9.2.1 Constructors

```sql
ST_Point(x, y) -> GEOMETRY(POINT, 0)
ST_MakePoint(x, y) -> GEOMETRY(POINT, 0)  -- alias of ST_Point
ST_PointZ(x, y, z) -> GEOMETRY(POINTZ, 0)
ST_PointM(x, y, m) -> GEOMETRY(POINTM, 0)
ST_PointZM(x, y, z, m) -> GEOMETRY(POINTZM, 0)
ST_GeogPoint(lon, lat) -> GEOGRAPHY(POINT, 4326)
ST_GeogPointZ(lon, lat, z) -> GEOGRAPHY(POINTZ, 4326)
ST_GeogPointM(lon, lat, m) -> GEOGRAPHY(POINTM, 4326)
ST_GeogPointZM(lon, lat, z, m) -> GEOGRAPHY(POINTZM, 4326)
```

Rules:

- `ST_Point` is planar and returns `GEOMETRY`
- `ST_MakePoint` is a compatibility alias for `ST_Point`
- `ST_GeogPoint` is geographic and returns `GEOGRAPHY`
- `ST_*PointZ/M/ZM` preserve the supplied ordinates in the stored payload
- do not overload `ST_Point` contextually based on surrounding type inference

### 9.2.2 Binary/Text/JSON Input

```sql
ST_GeomFromWKB(blob[, srid])
ST_GeogFromWKB(blob)
ST_GeomFromText(text[, srid])
ST_GeogFromText(text)
ST_GeomFromGeoJSON(text[, srid])
ST_GeogFromGeoJSON(text)
```

Rules:

- WKB/EWKB input is parsed, normalized, and stored as normalized EWKB
- if WKB includes SRID and an explicit SQL SRID argument is also supplied, they
  must match
- for `ST_GeomFromText` and `ST_GeomFromGeoJSON`, when neither embedded
  EWKT-style SRID metadata nor an explicit SQL SRID argument is provided, the
  result SRID is `0`
- `ST_GeogFrom*` always produces SRID `4326`
- `ST_GeomFromText('POINT(1 2)', 4326)` produces a geometry with SRID `4326`
- GeoJSON input for `GEOGRAPHY` is interpreted as lon/lat
- WKB/WKT/GeoJSON input may preserve `Z`, `M`, and `ZM` ordinates when present

### 9.2.3 Output

```sql
ST_AsBinary(spatial) -> BLOB
ST_AsText(spatial) -> TEXT
ST_AsGeoJSON(spatial) -> TEXT
ST_SRID(spatial) -> INT64
ST_GeometryType(spatial) -> TEXT
```

Rules:

- `ST_AsBinary` returns normalized EWKB bytes
- `ST_AsText` returns canonical uppercase type names and decimal formatting
- `ST_AsGeoJSON` outputs RFC 7946-style geometry JSON and preserves optional
  extra ordinates when present

### 9.2.4 Coordinate Accessors

```sql
ST_X(point) -> FLOAT64
ST_Y(point) -> FLOAT64
ST_Z(point) -> FLOAT64
ST_M(point) -> FLOAT64
```

Rules:

- only valid for point inputs
- non-point input returns a SQL error
- `ST_Z` returns `NULL` when the point has no `Z`
- `ST_M` returns `NULL` when the point has no `M`

### 9.2.5 Metadata Helpers

```sql
ST_SetSRID(spatial, srid) -> spatial
```

Rules:

- `ST_SetSRID` changes metadata only; it does not transform coordinates
- for `GEOMETRY`, any non-negative SRID accepted by the type family is allowed
- for `GEOGRAPHY`, only `4326` is accepted in the initial release
- `ST_SetSRID` must revalidate that the resulting value is compatible with the
  target type family

---

## 9.3 Predicates and Distance Functions

The following functions are required.

### 9.3.1 Distance

```sql
ST_Distance(a, b) -> FLOAT64
ST_DWithin(a, b, distance) -> BOOL
```

Rules:

- `GEOMETRY` distance uses planar coordinate units of the SRID and operates on
  XY only in the initial release
- `GEOGRAPHY` distance uses meters
- `GEOGRAPHY` distance in initial delivery uses **spherical** distance, not
  ellipsoidal/vincenty distance
- `GEOGRAPHY` distance operates on lon/lat only and ignores preserved `Z/M`
  ordinates in the initial release
- Earth radius constant rationale: use `6_371_008.8` meters, the IUGG mean
  Earth radius, as the engine's stable spherical constant
- Earth radius constant for `GEOGRAPHY` distance:
  `6_371_008.8` meters

### 9.3.2 Topological Predicates

```sql
ST_Intersects(a, b) -> BOOL
ST_Contains(a, b) -> BOOL
ST_Within(a, b) -> BOOL
ST_Equals(a, b) -> BOOL
```

Rules:

- predicates return `NULL` when any input is `NULL`
- mismatched type families (`GEOMETRY` vs `GEOGRAPHY`) are rejected
- mismatched SRIDs are rejected
- predicates operate on the XY projection in the initial release
- `ST_Equals` uses **topological equality** for supported valid inputs, not
  structural vertex-order equality
- `ST_Within(a, b)` is the logical inverse predicate of `ST_Contains(b, a)`

### 9.3.3 KNN Operator

The following operator is required:

```sql
spatial_col <-> query_point
```

Rules:

- returns `FLOAT64`
- for `GEOMETRY`, units are the column SRID's planar units
- for `GEOGRAPHY`, units are meters
- preserved `Z/M` ordinates do not affect initial `<->` semantics
- planner may use an index only when:
  - the left or right operand is an indexed spatial column
  - the other operand is a literal or a parameter
  - the expression appears in `ORDER BY`
  - the query has `LIMIT`

Initial support:

- point columns only
- exact final ordering must be preserved

### 9.3.4 Measurement Helpers

```sql
ST_Length(spatial) -> FLOAT64
ST_Area(spatial) -> FLOAT64
```

Rules:

- `ST_Length` is initially supported for `GEOMETRY(LINESTRING, ...)` and
  operates on XY only
- `ST_Area` is initially supported for `GEOMETRY(POLYGON, ...)` and operates on
  XY only
- geography variants are deferred until the corresponding non-point geography
  slices land

### 9.3.5 Validation Helpers

```sql
ST_IsValid(spatial) -> BOOL
```

Rules:

- returns `TRUE` when the value satisfies the currently implemented validity
  rules for its supported subtype
- for point-only slices, valid point values always return `TRUE`
- once polygon support lands, `ST_IsValid` must reflect the polygon validity
  checks documented in Section 13

---

## 9.4 Index Syntax

The public SQL syntax for spatial indexing is:

```sql
CREATE INDEX idx_name ON table_name USING SPATIAL(column_name);
CREATE INDEX idx_name ON table_name USING SPATIAL(column_name)
  WITH (max_cells = 8, min_level = 4, max_level = 16);
```

For planar geometries with SRID `0` or custom bounds:

```sql
CREATE INDEX idx_geom ON parcels USING SPATIAL(shape)
  WITH (
    max_cells = 8,
    min_level = 4,
    max_level = 16,
    min_x = 0,
    min_y = 0,
    max_x = 100000,
    max_y = 100000
  );
```

### 9.4.1 Validation Rules

These rules are mandatory:

- spatial indexes are not `UNIQUE`
- spatial indexes allow exactly one plain column key
- no expression keys in initial release
- no `INCLUDE` columns
- no partial predicate
- indexed column must be `GEOMETRY` or `GEOGRAPHY`
- for `GEOMETRY` with SRID `0`, explicit bounds are required in the index
  `WITH (...)` options in the initial implementation
- there is no implicit default world or infinite planar bound for SRID `0`

### 9.4.2 Default Options

Defaults:

- `max_cells = 8`
- `min_level = 4`
- `max_level = 16`

These defaults apply to both geometry and geography initially.

They are intentionally conservative:

- bounded index size
- acceptable false-positive rate for first delivery
- simple, deterministic behavior

More aggressive tuning is explicitly deferred until benchmark-backed follow-up
work.

---

## 9.5 Index-Eligible Query Shapes

The planner must recognize the following query shapes as spatial-index eligible.

### 9.5.1 Filter Predicates

```sql
WHERE ST_DWithin(spatial_col, $1, $2)
WHERE ST_DWithin($1, spatial_col, $2)
WHERE ST_Intersects(spatial_col, $1)
WHERE ST_Intersects($1, spatial_col)
WHERE ST_Contains(spatial_col, $1)
WHERE ST_Within(spatial_col, $1)
WHERE ST_Contains($1, spatial_col)
WHERE ST_Within($1, spatial_col)
```

### 9.5.2 KNN

```sql
ORDER BY spatial_col <-> $1
LIMIT 10
```

### 9.5.3 Explicitly Not Eligible Initially

- arbitrary boolean compositions with multiple spatial columns in one predicate
- index usage hidden behind arbitrary UDF wrappers
- non-point KNN
- full spatial joins in initial slices

The executor may still evaluate those queries correctly by scan. They simply do
not get the spatial access path initially.

---

## 10. Internal Type System Changes

## 10.1 `ColumnType`

Extend `crates/decentdb/src/catalog/schema.rs`:

```rust
pub(crate) enum ColumnType {
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
```

Add string mappings:

- `GEOMETRY`
- `GEOGRAPHY`

### 10.1.1 Spatial Type Metadata

Do not encode spatial subtype and SRID in the `ColumnType` enum itself.

Add a dedicated metadata structure:

```rust
pub(crate) enum SpatialDimensions {
    Any,
    Xy,
    Xyz,
    Xym,
    Xyzm,
}

pub(crate) enum SpatialSubtype {
    Any,
    Point,
    LineString,
    Polygon,
    MultiPoint,
    MultiLineString,
    MultiPolygon,
}

pub(crate) struct SpatialTypeInfo {
    pub(crate) subtype: SpatialSubtype,
    pub(crate) dimensions: SpatialDimensions,
    pub(crate) srid: i32,
}
```

Then add to `ColumnSchema`:

```rust
pub(crate) spatial_type: Option<SpatialTypeInfo>,
```

Rules:

- `spatial_type` is `Some(...)` iff `column_type` is `Geometry` or `Geography`
- `Geometry` + `None` is invalid
- `Geography` + `None` is invalid
- subtype and dimensions are validated independently
- future index-bounds metadata for planar geometry, if added, belongs in a
  separate structure rather than being overloaded onto `SpatialTypeInfo`

This is intentionally explicit rather than implicit.

## 10.2 AST / Binder / Normalizer Changes

Current type parsing flattens types too aggressively for spatial work.

Add a normalized type modifier path that preserves:

- base type family
- spatial subtype
- spatial dimensions
- SRID

The normalized AST must retain enough information that DDL execution does not
re-parse type strings later.

Minimum required normalized representation:

```rust
pub(crate) enum TypeModifier {
    None,
    Spatial(SpatialTypeInfo),
}
```

Then `CreateTable` column definitions must carry:

- `column_type`
- `type_modifier`

Rules:

- `GEOMETRY` without explicit modifier normalizes to
  `Geometry + Spatial { subtype: Any, dimensions: Any, srid: 0 }`
- `GEOGRAPHY` without explicit modifier normalizes to
  `Geography + Spatial { subtype: Any, dimensions: Any, srid: 4326 }`
- `GEOGRAPHY(..., srid != 4326)` is rejected during normalization
- dimension suffixes in subtype tokens map directly to `SpatialDimensions`

---

## 11. Runtime Value Model

## 11.1 `Value`

Extend `crates/decentdb/src/record/value.rs`:

```rust
pub enum Value {
    Null,
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Text(String),
    Blob(Vec<u8>),
    Decimal { scaled: i64, scale: u8 },
    Uuid([u8; 16]),
    TimestampMicros(i64),
    Geometry(Vec<u8>),
    Geography(Vec<u8>),
}
```

Stored payload:

- normalized EWKB bytes

Rules:

- `Value::Geometry` and `Value::Geography` always store normalized EWKB
- the payload always includes SRID
- the payload preserves `XY`, `XYZ`, `XYM`, or `XYZM` exactly as normalized

## 11.2 Comparison Semantics

Spatial values must **not** participate in generic ordered comparisons.

Initial rules:

- `IS NULL` / `IS NOT NULL` work normally
- `=` / `<>` on spatial values are rejected with a SQL error
- `<`, `<=`, `>`, `>=` on spatial values are rejected
- use `ST_Equals` for spatial equality semantics

This avoids implying a false total ordering and avoids accidental B+Tree
ordering semantics on spatial blobs.

## 11.3 Cast Rules

Supported:

- spatial-to-BLOB via `ST_AsBinary`
- BLOB-to-spatial via `ST_GeomFromWKB` / `ST_GeogFromWKB`
- TEXT-to-spatial via `ST_GeomFromText` / `ST_GeogFromText`

Rejected initially:

- generic `CAST(blob AS GEOMETRY)`
- generic `CAST(text AS GEOGRAPHY)`

Use explicit `ST_*From*` constructors instead. This keeps error reporting and
SRID handling explicit.

---

## 12. Row Encoding and Persistent Format Changes

This work now has a dedicated ADR draft:

- `design/adr/0124-geospatial-type-system-and-ewkb-storage.md`

That ADR owns the persistent-format decision around type tags, value tags, and
normalized EWKB storage.

## 12.1 Row Value Tags

Extend the row encoding tags in `record/row.rs`.

Required new tags:

- `TAG_GEOMETRY = 8`
- `TAG_GEOGRAPHY = 9`

Payload format:

- varint length
- normalized EWKB bytes

## 12.2 Column Type Tags

Extend the catalog type tags in the catalog persistence layer.

Required new tags:

- `ColumnType::Geometry = 8`
- `ColumnType::Geography = 9`

## 12.3 Catalog Encoding

The persisted catalog representation for a column must include:

- base column type
- nullable/default/generated metadata as today
- if spatial:
  - subtype tag
  - dimensions tag
  - SRID

Do not rely on reparsing human-readable SQL type strings from the catalog for
this feature.

The catalog loader must deserialize this directly into `ColumnSchema`.

## 12.4 Normalized EWKB Rules

When a spatial value is inserted or updated:

1. parse input
2. validate structural correctness
3. enforce type family
4. enforce subtype constraint
5. enforce SRID constraint
6. normalize to little-endian EWKB with SRID always present
7. store the normalized bytes

Normalization does **not** attempt full topological canonicalization.

Specifically:

- preserve vertex order
- preserve ring order as supplied
- normalize only the binary envelope:
  - endian
  - explicit SRID presence
  - dimensionality flags
  - ordinate ordering

---

## 13. Structural Validation Rules

Validation requirements in initial delivery:

### 13.1 Common Rules

- no NaN coordinates
- no infinite coordinates
- subtype tag must be supported
- dimensionality flags must be one of `XY`, `XYZ`, `XYM`, `XYZM`
- payload must parse fully with no trailing garbage
- every stored ordinate present in the payload must be finite

### 13.2 `GEOGRAPHY`

- longitude must be in `[-180, 180]`
- latitude must be in `[-90, 90]`
- SRID must be `4326`
- any preserved `Z` or `M` ordinate must be finite

### 13.3 `GEOMETRY`

- coordinates must be finite
- SRID may be `0` or a positive integer

### 13.4 Shape Minimums

- `POINT`: exactly one coordinate pair
- `LINESTRING`: at least 2 coordinate pairs
- `POLYGON` outer ring: at least 4 coordinates and first == last
- polygon inner rings: same closure rule

### 13.5 Polygon Validity Floor

For the first slice that accepts polygons, the engine must reject obviously
invalid polygon topology at insert/update time.

Minimum required polygon validity checks:

- self-intersecting outer rings are rejected
- self-intersecting inner rings are rejected
- holes crossing the shell are rejected
- holes crossing each other are rejected

More advanced OGC validity checks may still be deferred, but supported polygon
storage must not silently accept self-intersections and then produce undefined
predicate results.

---

## 14. Spatial Index Design

## 14.1 Public Abstraction

Add a new index kind:

```rust
pub(crate) enum IndexKind {
    Btree,
    Trigram,
    Spatial,
}
```

Add runtime support:

```rust
pub(crate) enum RuntimeIndex {
    Btree { keys: RuntimeBtreeKeys },
    Trigram { index: TrigramIndex },
    Spatial { index: RuntimeSpatialIndex },
}
```

## 14.2 Why the Initial Index Is Not an R*-Tree

The first DecentDB spatial index should not start as a separate R*-Tree storage
subsystem.

Reasons:

1. DecentDB is explicitly B+Tree-centered.
2. Existing hot paths, page cache logic, and WAL assumptions already center on
   B+Tree pages.
3. The engine already has a good conceptual pattern for "coarse candidate set
   first, exact refine later" in the trigram subsystem.
4. A cell-covering index can be implemented on the current durable storage path
   without introducing a second index page format family.

This is a major implementation simplification and is the right first delivery
shape.

## 14.3 Physical Strategy by Type Family

### 14.3.1 `GEOGRAPHY`

Use a **spherical covering-cell strategy** implemented in
`src/spatial/s2/`.

This module is responsible for:

- converting lon/lat coordinates into hierarchical cell ids
- generating coverings for points and supported shapes
- computing neighboring rings for KNN expansion
- computing lower-bound distances from a query point to a cell

### 14.3.2 `GEOMETRY`

Use a **planar quadtree covering strategy** implemented in
`src/spatial/quadcell/`.

This module is responsible for:

- mapping planar coordinates into normalized index bounds
- generating quadtree coverings
- encoding cells into a locality-preserving 64-bit cell id
- computing cell-to-point minimum distances for KNN lower bounds

The geometry quadtree uses configured bounds rather than assuming a global CRS
extent.

## 14.4 Index Entry Layout

The spatial index is stored in the existing B+Tree subsystem as repeated keys.

Each spatial row may produce one or more index entries.

### 14.4.1 Key Layout

Each key is:

```text
[strategy_tag:u8][level:u8][cell_id_be:u64][rowid_sortable_u64]
```

Where:

- `strategy_tag`
  - `0 = geography_s2`
  - `1 = geometry_quadcell`
- `level`
  - covering depth
- `cell_id_be`
  - the strategy-native canonical 64-bit cell id serialized in big-endian form
    for lexicographic key ordering
- `rowid_sortable_u64`
  - signed rowid mapped into sortable bytes, matching existing rowid ordering

### 14.4.2 Value Layout

To reduce base-table fetches for point-heavy workloads, the value payload stores
cheap refinement metadata.

For point columns:

```text
[payload_tag:u8=1][x_or_lon:f64][y_or_lat:f64]
```

For non-point columns:

```text
[payload_tag:u8=2][min_x:f64][min_y:f64][max_x:f64][max_y:f64]
```

Rules:

- point payload is sufficient for exact point distance computation without
  touching the base table row
- non-point envelope payload enables an extra cheap envelope pre-filter before
  exact geometry evaluation
- geography non-point envelope payloads introduced in Slice 3 must be
  antimeridian-aware and must not assume a simple wrapped-longitude envelope

### 14.4.3 Why Rowid Is in the Key

Putting rowid in the key guarantees:

- uniqueness of per-cell entries
- deterministic scan ordering
- simple delete/update semantics

## 14.5 Covering Generation Rules

The covering algorithm is intentionally shared in structure between geography
and geometry.

### 14.5.1 Point Values

- points produce exactly one cell entry at `max_level`

### 14.5.2 Non-Point Values

Recursive algorithm:

1. Start at `min_level`
2. If object fully covers a cell, emit that cell
3. Else if current level == `max_level`, emit that cell
4. Else if emitted cell count would exceed `max_cells`, emit the current cell
5. Else subdivide touched children and continue

This yields:

- no false negatives
- bounded index size
- tunable false-positive rate

## 14.6 Insert / Update / Delete Behavior

### 14.6.1 Insert

On row insert:

1. parse and validate spatial value
2. generate covering cells
3. emit one B+Tree entry per cell

### 14.6.2 Delete

On row delete:

1. read previous spatial value
2. regenerate previous covering cells
3. delete those exact key entries

### 14.6.3 Update

On row update:

1. regenerate old covering
2. regenerate new covering
3. delete old entries
4. insert new entries

There is no pending/lazy rebuild path like trigram in the initial design.
Spatial index entries are updated transactionally alongside other B+Tree index
maintenance.

## 14.7 Candidate Query Path

For an index-eligible predicate:

1. compute the query covering
2. scan all matching cell prefixes in the spatial index
3. union candidate rowids
4. optionally apply envelope pre-filter from the index value payload
5. perform exact refinement
6. return only exact matches

The coarse phase may return false positives.
The coarse phase must never return false negatives.

### 14.7.1 Dimensional Semantics

The initial spatial index always projects values to XY before generating
coverings.

Rules:

- `Z` is not indexed
- `M` is not indexed
- point payloads stored in the index contain `x/y` only in the initial release
- future 3D-aware indexing is governed by
  `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`

## 14.8 Exact Refinement

Exact refinement always runs in the executor.

The exact refine path is function-specific:

- `ST_DWithin`
  - exact distance check
- `ST_Intersects`
  - exact intersection check
- `ST_Contains`
  - exact containment check
- `ST_Within`
  - exact containment inverse

No query may return a row based on covering overlap alone.

## 14.9 KNN Algorithm

Initial KNN support is **point columns only**.

Algorithm:

1. compute the query point
2. locate the point's containing cell at `max_level`
3. scan candidate rows in that cell
4. compute exact point distance
5. maintain a max-heap of the best `k`
6. expand to neighboring cells ring by ring
7. before scanning the next ring, compute a lower bound from the query point to
   that ring
8. stop when the lower bound is greater than or equal to the current farthest
   result in the heap

Rules:

- exact final ordering is always by exact distance
- the planner only uses this path when `LIMIT` is present
- preserved `Z/M` do not affect initial KNN ordering
- the `s2` and `quadcell` modules must expose an internal
  `min_distance_to_cell(query, cell)` helper so the lower-bound computation is a
  first-class engine concept rather than an ad hoc planner heuristic
- if the lower-bound logic is unavailable for a given subtype, fall back to
  bounded iterative expansion with an explicit executor cap and exact sort

---

## 15. Geometry Math and Predicate Engines

## 15.1 Module Layout

Add a new `crates/decentdb/src/spatial/` module family:

```text
spatial/
  mod.rs
  types.rs
  ewkb.rs
  wkt.rs
  geojson.rs
  distance.rs
  predicate.rs
  s2/
  quadcell/
  index.rs
```

## 15.2 Required Responsibilities

### `ewkb.rs`

- parse WKB/EWKB
- normalize to canonical EWKB
- extract subtype and SRID
- produce point/envelope helpers without full allocation where possible

### `distance.rs`

- exact point-to-point geography distance on sphere
- exact planar geometry distance for supported types
- lower-bound point-to-cell distances for KNN

### `predicate.rs`

- exact `Intersects`
- exact `Contains`
- exact `Within`
- exact `Equals`
- `Equals` must implement topological equality for supported valid inputs

### `s2/`

- cell id generation
- point cell lookup
- covering generation
- neighbor ring expansion

### `quadcell/`

- geometry bounds normalization
- cell id generation
- covering generation
- cell distance lower bounds

## 15.3 Geography Semantics

Initial geography semantics:

- Earth is modeled as a sphere
- distance units are meters
- lon/lat input is degrees
- point-to-point calculations use great-circle distance
- preserved `Z/M` are metadata only in the initial release

Ellipsoidal/vincenty math is explicitly deferred from the first release.

## 15.4 Geometry Semantics

Initial geometry semantics:

- purely planar
- units are whatever the SRID implies outside the engine
- the engine does not perform unit conversion
- preserved `Z/M` are ignored by initial predicates and index semantics

---

## 16. Planner and Executor Changes

## 16.1 New Physical Plan Variants

Add:

```rust
PhysicalPlan::SpatialFilter {
    table: String,
    index: String,
    predicate: Expr,
}

PhysicalPlan::SpatialKnn {
    table: String,
    index: String,
    order_expr: Expr,
    limit: usize,
}
```

`SpatialFilter` returns candidate row ids which are then refined exactly by the
executor before row emission.

`SpatialKnn` returns top-k row ids ordered by exact distance.

## 16.2 Predicate Recognition

Add a spatial predicate recognizer parallel to the existing simple indexable
filter logic.

The recognizer must:

- identify supported `ST_*` calls
- identify which operand is the indexed column
- allow literals and parameters as the query shape operand
- capture function kind and distance argument when present

Prepared statements must be supported.

This means the plan may be parameterized and the query covering may be computed
at execution time rather than at prepare time.

## 16.3 Parameterized Planning Rule

Spatial index usage must not be limited to literal-only queries.

If the predicate shape is structurally indexable and the query geometry/point is
a parameter, the planner must still choose a spatial access path and the
executor computes the covering from bound parameter values when the statement is
executed.

This is a required difference from weaker "constant only" implementations.

## 16.4 Coarse-Then-Exact Execution

Execution model:

1. compute query covering from literals or bound params
2. run spatial index prefix scans
3. dedupe row ids
4. for points, optionally use index payload to do exact distance/order without
   base row fetch
5. for other shapes, fetch row values and perform exact refine
6. emit rows that pass

## 16.5 Cost Heuristics

Initial cost rules:

- if a supported spatial predicate targets an indexed spatial column, prefer the
  spatial index when table row count is greater than `1000`
- if row count is unknown, prefer the spatial index
- for point KNN with `LIMIT`, always prefer the spatial index when available

Spatial indexes do not participate in `ANALYZE` statistics in the initial
release. That is deferred.

## 16.6 `EXPLAIN` Output

Add explain lines such as:

```text
SpatialFilter table=locations index=idx_locations_coords predicate=ST_DWithin(...)
SpatialKnn table=locations index=idx_locations_coords limit=10
```

For `EXPLAIN ANALYZE`, include:

- cells scanned
- candidate row ids
- exact checks performed
- final rows emitted

---

## 17. DDL and Catalog Validation Rules

Update `exec/ddl.rs` so spatial index validation mirrors the project's existing
style for trigram and expression indexes.

Required validation errors:

- `spatial indexes cannot be UNIQUE`
- `spatial indexes require a single plain spatial column`
- `partial spatial indexes are not supported`
- `spatial indexes do not support INCLUDE columns`
- `spatial indexes require GEOMETRY or GEOGRAPHY columns`
- `GEOGRAPHY supports only SRID 4326 in DecentDB 1.0`
- `GEOMETRY spatial indexes on SRID 0 require explicit min/max bounds`

---

## 18. C ABI and Binding Contract

## 18.1 Value Tags

Extend `include/decentdb.h`:

```c
DDB_VALUE_GEOMETRY = 9,
DDB_VALUE_GEOGRAPHY = 10
```

The payload uses the existing `data` / `len` fields in `ddb_value_t` and
`ddb_value_view_t`.

Payload contract:

- normalized EWKB bytes

Do not add a second ABI-visible geometry struct in the initial release.

## 18.2 New Bind Helpers

Add:

```c
ddb_stmt_bind_geometry_wkb(...)
ddb_stmt_bind_geography_wkb(...)
```

Arguments:

- statement
- 1-based bind index
- byte pointer
- byte length

Rules:

- the engine validates and normalizes the WKB/EWKB payload
- bindings should prefer these functions over generic blob binders

## 18.3 Result Access

`ddb_stmt_value_copy` and row-view APIs return:

- `DDB_VALUE_GEOMETRY` with normalized EWKB bytes in `data/len`
- `DDB_VALUE_GEOGRAPHY` with normalized EWKB bytes in `data/len`

## 18.4 Binding Guidance

All language bindings must treat WKB/EWKB as the stable native interchange
format.

Binding-specific helper layers may expose:

- point helper constructors
- GeoJSON conversion helpers
- WKT conversion helpers

But they must not invent a second native engine contract.

---

## 19. CLI and Documentation Surface

## 19.1 CLI

The CLI does not need a dedicated new import subsystem in the first slice, but
it must support and document workflows such as:

```sql
CREATE TABLE locations (
  id INT64 PRIMARY KEY,
  name TEXT,
  coords GEOGRAPHY(POINT, 4326)
);

CREATE INDEX idx_locations_coords ON locations USING SPATIAL(coords);

INSERT INTO locations VALUES
  (1, 'a', ST_GeogPoint(-122.4194, 37.7749));

SELECT name
FROM locations
WHERE ST_DWithin(coords, ST_GeogPoint(-122.4194, 37.7749), 5000)
ORDER BY coords <-> ST_GeogPoint(-122.4194, 37.7749)
LIMIT 10;
```

## 19.2 User-Facing Docs Required

This feature is not done without:

- data type reference
- function reference
- spatial index reference
- tutorial for nearest-neighbor search
- tutorial for radius filtering
- tutorial for point-in-polygon once polygons ship
- binding examples in Python, .NET, and Node
- limitations page

---

## 20. Testing Requirements

Geospatial support is subject to the same correctness-first standards as the
rest of the engine.

## 20.1 Unit Tests

Required unit test categories:

- type parsing and normalization
- column schema encode/decode with subtype + dimensions + SRID
- EWKB normalization
- coordinate range validation
- WKT / GeoJSON roundtrip
- dimension-preserving roundtrip for `XYZ`, `XYM`, and `XYZM`
- point distance correctness
- geometry predicate correctness for supported shapes
- cell covering generation
- cell key encoding ordering
- spatial index entry generation on insert/update/delete

## 20.2 Property Tests

Required property/invariant tests:

- `spatial index result == exact scan result`
- covering generation yields no false negatives
- KNN exact top-k matches brute force for random point datasets
- repeated normalize(parse(bytes)) is idempotent
- update old/new covering delete+insert is equivalent to rebuild

## 20.3 Differential Tests

Nightly differential validation is required against PostgreSQL + PostGIS for
the supported subset.

Initial comparison set:

- point WKT/WKB roundtrip across `XY`, `XYZ`, `XYM`, and `XYZM`
- point-to-point `ST_Distance`
- `ST_DWithin`
- planar `ST_Intersects`, `ST_Contains`, `ST_Within` once geometry slices land

When geography polygons land, add explicit antimeridian and hemisphere cases.

## 20.4 Crash and Recovery Tests

Crash tests must cover:

- insert into spatially indexed table
- update spatially indexed row
- delete spatially indexed row
- bulk index build + crash before commit
- reopen and verify:
  - no corruption
  - spatial index rows match table rows
  - query results equal scan results

## 20.5 Binding Tests

Bindings must test:

- bind WKB in
- read WKB out
- roundtrip WKB with `XYZ`, `XYM`, and `XYZM`
- nearest-neighbor query
- radius query
- error propagation for invalid SRID / invalid geometry

## 20.6 WASM Compatibility Tests

Because geospatial is expected to work with future browser support, add:

- `wasm32-unknown-unknown` compile smoke
- no native-library linkage in the spatial module path

## 20.7 Benchmark Requirements

Benchmark datasets must include:

1. `1M` geographic points in WGS84
2. `100k` planar polygons
3. mixed update workload with point movement

Dataset sourcing guidance:

- use synthetic generators for controlled scale and edge-case coverage
- add at least one realism-oriented dataset derived from public geospatial data
  such as OpenStreetMap extracts or TIGER/Line shapes

Required benchmark shapes:

- KNN `LIMIT 10`
- `ST_DWithin` radius query
- point-in-polygon
- index build time
- update/delete maintenance time

Non-spatial regression requirement:

- adding geospatial support must not materially regress existing non-spatial
  benchmarks

---

## 21. Phased Delivery Plan

## 21.1 Slice 0: ADRs and Foundations

Deliverables:

- ADR: geospatial type system and normalized EWKB storage
- dimension model (`XY` / `XYZ` / `XYM` / `XYZM`) settled
- ADR: spatial covering-cell index design
- ADR: C ABI geospatial contract
- scaffolding modules under `src/spatial/`

No public SQL feature is complete in this slice.

Exit criteria:

- ADRs accepted
- parser/type metadata shape settled
- value and catalog tag assignments frozen

## 21.2 Slice 1: `GEOGRAPHY(POINT,4326)` Core

Scope:

- `GEOGRAPHY` / `GEOGRAPHY(POINT,4326)` type support
- `GEOGRAPHY(POINTZ|POINTM|POINTZM,4326)` type support and roundtrip
- `ST_GeogPoint`
- `ST_GeogPointZ`, `ST_GeogPointM`, `ST_GeogPointZM`
- `ST_IsValid` for supported geography point values
- `ST_SetSRID` with `4326`-only geography validation semantics
- `ST_AsBinary`, `ST_AsText`, `ST_AsGeoJSON`
- `ST_GeogFromWKB`, `ST_GeogFromText`, `ST_GeogFromGeoJSON`
- `ST_X`, `ST_Y`, `ST_Z`, `ST_M`, `ST_SRID`, `ST_GeometryType`
- exact spherical point-to-point `ST_Distance`
- exact spherical point-to-point `ST_DWithin`
- `USING SPATIAL` for geography point columns
- KNN `<->` on geography point columns
- ABI tag and bind helper support

Semantic constraint:

- all Slice 1 predicates and index behavior operate on lon/lat XY only

Explicitly out of scope in Slice 1:

- geography polygons
- geometry lines/polygons
- `ST_Contains`
- `ST_Within`
- `ST_Intersects`

Exit criteria:

- all point radius queries use the spatial index when eligible
- KNN point queries return exact top-k
- WKB/WKT/GeoJSON roundtrip tests pass

## 21.3 Slice 2: Planar `GEOMETRY`

Scope:

- `GEOMETRY(POINT|LINESTRING|POLYGON, srid)`
- dimension-aware variants of those subtypes (`Z`, `M`, `ZM`) at the type and
  storage layer
- planar quadtree covering index
- `ST_Point`
- `ST_MakePoint`
- `ST_PointZ`, `ST_PointM`, `ST_PointZM`
- `ST_SetSRID`
- `ST_GeomFromWKB`, `ST_GeomFromText`, `ST_GeomFromGeoJSON`
- `ST_Distance`
- `ST_DWithin`
- `ST_Intersects`
- `ST_Contains`
- `ST_Within`
- `ST_Equals`
- `ST_IsValid`
- `ST_Length`
- `ST_Area`
- point KNN for geometry point columns

Explicitly out of scope in Slice 2:

- multipolygon / multilinestring
- geography polygons
- spatial joins

Exit criteria:

- geometry scan vs index equivalence tests pass
- differential tests against PostGIS supported subset pass

## 21.4 Slice 3: Multi-Geometry and Geography Polygons

Scope:

- `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`
- geography polygons
- point-in-polygon on geography
- `ST_Area` for supported geography polygons
- `ST_IsValid` for supported geography polygon values
- antimeridian-safe coverings for geography polygons

Exit criteria:

- no false negatives across antimeridian tests
- index-assisted point-in-polygon works for supported query shapes

## 21.5 Slice 4: Planner/Operator Improvements

Scope:

- broader boolean predicate recognition
- better `EXPLAIN ANALYZE` reporting
- optional early spatial join prototype
- optional covering helper functions for advanced users

Possible helper functions:

```sql
ST_S2CellIdFromPoint(geography_point, level)
ST_S2Covering(geography, max_cells, max_level)
```

These are deferred and not required for the first shipping geospatial release.

## 21.6 Slice 5: True 3D and Measure-Aware Extensions

This slice requires acceptance of:

- `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`

Scope:

- explicit true 3D function family or other explicit 3D SQL surface
- explicit 3D-aware spatial index mode metadata
- benchmark-backed 3D index strategy selection
- expanded XYZ differential and crash/recovery validation
- explicit measure-aware helper functions if the project decides to add them

Hard constraint:

- existing XY semantics for the initial `ST_*` family and initial `USING
  SPATIAL` behavior remain backward compatible

---

## 22. File and Module Work Breakdown

Expected primary implementation files:

- `crates/decentdb/src/catalog/schema.rs`
- `crates/decentdb/src/sql/ast.rs`
- `crates/decentdb/src/sql/normalize.rs`
- `crates/decentdb/src/record/value.rs`
- `crates/decentdb/src/record/row.rs`
- `crates/decentdb/src/exec/ddl.rs`
- `crates/decentdb/src/planner/mod.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/src/spatial/`
- `include/decentdb.h`
- binding test suites
- user docs

Important implementation note:

The initial spatial index design intentionally avoids introducing a brand-new
page type. It reuses the existing B+Tree machinery for durable index storage.

That is the central implementation simplification in this plan.

---

## 23. Acceptance Criteria

Geospatial support is considered done for a given slice only when:

1. `cargo clippy` passes without warnings.
2. New parser/type/catalog tests cover subtype and SRID handling.
3. Spatial index query results equal exact scan results for supported query
   shapes.
4. Crash/recovery tests pass for spatially indexed tables.
5. Binding smoke tests pass for the affected surfaces.
6. Docs and examples are updated.
7. Non-spatial regressions are checked.
8. WASM compile smoke remains green.

---

## 24. Risks

## 24.1 Complexity Creep Risk

GIS scope expands easily. The phased plan must be enforced.

Mitigation:

- slice gates
- explicit non-goals
- start with points

## 24.2 Geometry Correctness Risk

Exact predicates are easy to get subtly wrong.

Mitigation:

- structural validation
- differential tests vs PostGIS
- scan-vs-index equivalence property tests

## 24.3 Index Size / False Positive Tradeoff

A covering-cell index can become too loose or too large.

Mitigation:

- fixed conservative defaults
- exposed tuning knobs
- benchmark before changing defaults

## 24.4 ABI Drift Risk

Bindings can drift if they invent their own native representations.

Mitigation:

- normalized EWKB as the only stable ABI representation
- helper APIs at bindings, not alternate engine contracts

---

## 25. Related ADRs

This SPEC is paired with the following ADR drafts:

1. `design/adr/0124-geospatial-type-system-and-ewkb-storage.md`
2. `design/adr/0125-spatial-covering-cell-secondary-index.md`
3. `design/adr/0126-geospatial-c-abi-contract.md`
4. `design/adr/0127-planner-native-spatial-access-paths.md`
5. `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`

If the implementation later adds spatial statistics or a dedicated
`SPATIAL_JOIN` operator, add additional ADRs rather than silently expanding
these.

---

## 26. References

### Local

- `design/DECENTDB_FUTURE_WINS_V2.md`
- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
- `docs/design/spec.md`
- `design/adr/0072-new-data-types-decimal-uuid.md`
- `design/adr/0091-decimal-uuid-implementation.md`
- `design/adr/0112-cost-based-optimizer-with-stats.md`
- `design/adr/0120-core-storage-engine-btree.md`
- `design/adr/0124-geospatial-type-system-and-ewkb-storage.md`
- `design/adr/0125-spatial-covering-cell-secondary-index.md`
- `design/adr/0126-geospatial-c-abi-contract.md`
- `design/adr/0127-planner-native-spatial-access-paths.md`
- `design/adr/0128-true-3d-semantics-and-3d-aware-indexing.md`

### External Blueprints

- PostGIS geometry vs geography FAQ  
  `https://postgis.net/documentation/faq/geometry-or-geography/`
- PostGIS KNN `<->` operator  
  `https://postgis.net/docs/geometry_distance_knn.html`
- DuckDB spatial docs and release notes  
  `https://duckdb.org/docs/stable/core_extensions/spatial/overview`  
  `https://duckdb.org/docs/stable/core_extensions/spatial/r-tree_indexes`  
  `https://duckdb.org/2025/05/21/announcing-duckdb-130.html`
- CockroachDB spatial indexes  
  `https://www.cockroachlabs.com/docs/stable/spatial-indexes`
- BigQuery geography functions  
  `https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions`
- SingleStore geospatial types  
  `https://docs.singlestore.com/cloud/reference/sql-reference/data-types/geospatial-types/`
