# ADR-0126: Geospatial C ABI Contract
**Date:** 2026-03-28
**Status:** Proposed

## Decision

Extend the stable C ABI to carry geospatial values explicitly while keeping
normalized EWKB as the only native interchange payload.

The required ABI decisions are:

1. Add new value tags:
   - `DDB_VALUE_GEOMETRY`
   - `DDB_VALUE_GEOGRAPHY`
2. Reuse the existing `data` / `len` fields of `ddb_value_t` and
   `ddb_value_view_t` for spatial values.
3. Define the payload as normalized EWKB bytes that may encode:
   - `XY`
   - `XYZ`
   - `XYM`
   - `XYZM`
4. Add explicit bind helpers:
   - `ddb_stmt_bind_geometry_wkb`
   - `ddb_stmt_bind_geography_wkb`
5. Return spatial results through existing row-view and value-copy APIs using
   the new tags plus normalized EWKB bytes.
6. Do **not** introduce a second ABI-visible geometry struct family in the
   first release.

## Rationale

### 1. Bindings Need Stable Native Type Identification

If geospatial values were returned as generic `BLOB`, bindings could not
distinguish:

- user BLOB data
- engine-native `GEOMETRY`
- engine-native `GEOGRAPHY`

New tags make the contract explicit and keep binding behavior aligned with the
SQL type system.

### 2. Existing ABI Shapes Already Carry Variable-Length Payloads

The current ABI already supports variable-length bytes through `data` / `len`.
Spatial values fit this model naturally.

### 3. EWKB Is Better Than WKT or GeoJSON at the ABI Layer

Text formats are valuable for user-facing SQL and documentation, but not as the
primary ABI contract:

- larger payloads
- more parse cost
- less exactness for roundtrips

Binary WKB/EWKB is the correct native boundary.

The ABI should preserve dimensions from the first release so bindings do not
need a future breaking change when `Z/M/ZM` values become common.

### 4. Avoid ABI Proliferation

Adding point structs, polygon structs, ring structs, and alternate native
layouts would make the C ABI unnecessarily large and fragile.

Bindings can always layer convenience helpers on top of the WKB contract.

## Alternatives Considered

### 1. Expose Geospatial Data as Generic `BLOB`

Rejected.

This would hide type identity and encourage binding drift.

### 2. Use WKT as the Native ABI Contract

Rejected.

WKT is useful for SQL constructors and docs, not as the canonical low-level FFI
shape.

### 3. Introduce Specialized ABI Structs for Points and Shapes

Rejected for the initial release.

This would multiply the ABI surface and force bindings into parallel contracts.

### 4. Use GeoJSON as the ABI Contract

Rejected.

GeoJSON is useful at the application layer but is too bulky and semantically
lossy for the engine boundary.

## Trade-offs

### Positive

- Minimal ABI churn
- Strong alignment with SQL types
- Efficient binary interchange
- Easier multi-language binding consistency
- Dimension-aware from day one without expanding the struct ABI

### Negative

- Bindings that want rich geometry objects must parse WKB/EWKB
- WKT/GeoJSON convenience remains a higher-layer concern
- The ABI does not provide subtype/SRID fields outside the EWKB payload

## References

- `design/WIN03_GEOSPATIAL_DATA_SUPPORT.md`
- `include/decentdb.h`
- `design/adr/0118-rust-ffi-panic-safety.md`
