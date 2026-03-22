## Add DECIMAL(p,s) and UUID v4 types
**Date:** 2026-02-06
**Status:** Accepted

### Decision

Introduce two new SQL-visible data types:

1. **UUID**
   - SQL type name: `UUID`
   - Storage: `BLOB(16)` internally (`Value(kind: vkBlob, bytes.len == 16)`)
   - Builtins:
     - `gen_random_uuid() -> UUID` generates RFC4122 UUID v4
     - `uuid_parse(text) -> UUID`
     - `uuid_to_string(uuid) -> TEXT`

2. **DECIMAL(p,s)** (primarily for currency and rates)
   - SQL type names: `DECIMAL(p,s)`, `NUMERIC(p,s)`
   - Storage: scaled integer with explicit runtime scale (`Value(kind: vkDecimal, int64Val = unscaled, decimalScale = s)`)
   - Cast/parse: `CAST(text AS DECIMAL(p,s))` parses a decimal string with rounding to scale `s`
   - Arithmetic: `+ - * /` supported for DECIMAL values; division uses half-up rounding to the destination scale

Implementation constraint (v1): DECIMAL precision is limited to what fits in signed 64-bit unscaled integers (precision cap enforced at type-definition and casts). This can be extended later by widening the storage.

### Rationale

- Currency requires exact arithmetic; binary floating point (`FLOAT64`) is unsuitable.
- UUIDs are commonly used identifiers; storing them as 16 bytes is smaller and faster than TEXT.
- UUID as 16 bytes is compact and fast.
- DECIMAL(p,s) provides exact arithmetic and avoids floating point error.

### Alternatives Considered

- Store UUID as TEXT only: simpler but larger storage and slower comparisons.
- Implement arbitrary-precision NUMERIC using big integers: more general but heavier and potentially slower.
- Store DECIMAL as canonical TEXT: exact, but arithmetic/ordering becomes costly and subtle.

### Trade-offs

- DECIMAL(p,s) support requires retaining type modifiers in the SQL AST and catalog metadata.
- Using an `INT64` unscaled representation caps precision; widening to `INT128`/BigInt is a follow-up.
- UUID parse/format functions add some surface area but are straightforward and self-contained.

### References

- Catalog type parsing: `parseColumnType()` in src/catalog/catalog.nim
- Record encoding: src/record/record.nim
- Error handling strategy: design/adr/0010-error-handling-strategy.md
- Snapshot isolation constraints: design/adr/0023-isolation-level-specification.md
