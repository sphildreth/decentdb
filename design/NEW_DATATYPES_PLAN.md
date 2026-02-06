# New Data Types Plan: DECIMAL + UUID v4

**Date:** 2026-02-06
**Status:** Draft

## Goals

- Add a **DECIMAL** type suitable for currency (exact arithmetic; avoid `FLOAT64` rounding).
- Add a **UUID v4** type with:
  - Storage as 16 bytes (preferred) rather than text
  - `gen_random_uuid()` builtin
  - Parse from canonical UUID string
  - Format/convert UUID to string

## Non-goals (initial slice)

- No multi-process concurrency or protocol changes.
- No new external dependencies.
- No generalized arbitrary-precision `NUMERIC` arithmetic in v1 unless it fits cleanly (see Decisions).

## Current Constraints / Observations

- Engine values are encoded via `ValueKind` in [src/record/record.nim](../src/record/record.nim); there is no existing decimal/uuid kind.
- Column schema types are currently the `ColumnType` enum in [src/catalog/catalog.nim](../src/catalog/catalog.nim) (`ctInt64/ctBool/ctFloat64/ctText/ctBlob`).
- `parseColumnType()` currently strips anything after `(`; type parameters like `DECIMAL(10,2)` are not preserved.
- Index keys for B-tree indexes are currently **uint64 keys** (hash-like for TEXT/BLOB) in [src/storage/storage.nim](../src/storage/storage.nim); they are used primarily for equality seeks + collision verification (not strict total ordering by value).

## Proposed User-Facing SQL Surface

### UUID

- Column type: `UUID`
- Primary function:
  - `gen_random_uuid() -> UUID` (UUID v4)
- Conversions:
  - `uuid_to_string(uuid) -> TEXT`
  - `uuid_parse(text) -> UUID` (or accept `CAST(text AS UUID)`)

**Literal story (v1):** UUID values are provided as TEXT and converted via `CAST('...uuid...' AS UUID)` (or `uuid_parse('...')`).

### DECIMAL

- Column type: `DECIMAL(p,s)` / `NUMERIC(p,s)` (fixed-point)
- Conversions:
  - `CAST(text AS DECIMAL(p,s))` parses and rounds to scale `s`
  - (Optional) `decimal_to_string(decimal) -> TEXT`

**Literal story (v1):** for exactness, decimal values should be passed as TEXT and parsed (`CAST('12.34' AS DECIMAL(10,2))`), rather than relying on float literals.

**Implementation constraint (v1):** DECIMAL precision will be capped to what fits in a signed 64-bit unscaled integer; enforce this at type definition and casts.

## Storage & Internal Representation

### UUID

- Store UUID values as **16 bytes**.
- Internal `Value` representation uses existing `vkBlob` with `bytes.len == 16`.
- Add schema recognition for `UUID` so type checking can enforce length 16 at boundaries.

### DECIMAL

- Store DECIMAL values as an **unscaled integer** plus a **scale**.
- Internal representation: `Value(kind: vkDecimal, int64Val = unscaled, decimalScale = s)`.

## Engine Semantics

### Type Checking

- Extend `parseColumnType()` to recognize `UUID` and `DECIMAL`/`NUMERIC`.
- Extend value boundary checks in `typeCheckValue()` (see [src/engine.nim](../src/engine.nim)):
  - For `UUID`: accept `vkBlob` only, and enforce `bytes.len == 16`.
  - For `DECIMAL(p,s)`: accept `vkDecimal` and enforce matching scale; enforce precision cap.

### Comparison & Equality

- UUID comparisons use bytewise comparison (lexicographic over 16 bytes) for `ORDER BY` stability.
- DECIMAL comparisons use integer comparison of unscaled values (same scale), avoiding float rounding.

### Arithmetic (DECIMAL)

- Provide `+ - * /` for DECIMAL with exact semantics.
- `+/-`: exact (scale = max(s1,s2))
- `*`: exact (scale = s1+s2; validate precision cap)
- `/`: destination scale (proposed: max(s1,s2)) with PostgreSQL-style tie-breaking (round away from zero)
- Mixed-type arithmetic:
  - Prefer rejecting `DECIMAL <op> FLOAT64` unless explicitly cast, to avoid silently reintroducing floating error.

## Built-in Functions

### gen_random_uuid()

- Uses a cryptographically-strong RNG from the OS (prefer Nim stdlib `std/sysrand` if available; otherwise `/dev/urandom`).
- Sets UUID v4 bits (version=4, variant=RFC4122).

### Parsing / Formatting

- `uuid_parse(text)` accepts canonical forms:
  - `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (lower/upper hex accepted)
  - (optional) allow braces `{}`.
- `uuid_to_string(uuid)` returns lowercase canonical form with hyphens.

## Catalog / Persistence Considerations

- Catalog currently persists column types as text via `encodeColumns()` and `columnTypeToText()`.
- Adding new base types (UUID/DECIMAL) changes the persisted schema strings for new databases.
- Catalog must preserve type parameters (e.g. `DECIMAL(10,2)`) rather than collapsing to `DECIMAL`.

Per repo policy, any change that affects persisted schema representation or SQL typing requires an ADR before implementation.

## Testing Plan

- Unit tests in `tests/nim/`:
  - UUID:
    - Roundtrip parse/stringify
    - `gen_random_uuid()` returns 16 bytes; version/variant bits correct
    - Reject invalid strings
  - DECIMAL:
    - Parse from string (including negatives; leading/trailing zeros)
    - Arithmetic and comparisons
    - Reject invalid scale/format (depending on ADR decision)
  - Integration:
    - Create table with UUID/DECIMAL, insert/select, ordering stability

If changes touch durability-critical paths (record encoding, WAL, page layout), add crash-injection coverage; the current proposal aims to avoid record-format changes by reusing existing `vkInt64` and `vkBlob`.

Note: DECIMAL support may require a new value encoding kind (or equivalent) to avoid loss of scale.

## Milestones

1. ADR: choose DECIMAL semantics + UUID representation and SQL surface.
2. Implement UUID type parsing + `gen_random_uuid()` + parse/format helpers.
3. Implement DECIMAL type semantics (Option A or B) + conversions.
4. Update CLI rendering (optional) and binding expectations (follow-up).
5. Update documentation (SPEC/PRD/docs/user-guide) to reflect new types/functions and any format-version compatibility notes.

## Decisions (v1)

- DECIMAL scope: support `DECIMAL(p,s)` / `NUMERIC(p,s)` (parameterized fixed-point).
- DECIMAL rounding (casts + division quantization): match PostgreSQL `numeric` (`round(...)`) tie-breaking: ties round **away from zero**.
- UUID text conversion: support `CAST(uuid AS TEXT)` as an alias of `uuid_to_string(uuid)`.
- DECIMAL inputs: do not accept float literals implicitly (require TEXT input + `CAST(... AS DECIMAL(p,s))`), to avoid reintroducing floating-point error.
