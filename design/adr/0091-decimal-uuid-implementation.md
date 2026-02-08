# Implementation Details for DECIMAL and UUID

**Date:** 2026-02-07
**Status:** Accepted

## Context
ADR 0072 introduced `DECIMAL(p,s)` and `UUID` types. This ADR clarifies the implementation details regarding runtime enforcement and function support.

## Decisions

### 1. Runtime Type Enforcement
`typeCheckValue` in `src/engine.nim` currently performs loose checks. It will be updated to:
- **UUID**: Enforce `vkBlob` with length exactly 16 bytes. `vkText` will no longer be accepted for `ctUuid` columns (breaking change for any pre-existing UUID data stored as Text, though none is expected in production yet).
- **DECIMAL**: Enforce `vkDecimal`. Validate that `value.decimalScale` matches the column's defined scale. Validate that the unscaled integer fits within precision constraints.

### 2. Built-in Functions
The following functions will be implemented in `evalExpr` (`src/exec/exec.nim`):
- `GEN_RANDOM_UUID()`: Returns a 16-byte `vkBlob` (UUID v4). Uses `std/sysrand`.
- `UUID_PARSE(text)`: Parses canonical UUID string to `vkBlob`.
- `UUID_TO_STRING(uuid)`: Converts 16-byte `vkBlob` to canonical UUID string.
- `CAST(val AS type)`: Implemented for `DECIMAL` (rounding) and `UUID` (parsing).

### 3. Arithmetic
- `DECIMAL` arithmetic (`+`, `-`, `*`, `/`) will be implemented in `evalExpr` using `int64` scaled arithmetic with overflow checks.
- Division will round away from zero (PostgreSQL compatible).

### 4. Comparison
- `compareValues` will be updated to handle `vkDecimal` (comparing unscaled integers after normalizing scale if necessary, though operations should align scales).
- `vkBlob` comparison (used for UUID) already supports bytewise lexicographical comparison.

## Risks
- **Data Compatibility**: Existing `UUID` columns (if any) containing `Text` data will fail validation. This is acceptable as `UUID` is a new feature.
