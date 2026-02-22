# ADR-0099: SQLite-Compatible Type Affinity in Comparisons

**Status:** Accepted  
**Date:** 2025-07-22  
**Context:** SQLite compatibility — comparisons between INTEGER and TEXT values must coerce types following SQLite's type affinity rules.

## Problem

DecentDB's `compareValues` performs strict type matching: if operands have different `ValueKind`s, it returns `cmp(a.kind, b.kind)` without attempting conversion. This means `42 = '42'` evaluates to `false`.

SQLite applies **type affinity** rules (§3.2 of the SQLite documentation): when comparing an INTEGER/REAL value with a TEXT value, SQLite attempts to convert the TEXT to a numeric value. If conversion succeeds, it compares numerically. If conversion fails, the numeric value is always considered less than the TEXT value.

This affects all users, not just EF Core. Any SQL migrated from SQLite (or written assuming SQLite-compatible behavior) that compares numeric columns to string literals or parameters will silently return wrong results.

Common patterns affected:
- `WHERE id = '42'` (string literal vs INTEGER column)
- `WHERE id = @p0` where parameter is bound as TEXT
- EF Core `EF.Property<string>(entity, "Id") == "42"` pattern
- Any ORM that passes all parameters as strings

## Decision

Add SQLite-compatible type affinity coercion to `compareValues` in `src/exec/exec.nim`. The rules are:

1. **INTEGER vs TEXT**: Try to parse the TEXT as an integer. If successful, compare as integers. Otherwise, INTEGER < TEXT.
2. **FLOAT64 vs TEXT**: Try to parse the TEXT as a float. If successful, compare as floats. Otherwise, FLOAT64 < TEXT.
3. **INTEGER vs FLOAT64**: Convert the integer to float and compare. (Already partially handled by existing code paths but made explicit.)
4. **BLOB vs any non-BLOB**: BLOB is always greater than INTEGER, FLOAT, and TEXT (SQLite rule).
5. **NULL**: NULL handling is unchanged (NULL is not equal to anything, handled by existing COALESCE/IS NULL logic).

The coercion is applied symmetrically (a vs b and b vs a).

## Performance Impact

- The fast path (same-kind comparison) is unchanged — the `if a.kind != b.kind` check still exits early when kinds match.
- The coercion path only triggers for cross-type comparisons, which are uncommon in well-typed schemas.
- Text-to-integer parsing uses `parseBiggestInt` which is a single pass over the string — negligible cost.

## Consequences

- `42 = '42'` now returns `true` (matches SQLite behavior)
- `42 < 'abc'` now returns `true` (numeric < non-numeric text, matches SQLite)
- `42.0 = '42.0'` now returns `true`
- Ordering of mixed-type columns now follows SQLite's collation: NULL < INTEGER/REAL < TEXT < BLOB
- No persistent format changes — this only affects in-memory evaluation
- No changes to indexing or storage — indexes store typed values as before

## Risks

- Applications that relied on strict type comparison (INTEGER ≠ TEXT) may see different results. This is considered acceptable because the previous behavior was a compatibility bug, not a feature.
