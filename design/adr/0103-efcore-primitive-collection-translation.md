# ADR 0103: EF Core Primitive Collection Query Translation

## Status

Accepted

## Context

EF Core 8+ supports primitive collections — properties like `string[]` stored as JSON arrays in a single column. The default translation pipeline creates `json_each()` table-valued function (TVF) calls to flatten arrays into rows for LINQ operations (`.Any()`, `.Count()`, `.Contains()`, `array[index]`).

DecentDB does not support table-valued functions. Implementing them would require significant planner and executor changes affecting all users.

## Decision

Intercept and rewrite all `json_each()` patterns in the EF Core provider's `QueryableMethodTranslatingExpressionVisitor` before SQL generation. Five overrides handle the complete pattern set:

1. **`TranslatePrimitiveCollection`**: Creates a `JsonEachExpression` intermediate representation (never reaches SQL generation)
2. **`TranslateAny`**: Detects `JsonEachExpression` in SELECT → rewrites to `json_array_length(col) > 0`
3. **`TranslateCount`**: Detects `JsonEachExpression` in SELECT → rewrites to `json_array_length(col)`
4. **`TranslateElementAtOrDefault`**: Detects `JsonEachExpression` with constant index → rewrites to `json_extract(col, '$[N]')`
5. **`TranslateContains`**: Detects `JsonEachExpression` source → rewrites to `col LIKE '%"' || @value || '"%'`

The `JsonEachExpression` is a custom `TableValuedFunctionExpression` that serves as a marker — it is always optimized away by one of the four translation overrides before reaching the SQL generator.

## Consequences

- All primitive collection LINQ patterns work without core TVF support
- No changes to DecentDB engine required (uses existing `json_array_length`, `json_extract`, and `LIKE`)
- `Contains` uses LIKE pattern matching (`'%"value"%'`) which is correct for simple string values but would false-match on values containing `"` characters
- The LIKE-based Contains cannot use indexes (requires full column scan)
- Future: if DecentDB adds TVF support, the provider could remove these overrides and use the default EF Core translation
