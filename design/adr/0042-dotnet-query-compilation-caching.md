# ADR-0042: .NET Query Compilation and Caching

**Status**: Accepted
**Date**: 2026-01-30

## Context
Micro-ORM query compilation (expression parsing + SQL generation + parameter extraction) must be fast and avoid repeated work to meet the <1ms managed overhead budget for typical SELECT operations.

## Decision
- Cache compiled WHERE translation for `Expression<Func<T,bool>>` instances in the Micro-ORM.
- The cache stores:
  - SQL WHERE fragment
  - parameter getter delegates that re-evaluate captured values
- Cache keying is by **expression instance identity** (not structural hashing) using `ConditionalWeakTable`.

## Consequences
- **Pros**: Zero manual cache invalidation; avoids leaks; keeps hot-path compilation work amortized when the caller reuses expression instances.
- **Cons**: Structurally identical expressions created as new instances will not hit cache; a future structural hash cache could be added if needed.

## References
- design/DAPPER_SUPPORT.md (Phase 5: Query Compilation Caching)
- bindings/dotnet/src/DecentDB.MicroOrm/ExpressionSqlBuilder.cs
