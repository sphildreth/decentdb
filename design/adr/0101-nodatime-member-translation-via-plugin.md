# ADR 0101: NodaTime Member Translation via IMemberTranslatorPlugin

## Status

Accepted

## Context

NodaTime types (`LocalDate`, `Instant`, `LocalDateTime`) are stored as integers in DecentDB (epoch days for `LocalDate`, ticks for `Instant`/`LocalDateTime`). EF Core LINQ queries that access date components (e.g., `x.ReleaseDate.Year`) need SQL translation.

DecentDB has no built-in date functions (`strftime`, `EXTRACT`, etc.), so date component extraction must use pure integer arithmetic.

## Decision

Implement NodaTime member translation using:

1. **`IMemberTranslatorPlugin` interface** — the EF Core extensibility point designed for adding new member translators without replacing existing ones. Registered as `Scoped` (matching `ISqlExpressionFactory` lifetime).

2. **Hinnant civil calendar algorithm** — pure integer arithmetic to extract Year, Month, Day, and DayOfYear from epoch days. No date functions required.

3. **Explicit CAST for type safety** — The input column (typed as `LocalDate` in CLR) is cast to `long` before arithmetic to prevent CLR type propagation through `SqlBinaryExpression`, which would cause `GroupBy` translation to fail with "No coercion operator" errors.

## Alternatives Considered

- **Replacing `IMemberTranslatorProvider`** — Caused `ArgumentNullException` in EF Core's service provider due to lifetime mismatches and hash code collisions. The plugin approach is the sanctioned EF Core extension pattern.

- **Adding date functions to DecentDB core** — Rejected: these are only needed by EF Core, not by general DecentDB users. Keeping them in the binding layer follows the principle that EF-specific logic stays in the EF provider.

- **Client-side evaluation** — Would require materializing entire result sets before grouping/filtering by date components. Unacceptable for performance.

## Consequences

- `LocalDate.Year`, `.Month`, `.Day`, `.DayOfYear` are translatable to SQL in LINQ queries
- `GroupBy(x => x.Date.Year)` works correctly for statistics and reporting queries
- Generated SQL is verbose (nested arithmetic) but correct and performant
- No changes to DecentDB core required
