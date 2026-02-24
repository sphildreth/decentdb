# ADR-0108: Generated Columns (STORED)

## Status
Accepted

## Context
DecentDB needs support for `GENERATED ALWAYS AS (expr) STORED` columns, matching the SQL standard and SQLite/PostgreSQL behavior. Generated columns are derived from other columns in the same row and are physically stored on disk (STORED), computed on INSERT and UPDATE.

## Decision
- **STORED only**: We implement STORED generated columns. VIRTUAL generated columns (computed on read) are out of scope.
- **Storage**: The generated expression is stored as canonical SQL text in the column metadata (catalog), alongside a `isGenerated` flag.
- **Column encoding**: The catalog column encoding adds a `gen=<percent-encoded-sql>` flag, backward-compatible with existing databases.
- **Evaluation**: On INSERT and UPDATE, generated column values are computed from the row's other column values by parsing and evaluating the stored expression. User-supplied values for generated columns are rejected.
- **Persistence**: Generated columns are stored as regular column values in the B+Tree leaf pages — no format change.
- **Constraints**: Generated columns may participate in indexes, UNIQUE, and NOT NULL constraints. They cannot have DEFAULT or be PRIMARY KEY.

## Consequences
- Backward-compatible catalog format extension (additive flag).
- No page layout or WAL format changes.
- Small runtime cost: one `parseSqlExpr` + `evalExpr` per generated column per INSERT/UPDATE row.
