# ADR 0049: Constraint Index Deduplication

**Date:** 2026-01-31  
**Status:** Proposed  
**Context:** Phase 3 of [SQLITE_GAPS_PLAN_V2.md](../SQLITE_GAPS_PLAN_V2.md)

## Context

DecentDB currently auto-creates B+Tree indexes to support column-level constraints:

- `PRIMARY KEY` (unless optimized `INT64 PRIMARY KEY`, see ADR 0036)
- `UNIQUE`
- Foreign keys (`REFERENCES`, see ADR 0006)

These auto-created indexes have predictable names (e.g. `pk_<table>_<col>_idx`, `uniq_<table>_<col>_idx`, `fk_<table>_<col>_idx`).

However, *index equivalence* is currently treated primarily as “same name”, not “same semantics”. This creates two problems:

1. **Redundant indexes can exist with different names**, especially when indexes are recreated by tools (e.g. `vacuum`) or when schemas are imported/transformed.
2. **Size gap impact**: redundant indexes increase page count and write amplification and directly contribute to the SQLite→DecentDB size gap discussed in [SQLITE_GAPS.md](../SQLITE_GAPS.md).

Given the MVP scope (single-column indexes; no collations; no per-index options beyond kind+unique), two indexes with the same `(table, column, kind)` and compatible `unique` flag are operationally redundant for our planner and enforcement.

## Decision

Introduce **semantic index matching** for constraint-related index creation and vacuum index recreation.

### 1) Define “index requirement”

A constraint that needs an index will be expressed as a requirement:

- `table`
- `column`
- `kind` (currently `btree` or `trigram`; constraints use `btree`)
- `requireUnique` (bool)

### 2) Define “index satisfies requirement”

An index satisfies a requirement if:

- `idx.table == req.table`
- `idx.column == req.column`
- `idx.kind == req.kind`
- If `req.requireUnique == true`, then `idx.unique == true`
- If `req.requireUnique == false`, then either `idx.unique == false` **or** `idx.unique == true` (a unique index is sufficient for FK lookups and equality seeks)

### 3) Constraint auto-index creation rule

When a constraint would auto-create an index:

- If an existing index already satisfies the requirement, **do not create a new index**.
- Otherwise, create a new index (with the existing predictable naming scheme).

This applies to:

- `PRIMARY KEY` (non-`INT64 PRIMARY KEY` case)
- `UNIQUE`
- FK child indexes

### 4) Vacuum index recreation rule

When recreating “extra” indexes from a source database into a destination database, skip creating an index if the destination already has an index that satisfies the same requirement.

This avoids creating redundant indexes that differ only by name.

## Rationale

- **Direct size reduction**: eliminates accidental duplicate indexes that can be significant on index-heavy schemas.
- **Improves “schema roundtrip” behavior** for tools like `vacuum` by treating indexes as semantics rather than names.
- **Low risk**: no changes to persistent page/record formats; only changes to index creation policy.
- **Compatible with ADR 0006**: still ensures FK columns have a usable B+Tree index, but avoids creating a second one when another suitable index already exists.

## Non-Goals

- Multi-column indexes or prefix indexes.
- Automatic dropping/merging of redundant indexes in existing databases.
- Changing SQL surface area (syntax/semantics) for constraints.
- Any change to on-disk formats (`FormatVersion` remains unchanged).

## Implementation Notes (follow-up work)

- Add a catalog helper that can search indexes by `(table, column, kind)` and check `unique` requirements.
- Update constraint-driven index creation to use semantic matching instead of checking by fixed name.
- Update the `vacuum` implementation to skip recreating indexes that are semantically satisfied in the destination.

## Testing Notes (follow-up work)

- Unit test: create a DB with a `UNIQUE` column, create an additional explicit unique index on the same column with a different name, then `vacuum` and assert the destination has only one unique B+Tree index for that column.
- Unit test: `REFERENCES` column that already has a B+Tree index should not trigger FK index creation.

## Alternatives Considered

- **Status quo (name-based dedupe)**: simplest but continues to allow redundant indexes by semantics.
- **Reject redundant indexes on `CREATE INDEX`**: stronger and simpler dedupe, but changes SQL behavior and could surprise users; can be revisited later.
- **Auto-drop redundant indexes during vacuum**: more aggressive; risks breaking user expectations around named indexes.

## Trade-offs

- Slightly more complex index-creation logic.
- In rare cases where “duplicate indexes” are intentionally created for experimentation, the vacuum tool may no longer reproduce the duplication.

## References

- [SQLITE_GAPS.md](../SQLITE_GAPS.md)
- [SQLITE_GAPS_PLAN_V2.md](../SQLITE_GAPS_PLAN_V2.md)
- [0006-foreign-key-index-creation.md](0006-foreign-key-index-creation.md)
- [0036-catalog-constraints-index-metadata.md](0036-catalog-constraints-index-metadata.md)
- [0036-integer-primary-key.md](0036-integer-primary-key.md)
