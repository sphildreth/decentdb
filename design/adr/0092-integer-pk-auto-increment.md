# 92. INTEGER PRIMARY KEY Auto-Increment

Date: 2026-02-08

## Status

Accepted

## Context

ADR-0036 established that a single `INT64 PRIMARY KEY` column becomes the table's `rowid`, and that the storage layer uses `nextRowId` to auto-generate a value when one is not explicitly provided (storage.nim `insertRowInternal`).

However, when a user omits the PK column from an INSERT:

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
INSERT INTO users (name) VALUES ('Alice');  -- omit id
```

the binder fills the omitted column with a NULL literal, and `enforceNotNull` rejects it because PRIMARY KEY columns should be NOT NULL. The auto-increment path in the storage layer is never reached.

This is the gap identified in ADR-0036 line 55: *"If the value is not provided (e.g. NULL or missing), we might need to fallback to auto-increment logic."*

PostgreSQL's `SERIAL` type provides this behavior via sequences. SQLite provides it implicitly for `INTEGER PRIMARY KEY` via ROWID aliasing. Both are widely expected by users.

## Decision

1. **`INTEGER PRIMARY KEY` implies `NOT NULL`.** Single-column integer primary keys are implicitly NOT NULL (matching PostgreSQL behavior where PRIMARY KEY implies NOT NULL).

2. **Auto-increment exception.** When enforcing NOT NULL constraints during INSERT, skip the check for a single-column `INT64 PRIMARY KEY` if the provided value is NULL. The storage layer will assign the next `nextRowId` value automatically.

3. **No new SQL syntax.** No `SERIAL`, `AUTOINCREMENT`, or `GENERATED` keyword is needed. The behavior is implicit for `INTEGER PRIMARY KEY` columns, matching SQLite's ROWID alias behavior.

4. **Explicit values still work.** Users can still provide an explicit integer value for the PK column, overriding auto-increment.

### Scope

- Only applies to single-column `INT64` primary keys (same constraint as ADR-0036).
- Composite primary keys are unaffected and still require explicit values.
- The on-disk format is unchanged; `nextRowId` tracking already exists.

## Consequences

### Positive
- Users can omit the PK column from INSERT statements, matching PostgreSQL/SQLite ergonomics.
- No format changes, no new dependencies.
- Completes the design anticipated by ADR-0036.

### Negative
- Slightly different NOT NULL semantics for the PK column vs other NOT NULL columns (auto-increment exception).

## Implementation status

Implemented in `engine.nim` (`enforceNotNull` skip for auto-increment PK) and covered by unit tests.
