# ADR-0096: Case-Insensitive Identifier Resolution

**Status:** Accepted  
**Date:** 2025-07-18  
**Context:** Bug fix — catalog and binder performed case-sensitive comparisons despite using pg_query which follows PostgreSQL identifier folding rules.

## Problem

DecentDB uses `libpg_query` (ADR-0035) to parse SQL. PostgreSQL's parser lowercases all unquoted identifiers — `SELECT Name FROM Users` becomes `select name from users`. Quoted identifiers preserve case — `SELECT "Name" FROM "Users"` keeps original casing.

DecentDB's catalog stored identifiers with their original case (as received from the parser) and compared them **case-sensitively**. This created a mismatch:

1. `CREATE TABLE "Users" ("Name" TEXT)` → catalog stores table `Users`, column `Name`
2. `SELECT Name FROM Users` → parser produces `name`, `users` (lowercased)
3. Catalog lookup for `users` fails because map key is `Users`

This is a correctness bug, not a feature request. Any SQL tool that mixes quoted and unquoted identifiers (EF Core, raw SQL, any PostgreSQL-compatible client) hits this.

## Decision

Normalize identifier comparisons to case-insensitive at the **comparison point**, not at the storage point.

### Why comparison-time, not storage-time?

- **No persistent format change** — column and table names stored on disk keep their original case
- **Backward compatible** — existing databases load correctly without migration
- **Display fidelity** — `DESCRIBE TABLE` shows the name the user originally chose
- **PostgreSQL semantics** — PostgreSQL also stores the original (folded) name and compares case-insensitively for unquoted identifiers

### What changed

| Module | Change | Hot path? |
|--------|--------|-----------|
| `catalog.nim` | Map keys (tables, views, indexes) normalized via `normalizedObjectName()` | Once per statement |
| `binder.nim` | `resolveColumn()` pre-normalizes lookup name, compares via `normalizedName()` | Once per column ref per statement |
| `binder.nim` | DDL/conflict column matching uses `eqIdent()` helper | DDL only (infrequent) |
| `engine.nim` | `columnIndexInTable()` pre-normalizes lookup name | Per column lookup in execution |
| `sql.nim` | CREATE TABLE constraint column matching lowercased | DDL only |

### Performance impact

- `toLowerAscii()` on identifier strings (typically <64 chars): sub-microsecond
- Lookup keys are normalized once outside loops, not per iteration
- `eqIdent()` is `{.inline.}` and used only in DDL paths
- No measurable impact on query throughput

## Consequences

- Mixed quoting now works: `CREATE TABLE "Users" (...)` + `SELECT * FROM Users` succeeds
- EF Core (which quotes all identifiers) and raw SQL (which typically doesn't quote) interoperate correctly
- All bindings (Python, .NET, Go, Node) benefit from the fix
- No database migration needed — existing files work unchanged
