# ADR-0109: Temporary Tables and Views

## Status
Accepted

## Context
DecentDB needs support for `CREATE TEMP TABLE` and `CREATE TEMP VIEW` (session-scoped temporary objects). These are commonly used for intermediate results in complex queries and application workflows.

## Decision
- **Session-scoped**: Temp tables and views exist only for the lifetime of a database connection (Db handle). They are not persisted to disk.
- **In-memory storage**: Temp tables store rows in an in-memory seq (not in paged B+Tree storage). This keeps the implementation mirustal and avoids WAL/durability complexity.
- **Namespace**: Temp objects live in a separate namespace from persistent objects. A temp table with the same name as a persistent table shadows it within the session.
- **Catalog tracking**: Temp objects are tracked via `tempTables` and `tempViews` fields on the Db object (not persisted to the catalog pages).
- **DROP**: `DROP TABLE` and `DROP VIEW` on temp objects removes them from the in-memory catalog only.
- **Limitations (v1)**:
  - No indexes on temp tables (full scan only)
  - No triggers on temp tables
  - No foreign key references to/from temp tables

## Consequences
- No persistent format changes (temp data is never written to WAL or data pages).
- No ADR conflict with existing storage model.
- Mirustal code: parsing the TEMP/TEMPORARY keyword, routing to in-memory storage, and adjusting table/view lookup precedence.
