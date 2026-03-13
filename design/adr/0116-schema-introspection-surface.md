# ADR-0116: Schema Introspection Surface

**Status:** Accepted
**Date:** 2026-03-11

## Context

DecentDB already tracks richer schema metadata in the engine catalog than it exposed publicly through the C API and Dart binding. Downstream tools could list basic tables, views, indexes, and columns, but they could not retrieve table DDL, table-level CHECK constraints, generated/default expressions, trigger metadata, or whether objects were temporary.

This created gaps for UI consumers such as `decent-bench`, which need a forward-looking schema surface to render accurate metadata without re-deriving DecentDB-specific semantics downstream.

## Decisions

### 1. Keep existing narrow APIs, add richer metadata APIs alongside them

**Decision:** Preserve existing simple schema calls (`listTables`, `getTableColumns`, `listIndexes`, `listViews`, `getViewDdl`) and add richer metadata APIs rather than changing the shape or semantics of the old ones incompatibly.

New C API entry points:
- `decentdb_get_table_ddl`
- `decentdb_list_tables_info_json`
- `decentdb_list_views_info_json`
- `decentdb_list_triggers_json`

The Dart binding mirrors these with typed `Schema` methods and richer model classes.

### 2. Reconstruct canonical DDL instead of storing raw table SQL

**Decision:** Reconstruct canonical schema SQL from catalog metadata for tables, views, indexes, and triggers.

`TableMeta` does not store raw `CREATE TABLE` text, so table DDL is synthesized from columns, defaults, generated expressions, foreign keys, CHECK constraints, and temporary-object state.

### 3. Preserve `get_view_ddl` compatibility

**Decision:** Keep `decentdb_get_view_ddl()` / `Schema.getViewDdl()` returning the canonical SELECT body, because that is the existing meaning of `ViewMeta.sqlText` and existing callers already rely on it.

Full `CREATE VIEW ... AS ...` text is exposed in the richer view metadata surface (`ddl` on view info objects).

### 4. Expose temporary-object metadata as a first-class catalog property

**Decision:** Make temporary-ness explicit on catalog metadata for tables, views, indexes, and triggers, instead of inferring it only from registration paths.

This keeps richer introspection and canonical DDL generation consistent for temporary objects.

## Consequences

- Downstream bindings can render a more complete schema model without reverse-engineering DecentDB internals.
- Canonical DDL is stable and engine-owned, but it may differ textually from the user’s original SQL formatting.
- Existing callers keep working, while newer callers can adopt the richer typed surface incrementally.
