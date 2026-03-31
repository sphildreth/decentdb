# ADR-0129: Rich Schema Snapshot Contract for Tooling Bindings

**Status:** Proposed
**Date:** 2026-03-29

## Context

ADR-0115 established that the Dart binding must sit on the stable `ddb_*` C ABI.
ADR-0116 established that downstream tooling needs richer schema introspection
than the narrow list/describe helpers alone can provide.

The current implementation still leaves a real gap between those decisions and
the actual usable binding surface:

- schema metadata is fragmented across multiple JSON/string calls
- there is no one-shot, deterministic schema snapshot for tooling and UI use
- named `CHECK` constraints, generated-column metadata, temporary-object flags,
  canonical index DDL, canonical trigger DDL, and richer trigger semantics are
  not consistently available through the Dart binding
- downstream tools such as `decent-bench` must currently stitch together an
  incomplete catalog view or re-derive semantics in application code

That violates the intent of ADR-0116 and pushes engine-owned semantics out of
the engine.

## Decision

### 1. Add a first-class rich schema snapshot contract

**Decision:** Introduce a new engine-owned schema snapshot contract for tooling
and bindings.

New Rust metadata types will be added alongside the existing narrow types:

- `SchemaSnapshot`
- `SchemaTableInfo`
- `SchemaColumnInfo`
- `SchemaViewInfo`
- `SchemaIndexInfo`
- `SchemaTriggerInfo`
- `CheckConstraintInfo`

The new C ABI function will be:

- `ddb_db_get_schema_snapshot_json(ddb_db_t *db, char **out_json)`

The snapshot is the authoritative rich schema surface for language bindings and
tooling. Existing narrow functions remain supported.

### 2. Fix the contract shape now and keep it stable

**Decision:** The schema snapshot JSON shape is fixed as:

```json
{
  "snapshot_version": 1,
  "schema_cookie": 0,
  "tables": [],
  "views": [],
  "indexes": [],
  "triggers": []
}
```

Rich table entries must contain:

- `name`
- `temporary`
- `ddl`
- `row_count`
- `primary_key_columns`
- `checks`
- `foreign_keys`
- `columns`

Rich column entries must contain:

- `name`
- `column_type`
- `nullable`
- `default_sql`
- `primary_key`
- `unique`
- `auto_increment`
- `generated_sql`
- `generated_stored`
- `checks`
- `foreign_key`

Rich `CHECK` entries must contain:

- `name` (`null` when unnamed in the engine/catalog)
- `expression_sql`

Rich view entries must contain:

- `name`
- `temporary`
- `sql_text`
- `column_names`
- `dependencies`
- `ddl`

Rich index entries must contain:

- `name`
- `table_name`
- `kind`
- `unique`
- `columns`
- `include_columns`
- `predicate_sql`
- `fresh`
- `temporary`
- `ddl`

Rich trigger entries must contain:

- `name`
- `target_name`
- `target_kind` (`table` or `view`)
- `timing` (`before`, `after`, `instead_of`)
- `events`
- `events_mask`
- `for_each_row`
- `temporary`
- `action_sql`
- `ddl`

### 3. Existing narrow APIs remain and become projections

**Decision:** Keep the existing narrow APIs for compatibility:

- `ddb_db_list_tables_json`
- `ddb_db_describe_table_json`
- `ddb_db_get_table_ddl`
- `ddb_db_list_indexes_json`
- `ddb_db_list_views_json`
- `ddb_db_get_view_ddl`
- `ddb_db_list_triggers_json`

These narrow APIs must be implemented from the same Rust metadata builders as
the rich schema snapshot, so the narrow and rich surfaces cannot drift.

The rich snapshot is the only place that must be complete. The narrow helpers
remain intentionally smaller.

### 4. Canonical DDL is engine-owned and downstream parsing is forbidden

**Decision:** Canonical DDL for tables, views, indexes, and triggers remains
generated inside the engine.

Bindings and downstream applications must not parse or reconstruct DDL in Dart,
Flutter, or other host languages to fill metadata gaps.

### 5. Ordering must be deterministic

**Decision:** Snapshot ordering must be deterministic:

- top-level `tables`, `views`, `indexes`, and `triggers` sorted by object name
- columns in declaration order
- table `checks` in declaration order
- `foreign_keys` in declaration order
- index columns in declaration order
- trigger `events` in enum/bit order

This is required for stable tests, repeatable UI rendering, and low-noise diffs.

### 6. Constraint naming rules are explicit

**Decision:** If the engine has an explicit user-visible constraint name, expose
it. If the engine does not have one, return `null`.

Do not synthesize fake names in Rust, C, Dart, or downstream tooling.

## Consequences

- `decent-bench` and future tooling can load one canonical schema snapshot
  without reverse-engineering DecentDB semantics
- the rich schema surface becomes testable at the engine, C ABI, and Dart layers
- the JSON payload is larger than the current fragmented helpers, but total
  round-trips are reduced and downstream code becomes simpler
- canonical DDL may differ textually from the user’s original formatting, but it
  remains engine-owned, deterministic, and semantically correct
- future schema-surface changes must extend `snapshot_version` deliberately

