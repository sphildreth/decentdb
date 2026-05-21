# SQL And PRAGMA Compatibility Quick Wins Spec

**Document Status:** Completed implementation spec

**Delivered In:** v2.6.0 work branch

**Roadmap:** Delivered; removed from active Future Wins

**Related:** [`WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`](WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md),
[`docs/user-guide/comparison.md`](../docs/user-guide/comparison.md),
[`docs/api/configuration.md`](../docs/api/configuration.md),
[`docs/user-guide/sql-feature-matrix.md`](../docs/user-guide/sql-feature-matrix.md)

## Purpose

This spec defines the completed implementation target for roadmap item 3,
"SQL and PRAGMA compatibility quick wins."

The goal is to remove high-friction migration and tooling failures caused by
small SQLite, PostgreSQL, and DuckDB-adjacent probes, helper functions, and
introspection queries. The goal is not to make DecentDB a SQLite clone or a
PostgreSQL server. Compatibility work in this spec must be narrow, explicit,
tested, and safe under DecentDB's durability-first embedded model.

This spec has been implemented. Roadmap item 3 has been removed from active
`FUTURE_WINS.md` entries and retained only as delivered context. Remaining
heavyweight SQL compatibility work stays in the later advanced compatibility
track.

## Summary

DecentDB already supports a broad practical SQL subset and a limited PRAGMA
surface. The current shipped PRAGMA subset includes:

- `PRAGMA page_size`
- `PRAGMA cache_size`
- `PRAGMA integrity_check`
- `PRAGMA database_list`
- `PRAGMA table_info(<table>)`
- constrained no-op assignment for `page_size` and `cache_size` when the value
  matches the current open configuration

This spec completes the quick-win compatibility layer by adding:

- safe SQLite-style PRAGMA probes and no-op assignments for settings DecentDB
  fixes by design, such as foreign keys, WAL journal mode, and synchronous
  durability
- persistent application metadata PRAGMAs such as `user_version` and
  `application_id`
- common schema introspection PRAGMAs beyond `table_info`
- SQLite compatibility catalog views such as `sqlite_schema`
- minimal `information_schema` views for generic SQL tooling
- a complete `generate_series(...)` helper contract for integer and temporal
  series
- narrow schema-qualified name handling for `main`, `temp`, compatibility
  catalog views, and built-in catalog namespaces
- query-time built-in collation syntax for `BINARY`, `NOCASE`, and `RTRIM`
- explicit unsupported behavior and error messages for PRAGMAs that would
  imply unsafe or unimplemented semantics

## Product Win

Many tools, ORMs, importers, schema browsers, and migration scripts begin by
probing a database with familiar SQLite or PostgreSQL queries. If DecentDB
fails on harmless probes, users often conclude the database is incompatible
before reaching the features DecentDB actually wants to be known for.

This work lets those tools get past common discovery and setup paths:

- tools can ask whether foreign keys are enabled
- tools can ask for journal and synchronous settings without being allowed to
  weaken durability
- SQLite-style schema browsers can read table, column, index, foreign-key, and
  catalog metadata
- PostgreSQL-adjacent SQL can use `generate_series(...)` and basic
  `information_schema` views
- SQL containing safe `main.` and `temp.` qualifiers can run without requiring
  a larger multi-schema catalog project
- common `COLLATE` clauses can be parsed and executed where they do not require
  persistent index collation semantics

The win is onboarding and interoperability polish. It should make DecentDB feel
less foreign without changing the product identity.

## Scope Boundaries

This is a completion spec for the quick-win compatibility milestone. It is not
an initial slice. All items marked "Required" in this document are part of the
done definition.

This spec intentionally does not include:

- full SQLite PRAGMA parity
- SQLite file-format compatibility
- SQLite virtual table compatibility
- PostgreSQL wire protocol compatibility
- PostgreSQL role, schema ownership, search path, permission, or server catalog
  semantics
- broad `pg_catalog` compatibility
- full user-defined types, sequences, materialized views, deferred
  constraints, exclusion constraints, or `MERGE`
- arbitrary import/export features
- persistent custom collation/index collation semantics
- any behavior that weakens DecentDB's default durability

If implementation discovers that one required item needs a file format version
bump, persistent catalog layout change with migration impact, WAL semantics
change, transaction timing change, or C ABI contract break, stop and write an
ADR before implementing that item.

## Compatibility Principles

1. **Never lie about durability.**
   A SQLite-compatible PRAGMA may report DecentDB's closest safe behavior, but
   it must not imply that unsafe SQLite modes are available when they are not.

2. **Safe no-ops are acceptable only when exact behavior is already true.**
   `PRAGMA foreign_keys = ON` can no-op because DecentDB always enforces
   foreign keys. `PRAGMA foreign_keys = OFF` must fail.

3. **Read-only compatibility views are preferred over mutable compatibility
   catalogs.**
   Tooling queries should work, but they must not become alternate catalog
   mutation paths.

4. **Prefer exact result shapes where common tools depend on them.**
   Column names and basic value types should match SQLite or
   `information_schema` conventions where possible.

5. **Keep namespace handling narrow.**
   Accept `main.` and `temp.` where they map to current DecentDB behavior.
   Do not implement full schema-owned objects here.

6. **Treat unsupported compatibility as product surface.**
   Unsupported PRAGMAs must produce clear, deterministic errors and docs, not
   confusing parser failures.

7. **WASM and bindings matter.**
   Compatibility should work through Rust, C ABI JSON execution, CLI, .NET,
   Python, Go, Node, Dart, and browser execution paths because it is ordinary
   SQL.

## Pre-Implementation Baseline

This section captures the baseline that existed before this spec was
implemented.

### Shipped PRAGMA State

| PRAGMA | Current Behavior |
|---|---|
| `page_size` | Query returns current page size. Assignment succeeds only when assigning the current value. |
| `cache_size` | Query returns configured cache size in pages. Assignment succeeds only when assigning the current value. |
| `integrity_check` | Query runs logical integrity checks and returns `ok` or error rows. |
| `database_list` | Query returns one `main` database row. |
| `table_info(<table>)` | Query returns SQLite-style table column rows. |
| unknown PRAGMAs | Error with `unsupported PRAGMA <name>`. |

### Shipped SQL Compatibility State Relevant To This Spec

- `CREATE SCHEMA` registers schema names and persists them.
- Schema-qualified relation names such as `app.users` are not supported.
- Some `pg_catalog.*` type and function names already normalize to DecentDB
  built-ins.
- JSON table-valued functions `json_each` and `json_tree` exist.
- `generate_series(...)` is explicitly rejected today.
- There is no general collation syntax or custom collation model.
- Rich metadata APIs exist in Rust and bindings, but common SQLite and
  `information_schema` SQL views are limited or absent.

## Completion Definition

Roadmap item 3 is complete only when all of the following are true:

1. Every Required PRAGMA in this spec is implemented with documented result
   shape, assignment behavior, and tests.
2. Every Required compatibility view/table-valued introspection helper is
   implemented as read-only SQL surface and covered by tests.
3. Required `generate_series(...)` variants are implemented and documented.
4. Required schema-qualified name compatibility is implemented and documented.
5. Required built-in query-time collation syntax is implemented and documented.
6. Unsupported compatibility surfaces listed here produce deliberate errors
   and are documented.
7. Docs are updated in:
   - `docs/api/configuration.md`
   - `docs/user-guide/comparison.md`
   - `docs/user-guide/sql-feature-matrix.md`
   - `docs/user-guide/sql-reference.md`
   - `docs/api/sql-functions.md`
   - binding docs if execution examples change
   - `docs/about/changelog.md`
8. Tests cover Rust engine behavior plus at least one CLI smoke path for each
   major surface.
9. Validation passes:
   - `cargo fmt --check`
   - `cargo check -p decentdb`
   - `cargo test -p decentdb -- pragma`
   - `cargo test -p decentdb -- compatibility`
   - `cargo test -p decentdb -- generate_series`
   - `cargo test -p decentdb -- collation`
   - `cargo test --workspace`
   - `cargo lint`

## Required Workstream A: PRAGMA Compatibility Contract

### Parsing Requirements

The PRAGMA parser must support:

- `PRAGMA name`
- `PRAGMA name;`
- `PRAGMA name = value`
- `PRAGMA name(value)`
- `PRAGMA schema.name`
- `PRAGMA schema.name(value)`

Allowed schema qualifiers for PRAGMAs:

- `main`
- `temp`

For this spec, any other PRAGMA schema qualifier must fail with:

```text
unsupported PRAGMA schema qualifier '<schema>'; supported qualifiers are main and temp
```

PRAGMA values must accept:

- integers where the existing parser already accepts integers
- quoted strings for PRAGMAs that use text values
- common SQLite boolean keywords for boolean PRAGMAs:
  - `ON`, `TRUE`, `YES`, `1`
  - `OFF`, `FALSE`, `NO`, `0`
- common SQLite mode keywords for mode PRAGMAs:
  - `WAL`
  - `FULL`
  - `NORMAL`
  - `OFF`
  - `EXTRA`

Unsupported value forms must fail with deterministic SQL errors naming the
PRAGMA and accepted values.

### Required PRAGMA Matrix

The following table is the complete Required PRAGMA target for this milestone.

| PRAGMA | Required Query Result | Required Assignment Behavior | Rationale |
|---|---|---|---|
| `page_size` | Existing: one `page_size` INT64 row. | Existing no-op only for current value; other values error with reopen guidance. | Already shipped; keep stable. |
| `cache_size` | Existing: one `cache_size` INT64 row in pages. | Existing no-op only for current value; other values error with reopen guidance. | Already shipped; keep stable. |
| `integrity_check` | Existing: one `integrity_check` TEXT column with `ok` or findings. | Assignment rejected. | Already shipped; keep stable. |
| `quick_check` | Same result shape as `integrity_check`. | Assignment rejected. | SQLite tools often probe `quick_check`; DecentDB may map it to the same logical check initially. |
| `database_list` | Existing: `seq`, `name`, `file`. | Assignment rejected. | Already shipped; keep stable. |
| `table_info(<table>)` | Existing SQLite shape: `cid`, `name`, `type`, `notnull`, `dflt_value`, `pk`. | Assignment rejected. | Already shipped; keep stable. |
| `table_xinfo(<table>)` | Same as `table_info` plus `hidden`. Generated and hidden columns must be represented where DecentDB can distinguish them. | Assignment rejected. | Common SQLite introspection path. |
| `table_list` | Rows for visible tables and views: `schema`, `name`, `type`, `ncol`, `wr`, `strict`. | Assignment rejected. | Common SQLite CLI/tooling path. |
| `index_list(<table>)` | `seq`, `name`, `unique`, `origin`, `partial`. | Assignment rejected. | Common SQLite schema introspection. |
| `index_info(<index>)` | `seqno`, `cid`, `name`. | Assignment rejected. | Common SQLite index introspection. |
| `index_xinfo(<index>)` | `seqno`, `cid`, `name`, `desc`, `coll`, `key`. | Assignment rejected. | Needed for covering indexes and expression-index metadata visibility. |
| `foreign_key_list(<table>)` | `id`, `seq`, `table`, `from`, `to`, `on_update`, `on_delete`, `match`. | Assignment rejected. | Lets tools discover FKs without binding-specific APIs. |
| `foreign_keys` | One `foreign_keys` INT64 row with `1`. | `ON`/true/`1` no-op. `OFF`/false/`0` errors. | DecentDB always enforces FKs; disabling is not safe or supported. |
| `journal_mode` | One `journal_mode` TEXT row with `wal`. | `WAL` no-op and returns `wal`. Other modes error. | DecentDB is WAL-only; tools often ask. |
| `synchronous` | One `synchronous` INT64 row reflecting current `DbConfig::wal_sync_mode`, using SQLite-compatible numeric mapping where possible. | Assignment succeeds only when requested mode matches current open config. Otherwise error with `DbConfig::wal_sync_mode` guidance. | Reports durability without allowing SQL to weaken it. |
| `wal_checkpoint` | SQLite-like `busy`, `log`, `checkpointed` INT64 columns. | Call syntax accepts `PASSIVE`, `FULL`, `RESTART`, `TRUNCATE`; all map to DecentDB's safe checkpoint semantics. | Common maintenance probe; must remain reader-safe. |
| `schema_version` | One `schema_version` INT64 row from DecentDB schema cookie. | Assignment rejected. | Tools often use this to detect schema changes. |
| `user_version` | One `user_version` INT64 row. | Assignment stores a signed 32-bit application value. | Common SQLite migration metadata. |
| `application_id` | One `application_id` INT64 row. | Assignment stores a signed 32-bit application value. | Common SQLite file/application metadata. |
| `encoding` | One `encoding` TEXT row with `UTF-8`. | `UTF-8` no-op. Other values error. | DecentDB text is UTF-8. |
| `busy_timeout` | One `busy_timeout` INT64 row in milliseconds. | Assignment stores a connection-local timeout value and applies it to queued write execution where that path is used. | Common driver/tool setup PRAGMA. |
| `locking_mode` | One `locking_mode` TEXT row with `normal`. | `NORMAL` no-op. `EXCLUSIVE` errors. | DecentDB does not expose SQLite exclusive locking. |
| `temp_store` | One `temp_store` INT64 row documenting current behavior. | Assignment succeeds only for current/default-compatible values; other values error with temp-dir guidance. | Common SQLite setup probe; must not imply memory-only temp semantics. |

### `synchronous` Mapping

`PRAGMA synchronous` must never weaken durability. Query mapping:

| `DbConfig::wal_sync_mode` | `PRAGMA synchronous` Result | Meaning |
|---|---:|---|
| `Full` | `2` | SQLite `FULL` equivalent for DecentDB's durability-first mode. |
| `Normal` | `1` | SQLite `NORMAL`-adjacent reduced sync mode. |
| `AsyncCommit { .. }` | `1` | Reduced post-crash durability window; docs must explain this is not exactly SQLite `NORMAL`. |
| `TestingOnlyUnsafeNoSync` | `0` | Test-only unsafe mode. |

Assignment behavior:

- `FULL`/`2` succeeds only when current mode is `Full`.
- `NORMAL`/`1` succeeds only when current mode is `Normal` or
  `AsyncCommit`.
- `OFF`/`0` succeeds only when current mode is `TestingOnlyUnsafeNoSync`.
- `EXTRA`/`3` always errors because DecentDB does not expose a distinct extra
  sync mode.
- Any request to change the current mode errors with guidance to reopen using
  `DbConfig::wal_sync_mode` or the binding-specific open option.

### `wal_checkpoint` Mapping

`PRAGMA wal_checkpoint` and `PRAGMA wal_checkpoint(<mode>)` must call
DecentDB's existing checkpoint path. The mode is accepted for compatibility,
but DecentDB keeps its reader-safe checkpoint behavior.

Accepted modes:

- `PASSIVE`
- `FULL`
- `RESTART`
- `TRUNCATE`

Result shape:

| Column | Type | DecentDB Meaning |
|---|---|---|
| `busy` | INT64 | `0` if checkpoint completed without reader-retention blockers; `1` if active readers or branch/sync retention prevented full truncation. |
| `log` | INT64 | Best available count of WAL frames or dirty page versions before checkpoint. If exact frame count is unavailable, return a documented approximation and test it as non-negative. |
| `checkpointed` | INT64 | Best available count checkpointed by the operation. If exact count is unavailable, return a documented approximation and test it as non-negative. |

If current engine APIs cannot expose exact `log` and `checkpointed` values
without invasive WAL changes, implement stable non-negative approximations and
document them. Do not change WAL format for this PRAGMA.

### Persistent Application Metadata PRAGMAs

`user_version` and `application_id` are application-owned values. They must be
stored durably without changing database file header layout unless an ADR
chooses otherwise.

Recommended storage:

- internal metadata table keyed by PRAGMA name
- values recorded in normal transactions
- included in dumps only if DecentDB docs decide dumps should reproduce
  application PRAGMA metadata
- visible through PRAGMA query and a DecentDB `sys.*` compatibility metadata
  view if one already fits the local pattern

Validation:

- value persists after reopen
- assignment is transactional under explicit transactions
- rollback restores previous value
- non-integer and out-of-range assignments error

### Unsupported PRAGMAs With Required Error Policy

The following common PRAGMAs are deliberately not implemented in this milestone.
They must produce clear errors if recognized by the PRAGMA parser, and the docs
must explain the DecentDB alternative or reason.

| PRAGMA | Required Behavior |
|---|---|
| `auto_vacuum` | Error: not applicable to DecentDB storage/checkpointing. |
| `cache_spill` | Error: no SQLite page-cache spill policy. |
| `case_sensitive_like` | Error unless DecentDB implements an exact connection-local LIKE mode; default should remain stable. |
| `defer_foreign_keys` | Error: deferred constraints are advanced compatibility work. |
| `ignore_check_constraints` | Error: DecentDB does not disable constraints. |
| `mmap_size` | Error: not exposed as SQL runtime tuning. |
| `optimize` | Error or documented no-op only if doctor/analyze integration is deliberately mapped. |
| `read_uncommitted` | Error: DecentDB does not expose dirty reads. |
| `recursive_triggers` | Error unless trigger recursion semantics are explicitly supported and tested. |
| `secure_delete` | Error: no SQLite-equivalent free-page overwrite guarantee. |
| `trusted_schema` | Error: no SQLite extension/trusted-schema model. |

Recognizing these names for clearer errors is part of the polish. Implementing
their SQLite behavior is not part of this roadmap item.

## Required Workstream B: SQLite Introspection Views And Table-Valued PRAGMAs

### `sqlite_schema` And `sqlite_master`

Implement read-only compatibility views:

- `sqlite_schema`
- `sqlite_master`
- `main.sqlite_schema`
- `main.sqlite_master`

Required columns:

| Column | Type | Notes |
|---|---|---|
| `type` | TEXT | `table`, `index`, `view`, or `trigger`. |
| `name` | TEXT | Object name. |
| `tbl_name` | TEXT | Owning table name for indexes/triggers; object name for tables/views. |
| `rootpage` | INT64 | `0` because DecentDB does not expose SQLite root pages. |
| `sql` | TEXT/NULL | Reconstructed DDL where available; NULL for internal auto-indexes if appropriate. |

Rules:

- User-visible persistent tables, indexes, views, and triggers are included.
- Internal DecentDB tables are excluded unless docs explicitly mark them as
  visible compatibility internals.
- Temp objects are not included in `main.sqlite_schema`.
- The view is read-only. `INSERT`, `UPDATE`, and `DELETE` must fail.
- Filtering and projection should work like normal views.

### `sqlite_temp_schema` And `sqlite_temp_master`

Implement read-only compatibility views:

- `sqlite_temp_schema`
- `sqlite_temp_master`
- `temp.sqlite_schema`
- `temp.sqlite_master`

They use the same columns as `sqlite_schema` but expose temp tables, temp
views, temp indexes, and temp triggers visible to the current connection.

### Table-Valued PRAGMA Functions

SQLite exposes many PRAGMAs as table-valued functions. Implement these
read-only helpers:

- `pragma_table_info(name)`
- `pragma_table_xinfo(name)`
- `pragma_table_list()`
- `pragma_index_list(table_name)`
- `pragma_index_info(index_name)`
- `pragma_index_xinfo(index_name)`
- `pragma_foreign_key_list(table_name)`
- `pragma_database_list()`

Rules:

- Result shapes must match the corresponding PRAGMA.
- Function names may be used in `FROM`.
- A `main.` or `temp.` prefix must be accepted where it maps cleanly:
  `main.pragma_table_info('users')`.
- These helpers are read-only and deterministic for a stable schema snapshot.
- Unknown table/index names return an empty result set where SQLite does so,
  unless DecentDB already documents an error for the equivalent direct PRAGMA.
  Pick one behavior per helper and test it.

### SQLite View Validation

Required tests:

- schema view lists tables, views, indexes, and triggers
- temp schema view is connection-local
- auto-created internal index visibility matches documented policy
- read-only mutation attempts fail
- DDL text round-trips enough for common tooling display
- filtering by `type`, `name`, and `tbl_name` works
- table-valued PRAGMA helpers can be joined and filtered

## Required Workstream C: Minimal `information_schema`

Implement a small read-only `information_schema` surface for generic tools that
avoid database-specific metadata APIs.

Required views:

- `information_schema.schemata`
- `information_schema.tables`
- `information_schema.columns`

### `information_schema.schemata`

Required columns:

- `catalog_name`
- `schema_name`
- `schema_owner`
- `default_character_set_catalog`
- `default_character_set_schema`
- `default_character_set_name`

Minimum rows:

- `main`
- `temp` when temp objects exist or when DecentDB chooses to expose it
  unconditionally
- registered schemas from `CREATE SCHEMA`, even though full schema-owned
  objects are not part of this quick-win item

### `information_schema.tables`

Required columns:

- `table_catalog`
- `table_schema`
- `table_name`
- `table_type`

Required `table_type` values:

- `BASE TABLE`
- `VIEW`
- `LOCAL TEMPORARY`

### `information_schema.columns`

Required columns:

- `table_catalog`
- `table_schema`
- `table_name`
- `column_name`
- `ordinal_position`
- `column_default`
- `is_nullable`
- `data_type`

Rules:

- Column order is 1-based.
- `is_nullable` is `YES` or `NO`.
- `data_type` uses DecentDB's documented type names, not PostgreSQL OIDs.
- Generated column metadata may be omitted in this minimal surface if it is
  available through DecentDB's richer metadata APIs and documented.

### Out Of Scope For `information_schema`

The following are not part of this quick-win item:

- constraints views
- routines/functions views
- privileges and roles
- domains and user-defined types
- PostgreSQL-specific `pg_catalog.pg_class`, `pg_attribute`, or OID catalogs

If a future ORM requires those, either add a narrow follow-up spec or move the
work to the advanced compatibility track.

## Required Workstream D: `generate_series(...)`

Implement `generate_series(...)` as a built-in table-valued function.

### Required Variants

1. Integer series:

```sql
SELECT * FROM generate_series(1, 5);
SELECT * FROM generate_series(1, 10, 2);
SELECT value FROM generate_series(5, 1, -1);
```

2. Timestamp series:

```sql
SELECT * FROM generate_series(
  TIMESTAMP '2026-01-01 00:00:00',
  TIMESTAMP '2026-01-01 03:00:00',
  INTERVAL '1 hour'
);
```

3. Date series:

```sql
SELECT * FROM generate_series(
  DATE '2026-01-01',
  DATE '2026-01-05',
  INTERVAL '1 day'
);
```

### Result Shape

Default column:

| Column | Type |
|---|---|
| `value` | Same logical type family as the start argument. |

PostgreSQL names the default output column `generate_series`. DecentDB should
use `value` for consistency with its existing table-valued helper style unless
parser/executor conventions make `generate_series` cheaper. The chosen name
must be documented and tested.

### Semantics

- Series are inclusive of `start` and `stop` when the step lands on `stop`.
- Positive step with `start > stop` returns zero rows.
- Negative step with `start < stop` returns zero rows.
- Step zero errors.
- Mixed integer/temporal arguments error.
- Temporal series require an explicit interval step.
- Integer two-argument form defaults to step `1`.
- Row generation must be streaming or bounded by an internal row cap to avoid
  unbounded memory allocation.
- A configured maximum row count must protect accidental huge series. The error
  must name the limit and how to rewrite the query.

### Validation

- positive integer series
- negative integer series
- empty series
- step zero error
- large series limit error
- timestamp and date series
- parameterized arguments
- use in joins, CTEs, and subqueries
- stable output order

## Required Workstream E: Narrow Schema-Qualified Name Compatibility

DecentDB currently persists schema registrations but does not own objects by
schema. Full schema-owned object support belongs to the later advanced
compatibility track.

This quick-win item must implement only the qualifiers that map cleanly to the
current catalog.

### Required Qualifiers

| Qualifier | Required Meaning |
|---|---|
| `main.<object>` | Persistent table/view/index/trigger lookup in the current DecentDB file. |
| `temp.<object>` | Current connection temp object lookup. |
| `sqlite_schema`, `sqlite_master`, `sqlite_temp_schema`, `sqlite_temp_master` | Compatibility catalog views. |
| `information_schema.<view>` | Minimal information schema views from this spec. |
| `pg_catalog.<builtin>` | Existing supported `pg_catalog` function/type aliases continue to work; do not broaden to `pg_catalog` tables here. |

### Required SQL Coverage

The following must accept `main.` and `temp.` where the object kind supports it:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE TABLE`
- `CREATE TEMP TABLE`
- `CREATE VIEW`
- `CREATE TEMP VIEW`
- `CREATE INDEX`
- `DROP TABLE`
- `DROP VIEW`
- `DROP INDEX`
- `ALTER TABLE` for existing supported operations
- `PRAGMA main.table_info(...)`
- `PRAGMA temp.table_info(...)`

### Required Rejections

User-defined registered schemas such as `app.users` must fail clearly until the
advanced schema namespace track is implemented:

```text
schema-qualified objects outside main/temp are not supported yet; schema 'app'
is registered but object ownership by schema is advanced compatibility work
```

This avoids quietly treating `app.users` as `users`, which would be dangerous.

### Search Path Policy

No search path is added in this quick-win item.

Unqualified lookup remains:

1. temp object if a temp object shadows a persistent object
2. persistent main object

There is no role-specific lookup and no mutable `search_path`.

## Required Workstream F: Built-In Query-Time Collations

This workstream makes common `COLLATE` clauses usable in ordinary queries
without introducing persistent index collation semantics.

### Required Built-In Collations

| Name | Required Semantics |
|---|---|
| `BINARY` | Current DecentDB default text ordering/equality. |
| `NOCASE` | ASCII case-insensitive comparison for `A-Z` and `a-z`; non-ASCII behavior must be documented as byte/codepoint-preserving unless a later ADR chooses Unicode collation. |
| `RTRIM` | Same as `BINARY` after ignoring trailing ASCII space U+0020. |

Names are case-insensitive.

### Required Query Support

Support `COLLATE` in:

- `ORDER BY expression COLLATE <name>`
- comparison expressions such as `a COLLATE NOCASE = b`
- `GROUP BY` and `DISTINCT` only if executor architecture can apply the same
  comparison consistently. If not, reject with a clear error and document the
  limitation before considering this workstream complete.

### DDL Policy

Column and index collation persistence are not part of this quick-win item
unless they can be implemented without a catalog/file-format compatibility
decision.

Required behavior:

- `COLLATE BINARY` in column definitions may be accepted as a no-op if parser
  support makes that straightforward.
- `COLLATE NOCASE` or `COLLATE RTRIM` in persistent column/index definitions
  must either be fully persisted and enforced or rejected clearly.
- `CREATE INDEX ... COLLATE <name>` must not create an index whose lookup
  semantics differ from its metadata. Reject non-default collations unless an
  ADR defines persistent index collation behavior.

### Validation

- `ORDER BY name COLLATE NOCASE`
- equality comparison with `COLLATE NOCASE`
- `RTRIM` trailing-space behavior
- unsupported collation name error
- persistent DDL collation rejection or documented no-op behavior
- no planner claim that a binary index satisfies a non-binary collation query

## Required Workstream G: Scalar Compatibility Helpers

These helpers are small but useful for migrations and tooling.

### Required Helpers

| Function | Required Result |
|---|---|
| `current_database()` | TEXT `main`. |
| `current_schema()` | TEXT `main`. |
| `version()` | DecentDB version string, clearly branded as DecentDB. |

### Optional But Recommended Helpers

| Function | Notes |
|---|---|
| `database()` | MySQL/DuckDB-adjacent alias returning `main`; include only if docs call it a compatibility alias. |
| `schema()` | Alias for `current_schema()`; include only if useful to tooling. |

### Rejected Helpers

| Function | Required Behavior |
|---|---|
| `sqlite_version()` | Do not return a fake SQLite version. Either reject or return a clearly DecentDB-specific error. |
| `pg_backend_pid()` | Reject; DecentDB is embedded and does not expose server backends. |
| `current_user`, `session_user` | Reject until local policy/audit context is implemented. |

## Workstream H: Error Codes And Diagnostics

Compatibility failures should be understandable. Add or reuse stable SQL error
codes/messages for:

- unsupported PRAGMA
- unsupported PRAGMA assignment
- unsupported PRAGMA value
- unsupported PRAGMA schema qualifier
- unsupported schema qualifier
- unsupported persistent collation
- unsupported SQLite/PostgreSQL compatibility catalog
- series row limit exceeded

The exact Rust `DbError` taxonomy can reuse existing SQL errors if the public
message is deterministic. If stable machine-readable error codes are available
for SQL errors, add specific codes only when local patterns support it.

## Workstream I: Documentation

Update these docs when implementation is complete:

- `docs/api/configuration.md`
  - full PRAGMA table
  - safe no-op and rejected assignment rules
  - `synchronous` and `journal_mode` DecentDB mapping
- `docs/user-guide/comparison.md`
  - replace old "small subset" text with exact supported list
  - explain unsupported SQLite PRAGMAs and alternatives
- `docs/user-guide/sql-feature-matrix.md`
  - add rows for new PRAGMAs, compatibility views, `generate_series`,
    schema-qualified `main`/`temp`, and collations
- `docs/user-guide/sql-reference.md`
  - `generate_series`
  - `COLLATE`
  - schema-qualified `main`/`temp`
  - compatibility catalog views
- `docs/api/sql-functions.md`
  - table-valued PRAGMA helpers
  - `information_schema` views
  - scalar compatibility helpers
- `docs/about/changelog.md`
  - user-facing compatibility summary

Do not update root `CHANGELOG.md`.

## Workstream J: Test Plan

### Engine Tests

Add focused tests under the core engine for:

- each Required PRAGMA query result shape
- each Required PRAGMA accepted assignment
- each Required PRAGMA rejected assignment
- `user_version` and `application_id` persistence and rollback
- `sqlite_schema` and temp schema views
- table-valued PRAGMA helpers
- `information_schema` views
- `generate_series` integer/date/timestamp variants
- `generate_series` error cases
- `main.` and `temp.` qualified object lookup
- registered non-main schema rejection
- `COLLATE BINARY`, `NOCASE`, and `RTRIM`
- unsupported collation in persistent index DDL
- scalar helpers

### CLI Tests

Add CLI smoke tests for:

- `decentdb exec --sql "PRAGMA foreign_keys"`
- `decentdb exec --sql "PRAGMA journal_mode=WAL"`
- `decentdb exec --sql "SELECT * FROM sqlite_schema"`
- `decentdb exec --sql "SELECT * FROM generate_series(1,3)"`
- `decentdb exec --sql "SELECT name FROM t ORDER BY name COLLATE NOCASE"`

### Binding/ABI Tests

Because this is ordinary SQL, most binding coverage can be smoke-level:

- C ABI JSON execute can run a new PRAGMA and `generate_series`
- .NET or Python smoke test can query `sqlite_schema`
- Web/WASM smoke only if parser support differs under WASM minimal parser

### Regression Tests

Add tests proving DecentDB does not weaken safety:

- `PRAGMA foreign_keys=OFF` fails and FK enforcement still works
- `PRAGMA journal_mode=OFF` fails
- `PRAGMA synchronous=OFF` fails unless the database was explicitly opened in
  test-only unsafe mode
- `PRAGMA read_uncommitted=1` fails
- `CREATE INDEX ... COLLATE NOCASE` does not silently create a binary index
  that claims no-case semantics

## Implementation Notes

### PRAGMA Parser

The current PRAGMA parser is intentionally small. Extending it should keep the
parser deterministic and local:

- add a richer `PragmaValue` enum rather than forcing every assignment into
  `i64`
- preserve direct handling before general SQL parser normalization
- keep unsupported PRAGMAs in a known-name table for better errors
- avoid ad hoc string parsing outside the PRAGMA parser

### Virtual Compatibility Views

Compatibility views should be virtual/system-backed rather than persisted user
views. They should:

- respect snapshots like other metadata inspection
- exclude internal tables by default
- be read-only
- avoid writing telemetry or metadata during reads
- use existing schema snapshot and DDL rendering helpers where possible

### `generate_series`

Prefer the existing table-valued function path used by `json_each` and
`json_tree`. Avoid materializing huge series if the executor can stream rows.
If streaming table functions are not available yet, enforce a strict row cap.

### Collations

Do not thread collations into persisted index metadata in this milestone unless
an ADR explicitly approves the catalog and planner semantics. Query-time
collations are enough for this compatibility win.

## Security And Durability Considerations

- Compatibility PRAGMAs must not provide a route to disable constraints.
- Compatibility PRAGMAs must not provide a route to weaken durability after a
  database is opened.
- Compatibility views must not expose key material, tokens, internal sync
  payloads, or hidden system tables unless those are already intentionally
  public through `sys.*`.
- `user_version` and `application_id` are application metadata, not trusted
  security controls.
- `busy_timeout` must not mask deadlocks or indefinite waits. It needs clear
  maximums and bounded behavior.

## Performance Considerations

- Metadata views should avoid loading every table row.
- `sqlite_schema`, `information_schema`, and PRAGMA helpers should use catalog
  metadata and DDL renderers, not table scans.
- `integrity_check` and `quick_check` are allowed to inspect storage and may be
  more expensive.
- `generate_series` must be bounded or streaming.
- Query-time `NOCASE` and `RTRIM` collations may be slower than binary
  comparison; docs should not imply index acceleration.

## Interaction With Other Roadmap Items

### Advanced SQL Compatibility

The following remain in `WIN_ADVANCED_SQL_COMPATIBILITY_SURFACE.md`:

- full schema-owned objects and search path
- explicit sequences
- materialized views
- covering-index execution
- SQL-defined functions
- deferred constraint timing
- user-defined types
- exclusion constraints
- scoped `MERGE`

### Local Data Security

Policy-aware SQL and audit context may later introduce role/user/session
concepts. This spec must not preempt that by faking `current_user` or access
control semantics.

### Lua Runtime

Lua-backed functions and collations are not part of this quick-win item.
Built-in collations here are engine-owned and fixed.

### Cross-Process WAL Coordination

`busy_timeout`, `locking_mode`, and `wal_checkpoint` must not imply
cross-process writer coordination. That remains a separate roadmap item.

## Resolved Implementation Decisions

The implementation made the following best-practice decisions without requiring
a new ADR because none changed file format, WAL semantics, broad ABI contracts,
or durability guarantees:

1. `quick_check` is a strict alias for the existing logical
   `integrity_check`.
2. `sqlite_schema.rootpage` is `0` for SQLite result-shape compatibility.
3. Direct `PRAGMA table_info(...)` keeps DecentDB's existing behavior for
   unknown tables. Table-valued PRAGMA helpers return zero rows for unknown
   table or index names where SQLite-style tooling expects filterable
   introspection.
4. `generate_series` returns a single output column named `value`, matching
   DecentDB table-valued function conventions.
5. `temp` always appears in `information_schema.schemata`, even when no temp
   objects exist.
6. `PRAGMA busy_timeout` affects the connection-local default for queued
   writes only. Direct APIs keep their existing behavior.
7. `PRAGMA wal_checkpoint(...)` maps supported checkpoint modes to DecentDB's
   safe checkpoint operation and returns non-negative SQLite-shaped counters.
8. Query-time `BINARY`, `NOCASE`, and `RTRIM` collations are supported for
   `ORDER BY`, comparisons, and `BETWEEN`; collated `DISTINCT` and `GROUP BY`
   keys are rejected until DecentDB has full key-equivalence semantics.
9. `user_version` and `application_id` are stored as durable transactional
   signed 32-bit application metadata in an internal hidden table.

## Done Checklist

- [x] PRAGMA parser accepts required forms and known values.
- [x] Required PRAGMA matrix implemented.
- [x] Required unsupported PRAGMAs produce deliberate errors.
- [x] Persistent application metadata PRAGMAs are transactional and durable.
- [x] SQLite schema views implemented and read-only.
- [x] Table-valued PRAGMA helpers implemented.
- [x] Minimal `information_schema` implemented.
- [x] `generate_series` integer/date/timestamp variants implemented.
- [x] `main.` and `temp.` qualified names implemented.
- [x] Non-main registered schema qualifiers rejected clearly.
- [x] Query-time `BINARY`, `NOCASE`, and `RTRIM` collations implemented.
- [x] Persistent collation DDL policy implemented and tested.
- [x] Scalar compatibility helpers implemented.
- [x] Docs updated.
- [x] Changelog updated in `docs/about/changelog.md`.
- [x] Engine, CLI, binding smoke, workspace tests, and lint pass.
- [x] Roadmap item 3 removed from active Future Wins or moved to Delivered
      Context after implementation is complete.
