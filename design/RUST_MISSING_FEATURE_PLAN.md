# Rust Missing Feature Plan

This document captures the delta between the SQL features documented as supported in `docs/user-guide/sql-feature-matrix.md` and the features that the current Rust rewrite actually implements today.

The matrix remains the source of truth. This document exists to drive implementation work, not to weaken or edit the matrix.

## Audit rule

For this audit, a feature only counts as implemented when the Rust rewrite supports it end-to-end:

- parser acceptance
- AST/normalization support
- planner/executor behavior
- regression coverage or otherwise strong runtime evidence

Parser-only support does not count as implemented.

## Engine core change guardrails

All future changes and additions to the DecentDB engine core that are driven by this plan must adhere to the seven tenets in `design/PRD.md:3-47`.

### 1. ACID compliance comes first

Durability and correctness are non-negotiable. No feature-parity work should weaken crash safety, WAL correctness, `fsync` discipline, or recovery guarantees (`design/PRD.md:5-9`).

### 2. Performance must remain uncompromising

Feature work must preserve the project’s performance goals for reads, writes, and memory behavior. Query-surface expansion is not allowed to quietly introduce obviously avoidable regressions in hot paths (`design/PRD.md:10-15`).

### 3. Disk footprint still matters

Schema, catalog, row, and index changes must remain conscious of on-disk size and page-layout efficiency. “Support the SQL feature” is not sufficient if the implementation bloats persistent structures without strong justification (`design/PRD.md:16-20`).

### 4. Documentation must stay accurate

This plan exists because documentation drift happened. Any slice that changes the real SQL surface should leave behind precise docs and executable tests so the user-guide stops getting ahead of the engine (`design/PRD.md:21-26`).

### 5. Bindings and tooling compatibility matter

Core-engine SQL changes should be evaluated for impact on the C ABI, CLI behavior, and downstream bindings. A SQL feature that exists only in the Rust core but breaks or surprises bindings is not a finished change (`design/PRD.md:27-37`).

### 6. The CLI experience must remain strong

New SQL features must surface good errors and predictable behavior in the CLI, not just in library-only execution paths. User-facing ergonomics matter for this work too (`design/PRD.md:39-43`).

### 7. The feedback loop must stay fast

Implementation slices should be testable with fast local and CI validation. Prefer coherent, incremental changes with focused regression coverage over giant rewrites that only become verifiable in long-running suites (`design/PRD.md:44-47`).

## Status

| Slice | Status | Scope |
| --- | --- | --- |
| Slice 0 - Audit capture and implementation plan | Completed | Capture current findings and turn them into coding slices. |
| Slice 1 - Query-semantic corrections close to current engine | Not started | Fix gaps that are already close to existing machinery. |
| Slice 2 - DISTINCT and pagination parity | Not started | `DISTINCT ON`, `LIMIT ALL`, `OFFSET ... FETCH` verification/implementation. |
| Slice 3 - Window parity completion | Not started | Fill the documented window-function gap. |
| Slice 4 - Join surface parity | Not started | `RIGHT`, `FULL OUTER`, `CROSS`, and `NATURAL` joins. |
| Slice 5 - Recursive CTE support | Not started | `WITH RECURSIVE` execution and guardrails. |
| Slice 6 - Scalar, date/time, and JSON scalar function expansion | Not started | Bring documented function surface closer to reality. |
| Slice 7 - JSON table functions | Not started | `json_each()` and `json_tree()` in `FROM`. |
| Slice 8 - Planner statistics and `ANALYZE` | Not started | Add SQL `ANALYZE` plus catalog-backed planner statistics to reduce optimizer guesswork. |
| Slice 9 - Generated columns and temp objects | Not started | `GENERATED ALWAYS AS (...) STORED`, `CREATE TEMP TABLE`, `CREATE TEMP VIEW`. |
| Slice 10 - Matrix regression harness | Not started | Convert documented claims into executable regression coverage. |

## Why this plan exists

The user-guide matrix is broad. It documents advanced SQL surface area including recursive CTEs, multiple non-left join kinds, advanced window functions, many scalar/date/time functions, JSON table functions, generated columns, and session-scoped temp objects (`docs/user-guide/sql-feature-matrix.md:21-26`, `91-93`, `123-128`, `167-169`, `202-204`, `236-243`, `275-337`, `463-465`, `627-629`).

The current Rust rewrite clearly implements a meaningful SQL subset, but the codebase also contains explicit rejections for some of those documented features and narrower execution support for others. This plan records the gaps so coding agents can work through them in coherent slices rather than treating the matrix drift as one giant amorphous task.

The deleted `design/DECENTDB_FUTURE_WINS.md` previously carried a handful of near-term ideas that were actually implementation-gap work rather than long-range product bets. Those actionable items now live here: JSON operators and table functions, date/time builtins, UUID generation, and planner statistics / `ANALYZE`.

## Important documentation drift inside the repository

There is already internal documentation drift, not just code drift.

The older baseline spec in `design/SPEC.md` still describes a much smaller SQL 1.0 surface: non-recursive CTEs only, `ROW_NUMBER()` as the only window function subset, `LEFT` and `INNER` joins only, and `INTERSECT ALL` / `EXCEPT ALL` explicitly unsupported (`design/SPEC.md:327-383`).

The newer design-facing document in `docs/design/spec.md` and the user-facing matrix both advertise a much broader surface, including `WITH RECURSIVE`, additional window functions, more join types, `DISTINCT ON`, JSON table functions, generated columns, temp objects, and many more scalar/date/time functions (`docs/design/spec.md:298-316`, `docs/user-guide/sql-feature-matrix.md:123-128`, `167-169`, `236-337`, `627-629`).

That means some work below is true implementation backlog, and some work is repository-wide documentation alignment backlog. This document assumes the matrix wins and therefore treats the code as needing to catch up.

## High-confidence implemented areas

These areas look real enough to avoid treating them as immediate backlog.

### Core DDL and DML are present

The AST and normalization layers include `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `CREATE TRIGGER`, `DROP TABLE`, `DROP INDEX`, `DROP VIEW`, `DROP TRIGGER`, and `ALTER TABLE` actions (`crates/decentdb/src/sql/ast.rs:7-42`, `287-403`; `crates/decentdb/src/sql/normalize.rs:31-60`, `325-381`, `498-565`, `585-725`, `727-779`).

Execution support exists for views, triggers, and DML paths (`crates/decentdb/src/exec/views.rs:11-98`, `crates/decentdb/src/exec/triggers.rs:10-155`, `crates/decentdb/src/exec/mod.rs:791-851`; `crates/decentdb/src/exec/dml.rs`).

### Insert-returning and upsert support are real

`InsertStatement` carries both `on_conflict` and `returning` (`crates/decentdb/src/sql/ast.rs:234-265`), and normalization preserves both (`crates/decentdb/src/sql/normalize.rs:160-245`). Execution code in `exec/dml.rs` contains both `ON CONFLICT` handling and `RETURNING` handling, including `EXCLUDED` scope checks (`crates/decentdb/src/exec/dml.rs:450-574`, `801-877`; `crates/decentdb/src/exec/mod.rs:4524-4568`).

### Constraints and foreign keys are not merely parsed

The catalog schema models table-level and column-level foreign keys, checks, uniqueness, primary keys, defaults, and nullability (`crates/decentdb/src/catalog/schema.rs:46-81`, `107-114`).

Execution code enforces foreign keys during DDL validation and DML mutation paths (`crates/decentdb/src/exec/ddl.rs:705-761`, `crates/decentdb/src/exec/constraints.rs:152-188`, `crates/decentdb/src/exec/dml.rs:928-1067`).

### Savepoints and BEGIN IMMEDIATE / EXCLUSIVE aliases are real

Transaction control is handled outside the SQL AST path in `db.rs`, and the control parser explicitly accepts `BEGIN IMMEDIATE`, `BEGIN EXCLUSIVE`, `SAVEPOINT`, `RELEASE SAVEPOINT`, and `ROLLBACK TO SAVEPOINT` (`crates/decentdb/src/db.rs:2028-2066`).

### A meaningful query subset already works

The current query AST and executor support:

- non-recursive `WITH` CTEs (`crates/decentdb/src/sql/ast.rs:51-83`, `crates/decentdb/src/sql/normalize.rs:1430-1456`, `crates/decentdb/src/exec/mod.rs:1119-1212`)
- `INNER JOIN` and `LEFT JOIN` (`crates/decentdb/src/sql/ast.rs:102-125`, `crates/decentdb/src/sql/normalize.rs:818-865`)
- `GROUP BY`, `HAVING`, and basic aggregates (`crates/decentdb/src/sql/ast.rs:86-93`, `crates/decentdb/src/exec/mod.rs:3935-4308`)
- set operations in baseline form (`crates/decentdb/src/sql/normalize.rs:95-123`, `crates/decentdb/src/exec/mod.rs:3518-3583`)
- a partial window-function surface (`crates/decentdb/src/sql/normalize.rs:1192-1240`, `crates/decentdb/src/exec/mod.rs:3585-3933`)

## High-confidence mismatches against the matrix

These are the clearest places where documented `✅` support is ahead of what the Rust rewrite actually does.

### 1. `WITH RECURSIVE` is explicitly rejected

The matrix marks `WITH RECURSIVE` as supported (`docs/user-guide/sql-feature-matrix.md:627-629`).

The normalizer explicitly rejects it:

- `crates/decentdb/src/sql/normalize.rs:1430-1436`
- parser rejection coverage: `crates/decentdb/src/sql/parser_tests.rs:25-35`

This is not a hidden executor limitation or a missing test. It is an explicit unsupported path today.

### 2. Join support is currently only `INNER` and `LEFT`

The matrix documents `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `CROSS`, and `NATURAL` joins as supported (`docs/user-guide/sql-feature-matrix.md:123-128`).

The current AST only models two join kinds:

- `JoinKind::Inner`
- `JoinKind::Left`

See `crates/decentdb/src/sql/ast.rs:120-124`.

The normalizer also rejects any other libpg_query join type:

- `crates/decentdb/src/sql/normalize.rs:848-859`

That means `RIGHT`, `FULL OUTER`, `CROSS`, and `NATURAL` should be treated as missing, not partially supported.

### 3. `DISTINCT ON` is not represented in the AST

The matrix marks `DISTINCT ON` as supported (`docs/user-guide/sql-feature-matrix.md:167`).

The current `Select` struct carries only a boolean `distinct` flag, not a `DISTINCT ON` expression list (`crates/decentdb/src/sql/ast.rs:86-93`).

The normalizer collapses any non-empty `distinct_clause` into that single boolean (`crates/decentdb/src/sql/normalize.rs:150-157`).

That means the Rust rewrite currently has ordinary `DISTINCT`, but not PostgreSQL-style `DISTINCT ON`.

### 4. Window support is only a documented subset, not the full matrix

The matrix marks the following as supported (`docs/user-guide/sql-feature-matrix.md:236-243`):

- `ROW_NUMBER()`
- `RANK()`
- `DENSE_RANK()`
- `LAG()`
- `LEAD()`
- `FIRST_VALUE()`
- `LAST_VALUE()`
- `NTH_VALUE()`

The normalizer allows only:

- `ROW_NUMBER()`
- `RANK()`
- `DENSE_RANK()`
- `LAG()`
- `LEAD()`

and explicitly rejects everything else with `OVER (...)` (`crates/decentdb/src/sql/normalize.rs:1199-1206`).

Execution support mirrors that subset. `compute_window_function_values` implements `rank`, `dense_rank`, `lag`, and `lead`, and errors on any other window-function name (`crates/decentdb/src/exec/mod.rs:3831-3928`).

So the window row in the matrix is partially true, but only for five of the eight documented functions.

### 5. Aggregate-function coverage is narrower than documented

The matrix marks these as supported (`docs/user-guide/sql-feature-matrix.md:196-207`):

- `COUNT(*)`
- `COUNT(expr)`
- `SUM`
- `AVG`
- `MIN`
- `MAX`
- `GROUP_CONCAT`
- `STRING_AGG`
- `TOTAL`
- distinct aggregate variants

The normalizer only recognizes aggregate names:

- `count`
- `sum`
- `avg`
- `min`
- `max`
- `group_concat`

See `crates/decentdb/src/sql/normalize.rs:1241-1250`.

Grouped execution implements the same set and errors on any other aggregate name (`crates/decentdb/src/exec/mod.rs:4075-4136`).

That means:

- `STRING_AGG` is missing
- `TOTAL` is missing
- `COUNT(DISTINCT)`, `SUM(DISTINCT)`, and `AVG(DISTINCT)` are present through the existing distinct-aggregate paths

### 6. `INTERSECT ALL` and `EXCEPT ALL` are not yet true multiset semantics

The matrix marks `INTERSECT ALL` and `EXCEPT ALL` as supported (`docs/user-guide/sql-feature-matrix.md:598-601`).

The AST does carry the `all` flag for set operations (`crates/decentdb/src/sql/ast.rs:61-76`, `crates/decentdb/src/sql/normalize.rs:95-123`), but the executor uses presence-based membership checks for `INTERSECT` and `EXCEPT` (`crates/decentdb/src/exec/mod.rs:3541-3580`).

That implementation is adequate for plain `INTERSECT` and `EXCEPT`, but it does not consume matching rows from the right-hand side, so it does not deliver correct multiset semantics for the `ALL` forms.

This should be treated as a semantic bug relative to the matrix, not as a merely missing parser hook.

### 7. Scalar function coverage is much smaller than the matrix claims

The matrix advertises broad math, string, date/time, and JSON scalar support (`docs/user-guide/sql-feature-matrix.md:275-337`).

The current scalar dispatch in `exec/mod.rs` only exposes:

- `coalesce`
- `nullif`
- `lower`
- `upper`
- `trim`
- `length`
- `substr`
- `replace`
- `instr`
- `json_array_length`
- `json_extract`

See `crates/decentdb/src/exec/mod.rs:4693-4806`.

That leaves large documented gaps:

- math functions such as `ABS`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POWER`, `MOD`, `SIGN`, `LN`, `LOG`, `EXP`, `RANDOM`
- string functions such as `LTRIM`, `RTRIM`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPEAT`, `REVERSE`, `CHR`, `HEX`
- date/time functions such as `NOW`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `date()`, `datetime()`, `strftime()`, `EXTRACT()`
- JSON scalar helpers such as `json_type`, `json_valid`, `json_object`, `json_array`

### 8. JSON operators and JSON table functions are not implemented end-to-end

The matrix marks `->`, `->>`, `json_each()`, and `json_tree()` as supported (`docs/user-guide/sql-feature-matrix.md:333-336`).

The current expression operator normalization only handles:

- comparison operators
- arithmetic operators `+`, `-`, `*`, `/`
- concatenation `||`

See `crates/decentdb/src/sql/normalize.rs:1033-1168`.

There is no `->` or `->>` operator case in the AST binary-op mapping, and the scalar-function dispatcher does not include `json_each` or `json_tree` (`crates/decentdb/src/exec/mod.rs:4693-4806`).

`parser_tests.rs` also explicitly labels set-returning functions as out of baseline scope (`crates/decentdb/src/sql/parser_tests.rs:27-31`), which is consistent with `json_each()` / `json_tree()` not yet being real `FROM`-clause table functions.

### 9. Generated columns appear absent from the schema model

The matrix marks stored generated columns as supported (`docs/user-guide/sql-feature-matrix.md:25`, `68-74`).

The current `ColumnDefinition` carries:

- `name`
- `column_type`
- `nullable`
- `default`
- `primary_key`
- `unique`
- `checks`
- `references`

See `crates/decentdb/src/sql/ast.rs:295-305`.

The persisted catalog `ColumnSchema` carries:

- type
- nullability
- default SQL
- primary-key / unique / auto-increment flags
- checks
- optional foreign key

See `crates/decentdb/src/catalog/schema.rs:71-81`.

There is no obvious generated-expression field in either structure, and there is no separate recomputation path in the audited DML execution code. Until runtime evidence proves otherwise, generated columns should be treated as missing.

### 10. Temp-object semantics appear absent from the catalog model

The matrix marks `CREATE TEMP TABLE` and `CREATE TEMP VIEW` as supported and session-scoped (`docs/user-guide/sql-feature-matrix.md:23-24`, `64-66`).

The catalog state stores only global maps of:

- tables
- indexes
- views
- triggers

See `crates/decentdb/src/catalog/schema.rs:148-154`.

There is no visible temp-object namespace, persistence flag, or session-only object container in the audited catalog structures. The normalization and DDL paths we inspected also do not preserve a temp/persistence bit.

This does not prove the syntax is rejected, but it is strong evidence that true session-scoped temp semantics are not yet implemented.

### 11. `ALTER TABLE` works, but the matrix overstates its breadth

The matrix marks the major alter-table variants as supported (`docs/user-guide/sql-feature-matrix.md:15-18`).

The executor supports those action families, but with important restrictions:

- tables with `CHECK` constraints reject `ALTER TABLE` (`crates/decentdb/src/exec/ddl.rs:391-395`)
- tables with expression indexes reject `ALTER TABLE` (`crates/decentdb/src/exec/ddl.rs:397-406`)
- `ADD COLUMN` rejects `PRIMARY KEY`, `UNIQUE`, and `REFERENCES` (`crates/decentdb/src/exec/ddl.rs:421-427`)
- `RENAME COLUMN` is rejected when dependent views exist (`crates/decentdb/src/exec/ddl.rs:521-525`)
- `ALTER COLUMN TYPE` only supports `INT64`, `FLOAT64`, `TEXT`, and `BOOL` transitions (`crates/decentdb/src/exec/ddl.rs:550-565`)

The correct classification here is partial support, not blanket support.

### 12. Date/type semantics need deliberate cleanup

The catalog has `Int64`, `Float64`, `Text`, `Bool`, `Blob`, `Decimal`, `Uuid`, and `Timestamp` types (`crates/decentdb/src/catalog/schema.rs:18-43`), and runtime values also carry `Decimal`, `Uuid`, and `TimestampMicros` (`crates/decentdb/src/record/value.rs:7-18`).

However, type normalization maps both `date` and `datetime` to `ColumnType::Timestamp` (`crates/decentdb/src/sql/normalize.rs:1374-1393`).

This is good enough for “typed temporal storage exists,” but it is not the same as having separate `DATE` and `TIMESTAMP` semantics across the SQL surface. The matrix itself is also internally inconsistent here: the type table says native microsecond UTC storage for `DATE` / `TIMESTAMP`, while the example comment later says date/timestamp values are stored as ISO-format text (`docs/user-guide/sql-feature-matrix.md:516-517`, `540-546`).

The implementation slice should resolve actual behavior and then add tests that make the matrix claim precise.

## Additional gaps surfaced by other `docs/user-guide/` pages

The matrix was not the only place where the docs got ahead of the Rust rewrite. Reviewing the rest of `docs/user-guide/` surfaced several additional SQL claims that either are not implemented, are only partially implemented, or need direct verification before they can be treated as true.

### 13. `ANALYZE` is documented in `sql-reference.md`, but no Rust SQL statement support was found

`docs/user-guide/sql-reference.md` documents `ANALYZE;` and `ANALYZE table_name;` as supported SQL commands and even describes transaction behavior for them (`docs/user-guide/sql-reference.md:279-291`).

However:

- `Statement` in the Rust AST has no `Analyze` variant (`crates/decentdb/src/sql/ast.rs:7-42`)
- `normalize_statement` handles `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, DDL, `EXPLAIN`, and triggers, but there is no `AnalyzeStmt` handling (`crates/decentdb/src/sql/normalize.rs:31-65`)
- repository-wide Rust code search did not find an execution path for SQL `ANALYZE`

This is a user-guide gap beyond the matrix and should be treated as a missing documented SQL feature.

### 14. `EXPLAIN (ANALYZE)` is documented, but current detection appears too narrow

`sql-reference.md` says both `EXPLAIN ANALYZE ...` and the parenthesized form `EXPLAIN (ANALYZE) ...` are supported (`docs/user-guide/sql-reference.md:597-605`).

The current normalization code derives the analyze flag with a raw string check:

- `original_sql.to_ascii_uppercase().contains("EXPLAIN ANALYZE")`

See `crates/decentdb/src/sql/normalize.rs:572-574`.

That is sufficient for `EXPLAIN ANALYZE ...`, but it strongly suggests the parenthesized form is not recognized as analyzed execution. This should be treated as an incomplete documented feature until verified with a direct regression test.

### 15. `CREATE VIEW IF NOT EXISTS` is documented, but the current view model does not expose it

`sql-reference.md` documents:

- `CREATE VIEW IF NOT EXISTS view_name AS SELECT ...;`

See `docs/user-guide/sql-reference.md:157-167`.

The current Rust view AST models:

- `view_name`
- `replace`
- `column_names`
- `query`

See `crates/decentdb/src/sql/ast.rs:360-365`.

`normalize_create_view` preserves `replace`, but there is no `if_not_exists` flag carried into the AST or executor (`crates/decentdb/src/sql/normalize.rs:544-565`), and the execution path only distinguishes ordinary create from replace semantics (`crates/decentdb/src/exec/views.rs:12-20`).

Until directly proven otherwise, `CREATE VIEW IF NOT EXISTS` should be treated as a documented gap.

### 16. Partial-index documentation is broader than the current executor restrictions

The user guide currently says:

- `sql-reference.md`: partial/filtered indexes are supported for BTREE indexes with arbitrary predicates, including multi-column and `UNIQUE` forms (`docs/user-guide/sql-reference.md:67-73`)
- `indexes.md`: partial/filtered indexes are supported for BTREE indexes with arbitrary predicates (`docs/user-guide/indexes.md:52-72`)

The executor is narrower:

- partial trigram indexes are rejected (`crates/decentdb/src/exec/ddl.rs:218-229`)
- partial expression indexes are rejected (`crates/decentdb/src/exec/ddl.rs:231-245`)
- only single-column BTREE partial indexes are supported (`crates/decentdb/src/exec/ddl.rs:247-251`)
- the indexed key for a partial index must be a plain column, not an expression (`crates/decentdb/src/exec/ddl.rs:253-258`)

This means the docs currently overstate the supported partial-index surface, especially for multi-column forms and any path that implies expression-key support.

### 17. `sql-reference.md` view notes inherit the `DISTINCT ON` gap

The `CREATE VIEW` section says views may include `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT/OFFSET`, and `DISTINCT ON` (`docs/user-guide/sql-reference.md:178-184`).

That note is too broad because `DISTINCT ON` is not represented in the current `Select` AST and is therefore not a real end-to-end feature today (`crates/decentdb/src/sql/ast.rs:86-93`, `crates/decentdb/src/sql/normalize.rs:150-157`).

### 18. `comparison.md` duplicates many unsupported claims and should not be treated as independent evidence

`docs/user-guide/comparison.md` repeats many of the same broad claims as the matrix, including:

- recursive CTEs (`docs/user-guide/comparison.md:51-52`, `82`)
- broader join support (`docs/user-guide/comparison.md:47-48`, `93`)
- `DISTINCT ON` (`docs/user-guide/comparison.md:48`, `94`)
- date/time functions (`docs/user-guide/comparison.md:56`, `87`)
- JSON table functions and JSON operators (`docs/user-guide/comparison.md:57-58`, `90`)
- generated columns and temp objects (`docs/user-guide/comparison.md:59`, `95-97`)
- UUID helper functions and `PRINTF` (`docs/user-guide/comparison.md:60`)

These are largely repeats of already-audited matrix drift, but they matter because the comparison page presents them as baseline-supported user-facing capabilities. Future documentation review should not treat duplicate claims across pages as corroboration.

### 19. `data-types.md` relies on non-implemented function support

`docs/user-guide/data-types.md` uses SQL examples that rely on currently missing or unverified function support, including:

- `DEFAULT GEN_RANDOM_UUID()` on `UUID` columns (`docs/user-guide/data-types.md:106-117`)
- `DEFAULT CURRENT_TIMESTAMP` (`docs/user-guide/data-types.md:119-145`)
- `NOW()` inserts (`docs/user-guide/data-types.md:140-141`)
- `EXTRACT(YEAR FROM created_at)` (`docs/user-guide/data-types.md:143-145`)

The current scalar dispatcher does not expose `GEN_RANDOM_UUID`, `CURRENT_TIMESTAMP`, `NOW`, or `EXTRACT` (`crates/decentdb/src/exec/mod.rs:4693-4806`), so the type docs currently imply a broader SQL runtime than exists.

## Areas that still need direct verification

These are not good enough to call implemented, but they also are not as cleanly disproven as the items above.

### `LIMIT ALL` and SQL-standard `OFFSET ... FETCH`

The matrix marks both as supported (`docs/user-guide/sql-feature-matrix.md:168-169`).

The normalized query model only stores `limit_count` and `limit_offset` expressions (`crates/decentdb/src/sql/normalize.rs:68-92`), which may be enough if libpg_query lowers `FETCH` into that form, but there is no dedicated AST shape or explicit regression coverage in the audited files.

This needs direct execution tests before it is promoted to “implemented.”

### `CREATE TEMP TABLE` / `CREATE TEMP VIEW`

Syntax may parse through libpg_query, but true session-only behavior is not evident in the catalog or storage model. This needs direct runtime tests plus implementation work if the behavior proves persistent.

## Recommended implementation order

The slices below are structured for coding agents. They are intentionally grouped by cohesion and blast radius, not by optimism.

## Slice 0 - Audit capture and implementation plan

**Status:** Completed

### Goal

Capture the current mismatch between the matrix and the Rust implementation in one durable place and turn it into coding slices.

### Done

- audited the matrix against parser, normalization, execution, catalog, and existing tests
- identified high-confidence implemented areas
- identified high-confidence missing or partial areas
- split the work into slices below

## Slice 1 - Query-semantic corrections close to the current engine

**Status:** Not started

### Goal

Close the smallest high-confidence matrix gaps that are already near existing executor machinery.

### Scope

- `STRING_AGG`
- `TOTAL`
- true multiset semantics for `INTERSECT ALL`
- true multiset semantics for `EXCEPT ALL`

### Why this slice comes first

These items live inside code that already exists:

- aggregate dispatch is already centralized in `crates/decentdb/src/exec/mod.rs:4075-4136`
- grouped aggregate helpers already exist (`crates/decentdb/src/exec/mod.rs:4592-4900`)
- set-operation execution already exists (`crates/decentdb/src/exec/mod.rs:3518-3583`)

This slice does not need catalog-format changes, temp-object semantics, or recursive query infrastructure.

### Concrete tasks

- add `string_agg` aggregate normalization next to existing aggregate names
- implement `STRING_AGG` behavior, likely by sharing or refactoring the existing `GROUP_CONCAT` path
- implement `TOTAL` semantics so empty sets return `0.0` instead of `NULL`
- change `INTERSECT ALL` and `EXCEPT ALL` to use counted row identities instead of presence-only membership
- add focused regression tests that exercise duplicates, empty inputs, and mixed numeric types

### Acceptance

- the matrix rows for `STRING_AGG`, `TOTAL`, `INTERSECT ALL`, and `EXCEPT ALL` have direct executor tests
- `INTERSECT` / `EXCEPT` non-`ALL` behavior remains unchanged

## Slice 2 - DISTINCT and pagination parity

**Status:** Not started

### Goal

Bring the distinct/pagination rows in the matrix in line with the actual query model.

### Scope

- `DISTINCT ON`
- `LIMIT ALL`
- `OFFSET ... FETCH`

### Concrete tasks

- extend `Select` to preserve `DISTINCT ON` expressions instead of collapsing everything to a boolean `distinct`
- implement order-sensitive “keep first row per distinct key” semantics
- add direct runtime verification for `LIMIT ALL`
- verify whether libpg_query already lowers `FETCH` into the current limit/offset form; if not, preserve explicit `FETCH` semantics in normalization
- add regression tests that prove row order behavior

### Acceptance

- `DISTINCT ON` round-trips through AST and executes correctly with `ORDER BY`
- `LIMIT ALL` and `OFFSET ... FETCH` have direct coverage

## Slice 3 - Window parity completion

**Status:** Not started

### Goal

Close the gap between the five currently supported window functions and the eight documented in the matrix.

### Scope

- `FIRST_VALUE`
- `LAST_VALUE`
- `NTH_VALUE`

### Concrete tasks

- remove the explicit normalizer rejection for these names
- extend `compute_window_function_values` to evaluate them on sorted partitions
- define behavior for out-of-range `NTH_VALUE`
- add regression tests for partitioned and non-partitioned windows
- add coverage that proves the already-supported `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, and `LEAD` behavior remains intact

### Acceptance

- all eight matrix-listed window functions have direct execution coverage

## Slice 4 - Join surface parity

**Status:** Not started

### Goal

Expand the query engine beyond the current `INNER`/`LEFT` subset.

### Scope

- `CROSS JOIN`
- `RIGHT JOIN`
- `FULL OUTER JOIN`
- `NATURAL JOIN`

### Concrete tasks

- extend `JoinKind` beyond `Inner` / `Left`
- preserve the required normalization details for each join form
- implement executor behavior for cartesian, right-preserving, full outer, and natural-column-matching joins
- add tests for duplicate column names, null-extended rows, and empty-side behavior

### Acceptance

- each documented join type has a direct execution test, not just parser acceptance

## Slice 5 - Recursive CTE support

**Status:** Not started

### Goal

Turn `WITH RECURSIVE` from an explicit rejection into a real feature.

### Scope

- recursive flag preservation in normalization/AST
- fixpoint execution
- recursion guardrails

### Concrete tasks

- stop rejecting `clause.recursive`
- introduce a recursive CTE execution path distinct from the current non-recursive CTE materialization path
- implement the documented iteration bound and failure behavior
- add tests for sequence generation and tree traversal

### Acceptance

- the canonical recursive examples from the matrix work end-to-end
- recursion-limit behavior is tested and explicit

## Slice 6 - Scalar, date/time, and JSON scalar function expansion

**Status:** Not started

### Goal

Reduce the largest user-visible mismatch: the function tables in the matrix are far ahead of `exec/mod.rs`.

### Scope

- math functions
- missing string functions
- date/time functions
- UUID helper functions
- missing JSON scalar functions and operators

### Concrete tasks

- implement the matrix-listed math functions in the scalar dispatcher
- add missing string helpers such as `LTRIM`, `RTRIM`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPEAT`, `REVERSE`, `CHR`, `HEX`
- add date/time entry points such as `NOW`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `date()`, `datetime()`, `strftime()`, `EXTRACT()`
- add UUID helper entry points such as `GEN_RANDOM_UUID`, `UUID_PARSE`, and `UUID_TO_STRING`
- add JSON scalar helpers such as `json_type`, `json_valid`, `json_object`, `json_array`
- add `->` and `->>` operator support in normalization and execution
- add tests that pin coercion and null semantics

### Acceptance

- the function tables in the matrix are backed by executable tests rather than grep-based confidence

## Slice 7 - JSON table functions

**Status:** Not started

### Goal

Implement `json_each()` and `json_tree()` as real `FROM`-clause table functions.

### Scope

- parser/normalization support for range functions in `FROM`
- row-shape definition for key/value/type/path columns
- executor support for expanding JSON into row sets

### Why this is separate from Slice 6

`json_each()` and `json_tree()` are not scalar functions. They require table-producing semantics and therefore touch `FROM` handling, not just expression evaluation.

### Acceptance

- the matrix examples that project `key`, `value`, and `type` from these functions execute as written

## Slice 8 - Planner statistics and `ANALYZE`

**Status:** Not started

### Goal

Add the missing SQL `ANALYZE` surface and the underlying planner statistics needed to make the cost-based optimizer less heuristic-driven.

### Scope

- SQL `ANALYZE` statement support
- catalog-backed table and index statistics
- planner integration for cardinality/selectivity estimates

### Why this slice belongs here

`ANALYZE` is already documented in the user guide, but there is no Rust AST, normalization path, or execution support for it today. This is both a documentation-gap fix and a meaningful optimizer-quality improvement.

### Concrete tasks

- add an `ANALYZE` statement shape to the SQL AST and normalization pipeline
- define a persisted statistics format that fits the current catalog/versioning constraints
- collect table and index statistics through `ANALYZE` execution
- teach the planner to consume the new statistics for row-count and selectivity estimates
- add regression coverage for both statement behavior and plan-shape changes where stable enough to assert

### Acceptance

- `ANALYZE` executes end-to-end with direct tests
- planner statistics persist and reload correctly
- the optimizer consults the collected statistics instead of relying only on fixed heuristics

## Slice 9 - Generated columns and temp objects

**Status:** Not started

### Goal

Implement the catalog and execution semantics for two major DDL features that currently appear absent from the schema model.

### Scope

- stored generated columns
- session-scoped temp tables and views

### Concrete tasks

- extend the column schema model to carry generated-column expressions and/or flags
- recompute stored generated values on insert and update
- extend catalog/runtime structures to distinguish temp objects from persisted ones
- ensure temp objects never survive reopen/checkpoint boundaries
- add tests covering generated expression recomputation and temp object lifetime

### Acceptance

- generated columns persist computed values correctly
- temp objects are session-scoped and not durable

## Slice 10 - Matrix regression harness

**Status:** Not started

### Goal

Prevent this drift from reappearing.

### Scope

- one executable check per documented claim, or one well-scoped test per matrix row family

### Concrete tasks

- create sectioned regression tests that mirror the matrix headings
- encode both positive coverage for documented `✅` rows and explicit negative coverage where the code still intentionally lags
- require new matrix claims to land with tests

### Acceptance

- future feature-matrix edits can be validated against code, not just reviewed manually

## Documentation follow-up tasks for implemented but under-documented features

These are not implementation slices. They are documentation tasks that should happen after the feature-gap audit is reconciled enough that the user-guide can be updated confidently.

### Doc task A - Document `IS DISTINCT FROM` / `IS NOT DISTINCT FROM`

The Rust SQL pipeline already supports these operators:

- AST operators: `BinaryOp::IsDistinctFrom` / `BinaryOp::IsNotDistinctFrom` (`crates/decentdb/src/sql/ast.rs:216-231`)
- normalization from libpg_query distinct predicates (`crates/decentdb/src/sql/normalize.rs:1036-1057`)
- executor semantics (`crates/decentdb/src/exec/mod.rs:5120-5125`)

But the user-guide predicate/operator sections only list ordinary comparison operators (`docs/user-guide/sql-reference.md:297-305`). Add a follow-up doc update task for `docs/user-guide/sql-reference.md` to describe these operators and their null semantics.

### Doc task B - Document `IN (subquery)` and scalar subqueries

The Rust SQL pipeline supports:

- `IN (subquery)` via `Expr::InSubquery` (`crates/decentdb/src/sql/ast.rs:155-164`, `crates/decentdb/src/sql/normalize.rs:1315-1334`, `crates/decentdb/src/exec/mod.rs:4393-4422`)
- scalar subqueries via `Expr::ScalarSubquery` (`crates/decentdb/src/sql/ast.rs:165-166`, `crates/decentdb/src/sql/normalize.rs:1341-1341`, `crates/decentdb/src/exec/mod.rs:4423-4435`)

The user-guide currently documents value-list `IN (...)` and `EXISTS (SELECT ...)`, but not these other subquery forms (`docs/user-guide/sql-reference.md:303-305`). Add a follow-up doc update task for `docs/user-guide/sql-reference.md`.

### Doc task C - Consider documenting plain `EXPLAIN` scope beyond `SELECT`

The current explain path is represented as a general `Statement::Explain` wrapper (`crates/decentdb/src/sql/ast.rs:7-47`, `crates/decentdb/src/sql/normalize.rs:46-49`, `crates/decentdb/src/exec/mod.rs:880-905`), and plain `EXPLAIN` is not restricted to queries in the same way `EXPLAIN ANALYZE` is.

The user guide currently only shows `EXPLAIN SELECT ...` examples (`docs/user-guide/sql-reference.md:589-605`). If plain `EXPLAIN` on mutating statements is intentionally supported and stable, add a follow-up doc update task to document that scope explicitly.

## Suggested next coding move

If work starts immediately after this document, start with **Slice 1**.

It has the best combination of:

- high confidence in the gap
- low architectural blast radius
- direct user-visible improvement
- strong fit with code that already exists

After Slice 1, move to **Slice 6**, **Slice 7**, or **Slice 8** if the priority is to close the most user-visible documentation gaps first. Then tackle the larger structural slices (`Join` parity, recursive CTEs, temp/generated semantics).

