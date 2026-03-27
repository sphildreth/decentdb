# Complete Remaining SQL Enhancement Gaps

**Purpose:** Focused implementation prompt for the small set of SQL enhancement gaps that still prevent `DECENTDB_SQL_ENHANCEMENTS.md` from being truthfully treated as complete and removable.

**Target Agent:** GitHub Copilot or similar coding agent working directly in this repository.

## Objective

Implement the remaining missing functionality and validation needed so the SQL enhancement roadmap can be retired instead of maintained as an active requirements document.

Do **not** re-implement slices that are already present and tested. Concentrate only on the verified gaps below.

## Required Reading

Before coding, read:

- `design/DECENTDB_SQL_ENHANCEMENTS.md`
- `design/PRD.md`
- `design/TESTING_STRATEGY.md`
- `.github/instructions/rust.instructions.md`
- `.github/copilot-instructions.md`
- `AGENTS.md`

## Verified Gaps To Close

These are the gaps confirmed by code review and targeted test runs.

### 1. S3 is not actually complete because `TRUNCATE TABLE` is not proven complete

The roadmap marks S3 complete, but the current code only shows a partial internal path:

- Slice status claims completion in `design/DECENTDB_SQL_ENHANCEMENTS.md`
- AST has `TruncateTable { table_name, restart_identity }` in `crates/decentdb/src/sql/ast.rs`
- Executor has `execute_truncate_table(...)` in `crates/decentdb/src/exec/ddl.rs`
- I did **not** find corresponding SQL normalization coverage or end-to-end SQL tests for `TRUNCATE TABLE`
- Current executor rejects referenced tables instead of supporting the documented `CASCADE` example

#### Required implementation work

Implement full SQL-surface support for:

- `TRUNCATE TABLE table_name`
- `TRUNCATE TABLE table_name RESTART IDENTITY`
- `TRUNCATE TABLE table_name CONTINUE IDENTITY`
- `TRUNCATE TABLE table_name CASCADE`

If `CASCADE` requires substantial extra work, implement it rather than weakening the requirement. The goal is to make the document deletable because the documented behavior exists in code.

#### Required validation

Add end-to-end tests for:

- basic truncate clears all rows
- `RESTART IDENTITY` resets generated row ids / identity behavior
- `CONTINUE IDENTITY` preserves identity progression
- truncate participates correctly in transaction rollback
- truncate on referenced tables without `CASCADE` errors clearly
- truncate with `CASCADE` removes dependent rows or otherwise applies the documented semantics consistently
- truncate rejects unsupported targets such as views/temp tables if that remains the intended behavior

### 2. S9 overstates `VALUES` support

Top-level `VALUES (...)` queries already work, but the roadmap explicitly documents **VALUES as a table source**:

- `FROM (VALUES (...), (...)) AS alias(col1, col2)`
- `JOIN (VALUES ...) ...`
- `INSERT ... SELECT * FROM (VALUES ...) ...`
- row-value comparisons using `IN (VALUES ...)`

Current evidence:

- `QueryBody::Values(...)` exists in `crates/decentdb/src/sql/normalize.rs`
- top-level `VALUES` is tested in `crates/decentdb/tests/sql_expressions_tests.rs`
- `FromItem` in `crates/decentdb/src/sql/ast.rs` has no dedicated `VALUES` table-source variant

#### Required implementation work

Extend the parser / AST / normalization / execution stack so `VALUES` works as a true table source in the forms documented by the roadmap.

At minimum support:

- `SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(num, name)`
- joining a real table against `(VALUES ...)`
- `INSERT INTO ... SELECT * FROM (VALUES ...) AS ...`
- tuple comparison form `WHERE (id, category) IN (VALUES (...), (...))`

Do not regress existing top-level `VALUES` behavior.

#### Required validation

Add integration tests covering each documented form above.

### 3. S11 overstates `ALTER TABLE ADD/DROP CONSTRAINT`

The roadmap marks S11 complete, but runtime behavior is still intentionally partial:

- `ALTER TABLE ADD CONSTRAINT` currently supports only `CHECK`
- current runtime error text in `crates/decentdb/src/exec/ddl.rs` says exactly that
- existing tests in `crates/decentdb/tests/sql_ddl_constraints_tests.rs` lock in that limited behavior

The roadmap documents generic `ALTER TABLE ... ADD CONSTRAINT ...` and gives examples for:

- `FOREIGN KEY`
- `CHECK`
- dropping a named constraint

#### Required implementation work

Implement enough `ALTER TABLE ADD/DROP CONSTRAINT` support to make the roadmap section truthful without qualifiers.

At minimum, add support for:

- adding named `FOREIGN KEY` constraints
- dropping named foreign-key constraints
- preserving existing named `CHECK` add/drop behavior

If the parser and catalog model make it straightforward, also support named `UNIQUE` constraints so the DDL slice is not left half-generic.

This work must handle:

- validation of existing rows before adding a foreign key or unique/check constraint
- catalog persistence and reload correctness
- dependent index behavior when needed
- clear errors for invalid existing data and duplicate constraint names

#### Required validation

Add integration tests for:

- adding a named foreign key to clean existing data
- rejecting addition when existing rows violate the foreign key
- dropping a named foreign key and verifying enforcement is removed
- persistence / reopen behavior for newly added constraints
- interaction with parent deletes / updates for supported FK actions

## Optional But Strongly Recommended Cleanup

Once the code gaps are closed, clean up the roadmap itself so it can be deleted or archived cleanly:

- remove or rewrite sections that still say "Missing Features" for now-implemented items
- ensure the slice table matches actual engine behavior
- update any user-facing SQL feature docs and changelog entries

## Files Likely To Change

Expect to touch some combination of:

- `crates/decentdb/src/sql/ast.rs`
- `crates/decentdb/src/sql/parser.rs`
- `crates/decentdb/src/sql/normalize.rs`
- `crates/decentdb/src/exec/ddl.rs`
- `crates/decentdb/src/exec/mod.rs`
- `crates/decentdb/tests/sql_expressions_tests.rs`
- `crates/decentdb/tests/sql_ddl_constraints_tests.rs`
- `crates/decentdb/tests/sql_subqueries_ctes_tests.rs`
- `CHANGELOG.md`
- SQL feature documentation under `docs/`

## Constraints

- Keep the changes incremental and explicit.
- Do not add new dependencies unless clearly necessary and already allowed by repository policy.
- Do not change unrelated SQL behavior while doing this work.
- Preserve ABI and on-disk compatibility unless a documented ADR is required.
- Prefer extending existing parser / planner / executor patterns rather than introducing parallel abstractions.

## Definition Of Done

This prompt is complete only when all of the following are true:

1. The documented `TRUNCATE TABLE` behavior exists end-to-end and is tested.
2. `VALUES` works as a documented table source, not just as a top-level query body, and is tested.
3. `ALTER TABLE ADD/DROP CONSTRAINT` supports the documented constraint forms strongly enough that S11 can be called complete without caveats.
4. `cargo fmt --check`, `cargo check`, and `cargo clippy --all-targets --all-features -- -D warnings` pass.
5. Targeted integration tests for each new behavior pass.
6. `design/DECENTDB_SQL_ENHANCEMENTS.md` can be updated, archived, or deleted without leaving false claims behind.

## Suggested Validation Commands

Run at least:

```bash
cargo fmt --check
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test -p decentdb --test sql_expressions_tests
cargo test -p decentdb --test sql_ddl_constraints_tests
cargo test -p decentdb --test sql_subqueries_ctes_tests
```

If the implementation adds dedicated test names or new test files, run those directly while iterating.

## Deliverable

Produce a single cohesive change set that closes the verified gaps above. When done, include a short summary of:

- what was implemented
- which tests were added
- what validation was run
- whether the roadmap document can now be retired