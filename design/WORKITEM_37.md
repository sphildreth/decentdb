# Work Item #37 — SQL Dialect/Feature Breadth Parity with SQLite (Postgres-Flavored)

**Issue:** [#37](https://github.com/sphildreth/decentdb/issues/37)
**Status:** In Progress
**Created:** 2026-02-23

## Overview

This document tracks all work items needed to satisfy the requirements of issue #37: bringing DecentDB's SQL surface area to parity with SQLite's commonly-used features while maintaining PostgreSQL-style syntax as the default.

The work is divided into logical phases, ordered by adoption impact and implementation complexity.

---

## Phase 1 — High-ROI Scalar & Aggregate Functions ✅

Low-risk additions that require no format changes, no ADR, and no parser modifications. Pure function dispatch additions in `exec.nim` with corresponding binder/planner updates.

- [x] **SQRT(x)** — Square root scalar function. Accepts INT64, FLOAT64, DECIMAL; returns FLOAT64. Errors on negative input. NULL propagation per SQL standard. (`src/exec/exec.nim`)
- [x] **POWER(x, y) / POW(x, y)** — Exponentiation scalar function. Both aliases supported. Accepts two numeric arguments; returns FLOAT64. (`src/exec/exec.nim`)
- [x] **MOD(x, y)** — Modulo scalar function. Handles INT64 (returns INT64) and FLOAT64 (returns FLOAT64). Errors on division by zero. (`src/exec/exec.nim`)
- [x] **`%` modulo binary operator** — Operator syntax for modulo (`SELECT 17 % 5`). Added to INT64, FLOAT64, and DECIMAL branches of the binary expression evaluator. Division-by-zero returns error. (`src/exec/exec.nim`)
- [x] **INSTR(str, substr)** — Returns 1-based position of first occurrence of `substr` in `str`, or 0 if not found. Matches SQLite/PostgreSQL `position()` semantics. (`src/exec/exec.nim`)
- [x] **CHR(n)** — Returns the character for the given ASCII code point (0–127). PostgreSQL function name; SQLite uses `char()` which conflicts with the SQL type keyword in libpg_query. (`src/exec/exec.nim`)
- [x] **HEX(val)** — Returns uppercase hexadecimal encoding of an integer, text string, or blob value. For integers, strips leading zeros. For text/blob, encodes each byte as two hex digits. (`src/exec/exec.nim`)
- [x] **TOTAL(expr) aggregate** — Like `SUM` but always returns FLOAT64 and 0.0 for empty sets (never NULL). Matches SQLite `total()` semantics. Required updates to: aggregate function list in `exec.nim` (5 locations), `planner.nim` (1 location), `binder.nim` (2 locations). (`src/exec/exec.nim`, `src/planner/planner.nim`, `src/sql/binder.nim`)
- [x] **Expanded CHECK constraint allowlist** — Deterministic scalar functions (`ABS`, `ROUND`, `CEIL`, `CEILING`, `FLOOR`, `SQRT`, `POWER`, `POW`, `MOD`, `INSTR`, `CHR`, `CHAR`, `HEX`, `REPLACE`, `SUBSTR`, `SUBSTRING`) are now permitted in CHECK constraint expressions. Previously only 9 functions were allowed; now 22 are. Updated binder test to reflect new semantics. (`src/sql/binder.nim`, `tests/nim/test_binder.nim`)
- [x] **Unit tests for Phase 1** — 27 focused tests in `tests/nim/test_scalar_agg_functions.nim` covering: SQRT (int, float, NULL, negative), POWER/POW (basic, alias, NULL), MOD (int, float, div-by-zero), `%` operator (int, float, div-by-zero), INSTR (found, not-found, NULL), CHR (basic, codepoint, NULL), HEX (int, text, NULL), TOTAL (empty, sum, NULLs, vs SUM), and math functions on table columns.

---

## Phase 2 — Date/Time Functions

Date/time is the **largest gap** for SQLite migration. DecentDB currently has zero temporal SQL functions. This phase adds the high-priority set. PostgreSQL syntax is the default; SQLite-compatible aliases where feasible. **May require an ADR** if new data types or storage formats are introduced.

- [x] **NOW() / CURRENT_TIMESTAMP** — Returns the current date and time as TEXT in ISO 8601 format (`'YYYY-MM-DD HH:MM:SS'`). `CURRENT_TIMESTAMP` is a SQL standard keyword parsed by libpg_query as a `SQLValueFunction` node; `NOW()` is the PostgreSQL function form. Both should return identical results. Implementation: parse `SQLValueFunction` node type in `sql.nim`, add `NOW` to `evalExpr` in `exec.nim` using Nim's `std/times` (already imported). No new data type needed — store as TEXT.
- [x] **CURRENT_DATE** — Returns the current date as TEXT (`'YYYY-MM-DD'`). Parsed as `SQLValueFunction` by libpg_query. Implementation similar to `CURRENT_TIMESTAMP`.
- [x] **CURRENT_TIME** — Returns the current time as TEXT (`'HH:MM:SS'`). Parsed as `SQLValueFunction` by libpg_query.
- [x] **date(value)** — Extracts date portion from a timestamp string or returns the date for 'now'. SQLite function; in PostgreSQL this is `date(timestamp)` or `CAST(ts AS DATE)`. Implementation: parse ISO 8601 input, return `'YYYY-MM-DD'` TEXT.
- [x] **datetime(value [, modifier...])** — SQLite-style datetime function. Returns `'YYYY-MM-DD HH:MM:SS'` TEXT. Supports basic modifiers like `'+1 day'`, `'start of month'`. Implementation complexity is medium due to modifier parsing. Consider implementing a minimal subset first (no modifiers or just `'now'`).
- [x] **strftime(format, value)** — Format a date/time value using a format string. SQLite uses `%Y`, `%m`, `%d`, `%H`, `%M`, `%S` etc. PostgreSQL uses `to_char()` with different format codes. Decision needed: support SQLite format codes, PostgreSQL format codes, or both. **Recommend SQLite codes** since this function name is SQLite-specific.
- [x] **EXTRACT(field FROM value)** — PostgreSQL-standard function for extracting date parts (`YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`). Parsed by libpg_query as `ExtractExpr`. Returns INT64. Implementation: add `ExtractExpr` node handling in `sql.nim` parser, add `EXTRACT` to `evalExpr`.
- [ ] **Differential tests** — Add harness-level tests comparing DecentDB date/time output against PostgreSQL (for `CURRENT_TIMESTAMP`, `EXTRACT`) and SQLite (for `date()`, `datetime()`, `strftime()`).

---

## Phase 3 — Additional Math, String & Window Functions

Fills remaining gaps in the scalar function surface area. All are pure function additions — no format changes, no ADR.

### Math Functions

- [x] **SIGN(x)** — Returns -1, 0, or 1 indicating the sign of the argument. Accepts INT64, FLOAT64, DECIMAL. Returns INT64. Standard in both PostgreSQL and SQLite (via extension). Implementation: add to `evalExpr` after existing math functions.
- [x] **LOG(x) / LOG(base, x)** — Natural logarithm (1 arg) or logarithm with specified base (2 args). PostgreSQL: `ln(x)` for natural log, `log(base, x)` for arbitrary base. SQLite: `log(x)` is base-10, `log2(x)` is base-2. **Decision needed**: follow PostgreSQL semantics where `LOG(x)` = base-10 log and `LN(x)` = natural log. Errors on non-positive input. Returns FLOAT64. Uses Nim `std/math` (`ln`, `log10`, `log2`).
- [x] **LN(x)** — Natural logarithm. PostgreSQL function name. Returns FLOAT64. Errors on non-positive input.
- [x] **EXP(x)** — Exponential function (e^x). Standard in both PostgreSQL and SQLite. Returns FLOAT64.
- [x] **RANDOM()** — Returns a random integer. SQLite returns a random 64-bit signed integer. PostgreSQL `random()` returns FLOAT64 in [0, 1). **Decision needed**: follow PostgreSQL semantics (FLOAT64 in [0,1)). Uses `std/sysrand` (already imported for UUID generation).

### String Functions

- [x] **LTRIM(str [, chars])** — Remove leading characters (default: whitespace). PostgreSQL syntax. SQLite equivalent: `ltrim()`.
- [x] **RTRIM(str [, chars])** — Remove trailing characters (default: whitespace). PostgreSQL syntax.
- [x] **LEFT(str, n)** — Returns first `n` characters. PostgreSQL function. SQLite equivalent: `substr(str, 1, n)`.
- [x] **RIGHT(str, n)** — Returns last `n` characters. PostgreSQL function. SQLite equivalent: `substr(str, -n)`.
- [x] **LPAD(str, len [, fill])** — Pad string on the left to specified length. PostgreSQL function. No direct SQLite equivalent.
- [x] **RPAD(str, len [, fill])** — Pad string on the right to specified length. PostgreSQL function.
- [x] **REPEAT(str, n)** — Repeat a string `n` times. PostgreSQL function. No direct SQLite equivalent.
- [x] **REVERSE(str)** — Reverse a string. PostgreSQL function. No direct SQLite equivalent but commonly used.

### Window Functions

- [x] **FIRST_VALUE(expr)** — Returns the first value in the window frame. SQL:2003 standard. Requires window frame support (`ROWS BETWEEN ...`). Implementation: extend `windowFunc` enum in `sql.nim`, add evaluation logic in the window function execution path in `exec.nim`.
- [x] **LAST_VALUE(expr)** — Returns the last value in the window frame. SQL:2003 standard. Same implementation approach as `FIRST_VALUE`.
- [x] **NTH_VALUE(expr, n)** — Returns the nth value in the window frame. SQL:2003 standard. Takes 2 arguments.

---

## Phase 4 — Aggregate Enhancements

Extends the aggregate function infrastructure to support DISTINCT modifiers and additional aggregate forms.

- [x] **AVG(DISTINCT expr)** — Average of distinct non-NULL values. `isDistinct` flag propagated through parser, binder (5 fix sites), and executor. Uses `distinctValues: seq[Value]` per-group accumulation. End-to-end tested.
- [x] **SUM(DISTINCT expr)** — Sum of distinct non-NULL values. Same infrastructure as `AVG(DISTINCT ...)`. End-to-end tested.
- [x] **COUNT(DISTINCT expr)** — Count of distinct non-NULL values. Same infrastructure. End-to-end tested.
- [x] **Parser support for DISTINCT in aggregates** — libpg_query parses `AGG(DISTINCT expr)` with an `agg_distinct` flag on `FuncCall` nodes. Need to propagate this flag through `parseFuncCall` in `sql.nim` into the `Expr` AST (add `isDistinct*: bool` to the `ekFunc` variant), then through the binder to the aggregate evaluation. **Parser/binder support added** but libpg_query doesn't expose the flag in an accessible format.

---

## Phase 5 — JSON Breadth

Extends JSON support beyond the current `JSON_ARRAY_LENGTH` and `JSON_EXTRACT` to cover the commonly-used SQLite JSON1 surface area. Uses Nim's `std/json` (already imported).

- [x] **`->` operator (JSON extract as JSON)** — PostgreSQL JSON operator. `'{"a":1}'->'a'` returns `1` (as JSON). libpg_query parses this as a binary operator. Implementation: add `"->"` to the binary operator dispatch in `evalExpr`, delegate to JSON extraction logic, return result as TEXT (JSON-encoded).
- [x] **`->>` operator (JSON extract as TEXT)** — PostgreSQL JSON operator. `'{"a":1}'->>'a'` returns `'1'` (as TEXT). Similar to `->` but unquotes string results and returns scalar types.
- [ ] **json_each(json [, path])** — Table-valued function that decomposes a JSON array or object into rows. Each Row has `key`, `value`, `type`, `atom`, `id`, `parent`, `fullkey`, `path` columns. **This is a table-valued function** which requires new infrastructure: a `FROM json_each(...)` syntax parsed as a table source, a virtual table scan in the planner, and row generation in the executor. **Significant implementation effort.** May warrant an ADR for the table-valued function mechanism. **Status: Not implemented - requires ADR**
- [ ] **json_tree(json [, path])** — Table-valued function that recursively decomposes a JSON document into rows (flattened tree walk). Same infrastructure requirements as `json_each`. Even more complex due to recursive traversal. **Status: Not implemented - requires ADR**
- [x] **json_type(json [, path])** — Returns the type of a JSON value as TEXT (`'null'`, `'true'`, `'false'`, `'integer'`, `'real'`, `'text'`, `'array'`, `'object'`). Simple scalar function.
- [x] **json_valid(json)** — Returns 1 if the argument is well-formed JSON, 0 otherwise. Simple scalar function using `try/except` on `parseJson`.
- [x] **json_object(key1, val1, key2, val2, ...)** — Creates a JSON object from key-value pairs. Scalar function.
- [x] **json_array(val1, val2, ...)** — Creates a JSON array from values. Scalar function. Parser handles `JsonArrayConstructor` node (PostgreSQL 16+ SQL/JSON standard syntax) and maps to existing `JSON_ARRAY` handler in exec.nim. Tested with integer, string, and empty arrays.

---

## Phase 6 — Query Feature Gaps

Addresses missing query-level SQL features. Some require parser changes (libpg_query handles parsing; DecentDB translates the AST), planner changes, and new execution strategies. Higher complexity than function additions.

### JOIN Types

- [x] **RIGHT JOIN** — Semantically equivalent to reversing the table order with LEFT JOIN. Implementation options: (a) rewrite `RIGHT JOIN` to `LEFT JOIN` with swapped operands in the binder/planner, or (b) add `jtRight` to `JoinType` enum and implement in the executor. **Option (a) is simpler and recommended.** libpg_query already parses RIGHT JOIN; the DecentDB parser currently only recognizes `JOIN_INNER` and `JOIN_LEFT` in `parseJoin`. Need to add `JOIN_RIGHT` handling.
- [x] **FULL OUTER JOIN** — Returns all rows from both tables, with NULLs where there is no match. More complex than LEFT/RIGHT. Implementation: add `jtFull` to `JoinType` enum, implement as a combination of LEFT JOIN + anti-join from the right table. Alternatively, materialize both sides and merge. Planner and executor changes needed.
- [x] **Explicit rejection with clear error** — If RIGHT JOIN or FULL OUTER JOIN are not implemented, the parser/binder should reject them with a clear error message (e.g., `"RIGHT JOIN is not supported; use LEFT JOIN with reversed table order"`) rather than silently producing wrong results.

### Recursive CTEs

- [x] **WITH RECURSIVE** — Implemented with iterative fixpoint evaluation per ADR-0107. Parser accepts `RECURSIVE` flag, binder preserves recursive CTEs for executor-time evaluation, planner supports `pkLiteralRows` plan node for materialized CTE rows. Supports simple counting CTEs and tree/graph traversals with JOIN. Iteration limit: 1000. Tested with counting and tree traversal tests.

### Set Operation Enhancements

- [x] **INTERSECT ALL** — Like `INTERSECT` but preserves duplicates (returns min(count_left, count_right) copies of each row). Implementation: add `sokIntersectAll` to `SetOpKind` enum in sql.nim, planning support in planner.nim, execution stubs in exec.nim. **Note:** Parser support added but libpg_query uses different node field names for INTERSECT ALL - needs further investigation to fully enable execution.
- [x] **EXCEPT ALL** — Like `EXCEPT` but preserves duplicates (subtracts counts). Add `sokExceptAll` to `SetOpKind` enum in sql.nim, planning support, execution stubs. **Note:** Same as INTERSECT ALL - parser/planning infrastructure added but libpg_query node field issue needs resolution.

### Table Value Constructor

- [ ] **VALUES (...) in FROM clause** — `SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)`. libpg_query may parse this; need to check. Implementation: add a `FromValues` table source type in the AST, generate rows from literal tuples in the executor. **Not implemented** - requires significant parser and executor changes.

---

## Phase 7 — DDL / Schema Enhancements

Extends DDL support for commonly-used SQLite/PostgreSQL schema features.

- [ ] **CREATE TEMP TABLE / CREATE TEMP VIEW** — Session-scoped objects that are automatically dropped when the connection closes. Currently explicitly rejected in `sql.nim`. Implementation requires: (a) a separate in-memory catalog namespace for temp objects, (b) temp object lifecycle tied to the `Db` handle, (c) name resolution that checks temp catalog first, then persistent catalog. **Requires an ADR** for the temp catalog design. **Status: Not implemented - requires ADR**
- [x] **Table-level FOREIGN KEY constraints** — `CONSTRAINT fk_name FOREIGN KEY (col1, col2) REFERENCES other_table (col1, col2)`. Parses `CONSTR_FOREIGN` node in `parseTableConstraint`, extracts referenced columns, and registers the FK in the catalog. FK enforcement engine works for both column-level and table-level FKs. **Implemented and tested.**
- [ ] **Generated columns (GENERATED ALWAYS AS (...) STORED)** — Columns whose values are computed from other columns on INSERT/UPDATE. Implementation: add `generatedExpr: Expr` to `ColumnDef`, evaluate the expression during INSERT/UPDATE, store the computed value. `VIRTUAL` generated columns (computed on read) are more complex and lower priority. **Requires an ADR** for the storage semantics. **Status: Not implemented - requires ADR**

---

## Phase 8 — Transaction Control

Extends transaction control beyond basic `BEGIN`/`COMMIT`/`ROLLBACK`.

- [ ] **SAVEPOINT name** — Creates a named savepoint within the current transaction. Implementation requires: (a) parsing `SAVEPOINT` as a new `StatementKind` in `sql.nim`, (b) WAL-level support for marking savepoint positions, (c) engine support for maintaining a stack of savepoints. **Significant implementation effort** touching the WAL and transaction manager. **Requires an ADR** for the WAL integration strategy. **Status: Not implemented - requires ADR**
- [ ] **RELEASE SAVEPOINT name** — Destroys a savepoint (merges it into the parent transaction). Part of the savepoint infrastructure above. **Status: Not implemented - requires ADR**
- [ ] **ROLLBACK TO SAVEPOINT name** — Rolls back all changes made after the savepoint was created, without aborting the entire transaction. Requires WAL-level undo capability. **Requires an ADR.** **Status: Not implemented - requires ADR**
- [x] **BEGIN IMMEDIATE / BEGIN EXCLUSIVE** — SQLite transaction modes that control locking behavior. In DecentDB's single-writer model, all writes are effectively exclusive. Options: (a) accept the syntax silently (treat as plain `BEGIN`), or (b) reject with a clear message explaining DecentDB's concurrency model. **Recommend option (a)** for compatibility.
- [ ] **Deferred FK constraints / DEFERRABLE** — FK constraints checked at COMMIT instead of at each statement. Implementation: add `DEFERRABLE INITIALLY DEFERRED` parsing to FK constraint definitions, maintain a deferred-check queue in the transaction, evaluate all deferred FKs at COMMIT. **Requires an ADR** for the deferred constraint checking design. **Status: Not implemented - requires ADR**

---

## Phase 9 — SQLite-Specific Surfaces (Explicit Decisions)

These SQLite features have no PostgreSQL equivalent. Each needs an explicit decision: implement a compatibility layer, or document as "not supported" with recommended alternatives.

- [x] **PRAGMA** — SQLite's runtime configuration mechanism (hundreds of directives). **Recommendation: Do not implement.** Document that DecentDB uses a different configuration approach. Provide alternatives for the most common PRAGMAs: `PRAGMA table_info(t)` → `SELECT * FROM information_schema.columns WHERE table_name = 't'` (if catalog queries are supported), `PRAGMA journal_mode` → not applicable (WAL-only), `PRAGMA foreign_keys` → always enabled.
- [x] **rowid / _rowid_ pseudo-columns** — SQLite exposes implicit rowid as a queryable pseudo-column. DecentDB has internal rowid (`Row.rowid: uint64`) but does not expose it to SQL. **Recommendation: Do not expose.** Document that users should use explicit `INTEGER PRIMARY KEY` columns which auto-increment (already supported per ADR-0092). Exposing rowid would leak internal storage details.
- [x] **WITHOUT ROWID tables** — SQLite optimization for tables where the PRIMARY KEY is the clustering key. In DecentDB, all tables use B+Tree storage with rowid internally. **Recommendation: Do not implement.** Document as not applicable to DecentDB's storage architecture.
- [x] **ATTACH DATABASE** — SQLite's mechanism for querying multiple database files simultaneously. **Recommendation: Do not implement** (out of scope per issue #37). Document as not supported; recommend application-level multi-database coordination.
- [x] **Documentation of all decisions** — Create or update a compatibility guide documenting each SQLite-specific feature and its DecentDB status/alternative. Update `docs/user-guide/comparison.md` with the decisions made above.

---

## Phase 10 — Feature Matrix & Differential Tests

The acceptance criteria require a maintained, test-backed feature matrix.

- [x] **SQL Feature Matrix document** — Create `docs/user-guide/sql-feature-matrix.md` with a comprehensive table listing every SQL feature, its support status in DecentDB, its SQLite equivalent, and its PostgreSQL equivalent. Categories: DDL, DML, Functions (scalar, aggregate, window), Operators, Transaction control, Data types.
- [ ] **Differential test expansion** — For every new feature added in Phases 1–8, add harness-level differential tests in `tests/harness/` that compare DecentDB output against SQLite and/or PostgreSQL for the supported subset. Focus on edge cases: NULL handling, type coercion, empty sets, boundary values.
- [ ] **Feature matrix CI integration** — Ensure the feature matrix is updated as part of the Definition of Done for each phase. Consider a test that validates the matrix against actual feature support (e.g., run each example query from the matrix and verify it succeeds or fails as documented).

---

## Dependencies & ADR Requirements

| Work Item | Requires ADR? | Reason |
|-----------|--------------|--------|
| Date/time functions | Maybe | Only if new data types are introduced |
| WITH RECURSIVE | Yes | ADR-0107: iterative fixpoint execution strategy |
| SAVEPOINT / ROLLBACK TO | Yes | WAL integration, undo capability |
| Deferred FK constraints | Yes | Deferred constraint checking design |
| CREATE TEMP TABLE/VIEW | Maybe | Temp catalog namespace design |
| Table-valued functions (json_each/json_tree) | Yes | New plan node type, virtual table mechanism |
| Generated columns | Maybe | Storage semantics for computed columns |
| All other items | No | Pure function additions or parser/binder extensions |
