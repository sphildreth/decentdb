# EXPLAIN implementation (DecentDB)

This document is an implementation playbook for adding `EXPLAIN` to DecentDB.

It is intentionally prescriptive: a coding agent should follow it without making product/design decisions.

## Non‑negotiables / repo constraints

- **Do not change any persistent formats** (db header, page layout, WAL formats, index formats). `EXPLAIN` is an observability feature only.
- **Do not add new dependencies**.
- Per [AGENTS.md](../AGENTS.md) and `.github/copilot-instructions.md`: changing SQL dialect behavior requires an **ADR** before landing code.

## Coding-agent checklist (do not improvise)

1. Confirm ADR is accepted: [design/adr/0050-explain-statement.md](adr/0050-explain-statement.md)
2. Implement parser/AST changes in `src/sql/sql.nim`
3. Implement binder changes in `src/sql/binder.nim`
4. Add plan rendering module `src/planner/explain.nim`
5. Wire execution in `src/engine.nim` (`execSql` only)
6. Add Nim tests under `tests/nim/`
7. Run the narrowest relevant Nim tests

## Scope (exact)

Implement:

1. SQL statement `EXPLAIN <statement>`.
2. Supported inner statements: **SELECT only** (for now).
3. `EXPLAIN` returns a **result set** (like a SELECT) consisting of **one TEXT column** and **N rows**, where each row is a single plan line.
4. `EXPLAIN` **does not execute** the inner statement. It only parses + binds + plans.

Do **not** implement in this iteration:

- `EXPLAIN ANALYZE`
- `EXPLAIN (options …)`
- `EXPLAIN` for INSERT/UPDATE/DELETE/DDL
- Any runtime statistics (row counts per operator, timing)

## User‑visible behavior (exact)

### Syntax

- `EXPLAIN <select_stmt>;`
  - `<select_stmt>` is any SELECT that DecentDB already supports.

### Errors

- If the inner statement is not a SELECT: return `ERR_SQL` with message **`EXPLAIN currently supports SELECT only`**.
- If `EXPLAIN` options are present (e.g. `EXPLAIN (ANALYZE) ...`): return `ERR_SQL` with message **`EXPLAIN options not supported`**.

### Output format

- One column named **`query_plan`** (lowercase).
- Each output row is one plan line.
- Plan lines are stable, deterministic, and ASCII-only.
- Plan uses a simple tree format:
  - Preorder traversal (node first, then children)
  - Indentation: **two spaces** per depth level

Example (illustrative only; exact node types depend on indexes):

```
Project(projections=*)
  Filter(predicate=(namenormalized LIKE '%Mind%'))
    TableScan(table=artists alias=)
```

## Required ADR

This change is already authorized by the accepted ADR:

- [design/adr/0050-explain-statement.md](adr/0050-explain-statement.md)

Do not modify scope/behavior beyond what ADR 0050 specifies.

## Implementation plan (file-by-file)

### 1) SQL AST: add an `EXPLAIN` statement kind

File: `src/sql/sql.nim`

1. Extend `StatementKind`:

- Add `skExplain`.

2. Extend `Statement` with a new variant:

- For `skExplain`, store:
  - `explainInner*: Statement` — the inner statement AST.
  - `explainHasOptions*: bool` — set to true if pg_query returns any options.

   Do **not** store the original SQL string.

3. Parsing:

- In `parseStatementNode(node: JsonNode)` add:
  - If `nodeHas(node, "ExplainStmt")`: call `parseExplainStmt(node["ExplainStmt"])`.

- Implement `proc parseExplainStmt(node: JsonNode): Result[Statement]`.

  Exact behavior:

  - Read `options` field:
    - If field exists and is a non-empty array: return `ERR_SQL` with message `EXPLAIN options not supported`.
    - Otherwise treat as no options.

  - Read inner query node:
    - Use `nodeGet(node, "query")`.
    - Pass that node to `parseStatementNode` to reuse existing parsing for SELECT/INSERT/etc.

  - Wrap result into:

    `Statement(kind: skExplain, explainInner: <inner>, explainHasOptions: false)`

4. Fast-path parser interaction:

- In `parseSql*(sql: string)` the fast-path `tryParseFastSelect` only recognizes `SELECT * FROM ... WHERE ...`.
- Ensure **EXPLAIN never goes through the fast-path**.

  Exact change:

  - Before calling `tryParseFastSelect(sql)`, check if the input starts with `EXPLAIN` (case-insensitive, leading whitespace allowed). If so, skip fast-path and go directly to pg_query.

  This avoids implementing a second fast-path for EXPLAIN.

### 2) Binder: bind inner statement

File: `src/sql/binder.nim`

1. Extend `bindStatement*(catalog: Catalog, stmt: Statement): Result[Statement]`:

- Add a case for `skExplain`:
  - Call `bindStatement(catalog, stmt.explainInner)`.
  - If it errors, propagate.
  - Return a new `Statement(kind: skExplain, explainInner: <boundInner>, explainHasOptions: stmt.explainHasOptions)`.

2. No additional semantic checks here. (SELECT-only enforcement happens during planning/execution.)

### 3) Planner: add a plan pretty-printer (no planning logic changes)

Create a new module:

- `src/planner/explain.nim`

This module must contain:

1. `proc renderExpr*(expr: Expr): string`

Exact rendering rules:

- `nil` → `<nil>`
- Literals:
  - NULL → `NULL`
  - INT → decimal (e.g. `123`)
  - FLOAT → Nim `$` formatting (acceptable)
  - BOOL → `true` / `false`
  - STRING → single-quoted with `''` escape for `'`.
- Column:
  - If `expr.table.len > 0`: `<table>.<name>`
  - Else: `<name>`
- Param:
  - `$<index>` (1-based, matching current parameterization ADR)
- Binary:
  - `(<left> <op> <right>)`
- Unary:
  - `(<op> <expr>)`
- Func:
  - `funcName(*)` if `isStar`
  - else `funcName(arg1, arg2, ...)`
- IN list:
  - `(<inExpr> IN (<item1>, <item2>, ...))`

2. `proc renderSelectItem*(item: SelectItem): string`

- If `item.isStar`: `*`
- Else: `renderExpr(item.expr)`
- If `item.alias.len > 0`: append ` AS <alias>`

3. `proc renderOrderItem*(item: OrderItem): string`

- `renderExpr(item.expr)` + ` ASC` or ` DESC`

4. `proc explainPlanLines*(catalog: Catalog, plan: Plan): seq[string]`

Traversal and formatting (exact):

- Use preorder traversal.
- Indent by 2 spaces per depth level.
- One node per line.

Node rendering (exact strings):

- `pkTableScan`:
  - `TableScan(table=<plan.table> alias=<plan.alias>)`
- `pkRowidSeek`:
  - `RowidSeek(table=<plan.table> alias=<plan.alias> column=<plan.column> value=<renderExpr(plan.valueExpr)>)`
- `pkIndexSeek`:
  - Look up the btree index meta from the catalog:
    - `let idxOpt = catalog.getBtreeIndexForColumn(plan.table, plan.column)`
  - Include index name if present:
    - `IndexSeek(table=... column=... value=... index=<idxOpt.get.name|?>)`
  - If missing: use `index=?`.
- `pkTrigramSeek`:
  - Look up trigram index meta:
    - `let idxOpt = catalog.getTrigramIndexForColumn(plan.table, plan.column)`
  - `TrigramSeek(table=... column=... pattern=<renderExpr(plan.likeExpr)> insensitive=<true|false> index=<name|?>)`
- `pkFilter`:
  - `Filter(predicate=<renderExpr(plan.predicate)>)`
- `pkProject`:
  - `Project(projections=<comma-joined renderSelectItem>)`
- `pkJoin`:
  - `Join(type=<INNER|LEFT> on=<renderExpr(plan.joinOn)>)`
- `pkSort`:
  - `Sort(orderBy=<comma-joined renderOrderItem>)`
- `pkAggregate`:
  - `Aggregate(groupBy=<comma-joined renderExpr> having=<renderExpr(plan.having)|<nil>> projections=<comma-joined renderSelectItem>)`
- `pkLimit`:
  - Resolve limit/offset as written (no param evaluation here):
    - If `plan.limitParam > 0`: show `limit=$<limitParam>` else `limit=<plan.limit>`
    - If `plan.offsetParam > 0`: show `offset=$<offsetParam>` else `offset=<plan.offset>`
  - Render:
    - `Limit(limit=<...> offset=<...>)`
- `pkStatement`:
  - `Statement(kind=<stmt.kind>)` (only used defensively)

Children ordering (exact):

- Unary nodes (`Filter`, `Project`, `Sort`, `Aggregate`, `Limit`): print the node line, then recurse to `plan.left`.
- Join: print node line, then recurse left, then recurse right.

### 4) Engine execution: return plan lines for EXPLAIN

File: `src/engine.nim`

A. Planning / SQL cache population

In `proc execSql*(db: Db, sqlText: string, params: seq[Value]): Result[seq[string]]`:

1. When computing `cachedPlans` for parsed statements, extend the logic:

- If `bound.kind == skSelect`: existing behavior.
- If `bound.kind == skExplain`:
  - Let `inner = bound.explainInner`.
  - If `inner.kind != skSelect`: store `nil` plan (the error will be thrown at execution time) OR store a sentinel nil and enforce during execution.
  - Else: compute `plan(db.catalog, inner)` and store it in `cachedPlans` at the same index.

2. Ensure SQL cache invalidation stays schema-cookie based (no changes).

B. Statement execution

In the big `for i, bound in boundStatements:` switch in `execSql`:

1. Add a new `of skExplain:` branch.

Exact behavior:

- Enforce SELECT-only:
  - If `bound.explainInner.kind != skSelect`: return `ERR_SQL` with message `EXPLAIN currently supports SELECT only`.
- Retrieve the plan:
  - `let p = if i < cachedPlans.len: cachedPlans[i] else: nil`
  - If `p == nil`: re-plan on the spot (`plan(db.catalog, bound.explainInner)`) and use that (this is a defensive fallback).
- Import `src/planner/explain.nim` and call:
  - `let lines = explainPlanLines(db.catalog, p)`
- Append each line to `output` as its own row:
  - `output.add(line)`

Important: `EXPLAIN` must not start transactions and must not touch WAL.

- Ensure `isWrite` does NOT treat `skExplain` as a write.
- Ensure no call to `runSelect`, `execPlan`, or `openRowCursor` occurs for this branch.

C. `execSqlNoRows`

No changes required. `execSqlNoRows` is intentionally SELECT-only. If user attempts `execSqlNoRows("EXPLAIN ...")`, it should remain an error.

### 5) CLI (optional but recommended)

File: `src/decentdb_cli.nim`

If the CLI prints results for `exec`, no special-case is necessary because `EXPLAIN` is a SQL statement returning rows.

However, ensure the JSON output formatting for `exec` remains correct:

- For normal rows: unchanged
- For explain rows (single TEXT column): it should print one line per row (current `execSql` returns `seq[string]` lines joined with `|`; for explain it will be only the line text).

Do not add new CLI flags in this iteration.

## Tests (required)

Add Nim tests covering:

1. Basic EXPLAIN works

- Create a small in-memory or temp DB (use existing test helpers/patterns in `tests/nim/`).
- Create table + insert a few rows.
- Call `execSql(db, "EXPLAIN SELECT * FROM t WHERE id = 1;")`.
- Assert:
  - Result has at least 1 row.
  - One of the rows contains `IndexSeek` **only if** you created the index; otherwise assert it contains `TableScan`.

2. EXPLAIN does not execute

- Create a table `t(x INT)`.
- Run `execSql(db, "EXPLAIN INSERT INTO t(x) VALUES (1);")` and assert it errors with `EXPLAIN currently supports SELECT only`.
- Run `execSql(db, "SELECT * FROM t;")` and assert 0 rows to prove EXPLAIN didn’t insert.

3. Trigram path visibility (optional, but good)

- Create table `t(name TEXT)`.
- Create trigram index: `CREATE INDEX ix_t_name_trgm ON t USING trigram (name);`
- Run `execSql(db, "EXPLAIN SELECT * FROM t WHERE name LIKE '%abc%';")`.
- Assert at least one plan line contains `TrigramSeek`.

## Implementation order (do exactly in this sequence)

1. Land ADR.
2. Implement SQL AST changes (`skExplain`) and parser changes.
3. Implement binder support.
4. Implement plan rendering module.
5. Wire engine execution (`execSql`) to return explain rows.
6. Add tests.
7. Run narrow Nim tests related to SQL execution.

## Notes / pitfalls

- **pg_query JSON shape**: If `parseExplainStmt` fails because the JSON fields differ, do not guess.
  - Add a small debug-only unit test that calls `parseSql("EXPLAIN SELECT 1;")` and inspects the parsed JSON tree shape by printing `$parseResult.parse_tree` (guarded behind a test-only flag) to adjust field names.
- Keep plan rendering cheap-ish but correctness > performance; `EXPLAIN` is not on hot paths.
- Do not include nondeterministic values (page ids, row counts, time) in output.
