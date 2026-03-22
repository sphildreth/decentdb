# Query Execution

This page describes the path from SQL text to results in DecentDB.

## End-to-end pipeline

1. **Parse** (`src/sql/sql.rs`): SQL text → AST (via `libpg_query`) → internal `Statement`s.
2. **Bind** (`src/sql/binder.rs`): resolve names against the catalog, validate types, and produce bound statements.
3. **Plan** (`src/planner/planner.rs`): choose access paths and build a `Plan` tree.
4. **Execute** (`src/engine.rs`, `src/exec/exec.rs`): run the plan and format results.

## Multi-statement execution (important)

`execSql(db, sqlText, params)` can accept multiple statements separated by semicolons.

The engine parses and binds **all statements up front** against the catalog state at the start of the call (see `engine.execSql`). This means DDL inside a multi-statement string does **not** affect binding of later statements.

Practical implication: run `CREATE TABLE ...` and `INSERT ...` into that table as separate calls, or use the CLI REPL (which evaluates statements one-at-a-time).

## Plan nodes

The planner produces a tree of `Plan` nodes (`PlanKind`), including:

- Access paths: `pkTableScan`, `pkRowidSeek`, `pkIndexSeek`, `pkTrigramSeek`
- Relational ops: `pkFilter`, `pkProject`, `pkJoin`
- Order/aggregation: `pkSort`, `pkAggregate`, `pkLimit`
- Subqueries/sets: `pkSubqueryScan`, `pkAppend`, `pkUnionDistinct`, `pkSetIntersect`, `pkSetExcept`, …
- Table-valued functions: `pkTvfScan` (e.g. `json_each`, `json_tree`)

## Execution model

For common SELECT paths, the executor uses a cursor/iterator-style interface (`RowCursor`). Some operators can stream rows, while others may materialize intermediate results (for example, sorts or some join/aggregation shapes).

## Snapshot isolation and WAL overlay

DecentDB uses snapshot isolation:

- a reader captures a snapshot LSN at the start of query execution
- the pager overlays committed WAL frames newer than the base file up to that snapshot
- writers are serialized (single writer), but readers do not block writers

## EXPLAIN

`EXPLAIN` is supported and uses `src/planner/explain.rs` to render a human-readable plan tree.
