# Query Execution

This page describes the path from SQL text to results in DecentDB.

## End-to-end pipeline

1. **Parse and normalize** (`crates/decentdb/src/sql/parser.rs`, `crates/decentdb/src/sql/normalize.rs`):
   SQL text is split with `libpg_query`, then normalized into DecentDB's internal `Statement` AST.
2. **Catalog load / refresh** (`crates/decentdb/src/db.rs`, `crates/decentdb/src/exec/mod.rs`):
   each `Db` handle refreshes its in-memory relational runtime from the WAL-backed catalog root when the shared WAL LSN advances.
3. **Plan** (`crates/decentdb/src/planner/mod.rs`):
   the rule-based planner chooses scans, seeks, trigram searches, joins, aggregates, sorts, limits, and `EXPLAIN` output.
4. **Execute** (`crates/decentdb/src/exec/mod.rs`, `crates/decentdb/src/exec/*.rs`):
   queries evaluate against the current runtime snapshot, while mutating statements execute against a cloned working runtime and persist on success.

## Multi-statement execution (important)

`Db::execute_batch` and `Db::execute_batch_with_params` accept multiple statements separated by semicolons.

The engine parses the full batch up front, but executes statements one-at-a-time in order. Successful mutating statements persist immediately, so later statements in the same batch observe earlier DDL and DML changes from that batch.

## Plan nodes

The planner produces a `PhysicalPlan` tree, including:

- Access paths: `TableScan`, `IndexSeek`, `TrigramSearch`
- Relational ops: `Filter`, `Project`, `NestedLoopJoin`
- Order/aggregation: `Sort`, `Aggregate`, `Limit`
- Set operations: `SetOp`
- Utility: `Empty`

## Execution model

The current executor materializes result datasets in reusable row buffers. Query planning still exposes physical operators, and execution uses those shapes for `EXPLAIN`, simple index-backed reads, trigram candidate reads, joins, aggregates, and post-filter projection.

Mutating statements execute against a cloned `EngineRuntime`. If a statement fails partway through, the working runtime is dropped and nothing is persisted, which gives statement-level rollback semantics.

## Snapshot isolation and WAL overlay

DecentDB uses snapshot isolation:

- a reader handle reloads the relational runtime from the latest committed WAL-backed catalog root when the shared WAL LSN changes
- page reads still use the pager + WAL overlay rules for low-level storage access
- writers are serialized, but read queries run against the last committed runtime snapshot

## EXPLAIN

`EXPLAIN` and `EXPLAIN ANALYZE` render the planner's `PhysicalPlan` tree directly from `crates/decentdb/src/planner/physical.rs`.
