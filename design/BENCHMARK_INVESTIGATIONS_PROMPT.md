# DecentDB Performance Investigation Prompt

**Role**: You are a Principal Systems Engineer specializing in high-performance Rust, database internals, and embedded storage engines.

**Task**: Investigate the current performance bottleneck in the Rust implementation of DecentDB and propose an architectural roadmap to resolve it.

## The Context

We have established a native Rust benchmarking harness (`crates/decentdb/benches/embedded_compare.rs`) to compare DecentDB against SQLite and DuckDB. The benchmark executes the following workload:
1. `CREATE TABLE users (id INT64 PRIMARY KEY, name TEXT);`
2. `BEGIN;`
3. Execute 100,000 parameterized `INSERT` statements in a loop.
4. `COMMIT;`

**The Problem**: 
SQLite and DuckDB process this batch at approximately **1.8 to 2.0 million rows per second**. 
DecentDB is currently processing this batch at approximately **160 rows per second** (taking ~10 minutes for 100,000 rows). 

While DecentDB is in "Phase 0" of its Rust rewrite, this massive discrepancy indicates fundamental $O(N^2)$ algorithmic issues or structural design flaws in the current execution/storage layers, rather than simple missing micro-optimizations.

## Areas of Investigation

Please start your investigation by deeply analyzing the following modules in `crates/decentdb/src/`:

### 1. The Execution Loop & $O(N^2)$ Scaling (`exec/mod.rs`)
Look closely at `EngineRuntime::execute_statement`. 
* **Hypothesis**: After every single mutating statement (`Insert`, `Update`, `Delete`), the engine calls `self.rebuild_indexes(page_size)?`. Does this iterate over the entire `TableData` and reconstruct every index from scratch on every single insert? If so, inserting $N$ rows has $O(N^2)$ complexity.

### 2. SQL Parsing Overhead (`db.rs` & `sql/parser.rs`)
Look at `db.execute_with_params`. 
* **Hypothesis**: The benchmark issues the same SQL string 100,000 times inside a loop. Does DecentDB invoke `libpg_query_sys` to re-parse the AST from the raw string on every single iteration? We likely need a Prepared Statement cache or AST caching layer.

### 3. In-Memory Data Storage and Wholesale Serialization (`exec/mod.rs`)
Look at `EngineRuntime`'s definition and `encode_runtime_payload()`. 
* **Hypothesis**: The engine currently holds all rows in memory (`tables: BTreeMap<String, TableData>`) rather than pushing them to B-Tree pages immediately. 
* Furthermore, look at `persist_to_db`. When a transaction commits, does it serialize the *entire* database (every row, every table) into a single giant byte vector and write it to overflow pages? This would mean commit time scales linearly with the total database size, which is fatal for a database.

### 4. Page Cache and WAL Interaction (`storage/` and `wal/`)
If the execution engine is writing pages during the loop, investigate the `DbTxnPageStore`. 
* **Hypothesis**: Are pages being unnecessarily copied, allocated, or flushed to the WAL before the `COMMIT` boundary? 

## Your Deliverables

1. **Root Cause Analysis**: Confirm or refute the hypotheses above by using `rg` or `read` tools on the Rust source code. Identify exactly where the time is being spent.
2. **Short-Term Mitigation Plan**: Are there low-hanging fruit (like moving `rebuild_indexes` to only trigger on `COMMIT` rather than on every `INSERT`) that we can implement immediately to get benchmarking times down to reasonable levels?
3. **Long-Term Architectural Plan (ADR)**: The current `EngineRuntime` holding data in memory and serializing wholesale to an overflow page is likely a bootstrap/stub implementation. Write a detailed proposal for how DecentDB must transition from this "Phase 0 in-memory state" to "Phase 1 B-Tree backed tables". Include how tuples should be serialized directly to pages (Slotted Page Architecture) and how indexes should be incrementally updated. 

## Instructions

- Do not guess the codebase structure. Use `read`, `bash`, and `grep` to inspect `exec/mod.rs`, `db.rs`, and `catalog/schema.rs`.
- Do not make changes to the code yet. Output your findings and your proposed architectural plan first.
- Only proceed with code changes once the user has approved the architectural roadmap.