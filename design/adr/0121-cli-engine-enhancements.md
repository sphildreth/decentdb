# ADR 0121: CLI Engine Enhancements for Performance Tuning and Transaction Control

**Status:** Accepted  
**Date:** 2026-01-28  
**Updated:** 2026-03-23

## Context

The Rust CLI needed a direct path to engine-level controls that materially affect
operability and performance:

1. page-cache sizing for ad hoc workloads
2. explicit WAL checkpoint control
3. explicit transaction control for multi-statement SQL batches

Earlier draft text in this ADR described a pre-implementation state using older
examples. The current implementation lives in the Rust CLI and engine.

## Decision

### 1. Cache configuration is exposed through the CLI `exec` path

**Decision:** `crates/decentdb-cli` exposes cache sizing flags for SQL execution:

- `--cachePages`
- `--cacheMb`

These values are threaded into the engine configuration before opening the
session database.

**Rationale:** Cache sizing is a legitimate operational tuning knob and belongs
close to the command that executes the workload.

### 2. WAL checkpoint control is a first-class engine/CLI operation

**Decision:** The engine exposes checkpointing on `Db`, and the CLI exposes that
capability through its checkpoint command / execution flow.

**Rationale:** Manual checkpointing is useful for tests, maintenance workflows,
and explicit WAL management without breaking encapsulation around the `Db`
owner.

### 3. Explicit transaction control is handled in the engine's SQL batch layer

**Decision:** The engine accepts transaction-control SQL inside batched execution,
including:

- `BEGIN` / `BEGIN TRANSACTION`
- `BEGIN DEFERRED` / `BEGIN IMMEDIATE` / `BEGIN EXCLUSIVE`
- `COMMIT`
- `ROLLBACK`
- `SAVEPOINT` / `RELEASE` / `ROLLBACK TO`

The `Db` owner maintains explicit SQL transaction state and in-memory savepoint
snapshots while keeping the single-writer concurrency model intact.

**Rationale:** SQL transaction control is part of normal database usage. Handling
it in the engine's batch-execution path keeps CLI behavior, embedded behavior,
and tests aligned.

## Implementation shape

The implemented Rust surfaces are:

- CLI flags under `crates/decentdb-cli/src/commands/`
- transaction/checkpoint methods and SQL batch control in `crates/decentdb/src/db.rs`
- validation through Rust integration tests in `crates/decentdb/tests/`

## Consequences

### Positive

- users can tune cache size for a specific CLI workload
- checkpoint control is available without hidden engine hooks
- multi-statement transactions and savepoints behave consistently across CLI and
  embedded use
- the engine now has an accurate, documented control surface for these
  operations

### Trade-offs

- more operational flags means more combinations to test
- explicit transaction state adds engine complexity around lifecycle management
- savepoint support is intentionally runtime-snapshot based, not a separate WAL
  format feature
