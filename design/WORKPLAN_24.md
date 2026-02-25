# Workplan: Cost-based query optimizer with statistics collection (Issue #24)

**Agent coding prompt:** See `design/ISSUE_24_AGENT_PROMPT.md`.

## Overview
This document outlines the implementation plan for introducing a cost-based query optimizer with statistics collection to DecentDB, as described in [Issue #24](https://github.com/sphildreth/decentdb/issues/24).

## Phase 0: Design and Architecture
- **Task 0.1: Update/Supersede ADR 0038**
  - Update `design/adr/0038-cost-based-optimization-deferred.md` (or supersede it with a new ADR) with the implementation-ready design for cost-based optimization.
  - Define the catalog schema changes for storing statistics (row counts, index cardinality).
  - Define the format versioning strategy and backward compatibility plan.
  - Define the cost model formulas and selectivity estimation logic.
  - Define the join reordering algorithm (e.g., dynamic programming for $\le$ 6 tables).
  - Review and get approval for the ADR before proceeding with implementation.

## Phase 1: Statistics Collection & Maintenance
- **Task 1.1: Catalog Updates for Statistics**
  - Update `src/catalog/catalog.nim` to support storing and retrieving table row counts and index cardinalities.
  - Handle format versioning to ensure backward compatibility for existing databases (no database format breakage for users who never run `ANALYZE`).
- **Task 1.2: Implement `ANALYZE` Command**
  - Add SQL parser support for the `ANALYZE` command.
  - Implement the execution logic to compute per-table row counts and per-index cardinality.
  - Persist the computed statistics to the catalog.
- **Task 1.3: Incremental Statistics Maintenance**
  - Update `INSERT`, `UPDATE`, and `DELETE` execution paths in `src/exec/exec.nim` to incrementally update row counts (exact or approximate).
- **Task 1.4: Testing Phase 1**
  - Add unit tests for the `ANALYZE` command and catalog statistics storage.
  - Add tests for incremental updates during DML operations.

## Phase 2: Cost Model & Selectivity Estimation
- **Task 2.1: Define Cost Functions**
  - Implement cost functions for each plan operator in `src/planner/planner.nim` based on estimated input cardinality and I/O.
- **Task 2.2: Selectivity Estimation for Predicates**
  - Implement logic to estimate selectivity for `WHERE` predicates (equality, range, `LIKE`) using the collected statistics.
- **Task 2.3: Update `EXPLAIN ANALYZE`**
  - Modify `src/planner/explain.nim` to output estimated vs. actual row counts for plan validation.
- **Task 2.4: Testing Phase 2**
  - Add unit tests for cost calculations and selectivity estimations.
  - Verify `EXPLAIN ANALYZE` output format.

## Phase 3: Index Selection Improvements
- **Task 3.1: Cost-Based Index Selection**
  - Update the planner to choose between table scans and index seeks based on the new cost model and estimated selectivity.
- **Task 3.2: Testing Phase 3**
  - Add tests to verify the planner chooses index seeks for highly selective queries and table scans for low selectivity.
  - Ensure existing queries produce correct results.

## Phase 4: Join Reordering
- **Task 4.1: Implement Join Reordering Algorithm**
  - Implement a dynamic programming (Selinger-style) algorithm in `src/planner/planner.nim` to evaluate multiple join orderings.
  - Set a threshold (e.g., $\le$ 6 tables) for exhaustive search, with a heuristic fallback for queries with more tables.
- **Task 4.2: Testing Phase 4**
  - Add tests with multi-table joins to verify the planner selects the lowest-cost join order.
  - Run differential tests against PostgreSQL to ensure results continue to match.

## Phase 5: Benchmarking and Final Validation
- **Task 5.1: Performance Benchmarking**
  - Run write benchmarks to ensure no regression from incremental statistics maintenance.
  - Run read benchmarks to validate performance improvements on multi-join queries.
- **Task 5.2: Full Test Suite Run**
  - Ensure all existing tests pass.
  - Verify backward compatibility with databases that have never run `ANALYZE`.
