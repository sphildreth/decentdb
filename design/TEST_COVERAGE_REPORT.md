# DecentDB Test Coverage Report

**Date:** 2026-02-24
**Overall Nim Coverage:** ~40.29% (58,039 / 144,047 lines)

This report outlines the current state of unit test coverage across the DecentDB codebase. The goal is to identify areas with low coverage and provide an actionable checklist for future coding agents to improve the robustness of the test suite.

## Current Coverage Summary

| Module | Coverage | Status |
| :--- | :--- | :--- |
| `src/search/search.nim` | 82.35% | 🟢 Excellent |
| `src/errors.nim` | 64.99% | 🟢 Good |
| `src/record/record.nim` | 63.61% | 🟢 Good |
| `src/catalog/catalog.nim` | 61.91% | 🟢 Good |
| `src/pager/db_header.nim` | 61.10% | 🟢 Good |
| `src/vfs/faulty_vfs.nim` | 59.47% | 🟡 Fair |
| `src/vfs/mem_vfs.nim` | 57.36% | 🟡 Fair |
| `src/vfs/types.nim` | 54.55% | 🟡 Fair |
| `src/pager/pager.nim` | 53.98% | 🟡 Fair |
| `src/wal/wal.nim` | 52.20% | 🟡 Fair |
| `src/planner/planner.nim` | 52.03% | 🟡 Fair |
| `src/vfs/os_vfs.nim` | 51.66% | 🟡 Fair |
| `src/storage/storage.nim` | 46.49% | 🟠 Needs Improvement |
| `src/sql/binder.nim` | 43.10% | 🟠 Needs Improvement |
| `src/sql/sql.nim` | 43.02% | 🟠 Needs Improvement |
| `src/btree/btree.nim` | 42.37% | 🟠 Needs Improvement |
| `src/decentdb_cli.nim` | 40.31% | 🟠 Needs Improvement |
| `src/exec/exec.nim` | 32.95% | 🔴 Critical |
| `src/engine.nim` | 31.83% | 🔴 Critical |
| `src/planner/explain.nim` | 80.00% | 🟢 Excellent |
| `src/c_api.nim` | 81.54% | 🟢 Excellent |

---

## Actionable Coverage Checklist

The following table breaks down the modules that require additional unit tests. Future coding agents should pick an unchecked item, write comprehensive tests for the edge cases and error paths in that module, and then check the box.

### 🔴 High Priority (Coverage < 40%)

| Done | Module / Area | Focus Areas for New Tests |
| :---: | :--- | :--- |
| [x] | **`src/c_api.nim`** | Test all exported C functions, handle invalid pointers, null arguments, and ensure proper error code propagation to the C boundary. |
| [x] | **`src/planner/explain.nim`** | Add tests for `EXPLAIN` and `EXPLAIN QUERY PLAN` outputs across various query types (JOINs, subqueries, aggregations). |
| [x] | **`src/engine.nim`** | Added test_engine_coverage.nim with ~60 tests covering: transactions, temp tables, NULL handling, aggregates, string/math/date functions, constraints, prepared statements, FK, operators. |
| [x] | **`src/exec/exec.nim`** | Added test_exec_coverage.nim with ~30 tests covering: UUID parsing, value conversion, LIKE matching, comparison, hashing, row functions, applyLimit, varint. |

### 🟠 Medium Priority (Coverage 40% - 50%)

| Done | Module / Area | Focus Areas for New Tests |
| :---: | :--- | :--- |
| [ ] | **`src/decentdb_cli.nim`** | Test CLI argument parsing, interactive shell commands, `.import`/`.dump` commands, and CLI error formatting. |
| [ ] | **`src/btree/btree.nim`** | Test B-Tree node splitting/merging edge cases, deep tree traversals, deletion of internal nodes, and corruption detection. |
| [ ] | **`src/sql/sql.nim`** | Test SQL parser edge cases, unsupported syntax rejection, and AST generation for complex expressions. |
| [ ] | **`src/sql/binder.nim`** | Test type coercion rules, parameter binding edge cases, scope resolution for subqueries, and ambiguous column name detection. |
| [ ] | **`src/storage/storage.nim`** | Test page allocation/deallocation, overflow page handling for large records, and storage-level corruption recovery. |

### 🟡 Maintenance Priority (Coverage 50% - 60%)

| Done | Module / Area | Focus Areas for New Tests |
| :---: | :--- | :--- |
| [ ] | **`src/vfs/os_vfs.nim`** | Test file locking semantics, concurrent access handling, and simulated I/O errors (disk full, permission denied). |
| [ ] | **`src/planner/planner.nim`** | Test query optimization rules, index selection logic, and join reordering. |
| [ ] | **`src/wal/wal.nim`** | Test WAL frame checksum validation, checkpointing edge cases, and crash recovery scenarios. |
| [ ] | **`src/pager/pager.nim`** | Test page cache eviction policies, dirty page tracking, and concurrent page access. |

## Guidelines for Adding Tests

1. **Follow the Testing Strategy:** Refer to `design/TESTING_STRATEGY.md` and `design/UNIT_TEST_PROMPT.md` before writing tests.
2. **Target Edge Cases:** Don't just test the "happy path". Focus on error conditions, boundary values, and resource exhaustion.
3. **Update this Report:** After merging new tests, re-run the coverage script (`./scripts/coverage_nim.sh`) and update the percentages and checkboxes in this document.