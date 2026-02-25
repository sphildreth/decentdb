# DecentDB Test Coverage Report

**Date:** 2026-02-24
**Overall Nim Coverage:** ~55.68% (84,945 / 152,552 lines)

This report outlines the current state of unit test coverage across the DecentDB codebase. The goal is to identify areas with low coverage and provide an actionable checklist for future coding agents to improve the robustness of the test suite.

## Current Coverage Summary

| Module | Coverage | Status |
| :--- | :--- | :--- |
| `src/pager/db_header.nim` | 95.35% | 🟢 Excellent |
| `src/search/search.nim` | 94.40% | 🟢 Excellent |
| `src/planner/explain.nim` | 93.21% | 🟢 Excellent |
| `src/c_api.nim` | 81.90% | 🟢 Excellent |
| `src/record/record.nim` | 80.25% | 🟢 Excellent |
| `src/catalog/catalog.nim` | 75.10% | 🟢 Good |
| `src/vfs/mem_vfs.nim` | 74.81% | 🟢 Good |
| `src/errors.nim` | 74.52% | 🟢 Good |
| `src/vfs/faulty_vfs.nim` | 74.41% | 🟢 Good |
| `src/pager/pager.nim` | 68.72% | 🟢 Good |
| `src/planner/planner.nim` | 68.41% | 🟢 Good |
| `src/wal/wal.nim` | 67.16% | 🟢 Good |
| `src/vfs/os_vfs.nim` | 65.88% | 🟢 Good |
| `src/decentdb_cli.nim` | 59.68% | 🟡 Fair |
| `src/storage/storage.nim` | 59.61% | 🟡 Fair |
| `src/vfs/types.nim` | 59.09% | 🟡 Fair |
| `src/sql/binder.nim` | 58.03% | 🟡 Fair |
| `src/sql/sql.nim` | 57.76% | 🟡 Fair |
| `src/btree/btree.nim` | 54.33% | 🟡 Fair |
| `src/engine.nim` | 44.64% | 🟠 Needs Improvement |
| `src/exec/exec.nim` | 43.54% | 🟠 Needs Improvement |

---

## Actionable Coverage Checklist

The following table breaks down the modules that require additional unit tests. Future coding agents should pick an unchecked item, write comprehensive tests for the edge cases and error paths in that module, and then check the box.

### 🔴 High Priority (Coverage < 50%)

| Done | Module / Area | Focus Areas for New Tests |
| :---: | :--- | :--- |
| [ ] | **`src/engine.nim`** | Added test_engine_coverage.nim with ~60 tests covering: transactions, temp tables, NULL handling, aggregates, string/math/date functions, constraints, prepared statements, FK, operators. Needs more coverage for edge cases. |
| [ ] | **`src/exec/exec.nim`** | Added test_exec_coverage.nim with ~30 tests covering: UUID parsing, value conversion, LIKE matching, comparison, hashing, row functions, applyLimit, varint. Needs more coverage for edge cases. |

### 🟠 Medium Priority (Coverage 50% - 60%)

| Done | Module / Area | Focus Areas for New Tests |
| :---: | :--- | :--- |
| [x] | **`src/decentdb_cli.nim`** | Added test_cli_coverage.nim to test CLI argument parsing, interactive schema/index, import/export/dump commands, heuristics, and diagnostics. Coverage increased to ~59.68%. |
| [ ] | **`src/btree/btree.nim`** | Test B-Tree node splitting/merging edge cases, deep tree traversals, deletion of internal nodes, and corruption detection. |
| [ ] | **`src/sql/sql.nim`** | Test SQL parser edge cases, unsupported syntax rejection, and AST generation for complex expressions. |
| [ ] | **`src/sql/binder.nim`** | Test type coercion rules, parameter binding edge cases, scope resolution for subqueries, and ambiguous column name detection. |
| [ ] | **`src/vfs/types.nim`** | Test VFS type definitions and edge cases. |
| [ ] | **`src/storage/storage.nim`** | Test page allocation/deallocation, overflow page handling for large records, and storage-level corruption recovery. |

### 🟡 Maintenance Priority (Coverage 60% - 70%)

| Done | Module / Area | Focus Areas for New Tests |
| :---: | :--- | :--- |
| [ ] | **`src/vfs/os_vfs.nim`** | Test file locking semantics, concurrent access handling, and simulated I/O errors (disk full, permission denied). |
| [ ] | **`src/wal/wal.nim`** | Test WAL frame checksum validation, checkpointing edge cases, and crash recovery scenarios. |
| [ ] | **`src/planner/planner.nim`** | Test query optimization rules, index selection logic, and join reordering. |
| [ ] | **`src/pager/pager.nim`** | Test page cache eviction policies, dirty page tracking, and concurrent page access. |

## Guidelines for Adding Tests

1. **Follow the Testing Strategy:** Refer to `design/TESTING_STRATEGY.md` and `design/UNIT_TEST_PROMPT.md` before writing tests.
2. **Target Edge Cases:** Don't just test the "happy path". Focus on error conditions, boundary values, and resource exhaustion.
3. **Update this Report:** After merging new tests, re-run the coverage script (`./scripts/coverage_nim.sh`) and update the percentages and checkboxes in this document.
