# Memory Leak Hunt Report

Date: 2026-03-20

## Scope Added In This Pass

New leak/lifecycle regression coverage was added for:

1. Repeated `openDb` failure on corrupt WAL headers (error-path lifecycle).
2. Engine open/close loops with mixed operations:
   - transaction begin/commit
   - savepoint create/rollback/release
   - temp table/view create/drop
   - failed parse/bind/resolve path in-loop
3. Shared-WAL cross-connection lifecycle:
   - open A + open B
   - close in alternating orders
   - continued operations on surviving connection
4. WAL timeout cleanup lifecycle:
   - reader timeout via checkpoint
   - intentionally skipping `endRead`
   - verifying close-path cleanup of abort flags
5. C API lifecycle loops:
   - repeated open/close
   - prepare/step/finalize loops
   - bind + execute loops
   - failed prepare loops
   - cross-connection close-order loops
6. Python binding smoke lifecycle:
   - repeated dual-connection write/read/error loops with bounded RSS check

## Initial Failures / Repro Signals

### 1) WAL error-path file descriptor leak (reproduced)

`tests/nim/test_error_path_lifecycle.nim` initially failed with:

- `fdGrowth was 240` after repeated `openDb` failures on a deliberately corrupted WAL file.

This was a high-signal leak: each failed open leaked a descriptor.

### 2) WAL reader abort-flag ownership leak risk

`beginRead` allocates `abortedFlag` via `alloc0` (manual memory).
Timed-out readers were removed from `wal.readers` during checkpoint timeout handling, but cleanup depended on callers eventually invoking `endRead`.

This made close/error/drop paths vulnerable to leaked raw allocations when `endRead` was skipped.

## Root Causes Found

1. **Unclosed WAL file handles in WAL open/recovery error paths**
   - `newWal` had multiple early `return err(...)` branches after opening the file, without closing it.
   - `acquireSharedWal` / `openDb` recovery-failure branches could return without closing WAL resources.

2. **Timeout-aborted reader flag lifetime not anchored to close-path cleanup**
   - `ReadTxn.aborted` uses manual `alloc0` / `dealloc`.
   - Timeout/limit abort paths removed readers but did not guarantee eventual free if `endRead` was not called.

## Fixes Applied

### A) WAL error-path and teardown closure fixes

- Added a centralized engine-side WAL close helper (`closeWalHandle`) that:
  1. cleans up outstanding reader abort flags,
  2. breaks writer cycles,
  3. unmaps WAL mmap region,
  4. closes WAL file.
- Wired this helper into:
  - `acquireSharedWal` race/discard path
  - `acquireSharedWal` recover-failure path
  - `openDb` in-memory recover-failure path
  - `openDb` catalog-init failure path
  - `closeDb` final WAL close path
- Hardened `newWal` with failure-close behavior for all early error returns after file open.

### B) Reader abort-flag lifecycle hardening + instrumentation

- Added WAL structures to track timeout-retired reader flags and IDs whose flags were already freed during close cleanup.
- Added `cleanupReaderFlagsForClose(wal)` and called it from WAL close path.
- Updated checkpoint timeout/size-abort paths to retain pointer ownership metadata for deterministic cleanup.
- Updated `endRead` to avoid double-free when close-path cleanup already freed a flag.
- Added low-risk debug counters:
  - `resetWalAbortFlagStats()`
  - `walAbortFlagStats()`

### C) Shared WAL registry observability

- Added:
  - `sharedWalRegistrySize()`
  - `sharedWalRegistryRefCount(path)`

These are used by lifecycle tests to assert registry refcount behavior across cross-connection close ordering.

## Tests Added / Updated

### New Nim test support

- `tests/nim/lifecycle_test_support.nim`
  - RSS sampling (Linux)
  - occupied Nim heap sampling
  - fd-count sampling (Linux `/proc/self/fd`)
  - warmup + amplification loop utilities
  - Linux `malloc_trim(0)` stabilization helper

### New Nim lifecycle tests

- `tests/nim/test_error_path_lifecycle.nim`
- `tests/nim/test_engine_lifecycle_leaks.nim`
- `tests/nim/test_wal_lifecycle_leaks.nim`
- `tests/nim/test_c_api_lifecycle_leaks.nim`

### New Python smoke

- `bindings/python/tests/test_lifecycle_leak_smoke.py`

### New nimble tasks

- `nimble test_lifecycle`
- `nimble test_arc_leaks` (alias)

## What Remains Suspicious / Not Fully Solved

1. Caller misuse scenarios where language bindings intentionally leak statement handles (`decentdb_finalize` never called) are still outside strict engine guarantees.
2. RSS-based checks remain allocator-sensitive across platforms; deterministic guards are mostly Linux-first.
3. Very long-running multi-threaded reader/writer churn should still be exercised in periodic/nightly stress jobs beyond this focused PR suite.

## How To Run The New Leak Suite

Primary focused suite:

```bash
nimble test_lifecycle
```

Individual Nim suites:

```bash
nim c -r --threads:on -d:useMalloc -d:libpg_query tests/nim/test_error_path_lifecycle.nim
nim c -r --threads:on -d:useMalloc -d:libpg_query tests/nim/test_engine_lifecycle_leaks.nim
nim c -r --threads:on -d:useMalloc -d:libpg_query tests/nim/test_wal_lifecycle_leaks.nim
nim c -r --threads:on -d:useMalloc -d:libpg_query tests/nim/test_c_api_lifecycle_leaks.nim
```

Python smoke:

```bash
cd bindings/python
pytest -q tests/test_lifecycle_leak_smoke.py
```

## Platform-Sensitive Notes

- Linux-only/biased checks:
  - fd counting via `/proc/self/fd`
  - RSS sampling via `/proc/self/status`
  - optional allocator trimming via `malloc_trim(0)` in test stabilization helper
- Non-Linux platforms still run functional lifecycle loops and Nim-heap checks, but skip Linux-only metrics.
