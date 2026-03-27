# Binding Review for V2

**Date:** 2026-03-27
**Status:** Draft
**Scope:** Comprehensive review of all language bindings (.NET, Python, Go, Java, Node.js, Dart) against the current C ABI (`include/decentdb.h`) and engine capabilities. Identifies performance gaps, correctness bugs, missing features, and test deficiencies. Produces a phased task list for each binding toward V2 quality.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [C ABI Baseline](#2-c-abi-baseline)
3. [Cross-Cutting Concerns](#3-cross-cutting-concerns)
4. [.NET Binding Review](#4-net-binding-review)
5. [Python Binding Review](#5-python-binding-review)
6. [Go Binding Review](#6-go-binding-review)
7. [Java Binding Review](#7-java-binding-review)
8. [Node.js Binding Review](#8-nodejs-binding-review)
9. [Dart Binding Review](#9-dart-binding-review)
10. [Benchmark and Example Quality](#10-benchmark-and-example-quality)
11. [Prioritized Global Task Matrix](#11-prioritized-global-task-matrix)
12. [Appendix A: C ABI Function Reference](#appendix-a-c-abi-function-reference)

---

## 1. Executive Summary

DecentDB's C ABI exposes 50 functions covering database lifecycle, prepared statements, typed parameter binding, fused bind+step operations, batch execution, re-execute patterns, zero-copy row views, result sets, transactions, schema introspection, and maintenance. The six language bindings exhibit significant variance in coverage, performance posture, and correctness.

### Coverage at a Glance

| Binding | Functions Exposed | Batch Ops | Fused Bind+Step | Re-Execute | Row Views | Result Set |
|---------|------------------|-----------|-----------------|------------|-----------|------------|
| .NET    | 29/50 (58%)      | 0/4       | 0/2             | 0/3        | 0/4       | 0/6        |
| Python  | 35/50 (70%)      | 3/4       | 1/2             | 3/3        | 3/4       | 0/6        |
| Go      | 28/50 (56%)      | 0/4       | 0/2             | 0/3        | 1/4       | 0/6        |
| Java    | 25/50 (50%)      | 0/4       | 0/2             | 0/3        | 1/4       | 0/6        |
| Node.js | 30/50 (60%)      | 2/4       | 0/2             | 0/3        | 1/4       | 0/6        |
| Dart    | 27/50 (54%)      | 0/4       | 0/2             | 0/3        | 0/4       | 6/6        |

### Critical Findings

1. **No binding exposes all batch, fused, and re-execute fast paths.** These are the exact functions designed for throughput — they reduce FFI crossings by 3-10x per operation. Python comes closest (through `_fastdecode.c` C extension), but even Python doesn't declare these in its Python-side ctypes layer.

2. **Data corruption bugs exist in Java.** BigDecimal binding loses scale information. Timestamp microsecond conversion uses incorrect arithmetic, losing 999 out of every 1000 microseconds.

3. **Dart bypasses native prepared statements entirely.** It uses `ddb_db_execute` for every query, sending the full SQL string across the FFI boundary each time. This makes it the slowest binding by design.

4. **Zero bindings expose the result set handle API** (`ddb_result_t`) except Dart. The result set API enables one-shot queries without separate prepare/step lifecycle and is essential for simple DDL/DML.

5. **No binding exposes `ddb_db_in_transaction`** for engine-truth transaction state checking. All bindings track transaction state in their own managed layer, which can drift from the engine's actual state.

---

## 2. C ABI Baseline

The C ABI (`include/decentdb.h`) is the stable native boundary that all bindings must target. It exposes 50 functions organized into these categories:

### Lifecycle (6 functions)
- `ddb_abi_version`, `ddb_version`, `ddb_last_error_message`
- `ddb_value_init`, `ddb_value_dispose`, `ddb_string_free`

### Database (4 functions)
- `ddb_db_create`, `ddb_db_open`, `ddb_db_open_or_create`, `ddb_db_free`

### Statements (4 functions)
- `ddb_db_prepare`, `ddb_stmt_free`, `ddb_stmt_reset`, `ddb_stmt_clear_bindings`

### Binding (8 functions)
- `ddb_stmt_bind_null`, `ddb_stmt_bind_int64`, `ddb_stmt_bind_float64`, `ddb_stmt_bind_bool`
- `ddb_stmt_bind_text`, `ddb_stmt_bind_blob`, `ddb_stmt_bind_decimal`, `ddb_stmt_bind_timestamp_micros`

### Fused Bind+Step (2 functions)
- `ddb_stmt_bind_int64_step_row_view` — bind int64 param + step + get row view in one call
- `ddb_stmt_bind_int64_step_i64_text_f64` — bind int64 param + step + get typed row in one call

### Batch Execution (3 functions)
- `ddb_stmt_execute_batch_i64` — bulk insert for single int64 column
- `ddb_stmt_execute_batch_i64_text_f64` — bulk insert for (int64, text, float64) rows
- `ddb_stmt_execute_batch_typed` — generic typed batch with signature string

### Execution (3 functions)
- `ddb_stmt_step`, `ddb_stmt_column_count`, `ddb_stmt_column_name_copy`, `ddb_stmt_affected_rows`

### Re-Execute (3 functions)
- `ddb_stmt_rebind_int64_execute` — re-bind one int64 + step + affected rows in one call
- `ddb_stmt_rebind_text_int64_execute` — re-bind text+int64 + step + affected rows
- `ddb_stmt_rebind_int64_text_execute` — re-bind int64+text + step + affected rows

### Value Access (5 functions)
- `ddb_stmt_value_copy`, `ddb_stmt_row_view`, `ddb_stmt_step_row_view`
- `ddb_stmt_fetch_row_views`, `ddb_stmt_fetch_rows_i64_text_f64`

### Immediate Execute (1 function)
- `ddb_db_execute` — one-shot query returning `ddb_result_t`

### Transactions (4 functions)
- `ddb_db_begin_transaction`, `ddb_db_commit_transaction`, `ddb_db_rollback_transaction`, `ddb_db_in_transaction`

### Maintenance (3 functions)
- `ddb_db_checkpoint`, `ddb_db_save_as`, `ddb_evict_shared_wal`

### Schema Introspection (7 functions)
- `ddb_db_list_tables_json`, `ddb_db_describe_table_json`, `ddb_db_get_table_ddl`
- `ddb_db_list_indexes_json`, `ddb_db_list_views_json`, `ddb_db_get_view_ddl`, `ddb_db_list_triggers_json`

### Result Set (6 functions)
- `ddb_result_free`, `ddb_result_row_count`, `ddb_result_column_count`
- `ddb_result_affected_rows`, `ddb_result_column_name_copy`, `ddb_result_value_copy`

---

## 3. Cross-Cutting Concerns

These issues apply to multiple bindings and should be addressed at the C ABI or documentation level before or alongside binding-level fixes.

### 3.1 Result Set API Adoption

**Issue:** Only Dart binds the `ddb_result_t` family. The result set API enables one-shot execution (`ddb_db_execute`) without separate prepare/step lifecycle management. This is simpler for DDL, simple DML, and schema introspection where the user doesn't need prepared statement reuse.

**Impact:** All other bindings either use raw SQL execution through the statement API or delegate to the engine's internal query path. The result set API provides a cleaner model for the common case.

**Task:** Each binding should expose `ddb_db_execute` returning a `Result`/`ResultSet` object that wraps `ddb_result_t` with row/column iteration, then auto-frees on dispose.

### 3.2 Version and ABI Introspection

**Issue:** `ddb_abi_version` and `ddb_version` are exposed by Dart only. Other bindings cannot programmatically verify that the loaded native library matches the expected ABI version at load time.

**Task:** All bindings should call `ddb_abi_version()` at library load and validate compatibility. `ddb_version()` should be exposed as a public API for diagnostics and logging.

### 3.3 Transaction State Query

**Issue:** `ddb_db_in_transaction` is declared in Python's ctypes layer but never called. No other binding exposes it. All bindings track transaction state in their own managed layer, which can drift from the engine's actual state (e.g., if `BEGIN`/`COMMIT` SQL is executed directly).

**Task:** All bindings should expose `in_transaction` as a read-only property that queries the engine directly.

### 3.4 Schema Introspection Gaps

**Issue:** Several bindings are missing schema API coverage:
- `ddb_db_get_table_ddl` — missing in .NET, Python, Go, Java, Node.js
- `ddb_db_list_views_json` — missing in .NET, Python, Go, Node.js
- `ddb_db_get_view_ddl` — missing in .NET, Python, Go, Node.js
- `ddb_db_list_triggers_json` — missing in .NET, Python, Go, Node.js

**Task:** All bindings should expose the full schema introspection surface: tables, table DDL, columns, indexes, views, view DDL, and triggers.

### 3.5 Database Open Mode

**Issue:** Several bindings only expose `ddb_db_open_or_create` and cannot enforce create-only or open-only semantics:
- .NET — always uses `open_or_create`
- Python — always uses `open_or_create`
- Node.js — default is `open_or_create`, mode parameter exists but is inconsistent

**Task:** All bindings should expose `ddb_db_create`, `ddb_db_open`, and `ddb_db_open_or_create` as distinct connection modes.

### 3.6 Thread Safety Documentation

**Issue:** The engine guarantees one-writer/multiple-readers per process. No binding documents this constraint at the API level. Users can accidentally share a connection across threads and violate the contract.

**Task:** All bindings should document thread safety constraints in class/method documentation and consider adding runtime guards or thread-affinity checks.

---

## 4. .NET Binding Review

**Location:** `bindings/dotnet/`
**Architecture:** P/Invoke layer (`DecentDB.Native`) → ADO.NET provider (`DecentDB.AdoNet`) → Micro ORM (`DecentDB.MicroOrm`) → EF Core provider (`DecentDB.EntityFrameworkCore`)
**Coverage:** 29/50 functions (58%)

### 4.1 Critical Issues

#### 4.1.1 Batch Operations Completely Missing

The C ABI provides `ddb_stmt_execute_batch_i64`, `ddb_stmt_execute_batch_i64_text_f64`, and `ddb_stmt_execute_batch_typed` for high-throughput bulk inserts. None are bound. `InsertManyAsync` in `DbSet.cs:245` loops individual INSERT+Step statements in a transaction. For bulk workloads this is the single largest performance gap in the .NET binding.

**Files:** `bindings/dotnet/src/DecentDB.Native/NativeMethods.cs`, `bindings/dotnet/src/DecentDB.MicroOrm/DbSet.cs`

#### 4.1.2 Fused Bind+Step Missing

`ddb_stmt_bind_int64_step_row_view` and `ddb_stmt_bind_int64_step_i64_text_f64` combine bind+execute+read in one native call, eliminating two P/Invoke crossings. Every point read goes through `BindInt64` → `Step` → `CopyValue` → `GetValueObject` → `DisposeValue` — 4+ P/Invoke crossings where 1 would suffice.

**Files:** `bindings/dotnet/src/DecentDB.Native/NativeMethods.cs`, `bindings/dotnet/src/DecentDB.Native/DecentDB.cs`

#### 4.1.3 Zero-Copy Row Views Missing

`ddb_stmt_row_view`, `ddb_stmt_step_row_view`, and `ddb_stmt_fetch_row_views` return pointers into native memory without copying. Every column read copies through `ddb_stmt_value_copy` → `Marshal.PtrToStringUTF8` / `Marshal.Copy`. No `ReadOnlySpan<byte>` overloads exist for text/blob binding.

**Files:** `bindings/dotnet/src/DecentDB.Native/DecentDB.cs`

#### 4.1.4 Re-Execute Patterns Missing

`ddb_stmt_rebind_int64_execute`, `ddb_stmt_rebind_text_int64_execute`, and `ddb_stmt_rebind_int64_text_execute` allow prepared statement reuse without `Reset+ClearBindings+Bind+Step`. Not bound. The MicroOrm's `UpdateAsync` and `DeleteAsync` would benefit directly.

**Files:** `bindings/dotnet/src/DecentDB.Native/NativeMethods.cs`

#### 4.1.5 Guid Bound as Blob, Not UUID

`DecentDBCommand.BindParameter` at line 606-608 converts `Guid` to `byte[]` via `ToByteArray()` and binds as blob. The engine stores and returns UUIDs natively via `DdbValueNative.uuidBytes`, but the .NET binding reads them back as blobs (`GetBlob` → `new Guid(bytes)`) rather than using the native UUID path.

**File:** `bindings/dotnet/src/DecentDB.AdoNet/DecentDBCommand.cs:606`

#### 4.1.6 `DecentdbValueView` Struct Mismatch

`NativeMethods.cs:295-305` defines a struct that does not match the C `ddb_value_view_t`. Its fields (`kind`, `is_null`, `int64_val`, `float64_val`, `bytes`, `bytes_len`, `decimal_scale`) don't correspond to the C layout (`tag`, `bool_value`, `reserved0`, `int64_value`, `float64_value`, `decimal_scaled`, `decimal_scale`, `reserved1`, `data`, `len`, `uuid_bytes`, `timestamp_micros`). While unused for interop, it's publicly exposed and misleading.

**File:** `bindings/dotnet/src/DecentDB.Native/NativeMethods.cs:295`

#### 4.1.7 MicroOrm DateTime Conversion Bug

`TypeConverters.cs:15` converts DateTime to Unix milliseconds via `ToUnixTimeMilliseconds()`. The engine stores microseconds. `DecentDBCommand.BindParameter` correctly converts to microseconds. If MicroOrm's `TypeConverters.ToDbValue` feeds into ADO.NET parameters, values will be 1000x too small.

**File:** `bindings/dotnet/src/DecentDB.MicroOrm/TypeConverters.cs:15`

#### 4.1.8 Ordinal Lookup is O(n)

`DecentDBDataReader.GetOrdinal` at line 335 does linear scan. Should build a `Dictionary<string, int>` on first use.

**File:** `bindings/dotnet/src/DecentDB.AdoNet/DecentDBDataReader.cs:335`

### 4.2 Moderate Issues

#### 4.2.1 No Async I/O Offload

`OpenAsync` just calls `Open()` synchronously. No actual I/O offload. All step/execute calls are synchronous.

**File:** `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnection.cs:172`

#### 4.2.2 `options` Parameter Silently Ignored

`DecentDBConnection` constructor accepts `options` but discards it (`_ = options` at line 40). The connection string builder supports `Cache Size` but it has no effect.

**File:** `bindings/dotnet/src/DecentDB.AdoNet/DecentDBConnection.cs:40`

#### 4.2.3 Statement Handle Lifetime Not Enforced

`DecentDBStatementHandle` holds a reference to `DecentDBHandle` but this doesn't prevent the DB from being freed first. No reference-counting or ownership ordering guarantee.

**File:** `bindings/dotnet/src/DecentDB.Native/SafeHandles.cs:45`

#### 4.2.4 `SelectAsync` Loads Full Entity

`DbSet.cs:448-468` projects via selector but materializes the full entity first, then applies the selector. Should push projection columns into SQL `SELECT col1, col2`.

**File:** `bindings/dotnet/src/DecentDB.MicroOrm/DbSet.cs:448`

### 4.3 Phased Tasks

#### Phase 1: Performance Foundation (Critical Path)

| # | Task | Files | Impact |
|---|------|-------|--------|
| D1.1 | Bind `ddb_stmt_execute_batch_i64`, `ddb_stmt_execute_batch_i64_text_f64`, `ddb_stmt_execute_batch_typed` | `NativeMethods.cs`, new batch helper | Enables 10-100x bulk insert throughput |
| D1.2 | Bind `ddb_stmt_bind_int64_step_row_view` and `ddb_stmt_bind_int64_step_i64_text_f64` | `NativeMethods.cs`, `DecentDB.cs` | Reduces point-read FFI crossings from 4+ to 1 |
| D1.3 | Bind `ddb_stmt_step_row_view` and `ddb_stmt_fetch_row_views` | `NativeMethods.cs`, new row view API | Reduces scan FFI crossings from 2N to ~N/B where B=batch size |
| D1.4 | Bind `ddb_stmt_rebind_int64_execute`, `ddb_stmt_rebind_text_int64_execute`, `ddb_stmt_rebind_int64_text_execute` | `NativeMethods.cs`, `PreparedStatement.cs` | Fast UPDATE/DELETE by primary key |
| D1.5 | Add `ReadOnlySpan<byte>` overloads for `BindText` and `BindBlob` | `DecentDB.cs` | Zero-allocation binding for short values |
| D1.6 | Wire batch operations into `InsertManyAsync` | `DbSet.cs` | Bulk insert performance for MicroOrm |

#### Phase 2: Correctness Fixes

| # | Task | Files | Impact |
|---|------|-------|--------|
| D2.1 | Fix Guid binding to use UUID type instead of blob | `DecentDBCommand.cs`, `DecentDBDataReader.cs` | Correct UUID round-trip |
| D2.2 | Fix MicroOrm DateTime to use microseconds, not milliseconds | `TypeConverters.cs` | Prevents 1000x timestamp error |
| D2.3 | Remove or align `DecentdbValueView` struct with C layout | `NativeMethods.cs` | Public API correctness |
| D2.4 | Build ordinal dictionary in `DecentDBDataReader` | `DecentDBDataReader.cs` | O(1) column lookup vs O(n) |
| D2.5 | Push projection into SQL in `SelectAsync` | `DbSet.cs` | Reduces data transfer and memory |

#### Phase 3: Feature Completeness

| # | Task | Files | Impact |
|---|------|-------|--------|
| D3.1 | Bind `ddb_db_create`, `ddb_db_open` as distinct modes | `NativeMethods.cs`, `DecentDBConnection.cs` | Create-only and open-only semantics |
| D3.2 | Bind `ddb_abi_version`, `ddb_version` | `NativeMethods.cs`, new version API | ABI verification at load time |
| D3.3 | Bind `ddb_db_in_transaction` | `NativeMethods.cs`, `DecentDBConnection.cs` | Engine-truth transaction state |
| D3.4 | Bind `ddb_db_get_table_ddl`, `ddb_db_list_views_json`, `ddb_db_get_view_ddl`, `ddb_db_list_triggers_json` | `NativeMethods.cs`, `DecentDBConnection.cs` | Full schema introspection |
| D3.5 | Bind `ddb_db_execute` + `ddb_result_*` family | `NativeMethods.cs`, new result class | One-shot query API |
| D3.6 | Bind `ddb_db_checkpoint`, `ddb_db_save_as`, `ddb_evict_shared_wal` in ADO.NET layer | `DecentDBConnection.cs` | Maintenance from managed code |
| D3.7 | Wire `options`/`Cache Size` through to native open | `DecentDBConnection.cs` | Connection configuration actually works |

#### Phase 4: Polish and Testing

| # | Task | Files | Impact |
|---|------|-------|--------|
| D4.1 | Add tests for batch operations | `tests/` | Verify bulk insert correctness |
| D4.2 | Add tests for fused bind+step | `tests/` | Verify fast-path data integrity |
| D4.3 | Add tests for concurrent reader threads | `tests/` | Validate multi-reader model |
| D4.4 | Add DECIMAL read/write round-trip test | `tests/` | Catch scale-related bugs |
| D4.5 | Add TIMESTAMP_MICROS round-trip test with microsecond precision | `tests/` | Catch precision loss bugs |
| D4.6 | Document thread-safety constraints on `DecentDB` class | `DecentDB.cs` | User-facing contract clarity |
| D4.7 | Update `bindings-matrix.md` with .NET feature status | `docs/api/bindings-matrix.md` | Documentation accuracy |

---

## 5. Python Binding Review

**Location:** `bindings/python/`
**Architecture:** ctypes FFI (`native.py`) → DB-API 2.0 driver (`__init__.py`) → CPython accelerator (`_fastdecode.c`) → SQLAlchemy dialect (`decentdb_sqlalchemy/`)
**Coverage:** 35/50 functions (70%)

Python has the most mature binding, with a C extension (`_fastdecode.c`, 2184 lines) providing 33 CPython functions for hot-path acceleration. The multi-tier fallback system (C extension → ctypes fused → ctypes generic → Python loop) is well-designed.

### 5.1 Critical Issues

#### 5.1.1 `__init__.py` is 3200+ Lines — Maintainability Crisis

The Cursor class alone spans ~2900 lines with extreme duplication. The value decoding tag-switch block is copy-pasted approximately 10 times across the file in `_decode_row_view_values`, `_decode_row_view_matrix`, and various fast-path handlers. This makes the code fragile and error-prone.

**File:** `bindings/python/decentdb/__init__.py`

#### 5.1.2 Missing `ddb_db_create` / `ddb_db_open`

Only `ddb_db_open_or_create` is exposed. Users cannot create a database that must not already exist or open a database that must already exist. This prevents fail-fast semantics for deployment and testing scenarios.

**File:** `bindings/python/decentdb/__init__.py:3044`

#### 5.1.3 Missing Schema Introspection for Views and Triggers

`ddb_db_list_views_json`, `ddb_db_get_view_ddl`, `ddb_db_list_triggers_json`, and `ddb_db_get_table_ddl` are not exposed. The `Connection` class has no `list_views()`, `get_view_ddl()`, `get_table_ddl()`, or `list_triggers()` methods despite the C ABI providing these functions.

**File:** `bindings/python/decentdb/__init__.py`

#### 5.1.4 `connect()` Silently Drops `**kwargs`

`cache_pages` and `cache_mb` kwargs accepted by `Connection.__init__` are silently dropped because `connect()` only pops `stmt_cache_size` and passes `**kwargs` as `Connection(dsn, stmt_cache_size=stmt_cache_size)` — discarding everything else.

**File:** `bindings/python/decentdb/__init__.py:3195`

#### 5.1.5 SQLAlchemy Type Mappings Incorrect

- `Numeric` mapped to TEXT (`dialect.py:187`) — should use native `DECIMAL`
- `Date`/`DateTime`/`Time` mapped to INT64 (`dialect.py:199-206`) — should use `TIMESTAMP_MICROS`
- `Uuid` mapped to BLOB (`dialect.py:208-209`) — should use native `UUID`

This loses the native type semantics the engine supports and causes downstream issues with ORMs that rely on correct type mapping.

**File:** `bindings/python/decentdb_sqlalchemy/dialect.py`

### 5.2 Moderate Issues

#### 5.2.1 `native.py` Missing Declarations for C-Extension Functions

Functions like `ddb_stmt_execute_batch_typed`, `ddb_stmt_fetch_rows_i64_text_f64`, and rebind functions are called by `_fastdecay.c` but not declared in the Python ctypes layer. This creates an incomplete picture of what the binding actually uses and makes it harder for contributors to understand the full surface.

**File:** `bindings/python/decentdb/native.py`

#### 5.2.2 Legacy Error Codes Are Dead Code

`native.py:27-33` defines error codes `ERR_ERROR=100` through `ERR_NOMEM=106` that don't exist in the C ABI header. They are handled in `_raise_error` but never returned by the engine. This dead code creates confusion.

**File:** `bindings/python/decentdb/native.py:27`

#### 5.2.3 `ddb_db_in_transaction` Declared but Never Called

`native.py:189` declares the function but `Connection._in_explicit_txn` is Python-side state, never querying the engine for truth.

**File:** `bindings/python/decentdb/__init__.py`

#### 5.2.4 No Version Introspection API

`ddb_abi_version` and `ddb_version` are not declared. Users cannot verify ABI compatibility or report engine version for diagnostics.

**File:** `bindings/python/decentdb/native.py`

### 5.3 Phased Tasks

#### Phase 1: Code Quality Foundation

| # | Task | Files | Impact |
|---|------|-------|--------|
| P1.1 | Extract `Cursor` into `decentdb/cursor.py` module | `decentdb/__init__.py` → new `cursor.py` | Reduces `__init__.py` from 3200 to ~300 lines |
| P1.2 | Deduplicate value decoding — extract `_decode_single_value(tag, value)` | `cursor.py` (new), all decode paths | Eliminates ~9 copy-pasted tag-switch blocks |
| P1.3 | Extract fast-path dispatch into `decentdb/fastpath.py` | `cursor.py` → new `fastpath.py` | Separates the 60+ enable flags and support caches from cursor logic |
| P1.4 | Remove dead legacy error codes | `native.py:27-33` | Reduces confusion |
| P1.5 | Fix `connect()` kwargs passthrough | `__init__.py:3195` | `cache_pages` and `cache_mb` actually work |

#### Phase 2: Feature Completeness

| # | Task | Files | Impact |
|---|------|-------|--------|
| P2.1 | Add `Connection.list_views()`, `get_view_ddl()`, `list_triggers()` | `__init__.py` | Full schema introspection |
| P2.2 | Add `Connection.get_table_ddl()` | `__init__.py` | DDL introspection for tables |
| P2.3 | Add `Connection.in_transaction` property using `ddb_db_in_transaction` | `__init__.py` | Engine-truth transaction state |
| P2.4 | Expose `ddb_db_create` and `ddb_db_open` as connection modes | `__init__.py` | Create-only and open-only semantics |
| P2.5 | Expose `ddb_abi_version` and `decentdb.version()` | `native.py`, `__init__.py` | Version introspection |
| P2.6 | Declare all C ABI functions in `native.py` | `native.py` | Complete ctypes picture |
| P2.7 | Add `Result` wrapper for `ddb_result_t` family | new `result.py` or in `__init__.py` | One-shot query API |

#### Phase 3: SQLAlchemy and Ecosystem

| # | Task | Files | Impact |
|---|------|-------|--------|
| P3.1 | Fix `Numeric` → DECIMAL mapping | `dialect.py` | Correct DECIMAL round-trip through SQLAlchemy |
| P3.2 | Fix `Date`/`DateTime`/`Time` → TIMESTAMP_MICROS mapping | `dialect.py` | Native timestamp semantics |
| P3.3 | Fix `Uuid` → UUID mapping | `dialect.py` | Native UUID semantics |
| P3.4 | Implement `get_unique_constraints` | `dialect.py` | SQLAlchemy DDL reflection |
| P3.5 | Implement `get_check_constraints` | `dialect.py` | SQLAlchemy DDL reflection |

#### Phase 4: Testing and Benchmarks

| # | Task | Files | Impact |
|---|------|-------|--------|
| P4.1 | Add tests for `list_views`, `get_view_ddl`, `list_triggers`, `get_table_ddl` | `tests/` | Verify schema introspection |
| P4.2 | Add DECIMAL round-trip test through SQLAlchemy | `tests/` | Catch type mapping bugs |
| P4.3 | Add concurrent reader thread test | `tests/` | Validate multi-reader model |
| P4.4 | Benchmark statement cache hit/miss rates | `benchmarks/` | Quantify cache effectiveness |
| P4.5 | Benchmark `_fastdecode.c` vs pure ctypes paths | `benchmarks/` | Quantify C extension speedup |

---

## 6. Go Binding Review

**Location:** `bindings/go/decentdb-go/`
**Architecture:** `database/sql` driver (`driver.go`) with cgo FFI
**Coverage:** 28/50 functions (56%)

### 6.1 Critical Issues

#### 6.1.1 All Batch/Fused/Re-Execute Fast Paths Missing

The Go binding misses every performance-oriented C ABI function: no batch insert, no fused bind+step, no re-execute, no batch fetch. For a 1M row scan, this means 2M+ cgo crossings where it could be ~1K with `ddb_stmt_fetch_row_views`. The `SQLC_SUPPORT.md` design doc explicitly identifies these as essential for meeting the <1ms overhead budget.

**File:** `bindings/go/decentdb-go/driver.go`

#### 6.1.2 DSN `mode=create` Bug

The `connector.Connect` method at line 73 calls `ddb_db_open_or_create` first, then at line 79-85 checks the `mode` parameter and calls `ddb_db_create` again if `mode=create`. This means for `?mode=create`, it first opens or creates, then **re-creates** — wasting a call and potentially opening the wrong database. The mode should be parsed before any native call.

**File:** `bindings/go/decentdb-go/driver.go:73-85`

#### 6.1.3 No `runtime.SetFinalizer` — Memory Leak Path

Neither `conn` nor `stmtStruct` have `runtime.SetFinalizer` registered. If a user abandons a `*sql.DB` without calling `Close()`, the underlying C handles leak until process exit. This is a memory leak path for long-lived applications.

**File:** `bindings/go/decentdb-go/driver.go`

#### 6.1.4 `driver.ErrBadConn` Never Returned

When `c.db == nil` after close, subsequent calls return `errors.New("connection is closed")` instead of `driver.ErrBadConn`. The `database/sql` connection pool uses `ErrBadConn` to retry on a fresh connection. Not returning it means pool recovery is broken.

**File:** `bindings/go/decentdb-go/driver.go:275`

#### 6.1.5 `modernc.org/sqlite` in Production Dependencies

The SQLite driver is a production dependency (`require`) in `go.mod`, not a test-only or benchmark-only dependency. It should be moved to a separate benchmark module or use build tags.

**File:** `bindings/go/decentdb-go/go.mod:6`

### 6.2 Moderate Issues

#### 6.2.1 Two cgo Crossings Per Row Instead of One

`rows.Next` calls `ddb_stmt_step` at line 721 and then `ddb_stmt_row_view` at line 731 — two cgo crossings per row. The fused `ddb_stmt_step_row_view` does exactly this in one call.

**File:** `bindings/go/decentdb-go/driver.go:721-731`

#### 6.2.2 No Buffer Pooling for TEXT/BLOB

Every `C.GoStringN` and `C.GoBytes` call allocates a new Go buffer. No `sync.Pool` reuse. For large TEXT columns, this creates significant GC pressure.

**File:** `bindings/go/decentdb-go/driver.go:757,763`

#### 6.2.3 LSN Discarded on Commit

`ddb_db_commit_transaction` returns an LSN via `out_lsn`. The Go binding captures it at line 496 but discards it. This LSN could be useful for WAL position tracking in replication scenarios.

**File:** `bindings/go/decentdb-go/driver.go:496`

#### 6.2.4 No Schema Introspection for Views, Triggers, Table DDL

`ddb_db_get_table_ddl`, `ddb_db_list_views_json`, `ddb_db_get_view_ddl`, `ddb_db_list_triggers_json` are not exposed.

**File:** `bindings/go/decentdb-go/driver.go`

### 6.3 Phased Tasks

#### Phase 1: Performance (Critical for sqlc Readiness)

| # | Task | Files | Impact |
|---|------|-------|--------|
| G1.1 | Use `ddb_stmt_step_row_view` instead of separate `step` + `row_view` | `driver.go:721` | Reduces cgo crossings per row from 2 to 1 |
| G1.2 | Bind `ddb_stmt_bind_int64_step_row_view` for point-read hotpath | `driver.go`, new fast-path method | Reduces point-read cgo crossings from ~5 to 1 |
| G1.3 | Bind `ddb_stmt_fetch_row_views` for batch iteration | `driver.go`, new batch fetch method | Reduces scan cgo crossings from 2N to 2(N/B) |
| G1.4 | Bind `ddb_stmt_rebind_int64_execute` and related | `driver.go`, `stmtStruct` methods | Fast UPDATE/DELETE by primary key |
| G1.5 | Bind `ddb_stmt_execute_batch_i64_text_f64` and `ddb_stmt_execute_batch_typed` | `driver.go`, new batch execute method | Bulk insert throughput |
| G1.6 | Pool byte buffers via `sync.Pool` for TEXT/BLOB reads | `driver.go:757` | Reduces GC pressure on scans |

#### Phase 2: Correctness

| # | Task | Files | Impact |
|---|------|-------|--------|
| G2.1 | Fix DSN `mode=create` bug — parse mode before native call | `driver.go:68-85` | Prevents wrong database opened |
| G2.2 | Return `driver.ErrBadConn` when `c.db == nil` | `driver.go:275` | Connection pool recovery works |
| G2.3 | Add `runtime.SetFinalizer` on `conn` and `stmtStruct` | `driver.go` | Prevents native handle leaks |
| G2.4 | Move `modernc.org/sqlite` to benchmark-only module | `go.mod` | Clean production dependency tree |

#### Phase 3: Feature Completeness

| # | Task | Files | Impact |
|---|------|-------|--------|
| G3.1 | Expose `ddb_db_execute` + `ddb_result_*` as fast-path `Exec` | `driver.go` | One-shot queries without prepare/step |
| G3.2 | Expose `ddb_db_get_table_ddl`, `ddb_db_list_views_json`, `ddb_db_get_view_ddl`, `ddb_db_list_triggers_json` | `driver.go` | Full schema introspection |
| G3.3 | Expose `ddb_abi_version`, `ddb_version` | `driver.go` | Version introspection |
| G3.4 | Expose `ddb_db_in_transaction` | `driver.go` | Engine-truth transaction state |
| G3.5 | Support DSN parameters: `cache_size`, `busy_timeout_ms` | `driver.go:68` | Connection configuration |
| G3.6 | Expose LSN from `ddb_db_commit_transaction` | `driver.go:496` | WAL position tracking |

#### Phase 4: Testing

| # | Task | Files | Impact |
|---|------|-------|--------|
| G4.1 | Add `time.Time` bind/scan round-trip test | `driver_test.go` | Verify microsecond precision |
| G4.2 | Add concurrent reader thread test | `driver_test.go` | Validate multi-reader model |
| G4.3 | Add DSN parsing edge case tests | `driver_test.go` | Catch mode bug regressions |
| G4.4 | Add batch operation tests | `driver_test.go` | Verify bulk correctness |
| G4.5 | Add error code mapping tests for each `DDB_ERR_*` | `driver_test.go` | Verify error propagation |

---

## 7. Java Binding Review

**Location:** `bindings/java/`
**Architecture:** JDBC driver → JNI bridge (`decentdb_jni.c`) → DecentDB C ABI
**Coverage:** 25/50 functions (50%)

### 7.1 Critical Issues

#### 7.1.1 BigDecimal Binding Loses Scale — Data Corruption

`DecentDBPreparedStatement.java:154-159` binds `BigDecimal` via `bindInt64`, discarding the scale. `setBigDecimal(1, new BigDecimal("123.45"))` stores `12345` as an integer with no scale. When read back, it will be `12345` not `123.45`. The C ABI has `ddb_stmt_bind_decimal(stmt, index, scaled, scale)` but it's not exposed in JNI.

**Files:** `bindings/java/native/decentdb_jni.c`, `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBNative.java`, `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBPreparedStatement.java:154`

#### 7.1.2 Timestamp Microsecond Conversion Bug — Data Corruption

`DecentDBPreparedStatement.java:168` computes:
```java
long micros = ts.getTime() * 1000L + ts.getNanos() / 1000L % 1000L;
```
`java.sql.Timestamp.getNanos()` returns nano-of-second (0-999,999,999). The `% 1000` truncates to microseconds-within-millisecond, losing 999 out of every 1000 microseconds. Correct formula: `ts.getTime() * 1000L + ts.getNanos() / 1000L`.

**File:** `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBPreparedStatement.java:168`

#### 7.1.3 Options String Silently Ignored by JNI Bridge

`classify_open_mode()` in `decentdb_jni.c:55-60` only checks for `mode=create/open`. The `cache_pages` and `busy_timeout_ms` key-value pairs passed by the JDBC driver are discarded. `cachePages` and `busyTimeoutMs` JDBC properties have **no effect**.

**File:** `bindings/java/native/decentdb_jni.c:55`

#### 7.1.4 All Batch Operations Missing

`executeBatch()` throws `notSupported`. The C ABI's `ddb_stmt_execute_batch_i64`, `ddb_stmt_execute_batch_i64_text_f64`, and `ddb_stmt_execute_batch_typed` are not exposed through JNI. This is the single largest performance gap for Java.

**Files:** `bindings/java/native/decentdb_jni.c`, `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBStatement.java:246`

#### 7.1.5 Global Error State is Thread-Unsafe

`g_last_code` in `decentdb_jni.c:27` is a global static with no synchronization. Under concurrent connections, error codes can leak between threads. Should use `__thread` / `_Thread_local` or per-handle error state.

**File:** `bindings/java/native/decentdb_jni.c:27`

#### 7.1.6 No Fused Bind+Step or Re-Execute

Every point read requires 3+ JNI crossings per column: `kind()` → `isNull()` → actual getter, each calling `ddb_stmt_row_view` separately. The row view pointer is not cached between column accesses.

**File:** `bindings/java/native/decentdb_jni.c`, `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBResultSet.java`

### 7.2 Moderate Issues

#### 7.2.1 Row View Not Cached Per Row

`row_view_at()` calls `ddb_stmt_row_view` per column access. In `getString()`, `isNull()` calls it once, `kind()` calls it again, then the actual accessor calls it a third time. 3 JNI crossings per column where 1 would suffice if the row view pointer were cached.

**File:** `bindings/java/native/decentdb_jni.c:322`

#### 7.2.2 `getURL()` Returns Null

`DecentDBDatabaseMetaData.java:77` returns null. Should return the connection URL for diagnostic purposes.

**File:** `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBDatabaseMetaData.java:77`

#### 7.2.3 No `DataSource` Implementation

No `javax.sql.DataSource`, no `ConnectionPoolDataSource`. Frameworks like Spring Boot, HikariCP expect a `DataSource`. While the single-process model doesn't need connection pooling, a `DataSource` wrapper improves usability.

**File:** `bindings/java/driver/`

#### 7.2.4 `ResultSetMetaData.getScale()` Hardcoded

Returns 6 for all DECIMAL columns. Should use actual column scale from the result.

**File:** `bindings/java/driver/src/main/java/com/decentdb/jdbc/DecentDBResultSetMetaData.java:108`

### 7.3 Phased Tasks

#### Phase 1: Data Correctness (Must Fix)

| # | Task | Files | Impact |
|---|------|-------|--------|
| J1.1 | Add `bindDecimal` JNI method using `ddb_stmt_bind_decimal` | `decentdb_jni.c`, `DecentDBNative.java`, `DecentDBPreparedStatement.java` | Fix BigDecimal scale loss |
| J1.2 | Fix timestamp microsecond formula | `DecentDBPreparedStatement.java:168` | Fix 1000x precision loss |
| J1.3 | Fix options string passthrough in JNI `classify_open_mode` | `decentdb_jni.c:55` | `cachePages` and `busyTimeoutMs` actually work |
| J1.4 | Add `bindBool` JNI method using `ddb_stmt_bind_bool` | `decentdb_jni.c`, `DecentDBNative.java` | Boolean type fidelity |

#### Phase 2: Performance (Critical Path)

| # | Task | Files | Impact |
|---|------|-------|--------|
| J2.1 | Bind `ddb_stmt_execute_batch_i64`, `ddb_stmt_execute_batch_i64_text_f64`, `ddb_stmt_execute_batch_typed` | `decentdb_jni.c`, `DecentDBStatement.java` | Enable `executeBatch()` |
| J2.2 | Cache row view pointer per row | `decentdb_jni.c`, `DecentDBResultSet.java` | Reduce JNI crossings from 3/col to 1/row |
| J2.3 | Bind `ddb_stmt_step_row_view` for fused step+view | `decentdb_jni.c` | One JNI crossing per row |
| J2.4 | Bind `ddb_stmt_fetch_row_views` for bulk fetch | `decentdb_jni.c`, new batch reader | Dramatically improve scan throughput |
| J2.5 | Bind `ddb_stmt_rebind_int64_execute` and related | `decentdb_jni.c` | Fast UPDATE/DELETE by primary key |
| J2.6 | Fix global error state to thread-local | `decentdb_jni.c:27` | Thread-safe error reporting |
| J2.7 | Use `GetPrimitiveArrayCritical` for blob binding | `decentdb_jni.c:colBlob` | Potential GC-free blob transfer |

#### Phase 3: Feature Completeness

| # | Task | Files | Impact |
|---|------|-------|--------|
| J3.1 | Expose `ddb_db_get_table_ddl`, `ddb_db_list_triggers_json` | `decentdb_jni.c`, `DecentDBDatabaseMetaData.java` | Full schema introspection |
| J3.2 | Expose `ddb_abi_version`, `ddb_version` | `decentdb_jni.c`, `DecentDBNative.java` | Version introspection |
| J3.3 | Expose `ddb_db_checkpoint`, `ddb_db_save_as` | `decentdb_jni.c`, `DecentDBConnection.java` | Maintenance from JDBC |
| J3.4 | Expose `ddb_db_execute` + `ddb_result_*` family | `decentdb_jni.c`, new result class | One-shot query API |
| J3.5 | Implement `javax.sql.DataSource` | new class | Framework compatibility |
| J3.6 | Fix `getURL()` in `DatabaseMetaData` | `DecentDBDatabaseMetaData.java:77` | Diagnostic support |
| J3.7 | Fix `ResultSetMetaData.getScale()` to use actual column scale | `DecentDBResultSetMetaData.java:108` | Correct metadata |

#### Phase 4: Testing

| # | Task | Files | Impact |
|---|------|-------|--------|
| J4.1 | Add DECIMAL read/write round-trip test | test files | Catch scale bugs |
| J4.2 | Add TIMESTAMP round-trip test with microsecond precision | test files | Catch precision bugs |
| J4.3 | Add batch operation tests | test files | Verify bulk correctness |
| J4.4 | Add concurrent connection test | test files | Validate thread safety |
| J4.5 | Add large BLOB handling test | test files | Verify memory correctness |

---

## 8. Node.js Binding Review

**Location:** `bindings/node/`
**Architecture:** N-API native addon (`decentdb/`) with runtime dlopen → Knex dialect client (`knex-decentdb/`)
**Coverage:** 30/50 functions (60%)

### 8.1 Critical Issues

#### 8.1.1 Async Iterator Dispatches One Worker Per Row

`stmtNextAsync` in `addon.c:1259` queues one `napi_async_work` per row to the libuv thread pool. For streaming 1M rows, this means 1M thread pool dispatches. The thread pool default is 4 threads. This is catastrophically slow for large result sets. Should batch: run N steps in the worker, return an array.

**File:** `bindings/node/decentdb/addon.c:1259`

#### 8.1.2 `assert()` Used for N-API Calls — Silent Failures in Release

Many N-API calls use `assert(st == napi_ok)` (e.g., `addon.c:161`). In release builds with `-DNDEBUG`, these become no-ops. Any N-API failure would silently continue with undefined behavior.

**File:** `bindings/node/decentdb/addon.c`

#### 8.1.3 Global Mutable State With No Synchronization

`g_sym`, `g_api`, `g_loaded`, `g_last_status`, `g_last_error` in `native_lib.c` are all global statics with no synchronization. If `decentdb_native_get()` is called from multiple threads simultaneously, there's a data race.

**File:** `bindings/node/decentdb/native_lib.c`

#### 8.1.4 `ddb_stmt_bind_timestamp_micros` Missing

Cannot bind TIMESTAMP_MICROS values from JavaScript. The binding supports reading timestamps but not writing them.

**File:** `bindings/node/decentdb/native_lib.c`, `bindings/node/decentdb/addon.c`

#### 8.1.5 Knex Batch Insert is Benchmark-Specific Hack

`client.js:267-323` detects `INSERT INTO ... VALUES ($1, $2, $3)` patterns and routes to `executeBatchI64TextF64`. This only works for `(int64, text, float64)` column types and the detection is hardcoded to a specific query shape. `isThreeColumnBenchSelect` at line 325 literally checks for `SELECT ID, VAL, F FROM BENCH`.

**File:** `bindings/node/knex-decentdb/client.js`

#### 8.1.6 No Fused Bind+Step or Re-Execute Exposed

`ddb_stmt_bind_int64_step_row_view` and `ddb_stmt_bind_int64_step_i64_text_f64` are not exposed. The benchmark's `stepWithParams` requires two N-API crossings (step then rowArray) where the fused API would need one. No re-execute functions are exposed.

**File:** `bindings/node/decentdb/addon.c`

### 8.2 Moderate Issues

#### 8.2.1 `positionBindings.js` Doesn't Handle Block Comments

The `?` → `$N` rewriter handles `--` line comments but not `/* */` block comments. `?` inside block comments would be incorrectly converted to parameter placeholders.

**File:** `bindings/node/knex-decentdb/positionBindings.js`

#### 8.2.2 Column Names Fetched and Freed Per Access

`wrap_column_name` calls `ddb_stmt_column_name_copy` which allocates, then `free_native_owned_string` frees it. Column names should be fetched once and cached.

**File:** `bindings/node/decentdb/addon.c:1017`

#### 8.2.3 No FinalizationRegistry Safety Net

If JS users drop references without calling `close()`/`finalize()`, cleanup depends entirely on GC timing. The design doc recommends `FinalizationRegistry` but it's not implemented.

**File:** `bindings/node/decentdb/index.js`

#### 8.2.4 Double-Close Race Condition

`js_db_close` sets `w->db = NULL` after closing, but there's no mutex or atomic operation. If called from two threads (finalizer + explicit close), it's a race.

**File:** `bindings/node/decentdb/addon.c:219`

### 8.3 Phased Tasks

#### Phase 1: Performance and Safety

| # | Task | Files | Impact |
|---|------|-------|--------|
| N1.1 | Batch the async iterator — run N steps in worker, return array | `addon.c:1259`, `index.js:343` | Eliminates 1M thread pool dispatches for large scans |
| N1.2 | Replace `assert(napi_ok)` with proper error checking | `addon.c` (many locations) | Prevents silent undefined behavior in release |
| N1.3 | Add synchronization to `native_lib.c` globals | `native_lib.c` | Thread-safe library loading |
| N1.4 | Expose `ddb_stmt_bind_timestamp_micros` | `addon.c`, `native_lib.c` | Timestamp binding support |
| N1.5 | Expose `ddb_stmt_bind_int64_step_row_view` | `addon.c` | Reduces point-read N-API crossings from ~5 to 1 |
| N1.6 | Expose `ddb_stmt_step_row_view` | `addon.c` | Fused step+row-view |
| N1.7 | Expose re-execute functions | `addon.c` | Fast UPDATE/DELETE by primary key |
| N1.8 | Expose `ddb_stmt_execute_batch_i64` and `ddb_stmt_execute_batch_typed` | `addon.c` | General-purpose batch insert |

#### Phase 2: Correctness and Safety

| # | Task | Files | Impact |
|---|------|-------|--------|
| N2.1 | Add atomic compare-and-swap for double-close prevention | `addon.c:219,280` | Prevents use-after-free races |
| N2.2 | Add FinalizationRegistry for Database and Statement | `index.js` | GC safety net |
| N2.3 | Handle `/* */` block comments in positionBindings | `positionBindings.js` | Correct parameter rewriting |
| N2.4 | Expose `ddb_abi_version`, `ddb_version` | `addon.c`, `native_lib.c`, `index.js` | ABI verification at load time |

#### Phase 3: Feature Completeness

| # | Task | Files | Impact |
|---|------|-------|--------|
| N3.1 | Expose `ddb_db_execute` + `ddb_result_*` family | `addon.c`, `index.js` | One-shot query API |
| N3.2 | Expose `ddb_db_get_table_ddl`, `ddb_db_list_views_json`, `ddb_db_get_view_ddl`, `ddb_db_list_triggers_json` | `addon.c`, `index.js` | Full schema introspection |
| N3.3 | Expose `ddb_db_in_transaction` | `addon.c`, `index.js` | Engine-truth transaction state |
| N3.4 | Expose `ddb_evict_shared_wal` | `addon.c` | WAL management |
| N3.5 | Replace benchmark-specific hacks with general INSERT pattern detection | `client.js` | Works for any column type |
| N3.6 | Add Knex schema builder customization | `client.js` | Correct DecentDB column types |
| N3.7 | Use `node-gyp-build` or `prebuildify` for prebuilt distribution | `package.json`, `binding.gyp` | Easier installation |

#### Phase 4: Testing

| # | Task | Files | Impact |
|---|------|-------|--------|
| N4.1 | Add async iteration error handling tests | test files | Verify error propagation in streams |
| N4.2 | Add blob round-trip test | test files | Verify BLOB correctness |
| N4.3 | Add timestamp_micros round-trip test | test files | Verify timestamp binding+reading |
| N4.4 | Add batch operation tests | test files | Verify bulk correctness |
| N4.5 | Add double-close/idempotent close tests | test files | Verify safety |
| N4.6 | Add memory leak tests | test files | Verify cleanup |
| N4.7 | Add Knex schema builder tests | `knex-decentdb/` test files | Verify DDL generation |

---

## 9. Dart Binding Review

**Location:** `bindings/dart/`
**Architecture:** Dart FFI (`native_bindings.dart`) → high-level `Database`/`Statement`/`Schema` API
**Coverage:** 27/50 functions (54%) — but notably 0/35 statement-level functions

Dart has the most complete schema introspection coverage (all 7 schema functions + all 6 result set functions) but the weakest execution model: it bypasses native prepared statements entirely, using `ddb_db_execute` for every query.

### 9.1 Critical Issues

#### 9.1.1 Native Prepared Statements Completely Absent

The Dart binding does not bind `ddb_db_prepare`, `ddb_stmt_free`, `ddb_stmt_reset`, `ddb_stmt_clear_bindings`, `ddb_stmt_bind_*`, `ddb_stmt_step`, or any statement-level function. Every `execute()` or `query()` sends the full SQL string across the FFI boundary via `ddb_db_execute`. For the benchmark's insert loop, this means 1M SQL string copies to native memory. The native prepared statement API would avoid this entirely.

**File:** `bindings/dart/dart/lib/src/statement.dart`

#### 9.1.2 No Batch Insert API

The C ABI offers `ddb_stmt_execute_batch_i64`, `ddb_stmt_execute_batch_i64_text_f64`, and `ddb_stmt_execute_batch_typed` for bulk operations. The Dart binding does not bind any of them. The benchmark loops single-row `execute()` calls. For a 1M row insert, this is 1M individual FFI round-trips where 1 batch call would suffice.

**File:** `bindings/dart/dart/lib/src/statement.dart`

#### 9.1.3 No Zero-Copy Result Views

`ddb_stmt_row_view`, `ddb_stmt_fetch_row_views`, and `ddb_stmt_fetch_rows_i64_text_f64` return borrowed value views that avoid copying cell data. The Dart binding uses `ddb_result_value_copy` which allocates and copies every cell individually via `calloc<DdbValue>()` per cell.

**File:** `bindings/dart/dart/lib/src/statement.dart:282`

#### 9.1.4 Per-Cell Allocation Overhead

For each cell in a result, `calloc<DdbValue>()` is allocated, used, then freed. For a 1000-row x 10-column result, that's 10,000 `calloc` + 10,000 `free` calls. A single bulk allocation or use of stack-allocated arrays would dramatically reduce allocation pressure.

**File:** `bindings/dart/dart/lib/src/statement.dart:282-300`

#### 9.1.5 Linear Column Lookup in Row

`Row.operator[]` does `columns.indexOf(name)` at line 19 — O(n) per named column access. For tables with many columns, this adds up. A `Map<String, int>` would be O(1).

**File:** `bindings/dart/dart/lib/src/statement.dart:19`

### 9.2 Moderate Issues

#### 9.2.1 No `NativeFinalizer` on Database

If `Database.close()` is never called (user error, exception path), the `ddb_db_t*` handle leaks until process exit. Dart's `NativeFinalizer` could call `ddb_db_free` as a safety net.

**File:** `bindings/dart/dart/lib/src/database.dart:11`

#### 9.2.2 `sqlite3` is a Runtime Dependency

`pubspec.yaml:13` lists `sqlite3: ^2.9.3` in `dependencies`, not `dev_dependencies`. Anyone who `depends: decentdb` pulls in the sqlite3 native library unnecessarily. It's only used in benchmarks.

**File:** `bindings/dart/dart/pubspec.yaml:13`

#### 9.2.3 `ErrorCode.fromCode` Silently Swallows Unknown Codes

`types.dart:20` maps unknown error codes to `ErrorCode.internal`. If the C ABI adds new error codes, the Dart binding would silently swallow them instead of surfacing them.

**File:** `bindings/dart/dart/lib/src/types.dart:20`

#### 9.2.4 Flutter Example is Not a Flutter App

`flutter_desktop/main.dart` is a console program that imports `dart:io` but not `package:flutter`. The `pubspec.yaml` has no Flutter SDK dependency. The name is misleading.

**File:** `bindings/dart/examples/flutter_desktop/`

#### 9.2.5 No Isolate Awareness

The binding makes no attempt to detect or prevent cross-isolate usage. The C ABI's one-writer model is not enforced at the Dart layer. Two isolates calling `execute()` concurrently on the same `Database` would violate the engine contract.

**File:** `bindings/dart/dart/lib/src/database.dart`

### 9.3 Phased Tasks

#### Phase 1: Performance Foundation (Critical Path)

| # | Task | Files | Impact |
|---|------|-------|--------|
| DT1.1 | Bind `ddb_db_prepare`, `ddb_stmt_free`, `ddb_stmt_reset`, `ddb_stmt_clear_bindings`, `ddb_stmt_bind_*`, `ddb_stmt_step`, `ddb_stmt_column_count`, `ddb_stmt_column_name_copy`, `ddb_stmt_affected_rows` | `native_bindings.dart`, new internal `NativeStatement` | Enable native prepared statements |
| DT1.2 | Bind `ddb_stmt_execute_batch_i64_text_f64` and `ddb_stmt_execute_batch_typed` | `native_bindings.dart`, `statement.dart` | Bulk insert throughput |
| DT1.3 | Bind `ddb_stmt_row_view`, `ddb_stmt_step_row_view`, `ddb_stmt_fetch_row_views` | `native_bindings.dart`, new zero-copy reader | Eliminate per-cell allocation |
| DT1.4 | Bind `ddb_stmt_bind_int64_step_row_view` and `ddb_stmt_bind_int64_step_i64_text_f64` | `native_bindings.dart` | Fused bind+step for point reads |
| DT1.5 | Bind re-execute functions | `native_bindings.dart` | Fast UPDATE/DELETE by primary key |
| DT1.6 | Replace linear column lookup with `Map<String, int>` in Row | `statement.dart:19` | O(1) named column access |
| DT1.7 | Bulk-allocate DdbValue array per row instead of per-cell | `statement.dart:282` | Reduces allocation from 10K to 1K per 1K-row result |

#### Phase 2: Correctness and Safety

| # | Task | Files | Impact |
|---|------|-------|--------|
| DT2.1 | Add `NativeFinalizer` to `Database` calling `ddb_db_free` | `database.dart` | Prevents native handle leaks |
| DT2.2 | Move `sqlite3` from `dependencies` to `dev_dependencies` | `pubspec.yaml` | Clean dependency tree |
| DT2.3 | Throw on unknown `ErrorCode` instead of silently mapping to `internal` | `types.dart:20` | Surface new error codes |
| DT2.4 | Bind `ddb_value_init` and use in `_EncodedParams._writeValue` | `native_bindings.dart`, `statement.dart` | Zero reserved fields safely |
| DT2.5 | Add `UuidValue` wrapper type with standard UUID string formatting | `types.dart` | Better UUID ergonomics |

#### Phase 3: Feature Completeness

| # | Task | Files | Impact |
|---|------|-------|--------|
| DT3.1 | Expose `ddb_db_create` and `ddb_db_open` as distinct modes | `database.dart` | Create-only and open-only semantics |
| DT3.2 | Expose `ddb_db_in_transaction` as `Database.inTransaction` | `database.dart` | Engine-truth transaction state |
| DT3.3 | Expose `ddb_evict_shared_wal` | `database.dart` | WAL management |
| DT3.4 | Bind `ddb_stmt_bind_bool` explicitly (currently handled via int64) | `native_bindings.dart` | Boolean type fidelity |

#### Phase 4: Flutter and Isolate Support

| # | Task | Files | Impact |
|---|------|-------|--------|
| DT4.1 | Fix or rename `flutter_desktop` example to be a real Flutter app | `examples/flutter_desktop/` | Accurate example |
| DT4.2 | Add isolate-aware wrapper or document single-isolate requirement | `database.dart` dartdoc | Thread safety contract |
| DT4.3 | Add `WidgetsBindingObserver` integration for app lifecycle | new Flutter helper | Proper pause/resume handling |

#### Phase 5: Testing

| # | Task | Files | Impact |
|---|------|-------|--------|
| DT5.1 | Add BLOB round-trip test | `decentdb_test.dart` | Verify BLOB correctness |
| DT5.2 | Add DECIMAL round-trip test | `decentdb_test.dart` | Verify DECIMAL correctness |
| DT5.3 | Add UUID read test | `decentdb_test.dart` | Verify UUID handling |
| DT5.4 | Add TIMESTAMP_MICROS round-trip test | `decentdb_test.dart` | Verify timestamp precision |
| DT5.5 | Add concurrent reader test | `decentdb_test.dart` | Validate multi-reader model |
| DT5.6 | Add error code propagation tests | `decentdb_test.dart` | Verify typed errors |
| DT5.7 | Add batch operation tests | `decentdb_test.dart` | Verify bulk correctness |
| DT5.8 | Add `nextPage` with exact page-size boundary test | `decentdb_test.dart` | Verify `isLast` edge case |

---

## 10. Benchmark and Example Quality

### 10.1 Benchmark Coverage

| Binding | Insert Throughput | Fetch All | Fetch Many | Point Reads (p50/p95) | vs SQLite | GC Disabled | Warmup |
|---------|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| .NET    | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python  | ✅ | ✅ | ✅ | ✅ | ✅ | N/A | ✅ |
| Go      | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Java    | ✅ | ✅ | ✅ | ✅ | ✅ | N/A | ✅ |
| Node.js | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Dart    | ✅ | ✅ | ✅ | ✅ | ✅ | N/A | ✅ |

All bindings have benchmarks comparing DecentDB to their native SQLite counterpart. Quality varies:

### 10.2 Benchmark Improvement Tasks

| # | Task | Binding(s) | Impact |
|---|------|-----------|--------|
| B1 | Add batch insert benchmark (when batch API is available) | All | Measures batch fast-path throughput |
| B2 | Add re-execute benchmark (when re-execute API is available) | All | Measures prepared statement reuse throughput |
| B3 | Add concurrent reader benchmark (multi-threaded scan) | All | Validates multi-reader scaling |
| B4 | Add DECIMAL-heavy benchmark | .NET, Java | Validates decimal performance |
| B5 | Add UUID column benchmark | All | Validates UUID type performance |
| B6 | Add large BLOB benchmark (1MB+ per cell) | All | Validates memory efficiency |
| B7 | Add connection open/close churn benchmark | All | Measures lifecycle overhead |
| B8 | Standardize benchmark output format across bindings | All | Enables cross-language comparison |
| B9 | Add memory usage tracking to all benchmarks | All | Measures allocation pressure |

### 10.3 Example Quality

| Binding | Example Exists | Covers Transactions | Covers Schema | Covers Data Types | Covers Error Handling |
|---------|:-:|:-:|:-:|:-:|:-:|
| .NET    | ✅ (benchmark) | ✅ | Partial | Partial | Partial |
| Python  | ✅ (README) | ✅ | ✅ | Partial | Partial |
| Go      | ✅ (benchmark) | ✅ | ✅ | Partial | Partial |
| Java    | ✅ (benchmark) | ✅ | Partial | Partial | Partial |
| Node.js | ✅ (README) | ✅ | ✅ | Partial | Partial |
| Dart    | ✅ (console, complex) | ✅ | ✅ | ✅ | Partial |

Dart has the best example quality with `console_complex/main.dart` (1122 lines) exercising 6 tables, FKs, indexes, joins, CTEs, aggregations, text search, transactions, views, and introspection.

### 10.4 Example Improvement Tasks

| # | Task | Binding(s) | Impact |
|---|------|-----------|--------|
| E1 | Add a standalone example (not benchmark) showing full CRUD + schema | .NET, Go, Java | Better onboarding |
| E2 | Add DECIMAL and UUID type examples | All | Showcases native type support |
| E3 | Add error handling example | All | Shows exception patterns |
| E4 | Add connection string / DSN configuration example | .NET, Go, Node.js, Java | Shows configuration options |
| E5 | Add transaction example with rollback | All | Shows transaction patterns |

---

## 11. Prioritized Global Task Matrix

### Tier 1: Performance (Blocks V2 Performance Goals)

These tasks enable the fast-path operations that DecentDB's engine is optimized for. Without them, bindings cannot achieve the throughput the engine is capable of.

| Task | .NET | Python | Go | Java | Node.js | Dart |
|------|:----:|:------:|:--:|:----:|:-------:|:----:|
| Bind batch execution (`ddb_stmt_execute_batch_*`) | D1.1 | ✅ (partial) | G1.5 | J2.1 | N1.8 | DT1.2 |
| Bind fused bind+step | D1.2 | ✅ (C ext) | G1.2 | J2.2 | N1.5 | DT1.4 |
| Bind fused step+row_view | D1.3 | ✅ (C ext) | G1.1 | J2.3 | N1.6 | DT1.3 |
| Bind re-execute patterns | D1.4 | ✅ (C ext) | G1.4 | J2.5 | N1.7 | DT1.5 |
| Bind batch fetch | D1.3 | ✅ (C ext) | G1.3 | J2.4 | — | DT1.3 |

### Tier 2: Correctness (Blocks V2 Quality Goals)

These tasks fix data corruption bugs, memory leaks, and correctness issues that would erode user trust.

| Task | .NET | Python | Go | Java | Node.js | Dart |
|------|:----:|:------:|:--:|:----:|:-------:|:----:|
| Fix data corruption bugs | D2.2 | — | — | J1.1, J1.2 | — | — |
| Add `runtime.SetFinalizer` / `NativeFinalizer` | — | — | G2.3 | — | N2.1 | DT2.1 |
| Fix DSN/connection config bugs | — | P1.5 | G2.1 | J1.3 | — | — |
| Return proper error types | — | — | G2.2 | — | N1.2 | DT2.3 |
| Thread safety fixes | — | — | — | J2.6 | N1.3 | — |

### Tier 3: Feature Completeness (Blocks V2 API Parity)

These tasks close feature gaps between bindings and the C ABI.

| Task | .NET | Python | Go | Java | Node.js | Dart |
|------|:----:|:------:|:--:|:----:|:-------:|:----:|
| Schema introspection (views, triggers, DDL) | D3.4 | P2.1, P2.2 | G3.1 | J3.1 | N3.2 | ✅ |
| Version/ABI introspection | D3.2 | P2.5 | G3.3 | J3.2 | N2.4 | ✅ |
| Transaction state query | D3.3 | P2.3 | G3.4 | — | N3.3 | DT3.2 |
| Database open mode variants | D3.1 | P2.4 | — | — | — | DT3.1 |
| Result set API | D3.5 | P2.7 | G3.1 | J3.4 | N3.1 | ✅ |

### Tier 4: Testing and Documentation

| Task | .NET | Python | Go | Java | Node.js | Dart |
|------|:----:|:------:|:--:|:----:|:-------:|:----:|
| Batch operation tests | D4.1 | P4.1 | G4.4 | J4.3 | N4.4 | DT5.7 |
| Concurrent reader tests | D4.3 | P4.3 | G4.2 | J4.4 | — | DT5.5 |
| DECIMAL round-trip tests | D4.4 | P4.2 | — | J4.1 | — | DT5.2 |
| Timestamp precision tests | D4.5 | — | G4.1 | J4.2 | N4.3 | DT5.4 |
| Thread safety documentation | D4.6 | — | — | — | — | DT4.2 |

---

## Appendix A: C ABI Function Reference

Complete list of 50 C ABI functions with their binding coverage status. ✅ = exposed, ⚠️ = declared but not used or partially used, ❌ = not exposed.

| # | Function | .NET | Python | Go | Java | Node | Dart |
|---|----------|:----:|:------:|:--:|:----:|:----:|:----:|
| 1 | `ddb_abi_version` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 2 | `ddb_version` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 3 | `ddb_last_error_message` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 4 | `ddb_value_init` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| 5 | `ddb_value_dispose` | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| 6 | `ddb_string_free` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 7 | `ddb_db_create` | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ |
| 8 | `ddb_db_open` | ❌ | ❌ | ✅ | ✅ | ✅ | ❌ |
| 9 | `ddb_db_open_or_create` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 10 | `ddb_db_free` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 11 | `ddb_db_prepare` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 12 | `ddb_stmt_free` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 13 | `ddb_stmt_reset` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 14 | `ddb_stmt_clear_bindings` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 15 | `ddb_stmt_bind_null` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 16 | `ddb_stmt_bind_int64` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 17 | `ddb_stmt_bind_int64_step_row_view` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 18 | `ddb_stmt_bind_int64_step_i64_text_f64` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 19 | `ddb_stmt_bind_float64` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 20 | `ddb_stmt_bind_bool` | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| 21 | `ddb_stmt_bind_text` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 22 | `ddb_stmt_bind_blob` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 23 | `ddb_stmt_bind_decimal` | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| 24 | `ddb_stmt_bind_timestamp_micros` | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| 25 | `ddb_stmt_execute_batch_i64` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 26 | `ddb_stmt_execute_batch_i64_text_f64` | ❌ | ⚠️ | ❌ | ❌ | ✅ | ❌ |
| 27 | `ddb_stmt_execute_batch_typed` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 28 | `ddb_stmt_step` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 29 | `ddb_stmt_column_count` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 30 | `ddb_stmt_column_name_copy` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 31 | `ddb_stmt_affected_rows` | ✅ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 32 | `ddb_stmt_rebind_int64_execute` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 33 | `ddb_stmt_rebind_text_int64_execute` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 34 | `ddb_stmt_rebind_int64_text_execute` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 35 | `ddb_stmt_value_copy` | ✅ | ✅ | ❌ | ❌ | ❌ | ✅ |
| 36 | `ddb_stmt_row_view` | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ |
| 37 | `ddb_stmt_step_row_view` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| 38 | `ddb_stmt_fetch_row_views` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| 39 | `ddb_stmt_fetch_rows_i64_text_f64` | ❌ | ⚠️ | ❌ | ❌ | ✅ | ❌ |
| 40 | `ddb_db_execute` | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ |
| 41 | `ddb_db_checkpoint` | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| 42 | `ddb_db_begin_transaction` | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| 43 | `ddb_db_commit_transaction` | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| 44 | `ddb_db_rollback_transaction` | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| 45 | `ddb_db_in_transaction` | ❌ | ⚠️ | ❌ | ❌ | ❌ | ❌ |
| 46 | `ddb_db_save_as` | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ |
| 47 | `ddb_db_list_tables_json` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 48 | `ddb_db_describe_table_json` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 49 | `ddb_db_get_table_ddl` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 50 | `ddb_db_list_indexes_json` | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 51 | `ddb_db_list_views_json` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| 52 | `ddb_db_get_view_ddl` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| 53 | `ddb_db_list_triggers_json` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 54 | `ddb_evict_shared_wal` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| 55 | `ddb_result_free` | ❌ | ✅ | ❌ | ❌ | ❌ | ✅ |
| 56 | `ddb_result_row_count` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 57 | `ddb_result_column_count` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 58 | `ddb_result_affected_rows` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 59 | `ddb_result_column_name_copy` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |
| 60 | `ddb_result_value_copy` | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |

**Legend:** ✅ = exposed to users, ⚠️ = used internally (C extension or declared but uncalled), ❌ = not exposed

---

*Document generated from deep analysis of all binding source code, the C ABI header, existing design documents, and benchmark code. Each task references specific files and line numbers for implementation.*
