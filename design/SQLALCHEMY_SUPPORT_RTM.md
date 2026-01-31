# SQLALCHEMY_SUPPORT.md — Requirements Traceability Matrix (RTM)

**Scope audited:** current repo state on 2026-01-31.

**Verification performed:**
- Built native C API: `nimble -y build_lib` (produces `build/libc_api.so`)
- Ran Python binding + SQLAlchemy tests: `python -m pytest -q` in `bindings/python`

Legend:
- ✅ = implemented and verified by tests / direct evidence
- ⚠️ = partially implemented, or implemented but not proven by tests, or has known limitations
- ❌ = missing / not implemented
- ❓ = unclear or unverifiable with current hooks/tests

---

## Phase 1 — Native C API (Nim)

| ID | Requirement (from SQLALCHEMY_SUPPORT.md) | Status | Evidence (code/tests) |
|---|---|---|---|
| P1-01 | Export C-compatible open/close and error reporting functions | ✅ | `decentdb_open`, `decentdb_close`, `decentdb_last_error_code/message` in `src/c_api.nim` | 
| P1-02 | Prepared/streaming statements: prepare/step/finalize | ✅ | `decentdb_prepare`, `decentdb_step`, `decentdb_finalize` in `src/c_api.nim` |
| P1-03 | Bind parameters as 1-based indexes matching `$1..$N` | ✅ | `decentdb_bind_*` uses 1-based indexing via `bindIndex0()` in `src/c_api.nim` |
| P1-04 | Column metadata + accessors (count/name/type + typed getters) | ✅ | `decentdb_column_count/name/type/...` in `src/c_api.nim` |
| P1-05 | Provide forward-only streaming cursor semantics (no forced materialization) | ✅ | `decentdb_step` iterates `RowCursor` and yields 1 row at a time; used by Python cursor iteration |
| P1-06 | Provide a Python-perf extension: row batch API OR row view API | ✅ | Row view API present: `decentdb_row_view` in `src/c_api.nim` |
| P1-07 | Ownership/lifetime rules for borrowed pointers are documented and enforceable | ⚠️ | Borrowed pointers exist (e.g., `cstring(h.columnNames[col])`, `unsafeAddr val.bytes[0]`), but no explicit runtime contract + no tests validating lifetime constraints |
| P1-08 | Avoid cross-thread use of a single `decentdb_stmt*` | ⚠️ | No enforcement; policy only |

---

## Phase 2 — Python DB-API 2.0 Driver (`bindings/python/decentdb`)

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| P2-01 | DB-API globals: `apilevel`, `threadsafety`, `paramstyle` | ✅ | Declared in `bindings/python/decentdb/__init__.py` |
| P2-02 | Implement `connect(...) -> Connection` | ✅ | `connect()` returns `Connection` in `bindings/python/decentdb/__init__.py`; tested |
| P2-03 | Implement `Connection.cursor() -> Cursor` | ✅ | `Connection.cursor()` in `bindings/python/decentdb/__init__.py`; tested |
| P2-04 | Implement `Cursor.execute(sql, params)` | ✅ | `Cursor.execute()` in `bindings/python/decentdb/__init__.py`; tested |
| P2-05 | Implement `Cursor.executemany(sql, seq_of_params)` | ⚠️ | Implemented as loop calling `execute()`; no batching, no stmt reuse |
| P2-06 | Implement `fetchone/fetchmany/fetchall` + iterator protocol | ✅ | `fetchone/fetchmany/fetchall`, `__iter__/__next__`; tested |
| P2-07 | Transactions: `commit()` / `rollback()` | ⚠️ | Implemented by executing SQL `COMMIT`/`ROLLBACK`; behavior depends on engine semantics; tested only indirectly |
| P2-08 | Parameter rewriting MUST produce `$1..$N` for engine | ✅ | `_convert_params()` rewrites `?` and `:name` forms before `decentdb_prepare`; tested |
| P2-09 | Stable parameter ordering within statement | ✅ | Named parameters ordered by first occurrence; tested via repeated named param usage |
| P2-10 | Support dict params and sequence params; reject mixed styles | ✅ | `_convert_params()` rejects mixed styles; tested |
| P2-11 | Driver tolerates frequent open/close (pooling-friendly) | ✅ | `connect()` + `close()` exercised by tests |
| P2-12 | Enforce/communicate single-writer model | ❌ | No explicit enforcement or warnings in driver |
| P2-13 | Avoid suggesting cross-process semantics beyond DecentDB | ⚠️ | Driver itself doesn’t claim multiprocess safety, but no explicit guardrails |

---

## Phase 3 — SQLAlchemy Dialect (`bindings/python/decentdb_sqlalchemy`)

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| P3-01 | `dbapi()` / `import_dbapi()` integrates `decentdb` module | ✅ | `DecentDbDialect.import_dbapi()` returns `decentdb` |
| P3-02 | URL forms support `decentdb+pysql:///path` | ✅ | Used in tests via `create_engine("decentdb+pysql:///...")` |
| P3-03 | connect args mapping for path + query options | ⚠️ | `create_connect_args()` passes `url.database` + query dict; no tests proving options affect engine |
| P3-04 | SQL compilation: LIMIT/OFFSET matches DecentDB | ✅ | `DecentDbCompiler.limit_clause()` + integration test |
| P3-05 | SQL compilation: RETURNING only enabled if supported / otherwise emulated | ⚠️ | Explicitly disabled: `implicit_returning=False` + compiler raises `CompileError` in `bindings/python/decentdb_sqlalchemy/dialect.py`; tested in `bindings/python/tests/test_sqlalchemy.py` |
| P3-06 | Type compiler maps SQLAlchemy types to DecentDB storage types | ⚠️ | Basic mappings implemented; no round-trip tests for Date/Time/UUID/Decimal semantics |
| P3-07 | Introspection best-effort: `get_table_names`, `get_columns`, indexes, FKs | ⚠️ | `get_table_names/has_table/get_columns` implemented via C API JSON catalog helpers; tested in `bindings/python/tests/test_sqlalchemy.py` (indexes/FKs still missing) |
| P3-08 | Dialect capabilities consistent with MVP + isolation constraints (Snapshot Isolation) | ⚠️ | `get_isolation_level()` returns `SNAPSHOT`; no full capability matrix or tests |
| P3-09 | Optional perf hooks: compiled cache integration | ❌ | Not implemented |
| P3-10 | Optional perf hooks: fast row/tuple mode | ❌ | Not implemented |

---

## Phase 4 — ORM/DAL/CRUD

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| P4-01 | ORM CRUD: `Session.add/delete/get`, flush/commit | ✅ | SQLAlchemy ORM test covers add/get/update/delete |
| P4-02 | Relationship support (one-to-many/many-to-one) | ❌ | No tests or implementation validation |
| P4-03 | Eager loading patterns (`joinedload`, `selectinload`) | ❌ | No tests |
| P4-04 | Bulk ops: Core `insert()` + executemany | ⚠️ | Core insert used; DB-API `executemany` is naive; no performance-oriented behavior |

---

## Type Mapping (Phase 5)

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| T-01 | int/Integer -> INT64 | ✅ | Dialect compiles `Integer` to `INT64`; DB-API binds ints; tested |
| T-02 | bool/Boolean -> BOOL | ✅ | Dialect compiles `Boolean` to `BOOL`; DB-API binds bool; tested |
| T-03 | float/Float -> FLOAT64 | ✅ | Dialect compiles `Float` to `FLOAT64`; DB-API binds float; tested |
| T-04 | str/String/Text -> TEXT (UTF-8) | ✅ | Dialect compiles string/text -> TEXT; DB-API binds/decodes UTF-8; tested |
| T-05 | bytes/LargeBinary -> BLOB | ✅ | Dialect compiles `LargeBinary` -> BLOB; DB-API binds/reads blobs; tested |
| T-06 | datetime/date/time stored as INT64 with UTC semantics | ❌ | Dialect compiles to INT64 but DB-API does not adapt datetime/date/time to integer encodings; no tests |
| T-07 | decimal/Numeric stored as TEXT preserving precision | ⚠️ | Dialect compiles `Numeric` to TEXT; DB-API binds unknown types via `str()`; no tests |
| T-08 | uuid.UUID stored as BLOB(16) | ❌ | Dialect compiles UUID -> BLOB but DB-API doesn’t adapt uuid.UUID to 16 bytes; no tests |
| T-09 | Enum stored as INT64 or TEXT (configurable) | ❌ | Not implemented |

---

## Error Handling / Observability / Performance Targets

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| E-01 | Map native errors into DB-API exception hierarchy | ⚠️ | `_raise_error()` maps known error codes; SQL parse errors covered by `bindings/python/tests/test_basic.py::test_error_includes_sql_and_code` |
| E-02 | SQLAlchemy exceptions include SQL + params + native error code | ✅ | `_raise_error(..., sql=..., params=...)` appends JSON Context; tested in `bindings/python/tests/test_basic.py::test_error_includes_sql_and_code` |
| O-01 | Support SQLAlchemy echo logging without excessive overhead | ⚠️ | Likely works via SQLAlchemy; not tested |
| O-02 | Optional driver-level tracing hooks (timings/rows/cache metrics) | ❌ | Not implemented |
| PERF-01 | Prepared statement cache (LRU) | ❌ | Not implemented |
| PERF-02 | Efficient `fetchmany()` with batch-native API | ⚠️ | `fetchmany()` loops `fetchone()`; C API has row_view but driver doesn’t use it |
| PERF-03 | Fast decoding (min allocations) | ⚠️ | Decodes per-cell via ctypes; no batch decode |
| PERF-04 | Tuple rows for Core, avoid dict materialization | ✅ | DB-API returns tuples |
| PERF-05 | Executemany optimized (single txn, stmt reuse, bind batching) | ❌ | Not implemented |
| TEST-01 | Unit tests: param rewriting/binding | ✅ | `bindings/python/tests/test_basic.py` |
| TEST-02 | Unit tests: type round-trips | ⚠️ | Only basic types covered |
| TEST-03 | Integration tests: SQLAlchemy Core | ✅ | `bindings/python/tests/test_sqlalchemy.py` |
| TEST-04 | Integration tests: SQLAlchemy ORM CRUD | ✅ | `bindings/python/tests/test_sqlalchemy.py` |
| TEST-05 | Benchmarks for <1ms overhead budget | ❌ | Not implemented |
| PKG-01 | Cross-platform wheels (Linux/macOS/Windows) | ❌ | Not implemented |

---

## Notes / Next smallest fixes

1. **Performance basics**: per-connection prepared statement cache + `reset()`/`clear_bindings()` reuse in `executemany()`; add microbench + correctness tests.
2. **Batch fetching**: use the row view API to reduce per-cell FFI overhead; add a benchmark and ensure `fetchmany()` uses it.
3. **Type conversions**: add bind/result processors for `Date/DateTime/Time/Numeric/UUID/Enum`; add round-trip tests.
4. **Introspection expansion**: implement indexes + foreign key reflection (best-effort) and add tests.
5. **ORM coverage**: add relationship + eager-loading integration tests.
6. **Packaging & perf gates**: cross-platform wheel automation and benchmarks to prove the <1ms overhead target.
