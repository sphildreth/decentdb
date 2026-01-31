# SQLAlchemy Support Requirements for DecentDB 1.0.0

## Overview

Enable Python 3 applications to perform **extremely fast** ORM/DAL/CRUD operations against DecentDB database files using SQLAlchemy, without requiring a server process.

Target audience:
- Python applications that want embedded, serverless database files
- SQLAlchemy Core + ORM users
- Systems that need predictable low-latency SELECT performance with full CRUD

## Goals

- Python apps can query DecentDB files directly (**embedded mode**)
- SQLAlchemy integration works out-of-the-box via a dialect + DB-API 2.0 driver
- Full SQLAlchemy ORM functionality for common patterns:
  - sessions + unit of work, flush/commit/rollback
  - ORM CRUD, relationships, eager/lazy loading
  - Core queries, compiled SQL, executemany
- **Performance-first SELECT operations**
  - driver + dialect overhead budget: **< 1ms** for typical operations
  - forward-only, streaming reads (no forced materialization)

## Compatibility Constraints (Non-Negotiable)

- **SQL parameters (engine):** DecentDB uses Postgres-style positional parameters (`$1, $2, ...`) per ADR-0005.
  - The Python DB-API layer MAY accept SQLAlchemy-generated parameter formats (named/pyformat) but MUST rewrite to `$N` before calling native.
- **Isolation (engine):** Default isolation is **Snapshot Isolation** per ADR-0023.
  - The SQLAlchemy dialect MUST not claim stronger guarantees.
- **Concurrency model (MVP):** single process, one writer, multiple concurrent readers.
  - The DB-API driver and dialect MUST avoid suggesting cross-process file sharing semantics beyond what DecentDB provides.

## Performance Targets

All SELECT operations must meet these performance criteria:

| Query Type | Target | Max Acceptable |
|------------|--------|----------------|
| Single record by PK | 0.5ms | 2ms |
| Simple list (no pagination) | 1ms + 0.1ms/row | 5ms + 0.5ms/row |
| Filtered list (1-2 conditions) | 2ms + 0.1ms/row | 10ms + 0.5ms/row |
| Paginated query (LIMIT/OFFSET) | 3ms + 0.1ms/row | 15ms + 0.5ms/row |
| Sorted + Paginated | 4ms + 0.1ms/row | 20ms + 0.5ms/row |
| Count with filter | 2ms | 10ms |
| Text search (trigram index) | 5ms + 0.2ms/row | 50ms + 1ms/row |

**Critical:** Query execution overhead (Python layer: SQLAlchemy + dialect + DB-API driver) must add **< 1ms** to native DecentDB query time for common SELECT paths.

## Non-Goals

- No DecentDB server process (embedded only)
- No automatic schema migration framework (Alembic integration is future work)
- No attempt to implement 100% PostgreSQL SQL dialect (only DecentDB’s supported subset)
- No multi-process concurrency claims (no shared-memory locking, no server)

## Optional Mapping Features (Python)

SQLAlchemy already provides explicit mapping constructs; the DecentDB dialect SHOULD be compatible with both:
- SQLAlchemy ORM Declarative mappings
- SQLAlchemy Core Table metadata

Optional features are included for ergonomics and performance tuning (not required for correctness):

| Feature | Use Case | When Needed |
|--------|----------|-------------|
| `__tablename__` conventions helper | Zero-config naming | Greenfield projects that want convention-first |
| Column naming helper (snake_case) | Map Python `created_at` / `createdAt` | Mixed naming styles |
| Optional PK conventions | Default `id` primary key | When users want “just work” defaults |
| Optional index helper | Create indexes for common filters | Performance tuning |
| Optional fast row materializer | Reduce ORM overhead | Hot-path read-heavy workloads |

**Philosophy:** SQLAlchemy remains the primary mapping system. DecentDB-specific helpers are optional and should not force decorators or new base classes.

### Example: SQLAlchemy ORM Usage (Goal State)

```python
from __future__ import annotations

from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

class Base(DeclarativeBase):
    pass

class Artist(Base):
    __tablename__ = "artists"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]

engine = create_engine("decentdb+pysql:////path/to.db", future=True)

with Session(engine) as s:
    artist = s.get(Artist, 1)
    rows = s.scalars(select(Artist).where(Artist.name.like("A%"))).all()
```

---

## Architecture

```
Python Application
  ├── SQLAlchemy ORM / Core
  └── decentdb_sqlalchemy (dialect + performance hooks)
          ↓
      decentdb (DB-API 2.0 driver)
          ↓
      DecentDb.Native (ctypes/cffi or CPython extension)
          ↓
      DecentDB (Nim engine, direct file I/O)
```

---

## Phase 1: Native C API (Nim)

### Requirements

Expose a C-compatible API from the Nim DecentDB engine suitable for Python bindings.

**Performance-first SELECT requirement:** Provide a forward-only, streaming cursor API so the Python driver can implement DB-API cursor iteration without materializing whole result sets.

Baseline API (compatible with existing provider designs):

```c
// Opaque handles
typedef struct decentdb_db decentdb_db;
typedef struct decentdb_stmt decentdb_stmt;

// Database lifecycle
decentdb_db* decentdb_open(const char* path_utf8, const char* options_utf8);
int decentdb_close(decentdb_db* db);

// Error reporting
int decentdb_last_error_code(decentdb_db* db);
const char* decentdb_last_error_message(decentdb_db* db);

// Prepared/streaming statements
int decentdb_prepare(decentdb_db* db, const char* sql_utf8, decentdb_stmt** out_stmt);

// Bind parameters: 1-based indexes match $1..$N
int decentdb_bind_null(decentdb_stmt* stmt, int index_1_based);
int decentdb_bind_int64(decentdb_stmt* stmt, int index_1_based, int64_t v);
int decentdb_bind_float64(decentdb_stmt* stmt, int index_1_based, double v);
int decentdb_bind_text(decentdb_stmt* stmt, int index_1_based, const char* utf8, int byte_len);
int decentdb_bind_blob(decentdb_stmt* stmt, int index_1_based, const uint8_t* data, int byte_len);

// Step rows: returns 1=row available, 0=done, <0=error
int decentdb_step(decentdb_stmt* stmt);

// Column metadata
int decentdb_column_count(decentdb_stmt* stmt);
const char* decentdb_column_name(decentdb_stmt* stmt, int col_0_based);
int decentdb_column_type(decentdb_stmt* stmt, int col_0_based);

// Column accessors (valid after step() returns 1)
int decentdb_column_is_null(decentdb_stmt* stmt, int col_0_based);
int64_t decentdb_column_int64(decentdb_stmt* stmt, int col_0_based);
double decentdb_column_float64(decentdb_stmt* stmt, int col_0_based);
const char* decentdb_column_text(decentdb_stmt* stmt, int col_0_based, int* out_byte_len);
const uint8_t* decentdb_column_blob(decentdb_stmt* stmt, int col_0_based, int* out_byte_len);

int64_t decentdb_rows_affected(decentdb_stmt* stmt);
void decentdb_finalize(decentdb_stmt* stmt);
```

### Python-Specific Performance Extension (Recommended)

Python function-call overhead can dominate if each cell requires an FFI call. Add one of:

1. **Row batch API**: fetch up to N rows in one call (best for `fetchmany()`):

```c
// Conceptual: fills user-provided arrays of pointers/types/lengths for a batch
int decentdb_fetch_batch(
    decentdb_stmt* stmt,
    int max_rows,
    int* out_rows,
    /* out: per-cell metadata */
    const void*** out_values,
    int* out_types,
    int* out_lengths
);
```

2. **Row view API**: one call returns a pointer to a row structure describing all columns.

Either approach should preserve the “borrowed view” lifetime model and allow the Python driver to decode values with minimal crossings.

### FFI Ownership + Lifetime Rules

- All pointers returned by `decentdb_last_error_message`, `decentdb_column_name`, and `decentdb_column_text/blob` are borrowed views.
- Borrowed pointers remain valid until the next call that mutates the same handle OR until `decentdb_finalize`/`decentdb_close`.
- Python MUST copy strings/blobs immediately into Python-managed objects.
- Avoid cross-thread use of a single `decentdb_stmt*`.

---

## Phase 2: Python DB-API 2.0 Driver (`decentdb`)

### Requirements

Implement a DB-API 2.0 compatible driver (PEP 249) usable by SQLAlchemy.

Minimum surface:
- `connect(...) -> Connection`
- `Connection.cursor() -> Cursor`
- `Cursor.execute(sql, params=None)`
- `Cursor.executemany(sql, seq_of_params)`
- `Cursor.fetchone()/fetchmany()/fetchall()`
- iterator protocol (`for row in cursor:`)
- transactions: `commit()`, `rollback()`

### Parameter Handling

SQLAlchemy often sends **named** parameters. The driver MUST rewrite into `$1..$N` positional format.

Rules:
- Preserve stable parameter ordering within a statement.
- Support dict params (`{"name": "Alice"}`) and sequence params (`[1, "Alice"]`) depending on SQLAlchemy compilation strategy.
- Reject mixed styles.

### Transactions and Isolation

- Default to Snapshot Isolation as defined by the engine.
- Provide `Connection.begin()` semantics via SQL (`BEGIN`, `COMMIT`, `ROLLBACK`) or direct engine hooks.
- SQLAlchemy expects autocommit behavior to be controlled by the dialect; the driver should remain explicit and predictable.

### Threading and Pooling

- SQLAlchemy pools connections by default; the driver must tolerate frequent open/close.
- Enforce DecentDB’s single-writer model:
  - Recommend one “writer Engine” or one writer connection at a time.
  - Allow concurrent readers through separate connections.

---

## Phase 3: SQLAlchemy Dialect (`decentdb_sqlalchemy`)

### Requirements

Implement a SQLAlchemy dialect that:
- declares capabilities consistent with DecentDB MVP
- compiles SQL using supported syntax
- integrates the DB-API driver
- provides correct reflection/inspection behavior where possible

Recommended URL forms:
- `decentdb+pysql:////absolute/path/to.db`
- (future) `decentdb+native:////absolute/path/to.db` for a CPython extension driver

### Dialect Tasks

1. **DB-API integration**
   - `dbapi()` returns the `decentdb` module
   - connect args mapping (path, cache size, WAL/fsync options)

2. **SQL compilation compatibility**
   - Ensure `LIMIT/OFFSET` compilation matches DecentDB
   - Ensure `RETURNING` is only enabled if DecentDB supports it (otherwise emulate)

3. **Type compiler / DDL**
   - Map SQLAlchemy types to DecentDB storage types (see Type Matrix)
   - Enforce conservative defaults (avoid features DecentDB doesn’t support)

4. **Introspection (best effort)**
   - `Inspector.get_table_names()`, `get_columns()`, indexes, FKs when supported by DecentDB catalogs

5. **Execution options / perf hooks**
   - Optional `compiled_cache` integration to reduce SQL compilation overhead
   - Optional “fast row” mode (tuple rows) for Core read-heavy usage

---

## Phase 4: ORM/DAL/CRUD Features

SQLAlchemy provides the ORM/DAL/CRUD semantics; DecentDB support is about correctness + performance.

### Required Capabilities

- Basic ORM CRUD:
  - `Session.add()`, `Session.delete()`, `Session.get()`, flush/commit
- Relationship support:
  - `relationship()` for one-to-many / many-to-one
  - basic eager loading patterns (`joinedload`, `selectinload`) where query shapes are supported
- Bulk operations:
  - SQLAlchemy Core `insert().values(...); executemany`
  - ORM bulk patterns where safe

### Recommended Patterns for Fast SELECT

- Prefer Core selects + `session.execute(select(...))` for hot paths.
- Use projection (`select(User.id, User.name)`) to avoid `SELECT *`.
- For large pagination, prefer keyset pagination when schema allows.

---

## Phase 5: Type Mapping

### Python / SQLAlchemy to DecentDB Type Matrix

| Python / SQLAlchemy Type | DecentDB Type | Notes |
|--------------------------|---------------|-------|
| `int` / `Integer` | INT64 | 64-bit signed |
| `bool` / `Boolean` | BOOL | 0/1 |
| `float` / `Float` | FLOAT64 | double precision |
| `str` / `String` / `Text` | TEXT | UTF-8 |
| `bytes` / `LargeBinary` | BLOB | returns `bytes` or `memoryview` |
| `datetime.datetime` / `DateTime` | INT64 | Unix epoch milliseconds (UTC) |
| `datetime.date` / `Date` | INT64 | days since epoch (UTC) |
| `datetime.time` / `Time` | INT64 | ticks since midnight (or ms) |
| `decimal.Decimal` / `Numeric` | TEXT | string representation (precision preserved) |
| `uuid.UUID` | BLOB | 16 bytes |
| `Enum` | INT64 or TEXT | configurable: value or name |

### Notes

- Store UTC only for `DateTime` to avoid per-row timezone conversions on hot paths.
- Prefer fixed-size representations for indexed fields (INT64/BLOB(16)) for predictable performance.
- If DecentDB later supports engine-enforced `VARCHAR(n)`, update DDL compilation behind an ADR.

---

## Error Handling Strategy

### Requirements

Map native DecentDB errors into:
- DB-API exception hierarchy (PEP 249)
- SQLAlchemy exceptions with preserved context

### Suggested Exception Mapping

| DecentDB Error Code | DB-API Exception | Notes |
|---------------------|------------------|------|
| `ERR_CONSTRAINT` | `IntegrityError` | FK/unique/check constraints |
| `ERR_TRANSACTION` | `OperationalError` | transaction/snapshot/WAL issues (busy/timeout not currently a distinct code) |
| `ERR_IO` | `OperationalError` | filesystem issues (disk full is not currently a distinct code) |
| `ERR_SQL` | `ProgrammingError` | SQL parse/bind/exec errors |
| `ERR_CORRUPTION` | `DatabaseError` | corruption detected |
| `ERR_INTERNAL` | `InternalError` | engine bug / invariant failure |

### Context Preservation

Errors raised through SQLAlchemy should include:
- rendered SQL string
- bound parameter values (redacted / size-capped)
- native error code + message

---

## SQL Logging and Observability

### Requirements

- Support SQLAlchemy standard logging (`echo=True`) without excessive overhead.
- Provide optional driver-level tracing hooks:
  - query timings (prepare/bind/step/decode)
  - rows returned / rows affected
  - cache hit/miss metrics for prepared statement cache

### Optional: `EXPLAIN` / Query Plan

If DecentDB exposes an `EXPLAIN`-style facility, the dialect should integrate with SQLAlchemy `EXPLAIN` patterns (best-effort, debug-only).

---

## Connection Configuration

### Connection URL / Parameters

Recommended URL:
- `decentdb+pysql:////path/to.db?cache_size=1024&fsync=on`

Parameters (illustrative):
- `cache_size`: page cache size in pages or MiB
- `fsync`: `on|off` (default on for ACID)
- `wal_mode`: `on|off` if configurable
- `busy_timeout_ms`: lock wait timeout

**Note:** Exact options must match DecentDB engine option parsing.

---

## Performance Optimization

### Requirements

**SELECT operations are critical path — every millisecond matters.**

**Overhead Budget:** Python layer must add **< 1ms** to native DecentDB execution time for typical SELECTs.

### Tasks (Critical)

1. **Prepared statement caching**
   - cache by SQL string + parameter shape
   - per-connection cache with LRU eviction

2. **Batch fetching**
   - implement `fetchmany(size)` efficiently
   - prefer batch-oriented native API (see Phase 1 extension)

3. **Fast decoding**
   - decode TEXT via `PyUnicode_DecodeUTF8` (C-extension) or `bytes.decode('utf-8')` with buffer reuse
   - minimize intermediate allocations

4. **Row materialization control**
   - provide tuple rows for Core queries
   - avoid per-row dict creation on hot paths

5. **Executemany for bulk inserts**
   - single transaction
   - prepared statement reuse
   - bind batching where possible

6. **Pagination guidance**

Bad (OFFSET scans N rows):
```sql
SELECT * FROM artists ORDER BY id LIMIT 20 OFFSET 100000
```

Good (keyset pagination):
```sql
SELECT * FROM artists WHERE id > $1 ORDER BY id LIMIT 20
```

---

## Testing Strategy

### Tasks

1. **Unit tests**
   - parameter rewriting and binding
   - type round-trips
   - DDL/type compiler outputs

2. **Integration tests (SQLAlchemy Core)**
   - `create_engine` + execute + fetch
   - transactions (commit/rollback)
   - executemany

3. **Integration tests (SQLAlchemy ORM)**
   - mapped classes CRUD
   - relationships and eager load patterns

4. **Performance benchmarks**
   - driver overhead microbenchmarks (prepare/bind/step/decode)
   - end-to-end ORM query latency for common patterns

5. **Cross-platform**
   - Linux, macOS, Windows
   - wheels for x64/arm64

---

## Implementation Order

1. Native C API stability + streaming semantics
2. DB-API driver skeleton (connect/execute/iterate)
3. Parameter rewriting + type conversions
4. SQLAlchemy dialect MVP (types + execution)
5. ORM integration tests and examples
6. Performance extensions (batch fetch) and benchmarks
7. Packaging: `pip` wheels + manylinux/macos/windows builds

---

## Architecture Decision Records (ADRs)

This document depends on:
- ADR-0005 (SQL parameterization style: `$1..$N`)
- ADR-0023 (Snapshot Isolation semantics)

Any changes that impact:
- persistent format
- WAL formats
- concurrency semantics
- SQL grammar

…require an ADR per repository policy.

---

## Success Criteria

- SQLAlchemy Core can execute queries with correct results and transaction semantics.
- SQLAlchemy ORM CRUD works for common patterns without dialect-specific hacks.
- SELECT overhead budget met (< 1ms added on typical hot paths).
- Cross-platform packaging works with reproducible wheels.

---

## Open Questions - Recommendations

1. Should the first Python driver be a C-extension (fast) or `cffi`/`ctypes` (faster iteration, slower hot path)?
2. Which DB-API paramstyle should the driver expose to SQLAlchemy (`named` vs `pyformat`), given the engine requires `$N`?
3. Should the driver expose a separate “read-only connection” mode for pool separation (readers vs writer)?
4. Should the dialect implement `RETURNING` if/when DecentDB supports it, or emulate via `last_insert_rowid`-style APIs?

---

## Dependencies

- Python 3.11+ (recommended baseline)
- SQLAlchemy 2.x
- Native binding layer:
  - MVP: `cffi` or `ctypes`
  - Performance tier: CPython extension / Cython (future)

---

## Risks

- Python-level overhead may exceed the <1ms budget without a batch fetch API.
- SQLAlchemy feature expectations (reflection, schema features) may exceed DecentDB MVP scope.
- Packaging native libraries for manylinux/macos/windows must be automated early.

---

## Future Considerations (Post-1.0.0)

- Alembic integration and schema autogeneration
- Async SQLAlchemy support (requires async DB-API adaptation)
- Extended reflection (views, more index metadata)
- Optional engine-side string length constraints (`VARCHAR(n)`) behind an ADR
- Additional SQL features as DecentDB SQL subset expands
