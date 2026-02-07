# sqlc Support Requirements for DecentDB 1.0.0

## Overview

Enable Go applications to perform **extremely fast** CRUD operations and compiled, type-safe queries against DecentDB database files using:
- Go’s `database/sql`
- [`sqlc`](https://sqlc.dev) code generation

This is **not** an ORM design. sqlc generates strongly typed query methods from SQL files; the goal is to make DecentDB feel like a first-class embedded database for Go with a **performance-first** `database/sql` driver.

## Goals

- Go apps can query DecentDB files directly (**embedded mode**, no server)
- `database/sql` driver works out-of-the-box
- `sqlc` works out-of-the-box (no manual scanning boilerplate)
- Full CRUD via `sqlc`-generated code (SELECT/INSERT/UPDATE/DELETE)
- **Performance-first SELECT operations**
  - driver overhead budget: **< 1ms** for typical operations
  - forward-only, streaming row iteration

## Compatibility Constraints (Non-Negotiable)

- **SQL parameters (engine):** DecentDB uses Postgres-style positional parameters (`$1, $2, ...`) per ADR-0005.
  - The Go driver MUST accept `$N` and SHOULD reject unsupported parameter styles (e.g., `?`, `@name`) rather than silently misbinding.
  - The sqlc integration MUST configure `engine: postgresql` so generated SQL uses `$N`.
- **Isolation (engine):** Default isolation is **Snapshot Isolation** per ADR-0023.
  - The driver MUST not claim stronger guarantees.
- **Concurrency model:** single process, one writer, multiple concurrent readers.
  - The driver SHOULD support many concurrent read connections and MUST handle writer contention predictably.

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

**Critical:** Query execution overhead (Go driver + cgo boundary + scanning) must add **< 1ms** to native DecentDB execution time for common SELECT paths.

## Non-Goals

- No ORM, no identity map, no unit-of-work abstraction beyond `database/sql`
- No reflection-based mapping layer
- No server mode
- No multi-process concurrency claims
- No full PostgreSQL protocol / wire compatibility

---

## Architecture

```
Go Application
  ├── sqlc (generates typed query methods)
  └── database/sql
          ↓
      decentdb-go (database/sql driver)
          ↓
      DecentDB.Native (cgo binding to Nim C API)
          ↓
      DecentDB (Nim engine, direct file I/O)
```

---

## Phase 1: Native C API (Nim)

### Requirements

Expose a C-compatible API from the Nim DecentDB engine suitable for `cgo`.

**Performance-first SELECT requirement:** Provide a forward-only, streaming statement API.

Baseline API:

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

### Go Performance Extension (Recommended)

A naive Go driver would cross the cgo boundary for every column read (e.g., `column_int64` per column). That overhead can dominate.

Add a **row-batch** or **row-view** API to reduce cgo calls:

- **Row view:** one cgo call returns a structure describing the current row (types + pointers + lengths). The Go driver then decodes columns without additional cgo calls.
- **Batch fetch:** one call fetches up to N rows for `Rows.Next` loops and `sql.Rows` scans.

Either option must keep pointer lifetime rules explicit and safe.

### FFI Ownership + Lifetime Rules

- Pointers returned by `decentdb_last_error_message`, `decentdb_column_name`, and `decentdb_column_text/blob` are borrowed views.
- Borrowed pointers remain valid until the next call that mutates the same handle OR until `decentdb_finalize`/`decentdb_close`.
- Go MUST copy strings/blobs immediately into Go-managed memory.
- Avoid cross-goroutine use of a single `decentdb_stmt*`.

---

## Phase 2: Go `database/sql` Driver (`decentdb-go`)

### Requirements

Implement a standard `database/sql/driver` driver:

- `driver.Driver`
- `driver.Connector` (recommended)
- `driver.Conn`
- `driver.Stmt`
- `driver.Rows`
- `driver.Tx`

Use cgo to call into the native C ABI.

### Key Semantics

1. **Context support**
   - Implement `QueryContext`, `ExecContext`, `BeginTx`.
   - Respect `context.Context` cancellation where feasible (best-effort; if native cannot interrupt execution, enforce cancellation at row iteration boundaries).

2. **Prepared statements**
   - `PrepareContext` creates a native `decentdb_stmt*`.
   - `Stmt.QueryContext` / `Stmt.ExecContext` bind params + step.
   - Statement reuse should be supported and efficient.

3. **Transactions**
   - `BeginTx` starts a transaction; implement `Commit`/`Rollback`.
   - Map Go’s `sql.TxOptions` isolation to DecentDB Snapshot semantics.

4. **Single-writer enforcement**
   - If the engine blocks writers, surface predictable errors/timeouts.
   - DSN option `busy_timeout_ms` should map to DecentDB lock timeout behavior.

### Parameter Binding Rules

- Support `database/sql` parameter values:
  - `nil`
  - `int64`, `float64`, `bool`, `string`, `[]byte`
  - `time.Time` (mapping described below)
- Reject unsupported types early with clear errors.

### Rows and Scanning

`database/sql` expects `Rows.Next(dest []driver.Value)`.

Performance requirement:
- avoid per-column cgo calls in hot loops
- decode into `driver.Value` with minimal allocations

Implementation approach:
- On `Rows.Next`, use the row-view/batch native API (Phase 1 extension) when available.
- Otherwise, fall back to per-column accessors with careful buffering (acceptable for the 0.x baseline, but may miss the <1ms overhead budget).

---

## Phase 3: sqlc Integration (No ORM)

### Requirements

Provide a documented, tested workflow where users:
- write SQL in `queries/*.sql`
- run sqlc to generate typed Go code
- call generated methods using `*sql.DB` / `*sql.Tx`

### sqlc Configuration

To match DecentDB’s parameter contract, sqlc MUST be configured as PostgreSQL:

```yaml
version: "2"
sql:
  - engine: "postgresql"
    schema: "schema.sql"
    queries: "queries.sql"
    gen:
      go:
        package: "db"
        out: "internal/db"
        sql_package: "database/sql"
```

### Example Queries

```sql
-- name: GetArtist :one
SELECT id, name
FROM artists
WHERE id = $1;

-- name: ListArtists :many
SELECT id, name
FROM artists
ORDER BY id
LIMIT $1 OFFSET $2;

-- name: CreateArtist :exec
INSERT INTO artists (id, name)
VALUES ($1, $2);

-- name: UpdateArtist :exec
UPDATE artists
SET name = $2
WHERE id = $1;

-- name: DeleteArtist :exec
DELETE FROM artists
WHERE id = $1;
```

### Usage (Goal State)

```go
db, err := sql.Open("decentdb", "file:/path/to.db?cache_size=4096&busy_timeout_ms=5000")
if err != nil { panic(err) }

q := dbgen.New(db) // sqlc-generated constructor

ctx := context.Background()
artist, err := q.GetArtist(ctx, 1)
```

**Important:** sqlc is the mapping layer. The DecentDB Go support should not invent an ORM-style API.

---

## Phase 4: Type Mapping

### Go / `database/sql` to DecentDB Type Matrix

| Go Type (scan/bind) | DecentDB Type | Notes |
|---------------------|---------------|------|
| `int64` | INT64 | primary numeric type |
| `int` | INT64 | bind/scan via int64 conversion |
| `float64` | FLOAT64 | |
| `bool` | BOOL | 0/1 |
| `string` | TEXT | UTF-8 |
| `[]byte` | BLOB | copy on read |
| `time.Time` | INT64 | Unix epoch milliseconds (UTC) |
| `uuid.UUID` (optional) | BLOB | 16 bytes |

### Time Handling (Critical for Correctness + Performance)

DecentDB should store time as INT64 for fast comparisons and indexing.

- Store: Unix epoch milliseconds (UTC)
- Bind: `time.Time` → `int64`
- Scan: `int64` → `time.Time`

sqlc integration options:
- keep schema columns as `INT64` and use `sqlc` type overrides to map to `time.Time`
- or use custom wrapper types (e.g., `type UnixMillis time.Time`) if you want explicitness

---

## Error Handling Strategy

### Requirements

Map native DecentDB errors to idiomatic Go errors and `database/sql` behaviors.

### Suggested Mapping

| DecentDB Error Code | Go / database/sql mapping |
|---------------------|---------------------------|
| `ERR_CONSTRAINT` | return `*DecentDBError` wrapping constraint code; callers may treat as application error |
| `ERR_TRANSACTION` | return `context.DeadlineExceeded` if ctx expired; else `*DecentDBError` (operational). Busy/timeout is not currently a distinct native code |
| `ERR_IO` | return `*DecentDBError` (operational). Disk full is not currently a distinct native code |
| `ERR_SQL` | return `*DecentDBError` (programmer error) |
| `ERR_CORRUPTION` | return `*DecentDBError` |
| `ERR_INTERNAL` | return `*DecentDBError` |

The driver SHOULD expose a structured error type:

```go
type DecentDBError struct {
    Code int
    Message string
    SQL string
}

func (e *DecentDBError) Error() string { /* ... */ return e.Message }
```

### Bad Connection Signaling

When the native handle is invalid or corrupted such that reuse is unsafe, return `driver.ErrBadConn` so `database/sql` can drop the connection.

---

## SQL Logging and Observability

### Requirements

**Zero-cost when disabled.**

- DSN flag: `logging=0|1`, `log_level=debug|info|warn|error`
- Hook interface (optional):
  - `OnQueryStart(sql string, args []any)`
  - `OnQueryEnd(sql string, args []any, dur time.Duration, rowsAffected int64, err error)`

When disabled, overhead must be a single branch with no allocations.

---

## DSN / Connection Parameters

Recommended DSN format (illustrative):

- `file:/absolute/path/to.db?cache_size=4096&busy_timeout_ms=5000&logging=0`

Parameters:
- `cache_size`: page cache size (pages or MiB)
- `busy_timeout_ms`: lock wait timeout
- `fsync`: `on|off` (default on)
- `checkpoint_threshold_mb`: auto-checkpoint threshold
- `logging`, `log_level`

Exact options must match DecentDB engine option parsing.

---

## Performance Optimization

### Requirements

**SELECT operations are critical path — every millisecond matters.**

**Overhead Budget:** Go driver must add **< 1ms** to native DecentDB execution time for typical SELECTs.

### Tasks (Critical)

1. **Reduce cgo boundary crossings**
   - implement row-view or batch-fetch native API (Phase 1 extension)
   - avoid per-column native calls in `Rows.Next`

2. **Prepared statement caching**
   - `database/sql` already caches prepared statements per connection in some patterns, but drivers often benefit from explicit reuse
   - cache by SQL string + parameter shape

3. **Fast decoding + minimal allocations**
   - reuse buffers for TEXT/BLOB decoding
   - return `[]byte` as copy (safe) or `RawBytes`-like semantics only if lifetime is explicit (dangerous)

4. **Executemany strategy**
   - sqlc often loops over `Exec` calls; provide a documented “bulk insert” pattern:
     - explicit transaction
     - explicit prepared statement reuse

5. **Pagination guidance**

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
   - DSN parsing
   - parameter binding
   - type conversions (including time)
   - error mapping and `ErrBadConn` behavior

2. **Integration tests (database/sql)**
   - open/close
   - query/exec with parameters
   - transactions
   - concurrency: many readers + one writer

3. **Integration tests (sqlc)**
   - generate queries for a representative schema
   - run CRUD methods end-to-end

4. **Performance benchmarks**
   - microbench: `Rows.Next` + scan cost per row
   - end-to-end: `sqlc` generated methods latency

5. **Cross-platform**
   - Linux, macOS, Windows
   - amd64 + arm64

---

## Implementation Order

### Sprint 0: Core Engine (Prerequisite)
1. High-performance streaming SELECT ABI (`prepare/bind/step/column/finalize`)
2. Error codes for mapping (`decentdb_last_error_code()`)
3. Parameter binding contract: `$1..$N` (ADR-0005) with unit tests

### Sprint 1: Foundation
1. Go cgo bindings for the C ABI
2. `database/sql` driver skeleton (Driver/Conn/Stmt/Rows/Tx)
3. Basic DSN parsing and option plumbing

### Sprint 2: sqlc baseline
1. sqlc config + examples
2. Integration tests running generated code
3. Type mapping (including time)

### Sprint 3: Performance
1. Native row-view/batch-fetch extension
2. `Rows.Next` implementation using row batching
3. Prepared statement cache tuning

### Sprint 4: Polish
1. Observability hooks
2. Packaging and cross-platform builds
3. Benchmarks and docs

---

## Architecture Decision Records (ADRs)

This document depends on:
- ADR-0005 (SQL parameterization style: `$1..$N`)
- ADR-0023 (Snapshot Isolation semantics)

Create an ADR before implementing:
- Go packaging strategy (static vs dynamic linking, wheel-like distribution for Go)
- Native row batching ABI shape (performance vs complexity)
- Time storage semantics if diverging from INT64 epoch-ms

---

## Success Criteria

### Functionality
1. ✅ Can open DecentDB file from Go via `database/sql` and execute SQL.
2. ✅ sqlc-generated code compiles and runs without manual scanning boilerplate.
3. ✅ CRUD works end-to-end (including transactions).
4. ✅ Concurrency model respected (many readers, one writer).

### Performance (Critical)
5. ✅ Single record query: < 2ms (P95)
6. ✅ Filtered list query: < 10ms + 0.5ms/row (P95)
7. ✅ Paginated + Sorted query: < 20ms + 0.5ms/row (P95)
8. ✅ Go driver overhead: < 1ms over native DecentDB execution on typical queries

---

## Open Questions - Recommendations

1. Should the Go driver target pure cgo with dynamic library loading, or link a static library for simpler distribution?
2. Do we want to support named parameters at the Go driver layer (not required for sqlc) or keep `$N` only for correctness/performance?
3. What is the minimal native batch-fetch ABI that meaningfully reduces cgo overhead without complicating pointer lifetimes?

---

## Dependencies

- Go 1.22+ (recommended baseline)
- sqlc 1.26+ (or current)
- CGO toolchain for supported platforms

---

## Risks

1. **cgo overhead**: without row batching, driver overhead may exceed the <1ms budget.
2. **Distribution complexity**: cross-platform native library shipping for Go needs early automation.
3. **Feature expectation mismatch**: users may expect PostgreSQL behaviors beyond DecentDB 0.x scope.
4. **Time type ergonomics**: mapping INT64 ↔ `time.Time` must be crystal clear and tested.

---

## Future Considerations (Post-1.0.0)

- Optional driver for `pgx`-style interfaces (still embedded)
- Async/cancellation improvements if native engine supports query interruption
- Extended introspection APIs for tooling
- Additional SQL features as DecentDB SQL subset expands
