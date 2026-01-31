# Node.js Support Requirements for DecentDB 1.0.0

## Overview

Enable Node.js applications to perform **extremely fast** queries and CRUD operations against DecentDB database files using:
- A native Node addon built on **N-API** (stable ABI)
- A **Knex** integration (custom client) for query building and execution

This is an embedded, serverless integration: the Node process reads/writes the database file directly via the DecentDB engine.

Target audience:
- Node services that want embedded, serverless database files
- Apps that want Knex’s query builder ergonomics
- Systems that need predictable low-latency SELECT performance with ACID transactions

## Goals

- Node apps can query DecentDB files directly (**embedded mode**, no server)
- N-API addon exposes a small, stable, high-performance API for:
  - open/close
  - prepared statements
  - parameter binding
  - forward-only streaming reads
  - transactions
- Knex integration works out-of-the-box via a custom Knex client
- **Performance-first SELECT operations**
  - binding + driver overhead budget: **< 1ms** for typical operations
  - avoid per-cell cross-boundary overhead in hot loops where feasible
- Clear, enforceable lifetime + ownership rules at the addon boundary

## Compatibility Constraints (Non-Negotiable)

- **SQL parameters (engine):** DecentDB uses Postgres-style positional parameters (`$1, $2, ...`) per ADR-0005.
  - The Knex client MUST compile/bind parameters to `$N` before calling native.
  - The N-API layer SHOULD reject unsupported parameter styles rather than silently misbinding.
- **Isolation (engine):** Default isolation is **Snapshot Isolation** per ADR-0023.
  - Node bindings MUST not claim stronger guarantees.
- **Concurrency model (MVP):** single process, one writer, multiple concurrent readers.
  - Node bindings MUST avoid implying cross-process coordination.
  - Statement handles MUST NOT be used concurrently from multiple threads.
- **Event loop discipline:** Long-running native work MUST NOT block the Node event loop.
  - Provide async execution for query/step loops, or clearly document that callers must use worker threads.

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

**Critical:** Node-layer overhead (N-API conversions + JS API + Knex plumbing) must add **< 1ms** to native DecentDB execution time for common SELECT paths.

## Non-Goals

- No DecentDB server process (embedded only)
- No multi-process concurrency guarantees
- No PostgreSQL wire protocol compatibility
- No promise of full Knex feature parity across all DBs (only what maps cleanly to DecentDB’s SQL subset)
- No ORM layer beyond what Knex provides

---

## Architecture

```
Node Application
  ├── Knex (optional, query builder)
  └── decentdb (N-API addon)
          ↓
      DecentDb.Native (N-API → C ABI)
          ↓
      DecentDB (Nim engine, direct file I/O)
```

Key principle: **all language/toolkit bindings should reuse a single, stable native ABI** so fixes and performance work benefit every ecosystem.

---

## Phase 1: Native ABI (Nim C API)

### Requirements

The Node addon should call into DecentDB through the existing C ABI (see `src/c_api.nim`). This keeps the addon small and avoids duplicating engine logic.

**Performance-first SELECT requirement:** Provide a forward-only, streaming statement API.

Baseline API (as used by other bindings):

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

### Node/JS Performance Extension (Recommended)

A naive addon will call from JS → N-API → C once per column read. That overhead can dominate.

Add a **row-view** or **batch fetch** API to reduce crossings:

- **Row view:** one call returns arrays of (type, pointer, length) for the current row.
- **Batch fetch:** one call fetches up to N rows (good for `fetchAll()` / `stream()` buffering).

Either option must keep pointer lifetime rules explicit and safe.

### FFI Ownership + Lifetime Rules

- Pointers returned by `decentdb_last_error_message`, `decentdb_column_name`, and `decentdb_column_text/blob` are borrowed views.
- Borrowed pointers remain valid until the next call that mutates the same handle OR until `decentdb_finalize`/`decentdb_close`.
- The addon MUST copy text/blob values into JS-owned buffers/strings before returning them to JS.
- Avoid cross-thread use of a single `decentdb_stmt*`.

---

## Phase 2: N-API Addon (`bindings/node/`)

### Goals

- Provide a small, unsurprising JS API with explicit lifetimes.
- Provide async operations so queries do not block the event loop.
- Surface errors as JS exceptions with actionable messages.

### Repo Scaffolding (Implemented)

The repository now contains an initial scaffold for Node support:

- `bindings/node/decentdb/`: N-API addon + thin JS wrapper
  - `bindings/node/decentdb/src/addon.c`: N-API module (C)
  - `bindings/node/decentdb/src/native_lib.c`: runtime `dlopen`/`dlsym` loader for `libdecentdb.*`
  - `bindings/node/decentdb/index.js`: minimal `Database` + `Statement` wrapper
- `bindings/node/knex-decentdb/`: Knex client scaffold
  - `bindings/node/knex-decentdb/src/client.js`: minimal custom Knex client
  - `bindings/node/knex-decentdb/src/positionBindings.js`: `?` → `$N` placeholder rewrite helper

### Proposed JS API (Goal State)

```js
import { Database } from "decentdb";

const db = new Database({ path: "/path/to.db" });

// Fast path: prepare + bind + iterate
const stmt = db.prepare("SELECT id, name FROM artists WHERE id = $1");
stmt.bindInt64(1, 123);

for await (const row of stmt.rows()) {
  // row can be an array or object depending on mode
}

stmt.finalize();
db.close();
```

Notes:
- Prefer explicit `finalize()`/`close()`; also support `FinalizationRegistry` as a safety net (best-effort), but do not rely on it for correctness.
- Provide both:
  - `rows()` async iterator (streaming)
  - `all()` materializing helper (convenience)

### Async Execution Model

- Any operation that can run longer than “trivially fast” should be available as async:
  - `stmt.stepAsync()` or `stmt.rows()` implemented via `napi_async_work` / thread pool
  - optional `db.execAsync(sql, params)` convenience
- Document that:
  - one writer / many readers is supported
  - a single `Database` instance is not meant for concurrent multi-threaded use unless explicitly designed

### Error Handling

- Map native errors to JS exceptions with:
  - operation name (open/prepare/bind/step)
  - native error code
  - native message

---

## Phase 3: Knex Integration (`bindings/node/knex-decentdb`)

Knex is primarily a SQL builder + execution orchestrator. Integrating DecentDB cleanly typically means implementing a **custom Knex client**.

### Requirements

- `client: "decentdb"` should be supported.
- Parameter binding MUST use `$1..$N` (engine contract).
  - Knex’s default binding style is typically `?`; the DecentDB client MUST override binding compilation.
- Transactions via `knex.transaction(...)` must use a single underlying connection/handle for the transaction scope.

### Example Usage (Goal State)

```js
import knex from "knex";

const db = knex({
  client: "decentdb",
  connection: {
    filename: "/path/to.db",
  },
});

const rows = await db("artists")
  .select(["id", "name"])
  .where("name", "like", "%ali%")
  .orderBy("id")
  .limit(10);
```

### Dialect/Compilation Notes

- Identifier quoting and escaping must match DecentDB’s supported SQL.
- Knex features that require unsupported SQL should fail fast with clear errors.
- Prefer reusing Knex’s PostgreSQL compilation rules where possible, but do not claim full PostgreSQL compatibility.

---

## Testing Strategy

- Add Node-level tests using the built-in Node test runner (`node:test`) to avoid adding dependencies.
- Core coverage:
  - open/close lifetime
  - prepare/bind/step and streaming iteration
  - text/blob copying semantics (no use-after-free)
  - transaction commit/rollback behavior
  - Knex integration: bindings use `$N`, basic SELECT/INSERT/UPDATE/DELETE
- Include a small set of crash-injection/durability tests via the existing harness where appropriate (the addon should not weaken durability guarantees).

---

## Packaging and Distribution (Goal State)

- Prefer N-API for ABI stability across Node versions.
- Provide prebuilt binaries for major platforms when practical, otherwise document local build requirements.
- Keep the addon’s native surface area small; push heavy logic into the DecentDB engine.

---

## Open Questions

1. Should the Node addon link directly against the engine, or `dlopen` a shared `libdecentdb` built from Nim?
2. The C ABI already exposes `decentdb_row_view` (row-view performance extension). Should Node prefer this exclusively, or also add a batch-fetch API for `all()`/buffered streaming?
3. What is the minimal Knex feature set we promise for 1.0.0 (schema building, migrations, pooling, etc.)?
