# C/C++ ABI

DecentDB exposes a stable C ABI through `include/decentdb.h`. This is the
lowest-level native integration surface and the shared boundary used by the
higher-level language bindings.

C++ applications can include the same header directly. The header wraps the
public declarations in `extern "C"` when compiled as C++, so the exported
symbols keep C linkage. DecentDB does not currently ship a separate idiomatic
C++ wrapper library.

## Source Of Truth

| Item | Location |
|---|---|
| Public header | `include/decentdb.h` |
| Rust ABI implementation | `crates/decentdb/src/c_api.rs` |
| C smoke test | `tests/bindings/c/smoke.c` |
| C memory churn test | `tests/bindings/c/memory_churn.c` |
| Local compile script | `tests/bindings/c/run.sh` |

## Build And Link

Build the shared library from source:

```bash
cargo build -p decentdb
```

Then compile a C program against the header and shared library:

```bash
cc \
  -I/path/to/decentdb/include \
  app.c \
  -L/path/to/decentdb/target/debug \
  -Wl,-rpath,/path/to/decentdb/target/debug \
  -ldecentdb \
  -o app
```

The repository smoke test uses the same pattern:

```bash
bash tests/bindings/c/run.sh
```

When using a release artifact, point `-I` at the directory containing
`decentdb.h` and point `-L` / your runtime library path at the extracted native
library.

## Status And Errors

Every fallible C ABI call returns `ddb_status_t`.

```c
static void check(ddb_status_t status, const char *context) {
  if (status != DDB_OK) {
    const char *error = ddb_last_error_message();
    fprintf(stderr, "%s failed with status %u: %s\n", context, status,
            error == NULL ? "<null>" : error);
    exit(1);
  }
}
```

Common status codes:

| Code | Meaning |
|---|---|
| `DDB_OK` | Success |
| `DDB_ERR_IO` | I/O failure |
| `DDB_ERR_CORRUPTION` | Corruption or invalid database state |
| `DDB_ERR_CONSTRAINT` | Constraint violation |
| `DDB_ERR_TRANSACTION` | Transaction error |
| `DDB_ERR_SQL` | SQL parse, bind, or execution error |
| `DDB_ERR_INTERNAL` | Internal engine error |
| `DDB_ERR_PANIC` | Panic caught at the ABI boundary |
| `DDB_ERR_UNSUPPORTED_FORMAT_VERSION` | Database file format is newer than this engine |
| `DDB_ERR_BUSY` | Resource is busy |
| `DDB_ERR_TIMEOUT` | Operation timed out before it could run or complete |
| `DDB_ERR_CANCELED` | Operation was canceled before execution started |
| `DDB_ERR_QUEUE_FULL` | Write queue capacity is exhausted |
| `DDB_ERR_QUEUE_CLOSED` | Write queue is shutting down or closed |

`ddb_last_error_message()` returns a borrowed thread-local error string. Treat
the pointer as valid only until the next DecentDB call on the same thread.

## Ownership Rules

The C ABI uses opaque handles for databases, prepared statements, and query
results:

| Owned value | Free function |
|---|---|
| `ddb_db_t *` | `ddb_db_free(&db)` |
| `ddb_stmt_t *` | `ddb_stmt_free(&stmt)` |
| `ddb_result_t *` | `ddb_result_free(&result)` |
| `ddb_watch_t *` | `ddb_watch_close(&watch)` |
| owned strings returned as `char *` | `ddb_string_free(&value)` |
| owned copied cell values | `ddb_value_dispose(&value)` |

Rules:

- Free each successful owned handle exactly once.
- Pass the address of the pointer to free functions; they null the pointer.
- Do not free DecentDB-owned memory with `free()`.
- `ddb_value_view_t` pointers are borrowed and must not be freed.
- `ddb_value_t` text/blob payloads returned by copy functions are owned and
  must be released with `ddb_value_dispose`.
- Do not call free functions concurrently from multiple threads on the same
  pointer or handle.

## Queued Writes

`ddb_db_execute_queued` submits one SQL statement to the engine-owned write
queue. It returns the same result handle shape as `ddb_db_execute`.

```c
ddb_result_t *result = NULL;
check(ddb_db_execute_queued(
          db,
          "INSERT INTO events (id, name) VALUES (1, 'queued')",
          NULL,
          0,
          DDB_WRITE_QUEUE_TIMEOUT_DEFAULT,
          &result),
      "queued insert");
check(ddb_result_free(&result), "free queued result");
```

Pass `DDB_WRITE_QUEUE_TIMEOUT_DEFAULT` to use the database configured default
timeout. Pass `0` for immediate timeout behavior.

Queue behavior and strict group commit are documented in
[Write Concurrency](../user-guide/write-concurrency.md). Metrics are available
through `ddb_db_write_queue_metrics`:

```c
ddb_write_queue_metrics_t metrics;
check(ddb_db_write_queue_metrics(db, &metrics), "queue metrics");
printf("admitted=%llu committed=%llu syncs=%llu\n",
       (unsigned long long)metrics.admitted,
       (unsigned long long)metrics.committed,
       (unsigned long long)metrics.group_commit_syncs);
```

## Reactive Watch Handles

The C ABI exposes reactive subscriptions as opaque `ddb_watch_t` handles with
JSON requests and JSON event polling. Watches are in-process only and observe
committed state after the initial event.

```c
ddb_watch_t *watch = NULL;
check(ddb_db_watch_query_json(
          db,
          "{\"sql\":\"SELECT name FROM users ORDER BY id\"}",
          &watch),
      "watch query");

char *event_json = NULL;
check(ddb_watch_next_json(watch, 1000, &event_json), "initial event");
puts(event_json);
check(ddb_string_free(&event_json), "free initial event");

/* Run writes through any handle in the same process. */

check(ddb_watch_next_json(watch, 1000, &event_json), "invalidation event");
puts(event_json);
check(ddb_string_free(&event_json), "free invalidation event");

check(ddb_watch_close(&watch), "close watch");
```

Available creation functions:

- `ddb_db_watch_table_json`
- `ddb_db_watch_range_json`
- `ddb_db_watch_query_json`
- `ddb_db_change_stream_json`

`ddb_watch_next_json` returns `DDB_ERR_TIMEOUT` when no event is available
before the requested timeout. Returned event strings are freed with
`ddb_string_free`.

## Minimal C Example

This example mirrors the repository smoke test.

```c
#include "decentdb.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void check(ddb_status_t status, const char *context) {
  if (status != DDB_OK) {
    const char *error = ddb_last_error_message();
    fprintf(stderr, "%s failed with status %u: %s\n", context, status,
            error == NULL ? "<null>" : error);
    exit(1);
  }
}

int main(void) {
  ddb_db_t *db = NULL;
  ddb_result_t *result = NULL;
  size_t rows = 0;

  check(ddb_db_open_or_create(":memory:", &db), "open_or_create");
  check(ddb_db_execute(db,
                       "CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT)",
                       NULL, 0, &result),
        "create");
  check(ddb_result_free(&result), "free create");

  check(ddb_db_execute(db,
                       "INSERT INTO smoke (id, name) VALUES (1, 'c-smoke')",
                       NULL, 0, &result),
        "insert");
  check(ddb_result_free(&result), "free insert");

  check(ddb_db_execute(db, "SELECT id, name FROM smoke", NULL, 0, &result),
        "select");
  check(ddb_result_row_count(result, &rows), "row_count");

  if (rows != 1) {
    fprintf(stderr, "expected 1 row, got %zu\n", rows);
    return 1;
  }

  check(ddb_result_free(&result), "free select");
  check(ddb_db_free(&db), "free db");
  return 0;
}
```

## Reading Result Values

`ddb_db_execute` returns a materialized `ddb_result_t`. Use
`ddb_result_row_count`, `ddb_result_column_count`, and
`ddb_result_value_copy` to inspect it.

```c
ddb_value_t value;
check(ddb_value_init(&value), "init value");

check(ddb_result_value_copy(result, 0, 1, &value), "copy value");
if (value.tag == DDB_VALUE_TEXT) {
  printf("name=%.*s\n", (int)value.len, (const char *)value.data);
}

check(ddb_value_dispose(&value), "dispose value");
```

Text, blob, geometry, and geography values are byte buffers. They are not
guaranteed to be NUL-terminated; always use the returned length. Spatial values
are returned as normalized EWKB with `DDB_VALUE_GEOMETRY` or
`DDB_VALUE_GEOGRAPHY` tags.

Semantic values have dedicated ABI tags in `ddb_value_t` and
`ddb_value_view_t`:

| Tag | Payload fields |
|---|---|
| `DDB_VALUE_ENUM` | `enum_type_id`, `enum_label_id` |
| `DDB_VALUE_IPADDR` | `ip_family`, `ip_cidr_addr_bytes` |
| `DDB_VALUE_CIDR` | `ip_family`, `cidr_prefix_len`, `ip_cidr_addr_bytes` |
| `DDB_VALUE_DATE` | `date_days` |
| `DDB_VALUE_TIME` | `time_micros` |
| `DDB_VALUE_TIMESTAMPTZ_MICROS` | `timestamptz_micros` |
| `DDB_VALUE_INTERVAL` | `interval_months`, `interval_days`, `interval_micros` |
| `DDB_VALUE_MACADDR` | `ip_family` as length (`6` or `8`), `ip_cidr_addr_bytes` |

For inserts, bind text or integer values in a statement where the destination
column type is known; the engine performs the semantic cast during execution.

For read-heavy streaming paths, prefer the statement row-view APIs described
below to avoid per-cell heap allocation.

## Prepared Statements

Use `ddb_db_prepare` for repeated statements and bind parameters with
one-based indexes:

```c
ddb_stmt_t *stmt = NULL;
check(ddb_db_prepare(db,
                     "INSERT INTO users (id, name) VALUES ($1, $2)",
                     &stmt),
      "prepare insert");

check(ddb_stmt_bind_int64(stmt, 1, 1), "bind id");
check(ddb_stmt_bind_text(stmt, 2, "Ada", 3), "bind name");

uint8_t has_row = 0;
check(ddb_stmt_step(stmt, &has_row), "step insert");
check(ddb_stmt_free(&stmt), "free stmt");
```

Available typed bind helpers include:

- `ddb_stmt_bind_null`
- `ddb_stmt_bind_int64`
- `ddb_stmt_bind_float64`
- `ddb_stmt_bind_bool`
- `ddb_stmt_bind_text`
- `ddb_stmt_bind_blob`
- `ddb_stmt_bind_geometry_wkb`
- `ddb_stmt_bind_geography_wkb`
- `ddb_stmt_bind_uuid`
- `ddb_stmt_bind_decimal`
- `ddb_stmt_bind_timestamp_micros`

The spatial bind helpers accept WKB/EWKB byte buffers. GEOGRAPHY bindings are
normalized to SRID 4326 on insert.

Use `ddb_stmt_reset` to clear a statement's result cursor and
`ddb_stmt_clear_bindings` to remove existing parameter values.

## Streaming Row Views

For read-heavy paths, the ABI exposes borrowed row views:

```c
ddb_stmt_t *stmt = NULL;
check(ddb_db_prepare(db,
                     "SELECT id, name FROM users WHERE id >= $1 ORDER BY id",
                     &stmt),
      "prepare select");
check(ddb_stmt_bind_int64(stmt, 1, 1), "bind min id");

for (;;) {
  const ddb_value_view_t *values = NULL;
  size_t columns = 0;
  uint8_t has_row = 0;

  check(ddb_stmt_step_row_view(stmt, &values, &columns, &has_row),
        "step row view");
  if (!has_row) {
    break;
  }

  if (columns >= 2 && values[1].tag == DDB_VALUE_TEXT) {
    printf("name=%.*s\n", (int)values[1].len,
           (const char *)values[1].data);
  }
}

check(ddb_stmt_free(&stmt), "free stmt");
```

Borrowed row-view pointers are valid until the next DecentDB call that mutates
or advances the same statement.

The ABI also includes specialized fast paths for common benchmark and binding
shapes:

- `ddb_stmt_bind_int64_step_row_view`
- `ddb_stmt_bind_int64_step_i64_text_f64`
- `ddb_stmt_fetch_row_views`
- `ddb_stmt_fetch_rows_i64_text_f64`

## Transactions

Explicit transactions are available through database-handle functions:

```c
uint64_t lsn = 0;

check(ddb_db_begin_transaction(db), "begin");
check(ddb_db_execute(db,
                     "INSERT INTO users (id, name) VALUES (2, 'Grace')",
                     NULL, 0, &result),
      "insert");
check(ddb_result_free(&result), "free insert");
check(ddb_db_commit_transaction(db, &lsn), "commit");
```

Use `ddb_db_rollback_transaction` to discard an active transaction and
`ddb_db_in_transaction` to inspect transaction state.

## Metadata And Maintenance

The C ABI exposes JSON-returning helpers for schema and storage metadata:

- `ddb_db_list_tables_json`
- `ddb_db_describe_table_json`
- `ddb_db_get_table_ddl`
- `ddb_db_list_indexes_json`
- `ddb_db_list_views_json`
- `ddb_db_get_view_ddl`
- `ddb_db_list_triggers_json`
- `ddb_db_get_schema_snapshot_json`
- `ddb_db_get_tooling_metadata_json`
- `ddb_db_describe_query_json`
- `ddb_db_inspect_storage_state_json`

Each successful string-returning call transfers ownership of a `char *` to the
caller:

```c
char *json = NULL;
check(ddb_db_list_tables_json(db, &json), "list tables");
puts(json);
check(ddb_string_free(&json), "free json");
```

`ddb_db_get_tooling_metadata_json` returns the stable schema/tooling contract:
engine version, format version, schema cookies, deterministic schema
fingerprint, rich schema snapshot, native type metadata, and capability flags.

`ddb_db_describe_query_json` parses and analyzes SQL without executing it:

```c
char *contract = NULL;
check(ddb_db_describe_query_json(db,
                                 "SELECT id, email FROM users WHERE id = $1",
                                 &contract),
      "describe query");
puts(contract);
check(ddb_string_free(&contract), "free contract");
```

Maintenance helpers:

- `ddb_db_checkpoint`
- `ddb_db_save_as`
- `ddb_evict_shared_wal`

## Local-First Sync JSON Bridge

The C ABI exposes sync operations through a compact JSON bridge:

```c
char *response = NULL;
check(ddb_db_sync_execute_json(db,
                               "{\"op\":\"status\"}",
                               &response),
      "sync status");
puts(response);
check(ddb_string_free(&response), "free sync response");
```

The higher-level sync command set is documented in
[Local-first sync](../user-guide/sync/index.md) and
[CLI Reference](cli-reference.md#sync-commands).

Production changesets also have dedicated C ABI JSON entry points:

```c
char *changeset = NULL;
check(ddb_sync_changeset_create_json(
          db,
          "{\"source\":{\"kind\":\"checkpoint\",\"peer\":\"relay\",\"since_sequence\":0}}",
          &changeset),
      "create changeset");
puts(changeset);
check(ddb_string_free(&changeset), "free changeset");
```

Available functions:

- `ddb_sync_changeset_create_json`
- `ddb_sync_changeset_apply_json`
- `ddb_sync_changeset_inspect_json`
- `ddb_sync_changeset_invert_json`

Each function returns an owned JSON string that must be freed with
`ddb_string_free`.

## Branch Workflow JSON Bridge

The C ABI also exposes snapshot, branch, diff, restore, and merge workflows
through a JSON bridge:

```c
char *response = NULL;
check(ddb_db_branch_execute_json(
          db,
          "{\"op\":\"branch_create\",\"name\":\"work\",\"from\":\"main\"}",
          &response),
      "create branch");
puts(response);
check(ddb_string_free(&response), "free branch response");
```

Supported `op` values are:

- `snapshot_create`, `snapshot_list`, `snapshot_delete`
- `branch_create`, `branch_list`, `branch_delete`, `branch_rename`
- `branch_commit`, `branch_log`, `branch_diff`
- `branch_restore`
- `branch_merge`

See [Branching, Diff, Restore, And Time Travel](../user-guide/branching.md)
and [CLI Reference](cli-reference.md#branch) for command semantics and safety
rules.

## C++ Usage

C++ code can include `decentdb.h` directly:

```cpp
#include "decentdb.h"

#include <stdexcept>
#include <string>

class DbHandle {
public:
  explicit DbHandle(const char *path) {
    ddb_status_t status = ddb_db_open_or_create(path, &db_);
    if (status != DDB_OK) {
      const char *msg = ddb_last_error_message();
      throw std::runtime_error(msg == nullptr ? "DecentDB open failed" : msg);
    }
  }

  DbHandle(const DbHandle &) = delete;
  DbHandle &operator=(const DbHandle &) = delete;

  ~DbHandle() {
    if (db_ != nullptr) {
      ddb_db_free(&db_);
    }
  }

  ddb_db_t *get() const { return db_; }

private:
  ddb_db_t *db_ = nullptr;
};
```

This pattern is a convenience wrapper around the C ABI. The stable public
contract remains `include/decentdb.h`.

## Validation

Run the C binding smoke test from the repository root:

```bash
cargo build -p decentdb
bash tests/bindings/c/run.sh
```

The release workflow also runs this smoke path. The nightly memory-safety
workflow builds and runs the C smoke and memory churn programs under Valgrind
where available.

## Current Limits

- There is no separate C++ package or object-oriented C++ API.
- Open-with-config options such as cache size are not currently exposed through
  the C ABI open functions.
- The C ABI is intentionally lower level than the .NET, Go, Python, Node,
  Dart, and JDBC bindings.
- Dot commands from `decentdb repl` are CLI behavior, not C ABI behavior.

## Related Pages

- [Binding Compatibility Matrix](bindings-matrix.md)
- [Rust API](rust-api.md)
- [CLI Reference](cli-reference.md)
- [Local-first sync](../user-guide/sync/index.md)
