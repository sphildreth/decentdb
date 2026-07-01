#ifndef DECENTDB_H
#define DECENTDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t ddb_status_t;

#define DDB_ABI_VERSION 7u

enum {
  DDB_OK = 0,
  DDB_ERR_IO = 1,
  DDB_ERR_CORRUPTION = 2,
  DDB_ERR_CONSTRAINT = 3,
  DDB_ERR_TRANSACTION = 4,
  DDB_ERR_SQL = 5,
  DDB_ERR_INTERNAL = 6,
  DDB_ERR_PANIC = 7,
  DDB_ERR_UNSUPPORTED_FORMAT_VERSION = 8,
  DDB_ERR_BUSY = 9,
  DDB_ERR_TIMEOUT = 10,
  DDB_ERR_CANCELED = 11,
  DDB_ERR_QUEUE_FULL = 12,
  DDB_ERR_QUEUE_CLOSED = 13
};

enum {
  DDB_WRITE_QUEUE_TIMEOUT_DEFAULT = UINT64_MAX
};

typedef struct ddb_db_handle ddb_db_t;
typedef struct ddb_result_handle ddb_result_t;
typedef struct ddb_stmt_handle ddb_stmt_t;
typedef struct ddb_watch_handle ddb_watch_t;

typedef enum ddb_value_tag_t {
  DDB_VALUE_NULL = 0,
  DDB_VALUE_INT64 = 1,
  DDB_VALUE_FLOAT64 = 2,
  DDB_VALUE_BOOL = 3,
  DDB_VALUE_TEXT = 4,
  DDB_VALUE_BLOB = 5,
  DDB_VALUE_DECIMAL = 6,
  DDB_VALUE_UUID = 7,
  DDB_VALUE_TIMESTAMP_MICROS = 8,
  DDB_VALUE_GEOMETRY = 9,
  DDB_VALUE_GEOGRAPHY = 10,
  DDB_VALUE_ENUM = 11,
  DDB_VALUE_IPADDR = 12,
  DDB_VALUE_CIDR = 13,
  DDB_VALUE_DATE = 14,
  DDB_VALUE_TIME = 15,
  DDB_VALUE_TIMESTAMPTZ_MICROS = 16,
  DDB_VALUE_INTERVAL = 17,
  DDB_VALUE_MACADDR = 18
} ddb_value_tag_t;

typedef struct ddb_value_t {
  uint32_t tag;
  uint8_t bool_value;
  uint8_t reserved0[7];
  int64_t int64_value;
  double float64_value;
  int64_t decimal_scaled;
  uint8_t decimal_scale;
  uint8_t reserved1[7];
  uint8_t *data;
  size_t len;
  uint8_t uuid_bytes[16];
  int64_t timestamp_micros;
  uint64_t enum_type_id;
  uint64_t enum_label_id;
  uint8_t ip_family;
  uint8_t cidr_prefix_len;
  uint8_t reserved2[6];
  uint8_t ip_cidr_addr_bytes[16];
  int32_t date_days;
  int64_t time_micros;
  int64_t timestamptz_micros;
  int32_t interval_months;
  int32_t interval_days;
  int64_t interval_micros;
} ddb_value_t;

typedef struct ddb_write_queue_metrics_t {
  size_t capacity;
  size_t current_depth;
  uint64_t admitted;
  uint64_t rejected;
  uint64_t timed_out;
  uint64_t canceled;
  uint64_t executed;
  uint64_t committed;
  uint64_t failed;
  uint64_t group_commit_batches;
  uint64_t group_commit_syncs;
  uint64_t group_commit_max_batch;
  uint64_t group_commit_commits_covered;
  uint64_t physical_syncs_saved;
  uint64_t total_queue_wait_ns;
} ddb_write_queue_metrics_t;

typedef struct ddb_value_view_t {
  uint32_t tag;
  uint8_t bool_value;
  uint8_t reserved0[7];
  int64_t int64_value;
  double float64_value;
  int64_t decimal_scaled;
  uint8_t decimal_scale;
  uint8_t reserved1[7];
  const uint8_t *data;
  size_t len;
  uint8_t uuid_bytes[16];
  int64_t timestamp_micros;
  uint64_t enum_type_id;
  uint64_t enum_label_id;
  uint8_t ip_family;
  uint8_t cidr_prefix_len;
  uint8_t reserved2[6];
  uint8_t ip_cidr_addr_bytes[16];
  int32_t date_days;
  int64_t time_micros;
  int64_t timestamptz_micros;
  int32_t interval_months;
  int32_t interval_days;
  int64_t interval_micros;
} ddb_value_view_t;

typedef struct ddb_row_i64_text_f64_view_t {
  int64_t int64_value;
  const uint8_t *text_data;
  size_t text_len;
  double float64_value;
} ddb_row_i64_text_f64_view_t;

/* Borrowed pointer valid until the next DecentDB call on the same thread. */
uint32_t ddb_abi_version(void);
const char *ddb_version(void);
const char *ddb_last_error_message(void);
/*
 * Returns owned JSON for the most recent DecentDB error on this thread.
 * On success, `*out_json` is either NULL when there is no last error or a
 * UTF-8 JSON string that must be freed with ddb_string_free.
 * Calling this accessor does not clear or replace ddb_last_error_message().
 */
ddb_status_t ddb_last_error_json(char **out_json);

/*
 * Initializes an owned value slot for use with ddb_*_value_copy APIs.
 * Call ddb_value_dispose when done with any initialized value slot.
 */
ddb_status_t ddb_value_init(ddb_value_t *value);
ddb_status_t ddb_value_dispose(ddb_value_t *value);

/*
 * Frees a string previously returned by this API.
 * Call ddb_string_free exactly once for each successful string-returning call.
 * Failing to free the string leaks that allocation until process exit.
 * Do not call ddb_string_free concurrently from multiple threads on the same pointer.
 */
ddb_status_t ddb_string_free(char **value);

/*
 * On success, ownership of the returned database handle transfers to the caller.
 * Call ddb_db_free exactly once for each successful create/open/open_or_create call.
 * The handle retains references to internal database state; failing to free it leaks that state
 * until process exit.
 * Do not call ddb_db_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_db_create(const char *path, ddb_db_t **out_db);
ddb_status_t ddb_db_open(const char *path, ddb_db_t **out_db);
ddb_status_t ddb_db_open_or_create(const char *path, ddb_db_t **out_db);
/*
 * Option-aware open variants. `options` is a UTF-8 key=value list separated
 * by whitespace, commas, or semicolons. Supported keys include profile,
 * cache_size, retain_paged_row_sources_after_commit, paged_row_storage,
 * persistent_pk_index, wal_autocheckpoint, wal_checkpoint_threshold_pages,
 * wal_checkpoint_threshold_bytes, process_coordination,
 * process_coordination_timeout_ms, write_queue_enabled, write_queue_capacity,
 * write_queue_default_timeout_ms, write_queue_strict_group_commit,
 * write_queue_max_batch, write_queue_max_group_delay_us, plan_cache_enabled,
 * plan_cache_max_bytes, encryption_key, and encryption_key_hex.
 */
ddb_status_t ddb_db_create_with_options(const char *path, const char *options, ddb_db_t **out_db);
ddb_status_t ddb_db_open_with_options(const char *path, const char *options, ddb_db_t **out_db);
ddb_status_t ddb_db_open_or_create_with_options(const char *path, const char *options, ddb_db_t **out_db);
ddb_status_t ddb_db_sync_execute_json(ddb_db_t *db, const char *request_json, char **out_json);
ddb_status_t ddb_db_branch_execute_json(ddb_db_t *db, const char *request_json, char **out_json);
/*
 * Executes SQL against a branch (or "main"), with positional parameters.
 *
 * On success, ownership of the returned result handle transfers to the caller.
 */
ddb_status_t ddb_db_execute_on_branch(
    ddb_db_t *db,
    const char *branch_name,
    const char *sql,
    const ddb_value_t *params,
    size_t params_len,
    ddb_result_t **out_result
);
ddb_status_t ddb_sync_changeset_create_json(ddb_db_t *db, const char *request_json, char **out_json);
ddb_status_t ddb_sync_changeset_apply_json(ddb_db_t *db, const char *request_json, char **out_json);
ddb_status_t ddb_sync_changeset_inspect_json(ddb_db_t *db, const char *request_json, char **out_json);
ddb_status_t ddb_sync_changeset_invert_json(ddb_db_t *db, const char *request_json, char **out_json);

/*
 * Frees a database handle returned by ddb_db_create, ddb_db_open, or ddb_db_open_or_create.
 * Call ddb_db_free exactly once for each successful handle-creating call.
 * Failing to free the handle leaks internal database state until process exit.
 * Do not call ddb_db_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_db_free(ddb_db_t **db);
ddb_status_t ddb_db_set_audit_context_text(
    ddb_db_t *db,
    const char *key,
    const char *value,
    size_t value_len
);
ddb_status_t ddb_db_clear_audit_context(ddb_db_t *db, const char *key);

/* Plan cache diagnostics (F023 / ADR 0193). */

typedef struct ddb_plan_cache_summary {
    /* Static engine-owned string. Do not pass this pointer to ddb_string_free. */
    const char *scope;
    uint64_t total_entries;
    uint64_t total_hits;
    uint64_t total_misses;
    uint64_t total_evictions;
    uint64_t total_size_bytes;
    uint64_t max_size_bytes;
    uint64_t total_oversized_refusals;
    double hit_rate;
} ddb_plan_cache_summary_t;

ddb_status_t ddb_plan_cache_summary(
    ddb_db_t *db,
    ddb_plan_cache_summary_t *out_summary
);
ddb_status_t ddb_plan_cache_flush(ddb_db_t *db);

/*
 * On success, ownership of the returned statement handle transfers to the caller.
 * Call ddb_stmt_free exactly once for each successful ddb_db_prepare call.
 * The handle retains references to internal database state; failing to free it leaks that state
 * until process exit.
 * Do not call ddb_stmt_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_db_prepare(ddb_db_t *db, const char *sql, ddb_stmt_t **out_stmt);

/*
 * Frees a statement handle returned by ddb_db_prepare.
 * Call ddb_stmt_free exactly once for each successful ddb_db_prepare call.
 * Failing to free the handle leaks retained database state until process exit.
 * Do not call ddb_stmt_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_stmt_free(ddb_stmt_t **stmt);
ddb_status_t ddb_stmt_reset(ddb_stmt_t *stmt);
ddb_status_t ddb_stmt_clear_bindings(ddb_stmt_t *stmt);
ddb_status_t ddb_stmt_bind_null(ddb_stmt_t *stmt, size_t index_1_based);
ddb_status_t ddb_stmt_bind_int64(ddb_stmt_t *stmt, size_t index_1_based, int64_t value);
ddb_status_t ddb_stmt_bind_int64_step_row_view(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    int64_t value,
    const ddb_value_view_t **out_values,
    size_t *out_columns,
    uint8_t *out_has_row);
ddb_status_t ddb_stmt_bind_text_step_row_view(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    const char *value,
    size_t byte_len,
    const ddb_value_view_t **out_values,
    size_t *out_columns,
    uint8_t *out_has_row);
ddb_status_t ddb_stmt_bind_int64_step_i64_text_f64(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    int64_t value,
    int64_t *out_int64,
    const uint8_t **out_text_data,
    size_t *out_text_len,
    double *out_float64,
    uint8_t *out_has_row);
ddb_status_t ddb_stmt_bind_float64(ddb_stmt_t *stmt, size_t index_1_based, double value);
ddb_status_t ddb_stmt_bind_bool(ddb_stmt_t *stmt, size_t index_1_based, uint8_t value);
ddb_status_t ddb_stmt_bind_text(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    const char *value,
    size_t byte_len);
ddb_status_t ddb_stmt_bind_blob(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    const uint8_t *data,
    size_t byte_len);
ddb_status_t ddb_stmt_bind_geometry_wkb(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    const uint8_t *data,
    size_t byte_len);
ddb_status_t ddb_stmt_bind_geography_wkb(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    const uint8_t *data,
    size_t byte_len);
ddb_status_t ddb_stmt_bind_uuid(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    const uint8_t uuid_bytes[16]);
ddb_status_t ddb_stmt_bind_decimal(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    int64_t scaled,
    uint8_t scale);
ddb_status_t ddb_stmt_bind_timestamp_micros(
    ddb_stmt_t *stmt,
    size_t index_1_based,
    int64_t timestamp_micros);
ddb_status_t ddb_stmt_execute_batch_i64(
    ddb_stmt_t *stmt,
    size_t row_count,
    const int64_t *values_i64,
    uint64_t *out_total_affected_rows);
ddb_status_t ddb_stmt_execute_batch_i64_text_f64(
    ddb_stmt_t *stmt,
    size_t row_count,
    const int64_t *values_i64,
    const char *const *values_text_ptrs,
    const size_t *values_text_lens,
    const double *values_f64,
    uint64_t *out_total_affected_rows);
ddb_status_t ddb_stmt_execute_batch_typed(
    ddb_stmt_t *stmt,
    size_t row_count,
    const char *signature, /* 'i'=INT64, 'b'=BOOLEAN, 'f'=FLOAT64, 't'=TEXT */
    const int64_t *values_i64, /* INT64 plus BOOLEAN slots; BOOLEAN uses 0/non-zero */
    const double *values_f64,
    const char *const *values_text_ptrs,
    const size_t *values_text_lens,
    uint64_t *out_total_affected_rows);
ddb_status_t ddb_stmt_step(ddb_stmt_t *stmt, uint8_t *out_has_row);
ddb_status_t ddb_stmt_column_count(ddb_stmt_t *stmt, size_t *out_columns);
ddb_status_t ddb_stmt_column_name_copy(
    ddb_stmt_t *stmt,
    size_t column_index,
    char **out_name);
ddb_status_t ddb_stmt_affected_rows(ddb_stmt_t *stmt, uint64_t *out_rows);
ddb_status_t ddb_stmt_rebind_int64_execute(
    ddb_stmt_t *stmt,
    int64_t value,
    uint64_t *out_affected);
ddb_status_t ddb_stmt_rebind_text_int64_execute(
    ddb_stmt_t *stmt,
    const char *text_value,
    size_t text_len,
    int64_t int_value,
    uint64_t *out_affected);
ddb_status_t ddb_stmt_rebind_int64_text_execute(
    ddb_stmt_t *stmt,
    int64_t int_value,
    const char *text_value,
    size_t text_len,
    uint64_t *out_affected);
/*
 * Performance note: for read-heavy workloads, prefer ddb_stmt_*_row_view
 * functions, which return borrowed pointers into the result set without heap
 * allocation. Use ddb_*_value_copy functions when ownership transfer is
 * required.
 *
 * Initialize out_value with ddb_value_init before first use, then call
 * ddb_value_dispose when done. Reusing the same initialized ddb_value_t for
 * multiple value-copy calls is supported; previous owned cell storage is
 * released before the new value is written.
 */
ddb_status_t ddb_stmt_value_copy(
    ddb_stmt_t *stmt,
    size_t column_index,
    ddb_value_t *out_value);
ddb_status_t ddb_stmt_row_view(
    ddb_stmt_t *stmt,
    const ddb_value_view_t **out_values,
    size_t *out_columns);
ddb_status_t ddb_stmt_step_row_view(
    ddb_stmt_t *stmt,
    const ddb_value_view_t **out_values,
    size_t *out_columns,
    uint8_t *out_has_row);
ddb_status_t ddb_stmt_fetch_row_views(
    ddb_stmt_t *stmt,
    uint8_t include_current_row,
    size_t max_rows,
    const ddb_value_view_t **out_values,
    size_t *out_rows,
    size_t *out_columns);
ddb_status_t ddb_stmt_fetch_rows_i64_text_f64(
    ddb_stmt_t *stmt,
    uint8_t include_current_row,
    size_t max_rows,
    const ddb_row_i64_text_f64_view_t **out_rows_ptr,
    size_t *out_rows);

/*
 * On success, ownership of the returned result handle transfers to the caller.
 * Call ddb_result_free exactly once for each successful ddb_db_execute call.
 * The handle retains references to internal database state; failing to free it leaks that state
 * until process exit.
 * Do not call ddb_result_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_db_execute(
    ddb_db_t *db,
    const char *sql,
    const ddb_value_t *params,
    size_t params_len,
    ddb_result_t **out_result);

/*
 * Executes one SQL statement through the engine-owned write queue.
 * Pass DDB_WRITE_QUEUE_TIMEOUT_DEFAULT to use the database configured default.
 * Pass 0 for immediate timeout behavior. On success, ownership of the returned
 * result handle transfers to the caller.
 */
ddb_status_t ddb_db_execute_queued(
    ddb_db_t *db,
    const char *sql,
    const ddb_value_t *params,
    size_t params_len,
    uint64_t timeout_ms,
    ddb_result_t **out_result);

ddb_status_t ddb_db_write_queue_metrics(
    ddb_db_t *db,
    ddb_write_queue_metrics_t *out_metrics);

ddb_status_t ddb_db_watch_table_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_db_watch_range_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_db_watch_query_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_db_change_stream_json(
    ddb_db_t *db,
    const char *request_json,
    ddb_watch_t **out_watch);

ddb_status_t ddb_watch_next_json(
    ddb_watch_t *watch,
    uint32_t timeout_ms,
    char **out_json);

ddb_status_t ddb_watch_close(ddb_watch_t **watch);

ddb_status_t ddb_db_checkpoint(ddb_db_t *db);
ddb_status_t ddb_db_begin_transaction(ddb_db_t *db);
ddb_status_t ddb_db_commit_transaction(ddb_db_t *db, uint64_t *out_lsn);
ddb_status_t ddb_db_rollback_transaction(ddb_db_t *db);
ddb_status_t ddb_db_in_transaction(ddb_db_t *db, uint8_t *out_flag);
ddb_status_t ddb_db_save_as(ddb_db_t *db, const char *dest_path);
ddb_status_t ddb_db_list_tables_json(ddb_db_t *db, char **out_json);
ddb_status_t ddb_db_describe_table_json(ddb_db_t *db, const char *name, char **out_json);
ddb_status_t ddb_db_get_table_ddl(ddb_db_t *db, const char *name, char **out_ddl);
ddb_status_t ddb_db_list_indexes_json(ddb_db_t *db, char **out_json);
ddb_status_t ddb_db_list_views_json(ddb_db_t *db, char **out_json);
ddb_status_t ddb_db_get_view_ddl(ddb_db_t *db, const char *name, char **out_ddl);
ddb_status_t ddb_db_list_triggers_json(ddb_db_t *db, char **out_json);
ddb_status_t ddb_db_get_schema_snapshot_json(ddb_db_t *db, char **out_json);
ddb_status_t ddb_db_get_tooling_metadata_json(ddb_db_t *db, char **out_json);
ddb_status_t ddb_db_describe_query_json(ddb_db_t *db, const char *sql, char **out_json);
ddb_status_t ddb_db_inspect_storage_state_json(ddb_db_t *db, char **out_json);

/*
 * Lua extension package lifecycle JSON APIs.
 *
 * Returned JSON strings are owned by the caller and must be released with
 * ddb_string_free.
 */
ddb_status_t ddb_extension_validate_json(const char *request_json, char **out_json);
ddb_status_t ddb_extension_install_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);
ddb_status_t ddb_extension_enable_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);
ddb_status_t ddb_extension_disable_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);
ddb_status_t ddb_extension_list_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);
ddb_status_t ddb_extension_dependencies_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);
ddb_status_t ddb_extension_rebuild_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);
ddb_status_t ddb_extension_purge_json(
    ddb_db_t *db,
    const char *request_json,
    char **out_json);

ddb_status_t ddb_evict_shared_wal(const char *path);

/*
 * Frees a result handle returned by ddb_db_execute.
 * Call ddb_result_free exactly once for each successful ddb_db_execute call.
 * Failing to free the handle leaks retained database state until process exit.
 * Do not call ddb_result_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_result_free(ddb_result_t **result);
ddb_status_t ddb_result_row_count(ddb_result_t *result, size_t *out_rows);
ddb_status_t ddb_result_column_count(ddb_result_t *result, size_t *out_columns);
ddb_status_t ddb_result_affected_rows(ddb_result_t *result, uint64_t *out_rows);
ddb_status_t ddb_result_column_name_copy(
    ddb_result_t *result,
    size_t column_index,
    char **out_name);
/*
 * Performance note: ddb_result_value_copy returns owned values and may allocate
 * per cell. For streaming read-heavy paths, prefer statement row-view APIs.
 *
 * Initialize out_value with ddb_value_init before first use, then call
 * ddb_value_dispose when done. Reusing the same initialized ddb_value_t for
 * multiple value-copy calls is supported; previous owned cell storage is
 * released before the new value is written.
 */
ddb_status_t ddb_result_value_copy(
    ddb_result_t *result,
    size_t row_index,
    size_t column_index,
    ddb_value_t *out_value);

/**
 * Runtime tracing snapshot.
 *
 * `kind` selects the trace view:
 *   "slow_queries", "lock_waits", "sessions",
 *   "index_usage", "doctor_findings", "fix_plan"
 *
 * On success, `out_json` receives an owned JSON string.
 * The caller must free it with `ddb_string_free`.
 */
ddb_status_t ddb_runtime_tracing_snapshot(
    ddb_db_t *db,
    const char *kind,
    char **out_json);

/**
 * Reset a specific runtime trace ring buffer.
 *
 * `kind` may be "slow_queries", "lock_waits", or "index_usage".
 */
ddb_status_t ddb_runtime_tracing_reset(
    ddb_db_t *db,
    const char *kind);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DECENTDB_H */
