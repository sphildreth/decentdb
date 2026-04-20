#ifndef DECENTDB_H
#define DECENTDB_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint32_t ddb_status_t;

enum {
  DDB_OK = 0,
  DDB_ERR_IO = 1,
  DDB_ERR_CORRUPTION = 2,
  DDB_ERR_CONSTRAINT = 3,
  DDB_ERR_TRANSACTION = 4,
  DDB_ERR_SQL = 5,
  DDB_ERR_INTERNAL = 6,
  DDB_ERR_PANIC = 7,
  DDB_ERR_UNSUPPORTED_FORMAT_VERSION = 8
};

typedef struct ddb_db_handle ddb_db_t;
typedef struct ddb_result_handle ddb_result_t;
typedef struct ddb_stmt_handle ddb_stmt_t;

typedef enum ddb_value_tag_t {
  DDB_VALUE_NULL = 0,
  DDB_VALUE_INT64 = 1,
  DDB_VALUE_FLOAT64 = 2,
  DDB_VALUE_BOOL = 3,
  DDB_VALUE_TEXT = 4,
  DDB_VALUE_BLOB = 5,
  DDB_VALUE_DECIMAL = 6,
  DDB_VALUE_UUID = 7,
  DDB_VALUE_TIMESTAMP_MICROS = 8
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
} ddb_value_t;

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
 * Frees a database handle returned by ddb_db_create, ddb_db_open, or ddb_db_open_or_create.
 * Call ddb_db_free exactly once for each successful handle-creating call.
 * Failing to free the handle leaks internal database state until process exit.
 * Do not call ddb_db_free concurrently from multiple threads on the same handle.
 */
ddb_status_t ddb_db_free(ddb_db_t **db);

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
    const char *signature,
    const int64_t *values_i64,
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
ddb_status_t ddb_db_inspect_storage_state_json(ddb_db_t *db, char **out_json);

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
ddb_status_t ddb_result_value_copy(
    ddb_result_t *result,
    size_t row_index,
    size_t column_index,
    ddb_value_t *out_value);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DECENTDB_H */
