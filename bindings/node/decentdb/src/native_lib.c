#include "native_lib.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
  #include <windows.h>
  static HMODULE g_lib = NULL;
  #define DL_HANDLE HMODULE
  static void* load_sym(DL_HANDLE h, const char* name) { return (void*)GetProcAddress(h, name); }
  static DL_HANDLE load_lib(const char* path) { return LoadLibraryA(path); }
#else
  #include <dlfcn.h>
  static void* g_lib = NULL;
  #define DL_HANDLE void*
  static void* load_sym(DL_HANDLE h, const char* name) { return dlsym(h, name); }
  static DL_HANDLE load_lib(const char* path) { return dlopen(path, RTLD_NOW); }
#endif

typedef ddb_status_t (*fn_abi_version_t)(void);
typedef const char* (*fn_version_t)(void);
typedef const char* (*fn_last_error_message_t)(void);
typedef ddb_status_t (*fn_string_free_t)(char** value);
typedef ddb_status_t (*fn_db_create_t)(const char* path, ddb_db_t** out_db);
typedef ddb_status_t (*fn_db_open_t)(const char* path, ddb_db_t** out_db);
typedef ddb_status_t (*fn_db_open_or_create_t)(const char* path, ddb_db_t** out_db);
typedef ddb_status_t (*fn_db_free_t)(ddb_db_t** db);
typedef ddb_status_t (*fn_db_prepare_t)(ddb_db_t* db, const char* sql, ddb_stmt_t** out_stmt);
typedef ddb_status_t (*fn_stmt_free_t)(ddb_stmt_t** stmt);
typedef ddb_status_t (*fn_stmt_reset_t)(ddb_stmt_t* stmt);
typedef ddb_status_t (*fn_stmt_clear_bindings_t)(ddb_stmt_t* stmt);
typedef ddb_status_t (*fn_stmt_bind_null_t)(ddb_stmt_t* stmt, size_t index_1_based);
typedef ddb_status_t (*fn_stmt_bind_int64_t)(ddb_stmt_t* stmt, size_t index_1_based, int64_t value);
typedef ddb_status_t (*fn_stmt_bind_float64_t)(ddb_stmt_t* stmt, size_t index_1_based, double value);
typedef ddb_status_t (*fn_stmt_bind_bool_t)(ddb_stmt_t* stmt, size_t index_1_based, uint8_t value);
typedef ddb_status_t (*fn_stmt_bind_text_t)(ddb_stmt_t* stmt, size_t index_1_based, const char* value, size_t byte_len);
typedef ddb_status_t (*fn_stmt_bind_blob_t)(ddb_stmt_t* stmt, size_t index_1_based, const uint8_t* data, size_t byte_len);
typedef ddb_status_t (*fn_stmt_bind_decimal_t)(ddb_stmt_t* stmt, size_t index_1_based, int64_t scaled, uint8_t scale);
typedef ddb_status_t (*fn_stmt_execute_batch_i64_text_f64_t)(
    ddb_stmt_t* stmt,
    size_t row_count,
    const int64_t* values_i64,
    const char* const* values_text_ptrs,
    const size_t* values_text_lens,
    const double* values_f64,
    uint64_t* out_total_affected_rows);
typedef ddb_status_t (*fn_stmt_step_t)(ddb_stmt_t* stmt, uint8_t* out_has_row);
typedef ddb_status_t (*fn_stmt_column_count_t)(ddb_stmt_t* stmt, size_t* out_columns);
typedef ddb_status_t (*fn_stmt_column_name_copy_t)(ddb_stmt_t* stmt, size_t column_index, char** out_name);
typedef ddb_status_t (*fn_stmt_affected_rows_t)(ddb_stmt_t* stmt, uint64_t* out_rows);
typedef ddb_status_t (*fn_stmt_row_view_t)(ddb_stmt_t* stmt, const ddb_value_view_t** out_values, size_t* out_columns);
typedef ddb_status_t (*fn_stmt_fetch_rows_i64_text_f64_t)(
    ddb_stmt_t* stmt,
    uint8_t include_current_row,
    size_t max_rows,
    const ddb_row_i64_text_f64_view_t** out_rows_ptr,
    size_t* out_rows);
typedef ddb_status_t (*fn_db_checkpoint_t)(ddb_db_t* db);
typedef ddb_status_t (*fn_db_begin_transaction_t)(ddb_db_t* db);
typedef ddb_status_t (*fn_db_commit_transaction_t)(ddb_db_t* db, uint64_t* out_lsn);
typedef ddb_status_t (*fn_db_rollback_transaction_t)(ddb_db_t* db);
typedef ddb_status_t (*fn_db_save_as_t)(ddb_db_t* db, const char* dest_path);
typedef ddb_status_t (*fn_db_list_tables_json_t)(ddb_db_t* db, char** out_json);
typedef ddb_status_t (*fn_db_describe_table_json_t)(ddb_db_t* db, const char* name, char** out_json);
typedef ddb_status_t (*fn_db_list_indexes_json_t)(ddb_db_t* db, char** out_json);

static struct {
  fn_abi_version_t abi_version;
  fn_version_t version;
  fn_last_error_message_t last_error_message;
  fn_string_free_t string_free;
  fn_db_create_t db_create;
  fn_db_open_t db_open;
  fn_db_open_or_create_t db_open_or_create;
  fn_db_free_t db_free;
  fn_db_prepare_t db_prepare;
  fn_stmt_free_t stmt_free;
  fn_stmt_reset_t stmt_reset;
  fn_stmt_clear_bindings_t stmt_clear_bindings;
  fn_stmt_bind_null_t stmt_bind_null;
  fn_stmt_bind_int64_t stmt_bind_int64;
  fn_stmt_bind_float64_t stmt_bind_float64;
  fn_stmt_bind_bool_t stmt_bind_bool;
  fn_stmt_bind_text_t stmt_bind_text;
  fn_stmt_bind_blob_t stmt_bind_blob;
  fn_stmt_bind_decimal_t stmt_bind_decimal;
  fn_stmt_execute_batch_i64_text_f64_t stmt_execute_batch_i64_text_f64;
  fn_stmt_step_t stmt_step;
  fn_stmt_column_count_t stmt_column_count;
  fn_stmt_column_name_copy_t stmt_column_name_copy;
  fn_stmt_affected_rows_t stmt_affected_rows;
  fn_stmt_row_view_t stmt_row_view;
  fn_stmt_fetch_rows_i64_text_f64_t stmt_fetch_rows_i64_text_f64;
  fn_db_checkpoint_t db_checkpoint;
  fn_db_begin_transaction_t db_begin_transaction;
  fn_db_commit_transaction_t db_commit_transaction;
  fn_db_rollback_transaction_t db_rollback_transaction;
  fn_db_save_as_t db_save_as;
  fn_db_list_tables_json_t db_list_tables_json;
  fn_db_describe_table_json_t db_describe_table_json;
  fn_db_list_indexes_json_t db_list_indexes_json;
} g_sym;

static decentdb_native_api g_api;
static int g_loaded = 0;
static int g_last_status = 0;
static char g_last_error[512];

static void set_last_error(const char* msg) {
  if (msg == NULL) msg = "unknown";
  strncpy(g_last_error, msg, sizeof(g_last_error) - 1);
  g_last_error[sizeof(g_last_error) - 1] = '\0';
}

const char* decentdb_native_last_load_error(void) {
  return g_last_error;
}

static int status_to_legacy_code(ddb_status_t status) {
  switch (status) {
    case DDB_OK:
      return 0;
    case DDB_ERR_IO:
      return 1;
    case DDB_ERR_CORRUPTION:
      return 2;
    case DDB_ERR_CONSTRAINT:
      return 3;
    case DDB_ERR_TRANSACTION:
      return 4;
    case DDB_ERR_SQL:
      return 5;
    case DDB_ERR_INTERNAL:
      return 6;
    case DDB_ERR_PANIC:
      return 7;
    default:
      return 6;
  }
}

static void set_status(ddb_status_t status) {
  g_last_status = status_to_legacy_code(status);
}

static const char* current_last_error_message(void) {
  if (!g_sym.last_error_message) return "";
  const char* msg = g_sym.last_error_message();
  return msg ? msg : "";
}

static int maybe_zero_length_db_file(const char* path_utf8) {
  if (!path_utf8) return 0;
  if (path_utf8[0] == '\0') return 0;
  if (strcmp(path_utf8, ":memory:") == 0) return 0;
  FILE* fp = fopen(path_utf8, "rb");
  if (!fp) return 0;
  if (fseek(fp, 0, SEEK_END) != 0) {
    fclose(fp);
    return 0;
  }
  long len = ftell(fp);
  fclose(fp);
  return len == 0;
}

static decentdb_db* wrap_open(const char* path_utf8, const char* options_utf8) {
  ddb_db_t* db = NULL;
  ddb_status_t status = DDB_ERR_INTERNAL;

  if (options_utf8 && options_utf8[0] != '\0') {
    if (strcmp(options_utf8, "mode=create") == 0) {
      status = g_sym.db_create(path_utf8, &db);
    } else if (strcmp(options_utf8, "mode=open") == 0) {
      status = g_sym.db_open(path_utf8, &db);
    } else {
      status = g_sym.db_open_or_create(path_utf8, &db);
    }
  } else {
    status = g_sym.db_open_or_create(path_utf8, &db);
  }

  if ((status != DDB_OK || db == NULL) && maybe_zero_length_db_file(path_utf8)) {
    remove(path_utf8);
    db = NULL;
    status = g_sym.db_open_or_create(path_utf8, &db);
  }

  set_status(status);
  if (status != DDB_OK || db == NULL) return NULL;
  return db;
}

static int wrap_close(decentdb_db* db) {
  ddb_db_t* dbp = db;
  ddb_status_t status = g_sym.db_free(&dbp);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_last_error_code(decentdb_db* db) {
  (void)db;
  return g_last_status;
}

static const char* wrap_last_error_message(decentdb_db* db) {
  (void)db;
  return current_last_error_message();
}

static int wrap_prepare(decentdb_db* db, const char* sql_utf8, decentdb_stmt** out_stmt) {
  ddb_status_t status = g_sym.db_prepare(db, sql_utf8, out_stmt);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_null(decentdb_stmt* stmt, int index_1_based) {
  ddb_status_t status = g_sym.stmt_bind_null(stmt, (size_t)index_1_based);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_int64(decentdb_stmt* stmt, int index_1_based, int64_t v) {
  ddb_status_t status = g_sym.stmt_bind_int64(stmt, (size_t)index_1_based, v);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_bool(decentdb_stmt* stmt, int index_1_based, int v) {
  ddb_status_t status = g_sym.stmt_bind_bool(stmt, (size_t)index_1_based, v ? 1u : 0u);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_float64(decentdb_stmt* stmt, int index_1_based, double v) {
  ddb_status_t status = g_sym.stmt_bind_float64(stmt, (size_t)index_1_based, v);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_text(decentdb_stmt* stmt, int index_1_based, const char* utf8, int byte_len) {
  size_t len = byte_len < 0 ? 0 : (size_t)byte_len;
  ddb_status_t status = g_sym.stmt_bind_text(stmt, (size_t)index_1_based, utf8, len);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_blob(decentdb_stmt* stmt, int index_1_based, const uint8_t* data, int byte_len) {
  size_t len = byte_len < 0 ? 0 : (size_t)byte_len;
  ddb_status_t status = g_sym.stmt_bind_blob(stmt, (size_t)index_1_based, data, len);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_bind_decimal(decentdb_stmt* stmt, int index_1_based, int64_t unscaled, int scale) {
  uint8_t scale_u8 = scale < 0 ? 0 : (scale > 255 ? 255 : (uint8_t)scale);
  ddb_status_t status = g_sym.stmt_bind_decimal(stmt, (size_t)index_1_based, unscaled, scale_u8);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_execute_batch_i64_text_f64(
    decentdb_stmt* stmt,
    size_t row_count,
    const int64_t* values_i64,
    const char* const* values_text_ptrs,
    const size_t* values_text_lens,
    const double* values_f64,
    uint64_t* out_total_affected_rows) {
  ddb_status_t status = g_sym.stmt_execute_batch_i64_text_f64(
      stmt,
      row_count,
      values_i64,
      values_text_ptrs,
      values_text_lens,
      values_f64,
      out_total_affected_rows);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_reset(decentdb_stmt* stmt) {
  ddb_status_t status = g_sym.stmt_reset(stmt);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_clear_bindings(decentdb_stmt* stmt) {
  ddb_status_t status = g_sym.stmt_clear_bindings(stmt);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_step(decentdb_stmt* stmt) {
  uint8_t has_row = 0;
  ddb_status_t status = g_sym.stmt_step(stmt, &has_row);
  set_status(status);
  if (status != DDB_OK) return -1;
  return has_row ? 1 : 0;
}

static int wrap_column_count(decentdb_stmt* stmt) {
  size_t columns = 0;
  ddb_status_t status = g_sym.stmt_column_count(stmt, &columns);
  set_status(status);
  if (status != DDB_OK) return -1;
  return (int)columns;
}

static const char* wrap_column_name(decentdb_stmt* stmt, int col_0_based) {
  char* out_name = NULL;
  ddb_status_t status = g_sym.stmt_column_name_copy(stmt, (size_t)col_0_based, &out_name);
  set_status(status);
  if (status != DDB_OK) return NULL;
  return out_name;
}

static int wrap_row_view(decentdb_stmt* stmt, const decentdb_value_view** out_values, int* out_count) {
  size_t columns = 0;
  ddb_status_t status = g_sym.stmt_row_view(stmt, out_values, &columns);
  set_status(status);
  if (status != DDB_OK) return -1;
  *out_count = (int)columns;
  return 0;
}

static int wrap_fetch_rows_i64_text_f64(
    decentdb_stmt* stmt,
    int include_current_row,
    size_t max_rows,
    const decentdb_row_i64_text_f64_view** out_rows_ptr,
    size_t* out_rows) {
  ddb_status_t status = g_sym.stmt_fetch_rows_i64_text_f64(
      stmt, include_current_row ? 1u : 0u, max_rows, out_rows_ptr, out_rows);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int64_t wrap_rows_affected(decentdb_stmt* stmt) {
  uint64_t rows = 0;
  ddb_status_t status = g_sym.stmt_affected_rows(stmt, &rows);
  set_status(status);
  if (status != DDB_OK) return 0;
  return (int64_t)rows;
}

static void wrap_finalize(decentdb_stmt* stmt) {
  ddb_stmt_t* stmtp = stmt;
  ddb_status_t status = g_sym.stmt_free(&stmtp);
  set_status(status);
}

static int wrap_checkpoint(decentdb_db* db) {
  ddb_status_t status = g_sym.db_checkpoint(db);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_begin_transaction(decentdb_db* db) {
  ddb_status_t status = g_sym.db_begin_transaction(db);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_commit_transaction(decentdb_db* db) {
  uint64_t out_lsn = 0;
  ddb_status_t status = g_sym.db_commit_transaction(db, &out_lsn);
  (void)out_lsn;
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_rollback_transaction(decentdb_db* db) {
  ddb_status_t status = g_sym.db_rollback_transaction(db);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static int wrap_save_as(decentdb_db* db, const char* dest_path_utf8) {
  ddb_status_t status = g_sym.db_save_as(db, dest_path_utf8);
  set_status(status);
  return status == DDB_OK ? 0 : -1;
}

static void wrap_free(void* p) {
  char* ptr = (char*)p;
  ddb_status_t status = g_sym.string_free(&ptr);
  set_status(status);
}

static const char* wrap_list_tables_json(decentdb_db* db, int* out_len) {
  char* json = NULL;
  ddb_status_t status = g_sym.db_list_tables_json(db, &json);
  set_status(status);
  if (status != DDB_OK || json == NULL) return NULL;
  *out_len = (int)strlen(json);
  return json;
}

static const char* wrap_get_table_columns_json(decentdb_db* db, const char* table_utf8, int* out_len) {
  char* json = NULL;
  ddb_status_t status = g_sym.db_describe_table_json(db, table_utf8, &json);
  set_status(status);
  if (status != DDB_OK || json == NULL) return NULL;
  *out_len = (int)strlen(json);
  return json;
}

static const char* wrap_list_indexes_json(decentdb_db* db, int* out_len) {
  char* json = NULL;
  ddb_status_t status = g_sym.db_list_indexes_json(db, &json);
  set_status(status);
  if (status != DDB_OK || json == NULL) return NULL;
  *out_len = (int)strlen(json);
  return json;
}

static int resolve_all(DL_HANDLE h) {
  memset(&g_sym, 0, sizeof(g_sym));
  memset(&g_api, 0, sizeof(g_api));

  g_sym.abi_version = (fn_abi_version_t)load_sym(h, "ddb_abi_version");
  g_sym.version = (fn_version_t)load_sym(h, "ddb_version");
  g_sym.last_error_message = (fn_last_error_message_t)load_sym(h, "ddb_last_error_message");
  g_sym.string_free = (fn_string_free_t)load_sym(h, "ddb_string_free");
  g_sym.db_create = (fn_db_create_t)load_sym(h, "ddb_db_create");
  g_sym.db_open = (fn_db_open_t)load_sym(h, "ddb_db_open");
  g_sym.db_open_or_create = (fn_db_open_or_create_t)load_sym(h, "ddb_db_open_or_create");
  g_sym.db_free = (fn_db_free_t)load_sym(h, "ddb_db_free");
  g_sym.db_prepare = (fn_db_prepare_t)load_sym(h, "ddb_db_prepare");
  g_sym.stmt_free = (fn_stmt_free_t)load_sym(h, "ddb_stmt_free");
  g_sym.stmt_reset = (fn_stmt_reset_t)load_sym(h, "ddb_stmt_reset");
  g_sym.stmt_clear_bindings = (fn_stmt_clear_bindings_t)load_sym(h, "ddb_stmt_clear_bindings");
  g_sym.stmt_bind_null = (fn_stmt_bind_null_t)load_sym(h, "ddb_stmt_bind_null");
  g_sym.stmt_bind_int64 = (fn_stmt_bind_int64_t)load_sym(h, "ddb_stmt_bind_int64");
  g_sym.stmt_bind_float64 = (fn_stmt_bind_float64_t)load_sym(h, "ddb_stmt_bind_float64");
  g_sym.stmt_bind_bool = (fn_stmt_bind_bool_t)load_sym(h, "ddb_stmt_bind_bool");
  g_sym.stmt_bind_text = (fn_stmt_bind_text_t)load_sym(h, "ddb_stmt_bind_text");
  g_sym.stmt_bind_blob = (fn_stmt_bind_blob_t)load_sym(h, "ddb_stmt_bind_blob");
  g_sym.stmt_bind_decimal = (fn_stmt_bind_decimal_t)load_sym(h, "ddb_stmt_bind_decimal");
  g_sym.stmt_execute_batch_i64_text_f64 =
      (fn_stmt_execute_batch_i64_text_f64_t)load_sym(h, "ddb_stmt_execute_batch_i64_text_f64");
  g_sym.stmt_step = (fn_stmt_step_t)load_sym(h, "ddb_stmt_step");
  g_sym.stmt_column_count = (fn_stmt_column_count_t)load_sym(h, "ddb_stmt_column_count");
  g_sym.stmt_column_name_copy = (fn_stmt_column_name_copy_t)load_sym(h, "ddb_stmt_column_name_copy");
  g_sym.stmt_affected_rows = (fn_stmt_affected_rows_t)load_sym(h, "ddb_stmt_affected_rows");
  g_sym.stmt_row_view = (fn_stmt_row_view_t)load_sym(h, "ddb_stmt_row_view");
  g_sym.stmt_fetch_rows_i64_text_f64 =
      (fn_stmt_fetch_rows_i64_text_f64_t)load_sym(h, "ddb_stmt_fetch_rows_i64_text_f64");
  g_sym.db_checkpoint = (fn_db_checkpoint_t)load_sym(h, "ddb_db_checkpoint");
  g_sym.db_begin_transaction = (fn_db_begin_transaction_t)load_sym(h, "ddb_db_begin_transaction");
  g_sym.db_commit_transaction = (fn_db_commit_transaction_t)load_sym(h, "ddb_db_commit_transaction");
  g_sym.db_rollback_transaction = (fn_db_rollback_transaction_t)load_sym(h, "ddb_db_rollback_transaction");
  g_sym.db_save_as = (fn_db_save_as_t)load_sym(h, "ddb_db_save_as");
  g_sym.db_list_tables_json = (fn_db_list_tables_json_t)load_sym(h, "ddb_db_list_tables_json");
  g_sym.db_describe_table_json = (fn_db_describe_table_json_t)load_sym(h, "ddb_db_describe_table_json");
  g_sym.db_list_indexes_json = (fn_db_list_indexes_json_t)load_sym(h, "ddb_db_list_indexes_json");

  if (!g_sym.last_error_message ||
      !g_sym.string_free ||
      !g_sym.db_create ||
      !g_sym.db_open ||
      !g_sym.db_open_or_create ||
      !g_sym.db_free ||
      !g_sym.db_prepare ||
      !g_sym.stmt_free ||
      !g_sym.stmt_reset ||
      !g_sym.stmt_clear_bindings ||
      !g_sym.stmt_bind_null ||
      !g_sym.stmt_bind_int64 ||
      !g_sym.stmt_bind_float64 ||
      !g_sym.stmt_bind_bool ||
      !g_sym.stmt_bind_text ||
      !g_sym.stmt_bind_blob ||
      !g_sym.stmt_bind_decimal ||
      !g_sym.stmt_execute_batch_i64_text_f64 ||
      !g_sym.stmt_step ||
      !g_sym.stmt_column_count ||
      !g_sym.stmt_column_name_copy ||
      !g_sym.stmt_affected_rows ||
      !g_sym.stmt_row_view ||
      !g_sym.stmt_fetch_rows_i64_text_f64 ||
      !g_sym.db_checkpoint ||
      !g_sym.db_begin_transaction ||
      !g_sym.db_commit_transaction ||
      !g_sym.db_rollback_transaction ||
      !g_sym.db_save_as ||
      !g_sym.db_list_tables_json ||
      !g_sym.db_describe_table_json ||
      !g_sym.db_list_indexes_json) {
    set_last_error("missing required ddb_* symbol(s) in DecentDB native library");
    return 0;
  }

  g_api.open = wrap_open;
  g_api.close = wrap_close;
  g_api.last_error_code = wrap_last_error_code;
  g_api.last_error_message = wrap_last_error_message;
  g_api.prepare = wrap_prepare;
  g_api.bind_null = wrap_bind_null;
  g_api.bind_int64 = wrap_bind_int64;
  g_api.bind_bool = wrap_bind_bool;
  g_api.bind_float64 = wrap_bind_float64;
  g_api.bind_text = wrap_bind_text;
  g_api.bind_blob = wrap_bind_blob;
  g_api.bind_decimal = wrap_bind_decimal;
  g_api.execute_batch_i64_text_f64 = wrap_execute_batch_i64_text_f64;
  g_api.reset = wrap_reset;
  g_api.clear_bindings = wrap_clear_bindings;
  g_api.step = wrap_step;
  g_api.column_count = wrap_column_count;
  g_api.column_name = wrap_column_name;
  g_api.row_view = wrap_row_view;
  g_api.fetch_rows_i64_text_f64 = wrap_fetch_rows_i64_text_f64;
  g_api.rows_affected = wrap_rows_affected;
  g_api.finalize = wrap_finalize;
  g_api.checkpoint = wrap_checkpoint;
  g_api.begin_transaction = wrap_begin_transaction;
  g_api.commit_transaction = wrap_commit_transaction;
  g_api.rollback_transaction = wrap_rollback_transaction;
  g_api.save_as = wrap_save_as;
  g_api.free = wrap_free;
  g_api.list_tables_json = wrap_list_tables_json;
  g_api.get_table_columns_json = wrap_get_table_columns_json;
  g_api.list_indexes_json = wrap_list_indexes_json;

  return 1;
}

const decentdb_native_api* decentdb_native_get(void) {
  if (g_loaded) return &g_api;

  set_last_error("not loaded");
  g_last_status = 0;

  const char* explicitPath = getenv("DECENTDB_NATIVE_LIB_PATH");
  const char* candidates[8];
  int n = 0;

  if (explicitPath && explicitPath[0] != '\0') {
    candidates[n++] = explicitPath;
  }

#if defined(_WIN32)
  candidates[n++] = "decentdb.dll";
#elif defined(__APPLE__)
  candidates[n++] = "libdecentdb.dylib";
  candidates[n++] = "decentdb.dylib";
#else
  candidates[n++] = "libdecentdb.so";
  candidates[n++] = "decentdb.so";
#endif

  for (int i = 0; i < n; i++) {
    DL_HANDLE h = load_lib(candidates[i]);
    if (!h) {
#if !defined(_WIN32)
      const char* err = dlerror();
      if (err && err[0] != '\0') set_last_error(err);
#else
      set_last_error("LoadLibrary failed");
#endif
      continue;
    }

    g_lib = h;
    if (!resolve_all(h)) {
      return NULL;
    }

    g_loaded = 1;
    set_last_error("");
    return &g_api;
  }

  return NULL;
}
