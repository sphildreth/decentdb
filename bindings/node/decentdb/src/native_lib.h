#ifndef DECENTDB_NODE_NATIVE_LIB_H
#define DECENTDB_NODE_NATIVE_LIB_H

#include <stddef.h>
#include <stdint.h>

#include "../../../../include/decentdb.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef ddb_db_t decentdb_db;
typedef ddb_stmt_t decentdb_stmt;
typedef ddb_value_view_t decentdb_value_view;
typedef ddb_row_i64_text_f64_view_t decentdb_row_i64_text_f64_view;

typedef struct decentdb_native_api {
  decentdb_db* (*open)(const char* path_utf8, const char* options_utf8);
  int (*close)(decentdb_db* db);

  int (*last_error_code)(decentdb_db* db);
  const char* (*last_error_message)(decentdb_db* db);

  int (*prepare)(decentdb_db* db, const char* sql_utf8, decentdb_stmt** out_stmt);

  int (*bind_null)(decentdb_stmt* stmt, int index_1_based);
  int (*bind_int64)(decentdb_stmt* stmt, int index_1_based, int64_t v);
  int (*bind_bool)(decentdb_stmt* stmt, int index_1_based, int v);
  int (*bind_float64)(decentdb_stmt* stmt, int index_1_based, double v);
  int (*bind_text)(decentdb_stmt* stmt, int index_1_based, const char* utf8, int byte_len);
  int (*bind_blob)(decentdb_stmt* stmt, int index_1_based, const uint8_t* data, int byte_len);
  int (*bind_decimal)(decentdb_stmt* stmt, int index_1_based, int64_t unscaled, int scale);
  int (*execute_batch_i64_text_f64)(
      decentdb_stmt* stmt,
      size_t row_count,
      const int64_t* values_i64,
      const char* const* values_text_ptrs,
      const size_t* values_text_lens,
      const double* values_f64,
      uint64_t* out_total_affected_rows);

  int (*reset)(decentdb_stmt* stmt);
  int (*clear_bindings)(decentdb_stmt* stmt);

  int (*step)(decentdb_stmt* stmt);

  int (*column_count)(decentdb_stmt* stmt);
  const char* (*column_name)(decentdb_stmt* stmt, int col_0_based);

  int (*row_view)(decentdb_stmt* stmt, const decentdb_value_view** out_values, int* out_count);
  int (*fetch_rows_i64_text_f64)(
      decentdb_stmt* stmt,
      int include_current_row,
      size_t max_rows,
      const decentdb_row_i64_text_f64_view** out_rows_ptr,
      size_t* out_rows);
  int64_t (*rows_affected)(decentdb_stmt* stmt);
  void (*finalize)(decentdb_stmt* stmt);

  // Checkpoint
  int (*checkpoint)(decentdb_db* db);
  int (*begin_transaction)(decentdb_db* db);
  int (*commit_transaction)(decentdb_db* db);
  int (*rollback_transaction)(decentdb_db* db);

  // SaveAs (export database to a new on-disk file)
  int (*save_as)(decentdb_db* db, const char* dest_path_utf8);

  // Memory management
  void (*free)(void* p);

  // Schema introspection (JSON)
  const char* (*list_tables_json)(decentdb_db* db, int* out_len);
  const char* (*get_table_columns_json)(decentdb_db* db, const char* table_utf8, int* out_len);
  const char* (*list_indexes_json)(decentdb_db* db, int* out_len);
} decentdb_native_api;

// Loads the native library once and returns its resolved symbol table.
// Returns NULL on failure; call decentdb_native_last_load_error() for details.
const decentdb_native_api* decentdb_native_get(void);

// Returns a thread-local-ish static error string for the last load failure.
// Pointer remains valid until the next load attempt.
const char* decentdb_native_last_load_error(void);

#ifdef __cplusplus
}
#endif

#endif
