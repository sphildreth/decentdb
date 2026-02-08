#ifndef DECENTDB_H
#define DECENTDB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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
int decentdb_bind_bool(decentdb_stmt* stmt, int index_1_based, int v);
int decentdb_bind_float64(decentdb_stmt* stmt, int index_1_based, double v);
int decentdb_bind_text(decentdb_stmt* stmt, int index_1_based, const char* utf8, int byte_len);
int decentdb_bind_blob(decentdb_stmt* stmt, int index_1_based, const uint8_t* data, int byte_len);

// Statement reuse
int decentdb_reset(decentdb_stmt* stmt);
int decentdb_clear_bindings(decentdb_stmt* stmt);

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

// Row view (performance extension): borrowed until next step/reset/finalize.
typedef struct decentdb_value_view {
	int kind;
	int is_null;
	int64_t int64_val;
	double float64_val;
	const uint8_t* bytes;
	int bytes_len;
	int decimal_scale;
} decentdb_value_view;

int decentdb_row_view(decentdb_stmt* stmt, const decentdb_value_view** out_values, int* out_count);

// Decimal support
int decentdb_bind_decimal(decentdb_stmt* stmt, int index_1_based, int64_t unscaled, int scale);
int64_t decentdb_column_decimal_unscaled(decentdb_stmt* stmt, int col_0_based);
int decentdb_column_decimal_scale(decentdb_stmt* stmt, int col_0_based);

// Convenience API for high-overhead FFI layers (e.g. Python/ctypes):
// reset + clear bindings + bind params (from value_view array) + step once + row_view.
// Returns 0 on success, -1 on error. `out_has_row` is set to 1 if a row is available.
int decentdb_step_with_params_row_view(
	decentdb_stmt* stmt,
	const decentdb_value_view* in_params,
	int in_count,
	const decentdb_value_view** out_values,
	int* out_count,
	int* out_has_row
);

int64_t decentdb_rows_affected(decentdb_stmt* stmt);
void decentdb_finalize(decentdb_stmt* stmt);

// Checkpoint (flush WAL to main database file)
int decentdb_checkpoint(decentdb_db* db);

// Memory management for API-allocated buffers
void decentdb_free(void* p);

// Schema introspection (JSON payloads; caller frees with decentdb_free)
const char* decentdb_list_tables_json(decentdb_db* db, int* out_len);
const char* decentdb_get_table_columns_json(decentdb_db* db, const char* table_utf8, int* out_len);
const char* decentdb_list_indexes_json(decentdb_db* db, int* out_len);

#ifdef __cplusplus
}
#endif

#endif // DECENTDB_H
