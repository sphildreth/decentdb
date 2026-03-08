#ifndef DECENTDB_H
#define DECENTDB_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// --------------------------------------------------------------------------
// ABI versioning
// --------------------------------------------------------------------------

// Returns the ABI version number. Callers should check at load time.
int decentdb_abi_version(void);

// Returns the engine version string (e.g. "1.6.0"). Static; do NOT free.
const char* decentdb_engine_version(void);

// --------------------------------------------------------------------------
// Opaque handles
// --------------------------------------------------------------------------
typedef struct decentdb_db decentdb_db;
typedef struct decentdb_stmt decentdb_stmt;

// --------------------------------------------------------------------------
// Database lifecycle
// --------------------------------------------------------------------------
decentdb_db* decentdb_open(const char* path_utf8, const char* options_utf8);
int decentdb_close(decentdb_db* db);

// --------------------------------------------------------------------------
// Error reporting
// --------------------------------------------------------------------------
// Error codes (internal ErrorCode + 1): 0=OK, 1=IO, 2=CORRUPTION,
// 3=CONSTRAINT, 4=TRANSACTION, 5=SQL, 6=INTERNAL
int decentdb_last_error_code(decentdb_db* db);
const char* decentdb_last_error_message(decentdb_db* db);

// --------------------------------------------------------------------------
// Transaction control
// --------------------------------------------------------------------------
int decentdb_begin(decentdb_db* db);
int decentdb_commit(decentdb_db* db);
int decentdb_rollback(decentdb_db* db);

// --------------------------------------------------------------------------
// Prepared/streaming statements
// --------------------------------------------------------------------------
int decentdb_prepare(decentdb_db* db, const char* sql_utf8, decentdb_stmt** out_stmt);

// Bind parameters: 1-based indexes match $1..$N
int decentdb_bind_null(decentdb_stmt* stmt, int index_1_based);
int decentdb_bind_int64(decentdb_stmt* stmt, int index_1_based, int64_t v);
int decentdb_bind_bool(decentdb_stmt* stmt, int index_1_based, int v);
int decentdb_bind_float64(decentdb_stmt* stmt, int index_1_based, double v);
int decentdb_bind_text(decentdb_stmt* stmt, int index_1_based, const char* utf8, int byte_len);
int decentdb_bind_blob(decentdb_stmt* stmt, int index_1_based, const uint8_t* data, int byte_len);
int decentdb_bind_decimal(decentdb_stmt* stmt, int index_1_based, int64_t unscaled, int scale);
int decentdb_bind_datetime(decentdb_stmt* stmt, int index_1_based, int64_t micros_utc);

// Statement reuse
int decentdb_reset(decentdb_stmt* stmt);
int decentdb_clear_bindings(decentdb_stmt* stmt);

// Step rows: returns 1=row available, 0=done, <0=error
int decentdb_step(decentdb_stmt* stmt);

// --------------------------------------------------------------------------
// Column metadata
// --------------------------------------------------------------------------
int decentdb_column_count(decentdb_stmt* stmt);
const char* decentdb_column_name(decentdb_stmt* stmt, int col_0_based);

// Column type codes use the underlying ValueKind ordinals.
// Common logical codes: 0=NULL, 1=INT64, 2=BOOL, 3=FLOAT64, 4=TEXT, 5=BLOB,
//                       12=DECIMAL, 17=DATETIME.
// TEXT/BLOB values may also report overflow/compressed ValueKind codes.
int decentdb_column_type(decentdb_stmt* stmt, int col_0_based);

// --------------------------------------------------------------------------
// Column accessors (valid after step() returns 1)
//
// LIFETIME: Pointers returned by decentdb_column_text() and decentdb_column_blob()
// are borrowed from the statement's internal row buffer. They become INVALID when:
//   - decentdb_step() is called again (next row overwrites the buffer)
//   - decentdb_reset() is called
//   - decentdb_finalize() is called
//   - decentdb_close() is called on the parent database
// Callers must copy the data if they need it beyond the next step/reset/finalize.
// The same lifetime rules apply to decentdb_row_view() output pointers.
// --------------------------------------------------------------------------
int decentdb_column_is_null(decentdb_stmt* stmt, int col_0_based);
int64_t decentdb_column_int64(decentdb_stmt* stmt, int col_0_based);
double decentdb_column_float64(decentdb_stmt* stmt, int col_0_based);
const char* decentdb_column_text(decentdb_stmt* stmt, int col_0_based, int* out_byte_len);
const uint8_t* decentdb_column_blob(decentdb_stmt* stmt, int col_0_based, int* out_byte_len);
int64_t decentdb_column_decimal_unscaled(decentdb_stmt* stmt, int col_0_based);
int decentdb_column_decimal_scale(decentdb_stmt* stmt, int col_0_based);
int64_t decentdb_column_datetime(decentdb_stmt* stmt, int col_0_based);

// --------------------------------------------------------------------------
// Row view (performance extension)
// --------------------------------------------------------------------------
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

// Convenience: reset+clear+bind+step+row_view in one call.
int decentdb_step_with_params_row_view(
	decentdb_stmt* stmt,
	const decentdb_value_view* in_params,
	int in_count,
	const decentdb_value_view** out_values,
	int* out_count,
	int* out_has_row
);

// --------------------------------------------------------------------------
// Result metadata
// --------------------------------------------------------------------------
int64_t decentdb_rows_affected(decentdb_stmt* stmt);
void decentdb_finalize(decentdb_stmt* stmt);

// --------------------------------------------------------------------------
// Maintenance
// --------------------------------------------------------------------------
int decentdb_checkpoint(decentdb_db* db);
int decentdb_save_as(decentdb_db* db, const char* dest_path_utf8);

// --------------------------------------------------------------------------
// Memory management
// --------------------------------------------------------------------------
void decentdb_free(void* p);

// --------------------------------------------------------------------------
// Schema introspection (JSON; caller frees with decentdb_free)
// --------------------------------------------------------------------------
const char* decentdb_list_tables_json(decentdb_db* db, int* out_len);
const char* decentdb_get_table_columns_json(decentdb_db* db, const char* table_utf8, int* out_len);
const char* decentdb_list_indexes_json(decentdb_db* db, int* out_len);
const char* decentdb_list_views_json(decentdb_db* db, int* out_len);
const char* decentdb_get_view_ddl(decentdb_db* db, const char* view_utf8, int* out_len);

#ifdef __cplusplus
}
#endif

#endif // DECENTDB_H
