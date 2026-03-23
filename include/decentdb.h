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
  DDB_ERR_PANIC = 7
};

typedef struct ddb_db_handle ddb_db_t;
typedef struct ddb_result_handle ddb_result_t;

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

/* Borrowed pointer valid until the next DecentDB call on the same thread. */
const char *ddb_version(void);
const char *ddb_last_error_message(void);

ddb_status_t ddb_value_init(ddb_value_t *value);
ddb_status_t ddb_value_dispose(ddb_value_t *value);
ddb_status_t ddb_string_free(char **value);

ddb_status_t ddb_db_create(const char *path, ddb_db_t **out_db);
ddb_status_t ddb_db_open(const char *path, ddb_db_t **out_db);
ddb_status_t ddb_db_open_or_create(const char *path, ddb_db_t **out_db);
ddb_status_t ddb_db_free(ddb_db_t **db);

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

ddb_status_t ddb_evict_shared_wal(const char *path);

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
