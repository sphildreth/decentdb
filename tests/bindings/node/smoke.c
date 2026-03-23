#include <node_api.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "decentdb.h"

static napi_value fail(napi_env env, const char *message) {
  napi_throw_error(env, NULL, message);
  return NULL;
}

static int check(ddb_status_t status, char *buffer, size_t buffer_len,
                 const char *context) {
  if (status == DDB_OK) {
    return 1;
  }
  snprintf(buffer, buffer_len, "%s failed with status %u: %s", context, status,
           ddb_last_error_message() == NULL ? "<null>" : ddb_last_error_message());
  return 0;
}

static napi_value run_smoke(napi_env env, napi_callback_info info) {
  (void)info;
  char error[512];
  ddb_db_t *db = NULL;
  ddb_result_t *result = NULL;
  size_t rows = 0;

  if (!check(ddb_db_open_or_create(":memory:", &db), error, sizeof(error),
             "open_or_create")) {
    return fail(env, error);
  }
  if (!check(ddb_db_execute(db,
                            "CREATE TABLE smoke (id INT64 PRIMARY KEY, name TEXT)",
                            NULL, 0, &result),
             error, sizeof(error), "create")) {
    return fail(env, error);
  }
  ddb_result_free(&result);
  if (!check(ddb_db_execute(
                 db, "INSERT INTO smoke (id, name) VALUES (1, 'node-smoke')",
                 NULL, 0, &result),
             error, sizeof(error), "insert")) {
    return fail(env, error);
  }
  ddb_result_free(&result);
  if (!check(ddb_db_execute(db, "SELECT id, name FROM smoke", NULL, 0, &result),
             error, sizeof(error), "select")) {
    return fail(env, error);
  }
  if (!check(ddb_result_row_count(result, &rows), error, sizeof(error),
             "row count")) {
    return fail(env, error);
  }
  ddb_result_free(&result);
  if (rows != 1) {
    return fail(env, "expected 1 row");
  }
  if (ddb_db_execute(db, "SELECT * FROM nope", NULL, 0, &result) !=
      DDB_ERR_SQL) {
    return fail(env, "expected SQL error");
  }
  if (strstr(ddb_last_error_message(), "nope") == NULL) {
    return fail(env, "unexpected error message");
  }
  ddb_db_free(&db);

  napi_value ok;
  napi_get_boolean(env, 1, &ok);
  return ok;
}

NAPI_MODULE_INIT() {
  napi_value fn;
  napi_create_function(env, "runSmoke", NAPI_AUTO_LENGTH, run_smoke, NULL, &fn);
  napi_set_named_property(env, exports, "runSmoke", fn);
  return exports;
}
