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

static int check_last_error_json(char *buffer, size_t buffer_len) {
  char *json = NULL;
  ddb_status_t status = ddb_last_error_json(&json);
  if (status != DDB_OK) {
    snprintf(buffer, buffer_len, "last_error_json failed with status %u",
             status);
    return 0;
  }
  if (json == NULL) {
    snprintf(buffer, buffer_len, "last_error_json returned NULL");
    return 0;
  }
  int ok = strstr(json, "\"code_name\":\"ERR_SQL\"") != NULL &&
           strstr(json, "\"subcode\":\"sql.relation_not_found\"") != NULL &&
           strstr(json, "\"relation\":\"nope\"") != NULL;
  if (!ok) {
    snprintf(buffer, buffer_len, "unexpected diagnostic JSON: %.400s", json);
  }
  ddb_string_free(&json);
  return ok;
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
  if (!check(ddb_db_execute_queued(
                 db, "INSERT INTO smoke (id, name) VALUES (2, 'node-queued')",
                 NULL, 0, DDB_WRITE_QUEUE_TIMEOUT_DEFAULT, &result),
             error, sizeof(error), "queued insert")) {
    return fail(env, error);
  }
  ddb_result_free(&result);
  ddb_write_queue_metrics_t metrics;
  if (!check(ddb_db_write_queue_metrics(db, &metrics), error, sizeof(error),
             "queue metrics")) {
    return fail(env, error);
  }
  if (metrics.admitted != 1 || metrics.committed != 1 || metrics.failed != 0) {
    return fail(env, "unexpected queue metrics");
  }
  ddb_watch_t *watch = NULL;
  char *watch_event = NULL;
  if (!check(ddb_db_watch_query_json(
                 db, "{\"sql\":\"SELECT id, name FROM smoke ORDER BY id\"}",
                 &watch),
             error, sizeof(error), "watch query")) {
    return fail(env, error);
  }
  if (!check(ddb_watch_next_json(watch, 1000, &watch_event), error,
             sizeof(error), "watch initial")) {
    return fail(env, error);
  }
  if (strstr(watch_event, "\"type\":\"initial\"") == NULL) {
    return fail(env, "unexpected initial watch event");
  }
  ddb_string_free(&watch_event);
  if (!check(ddb_db_execute(
                 db, "INSERT INTO smoke (id, name) VALUES (3, 'node-watch')",
                 NULL, 0, &result),
             error, sizeof(error), "watch insert")) {
    return fail(env, error);
  }
  ddb_result_free(&result);
  if (!check(ddb_watch_next_json(watch, 1000, &watch_event), error,
             sizeof(error), "watch invalidate")) {
    return fail(env, error);
  }
  if (strstr(watch_event, "\"type\":\"invalidate\"") == NULL ||
      strstr(watch_event, "\"smoke\"") == NULL) {
    return fail(env, "unexpected invalidate watch event");
  }
  ddb_string_free(&watch_event);
  if (ddb_watch_next_json(watch, 1, &watch_event) != DDB_ERR_TIMEOUT) {
    return fail(env, "expected watch timeout");
  }
  if (!check(ddb_watch_close(&watch), error, sizeof(error), "watch close")) {
    return fail(env, error);
  }
  if (!check(ddb_db_execute(db, "SELECT id, name FROM smoke", NULL, 0, &result),
             error, sizeof(error), "select")) {
    return fail(env, error);
  }
  if (!check(ddb_result_row_count(result, &rows), error, sizeof(error),
             "row count")) {
    return fail(env, error);
  }
  ddb_result_free(&result);
  if (rows != 3) {
    return fail(env, "expected 3 rows");
  }
  if (ddb_db_execute(db, "SELECT * FROM nope", NULL, 0, &result) !=
      DDB_ERR_SQL) {
    return fail(env, "expected SQL error");
  }
  if (strstr(ddb_last_error_message(), "nope") == NULL) {
    return fail(env, "unexpected error message");
  }
  if (!check_last_error_json(error, sizeof(error))) {
    return fail(env, error);
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
