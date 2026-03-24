#include <node_api.h>

#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "native_lib.h"

typedef struct db_wrap {
  decentdb_db* db;
} db_wrap;

typedef struct stmt_wrap {
  decentdb_stmt* stmt;
  bool busy;
} stmt_wrap;

enum {
  DECENTDB_KIND_INT64 = DDB_VALUE_INT64,
  DECENTDB_KIND_FLOAT64 = DDB_VALUE_FLOAT64,
  DECENTDB_KIND_BOOL = DDB_VALUE_BOOL,
  DECENTDB_KIND_TEXT = DDB_VALUE_TEXT,
  DECENTDB_KIND_BLOB = DDB_VALUE_BLOB,
  DECENTDB_KIND_DECIMAL = DDB_VALUE_DECIMAL,
  DECENTDB_KIND_DATETIME = DDB_VALUE_TIMESTAMP_MICROS
};

typedef struct {
  napi_async_work work;
  napi_deferred deferred;
  stmt_wrap* wrap;
  const decentdb_native_api* api;
  int rc;
  int native_err_code;
  char native_err_msg[512];
} async_step_data;

static napi_value build_row_array(napi_env env, const decentdb_native_api* api, decentdb_stmt* stmt);

static napi_value throw_error(napi_env env, const char* code, const char* msg) {
  napi_value err, msgv, codev;
  napi_status st;

  if (msg == NULL) msg = "error";

  st = napi_create_string_utf8(env, msg, NAPI_AUTO_LENGTH, &msgv);
  assert(st == napi_ok);

  st = napi_create_error(env, NULL, msgv, &err);
  assert(st == napi_ok);

  if (code != NULL) {
    st = napi_create_string_utf8(env, code, NAPI_AUTO_LENGTH, &codev);
    assert(st == napi_ok);
    st = napi_set_named_property(env, err, "code", codev);
    assert(st == napi_ok);
  }

  napi_throw(env, err);
  return NULL;
}

static napi_value throw_last_native_error(napi_env env, const decentdb_native_api* api) {
  int code = 0;
  const char* msg = "native error";
  if (api && api->last_error_code && api->last_error_message) {
    code = api->last_error_code(NULL);
    msg = api->last_error_message(NULL);
  }

  char buf[768];
  snprintf(buf, sizeof(buf), "DecentDB native error (%d): %s", code, msg ? msg : "");
  return throw_error(env, "DECENTDB_NATIVE", buf);
}

static void free_native_owned_string(const decentdb_native_api* api, const char* ptr) {
  if (api && api->free && ptr) {
    api->free((void*)ptr);
  }
}

static const decentdb_native_api* require_api(napi_env env) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) {
    return (const decentdb_native_api*)throw_error(env, "DECENTDB_NATIVE_LOAD", decentdb_native_last_load_error());
  }
  return api;
}

static void db_finalize(napi_env env, void* data, void* hint) {
  (void)env;
  (void)hint;
  db_wrap* w = (db_wrap*)data;
  if (!w) return;

  const decentdb_native_api* api = decentdb_native_get();
  if (api && w->db) {
    api->close(w->db);
    w->db = NULL;
  }

  free(w);
}

static void stmt_finalize(napi_env env, void* data, void* hint) {
  (void)env;
  (void)hint;
  stmt_wrap* w = (stmt_wrap*)data;
  if (!w) return;

  const decentdb_native_api* api = decentdb_native_get();
  if (api && w->stmt) {
    api->finalize(w->stmt);
    w->stmt = NULL;
  }

  free(w);
}

static stmt_wrap* unwrap_stmt(napi_env env, napi_value v) {
  stmt_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, v, (void**)&w);
  if (st != napi_ok || w == NULL || w->stmt == NULL) {
    throw_error(env, "DECENTDB_BAD_HANDLE", "Invalid statement handle");
    return NULL;
  }
  return w;
}

static db_wrap* unwrap_db(napi_env env, napi_value v) {
  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, v, (void**)&w);
  if (st != napi_ok || w == NULL || w->db == NULL) {
    throw_error(env, "DECENTDB_BAD_HANDLE", "Invalid database handle");
    return NULL;
  }
  return w;
}

static napi_value js_db_open(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 2;
  napi_value argv[2];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 1) return throw_error(env, "DECENTDB_ARGS", "dbOpen(path, options?) requires a path");

  // path
  size_t pathLen = 0;
  st = napi_get_value_string_utf8(env, argv[0], NULL, 0, &pathLen);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "path must be a string");

  char* path = (char*)malloc(pathLen + 1);
  st = napi_get_value_string_utf8(env, argv[0], path, pathLen + 1, &pathLen);
  assert(st == napi_ok);

  // options (nullable)
  char* opts = NULL;
  size_t optsLen = 0;
  if (argc >= 2) {
    napi_valuetype t;
    st = napi_typeof(env, argv[1], &t);
    assert(st == napi_ok);
    if (t == napi_string) {
      st = napi_get_value_string_utf8(env, argv[1], NULL, 0, &optsLen);
      assert(st == napi_ok);
      opts = (char*)malloc(optsLen + 1);
      st = napi_get_value_string_utf8(env, argv[1], opts, optsLen + 1, &optsLen);
      assert(st == napi_ok);
    }
  }

  decentdb_db* db = api->open(path, opts);
  free(path);
  if (opts) free(opts);

  if (!db) return throw_last_native_error(env, api);

  db_wrap* w = (db_wrap*)calloc(1, sizeof(db_wrap));
  w->db = db;

  napi_value ext;
  st = napi_create_external(env, w, db_finalize, NULL, &ext);
  assert(st == napi_ok);
  return ext;
}

static napi_value js_db_close(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 1) return throw_error(env, "DECENTDB_ARGS", "dbClose(handle) requires a handle");

  db_wrap* w = NULL;
  st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w) return throw_error(env, "DECENTDB_BAD_HANDLE", "Invalid database handle");

  if (w->db) {
    api->close(w->db);
    w->db = NULL;
  }

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_prepare(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 2;
  napi_value argv[2];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 2) return throw_error(env, "DECENTDB_ARGS", "stmtPrepare(dbHandle, sql) requires 2 args");

  db_wrap* dbw = unwrap_db(env, argv[0]);
  if (!dbw) return NULL;

  size_t sqlLen = 0;
  st = napi_get_value_string_utf8(env, argv[1], NULL, 0, &sqlLen);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "sql must be a string");

  char* sql = (char*)malloc(sqlLen + 1);
  st = napi_get_value_string_utf8(env, argv[1], sql, sqlLen + 1, &sqlLen);
  assert(st == napi_ok);

  decentdb_stmt* stmt = NULL;
  int rc = api->prepare(dbw->db, sql, &stmt);
  free(sql);

  if (rc != 0 || !stmt) return throw_last_native_error(env, api);

  stmt_wrap* sw = (stmt_wrap*)calloc(1, sizeof(stmt_wrap));
  sw->stmt = stmt;

  napi_value ext;
  st = napi_create_external(env, sw, stmt_finalize, NULL, &ext);
  assert(st == napi_ok);
  return ext;
}

static napi_value js_stmt_finalize(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 1) return throw_error(env, "DECENTDB_ARGS", "stmtFinalize(handle) requires a handle");

  stmt_wrap* w = NULL;
  st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w) return throw_error(env, "DECENTDB_BAD_HANDLE", "Invalid statement handle");

  if (w->stmt) {
    api->finalize(w->stmt);
    w->stmt = NULL;
  }

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_reset(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int rc = api->reset(w->stmt);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_clear_bindings(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int rc = api->clear_bindings(w->stmt);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_null(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 2;
  napi_value argv[2];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  int rc = api->bind_null(w->stmt, idx);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_int64(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 3;
  napi_value argv[3];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  int64_t v = 0;
  bool lossless = false;
  st = napi_get_value_bigint_int64(env, argv[2], &v, &lossless);
  if (st != napi_ok || !lossless) {
    return throw_error(env, "DECENTDB_ARGS", "value must be a BigInt (int64)" );
  }

  int rc = api->bind_int64(w->stmt, idx, v);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_int64_number(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 3;
  napi_value argv[3];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  int64_t v = 0;
  st = napi_get_value_int64(env, argv[2], &v);
  if (st != napi_ok) {
    return throw_error(env, "DECENTDB_ARGS", "value must be a safe integer number");
  }

  int rc = api->bind_int64(w->stmt, idx, v);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_bool(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 3;
  napi_value argv[3];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  bool v;
  st = napi_get_value_bool(env, argv[2], &v);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "value must be a boolean" );

  int rc = api->bind_bool(w->stmt, idx, v ? 1 : 0);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_float64(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 3;
  napi_value argv[3];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  double v;
  st = napi_get_value_double(env, argv[2], &v);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "value must be a number" );

  int rc = api->bind_float64(w->stmt, idx, v);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_text(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 3;
  napi_value argv[3];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  size_t len = 0;
  st = napi_get_value_string_utf8(env, argv[2], NULL, 0, &len);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "value must be a string" );

  char* s = (char*)malloc(len + 1);
  st = napi_get_value_string_utf8(env, argv[2], s, len + 1, &len);
  assert(st == napi_ok);

  int rc = api->bind_text(w->stmt, idx, s, (int)len);
  free(s);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_bind_blob(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 3;
  napi_value argv[3];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  void* data = NULL;
  size_t len = 0;
  st = napi_get_buffer_info(env, argv[2], &data, &len);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "value must be a Buffer" );

  const uint8_t* bytes = (const uint8_t*)data;
  int rc = api->bind_blob(w->stmt, idx, bytes, (int)len);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value js_stmt_step(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int rc = api->step(w->stmt);
  if (rc < 0) return throw_last_native_error(env, api);

  napi_value b;
  st = napi_get_boolean(env, rc == 1, &b);
  assert(st == napi_ok);
  return b;
}

static napi_value js_stmt_step_with_params(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 2;
  napi_value argv[2];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 2) {
    return throw_error(env, "DECENTDB_ARGS", "stmtStepWithParams(handle, bindings) requires 2 args");
  }

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  bool is_array = false;
  st = napi_is_array(env, argv[1], &is_array);
  assert(st == napi_ok);
  if (!is_array) {
    return throw_error(env, "DECENTDB_ARGS", "bindings must be an array");
  }

  uint32_t bind_count = 0;
  st = napi_get_array_length(env, argv[1], &bind_count);
  assert(st == napi_ok);

  int rc = api->reset(w->stmt);
  if (rc != 0) return throw_last_native_error(env, api);
  rc = api->clear_bindings(w->stmt);
  if (rc != 0) return throw_last_native_error(env, api);

  for (uint32_t i = 0; i < bind_count; i++) {
    napi_value value;
    st = napi_get_element(env, argv[1], i, &value);
    assert(st == napi_ok);

    int idx = (int)i + 1;
    napi_valuetype t;
    st = napi_typeof(env, value, &t);
    assert(st == napi_ok);

    if (t == napi_undefined || t == napi_null) {
      rc = api->bind_null(w->stmt, idx);
    } else if (t == napi_bigint) {
      int64_t v = 0;
      bool lossless = false;
      st = napi_get_value_bigint_int64(env, value, &v, &lossless);
      if (st != napi_ok || !lossless) {
        return throw_error(env, "DECENTDB_ARGS", "BigInt value must fit in int64");
      }
      rc = api->bind_int64(w->stmt, idx, v);
    } else if (t == napi_number) {
      double dv = 0.0;
      st = napi_get_value_double(env, value, &dv);
      if (st != napi_ok) {
        return throw_error(env, "DECENTDB_ARGS", "number binding is invalid");
      }

      if (isfinite(dv) && floor(dv) == dv &&
          dv >= (double)INT64_MIN && dv <= (double)INT64_MAX) {
        rc = api->bind_int64(w->stmt, idx, (int64_t)dv);
      } else {
        rc = api->bind_float64(w->stmt, idx, dv);
      }
    } else if (t == napi_boolean) {
      bool bv = false;
      st = napi_get_value_bool(env, value, &bv);
      if (st != napi_ok) {
        return throw_error(env, "DECENTDB_ARGS", "boolean binding is invalid");
      }
      rc = api->bind_bool(w->stmt, idx, bv ? 1 : 0);
    } else if (t == napi_string) {
      size_t len = 0;
      st = napi_get_value_string_utf8(env, value, NULL, 0, &len);
      if (st != napi_ok) {
        return throw_error(env, "DECENTDB_ARGS", "string binding is invalid");
      }
      char* s = (char*)malloc(len + 1);
      if (!s) {
        return throw_error(env, "DECENTDB_OOM", "out of memory while binding string");
      }
      st = napi_get_value_string_utf8(env, value, s, len + 1, &len);
      assert(st == napi_ok);
      rc = api->bind_text(w->stmt, idx, s, (int)len);
      free(s);
    } else if (t == napi_object) {
      bool is_buffer = false;
      st = napi_is_buffer(env, value, &is_buffer);
      assert(st == napi_ok);
      if (is_buffer) {
        void* data = NULL;
        size_t len = 0;
        st = napi_get_buffer_info(env, value, &data, &len);
        if (st != napi_ok) {
          return throw_error(env, "DECENTDB_ARGS", "buffer binding is invalid");
        }
        rc = api->bind_blob(w->stmt, idx, (const uint8_t*)data, (int)len);
      } else {
        bool is_typedarray = false;
        st = napi_is_typedarray(env, value, &is_typedarray);
        assert(st == napi_ok);
        if (is_typedarray) {
          napi_typedarray_type ta_type;
          size_t ta_len = 0;
          void* ta_data = NULL;
          napi_value ta_arraybuffer;
          size_t ta_offset = 0;
          st = napi_get_typedarray_info(
              env, value, &ta_type, &ta_len, &ta_data, &ta_arraybuffer, &ta_offset);
          if (st != napi_ok) {
            return throw_error(env, "DECENTDB_ARGS", "typed array binding is invalid");
          }
          rc = api->bind_blob(
              w->stmt, idx, ((const uint8_t*)ta_data) + ta_offset, (int)ta_len);
        } else {
          return throw_error(env, "DECENTDB_ARGS", "unsupported object binding type");
        }
      }
    } else {
      return throw_error(env, "DECENTDB_ARGS", "unsupported binding type");
    }

    if (rc != 0) return throw_last_native_error(env, api);
  }

  rc = api->step(w->stmt);
  if (rc < 0) return throw_last_native_error(env, api);

  napi_value b;
  st = napi_get_boolean(env, rc == 1, &b);
  assert(st == napi_ok);
  return b;
}

static napi_value js_stmt_execute_batch_i64_text_f64(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);
  if (!api->execute_batch_i64_text_f64) {
    return throw_error(env, "DECENTDB_UNSUPPORTED", "native batch insert API is unavailable");
  }

  size_t argc = 4;
  napi_value argv[4];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 4) {
    return throw_error(
        env,
        "DECENTDB_ARGS",
        "stmtExecuteBatchI64TextF64(handle, ids, texts, floats) requires 4 args");
  }

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  bool is_ids_array = false;
  bool is_texts_array = false;
  bool is_floats_array = false;
  st = napi_is_array(env, argv[1], &is_ids_array);
  assert(st == napi_ok);
  st = napi_is_array(env, argv[2], &is_texts_array);
  assert(st == napi_ok);
  st = napi_is_array(env, argv[3], &is_floats_array);
  assert(st == napi_ok);
  if (!is_ids_array || !is_texts_array || !is_floats_array) {
    return throw_error(env, "DECENTDB_ARGS", "ids/texts/floats must be arrays");
  }

  uint32_t row_count_ids = 0;
  uint32_t row_count_texts = 0;
  uint32_t row_count_floats = 0;
  st = napi_get_array_length(env, argv[1], &row_count_ids);
  assert(st == napi_ok);
  st = napi_get_array_length(env, argv[2], &row_count_texts);
  assert(st == napi_ok);
  st = napi_get_array_length(env, argv[3], &row_count_floats);
  assert(st == napi_ok);

  if (row_count_ids != row_count_texts || row_count_ids != row_count_floats) {
    return throw_error(env, "DECENTDB_ARGS", "ids/texts/floats must have equal length");
  }

  size_t row_count = (size_t)row_count_ids;
  int64_t* ids = NULL;
  double* floats = NULL;
  const char** text_ptrs = NULL;
  size_t* text_lens = NULL;
  char** text_buffers = NULL;

  if (row_count > 0) {
    ids = (int64_t*)malloc(sizeof(int64_t) * row_count);
    floats = (double*)malloc(sizeof(double) * row_count);
    text_ptrs = (const char**)malloc(sizeof(char*) * row_count);
    text_lens = (size_t*)malloc(sizeof(size_t) * row_count);
    text_buffers = (char**)calloc(row_count, sizeof(char*));
    if (!ids || !floats || !text_ptrs || !text_lens || !text_buffers) {
      if (ids) free(ids);
      if (floats) free(floats);
      if (text_ptrs) free(text_ptrs);
      if (text_lens) free(text_lens);
      if (text_buffers) free(text_buffers);
      return throw_error(env, "DECENTDB_OOM", "out of memory for batch buffers");
    }
  }

  for (uint32_t i = 0; i < row_count_ids; i++) {
    napi_value id_value;
    st = napi_get_element(env, argv[1], i, &id_value);
    assert(st == napi_ok);
    napi_valuetype id_type;
    st = napi_typeof(env, id_value, &id_type);
    assert(st == napi_ok);
    if (id_type == napi_bigint) {
      bool lossless = false;
      st = napi_get_value_bigint_int64(env, id_value, &ids[i], &lossless);
      if (st != napi_ok || !lossless) {
        for (uint32_t j = 0; j <= i; j++) free(text_buffers[j]);
        free(text_buffers);
        free(text_lens);
        free(text_ptrs);
        free(floats);
        free(ids);
        return throw_error(env, "DECENTDB_ARGS", "ids must be int64-safe BigInt/Number values");
      }
    } else if (id_type == napi_number) {
      st = napi_get_value_int64(env, id_value, &ids[i]);
      if (st != napi_ok) {
        for (uint32_t j = 0; j <= i; j++) free(text_buffers[j]);
        free(text_buffers);
        free(text_lens);
        free(text_ptrs);
        free(floats);
        free(ids);
        return throw_error(env, "DECENTDB_ARGS", "ids must be int64-safe BigInt/Number values");
      }
    } else {
      for (uint32_t j = 0; j <= i; j++) free(text_buffers[j]);
      free(text_buffers);
      free(text_lens);
      free(text_ptrs);
      free(floats);
      free(ids);
      return throw_error(env, "DECENTDB_ARGS", "ids must be int64-safe BigInt/Number values");
    }

    napi_value text_value;
    st = napi_get_element(env, argv[2], i, &text_value);
    assert(st == napi_ok);
    size_t text_len = 0;
    st = napi_get_value_string_utf8(env, text_value, NULL, 0, &text_len);
    if (st != napi_ok) {
      for (uint32_t j = 0; j <= i; j++) free(text_buffers[j]);
      free(text_buffers);
      free(text_lens);
      free(text_ptrs);
      free(floats);
      free(ids);
      return throw_error(env, "DECENTDB_ARGS", "texts must be strings");
    }
    char* text_buf = (char*)malloc(text_len + 1);
    if (!text_buf) {
      for (uint32_t j = 0; j <= i; j++) free(text_buffers[j]);
      free(text_buffers);
      free(text_lens);
      free(text_ptrs);
      free(floats);
      free(ids);
      return throw_error(env, "DECENTDB_OOM", "out of memory for text buffer");
    }
    st = napi_get_value_string_utf8(env, text_value, text_buf, text_len + 1, &text_len);
    assert(st == napi_ok);
    text_buffers[i] = text_buf;
    text_ptrs[i] = text_buf;
    text_lens[i] = text_len;

    napi_value float_value;
    st = napi_get_element(env, argv[3], i, &float_value);
    assert(st == napi_ok);
    st = napi_get_value_double(env, float_value, &floats[i]);
    if (st != napi_ok) {
      for (uint32_t j = 0; j <= i; j++) free(text_buffers[j]);
      free(text_buffers);
      free(text_lens);
      free(text_ptrs);
      free(floats);
      free(ids);
      return throw_error(env, "DECENTDB_ARGS", "floats must be numbers");
    }
  }

  uint64_t affected = 0;
  int rc = api->execute_batch_i64_text_f64(
      w->stmt, row_count, ids, text_ptrs, text_lens, floats, &affected);

  if (text_buffers) {
    for (uint32_t i = 0; i < row_count_ids; i++) free(text_buffers[i]);
    free(text_buffers);
  }
  if (text_lens) free(text_lens);
  if (text_ptrs) free(text_ptrs);
  if (floats) free(floats);
  if (ids) free(ids);

  if (rc != 0) return throw_last_native_error(env, api);

  napi_value out;
  st = napi_create_bigint_uint64(env, affected, &out);
  assert(st == napi_ok);
  return out;
}

static napi_value js_stmt_fetch_rows_i64_text_f64(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);
  if (!api->fetch_rows_i64_text_f64) {
    return throw_error(env, "DECENTDB_UNSUPPORTED", "native batch fetch API is unavailable");
  }

  size_t argc = 2;
  napi_value argv[2];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  if (argc < 2) {
    return throw_error(
        env,
        "DECENTDB_ARGS",
        "stmtFetchRowsI64TextF64(handle, maxRows) requires 2 args");
  }

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int64_t max_rows_i64 = 0;
  st = napi_get_value_int64(env, argv[1], &max_rows_i64);
  if (st != napi_ok || max_rows_i64 < 0) {
    return throw_error(env, "DECENTDB_ARGS", "maxRows must be a non-negative integer");
  }

  const decentdb_row_i64_text_f64_view* rows_ptr = NULL;
  size_t out_rows = 0;
  int rc = api->fetch_rows_i64_text_f64(
      w->stmt, 0, (size_t)max_rows_i64, &rows_ptr, &out_rows);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value arr;
  st = napi_create_array_with_length(env, out_rows, &arr);
  assert(st == napi_ok);

  for (size_t i = 0; i < out_rows; i++) {
    napi_value row_arr;
    st = napi_create_array_with_length(env, 3, &row_arr);
    assert(st == napi_ok);

    napi_value idv;
    st = napi_create_bigint_int64(env, rows_ptr[i].int64_value, &idv);
    assert(st == napi_ok);
    st = napi_set_element(env, row_arr, 0, idv);
    assert(st == napi_ok);

    const char* txt = (const char*)rows_ptr[i].text_data;
    size_t txt_len = rows_ptr[i].text_len;
    if (!txt) txt = "";
    napi_value textv;
    st = napi_create_string_utf8(env, txt, txt_len, &textv);
    assert(st == napi_ok);
    st = napi_set_element(env, row_arr, 1, textv);
    assert(st == napi_ok);

    napi_value floatv;
    st = napi_create_double(env, rows_ptr[i].float64_value, &floatv);
    assert(st == napi_ok);
    st = napi_set_element(env, row_arr, 2, floatv);
    assert(st == napi_ok);

    st = napi_set_element(env, arr, (uint32_t)i, row_arr);
    assert(st == napi_ok);
  }

  return arr;
}

static napi_value js_stmt_column_names(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int count = api->column_count(w->stmt);
  if (count < 0) return throw_last_native_error(env, api);
  napi_value arr;
  st = napi_create_array_with_length(env, (size_t)count, &arr);
  assert(st == napi_ok);

  for (int i = 0; i < count; i++) {
    const char* name = api->column_name(w->stmt, i);
    if (!name) return throw_last_native_error(env, api);
    napi_value namev;
    st = napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &namev);
    assert(st == napi_ok);
    free_native_owned_string(api, name);
    st = napi_set_element(env, arr, (uint32_t)i, namev);
    assert(st == napi_ok);
  }

  return arr;
}

static napi_value js_stmt_rows_affected(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int64_t v = api->rows_affected(w->stmt);

  napi_value out;
  st = napi_create_bigint_int64(env, v, &out);
  assert(st == napi_ok);
  return out;
}

static napi_value js_stmt_bind_decimal(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 4;
  napi_value argv[4];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  int32_t idx;
  st = napi_get_value_int32(env, argv[1], &idx);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "index must be an int");

  int64_t v = 0;
  bool lossless = false;
  st = napi_get_value_bigint_int64(env, argv[2], &v, &lossless);
  if (st != napi_ok || !lossless) {
    return throw_error(env, "DECENTDB_ARGS", "unscaled value must be a BigInt (int64)" );
  }

  int32_t scale;
  st = napi_get_value_int32(env, argv[3], &scale);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ARGS", "scale must be an int");

  int rc = api->bind_decimal(w->stmt, idx, v, scale);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_value undef;
  st = napi_get_undefined(env, &undef);
  assert(st == napi_ok);
  return undef;
}

static napi_value build_row_array(napi_env env, const decentdb_native_api* api, decentdb_stmt* stmt) {
  const decentdb_value_view* values = NULL;
  int count = 0;
  int rc = api->row_view(stmt, &values, &count);
  if (rc != 0) return throw_last_native_error(env, api);

  napi_status st;
  napi_value arr;
  st = napi_create_array_with_length(env, (size_t)count, &arr);
  assert(st == napi_ok);

  for (int i = 0; i < count; i++) {
    const decentdb_value_view* v = &values[i];
    napi_value cell;

    if (v->tag == DDB_VALUE_NULL) {
      st = napi_get_null(env, &cell);
      assert(st == napi_ok);
    } else {
      switch (v->tag) {
        case DECENTDB_KIND_INT64: {
          st = napi_create_bigint_int64(env, v->int64_value, &cell);
          assert(st == napi_ok);
          break;
        }
        case DECENTDB_KIND_BOOL: {
          st = napi_get_boolean(env, v->bool_value != 0, &cell);
          assert(st == napi_ok);
          break;
        }
        case DECENTDB_KIND_FLOAT64: {
          st = napi_create_double(env, v->float64_value, &cell);
          assert(st == napi_ok);
          break;
        }
        case DECENTDB_KIND_TEXT: {
          const char* s = (const char*)v->data;
          size_t len = v->len;
          if (!s) s = "";
          st = napi_create_string_utf8(env, s, len, &cell);
          assert(st == napi_ok);
          break;
        }
        case DECENTDB_KIND_BLOB:
        case DDB_VALUE_UUID: {
          const uint8_t* b = v->tag == DDB_VALUE_UUID ? v->uuid_bytes : v->data;
          size_t len = v->tag == DDB_VALUE_UUID ? 16u : v->len;
          st = napi_create_buffer_copy(env, len, b, NULL, &cell);
          assert(st == napi_ok);
          break;
        }
        case DECENTDB_KIND_DECIMAL: {
          napi_value obj;
          st = napi_create_object(env, &obj);
          assert(st == napi_ok);
          
          napi_value unscaledv;
          st = napi_create_bigint_int64(env, v->decimal_scaled, &unscaledv);
          assert(st == napi_ok);
          st = napi_set_named_property(env, obj, "unscaled", unscaledv);
          assert(st == napi_ok);

          napi_value scalev;
          st = napi_create_int32(env, (int32_t)v->decimal_scale, &scalev);
          assert(st == napi_ok);
          st = napi_set_named_property(env, obj, "scale", scalev);
          assert(st == napi_ok);

          cell = obj;
          break;
        }
        case DECENTDB_KIND_DATETIME: {
          double ms = (double)v->timestamp_micros / 1000.0;
          napi_value msv;
          st = napi_create_double(env, ms, &msv);
          assert(st == napi_ok);
          cell = msv;
          break;
        }
        default: {
          // Unknown kind: surface as null to avoid UB.
          st = napi_get_null(env, &cell);
          assert(st == napi_ok);
          break;
        }
      }
    }

    st = napi_set_element(env, arr, (uint32_t)i, cell);
    assert(st == napi_ok);
  }

  return arr;
}

static napi_value js_stmt_row_array(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;
  
  if (w->busy) return throw_error(env, "DECENTDB_BUSY", "Statement is busy");

  return build_row_array(env, api, w->stmt);
}

static void async_step_execute(napi_env env, void* data) {
  (void)env;
  async_step_data* d = (async_step_data*)data;
  d->rc = d->api->step(d->wrap->stmt);
  if (d->rc < 0) {
    d->native_err_code = d->api->last_error_code(NULL);
    const char* m = d->api->last_error_message(NULL);
    if (m) {
      strncpy(d->native_err_msg, m, sizeof(d->native_err_msg) - 1);
      d->native_err_msg[sizeof(d->native_err_msg) - 1] = '\0';
    } else {
      d->native_err_msg[0] = '\0';
    }
  }
}

static void async_step_complete(napi_env env, napi_status status, void* data) {
  async_step_data* d = (async_step_data*)data;
  
  if (status != napi_ok) {
    napi_value err, msg;
    napi_create_string_utf8(env, "N-API async work failed", NAPI_AUTO_LENGTH, &msg);
    napi_create_error(env, NULL, msg, &err);
    napi_reject_deferred(env, d->deferred, err);
  } else if (d->rc < 0) {
    // Native error
    napi_value err, codev, msgv;
    napi_create_string_utf8(env, d->native_err_msg, NAPI_AUTO_LENGTH, &msgv);
    napi_create_error(env, NULL, msgv, &err);
    char codeBuf[32];
    snprintf(codeBuf, sizeof(codeBuf), "%d", d->native_err_code);
    napi_create_string_utf8(env, codeBuf, NAPI_AUTO_LENGTH, &codev);
    napi_set_named_property(env, err, "code", codev);
    napi_reject_deferred(env, d->deferred, err);
  } else if (d->rc == 0) {
    // Done
    napi_value nullv;
    napi_get_null(env, &nullv);
    napi_resolve_deferred(env, d->deferred, nullv);
  } else {
    // Row available (rc == 1)
    napi_value arr = build_row_array(env, d->api, d->wrap->stmt);
    bool has_exception = false;
    napi_is_exception_pending(env, &has_exception);
    if (has_exception) {
        napi_value ex;
        napi_get_and_clear_last_exception(env, &ex);
        napi_reject_deferred(env, d->deferred, ex);
    } else if (arr) {
        napi_resolve_deferred(env, d->deferred, arr);
    } else {
        // Should not happen if build_row_array returns NULL only on exception
        napi_value err, msg;
        napi_create_string_utf8(env, "Unknown error building row", NAPI_AUTO_LENGTH, &msg);
        napi_create_error(env, NULL, msg, &err);
        napi_reject_deferred(env, d->deferred, err);
    }
  }

  d->wrap->busy = false;
  napi_delete_async_work(env, d->work);
  free(d);
}

static napi_value js_stmt_next_async(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = require_api(env);

  size_t argc = 1;
  napi_value argv[1];
  napi_status st = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
  assert(st == napi_ok);

  stmt_wrap* w = unwrap_stmt(env, argv[0]);
  if (!w) return NULL;

  if (w->busy) return throw_error(env, "DECENTDB_BUSY", "Statement is busy");

  w->busy = true;

  async_step_data* d = (async_step_data*)calloc(1, sizeof(async_step_data));
  d->wrap = w;
  d->api = api;

  napi_value promise;
  st = napi_create_promise(env, &d->deferred, &promise);
  assert(st == napi_ok);

  napi_value resource_name;
  napi_create_string_utf8(env, "DecentDB_AsyncStep", NAPI_AUTO_LENGTH, &resource_name);

  st = napi_create_async_work(env, NULL, resource_name, 
                              async_step_execute, async_step_complete, 
                              d, &d->work);
  assert(st == napi_ok);

  st = napi_queue_async_work(env, d->work);
  assert(st == napi_ok);

  return promise;
}

// --------------- Checkpoint ---------------
static napi_value js_db_checkpoint(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  int rc = api->checkpoint(w->db);
  if (rc != 0) {
    const char* msg = api->last_error_message(w->db);
    return throw_error(env, "DECENTDB_ERR", msg);
  }

  napi_value result;
  napi_get_undefined(env, &result);
  return result;
}

static napi_value js_db_save_as(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  char path_buf[4096];
  size_t path_len = 0;
  st = napi_get_value_string_utf8(env, argv[1], path_buf, sizeof(path_buf), &path_len);
  if (st != napi_ok) return throw_error(env, "DECENTDB_ERR", "Invalid destination path");

  int rc = api->save_as(w->db, path_buf);
  if (rc != 0) {
    const char* msg = api->last_error_message(w->db);
    return throw_error(env, "DECENTDB_ERR", msg);
  }

  napi_value result;
  napi_get_undefined(env, &result);
  return result;
}

static napi_value js_db_begin_transaction(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  int rc = api->begin_transaction(w->db);
  if (rc != 0) {
    return throw_last_native_error(env, api);
  }

  napi_value result;
  napi_get_undefined(env, &result);
  return result;
}

static napi_value js_db_commit_transaction(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  int rc = api->commit_transaction(w->db);
  if (rc != 0) {
    return throw_last_native_error(env, api);
  }

  napi_value result;
  napi_get_undefined(env, &result);
  return result;
}

static napi_value js_db_rollback_transaction(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  int rc = api->rollback_transaction(w->db);
  if (rc != 0) {
    return throw_last_native_error(env, api);
  }

  napi_value result;
  napi_get_undefined(env, &result);
  return result;
}

// --------------- Schema introspection helpers ---------------
static napi_value json_api_call(napi_env env, db_wrap* w, const decentdb_native_api* api,
                                const char* (*fn)(decentdb_db*, int*)) {
  int out_len = 0;
  const char* ptr = fn(w->db, &out_len);
  if (!ptr) {
    const char* msg = api->last_error_message(w->db);
    return throw_error(env, "DECENTDB_ERR", msg);
  }
  napi_value result;
  napi_create_string_utf8(env, ptr, (size_t)out_len, &result);
  api->free((void*)ptr);
  return result;
}

static napi_value js_db_list_tables_json(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  return json_api_call(env, w, api, api->list_tables_json);
}

static napi_value js_db_get_table_columns_json(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 2;
  napi_value argv[2];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  size_t table_len;
  napi_get_value_string_utf8(env, argv[1], NULL, 0, &table_len);
  char* table_name = (char*)malloc(table_len + 1);
  napi_get_value_string_utf8(env, argv[1], table_name, table_len + 1, &table_len);

  int out_len = 0;
  const char* ptr = api->get_table_columns_json(w->db, table_name, &out_len);
  free(table_name);

  if (!ptr) {
    const char* msg = api->last_error_message(w->db);
    return throw_error(env, "DECENTDB_ERR", msg);
  }
  napi_value result;
  napi_create_string_utf8(env, ptr, (size_t)out_len, &result);
  api->free((void*)ptr);
  return result;
}

static napi_value js_db_list_indexes_json(napi_env env, napi_callback_info info) {
  const decentdb_native_api* api = decentdb_native_get();
  if (!api) return throw_error(env, "DECENTDB_LOAD", decentdb_native_last_load_error());

  size_t argc = 1;
  napi_value argv[1];
  napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

  db_wrap* w = NULL;
  napi_status st = napi_get_value_external(env, argv[0], (void**)&w);
  if (st != napi_ok || !w || !w->db) return throw_error(env, "DECENTDB_ERR", "Invalid db handle");

  return json_api_call(env, w, api, api->list_indexes_json);
}

static napi_value init(napi_env env, napi_value exports) {
  napi_status st;

  napi_property_descriptor props[] = {
    {"dbOpen", 0, js_db_open, 0, 0, 0, napi_default, 0},
    {"dbClose", 0, js_db_close, 0, 0, 0, napi_default, 0},

    {"stmtPrepare", 0, js_stmt_prepare, 0, 0, 0, napi_default, 0},
    {"stmtFinalize", 0, js_stmt_finalize, 0, 0, 0, napi_default, 0},
    {"stmtReset", 0, js_stmt_reset, 0, 0, 0, napi_default, 0},
    {"stmtClearBindings", 0, js_stmt_clear_bindings, 0, 0, 0, napi_default, 0},

    {"stmtBindNull", 0, js_stmt_bind_null, 0, 0, 0, napi_default, 0},
    {"stmtBindInt64", 0, js_stmt_bind_int64, 0, 0, 0, napi_default, 0},
    {"stmtBindInt64Number", 0, js_stmt_bind_int64_number, 0, 0, 0, napi_default, 0},
    {"stmtBindBool", 0, js_stmt_bind_bool, 0, 0, 0, napi_default, 0},
    {"stmtBindFloat64", 0, js_stmt_bind_float64, 0, 0, 0, napi_default, 0},
    {"stmtBindText", 0, js_stmt_bind_text, 0, 0, 0, napi_default, 0},
    {"stmtBindBlob", 0, js_stmt_bind_blob, 0, 0, 0, napi_default, 0},
    {"stmtBindDecimal", 0, js_stmt_bind_decimal, 0, 0, 0, napi_default, 0},

    {"stmtStep", 0, js_stmt_step, 0, 0, 0, napi_default, 0},
    {"stmtStepWithParams", 0, js_stmt_step_with_params, 0, 0, 0, napi_default, 0},
    {"stmtExecuteBatchI64TextF64", 0, js_stmt_execute_batch_i64_text_f64, 0, 0, 0, napi_default, 0},
    {"stmtFetchRowsI64TextF64", 0, js_stmt_fetch_rows_i64_text_f64, 0, 0, 0, napi_default, 0},
    {"stmtNextAsync", 0, js_stmt_next_async, 0, 0, 0, napi_default, 0},
    {"stmtRowArray", 0, js_stmt_row_array, 0, 0, 0, napi_default, 0},
    {"stmtColumnNames", 0, js_stmt_column_names, 0, 0, 0, napi_default, 0},
    {"stmtRowsAffected", 0, js_stmt_rows_affected, 0, 0, 0, napi_default, 0},

    {"dbCheckpoint", 0, js_db_checkpoint, 0, 0, 0, napi_default, 0},
    {"dbBeginTransaction", 0, js_db_begin_transaction, 0, 0, 0, napi_default, 0},
    {"dbCommitTransaction", 0, js_db_commit_transaction, 0, 0, 0, napi_default, 0},
    {"dbRollbackTransaction", 0, js_db_rollback_transaction, 0, 0, 0, napi_default, 0},
    {"dbSaveAs", 0, js_db_save_as, 0, 0, 0, napi_default, 0},
    {"dbListTablesJson", 0, js_db_list_tables_json, 0, 0, 0, napi_default, 0},
    {"dbGetTableColumnsJson", 0, js_db_get_table_columns_json, 0, 0, 0, napi_default, 0},
    {"dbListIndexesJson", 0, js_db_list_indexes_json, 0, 0, 0, napi_default, 0},
  };

  st = napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props);
  assert(st == napi_ok);

  return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
