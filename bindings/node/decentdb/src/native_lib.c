#include "native_lib.h"

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

static decentdb_native_api g_api;
static int g_loaded = 0;
static char g_last_error[512];

static void set_last_error(const char* msg) {
  if (msg == NULL) msg = "unknown";
  strncpy(g_last_error, msg, sizeof(g_last_error) - 1);
  g_last_error[sizeof(g_last_error) - 1] = '\0';
}

const char* decentdb_native_last_load_error(void) {
  return g_last_error;
}

static int resolve_all(DL_HANDLE h) {
  memset(&g_api, 0, sizeof(g_api));

  g_api.open = (decentdb_db* (*)(const char*, const char*))load_sym(h, "decentdb_open");
  g_api.close = (int (*)(decentdb_db*))load_sym(h, "decentdb_close");

  g_api.last_error_code = (int (*)(decentdb_db*))load_sym(h, "decentdb_last_error_code");
  g_api.last_error_message = (const char* (*)(decentdb_db*))load_sym(h, "decentdb_last_error_message");

  g_api.prepare = (int (*)(decentdb_db*, const char*, decentdb_stmt**))load_sym(h, "decentdb_prepare");

  g_api.bind_null = (int (*)(decentdb_stmt*, int))load_sym(h, "decentdb_bind_null");
  g_api.bind_int64 = (int (*)(decentdb_stmt*, int, int64_t))load_sym(h, "decentdb_bind_int64");
  g_api.bind_bool = (int (*)(decentdb_stmt*, int, int))load_sym(h, "decentdb_bind_bool");
  g_api.bind_float64 = (int (*)(decentdb_stmt*, int, double))load_sym(h, "decentdb_bind_float64");
  g_api.bind_text = (int (*)(decentdb_stmt*, int, const char*, int))load_sym(h, "decentdb_bind_text");
  g_api.bind_blob = (int (*)(decentdb_stmt*, int, const uint8_t*, int))load_sym(h, "decentdb_bind_blob");
  g_api.bind_decimal = (int (*)(decentdb_stmt*, int, int64_t, int))load_sym(h, "decentdb_bind_decimal");

  g_api.reset = (int (*)(decentdb_stmt*))load_sym(h, "decentdb_reset");
  g_api.clear_bindings = (int (*)(decentdb_stmt*))load_sym(h, "decentdb_clear_bindings");

  g_api.step = (int (*)(decentdb_stmt*))load_sym(h, "decentdb_step");
  g_api.column_count = (int (*)(decentdb_stmt*))load_sym(h, "decentdb_column_count");
  g_api.column_name = (const char* (*)(decentdb_stmt*, int))load_sym(h, "decentdb_column_name");
  g_api.row_view = (int (*)(decentdb_stmt*, const decentdb_value_view**, int*))load_sym(h, "decentdb_row_view");
  g_api.rows_affected = (int64_t (*)(decentdb_stmt*))load_sym(h, "decentdb_rows_affected");
  g_api.finalize = (void (*)(decentdb_stmt*))load_sym(h, "decentdb_finalize");

  g_api.checkpoint = (int (*)(decentdb_db*))load_sym(h, "decentdb_checkpoint");
  g_api.save_as = (int (*)(decentdb_db*, const char*))load_sym(h, "decentdb_save_as");
  g_api.free = (void (*)(void*))load_sym(h, "decentdb_free");
  g_api.list_tables_json = (const char* (*)(decentdb_db*, int*))load_sym(h, "decentdb_list_tables_json");
  g_api.get_table_columns_json = (const char* (*)(decentdb_db*, const char*, int*))load_sym(h, "decentdb_get_table_columns_json");
  g_api.list_indexes_json = (const char* (*)(decentdb_db*, int*))load_sym(h, "decentdb_list_indexes_json");

  if (!g_api.open || !g_api.close || !g_api.last_error_code || !g_api.last_error_message ||
      !g_api.prepare || !g_api.bind_null || !g_api.bind_int64 || !g_api.bind_bool || !g_api.bind_float64 ||
      !g_api.bind_text || !g_api.bind_blob || !g_api.bind_decimal || !g_api.reset || !g_api.clear_bindings ||
      !g_api.step || !g_api.row_view || !g_api.rows_affected || !g_api.finalize ||
      !g_api.column_count || !g_api.column_name ||
      !g_api.checkpoint || !g_api.free ||
      !g_api.list_tables_json || !g_api.get_table_columns_json || !g_api.list_indexes_json) {
    set_last_error("missing required symbol(s) in DecentDB native library");
    return 0;
  }

  return 1;
}

const decentdb_native_api* decentdb_native_get(void) {
  if (g_loaded) return &g_api;

  set_last_error("not loaded");

  const char* explicitPath = NULL;
#if !defined(_WIN32)
  explicitPath = getenv("DECENTDB_NATIVE_LIB_PATH");
#else
  // getenv is available but path may be UTF-8; keep scaffold simple.
  explicitPath = getenv("DECENTDB_NATIVE_LIB_PATH");
#endif

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
