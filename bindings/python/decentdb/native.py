import ctypes
import os
from ctypes import (
    POINTER,
    Structure,
    c_char_p,
    c_double,
    c_int64,
    c_size_t,
    c_uint8,
    c_uint32,
    c_uint64,
    c_void_p,
)

# Stable engine status codes from include/decentdb.h.
ERR_OK = 0
ERR_IO = 1
ERR_CORRUPTION = 2
ERR_CONSTRAINT = 3
ERR_TRANSACTION = 4
ERR_SQL = 5
ERR_INTERNAL = 6
ERR_PANIC = 7
ERR_UNSUPPORTED_FORMAT_VERSION = 8

# Legacy compatibility aliases kept for higher-level Python imports/tests.
ERR_ERROR = 100
ERR_LOCKED = 101
ERR_NOT_FOUND = 102
ERR_FULL = 103
ERR_PERMISSION = 104
ERR_INVALID = 105
ERR_NOMEM = 106

# Stable value tags from include/decentdb.h.
DDB_VALUE_NULL = 0
DDB_VALUE_INT64 = 1
DDB_VALUE_FLOAT64 = 2
DDB_VALUE_BOOL = 3
DDB_VALUE_TEXT = 4
DDB_VALUE_BLOB = 5
DDB_VALUE_DECIMAL = 6
DDB_VALUE_UUID = 7
DDB_VALUE_TIMESTAMP_MICROS = 8


class DdbValue(Structure):
    _fields_ = [
        ("tag", c_uint32),
        ("bool_value", c_uint8),
        ("reserved0", c_uint8 * 7),
        ("int64_value", c_int64),
        ("float64_value", c_double),
        ("decimal_scaled", c_int64),
        ("decimal_scale", c_uint8),
        ("reserved1", c_uint8 * 7),
        ("data", POINTER(c_uint8)),
        ("len", c_size_t),
        ("uuid_bytes", c_uint8 * 16),
        ("timestamp_micros", c_int64),
    ]


class DdbValueView(Structure):
    _fields_ = [
        ("tag", c_uint32),
        ("bool_value", c_uint8),
        ("reserved0", c_uint8 * 7),
        ("int64_value", c_int64),
        ("float64_value", c_double),
        ("decimal_scaled", c_int64),
        ("decimal_scale", c_uint8),
        ("reserved1", c_uint8 * 7),
        ("data", POINTER(c_uint8)),
        ("len", c_size_t),
        ("uuid_bytes", c_uint8 * 16),
        ("timestamp_micros", c_int64),
    ]


_lib = None
_preloaded_lib = None


def _candidate_library_paths():
    lib_path = os.environ.get("DECENTDB_NATIVE_LIB")
    if lib_path:
        return [lib_path]

    here = os.path.abspath(__file__)
    candidates = []
    lib_names = [
        "libdecentdb.so",
        "libdecentdb.dylib",
        "decentdb.dll",
        "libc_api.so",
        "libc_api.dylib",
        "c_api.dll",
    ]

    cwd = os.getcwd()
    for name in lib_names:
        candidates.append(os.path.join(cwd, "build", name))
        candidates.append(os.path.join(cwd, "target", "release", name))
        candidates.append(os.path.join(cwd, "target", "debug", name))

    cur_dir = os.path.dirname(here)
    for _ in range(0, 8):
        for name in lib_names:
            candidates.append(os.path.join(cur_dir, "build", name))
            candidates.append(os.path.join(cur_dir, "target", "release", name))
            candidates.append(os.path.join(cur_dir, "target", "debug", name))
        parent = os.path.dirname(cur_dir)
        if parent == cur_dir:
            break
        cur_dir = parent

    return candidates


def resolve_library_path():
    for candidate in _candidate_library_paths():
        if os.path.exists(candidate):
            return candidate
    raise RuntimeError(
        "Could not find decentdb native library. Set DECENTDB_NATIVE_LIB "
        "or build with `cargo build -p decentdb`."
    )


def preload_library_for_extensions():
    global _preloaded_lib
    if _preloaded_lib is not None:
        return _preloaded_lib

    try:
        lib_path = resolve_library_path()
    except RuntimeError:
        return None

    try:
        if hasattr(ctypes, "RTLD_GLOBAL"):
            _preloaded_lib = ctypes.CDLL(lib_path, mode=ctypes.RTLD_GLOBAL)
        else:
            _preloaded_lib = ctypes.CDLL(lib_path)
    except OSError:
        return None

    return _preloaded_lib


def load_library():
    global _lib
    if _lib is not None:
        return _lib

    lib_path = resolve_library_path()

    try:
        if (
            _preloaded_lib is not None
            and getattr(_preloaded_lib, "_name", None) == lib_path
        ):
            _lib = _preloaded_lib
        else:
            _lib = ctypes.CDLL(lib_path)
    except OSError as exc:
        raise RuntimeError(
            f"Failed to load decentdb native library at {lib_path}: {exc}"
        )

    _lib.ddb_last_error_message.argtypes = []
    _lib.ddb_last_error_message.restype = c_char_p

    _lib.ddb_value_init.argtypes = [POINTER(DdbValue)]
    _lib.ddb_value_init.restype = c_uint32
    _lib.ddb_value_dispose.argtypes = [POINTER(DdbValue)]
    _lib.ddb_value_dispose.restype = c_uint32
    _lib.ddb_string_free.argtypes = [POINTER(c_char_p)]
    _lib.ddb_string_free.restype = c_uint32

    _lib.ddb_db_open_or_create.argtypes = [c_char_p, POINTER(c_void_p)]
    _lib.ddb_db_open_or_create.restype = c_uint32
    _lib.ddb_db_free.argtypes = [POINTER(c_void_p)]
    _lib.ddb_db_free.restype = c_uint32
    _lib.ddb_db_prepare.argtypes = [c_void_p, c_char_p, POINTER(c_void_p)]
    _lib.ddb_db_prepare.restype = c_uint32
    _lib.ddb_db_execute.argtypes = [
        c_void_p,
        c_char_p,
        ctypes.c_void_p,
        c_size_t,
        POINTER(c_void_p),
    ]
    _lib.ddb_db_execute.restype = c_uint32
    _lib.ddb_result_free.argtypes = [POINTER(c_void_p)]
    _lib.ddb_result_free.restype = c_uint32
    _lib.ddb_db_begin_transaction.argtypes = [c_void_p]
    _lib.ddb_db_begin_transaction.restype = c_uint32
    _lib.ddb_db_in_transaction.argtypes = [c_void_p, POINTER(c_uint8)]
    _lib.ddb_db_in_transaction.restype = c_uint32
    _lib.ddb_db_commit_transaction.argtypes = [c_void_p, POINTER(c_uint64)]
    _lib.ddb_db_commit_transaction.restype = c_uint32
    _lib.ddb_db_rollback_transaction.argtypes = [c_void_p]
    _lib.ddb_db_rollback_transaction.restype = c_uint32
    _lib.ddb_db_checkpoint.argtypes = [c_void_p]
    _lib.ddb_db_checkpoint.restype = c_uint32
    _lib.ddb_db_save_as.argtypes = [c_void_p, c_char_p]
    _lib.ddb_db_save_as.restype = c_uint32
    _lib.ddb_db_list_tables_json.argtypes = [c_void_p, POINTER(c_char_p)]
    _lib.ddb_db_list_tables_json.restype = c_uint32
    _lib.ddb_db_describe_table_json.argtypes = [c_void_p, c_char_p, POINTER(c_char_p)]
    _lib.ddb_db_describe_table_json.restype = c_uint32
    _lib.ddb_db_list_indexes_json.argtypes = [c_void_p, POINTER(c_char_p)]
    _lib.ddb_db_list_indexes_json.restype = c_uint32
    if hasattr(_lib, "ddb_db_inspect_storage_state_json"):
        _lib.ddb_db_inspect_storage_state_json.argtypes = [c_void_p, POINTER(c_char_p)]
        _lib.ddb_db_inspect_storage_state_json.restype = c_uint32
    _lib.ddb_evict_shared_wal.argtypes = [c_char_p]
    _lib.ddb_evict_shared_wal.restype = c_uint32

    _lib.ddb_stmt_free.argtypes = [POINTER(c_void_p)]
    _lib.ddb_stmt_free.restype = c_uint32
    _lib.ddb_stmt_reset.argtypes = [c_void_p]
    _lib.ddb_stmt_reset.restype = c_uint32
    _lib.ddb_stmt_clear_bindings.argtypes = [c_void_p]
    _lib.ddb_stmt_clear_bindings.restype = c_uint32
    _lib.ddb_stmt_bind_null.argtypes = [c_void_p, c_size_t]
    _lib.ddb_stmt_bind_null.restype = c_uint32
    _lib.ddb_stmt_bind_int64.argtypes = [c_void_p, c_size_t, c_int64]
    _lib.ddb_stmt_bind_int64.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_bind_int64_step_row_view"):
        _lib.ddb_stmt_bind_int64_step_row_view.argtypes = [
            c_void_p,
            c_size_t,
            c_int64,
            POINTER(POINTER(DdbValueView)),
            POINTER(c_size_t),
            POINTER(c_uint8),
        ]
        _lib.ddb_stmt_bind_int64_step_row_view.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_bind_int64_step_i64_text_f64"):
        _lib.ddb_stmt_bind_int64_step_i64_text_f64.argtypes = [
            c_void_p,
            c_size_t,
            c_int64,
            POINTER(c_int64),
            POINTER(c_char_p),
            POINTER(c_size_t),
            POINTER(c_double),
            POINTER(c_size_t),
            POINTER(c_uint8),
        ]
        _lib.ddb_stmt_bind_int64_step_i64_text_f64.restype = c_uint32
    _lib.ddb_stmt_bind_float64.argtypes = [c_void_p, c_size_t, c_double]
    _lib.ddb_stmt_bind_float64.restype = c_uint32
    _lib.ddb_stmt_bind_bool.argtypes = [c_void_p, c_size_t, c_uint8]
    _lib.ddb_stmt_bind_bool.restype = c_uint32
    _lib.ddb_stmt_bind_text.argtypes = [c_void_p, c_size_t, c_char_p, c_size_t]
    _lib.ddb_stmt_bind_text.restype = c_uint32
    _lib.ddb_stmt_bind_blob.argtypes = [c_void_p, c_size_t, POINTER(c_uint8), c_size_t]
    _lib.ddb_stmt_bind_blob.restype = c_uint32
    _lib.ddb_stmt_bind_decimal.argtypes = [c_void_p, c_size_t, c_int64, c_uint8]
    _lib.ddb_stmt_bind_decimal.restype = c_uint32
    _lib.ddb_stmt_bind_timestamp_micros.argtypes = [c_void_p, c_size_t, c_int64]
    _lib.ddb_stmt_bind_timestamp_micros.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_execute_batch_i64"):
        _lib.ddb_stmt_execute_batch_i64.argtypes = [
            c_void_p,
            c_size_t,
            POINTER(c_int64),
            POINTER(c_uint64),
        ]
        _lib.ddb_stmt_execute_batch_i64.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_execute_batch_i64_text_f64"):
        _lib.ddb_stmt_execute_batch_i64_text_f64.argtypes = [
            c_void_p,
            c_size_t,
            POINTER(c_int64),
            POINTER(c_char_p),
            POINTER(c_size_t),
            POINTER(c_double),
            POINTER(c_uint64),
        ]
        _lib.ddb_stmt_execute_batch_i64_text_f64.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_execute_batch_typed"):
        _lib.ddb_stmt_execute_batch_typed.argtypes = [
            c_void_p,
            c_char_p,
            c_size_t,
            POINTER(c_uint64),
        ]
        _lib.ddb_stmt_execute_batch_typed.restype = c_uint32
    _lib.ddb_stmt_step.argtypes = [c_void_p, POINTER(c_uint8)]
    _lib.ddb_stmt_step.restype = c_uint32
    _lib.ddb_stmt_column_count.argtypes = [c_void_p, POINTER(c_size_t)]
    _lib.ddb_stmt_column_count.restype = c_uint32
    _lib.ddb_stmt_column_name_copy.argtypes = [c_void_p, c_size_t, POINTER(c_char_p)]
    _lib.ddb_stmt_column_name_copy.restype = c_uint32
    _lib.ddb_stmt_affected_rows.argtypes = [c_void_p, POINTER(c_uint64)]
    _lib.ddb_stmt_affected_rows.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_rebind_int64_execute"):
        _lib.ddb_stmt_rebind_int64_execute.argtypes = [
            c_void_p,
            c_size_t,
            c_int64,
            POINTER(c_uint64),
        ]
        _lib.ddb_stmt_rebind_int64_execute.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_rebind_text_int64_execute"):
        _lib.ddb_stmt_rebind_text_int64_execute.argtypes = [
            c_void_p,
            c_char_p,
            c_size_t,
            c_size_t,
            c_int64,
            POINTER(c_uint64),
        ]
        _lib.ddb_stmt_rebind_text_int64_execute.restype = c_uint32
    if hasattr(_lib, "ddb_stmt_rebind_int64_text_execute"):
        _lib.ddb_stmt_rebind_int64_text_execute.argtypes = [
            c_void_p,
            c_size_t,
            c_int64,
            c_char_p,
            c_size_t,
            POINTER(c_uint64),
        ]
        _lib.ddb_stmt_rebind_int64_text_execute.restype = c_uint32
    _lib.ddb_stmt_value_copy.argtypes = [c_void_p, c_size_t, POINTER(DdbValue)]
    _lib.ddb_stmt_value_copy.restype = c_uint32
    _lib.ddb_stmt_row_view.argtypes = [
        c_void_p,
        POINTER(POINTER(DdbValueView)),
        POINTER(c_size_t),
    ]
    _lib.ddb_stmt_row_view.restype = c_uint32
    _lib.ddb_stmt_step_row_view.argtypes = [
        c_void_p,
        POINTER(POINTER(DdbValueView)),
        POINTER(c_size_t),
        POINTER(c_uint8),
    ]
    _lib.ddb_stmt_step_row_view.restype = c_uint32
    _lib.ddb_stmt_fetch_row_views.argtypes = [
        c_void_p,
        c_uint8,
        c_size_t,
        POINTER(POINTER(DdbValueView)),
        POINTER(c_size_t),
        POINTER(c_size_t),
    ]
    _lib.ddb_stmt_fetch_row_views.restype = c_uint32

    _lib.ddb_abi_version.argtypes = []
    _lib.ddb_abi_version.restype = c_uint32

    _lib.ddb_version.argtypes = []
    _lib.ddb_version.restype = c_char_p

    _lib.ddb_db_create.argtypes = [c_char_p, POINTER(c_void_p)]
    _lib.ddb_db_create.restype = c_uint32

    _lib.ddb_db_open.argtypes = [c_char_p, POINTER(c_void_p)]
    _lib.ddb_db_open.restype = c_uint32

    _lib.ddb_db_get_table_ddl.argtypes = [c_void_p, c_char_p, POINTER(c_char_p)]
    _lib.ddb_db_get_table_ddl.restype = c_uint32

    _lib.ddb_db_list_views_json.argtypes = [c_void_p, POINTER(c_char_p)]
    _lib.ddb_db_list_views_json.restype = c_uint32

    _lib.ddb_db_get_view_ddl.argtypes = [c_void_p, c_char_p, POINTER(c_char_p)]
    _lib.ddb_db_get_view_ddl.restype = c_uint32

    _lib.ddb_db_list_triggers_json.argtypes = [c_void_p, POINTER(c_char_p)]
    _lib.ddb_db_list_triggers_json.restype = c_uint32

    _lib.ddb_result_row_count.argtypes = [c_void_p, POINTER(c_size_t)]
    _lib.ddb_result_row_count.restype = c_uint32

    _lib.ddb_result_column_count.argtypes = [c_void_p, POINTER(c_size_t)]
    _lib.ddb_result_column_count.restype = c_uint32

    _lib.ddb_result_affected_rows.argtypes = [c_void_p, POINTER(c_uint64)]
    _lib.ddb_result_affected_rows.restype = c_uint32

    _lib.ddb_result_column_name_copy.argtypes = [c_void_p, c_size_t, POINTER(c_char_p)]
    _lib.ddb_result_column_name_copy.restype = c_uint32

    _lib.ddb_result_value_copy.argtypes = [
        c_void_p,
        c_size_t,
        c_size_t,
        POINTER(DdbValue),
    ]
    _lib.ddb_result_value_copy.restype = c_uint32

    _lib.decentdb_last_error_message = _lib.ddb_last_error_message
    _lib.decentdb_last_error_code = lambda *_args: getattr(
        _lib, "_last_error_code", ERR_INTERNAL
    )
    _lib.decentdb_list_tables_json = _lib.ddb_db_list_tables_json

    return _lib
