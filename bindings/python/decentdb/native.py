import ctypes
import os
import sys
import platform
from ctypes import c_int, c_int64, c_double, c_char_p, c_void_p, POINTER, c_uint8, Structure

# Error Codes (must match the C API).
#
# Internally, `src/errors.nim` has `ErrorCode` starting at 0.
# The exported C API reserves 0 for OK and maps internal codes to 1..N.
ERR_OK = 0
ERR_IO = 1
ERR_CORRUPTION = 2
ERR_CONSTRAINT = 3
ERR_TRANSACTION = 4
ERR_SQL = 5
ERR_INTERNAL = 6

# Legacy / not-yet-exposed codes (not currently produced by the C API).
# Keep these defined so higher-level Python code can import them.
ERR_ERROR = 100
ERR_LOCKED = 101
ERR_NOT_FOUND = 102
ERR_FULL = 103
ERR_PERMISSION = 104
ERR_INVALID = 105
ERR_NOMEM = 106

# Value Kinds (must match value.nim / c_api.nim logic)
# In c_api.nim:
# if val.kind == vkNull: view.isNull = 1
# vkNull usually 0
# vkInt64
# vkFloat64
# vkText
# vkBlob
# vkBool
# We can look up the enum values in `src/sql/sql.nim` or `src/record/record.nim` or `src/exec/exec.nim`.
# Based on c_api.nim checks:
# kind is passed through from internal Value kind.
# Let's check `src/sql/sql.nim` for `ValueKind` enum.

class DecentdbValueView(Structure):
    _fields_ = [
        ("kind", c_int),
        ("isNull", c_int),
        ("int64Val", c_int64),
        ("float64Val", c_double),
        ("bytes", POINTER(c_uint8)),
        ("bytesLen", c_int),
    ]

_lib = None

def load_library():
    global _lib
    if _lib is not None:
        return _lib

    lib_path = os.environ.get("DECENTDB_NATIVE_LIB")
    
    if not lib_path:
        # Search relative to this file, walking up ancestors so this works when
        # running from the repo (e.g. `.../build/libc_api.so`) as well as when
        # invoked from different working directories.

        here = os.path.abspath(__file__)
        candidates = []

        # Common build artifact names across platforms
        lib_names = [
            "libc_api.so",
            "libdecentdb.so",
            "libc_api.dylib",
            "libdecentdb.dylib",
            "decentdb.dll",
        ]

        # Check current working directory build first (useful in CI/scripts)
        cwd = os.getcwd()
        for name in lib_names:
            candidates.append(os.path.join(cwd, "build", name))

        # Walk up a few parents from this module's location
        # (native.py -> decentdb/ -> python/ -> bindings/ -> repo root)
        cur_dir = os.path.dirname(here)
        for _ in range(0, 8):
            for name in lib_names:
                candidates.append(os.path.join(cur_dir, "build", name))
            parent = os.path.dirname(cur_dir)
            if parent == cur_dir:
                break
            cur_dir = parent

        for p in candidates:
            if os.path.exists(p):
                lib_path = p
                break
    
    if not lib_path:
        raise RuntimeError("Could not find decentdb native library. Set DECENTDB_NATIVE_LIB env var.")

    try:
        _lib = ctypes.CDLL(lib_path)
    except OSError as e:
        raise RuntimeError(f"Failed to load decentdb native library at {lib_path}: {e}")

    # Define signatures

    # Memory management for API-allocated buffers
    _lib.decentdb_free.argtypes = [c_void_p]
    _lib.decentdb_free.restype = None

    # Reflection helpers (JSON payloads; caller frees)
    _lib.decentdb_list_tables_json.argtypes = [c_void_p, POINTER(c_int)]
    _lib.decentdb_list_tables_json.restype = c_void_p

    _lib.decentdb_get_table_columns_json.argtypes = [c_void_p, c_char_p, POINTER(c_int)]
    _lib.decentdb_get_table_columns_json.restype = c_void_p
    
    # decentdb_open
    _lib.decentdb_open.argtypes = [c_char_p, c_char_p]
    _lib.decentdb_open.restype = c_void_p

    # decentdb_close
    _lib.decentdb_close.argtypes = [c_void_p]
    _lib.decentdb_close.restype = c_int

    # decentdb_last_error_code
    _lib.decentdb_last_error_code.argtypes = [c_void_p]
    _lib.decentdb_last_error_code.restype = c_int

    # decentdb_last_error_message
    _lib.decentdb_last_error_message.argtypes = [c_void_p]
    _lib.decentdb_last_error_message.restype = c_char_p

    # decentdb_prepare
    _lib.decentdb_prepare.argtypes = [c_void_p, c_char_p, POINTER(c_void_p)]
    _lib.decentdb_prepare.restype = c_int

    # decentdb_finalize
    _lib.decentdb_finalize.argtypes = [c_void_p]
    _lib.decentdb_finalize.restype = None

    # decentdb_reset
    _lib.decentdb_reset.argtypes = [c_void_p]
    _lib.decentdb_reset.restype = c_int

    # decentdb_clear_bindings
    _lib.decentdb_clear_bindings.argtypes = [c_void_p]
    _lib.decentdb_clear_bindings.restype = c_int

    # Bindings
    _lib.decentdb_bind_null.argtypes = [c_void_p, c_int]
    _lib.decentdb_bind_null.restype = c_int

    _lib.decentdb_bind_int64.argtypes = [c_void_p, c_int, c_int64]
    _lib.decentdb_bind_int64.restype = c_int

    _lib.decentdb_bind_bool.argtypes = [c_void_p, c_int, c_int]
    _lib.decentdb_bind_bool.restype = c_int

    _lib.decentdb_bind_float64.argtypes = [c_void_p, c_int, c_double]
    _lib.decentdb_bind_float64.restype = c_int

    _lib.decentdb_bind_text.argtypes = [c_void_p, c_int, c_char_p, c_int]
    _lib.decentdb_bind_text.restype = c_int

    _lib.decentdb_bind_blob.argtypes = [c_void_p, c_int, POINTER(c_uint8), c_int]
    _lib.decentdb_bind_blob.restype = c_int

    # Step
    _lib.decentdb_step.argtypes = [c_void_p]
    _lib.decentdb_step.restype = c_int

    # Columns
    _lib.decentdb_column_count.argtypes = [c_void_p]
    _lib.decentdb_column_count.restype = c_int

    _lib.decentdb_column_name.argtypes = [c_void_p, c_int]
    _lib.decentdb_column_name.restype = c_char_p

    _lib.decentdb_column_type.argtypes = [c_void_p, c_int]
    _lib.decentdb_column_type.restype = c_int

    # Column Accessors (optional usage if not using row view)
    _lib.decentdb_column_is_null.argtypes = [c_void_p, c_int]
    _lib.decentdb_column_is_null.restype = c_int
    
    _lib.decentdb_column_int64.argtypes = [c_void_p, c_int]
    _lib.decentdb_column_int64.restype = c_int64

    _lib.decentdb_column_float64.argtypes = [c_void_p, c_int]
    _lib.decentdb_column_float64.restype = c_double

    _lib.decentdb_column_text.argtypes = [c_void_p, c_int, POINTER(c_int)]
    _lib.decentdb_column_text.restype = c_char_p

    _lib.decentdb_column_blob.argtypes = [c_void_p, c_int, POINTER(c_int)]
    _lib.decentdb_column_blob.restype = POINTER(c_uint8)

    # Row View
    _lib.decentdb_row_view.argtypes = [c_void_p, POINTER(POINTER(DecentdbValueView)), POINTER(c_int)]
    _lib.decentdb_row_view.restype = c_int

    # Combined helper (optional; newer libs only)
    if hasattr(_lib, "decentdb_step_with_params_row_view"):
        _lib.decentdb_step_with_params_row_view.argtypes = [
            c_void_p,
            POINTER(DecentdbValueView),
            c_int,
            POINTER(POINTER(DecentdbValueView)),
            POINTER(c_int),
            POINTER(c_int),
        ]
        _lib.decentdb_step_with_params_row_view.restype = c_int
    
    # Rows Affected
    _lib.decentdb_rows_affected.argtypes = [c_void_p]
    _lib.decentdb_rows_affected.restype = c_int64

    return _lib

