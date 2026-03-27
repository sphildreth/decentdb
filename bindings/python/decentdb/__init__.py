import collections
import ctypes
import datetime
import decimal
import json
import os
import re
import uuid
import weakref
from collections.abc import Mapping

from . import native as _native

_native.preload_library_for_extensions()

try:
    from . import _fastdecode as _fastdecode_native
except Exception:
    _fastdecode_native = None

from .native import (
    DDB_VALUE_BLOB,
    DDB_VALUE_BOOL,
    DDB_VALUE_DECIMAL,
    DDB_VALUE_FLOAT64,
    DDB_VALUE_INT64,
    DDB_VALUE_NULL,
    DDB_VALUE_TEXT,
    DDB_VALUE_TIMESTAMP_MICROS,
    DDB_VALUE_UUID,
    DdbValue,
    DdbValueView,
    ERR_CONSTRAINT,
    ERR_CORRUPTION,
    ERR_ERROR,
    ERR_FULL,
    ERR_INTERNAL,
    ERR_INVALID,
    ERR_IO,
    ERR_LOCKED,
    ERR_NOMEM,
    ERR_NOT_FOUND,
    ERR_OK,
    ERR_PERMISSION,
    ERR_SQL,
    ERR_TRANSACTION,
    load_library,
)

# DB-API 2.0 globals
apilevel = "2.0"
threadsafety = 1
paramstyle = "qmark"


class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime.datetime
ROWID = int
_UNIX_EPOCH_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


def DateFromTicks(ticks):
    return datetime.datetime.fromtimestamp(ticks, datetime.timezone.utc).date()


def TimeFromTicks(ticks):
    return datetime.datetime.fromtimestamp(ticks, datetime.timezone.utc).time()


def TimestampFromTicks(ticks):
    return datetime.datetime.fromtimestamp(ticks, datetime.timezone.utc).replace(
        tzinfo=None
    )


def Binary(value):
    return bytes(value)


def _format_value_for_error(value, *, max_str=200, max_bytes=64):
    if value is None:
        return None
    if isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if len(raw) <= max_bytes:
            return {"_type": "bytes", "hex": raw.hex(), "len": len(raw)}
        return {
            "_type": "bytes",
            "hex_prefix": raw[:max_bytes].hex(),
            "len": len(raw),
        }
    if isinstance(value, str):
        return value if len(value) <= max_str else value[:max_str] + "…"
    text = repr(value)
    return text if len(text) <= max_str else text[:max_str] + "…"


def _format_params_for_error(params, *, max_items=50):
    if params is None:
        return None
    if isinstance(params, Mapping):
        out = {}
        for i, (key, value) in enumerate(params.items()):
            if i >= max_items:
                out["_truncated"] = True
                break
            out[str(key)] = _format_value_for_error(value)
        return out
    try:
        seq = list(params)
    except TypeError:
        return _format_value_for_error(params)
    if len(seq) > max_items:
        seq = seq[:max_items] + ["<truncated>"]
    return [_format_value_for_error(value) for value in seq]


def _last_error_message():
    lib = load_library()
    message = getattr(lib, "decentdb_last_error_message", lib.ddb_last_error_message)()
    return message.decode("utf-8", errors="replace") if message else None


def _raise_error(code, *, sql=None, params=None):
    lib = load_library()
    if isinstance(code, int):
        native_code = int(code)
        lib._last_error_code = native_code
    else:
        native_code = int(
            getattr(lib, "decentdb_last_error_code", lambda *_args: ERR_INTERNAL)(code)
        )
    message = _last_error_message() or f"Unknown error {native_code}"
    if native_code == ERR_TRANSACTION and "no active SQL transaction" in message:
        message = "No active transaction: " + message
    if sql is not None:
        context = {
            "native_code": native_code,
            "sql": sql,
            "params": _format_params_for_error(params),
        }
        message += "\nContext: " + json.dumps(context, ensure_ascii=False)

    if native_code == ERR_CONSTRAINT:
        raise IntegrityError(message)
    if native_code in (ERR_TRANSACTION, ERR_IO, ERR_LOCKED, ERR_NOT_FOUND):
        raise OperationalError(message)
    if native_code == ERR_SQL:
        raise ProgrammingError(message)
    if native_code == ERR_CORRUPTION:
        raise DatabaseError(message)
    if native_code == ERR_INTERNAL:
        raise InternalError(message)
    if native_code in (ERR_INVALID, ERR_PERMISSION, ERR_FULL, ERR_NOMEM, ERR_ERROR):
        raise DatabaseError(message)
    raise DatabaseError(message)


def _convert_params(sql, params):
    if params is None:
        return sql, []

    if isinstance(params, Mapping):
        if "?" in sql:
            raise ProgrammingError(
                "Mixed parameter styles are not supported: got named parameters with qmark placeholders"
            )

        param_map = {}
        new_params = []

        def replace(match):
            name = match.group(1)
            if name not in param_map:
                if name not in params:
                    raise ProgrammingError(f"Missing parameter '{name}'")
                param_map[name] = len(new_params) + 1
                new_params.append(params[name])
            return f"${param_map[name]}"

        new_sql = re.sub(r":([a-zA-Z_][a-zA-Z0-9_]*)", replace, sql)
        return new_sql, new_params

    if re.search(r":([a-zA-Z_][a-zA-Z0-9_]*)", sql) is not None:
        raise ProgrammingError(
            "Mixed parameter styles are not supported: got positional parameters with named placeholders"
        )

    if "?" in sql:
        parts = sql.split("?")
        if len(parts) - 1 != len(params):
            raise ProgrammingError(
                f"Incorrect number of parameters: expected {len(parts) - 1}, got {len(params)}"
            )
        new_sql = ""
        for i in range(len(parts) - 1):
            new_sql += parts[i] + f"${i + 1}"
        new_sql += parts[-1]
        return new_sql, params

    return sql, params


_TXN_CONTROL_RE = re.compile(
    r"^\s*(BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE)\b",
    re.IGNORECASE,
)

_BEGIN_CONTROL_SQL = frozenset(
    {
        "BEGIN",
        "BEGIN TRANSACTION",
        "BEGIN DEFERRED",
        "BEGIN DEFERRED TRANSACTION",
        "BEGIN IMMEDIATE",
        "BEGIN IMMEDIATE TRANSACTION",
        "BEGIN EXCLUSIVE",
        "BEGIN EXCLUSIVE TRANSACTION",
    }
)
_COMMIT_CONTROL_SQL = frozenset({"COMMIT", "END", "END TRANSACTION"})
_ROLLBACK_CONTROL_SQL = frozenset({"ROLLBACK", "ROLLBACK TRANSACTION"})


def _normalize_control_sql(sql):
    return " ".join(sql.strip().rstrip(";").split()).upper()


def _transaction_control_kind(sql):
    normalized = _normalize_control_sql(sql)
    if normalized in _BEGIN_CONTROL_SQL:
        return "begin"
    if normalized in _COMMIT_CONTROL_SQL:
        return "commit"
    if normalized in _ROLLBACK_CONTROL_SQL:
        return "rollback"
    return None


def _is_direct_execute_sql(sql):
    return _TXN_CONTROL_RE.match(sql) is not None


def _decode_ffi_value(lib, value):
    tag = int(value.tag)
    if tag == DDB_VALUE_NULL:
        return None
    if tag == DDB_VALUE_INT64:
        return int(value.int64_value)
    if tag == DDB_VALUE_FLOAT64:
        return float(value.float64_value)
    if tag == DDB_VALUE_BOOL:
        return bool(value.bool_value)
    if tag == DDB_VALUE_TEXT:
        if not value.data or value.len == 0:
            return ""
        return ctypes.string_at(value.data, value.len).decode("utf-8")
    if tag == DDB_VALUE_BLOB:
        if not value.data or value.len == 0:
            return b""
        return bytes(ctypes.string_at(value.data, value.len))
    if tag == DDB_VALUE_DECIMAL:
        return decimal.Decimal(int(value.decimal_scaled)) / (
            decimal.Decimal(10) ** int(value.decimal_scale)
        )
    if tag == DDB_VALUE_UUID:
        return bytes(bytearray(value.uuid_bytes))
    if tag == DDB_VALUE_TIMESTAMP_MICROS:
        return _UNIX_EPOCH_UTC + datetime.timedelta(
            microseconds=int(value.timestamp_micros)
        )
    return None


def _string_out(lib, func, *args):
    out = ctypes.c_char_p()
    code = func(*args, ctypes.byref(out))
    if code != ERR_OK:
        _raise_error(code)
    try:
        return out.value.decode("utf-8") if out.value else ""
    finally:
        lib.ddb_string_free(ctypes.byref(out))


class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self._lib = load_library()
        self._stmt = None
        self._last_sql = None
        self._col_count = 0
        self._has_buffered_row = False
        self._buffered_row = None
        self._prefetched_rows = None
        self._query_active = False
        self.description = None
        self.rowcount = -1
        self.arraysize = 1
        self._closed = False
        self._rewrite_sql_cache = {}
        self._metadata_cache = {}
        self._bound_param_count = None
        self._is_direct_execute_sql_cache = {}
        self._should_buffer_first_row_sql_cache = {}
        self._should_prefetch_small_result_sql_cache = {}
        self._should_prefetch_zero_param_result_sql_cache = {}
        self._batch_rows = int(os.environ.get("DECENTDB_PY_BATCH_ROWS", "32768") or "32768")
        if self._batch_rows <= 0:
            self._batch_rows = 32768
        self._fetchall_chunk_rows = int(
            os.environ.get("DECENTDB_PY_FETCHALL_CHUNK_ROWS", "0") or "0"
        )
        if self._fetchall_chunk_rows < 0:
            self._fetchall_chunk_rows = 0
        self._use_row_view = (
            os.environ.get("DECENTDB_PY_USE_ROW_VIEW", "1") != "0"
            and hasattr(self._lib, "ddb_stmt_row_view")
        )
        self._use_step_row_view = self._use_row_view and hasattr(
            self._lib, "ddb_stmt_step_row_view"
        )
        self._use_bind_int64_step_row_view = self._use_step_row_view and hasattr(
            self._lib, "ddb_stmt_bind_int64_step_row_view"
        )
        self._use_fetch_row_views = self._use_row_view and hasattr(
            self._lib, "ddb_stmt_fetch_row_views"
        )
        self._use_batch_i64 = hasattr(self._lib, "ddb_stmt_execute_batch_i64")
        self._use_batch_i64_text_f64 = hasattr(
            self._lib, "ddb_stmt_execute_batch_i64_text_f64"
        )
        self._decode_row_i64_text_f64_native = (
            getattr(_fastdecode_native, "decode_row_i64_text_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_matrix_i64_text_f64_native = (
            getattr(_fastdecode_native, "decode_matrix_i64_text_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_row_i64_text_text_native = (
            getattr(_fastdecode_native, "decode_row_i64_text_text", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_matrix_i64_text_text_native = (
            getattr(_fastdecode_native, "decode_matrix_i64_text_text", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_row_i64_f64_text_native = (
            getattr(_fastdecode_native, "decode_row_i64_f64_text", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_matrix_i64_f64_text_native = (
            getattr(_fastdecode_native, "decode_matrix_i64_f64_text", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_row_text_i64_f64_native = (
            getattr(_fastdecode_native, "decode_row_text_i64_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_matrix_text_i64_f64_native = (
            getattr(_fastdecode_native, "decode_matrix_text_i64_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_row_i64_native = (
            getattr(_fastdecode_native, "decode_row_i64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_row_i64_f64_text_text_i64_f64_native = (
            getattr(_fastdecode_native, "decode_row_i64_f64_text_text_i64_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_matrix_i64_native = (
            getattr(_fastdecode_native, "decode_matrix_i64", None)
            if _fastdecode_native is not None
            else None
        )
        self._decode_matrix_i64_f64_text_text_i64_f64_native = (
            getattr(_fastdecode_native, "decode_matrix_i64_f64_text_text_i64_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_execute_batch_i64_text_f64 = (
            getattr(_fastdecode_native, "execute_batch_i64_text_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_execute_batch_i64_text_f64_iter = (
            getattr(_fastdecode_native, "execute_batch_i64_text_f64_iter", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_execute_batch_i64 = (
            getattr(_fastdecode_native, "execute_batch_i64", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_execute_batch_i64_iter = (
            getattr(_fastdecode_native, "execute_batch_i64_iter", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_execute_batch_typed_iter = (
            getattr(_fastdecode_native, "execute_batch_typed_iter", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_execute_batch_typed_collected = (
            getattr(_fastdecode_native, "execute_batch_typed_collected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_int64_step_row_view = (
            getattr(_fastdecode_native, "bind_int64_step_row_view", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_text_step_row_view = (
            getattr(_fastdecode_native, "bind_text_step_row_view", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_int64_fetch_all_row_views = (
            getattr(_fastdecode_native, "bind_int64_fetch_all_row_views", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_reset_bind_int64_fetch_all_row_views = (
            getattr(
                _fastdecode_native, "reset_bind_int64_fetch_all_row_views", None
            )
            if _fastdecode_native is not None
            else None
        )
        self._native_step_fetch_all_row_views = (
            getattr(_fastdecode_native, "step_fetch_all_row_views", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_reset_step_fetch_all_row_views = (
            getattr(
                _fastdecode_native, "reset_step_fetch_all_row_views", None
            )
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_text_fetch_all_row_views = (
            getattr(_fastdecode_native, "bind_text_fetch_all_row_views", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_f64_f64_fetch_all_row_views = (
            getattr(_fastdecode_native, "bind_float64_float64_fetch_all_row_views", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_int64_step_i64_text_f64 = (
            getattr(_fastdecode_native, "bind_int64_step_i64_text_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_i64_text_step = (
            getattr(_fastdecode_native, "bind_i64_text_step", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_i64_text_step_affected = (
            getattr(_fastdecode_native, "bind_i64_text_step_affected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_reset_bind_i64_text_step_affected = (
            getattr(_fastdecode_native, "reset_bind_i64_text_step_affected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_text_i64_step = (
            getattr(_fastdecode_native, "bind_text_i64_step", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_text_i64_step_affected = (
            getattr(_fastdecode_native, "bind_text_i64_step_affected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_reset_bind_text_i64_step_affected = (
            getattr(_fastdecode_native, "reset_bind_text_i64_step_affected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_int64_step_affected = (
            getattr(_fastdecode_native, "bind_int64_step_affected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_reset_bind_int64_step_affected = (
            getattr(_fastdecode_native, "reset_bind_int64_step_affected", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_fetch_rows_i64_text_f64 = (
            getattr(_fastdecode_native, "fetch_rows_i64_text_f64", None)
            if _fastdecode_native is not None
            else None
        )
        self._native_bind_int64_step_i64_text_f64_enabled = (
            self._native_bind_int64_step_i64_text_f64 is not None
        )
        self._native_bind_int64_step_row_view_enabled = (
            self._native_bind_int64_step_row_view is not None
        )
        self._native_bind_text_step_row_view_enabled = (
            self._native_bind_text_step_row_view is not None
        )
        self._native_bind_int64_fetch_all_row_views_enabled = (
            self._native_bind_int64_fetch_all_row_views is not None
        )
        self._native_step_fetch_all_row_views_enabled = (
            self._native_step_fetch_all_row_views is not None
        )
        self._native_bind_text_fetch_all_row_views_enabled = (
            self._native_bind_text_fetch_all_row_views is not None
        )
        self._native_bind_f64_f64_fetch_all_row_views_enabled = (
            self._native_bind_f64_f64_fetch_all_row_views is not None
        )
        self._native_bind_i64_text_step_enabled = self._native_bind_i64_text_step is not None
        self._native_bind_text_i64_step_enabled = self._native_bind_text_i64_step is not None
        self._native_bind_i64_text_step_affected_enabled = (
            self._native_bind_i64_text_step_affected is not None
        )
        self._native_reset_bind_i64_text_step_affected_enabled = (
            self._native_reset_bind_i64_text_step_affected is not None
        )
        self._native_bind_text_i64_step_affected_enabled = (
            self._native_bind_text_i64_step_affected is not None
        )
        self._native_reset_bind_text_i64_step_affected_enabled = (
            self._native_reset_bind_text_i64_step_affected is not None
        )
        self._native_bind_int64_step_affected_enabled = (
            self._native_bind_int64_step_affected is not None
        )
        self._native_reset_bind_int64_step_affected_enabled = (
            self._native_reset_bind_int64_step_affected is not None
        )
        self._native_fetch_rows_i64_text_f64_sql_support = {}
        self._decode_matrix_i64_text_f64_sql_support = {}
        self._decode_matrix_i64_text_text_sql_support = {}
        self._decode_matrix_i64_f64_text_sql_support = {}
        self._decode_matrix_text_i64_f64_sql_support = {}
        self._decode_matrix_i64_sql_support = {}
        self._decode_matrix_i64_f64_text_text_i64_f64_sql_support = {}
        self._native_bind_int64_step_row_view_sql_support = {}
        self._native_bind_text_step_row_view_sql_support = {}
        self._native_bind_int64_fetch_all_row_views_sql_support = {}
        self._native_step_fetch_all_row_views_sql_support = {}
        self._native_bind_text_fetch_all_row_views_sql_support = {}
        self._native_bind_f64_f64_fetch_all_row_views_sql_support = {}
        self._fast_repeat_cache = {}
        self._select_fast_info = {}
        self._cursor_stmt_slots = []

    def close(self):
        if self._closed:
            return
        recycled_stmts = set()
        if self._stmt:
            self._connection._recycle_statement(self._last_sql, self._stmt)
            recycled_stmts.add(id(self._stmt))
            self._stmt = None
        for slot_sql, slot_stmt in self._cursor_stmt_slots:
            recycled_stmts.add(id(slot_stmt))
            self._connection._recycle_statement(slot_sql, slot_stmt)
        self._cursor_stmt_slots.clear()
        self._fast_repeat_cache.clear()
        self._last_sql = None
        self._col_count = 0
        self._has_buffered_row = False
        self._buffered_row = None
        self._prefetched_rows = None
        self._query_active = False
        self.description = None
        self._metadata_cache.clear()
        self._bound_param_count = None
        self._closed = True

    def __del__(self):
        if getattr(self, "_closed", True):
            return
        try:
            self.close()
        except Exception:
            pass

    def _ensure_open(self):
        if self._closed:
            raise ProgrammingError("Cursor is closed")

    def _get_cached_metadata(self, sql):
        return self._metadata_cache.get(sql)

    def _store_cached_metadata(self, sql, col_count, description):
        self._metadata_cache[sql] = (col_count, description)

    def _store_cached_non_query_metadata(self, sql):
        self._metadata_cache[sql] = (0, None)

    def _load_description(self):
        count = ctypes.c_size_t()
        code = self._lib.ddb_stmt_column_count(self._stmt, ctypes.byref(count))
        if code != ERR_OK:
            _raise_error(code, sql=self._last_sql, params=None)
        self._col_count = int(count.value)
        if self._col_count == 0:
            self.description = None
            return
        desc = []
        for i in range(self._col_count):
            name = ctypes.c_char_p()
            code = self._lib.ddb_stmt_column_name_copy(
                self._stmt, i, ctypes.byref(name)
            )
            if code != ERR_OK:
                _raise_error(code, sql=self._last_sql, params=None)
            try:
                column_name = name.value.decode("utf-8") if name.value else ""
            finally:
                self._lib.ddb_string_free(ctypes.byref(name))
            desc.append((column_name, None, None, None, None, None, None))
        self.description = desc

    def _bind_param(self, index_1_based, param, sql, params):
        if param is None:
            code = self._lib.ddb_stmt_bind_null(self._stmt, index_1_based)
        elif isinstance(param, bool):
            code = self._lib.ddb_stmt_bind_bool(
                self._stmt, index_1_based, 1 if param else 0
            )
        elif isinstance(param, int):
            code = self._lib.ddb_stmt_bind_int64(self._stmt, index_1_based, param)
        elif isinstance(param, float):
            code = self._lib.ddb_stmt_bind_float64(self._stmt, index_1_based, param)
        elif isinstance(param, str):
            raw = param.encode("utf-8")
            code = self._lib.ddb_stmt_bind_text(self._stmt, index_1_based, raw, len(raw))
        elif isinstance(param, (bytes, bytearray, memoryview)):
            raw = bytes(param)
            if raw:
                array_type = ctypes.c_uint8 * len(raw)
                payload = array_type.from_buffer_copy(raw)
                code = self._lib.ddb_stmt_bind_blob(
                    self._stmt, index_1_based, payload, len(raw)
                )
            else:
                code = self._lib.ddb_stmt_bind_blob(
                    self._stmt, index_1_based, None, 0
                )
        elif isinstance(param, decimal.Decimal):
            t = param.as_tuple()
            exponent = t.exponent
            if not isinstance(exponent, int):
                raise DataError("Decimal NaN/Inf not supported")
            scale = -exponent
            if scale < 0:
                int_val = int(param)
                scale = 0
            elif scale > 18:
                quantized = param.quantize(decimal.Decimal(10) ** -18)
                scale = 18
                int_val = int(quantized * (decimal.Decimal(10) ** 18))
            else:
                int_val = int(param * (decimal.Decimal(10) ** scale))
            if int_val < -9223372036854775808 or int_val > 9223372036854775807:
                raise DataError("Decimal value too large for DecentDB")
            code = self._lib.ddb_stmt_bind_decimal(
                self._stmt, index_1_based, int_val, scale
            )
        elif isinstance(param, uuid.UUID):
            raw = param.bytes
            array_type = ctypes.c_uint8 * len(raw)
            payload = array_type.from_buffer_copy(raw)
            code = self._lib.ddb_stmt_bind_blob(
                self._stmt, index_1_based, payload, len(raw)
            )
        elif isinstance(param, datetime.datetime):
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            aware = (
                param.astimezone(datetime.timezone.utc)
                if param.tzinfo is not None
                else param.replace(tzinfo=datetime.timezone.utc)
            )
            micros = int((aware - epoch).total_seconds() * 1_000_000)
            code = self._lib.ddb_stmt_bind_timestamp_micros(
                self._stmt, index_1_based, micros
            )
        elif isinstance(param, datetime.date):
            epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
            dt = datetime.datetime(
                param.year, param.month, param.day, tzinfo=datetime.timezone.utc
            )
            micros = int((dt - epoch).total_seconds() * 1_000_000)
            code = self._lib.ddb_stmt_bind_timestamp_micros(
                self._stmt, index_1_based, micros
            )
        else:
            raw = str(param).encode("utf-8")
            code = self._lib.ddb_stmt_bind_text(self._stmt, index_1_based, raw, len(raw))

        if code != ERR_OK:
            _raise_error(code, sql=sql, params=params)

    def _execute_direct(self, sql, params):
        control_kind = _transaction_control_kind(sql)
        if control_kind == "begin":
            code = self._lib.ddb_db_begin_transaction(self._connection._db)
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            self._connection._in_explicit_txn = True
        elif control_kind == "commit":
            lsn = ctypes.c_uint64()
            code = self._lib.ddb_db_commit_transaction(self._connection._db, ctypes.byref(lsn))
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            self._connection._in_explicit_txn = False
        elif control_kind == "rollback":
            code = self._lib.ddb_db_rollback_transaction(self._connection._db)
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            self._connection._in_explicit_txn = False
        else:
            result = ctypes.c_void_p()
            code = self._lib.ddb_db_execute(
                self._connection._db, sql.encode("utf-8"), None, 0, ctypes.byref(result)
            )
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            try:
                self.description = None
                self._col_count = 0
                self._query_active = False
                self._has_buffered_row = False
                self._buffered_row = None
                self._prefetched_rows = None
                self.rowcount = 0
            finally:
                self._lib.ddb_result_free(ctypes.byref(result))
            return

        self.description = None
        self._col_count = 0
        self._query_active = False
        self._has_buffered_row = False
        self._buffered_row = None
        self._prefetched_rows = None
        self.rowcount = 0

    def _activate_statement(self, sql, params, param_count):
        self._prefetched_rows = None
        if self._stmt and self._last_sql != sql:
            for i, (slot_sql, slot_stmt) in enumerate(self._cursor_stmt_slots):
                if slot_sql == sql:
                    old_sql, old_stmt = self._last_sql, self._stmt
                    self._stmt = slot_stmt
                    self._last_sql = sql
                    self._cursor_stmt_slots[i] = (old_sql, old_stmt)
                    code = self._lib.ddb_stmt_reset(self._stmt)
                    if code != ERR_OK:
                        _raise_error(code, sql=sql, params=params)
                    self._bound_param_count = None
                    return
            if len(self._cursor_stmt_slots) < 4:
                self._cursor_stmt_slots.append((self._last_sql, self._stmt))
            else:
                evict_sql, evict_stmt = self._cursor_stmt_slots.pop(0)
                self._connection._recycle_statement(evict_sql, evict_stmt)
                self._cursor_stmt_slots.append((self._last_sql, self._stmt))
            self._stmt = None
            self._last_sql = None
            self._bound_param_count = None

        if self._stmt and self._last_sql == sql:
            code = self._lib.ddb_stmt_reset(self._stmt)
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            if self._bound_param_count is None or param_count is None:
                code = self._lib.ddb_stmt_clear_bindings(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
            elif self._bound_param_count != param_count:
                code = self._lib.ddb_stmt_clear_bindings(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
            return

        cached_stmt, hit = self._connection._get_cached_statement(sql)
        if hit:
            self._stmt = cached_stmt
            self._last_sql = sql
            code = self._lib.ddb_stmt_reset(self._stmt)
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            self._bound_param_count = None
            return

        stmt_ptr = ctypes.c_void_p()
        self._connection._stats["prepare_count"] += 1
        code = self._lib.ddb_db_prepare(
            self._connection._db, sql.encode("utf-8"), ctypes.byref(stmt_ptr)
        )
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=params)
        self._stmt = stmt_ptr
        self._last_sql = sql
        self._bound_param_count = None

    def _resolve_sql_and_params(self, operation, parameters):
        if parameters is None:
            cache_key = (operation, "none", 0)
        else:
            params_type = type(parameters)
            if params_type is tuple or params_type is list:
                cache_key = (operation, "seq", len(parameters))
            elif isinstance(parameters, Mapping):
                cache_key = None
            else:
                try:
                    param_count = len(parameters)
                except TypeError:
                    param_count = None
                cache_key = (operation, "seq", param_count)

        if cache_key is not None:
            cached_sql = self._rewrite_sql_cache.get(cache_key)
            if cached_sql is not None:
                params = [] if parameters is None else parameters
                return cached_sql, params

        sql, params = _convert_params(operation, parameters)
        if cache_key is not None:
            self._rewrite_sql_cache[cache_key] = sql
        return sql, params

    def _is_direct_execute_sql_cached(self, sql):
        cached = self._is_direct_execute_sql_cache.get(sql)
        if cached is not None:
            return cached
        cached = _is_direct_execute_sql(sql)
        self._is_direct_execute_sql_cache[sql] = cached
        return cached

    def _execute_current_statement(self, sql, params):
        bound_count = 0
        for i, param in enumerate(params, start=1):
            self._bind_param(i, param, sql, params)
            bound_count = i

        has_row = ctypes.c_uint8()
        code = self._lib.ddb_stmt_step(self._stmt, ctypes.byref(has_row))
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=params)
        return bool(has_row.value), bound_count

    def _execute_current_statement_bind_i64_step_row_view(self, sql, param, params):
        if self._native_bind_int64_step_i64_text_f64_enabled and self._stmt is not None:
            try:
                row = self._native_bind_int64_step_i64_text_f64(
                    self._stmt.value, param
                )
                if row is None:
                    return False, 1, None
                return True, 1, row
            except Exception:
                self._native_bind_int64_step_i64_text_f64_enabled = False

        if self._native_bind_int64_step_row_view_enabled and self._stmt is not None:
            native_supported = self._native_bind_int64_step_row_view_sql_support.get(sql, True)
            if native_supported:
                try:
                    row = self._native_bind_int64_step_row_view(self._stmt.value, param)
                    self._native_bind_int64_step_row_view_sql_support[sql] = True
                    if row is None:
                        return False, 1, None
                    return True, 1, row
                except Exception:
                    self._native_bind_int64_step_row_view_sql_support[sql] = False

        values_ptr = ctypes.POINTER(DdbValueView)()
        out_count = ctypes.c_size_t()
        has_row = ctypes.c_uint8()
        code = self._lib.ddb_stmt_bind_int64_step_row_view(
            self._stmt,
            1,
            param,
            ctypes.byref(values_ptr),
            ctypes.byref(out_count),
            ctypes.byref(has_row),
        )
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=params)
        if not has_row.value:
            return False, 1, None
        row = self._decode_row_view_values(values_ptr, int(out_count.value))
        return True, 1, row

    def _execute_current_statement_bind_text_step_row_view(self, sql, param, params):
        if self._native_bind_text_step_row_view_enabled and self._stmt is not None:
            native_supported = self._native_bind_text_step_row_view_sql_support.get(sql, True)
            if native_supported:
                try:
                    row = self._native_bind_text_step_row_view(self._stmt.value, param)
                    self._native_bind_text_step_row_view_sql_support[sql] = True
                    if row is None:
                        return False, 1, None
                    return True, 1, row
                except Exception:
                    self._native_bind_text_step_row_view_sql_support[sql] = False

        has_row, bound_count = self._execute_current_statement(sql, params)
        return has_row, bound_count, None

    def _execute_current_statement_bind_i64_text_step(self, sql, params):
        if self._native_bind_i64_text_step_affected_enabled and self._stmt is not None:
            try:
                affected_rows, has_row = self._native_bind_i64_text_step_affected(
                    self._stmt.value, params[0], params[1]
                )
                return bool(has_row), 2, int(affected_rows)
            except Exception:
                self._native_bind_i64_text_step_affected_enabled = False

        if self._native_bind_i64_text_step_enabled and self._stmt is not None:
            try:
                has_row = self._native_bind_i64_text_step(
                    self._stmt.value, params[0], params[1]
                )
                return bool(has_row), 2, None
            except Exception:
                self._native_bind_i64_text_step_enabled = False

        self._bind_param(1, params[0], sql, params)
        self._bind_param(2, params[1], sql, params)
        has_row = ctypes.c_uint8()
        code = self._lib.ddb_stmt_step(self._stmt, ctypes.byref(has_row))
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=params)
        return bool(has_row.value), 2, None

    def _execute_current_statement_bind_text_i64_step(self, sql, params):
        if self._native_bind_text_i64_step_affected_enabled and self._stmt is not None:
            try:
                affected_rows, has_row = self._native_bind_text_i64_step_affected(
                    self._stmt.value, params[0], params[1]
                )
                return bool(has_row), 2, int(affected_rows)
            except Exception:
                self._native_bind_text_i64_step_affected_enabled = False

        if self._native_bind_text_i64_step_enabled and self._stmt is not None:
            try:
                has_row = self._native_bind_text_i64_step(
                    self._stmt.value, params[0], params[1]
                )
                return bool(has_row), 2, None
            except Exception:
                self._native_bind_text_i64_step_enabled = False

        self._bind_param(1, params[0], sql, params)
        self._bind_param(2, params[1], sql, params)
        has_row = ctypes.c_uint8()
        code = self._lib.ddb_stmt_step(self._stmt, ctypes.byref(has_row))
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=params)
        return bool(has_row.value), 2, None

    def _execute_current_statement_bind_int64_step_affected(self, sql, param, params):
        if self._native_bind_int64_step_affected_enabled and self._stmt is not None:
            try:
                affected_rows, has_row = self._native_bind_int64_step_affected(
                    self._stmt.value, param
                )
                return bool(has_row), 1, int(affected_rows)
            except Exception:
                self._native_bind_int64_step_affected_enabled = False

        has_row, bound_count = self._execute_current_statement(sql, params)
        return has_row, bound_count, None

    def _execute_cached_non_query_current_statement(self, sql, params):
        metadata = self._get_cached_metadata(sql)
        if (
            self._stmt is None
            or self._last_sql != sql
            or metadata is None
            or metadata[0] != 0
        ):
            return None

        try:
            param_count = len(params)
        except TypeError:
            return None

        if param_count == 1 and type(params[0]) is int:
            if self._native_reset_bind_int64_step_affected_enabled:
                try:
                    affected_rowcount, has_row = self._native_reset_bind_int64_step_affected(
                        self._stmt.value, params[0]
                    )
                    bound_count = 1
                    affected_rowcount = int(affected_rowcount)
                except Exception:
                    self._native_reset_bind_int64_step_affected_enabled = False
                    code = self._lib.ddb_stmt_reset(self._stmt)
                    if code != ERR_OK:
                        _raise_error(code, sql=sql, params=params)
                    has_row, bound_count, affected_rowcount = (
                        self._execute_current_statement_bind_int64_step_affected(
                            sql, params[0], params
                        )
                    )
            else:
                code = self._lib.ddb_stmt_reset(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
                has_row, bound_count, affected_rowcount = (
                    self._execute_current_statement_bind_int64_step_affected(
                        sql, params[0], params
                    )
                )
        elif param_count == 2 and type(params[0]) is int and type(params[1]) is str:
            if self._native_reset_bind_i64_text_step_affected_enabled:
                try:
                    affected_rowcount, has_row = (
                        self._native_reset_bind_i64_text_step_affected(
                            self._stmt.value, params[0], params[1]
                        )
                    )
                    bound_count = 2
                    affected_rowcount = int(affected_rowcount)
                except Exception:
                    self._native_reset_bind_i64_text_step_affected_enabled = False
                    code = self._lib.ddb_stmt_reset(self._stmt)
                    if code != ERR_OK:
                        _raise_error(code, sql=sql, params=params)
                    has_row, bound_count, affected_rowcount = (
                        self._execute_current_statement_bind_i64_text_step(sql, params)
                    )
            else:
                code = self._lib.ddb_stmt_reset(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
                has_row, bound_count, affected_rowcount = (
                    self._execute_current_statement_bind_i64_text_step(sql, params)
                )
        elif param_count == 2 and type(params[0]) is str and type(params[1]) is int:
            if self._native_reset_bind_text_i64_step_affected_enabled:
                try:
                    affected_rowcount, has_row = (
                        self._native_reset_bind_text_i64_step_affected(
                            self._stmt.value, params[0], params[1]
                        )
                    )
                    bound_count = 2
                    affected_rowcount = int(affected_rowcount)
                except Exception:
                    self._native_reset_bind_text_i64_step_affected_enabled = False
                    code = self._lib.ddb_stmt_reset(self._stmt)
                    if code != ERR_OK:
                        _raise_error(code, sql=sql, params=params)
                    has_row, bound_count, affected_rowcount = (
                        self._execute_current_statement_bind_text_i64_step(sql, params)
                    )
            else:
                code = self._lib.ddb_stmt_reset(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
                has_row, bound_count, affected_rowcount = (
                    self._execute_current_statement_bind_text_i64_step(sql, params)
                )
        else:
            return None

        self._bound_param_count = bound_count
        self._col_count = 0
        self.description = None
        self._query_active = False
        self._has_buffered_row = False
        self._buffered_row = None
        self._prefetched_rows = None
        if affected_rowcount is None:
            affected = ctypes.c_uint64()
            code = self._lib.ddb_stmt_affected_rows(self._stmt, ctypes.byref(affected))
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            affected_rowcount = int(affected.value)
        self.rowcount = affected_rowcount
        return self

    def _execute_last_statement_fast(self, operation, parameters):
        params_type = type(parameters)
        if params_type is tuple or params_type is list:
            params = parameters
            sql = self._rewrite_sql_cache.get((operation, "seq", len(parameters)))
        else:
            return None
        if sql is None or self._stmt is None:
            return None
        if self._last_sql != sql:
            found = False
            for i, (slot_sql, slot_stmt) in enumerate(self._cursor_stmt_slots):
                if slot_sql == sql:
                    self._cursor_stmt_slots[i] = (self._last_sql, self._stmt)
                    self._stmt = slot_stmt
                    self._last_sql = sql
                    found = True
                    break
            if not found:
                return None
        metadata = self._get_cached_metadata(sql)
        if metadata != (0, None):
            return None
        return self._execute_cached_non_query_current_statement(sql, params)

    def _should_prefetch_small_result(self, sql):
        cached = self._should_prefetch_small_result_sql_cache.get(sql)
        if cached is not None:
            return cached
        normalized = " ".join(sql.lower().split())
        cached = (
            self._should_buffer_first_row(sql)
            or "count(" in normalized
            or " limit " in f" {normalized} "
            or "order by o.id desc" in normalized
        )
        self._should_prefetch_small_result_sql_cache[sql] = cached
        return cached

    def _should_prefetch_zero_param_result(self, sql):
        cached = self._should_prefetch_zero_param_result_sql_cache.get(sql)
        if cached is not None:
            return cached
        normalized = " ".join(sql.lower().split())
        cached = "count(" in normalized or " limit " in f" {normalized} "
        self._should_prefetch_zero_param_result_sql_cache[sql] = cached
        return cached

    def _setup_fast_repeat(self, operation, parameters):
        if parameters is None or self._stmt is None:
            return
        if not isinstance(parameters, (tuple, list)):
            return
        try:
            param_count = len(parameters)
        except TypeError:
            return
        code = 0
        if param_count == 1 and type(parameters[0]) is int:
            if self._native_reset_bind_int64_step_affected_enabled:
                code = 1
        elif param_count == 2:
            t0 = type(parameters[0])
            t1 = type(parameters[1])
            if t0 is str and t1 is int:
                if self._native_reset_bind_text_i64_step_affected_enabled:
                    code = 2
            elif t0 is int and t1 is str:
                if self._native_reset_bind_i64_text_step_affected_enabled:
                    code = 3
        if code:
            self._fast_repeat_cache[id(operation)] = (code, operation, self._last_sql)

    def _fast_repeat_find_stmt(self, cached_sql):
        """Find a stmt by SQL, checking active stmt then LRU slots."""
        if self._last_sql == cached_sql:
            return self._stmt
        for i, (slot_sql, slot_stmt) in enumerate(self._cursor_stmt_slots):
            if slot_sql == cached_sql:
                old_sql, old_stmt = self._last_sql, self._stmt
                self._stmt = slot_stmt
                self._last_sql = cached_sql
                self._cursor_stmt_slots[i] = (old_sql, old_stmt)
                return slot_stmt
        return None

    def _reset_statement_after_native_prefetch_failure(self):
        if self._stmt is None:
            return
        try:
            self._lib.ddb_stmt_reset(self._stmt)
            self._lib.ddb_stmt_clear_bindings(self._stmt)
        except Exception:
            pass

    def _execute_current_statement_bind_int64_fetch_all_row_views(self, sql, param):
        native_supported = self._native_bind_int64_fetch_all_row_views_sql_support.get(sql, True)
        if (
            self._native_bind_int64_fetch_all_row_views_enabled
            and self._stmt is not None
            and native_supported
        ):
            try:
                rows = self._native_bind_int64_fetch_all_row_views(
                    self._stmt.value, param
                )
                self._native_bind_int64_fetch_all_row_views_sql_support[sql] = True
                return rows
            except Exception:
                self._reset_statement_after_native_prefetch_failure()
                self._native_bind_int64_fetch_all_row_views_sql_support[sql] = False
        return None

    def _execute_current_statement_step_fetch_all_row_views(self, sql):
        native_supported = self._native_step_fetch_all_row_views_sql_support.get(sql, True)
        if (
            self._native_step_fetch_all_row_views_enabled
            and self._stmt is not None
            and native_supported
        ):
            try:
                rows = self._native_step_fetch_all_row_views(self._stmt.value)
                self._native_step_fetch_all_row_views_sql_support[sql] = True
                return rows
            except Exception:
                self._reset_statement_after_native_prefetch_failure()
                self._native_step_fetch_all_row_views_sql_support[sql] = False
        return None

    def _execute_current_statement_bind_text_fetch_all_row_views(self, sql, param):
        native_supported = self._native_bind_text_fetch_all_row_views_sql_support.get(sql, True)
        if (
            self._native_bind_text_fetch_all_row_views_enabled
            and self._stmt is not None
            and native_supported
        ):
            try:
                rows = self._native_bind_text_fetch_all_row_views(
                    self._stmt.value, param
                )
                self._native_bind_text_fetch_all_row_views_sql_support[sql] = True
                return rows
            except Exception:
                self._reset_statement_after_native_prefetch_failure()
                self._native_bind_text_fetch_all_row_views_sql_support[sql] = False
        return None

    def _execute_current_statement_bind_f64_f64_fetch_all_row_views(self, sql, params):
        native_supported = self._native_bind_f64_f64_fetch_all_row_views_sql_support.get(
            sql, True
        )
        if (
            self._native_bind_f64_f64_fetch_all_row_views_enabled
            and self._stmt is not None
            and native_supported
        ):
            try:
                rows = self._native_bind_f64_f64_fetch_all_row_views(
                    self._stmt.value, params[0], params[1]
                )
                self._native_bind_f64_f64_fetch_all_row_views_sql_support[sql] = True
                return rows
            except Exception:
                self._reset_statement_after_native_prefetch_failure()
                self._native_bind_f64_f64_fetch_all_row_views_sql_support[sql] = False
        return None

    @staticmethod
    def _row_is_single_i64(params):
        if len(params) != 1:
            return False
        return type(params[0]) is int

    @staticmethod
    def _row_matches_signature(params, signature):
        if len(params) != len(signature):
            return False
        for value, code in zip(params, signature):
            if code == "i":
                if type(value) is not int:
                    return False
            elif code == "t":
                if type(value) is not str:
                    return False
            elif code == "f":
                if type(value) not in (float, int):
                    return False
            else:
                return False
        return True

    @staticmethod
    def _row_is_i64_text_f64(params):
        return Cursor._row_matches_signature(params, "itf")

    @staticmethod
    def _row_is_i64_text_text(params):
        return Cursor._row_matches_signature(params, "itt")

    @staticmethod
    def _row_is_i64_i64_text_f64(params):
        return Cursor._row_matches_signature(params, "iitf")

    def _should_buffer_first_row(self, sql):
        cached = self._should_buffer_first_row_sql_cache.get(sql)
        if cached is not None:
            return cached
        normalized = sql.lstrip().upper()
        if not normalized.startswith(("SELECT", "WITH", "EXPLAIN", "VALUES")):
            self._should_buffer_first_row_sql_cache[sql] = False
            return False
        cached = not any(
            token in normalized
            for token in (" JOIN ", " ORDER BY ", " GROUP BY ", " LIMIT ", " OFFSET ")
        )
        self._should_buffer_first_row_sql_cache[sql] = cached
        return cached

    def _execute_batch_i64_text_f64_chunk(self, sql, rows):
        row_count = len(rows)
        if row_count == 0:
            return 0
        if self._native_execute_batch_i64_text_f64 is not None and self._stmt is not None:
            try:
                return int(
                    self._native_execute_batch_i64_text_f64(self._stmt.value, rows)
                )
            except Exception:
                pass

        ids = (ctypes.c_int64 * row_count)()
        text_ptrs = (ctypes.c_char_p * row_count)()
        text_lens = (ctypes.c_size_t * row_count)()
        floats = (ctypes.c_double * row_count)()
        text_payloads = []

        for idx, params in enumerate(rows):
            id_value, text_value, float_value = params
            ids[idx] = id_value
            raw = text_value.encode("utf-8")
            text_payloads.append(raw)
            text_ptrs[idx] = raw
            text_lens[idx] = len(raw)
            floats[idx] = float(float_value)

        out_affected = ctypes.c_uint64()
        code = self._lib.ddb_stmt_execute_batch_i64_text_f64(
            self._stmt,
            row_count,
            ids,
            text_ptrs,
            text_lens,
            floats,
            ctypes.byref(out_affected),
        )
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=rows[0])
        return int(out_affected.value)

    def _execute_batch_i64_chunk(self, sql, rows):
        row_count = len(rows)
        if row_count == 0:
            return 0
        if self._native_execute_batch_i64 is not None and self._stmt is not None:
            try:
                return int(self._native_execute_batch_i64(self._stmt.value, rows))
            except Exception:
                pass

        ids = (ctypes.c_int64 * row_count)()
        for idx, params in enumerate(rows):
            ids[idx] = params[0]

        out_affected = ctypes.c_uint64()
        code = self._lib.ddb_stmt_execute_batch_i64(
            self._stmt,
            row_count,
            ids,
            ctypes.byref(out_affected),
        )
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=rows[0])
        return int(out_affected.value)

    def _executemany_i64(self, sql, expected_count, first_params, iterator):
        if not self._use_batch_i64:
            return None
        if expected_count != 1 or not self._row_is_single_i64(first_params):
            return None

        step_out = ctypes.c_uint8()
        step_stmt = self._lib.ddb_stmt_step
        bind_param = self._bind_param
        reset_stmt = self._lib.ddb_stmt_reset

        batch_size = self._batch_rows

        if self._native_execute_batch_i64_iter is not None and self._stmt is not None:
            total_affected = int(
                self._native_execute_batch_i64_iter(
                    self._stmt.value,
                    first_params,
                    iterator,
                    batch_size,
                )
            )
            self._bound_param_count = expected_count
            return total_affected

        total_affected = 0
        fast_batch = [first_params]

        def flush_fast_batch():
            nonlocal total_affected
            if not fast_batch:
                return
            total_affected += self._execute_batch_i64_chunk(sql, fast_batch)
            fast_batch.clear()

        def execute_row_generic(params):
            code = reset_stmt(self._stmt)
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            bind_param(1, params[0], sql, params)
            code = step_stmt(self._stmt, ctypes.byref(step_out))
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)

        for params in iterator:
            params_type = type(params)
            if params_type is tuple or params_type is list:
                param_len = len(params)
            else:
                if isinstance(params, Mapping):
                    raise ProgrammingError(
                        "Mixed parameter styles are not supported in executemany"
                    )
                try:
                    param_len = len(params)
                except TypeError:
                    raise ProgrammingError(
                        "Incorrect number of parameters: "
                        f"expected {expected_count}, got unknown"
                    )
            if param_len != expected_count:
                raise ProgrammingError(
                    f"Incorrect number of parameters: expected {expected_count}, got {param_len}"
                )

            if self._row_is_single_i64(params):
                fast_batch.append(params)
                if len(fast_batch) >= batch_size:
                    flush_fast_batch()
                continue

            flush_fast_batch()
            execute_row_generic(params)

        flush_fast_batch()
        self._bound_param_count = expected_count
        return total_affected

    def _executemany_i64_text_f64(self, sql, expected_count, first_params, iterator):
        if not self._use_batch_i64_text_f64:
            return None
        if expected_count != 3 or not self._row_is_i64_text_f64(first_params):
            return None

        step_out = ctypes.c_uint8()
        step_stmt = self._lib.ddb_stmt_step
        bind_param = self._bind_param
        reset_stmt = self._lib.ddb_stmt_reset

        batch_size = self._batch_rows

        if (
            self._native_execute_batch_i64_text_f64_iter is not None
            and self._stmt is not None
        ):
            total_affected = int(
                self._native_execute_batch_i64_text_f64_iter(
                    self._stmt.value,
                    first_params,
                    iterator,
                    batch_size,
                )
            )
            self._bound_param_count = expected_count
            return total_affected

        total_affected = 0
        fast_batch = [first_params]

        def flush_fast_batch():
            nonlocal total_affected
            if not fast_batch:
                return
            total_affected += self._execute_batch_i64_text_f64_chunk(sql, fast_batch)
            fast_batch.clear()

        def execute_row_generic(params):
            code = reset_stmt(self._stmt)
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)
            for i, param in enumerate(params, start=1):
                bind_param(i, param, sql, params)
            code = step_stmt(self._stmt, ctypes.byref(step_out))
            if code != ERR_OK:
                _raise_error(code, sql=sql, params=params)

        for params in iterator:
            params_type = type(params)
            if params_type is tuple or params_type is list:
                param_len = len(params)
            else:
                if isinstance(params, Mapping):
                    raise ProgrammingError(
                        "Mixed parameter styles are not supported in executemany"
                    )
                try:
                    param_len = len(params)
                except TypeError:
                    raise ProgrammingError(
                        "Incorrect number of parameters: "
                        f"expected {expected_count}, got unknown"
                    )
            if param_len != expected_count:
                raise ProgrammingError(
                    f"Incorrect number of parameters: expected {expected_count}, got {param_len}"
                )

            if self._row_is_i64_text_f64(params):
                fast_batch.append(params)
                if len(fast_batch) >= batch_size:
                    flush_fast_batch()
                continue

            flush_fast_batch()
            execute_row_generic(params)

        flush_fast_batch()
        self._bound_param_count = expected_count
        return total_affected

    def _executemany_typed_iter(
        self, expected_count, first_params, iterator, signature, row_predicate
    ):
        if self._stmt is None:
            return None
        if len(signature) != expected_count or not row_predicate(first_params):
            return None

        # Prefer the collected batch path (single FFI crossing).
        if self._native_execute_batch_typed_collected is not None:
            def checked_rows():
                for params in iterator:
                    params_type = type(params)
                    if params_type is tuple or params_type is list:
                        param_len = len(params)
                    else:
                        if isinstance(params, Mapping):
                            raise ProgrammingError(
                                "Mixed parameter styles are not supported in executemany"
                            )
                        try:
                            param_len = len(params)
                        except TypeError:
                            raise ProgrammingError(
                                "Incorrect number of parameters: "
                                f"expected {expected_count}, got unknown"
                            )
                        if param_len != expected_count:
                            raise ProgrammingError(
                                f"Incorrect number of parameters: expected {expected_count}, got {param_len}"
                            )
                    yield params

            total_affected = int(
                self._native_execute_batch_typed_collected(
                    self._stmt.value, first_params, checked_rows(), signature
                )
            )
            self._bound_param_count = expected_count
            return total_affected

        if self._native_execute_batch_typed_iter is None:
            return None

        def checked_rows():
            for params in iterator:
                params_type = type(params)
                if params_type is tuple or params_type is list:
                    param_len = len(params)
                else:
                    if isinstance(params, Mapping):
                        raise ProgrammingError(
                            "Mixed parameter styles are not supported in executemany"
                        )
                    try:
                        param_len = len(params)
                    except TypeError:
                        raise ProgrammingError(
                            "Incorrect number of parameters: "
                            f"expected {expected_count}, got unknown"
                        )
                if param_len != expected_count:
                    raise ProgrammingError(
                        f"Incorrect number of parameters: expected {expected_count}, got {param_len}"
                    )
                yield params

        total_affected = int(
            self._native_execute_batch_typed_iter(
                self._stmt.value, first_params, checked_rows(), signature
            )
        )
        self._bound_param_count = expected_count
        return total_affected

    def execute(self, operation, parameters=None):
        if (
            parameters is None
            and self._fast_repeat_cache
        ):
            cached = self._fast_repeat_cache.get(id(operation))
            if cached is not None:
                frc, op_ref, cached_sql = cached
                if frc == 5 and op_ref is operation:
                    stmt = self._fast_repeat_find_stmt(cached_sql)
                    if stmt is not None:
                        try:
                            rows = (
                                self._native_reset_step_fetch_all_row_views(
                                    stmt.value
                                )
                            )
                            sel_info = self._select_fast_info.get(
                                cached_sql
                            )
                            if sel_info is not None:
                                self._has_buffered_row = False
                                self._buffered_row = None
                                self._prefetched_rows = rows
                                self._query_active = True
                                self.description = sel_info[0]
                                self._col_count = sel_info[1]
                                self.rowcount = -1
                                return self
                        except Exception:
                            self._fast_repeat_cache.clear()
        if parameters is not None and self._fast_repeat_cache:
            cached = self._fast_repeat_cache.get(id(operation))
            if cached is not None:
                frc, op_ref, cached_sql = cached
                if op_ref is operation:
                    stmt = self._fast_repeat_find_stmt(cached_sql)
                    if stmt is not None:
                        try:
                            sv = stmt.value
                            if frc == 1:
                                affected, _ = (
                                    self._native_reset_bind_int64_step_affected(
                                        sv, parameters[0]
                                    )
                                )
                            elif frc == 2:
                                affected, _ = (
                                    self._native_reset_bind_text_i64_step_affected(
                                        sv, parameters[0], parameters[1]
                                    )
                                )
                            elif frc == 3:
                                affected, _ = (
                                    self._native_reset_bind_i64_text_step_affected(
                                        sv, parameters[0], parameters[1]
                                    )
                                )
                            elif frc == 4:
                                rows = (
                                    self._native_reset_bind_int64_fetch_all_row_views(
                                        sv, parameters[0]
                                    )
                                )
                                sel_info = self._select_fast_info.get(
                                    cached_sql
                                )
                                if sel_info is not None:
                                    self._has_buffered_row = False
                                    self._buffered_row = None
                                    self._prefetched_rows = rows
                                    self._query_active = True
                                    self.description = sel_info[0]
                                    self._col_count = sel_info[1]
                                    self.rowcount = -1
                                    return self
                                affected = None
                            if affected is not None:
                                self.rowcount = int(affected)
                                self._has_buffered_row = False
                                self._query_active = False
                                return self
                        except Exception:
                            self._fast_repeat_cache.clear()

        self._ensure_open()
        self._has_buffered_row = False
        self._buffered_row = None
        self._prefetched_rows = None
        self._query_active = False
        self.description = None
        self.rowcount = -1

        fast_current = self._execute_last_statement_fast(operation, parameters)
        if fast_current is not None:
            self._setup_fast_repeat(operation, parameters)
            return fast_current

        sql, params = self._resolve_sql_and_params(operation, parameters)
        try:
            param_count = len(params)
        except TypeError:
            param_count = None

        if self._is_direct_execute_sql_cached(sql):
            if self._stmt and _transaction_control_kind(sql) is None:
                self._connection._recycle_statement(self._last_sql, self._stmt)
                self._stmt = None
                self._last_sql = None
                self._bound_param_count = None
            self._execute_direct(sql, params)
        else:
            cached_non_query = self._execute_cached_non_query_current_statement(sql, params)
            if cached_non_query is not None:
                self._setup_fast_repeat(operation, parameters)
                return cached_non_query
            self._activate_statement(sql, params, param_count)
            affected_rowcount = None
            prefetched_rows = None
            prefetched_bound_count = None
            if (
                self._use_fetch_row_views
                and param_count == 0
                and self._should_prefetch_zero_param_result(sql)
            ):
                prefetched_rows = self._execute_current_statement_step_fetch_all_row_views(sql)
                if prefetched_rows is not None:
                    prefetched_bound_count = 0
            elif (
                self._use_fetch_row_views
                and param_count == 1
                and type(params[0]) is int
                and self._should_prefetch_small_result(sql)
            ):
                prefetched_rows = self._execute_current_statement_bind_int64_fetch_all_row_views(
                    sql, params[0]
                )
                if prefetched_rows is not None:
                    prefetched_bound_count = 1
            elif (
                self._use_fetch_row_views
                and param_count == 1
                and type(params[0]) is str
                and self._should_prefetch_small_result(sql)
            ):
                prefetched_rows = self._execute_current_statement_bind_text_fetch_all_row_views(
                    sql, params[0]
                )
                if prefetched_rows is not None:
                    prefetched_bound_count = 1
            elif (
                self._use_fetch_row_views
                and param_count == 2
                and type(params[0]) is float
                and type(params[1]) is float
                and self._should_prefetch_small_result(sql)
            ):
                prefetched_rows = (
                    self._execute_current_statement_bind_f64_f64_fetch_all_row_views(sql, params)
                )
                if prefetched_rows is not None:
                    prefetched_bound_count = 2
            if prefetched_rows is not None:
                has_row = bool(prefetched_rows)
                bound_count = 0 if prefetched_bound_count is None else prefetched_bound_count
                buffered_row = None
            elif (
                self._use_bind_int64_step_row_view
                and self._should_buffer_first_row(sql)
                and param_count == 1
                and type(params[0]) is int
            ):
                has_row, bound_count, buffered_row = (
                    self._execute_current_statement_bind_i64_step_row_view(
                        sql, params[0], params
                    )
                )
            elif (
                self._native_bind_text_step_row_view_enabled
                and self._use_fetch_row_views
                and param_count == 1
                and type(params[0]) is str
            ):
                has_row, bound_count, buffered_row = (
                    self._execute_current_statement_bind_text_step_row_view(
                        sql, params[0], params
                    )
                )
            elif (
                self._get_cached_metadata(sql) == (0, None)
                and param_count == 1
                and type(params[0]) is int
            ):
                has_row, bound_count, affected_rowcount = (
                    self._execute_current_statement_bind_int64_step_affected(
                        sql, params[0], params
                    )
                )
                buffered_row = None
            elif (
                param_count == 2
                and type(params[0]) is int
                and type(params[1]) is str
            ):
                has_row, bound_count, affected_rowcount = (
                    self._execute_current_statement_bind_i64_text_step(
                        sql, params
                    )
                )
                buffered_row = None
            elif (
                param_count == 2
                and type(params[0]) is str
                and type(params[1]) is int
            ):
                has_row, bound_count, affected_rowcount = (
                    self._execute_current_statement_bind_text_i64_step(
                        sql, params
                    )
                )
                buffered_row = None
            else:
                has_row, bound_count = self._execute_current_statement(sql, params)
                buffered_row = None
            self._bound_param_count = bound_count

            metadata = self._get_cached_metadata(sql)
            if metadata is not None:
                self._col_count, self.description = metadata
            else:
                self._load_description()
                self._store_cached_metadata(sql, self._col_count, self.description)
            self._query_active = self._col_count > 0
            self._has_buffered_row = has_row and self._query_active
            self._buffered_row = buffered_row if self._has_buffered_row else None
            self._prefetched_rows = (
                prefetched_rows if self._query_active and prefetched_rows is not None else None
            )
            if self._prefetched_rows is not None:
                self._has_buffered_row = False
                self._buffered_row = None

            if self._query_active:
                self.rowcount = -1
                if (
                    self._prefetched_rows is not None
                    and self._native_reset_bind_int64_fetch_all_row_views
                        is not None
                    and param_count == 1
                    and type(params[0]) is int
                ):
                    self._select_fast_info[sql] = (
                        self.description,
                        self._col_count,
                    )
                    self._fast_repeat_cache[id(operation)] = (
                        4,
                        operation,
                        sql,
                    )
                elif (
                    self._prefetched_rows is not None
                    and self._native_reset_step_fetch_all_row_views
                        is not None
                    and param_count == 0
                ):
                    self._select_fast_info[sql] = (
                        self.description,
                        self._col_count,
                    )
                    self._fast_repeat_cache[id(operation)] = (
                        5,
                        operation,
                        sql,
                    )
            else:
                self._buffered_row = None
                self._prefetched_rows = None
                if affected_rowcount is None:
                    affected = ctypes.c_uint64()
                    code = self._lib.ddb_stmt_affected_rows(
                        self._stmt, ctypes.byref(affected)
                    )
                    if code != ERR_OK:
                        _raise_error(code, sql=sql, params=params)
                    affected_rowcount = int(affected.value)
                self.rowcount = affected_rowcount
        return self

    def executemany(self, operation, seq_of_parameters):
        self._ensure_open()
        iterator = iter(seq_of_parameters)
        try:
            first_params = next(iterator)
        except StopIteration:
            return self

        # Fast path for repeated positional DML binds.
        if isinstance(first_params, Mapping):
            self.execute(operation, first_params)
            for params in iterator:
                self.execute(operation, params)
            return self

        try:
            expected_count = len(first_params)
        except TypeError:
            self.execute(operation, first_params)
            for params in iterator:
                self.execute(operation, params)
            return self

        sql, normalized_first = self._resolve_sql_and_params(operation, first_params)
        if _is_direct_execute_sql(sql):
            self.execute(operation, first_params)
            for params in iterator:
                self.execute(operation, params)
            return self

        self._has_buffered_row = False
        self._query_active = False
        self.description = None
        self.rowcount = -1

        self._activate_statement(sql, normalized_first, expected_count)

        fast_rowcount = self._executemany_i64(
            sql, expected_count, normalized_first, iterator
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        fast_rowcount = self._executemany_typed_iter(
            expected_count,
            normalized_first,
            iterator,
            "itt",
            self._row_is_i64_text_text,
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        fast_rowcount = self._executemany_typed_iter(
            expected_count,
            normalized_first,
            iterator,
            "itfi",
            lambda params: self._row_matches_signature(params, "itfi"),
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        fast_rowcount = self._executemany_typed_iter(
            expected_count,
            normalized_first,
            iterator,
            "iitf",
            self._row_is_i64_i64_text_f64,
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        fast_rowcount = self._executemany_typed_iter(
            expected_count,
            normalized_first,
            iterator,
            "iiif",
            lambda params: self._row_matches_signature(params, "iiif"),
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        fast_rowcount = self._executemany_typed_iter(
            expected_count,
            normalized_first,
            iterator,
            "iiftt",
            lambda params: self._row_matches_signature(params, "iiftt"),
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        fast_rowcount = self._executemany_i64_text_f64(
            sql, expected_count, normalized_first, iterator
        )
        if fast_rowcount is not None:
            self._col_count = 0
            self.description = None
            self._store_cached_non_query_metadata(sql)
            self._query_active = False
            self._has_buffered_row = False
            self.rowcount = fast_rowcount
            return self

        step_out = ctypes.c_uint8()
        step_stmt = self._lib.ddb_stmt_step
        byref = ctypes.byref
        bind_param = self._bind_param
        bind_null = self._lib.ddb_stmt_bind_null
        bind_bool = self._lib.ddb_stmt_bind_bool
        bind_int64 = self._lib.ddb_stmt_bind_int64
        bind_float64 = self._lib.ddb_stmt_bind_float64
        bind_text = self._lib.ddb_stmt_bind_text
        err_ok = ERR_OK

        def bind_row_fast(params):
            for i, param in enumerate(params, start=1):
                ptype = type(param)
                if param is None:
                    code = bind_null(self._stmt, i)
                elif ptype is bool:
                    code = bind_bool(self._stmt, i, 1 if param else 0)
                elif ptype is int:
                    code = bind_int64(self._stmt, i, param)
                elif ptype is float:
                    code = bind_float64(self._stmt, i, param)
                elif ptype is str:
                    raw = param.encode("utf-8")
                    code = bind_text(self._stmt, i, raw, len(raw))
                else:
                    bind_param(i, param, sql, params)
                    continue

                if code != err_ok:
                    _raise_error(code, sql=sql, params=params)

        bind_row_fast(normalized_first)
        code = step_stmt(self._stmt, byref(step_out))
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=normalized_first)
        self._bound_param_count = expected_count

        if "?" in operation:
            for params in iterator:
                try:
                    param_len = len(params)
                except TypeError:
                    raise ProgrammingError(
                        "Incorrect number of parameters: "
                        f"expected {expected_count}, got unknown"
                    )
                if param_len != expected_count:
                    raise ProgrammingError(
                        f"Incorrect number of parameters: expected {expected_count}, got {param_len}"
                    )
                code = self._lib.ddb_stmt_reset(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
                bind_row_fast(params)
                code = step_stmt(self._stmt, byref(step_out))
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
        else:
            for params in iterator:
                if isinstance(params, Mapping):
                    raise ProgrammingError(
                        "Mixed parameter styles are not supported in executemany"
                    )
                try:
                    param_len = len(params)
                except TypeError:
                    raise ProgrammingError(
                        "Incorrect number of parameters: "
                        f"expected {expected_count}, got unknown"
                    )
                if param_len != expected_count:
                    raise ProgrammingError(
                        f"Incorrect number of parameters: expected {expected_count}, got {param_len}"
                    )
                code = self._lib.ddb_stmt_reset(self._stmt)
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)
                bind_row_fast(params)
                code = step_stmt(self._stmt, byref(step_out))
                if code != ERR_OK:
                    _raise_error(code, sql=sql, params=params)

        count = ctypes.c_size_t()
        code = self._lib.ddb_stmt_column_count(self._stmt, ctypes.byref(count))
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=None)
        self._col_count = int(count.value)

        if self._col_count > 0:
            self._load_description()
            self._store_cached_metadata(sql, self._col_count, self.description)
            self._query_active = True
            self._has_buffered_row = True
            self.rowcount = -1
            return self

        self.description = None
        self._store_cached_non_query_metadata(sql)
        self._query_active = False
        self._has_buffered_row = False
        affected = ctypes.c_uint64()
        code = self._lib.ddb_stmt_affected_rows(self._stmt, ctypes.byref(affected))
        if code != ERR_OK:
            _raise_error(code, sql=sql, params=None)
        self.rowcount = int(affected.value)
        return self

    def _decode_current_row(self):
        if self._use_row_view:
            return self._decode_current_row_view()

        row = []
        value = DdbValue()
        code = self._lib.ddb_value_init(ctypes.byref(value))
        if code != ERR_OK:
            _raise_error(code, sql=self._last_sql, params=None)
        needs_dispose = False
        string_at = ctypes.string_at
        append_row = row.append
        ddb_stmt_value_copy = self._lib.ddb_stmt_value_copy
        ddb_value_dispose = self._lib.ddb_value_dispose
        byref = ctypes.byref
        text_tag = DDB_VALUE_TEXT
        blob_tag = DDB_VALUE_BLOB
        try:
            for column_index in range(self._col_count):
                if needs_dispose:
                    dispose = ddb_value_dispose(byref(value))
                    if dispose != ERR_OK:
                        _raise_error(dispose, sql=self._last_sql, params=None)
                    needs_dispose = False

                code = ddb_stmt_value_copy(self._stmt, column_index, byref(value))
                if code != ERR_OK:
                    _raise_error(code, sql=self._last_sql, params=None)

                tag = int(value.tag)
                if tag == DDB_VALUE_NULL:
                    append_row(None)
                elif tag == DDB_VALUE_INT64:
                    append_row(int(value.int64_value))
                elif tag == DDB_VALUE_FLOAT64:
                    append_row(float(value.float64_value))
                elif tag == DDB_VALUE_BOOL:
                    append_row(bool(value.bool_value))
                elif tag == text_tag:
                    if not value.data or value.len == 0:
                        append_row("")
                    else:
                        append_row(string_at(value.data, value.len).decode("utf-8"))
                    needs_dispose = True
                elif tag == blob_tag:
                    if not value.data or value.len == 0:
                        append_row(b"")
                    else:
                        append_row(bytes(string_at(value.data, value.len)))
                    needs_dispose = True
                else:
                    append_row(_decode_ffi_value(self._lib, value))
                    needs_dispose = False
        finally:
            if needs_dispose:
                dispose = ddb_value_dispose(byref(value))
                if dispose != ERR_OK:
                    _raise_error(dispose, sql=self._last_sql, params=None)
        return tuple(row)

    def _decode_current_row_view(self):
        values_ptr = ctypes.POINTER(DdbValueView)()
        out_count = ctypes.c_size_t()
        code = self._lib.ddb_stmt_row_view(
            self._stmt, ctypes.byref(values_ptr), ctypes.byref(out_count)
        )
        if code != ERR_OK:
            _raise_error(code, sql=self._last_sql, params=None)

        count = int(out_count.value)
        return self._decode_row_view_values(values_ptr, count)

    def _decode_row_view_values(self, values_ptr, count):
        if count == 0:
            return ()

        string_at = ctypes.string_at

        if count == 3:
            v0 = values_ptr[0]
            v1 = values_ptr[1]
            v2 = values_ptr[2]
            t0 = int(v0.tag)
            t1 = int(v1.tag)
            t2 = int(v2.tag)
            if t0 == DDB_VALUE_INT64 and t1 == DDB_VALUE_TEXT and t2 == DDB_VALUE_FLOAT64:
                if self._decode_row_i64_text_f64_native is not None:
                    try:
                        return self._decode_row_i64_text_f64_native(
                            ctypes.addressof(values_ptr.contents)
                        )
                    except Exception:
                        pass
                if not v1.data or v1.len == 0:
                    text_value = ""
                else:
                    text_value = string_at(v1.data, v1.len).decode("utf-8")
                return (v0.int64_value, text_value, v2.float64_value)
            if t0 == DDB_VALUE_INT64 and t1 == DDB_VALUE_TEXT and t2 == DDB_VALUE_TEXT:
                if self._decode_row_i64_text_text_native is not None:
                    try:
                        return self._decode_row_i64_text_text_native(
                            ctypes.addressof(values_ptr.contents)
                        )
                    except Exception:
                        pass
                if not v1.data or v1.len == 0:
                    text1 = ""
                else:
                    text1 = string_at(v1.data, v1.len).decode("utf-8")
                if not v2.data or v2.len == 0:
                    text2 = ""
                else:
                    text2 = string_at(v2.data, v2.len).decode("utf-8")
                return (v0.int64_value, text1, text2)
            if t0 == DDB_VALUE_INT64 and t1 == DDB_VALUE_FLOAT64 and t2 == DDB_VALUE_TEXT:
                if self._decode_row_i64_f64_text_native is not None:
                    try:
                        return self._decode_row_i64_f64_text_native(
                            ctypes.addressof(values_ptr.contents)
                        )
                    except Exception:
                        pass
                if not v2.data or v2.len == 0:
                    text2 = ""
                else:
                    text2 = string_at(v2.data, v2.len).decode("utf-8")
                return (v0.int64_value, v1.float64_value, text2)
            if t0 == DDB_VALUE_TEXT and t1 == DDB_VALUE_INT64 and t2 == DDB_VALUE_FLOAT64:
                if self._decode_row_text_i64_f64_native is not None:
                    try:
                        return self._decode_row_text_i64_f64_native(
                            ctypes.addressof(values_ptr.contents)
                        )
                    except Exception:
                        pass
                if not v0.data or v0.len == 0:
                    text0 = ""
                else:
                    text0 = string_at(v0.data, v0.len).decode("utf-8")
                return (text0, v1.int64_value, v2.float64_value)

        if count == 1:
            v0 = values_ptr[0]
            tag = int(v0.tag)
            if tag == DDB_VALUE_INT64:
                if self._decode_row_i64_native is not None:
                    try:
                        return self._decode_row_i64_native(
                            ctypes.addressof(values_ptr.contents)
                        )
                    except Exception:
                        pass
                return (v0.int64_value,)
            if tag == DDB_VALUE_FLOAT64:
                return (v0.float64_value,)
            if tag == DDB_VALUE_NULL:
                return (None,)

        row = []
        append_row = row.append

        for index in range(count):
            value = values_ptr[index]
            tag = int(value.tag)
            if tag == DDB_VALUE_NULL:
                append_row(None)
            elif tag == DDB_VALUE_INT64:
                append_row(value.int64_value)
            elif tag == DDB_VALUE_FLOAT64:
                append_row(value.float64_value)
            elif tag == DDB_VALUE_BOOL:
                append_row(value.bool_value != 0)
            elif tag == DDB_VALUE_TEXT:
                if not value.data or value.len == 0:
                    append_row("")
                else:
                    append_row(string_at(value.data, value.len).decode("utf-8"))
            elif tag == DDB_VALUE_BLOB:
                if not value.data or value.len == 0:
                    append_row(b"")
                else:
                    append_row(bytes(string_at(value.data, value.len)))
            elif tag == DDB_VALUE_DECIMAL:
                append_row(decimal.Decimal(int(value.decimal_scaled)) / (
                    decimal.Decimal(10) ** int(value.decimal_scale)
                ))
            elif tag == DDB_VALUE_UUID:
                append_row(bytes(value.uuid_bytes))
            elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                append_row(
                    _UNIX_EPOCH_UTC
                    + datetime.timedelta(microseconds=int(value.timestamp_micros))
                )
            else:
                append_row(None)
        return tuple(row)

    def _decode_row_view_matrix(self, values_ptr, row_count, col_count):
        if row_count == 0:
            return []
        if col_count == 0:
            return [()] * row_count

        rows = []
        append_rows = rows.append
        string_at = ctypes.string_at

        if col_count == 3:
            sql = self._last_sql
            first_t0 = int(values_ptr[0].tag)
            first_t1 = int(values_ptr[1].tag)
            first_t2 = int(values_ptr[2].tag)
            if (
                first_t0 == DDB_VALUE_INT64
                and first_t1 == DDB_VALUE_TEXT
                and first_t2 == DDB_VALUE_FLOAT64
            ):
                native_supported = self._decode_matrix_i64_text_f64_sql_support.get(sql, True)
                if self._decode_matrix_i64_text_f64_native is not None and native_supported:
                    try:
                        return self._decode_matrix_i64_text_f64_native(
                            ctypes.addressof(values_ptr.contents), row_count
                        )
                    except Exception:
                        self._decode_matrix_i64_text_f64_sql_support[sql] = False
            if (
                first_t0 == DDB_VALUE_INT64
                and first_t1 == DDB_VALUE_TEXT
                and first_t2 == DDB_VALUE_TEXT
            ):
                native_supported = self._decode_matrix_i64_text_text_sql_support.get(sql, True)
                if self._decode_matrix_i64_text_text_native is not None and native_supported:
                    try:
                        return self._decode_matrix_i64_text_text_native(
                            ctypes.addressof(values_ptr.contents), row_count
                        )
                    except Exception:
                        self._decode_matrix_i64_text_text_sql_support[sql] = False
                for row_index in range(row_count):
                    base = row_index * 3
                    v0 = values_ptr[base]
                    v1 = values_ptr[base + 1]
                    v2 = values_ptr[base + 2]
                    if (
                        int(v0.tag) == DDB_VALUE_INT64
                        and int(v1.tag) == DDB_VALUE_TEXT
                        and int(v2.tag) == DDB_VALUE_TEXT
                    ):
                        if not v1.data or v1.len == 0:
                            text1 = ""
                        else:
                            text1 = string_at(v1.data, v1.len).decode("utf-8")
                        if not v2.data or v2.len == 0:
                            text2 = ""
                        else:
                            text2 = string_at(v2.data, v2.len).decode("utf-8")
                        append_rows((v0.int64_value, text1, text2))
                        continue
                    row = []
                    append_row = row.append
                    for col_index in range(3):
                        value = values_ptr[base + col_index]
                        tag = int(value.tag)
                        if tag == DDB_VALUE_NULL:
                            append_row(None)
                        elif tag == DDB_VALUE_INT64:
                            append_row(value.int64_value)
                        elif tag == DDB_VALUE_FLOAT64:
                            append_row(value.float64_value)
                        elif tag == DDB_VALUE_BOOL:
                            append_row(value.bool_value != 0)
                        elif tag == DDB_VALUE_TEXT:
                            if not value.data or value.len == 0:
                                append_row("")
                            else:
                                append_row(string_at(value.data, value.len).decode("utf-8"))
                        elif tag == DDB_VALUE_BLOB:
                            if not value.data or value.len == 0:
                                append_row(b"")
                            else:
                                append_row(bytes(string_at(value.data, value.len)))
                        elif tag == DDB_VALUE_DECIMAL:
                            append_row(
                                decimal.Decimal(int(value.decimal_scaled))
                                / (decimal.Decimal(10) ** int(value.decimal_scale))
                            )
                        elif tag == DDB_VALUE_UUID:
                            append_row(bytes(value.uuid_bytes))
                        elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                            append_row(
                                _UNIX_EPOCH_UTC
                                + datetime.timedelta(microseconds=int(value.timestamp_micros))
                            )
                        else:
                            append_row(None)
                    append_rows(tuple(row))
                return rows
            if (
                first_t0 == DDB_VALUE_INT64
                and first_t1 == DDB_VALUE_FLOAT64
                and first_t2 == DDB_VALUE_TEXT
            ):
                native_supported = self._decode_matrix_i64_f64_text_sql_support.get(sql, True)
                if self._decode_matrix_i64_f64_text_native is not None and native_supported:
                    try:
                        return self._decode_matrix_i64_f64_text_native(
                            ctypes.addressof(values_ptr.contents), row_count
                        )
                    except Exception:
                        self._decode_matrix_i64_f64_text_sql_support[sql] = False
                for row_index in range(row_count):
                    base = row_index * 3
                    v0 = values_ptr[base]
                    v1 = values_ptr[base + 1]
                    v2 = values_ptr[base + 2]
                    if (
                        int(v0.tag) == DDB_VALUE_INT64
                        and int(v1.tag) == DDB_VALUE_FLOAT64
                        and int(v2.tag) == DDB_VALUE_TEXT
                    ):
                        if not v2.data or v2.len == 0:
                            text2 = ""
                        else:
                            text2 = string_at(v2.data, v2.len).decode("utf-8")
                        append_rows((v0.int64_value, v1.float64_value, text2))
                        continue
                    row = []
                    append_row = row.append
                    for col_index in range(3):
                        value = values_ptr[base + col_index]
                        tag = int(value.tag)
                        if tag == DDB_VALUE_NULL:
                            append_row(None)
                        elif tag == DDB_VALUE_INT64:
                            append_row(value.int64_value)
                        elif tag == DDB_VALUE_FLOAT64:
                            append_row(value.float64_value)
                        elif tag == DDB_VALUE_BOOL:
                            append_row(value.bool_value != 0)
                        elif tag == DDB_VALUE_TEXT:
                            if not value.data or value.len == 0:
                                append_row("")
                            else:
                                append_row(string_at(value.data, value.len).decode("utf-8"))
                        elif tag == DDB_VALUE_BLOB:
                            if not value.data or value.len == 0:
                                append_row(b"")
                            else:
                                append_row(bytes(string_at(value.data, value.len)))
                        elif tag == DDB_VALUE_DECIMAL:
                            append_row(
                                decimal.Decimal(int(value.decimal_scaled))
                                / (decimal.Decimal(10) ** int(value.decimal_scale))
                            )
                        elif tag == DDB_VALUE_UUID:
                            append_row(bytes(value.uuid_bytes))
                        elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                            append_row(
                                _UNIX_EPOCH_UTC
                                + datetime.timedelta(microseconds=int(value.timestamp_micros))
                            )
                        else:
                            append_row(None)
                    append_rows(tuple(row))
                return rows
            if (
                first_t0 == DDB_VALUE_TEXT
                and first_t1 == DDB_VALUE_INT64
                and first_t2 == DDB_VALUE_FLOAT64
            ):
                native_supported = self._decode_matrix_text_i64_f64_sql_support.get(sql, True)
                if self._decode_matrix_text_i64_f64_native is not None and native_supported:
                    try:
                        return self._decode_matrix_text_i64_f64_native(
                            ctypes.addressof(values_ptr.contents), row_count
                        )
                    except Exception:
                        self._decode_matrix_text_i64_f64_sql_support[sql] = False
                for row_index in range(row_count):
                    base = row_index * 3
                    v0 = values_ptr[base]
                    v1 = values_ptr[base + 1]
                    v2 = values_ptr[base + 2]
                    if (
                        int(v0.tag) == DDB_VALUE_TEXT
                        and int(v1.tag) == DDB_VALUE_INT64
                        and int(v2.tag) == DDB_VALUE_FLOAT64
                    ):
                        if not v0.data or v0.len == 0:
                            text0 = ""
                        else:
                            text0 = string_at(v0.data, v0.len).decode("utf-8")
                        append_rows((text0, v1.int64_value, v2.float64_value))
                        continue
                    row = []
                    append_row = row.append
                    for col_index in range(3):
                        value = values_ptr[base + col_index]
                        tag = int(value.tag)
                        if tag == DDB_VALUE_NULL:
                            append_row(None)
                        elif tag == DDB_VALUE_INT64:
                            append_row(value.int64_value)
                        elif tag == DDB_VALUE_FLOAT64:
                            append_row(value.float64_value)
                        elif tag == DDB_VALUE_BOOL:
                            append_row(value.bool_value != 0)
                        elif tag == DDB_VALUE_TEXT:
                            if not value.data or value.len == 0:
                                append_row("")
                            else:
                                append_row(string_at(value.data, value.len).decode("utf-8"))
                        elif tag == DDB_VALUE_BLOB:
                            if not value.data or value.len == 0:
                                append_row(b"")
                            else:
                                append_row(bytes(string_at(value.data, value.len)))
                        elif tag == DDB_VALUE_DECIMAL:
                            append_row(
                                decimal.Decimal(int(value.decimal_scaled))
                                / (decimal.Decimal(10) ** int(value.decimal_scale))
                            )
                        elif tag == DDB_VALUE_UUID:
                            append_row(bytes(value.uuid_bytes))
                        elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                            append_row(
                                _UNIX_EPOCH_UTC
                                + datetime.timedelta(microseconds=int(value.timestamp_micros))
                            )
                        else:
                            append_row(None)
                    append_rows(tuple(row))
                return rows
            for row_index in range(row_count):
                base = row_index * 3
                v0 = values_ptr[base]
                v1 = values_ptr[base + 1]
                v2 = values_ptr[base + 2]
                if (
                    int(v0.tag) == DDB_VALUE_INT64
                    and int(v1.tag) == DDB_VALUE_TEXT
                    and int(v2.tag) == DDB_VALUE_FLOAT64
                ):
                    if not v1.data or v1.len == 0:
                        text_value = ""
                    else:
                        text_value = string_at(v1.data, v1.len).decode("utf-8")
                    append_rows((v0.int64_value, text_value, v2.float64_value))
                    continue

                row = []
                append_row = row.append
                for col_index in range(3):
                    value = values_ptr[base + col_index]
                    tag = int(value.tag)
                    if tag == DDB_VALUE_NULL:
                        append_row(None)
                    elif tag == DDB_VALUE_INT64:
                        append_row(value.int64_value)
                    elif tag == DDB_VALUE_FLOAT64:
                        append_row(value.float64_value)
                    elif tag == DDB_VALUE_BOOL:
                        append_row(value.bool_value != 0)
                    elif tag == DDB_VALUE_TEXT:
                        if not value.data or value.len == 0:
                            append_row("")
                        else:
                            append_row(string_at(value.data, value.len).decode("utf-8"))
                    elif tag == DDB_VALUE_BLOB:
                        if not value.data or value.len == 0:
                            append_row(b"")
                        else:
                            append_row(bytes(string_at(value.data, value.len)))
                    elif tag == DDB_VALUE_DECIMAL:
                        append_row(
                            decimal.Decimal(int(value.decimal_scaled))
                            / (decimal.Decimal(10) ** int(value.decimal_scale))
                        )
                    elif tag == DDB_VALUE_UUID:
                        append_row(bytes(value.uuid_bytes))
                    elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                        append_row(
                            _UNIX_EPOCH_UTC
                            + datetime.timedelta(microseconds=int(value.timestamp_micros))
                        )
                    else:
                        append_row(None)
                append_rows(tuple(row))
            return rows

        if col_count == 1:
            sql = self._last_sql
            native_supported = self._decode_matrix_i64_sql_support.get(sql, True)
            if (
                self._decode_matrix_i64_native is not None
                and native_supported
                and int(values_ptr[0].tag) == DDB_VALUE_INT64
            ):
                try:
                    return self._decode_matrix_i64_native(
                        ctypes.addressof(values_ptr.contents), row_count
                    )
                except Exception:
                    self._decode_matrix_i64_sql_support[sql] = False
            for row_index in range(row_count):
                value = values_ptr[row_index]
                tag = int(value.tag)
                if tag == DDB_VALUE_INT64:
                    append_rows((value.int64_value,))
                elif tag == DDB_VALUE_FLOAT64:
                    append_rows((value.float64_value,))
                elif tag == DDB_VALUE_NULL:
                    append_rows((None,))
                elif tag == DDB_VALUE_TEXT:
                    if not value.data or value.len == 0:
                        append_rows(("",))
                    else:
                        append_rows((string_at(value.data, value.len).decode("utf-8"),))
                elif tag == DDB_VALUE_BLOB:
                    if not value.data or value.len == 0:
                        append_rows((b"",))
                    else:
                        append_rows((bytes(string_at(value.data, value.len)),))
                elif tag == DDB_VALUE_BOOL:
                    append_rows((value.bool_value != 0,))
                elif tag == DDB_VALUE_DECIMAL:
                    append_rows((
                        decimal.Decimal(int(value.decimal_scaled))
                        / (decimal.Decimal(10) ** int(value.decimal_scale)),
                    ))
                elif tag == DDB_VALUE_UUID:
                    append_rows((bytes(value.uuid_bytes),))
                elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                    append_rows((
                        _UNIX_EPOCH_UTC
                        + datetime.timedelta(microseconds=int(value.timestamp_micros)),
                    ))
                else:
                        append_rows((None,))
            return rows

        if col_count == 6:
            sql = self._last_sql
            native_supported = self._decode_matrix_i64_f64_text_text_i64_f64_sql_support.get(
                sql, True
            )
            if (
                self._decode_matrix_i64_f64_text_text_i64_f64_native is not None
                and native_supported
                and int(values_ptr[0].tag) == DDB_VALUE_INT64
                and int(values_ptr[1].tag) == DDB_VALUE_FLOAT64
                and int(values_ptr[2].tag) == DDB_VALUE_TEXT
                and int(values_ptr[3].tag) == DDB_VALUE_TEXT
                and int(values_ptr[4].tag) == DDB_VALUE_INT64
                and int(values_ptr[5].tag) == DDB_VALUE_FLOAT64
            ):
                try:
                    return self._decode_matrix_i64_f64_text_text_i64_f64_native(
                        ctypes.addressof(values_ptr.contents), row_count
                    )
                except Exception:
                    self._decode_matrix_i64_f64_text_text_i64_f64_sql_support[sql] = False

        for row_index in range(row_count):
            base = row_index * col_count
            row = []
            append_row = row.append
            for col_index in range(col_count):
                value = values_ptr[base + col_index]
                tag = int(value.tag)
                if tag == DDB_VALUE_NULL:
                    append_row(None)
                elif tag == DDB_VALUE_INT64:
                    append_row(value.int64_value)
                elif tag == DDB_VALUE_FLOAT64:
                    append_row(value.float64_value)
                elif tag == DDB_VALUE_BOOL:
                    append_row(value.bool_value != 0)
                elif tag == DDB_VALUE_TEXT:
                    if not value.data or value.len == 0:
                        append_row("")
                    else:
                        append_row(string_at(value.data, value.len).decode("utf-8"))
                elif tag == DDB_VALUE_BLOB:
                    if not value.data or value.len == 0:
                        append_row(b"")
                    else:
                        append_row(bytes(string_at(value.data, value.len)))
                elif tag == DDB_VALUE_DECIMAL:
                    append_row(
                        decimal.Decimal(int(value.decimal_scaled))
                        / (decimal.Decimal(10) ** int(value.decimal_scale))
                    )
                elif tag == DDB_VALUE_UUID:
                    append_row(bytes(value.uuid_bytes))
                elif tag == DDB_VALUE_TIMESTAMP_MICROS:
                    append_row(
                        _UNIX_EPOCH_UTC
                        + datetime.timedelta(microseconds=int(value.timestamp_micros))
                    )
                else:
                    append_row(None)
            append_rows(tuple(row))
        return rows

    def fetchone(self):
        self._ensure_open()
        if not self._stmt:
            raise ProgrammingError("No statement")
        if not self._query_active:
            return None

        if self._prefetched_rows is not None:
            if not self._prefetched_rows:
                self._prefetched_rows = None
                return None
            row = self._prefetched_rows.pop(0)
            if not self._prefetched_rows:
                self._prefetched_rows = None
            return row

        if self._has_buffered_row:
            self._has_buffered_row = False
            if self._buffered_row is not None:
                row = self._buffered_row
                self._buffered_row = None
                return row
            return self._decode_current_row()

        if self._use_step_row_view:
            values_ptr = ctypes.POINTER(DdbValueView)()
            out_count = ctypes.c_size_t()
            has_row = ctypes.c_uint8()
            code = self._lib.ddb_stmt_step_row_view(
                self._stmt,
                ctypes.byref(values_ptr),
                ctypes.byref(out_count),
                ctypes.byref(has_row),
            )
            if code != ERR_OK:
                _raise_error(code, sql=self._last_sql, params=None)
            if not has_row.value:
                return None
            return self._decode_row_view_values(values_ptr, int(out_count.value))

        has_row = ctypes.c_uint8()
        code = self._lib.ddb_stmt_step(self._stmt, ctypes.byref(has_row))
        if code != ERR_OK:
            _raise_error(code, sql=self._last_sql, params=None)
        if not has_row.value:
            return None
        return self._decode_current_row()

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        if size <= 0:
            return []
        return self._fetch_rows(limit=size)

    def fetchall(self):
        chunk_size = self._fetchall_chunk_rows
        if chunk_size <= 0:
            return self._fetch_rows(limit=None)

        rows = []
        while True:
            batch = self._fetch_rows(limit=chunk_size)
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < chunk_size:
                break
        return rows

    def _fetch_rows(self, limit):
        self._ensure_open()
        if not self._stmt:
            raise ProgrammingError("No statement")
        if not self._query_active:
            return []

        if self._prefetched_rows is not None:
            if limit is None:
                rows = self._prefetched_rows
                self._prefetched_rows = None
                return rows
            rows = self._prefetched_rows[:limit]
            self._prefetched_rows = self._prefetched_rows[limit:]
            if not self._prefetched_rows:
                self._prefetched_rows = None
            return rows

        if self._use_fetch_row_views:
            rows = []
            include_current_row = 0
            if self._has_buffered_row:
                if self._buffered_row is not None:
                    rows.append(self._buffered_row)
                    self._buffered_row = None
                    self._has_buffered_row = False
                    if limit is not None and len(rows) >= limit:
                        return rows
                else:
                    include_current_row = 1

            remaining_limit = None if limit is None else limit - len(rows)
            if remaining_limit == 0:
                return rows

            sql = self._last_sql
            native_supported = self._native_fetch_rows_i64_text_f64_sql_support.get(sql, True)
            if (
                self._native_fetch_rows_i64_text_f64 is not None
                and self._stmt is not None
                and self._col_count == 3
                and native_supported
            ):
                try:
                    batch_rows = self._native_fetch_rows_i64_text_f64(
                        self._stmt.value,
                        include_current_row,
                        0 if remaining_limit is None else int(remaining_limit),
                    )
                    self._has_buffered_row = False
                    self._buffered_row = None
                    self._native_fetch_rows_i64_text_f64_sql_support[sql] = True
                    if rows:
                        rows.extend(batch_rows)
                        return rows
                    return batch_rows
                except Exception:
                    self._native_fetch_rows_i64_text_f64_sql_support[sql] = False

            values_ptr = ctypes.POINTER(DdbValueView)()
            out_rows = ctypes.c_size_t()
            out_columns = ctypes.c_size_t()
            code = self._lib.ddb_stmt_fetch_row_views(
                self._stmt,
                include_current_row,
                0 if remaining_limit is None else int(remaining_limit),
                ctypes.byref(values_ptr),
                ctypes.byref(out_rows),
                ctypes.byref(out_columns),
            )
            if code != ERR_OK:
                _raise_error(code, sql=self._last_sql, params=None)
            self._has_buffered_row = False
            self._buffered_row = None
            batch_rows = self._decode_row_view_matrix(
                values_ptr, int(out_rows.value), int(out_columns.value)
            )
            if rows:
                rows.extend(batch_rows)
                return rows
            return batch_rows

        rows = []
        append_row = rows.append
        decode_row = self._decode_current_row

        if self._has_buffered_row:
            self._has_buffered_row = False
            if self._buffered_row is not None:
                append_row(self._buffered_row)
                self._buffered_row = None
            else:
                append_row(decode_row())
            if limit is not None and len(rows) >= limit:
                return rows

        if self._use_step_row_view:
            values_ptr = ctypes.POINTER(DdbValueView)()
            out_count = ctypes.c_size_t()
            has_row = ctypes.c_uint8()
            step_row_view = self._lib.ddb_stmt_step_row_view
            byref = ctypes.byref
            decode_values = self._decode_row_view_values
            while limit is None or len(rows) < limit:
                code = step_row_view(
                    self._stmt,
                    byref(values_ptr),
                    byref(out_count),
                    byref(has_row),
                )
                if code != ERR_OK:
                    _raise_error(code, sql=self._last_sql, params=None)
                if not has_row.value:
                    break
                append_row(decode_values(values_ptr, int(out_count.value)))
            return rows

        step_stmt = self._lib.ddb_stmt_step
        byref = ctypes.byref
        has_row = ctypes.c_uint8()
        while limit is None or len(rows) < limit:
            code = step_stmt(self._stmt, byref(has_row))
            if code != ERR_OK:
                _raise_error(code, sql=self._last_sql, params=None)
            if not has_row.value:
                break
            append_row(decode_row())

        return rows

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class Connection:
    def __init__(self, path, *, stmt_cache_size=128):
        self._lib = load_library()
        self._db = ctypes.c_void_p()
        self._closed = False
        self._in_explicit_txn = False
        self.cursors = weakref.WeakSet()
        self._stmt_cache = collections.OrderedDict()
        self._stmt_cache_size = stmt_cache_size
        self._stats = collections.Counter()
        self._exec_cursor = None

        fs_path = os.fspath(path)
        raw_path = fs_path.encode("utf-8") if isinstance(fs_path, str) else fs_path
        code = self._lib.ddb_db_open_or_create(raw_path, ctypes.byref(self._db))
        if code != ERR_OK:
            _raise_error(code)

    def _ensure_open(self):
        if self._closed:
            raise ProgrammingError("Connection closed")

    def _get_cached_statement(self, sql):
        if sql in self._stmt_cache:
            stmt = self._stmt_cache.pop(sql)
            self._stats["cache_hit"] += 1
            return stmt, True
        self._stats["cache_miss"] += 1
        return None, False

    def _recycle_statement(self, sql, stmt_ptr):
        if self._closed or not stmt_ptr:
            return
        code = self._lib.ddb_stmt_reset(stmt_ptr)
        if code != ERR_OK:
            self._lib.ddb_stmt_free(ctypes.byref(ctypes.c_void_p(stmt_ptr.value)))
            return
        code = self._lib.ddb_stmt_clear_bindings(stmt_ptr)
        if code != ERR_OK:
            self._lib.ddb_stmt_free(ctypes.byref(ctypes.c_void_p(stmt_ptr.value)))
            return

        if self._stmt_cache_size <= 0:
            ptr = ctypes.c_void_p(stmt_ptr.value)
            self._lib.ddb_stmt_free(ctypes.byref(ptr))
            return

        if sql in self._stmt_cache:
            old = self._stmt_cache.pop(sql)
            ptr = ctypes.c_void_p(old.value)
            self._lib.ddb_stmt_free(ctypes.byref(ptr))

        self._stmt_cache[sql] = stmt_ptr
        while len(self._stmt_cache) > self._stmt_cache_size:
            _, old_stmt = self._stmt_cache.popitem(last=False)
            ptr = ctypes.c_void_p(old_stmt.value)
            self._lib.ddb_stmt_free(ctypes.byref(ptr))

    def close(self):
        if self._closed:
            return
        for cursor in list(self.cursors):
            cursor.close()
        for stmt in list(self._stmt_cache.values()):
            ptr = ctypes.c_void_p(stmt.value)
            self._lib.ddb_stmt_free(ctypes.byref(ptr))
        self._stmt_cache.clear()
        self._lib.ddb_db_free(ctypes.byref(self._db))
        self._db = None
        self._closed = True

    def begin_transaction(self):
        self._ensure_open()
        code = self._lib.ddb_db_begin_transaction(self._db)
        if code != ERR_OK:
            _raise_error(code, sql="BEGIN", params=None)
        self._in_explicit_txn = True

    def commit(self):
        self._ensure_open()
        if not self._in_explicit_txn:
            return
        lsn = ctypes.c_uint64()
        code = self._lib.ddb_db_commit_transaction(self._db, ctypes.byref(lsn))
        if code != ERR_OK:
            _raise_error(code, sql="COMMIT", params=None)
        self._in_explicit_txn = False

    def rollback(self):
        self._ensure_open()
        if not self._in_explicit_txn:
            return
        code = self._lib.ddb_db_rollback_transaction(self._db)
        if code != ERR_OK:
            _raise_error(code, sql="ROLLBACK", params=None)
        self._in_explicit_txn = False

    def cursor(self):
        self._ensure_open()
        cursor = Cursor(self)
        self.cursors.add(cursor)
        return cursor

    def execute(self, operation, parameters=None):
        cur = self._exec_cursor() if self._exec_cursor is not None else None
        if cur is None or cur._closed:
            cur = self.cursor()
            self._exec_cursor = weakref.ref(cur)
        cur.execute(operation, parameters)
        return cur

    def _call_json_api(self, func, *args):
        self._ensure_open()
        out = ctypes.c_char_p()
        code = func(self._db, *args, ctypes.byref(out))
        if code != ERR_OK:
            _raise_error(code)
        try:
            raw = out.value.decode("utf-8") if out.value else ""
            return json.loads(raw)
        finally:
            self._lib.ddb_string_free(ctypes.byref(out))

    def list_tables(self):
        payload = self._call_json_api(
            getattr(self._lib, "decentdb_list_tables_json", self._lib.ddb_db_list_tables_json)
        )
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            return [entry["name"] for entry in payload]
        return payload

    def get_table_columns(self, table_name):
        name = table_name.encode("utf-8")
        payload = self._call_json_api(self._lib.ddb_db_describe_table_json, name)
        columns = payload.get("columns", [])
        for column in columns:
            if "not_null" not in column:
                column["not_null"] = not column.get("nullable", True)
        return columns

    def list_indexes(self):
        return self._call_json_api(self._lib.ddb_db_list_indexes_json)

    def checkpoint(self):
        self._ensure_open()
        code = self._lib.ddb_db_checkpoint(self._db)
        if code != ERR_OK:
            _raise_error(code, sql="checkpoint", params=None)

    def save_as(self, dest_path):
        self._ensure_open()
        fs_path = os.fspath(dest_path)
        raw = fs_path.encode("utf-8") if isinstance(fs_path, str) else fs_path
        code = self._lib.ddb_db_save_as(self._db, raw)
        if code != ERR_OK:
            _raise_error(code, sql="save_as", params=None)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()


def connect(dsn, **kwargs):
    stmt_cache_size = kwargs.pop("stmt_cache_size", 128)
    return Connection(dsn, stmt_cache_size=stmt_cache_size)


def evict_shared_wal(path):
    lib = load_library()
    fs_path = os.fspath(path)
    raw = fs_path.encode("utf-8") if isinstance(fs_path, str) else fs_path
    code = lib.ddb_evict_shared_wal(raw)
    if code != ERR_OK:
        _raise_error(code)
