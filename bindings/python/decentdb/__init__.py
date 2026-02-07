from .native import (
    load_library, DecentdbValueView,
    ERR_OK, ERR_ERROR, ERR_IO, ERR_LOCKED, ERR_CORRUPTION, ERR_NOT_FOUND,
    ERR_FULL, ERR_PERMISSION, ERR_INTERNAL, ERR_INVALID, ERR_CONSTRAINT,
    ERR_TRANSACTION, ERR_SQL, ERR_NOMEM
)
import ctypes
import os
import datetime
import decimal
import uuid
import re
import collections
import json

# DB-API 2.0 Globals
apilevel = "2.0"
threadsafety = 1  # Threads may share the module, but not connections
paramstyle = "qmark" # We accept qmark (?) or named (:name) and rewrite to $N for engine

# Exceptions
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

def _format_value_for_error(v, *, max_str=200, max_bytes=64):
    if v is None:
        return None
    if isinstance(v, (bool, int, float)):
        return v
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        if len(b) <= max_bytes:
            return {"_type": "bytes", "hex": b.hex(), "len": len(b)}
        head = b[:max_bytes]
        return {"_type": "bytes", "hex_prefix": head.hex(), "len": len(b)}
    if isinstance(v, str):
        if len(v) <= max_str:
            return v
        return v[:max_str] + "…"
    # Fall back to capped repr
    s = repr(v)
    if len(s) <= max_str:
        return s
    return s[:max_str] + "…"

def _format_params_for_error(params, *, max_items=50):
    if params is None:
        return None
    if isinstance(params, (dict, collections.abc.Mapping)):
        out = {}
        for i, (k, v) in enumerate(params.items()):
            if i >= max_items:
                out["_truncated"] = True
                break
            out[str(k)] = _format_value_for_error(v)
        return out
    # Sequence-like
    try:
        seq = list(params)
    except TypeError:
        return _format_value_for_error(params)
    if len(seq) > max_items:
        seq = seq[:max_items] + ["<truncated>"]
    return [_format_value_for_error(v) for v in seq]

def _raise_error(db_handle, *, sql=None, params=None):
    lib = load_library()
    code = lib.decentdb_last_error_code(db_handle)
    msg = lib.decentdb_last_error_message(db_handle)
    # Be defensive: native messages should be UTF-8, but don't crash if not.
    msg_str = msg.decode('utf-8', errors='replace') if msg else f"Unknown error {code}"

    if sql is not None:
        ctx = {
            "native_code": int(code),
            "sql": sql,
            "params": _format_params_for_error(params),
        }
        msg_str = msg_str + "\nContext: " + json.dumps(ctx, ensure_ascii=False)
    
    if code == ERR_CONSTRAINT:
        raise IntegrityError(msg_str)
    elif code == ERR_TRANSACTION or code == ERR_IO or code == ERR_LOCKED:
        raise OperationalError(msg_str)
    elif code == ERR_SQL:
        raise ProgrammingError(msg_str)
    elif code == ERR_CORRUPTION:
        raise DatabaseError(msg_str)
    elif code == ERR_INTERNAL:
        raise InternalError(msg_str)
    elif code == ERR_NOT_FOUND:
        raise OperationalError(msg_str)
    else:
        raise DatabaseError(msg_str)

# Types
Date = datetime.date
Time = datetime.time
Timestamp = datetime.datetime
def DateFromTicks(ticks): return datetime.date.fromtimestamp(ticks)
def TimeFromTicks(ticks): return datetime.time(0, 0, 0) # Simplification
def TimestampFromTicks(ticks): return datetime.datetime.fromtimestamp(ticks)
def Binary(string): return bytes(string)
STRING = str
BINARY = bytes
NUMBER = float
DATETIME = datetime.datetime
ROWID = int

# Helper for parameter rewriting
# DecentDB uses $1, $2, ...
# SQLAlchemy might use ? (qmark) or :name (named)
# We will rewrite ? to $1, $2... and :name to $1, $2... keeping track of mapping.

def _convert_params(sql, params):
    if params is None:
        return sql, []
    
    # Check if params is a dict (named) or sequence (positional)
    if isinstance(params, (dict, collections.abc.Mapping)):
        # Reject mixed placeholder styles early.
        if '?' in sql:
            raise ProgrammingError("Mixed parameter styles are not supported: got named parameters with qmark placeholders")
        # Rewrite :name to $N
        # We need to find all :name in SQL and replace with $1, $2...
        # and order values accordingly.
        # Simplistic regex for :name, ignoring quotes for MVP
        # Ideally we use a proper tokenizer or trust SQLAlchemy to pass qmark if configured.
        # But requirements say "Driver MAY accept named params".
        
        # Regex to find :param_name (alphanumeric + underscore)
        # Avoid :: cast syntax (Postgres) - decentdb doesn't have it yet but good to be careful
        # Note: DecentDB SQL dialect is limited, so maybe less collision risk.
        
        param_map = {}
        new_params = []
        
        def replace(match):
            name = match.group(1)
            if name not in param_map:
                param_map[name] = len(new_params) + 1
                if name not in params:
                     raise ProgrammingError(f"Missing parameter '{name}'")
                new_params.append(params[name])
            return f"${param_map[name]}"
            
        new_sql = re.sub(r':([a-zA-Z_][a-zA-Z0-9_]*)', replace, sql)
        return new_sql, new_params
        
    else:
        # Sequence
        # Reject named placeholders with positional params.
        if re.search(r':([a-zA-Z_][a-zA-Z0-9_]*)', sql) is not None:
            raise ProgrammingError("Mixed parameter styles are not supported: got positional parameters with named placeholders")
        # If SQL uses ?, rewrite to $1, $2...
        if '?' in sql:
            # Replace each ? with $1, $2, ...
            # We can't use simple replace because we need counters
            parts = sql.split('?')
            if len(parts) - 1 != len(params):
                raise ProgrammingError(f"Incorrect number of parameters: expected {len(parts)-1}, got {len(params)}")
            
            new_sql = ""
            for i in range(len(parts) - 1):
                new_sql += parts[i] + f"${i+1}"
            new_sql += parts[-1]
            return new_sql, params
        else:
            # Assume it's already using $N or no params, pass through
            return sql, params

class Cursor:
    def __init__(self, connection):
        self._connection = connection
        self._stmt = None
        self._last_sql = None # The SQL string for the current _stmt
        self._lib = load_library()
        self.description = None
        self._col_count = None
        self.rowcount = -1
        self.arraysize = 1
        self._closed = False

        # Cache for SQL placeholder rewriting.
        # Key is (operation_sql, mode, param_len) where mode is "none"|"seq".
        self._rewrite_cache_key = None
        self._rewrite_cache_sql = None

        self._last_bound_sql = None
        self._last_bound_param_count = None

        self._has_step_with_params_row_view = hasattr(self._lib, "decentdb_step_with_params_row_view")
        self._pending_select = False
        self._pending_select_params = None
        self._pending_select_sql = None
        self._pending_select_operation = None

        # Row decoding strategy.
        # Default to row_view (fewer ctypes calls per row). Can be disabled for debugging.
        use_row_view = os.environ.get("DECENTDB_PY_USE_ROW_VIEW", "1").strip().lower()
        self._use_row_view = use_row_view not in {"0", "false", "no", "off"}
        
    def close(self):
        if self._closed:
            return
        if self._stmt:
            # Return to cache instead of finalizing directly
            self._connection._recycle_statement(self._last_sql, self._stmt)
            self._stmt = None
            self._last_sql = None
            self._col_count = None
            self._pending_select = False
            self._pending_select_params = None
            self._pending_select_sql = None
            self._pending_select_operation = None
        self._closed = True

    def execute(self, operation, parameters=None):
        if self._closed:
            raise ProgrammingError("Cursor is closed")

        # New execute invalidates any pending SELECT fetch.
        self._pending_select = False
        self._pending_select_params = None
        self._pending_select_sql = None
        self._pending_select_operation = None

        # Rewrite parameters (cached for hot loops).
        # Most microbenchmarks call execute() repeatedly with identical SQL.
        params_is_mapping = isinstance(parameters, (dict, collections.abc.Mapping))
        if parameters is None:
            cache_key = (operation, "none", 0)
        elif params_is_mapping:
            cache_key = None
        else:
            try:
                n_params = len(parameters)
            except TypeError:
                n_params = None
            cache_key = (operation, "seq", n_params)

        if cache_key is not None and cache_key == self._rewrite_cache_key:
            sql = self._rewrite_cache_sql
            params = [] if parameters is None else parameters
        else:
            sql, params = _convert_params(operation, parameters)
            if cache_key is not None:
                self._rewrite_cache_key = cache_key
                self._rewrite_cache_sql = sql
        
        # If we have an existing statement, check if we can reuse it?
        # Only if SQL matches exactly.
        # But per logic: execute() should reset usage.
        # Simplest: recycle current statement, try to get new one (maybe same from cache).
        
        stmt_reused_same_sql = False
        if self._stmt:
            if self._last_sql == sql:
                # OPTIMIZATION: We have the right statement already.
                # For the SELECT fast path, defer reset/bind/step until fetch.
                can_defer_select = (
                    self._use_row_view
                    and self._has_step_with_params_row_view
                    and (self._col_count or 0) > 0
                )

                if not can_defer_select:
                    # Just reset it.
                    self._lib.decentdb_reset(self._stmt)
                    # Clearing bindings is not required if we are about to re-bind
                    # the full parameter set. This saves a ctypes call on hot loops.
                    param_count = 0
                    try:
                        param_count = len(params)
                    except TypeError:
                        param_count = 0

                    can_prove_full_rebind = ("?" in operation) or (re.search(r':([a-zA-Z_][a-zA-Z0-9_]*)', operation) is not None)
                    if not (
                        can_prove_full_rebind
                        and self._last_bound_sql == sql
                        and self._last_bound_param_count == param_count
                    ):
                        self._lib.decentdb_clear_bindings(self._stmt)
                # Keep self._stmt and self._last_sql
                stmt_reused_same_sql = True
            else:
                # Different SQL. Recycle old one.
                self._connection._recycle_statement(self._last_sql, self._stmt)
                self._stmt = None
                self._last_sql = None
                self._col_count = None
        
        if not self._stmt:
            # Try cache
            cached_stmt, hit = self._connection._get_cached_statement(sql)
            if hit:
                self._stmt = cached_stmt
                self._last_sql = sql
                self._col_count = None
            else:
                # Prepare new
                stmt_ptr = ctypes.c_void_p()
                self._connection._stats['prepare_count'] += 1
                res = self._lib.decentdb_prepare(
                    self._connection._db, 
                    sql.encode('utf-8'), 
                    ctypes.byref(stmt_ptr)
                )
                
                if res != ERR_OK:
                    _raise_error(self._connection._db, sql=sql, params=params)
                    
                self._stmt = stmt_ptr
                self._last_sql = sql
                self._col_count = None
        
        # Column metadata is stable for a prepared statement.
        # Avoid re-doing reflection on tight loops where the same SQL is executed repeatedly.
        if not stmt_reused_same_sql or self._col_count is None:
            col_count = self._lib.decentdb_column_count(self._stmt)
            self._col_count = int(col_count)
            if col_count > 0:
                desc = []
                for i in range(col_count):
                    name_ptr = self._lib.decentdb_column_name(self._stmt, i)
                    name = name_ptr.decode('utf-8') if name_ptr else ""
                    # Type info is only valid after stepping a row.
                    desc.append((name, None, None, None, None, None, None))
                self.description = desc
            else:
                self.description = None

        # SELECT fast path: defer binding/stepping until the first fetch.
        if (self._col_count or 0) > 0 and self._use_row_view and self._has_step_with_params_row_view:
            self._pending_select = True
            self._pending_select_params = params
            self._pending_select_sql = sql
            self._pending_select_operation = operation
            self.rowcount = -1
            return

        # Bind parameters (non-SELECT, or when fast path is unavailable/disabled)
        for i, param in enumerate(params):
            idx = i + 1
            if param is None:
                res = self._lib.decentdb_bind_null(self._stmt, idx)
            elif isinstance(param, bool):
                res = self._lib.decentdb_bind_bool(self._stmt, idx, 1 if param else 0)
            elif isinstance(param, int):
                res = self._lib.decentdb_bind_int64(self._stmt, idx, param)
            elif isinstance(param, float):
                res = self._lib.decentdb_bind_float64(self._stmt, idx, param)
            elif isinstance(param, str):
                b = param.encode('utf-8')
                res = self._lib.decentdb_bind_text(self._stmt, idx, b, len(b))
            elif isinstance(param, (bytes, bytearray)):
                # Cast to uint8 pointer
                ArrayType = ctypes.c_uint8 * len(param)
                b_arr = ArrayType.from_buffer_copy(param)
                res = self._lib.decentdb_bind_blob(self._stmt, idx, b_arr, len(param))
            elif isinstance(param, decimal.Decimal):
                # DecentDB supports DECIMAL as int64 + scale (0..18)
                t = param.as_tuple()
                exponent = t.exponent
                if not isinstance(exponent, int):
                    raise DataError("Decimal NaN/Inf not supported")
                
                scale = -exponent
                if scale < 0:
                    int_val = int(param)
                    scale = 0
                elif scale > 18:
                    # Truncate to 18 scale
                    quantized = param.quantize(decimal.Decimal(10) ** -18)
                    scale = 18
                    int_val = int(quantized * (decimal.Decimal(10) ** 18))
                else:
                    int_val = int(param * (decimal.Decimal(10) ** scale))
                
                # Check bounds
                if int_val < -9223372036854775808 or int_val > 9223372036854775807:
                    raise DataError("Decimal value too large for DecentDB")
                
                res = self._lib.decentdb_bind_decimal(self._stmt, idx, int_val, scale)
            elif isinstance(param, uuid.UUID):
                b = param.bytes
                ArrayType = ctypes.c_uint8 * 16
                b_arr = ArrayType.from_buffer_copy(b)
                res = self._lib.decentdb_bind_blob(self._stmt, idx, b_arr, 16)
            else:
                # Try string conversion for unknown types (e.g. Date)
                s = str(param)
                b = s.encode('utf-8')
                res = self._lib.decentdb_bind_text(self._stmt, idx, b, len(b))

            if res != ERR_OK:
                _raise_error(self._connection._db, sql=sql, params=params)

        # Remember param arity for safe rebind reuse.
        try:
            self._last_bound_param_count = len(params)
        except TypeError:
            self._last_bound_param_count = None
        self._last_bound_sql = sql
            
        # If it's an INSERT/UPDATE/DELETE, we should probably run it now to get rowcount?
        # Standard DB-API execute() should execute the statement.
        # For SELECT, it initializes.
        
        # If col_count == 0, run it to completion
        if (self._col_count or 0) == 0:
            while True:
                r = self._lib.decentdb_step(self._stmt)
                if r == 0: break # Done
                if r == -1: _raise_error(self._connection._db, sql=sql, params=params)
            self.rowcount = self._lib.decentdb_rows_affected(self._stmt)
        else:
            self.rowcount = -1

    def executemany(self, operation, seq_of_parameters):
        for params in seq_of_parameters:
            self.execute(operation, params)
            # Optimize: use reset() and reuse prepared stmt
            # But execute() currently finalizes.
            # Future optimization.

    def fetchone(self):
        if self._closed:
            raise ProgrammingError("Cursor is closed")
        if not self._stmt:
            raise ProgrammingError("No statement")
        
        if self._pending_select:
            # Single native call: reset + bind all params + step once + row_view.
            self._pending_select = False

            pending_params = self._pending_select_params or []
            n = len(pending_params)
            ParamArray = DecentdbValueView * n
            in_arr = ParamArray()
            keepalive = []

            for i, param in enumerate(pending_params):
                v = DecentdbValueView()
                if param is None:
                    v.kind = 0
                    v.isNull = 1
                elif isinstance(param, bool):
                    v.kind = 2
                    v.isNull = 0
                    v.int64Val = 1 if param else 0
                elif isinstance(param, int):
                    v.kind = 1
                    v.isNull = 0
                    v.int64Val = int(param)
                elif isinstance(param, float):
                    v.kind = 3
                    v.isNull = 0
                    v.float64Val = float(param)
                elif isinstance(param, str):
                    b = param.encode("utf-8")
                    buf = ctypes.create_string_buffer(b)
                    keepalive.append(buf)
                    v.kind = 4
                    v.isNull = 0
                    v.bytes = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8))
                    v.bytesLen = len(b)
                elif isinstance(param, (bytes, bytearray)):
                    b = bytes(param)
                    buf = ctypes.create_string_buffer(b)
                    keepalive.append(buf)
                    v.kind = 5
                    v.isNull = 0
                    v.bytes = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8))
                    v.bytesLen = len(b)
                else:
                    s = str(param)
                    b = s.encode("utf-8")
                    buf = ctypes.create_string_buffer(b)
                    keepalive.append(buf)
                    v.kind = 4
                    v.isNull = 0
                    v.bytes = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8))
                    v.bytesLen = len(b)

                in_arr[i] = v

            out_count = ctypes.c_int()
            out_values = ctypes.POINTER(DecentdbValueView)()
            out_has_row = ctypes.c_int()

            res = self._lib.decentdb_step_with_params_row_view(
                self._stmt,
                in_arr if n > 0 else None,
                n,
                ctypes.byref(out_values),
                ctypes.byref(out_count),
                ctypes.byref(out_has_row),
            )

            if res != ERR_OK:
                _raise_error(self._connection._db, sql=self._pending_select_sql or self._last_sql, params=pending_params)

            if out_has_row.value == 0:
                self._pending_select_params = None
                self._pending_select_sql = None
                self._pending_select_operation = None
                return None

            # Decode directly from returned row view.
            ncols = int(out_count.value)
            string_at = ctypes.string_at
            row = [None] * ncols
            for i in range(ncols):
                v = out_values[i]
                if v.isNull:
                    row[i] = None
                    continue
                k = int(v.kind)
                if k == 1:
                    row[i] = int(v.int64Val)
                elif k == 2:
                    row[i] = bool(v.int64Val)
                elif k == 3:
                    row[i] = float(v.float64Val)
                elif k == 4:
                    if bool(v.bytes) and v.bytesLen > 0:
                        row[i] = string_at(v.bytes, v.bytesLen).decode("utf-8", errors="replace")
                    else:
                        row[i] = ""
                elif k == 5:
                    if bool(v.bytes) and v.bytesLen > 0:
                        row[i] = string_at(v.bytes, v.bytesLen)
                    else:
                        row[i] = b""
                elif k == 12: # vkDecimal
                    scale = v.decimalScale
                    val = v.int64Val
                    row[i] = decimal.Decimal(val) / (decimal.Decimal(10) ** scale)
                else:
                    return self._get_row_slow()
            self._pending_select_params = None
            self._pending_select_sql = None
            self._pending_select_operation = None
            return tuple(row)

        res = self._lib.decentdb_step(self._stmt)
        if res == 0:
            return None # Done
        if res == -1:
            _raise_error(self._connection._db)

        # Row available
        return self._get_row()

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        rows = []
        for _ in range(size):
            r = self.fetchone()
            if r is None:
                break
            rows.append(r)
        return rows

    def fetchall(self):
        rows = []
        while True:
            r = self.fetchone()
            if r is None:
                break
            rows.append(r)
        return rows

    def _get_row(self):
        if self._use_row_view:
            return self._get_row_view_impl()
        return self._get_row_slow()

    def _get_row_view_impl(self):
        # Use row view for performance
        count = ctypes.c_int()
        values_ptr = ctypes.POINTER(DecentdbValueView)()
        
        res = self._lib.decentdb_row_view(self._stmt, ctypes.byref(values_ptr), ctypes.byref(count))
        
        if res == 0: # ERR_OK
            n = int(count.value)
            if n <= 0:
                return tuple()

            string_at = ctypes.string_at
            row = [None] * n
            # values_ptr acts as an array
            for i in range(n):
                v = values_ptr[i]
                if v.isNull:
                    row[i] = None
                    continue

                k = int(v.kind)
                if k == 1: # vkInt64
                    row[i] = int(v.int64Val)
                elif k == 2: # vkBool
                    row[i] = bool(v.int64Val)
                elif k == 3: # vkFloat64
                    row[i] = float(v.float64Val)
                elif k == 4: # vkText
                    if bool(v.bytes) and v.bytesLen > 0:
                        row[i] = string_at(v.bytes, v.bytesLen).decode('utf-8', errors='replace')
                    else:
                        row[i] = ""
                elif k == 5: # vkBlob
                    if bool(v.bytes) and v.bytesLen > 0:
                        row[i] = string_at(v.bytes, v.bytesLen)
                    else:
                        row[i] = b""
                elif k == 12: # vkDecimal
                    scale = v.decimalScale
                    val = v.int64Val
                    row[i] = decimal.Decimal(val) / (decimal.Decimal(10) ** scale)
                else:
                    # Unknown kinds should not occur in normalized result rows.
                    # Fall back to slow path for correctness.
                    return self._get_row_slow()
            return tuple(row)
            
        # Fallback to column accessors
        return self._get_row_slow()

    def _get_row_slow(self):
        # Use individual accessors for debugging
        col_count = self._lib.decentdb_column_count(self._stmt)
        row = []
        for i in range(col_count):
            kind = self._lib.decentdb_column_type(self._stmt, i)
            if kind == 0: # vkNull
                row.append(None)
            elif kind == 1: # vkInt64
                row.append(self._lib.decentdb_column_int64(self._stmt, i))
            elif kind == 2: # vkBool
                 # vkBool is mapped to int64 accessor too in c_api.nim?
                 # decentdb_column_int64 handles vkBool (returns 0/1)
                 val = self._lib.decentdb_column_int64(self._stmt, i)
                 row.append(bool(val))
            elif kind == 3: # vkFloat64
                row.append(self._lib.decentdb_column_float64(self._stmt, i))
            elif kind == 4: # vkText
                length = ctypes.c_int()
                ptr = self._lib.decentdb_column_text(self._stmt, i, ctypes.byref(length))
                if ptr:
                    row.append(ctypes.string_at(ptr, length.value).decode('utf-8'))
                else:
                    row.append("")
            elif kind == 5: # vkBlob
                length = ctypes.c_int()
                ptr = self._lib.decentdb_column_blob(self._stmt, i, ctypes.byref(length))
                if ptr:
                    row.append(ctypes.string_at(ptr, length.value))
                else:
                    row.append(b"")
            elif kind == 12: # vkDecimal
                scale = self._lib.decentdb_column_decimal_scale(self._stmt, i)
                val = self._lib.decentdb_column_decimal_unscaled(self._stmt, i)
                row.append(decimal.Decimal(val) / (decimal.Decimal(10) ** scale))
            else:
                row.append(None)
        return tuple(row)

    def __iter__(self):
        return self
        
    def __next__(self):
        r = self.fetchone()
        if r is None:
            raise StopIteration
        return r

class Connection:
    def __init__(self, path, options="", stmt_cache_size=128):
        self._lib = load_library()
        self._db = self._lib.decentdb_open(path.encode('utf-8'), options.encode('utf-8'))
        if not self._db:
            # Need to get global error if handle is null?
            # decentdb_open returns nil on failure and sets global error.
            # But we don't have global error accessor exposed nicely in native.py yet?
            # decentdb_last_error_message(NULL) works per c_api.nim
            msg = self._lib.decentdb_last_error_message(None)
            msg_str = msg.decode('utf-8') if msg else "Failed to open database"
            raise OperationalError(msg_str)
        self._closed = False
        self.cursors = []
        
        # Prepared statement cache
        self._stmt_cache = collections.OrderedDict()
        self._stmt_cache_size = stmt_cache_size
        
        # Statistics for testing
        self._stats = collections.Counter()

    def _get_cached_statement(self, sql):
        """
        Get a prepared statement from the cache if available.
        Returns (stmt_ptr, hit_bool).
        """
        if sql in self._stmt_cache:
            # Move to end (LRU)
            stmt = self._stmt_cache.pop(sql)
            self._stats['cache_hit'] += 1
            return stmt, True
        self._stats['cache_miss'] += 1
        return None, False

    def _recycle_statement(self, sql, stmt_ptr):
        """
        Return a statement to the cache.
        Resets execution state and clears bindings.
        """
        if self._closed or not stmt_ptr:
            return

        # Reset and clear bindings before caching
        self._lib.decentdb_reset(stmt_ptr)
        self._lib.decentdb_clear_bindings(stmt_ptr)

        # If cache is disabled (size 0), finalize immediately
        if self._stmt_cache_size <= 0:
            self._lib.decentdb_finalize(stmt_ptr)
            return

        # Add to cache (remove if exists to update LRU position)
        if sql in self._stmt_cache:
            self._stmt_cache.pop(sql)
        
        self._stmt_cache[sql] = stmt_ptr
        
        # Evict if full
        while len(self._stmt_cache) > self._stmt_cache_size:
            _, old_stmt = self._stmt_cache.popitem(last=False)
            self._lib.decentdb_finalize(old_stmt)

    def close(self):
        if self._closed:
            return
        for c in self.cursors:
            c.close()
        # Finalize all cached statements
        for stmt in self._stmt_cache.values():
            self._lib.decentdb_finalize(stmt)
        self._stmt_cache.clear()
        
        self._lib.decentdb_close(self._db)
        self._db = None
        self._closed = True

    def commit(self):
        if self._closed:
            raise ProgrammingError("Connection closed")
        # DecentDB MVP is auto-commit or explicit BEGIN/COMMIT via SQL.
        # DB-API expects commit() to commit the transaction.
        # If we are in explicit transaction mode started by SQL, this does nothing at C level?
        # Or should we execute "COMMIT"?
        # Python DB-API usually implies "BEGIN" on first execute.
        # But DecentDB documentation says "WAL-only durability (fsync on commit by default)".
        # And "single process, one writer".
        # If we are in a write transaction, we should commit it.
        # For now, let's execute "COMMIT" SQL command.
        try:
            self.execute("COMMIT")
        except DatabaseError:
            # If no transaction active, might error or be no-op.
            # Postgres warns "there is no transaction in progress".
            pass

    def rollback(self):
        if self._closed:
            raise ProgrammingError("Connection closed")
        try:
            self.execute("ROLLBACK")
        except DatabaseError:
            pass

    def cursor(self):
        if self._closed:
            raise ProgrammingError("Connection closed")
        c = Cursor(self)
        self.cursors.append(c)
        return c

    def execute(self, operation, parameters=None):
        # Convenience method
        c = self.cursor()
        c.execute(operation, parameters)
        return c

    def _call_json_api(self, func_name, *args):
        if self._closed:
            raise ProgrammingError("Connection closed")
        func = getattr(self._lib, func_name)
        out_len = ctypes.c_int()
        ptr = func(self._db, *args, ctypes.byref(out_len))
        if not ptr:
            _raise_error(self._db, sql=f"{func_name}(...)", params=None)
        try:
            raw = ctypes.string_at(ptr, out_len.value)
            return raw.decode("utf-8")
        finally:
            # Free API-allocated memory
            self._lib.decentdb_free(ptr)

    def list_tables(self):
        payload = self._call_json_api("decentdb_list_tables_json")
        return json.loads(payload)

    def get_table_columns(self, table_name: str):
        payload = self._call_json_api(
            "decentdb_get_table_columns_json",
            table_name.encode("utf-8"),
        )
        return json.loads(payload)

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()

def connect(dsn, **kwargs):
    # dsn might be the path
    # kwargs might contain options
    
    # Parse options from kwargs into query string style?
    # decentdb_open takes "options" string (e.g. "cache_size=1024").
    
    # Extract cache size from kwargs if present, default to 128
    stmt_cache_size = kwargs.pop("stmt_cache_size", 128)
    
    opts_parts = []
    for k, v in kwargs.items():
        opts_parts.append(f"{k}={v}")
    options = "&".join(opts_parts)
    
    return Connection(dsn, options, stmt_cache_size=stmt_cache_size)
