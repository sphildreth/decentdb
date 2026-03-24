using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace DecentDB.Native;

public sealed class DecentDB : IDisposable
{
    private readonly DecentDBHandle _handle;
    private bool _disposed;
    private int _lastErrorCode;
    private string _lastErrorMessage = string.Empty;

    public IntPtr Handle => _handle.Handle;

    internal DecentDBHandle DbHandle => _handle;

    public DecentDB(string path, string? options = null)
    {
        var pathBytes = Encoding.UTF8.GetBytes(path + "\0");
        IntPtr ptr;
        unsafe
        {
            fixed (byte* pPath = pathBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_open_or_create(pPath, out ptr));
                if (res != 0 || ptr == IntPtr.Zero)
                {
                    throw new DecentDBException(_lastErrorCode, LastErrorMessage, "Open");
                }
            }
        }

        _handle = new DecentDBHandle(ptr);

        // The stable Rust ddb_* ABI currently exposes a single default open path.
        // Keep accepting the managed options string for API compatibility even
        // though it does not yet feed a native open-time configuration surface.
        _ = options;
    }

    public int LastErrorCode => _lastErrorCode;

    public string LastErrorMessage => _lastErrorMessage;

    internal uint RecordStatus(uint status)
    {
        _lastErrorCode = checked((int)status);
        _lastErrorMessage = status == 0 ? string.Empty : GetErrorMessage();
        return status;
    }

    internal void SetManagedError(int code, string message)
    {
        _lastErrorCode = code;
        _lastErrorMessage = message;
    }

    private static string GetErrorMessage()
    {
        var ptr = DecentDBNative.ddb_last_error_message();
        if (ptr == IntPtr.Zero)
        {
            return string.Empty;
        }

        return Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _handle.Dispose();
    }

    public PreparedStatement Prepare(string sql)
    {
        var sqlBytes = Encoding.UTF8.GetBytes(sql + "\0");
        IntPtr stmtPtr;
        unsafe
        {
            fixed (byte* pSql = sqlBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_prepare(Handle, pSql, out stmtPtr));
                if (res != 0 || stmtPtr == IntPtr.Zero)
                {
                    throw new DecentDBException(_lastErrorCode, LastErrorMessage, sql);
                }
            }
        }
        return new PreparedStatement(this, stmtPtr, sql);
    }

    public void Checkpoint()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_checkpoint(Handle));
        if (res != 0)
        {
            throw new DecentDBException(_lastErrorCode, LastErrorMessage, "Checkpoint");
        }
    }

    public void BeginTransaction()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_begin_transaction(Handle));
        if (res != 0)
        {
            throw new DecentDBException(_lastErrorCode, LastErrorMessage, "BEGIN");
        }
    }

    public void CommitTransaction()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_commit_transaction(Handle, out _));
        if (res != 0)
        {
            throw new DecentDBException(_lastErrorCode, LastErrorMessage, "COMMIT");
        }
    }

    public void RollbackTransaction()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_rollback_transaction(Handle));
        if (res != 0)
        {
            throw new DecentDBException(_lastErrorCode, LastErrorMessage, "ROLLBACK");
        }
    }

    /// <summary>
    /// Export the database to a new on-disk file at the specified path.
    /// The destination file must not already exist.
    /// </summary>
    public void SaveAs(string destPath)
    {
        var pathBytes = Encoding.UTF8.GetBytes(destPath + "\0");
        var pathPtr = Marshal.AllocHGlobal(pathBytes.Length);
        try
        {
            Marshal.Copy(pathBytes, 0, pathPtr, pathBytes.Length);
            var res = RecordStatus(DecentDBNative.ddb_db_save_as(Handle, pathPtr));
            if (res != 0)
            {
                throw new DecentDBException(_lastErrorCode, LastErrorMessage, "SaveAs");
            }
        }
        finally
        {
            Marshal.FreeHGlobal(pathPtr);
        }
    }

    internal IntPtr GetDbHandle() => Handle;

    /// <summary>
    /// Returns a JSON array of table names, e.g. ["users","items"].
    /// </summary>
    public string ListTablesJson()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_list_tables_json(Handle, out var ptr));
        if (res != 0)
        {
            throw new DecentDBException(_lastErrorCode, LastErrorMessage, "ListTablesJson");
        }

        if (ptr == IntPtr.Zero)
        {
            return "[]";
        }

        try
        {
            using var document = JsonDocument.Parse(Marshal.PtrToStringUTF8(ptr) ?? "[]");
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                return "[]";
            }

            var tableNames = new List<string>();
            foreach (var element in document.RootElement.EnumerateArray())
            {
                if (element.ValueKind == JsonValueKind.String)
                {
                    tableNames.Add(element.GetString() ?? string.Empty);
                }
                else if (element.ValueKind == JsonValueKind.Object &&
                         element.TryGetProperty("name", out var nameProperty))
                {
                    tableNames.Add(nameProperty.GetString() ?? string.Empty);
                }
            }

            return JsonSerializer.Serialize(tableNames);
        }
        finally
        {
            DecentDBNative.ddb_string_free(ref ptr);
        }
    }

    /// <summary>
    /// Returns a JSON array of column metadata for a given table.
    /// </summary>
    public string GetTableColumnsJson(string tableName)
    {
        var nameBytes = Encoding.UTF8.GetBytes(tableName + "\0");
        IntPtr ptr;
        unsafe
        {
            fixed (byte* pName = nameBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_describe_table_json(Handle, pName, out ptr));
                if (res != 0)
                {
                    throw new DecentDBException(_lastErrorCode, LastErrorMessage, "GetTableColumnsJson");
                }
            }
        }

        try
        {
            if (ptr == IntPtr.Zero)
            {
                return "[]";
            }

            using var document = JsonDocument.Parse(Marshal.PtrToStringUTF8(ptr) ?? "{}");
            if (!document.RootElement.TryGetProperty("columns", out var columns) ||
                columns.ValueKind != JsonValueKind.Array)
            {
                return "[]";
            }

            var normalized = new List<Dictionary<string, object?>>();
            foreach (var column in columns.EnumerateArray())
            {
                var type = column.TryGetProperty("type", out var typeProperty)
                    ? typeProperty.GetString()
                    : column.TryGetProperty("column_type", out var columnTypeProperty)
                        ? columnTypeProperty.GetString()
                        : string.Empty;

                var notNull = column.TryGetProperty("not_null", out var notNullProperty)
                    ? notNullProperty.GetBoolean()
                    : column.TryGetProperty("nullable", out var nullableProperty) && !nullableProperty.GetBoolean();

                normalized.Add(new Dictionary<string, object?>
                {
                    ["name"] = column.TryGetProperty("name", out var nameProperty) ? nameProperty.GetString() : string.Empty,
                    ["type"] = type ?? string.Empty,
                    ["not_null"] = notNull,
                    ["unique"] = column.TryGetProperty("unique", out var uniqueProperty) && uniqueProperty.GetBoolean(),
                    ["primary_key"] = column.TryGetProperty("primary_key", out var primaryKeyProperty) && primaryKeyProperty.GetBoolean()
                });
            }

            return JsonSerializer.Serialize(normalized);
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                DecentDBNative.ddb_string_free(ref ptr);
            }
        }
    }

    /// <summary>
    /// Returns a JSON array of index metadata objects.
    /// </summary>
    public string ListIndexesJson()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_list_indexes_json(Handle, out var ptr));
        if (res != 0)
        {
            throw new DecentDBException(_lastErrorCode, LastErrorMessage, "ListIndexesJson");
        }

        if (ptr == IntPtr.Zero)
        {
            return "[]";
        }

        try
        {
            using var document = JsonDocument.Parse(Marshal.PtrToStringUTF8(ptr) ?? "[]");
            if (document.RootElement.ValueKind != JsonValueKind.Array)
            {
                return "[]";
            }

            var normalized = new List<Dictionary<string, object?>>();
            foreach (var index in document.RootElement.EnumerateArray())
            {
                var columns = new List<string>();
                if (index.TryGetProperty("columns", out var columnList) &&
                    columnList.ValueKind == JsonValueKind.Array)
                {
                    foreach (var column in columnList.EnumerateArray())
                    {
                        columns.Add(column.GetString() ?? string.Empty);
                    }
                }

                normalized.Add(new Dictionary<string, object?>
                {
                    ["name"] = index.TryGetProperty("name", out var nameProperty) ? nameProperty.GetString() : string.Empty,
                    ["table"] = index.TryGetProperty("table", out var tableProperty)
                        ? tableProperty.GetString()
                        : index.TryGetProperty("table_name", out var tableNameProperty)
                            ? tableNameProperty.GetString()
                            : string.Empty,
                    ["kind"] = index.TryGetProperty("kind", out var kindProperty) ? kindProperty.GetString() : string.Empty,
                    ["unique"] = index.TryGetProperty("unique", out var uniqueProperty) && uniqueProperty.GetBoolean(),
                    ["columns"] = columns,
                    ["predicate_sql"] = index.TryGetProperty("predicate_sql", out var predicateProperty) && predicateProperty.ValueKind != JsonValueKind.Null
                        ? predicateProperty.GetString()
                        : null,
                    ["fresh"] = index.TryGetProperty("fresh", out var freshProperty) && freshProperty.GetBoolean()
                });
            }

            return JsonSerializer.Serialize(normalized);
        }
        finally
        {
            DecentDBNative.ddb_string_free(ref ptr);
        }
    }
}

public sealed class PreparedStatement : IDisposable
{
    private readonly DecentDB _db;
    private readonly DecentDBStatementHandle _handle;
    private bool _disposed;
    private readonly string _sql;
    private readonly int _parameterCount;

    public IntPtr Handle => _handle.Handle;

    internal PreparedStatement(DecentDB db, IntPtr stmtPtr, string sql)
    {
        _db = db;
        _sql = sql;
        _parameterCount = DetectParameterCount(sql);
        _handle = new DecentDBStatementHandle(stmtPtr, db.DbHandle);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _handle.Dispose();
    }

    public PreparedStatement Reset()
    {
        var res = _db.RecordStatus(DecentDBNative.ddb_stmt_reset(Handle));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement ClearBindings()
    {
        var res = _db.RecordStatus(DecentDBNative.ddb_stmt_clear_bindings(Handle));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindNull(int index1Based)
    {
        ValidateBindIndex(index1Based);
        var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_null(Handle, checked((nuint)index1Based)));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindInt64(int index1Based, long value)
    {
        ValidateBindIndex(index1Based);
        var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_int64(Handle, checked((nuint)index1Based), value));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindFloat64(int index1Based, double value)
    {
        ValidateBindIndex(index1Based);
        var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_float64(Handle, checked((nuint)index1Based), value));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindBool(int index1Based, bool value)
    {
        ValidateBindIndex(index1Based);
        var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_bool(Handle, checked((nuint)index1Based), value ? (byte)1 : (byte)0));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindGuid(int index1Based, Guid value)
    {
        ValidateBindIndex(index1Based);
        unsafe
        {
            var bytes = stackalloc byte[16];
            if (!value.TryWriteBytes(new Span<byte>(bytes, 16)))
                throw new InvalidOperationException("Failed to write Guid bytes");

            var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_blob(Handle, checked((nuint)index1Based), bytes, 16));
            if (res != 0)
            {
                throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
            }
        }
        return this;
    }

    public PreparedStatement BindDatetime(int index1Based, long microsUtc)
    {
        ValidateBindIndex(index1Based);
        var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_timestamp_micros(Handle, checked((nuint)index1Based), microsUtc));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindDecimal(int index1Based, decimal value)
    {
        ValidateBindIndex(index1Based);
        // DecentDB currently supports DECIMAL backed by INT64 (approx 18 digits).
        // C# decimal is 96-bit integer + scale. We must check if it fits in 64-bit.

        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        int low = bits[0];
        int mid = bits[1];
        int high = bits[2];
        int flags = bits[3];
        int scale = (flags >> 16) & 0xFF;
        bool isNegative = (flags & 0x80000000) != 0;

        if (high != 0)
        {
            throw new OverflowException("Value is too large for DecentDB DECIMAL (must fit in 64-bit unscaled integer)");
        }

        // Combine Mid and Low
        ulong unscaledU = ((ulong)(uint)mid << 32) | (uint)low;

        if (unscaledU > (ulong)long.MaxValue + (ulong)(isNegative ? 1 : 0))
        {
            throw new OverflowException("Value is too large for DecentDB DECIMAL (must fit in 64-bit unscaled integer)");
        }

        long unscaled = (long)unscaledU;
        if (isNegative) unscaled = -unscaled;

        var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_decimal(Handle, checked((nuint)index1Based), unscaled, checked((byte)scale)));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindText(int index1Based, string? value)
    {
        if (value == null) return BindNull(index1Based);
        var bytes = Encoding.UTF8.GetBytes(value);
        return BindTextBytes(index1Based, bytes);
    }

    public PreparedStatement BindTextBytes(int index1Based, byte[] bytes)
    {
        var len = bytes?.Length ?? 0;
        if (len == 0)
        {
            unsafe
            {
                var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_text(Handle, checked((nuint)index1Based), null, 0));
                if (res != 0)
                {
                    throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
            return this;
        }

        unsafe
        {
            fixed (byte* pBytes = bytes)
            {
                var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_text(Handle, checked((nuint)index1Based), pBytes, checked((nuint)len)));
                if (res != 0)
                {
                    throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
        }
        return this;
    }

    public PreparedStatement BindBlob(int index1Based, byte[] bytes)
    {
        var len = bytes?.Length ?? 0;
        if (len == 0)
        {
            unsafe
            {
                var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_blob(Handle, checked((nuint)index1Based), null, 0));
                if (res != 0)
                {
                    throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
            return this;
        }

        unsafe
        {
            fixed (byte* pBytes = bytes)
            {
                var res = _db.RecordStatus(DecentDBNativeUnsafe.ddb_stmt_bind_blob(Handle, checked((nuint)index1Based), pBytes, checked((nuint)len)));
                if (res != 0)
                {
                    throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
        }
        return this;
    }

    public int Step()
    {
        var res = _db.RecordStatus(DecentDBNative.ddb_stmt_step(Handle, out var hasRow));
        if (res != 0)
        {
            return -checked((int)res);
        }

        return hasRow != 0 ? 1 : 0;
    }

    public int ColumnCount
    {
        get
        {
            var res = _db.RecordStatus(DecentDBNative.ddb_stmt_column_count(Handle, out var columns));
            if (res != 0)
            {
                throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
            }

            return checked((int)columns);
        }
    }

    public string ColumnName(int col0Based)
    {
        if (!IsAccessibleColumnIndex(col0Based))
        {
            return string.Empty;
        }

        var res = _db.RecordStatus(DecentDBNative.ddb_stmt_column_name_copy(Handle, checked((nuint)col0Based), out var ptr));
        if (res != 0)
        {
            return string.Empty;
        }

        try
        {
            return ptr == IntPtr.Zero ? string.Empty : Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
        }
        finally
        {
            if (ptr != IntPtr.Zero)
            {
                DecentDBNative.ddb_string_free(ref ptr);
            }
        }
    }

    public int ColumnType(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return LegacyColumnTypeFromTag((DdbValueTag)value.tag);
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public bool IsNull(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return (DdbValueTag)value.tag == DdbValueTag.Null;
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public bool GetBool(int col0Based)
    {
        return GetInt64(col0Based) != 0;
    }

    public Guid GetGuid(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            switch ((DdbValueTag)value.tag)
            {
                case DdbValueTag.Uuid:
                    unsafe
                    {
                        byte* uuidBytes = value.uuid_bytes;
                        return new Guid(new ReadOnlySpan<byte>(uuidBytes, 16));
                    }
                case DdbValueTag.Blob:
                    unsafe
                    {
                        if (value.data != null && value.len == 16)
                        {
                            return new Guid(new ReadOnlySpan<byte>(value.data, 16));
                        }
                    }
                    break;
                case DdbValueTag.Text:
                {
                    var text = GetTextFromValue(value);
                    if (Guid.TryParse(text, out var guid))
                    {
                        return guid;
                    }

                    break;
                }
            }

            return Guid.Empty;
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public long GetTimestampMicros(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return (DdbValueTag)value.tag switch
            {
                DdbValueTag.TimestampMicros => value.timestamp_micros,
                DdbValueTag.Int64 => value.int64_value,
                DdbValueTag.Bool => value.bool_value != 0 ? 1L : 0L,
                _ => 0L
            };
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public long GetInt64(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return (DdbValueTag)value.tag switch
            {
                DdbValueTag.Int64 => value.int64_value,
                DdbValueTag.Bool => value.bool_value != 0 ? 1L : 0L,
                DdbValueTag.TimestampMicros => value.timestamp_micros,
                _ => 0L
            };
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public double GetFloat64(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return (DdbValueTag)value.tag switch
            {
                DdbValueTag.Float64 => value.float64_value,
                DdbValueTag.Int64 => value.int64_value,
                _ => 0.0
            };
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public decimal GetDecimal(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            if ((DdbValueTag)value.tag != DdbValueTag.Decimal)
            {
                return decimal.Parse(GetTextFromValue(value));
            }

            bool isNegative = value.decimal_scaled < 0;
            ulong magnitude = isNegative
                ? unchecked((ulong)(-value.decimal_scaled))
                : unchecked((ulong)value.decimal_scaled);

            int lo = (int)(magnitude & 0xFFFFFFFF);
            int mid = (int)(magnitude >> 32);
            int hi = 0;

            return new decimal(lo, mid, hi, isNegative, value.decimal_scale);
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public string GetText(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return GetTextFromValue(value);
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public byte[] GetBlob(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return GetBlobFromValue(value);
        }
        finally
        {
            DisposeValue(ref value);
        }
    }

    public long RowsAffected
    {
        get
        {
            var res = _db.RecordStatus(DecentDBNative.ddb_stmt_affected_rows(Handle, out var rows));
            if (res != 0)
            {
                throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
            }

            return checked((long)rows);
        }
    }

    public RowView GetRowView()
    {
        var count = ColumnCount;
        var values = new DecentdbValueView[count];
        for (int i = 0; i < count; i++)
        {
            var value = CopyValue(i);
            try
            {
                values[i] = ToRowViewValue(value);
            }
            finally
            {
                DisposeValue(ref value);
            }
        }

        return new RowView(values);
    }

    private DdbValueNative CopyValue(int col0Based)
    {
        if (!IsAccessibleColumnIndex(col0Based))
        {
            return default;
        }

        var res = _db.RecordStatus(DecentDBNative.ddb_stmt_value_copy(Handle, checked((nuint)col0Based), out var value));
        if (res != 0)
        {
            return default;
        }

        return value;
    }

    private bool IsAccessibleColumnIndex(int col0Based)
    {
        if (col0Based < 0)
        {
            return false;
        }

        return col0Based < ColumnCount;
    }

    private void ValidateBindIndex(int index1Based)
    {
        if (index1Based <= 0 || index1Based > _parameterCount)
        {
            var message = $"parameter index {index1Based} is out of range for this statement";
            _db.SetManagedError(5, message);
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
    }

    private static int DetectParameterCount(string sql)
    {
        var maxIndex = 0;
        for (var i = 0; i < sql.Length; i++)
        {
            if (sql[i] != '$' || i + 1 >= sql.Length || !char.IsAsciiDigit(sql[i + 1]))
            {
                continue;
            }

            var j = i + 1;
            var value = 0;
            while (j < sql.Length && char.IsAsciiDigit(sql[j]))
            {
                value = checked((value * 10) + (sql[j] - '0'));
                j++;
            }

            maxIndex = Math.Max(maxIndex, value);
            i = j - 1;
        }

        return maxIndex;
    }

    private static void DisposeValue(ref DdbValueNative value)
    {
        DecentDBNative.ddb_value_dispose(ref value);
    }

    private static int LegacyColumnTypeFromTag(DdbValueTag tag)
    {
        return tag switch
        {
            DdbValueTag.Int64 => (int)DbValueKind.Int64,
            DdbValueTag.Bool => (int)DbValueKind.Bool,
            DdbValueTag.Float64 => (int)DbValueKind.Float64,
            DdbValueTag.Text => (int)DbValueKind.Text,
            DdbValueTag.Blob => (int)DbValueKind.Blob,
            DdbValueTag.Uuid => (int)DbValueKind.Blob,
            DdbValueTag.Decimal => (int)DbValueKind.Decimal,
            DdbValueTag.TimestampMicros => (int)DbValueKind.Timestamp,
            _ => (int)DbValueKind.Null
        };
    }

    private static unsafe string GetTextFromValue(DdbValueNative value)
    {
        return (DdbValueTag)value.tag switch
        {
            DdbValueTag.Text => value.data == null || value.len == 0
                ? string.Empty
                : Marshal.PtrToStringUTF8((IntPtr)value.data, checked((int)value.len)) ?? string.Empty,
            DdbValueTag.Uuid => GetGuidString(value),
            DdbValueTag.Int64 => value.int64_value.ToString(),
            DdbValueTag.Float64 => value.float64_value.ToString(),
            DdbValueTag.Bool => value.bool_value != 0 ? bool.TrueString : bool.FalseString,
            DdbValueTag.Decimal => GetDecimalString(value),
            DdbValueTag.TimestampMicros => value.timestamp_micros.ToString(),
            _ => string.Empty
        };
    }

    private static unsafe byte[] GetBlobFromValue(DdbValueNative value)
    {
        return (DdbValueTag)value.tag switch
        {
            DdbValueTag.Blob => CopyBytes(value.data, value.len),
            DdbValueTag.Uuid =>
                value.len == 0
                    ? CopyUuidBytes(value)
                    : CopyBytes(value.data, value.len),
            _ => Array.Empty<byte>()
        };
    }

    private static unsafe byte[] CopyBytes(byte* data, nuint len)
    {
        if (data == null || len == 0)
        {
            return Array.Empty<byte>();
        }

        var bytes = new byte[checked((int)len)];
        Marshal.Copy((IntPtr)data, bytes, 0, bytes.Length);
        return bytes;
    }

    private static unsafe byte[] CopyUuidBytes(DdbValueNative value)
    {
        var bytes = new byte[16];
        byte* uuidBytes = value.uuid_bytes;
        Marshal.Copy((IntPtr)uuidBytes, bytes, 0, bytes.Length);

        return bytes;
    }

    private static unsafe string GetGuidString(DdbValueNative value)
    {
        byte* uuidBytes = value.uuid_bytes;
        return new Guid(new ReadOnlySpan<byte>(uuidBytes, 16)).ToString();
    }

    private static string GetDecimalString(DdbValueNative value)
    {
        bool isNegative = value.decimal_scaled < 0;
        ulong magnitude = isNegative
            ? unchecked((ulong)(-value.decimal_scaled))
            : unchecked((ulong)value.decimal_scaled);

        int lo = (int)(magnitude & 0xFFFFFFFF);
        int mid = (int)(magnitude >> 32);
        int hi = 0;

        return new decimal(lo, mid, hi, isNegative, value.decimal_scale).ToString();
    }

    private static DecentdbValueView ToRowViewValue(DdbValueNative value)
    {
        var tag = (DdbValueTag)value.tag;
        return new DecentdbValueView
        {
            kind = LegacyColumnTypeFromTag(tag),
            is_null = tag == DdbValueTag.Null ? 1 : 0,
            int64_val = tag switch
            {
                DdbValueTag.Int64 => value.int64_value,
                DdbValueTag.Bool => value.bool_value != 0 ? 1L : 0L,
                DdbValueTag.Decimal => value.decimal_scaled,
                DdbValueTag.TimestampMicros => value.timestamp_micros,
                _ => 0L
            },
            float64_val = tag == DdbValueTag.Float64 ? value.float64_value : 0.0,
            bytes = IntPtr.Zero,
            bytes_len = checked((int)value.len),
            decimal_scale = value.decimal_scale
        };
    }
}

public readonly struct RowView
{
    private readonly DecentdbValueView[] _values;

    public RowView(DecentdbValueView[] values)
    {
        _values = values ?? Array.Empty<DecentdbValueView>();
    }

    public int Count => _values.Length;

    public DecentdbValueView this[int index]
    {
        get
        {
            if ((uint)index >= (uint)_values.Length) throw new IndexOutOfRangeException();
            return _values[index];
        }
    }
}

public class DecentDBException : Exception
{
    public int ErrorCode { get; }
    public string Sql { get; }

    public DecentDBException(int errorCode, string message, string sql) : base($"DecentDB error {errorCode}: {message}")
    {
        ErrorCode = errorCode;
        Sql = sql;
    }
}
