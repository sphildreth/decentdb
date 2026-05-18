using System;
using System.Buffers;
using System.Globalization;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace DecentDB.Native;

public enum DbOpenMode
{
    OpenOrCreate = 0,
    Create = 1,
    Open = 2
}

public readonly struct DecentDBEnumValue
{
    public DecentDBEnumValue(ulong typeId, ulong labelId)
    {
        TypeId = typeId;
        LabelId = labelId;
    }

    public ulong TypeId { get; }

    public ulong LabelId { get; }

    public override string ToString() => $"enum:{TypeId}:{LabelId}";
}

public readonly struct DecentDBIntervalValue
{
    public DecentDBIntervalValue(int months, int days, long microseconds)
    {
        Months = months;
        Days = days;
        Microseconds = microseconds;
    }

    public int Months { get; }

    public int Days { get; }

    public long Microseconds { get; }

    public bool TryAsTimeSpan(out TimeSpan value)
    {
        if (Months != 0)
        {
            value = default;
            return false;
        }

        try
        {
            var dayTicks = checked((long)Days * TimeSpan.TicksPerDay);
            var microTicks = checked(Microseconds * 10L);
            value = new TimeSpan(checked(dayTicks + microTicks));
            return true;
        }
        catch (OverflowException)
        {
            value = default;
            return false;
        }
    }

    public override string ToString() => $"months={Months},days={Days},micros={Microseconds}";
}

public sealed class DecentDB : IDisposable
{
    private readonly DecentDBHandle _handle;
    private bool _disposed;
    private int _lastErrorCode;
    private string _lastErrorMessage = string.Empty;

    public IntPtr Handle => _handle.Handle;
    public DecentDBSyncClient Sync { get; }

    internal DecentDBHandle DbHandle => _handle;

    public static uint AbiVersion() => DecentDBNative.ddb_abi_version();

    public static string EngineVersion()
    {
        var ptr = DecentDBNative.ddb_version();
        return ptr == IntPtr.Zero ? string.Empty : Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
    }

    public DecentDB(string path, string? options = null)
        : this(path, DbOpenMode.OpenOrCreate, options)
    {
    }

    public DecentDB(string path, DbOpenMode mode, string? options = null)
    {
        var pathBytes = Encoding.UTF8.GetBytes(path + "\0");
        IntPtr ptr;
        unsafe
        {
            fixed (byte* pPath = pathBytes)
            {
                uint res = mode switch
                {
                    DbOpenMode.Create => RecordStatus(DecentDBNativeUnsafe.ddb_db_create(pPath, out ptr)),
                    DbOpenMode.Open => RecordStatus(DecentDBNativeUnsafe.ddb_db_open(pPath, out ptr)),
                    _ => RecordStatus(DecentDBNativeUnsafe.ddb_db_open_or_create(pPath, out ptr))
                };
                if (res != 0 || ptr == IntPtr.Zero)
                {
                    throw new DecentDBException(_lastErrorCode, LastErrorMessage, $"Open({mode})");
                }
            }
        }

        _handle = new DecentDBHandle(ptr);
        Sync = new DecentDBSyncClient(this);
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

    public bool InTransaction
    {
        get
        {
            var res = RecordStatus(DecentDBNative.ddb_db_in_transaction(Handle, out var flag));
            if (res != 0)
            {
                throw new DecentDBException(_lastErrorCode, LastErrorMessage, "InTransaction");
            }

            return flag != 0;
        }
    }

    public string GetTableDdl(string tableName)
    {
        var nameBytes = Encoding.UTF8.GetBytes(tableName + "\0");
        unsafe
        {
            fixed (byte* p = nameBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_get_table_ddl(Handle, p, out var ptr));
                if (res != 0) throw new DecentDBException(_lastErrorCode, LastErrorMessage, "GetTableDdl");
                return FreeStringOrEmpty(ptr);
            }
        }
    }

    public string ListViewsJson()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_list_views_json(Handle, out var ptr));
        if (res != 0) throw new DecentDBException(_lastErrorCode, LastErrorMessage, "ListViewsJson");
        return FreeStringOrEmpty(ptr);
    }

    public string GetViewDdl(string viewName)
    {
        var nameBytes = Encoding.UTF8.GetBytes(viewName + "\0");
        unsafe
        {
            fixed (byte* p = nameBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_get_view_ddl(Handle, p, out var ptr));
                if (res != 0) throw new DecentDBException(_lastErrorCode, LastErrorMessage, "GetViewDdl");
                return FreeStringOrEmpty(ptr);
            }
        }
    }

    public string ListTriggersJson()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_list_triggers_json(Handle, out var ptr));
        if (res != 0) throw new DecentDBException(_lastErrorCode, LastErrorMessage, "ListTriggersJson");
        return FreeStringOrEmpty(ptr);
    }

    public string GetToolingMetadataJson()
    {
        var res = RecordStatus(DecentDBNative.ddb_db_get_tooling_metadata_json(Handle, out var ptr));
        if (res != 0) throw new DecentDBException(_lastErrorCode, LastErrorMessage, "GetToolingMetadataJson");
        return FreeStringOrEmpty(ptr);
    }

    public string DescribeQueryJson(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        var sqlBytes = Encoding.UTF8.GetBytes(sql + "\0");
        IntPtr ptr;
        unsafe
        {
            fixed (byte* pSql = sqlBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_describe_query_json(Handle, pSql, out ptr));
                if (res != 0) throw new DecentDBException(_lastErrorCode, LastErrorMessage, "DescribeQueryJson");
            }
        }

        return FreeStringOrEmpty(ptr);
    }

    public string SyncExecuteJson(string requestJson)
    {
        ArgumentNullException.ThrowIfNull(requestJson);
        var requestBytes = Encoding.UTF8.GetBytes(requestJson + "\0");
        IntPtr ptr;
        unsafe
        {
            fixed (byte* pRequest = requestBytes)
            {
                var res = RecordStatus(DecentDBNativeUnsafe.ddb_db_sync_execute_json(Handle, pRequest, out ptr));
                if (res != 0)
                {
                    throw new DecentDBException(_lastErrorCode, LastErrorMessage, "SyncExecuteJson");
                }
            }
        }

        return FreeStringOrEmpty(ptr);
    }

    private static string FreeStringOrEmpty(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero) return string.Empty;
        try { return Marshal.PtrToStringUTF8(ptr) ?? string.Empty; }
        finally { DecentDBNative.ddb_string_free(ref ptr); }
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
    private int _columnCount = -1;

    public IntPtr Handle => _handle.Handle;

    internal PreparedStatement(DecentDB db, IntPtr stmtPtr, string sql)
    {
        _db = db;
        _sql = sql;
        _parameterCount = DetectParameterCount(sql);
        _handle = new DecentDBStatementHandle(stmtPtr, db.DbHandle);
    }

    public int LastErrorCode => _db.LastErrorCode;

    public string LastErrorMessage => _db.LastErrorMessage;

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

            var res = _db.RecordStatus(
                DecentDBNativeUnsafe.ddb_stmt_bind_uuid(
                    Handle,
                    checked((nuint)index1Based),
                    bytes));
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

        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount == 0)
        {
            return BindTextBytes(index1Based, Array.Empty<byte>());
        }

        if (byteCount <= 512)
        {
            Span<byte> stackBuffer = stackalloc byte[byteCount];
            var written = Encoding.UTF8.GetBytes(value.AsSpan(), stackBuffer);
            return BindTextSpan(index1Based, stackBuffer[..written]);
        }

        var pooled = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var written = Encoding.UTF8.GetBytes(value.AsSpan(), pooled.AsSpan(0, byteCount));
            return BindTextSpan(index1Based, pooled.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(pooled);
        }
    }

    public PreparedStatement BindTextBytes(int index1Based, byte[] bytes)
    {
        if (bytes == null || bytes.Length == 0)
        {
            return BindTextSpan(index1Based, ReadOnlySpan<byte>.Empty);
        }

        return BindTextSpan(index1Based, bytes);
    }

    private PreparedStatement BindTextSpan(int index1Based, ReadOnlySpan<byte> bytes)
    {
        var len = bytes.Length;
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

    public PreparedStatement BindGeometryWkb(int index1Based, byte[] bytes)
    {
        return BindSpatialWkb(index1Based, bytes, geography: false);
    }

    public PreparedStatement BindGeographyWkb(int index1Based, byte[] bytes)
    {
        return BindSpatialWkb(index1Based, bytes, geography: true);
    }

    private PreparedStatement BindSpatialWkb(int index1Based, byte[] bytes, bool geography)
    {
        var len = bytes?.Length ?? 0;
        unsafe
        {
            if (len == 0)
            {
                var emptyStatus = geography
                    ? DecentDBNativeUnsafe.ddb_stmt_bind_geography_wkb(Handle, checked((nuint)index1Based), null, 0)
                    : DecentDBNativeUnsafe.ddb_stmt_bind_geometry_wkb(Handle, checked((nuint)index1Based), null, 0);
                var emptyRes = _db.RecordStatus(emptyStatus);
                if (emptyRes != 0)
                {
                    throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
                return this;
            }

            fixed (byte* pBytes = bytes)
            {
                var status = geography
                    ? DecentDBNativeUnsafe.ddb_stmt_bind_geography_wkb(Handle, checked((nuint)index1Based), pBytes, checked((nuint)len))
                    : DecentDBNativeUnsafe.ddb_stmt_bind_geometry_wkb(Handle, checked((nuint)index1Based), pBytes, checked((nuint)len));
                var res = _db.RecordStatus(status);
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
            if (_columnCount >= 0)
            {
                return _columnCount;
            }

            var res = _db.RecordStatus(DecentDBNative.ddb_stmt_column_count(Handle, out var columns));
            if (res != 0)
            {
                throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
            }

            _columnCount = checked((int)columns);
            return _columnCount;
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
                DdbValueTag.TimestamptzMicros => value.timestamptz_micros,
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
                DdbValueTag.TimestamptzMicros => value.timestamptz_micros,
                DdbValueTag.Date => value.date_days,
                DdbValueTag.Time => value.time_micros,
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
            return GetDecimalValue(value);
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

    public object GetValueObject(int col0Based)
    {
        var value = CopyValue(col0Based);
        try
        {
            return (DdbValueTag)value.tag switch
            {
                DdbValueTag.Null => DBNull.Value,
                DdbValueTag.Int64 => value.int64_value,
                DdbValueTag.Bool => value.bool_value != 0,
                DdbValueTag.Float64 => value.float64_value,
                DdbValueTag.Text => GetTextFromValue(value),
                DdbValueTag.Blob => GetBlobFromValue(value),
                DdbValueTag.Geometry => GetBlobFromValue(value),
                DdbValueTag.Geography => GetBlobFromValue(value),
                DdbValueTag.Decimal => GetDecimalValue(value),
                DdbValueTag.Uuid => GetBlobFromValue(value),
                DdbValueTag.TimestampMicros => FromUnixEpochMicroseconds(value.timestamp_micros),
                DdbValueTag.Enum => new DecentDBEnumValue(value.enum_type_id, value.enum_label_id),
                DdbValueTag.IpAddr => GetIpAddressString(value),
                DdbValueTag.Cidr => GetCidrString(value),
                DdbValueTag.Date => GetDateValue(value.date_days),
                DdbValueTag.Time => GetTimeValue(value.time_micros),
                DdbValueTag.TimestamptzMicros => FromUnixEpochMicrosecondsOffset(value.timestamptz_micros),
                DdbValueTag.Interval => GetIntervalValue(value.interval_months, value.interval_days, value.interval_micros),
                DdbValueTag.MacAddr => GetMacAddressString(value),
                _ => DBNull.Value
            };
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

    public long StepRowsAffected()
    {
        var res = _db.RecordStatus(DecentDBNative.ddb_stmt_step(Handle, out _));
        if (res != 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return RowsAffected;
    }

    public long ExecuteBatchInt64(ReadOnlySpan<long> values)
    {
        unsafe
        {
            fixed (long* pValues = values)
            {
                var res = _db.RecordStatus(
                    DecentDBNativeUnsafe.ddb_stmt_execute_batch_i64(Handle, (nuint)values.Length, pValues, out var affected));
                if (res != 0) throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                return (long)affected;
            }
        }
    }

    public long ExecuteBatchTypedOneRow(
        ReadOnlySpan<byte> signatureUtf8,
        ReadOnlySpan<long> i64Values,
        ReadOnlySpan<double> f64Values,
        byte[]? text0,
        byte[]? text1,
        int textCount)
    {
        unsafe
        {
            fixed (byte* pSignature = signatureUtf8)
            {
                long affected;
                if (i64Values.IsEmpty)
                {
                    if (f64Values.IsEmpty)
                    {
                        affected = ExecuteBatchTypedOneRowCore(
                            pSignature,
                            null,
                            null,
                            text0,
                            text1,
                            textCount);
                    }
                    else
                    {
                        fixed (double* pF64 = f64Values)
                        {
                            affected = ExecuteBatchTypedOneRowCore(
                                pSignature,
                                null,
                                pF64,
                                text0,
                                text1,
                                textCount);
                        }
                    }
                }
                else
                {
                    fixed (long* pI64 = i64Values)
                    {
                        if (f64Values.IsEmpty)
                        {
                            affected = ExecuteBatchTypedOneRowCore(
                                pSignature,
                                pI64,
                                null,
                                text0,
                                text1,
                                textCount);
                        }
                        else
                        {
                            fixed (double* pF64 = f64Values)
                            {
                                affected = ExecuteBatchTypedOneRowCore(
                                    pSignature,
                                    pI64,
                                    pF64,
                                    text0,
                                    text1,
                                    textCount);
                            }
                        }
                    }
                }

                return affected;
            }
        }
    }

    private unsafe long ExecuteBatchTypedOneRowCore(
        byte* signatureUtf8,
        long* valuesI64,
        double* valuesF64,
        byte[]? text0,
        byte[]? text1,
        int textCount)
    {
        unsafe
        {
            switch (textCount)
            {
                case 0:
                {
                    var res = _db.RecordStatus(
                        DecentDBNativeUnsafe.ddb_stmt_execute_batch_typed(
                            Handle,
                            1,
                            signatureUtf8,
                            valuesI64,
                            valuesF64,
                            null,
                            null,
                            out var affected));
                    if (res != 0)
                    {
                        throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                    }

                    return (long)affected;
                }
                case 1:
                {
                    fixed (byte* pText0 = text0)
                    {
                        byte** textPtrs = stackalloc byte*[1];
                        nuint* textLens = stackalloc nuint[1];
                        textPtrs[0] = pText0;
                        textLens[0] = (nuint)(text0?.Length ?? 0);
                        var res = _db.RecordStatus(
                            DecentDBNativeUnsafe.ddb_stmt_execute_batch_typed(
                                Handle,
                                1,
                                signatureUtf8,
                                valuesI64,
                                valuesF64,
                                textPtrs,
                                textLens,
                                out var affected));
                        if (res != 0)
                        {
                            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                        }

                        return (long)affected;
                    }
                }
                case 2:
                {
                    fixed (byte* pText0 = text0)
                    fixed (byte* pText1 = text1)
                    {
                        byte** textPtrs = stackalloc byte*[2];
                        nuint* textLens = stackalloc nuint[2];
                        textPtrs[0] = pText0;
                        textPtrs[1] = pText1;
                        textLens[0] = (nuint)(text0?.Length ?? 0);
                        textLens[1] = (nuint)(text1?.Length ?? 0);
                        var res = _db.RecordStatus(
                            DecentDBNativeUnsafe.ddb_stmt_execute_batch_typed(
                                Handle,
                                1,
                                signatureUtf8,
                                valuesI64,
                                valuesF64,
                                textPtrs,
                                textLens,
                                out var affected));
                        if (res != 0)
                        {
                            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                        }

                        return (long)affected;
                    }
                }
                default:
                    throw new ArgumentOutOfRangeException(nameof(textCount));
            }
        }
    }

    public long RebindInt64Execute(long value)
    {
        var res = _db.RecordStatus(
            DecentDBNativeUnsafe.ddb_stmt_rebind_int64_execute(Handle, value, out var affected));
        if (res != 0) throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        return (long)affected;
    }

    public long RebindTextInt64Execute(byte[] utf8Text, long intValue)
    {
        unsafe
        {
            fixed (byte* pText = utf8Text)
            {
                var res = _db.RecordStatus(
                    DecentDBNativeUnsafe.ddb_stmt_rebind_text_int64_execute(Handle, pText, (nuint)utf8Text.Length, intValue, out var affected));
                if (res != 0) throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                return (long)affected;
            }
        }
    }

    public long RebindInt64TextExecute(long intValue, byte[] utf8Text)
    {
        unsafe
        {
            fixed (byte* pText = utf8Text)
            {
                var res = _db.RecordStatus(
                    DecentDBNativeUnsafe.ddb_stmt_rebind_int64_text_execute(Handle, intValue, pText, (nuint)utf8Text.Length, out var affected));
                if (res != 0) throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                return (long)affected;
            }
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
            DdbValueTag.Geometry => (int)DbValueKind.Blob,
            DdbValueTag.Geography => (int)DbValueKind.Blob,
            DdbValueTag.Uuid => (int)DbValueKind.Blob,
            DdbValueTag.Decimal => (int)DbValueKind.Decimal,
            DdbValueTag.TimestampMicros => (int)DbValueKind.Timestamp,
            DdbValueTag.TimestamptzMicros => (int)DbValueKind.Timestamp,
            DdbValueTag.IpAddr => (int)DbValueKind.Text,
            DdbValueTag.Cidr => (int)DbValueKind.Text,
            DdbValueTag.Enum => (int)DbValueKind.Text,
            DdbValueTag.Interval => (int)DbValueKind.Text,
            DdbValueTag.Date => (int)DbValueKind.Int64,
            DdbValueTag.Time => (int)DbValueKind.Int64,
            DdbValueTag.MacAddr => (int)DbValueKind.Text,
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
            DdbValueTag.TimestamptzMicros => FromUnixEpochMicrosecondsOffset(value.timestamptz_micros).ToString("O", CultureInfo.InvariantCulture),
            DdbValueTag.Enum => new DecentDBEnumValue(value.enum_type_id, value.enum_label_id).ToString(),
            DdbValueTag.IpAddr => GetIpAddressString(value),
            DdbValueTag.Cidr => GetCidrString(value),
            DdbValueTag.Date => GetDateString(value.date_days),
            DdbValueTag.Time => GetTimeString(value.time_micros),
            DdbValueTag.Interval => new DecentDBIntervalValue(value.interval_months, value.interval_days, value.interval_micros).ToString(),
            DdbValueTag.MacAddr => GetMacAddressString(value),
            _ => string.Empty
        };
    }

    private static unsafe byte[] GetBlobFromValue(DdbValueNative value)
    {
        return (DdbValueTag)value.tag switch
        {
            DdbValueTag.Blob => CopyBytes(value.data, value.len),
            DdbValueTag.Geometry => CopyBytes(value.data, value.len),
            DdbValueTag.Geography => CopyBytes(value.data, value.len),
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
        return GetDecimalValue(value).ToString();
    }

    private static decimal GetDecimalValue(DdbValueNative value)
    {
        bool isNegative = value.decimal_scaled < 0;
        ulong magnitude = isNegative
            ? unchecked((ulong)(-value.decimal_scaled))
            : unchecked((ulong)value.decimal_scaled);

        int lo = (int)(magnitude & 0xFFFFFFFF);
        int mid = (int)(magnitude >> 32);
        int hi = 0;

        return new decimal(lo, mid, hi, isNegative, value.decimal_scale);
    }

    private static DateTime FromUnixEpochMicroseconds(long micros)
        => new DateTime(micros * 10L + DateTime.UnixEpoch.Ticks, DateTimeKind.Utc);

    private static DateTimeOffset FromUnixEpochMicrosecondsOffset(long micros)
        => new DateTimeOffset(micros * 10L + DateTime.UnixEpoch.Ticks, TimeSpan.Zero);

    private static object GetDateValue(int dateDays)
    {
#if NET6_0_OR_GREATER
        var epoch = DateOnly.FromDateTime(DateTime.UnixEpoch);
        return epoch.AddDays(dateDays);
#else
        return DateTime.UnixEpoch.Date.AddDays(dateDays);
#endif
    }

    private static string GetDateString(int dateDays)
    {
#if NET6_0_OR_GREATER
        return ((DateOnly)GetDateValue(dateDays)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
#else
        return ((DateTime)GetDateValue(dateDays)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
#endif
    }

    private static object GetTimeValue(long microsAfterMidnight)
    {
        var ticks = checked(microsAfterMidnight * 10L);
#if NET6_0_OR_GREATER
        return new TimeOnly(ticks);
#else
        return TimeSpan.FromTicks(ticks);
#endif
    }

    private static string GetTimeString(long microsAfterMidnight)
    {
        var value = GetTimeValue(microsAfterMidnight);
#if NET6_0_OR_GREATER
        return ((TimeOnly)value).ToString("HH':'mm':'ss'.'FFFFFF", CultureInfo.InvariantCulture);
#else
        return ((TimeSpan)value).ToString("c", CultureInfo.InvariantCulture);
#endif
    }

    private static object GetIntervalValue(int months, int days, long micros)
    {
        var interval = new DecentDBIntervalValue(months, days, micros);
        return interval.TryAsTimeSpan(out var span) ? span : interval;
    }

    private static unsafe string GetIpAddressString(DdbValueNative value)
    {
        var family = value.ip_family;
        var len = family switch
        {
            4 => 4,
            6 => 16,
            _ => 0
        };
        if (len == 0)
        {
            return string.Empty;
        }

        var bytes = new byte[len];
        for (var i = 0; i < len; i++)
        {
            bytes[i] = value.ip_cidr_addr_bytes[i];
        }

        return new IPAddress(bytes).ToString();
    }

    private static string GetCidrString(DdbValueNative value)
    {
        var ipText = GetIpAddressString(value);
        if (ipText.Length == 0)
        {
            return string.Empty;
        }

        return $"{ipText}/{value.cidr_prefix_len.ToString(CultureInfo.InvariantCulture)}";
    }

    private static string GetMacAddressString(DdbValueNative value)
    {
        var len = value.ip_family;
        if (len != 6 && len != 8)
        {
            return string.Empty;
        }

        Span<char> chars = stackalloc char[len * 3 - 1];
        const string hex = "0123456789abcdef";
        for (var i = 0; i < len; i++)
        {
            if (i > 0)
            {
                chars[i * 3 - 1] = ':';
            }
            var b = value.ip_cidr_addr_bytes[i];
            chars[i * 3] = hex[b >> 4];
            chars[i * 3 + 1] = hex[b & 0x0f];
        }
        return new string(chars);
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
                DdbValueTag.TimestamptzMicros => value.timestamptz_micros,
                DdbValueTag.Date => value.date_days,
                DdbValueTag.Time => value.time_micros,
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

    public DecentDBException(int errorCode, string message, string sql) : base($"DecentDB error {errorCode}: {message}\nSQL: {sql}")
    {
        ErrorCode = errorCode;
        Sql = sql;
    }
}
