using System;
using System.Runtime.InteropServices;
using System.Text;

namespace DecentDb.Native;

public sealed class DecentDb : IDisposable
{
    private readonly DecentDbHandle _handle;
    private bool _disposed;

    public IntPtr Handle => _handle.Handle;

    public DecentDb(string path, string? options = null)
    {
        var pathBytes = Encoding.UTF8.GetBytes(path + "\0");
        var optBytes = options != null ? Encoding.UTF8.GetBytes(options + "\0") : Array.Empty<byte>();

        IntPtr ptr;
        unsafe
        {
            fixed (byte* pPath = pathBytes)
            fixed (byte* pOpts = optBytes)
            {
                ptr = DecentDbNativeUnsafe.decentdb_open(pPath, optBytes.Length > 0 ? pOpts : null);
            }
        }

        if (ptr == IntPtr.Zero)
        {
            var code = DecentDbNative.decentdb_last_error_code(IntPtr.Zero);
            var msg = GetErrorMessage(IntPtr.Zero);
            throw new DecentDbException(code, msg, "Open");
        }

        _handle = new DecentDbHandle(ptr);
    }

    public int LastErrorCode => DecentDbNative.decentdb_last_error_code(Handle);

    public string LastErrorMessage => GetErrorMessage(Handle);

    private static string GetErrorMessage(IntPtr db)
    {
        unsafe
        {
            var ptr = DecentDbNativeUnsafe.decentdb_last_error_message(db);
            if (ptr == null) return string.Empty;
            return Marshal.PtrToStringUTF8((IntPtr)ptr) ?? string.Empty;
        }
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
                var res = DecentDbNativeUnsafe.decentdb_prepare(Handle, pSql, out stmtPtr);
                if (res != 0)
                {
                    throw new DecentDbException(res, LastErrorMessage, sql);
                }
            }
        }
        return new PreparedStatement(this, stmtPtr);
    }

    public void Checkpoint()
    {
        using var stmt = Prepare("PRAGMA wal_checkpoint(TRUNCATE)");
    }

    internal IntPtr GetDbHandle() => Handle;
}

public sealed class PreparedStatement : IDisposable
{
    private readonly DecentDb _db;
    private readonly DecentDbStatementHandle _handle;
    private bool _disposed;
    private string _sql = string.Empty;

    public IntPtr Handle => _handle.Handle;

    internal PreparedStatement(DecentDb db, IntPtr stmtPtr)
    {
        _db = db;
        _handle = new DecentDbStatementHandle(stmtPtr);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _handle.Dispose();
    }

    public PreparedStatement Reset()
    {
        var res = DecentDbNative.decentdb_reset(Handle);
        if (res < 0)
        {
            throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement ClearBindings()
    {
        var res = DecentDbNative.decentdb_clear_bindings(Handle);
        if (res < 0)
        {
            throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindNull(int index1Based)
    {
        var res = DecentDbNativeUnsafe.decentdb_bind_null(Handle, index1Based);
        if (res < 0)
        {
            throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindInt64(int index1Based, long value)
    {
        var res = DecentDbNativeUnsafe.decentdb_bind_int64(Handle, index1Based, value);
        if (res < 0)
        {
            throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindFloat64(int index1Based, double value)
    {
        var res = DecentDbNativeUnsafe.decentdb_bind_float64(Handle, index1Based, value);
        if (res < 0)
        {
            throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindText(int index1Based, string value)
    {
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
                var res = DecentDbNativeUnsafe.decentdb_bind_text(Handle, index1Based, null, 0);
                if (res < 0)
                {
                    throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
            return this;
        }

        unsafe
        {
            fixed (byte* pBytes = bytes)
            {
                var res = DecentDbNativeUnsafe.decentdb_bind_text(Handle, index1Based, pBytes, len);
                if (res < 0)
                {
                    throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
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
                var res = DecentDbNativeUnsafe.decentdb_bind_blob(Handle, index1Based, null, 0);
                if (res < 0)
                {
                    throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
            return this;
        }

        unsafe
        {
            fixed (byte* pBytes = bytes)
            {
                var res = DecentDbNativeUnsafe.decentdb_bind_blob(Handle, index1Based, pBytes, len);
                if (res < 0)
                {
                    throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
        }
        return this;
    }

    public int Step()
    {
        return DecentDbNative.decentdb_step(Handle);
    }

    public int ColumnCount => DecentDbNative.decentdb_column_count(Handle);

    public string ColumnName(int col0Based)
    {
        unsafe
        {
            var ptr = DecentDbNativeUnsafe.decentdb_column_name(Handle, col0Based);
            if (ptr == null) return string.Empty;
            return Marshal.PtrToStringUTF8((IntPtr)ptr) ?? string.Empty;
        }
    }

    public int ColumnType(int col0Based)
    {
        return DecentDbNative.decentdb_column_type(Handle, col0Based);
    }

    public bool IsNull(int col0Based)
    {
        return DecentDbNative.decentdb_column_is_null(Handle, col0Based) != 0;
    }

    public long GetInt64(int col0Based)
    {
        return DecentDbNative.decentdb_column_int64(Handle, col0Based);
    }

    public double GetFloat64(int col0Based)
    {
        return DecentDbNative.decentdb_column_float64(Handle, col0Based);
    }

    public string GetText(int col0Based)
    {
        unsafe
        {
            var ptr = DecentDbNativeUnsafe.decentdb_column_text(Handle, col0Based, out var len);
            if (ptr == null || len == 0) return string.Empty;
            return Marshal.PtrToStringUTF8((IntPtr)ptr, len) ?? string.Empty;
        }
    }

    public byte[] GetBlob(int col0Based)
    {
        var ptr = DecentDbNative.decentdb_column_blob(Handle, col0Based, out var len);
        if (ptr == IntPtr.Zero || len == 0) return Array.Empty<byte>();
        var bytes = new byte[len];
        Marshal.Copy(ptr, bytes, 0, len);
        return bytes;
    }

    public long RowsAffected => DecentDbNative.decentdb_rows_affected(Handle);

    public RowView GetRowView()
    {
        var res = DecentDbNative.decentdb_row_view(Handle, out var valuesPtr, out var count);
        if (res < 0)
        {
            throw new DecentDbException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return new RowView(valuesPtr, count);
    }
}

public readonly struct RowView
{
    private readonly IntPtr _valuesPtr;
    private readonly int _count;

    public RowView(IntPtr valuesPtr, int count)
    {
        _valuesPtr = valuesPtr;
        _count = count;
    }

    public int Count => _count;

    public DecentdbValueView this[int index]
    {
        get
        {
            if (index < 0 || index >= _count) throw new IndexOutOfRangeException();
            var offset = IntPtr.Add(_valuesPtr, index * Marshal.SizeOf<DecentdbValueView>());
            return Marshal.PtrToStructure<DecentdbValueView>(offset);
        }
    }
}

public class DecentDbException : Exception
{
    public int ErrorCode { get; }
    public string Sql { get; }

    public DecentDbException(int errorCode, string message, string sql) : base($"DecentDB error {errorCode}: {message}")
    {
        ErrorCode = errorCode;
        Sql = sql;
    }
}
