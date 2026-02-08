using System;
using System.Runtime.InteropServices;
using System.Text;

namespace DecentDB.Native;

public sealed class DecentDB : IDisposable
{
    private readonly DecentDBHandle _handle;
    private bool _disposed;

    public IntPtr Handle => _handle.Handle;

    internal DecentDBHandle DbHandle => _handle;

    public DecentDB(string path, string? options = null)
    {
        var pathBytes = Encoding.UTF8.GetBytes(path + "\0");
        var optBytes = options != null ? Encoding.UTF8.GetBytes(options + "\0") : Array.Empty<byte>();

        IntPtr ptr;
        unsafe
        {
            fixed (byte* pPath = pathBytes)
            fixed (byte* pOpts = optBytes)
            {
                ptr = DecentDBNativeUnsafe.decentdb_open(pPath, optBytes.Length > 0 ? pOpts : null);
            }
        }

        if (ptr == IntPtr.Zero)
        {
            var code = DecentDBNative.decentdb_last_error_code(IntPtr.Zero);
            var msg = GetErrorMessage(IntPtr.Zero);
            throw new DecentDBException(code, msg, "Open");
        }

        _handle = new DecentDBHandle(ptr);
    }

    public int LastErrorCode => DecentDBNative.decentdb_last_error_code(Handle);

    public string LastErrorMessage => GetErrorMessage(Handle);

    private static string GetErrorMessage(IntPtr db)
    {
        unsafe
        {
            var ptr = DecentDBNativeUnsafe.decentdb_last_error_message(db);
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
                var res = DecentDBNativeUnsafe.decentdb_prepare(Handle, pSql, out stmtPtr);
                if (res != 0)
                {
                    throw new DecentDBException(res, LastErrorMessage, sql);
                }
            }
        }
        return new PreparedStatement(this, stmtPtr);
    }

    public void Checkpoint()
    {
        var res = DecentDBNative.decentdb_checkpoint(Handle);
        if (res != 0)
        {
            throw new DecentDBException(res, LastErrorMessage, "Checkpoint");
        }
    }

    internal IntPtr GetDbHandle() => Handle;
}

public sealed class PreparedStatement : IDisposable
{
    private readonly DecentDB _db;
    private readonly DecentDBStatementHandle _handle;
    private bool _disposed;
    private string _sql = string.Empty;

    public IntPtr Handle => _handle.Handle;

    internal PreparedStatement(DecentDB db, IntPtr stmtPtr)
    {
        _db = db;
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
        var res = DecentDBNative.decentdb_reset(Handle);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement ClearBindings()
    {
        var res = DecentDBNative.decentdb_clear_bindings(Handle);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindNull(int index1Based)
    {
        var res = DecentDBNativeUnsafe.decentdb_bind_null(Handle, index1Based);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindInt64(int index1Based, long value)
    {
        var res = DecentDBNativeUnsafe.decentdb_bind_int64(Handle, index1Based, value);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindFloat64(int index1Based, double value)
    {
        var res = DecentDBNativeUnsafe.decentdb_bind_float64(Handle, index1Based, value);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindBool(int index1Based, bool value)
    {
        var res = DecentDBNativeUnsafe.decentdb_bind_bool(Handle, index1Based, value ? 1 : 0);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
        }
        return this;
    }

    public PreparedStatement BindGuid(int index1Based, Guid value)
    {
        unsafe
        {
            var bytes = stackalloc byte[16];
            if (!value.TryWriteBytes(new Span<byte>(bytes, 16)))
                throw new InvalidOperationException("Failed to write Guid bytes");
            
            var res = DecentDBNativeUnsafe.decentdb_bind_blob(Handle, index1Based, bytes, 16);
            if (res < 0)
            {
                throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
            }
        }
        return this;
    }

    public PreparedStatement BindDecimal(int index1Based, decimal value)
    {
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
        
        var res = DecentDBNativeUnsafe.decentdb_bind_decimal(Handle, index1Based, unscaled, scale);
        if (res < 0)
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
                var res = DecentDBNativeUnsafe.decentdb_bind_text(Handle, index1Based, null, 0);
                if (res < 0)
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
                var res = DecentDBNativeUnsafe.decentdb_bind_text(Handle, index1Based, pBytes, len);
                if (res < 0)
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
                var res = DecentDBNativeUnsafe.decentdb_bind_blob(Handle, index1Based, null, 0);
                if (res < 0)
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
                var res = DecentDBNativeUnsafe.decentdb_bind_blob(Handle, index1Based, pBytes, len);
                if (res < 0)
                {
                    throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
                }
            }
        }
        return this;
    }

    public int Step()
    {
        return DecentDBNative.decentdb_step(Handle);
    }

    public int ColumnCount => DecentDBNative.decentdb_column_count(Handle);

    public string ColumnName(int col0Based)
    {
        unsafe
        {
            var ptr = DecentDBNativeUnsafe.decentdb_column_name(Handle, col0Based);
            if (ptr == null) return string.Empty;
            return Marshal.PtrToStringUTF8((IntPtr)ptr) ?? string.Empty;
        }
    }

    public int ColumnType(int col0Based)
    {
        return DecentDBNative.decentdb_column_type(Handle, col0Based);
    }

    public bool IsNull(int col0Based)
    {
        return DecentDBNative.decentdb_column_is_null(Handle, col0Based) != 0;
    }

    public bool GetBool(int col0Based)
    {
        // Check if actually boolean or int64?
        // Native returns INT64 for BOOL type in C API (row_view returns kind=2 bool, val=0/1).
        // decentdb_column_int64 returns 0/1.
        // We can just check != 0.
        return GetInt64(col0Based) != 0;
    }
    
    public Guid GetGuid(int col0Based)
    {
        unsafe
        {
            // Try to get as blob first, as UUIDs are stored as blobs
            int len;
            var ptr = (byte*)DecentDBNative.decentdb_column_blob(Handle, col0Based, out len);
            
            if (ptr != null && len == 16)
            {
                return new Guid(new ReadOnlySpan<byte>(ptr, 16));
            }
            
            // Fallback: check text if blob failed or length mismatch (e.g. legacy text UUIDs?)
            // Although ADR 0091 says text will no longer be accepted for ctUuid, 
            // we might still have text columns that contain UUID strings.
            ptr = DecentDBNativeUnsafe.decentdb_column_text(Handle, col0Based, out len);
            if (ptr != null && len > 0)
            {
                var s = new string((sbyte*)ptr, 0, len, System.Text.Encoding.UTF8);
                if (Guid.TryParse(s, out var g)) return g;
            }
            
            return Guid.Empty;
        }
    }

    public long GetInt64(int col0Based)
    {
        return DecentDBNative.decentdb_column_int64(Handle, col0Based);
    }

    public double GetFloat64(int col0Based)
    {
        return DecentDBNative.decentdb_column_float64(Handle, col0Based);
    }

    public decimal GetDecimal(int col0Based)
    {
        long unscaled = DecentDBNativeUnsafe.decentdb_column_decimal_unscaled(Handle, col0Based);
        int scale = DecentDBNativeUnsafe.decentdb_column_decimal_scale(Handle, col0Based);
        // decimal(int lo, int mid, int hi, bool isNegative, byte scale)
        bool isNegative = unscaled < 0;
        ulong u = isNegative ? (ulong)(-unscaled) : (ulong)unscaled;
        
        int lo = (int)(u & 0xFFFFFFFF);
        int mid = (int)(u >> 32);
        int hi = 0;
        
        return new decimal(lo, mid, hi, isNegative, (byte)scale);
    }

    public string GetText(int col0Based)
    {
        unsafe
        {
            var ptr = DecentDBNativeUnsafe.decentdb_column_text(Handle, col0Based, out var len);
            if (ptr == null || len == 0) return string.Empty;
            return Marshal.PtrToStringUTF8((IntPtr)ptr, len) ?? string.Empty;
        }
    }

    public byte[] GetBlob(int col0Based)
    {
        var ptr = DecentDBNative.decentdb_column_blob(Handle, col0Based, out var len);
        if (ptr == IntPtr.Zero || len == 0) return Array.Empty<byte>();
        var bytes = new byte[len];
        Marshal.Copy(ptr, bytes, 0, len);
        return bytes;
    }

    public long RowsAffected => DecentDBNative.decentdb_rows_affected(Handle);

    public RowView GetRowView()
    {
        var res = DecentDBNative.decentdb_row_view(Handle, out var valuesPtr, out var count);
        if (res < 0)
        {
            throw new DecentDBException(_db.LastErrorCode, _db.LastErrorMessage, _sql);
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
