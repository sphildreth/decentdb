using System.Runtime.InteropServices;
using System.Text;

const uint DdbOk = 0;
const uint DdbErrSql = 5;
const uint DdbValueInt64 = 1;
const uint DdbValueText = 4;

Run();

static unsafe void Run()
{
    IntPtr db = IntPtr.Zero;
    using var memoryPath = Utf8CString.FromString(":memory:");
    Check(Native.ddb_db_open_or_create(memoryPath.Pointer, ref db), "open_or_create");

    IntPtr result = IntPtr.Zero;
    using (var create = Utf8CString.FromString("CREATE TABLE items (id INT64 PRIMARY KEY, name TEXT NOT NULL)"))
    {
        Check(Native.ddb_db_execute(db, create.Pointer, IntPtr.Zero, 0, ref result), "create table");
        Check(Native.ddb_result_free(ref result), "free create result");
    }

    Check(Native.ddb_db_begin_transaction(db), "begin transaction");
    byte inTransaction = 0;
    Check(Native.ddb_db_in_transaction(db, ref inTransaction), "in_transaction");
    if (inTransaction != 1)
    {
        throw new InvalidOperationException("expected active transaction");
    }

    using (var sql = Utf8CString.FromString("INSERT INTO items (id, name) VALUES ($1, $2)"))
    using (var text = Utf8CString.FromString("Ada"))
    {
        var values = new DdbValue[2];
        values[0].Tag = DdbValueInt64;
        values[0].Int64Value = 1;
        values[1].Tag = DdbValueText;
        values[1].Data = text.Pointer;
        values[1].Len = (nuint)text.ByteLength;
        fixed (DdbValue* ptr = values)
        {
            Check(Native.ddb_db_execute(db, sql.Pointer, (IntPtr)ptr, (nuint)values.Length, ref result), "insert rollback row");
        }
        Check(Native.ddb_result_free(ref result), "free rollback insert result");
    }

    Check(Native.ddb_db_rollback_transaction(db), "rollback transaction");

    using (var sql = Utf8CString.FromString("SELECT id, name FROM items"))
    {
        Check(Native.ddb_db_execute(db, sql.Pointer, IntPtr.Zero, 0, ref result), "select after rollback");
        nuint rows = 0;
        Check(Native.ddb_result_row_count(result, ref rows), "row count after rollback");
        if (rows != 0)
        {
            throw new InvalidOperationException($"expected 0 rows after rollback, got {rows}");
        }
        Check(Native.ddb_result_free(ref result), "free rollback select result");
    }

    Check(Native.ddb_db_begin_transaction(db), "begin second transaction");
    using (var sql = Utf8CString.FromString("INSERT INTO items (id, name) VALUES ($1, $2)"))
    using (var text = Utf8CString.FromString("Grace"))
    {
        var values = new DdbValue[2];
        values[0].Tag = DdbValueInt64;
        values[0].Int64Value = 2;
        values[1].Tag = DdbValueText;
        values[1].Data = text.Pointer;
        values[1].Len = (nuint)text.ByteLength;
        fixed (DdbValue* ptr = values)
        {
            Check(Native.ddb_db_execute(db, sql.Pointer, (IntPtr)ptr, (nuint)values.Length, ref result), "insert committed row");
        }
        Check(Native.ddb_result_free(ref result), "free committed insert result");
    }

    ulong lsn = 0;
    Check(Native.ddb_db_commit_transaction(db, ref lsn), "commit transaction");
    if (lsn == 0)
    {
        throw new InvalidOperationException("expected positive commit LSN");
    }

    using (var sql = Utf8CString.FromString("SELECT id, name FROM items ORDER BY id"))
    {
        Check(Native.ddb_db_execute(db, sql.Pointer, IntPtr.Zero, 0, ref result), "select committed rows");
        nuint rows = 0;
        nuint columns = 0;
        Check(Native.ddb_result_row_count(result, ref rows), "row count");
        Check(Native.ddb_result_column_count(result, ref columns), "column count");
        if (rows != 1 || columns != 2)
        {
            throw new InvalidOperationException($"unexpected result shape rows={rows} columns={columns}");
        }

        var copied = new DdbValue();
        Check(Native.ddb_result_value_copy(result, 0, 1, ref copied), "copy text value");
        try
        {
            var text = Utf8FromValue(in copied);
            if (text != "Grace")
            {
                throw new InvalidOperationException($"unexpected copied text {text}");
            }
        }
        finally
        {
            Check(Native.ddb_value_dispose(ref copied), "dispose copied value");
        }
        Check(Native.ddb_result_free(ref result), "free select result");
    }

    var snapshotPath = Path.Combine(Path.GetTempPath(), $"decentdb-dotnet-{Guid.NewGuid():N}.ddb");
    try
    {
        using var snapshot = Utf8CString.FromString(snapshotPath);
        Check(Native.ddb_db_save_as(db, snapshot.Pointer), "save_as");
        if (!File.Exists(snapshotPath))
        {
            throw new FileNotFoundException("snapshot not created", snapshotPath);
        }
    }
    finally
    {
        if (File.Exists(snapshotPath))
        {
            File.Delete(snapshotPath);
        }
    }

    using (var sql = Utf8CString.FromString("SELECT * FROM missing_table"))
    {
        var status = Native.ddb_db_execute(db, sql.Pointer, IntPtr.Zero, 0, ref result);
        if (status != DdbErrSql)
        {
            throw new InvalidOperationException($"expected SQL error, got {status} ({GetLastError()})");
        }
        var message = GetLastError();
        if (!message.Contains("missing_table", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"unexpected error message: {message}");
        }
    }

    Check(Native.ddb_db_free(ref db), "free db");
    Check(Native.ddb_db_free(ref db), "double free db");
}

static void Check(uint status, string context)
{
    if (status != DdbOk)
    {
        throw new InvalidOperationException($"{context} failed with status {status}: {GetLastError()}");
    }
}

static string GetLastError()
{
    var ptr = Native.ddb_last_error_message();
    return ptr == IntPtr.Zero ? string.Empty : Marshal.PtrToStringUTF8(ptr) ?? string.Empty;
}

static unsafe string Utf8FromValue(in DdbValue value)
{
    if (value.Data == IntPtr.Zero || value.Len == 0)
    {
        return string.Empty;
    }

    var buffer = new byte[(int)value.Len];
    Marshal.Copy(value.Data, buffer, 0, buffer.Length);
    return Encoding.UTF8.GetString(buffer);
}

internal sealed class Utf8CString : IDisposable
{
    private Utf8CString(IntPtr pointer, int byteLength)
    {
        Pointer = pointer;
        ByteLength = byteLength;
    }

    public IntPtr Pointer { get; private set; }

    public int ByteLength { get; }

    public static Utf8CString FromString(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        var pointer = Marshal.AllocHGlobal(bytes.Length + 1);
        Marshal.Copy(bytes, 0, pointer, bytes.Length);
        Marshal.WriteByte(pointer, bytes.Length, 0);
        return new Utf8CString(pointer, bytes.Length);
    }

    public void Dispose()
    {
        if (Pointer != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(Pointer);
            Pointer = IntPtr.Zero;
        }
    }
}

[StructLayout(LayoutKind.Sequential)]
internal unsafe struct DdbValue
{
    public uint Tag;
    public byte BoolValue;
    public fixed byte Reserved0[7];
    public long Int64Value;
    public double Float64Value;
    public long DecimalScaled;
    public byte DecimalScale;
    public fixed byte Reserved1[7];
    public IntPtr Data;
    public nuint Len;
    public fixed byte UuidBytes[16];
    public long TimestampMicros;
}

internal static partial class Native
{
    [LibraryImport("decentdb")]
    internal static partial IntPtr ddb_last_error_message();

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_open_or_create(IntPtr path, ref IntPtr db);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_free(ref IntPtr db);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_execute(
        IntPtr db,
        IntPtr sql,
        IntPtr values,
        nuint valueCount,
        ref IntPtr result);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_begin_transaction(IntPtr db);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_commit_transaction(IntPtr db, ref ulong lsn);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_rollback_transaction(IntPtr db);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_in_transaction(IntPtr db, ref byte flag);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_db_save_as(IntPtr db, IntPtr destPath);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_result_free(ref IntPtr result);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_result_row_count(IntPtr result, ref nuint rowCount);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_result_column_count(IntPtr result, ref nuint columnCount);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_result_value_copy(
        IntPtr result,
        nuint rowIndex,
        nuint columnIndex,
        ref DdbValue value);

    [LibraryImport("decentdb")]
    internal static partial uint ddb_value_dispose(ref DdbValue value);
}
